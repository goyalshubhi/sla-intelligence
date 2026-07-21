# src/engine.py
# All intelligence logic for PayWatch.
# lambda_processor.py is a thin orchestrator that imports from here.
# Nothing in this file reads from or writes to any external system directly —
# it only transforms DataFrames. That separation makes every function testable.

import sys, os
sys.path.insert(0, os.path.dirname(__file__))

import pandas as pd, networkx as nx, boto3, json
from datetime import datetime, timedelta
from job_config import JOBS, IMPACT_WEIGHT

BUCKET   = "sla-intelligence-shubhangi"
REGION   = "eu-north-1"
MIN_RUNS = 30  # z-scores are meaningless with fewer than 30 samples

# Regulatory deadlines — RBI/NPCI operational guidelines
# Source: RBI Payment and Settlement Systems Circular DPSS.CO.OD.No.1852/06.08.005/2020-21
# and NPCI Settlement Operating Procedures (published on npci.org.in)
DEADLINES = [
    {"name": "NEFT_EOD_Cutoff",  "hour": 19, "minute":  0,
     "jobs": ["settlement_intraday", "upi_reconcile", "fraud_signal_agg"]},
    {"name": "EOD_Settlement",   "hour": 23, "minute":  0,
     "jobs": ["settlement_eod", "settlement_reconcile", "nostro_reconcile"]},
    {"name": "RBI_Daily_Return", "hour": 23, "minute": 59,
     "jobs": ["rbi_report_gen", "audit_log_archive", "pci_data_mask"]},
]

_LOOKUP = {j["name"]: j for j in JOBS}


def compute_metrics(df):
    """Per-job statistics: avg, p95, stddev, z-scores. Returns one row per job."""
    rows = []
    for job, grp in df.groupby("job_name"):
        latest = grp.sort_values("start_time").iloc[-1]
        n = len(grp)
        base = {"job_name": job, "team": latest["team"], "impact": latest["impact"],
                "sla_minutes": latest["sla_minutes"], "total_runs": n,
                "latest_duration": round(float(latest["duration_minutes"]), 2),
                "latest_rows": int(latest["rows_processed"]),
                "latest_status": latest["status"]}
        if n < MIN_RUNS:
            rows.append({**base, "avg_duration": None, "p95_duration": None,
                         "stddev_duration": None, "avg_rows": None,
                         "time_drift_score": None, "volume_drift_score": None,
                         "priority_score": None})
            continue
        avg_d = grp["duration_minutes"].mean()
        std_d = grp["duration_minutes"].std()
        avg_r = grp["rows_processed"].mean()
        std_r = grp["rows_processed"].std()
        td = round((float(latest["duration_minutes"]) - avg_d) / std_d, 3) if std_d > 0 else 0.0
        vd = round((float(latest["rows_processed"])   - avg_r) / std_r, 3) if std_r > 0 else 0.0
        rows.append({**base, "avg_duration": round(avg_d, 2),
                     "p95_duration": round(grp["duration_minutes"].quantile(0.95), 2),
                     "stddev_duration": round(std_d, 2), "avg_rows": round(avg_r, 2),
                     "time_drift_score": td, "volume_drift_score": vd,
                     "priority_score": round(abs(td) * IMPACT_WEIGHT.get(latest["impact"], 1), 2)})
    return pd.DataFrame(rows)


def compute_verdicts(metrics_df):
    """Classify each job: safe / at_risk / breached / insufficient_data."""
    def _v(row):
        if row["time_drift_score"] is None:
            return "insufficient_data"
        if row["latest_duration"] > row["sla_minutes"]:
            return "breached"
        if (row["time_drift_score"] or 0) > 1.5 or (row["volume_drift_score"] or 0) < -1.5:
            return "at_risk"
        return "safe"
    df = metrics_df.copy()
    df["verdict"] = df.apply(_v, axis=1)
    return df


def compute_fingerprint(metrics_df, cw_signals, gh_signals):
    """Classify WHY anomalous jobs are slow using 5 failure signatures."""
    G = nx.DiGraph()
    for j in JOBS:
        G.add_node(j["name"])
        for d in j["depends_on"]:
            G.add_edge(d, j["name"])

    anomalous = metrics_df[metrics_df["verdict"].isin(["breached", "at_risk"])]
    if anomalous.empty:
        return pd.DataFrame()

    lookup = metrics_df.set_index("job_name").to_dict("index")
    slow   = set(metrics_df[metrics_df["time_drift_score"].fillna(0) > 1.5]["job_name"])
    run_times = [datetime.fromisoformat(g["created_at"].replace("Z", "+00:00")).replace(tzinfo=None)
                 for g in gh_signals if "created_at" in g and g.get("conclusion") == "failure"]

    rows = []
    for _, r in anomalous.iterrows():
        job = r["job_name"]
        td  = r["time_drift_score"] or 0
        vd  = r["volume_drift_score"] or 0

        if vd < -1.5 and abs(td) < 0.5:
            ftype, stat_e, action, route, score, ext_e = (
                "data_quality_issue", f"Volume drift {round(vd,2)}σ — time drift normal",
                "Check upstream data source, not a pipeline bug", "ingestion_team", 70, "none")

        elif (len([j for j in slow if j != job and not nx.has_path(G, job, j)
                   and not nx.has_path(G, j, job)]) >= 2
              and (cw_signals["s3_errors"] > 0 or cw_signals["lambda_errors"] > 0)):
            ftype, stat_e, action, route, score = (
                "infrastructure_issue", f"{len(slow)-1} unrelated jobs slow simultaneously",
                "Route to DevOps — do NOT page pipeline engineers", "devops_team", 85)
            ext_e = f"S3 5xx: {cw_signals['s3_errors']} | Lambda errors: {cw_signals['lambda_errors']}"

        elif job in G and any(a in slow for a in nx.ancestors(G, job)):
            anc  = [a for a in nx.ancestors(G, job) if a in slow]
            root = max(anc, key=lambda a: len(nx.ancestors(G, a)) if a in G else 0)
            try:    chain = " → ".join(nx.shortest_path(G, root, job))
            except: chain = f"{root} → {job}"
            ftype, stat_e, action, route, score, ext_e = (
                "cascade_failure", f"Root: {root} | Chain: {chain}",
                f"Fix {root} first — this job recovers automatically",
                lookup.get(root, {}).get("team", r["team"]), 75, "none")

        elif (vd > 1.5 and td > 1.5 and r["avg_duration"]
              and abs((r["latest_duration"]/r["avg_duration"]) - (r["latest_rows"]/r["avg_rows"])) < 0.3):
            ftype, stat_e, action, route, score, ext_e = (
                "volume_spike", "Duration and rows up proportionally — expected linear scaling",
                "Monitor — will self-resolve as volume normalises", "ingestion_team", 65, "none")

        else:
            score, ext_e = 40, "none"
            if run_times:
                mins = min(abs((datetime.utcnow()-t).total_seconds()/60) for t in run_times)
                if mins < 30:
                    score += 25 if (cw_signals["lambda_errors"] > 0 and mins < 5) else 35
                    ext_e = f"Failed Actions run {round(mins,1)}min ago"
            ftype, stat_e, action, route = (
                "logic_bug", f"Isolated: time drift {round(td,2)}σ — dependencies healthy",
                "Check recent deployments and job logs", r["team"])

        rows.append({"job_name": job, "team": r["team"], "verdict": r["verdict"],
                     "fingerprint_type": ftype, "evidence_score": min(100, score),
                     "statistical_evidence": stat_e, "external_evidence": ext_e,
                     "route_to_team": route, "recommended_action": action,
                     "detected_at": datetime.now().isoformat()})
    return pd.DataFrame(rows)


def compute_sla_health(df, metrics_df):
    """Is each SLA contract still realistic given current volume trends?"""
    rows = []
    for _, m in metrics_df.iterrows():
        if m["avg_duration"] is None:
            continue
        runs  = df[df["job_name"] == m["job_name"]].sort_values("start_time")
        split = max(1, len(runs) // 5)
        # Volume growth = compare first 20% of runs to last 20%
        growth = ("growing"   if runs.iloc[-split:]["rows_processed"].mean() > runs.iloc[:split]["rows_processed"].mean() * 1.1
                  else "shrinking" if runs.iloc[-split:]["rows_processed"].mean() < runs.iloc[:split]["rows_processed"].mean() * 0.9
                  else "stable")
        tightness = round(m["avg_duration"] / m["sla_minutes"], 3)
        rows.append({"job_name": m["job_name"], "team": m["team"], "impact": m["impact"],
                     "tightness_score": tightness, "volume_growth": growth,
                     "avg_duration": m["avg_duration"], "sla_minutes": m["sla_minutes"],
                     "recommended_sla": round(m["sla_minutes"] * 1.3) if tightness > 0.8 else m["sla_minutes"]})
    return pd.DataFrame(rows)


def compute_deadline_risk(metrics_df):
    """Can we make RBI/NPCI regulatory deadlines given current job paces?"""
    now    = datetime.now()
    lookup = metrics_df.set_index("job_name").to_dict("index")
    rows   = []
    for dl in DEADLINES:
        deadline = now.replace(hour=dl["hour"], minute=dl["minute"], second=0, microsecond=0)
        if deadline < now:
            deadline += timedelta(days=1)
        buffer_mins = (deadline - now).total_seconds() / 60
        critical = [j for j in dl["jobs"] if lookup.get(j, {}).get("verdict") in ("at_risk", "breached")]
        rows.append({"deadline_name": dl["name"], "deadline_time": deadline.isoformat(),
                     "buffer_minutes": round(buffer_mins, 1), "critical_jobs": ",".join(critical),
                     "risk_level": "high" if (buffer_mins < 30 or critical) else ("medium" if buffer_mins < 90 else "low"),
                     "checked_at": now.isoformat()})
    return pd.DataFrame(rows)


def write_s3_append(df, prefix):
    """Append DataFrame to S3 in date-partitioned path (controls Athena scan cost)."""
    if df.empty:
        return
    s3  = boto3.client("s3", region_name=REGION)
    now = datetime.now()
    key = f"{prefix}/year={now.year}/month={now.month:02d}/day={now.day:02d}/part_{now.strftime('%H%M%S')}.json"
    s3.put_object(Bucket=BUCKET, Key=key, Body=df.to_json(orient="records", date_format="iso").encode())
    print(f"  Appended → s3://{BUCKET}/{key}  ({len(df)} rows)")


def write_s3_overwrite(df, prefix):
    """Atomic overwrite via staging swap — if write fails, old data is untouched."""
    if df.empty:
        return
    s3      = boto3.client("s3", region_name=REGION)
    staging = f"{prefix}_staging/current.json"
    final   = f"{prefix}/current.json"
    s3.put_object(Bucket=BUCKET, Key=staging, Body=df.to_json(orient="records", date_format="iso").encode())
    s3.copy_object(Bucket=BUCKET, CopySource={"Bucket": BUCKET, "Key": staging}, Key=final)
    s3.delete_object(Bucket=BUCKET, Key=staging)
    print(f"  Overwrite → s3://{BUCKET}/{final}  ({len(df)} rows)")
