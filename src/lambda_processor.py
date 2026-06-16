# lambda_processor.py
# ---------------------------------------------------------------
# PayWatch — Core Processing Logic
# Runs every 15 minutes (locally now, AWS Lambda later)
#
# What it does in order:
# 1. Read raw events from S3
# 2. Pull CloudWatch + GitHub signals
# 3. Check data sufficiency (need 30+ runs per job)
# 4. Compute per-job metrics (avg, stddev, p95)
# 5. Z-score drift detection (time + volume)
# 6. SLA verdict (safe / at_risk / breached)
# 7. Anomaly fingerprinting (5 failure types)
# 8. Write partitioned output to S3
# ---------------------------------------------------------------

import boto3, pandas as pd, json, os, requests
from datetime import datetime, timedelta

# ── CONFIG ──────────────────────────────────────────────────────
BUCKET         = "sla-intelligence-shubhangi"
REGION         = "eu-north-1"
GITHUB_OWNER   = "goyalshubhi"     
GITHUB_REPO    = "sla-intelligence"
GITHUB_TOKEN   = os.environ.get("GITHUB_TOKEN", "")
MIN_RUNS       = 30   # minimum runs before z-score is meaningful
RAW_PREFIX       = "raw/pipeline_events/"
SILVER           = "processed/silver_job_runs"
GOLD_METRICS     = "processed/gold_job_metrics"
GOLD_FINGERPRINT = "processed/gold_fingerprint"

s3  = boto3.client("s3",          region_name=REGION)
cw  = boto3.client("cloudwatch",  region_name=REGION)


# ── STEP 1: READ RAW EVENTS ─────────────────────────────────────
def read_events():
    files = s3.list_objects_v2(Bucket=BUCKET, Prefix=RAW_PREFIX).get("Contents", [])
    rows  = []
    for f in files:
        if f["Key"].endswith("/"): continue
        body = s3.get_object(Bucket=BUCKET, Key=f["Key"])["Body"].read()
        rows.extend(json.loads(body))
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["start_time"]       = pd.to_datetime(df["start_time"])
    df["duration_minutes"] = df["duration_minutes"].astype(float)
    df["rows_processed"]   = df["rows_processed"].astype(int)
    print(f"  Loaded {len(df)} events across {df['job_name'].nunique()} jobs")
    return df


# ── STEP 2: PULL CLOUDWATCH SIGNALS ─────────────────────────────
# Real metrics from your actual AWS resources
# S3 5xx errors catches storage/network issues CPU alone misses
def pull_cloudwatch():
    signals = {"s3_errors": 0, "lambda_errors": 0}
    try:
        window = {"StartTime": datetime.utcnow() - timedelta(minutes=30),
                  "EndTime":   datetime.utcnow(),
                  "Period":    1800, "Statistics": ["Sum"]}
        for metric, ns, dims, key in [
            ("5xxErrors", "AWS/S3", [{"Name":"BucketName","Value":BUCKET},
                                     {"Name":"FilterId","Value":"EntireBucket"}],
             "s3_errors"),
            ("Errors", "AWS/Lambda", [], "lambda_errors")
        ]:
            pts = cw.get_metric_statistics(
                Namespace=ns, MetricName=metric,
                Dimensions=dims, **window
            )["Datapoints"]
            if pts:
                signals[key] = pts[0]["Sum"]
    except Exception as e:
        print(f"  CloudWatch unavailable: {e}")
    print(f"  CloudWatch → S3 errors: {signals['s3_errors']}, "
          f"Lambda errors: {signals['lambda_errors']}")
    return signals


# ── STEP 3: PULL GITHUB SIGNALS ─────────────────────────────────
# Real commits from your repo
# Recent commit + isolated job slowdown = logic bug fingerprint
def pull_github():
    if not GITHUB_TOKEN:
        print("  GitHub token not set — skipping")
        return []
    try:
        since = (datetime.utcnow() - timedelta(hours=2)).isoformat() + "Z"
        res = requests.get(
            f"https://api.github.com/repos/{GITHUB_OWNER}/{GITHUB_REPO}/commits",
            headers={"Authorization": f"token {GITHUB_TOKEN}"},
            params={"since": since, "per_page": 10}
        )
        commits = [{"message": c["commit"]["message"],
                    "author":  c["commit"]["author"]["name"],
                    "time":    c["commit"]["author"]["date"]}
                   for c in res.json()] if res.status_code == 200 else []
        print(f"  GitHub → {len(commits)} recent commits")
        return commits
    except Exception as e:
        print(f"  GitHub unavailable: {e}")
        return []


# ── STEP 4-6: METRICS + DRIFT + VERDICT ─────────────────────────
def compute_metrics(df):
    results = []
    for job, grp in df.groupby("job_name"):
        latest = grp.sort_values("start_time").iloc[-1]
        runs   = len(grp)
        base   = {"job_name": job, "team": latest["team"],
                  "impact": latest["impact"],
                  "sla_minutes": latest["sla_minutes"],
                  "total_runs": runs,
                  "latest_duration": round(latest["duration_minutes"], 2),
                  "latest_rows": int(latest["rows_processed"]),
                  "latest_status": latest["status"]}

        # Data sufficiency gate
        # Z-scores are meaningless below 30 data points
        # Every real monitoring system has a warm-up period
        if runs < MIN_RUNS:
            results.append({**base, "verdict": "insufficient_data",
                            "time_drift_score": None,
                            "volume_drift_score": None,
                            "priority_score": None,
                            "avg_duration": None, "p95_duration": None,
                            "stddev_duration": None, "avg_rows": None})
            continue

        avg_d = grp["duration_minutes"].mean()
        std_d = grp["duration_minutes"].std()
        avg_r = grp["rows_processed"].mean()
        std_r = grp["rows_processed"].std()

        # Z-score: (latest - mean) / stddev
        # How many standard deviations from normal?
        td = round((latest["duration_minutes"] - avg_d) / std_d, 3) if std_d > 0 else 0.0
        vd = round((latest["rows_processed"]   - avg_r) / std_r, 3) if std_r > 0 else 0.0

        verdict = ("breached" if latest["duration_minutes"] > latest["sla_minutes"]
                   else "at_risk" if td > 1.5 or vd < -1.5
                   else "safe")

        weight   = {"high": 3, "medium": 2, "low": 1}.get(latest["impact"], 1)
        priority = round(abs(td) * weight, 2)

        results.append({**base, "verdict": verdict,
                        "avg_duration": round(avg_d, 2),
                        "p95_duration": round(grp["duration_minutes"].quantile(0.95), 2),
                        "stddev_duration": round(std_d, 2),
                        "avg_rows": round(avg_r, 2),
                        "time_drift_score": td,
                        "volume_drift_score": vd,
                        "priority_score": priority})
    return pd.DataFrame(results)


# ── STEP 7: ANOMALY FINGERPRINTING ──────────────────────────────
# Classify WHY a job is slow using 5 failure signatures
# Checked in priority order
# External signals (CloudWatch, GitHub) add evidence score
def fingerprint(metrics_df, cw_signals, gh_commits):
    import networkx as nx
    from job_config import JOBS

    # Build dependency graph once
    G = nx.DiGraph()
    for j in JOBS: G.add_node(j["name"])
    for j in JOBS:
        for d in j["depends_on"]: G.add_edge(d, j["name"])

    anomalous = metrics_df[metrics_df["verdict"].isin(["breached", "at_risk"])]
    if anomalous.empty: return pd.DataFrame()

    lookup = metrics_df.set_index("job_name").to_dict("index")
    slow   = set(metrics_df[metrics_df["time_drift_score"].fillna(0) > 1.5]["job_name"])

    # Parse recent commit times for logic bug detection
    commit_times = []
    for c in gh_commits:
        try:
            commit_times.append(
                datetime.fromisoformat(
                    c["time"].replace("Z", "+00:00")
                ).replace(tzinfo=None)
            )
        except: pass

    rows = []
    for _, r in anomalous.iterrows():
        job = r["job_name"]
        td  = r["time_drift_score"] or 0
        vd  = r["volume_drift_score"] or 0

        # 1. DATA QUALITY — volume dropped, time normal
        #    Job finished fast with fewer rows = missing upstream data
        if vd < -1.5 and abs(td) < 0.5:
            ftype, stat_e, action, route, score = (
                "data_quality_issue",
                f"Volume drift {round(vd,2)}σ | Time drift normal",
                "Check upstream data source — not a pipeline bug",
                "ingestion_team", 70
            )
            ext_e = "none"

        # 2. INFRASTRUCTURE — unrelated jobs slow + CloudWatch errors
        #    S3 5xx errors catches network/storage issues CPU misses
        elif (len([j for j in slow if j != job
                   and not nx.has_path(G, job, j)
                   and not nx.has_path(G, j, job)]) >= 2
              and (cw_signals["s3_errors"] > 0
                   or cw_signals["lambda_errors"] > 0)):
            ftype, stat_e, action, route, score = (
                "infrastructure_issue",
                f"{len(slow)-1} unrelated jobs slow simultaneously",
                "Route to DevOps — do NOT page pipeline engineers",
                "devops_team", 85
            )
            ext_e = (f"S3 5xx: {cw_signals['s3_errors']} | "
                     f"Lambda errors: {cw_signals['lambda_errors']}")

        # 3. CASCADE — slowness follows the dependency graph path
        elif job in G and any(a in slow for a in nx.ancestors(G, job)):
            slow_anc = [a for a in nx.ancestors(G, job) if a in slow]
            root = max(slow_anc, key=lambda a: len(nx.ancestors(G, a)) if a in G else 0)
            try:    chain = " → ".join(nx.shortest_path(G, root, job))
            except: chain = f"{root} → {job}"
            ftype, stat_e, action, route, score = (
                "cascade_failure",
                f"Root: {root} | Chain: {chain}",
                f"Fix {root} first — this job recovers automatically",
                lookup.get(root, {}).get("team", r["team"]), 75
            )
            ext_e = "none"

        # 4. VOLUME SPIKE — duration and rows up proportionally
        #    Expected linear scaling — will self-resolve
        elif (vd > 1.5 and td > 1.5 and r["avg_duration"]
              and abs((r["latest_duration"] / r["avg_duration"])
                      - (r["latest_rows"] / r["avg_rows"])) < 0.3):
            ftype, stat_e, action, route, score = (
                "volume_spike",
                f"Duration and rows both up proportionally",
                "Monitor — will self-resolve",
                "ingestion_team", 65
            )
            ext_e = "none"

        # 5. LOGIC BUG — isolated slowness, healthy dependencies
        #    GitHub commit within 30 min = strong evidence
        else:
            score = 40
            ext_e = "none"
            if commit_times:
                mins_ago = min(
                    abs((datetime.utcnow() - t).total_seconds() / 60)
                    for t in commit_times
                )
                if mins_ago < 30:
                    # Signal independence check
                    # If CloudWatch also fired within 5 min they share root cause
                    # Don't double count correlated signals
                    if cw_signals["lambda_errors"] > 0 and mins_ago < 5:
                        score += 25
                        ext_e = f"GitHub commit {round(mins_ago,1)}min ago (correlated with CloudWatch — not double counted)"
                    else:
                        score += 35
                        ext_e = f"GitHub commit {round(mins_ago,1)}min ago"

            ftype, stat_e, action, route = (
                "logic_bug",
                f"Isolated slowness: time drift {round(td,2)}σ | Dependencies healthy",
                "Check recent deployments and job logs",
                r["team"]
            )

        rows.append({
            "job_name":             job,
            "team":                 r["team"],
            "verdict":              r["verdict"],
            "fingerprint_type":     ftype,
            "evidence_score":       min(100, score),
            "statistical_evidence": stat_e,
            "external_evidence":    ext_e,
            "route_to_team":        route,
            "recommended_action":   action,
            "detected_at":          datetime.now().isoformat()
        })

    return pd.DataFrame(rows)


# ── STEP 8: WRITE TO S3 ──────────────────────────────────────────
# Silver: append + partitioned by date (controls Athena scan cost)
# Gold:   staging swap (atomicity guarantee on failure)
def write_s3(df, prefix, mode="overwrite"):
    if df.empty: return
    now  = datetime.now()
    body = df.to_json(orient="records", date_format="iso").encode()

    if mode == "append":
        key = (f"{prefix}/year={now.year}/month={now.month:02d}"
               f"/day={now.day:02d}/part_{now.strftime('%H%M%S')}.json")
        s3.put_object(Bucket=BUCKET, Key=key, Body=body)
    else:
        staging = f"{prefix}_staging/current.json"
        final   = f"{prefix}/current.json"
        s3.put_object(Bucket=BUCKET, Key=staging, Body=body)
        s3.copy_object(Bucket=BUCKET,
                       CopySource={"Bucket": BUCKET, "Key": staging},
                       Key=final)
        s3.delete_object(Bucket=BUCKET, Key=staging)

    print(f"  Written → s3://{BUCKET}/{prefix}")


# ── MAIN ─────────────────────────────────────────────────────────
def handler(event=None, context=None):
    print(f"\n{'='*50}")
    print(f"PayWatch run: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*50}")

    df = read_events()
    if df.empty:
        print("No events found. Is generate_events.py running?")
        return

    cw_signals = pull_cloudwatch()
    gh_commits = pull_github()
    metrics_df = compute_metrics(df)
    fp_df      = fingerprint(metrics_df, cw_signals, gh_commits)

    write_s3(df,         SILVER,           mode="append")
    write_s3(metrics_df, GOLD_METRICS,     mode="overwrite")
    write_s3(fp_df,      GOLD_FINGERPRINT, mode="overwrite")

    v = metrics_df["verdict"].value_counts()
    print(f"\nResults: {v.get('safe',0)} safe | "
          f"{v.get('at_risk',0)} at risk | "
          f"{v.get('breached',0)} breached | "
          f"{v.get('insufficient_data',0)} warming up")

    if not fp_df.empty:
        print("\nActive anomalies:")
        for _, r in fp_df.iterrows():
            print(f"  {r['job_name']:30} {r['fingerprint_type']:20} "
                  f"score={r['evidence_score']}")

if __name__ == "__main__":
    handler()
