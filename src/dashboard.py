# src/dashboard.py
# PayWatch 5-page Streamlit dashboard.
# Run: py -3.11 -m streamlit run src/dashboard.py
# Reads all data from Athena via boto3. Auto-refreshes every 30 seconds.

import sys, os
sys.path.insert(0, os.path.dirname(__file__))

import boto3, json, time, pandas as pd
import streamlit as st
from datetime import datetime

BUCKET  = "sla-intelligence-shubhangi"
REGION  = "eu-north-1"
DB      = "sla_intelligence_db"
OUTPUT  = f"s3://{BUCKET}/athena_results/"
MIN_RUNS_FOR_ZSCORE    = 30
MIN_DAYS_FOR_PROJECTION = 14


# ── ATHENA QUERY HELPER ──────────────────────────────────────────────────────
@st.cache_data(ttl=30)
def query(sql):
    """Run SQL on Athena. Cached for 30s so refresh doesn't spam the API."""
    ath  = boto3.client("athena", region_name=REGION)
    resp = ath.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": DB},
        ResultConfiguration={"OutputLocation": OUTPUT}
    )
    qid = resp["QueryExecutionId"]
    for _ in range(30):
        state = ath.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]["State"]
        if state == "SUCCEEDED":
            break
        if state in ("FAILED", "CANCELLED"):
            return pd.DataFrame()
        time.sleep(2)
    rows    = ath.get_query_results(QueryExecutionId=qid)["ResultSet"]["Rows"]
    if len(rows) < 2:
        return pd.DataFrame()
    headers = [c["VarCharValue"] for c in rows[0]["Data"]]
    data    = [{headers[i]: col.get("VarCharValue", "") for i, col in enumerate(r["Data"])}
               for r in rows[1:]]
    return pd.DataFrame(data)


# ── PAGE CONFIG ──────────────────────────────────────────────────────────────
st.set_page_config(page_title="PayWatch", page_icon="🔍", layout="wide")
st.title("PayWatch — Pipeline Observability")
st.caption(f"Auto-refreshes every 30s | Last refresh: {datetime.now().strftime('%H:%M:%S')}")

page = st.sidebar.radio("Navigate", [
    "Pipeline Health Heatmap",
    "Anomaly Fingerprint",
    "Regulatory Deadline Countdown",
    "SLA Health Report",
    "Incident Log"
])


# ── PAGE 1: HEATMAP ──────────────────────────────────────────────────────────
if page == "Pipeline Health Heatmap":
    st.header("Pipeline Health — All 50 Jobs")
    df = query("SELECT job_name, team, verdict, priority_score, latest_duration, sla_minutes, total_runs FROM gold_job_metrics ORDER BY verdict DESC, priority_score DESC")

    if df.empty or df["verdict"].eq("insufficient_data").all():
        total = len(df) if not df.empty else 0
        warmed = (df["verdict"] != "insufficient_data").sum() if not df.empty else 0
        st.info(f"Warming up — {warmed}/{total} jobs have enough runs for z-score analysis. Need {MIN_RUNS_FOR_ZSCORE}+ runs per job (~7–8 hours at 15-min schedule).")
        if not df.empty:
            st.dataframe(df[["job_name","team","total_runs","verdict"]].rename(columns={"total_runs":"runs_so_far"}))
    else:
        COLOURS = {"breached": "🔴", "at_risk": "🟡", "safe": "🟢", "insufficient_data": "⚫"}
        df["status"] = df["verdict"].map(COLOURS)
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Breached",    (df["verdict"]=="breached").sum())
        col2.metric("At Risk",     (df["verdict"]=="at_risk").sum())
        col3.metric("Safe",        (df["verdict"]=="safe").sum())
        col4.metric("Warming Up",  (df["verdict"]=="insufficient_data").sum())
        st.dataframe(df[["status","job_name","team","verdict","latest_duration","sla_minutes","priority_score"]],
                     use_container_width=True, hide_index=True)
        sel = st.selectbox("Drill into a job", ["—"] + sorted(df["job_name"].tolist()))
        if sel != "—":
            row = df[df["job_name"]==sel].iloc[0]
            c1, c2, c3 = st.columns(3)
            c1.metric("Duration", f"{row['latest_duration']} min", delta=f"SLA: {row['sla_minutes']} min")
            c2.metric("Verdict",  row["verdict"])
            c3.metric("Priority", row["priority_score"])


# ── PAGE 2: FINGERPRINT ──────────────────────────────────────────────────────
elif page == "Anomaly Fingerprint":
    st.header("Anomaly Fingerprints — Active Incidents")
    df = query("SELECT * FROM gold_fingerprint ORDER BY evidence_score DESC")

    if df.empty:
        st.success("All pipelines healthy — no active fingerprints.")
    else:
        for _, r in df.iterrows():
            with st.expander(f"{r['job_name']} — {r['fingerprint_type']}  (score {r['evidence_score']})"):
                st.progress(int(r["evidence_score"]) / 100, text=f"Evidence Score: {r['evidence_score']}/100")
                c1, c2 = st.columns(2)
                c1.markdown(f"**Statistical evidence**\n\n{r['statistical_evidence']}")
                c2.markdown(f"**External evidence**\n\n{r['external_evidence']}")
                st.error(f"Action: {r['recommended_action']}")
                st.markdown(f"**Route to: {r['route_to_team']}**")


# ── PAGE 3: DEADLINE COUNTDOWN ───────────────────────────────────────────────
elif page == "Regulatory Deadline Countdown":
    st.header("Regulatory Deadline Countdown")
    st.caption("Deadline times based on RBI circular DPSS.CO.OD.No.1852/06.08.005/2020-21")
    df = query("SELECT * FROM gold_deadline_risk ORDER BY buffer_minutes ASC")

    silver = query("SELECT COUNT(DISTINCT job_name) AS n FROM silver_job_runs")
    runs   = int(silver.iloc[0]["n"]) if not silver.empty else 0

    if runs < MIN_RUNS_FOR_ZSCORE:
        st.info(f"Warming up — need {MIN_RUNS_FOR_ZSCORE}+ runs to compute reliable deadline risk. Current: {runs}.")
    elif df.empty:
        st.success("No deadline risk data yet.")
    else:
        for _, r in df.iterrows():
            buf   = float(r["buffer_minutes"])
            color = "red" if r["risk_level"]=="high" else ("orange" if r["risk_level"]=="medium" else "green")
            critical = r.get("critical_jobs","")
            st.markdown(f"**{r['deadline_name']}** — :{color}[{round(buf)} min buffer]")
            if critical:
                st.warning(f"Critical jobs at risk: {critical}")
            st.divider()


# ── PAGE 4: SLA HEALTH ───────────────────────────────────────────────────────
elif page == "SLA Health Report":
    st.header("SLA Health Report")
    df = query("SELECT * FROM gold_sla_health ORDER BY tightness_score DESC")

    if df.empty:
        st.info("No SLA health data yet. Run lambda_processor.py to generate data.")
    else:
        ARROW = {"growing": "↑", "shrinking": "↓", "stable": "→"}
        st.caption("Note: Volume growth projections require 14+ days of baseline data. Currently showing trend direction only.")
        for _, r in df.iterrows():
            tightness = float(r.get("tightness_score","0") or 0)
            bar_color = "red" if tightness > 0.8 else ("orange" if tightness > 0.6 else "green")
            col1, col2, col3, col4 = st.columns([3,1,1,2])
            col1.write(r["job_name"])
            col2.write(f":{bar_color}[{int(tightness*100)}% of SLA]")
            col3.write(ARROW.get(r.get("volume_growth","stable"), "→"))
            if tightness > 0.8:
                col4.write(f"Recommend SLA: {r.get('recommended_sla','-')} min")


# ── PAGE 5: INCIDENT LOG ─────────────────────────────────────────────────────
elif page == "Incident Log":
    st.header("Incident Log")
    s3 = boto3.client("s3", region_name=REGION)
    resp  = s3.list_objects_v2(Bucket=BUCKET, Prefix="incidents/")
    files = sorted([o["Key"] for o in resp.get("Contents", [])], reverse=True)

    if not files:
        st.info("No incidents yet. Verdict engine will populate this when jobs breach SLA.")
    else:
        for key in files[:20]:
            body    = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read().decode()
            job     = key.split("/")[1]
            is_resolved = "resolved_at" in body
            label   = f"{'✅' if is_resolved else '🔴'} {job} — {key.split('/')[-1][:15]}"
            with st.expander(label):
                st.code(body)
                if not is_resolved:
                    if st.button("Mark as Resolved", key=key):
                        resolved = body + f"\nresolved_at: {datetime.now().isoformat()}"
                        s3.put_object(Bucket=BUCKET, Key=key, Body=resolved.encode())
                        st.success("Marked resolved. Refresh to update MTTR.")
                elif "resolved_at:" in body:
                    lines  = body.split("\n")
                    det    = next((l for l in lines if "Detected at:" in l), "")
                    res    = next((l for l in lines if "resolved_at:" in l), "")
                    st.caption(f"{det.strip()} | {res.strip()}")

# Auto-refresh every 30 seconds
time.sleep(0.1)
st.rerun() if hasattr(st, "rerun") else st.experimental_rerun()
