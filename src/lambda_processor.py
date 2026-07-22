# src/lambda_processor.py
# PayWatch orchestrator — runs every 15 min (locally) or via EventBridge (Lambda).
# This file only reads data and calls engine.py functions. No intelligence logic here.

import sys, os
sys.path.insert(0, os.path.dirname(__file__))

import boto3, pandas as pd, json, requests
from datetime import datetime, timedelta, timezone
from engine import (compute_metrics, compute_verdicts, compute_fingerprint,
                    compute_sla_health, compute_deadline_risk,
                    write_s3_append, write_s3_overwrite)
from job_config import JOBS

AIRFLOW_URL  = os.environ.get("AIRFLOW_URL", "http://localhost:8080")
GITHUB_OWNER = "goyalshubhi"
GITHUB_REPO  = "sla-intelligence"
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "")
BUCKET       = "sla-intelligence-shubhangi"
REGION       = "eu-north-1"
_JOBS        = {j["name"]: j for j in JOBS}


def _airflow_token():
    r = requests.post(f"{AIRFLOW_URL}/auth/token",
                      json={"username": "admin", "password": "admin"}, timeout=10)
    return r.json()["access_token"]


def read_airflow_task_instances():
    """Fetch completed task instances from the last 24 hours via Airflow REST API v2.

    24h (not 2h) so 15-min-cadence jobs can cross MIN_RUNS=30 within the window;
    daily jobs (rbi_neft_stats, npci_upi_stats) still need multiple days to warm up.
    """
    headers    = {"Authorization": f"Bearer {_airflow_token()}"}
    lookback_h = int(os.environ.get("LOOKBACK_HOURS", "24"))
    cutoff     = (datetime.now(timezone.utc) - timedelta(hours=lookback_h)).isoformat()

    # Airflow's API caps each page at 100 regardless of the limit param, so we
    # page through with offset until we've read everything (or hit the safety cap).
    task_instances, offset, MAX_ROWS = [], 0, 3000
    while offset < MAX_ROWS:
        resp = requests.get(f"{AIRFLOW_URL}/api/v2/dags/~/dagRuns/~/taskInstances",
                            headers=headers, params={"limit": 100, "offset": offset,
                            "start_date_gte": cutoff, "state": "success",
                            "order_by": "-start_date"}, timeout=15)
        page = resp.json().get("task_instances", [])
        if not page:
            break
        task_instances.extend(page)
        offset += 100

    rows, rows_processed_cache = [], {}
    for ti in task_instances:
        if ti.get("duration") is None:
            continue
        job = ti["dag_id"]
        if job not in _JOBS:
            continue
        if job not in rows_processed_cache:
            rows_processed_cache[job] = _get_rows(job)
        rows.append({"job_name": job,
                     "start_time":        ti.get("start_date") or datetime.now(timezone.utc).isoformat(),
                     "duration_minutes":  round(float(ti["duration"]) / 60, 4),
                     "status":            ti["state"],
                     "rows_processed":    rows_processed_cache[job],
                     "team":              _JOBS[job]["team"],
                     "impact":            _JOBS[job]["impact"],
                     "sla_minutes":       _JOBS[job]["sla_minutes"]})
    print(f"  Loaded {len(rows)} completed task instances from Airflow")
    return pd.DataFrame(rows) if rows else pd.DataFrame()


def _get_rows(job_name):
    """Read rows_processed from latest S3 task result; fall back to normal_rows from config."""
    try:
        s3    = boto3.client("s3", region_name=REGION)
        files = sorted([o["Key"] for o in s3.list_objects_v2(Bucket=BUCKET,
                        Prefix=f"airflow_runs/{job_name}/", MaxKeys=10).get("Contents", [])], reverse=True)
        if files:
            return json.loads(s3.get_object(Bucket=BUCKET, Key=files[0])["Body"].read()).get(
                "rows_processed", _JOBS[job_name]["normal_rows"])
    except Exception:
        pass
    return _JOBS[job_name]["normal_rows"]


def pull_cloudwatch():
    """S3 5xx errors + Lambda errors from last 30 min. Real signals from actual AWS resources."""
    signals = {"s3_errors": 0, "lambda_errors": 0}
    try:
        cw  = boto3.client("cloudwatch", region_name=REGION)
        win = {"StartTime": datetime.utcnow()-timedelta(minutes=30),
               "EndTime": datetime.utcnow(), "Period": 1800, "Statistics": ["Sum"]}
        for metric, ns, dims, key in [
            ("5xxErrors", "AWS/S3",    [{"Name":"BucketName","Value":BUCKET},{"Name":"FilterId","Value":"EntireBucket"}], "s3_errors"),
            ("Errors",    "AWS/Lambda", [], "lambda_errors")]:
            pts = cw.get_metric_statistics(Namespace=ns, MetricName=metric, Dimensions=dims, **win)["Datapoints"]
            if pts: signals[key] = pts[0]["Sum"]
    except Exception as e:
        print(f"  CloudWatch unavailable: {e}")
    return signals


def pull_github_actions():
    """Recent GitHub Actions workflow runs. A failed run near a job slowdown = logic bug signal."""
    if not GITHUB_TOKEN:
        print("  GitHub token not set — skipping")
        return []
    try:
        resp = requests.get(
            f"https://api.github.com/repos/{GITHUB_OWNER}/{GITHUB_REPO}/actions/runs",
            headers={"Authorization": f"token {GITHUB_TOKEN}"}, params={"per_page": 10}, timeout=10)
        return [{"created_at": r["created_at"], "conclusion": r["conclusion"], "name": r["name"]}
                for r in resp.json().get("workflow_runs", [])] if resp.status_code == 200 else []
    except Exception as e:
        print(f"  GitHub unavailable: {e}")
        return []


def handler(event=None, context=None):
    print(f"\n{'='*50}\nPayWatch: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n{'='*50}")
    df = read_airflow_task_instances()
    if df.empty:
        print("No completed task instances yet — Airflow DAGs still warming up.")
        return
    cw = pull_cloudwatch()
    gh = pull_github_actions()
    metrics_df  = compute_verdicts(compute_metrics(df))
    fp_df       = compute_fingerprint(metrics_df, cw, gh)
    sla_df      = compute_sla_health(df, metrics_df)
    deadline_df = compute_deadline_risk(metrics_df)
    write_s3_append   (df,          "processed/silver_job_runs")
    write_s3_overwrite(metrics_df,  "processed/gold_job_metrics")
    write_s3_overwrite(fp_df,       "processed/gold_fingerprint")
    write_s3_overwrite(sla_df,      "processed/gold_sla_health")
    write_s3_overwrite(deadline_df, "processed/gold_deadline_risk")
    v = metrics_df["verdict"].value_counts()
    print(f"\nResults: {v.get('safe',0)} safe | {v.get('at_risk',0)} at risk | "
          f"{v.get('breached',0)} breached | {v.get('insufficient_data',0)} warming up")
    if not fp_df.empty:
        for _, r in fp_df.iterrows():
            print(f"  {r['job_name']:30} {r['fingerprint_type']:20} score={r['evidence_score']}")


if __name__ == "__main__":
    handler()
