# src/verdict_engine.py
# Polls Athena every 30 seconds for breached/at_risk jobs.
# Sends SNS email alerts and writes S3 incident reports.
# Run this in Terminal 2 while lambda_processor.py runs in Terminal 1.

import sys, os
sys.path.insert(0, os.path.dirname(__file__))

import boto3, json, time
from datetime import datetime

BUCKET         = "sla-intelligence-shubhangi"
REGION         = "eu-north-1"
DB             = "sla_intelligence_db"
OUTPUT_BUCKET  = f"s3://{BUCKET}/athena_results/"
SNS_TOPIC_ARN  = os.environ.get("SNS_TOPIC_ARN", "")
POLL_SECONDS   = 30

s3  = boto3.client("s3",      region_name=REGION)
sns = boto3.client("sns",     region_name=REGION)
ath = boto3.client("athena",  region_name=REGION)

# Track jobs we have already alerted on this session (prevents duplicate emails)
_alerted = set()


def run_athena_query(sql):
    """Submit SQL to Athena, wait for it to complete, return rows as list of dicts."""
    resp = ath.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": DB},
        ResultConfiguration={"OutputLocation": OUTPUT_BUCKET}
    )
    qid = resp["QueryExecutionId"]

    # Poll until query finishes (Athena is async)
    for _ in range(30):
        status = ath.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]["State"]
        if status == "SUCCEEDED":
            break
        if status in ("FAILED", "CANCELLED"):
            print(f"  Athena query failed: {status}")
            return []
        time.sleep(2)

    rows   = ath.get_query_results(QueryExecutionId=qid)["ResultSet"]["Rows"]
    if len(rows) < 2:
        return []
    headers = [c["VarCharValue"] for c in rows[0]["Data"]]
    return [{headers[i]: col.get("VarCharValue", "") for i, col in enumerate(r["Data"])}
            for r in rows[1:]]


def build_incident_report(job, team, verdict, fp_type, score, stat_e, ext_e, action, route):
    """Format the structured incident report that gets emailed and saved to S3."""
    detected = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return f"""PAYWATCH INCIDENT REPORT
========================
Job:           {job}
Team:          {team}
Verdict:       {verdict}
Fingerprint:   {fp_type} (evidence score: {score})
Root cause:    {stat_e}
Evidence:      {ext_e}
Action:        {action}
Route to:      {route}
Detected at:   {detected}
========================"""


def send_alert(job, report_text):
    """Send SNS email and write incident report to S3."""
    # Write incident report to S3
    key = f"incidents/{job}/{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    s3.put_object(Bucket=BUCKET, Key=key, Body=report_text.encode())

    # Send SNS email if topic ARN is configured
    if SNS_TOPIC_ARN:
        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=f"[PayWatch] {job} — incident detected",
            Message=report_text
        )
        print(f"  Alert sent → SNS + s3://{BUCKET}/{key}")
    else:
        print(f"  No SNS_TOPIC_ARN set — wrote to s3://{BUCKET}/{key} only")


def poll_once():
    """One polling cycle: check metrics, fingerprints, send new alerts."""
    # Query gold_job_metrics for jobs that need attention
    metrics_rows = run_athena_query("""
        SELECT job_name, team, verdict, latest_duration, sla_minutes
        FROM gold_job_metrics
        WHERE verdict IN ('breached', 'at_risk')
        ORDER BY verdict DESC
    """)

    # Query gold_fingerprint for classification details
    fp_rows = run_athena_query("""
        SELECT job_name, fingerprint_type, evidence_score,
               statistical_evidence, external_evidence,
               recommended_action, route_to_team
        FROM gold_fingerprint
    """)
    fp_lookup = {r["job_name"]: r for r in fp_rows}

    new_alerts = 0
    for m in metrics_rows:
        job = m["job_name"]
        if job in _alerted:
            continue   # already reported this session — skip

        fp   = fp_lookup.get(job, {})
        report = build_incident_report(
            job     = job,
            team    = m["team"],
            verdict = m["verdict"],
            fp_type = fp.get("fingerprint_type", "unknown"),
            score   = fp.get("evidence_score", "?"),
            stat_e  = fp.get("statistical_evidence", "No fingerprint available"),
            ext_e   = fp.get("external_evidence", "none"),
            action  = fp.get("recommended_action", "Investigate manually"),
            route   = fp.get("route_to_team", m["team"])
        )
        send_alert(job, report)
        _alerted.add(job)
        new_alerts += 1

    status = f"{len(metrics_rows)} urgent jobs" if metrics_rows else "all clear"
    print(f"  [{datetime.now().strftime('%H:%M:%S')}] {status} | {new_alerts} new alerts sent")


def main():
    print(f"\nVerdict Engine started — polling Athena every {POLL_SECONDS}s")
    print(f"SNS topic: {SNS_TOPIC_ARN or '(not set — console output only)'}\n")
    while True:
        try:
            poll_once()
        except Exception as e:
            print(f"  Poll error: {e}")
        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()
