# dags/data_quality_check.py
# DAG 5 of 5 — PayWatch monitoring its own data quality
#
# This DAG reads the silver_job_runs table (written by lambda_processor.py)
# and validates it for nulls, schema drift, and volume anomalies.
# Meta: PayWatch monitors itself. This is what makes it production-grade.
#
# Also monitored by PayWatch as a tracked pipeline job.

import os, sys, json
sys.path.insert(0, os.path.join(os.environ.get('AIRFLOW_HOME', '/usr/local/airflow'), 'include'))

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

BUCKET     = "sla-intelligence-shubhangi"
REGION     = "eu-north-1"
SILVER_PFX = "processed/silver_job_runs"
DQ_PFX     = "rbi_data/data_quality"

# Expected columns in silver_job_runs
REQUIRED_COLUMNS = {
    "job_name", "start_time", "duration_minutes",
    "status", "rows_processed", "team", "sla_minutes"
}


def read_silver_table():
    """Read all partitions of silver_job_runs from S3."""
    import boto3, json
    from datetime import datetime

    s3 = boto3.client('s3', region_name=REGION)
    resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=SILVER_PFX)
    files = [obj["Key"] for obj in resp.get("Contents", []) if not obj["Key"].endswith("/")]

    all_rows = []
    for key in files[-20:]:  # cap at last 20 files to avoid memory issues
        try:
            body = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
            rows = json.loads(body)
            all_rows.extend(rows if isinstance(rows, list) else [rows])
        except Exception as e:
            print(f"  Could not read {key}: {e}")

    result = {"row_count": len(all_rows), "files_read": len(files), "rows": all_rows}
    s3.put_object(
        Bucket=BUCKET,
        Key=f"{DQ_PFX}/staging_silver.json",
        Body=json.dumps(result).encode()
    )
    print(f"Read {len(all_rows)} rows from {len(files)} silver files")


def run_quality_checks():
    """Check for nulls, missing columns, and volume anomalies."""
    import boto3, pandas as pd, json

    s3 = boto3.client('s3', region_name=REGION)
    body = s3.get_object(Bucket=BUCKET, Key=f"{DQ_PFX}/staging_silver.json")["Body"].read()
    data = json.loads(body)
    rows = data.get("rows", [])

    if not rows:
        # No data yet — system in warm-up phase
        report = {
            "status": "WARM_UP",
            "message": "No silver_job_runs data yet. Run lambda_processor.py to generate data.",
            "checks_passed": 0,
            "checks_failed": 0,
            "issues": []
        }
    else:
        df = pd.DataFrame(rows)
        issues = []

        # Check 1: Required columns present
        missing_cols = REQUIRED_COLUMNS - set(df.columns)
        if missing_cols:
            issues.append(f"Schema drift — missing columns: {missing_cols}")

        # Check 2: Null rate > 5% in any required column
        for col in REQUIRED_COLUMNS & set(df.columns):
            null_pct = df[col].isnull().sum() / len(df) * 100
            if null_pct > 5:
                issues.append(f"High null rate in {col}: {null_pct:.1f}%")

        # Check 3: Volume anomaly — fewer than 10 rows suggests upstream failure
        if len(df) < 10:
            issues.append(f"Low volume: only {len(df)} rows (expected 30+ for healthy system)")

        report = {
            "status":        "FAIL" if issues else "PASS",
            "row_count":     len(df),
            "checks_passed": 3 - len(issues),
            "checks_failed": len(issues),
            "issues":        issues
        }

    s3.put_object(
        Bucket=BUCKET,
        Key=f"{DQ_PFX}/staging_quality_result.json",
        Body=json.dumps(report).encode()
    )
    print(f"Quality check: {report['status']} | {report['checks_passed']} passed, "
          f"{report['checks_failed']} failed")


def write_quality_report():
    """Write final quality report with timestamp."""
    import boto3, json
    from datetime import datetime

    s3 = boto3.client('s3', region_name=REGION)
    body = s3.get_object(Bucket=BUCKET, Key=f"{DQ_PFX}/staging_quality_result.json")["Body"].read()
    report = json.loads(body)
    report["checked_at"] = datetime.now().isoformat()

    key = f"{DQ_PFX}/reports/{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    s3.put_object(Bucket=BUCKET, Key=key, Body=json.dumps(report).encode())
    print(f"Quality report → s3://{BUCKET}/{key} | Status: {report['status']}")


with DAG(
    dag_id='data_quality_check',
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule='*/30 * * * *',  # every 30 minutes — check quality twice an hour
    catchup=False,
    tags=['paywatch', 'platform', 'monitored', 'meta'],
    doc_md="PayWatch monitors its own silver_job_runs table for quality issues. Meta-observability."
) as dag:
    t1 = PythonOperator(task_id='read_silver_table',    python_callable=read_silver_table)
    t2 = PythonOperator(task_id='run_quality_checks',   python_callable=run_quality_checks)
    t3 = PythonOperator(task_id='write_quality_report', python_callable=write_quality_report)
    t1 >> t2 >> t3
