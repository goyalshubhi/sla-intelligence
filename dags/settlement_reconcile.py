# dags/settlement_reconcile.py
# DAG 3 of 5 — EOD settlement reconciliation
#
# Simulates end-of-day settlement matching at a payment processor.
# RBI mandates same-day settlement for NEFT/RTGS per circular RBI/2021-22/25.
# "Reconciliation" = comparing transaction counts between systems to find gaps.
#
# This DAG is also monitored by PayWatch as a tracked job (job name matches JOBS list).

import os, sys, json
sys.path.insert(0, os.path.join(os.environ.get('AIRFLOW_HOME', '/usr/local/airflow'), 'include'))

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

BUCKET    = "sla-intelligence-shubhangi"
REGION    = "eu-north-1"
RECON_PFX = "rbi_data/settlement_reconcile"


def read_transactions():
    """Read today's transaction count from S3 (or generate synthetic data if empty)."""
    import boto3, random, json
    from datetime import datetime

    s3 = boto3.client('s3', region_name=REGION)

    # Try to read from airflow_runs/ written by simulated DAGs
    # Fall back to synthetic data if no real runs exist yet
    try:
        resp = s3.list_objects_v2(
            Bucket=BUCKET, Prefix="airflow_runs/settlement_intraday/", MaxKeys=10
        )
        files = resp.get("Contents", [])
    except Exception:
        files = []

    if files:
        body = s3.get_object(Bucket=BUCKET, Key=files[-1]["Key"])["Body"].read()
        event = json.loads(body)
        txn_count = event.get("rows_processed", 150_000)
    else:
        # Synthetic: realistic intraday settlement volume
        # Based on typical NEFT intraday batch: 50k–200k transactions
        txn_count = random.randint(120_000, 180_000)

    result = {
        "date": datetime.now().strftime("%Y-%m-%d"),
        "source_count": txn_count,
        # Destination system count has ±0.5% variance (expected in real systems)
        "destination_count": int(txn_count * random.uniform(0.995, 1.005))
    }
    s3.put_object(
        Bucket=BUCKET,
        Key=f"{RECON_PFX}/staging_txn_counts.json",
        Body=json.dumps(result).encode()
    )
    print(f"Transaction counts → source: {result['source_count']:,}, destination: {result['destination_count']:,}")


def run_reconciliation():
    """Compare source vs destination counts, flag mismatches > 0.1% threshold."""
    import boto3, json

    s3 = boto3.client('s3', region_name=REGION)
    body = s3.get_object(Bucket=BUCKET, Key=f"{RECON_PFX}/staging_txn_counts.json")["Body"].read()
    data = json.loads(body)

    src  = data["source_count"]
    dst  = data["destination_count"]
    diff = abs(src - dst)
    pct  = diff / src * 100 if src > 0 else 0

    # RBI settlement reconciliation threshold: mismatches > 0.1% require investigation
    status = "MISMATCH" if pct > 0.1 else "RECONCILED"
    result = {**data, "diff": diff, "diff_pct": round(pct, 4), "status": status}

    s3.put_object(
        Bucket=BUCKET,
        Key=f"{RECON_PFX}/staging_recon_result.json",
        Body=json.dumps(result).encode()
    )
    print(f"Reconciliation: {status} | diff={diff:,} ({pct:.4f}%)")


def write_report():
    """Write final reconciliation report with timestamp."""
    import boto3, json
    from datetime import datetime

    s3 = boto3.client('s3', region_name=REGION)
    body = s3.get_object(Bucket=BUCKET, Key=f"{RECON_PFX}/staging_recon_result.json")["Body"].read()
    result = json.loads(body)
    result["reported_at"] = datetime.now().isoformat()

    key = f"{RECON_PFX}/{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    s3.put_object(Bucket=BUCKET, Key=key, Body=json.dumps(result).encode())
    print(f"Report written → s3://{BUCKET}/{key} | Status: {result['status']}")


with DAG(
    dag_id='settlement_reconcile',
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule='@daily',
    catchup=False,
    tags=['paywatch', 'finance', 'monitored'],
    doc_md="EOD settlement reconciliation. Compares source vs destination transaction counts."
) as dag:
    t1 = PythonOperator(task_id='read_transactions',   python_callable=read_transactions)
    t2 = PythonOperator(task_id='run_reconciliation',  python_callable=run_reconciliation)
    t3 = PythonOperator(task_id='write_report',        python_callable=write_report)
    t1 >> t2 >> t3
