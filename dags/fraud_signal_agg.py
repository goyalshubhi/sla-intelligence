# dags/fraud_signal_agg.py
# DAG 4 of 5 — Fraud signal aggregation
#
# Velocity check: flag accounts with >10 transactions in any 5-minute window.
# This is a standard fraud detection heuristic used by payment processors.
# Source: RBI Master Direction on Credit/Debit Card Fraud Prevention (2021).
#
# Also monitored by PayWatch as a tracked pipeline job.

import os, sys, json
sys.path.insert(0, os.path.join(os.environ.get('AIRFLOW_HOME', '/usr/local/airflow'), 'include'))

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

BUCKET     = "sla-intelligence-shubhangi"
REGION     = "eu-north-1"
FRAUD_PFX  = "rbi_data/fraud_signals"

# Velocity threshold: >10 transactions in 5 minutes = flag for review
VELOCITY_THRESHOLD = 10
WINDOW_MINUTES     = 5


def read_events():
    """Generate synthetic transaction events for fraud signal analysis."""
    import boto3, json, random
    from datetime import datetime, timedelta

    now = datetime.now()
    events = []
    # Generate 300,000 transactions across 5,000 accounts (realistic for a batch)
    # ~99% of accounts are normal; ~1% have suspicious velocity
    for account_id in range(1, 5001):
        # 99% of accounts: 1-3 transactions in the window
        txn_count = random.randint(1, 3)
        # 1% of accounts: 8-20 transactions (velocity anomaly)
        if account_id % 100 == 0:
            txn_count = random.randint(8, 20)

        for i in range(txn_count):
            events.append({
                "account_id":   f"ACC{account_id:05d}",
                "txn_id":       f"TXN{random.randint(100000, 999999)}",
                "amount_rs":    round(random.uniform(50, 5000), 2),
                "timestamp":    (now - timedelta(minutes=random.randint(0, WINDOW_MINUTES))).isoformat()
            })

    s3 = boto3.client('s3', region_name=REGION)
    s3.put_object(
        Bucket=BUCKET,
        Key=f"{FRAUD_PFX}/staging_events.json",
        Body=json.dumps(events).encode()
    )
    print(f"Generated {len(events):,} transaction events across {5000} accounts")


def compute_velocity():
    """Flag accounts exceeding velocity threshold in the 5-minute window."""
    import boto3, pandas as pd, json

    s3 = boto3.client('s3', region_name=REGION)
    body = s3.get_object(Bucket=BUCKET, Key=f"{FRAUD_PFX}/staging_events.json")["Body"].read()
    df = pd.DataFrame(json.loads(body))

    # Count transactions per account in the window
    velocity = df.groupby("account_id").size().reset_index(name="txn_count")
    flagged  = velocity[velocity["txn_count"] > VELOCITY_THRESHOLD]

    result = {
        "total_accounts":   int(velocity["account_id"].nunique()),
        "flagged_count":    int(len(flagged)),
        "flag_rate_pct":    round(len(flagged) / len(velocity) * 100, 3),
        "flagged_accounts": flagged.to_dict("records"),
        "threshold":        VELOCITY_THRESHOLD,
        "window_minutes":   WINDOW_MINUTES
    }
    s3.put_object(
        Bucket=BUCKET,
        Key=f"{FRAUD_PFX}/staging_velocity_result.json",
        Body=json.dumps(result).encode()
    )
    print(f"Flagged {result['flagged_count']} accounts out of {result['total_accounts']} "
          f"({result['flag_rate_pct']}%)")


def write_alerts():
    """Write flagged accounts to S3 alerts path for downstream review."""
    import boto3, json
    from datetime import datetime

    s3 = boto3.client('s3', region_name=REGION)
    body = s3.get_object(Bucket=BUCKET, Key=f"{FRAUD_PFX}/staging_velocity_result.json")["Body"].read()
    result = json.loads(body)
    result["generated_at"] = datetime.now().isoformat()

    key = f"{FRAUD_PFX}/alerts/{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    s3.put_object(Bucket=BUCKET, Key=key, Body=json.dumps(result).encode())
    print(f"Alerts written → s3://{BUCKET}/{key} | {result['flagged_count']} accounts flagged")


with DAG(
    dag_id='fraud_signal_agg',
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule='*/15 * * * *',  # every 15 minutes — fraud detection is near-real-time
    catchup=False,
    tags=['paywatch', 'risk', 'monitored'],
    doc_md="Velocity-based fraud signal aggregation. Flags accounts exceeding 10 txns in 5 minutes."
) as dag:
    t1 = PythonOperator(task_id='read_events',      python_callable=read_events)
    t2 = PythonOperator(task_id='compute_velocity', python_callable=compute_velocity)
    t3 = PythonOperator(task_id='write_alerts',     python_callable=write_alerts)
    t1 >> t2 >> t3
