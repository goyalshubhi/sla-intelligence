# dags/payment_ops_dags.py
# Programmatically generates Airflow DAGs for all 47 simulated payment pipeline jobs.
#
# The 3 jobs with real DAG implementations (settlement_reconcile, fraud_signal_agg,
# data_quality_check) are skipped here — their own files handle them.
#
# Pattern: each job runs every 15 minutes, simulates realistic duration based on
# its SLA contract, writes task results to S3 for lambda_processor.py to read.

import os, sys, uuid
sys.path.insert(0, os.path.join(os.environ.get('AIRFLOW_HOME', '/usr/local/airflow'), 'include'))

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from job_config import JOBS

# These 3 have their own real DAG files — do not create duplicates
REAL_DAG_NAMES = {'settlement_reconcile', 'fraud_signal_agg', 'data_quality_check'}

BUCKET = "sla-intelligence-shubhangi"
REGION = "eu-north-1"


def make_task_callable(job_name, sla_minutes, normal_rows, team):
    """
    Returns a function that simulates a pipeline job run.
    Using a closure (function that returns a function) so each DAG gets
    its own copy of the job parameters — without this, all 47 DAGs
    would share the same variable values from the last iteration of the loop.
    """
    def run_job():
        import time, random, boto3, json
        from datetime import datetime

        # Simulate realistic duration: 70% of SLA on average, ±30% variance
        base_seconds    = sla_minutes * 0.7 * 60
        actual_seconds  = base_seconds * random.uniform(0.7, 1.3)

        # 5% chance of anomaly (slower run — what PayWatch detects)
        if random.random() < 0.05:
            actual_seconds *= random.uniform(1.8, 2.5)

        # Cap sleep at 10 seconds in local dev (don't block the scheduler)
        time.sleep(min(actual_seconds, 10))

        rows = int(normal_rows * random.uniform(0.88, 1.12))
        event = {
            "job_name":          job_name,
            "run_id":            str(uuid.uuid4())[:8],
            "team":              team,
            "duration_seconds":  round(actual_seconds, 2),
            "duration_minutes":  round(actual_seconds / 60, 4),
            "rows_processed":    rows,
            "sla_minutes":       sla_minutes,
            "status":            "success",
            "recorded_at":       datetime.now().isoformat()
        }

        s3 = boto3.client('s3', region_name=REGION)
        key = f"airflow_runs/{job_name}/{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        s3.put_object(Bucket=BUCKET, Key=key, Body=json.dumps(event).encode())
        print(f"{job_name} | {round(actual_seconds/60, 2)}min | {rows:,} rows → s3://{BUCKET}/{key}")

    return run_job


# Build one DAG per job (skipping the 3 with real implementations)
for job in JOBS:
    if job["name"] in REAL_DAG_NAMES:
        continue

    with DAG(
        dag_id=job["name"],
        start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
        schedule='*/15 * * * *',
        catchup=False,
        tags=['paywatch', job["team"], 'simulated'],
        default_args={"retries": 1}
    ) as _dag:
        PythonOperator(
            task_id='run_pipeline_job',
            python_callable=make_task_callable(
                job["name"], job["sla_minutes"], job["normal_rows"], job["team"]
            )
        )

    # Save DAG to module globals so Airflow's file scanner can discover it.
    # Without this line, the loop variable gets overwritten and only the
    # last DAG survives. globals() persists each DAG by its unique name.
    globals()[job["name"]] = _dag
