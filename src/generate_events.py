# generate_events.py
# ---------------------------------------------------------------
# What this file does:
# Simulates 50 payment pipeline jobs running in real time
# Every 10 seconds it generates one batch of job events
# and uploads it directly to S3 as a JSON file
#
# This replaces writing to local data/raw/ folder
# Now files go to: s3://sla-intelligence-shubhangi/raw/
#
# How to run: py -3.11 generate_events.py (in Terminal 1)
# Keep this running while everything else runs
# ---------------------------------------------------------------

import json
import random
import time
import os
import boto3
from datetime import datetime
from job_config import JOBS

# Your S3 bucket name
BUCKET_NAME = "sla-intelligence-shubhangi"

# Folder inside the bucket where raw events go
S3_RAW_PREFIX = "raw/pipeline_events/"

# Create an S3 client using the credentials you configured with aws configure
# boto3 automatically reads from ~/.aws/credentials - no need to paste keys here
s3_client = boto3.client("s3", region_name="ap-south-1")


def get_volume_multiplier():
    # Simulate realistic volume spikes based on time of day
    # Payment systems are busiest in morning, lunch, and evening
    hour = datetime.now().hour

    if hour in [9, 10, 13, 19, 20]:
        return random.uniform(1.5, 2.5)   # peak hours - high volume
    elif hour in [2, 3, 4]:
        return random.uniform(0.3, 0.6)   # quiet at night - low volume
    else:
        return random.uniform(0.8, 1.2)   # normal hours


def should_inject_anomaly():
    # 8% chance any given cycle has anomalous jobs
    return random.random() < 0.08


def generate_one_job_event(job, cycle_number, volume_multiplier, anomaly_jobs):
    # Generate a single event for one pipeline job
    # Each event represents one completed run of that job

    start_time = datetime.now()

    # Normal duration is 70% of SLA - jobs usually finish well before deadline
    normal_duration = job["sla_minutes"] * 0.7

    if job["name"] in anomaly_jobs:
        # Anomalous run - takes much longer than normal
        actual_duration = normal_duration * random.uniform(1.5, 3.0)
    else:
        # Normal run - small variation around normal duration
        actual_duration = normal_duration * random.uniform(0.8, 1.2)

    # Calculate rows processed this run
    normal_rows = job["normal_rows"] * volume_multiplier

    if job["name"] in anomaly_jobs:
        # Anomalous runs also process fewer rows - data might be missing upstream
        actual_rows = int(normal_rows * random.uniform(0.3, 0.7))
    else:
        actual_rows = int(normal_rows * random.uniform(0.85, 1.15))

    # Anomalous jobs fail more often
    if job["name"] in anomaly_jobs:
        status = random.choice(["failed", "failed", "success"])   # 67% fail rate
    else:
        status = random.choice(["success"] * 19 + ["failed"])     # 5% fail rate

    # Build the event - this becomes one row in our data
    event = {
        "job_name":          job["name"],
        "team":              job["team"],
        "impact":            job["impact"],
        "sla_minutes":       job["sla_minutes"],
        "normal_rows":       job["normal_rows"],
        "depends_on":        job["depends_on"],
        "cycle_number":      cycle_number,
        "start_time":        start_time.isoformat(),
        "duration_minutes":  round(actual_duration, 2),
        "rows_processed":    actual_rows,
        "status":            status,
        "volume_multiplier": round(volume_multiplier, 2)
    }

    return event


def upload_to_s3(events, cycle_number):
    # Convert events list to JSON string
    json_data = json.dumps(events, indent=2)

    # Create a unique filename using timestamp so files never overwrite each other
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"events_cycle_{cycle_number}_{timestamp}.json"

    # S3 key = the full path inside the bucket
    # e.g. raw/events_cycle_1_20260601_142301.json
    s3_key = f"{S3_RAW_PREFIX}{filename}"

    # Upload the JSON string directly to S3
    # We don't need to save it locally first - boto3 handles it in memory
    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=s3_key,
        Body=json_data.encode("utf-8"),   # convert string to bytes for upload
        ContentType="application/json"
    )

    return s3_key


def run_generator():
    print("=" * 50)
    print("Pipeline SLA Intelligence Engine - Generator")
    print(f"Uploading events to s3://{BUCKET_NAME}/{S3_RAW_PREFIX}")
    print("Press Ctrl+C to stop")
    print("=" * 50)
    print()

    cycle_number = 1

    while True:
        print(f"Cycle {cycle_number} - generating events for all 50 jobs...")

        # Get current volume multiplier based on time of day
        volume_multiplier = get_volume_multiplier()

        # Randomly pick 0 to 4 jobs to behave anomalously this cycle
        number_of_anomalies = random.randint(0, 4)
        all_job_names = [job["name"] for job in JOBS]
        anomaly_jobs = random.sample(all_job_names, number_of_anomalies)

        if anomaly_jobs:
            print(f"  Injecting anomalies into: {anomaly_jobs}")

        # Generate one event for each of the 50 jobs
        events = []
        for job in JOBS:
            event = generate_one_job_event(job, cycle_number, volume_multiplier, anomaly_jobs)
            events.append(event)

        # Upload all 50 events to S3 as one JSON file
        s3_key = upload_to_s3(events, cycle_number)

        print(f"  Uploaded: s3://{BUCKET_NAME}/{s3_key}")
        print(f"  Volume multiplier: {volume_multiplier:.2f}x")
        print(f"  Events in this batch: {len(events)}")
        print()

        cycle_number += 1

        # Wait 10 seconds before next batch
        time.sleep(10)


# Start the generator
run_generator()
