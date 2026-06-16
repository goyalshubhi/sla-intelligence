# What this file does:
# This is an AWS Glue job - it runs as managed PySpark on AWS
# It reads all raw JSON events from S3 raw/ folder
# Computes per-job metrics, drift scores, SLA verdicts
# Writes results back to S3 processed/ folder as Parquet
# Glue Crawler then picks up processed/ and registers it
# in Glue Catalog so Athena can query it with plain SQL
#
# How it runs:
# You upload this file to s3://sla-intelligence-shubhangi/scripts/
# Then create a Glue job in AWS Console pointing to that script
# Run the Glue job manually or on a schedule
# ---------------------------------------------------------------

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType
from datetime import datetime

# ---------------------------------------------------------------
# Glue job initialization
# Every Glue job must start with this boilerplate
# GlueContext wraps SparkContext and adds AWS-specific features
# like reading from S3 and writing to Glue Catalog
# ---------------------------------------------------------------
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Reduce log noise
spark.sparkContext.setLogLevel("ERROR")

# ---------------------------------------------------------------
# S3 paths
# ---------------------------------------------------------------
BUCKET = "sla-intelligence-shubhangi"
RAW_PATH = f"s3://{BUCKET}/raw/"
PROCESSED_SILVER = f"s3://{BUCKET}/processed/silver_job_runs/"
PROCESSED_GOLD = f"s3://{BUCKET}/processed/gold_job_metrics/"
PROCESSED_VERDICTS = f"s3://{BUCKET}/processed/gold_sla_verdicts/"

# ---------------------------------------------------------------
# Impact weights - same as job_config.py
# high impact job breaching = more urgent than low impact
# ---------------------------------------------------------------
IMPACT_WEIGHT = {"high": 3, "medium": 2, "low": 1}

# ---------------------------------------------------------------
# Step 1: Read all raw JSON events from S3
# Spark reads all JSON files in the raw/ folder at once
# It automatically figures out the schema from the JSON structure
# ---------------------------------------------------------------
print("Step 1: Reading raw events from S3...")

# multiLine=True tells Spark the JSON spans multiple lines
# This handles JSON arrays like [{...}, {...}, {...}]
raw_df = spark.read.option("multiLine", "true").json(RAW_PATH)

total_events = raw_df.count()
print(f"  Total events loaded: {total_events}")

# ---------------------------------------------------------------
# Step 2: Clean and cast the data
# Make sure all columns have the right data types
# JSON sometimes reads numbers as strings - we fix that here
# ---------------------------------------------------------------
print("Step 2: Cleaning and casting data types...")

clean_df = raw_df.select(
    F.col("job_name").cast(StringType()),
    F.col("team").cast(StringType()),
    F.col("impact").cast(StringType()),
    F.col("sla_minutes").cast(DoubleType()),
    F.col("normal_rows").cast(IntegerType()),
    F.col("cycle_number").cast(IntegerType()),
    F.col("start_time").cast(StringType()),
    F.col("duration_minutes").cast(DoubleType()),
    F.col("rows_processed").cast(IntegerType()),
    F.col("status").cast(StringType()),
    F.col("volume_multiplier").cast(DoubleType())
)

# Add a processing timestamp so we know when Glue ran
clean_df = clean_df.withColumn("processed_at", F.lit(datetime.now().isoformat()))

# ---------------------------------------------------------------
# Step 3: Write silver table - cleaned raw events
# Silver = raw data cleaned up, no transformation yet
# This is useful for debugging and historical replay
# ---------------------------------------------------------------
print("Step 3: Writing silver table to S3...")

clean_df.write \
    .mode("overwrite") \
    .parquet(PROCESSED_SILVER)

print(f"  Silver table written: {PROCESSED_SILVER}")

# ---------------------------------------------------------------
# Step 4: Compute per-job metrics
# Group all events by job name and compute summary statistics
# This tells us what "normal" looks like for each job
# Same as SQL: SELECT job_name, AVG(duration), STDDEV(duration)
#              FROM events GROUP BY job_name
# ---------------------------------------------------------------
print("Step 4: Computing per-job metrics...")

metrics = clean_df.groupBy(
    "job_name",
    "team",
    "impact",
    "sla_minutes",
    "normal_rows"
).agg(

    # Total number of runs seen for this job
    F.count("duration_minutes").alias("total_runs"),

    # Average duration across all runs
    F.avg("duration_minutes").alias("avg_duration"),

    # p95 - 95% of runs finish within this time
    # Better than max because it ignores rare extreme outliers
    F.percentile_approx("duration_minutes", 0.95).alias("p95_duration"),

    # Standard deviation of duration
    # Measures how consistent the job is run to run
    # Low stddev = consistent job, High stddev = unpredictable job
    F.stddev("duration_minutes").alias("stddev_duration"),

    # Average rows processed per run
    F.avg("rows_processed").alias("avg_rows"),

    # Standard deviation of rows
    F.stddev("rows_processed").alias("stddev_rows"),

    # Failure rate - fraction of runs that failed
    # We give failed=1, success=0, then average them
    F.avg(
        F.when(F.col("status") == "failed", 1).otherwise(0)
    ).alias("failure_rate"),

    # Latest run details - what happened most recently?
    F.last("duration_minutes").alias("latest_duration"),
    F.last("rows_processed").alias("latest_rows"),
    F.last("status").alias("latest_status"),
    F.last("start_time").alias("latest_run_time"),
    F.last("cycle_number").alias("latest_cycle")
)

print(f"  Metrics computed for {metrics.count()} jobs")

# ---------------------------------------------------------------
# Step 5: Add drift scores
# Drift score = z-score = how unusual is the latest run?
# Formula: (latest_value - average) / standard_deviation
#
# Score 0   = perfectly normal
# Score 1.5 = a bit unusual, worth watching
# Score 2.0 = clearly unusual, likely a problem
# Score 3.0+ = very unusual, almost certainly a problem
#
# We check BOTH time drift and volume drift
# A job finishing fast but processing fewer rows is also dangerous
# ---------------------------------------------------------------
print("Step 5: Computing drift scores...")

# Time drift - is the job running slower than usual?
metrics = metrics.withColumn(
    "time_drift_score",
    F.when(
        F.col("stddev_duration") > 0,
        (F.col("latest_duration") - F.col("avg_duration")) / F.col("stddev_duration")
    ).otherwise(0.0)
)

# Volume drift - is the job processing fewer rows than usual?
# Negative score = fewer rows than normal = dangerous
metrics = metrics.withColumn(
    "volume_drift_score",
    F.when(
        F.col("stddev_rows") > 0,
        (F.col("latest_rows") - F.col("avg_rows")) / F.col("stddev_rows")
    ).otherwise(0.0)
)

# ---------------------------------------------------------------
# Step 6: Add SLA verdict
# Three possible verdicts based on drift scores and SLA limits:
#   breached = job already exceeded its SLA time limit
#   at_risk  = job is trending toward breach, not there yet
#   safe     = job is running normally
# ---------------------------------------------------------------
print("Step 6: Computing SLA verdicts...")

metrics = metrics.withColumn(
    "verdict",
    F.when(
        # Already exceeded the SLA time promise
        F.col("latest_duration") > F.col("sla_minutes"),
        "breached"
    ).when(
        # Drifting significantly in time OR volume
        (F.col("time_drift_score") > 1.5) | (F.col("volume_drift_score") < -1.5),
        "at_risk"
    ).otherwise(
        "safe"
    )
)

# ---------------------------------------------------------------
# Step 7: Add business priority score
# Priority = how much it drifted × how important the job is
# High impact job drifting a lot = fix this first
# ---------------------------------------------------------------
print("Step 7: Computing priority scores...")

# Map impact string to a number
metrics = metrics.withColumn(
    "impact_weight",
    F.when(F.col("impact") == "high", 3)
     .when(F.col("impact") == "medium", 2)
     .otherwise(1)
)

# Priority score = drift amount × impact weight
metrics = metrics.withColumn(
    "priority_score",
    F.round(
        F.abs(F.col("time_drift_score")) * F.col("impact_weight"),
        2
    )
)

# Add a timestamp for when this verdict was computed
metrics = metrics.withColumn(
    "verdict_computed_at",
    F.lit(datetime.now().isoformat())
)

# ---------------------------------------------------------------
# Step 8: Write gold metrics table to S3
# Gold = fully processed, business-ready data
# This is what Athena and Redshift will query
# ---------------------------------------------------------------
print("Step 8: Writing gold metrics table to S3...")

metrics.write \
    .mode("overwrite") \
    .parquet(PROCESSED_GOLD)

print(f"  Gold metrics table written: {PROCESSED_GOLD}")

# ---------------------------------------------------------------
# Step 9: Write a separate verdicts table
# This contains only jobs that are at_risk or breached
# Easier to query for alerts and the dashboard
# ---------------------------------------------------------------
print("Step 9: Writing verdicts table...")

verdicts = metrics.filter(
    F.col("verdict") != "safe"
).select(
    "job_name",
    "team",
    "impact",
    "verdict",
    "time_drift_score",
    "volume_drift_score",
    "priority_score",
    "latest_duration",
    "sla_minutes",
    "latest_rows",
    "avg_rows",
    "latest_status",
    "failure_rate",
    "verdict_computed_at"
).orderBy(F.col("priority_score").desc())

verdicts.write \
    .mode("overwrite") \
    .parquet(PROCESSED_VERDICTS)

print(f"  Verdicts table written: {PROCESSED_VERDICTS}")

# ---------------------------------------------------------------
# Step 10: Print summary to Glue logs
# You can see this in CloudWatch logs after the job runs
# ---------------------------------------------------------------
print("\n" + "=" * 50)
print("JOB SUMMARY")
print("=" * 50)

total_jobs = metrics.count()
breached_count = metrics.filter(F.col("verdict") == "breached").count()
at_risk_count  = metrics.filter(F.col("verdict") == "at_risk").count()
safe_count     = metrics.filter(F.col("verdict") == "safe").count()

print(f"Total jobs monitored : {total_jobs}")
print(f"Safe                 : {safe_count}")
print(f"At risk              : {at_risk_count}")
print(f"Breached             : {breached_count}")
print(f"Total events processed: {total_events}")
print("=" * 50)

if breached_count > 0 or at_risk_count > 0:
    print("\nUrgent jobs:")
    metrics.filter(F.col("verdict") != "safe") \
           .orderBy(F.col("priority_score").desc()) \
           .select(
               "job_name",
               "team",
               "verdict",
               F.round("time_drift_score", 2).alias("time_drift"),
               F.round("volume_drift_score", 2).alias("vol_drift"),
               "priority_score"
           ) \
           .show(10, truncate=False)

# Commit the Glue job - required at the end of every Glue script
job.commit()
print("\nGlue job completed successfully")
