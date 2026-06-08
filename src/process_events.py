# process_events.py
# ---------------------------------------------------------------
# What this file does:
# Reads raw JSON events from data/raw/ using PySpark
# Computes per-job metrics (avg duration, stddev, failure rate etc.)
# Adds drift scores to detect anomalies
# Adds SLA verdict (safe / at_risk / breached) per job
# Writes everything to Delta Lake in output/
#
# How to run: python process_events.py (in Terminal 2)
# Keep generate_events.py running in Terminal 1 while this runs
# ---------------------------------------------------------------
import os
os.environ["SPARK_LOCAL_HOSTNAME"] = "localhost"
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from job_config import JOBS, IMPACT_WEIGHT

# Where to read raw events from
RAW_DATA_PATH = "../data/raw"

# Where to write our Delta Lake tables
SILVER_PATH = "../output/silver_job_runs"    # cleaned raw events
METRICS_PATH = "../output/gold_job_metrics"  # aggregated stats + verdicts


def create_spark_session():
    # Start the Spark engine on your laptop
    # Think of this like starting a car before you can drive
    # We load the Delta Lake plugin so Spark can read/write Delta format
    # This runs once at the start and we reuse spark everywhere

    spark = SparkSession.builder \
    .appName("SLA Intelligence Engine") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

    # Hide Spark's internal logs so we can see our own print statements clearly
    spark.sparkContext.setLogLevel("ERROR")

    print("Spark session started successfully")
    return spark


def read_new_files(spark, already_processed):
    # Look inside data/raw/ for any JSON files we haven't processed yet
    # already_processed is a set that tracks which files we've already read
    # This way we never read the same file twice

    # Get all JSON files in the raw folder
    all_files = [f for f in os.listdir(RAW_DATA_PATH) if f.endswith(".json")]

    # Filter to only files we haven't seen before
    new_files = [f for f in all_files if f not in already_processed]

    if not new_files:
        # No new files arrived since last check
        return None, already_processed

    print(f"  Found {len(new_files)} new file(s) to process")

    # Build the full file paths so Spark can find them
    full_paths = [os.path.abspath(f"{RAW_DATA_PATH}/{f}") for f in new_files]

    # Read all new JSON files into one Spark DataFrame
    # A DataFrame is like a table - rows and columns - just like pandas or Excel
    # Spark automatically figures out the column names from the JSON structure
    df = spark.read.json(full_paths)

    # Add all new files to our processed set so we skip them next cycle
    already_processed.update(new_files)

    return df, already_processed


def compute_metrics(df):
    # Group all events by job name and compute summary statistics
    # This is exactly like SQL: SELECT job_name, AVG(duration), STDDEV(duration)...
    # FROM events GROUP BY job_name
    #
    # These stats become our "baseline" - what is normal for each job
    # Later we compare the latest run against this baseline to detect anomalies

    metrics = df.groupBy(
        "job_name",
        "team",
        "impact",
        "sla_minutes",
        "normal_rows"
    ).agg(

        # How many times has this job run in total?
        F.count("duration_minutes").alias("total_runs"),

        # What is the average duration across all runs?
        F.avg("duration_minutes").alias("avg_duration"),

        # p95 duration - 95% of runs finish within this time
        # More useful than max because it ignores rare extreme outliers
        F.percentile_approx("duration_minutes", 0.95).alias("p95_duration"),

        # Standard deviation of duration
        # Low stddev = job is consistent, High stddev = job is unpredictable
        F.stddev("duration_minutes").alias("stddev_duration"),

        # Average number of rows processed per run
        F.avg("rows_processed").alias("avg_rows"),

        # Standard deviation of rows - how much does volume vary?
        F.stddev("rows_processed").alias("stddev_rows"),

        # Failure rate - what fraction of runs failed?
        # When status == failed we give it 1, otherwise 0, then average those
        F.avg(
            F.when(F.col("status") == "failed", 1).otherwise(0)
        ).alias("failure_rate"),

        # What happened in the most recent run?
        # F.last() picks the last value seen in the group
        F.last("duration_minutes").alias("latest_duration"),
        F.last("rows_processed").alias("latest_rows"),
        F.last("status").alias("latest_status"),
        F.last("start_time").alias("latest_run_time")
    )

    return metrics


def add_drift_scores(metrics):
    # Drift score = z-score = how unusual is the latest run compared to normal?
    # Formula: (latest_value - average) / standard_deviation
    #
    # Score of 0   = perfectly normal
    # Score of 1.5 = a bit unusual, worth watching
    # Score of 2.0 = clearly unusual, likely a problem
    # Score of 3.0 = very unusual, almost certainly a problem
    #
    # We compute two drift scores:
    # 1. time_drift_score  - is the job running slower than usual?
    # 2. volume_drift_score - is the job processing fewer rows than usual?
    #
    # A job that finishes fast but processed half the expected rows
    # is actually MORE dangerous than a slow job - data might be missing

    # Time drift score
    metrics = metrics.withColumn(
        "time_drift_score",
        # Only compute if stddev > 0, otherwise we get division by zero
        F.when(
            F.col("stddev_duration") > 0,
            (F.col("latest_duration") - F.col("avg_duration")) / F.col("stddev_duration")
        ).otherwise(0.0)
    )

    # Volume drift score
    # Negative score means fewer rows than normal - that is the dangerous direction
    metrics = metrics.withColumn(
        "volume_drift_score",
        F.when(
            F.col("stddev_rows") > 0,
            (F.col("latest_rows") - F.col("avg_rows")) / F.col("stddev_rows")
        ).otherwise(0.0)
    )

    return metrics


def add_sla_verdict(metrics):
    # Assign a verdict to each job based on its drift scores and SLA
    # Three possible verdicts:
    #   breached = job already exceeded its SLA time limit
    #   at_risk  = job hasn't breached yet but is trending toward breach
    #   safe     = job is running normally
    #
    # F.when().when().otherwise() is just like if/elif/else in Python
    # Spark evaluates these rules in order for every row

    metrics = metrics.withColumn(
        "verdict",
        F.when(
            # Rule 1: already took longer than the SLA promise -> breached
            F.col("latest_duration") > F.col("sla_minutes"),
            "breached"
        ).when(
            # Rule 2: drifting significantly in time OR volume -> at risk
            # | means OR - either condition triggers at_risk
            (F.col("time_drift_score") > 1.5) | (F.col("volume_drift_score") < -1.5),
            "at_risk"
        ).otherwise(
            # Rule 3: everything looks normal -> safe
            "safe"
        )
    )

    # Add impact weight as a number so we can multiply it
    # high impact = 3, medium = 2, low = 1
    # This tells us HOW IMPORTANT the job is to the business
    metrics = metrics.withColumn(
        "impact_weight",
        F.when(F.col("impact") == "high", 3)
         .when(F.col("impact") == "medium", 2)
         .otherwise(1)
    )

    # Priority score = how much it drifted × how important the job is
    # Example: high impact job (weight 3) with drift score 2.5 = priority 7.5
    # Example: low impact job (weight 1) with drift score 2.5 = priority 2.5
    # We fix the high impact job first
    metrics = metrics.withColumn(
        "priority_score",
        F.round(
            F.abs(F.col("time_drift_score")) * F.col("impact_weight"),
            2  # round to 2 decimal places
        )
    )

    return metrics


def write_to_parquet(df, path):
    # Save DataFrame as Parquet format
    # Parquet is a columnar storage format - stores data column by column
    # This makes it much faster for analytics queries than row-based formats like CSV
    # Delta Lake is built on top of Parquet - same underlying format, just smarter
    df.write \
        .mode("overwrite") \
        .parquet(path)
    print(f"  Saved to Parquet: {path}")


def print_summary(metrics):
    # Print a quick human readable summary to the terminal
    # This is just for us to see what's happening while the system runs

    total = metrics.count()
    breached = metrics.filter(F.col("verdict") == "breached").count()
    at_risk  = metrics.filter(F.col("verdict") == "at_risk").count()
    safe     = metrics.filter(F.col("verdict") == "safe").count()

    print(f"  Status: {total} jobs | {safe} safe | {at_risk} at risk | {breached} BREACHED")

    # Show top 5 most urgent jobs (highest priority score, not safe)
    urgent = metrics.filter(F.col("verdict") != "safe")

    if urgent.count() > 0:
        print("  Most urgent jobs:")
        urgent.orderBy(F.col("priority_score").desc()) \
              .select(
                  "job_name",
                  "team",
                  "verdict",
                  F.round("time_drift_score", 2).alias("time_drift"),
                  F.round("volume_drift_score", 2).alias("vol_drift"),
                  "priority_score"
              ) \
              .show(5, truncate=False)
    else:
        print("  All jobs are running safely")


def run_processor():
    print("=" * 50)
    print("Pipeline SLA Intelligence Engine - Processor")
    print("Checking for new events every 15 seconds")
    print("=" * 50)
    print()

    # Start Spark once - reuse for every cycle
    spark = create_spark_session()

    # This set tracks which files we have already processed
    # Starts empty, grows as we process more files
    already_processed = set()

    # This holds ALL events seen so far across all cycles
    # We keep adding to it so our metrics improve over time
    all_events_df = None

    cycle = 1

    while True:
        print(f"--- Cycle {cycle} ---")

        # Step 1: Check for new files
        new_df, already_processed = read_new_files(spark, already_processed)

        if new_df is not None:

            # Step 2: Add new events to our running total
            if all_events_df is None:
                # First cycle - no existing data yet
                all_events_df = new_df
            else:
                # Stack new events on top of existing events
                # union() is like SQL UNION ALL - combines two tables vertically
                all_events_df = all_events_df.union(new_df)

            # Step 3: Save all raw events to silver Delta Lake table
            write_to_parquet(all_events_df, SILVER_PATH)

            # Step 4: Compute metrics across everything we've seen
            metrics = compute_metrics(all_events_df)

            # Step 5: Add drift scores (z-scores for time and volume)
            metrics = add_drift_scores(metrics)

            # Step 6: Add SLA verdict and priority score
            metrics = add_sla_verdict(metrics)

            # Step 7: Save metrics to gold Delta Lake table
            write_to_parquet(metrics, METRICS_PATH)

            # Step 8: Print summary so we can see what's happening
            print_summary(metrics)

        else:
            print("  No new files yet - waiting for generator...")

        print()
        cycle += 1

        # Wait 15 seconds before checking again
        # Generator drops a file every 10 seconds so we never miss one
        time.sleep(15)


# Start the processor
run_processor()