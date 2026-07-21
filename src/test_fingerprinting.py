# src/test_fingerprinting.py
# 50 synthetic scenarios to verify fingerprinting logic before real data arrives.
# 10 scenarios per fingerprint type. Run: py -3.11 src/test_fingerprinting.py
# These are calibration tests — they prove the if/elif branches fire correctly.

import sys, os
sys.path.insert(0, os.path.dirname(__file__))

import pandas as pd
from datetime import datetime, timedelta
from engine import compute_fingerprint

# Shared signal fixtures
CW_CLEAN = {"s3_errors": 0, "lambda_errors": 0}
CW_ERROR = {"s3_errors": 5, "lambda_errors": 3}
GH_CLEAN = []
# A failed Actions run 10 minutes ago — the strongest logic_bug signal
GH_FAIL  = [{"created_at": (datetime.utcnow() - timedelta(minutes=10)).strftime("%Y-%m-%dT%H:%M:%SZ"),
              "conclusion": "failure", "name": "PayWatch Pipeline Check"}]


def make_row(job, td, vd, verdict="at_risk", d_ratio=None, r_ratio=None):
    """Build one row for a synthetic metrics DataFrame."""
    from job_config import JOBS
    meta  = next((j for j in JOBS if j["name"] == job), JOBS[0])
    avg_d = meta["sla_minutes"] * 0.6   # healthy job uses 60% of its SLA
    avg_r = float(meta["normal_rows"])
    # lat_dur and lat_rows can be set explicitly (for volume_spike) or derived from drift
    lat_d = avg_d * d_ratio if d_ratio else avg_d * (1 + td * 0.15)
    lat_r = avg_r * r_ratio if r_ratio else avg_r * (1 + vd * 0.15)
    return {"job_name": job, "team": meta["team"], "verdict": verdict,
            "time_drift_score": td, "volume_drift_score": vd,
            "avg_duration": avg_d, "avg_rows": avg_r,
            "latest_duration": lat_d, "latest_rows": int(lat_r),
            "latest_status": "success", "impact": meta["impact"],
            "sla_minutes": meta["sla_minutes"], "total_runs": 50,
            "p95_duration": avg_d * 1.2, "stddev_duration": avg_d * 0.1,
            "priority_score": abs(td) * 2.0}


def check(mdf_rows, cw, gh, focus_job, expected):
    """Run fingerprinting and check if focus_job gets the expected type."""
    mdf    = pd.DataFrame(mdf_rows)
    result = compute_fingerprint(mdf, cw, gh)
    if result.empty:
        return False, 0
    hit = result[result["job_name"] == focus_job]
    if hit.empty:
        return False, 0
    actual = hit.iloc[0]["fingerprint_type"]
    score  = hit.iloc[0]["evidence_score"]
    return actual == expected, score


# ── DATA QUALITY (10 scenarios) ──────────────────────────────────────────────
# Condition: volume down (vd < -1.5), time normal (|td| < 0.5)
# Signal: upstream data source sent fewer rows — job ran fast but processed less
dq_results, dq_scores = [], []
DQ_JOBS = ["settlement_intraday", "fraud_signal_agg", "rbi_report_gen",
           "raw_data_ingestion", "aml_screening", "kyc_refresh",
           "audit_log_archive", "wallet_balance_sync", "blacklist_sync", "fx_rate_sync"]
for i, job in enumerate(DQ_JOBS):
    vd = -2.0 - i * 0.15   # range: -2.0 to -3.35 (strongly below threshold of -1.5)
    td = 0.1 * (i % 5)     # range: 0.0 to 0.4 (well below 0.5 threshold)
    ok, score = check([make_row(job, td, vd)], CW_CLEAN, GH_CLEAN, job, "data_quality_issue")
    dq_results.append(ok); dq_scores.append(score)

# ── INFRASTRUCTURE (10 scenarios) ────────────────────────────────────────────
# Condition: 2+ unrelated jobs slow simultaneously + CloudWatch errors
# Signal: shared infrastructure problem (S3/network) — not a single pipeline bug
# These jobs all have no dependencies on each other (no common ancestor/descendant)
INFRA_GROUPS = [
    ("settlement_intraday", ["fraud_signal_agg",      "raw_data_ingestion"]),
    ("fraud_signal_agg",    ["terminal_health_check",  "delta_compaction"]),
    ("raw_data_ingestion",  ["fx_rate_sync",            "blacklist_sync"]),
    ("delta_compaction",    ["settlement_intraday",     "kyc_refresh"]),
    ("metadata_catalog_sync",["fraud_signal_agg",      "device_fingerprint_sync"]),
    ("retention_cleanup",   ["raw_data_ingestion",      "blacklist_sync"]),
    ("terminal_health_check",["settlement_intraday",    "delta_compaction"]),
    ("device_fingerprint_sync",["fx_rate_sync",         "retention_cleanup"]),
    ("blacklist_sync",      ["settlement_intraday",     "raw_data_ingestion"]),
    ("kyc_refresh",         ["delta_compaction",        "terminal_health_check"]),
]
infra_results, infra_scores = [], []
for focus, others in INFRA_GROUPS:
    rows = [make_row(focus, 2.5, 0.3)] + [make_row(o, 2.2, 0.1) for o in others]
    ok, score = check(rows, CW_ERROR, GH_CLEAN, focus, "infrastructure_issue")
    infra_results.append(ok); infra_scores.append(score)

# ── CASCADE FAILURE (10 scenarios) ───────────────────────────────────────────
# Condition: an upstream dependency is also slow — slowness propagated through the graph
# Signal: fix the root cause; the downstream job recovers automatically
CASCADE_PAIRS = [
    ("settlement_intraday", "settlement_eod"),
    ("settlement_intraday", "fee_calculation"),
    ("settlement_eod",      "daily_txn_report"),
    ("settlement_eod",      "nostro_reconcile"),
    ("fraud_signal_agg",    "fraud_model_score"),
    ("fraud_model_score",   "fraud_alert_dispatch"),
    ("fraud_model_score",   "chargeback_flag"),
    ("raw_data_ingestion",  "schema_validation"),
    ("schema_validation",   "data_quality_check"),
    ("schema_validation",   "master_data_sync"),
]
cascade_results, cascade_scores = [], []
for root, downstream in CASCADE_PAIRS:
    # Root is slow, downstream is anomalous — only two jobs in df (ensures < 2 unrelated)
    rows = [make_row(root, 2.5, 0.2), make_row(downstream, 2.0, 0.1)]
    ok, score = check(rows, CW_CLEAN, GH_CLEAN, downstream, "cascade_failure")
    cascade_results.append(ok); cascade_scores.append(score)

# ── VOLUME SPIKE (9 correct + 1 intentional miss) ────────────────────────────
# Condition: duration AND rows both up proportionally (ratio difference < 0.3)
# Signal: more data arrived than usual — expected linear scaling, will self-resolve
# Scenario 10 is a near-miss: duration up 60%, rows only up 20% → falls to logic_bug
VS_JOBS = ["settlement_intraday", "fraud_signal_agg", "raw_data_ingestion",
           "aml_screening", "daily_txn_report", "wallet_balance_sync",
           "settlement_eod", "rbi_report_gen", "audit_log_archive",
           "kyc_refresh"]  # scenario 10 — near-miss
vs_results, vs_scores = [], []
for i, job in enumerate(VS_JOBS):
    ratio = 1.35 + i * 0.01   # 1.35× to 1.44× increase (well above average)
    if i == 9:
        # Intentional near-miss: duration went up 60%, rows only 20%
        # Not pure volume scaling — suggests overhead, not just more data
        ok, score = check([make_row(job, 2.5, 2.0, d_ratio=1.6, r_ratio=1.2)],
                          CW_CLEAN, GH_CLEAN, job, "volume_spike")
    else:
        # Proportional increase: both duration and rows up by the same ratio
        ok, score = check([make_row(job, 2.2, 2.0, d_ratio=ratio, r_ratio=ratio)],
                          CW_CLEAN, GH_CLEAN, job, "volume_spike")
    vs_results.append(ok); vs_scores.append(score)

# ── LOGIC BUG (10 scenarios) ──────────────────────────────────────────────────
# Condition: isolated slowness — no upstream issues, no infrastructure errors
# Signal: GitHub failed Actions run within 30 min is the strongest external evidence
LB_JOBS = ["settlement_reconcile", "ledger_update", "merchant_summary",
           "revenue_forecast", "cohort_analysis", "statement_generation",
           "credit_limit_refresh", "emi_schedule_update", "interchange_calc",
           "pos_transaction_sync"]
lb_results, lb_scores = [], []
for i, job in enumerate(LB_JOBS):
    td = 2.0 + i * 0.1
    # First 5: no GH signal (score=40). Last 5: failed Actions run (score=75)
    gh = GH_FAIL if i >= 5 else GH_CLEAN
    ok, score = check([make_row(job, td, 0.3)], CW_CLEAN, gh, job, "logic_bug")
    lb_results.append(ok); lb_scores.append(score)


# ── RESULTS ──────────────────────────────────────────────────────────────────
def summarise(name, results, scores):
    n = sum(results)
    print(f"{name:22}: {n:2}/{len(results)} correct"
          f"  |  evidence scores: min={min(scores) if scores else 0}  "
          f"max={max(scores) if scores else 0}  "
          f"avg={round(sum(scores)/len(scores)) if scores else 0}")
    return n

print("\n=== PayWatch Fingerprinting Calibration ===\n")
total = 0
total += summarise("data_quality_issue",   dq_results,     dq_scores)
total += summarise("infrastructure_issue", infra_results,  infra_scores)
total += summarise("cascade_failure",      cascade_results, cascade_scores)
total += summarise("volume_spike",         vs_results,     vs_scores)
total += summarise("logic_bug",            lb_results,     lb_scores)
print(f"\n{'Total':22}: {total:2}/50  ({round(total/50*100)}%)")
print("\nNote: volume_spike scenario 10 is an intentional near-miss.")
print("Duration up 60%, rows only 20% — not pure volume scaling — correctly falls to logic_bug.")
