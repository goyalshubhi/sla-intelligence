# job_config.py
# This file defines all 50 pipeline jobs and their SLA contracts
# Think of this as our "database" of what jobs exist and what they promise

# Each job has:
# name         - what the job is called
# team         - which team owns it
# impact       - how critical it is to the business (high/medium/low)
# sla_minutes  - how long the job is allowed to take (the promise)
# normal_rows  - how many rows it normally processes
# depends_on   - which jobs must finish before this one can run

JOBS = [
    # --- Settlement jobs (Finance team, high impact) ---
    {"name": "settlement_intraday",      "team": "finance",   "impact": "high",   "sla_minutes": 30,  "normal_rows": 50000,  "depends_on": []},
    {"name": "settlement_eod",           "team": "finance",   "impact": "high",   "sla_minutes": 60,  "normal_rows": 200000, "depends_on": ["settlement_intraday"]},
    {"name": "settlement_reconcile",     "team": "finance",   "impact": "high",   "sla_minutes": 45,  "normal_rows": 150000, "depends_on": ["settlement_eod"]},
    {"name": "nostro_reconcile",         "team": "finance",   "impact": "high",   "sla_minutes": 40,  "normal_rows": 80000,  "depends_on": ["settlement_eod"]},
    {"name": "fx_rate_sync",             "team": "finance",   "impact": "medium", "sla_minutes": 15,  "normal_rows": 5000,   "depends_on": []},
    {"name": "ledger_update",            "team": "finance",   "impact": "high",   "sla_minutes": 50,  "normal_rows": 120000, "depends_on": ["settlement_reconcile"]},
    {"name": "interest_accrual",         "team": "finance",   "impact": "medium", "sla_minutes": 35,  "normal_rows": 90000,  "depends_on": ["ledger_update"]},
    {"name": "fee_calculation",          "team": "finance",   "impact": "medium", "sla_minutes": 25,  "normal_rows": 40000,  "depends_on": ["settlement_intraday"]},

    # --- Fraud jobs (Risk team, high impact) ---
    {"name": "fraud_signal_agg",         "team": "risk",      "impact": "high",   "sla_minutes": 20,  "normal_rows": 300000, "depends_on": []},
    {"name": "fraud_model_score",        "team": "risk",      "impact": "high",   "sla_minutes": 25,  "normal_rows": 300000, "depends_on": ["fraud_signal_agg"]},
    {"name": "fraud_alert_dispatch",     "team": "risk",      "impact": "high",   "sla_minutes": 10,  "normal_rows": 1000,   "depends_on": ["fraud_model_score"]},
    {"name": "blacklist_sync",           "team": "risk",      "impact": "high",   "sla_minutes": 15,  "normal_rows": 20000,  "depends_on": []},
    {"name": "velocity_check",           "team": "risk",      "impact": "medium", "sla_minutes": 20,  "normal_rows": 150000, "depends_on": ["fraud_signal_agg"]},
    {"name": "device_fingerprint_sync",  "team": "risk",      "impact": "medium", "sla_minutes": 30,  "normal_rows": 50000,  "depends_on": []},
    {"name": "chargeback_flag",          "team": "risk",      "impact": "medium", "sla_minutes": 25,  "normal_rows": 8000,   "depends_on": ["fraud_model_score"]},

    # --- Reporting jobs (Analytics team, medium impact) ---
    {"name": "daily_txn_report",         "team": "analytics", "impact": "medium", "sla_minutes": 45,  "normal_rows": 500000, "depends_on": ["settlement_eod"]},
    {"name": "weekly_revenue_report",    "team": "analytics", "impact": "medium", "sla_minutes": 90,  "normal_rows": 800000, "depends_on": ["ledger_update"]},
    {"name": "merchant_summary",         "team": "analytics", "impact": "low",    "sla_minutes": 60,  "normal_rows": 200000, "depends_on": ["daily_txn_report"]},
    {"name": "customer_spend_report",    "team": "analytics", "impact": "low",    "sla_minutes": 60,  "normal_rows": 300000, "depends_on": ["daily_txn_report"]},
    {"name": "channel_performance",      "team": "analytics", "impact": "low",    "sla_minutes": 45,  "normal_rows": 100000, "depends_on": ["daily_txn_report"]},
    {"name": "cohort_analysis",          "team": "analytics", "impact": "low",    "sla_minutes": 120, "normal_rows": 400000, "depends_on": ["customer_spend_report"]},
    {"name": "revenue_forecast",         "team": "analytics", "impact": "medium", "sla_minutes": 75,  "normal_rows": 250000, "depends_on": ["weekly_revenue_report"]},

    # --- Regulatory jobs (Compliance team, high impact) ---
    {"name": "rbi_report_gen",           "team": "compliance","impact": "high",   "sla_minutes": 60,  "normal_rows": 100000, "depends_on": ["settlement_reconcile", "ledger_update"]},
    {"name": "aml_screening",            "team": "compliance","impact": "high",   "sla_minutes": 45,  "normal_rows": 200000, "depends_on": ["fraud_signal_agg"]},
    {"name": "kyc_refresh",              "team": "compliance","impact": "high",   "sla_minutes": 50,  "normal_rows": 30000,  "depends_on": []},
    {"name": "sanctions_check",          "team": "compliance","impact": "high",   "sla_minutes": 20,  "normal_rows": 50000,  "depends_on": ["blacklist_sync"]},
    {"name": "audit_log_archive",        "team": "compliance","impact": "medium", "sla_minutes": 40,  "normal_rows": 500000, "depends_on": []},
    {"name": "pci_data_mask",            "team": "compliance","impact": "high",   "sla_minutes": 35,  "normal_rows": 180000, "depends_on": ["audit_log_archive"]},

    # --- Data platform jobs (Platform team, medium impact) ---
    {"name": "raw_data_ingestion",       "team": "platform",  "impact": "high",   "sla_minutes": 20,  "normal_rows": 1000000,"depends_on": []},
    {"name": "schema_validation",        "team": "platform",  "impact": "high",   "sla_minutes": 15,  "normal_rows": 1000000,"depends_on": ["raw_data_ingestion"]},
    {"name": "data_quality_check",       "team": "platform",  "impact": "medium", "sla_minutes": 25,  "normal_rows": 500000, "depends_on": ["schema_validation"]},
    {"name": "master_data_sync",         "team": "platform",  "impact": "medium", "sla_minutes": 30,  "normal_rows": 80000,  "depends_on": ["schema_validation"]},
    {"name": "delta_compaction",         "team": "platform",  "impact": "low",    "sla_minutes": 45,  "normal_rows": 2000000,"depends_on": []},
    {"name": "metadata_catalog_sync",    "team": "platform",  "impact": "low",    "sla_minutes": 20,  "normal_rows": 10000,  "depends_on": []},
    {"name": "retention_cleanup",        "team": "platform",  "impact": "low",    "sla_minutes": 60,  "normal_rows": 300000, "depends_on": []},

    # --- Customer jobs (Product team, medium impact) ---
    {"name": "wallet_balance_sync",      "team": "product",   "impact": "high",   "sla_minutes": 10,  "normal_rows": 400000, "depends_on": ["settlement_intraday"]},
    {"name": "rewards_calculation",      "team": "product",   "impact": "medium", "sla_minutes": 30,  "normal_rows": 200000, "depends_on": ["fee_calculation"]},
    {"name": "notification_dispatch",    "team": "product",   "impact": "medium", "sla_minutes": 15,  "normal_rows": 50000,  "depends_on": ["wallet_balance_sync"]},
    {"name": "statement_generation",     "team": "product",   "impact": "medium", "sla_minutes": 45,  "normal_rows": 150000, "depends_on": ["ledger_update"]},
    {"name": "emi_schedule_update",      "team": "product",   "impact": "medium", "sla_minutes": 25,  "normal_rows": 80000,  "depends_on": ["interest_accrual"]},
    {"name": "credit_limit_refresh",     "team": "product",   "impact": "medium", "sla_minutes": 35,  "normal_rows": 100000, "depends_on": ["fraud_model_score"]},

    # --- Operations jobs (Ops team, low-medium impact) ---
    {"name": "merchant_onboard_sync",    "team": "ops",       "impact": "medium", "sla_minutes": 30,  "normal_rows": 5000,   "depends_on": ["kyc_refresh"]},
    {"name": "terminal_health_check",    "team": "ops",       "impact": "low",    "sla_minutes": 20,  "normal_rows": 20000,  "depends_on": []},
    {"name": "settlement_advice_gen",    "team": "ops",       "impact": "medium", "sla_minutes": 40,  "normal_rows": 30000,  "depends_on": ["settlement_reconcile"]},
    {"name": "dispute_case_update",      "team": "ops",       "impact": "medium", "sla_minutes": 35,  "normal_rows": 8000,   "depends_on": ["chargeback_flag"]},
    {"name": "refund_processing",        "team": "ops",       "impact": "high",   "sla_minutes": 20,  "normal_rows": 15000,  "depends_on": ["settlement_intraday"]},
    {"name": "vendor_payout",            "team": "ops",       "impact": "high",   "sla_minutes": 45,  "normal_rows": 25000,  "depends_on": ["settlement_eod"]},
    {"name": "interchange_calc",         "team": "ops",       "impact": "medium", "sla_minutes": 30,  "normal_rows": 60000,  "depends_on": ["settlement_reconcile"]},
    {"name": "pos_transaction_sync",     "team": "ops",       "impact": "medium", "sla_minutes": 25,  "normal_rows": 180000, "depends_on": ["raw_data_ingestion"]},
    {"name": "upi_reconcile",            "team": "ops",       "impact": "high",   "sla_minutes": 30,  "normal_rows": 400000, "depends_on": ["settlement_intraday"]}
]

# Impact weights — used later to calculate business priority score
# A high impact job breaching is far more urgent than a low impact one
IMPACT_WEIGHT = {
    "high": 3,
    "medium": 2,
    "low": 1
}