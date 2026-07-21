# PayWatch

**A multi-signal pipeline observability framework demonstrated on India's payment infrastructure context.**

PayWatch monitors Apache Airflow DAG runs — some processing actual RBI and NPCI published data, others simulating operational payment workflows — and correlates signals from multiple sources to produce evidence-based anomaly classification.

**This is not a payment system.** It is a pipeline observability framework with a fintech use case.

---

## The Three Questions It Answers

| Question | Where answered |
|---|---|
| What TYPE of failure is this and who do I call? | Anomaly Fingerprint page / gold_fingerprint table |
| Will we make tonight's RBI/NPCI regulatory deadlines? | Deadline Countdown page / gold_deadline_risk table |
| Are our SLA contracts sustainable as volumes grow? | SLA Health page / gold_sla_health table |

---

## Architecture

```
Airflow DAGs (astro dev start → localhost:8080)
├── 5 real data DAGs (RBI NEFT, NPCI UPI, settlement reconcile,
│                     fraud signal agg, data quality check)
└── 47 simulated payment DAGs (from job_config.py)
         ↓ all write task results to S3 airflow_runs/

lambda_processor.py  (runs every 15 min locally; EventBridge → Lambda in prod)
├── reads Airflow REST API  /api/v2/dags/~/dagRuns/~/taskInstances
├── reads S3 airflow_runs/ for rows_processed
├── reads CloudWatch: S3 5xx errors + Lambda errors
├── reads GitHub Actions: recent workflow run results
└── calls engine.py → writes 5 tables to S3 processed/

engine.py  (imported by lambda_processor.py)
├── compute_metrics    → per-job avg, p95, stddev, z-scores
├── compute_verdicts   → safe / at_risk / breached / insufficient_data
├── compute_fingerprint → 5 failure type classification (uses networkx)
├── compute_sla_health  → volume growth + tightness score
├── compute_deadline_risk → RBI/NPCI deadline buffer analysis
├── write_s3_append    → date-partitioned silver table
└── write_s3_overwrite → atomic staging swap for gold tables

AWS Glue Crawler → Glue Catalog (sla_intelligence_db)
         ↓
Amazon Athena (SQL on 5 tables + airflow_runs)
         ↓
verdict_engine.py  (Terminal 2 — polls Athena every 30s)
└── sends SNS email + writes S3 incident reports + tracks MTTR

dashboard.py  (Terminal 3 — Streamlit, Athena via boto3, auto-refresh 30s)
└── 5 pages: heatmap, fingerprint, deadlines, SLA health, incident log
```

---

## Signal Sources

| Signal | Source | What it detects |
|---|---|---|
| Task duration + rows | Airflow REST API v2 | Statistical anomalies via z-score |
| S3 5xx errors | CloudWatch | Storage/network infrastructure issues |
| Lambda errors | CloudWatch | Compute infrastructure issues |
| GitHub Actions runs | GitHub REST API | Code deployments that may have caused regression |
| Dependency graph | networkx + job_config.py | Cascade failures propagating through the DAG chain |

---

## Five Fingerprint Types

| Type | Condition | Evidence Score | Action |
|---|---|---|---|
| data_quality_issue | Volume down >1.5σ, time normal | 70 | Check upstream data source |
| infrastructure_issue | 2+ unrelated jobs slow + CloudWatch errors | 85 | Route to DevOps |
| cascade_failure | Upstream ancestor in dependency graph is slow | 75 | Fix root job first |
| volume_spike | Duration AND rows up proportionally | 65 | Monitor — self-resolves |
| logic_bug | Isolated slowness, failed CI within 30 min | 40–75 | Check recent deployments |

---

## Airflow DAGs

### Real Data DAGs (5)
| DAG | Schedule | Data Source |
|---|---|---|
| rbi_neft_stats | Daily | RBI Payment System Indicators — 186 crore txns/month (Apr 2025) |
| npci_upi_stats | Daily | NPCI UPI Ecosystem Statistics — 1,656 crore txns/month (Apr 2025) |
| settlement_reconcile | Daily | Simulates EOD reconciliation per RBI circular DPSS.CO.OD.No.1852 |
| fraud_signal_agg | Every 15 min | Velocity check: >10 txns/5min per RBI fraud direction |
| data_quality_check | Every 30 min | PayWatch monitors its own silver_job_runs (meta-observability) |

### Simulated Payment DAGs (47)
Generated programmatically from `job_config.py` via `payment_ops_dags.py`. Each job runs every 15 minutes with realistic duration (70% of SLA ± variance) and a 5% anomaly injection rate. Results written to `s3://sla-intelligence-shubhangi/airflow_runs/`.

---

## How to Run

**Prerequisites:** Docker Desktop running, Astronomer CLI installed, AWS credentials configured.

```bash
# Terminal 1 — Start Airflow
astro dev start
# → Airflow UI: http://localhost:8080 (admin/admin)

# Terminal 1 — Run processor (after Airflow warms up ~7 hours)
py -3.11 src/lambda_processor.py

# Terminal 2 — Run verdict engine
set SNS_TOPIC_ARN=arn:aws:sns:eu-north-1:264384440756:paywatch-alerts
py -3.11 src/verdict_engine.py

# Terminal 3 — Launch dashboard
py -3.11 -m streamlit run src/dashboard.py

# Run calibration tests
py -3.11 src/test_fingerprinting.py
```

---

## Data Sources

All simulation parameters trace to published sources. See [docs/data_sources.md](data_sources.md).

- **RBI NEFT:** https://www.rbi.org.in/Scripts/PaymentSystems_BI.aspx (Apr 2025: 186 crore txns)
- **NPCI UPI:** https://www.npci.org.in/what-we-do/upi/upi-ecosystem-statistics (Apr 2025: 1,656 crore txns)
- **Settlement deadlines:** RBI circular DPSS.CO.OD.No.1852/06.08.005/2020-21
- **Fraud velocity threshold:** RBI Master Direction on Credit Card and Debit Card Operations 2022

---

## Design Decisions

Full rationale in [docs/design_decisions.md](design_decisions.md). Key decisions:

| Decision | Choice | Why |
|---|---|---|
| Orchestration | Apache Airflow (Astronomer CLI) | Real task instances with API; not fake JSON events |
| Intelligence isolation | engine.py adapter | Testable without AWS or Airflow running |
| Anomaly detection | Z-score (σ) | Explainable, calibrated, works without labelled data |
| API auth | Airflow v2 JWT | Documented public API, version-stable |
| GitHub signal | Actions runs (not commits) | Failed CI is stronger evidence than any commit |

---

## Honest Limitations

**What this is not:**
- Not a real payment processor — no actual money moves through this system
- Not a production monitoring system — lacks authentication, multi-tenancy, alerting SLAs
- Not validated on real incidents — fingerprinting accuracy on real failures is unknown
- Not horizontally scalable — pandas in Lambda has memory limits; PySpark or Flink for TPS

**What would change in production:**
- Replace local Airflow with AWS MWAA or Astronomer Cloud
- Replace pandas with PySpark for TB-scale data
- Replace rule-based fingerprinting with a classifier trained on historical incidents
- Replace Athena polling in verdict_engine with Kinesis Data Streams for sub-second latency
- Replace JWT auth with AWS IAM-based Airflow authentication
- Add multi-tenant isolation, audit logging, and role-based access control

---

## Tech Stack

Python 3.11 · Apache Airflow 3.x (Astronomer CLI) · AWS Lambda · Amazon EventBridge · Amazon S3 (partitioned) · AWS Glue Catalog · Amazon Athena · Amazon SNS · Streamlit · networkx · pandas · numpy · boto3 · CloudWatch API · GitHub Actions API · RBI/NPCI Payment System Indicators data
