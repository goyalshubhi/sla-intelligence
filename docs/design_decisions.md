# PayWatch — Design Decisions Log

> Every architectural choice in this project is documented here.
> For each decision: what we chose, what we considered, why, and what would change at scale.
> This is the document you open when an interviewer asks "why did you build it that way?"

---

## Decision 1: IAM User vs Root Account

**Decision:** Use IAM user (Shubhangi) with scoped permissions, not the root account.

**Options considered:**
- Root account — full access, simplest setup
- IAM user with broad permissions — what we use
- IAM role (assumed by EC2/Lambda) — the production-grade approach

**Why this one:**
Root credentials are permanent and cannot be scoped. If they leak, everything in the account is exposed. An IAM user lets us grant only AmazonS3FullAccess and AWSGlueServiceRole — the exact permissions PayWatch needs. This is the AWS Principle of Least Privilege.

**What would change at scale:**
In production, there would be no IAM user at all. Lambda functions would assume an IAM role at runtime, and credentials would be injected automatically by AWS. No developer would ever hold long-term credentials. Secrets would live in AWS Secrets Manager, not environment variables.

---

## Decision 2: Environment Variables vs Config Files for Secrets

**Decision:** All secrets (AWS keys, GitHub token, SNS ARN) via environment variables. Never in code or committed config files.

**Options considered:**
- Hardcoded in Python source — fastest to write, catastrophically insecure
- `.env` file checked into Git — still visible in version history
- `.env` file gitignored + python-dotenv — reasonable for solo projects
- OS environment variables — what we use for portability
- AWS Secrets Manager — the production approach

**Why this one:**
Environment variables are the standard twelve-factor app approach. They work identically in local development (set via `$env:VAR = "..."` in PowerShell), in Lambda (set in function configuration), and in CI/CD pipelines. The code never changes between environments.

**What would change at scale:**
AWS Secrets Manager with automatic rotation. The application calls `secretsmanager.get_secret_value()` at startup and caches for the session. Keys rotate every 90 days automatically. No human ever sees the current active secret.

---

---

## Decision 3: Airflow vs generate_events.py Simulation

**Decision:** Replace generate_events.py with real Apache Airflow DAGs. PayWatch now monitors actual DAG task instances via the Airflow REST API.

**Options considered:**
- generate_events.py — simple Python script generating fake JSON events, already built
- Airflow — real pipeline orchestrator, creates queryable task instances
- Kafka or another event stream — over-engineered for portfolio scale

**Why this one:**
With generate_events.py, the "monitoring" is fake end-to-end — fake events feeding fake analysis. With Airflow, the DAG runs are real. The task instances have real states (queued, running, success, failed), real durations, and are queryable via a REST API that mirrors what production teams use. The portfolio can truthfully say "this monitors real Airflow runs."

**What would change at scale:**
Managed Airflow (AWS MWAA or Astronomer Cloud) instead of local. The code doesn't change — only the Airflow URL in the API calls switches from localhost:8080 to the managed endpoint.

---

## Decision 4: Astronomer CLI vs Raw Docker vs pip install airflow

**Decision:** Astronomer CLI (`astro dev start`).

**Options considered:**
- `pip install apache-airflow` — installs the Python package only; you still need Postgres, Redis, and scheduler manually
- Raw Docker + docker-compose.yml — full control but requires writing a multi-service compose file and knowing Airflow's internal wiring
- Astronomer CLI — one command spins up all four Airflow components correctly

**Why this one:**
Astronomer CLI produces a project structure (dags/, include/, Dockerfile, requirements.txt) that deploys to managed Airflow without changes. It's the industry standard for local Airflow development. A fresher can explain it in one sentence.

**What would change at scale:**
`astro deploy` instead of `astro dev start`. The same project structure deploys to Astronomer Cloud or MWAA. Zero code changes.

---

## Decision 5: Local Airflow vs Managed Airflow (MWAA)

**Decision:** Local Airflow via Astronomer CLI for development; architecture documented for MWAA deployment.

**Options considered:**
- AWS MWAA — managed Airflow, ~$300/month minimum, no free tier
- Astronomer Cloud — managed Astronomer, free trial available
- Local via Astronomer CLI — free, runs on any machine with Docker

**Why this one:**
Cost. MWAA has no free tier and starts at ~$300/month. For a portfolio project, local development is correct. The architecture (EventBridge → Lambda → S3 → Athena) and the DAG code are identical to what a production MWAA deployment would use.

**What would change at scale:**
Change `AIRFLOW_API_URL = 'http://localhost:8080'` to the MWAA endpoint URL. Add IAM authentication to the API calls. Everything else — DAG structure, S3 paths, lambda_processor logic — stays the same.

---

## Decision 6: 5 Real DAGs + 47 Simulated vs All Simulated vs All Real

**Decision:** 5 real operational DAGs (using RBI/NPCI published data) plus 47 simulated jobs from job_config.py.

**Options considered:**
- All simulated — simpler, but portfolio has no real data claim
- All real — some jobs (fraud detection, settlement) can't use real customer data; not possible
- Hybrid (5 real + 47 simulated) — what we built

**Why this one:**
Five DAGs process genuine public data from RBI and NPCI. This gives the project an honest "real data" claim. The 47 simulated jobs have realistic volume parameters derived from the same RBI/NPCI publications, so they're documented simulations, not pure invention.

**What would change at scale:**
The simulated jobs would be replaced by actual upstream data pipelines. The monitoring framework (lambda_processor, engine.py, dashboard) requires zero changes — it reads from Airflow REST API regardless of what the DAG actually does.

---

## Decision 7: Programmatic DAG Generation vs Individual Files

**Decision:** One file (payment_ops_dags.py) generates 47 DAGs via a for loop over job_config.py.

**Options considered:**
- 47 separate .py files — completely impractical to maintain; 47 files × 50 lines = 2,350 lines of nearly identical code
- One file with a loop + globals() — what we built
- Airflow dynamic DAGs (newer Airflow 3.x feature) — more complex, less explainable

**Why this one:**
The JOBS list in job_config.py is the single source of truth for all job parameters. The DAG generator reads from it. If a job's SLA changes, you update job_config.py once and the DAG automatically reflects it. This is the DRY principle (Don't Repeat Yourself) applied to infrastructure.

**What would change at scale:**
The job parameters would come from a database or config service rather than a Python list. The generator code doesn't change — only the data source.

---

---

## Decision 8: Airflow REST API vs Airflow Metadata DB Direct Connection

**Decision:** Read task instances from the Airflow REST API (`/api/v2/dags/~/dagRuns/~/taskInstances`), not by connecting directly to the Postgres metadata database.

**Options considered:**
- Direct Postgres query — fastest, but tight coupling to Airflow internals
- Airflow REST API — the documented, stable interface
- XCom / S3 task results only — misses task-level duration and state

**Why this one:**
The Airflow REST API is the documented public interface. Airflow's internal schema (which tables it uses, column names) can change between versions. The API is versioned (`v2`) and documented. If Airflow upgrades, the API contract is maintained. Direct DB access would break silently on an Airflow upgrade.

**What would change at scale:**
In production (MWAA), the Airflow REST API endpoint changes from `localhost:8080` to the MWAA endpoint URL. IAM authentication replaces the JWT token flow. Zero code changes in engine.py.

---

## Decision 9: Single engine.py vs Logic in Each Processor

**Decision:** Single `src/engine.py` contains all intelligence functions. `lambda_processor.py` is a thin orchestrator that imports from it.

**Options considered:**
- Logic in lambda_processor.py — simpler, one file
- Logic spread across multiple files — harder to maintain
- Single engine.py — what we built

**Why this one:**
engine.py functions only take DataFrames and return DataFrames — no I/O dependencies. This makes every function unit testable without mocking AWS or Airflow. lambda_processor.py handles I/O (hard to test); engine.py handles logic (easy to test). The separation also means: if you swap Airflow for a different scheduler, only lambda_processor.py changes.

**What would change at scale:**
engine.py would stay identical. lambda_processor.py would point to a different data source (e.g., MWAA API endpoint, Databricks Jobs API, or a custom metrics store). The intelligence layer is environment-agnostic by design.

---

## Decision 10: GitHub Actions Runs vs Commits as External Signal

**Decision:** Query `/actions/runs` endpoint to get workflow run results (success/failure), not `/commits` to get push timestamps.

**Options considered:**
- Commits endpoint — tells you code was pushed, not whether it worked
- Actions runs endpoint — tells you if CI passed or failed

**Why this one:**
A recent commit doesn't mean the code is broken — it could be a documentation fix. A failed Actions run within 30 minutes of a job slowdown is a much stronger signal that broken code caused the performance issue. The evidence score adds 35 points for a failed run (vs 0 for a passing run). This makes the logic_bug fingerprint more precise.

**What would change at scale:**
Pull additional signals: deployment events from Kubernetes, feature flag changes from LaunchDarkly, database migration logs. Each additional corroborating signal increases the evidence score. The fingerprinting framework is additive — new signals plug into existing elif chains without touching other fingerprint types.

---

## Decision 11: Synthetic Test Scenarios vs Real Data for Calibration

**Decision:** 50 hand-crafted synthetic scenarios (10 per fingerprint type) to verify the classification logic before real data accumulates.

**Options considered:**
- No tests — trust the code review
- Real historical incidents — not available; this is a new system
- ML training set — requires labelled real incidents; same problem
- Synthetic calibration tests — what we built

**Why this one:**
The fingerprinting logic is an if/elif chain with specific numerical thresholds (vd < -1.5, td > 1.5, etc.). The only way to verify those thresholds work as intended is to construct inputs that sit on either side of each threshold. Synthetic data lets us do this precisely. Real data doesn't arrive in neat categories — it's noisy and ambiguous.

**What would change at scale:**
Add a feedback loop: when an on-call engineer resolves an incident and marks the root cause, that becomes a labelled data point. After 100+ incidents, train a logistic regression or gradient boosting classifier on the features (time_drift_score, volume_drift_score, unrelated_slow_count, cw_errors, github_conclusion) and replace the rule-based heuristic. The evidence_score becomes a calibrated probability.

---

## Decision 12: Simple Assertions vs pytest

**Decision:** Simple if-statements and print output, no pytest framework.

**Options considered:**
- pytest with fixtures — adds a dependency and a learning curve
- unittest — verbose, more learning curve
- Simple assertions + print — what we built

**Why this one:**
A fresher can read `if actual == expected: correct += 1` and explain it in an interview without knowing pytest syntax. The test file is calibration code, not a production test suite. Adding pytest would make the file harder to explain without adding meaningful capability for this specific use case.

**What would change at scale:**
Convert to pytest with proper fixtures, parametrize decorators, and CI integration. Add property-based testing (Hypothesis) for edge cases. Set up coverage reporting.

---

## Decision 13: Separate Dockerfile for Lambda vs. Astro Runtime

**Decision:** `Dockerfile.lambda` (AWS Lambda base image) is a distinct file from `Dockerfile` (Astro/Airflow runtime), each with its own requirements file (`requirements-lambda.txt` vs `requirements.txt`).

**Options considered:**
- One shared Dockerfile — Astro's runtime image and AWS's Lambda base image are different, incompatible base images
- Separate files per deployment target — what we built

**Why this one:**
The Airflow container needs `apache-airflow-providers-amazon` and runs 24/7 serving the webserver/scheduler. The Lambda container only needs `boto3`, `pandas`, `requests`, `networkx` and runs for seconds every 15 minutes. Mixing the two dependency sets would bloat both images and couple two independently-deployed pieces. `lambda_processor.py` already exposes a `handler(event, context)` function matching Lambda's expected signature — the `if __name__ == "__main__": handler()` block lets the same file run identically both locally and inside Lambda.

**What would change at scale:**
Build both images in CI (GitHub Actions), push `Dockerfile.lambda`'s image to ECR on every merge to main, and let EventBridge trigger the Lambda on a schedule instead of running `lambda_processor.py` in a local terminal.

---

## Decision 14: First End-to-End Run Surfaced Four Real Bugs

**Decision:** Fixed all four rather than shipping a "looks complete but never actually ran" pipeline.

**What broke, in the order discovered:**
1. **Console crash on `→`** — `print()` statements in `engine.py`/`verdict_engine.py` used a unicode arrow. Windows' default console encoding (cp1252) can't render it, so every run crashed *after* the S3 write succeeded but *before* later tables were written. Fix: ASCII `->`.
2. **2-hour Airflow lookback vs. 30-run minimum** — `read_airflow_task_instances()` only looked back 2 hours, but `MIN_RUNS=30` in `engine.py`. A 15-minute-cadence job produces ~8 runs in 2 hours — it can never cross 30. Fix: made the lookback window configurable (`LOOKBACK_HOURS`, default 24).
3. **Airflow API silently caps pages at 100** — requesting `limit=3000` still returned only 100 rows per call, regardless of the requested value. Fix: added offset-based pagination.
4. **JSON array instead of NDJSON** — `df.to_json(orient="records")` writes one JSON array per file. Glue's crawler can't infer per-field columns from that shape without a custom classifier — it just creates a single `array` column, which Athena can't meaningfully query. Fix: `orient="records", lines=True` (newline-delimited JSON), which Glue reads natively.

**Why this matters for the portfolio narrative:** None of these four bugs were visible by reading the code — they only surfaced by actually running the pipeline against real Airflow + S3 + Glue + Athena. "I wrote it" and "I ran it and it worked" are different claims, and an interviewer who asks "walk me through a bug you hit building this" now has a true, specific answer instead of a hypothetical one.

**What would change at scale:** A CI smoke test that runs `lambda_processor.py` against a disposable Airflow/LocalStack environment on every merge would have caught all four before they reached "done."

---

*End of design decisions log.*
