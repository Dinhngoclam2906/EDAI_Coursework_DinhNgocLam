# FinGuard — Progress Log

Chronological record of development sessions, fixes, and pipeline run outcomes.
For current state / next steps, see `CLAUDE.md`.

---

## 2026-06-14 — Image Rebuild, Ivy Cache Volume & Clarifications

### Changes
- **`infra/Dockerfile.airflow`**: Added `procps` — eliminates `ps: command not found` warning on every Spark task
- **`infra/docker-compose.airflow.yml`**: Added `airflow_ivy_cache:/home/airflow/.ivy2` volume — Delta JARs (io.delta:delta-spark_2.12:3.2.0) now survive container restarts; first post-rebuild run still re-downloads once, subsequent runs use cache
- **`infra/docker-compose.airflow.yml`**: Added `DATAHUB_GMS_URL: "http://datahub-gms:8080"` — lineage.py now resolves the correct DataHub endpoint inside Docker network
- Rebuilt image (~20 min total: Java apt install + pip pyspark 317MB) and force-recreated all 4 containers

### Debezium & Feast — Design-Layer Only
Confirmed via codebase search: neither `debezium` nor `feast` appears in any Python pipeline file.
- **Debezium**: container defined in `docker-compose.yml` but Bronze ingestion reads from Parquet/JSONL (data generator shortcut), not from CDC events
- **Feast**: listed in tech stack; `pipelines/feature/` writes Delta tables but never calls `feast apply` or any Feast API. Integration planned for Section 04.1 (ML track) using reference material in `L11_Consumption_Layer/`

### Result
All 4 Airflow containers running with new image. `ps: command not found` warning resolved. Ivy cache will warm on next DAG run.

---

## 2026-06-14 — Airflow Stack Bring-Up & Delta Lake Fix

### Goal
Get the `finguard_pipeline` DAG running end-to-end inside the Airflow Docker stack.

### Issues Found & Fixed

#### 1. `infra/Dockerfile.airflow` — Invalid pip flags + blocked mirror
- `pip install --resume-retries` → `--resume-retries` is a `uv` flag, rejected by pip silently, skipping the entire install layer
- `-i https://pypi.tuna.tsinghua.edu.cn/simple/` → Tsinghua mirror returned HTTP 403 for `pyspark==3.5.3` (317 MB tarball blocked)
- **Fix:** Removed `--resume-retries`; removed the `-i` mirror flag; PyPI used directly

#### 2. `infra/docker-compose.airflow.yml` — Wrong healthcheck URL
- Healthcheck hit `/health` → 404 on Airflow 3.x (webserver now runs as `api-server`)
- **Fix:** Changed to `/api/v1/health`

#### 3. `pipelines/common/spark_session.py` — Delta Lake JAR not on Spark classpath in Docker
- Root cause chain (3 failed attempts, each revealing a new layer):
  1. `SPARK_DELTA_JARS_DIR` defaulted to `D:/spark-delta-jars` — doesn't exist in the container → `glob.glob()` returns `[]` → no JARs loaded → `ClassNotFoundException`
  2. Switched to `configure_spark_with_delta_pip()` — only sets `spark.jars.packages` (Ivy/Maven), does NOT set `spark.sql.extensions` or `spark.sql.catalog.spark_catalog` → Ivy downloaded the JAR fine but extensions were never registered → `DELTA_CONFIGURE_SPARK_SESSION_WITH_EXTENSION_AND_CATALOG`
  3. Tried `importlib.util.find_spec("delta")` to find bundled JAR → `delta-spark` 3.x ships **no bundled JAR** (`delta/jars/` directory doesn't exist) → returns empty → back to `ClassNotFoundException`
- **Fix:** Set `spark.sql.extensions` + `spark.sql.catalog.spark_catalog` unconditionally in the builder; Docker path sets `spark.jars.packages = io.delta:delta-spark_2.12:3.2.0` (Ivy pulls from `~/.ivy2` cache after first run)

#### 4. Containers needed force-recreate
- Old containers were running without the project volume mount
- **Fix:** `docker compose -f infra/docker-compose.airflow.yml up -d --force-recreate`

### Pipeline Run Results (DAG: `manual__2026-06-14T11:19:30+00:00`)

| Task | Result | Notes |
|---|---|---|
| `generate_streaming` | ✅ success | |
| `bronze_offline` | ✅ success | customers 50k, accounts 60k, cards 75k, merchants 8k, fraud_labels 12,948; transactions 0 new (watermark at 2026-05-27 — idempotent) |
| `bronze_streaming` | ✅ success | 131,891 events ingested |
| `silver_events` | ✅ success | 131,891 in → 129,844 out; 13,922 late arrivals flagged |
| `silver_transactions` | ✅ success | |
| `feat_stream_1h` | ✅ success | |
| `silver_customers` | ✅ success | customers 200k→50k, accounts 240k→60k, cards 300k→75k, merchants 32k→8k, fraud_labels 12,948→12,948 (4:1 dedup from prior failed runs) |
| `gold_dim_date` | ✅ success | |
| `gold_dims` | ✅ success | dim_customer 50k, dim_merchant 8k, dim_card 75k, dim_channel 3 rows; ~2m43s |
| `gold_facts` | ✅ success | fact_transaction 0 new (watermark at 2026-05-28); fact_fraud_label 12,948 merged; ~2m23s |
| `feat_customer_30d` | ✅ success | |
| `feat_unified` | ✅ success | |
| `gold_obt` | ✅ success (retry 2) | 0 new rows — watermark at 2026-05-27, consistent with fact_transaction |
| `gold_monitoring` | ✅ success | agg_feature_health_daily 362 rows; feature_drift_alerts 6 alerts |
| `drift_check` | ✅ success | No new alerts since 2026-05-05 |
| `gold_ml_tables` | ✅ success | ml_fraud_label 474,755 rows (2.73% fraud); ml_fraud_training 474,755 rows with 15 features (customer_id, transaction_id, event_timestamp, created_ts, label, is_pre_drift, f_total_tx_30d, f_avg_amount_30d, f_distinct_merchants_30d, f_declined_rate_30d, f_fraud_rate_90d, f_tx_velocity_1h, f_total_amount_1h, f_foreign_flag, f_unusual_hour_flag) |

### Result
**DAG run `manual__2026-06-14T11:19:30+00:00` — COMPLETE ✅**
All 16 tasks succeeded. First successful end-to-end run of `finguard_pipeline`.

### Files Changed
- `infra/Dockerfile.airflow`
- `infra/docker-compose.airflow.yml`
- `pipelines/common/spark_session.py`
- `pipelines/gold/obt_transaction_performance.py`

---

## Post-Run Evaluation (2026-06-14)

### 1. Stale Watermarks — Most Critical Issue

Several tasks reported 0 new rows not because the pipeline is broken, but because watermarks were already advanced by previous partial runs:

| Task | Watermark stuck at | Impact |
|---|---|---|
| `bronze.ingest_transactions` | 2026-05-27 | All transaction data already ingested; Bronze has rows but incremental check skips them |
| `gold.fact_transaction` | 2026-05-28 | 0 new fact rows written this run — OBT and downstream saw no new transaction data |
| `gold.obt_transaction_performance` | 2026-05-27 | 0 rows processed |

**Implication:** The ML training set (474,755 rows) and all gold transaction tables reflect data from a PREVIOUS run, not this run. The pipeline is idempotent and consistent — just not "fresh" relative to today's data generation.

**What to do next session:** If you want a clean full-data run, reset watermarks in `data/watermarks/` for `bronze.ingest_transactions`, `gold.fact_transaction`, and `gold.obt_transaction_performance` before triggering a new DAG run.

---

### 2. Bronze Deduplication Ratios (4:1)

Silver entity tables showed 4:1 in/out ratios (e.g. customers 200k→50k). This is not a bug — Bronze ingestion (non-incremental tables) wrote the same 50k customers 4 times across 4 previous failed runs, and Silver's MERGE deduplicated them correctly. The idempotent MERGE design is working as intended.

**Watch for:** If dedup ratios keep growing (8:1, 16:1), it means many failed runs are piling up in Bronze. Not harmful but wastes storage. Run `pipelines.gold.maintenance --vacuum` periodically.

---

### 3. Spark Session Overhead Per Task (~2 min each)

Each Airflow task spawns a brand-new JVM + Ivy resolution. With 16 tasks, that's ~32 minutes of pure JVM startup overhead across the run. Key observations:
- Ivy resolution from `~/.ivy2` cache: ~0.5s (fast after first warm-up)
- JVM + SparkContext init: ~90–120s per task
- `ps: command not found` warning on every task: benign — caused by `procps` not being installed in the Airflow image. Not breaking.
- `SparkUI could not bind on port 4040/4041/4042`: benign — parallel tasks competing for the SparkUI port. Spark falls back automatically.

**To reduce overhead:** Consider consolidating related small tasks (e.g. all dim tables in one task, all Silver transforms in one task) to reduce the number of JVM spawns.

---

### 4. Schema Evolution Gap (`_gold_ts`)

`obt_transaction_performance.py` assumed `_gold_ts` existed in `fact_transaction`. The column IS written by `fact_transaction.py` on MERGE, but the existing table was written by an older version of the code before the column was added. Because `fact_transaction` processed 0 new rows this run, no MERGE ran to backfill the column.

**Fix applied:** `obt_transaction_performance.py` now falls back to `created_ts` when `_gold_ts` is absent.

**Lesson:** When adding a new column to a Gold table that other pipelines depend on, either (a) add a migration step to backfill it, or (b) write the downstream pipeline defensively with a column existence check. Option (b) is what we did.

---

### 5. DataHub Lineage — Skipped Every Task

Every task logged `DataHub unavailable, skipping`. DataHub is defined in `infra/docker-compose.datahub.yml` but was never started. Lineage is published best-effort so this doesn't break anything, but lineage tracking is entirely absent.

**To enable:** `docker compose -f infra/docker-compose.datahub.yml up -d` before triggering the DAG. DataHub startup takes ~3–5 min.

---

### 6. `configure_spark_with_delta_pip` Gotcha — Document for Future

`configure_spark_with_delta_pip()` from `delta-spark` 3.x **only** sets `spark.jars.packages`. It does NOT set `spark.sql.extensions` or `spark.sql.catalog.spark_catalog`. These must be set explicitly on the builder. This is counterintuitive given the function name suggests it fully configures Delta. Always set extensions manually.

---

### 7. `gold_obt` Retry Behaviour

The DAG has retries configured for `gold_obt`. The auto-retry picked up the code fix (applied via volume mount between attempt 1 and attempt 2) without any manual intervention. This confirms the volume-mount live-reload pattern works correctly for mid-run fixes.

---
