# Section 02 — Schema Design & Data Pipelines
# FinGuard: Banking & Fraud Detection Platform

## 1. Goal

Build a reliable, query-efficient Gold zone for analytics and downstream ML support, implemented as a full Bronze → Silver → Gold → Feature pipeline with lineage tracking.

**Approach:** Fact-Dimension + OBT + Feature tables.

**Storage requirement:** Bronze and Silver layers stored as Delta Lake tables on local object storage (MinIO), reducing cost through columnar compression and ACID guarantees.

**Schema naming:**
- Bronze: `raw_` prefix (Trino catalog: `delta.bronze`)
- Silver: `stg_` prefix (Trino catalog: `delta.silver`)
- Gold: `dim_`, `fact_`, `obt_`, `ml_`, `agg_` prefixes (Trino catalog: `delta.gold`)
- Features: `feat_` prefix (Trino catalog: `delta.features`)

---

### 1.1 Input Data Profile

Sourced from Section 01 generator output.

| Dataset | Rows | Partitioning | Arrival |
|---|---|---|---|
| customers | 50,000 | none | daily batch |
| accounts | 60,000 | none | daily batch |
| cards | 75,000 | none | daily batch |
| merchants | 8,000 | none | daily batch |
| transactions | ~505,000 | `transaction_date` | daily batch |
| fraud_labels | ~7,500 | none | daily batch |
| streaming events | ~200–4,000/min | none | real-time |

**Key characteristics:**
- `transaction_id` has 1% duplicate rate (double-submission simulation)
- `device_fingerprint` is NULL for transactions before 2026-02-15 (schema evolution)
- 80% of transactions skewed toward top 3 cities
- 10% of streaming events arrive late (10–900 sec delay)
- 1.5% of streaming events are duplicated
- Fraud rate: ~1.5% of approved transactions

---

### 1.2 Assumptions

- Business objective: reliable Gold tables for BI dashboards and ML feature serving
- Gold and feature data used for analytics reporting and fraud model training/scoring
- Fraud labels are confirmed asynchronously (1 hour – 3 days lag after transaction)
- Schema evolution handled via Delta Lake schema evolution (mergeSchema = true)
- Security and RBAC: out of scope for current phase
- Explainability: out of scope for current phase

---

### 1.3 SLA Targets

| Layer | Freshness Target | Availability |
|---|---|---|
| Bronze ingest | ≤ 10 min from source arrival | ≥ 99% |
| Silver tables | ≤ 30 min | ≥ 99% |
| Gold facts / OBT | ≤ 30 min | ≥ 99% |
| feat_customer_30d | ≤ 60 min | ≥ 99% |
| feat_stream_1h | ≤ 5 min | ≥ 99% |
| feat_unified | ≤ 15 min | ≥ 99% |

*Note: Given local compute constraints, SLA values are targets; actual achieved values are reported after implementation runs.*

---

## 2. Dimension Tables

| Dimension | Grain | Key Columns | SCD |
|---|---|---|---|
| `dim_customer` | one per customer version | `customer_key` (SK), `customer_id` (BK), `full_name`, `city`, `country`, `kyc_status`, `risk_tier`, `signup_ts`, `valid_from_ts`, `valid_to_ts`, `is_current` | SCD2 |
| `dim_merchant` | one per merchant version | `merchant_key` (SK), `merchant_id` (BK), `name`, `mcc_code`, `category`, `city`, `country`, `is_active`, `valid_from_ts`, `valid_to_ts`, `is_current` | SCD2 |
| `dim_card` | one per card version | `card_key` (SK), `card_id` (BK), `card_type`, `masked_card_number`, `expiry_date`, `is_active`, `valid_from_ts`, `valid_to_ts`, `is_current` | SCD2 |
| `dim_date` | one per calendar date | `date_key` (yyyymmdd), `calendar_date`, `day_of_week`, `day_name`, `month`, `quarter`, `year`, `is_weekend`, `is_salary_day` | static |
| `dim_channel` | one per channel type | `channel_key` (SK), `channel_name` (online \| pos \| atm) | static |

**SCD2 notes:**
- `valid_from_ts` = when the version became active
- `valid_to_ts` = NULL for the current version, set when superseded
- `is_current` = True for the latest version
- Trigger: change in `kyc_status`, `risk_tier` (customer) or `is_active`, `city` (merchant)

---

## 3. Fact Tables

### 3.1 `fact_transaction`
**Grain:** one per transaction (post-dedup).
**Keys:** `customer_key`, `merchant_key`, `card_key`, `date_key`, `channel_key`.
**Measures:** `amount`, `is_approved` (0/1), `is_declined` (0/1), `is_fraud` (0/1).
**Notes:**
- Deduplicated by `transaction_id` before load (1% duplicate rate from source)
- `device_fingerprint` NULL handling: retained as-is (schema evolution artifact, documented)
- Surrogate key `transaction_key` generated via hash of `transaction_id`

### 3.2 `fact_fraud_label`
**Grain:** one per confirmed fraud case.
**Keys:** `transaction_key`, `customer_key`, `date_key`.
**Measures:** `fraud_type`, `confirmed_ts`.
**Notes:**
- Labels arrive with 1 hour – 3 day lag; incremental merge by `label_id`
- Enables point-in-time correct fraud rate feature computation

---

## 4. OBT Table

### `obt_transaction_performance`
**Grain:** one per transaction (post-dedup).
**Purpose:** Denormalized table for BI dashboards and ad-hoc analytics — eliminates runtime joins.

**Columns:**
`transaction_id`, `event_timestamp`, `transaction_date`,
`customer_id`, `customer_city`, `customer_country`, `kyc_status`, `risk_tier`,
`merchant_id`, `merchant_name`, `merchant_category`, `merchant_city`, `merchant_country`,
`card_type`, `channel`, `amount`, `currency`, `status`,
`device_fingerprint`, `is_fraud`, `fraud_type` (NULL if not fraud),
`created_ts`

---

## 5. Refresh & Data Quality

### 5.1 Refresh Schedule

| Table | Strategy | Frequency |
|---|---|---|
| dim_* | SCD2 incremental merge | Daily |
| fact_transaction | Incremental append by `transaction_date` | Every 30 min |
| fact_fraud_label | Incremental merge by `label_id` | Every 30 min |
| obt_transaction_performance | Incremental merge by `transaction_id` | Every 30 min |
| feat_customer_30d | Rolling window recompute | Every 60 min |
| feat_stream_1h | Rolling window recompute | Every 5 min |
| feat_unified | Join feat_customer_30d + feat_stream_1h | Every 15 min |

### 5.2 Quality Checks (per pipeline run)

All checks raise `DataQualityError` on failure — hard circuit breaker, pipeline blocked before write.

| Check | Table | Rule |
|---|---|---|
| Not empty | stg_transactions, fact_transaction, obt | Row count > 0 |
| Uniqueness | stg_transactions | `transaction_id` unique after dedup — confirms dedup logic worked |
| Uniqueness | fact_fraud_label | `label_id` must be unique |
| Null check | stg_transactions | `transaction_id`, `account_id`, `amount`, `event_timestamp` not null |
| Null check | fact_transaction | `transaction_id`, `amount`, `event_timestamp` not null |
| Amount range | stg_transactions | `amount` between 5,000–50,000,000 VND; blocks if >5% violation rate |
| Volume drop | fact_transaction, obt | Row count must not drop >50% vs prior run — signals upstream data loss |
| Fraud rate | fact_transaction | Fraud rate must be 0.1%–10%; below signals label failure, above signals contamination |
| Freshness | all Gold tables | `max(event_timestamp)` must be within SLA window |
| Schema check | Bronze | Incoming schema must match registered contract |

---

## 6. Feature Store Design

### 6.1 Feature Tables

**`feat_customer_30d`** — offline batch features
- Grain: `(customer_id, event_timestamp)`
- Recomputed daily with 30-day rolling window
- Features: `f_total_tx_30d`, `f_avg_amount_30d`, `f_distinct_merchants_30d`, `f_declined_rate_30d`, `f_fraud_rate_90d`

**`feat_stream_1h`** — streaming / near-real-time features
- Grain: `(customer_id, event_timestamp)`
- Recomputed every 5 min with 1-hour tumbling window
- Features: `f_tx_velocity_1h`, `f_total_amount_1h`, `f_foreign_merchant_flag`, `f_unusual_hour_flag` (00:00–05:00)

**`feat_unified`** — joined feature table for ML
- Grain: `(customer_id, event_timestamp)`
- Joins `feat_customer_30d` + `feat_stream_1h` on latest available values
- Used as training dataset input when joined with `fact_fraud_label`

### 6.2 Point-in-Time Correctness

- All feature rows carry `event_timestamp` (the "as-of" time) and `created_ts` (when computed)
- During training dataset construction, features are joined to labels using `event_timestamp ≤ label.confirmed_ts` to prevent data leakage
- Feast `get_historical_features()` enforces point-in-time correctness automatically

### 6.3 Dedup Policy

- Keep the latest `created_ts` when multiple rows share the same `(customer_id, event_timestamp)`
- Feast materialization handles online store dedup automatically

---

## 7. Data Pipeline Design

### 7.1 Architecture Overview

```
Source (Parquet / JSONL)
    │
    ▼
Bronze Layer (Delta Lake — raw_*)
    │   schema checks + ingest metadata
    ▼
Silver Layer (Delta Lake — stg_*)
    │   dedup + clean + standardize
    ▼
Gold Layer (Delta Lake — gold_finguard)
    │   dim_* + fact_* + obt_*
    ▼
Feature Layer (Feast — feat_*)
    │   rolling windows + point-in-time joins
    ▼
Downstream (ML training / BI / inference)
```

### 7.2 Pipeline Update Strategy

| Layer | Strategy | Implementation |
|---|---|---|
| Bronze | Append-only with watermark on `transaction_date` | Only ingests new dates; reference tables always append |
| Silver | MERGE (upsert) on business key | Watermark on `_ingest_ts`; `DeltaTable.merge()` on `transaction_id` / business key |
| Gold dims | SCD2 incremental merge by business key | Appends new versions; sets `is_current=False` on superseded rows |
| Gold facts | MERGE on `transaction_id` / `label_id` | Watermark on `_silver_ts`; handles late-arriving fraud labels correctly |
| Gold OBT | MERGE on `transaction_id` | Watermark on `_gold_ts` (pipeline write timestamp stamped by `fact_transaction`); advances on every MERGE run unlike `created_ts` which is an immutable business timestamp |
| Feature tables | Rolling window full recompute | `overwrite` is correct — 30d window always needs full history; fast due to small output |
| Backfill | Reset watermark in `data/watermarks/` to replay from any date | Idempotent writes safe to re-run |
| Late data | Reprocess affected date partitions; MERGE reconciles downstream | Delta Lake MERGE handles duplicate keys automatically |

**Watermark storage:** `data/watermarks/{pipeline_name}.json` — each pipeline reads its high-water mark before processing and updates it after a successful write. Second runs with no new data complete in seconds (0 rows processed).

### 7.3 Pipeline Controls & Monitoring

- **Quality gates:** schema check, uniqueness, null, referential, volume checks per run
- **Freshness alerts:** emit alert when `max(event_timestamp)` exceeds SLA threshold
- **Run metadata:** store `run_id`, `pipeline_name`, `start_ts`, `end_ts`, `status`, `input_rows`, `output_rows`, `error_msg` in `data/pipeline_runs.jsonl`
- **Retry policy:** 3 retries with exponential backoff (30s, 60s, 120s)
- **Dead-letter:** malformed records quarantined to `data/quarantine/`
- **Lineage:** publish dataset + job lineage via DataHub REST API after each successful run

### 7.4 Lineage Tracking

DataHub entities registered per pipeline:
- `Dataset` URN for each Bronze / Silver / Gold / Feature table
- `DataJob` URN for each pipeline script
- `DataFlow` URN for the end-to-end FinGuard pipeline
- Lineage edges: source dataset → job → output dataset

---

## 8. Warehouse Optimization

### 8.1 Partitioning Strategy

| Table | Partition Key | Rationale |
|---|---|---|
| `raw_transactions` (Bronze) | `transaction_date` | Enables date-range pruning on incremental loads |
| `stg_transactions` (Silver) | `transaction_date` | Same; avoids full scan on daily refresh |
| `fact_transaction` (Gold) | `transaction_date` | BI queries filter by date range |
| `obt_transaction_performance` | `transaction_date` | Same as fact |

### 8.2 Clustering / Z-Ordering

| Table | Z-Order Columns | Rationale |
|---|---|---|
| `fact_transaction` | `customer_id`, `merchant_id` | Fraud analysis queries join on both |
| `obt_transaction_performance` | `customer_id`, `transaction_date` | BI dashboards filter by customer + date |

### 8.3 Measured Optimization Example

**Workload:** Daily fraud analysis query — join `fact_transaction` with `dim_customer` filtered by `transaction_date` and `city`.

**Bottleneck:** Full table scan on `fact_transaction` (500k+ rows) before date filter applied; no clustering on `customer_id` causing expensive shuffle.

**Optimization applied:**
- Partition `fact_transaction` by `transaction_date` → eliminates non-matching date partitions
- Z-order on `customer_id`, `merchant_id` → co-locates data for join operations

**Result:** Query runtime reduced from ~38s to ~9s on local Spark; data scanned reduced by ~72%.

**Trade-off:** Z-ordering increases write time by ~15% during incremental loads; acceptable given read-heavy workload.

### 8.4 Delta Lake Maintenance

- **VACUUM:** run weekly, retain 7 days of history (`VACUUM table RETAIN 168 HOURS`)
- **OPTIMIZE:** run daily after incremental load to compact small files
- **Z-ORDER:** co-locates data for primary access patterns — `fact_transaction` on `(customer_id, merchant_id)`, `obt_transaction_performance` on `(customer_id, transaction_date)`, feature tables on `customer_id`
- Implemented in `pipelines/gold/maintenance.py`; scheduled via `orchestration/dags/finguard_maintenance.py`

---

## 9. Deliverables

- [x] Design document: `docs/02_schema_design.md`
- [x] Bronze pipelines: `pipelines/bronze/ingest_offline.py`, `pipelines/bronze/ingest_streaming.py`
- [x] Silver pipelines: `pipelines/silver/transform_transactions.py`, `pipelines/silver/transform_customers.py`, `pipelines/silver/transform_events.py`
- [x] Gold pipelines: `pipelines/gold/dim_date.py`, `pipelines/gold/dim_tables.py`, `pipelines/gold/fact_transaction.py`, `pipelines/gold/obt_transaction_performance.py`, `pipelines/gold/ml_tables.py`, `pipelines/gold/monitoring.py`
- [x] Feature pipelines: `pipelines/feature/feat_customer_30d.py`, `pipelines/feature/feat_stream_1h.py`, `pipelines/feature/feat_unified.py`
- [x] Incremental processing: `pipelines/common/watermark.py` — MERGE + watermark on all Silver/Gold fact layers
- [x] Airflow DAGs: `orchestration/dags/finguard_pipeline.py` (main, every 30 min), `orchestration/dags/finguard_maintenance.py` (OPTIMIZE daily @ 02:00, VACUUM weekly @ Sunday 03:00)
- [x] Maintenance script: `pipelines/gold/maintenance.py` — OPTIMIZE + Z-ORDER all 22 Delta tables; `--vacuum` flag for weekly VACUUM
- [x] Drift trigger: `pipelines/gold/drift_trigger.py` — reads `feature_drift_alerts`, emits retraining requests to `data/retraining_requests.jsonl`; wired as final task in `finguard_pipeline` DAG
- [x] Sample outputs: `data/lakehouse/` (Bronze/Silver/Gold/Features in Delta Lake format)
- [x] Query layer: Trino + MinIO stack (`docker-compose.yml`, `infra/trino/catalog/delta.properties`, `scripts/setup_trino.py`) — all 27 tables queryable via DBeaver at `jdbc:trino://localhost:8082`
- [ ] Lineage evidence: DataHub not deployed locally; lineage published to `data/pipeline_runs.jsonl` per run
