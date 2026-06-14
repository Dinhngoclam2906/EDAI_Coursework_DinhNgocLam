# FinGuard — Banking & Fraud Detection Platform

> **Module:** Engineering for Data and AI (EDAI)
> **Author:** Dinh Ngoc Lam

An end-to-end data engineering platform simulating a retail banking system with real-time fraud detection capabilities. Built on a modern open-source lakehouse stack — from raw event ingestion all the way through to ML-ready feature tables.

---

## What does this project do?

FinGuard generates realistic banking data (customers, transactions, fraud events), streams it through a data pipeline, and stores it in a structured **medallion lakehouse** (Bronze → Silver → Gold). It deliberately injects real-world data quality challenges like late-arriving events, schema changes, and fraud pattern drift — the kinds of problems data engineers actually deal with.

Think of it as a mini production data platform you can run on your laptop.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                        Data Sources                             │
│  PostgreSQL (banking DB)          Streaming events (Kafka)      │
│  customers, accounts, cards,      transaction_initiated,        │
│  merchants, transactions,         login_attempt, card_blocked…  │
│  fraud_labels                                                   │
└────────────────┬────────────────────────┬───────────────────────┘
                 │ CDC (Debezium)         │ Kafka topics
                 ▼                        ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Bronze Layer (Delta Lake)                    │
│         Raw ingestion with watermark-based deduplication        │
└────────────────────────────┬────────────────────────────────────┘
                             │ Apache Spark
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Silver Layer (Delta Lake)                    │
│    Cleaned, typed, deduplicated — one table per domain          │
└────────────────────────────┬────────────────────────────────────┘
                             │ Apache Spark
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                     Gold Layer (Delta Lake)                     │
│  Dimensional model (SCD2 dims + facts) · OBT · ML feature tables│
└────────────────────────────┬────────────────────────────────────┘
                             │
              ┌──────────────┼──────────────┐
              ▼              ▼              ▼
           Trino          Feast        ClickHouse
        (SQL queries)  (feature store)  (analytics)
```

**Orchestration:** Apache Airflow (runs every 30 min)
**Storage:** MinIO (S3-compatible) + Hive Metastore
**Lineage:** DataHub

---

## Tech Stack

| Layer | Technology |
|---|---|
| Source / CDC | PostgreSQL, Debezium |
| Message Broker | Apache Kafka |
| Storage | Delta Lake, MinIO, Hive Metastore |
| Batch Processing | Apache Spark |
| Stream Processing | Apache Flink |
| Consumption | Feast (feature store), Trino, ClickHouse |
| Orchestration | Apache Airflow |
| Lineage | DataHub |

---

## Project Structure

```
├── data_generator/          # Synthetic banking data generator
│   ├── offline/             # Parquet table generators (customers, transactions…)
│   └── streaming/           # Real-time event producer (Kafka or JSONL)
│
├── pipelines/               # Medallion pipeline (Bronze → Silver → Gold)
│   ├── bronze/              # Raw ingestion from Parquet + JSONL
│   ├── silver/              # Cleaning, typing, deduplication
│   ├── gold/                # Dimensional model, OBT, ML tables, monitoring
│   ├── feature/             # Feast feature engineering (offline + streaming)
│   └── common/              # SparkSession, watermarks, quality checks, lineage
│
├── orchestration/dags/      # Airflow DAGs
├── infra/                   # Docker Compose stacks (main + Airflow)
├── docs/                    # Design documents (per section)
└── tests/                   # Unit tests (pytest)
```

---

## Injected Data Challenges

These are built into the data generator to simulate real-world messiness:

| Challenge | Details |
|---|---|
| **Class imbalance** | ~1.5% of transactions are fraudulent |
| **Geographic skew** | 80% of traffic from just 3 cities |
| **Burst traffic** | Salary days (1st & 15th) = 20× normal volume |
| **Late arrivals** | Cross-border transactions arrive 10–15 min late |
| **Schema evolution** | `device_fingerprint` column added mid-dataset (day 60) |
| **Fraud drift** | Small-amount probing attacks spike after day 90 |

---

## Getting Started

### Prerequisites

- [uv](https://docs.astral.sh/uv/) (Python package manager)
- [Docker Desktop](https://www.docker.com/products/docker-desktop/)
- Java 17+ (required for PySpark)

### 1. Install dependencies

```bash
uv sync
```

### 2. Start the infrastructure

```bash
# Main stack: Kafka, PostgreSQL, MinIO, Hive Metastore, Trino, Flink, Debezium
docker compose up -d

# Airflow stack
docker compose -f infra/docker-compose.airflow.yml build
docker compose -f infra/docker-compose.airflow.yml up -d
```

### 3. Generate data

```bash
# Generate offline tables (customers, accounts, transactions, fraud labels…)
uv run python -m data_generator.offline.run

# Generate streaming events (writes to data/streaming/events.jsonl)
uv run python -m data_generator.streaming.producer --duration 60 --fast
```

### 4. Run the pipeline manually

```bash
# Bronze
uv run python -m pipelines.bronze.ingest_offline
uv run python -m pipelines.bronze.ingest_streaming

# Silver
uv run python -m pipelines.silver.transform_transactions
uv run python -m pipelines.silver.transform_customers
uv run python -m pipelines.silver.transform_events

# Gold
uv run python -m pipelines.gold.dim_date
uv run python -m pipelines.gold.dim_tables
uv run python -m pipelines.gold.fact_transaction
uv run python -m pipelines.gold.obt_transaction_performance
uv run python -m pipelines.gold.ml_tables
uv run python -m pipelines.gold.monitoring
uv run python -m pipelines.gold.drift_trigger

# Features
uv run python -m pipelines.feature.feat_customer_30d
uv run python -m pipelines.feature.feat_stream_1h
uv run python -m pipelines.feature.feat_unified
```

### 5. Or let Airflow run everything

Open `http://localhost:8084` (username: `airflow`, password: `airflow`), unpause `finguard_pipeline`, and trigger a run. The DAG runs every 30 minutes automatically.

### 6. Run tests

```bash
uv run pytest tests/
```

---

## Data Model (Gold Layer)

```
dim_customer ──┐
dim_merchant ──┼──► fact_transaction ──► obt_transaction_performance
dim_card ──────┤
dim_date ──────┘
               └──► fact_fraud_label

               ──► ml_fraud_training   (ML-ready feature table)
               ──► agg_feature_health_daily  (monitoring)
```

All Gold tables are Delta Lake format with OPTIMIZE + VACUUM on a daily schedule.

---

## Service URLs (when running locally)

| Service | URL | Credentials |
|---|---|---|
| Airflow UI | http://localhost:8084 | airflow / airflow |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| Trino UI | http://localhost:8082 | — |
| Kafka UI (Redpanda) | http://localhost:8085 | — |
| Flink UI | http://localhost:8881 | — |
| Debezium REST | http://localhost:8083 | — |

---

## Sections

| Section | Description | Status |
|---|---|---|
| 01 | Data Generator Design | Complete |
| 02 | Schema Design & Data Pipelines | Complete |
| 03 | Data Generator Improvement & Drift | Complete |
| 04.1 | ML System — Fraud Detection | Deferred |
| 04.2 | LLM System — RAG Financial Assistant | Deferred |

Design documents for each section are in [`docs/`](docs/).
