"""
Airflow DAG — FinGuard end-to-end data pipeline.

Schedule: every 30 minutes.

DAG structure:
  generate_streaming ── bronze_streaming ─┐
  bronze_offline ────────────────────────┤
                                          ├─ silver_transactions ─┐
                                          ├─ silver_customers ────┤
                                          └─ silver_events ───────┤
                                                                   ├─ gold_dim_date ──┐
                                                                   ├─ gold_dims ──────┤
                                                                   └─ gold_facts ─────┤
                                                                                       ├─ gold_obt
                                                                                       ├─ feat_30d ──┐
                                                                                       │             ├─ feat_unified ─┐
                                                                                       └─ feat_1h ───┘                ├─ gold_monitoring ── drift_check
                                                                                                                      └─ gold_ml_tables
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

default_args = {
    "owner":            "finguard",
    "depends_on_past":  False,
    "start_date":       datetime(2026, 1, 1),
    "retries":          3,
    "retry_delay":      timedelta(minutes=1),
    "retry_exponential_backoff": True,
    "email_on_failure": False,
}

with DAG(
    dag_id="finguard_pipeline",
    default_args=default_args,
    schedule="*/30 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["finguard", "data-engineering"],
) as dag:

    def _run_module(module: str, extra_args: list[str] | None = None):
        import subprocess, sys, os
        project_root = os.environ.get("FINGUARD_PROJECT_ROOT", "/opt/airflow/project")
        result = subprocess.run(
            [sys.executable, "-m", module, *(extra_args or [])],
            capture_output=True, text=True,
            cwd=project_root,
        )
        print(result.stdout)
        if result.returncode != 0:
            raise RuntimeError(f"{module} failed:\n{result.stderr}")

    # ── Streaming producer ────────────────────────────────────────────────
    # Generates 60 s of events in fast mode (~thousands of events) into
    # data/streaming/events.jsonl before bronze ingestion reads them.
    generate_streaming = PythonOperator(
        task_id="generate_streaming",
        python_callable=lambda: _run_module(
            "data_generator.streaming.producer",
            ["--duration", "60", "--fast"],
        ),
    )

    # ── Bronze ────────────────────────────────────────────────────────────
    bronze_offline = PythonOperator(
        task_id="bronze_offline",
        python_callable=lambda: _run_module("pipelines.bronze.ingest_offline"),
    )

    bronze_streaming = PythonOperator(
        task_id="bronze_streaming",
        python_callable=lambda: _run_module("pipelines.bronze.ingest_streaming"),
    )

    # ── Silver ────────────────────────────────────────────────────────────
    silver_transactions = PythonOperator(
        task_id="silver_transactions",
        python_callable=lambda: _run_module("pipelines.silver.transform_transactions"),
    )

    silver_customers = PythonOperator(
        task_id="silver_customers",
        python_callable=lambda: _run_module("pipelines.silver.transform_customers"),
    )

    silver_events = PythonOperator(
        task_id="silver_events",
        python_callable=lambda: _run_module("pipelines.silver.transform_events"),
    )

    # ── Gold ──────────────────────────────────────────────────────────────
    gold_dim_date = PythonOperator(
        task_id="gold_dim_date",
        python_callable=lambda: _run_module("pipelines.gold.dim_date"),
    )

    gold_dims = PythonOperator(
        task_id="gold_dims",
        python_callable=lambda: _run_module("pipelines.gold.dim_tables"),
    )

    gold_facts = PythonOperator(
        task_id="gold_facts",
        python_callable=lambda: _run_module("pipelines.gold.fact_transaction"),
    )

    gold_obt = PythonOperator(
        task_id="gold_obt",
        python_callable=lambda: _run_module("pipelines.gold.obt_transaction_performance"),
    )

    # ── Features ──────────────────────────────────────────────────────────
    feat_30d = PythonOperator(
        task_id="feat_customer_30d",
        python_callable=lambda: _run_module("pipelines.feature.feat_customer_30d"),
    )

    feat_1h = PythonOperator(
        task_id="feat_stream_1h",
        python_callable=lambda: _run_module("pipelines.feature.feat_stream_1h"),
    )

    feat_unified = PythonOperator(
        task_id="feat_unified",
        python_callable=lambda: _run_module("pipelines.feature.feat_unified"),
    )

    # ── Monitoring & drift ────────────────────────────────────────────────
    gold_monitoring = PythonOperator(
        task_id="gold_monitoring",
        python_callable=lambda: _run_module("pipelines.gold.monitoring"),
    )

    drift_check = PythonOperator(
        task_id="drift_check",
        python_callable=lambda: _run_module("pipelines.gold.drift_trigger"),
    )

    # ── ML training tables ────────────────────────────────────────────────
    gold_ml_tables = PythonOperator(
        task_id="gold_ml_tables",
        python_callable=lambda: _run_module("pipelines.gold.ml_tables"),
    )

    # ── Dependencies ──────────────────────────────────────────────────────
    generate_streaming                 >> bronze_streaming
    [bronze_offline, bronze_streaming] >> silver_transactions
    bronze_offline                     >> silver_customers
    bronze_streaming                   >> silver_events

    [silver_transactions, silver_customers] >> gold_dim_date
    [silver_transactions, silver_customers] >> gold_dims
    [gold_dim_date, gold_dims]              >> gold_facts
    gold_facts                              >> gold_obt
    gold_facts                              >> feat_30d
    silver_events                           >> feat_1h
    [feat_30d, feat_1h]                     >> feat_unified
    feat_unified                            >> gold_monitoring
    feat_unified                            >> gold_ml_tables
    gold_monitoring                         >> drift_check
