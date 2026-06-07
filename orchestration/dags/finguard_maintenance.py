"""
Airflow DAG — FinGuard Delta Lake maintenance.

Two independent schedules in one file:

  finguard_optimize  — daily at 02:00 UTC (after nightly pipeline runs)
      optimize_all    OPTIMIZE + ZORDER all Delta tables

  finguard_vacuum    — weekly, every Sunday at 03:00 UTC
      vacuum_all      OPTIMIZE + VACUUM (--vacuum flag) all Delta tables
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

_default_args = {
    "owner":            "finguard",
    "depends_on_past":  False,
    "start_date":       datetime(2026, 1, 1),
    "retries":          1,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}


def _run_optimize():
    import subprocess, sys
    result = subprocess.run(
        [sys.executable, "-m", "pipelines.gold.maintenance"],
        capture_output=True, text=True,
    )
    print(result.stdout)
    if result.returncode != 0:
        raise RuntimeError(f"maintenance (optimize) failed:\n{result.stderr}")


def _run_vacuum():
    import subprocess, sys
    result = subprocess.run(
        [sys.executable, "-m", "pipelines.gold.maintenance", "--vacuum"],
        capture_output=True, text=True,
    )
    print(result.stdout)
    if result.returncode != 0:
        raise RuntimeError(f"maintenance (vacuum) failed:\n{result.stderr}")


# ── Daily OPTIMIZE ─────────────────────────────────────────────────────────────
with DAG(
    dag_id="finguard_optimize",
    default_args=_default_args,
    schedule="0 2 * * *",   # 02:00 UTC daily
    catchup=False,
    max_active_runs=1,
    tags=["finguard", "maintenance"],
    description="Compact Delta Lake files and apply Z-ORDER daily.",
) as dag_optimize:

    PythonOperator(
        task_id="optimize_all",
        python_callable=_run_optimize,
    )


# ── Weekly VACUUM ──────────────────────────────────────────────────────────────
with DAG(
    dag_id="finguard_vacuum",
    default_args=_default_args,
    schedule="0 3 * * 0",   # 03:00 UTC every Sunday
    catchup=False,
    max_active_runs=1,
    tags=["finguard", "maintenance"],
    description="Optimize + remove old Delta Lake snapshots (7-day retention).",
) as dag_vacuum:

    PythonOperator(
        task_id="vacuum_all",
        python_callable=_run_vacuum,
    )
