"""
One-shot script: upload local Delta tables to MinIO, then register them in Trino.

Run after `docker compose up -d minio metastore-db hive-metastore trino`:
    uv run python scripts/setup_trino.py
"""

import subprocess
import time
from pathlib import Path

import requests
from minio import Minio
from minio.error import S3Error

MINIO_ENDPOINT  = "localhost:9002"
MINIO_ACCESS    = "minioadmin"
MINIO_SECRET    = "minioadmin"
BUCKET          = "lakehouse"
LOCAL_LAKEHOUSE = Path("data/lakehouse")
TRINO_URL       = "http://localhost:8082"

# All tables to register: (schema, table_name, local_subpath)
TABLES = [
    # Bronze
    ("bronze", "raw_customers",     "bronze/raw_customers"),
    ("bronze", "raw_accounts",      "bronze/raw_accounts"),
    ("bronze", "raw_cards",         "bronze/raw_cards"),
    ("bronze", "raw_merchants",     "bronze/raw_merchants"),
    ("bronze", "raw_transactions",  "bronze/raw_transactions"),
    ("bronze", "raw_fraud_labels",  "bronze/raw_fraud_labels"),
    ("bronze", "raw_events",        "bronze/raw_events"),
    # Silver
    ("silver", "stg_transactions",  "silver/stg_transactions"),
    ("silver", "stg_customers",     "silver/stg_customers"),
    ("silver", "stg_accounts",      "silver/stg_accounts"),
    ("silver", "stg_cards",         "silver/stg_cards"),
    ("silver", "stg_merchants",     "silver/stg_merchants"),
    ("silver", "stg_fraud_labels",  "silver/stg_fraud_labels"),
    ("silver", "stg_events",        "silver/stg_events"),
    # Gold
    ("gold", "dim_date",                    "gold/dim_date"),
    ("gold", "dim_customer",                "gold/dim_customer"),
    ("gold", "dim_merchant",                "gold/dim_merchant"),
    ("gold", "dim_card",                    "gold/dim_card"),
    ("gold", "dim_channel",                 "gold/dim_channel"),
    ("gold", "fact_transaction",            "gold/fact_transaction"),
    ("gold", "fact_fraud_label",            "gold/fact_fraud_label"),
    ("gold", "obt_transaction_performance", "gold/obt_transaction_performance"),
    ("gold", "ml_fraud_label",              "gold/ml_fraud_label"),
    ("gold", "ml_fraud_training",           "gold/ml_fraud_training"),
    ("gold", "agg_feature_health_daily",    "gold/agg_feature_health_daily"),
    ("gold", "feature_drift_alerts",        "gold/feature_drift_alerts"),
    # Features
    ("features", "feat_customer_30d", "features/feat_customer_30d"),
    ("features", "feat_unified",      "features/feat_unified"),
    ("features", "feat_stream_1h",    "features/feat_stream_1h"),
]


def wait_for_minio():
    print("Waiting for MinIO...", end="", flush=True)
    for _ in range(60):
        try:
            r = requests.get(f"http://{MINIO_ENDPOINT}/minio/health/live", timeout=2)
            if r.status_code == 200:
                print(" ready.")
                return
        except Exception:
            pass
        print(".", end="", flush=True)
        time.sleep(3)
    raise RuntimeError("MinIO did not become healthy in time.")


def wait_for_trino():
    print("Waiting for Trino...", end="", flush=True)
    for _ in range(60):
        try:
            r = requests.get(f"{TRINO_URL}/v1/info", timeout=2)
            if r.status_code == 200 and r.json().get("starting") is False:
                print(" ready.")
                return
        except Exception:
            pass
        print(".", end="", flush=True)
        time.sleep(5)
    raise RuntimeError("Trino did not become healthy in time.")


def upload_to_minio():
    client = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS, secret_key=MINIO_SECRET, secure=False)

    if not client.bucket_exists(BUCKET):
        client.make_bucket(BUCKET)
        print(f"Created bucket: {BUCKET}")

    total = 0
    for schema, table, subpath in TABLES:
        local_dir = LOCAL_LAKEHOUSE / subpath
        if not local_dir.exists():
            print(f"  [SKIP] {subpath} not found locally")
            continue

        files = list(local_dir.rglob("*"))
        data_files = [f for f in files if f.is_file()]
        for f in data_files:
            obj_name = f"{subpath}/{f.relative_to(local_dir).as_posix()}"
            client.fput_object(BUCKET, obj_name, str(f))
            total += 1

        print(f"  [OK] {schema}.{table} - {len(data_files)} files -> s3://{BUCKET}/{subpath}/")

    print(f"\nUploaded {total} files total.")


def run_trino_query(sql: str, user: str = "admin", ignore_errors: list = None) -> bool:
    """Execute a Trino statement via HTTP API."""
    r = requests.post(
        f"{TRINO_URL}/v1/statement",
        data=sql,
        headers={"X-Trino-User": user, "X-Trino-Catalog": "delta", "X-Trino-Schema": "default"},
    )
    if r.status_code != 200:
        print(f"  [ERR] HTTP {r.status_code}: {r.text[:200]}")
        return False

    # Poll until done
    result = r.json()
    while "nextUri" in result:
        time.sleep(0.5)
        result = requests.get(result["nextUri"], headers={"X-Trino-User": user}).json()

    if result.get("error"):
        msg = result["error"].get("message", "?")
        if ignore_errors and any(e in msg for e in ignore_errors):
            return True
        print(f"  [ERR] {msg}")
        return False
    return True


def register_tables():
    # Create schemas (location required by Delta Lake connector)
    schemas = {s for s, _, _ in TABLES}
    for schema in schemas:
        location = f"s3a://{BUCKET}/{schema}"
        sql = f"CREATE SCHEMA IF NOT EXISTS delta.{schema} WITH (location = '{location}')"
        ok = run_trino_query(sql)
        status = "[OK]" if ok else "[ERR]"
        print(f"  Schema: delta.{schema}  {status}")

    # Register each table
    for schema, table, subpath in TABLES:
        local_dir = LOCAL_LAKEHOUSE / subpath
        if not local_dir.exists():
            continue
        location = f"s3a://{BUCKET}/{subpath}"
        sql = (
            f"CALL delta.system.register_table("
            f"schema_name => '{schema}', "
            f"table_name => '{table}', "
            f"table_location => '{location}')"
        )
        ok = run_trino_query(sql, ignore_errors=["already exists"])
        status = "[OK]" if ok else "[ERR]"
        print(f"  {status} delta.{schema}.{table}")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--skip-upload", action="store_true", help="Skip MinIO upload (files already there)")
    args = parser.parse_args()

    print("=== FinGuard: Trino Setup ===\n")

    if not args.skip_upload:
        print("Step 1/3: Upload Delta tables to MinIO")
        wait_for_minio()
        upload_to_minio()
    else:
        print("Step 1/3: Skipping upload (--skip-upload)")

    print("\nStep 2/3: Wait for Trino")
    wait_for_trino()

    print("\nStep 3/3: Register tables in Trino delta catalog")
    register_tables()

    print("\nDone! Connect DBeaver to Trino at jdbc:trino://localhost:8082")
    print("Catalog: delta   Schemas: bronze / silver / gold / features")
