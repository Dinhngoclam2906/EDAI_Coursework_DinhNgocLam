"""
Bronze ingestion — offline tables.

Reads Parquet files from data/offline/, adds ingest metadata,
writes to Delta Lake at data/lakehouse/bronze/.

Usage:
    uv run python -m pipelines.bronze.ingest_offline
    uv run python -m pipelines.bronze.ingest_offline --table transactions
"""

import argparse
import uuid
from datetime import datetime
from pathlib import Path

from pyspark.sql import functions as F

from pipelines.common.lineage import publish_lineage
from pipelines.common.run_metadata import RunLogger
from pipelines.common.spark_session import get_spark
from pipelines.common.watermark import get_watermark, set_watermark

TABLES = ["customers", "accounts", "cards", "merchants", "transactions", "fraud_labels"]

SOURCE_BASE  = "data/offline"
BRONZE_BASE  = "data/lakehouse/bronze"


def ingest_table(spark, table: str) -> tuple[int, int]:
    source_path = f"{SOURCE_BASE}/{table}"
    bronze_path = f"{BRONZE_BASE}/raw_{table}"

    print(f"[BRONZE] Ingesting {table} from {source_path} ...")

    df = spark.read.parquet(source_path)

    batch_id = str(uuid.uuid4())[:8]
    ingest_ts = datetime.utcnow().isoformat()

    # Watermark filter: only ingest new transaction dates
    if table == "transactions":
        wm_key = "bronze.transactions"
        last_date = get_watermark(wm_key, default="1970-01-01")
        df = df.filter(F.col("transaction_date") > last_date)
        print(f"[BRONZE] transactions: incremental from {last_date}")

    input_rows = df.count()
    if input_rows == 0:
        print(f"[BRONZE] {table}: no new rows since last run")
        return 0, 0

    df = df.withColumn("_ingest_ts", F.lit(ingest_ts)) \
           .withColumn("_batch_id",  F.lit(batch_id)) \
           .withColumn("_source",    F.lit(f"offline/{table}"))

    required_cols = {"created_ts"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"[BRONZE] {table}: missing required columns {missing}")

    write_opts = {"mergeSchema": "true"}

    if table == "transactions":
        df.write.format("delta") \
            .options(**write_opts) \
            .partitionBy("transaction_date") \
            .mode("append") \
            .save(bronze_path)
        max_date = df.agg(F.max("transaction_date")).collect()[0][0]
        set_watermark("bronze.transactions", str(max_date))
    else:
        df.write.format("delta") \
            .options(**write_opts) \
            .mode("append") \
            .save(bronze_path)

    output_rows = input_rows
    print(f"[BRONZE] {table}: {input_rows:,} rows -> {bronze_path}")
    return input_rows, output_rows


def run(table: str | None = None) -> None:
    spark = get_spark("finguard.bronze.ingest_offline")
    tables = [table] if table else TABLES

    for t in tables:
        logger = RunLogger(f"bronze.ingest_{t}")
        try:
            in_rows, out_rows = ingest_table(spark, t)
            publish_lineage(f"bronze_ingest_{t}", [f"offline/{t}"], [f"bronze/raw_{t}"])
            logger.finish("success", in_rows, out_rows)
        except Exception as exc:
            logger.finish("failed", error_msg=str(exc))
            raise

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest offline tables to Bronze")
    parser.add_argument("--table", default=None, help="Specific table to ingest (default: all)")
    args = parser.parse_args()
    run(args.table)
