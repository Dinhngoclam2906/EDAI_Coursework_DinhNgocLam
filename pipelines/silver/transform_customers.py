"""
Silver transformation — customers, accounts, cards, merchants, fraud_labels.

Reads from Bronze, cleans and standardizes, MERGEs into Silver on business key
(upsert — handles re-ingestion and updates idempotently).

Usage:
    uv run python -m pipelines.silver.transform_customers
"""

from delta.tables import DeltaTable
from pyspark.sql import functions as F, Window

from pipelines.common.lineage import publish_lineage
from pipelines.common.quality_checks import check_no_nulls, check_not_empty, check_unique
from pipelines.common.run_metadata import RunLogger
from pipelines.common.spark_session import get_spark
from pipelines.common.watermark import get_watermark, set_watermark

BRONZE_BASE = "data/lakehouse/bronze"
SILVER_BASE = "data/lakehouse/silver"


def _transform_reference(spark, table: str, key_col: str, ts_cols: list[str]) -> tuple[int, int]:
    bronze_path = f"{BRONZE_BASE}/raw_{table}"
    silver_path = f"{SILVER_BASE}/stg_{table}"

    df = spark.read.format("delta").load(bronze_path)
    input_rows = df.count()

    for col in ts_cols:
        if col in df.columns:
            df = df.withColumn(col, F.to_timestamp(col))

    # Keep latest version per business key
    window = Window.partitionBy(key_col).orderBy(F.col("created_ts").desc())
    df = df.withColumn("_rank", F.row_number().over(window)) \
           .filter(F.col("_rank") == 1) \
           .drop("_rank") \
           .withColumn("_silver_ts", F.current_timestamp())

    check_not_empty(df, f"stg_{table}")
    check_no_nulls(df, f"stg_{table}", [key_col])
    check_unique(df, f"stg_{table}", key_col)

    output_rows = df.count()

    if DeltaTable.isDeltaTable(spark, silver_path):
        DeltaTable.forPath(spark, silver_path).alias("target") \
            .merge(df.alias("source"), f"target.{key_col} = source.{key_col}") \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
    else:
        df.write.format("delta") \
            .option("mergeSchema", "true") \
            .mode("overwrite") \
            .save(silver_path)

    print(f"[SILVER] stg_{table}: merged {output_rows:,} rows")
    return input_rows, output_rows


def _transform_fraud_labels(spark) -> tuple[int, int]:
    wm_key = "silver.stg_fraud_labels"
    last_ingest_ts = get_watermark(wm_key)

    df = spark.read.format("delta").load(f"{BRONZE_BASE}/raw_fraud_labels") \
              .filter(F.col("_ingest_ts") > last_ingest_ts)

    input_rows = df.count()
    print(f"[SILVER] stg_fraud_labels: {input_rows:,} new Bronze rows since {last_ingest_ts}")

    if input_rows == 0:
        print("[SILVER] stg_fraud_labels: nothing new to process")
        return 0, 0

    max_ingest_ts = df.agg(F.max("_ingest_ts")).collect()[0][0]

    for col in ["confirmed_ts", "created_ts"]:
        df = df.withColumn(col, F.to_timestamp(col))

    window = Window.partitionBy("label_id").orderBy(F.col("created_ts").desc())
    df = df.withColumn("_rank", F.row_number().over(window)) \
           .filter(F.col("_rank") == 1) \
           .drop("_rank") \
           .withColumn("_silver_ts", F.current_timestamp())

    check_not_empty(df, "stg_fraud_labels")
    check_unique(df, "stg_fraud_labels", "label_id")

    output_rows = df.count()
    silver_path = f"{SILVER_BASE}/stg_fraud_labels"

    if DeltaTable.isDeltaTable(spark, silver_path):
        DeltaTable.forPath(spark, silver_path).alias("target") \
            .merge(df.alias("source"), "target.label_id = source.label_id") \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
    else:
        df.write.format("delta") \
            .option("mergeSchema", "true") \
            .mode("overwrite") \
            .save(silver_path)

    set_watermark(wm_key, str(max_ingest_ts))
    print(f"[SILVER] stg_fraud_labels: merged {output_rows:,} rows")
    return input_rows, output_rows


def run() -> None:
    spark = get_spark("finguard.silver.transform_customers")

    configs = [
        ("customers",   "customer_id",  ["signup_ts", "created_ts"]),
        ("accounts",    "account_id",   ["opened_ts", "created_ts"]),
        ("cards",       "card_id",      ["issued_ts", "created_ts"]),
        ("merchants",   "merchant_id",  ["created_ts"]),
    ]

    for table, key_col, ts_cols in configs:
        logger = RunLogger(f"silver.transform_{table}")
        try:
            in_r, out_r = _transform_reference(spark, table, key_col, ts_cols)
            publish_lineage(f"silver_transform_{table}", [f"bronze/raw_{table}"], [f"silver/stg_{table}"])
            logger.finish("success", in_r, out_r)
        except Exception as exc:
            logger.finish("failed", error_msg=str(exc))
            raise

    logger = RunLogger("silver.transform_fraud_labels")
    try:
        in_r, out_r = _transform_fraud_labels(spark)
        publish_lineage("silver_transform_fraud_labels", ["bronze/raw_fraud_labels"], ["silver/stg_fraud_labels"])
        logger.finish("success", in_r, out_r)
    except Exception as exc:
        logger.finish("failed", error_msg=str(exc))
        raise

    spark.stop()


if __name__ == "__main__":
    run()
