"""
Silver transformation — transactions.

Reads new Bronze records since last watermark, deduplicates, cleans,
and MERGEs into stg_transactions (upsert on transaction_id).

Usage:
    uv run python -m pipelines.silver.transform_transactions
"""

from delta.tables import DeltaTable
from pyspark.sql import functions as F, Window

from pipelines.common.lineage import publish_lineage
from pipelines.common.quality_checks import (
    check_amount_range, check_no_nulls, check_not_empty, check_unique,
)
from pipelines.common.run_metadata import RunLogger
from pipelines.common.spark_session import get_spark
from pipelines.common.watermark import get_watermark, set_watermark

BRONZE_PATH = "data/lakehouse/bronze/raw_transactions"
SILVER_PATH = "data/lakehouse/silver/stg_transactions"

REQUIRED_COLS = ["transaction_id", "account_id", "card_id", "merchant_id", "amount", "event_timestamp"]


def run() -> None:
    spark = get_spark("finguard.silver.transform_transactions")
    logger = RunLogger("silver.transform_transactions")

    try:
        wm_key = "silver.stg_transactions"
        last_ingest_ts = get_watermark(wm_key)

        df = spark.read.format("delta").load(BRONZE_PATH) \
                  .filter(F.col("_ingest_ts") > last_ingest_ts)

        input_rows = df.count()
        print(f"[SILVER] transactions: {input_rows:,} new Bronze rows since {last_ingest_ts}")

        if input_rows == 0:
            print("[SILVER] stg_transactions: nothing new to process")
            logger.finish("success", 0, 0)
            return

        max_ingest_ts = df.agg(F.max("_ingest_ts")).collect()[0][0]

        # 1. Cast timestamps
        df = df \
            .withColumn("event_timestamp", F.to_timestamp("event_timestamp")) \
            .withColumn("created_ts",      F.to_timestamp("created_ts"))

        # 2. Drop rows missing required fields
        df = df.dropna(subset=REQUIRED_COLS)

        # 3. Deduplicate within this batch: keep latest created_ts per transaction_id
        window = Window.partitionBy("transaction_id").orderBy(F.col("created_ts").desc())
        df = df.withColumn("_rank", F.row_number().over(window)) \
               .filter(F.col("_rank") == 1) \
               .drop("_rank")

        # 4. Amount range flag
        df = df.withColumn(
            "_amount_flag",
            F.when((F.col("amount") < 5_000) | (F.col("amount") > 50_000_000), True).otherwise(False)
        )

        # 5. Silver metadata
        df = df.withColumn("_silver_ts", F.current_timestamp())

        check_not_empty(df, "stg_transactions")
        check_no_nulls(df, "stg_transactions", REQUIRED_COLS)
        check_unique(df, "stg_transactions", "transaction_id")
        check_amount_range(df, "stg_transactions")

        output_rows = df.count()

        # MERGE (upsert) into Silver — insert new, update if re-processed
        if DeltaTable.isDeltaTable(spark, SILVER_PATH):
            DeltaTable.forPath(spark, SILVER_PATH).alias("target") \
                .merge(df.alias("source"), "target.transaction_id = source.transaction_id") \
                .whenMatchedUpdateAll() \
                .whenNotMatchedInsertAll() \
                .execute()
        else:
            df.write.format("delta") \
                .option("mergeSchema", "true") \
                .partitionBy("transaction_date") \
                .mode("overwrite") \
                .save(SILVER_PATH)

        set_watermark(wm_key, str(max_ingest_ts))
        print(f"[SILVER] stg_transactions: merged {output_rows:,} rows (removed {input_rows - output_rows:,} dupes/nulls)")

        publish_lineage("silver_transform_transactions", ["bronze/raw_transactions"], ["silver/stg_transactions"])
        logger.finish("success", input_rows, output_rows)

    except Exception as exc:
        logger.finish("failed", error_msg=str(exc))
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    run()
