"""
Silver transformation — streaming events.

Reads new Bronze records since last watermark, applies:
  - deduplication by event_id within the batch (keep latest created_ts)
  - late arrival flagging
  - timestamp casting

MERGEs into stg_events (upsert on event_id) — incremental, not full overwrite.

Usage:
    uv run python -m pipelines.silver.transform_events
"""

from delta.tables import DeltaTable
from pyspark.sql import functions as F, Window

from pipelines.common.lineage import publish_lineage
from pipelines.common.quality_checks import check_no_nulls, check_not_empty, check_unique
from pipelines.common.run_metadata import RunLogger
from pipelines.common.spark_session import get_spark
from pipelines.common.watermark import get_watermark, set_watermark

BRONZE_PATH = "data/lakehouse/bronze/raw_events"
SILVER_PATH = "data/lakehouse/silver/stg_events"


def run() -> None:
    spark = get_spark("finguard.silver.transform_events")
    logger = RunLogger("silver.transform_events")

    try:
        wm_key        = "silver.stg_events"
        last_ingest_ts = get_watermark(wm_key)

        df = spark.read.format("delta").load(BRONZE_PATH) \
                  .filter(F.col("_ingest_ts") > last_ingest_ts)

        input_rows = df.count()
        print(f"[SILVER] stg_events: {input_rows:,} new Bronze rows since {last_ingest_ts}")

        if input_rows == 0:
            print("[SILVER] stg_events: nothing new to process")
            logger.finish("success", 0, 0)
            return

        max_ingest_ts = df.agg(F.max("_ingest_ts")).collect()[0][0]

        # Cast timestamps
        df = df \
            .withColumn("event_timestamp", F.to_timestamp("event_timestamp")) \
            .withColumn("created_ts",      F.to_timestamp("created_ts"))

        # Flag late arrivals: event arrived > 60s after its event_timestamp.
        # The producer generates genuine late arrivals with 600–900s delays;
        # 60s separates real late arrivals from normal pipeline I/O latency.
        df = df.withColumn(
            "is_late_arrival",
            F.col("created_ts").cast("long") - F.col("event_timestamp").cast("long") > 60
        )

        # Deduplicate within this batch — keep latest created_ts per event_id
        window = Window.partitionBy("event_id").orderBy(F.col("created_ts").desc())
        df = df.withColumn("_rank", F.row_number().over(window)) \
               .filter(F.col("_rank") == 1) \
               .drop("_rank")

        df = df.withColumn("_silver_ts", F.current_timestamp())

        check_not_empty(df, "stg_events")
        check_no_nulls(df, "stg_events", ["event_id", "event_type", "event_timestamp"])
        check_unique(df, "stg_events", "event_id")

        output_rows = df.count()

        if DeltaTable.isDeltaTable(spark, SILVER_PATH):
            DeltaTable.forPath(spark, SILVER_PATH).alias("target") \
                .merge(df.alias("source"), "target.event_id = source.event_id") \
                .whenMatchedUpdateAll() \
                .whenNotMatchedInsertAll() \
                .execute()
        else:
            df.write.format("delta") \
                .option("mergeSchema", "true") \
                .mode("overwrite") \
                .save(SILVER_PATH)

        set_watermark(wm_key, str(max_ingest_ts))
        late_count = df.filter(F.col("is_late_arrival")).count()
        print(f"[SILVER] stg_events: merged {output_rows:,} rows  ({late_count:,} late arrivals flagged)")

        publish_lineage("silver_transform_events", ["bronze/raw_events"], ["silver/stg_events"])
        logger.finish("success", input_rows, output_rows)

    except Exception as exc:
        logger.finish("failed", error_msg=str(exc))
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    run()
