"""
Bronze ingestion — streaming events.

Reads events.jsonl from data/streaming/, adds ingest metadata,
writes to Delta Lake at data/lakehouse/bronze/raw_events.

Usage:
    uv run python -m pipelines.bronze.ingest_streaming
"""

import argparse
import uuid
from datetime import datetime

from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType, DoubleType, StringType, StructField, StructType,
)

from pipelines.common.lineage import publish_lineage
from pipelines.common.run_metadata import RunLogger
from pipelines.common.spark_session import get_spark
from pipelines.common.watermark import get_watermark, set_watermark

SOURCE_PATH = "data/streaming/events.jsonl"
BRONZE_PATH = "data/lakehouse/bronze/raw_events"

EVENT_SCHEMA = StructType([
    StructField("event_id",        StringType(),  False),
    StructField("event_type",      StringType(),  False),
    StructField("event_timestamp", StringType(),  False),
    StructField("created_ts",      StringType(),  False),
    StructField("account_id",      StringType(),  True),
    StructField("card_id",         StringType(),  True),
    StructField("merchant_id",     StringType(),  True),
    StructField("amount",          DoubleType(),  True),
    StructField("currency",        StringType(),  True),
    StructField("channel",         StringType(),  True),
    StructField("device_type",     StringType(),  True),
    StructField("ip_address",      StringType(),  True),
    StructField("city",            StringType(),  True),
    StructField("is_cross_border", BooleanType(), True),
])


def run() -> None:
    spark = get_spark("finguard.bronze.ingest_streaming")
    logger = RunLogger("bronze.ingest_streaming")

    try:
        wm_key    = "bronze.raw_events"
        last_ts   = get_watermark(wm_key, default="1970-01-01T00:00:00")

        print(f"[BRONZE] Reading streaming events from {SOURCE_PATH} (since {last_ts}) ...")
        df = spark.read.schema(EVENT_SCHEMA).json(SOURCE_PATH)

        # Schema check: quarantine rows that failed schema parsing (null event_id)
        bad = df.filter(F.col("event_id").isNull())
        bad_count = bad.count()
        if bad_count > 0:
            quarantine_path = "data/quarantine/raw_events"
            bad.write.mode("append").json(quarantine_path)
            print(f"[BRONZE] Quarantined {bad_count:,} malformed events -> {quarantine_path}")

        df = df.filter(F.col("event_id").isNotNull())

        # Watermark filter — cast to timestamp before comparing to avoid relying
        # on lexicographic string ordering (fragile if format ever varies).
        df = df.filter(F.to_timestamp(F.col("event_timestamp")) > F.lit(last_ts).cast("timestamp"))

        input_rows = df.count()
        if input_rows == 0:
            print(f"[BRONZE] raw_events: no new events since {last_ts}")
            logger.finish("success", 0, 0)
            return

        max_event_ts = df.agg(F.max("event_timestamp")).collect()[0][0]

        batch_id = str(uuid.uuid4())[:8]
        df = df \
            .withColumn("_ingest_ts", F.lit(datetime.utcnow().isoformat())) \
            .withColumn("_batch_id",  F.lit(batch_id)) \
            .withColumn("_source",    F.lit("streaming/events"))

        df.write.format("delta") \
            .option("mergeSchema", "true") \
            .mode("append") \
            .save(BRONZE_PATH)

        set_watermark(wm_key, str(max_event_ts))
        output_rows = input_rows
        print(f"[BRONZE] raw_events: {output_rows:,} new events -> {BRONZE_PATH}")

        publish_lineage("bronze_ingest_streaming", ["streaming/events"], ["bronze/raw_events"])
        logger.finish("success", input_rows, output_rows)

    except Exception as exc:
        logger.finish("failed", error_msg=str(exc))
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest streaming events to Bronze")
    args = parser.parse_args()
    run()
