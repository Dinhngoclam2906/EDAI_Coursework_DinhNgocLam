"""
Feature pipeline — feat_stream_1h.

Computes near-real-time features from streaming events (stg_events)
using a 1-hour tumbling window.

Features:
  f_tx_velocity_1h     — transaction count in last 1 hour
  f_total_amount_1h    — total transaction amount in last 1 hour
  f_foreign_flag       — 1 if any cross-border transaction in last 1 hour
  f_unusual_hour_flag  — 1 if any transaction between 00:00–05:00

Usage:
    uv run python -m pipelines.feature.feat_stream_1h
"""

from datetime import datetime, timedelta

from pyspark.sql import functions as F

from pipelines.common.lineage import publish_lineage
from pipelines.common.run_metadata import RunLogger
from pipelines.common.spark_session import get_spark

SILVER_BASE  = "data/lakehouse/silver"
GOLD_BASE    = "data/lakehouse/gold"
FEATURE_PATH = "data/lakehouse/features/feat_stream_1h"


def run() -> None:
    as_of     = datetime.utcnow()
    window_1h = as_of - timedelta(hours=1)

    spark = get_spark("finguard.feature.feat_stream_1h")
    logger = RunLogger("feature.feat_stream_1h")

    try:
        events = spark.read.format("delta").load(f"{SILVER_BASE}/stg_events") \
            .filter(F.col("event_timestamp") >= F.lit(window_1h.isoformat())) \
            .filter(F.col("event_type").isin("transaction_initiated", "transaction_approved"))

        stg_accounts = spark.read.format("delta").load(f"{SILVER_BASE}/stg_accounts") \
            .select("account_id", "customer_id")

        events = events.join(stg_accounts, "account_id", "left")

        features = events.groupBy("customer_id").agg(
            F.count("event_id").alias("f_tx_velocity_1h"),
            F.sum(F.coalesce("amount", F.lit(0.0))).alias("f_total_amount_1h"),
            F.max(F.col("is_cross_border").cast("int")).alias("f_foreign_flag"),
            F.max(
                F.when(F.hour("event_timestamp").between(0, 4), 1).otherwise(0)
            ).alias("f_unusual_hour_flag"),
        ) \
        .withColumn("event_timestamp", F.lit(as_of.isoformat()).cast("timestamp")) \
        .withColumn("created_ts",      F.current_timestamp()) \
        .fillna(0)

        output_rows = features.count()

        features.write.format("delta") \
            .option("mergeSchema", "true") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .save(FEATURE_PATH)

        print(f"[FEATURE] feat_stream_1h: {output_rows:,} rows  (window: last 1h)")
        publish_lineage("feature_stream_1h",
                        ["silver/stg_events"],
                        ["features/feat_stream_1h"])
        logger.finish("success", events.count(), output_rows)

    except Exception as exc:
        logger.finish("failed", error_msg=str(exc))
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    run()
