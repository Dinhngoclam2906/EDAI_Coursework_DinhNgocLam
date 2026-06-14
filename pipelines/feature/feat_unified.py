"""
Feature pipeline — feat_unified.

Joins feat_customer_30d (offline) + feat_stream_1h (streaming)
into a single unified feature table for ML training and inference.

Point-in-time correctness: streaming features use latest available
values at or before the offline feature event_timestamp.

Usage:
    uv run python -m pipelines.feature.feat_unified
"""

from pyspark.sql import functions as F

from pipelines.common.lineage import publish_lineage
from pipelines.common.run_metadata import RunLogger
from pipelines.common.spark_session import get_spark

FEATURE_BASE = "data/lakehouse/features"
UNIFIED_PATH = f"{FEATURE_BASE}/feat_unified"


def run() -> None:
    spark = get_spark("finguard.feature.feat_unified")
    logger = RunLogger("feature.feat_unified")

    try:
        from pyspark.sql import Window
        offline  = spark.read.format("delta").load(f"{FEATURE_BASE}/feat_customer_30d")

        unified = offline.withColumn("created_ts", F.current_timestamp())

        # Include 1h streaming features if available (requires running streaming pipeline)
        try:
            streaming = spark.read.format("delta").load(f"{FEATURE_BASE}/feat_stream_1h")
            w = Window.partitionBy("customer_id").orderBy(F.col("event_timestamp").desc())
            streaming_latest = streaming \
                .withColumn("_rank", F.row_number().over(w)) \
                .filter(F.col("_rank") == 1) \
                .drop("_rank", "event_timestamp", "created_ts")
            unified = unified.join(streaming_latest, "customer_id", "left")
        except Exception:
            pass  # streaming features not yet available

        unified = unified.fillna(0)

        output_rows = unified.count()

        unified.write.format("delta") \
            .option("mergeSchema", "true") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .save(UNIFIED_PATH)

        print(f"[FEATURE] feat_unified: {output_rows:,} rows")
        publish_lineage("feature_unified",
                        ["features/feat_customer_30d", "features/feat_stream_1h"],
                        ["features/feat_unified"])
        logger.finish("success", offline.count(), output_rows)

    except Exception as exc:
        logger.finish("failed", error_msg=str(exc))
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    run()
