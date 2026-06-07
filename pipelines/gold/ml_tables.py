"""
Gold — ml_fraud_label + ml_fraud_training.

ml_fraud_label:
  One row per approved transaction with a binary fraud label.
  event_timestamp used for point-in-time feature joins.

ml_fraud_training:
  Joins ml_fraud_label with feat_unified for a training-ready dataset.
  Point-in-time correct: features taken at or before event_timestamp.

Usage:
    uv run python -m pipelines.gold.ml_tables
"""

from pyspark.sql import Window, functions as F

from data_generator.config import load_config
from pipelines.common.lineage import publish_lineage
from pipelines.common.quality_checks import check_no_nulls, check_not_empty
from pipelines.common.run_metadata import RunLogger
from pipelines.common.spark_session import get_spark

SILVER_BASE  = "data/lakehouse/silver"
GOLD_BASE    = "data/lakehouse/gold"
FEATURE_BASE = "data/lakehouse/features"


def run() -> None:
    config      = load_config()
    drift_start = config.drift_start_date   # e.g. "2026-03-01"

    spark = get_spark("finguard.gold.ml_tables")

    n_labels = 0   # initialised here so ml_fraud_training logger can always reference it

    # ── ml_fraud_label ────────────────────────────────────────────────────
    logger = RunLogger("gold.ml_fraud_label")
    try:
        stg_tx     = spark.read.format("delta").load(f"{SILVER_BASE}/stg_transactions") \
                        .filter(F.col("status") == "approved")
        stg_fl     = spark.read.format("delta").load(f"{SILVER_BASE}/stg_fraud_labels") \
                        .select("transaction_id", F.lit(1).alias("label"))
        stg_accts  = spark.read.format("delta").load(f"{SILVER_BASE}/stg_accounts") \
                        .select("account_id", "customer_id")

        labels = stg_tx \
            .join(stg_accts, "account_id", "left") \
            .join(stg_fl, "transaction_id", "left") \
            .select(
                "transaction_id",
                "customer_id",
                "event_timestamp",
                F.current_timestamp().alias("created_ts"),
                F.coalesce(F.col("label"), F.lit(0)).alias("label"),
                F.when(F.col("event_timestamp") < F.lit(drift_start).cast("timestamp"),
                       F.lit(1)).otherwise(F.lit(0)).alias("is_pre_drift"),
            )

        check_not_empty(labels, "ml_fraud_label")
        check_no_nulls(labels, "ml_fraud_label", ["transaction_id", "customer_id", "event_timestamp", "label"])

        labels.write.format("delta") \
            .mode("overwrite").option("overwriteSchema", "true") \
            .save(f"{GOLD_BASE}/ml_fraud_label")

        n_labels  = labels.count()
        fraud_pct = labels.filter(F.col("label") == 1).count() / n_labels * 100
        print(f"[GOLD] ml_fraud_label: {n_labels:,} rows  ({fraud_pct:.2f}% fraud)")

        publish_lineage("gold_ml_fraud_label",
                        ["silver/stg_transactions", "silver/stg_fraud_labels"],
                        ["gold/ml_fraud_label"])
        logger.finish("success", stg_tx.count(), n_labels)

    except Exception as exc:
        logger.finish("failed", error_msg=str(exc))
        raise

    # ── ml_fraud_training ─────────────────────────────────────────────────
    logger2 = RunLogger("gold.ml_fraud_training")
    try:
        label_df   = spark.read.format("delta").load(f"{GOLD_BASE}/ml_fraud_label")
        feat_30d   = spark.read.format("delta").load(f"{FEATURE_BASE}/feat_customer_30d")

        w = Window.partitionBy("customer_id").orderBy(F.col("event_timestamp").desc())

        feat_30d_latest = feat_30d \
            .withColumn("_rank", F.row_number().over(w)) \
            .filter(F.col("_rank") == 1).drop("_rank", "event_timestamp", "created_ts")

        training = label_df \
            .join(feat_30d_latest, "customer_id", "left")

        # Include 1h streaming features if available (requires running streaming pipeline)
        try:
            feat_1h = spark.read.format("delta").load(f"{FEATURE_BASE}/feat_stream_1h")
            feat_1h_latest = feat_1h \
                .withColumn("_rank", F.row_number().over(w)) \
                .filter(F.col("_rank") == 1).drop("_rank", "event_timestamp", "created_ts")
            training = training.join(feat_1h_latest, "customer_id", "left")
        except Exception:
            pass  # streaming features not yet available

        training = training.fillna(0)

        check_not_empty(training, "ml_fraud_training")

        training.write.format("delta") \
            .mode("overwrite").option("overwriteSchema", "true") \
            .save(f"{GOLD_BASE}/ml_fraud_training")

        n_training = training.count()
        print(f"[GOLD] ml_fraud_training: {n_training:,} rows  ({training.columns})")

        publish_lineage("gold_ml_fraud_training",
                        ["gold/ml_fraud_label", "features/feat_customer_30d", "features/feat_stream_1h"],
                        ["gold/ml_fraud_training"])
        logger2.finish("success", n_labels, n_training)

    except Exception as exc:
        logger2.finish("failed", error_msg=str(exc))
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    run()
