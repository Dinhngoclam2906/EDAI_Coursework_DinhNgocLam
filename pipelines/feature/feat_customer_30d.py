"""
Feature pipeline — feat_customer_30d.

Computes offline batch features per customer using a 30-day rolling window.
Reads from Gold fact_transaction + fact_fraud_label.

Features:
  f_total_tx_30d          — total transaction count in last 30 days
  f_avg_amount_30d        — average transaction amount in last 30 days
  f_distinct_merchants_30d — unique merchants visited in last 30 days
  f_declined_rate_30d     — fraction of declined transactions in last 30 days
  f_fraud_rate_90d        — confirmed fraud rate in last 90 days

Usage:
    uv run python -m pipelines.feature.feat_customer_30d
"""

from datetime import datetime, timedelta

from pyspark.sql import functions as F

from pipelines.common.lineage import publish_lineage
from pipelines.common.run_metadata import RunLogger
from pipelines.common.spark_session import get_spark

GOLD_BASE    = "data/lakehouse/gold"
FEATURE_PATH = "data/lakehouse/features/feat_customer_30d"


def run() -> None:
    as_of      = datetime.utcnow()
    window_30d = as_of - timedelta(days=30)
    window_90d = as_of - timedelta(days=90)

    spark = get_spark("finguard.feature.feat_customer_30d")
    logger = RunLogger("feature.feat_customer_30d")

    try:
        fact_tx    = spark.read.format("delta").load(f"{GOLD_BASE}/fact_transaction")
        fact_fraud = spark.read.format("delta").load(f"{GOLD_BASE}/fact_fraud_label") \
                        .select("transaction_id", F.lit(1).alias("confirmed_fraud"))

        tx = fact_tx.join(fact_fraud, "transaction_id", "left")

        # 30-day window features — partition pre-filter on transaction_date enables
        # partition pruning; timestamp filter then applies the precise boundary.
        w30 = tx.filter(F.col("transaction_date") >= window_30d.date().isoformat()) \
                .filter(F.col("event_timestamp")  >= F.lit(window_30d.isoformat()))
        feat_30d = w30.groupBy("customer_id").agg(
            F.count("transaction_id").alias("f_total_tx_30d"),
            F.avg("amount").alias("f_avg_amount_30d"),
            F.countDistinct("merchant_id").alias("f_distinct_merchants_30d"),
            (F.sum("is_declined") / F.count("transaction_id")).alias("f_declined_rate_30d"),
        )

        # 90-day fraud rate — same partition pruning pattern
        w90 = tx.filter(F.col("transaction_date") >= window_90d.date().isoformat()) \
                .filter(F.col("event_timestamp")  >= F.lit(window_90d.isoformat()))
        feat_90d = w90.groupBy("customer_id").agg(
            (F.sum(F.coalesce("confirmed_fraud", F.lit(0))) / F.count("transaction_id")).alias("f_fraud_rate_90d"),
        )

        features = feat_30d.join(feat_90d, "customer_id", "left") \
            .withColumn("event_timestamp", F.lit(as_of.isoformat()).cast("timestamp")) \
            .withColumn("created_ts",      F.current_timestamp()) \
            .fillna(0.0)

        output_rows = features.count()

        features.write.format("delta") \
            .option("mergeSchema", "true") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .save(FEATURE_PATH)

        print(f"[FEATURE] feat_customer_30d: {output_rows:,} rows  (as-of {as_of.date()})")
        publish_lineage("feature_customer_30d",
                        ["gold/fact_transaction", "gold/fact_fraud_label"],
                        ["features/feat_customer_30d"])
        logger.finish("success", fact_tx.count(), output_rows)

    except Exception as exc:
        logger.finish("failed", error_msg=str(exc))
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    run()
