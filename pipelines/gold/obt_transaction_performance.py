"""
Gold — obt_transaction_performance.

Denormalized One-Big-Table joining fact_transaction with all relevant dimensions.
Optimized for BI dashboards and ad-hoc analytics (eliminates runtime joins).

Usage:
    uv run python -m pipelines.gold.obt_transaction_performance
"""

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from pipelines.common.lineage import publish_lineage
from pipelines.common.quality_checks import check_not_empty, check_volume_drop
from pipelines.common.run_metadata import RunLogger
from pipelines.common.spark_session import get_spark
from pipelines.common.watermark import get_watermark, set_watermark

GOLD_BASE = "data/lakehouse/gold"
OBT_PATH  = f"{GOLD_BASE}/obt_transaction_performance"


def run() -> None:
    spark = get_spark("finguard.gold.obt_transaction_performance")
    logger = RunLogger("gold.obt_transaction_performance")

    try:
        wm_key = "gold.obt_transaction_performance"
        last_ts = get_watermark(wm_key)

        # Only process fact rows that are new since last OBT run.
        # _gold_ts is stamped by fact_transaction at MERGE time and advances on every
        # write. Fall back to created_ts for tables written before _gold_ts was added.
        fact_df = spark.read.format("delta").load(f"{GOLD_BASE}/fact_transaction")
        if "_gold_ts" not in fact_df.columns:
            fact_df = fact_df.withColumn("_gold_ts", F.col("created_ts"))
        fact_tx_new = fact_df.filter(F.col("_gold_ts") > last_ts)

        input_rows = fact_tx_new.count()
        print(f"[GOLD] obt_transaction_performance: {input_rows:,} new fact rows since {last_ts}")

        if input_rows == 0:
            print("[GOLD] obt_transaction_performance: nothing new to process")
            logger.finish("success", 0, 0)
            return

        max_ts = fact_tx_new.agg(F.max("_gold_ts")).collect()[0][0]

        fact_tx = fact_tx_new
        fact_fraud  = spark.read.format("delta").load(f"{GOLD_BASE}/fact_fraud_label") \
                         .select("transaction_id", "fraud_type")
        dim_customer = spark.read.format("delta").load(f"{GOLD_BASE}/dim_customer") \
                          .filter(F.col("is_current")) \
                          .select("customer_id", "city", "country", "kyc_status", "risk_tier")
        dim_merchant = spark.read.format("delta").load(f"{GOLD_BASE}/dim_merchant") \
                          .filter(F.col("is_current")) \
                          .select("merchant_id", "name", "category",
                                  F.col("city").alias("merchant_city"),
                                  F.col("country").alias("merchant_country"))
        dim_card     = spark.read.format("delta").load(f"{GOLD_BASE}/dim_card") \
                          .filter(F.col("is_current")) \
                          .select("card_id", "card_type")
        dim_channel  = spark.read.format("delta").load(f"{GOLD_BASE}/dim_channel")

        obt = fact_tx \
            .join(dim_customer, "customer_id",   "left") \
            .join(dim_merchant, "merchant_id",   "left") \
            .join(dim_card,     "card_id",        "left") \
            .join(dim_channel,  "channel_key",    "left") \
            .join(fact_fraud,   "transaction_id", "left") \
            .select(
                fact_tx["transaction_id"],
                fact_tx["event_timestamp"],
                fact_tx["transaction_date"],
                # Customer
                fact_tx["customer_id"],
                dim_customer["city"].alias("customer_city"),
                dim_customer["country"].alias("customer_country"),
                dim_customer["kyc_status"],
                dim_customer["risk_tier"],
                # Merchant
                fact_tx["merchant_id"],
                dim_merchant["name"].alias("merchant_name"),
                dim_merchant["category"].alias("merchant_category"),
                dim_merchant["merchant_city"],
                dim_merchant["merchant_country"],
                # Transaction
                dim_card["card_type"],
                dim_channel["channel_name"].alias("channel"),
                fact_tx["amount"],
                fact_tx["currency"],
                fact_tx["is_approved"],
                fact_tx["is_declined"],
                fact_tx["is_fraud"],
                fact_fraud["fraud_type"],
                fact_tx["device_fingerprint"],
                fact_tx["created_ts"],
            )

        output_rows = obt.count()

        check_not_empty(obt, "obt_transaction_performance")
        check_volume_drop(obt, "obt_transaction_performance")

        if DeltaTable.isDeltaTable(spark, OBT_PATH):
            DeltaTable.forPath(spark, OBT_PATH).alias("target") \
                .merge(obt.alias("source"), "target.transaction_id = source.transaction_id") \
                .whenMatchedUpdateAll() \
                .whenNotMatchedInsertAll() \
                .execute()
        else:
            obt.orderBy("customer_id", "transaction_date") \
                .write.format("delta") \
                .option("mergeSchema", "true") \
                .partitionBy("transaction_date") \
                .mode("overwrite") \
                .save(OBT_PATH)

        set_watermark(wm_key, str(max_ts))
        print(f"[GOLD] obt_transaction_performance: merged {output_rows:,} rows")
        publish_lineage(
            "gold_obt_transaction_performance",
            ["gold/fact_transaction", "gold/fact_fraud_label",
             "gold/dim_customer", "gold/dim_merchant", "gold/dim_card"],
            ["gold/obt_transaction_performance"],
        )
        logger.finish("success", input_rows, output_rows)


    except Exception as exc:
        logger.finish("failed", error_msg=str(exc))
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    run()
