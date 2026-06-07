"""
Gold — fact_transaction + fact_fraud_label.

Reads new Silver records since last watermark, joins to dims,
and MERGEs into Gold fact tables (upsert on business key).

Usage:
    uv run python -m pipelines.gold.fact_transaction
"""

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from pipelines.common.lineage import publish_lineage
from pipelines.common.quality_checks import (
    check_fraud_rate,
    check_no_nulls,
    check_not_empty,
    check_volume_drop,
)
from pipelines.common.run_metadata import RunLogger
from pipelines.common.spark_session import get_spark
from pipelines.common.watermark import get_watermark, set_watermark

SILVER_BASE = "data/lakehouse/silver"
GOLD_BASE   = "data/lakehouse/gold"


def run() -> None:
    spark = get_spark("finguard.gold.fact_transaction")

    # ── Load dims (always full — they're small) ───────────────────────────
    dim_customer = spark.read.format("delta").load(f"{GOLD_BASE}/dim_customer") \
                       .filter(F.col("is_current")) \
                       .select("customer_id", F.abs(F.hash(F.col("customer_id"))).alias("customer_key"))
    dim_merchant = spark.read.format("delta").load(f"{GOLD_BASE}/dim_merchant") \
                       .filter(F.col("is_current")) \
                       .select("merchant_id", F.abs(F.hash(F.col("merchant_id"))).alias("merchant_key"))
    dim_card     = spark.read.format("delta").load(f"{GOLD_BASE}/dim_card") \
                       .filter(F.col("is_current")) \
                       .select("card_id", F.abs(F.hash(F.col("card_id"))).alias("card_key"))
    dim_channel  = spark.read.format("delta").load(f"{GOLD_BASE}/dim_channel")
    dim_date     = spark.read.format("delta").load(f"{GOLD_BASE}/dim_date")
    stg_accounts = spark.read.format("delta").load(f"{SILVER_BASE}/stg_accounts") \
                       .select("account_id", "customer_id")

    # ── fact_transaction ──────────────────────────────────────────────────
    logger = RunLogger("gold.fact_transaction")
    try:
        wm_key = "gold.fact_transaction"
        last_silver_ts = get_watermark(wm_key)

        stg_tx = spark.read.format("delta").load(f"{SILVER_BASE}/stg_transactions") \
                      .filter(F.col("_silver_ts") > last_silver_ts)

        input_rows = stg_tx.count()
        print(f"[GOLD] fact_transaction: {input_rows:,} new Silver rows since {last_silver_ts}")

        if input_rows > 0:
            max_silver_ts = stg_tx.agg(F.max("_silver_ts")).collect()[0][0]

            fact = stg_tx \
                .join(stg_accounts, "account_id", "left") \
                .join(dim_customer,  "customer_id",  "left") \
                .join(dim_merchant,  "merchant_id",  "left") \
                .join(dim_card,      "card_id",       "left") \
                .join(dim_channel,   stg_tx["channel"] == dim_channel["channel_name"], "left") \
                .join(
                    dim_date,
                    F.date_format(F.col("event_timestamp"), "yyyyMMdd").cast("int") == dim_date["date_key"],
                    "left"
                ) \
                .select(
                    F.md5("transaction_id").alias("transaction_key"),
                    "transaction_id",
                    stg_tx["account_id"],
                    stg_accounts["customer_id"],
                    stg_tx["merchant_id"],
                    stg_tx["card_id"],
                    "customer_key",
                    "merchant_key",
                    "card_key",
                    "channel_key",
                    "date_key",
                    "amount",
                    "currency",
                    F.when(F.col("status") == "approved", 1).otherwise(0).alias("is_approved"),
                    F.when(F.col("status") == "declined", 1).otherwise(0).alias("is_declined"),
                    F.col("is_fraud").cast("int").alias("is_fraud"),
                    "device_fingerprint",
                    "event_timestamp",
                    "transaction_date",
                    "created_ts",
                    F.current_timestamp().alias("_gold_ts"),
                )

            check_not_empty(fact, "fact_transaction")
            check_no_nulls(fact, "fact_transaction", ["transaction_id", "amount", "event_timestamp"])
            check_volume_drop(fact, "fact_transaction")
            check_fraud_rate(fact, "fact_transaction")

            output_rows = fact.count()
            gold_path = f"{GOLD_BASE}/fact_transaction"

            if DeltaTable.isDeltaTable(spark, gold_path):
                DeltaTable.forPath(spark, gold_path).alias("target") \
                    .merge(fact.alias("source"), "target.transaction_id = source.transaction_id") \
                    .whenMatchedUpdateAll() \
                    .whenNotMatchedInsertAll() \
                    .execute()
            else:
                fact.write.format("delta") \
                    .option("mergeSchema", "true") \
                    .partitionBy("transaction_date") \
                    .mode("overwrite") \
                    .save(gold_path)

            set_watermark(wm_key, str(max_silver_ts))
            print(f"[GOLD] fact_transaction: merged {output_rows:,} rows")
            publish_lineage("gold_fact_transaction",
                            ["silver/stg_transactions", "gold/dim_customer", "gold/dim_merchant", "gold/dim_card"],
                            ["gold/fact_transaction"])
            logger.finish("success", input_rows, output_rows)
        else:
            print("[GOLD] fact_transaction: nothing new to process")
            logger.finish("success", 0, 0)

    except Exception as exc:
        logger.finish("failed", error_msg=str(exc))
        raise

    # ── fact_fraud_label ──────────────────────────────────────────────────
    logger2 = RunLogger("gold.fact_fraud_label")
    try:
        wm_key2 = "gold.fact_fraud_label"
        last_silver_ts2 = get_watermark(wm_key2)

        stg_fl = spark.read.format("delta").load(f"{SILVER_BASE}/stg_fraud_labels") \
                      .filter(F.col("_silver_ts") > last_silver_ts2)

        fl_input = stg_fl.count()
        print(f"[GOLD] fact_fraud_label: {fl_input:,} new Silver rows since {last_silver_ts2}")

        if fl_input > 0:
            max_silver_ts2 = stg_fl.agg(F.max("_silver_ts")).collect()[0][0]

            fact_tx_lookup = spark.read.format("delta").load(f"{GOLD_BASE}/fact_transaction") \
                .select("transaction_id", "transaction_key", "customer_key", "date_key")

            fact_fraud = stg_fl \
                .join(fact_tx_lookup, "transaction_id", "left") \
                .select(
                    F.md5("label_id").alias("fraud_key"),
                    "label_id",
                    "transaction_id",
                    "transaction_key",
                    "customer_key",
                    "date_key",
                    "fraud_type",
                    "confirmed_ts",
                    "created_ts",
                )

            gold_path2 = f"{GOLD_BASE}/fact_fraud_label"

            if DeltaTable.isDeltaTable(spark, gold_path2):
                DeltaTable.forPath(spark, gold_path2).alias("target") \
                    .merge(fact_fraud.alias("source"), "target.label_id = source.label_id") \
                    .whenMatchedUpdateAll() \
                    .whenNotMatchedInsertAll() \
                    .execute()
            else:
                fact_fraud.write.format("delta") \
                    .option("mergeSchema", "true") \
                    .mode("overwrite") \
                    .save(gold_path2)

            set_watermark(wm_key2, str(max_silver_ts2))
            fl_rows = fact_fraud.count()
            print(f"[GOLD] fact_fraud_label: merged {fl_rows:,} rows")
            publish_lineage("gold_fact_fraud_label",
                            ["silver/stg_fraud_labels", "gold/fact_transaction"],
                            ["gold/fact_fraud_label"])
            logger2.finish("success", fl_input, fl_rows)
        else:
            print("[GOLD] fact_fraud_label: nothing new to process")
            logger2.finish("success", 0, 0)

    except Exception as exc:
        logger2.finish("failed", error_msg=str(exc))
        raise

    spark.stop()


if __name__ == "__main__":
    run()
