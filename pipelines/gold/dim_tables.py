"""
Gold — dim_customer, dim_merchant, dim_card (SCD2).

Reads from Silver, applies SCD2 logic:
  - new records: insert with valid_from_ts = now, valid_to_ts = NULL, is_current = True
  - changed records: expire old version (set valid_to_ts, is_current = False), insert new version
  - unchanged records: no-op

Usage:
    uv run python -m pipelines.gold.dim_tables
"""

from datetime import datetime

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from pipelines.common.lineage import publish_lineage
from pipelines.common.run_metadata import RunLogger
from pipelines.common.spark_session import get_spark

SILVER_BASE = "data/lakehouse/silver"
GOLD_BASE   = "data/lakehouse/gold"

NOW = datetime.utcnow().isoformat()


def _build_scd2(spark: SparkSession, silver_path: str, gold_path: str,
                bk: str, change_cols: list[str]) -> tuple[int, int]:
    """
    Generic SCD2 merge.
    bk: business key column name
    change_cols: columns that trigger a new version when changed
    """
    incoming = spark.read.format("delta").load(silver_path) \
        .withColumn("valid_from_ts", F.lit(NOW)) \
        .withColumn("valid_to_ts",   F.lit(None).cast("string")) \
        .withColumn("is_current",    F.lit(True))

    try:
        target = DeltaTable.forPath(spark, gold_path)
        existing = target.toDF()
        input_rows = incoming.count()

        current_existing = existing.filter(F.col("is_current")).select(bk, *change_cols)

        # Expire old versions for changed records — SQL string uses fully qualified aliases
        # set on both sides of the merge, so there is no column ambiguity here.
        change_condition = " OR ".join(
            [f"existing.{c} != incoming.{c}" for c in change_cols]
        )
        target.alias("existing").merge(
            incoming.alias("incoming"),
            f"existing.{bk} = incoming.{bk} AND existing.is_current = true"
        ).whenMatchedUpdate(
            condition=change_condition,
            set={
                "valid_to_ts": F.lit(NOW),
                "is_current":  F.lit(False),
            }
        ).execute()

        # Insert new versions (new + changed records).
        # Use DataFrame column objects — not SQL strings — to avoid ambiguity after join.
        new_records = incoming.join(current_existing, on=bk, how="left_anti")

        joined = incoming.alias("inc").join(
            current_existing.alias("cur"), on=bk, how="inner"
        )
        change_expr = F.lit(False)
        for c in change_cols:
            change_expr = change_expr | (F.col(f"inc.{c}") != F.col(f"cur.{c}"))
        changed_records = joined.filter(change_expr).select(
            [F.col(f"inc.{col}").alias(col) for col in incoming.columns]
        )

        to_insert = new_records.union(changed_records)
        to_insert.write.format("delta").mode("append").save(gold_path)

        output_rows = spark.read.format("delta").load(gold_path).count()

    except Exception:
        # First run: write directly
        input_rows = incoming.count()
        incoming.write.format("delta") \
            .option("overwriteSchema", "true") \
            .mode("overwrite") \
            .save(gold_path)
        output_rows = incoming.count()

    return input_rows, output_rows


def run() -> None:
    spark = get_spark("finguard.gold.dim_tables")

    dim_configs = [
        (
            "customers", "customer",
            f"{SILVER_BASE}/stg_customers",
            f"{GOLD_BASE}/dim_customer",
            "customer_id",
            ["kyc_status", "risk_tier", "city"],
        ),
        (
            "merchants", "merchant",
            f"{SILVER_BASE}/stg_merchants",
            f"{GOLD_BASE}/dim_merchant",
            "merchant_id",
            ["is_active", "city"],
        ),
        (
            "cards", "card",
            f"{SILVER_BASE}/stg_cards",
            f"{GOLD_BASE}/dim_card",
            "card_id",
            ["is_active"],
        ),
    ]

    for src_table, dim_name, silver_path, gold_path, bk, change_cols in dim_configs:
        logger = RunLogger(f"gold.dim_{dim_name}")
        try:
            in_r, out_r = _build_scd2(spark, silver_path, gold_path, bk, change_cols)
            publish_lineage(f"gold_dim_{dim_name}", [f"silver/stg_{src_table}"], [f"gold/dim_{dim_name}"])
            logger.finish("success", in_r, out_r)
            print(f"[GOLD] dim_{dim_name}: {out_r:,} rows (all versions)")
        except Exception as exc:
            logger.finish("failed", error_msg=str(exc))
            raise

    # dim_channel: static lookup
    _build_dim_channel(spark)
    spark.stop()


def _build_dim_channel(spark: SparkSession) -> None:
    import pandas as pd
    df = spark.createDataFrame(pd.DataFrame([
        {"channel_key": 1, "channel_name": "online"},
        {"channel_key": 2, "channel_name": "pos"},
        {"channel_key": 3, "channel_name": "atm"},
    ]))
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
        .save(f"{GOLD_BASE}/dim_channel")
    print(f"[GOLD] dim_channel: {df.count()} rows")


if __name__ == "__main__":
    run()
