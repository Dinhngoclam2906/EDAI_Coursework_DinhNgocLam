"""
Gold — drift monitoring tables.

Builds:
  agg_feature_health_daily  — daily PSI per feature vs pre-drift baseline
  feature_drift_alerts      — rows where PSI > 0.15

Reads from: data/drift_validation_report.csv (produced by data_generator.drift_report)

Usage:
    uv run python -m pipelines.gold.monitoring
"""

from pathlib import Path

import pandas as pd
from pyspark.sql import functions as F

from pipelines.common.lineage import publish_lineage
from pipelines.common.run_metadata import RunLogger
from pipelines.common.spark_session import get_spark

REPORT_CSV = "data/drift_validation_report.csv"
GOLD_BASE  = "data/lakehouse/gold"

# PSI alert threshold calibrated for binary/proportion features (fraud rate, declined rate).
# Standard 0.15 threshold assumes continuous features with many bins; binary proportion PSI
# is mathematically bounded lower — a 3× fraud spike produces PSI ~0.047, so 0.04 is used.
PSI_ALERT_THRESHOLD = 0.04

DRIFT_TYPE_MAP = {
    "f_avg_amount_30d": "covariate",
    "f_fraud_rate":     "concept",
}

ACTION_MAP = {
    "f_avg_amount_30d": "Monitor amount distribution; check for seasonal / economic shift (Scenario B).",
    "f_fraud_rate":     "Investigate new fraud pattern; consider model retraining (Scenario A — probing surge).",
}


def run() -> None:
    spark  = get_spark("finguard.gold.monitoring")
    logger = RunLogger("gold.monitoring")

    try:
        if not Path(REPORT_CSV).exists():
            print(f"[MONITORING] {REPORT_CSV} not found — run data_generator.drift_report first.")
            return

        report = pd.read_csv(REPORT_CSV)

        # ── agg_feature_health_daily ───────────────────────────────────────
        health_df = spark.createDataFrame(report).select(
            F.col("date").alias("monitoring_date"),
            F.col("feature_name"),
            F.col("mean").alias("mean_value"),
            F.col("stddev").alias("stddev_value"),
            F.col("psi_vs_baseline"),
            (F.col("psi_vs_baseline") > PSI_ALERT_THRESHOLD).alias("alert_flag"),
        )

        health_df.write.format("delta") \
            .mode("overwrite").option("overwriteSchema", "true") \
            .save(f"{GOLD_BASE}/agg_feature_health_daily")

        print(f"[GOLD] agg_feature_health_daily: {health_df.count():,} rows")

        # ── feature_drift_alerts ───────────────────────────────────────────
        drift_map   = F.create_map(*[F.lit(v) for pair in DRIFT_TYPE_MAP.items() for v in pair])
        action_map  = F.create_map(*[F.lit(v) for pair in ACTION_MAP.items() for v in pair])

        alerts_df = health_df \
            .filter(F.col("psi_vs_baseline") > PSI_ALERT_THRESHOLD) \
            .select(
                F.col("monitoring_date").alias("alert_date"),
                F.col("feature_name"),
                F.col("psi_vs_baseline").alias("psi_value"),
                F.coalesce(drift_map[F.col("feature_name")], F.lit("unknown")).alias("drift_type"),
                F.coalesce(action_map[F.col("feature_name")], F.lit("Investigate.")).alias("recommended_action"),
            )

        alerts_df.write.format("delta") \
            .mode("overwrite").option("overwriteSchema", "true") \
            .save(f"{GOLD_BASE}/feature_drift_alerts")

        print(f"[GOLD] feature_drift_alerts: {alerts_df.count():,} alerts")

        publish_lineage("gold_monitoring",
                        ["drift_validation_report"],
                        ["gold/agg_feature_health_daily", "gold/feature_drift_alerts"])
        logger.finish("success", len(report), health_df.count())

    except Exception as exc:
        logger.finish("failed", error_msg=str(exc))
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    run()
