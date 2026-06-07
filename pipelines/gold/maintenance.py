"""
Delta Lake maintenance — OPTIMIZE (with Z-ORDER) + VACUUM.

Run daily for OPTIMIZE; add --vacuum flag for the weekly full run.

Usage:
    uv run python -m pipelines.gold.maintenance              # OPTIMIZE only
    uv run python -m pipelines.gold.maintenance --vacuum     # OPTIMIZE + VACUUM
"""

import argparse
import time
from pathlib import Path

from pipelines.common.spark_session import get_spark

# ── Table registry ────────────────────────────────────────────────────────────
# (relative_path, z_order_cols)
# Z-order columns chosen for the primary access patterns (fraud analysis queries).
# None means plain OPTIMIZE (compaction only, no spatial sort).
TABLES = [
    # Gold facts — highest query frequency
    ("data/lakehouse/gold/fact_transaction",            ["customer_id", "merchant_id"]),
    ("data/lakehouse/gold/obt_transaction_performance", ["customer_id"]),
    ("data/lakehouse/gold/fact_fraud_label",            None),
    # Gold dims — small but worth compacting
    ("data/lakehouse/gold/dim_customer",                None),
    ("data/lakehouse/gold/dim_merchant",                None),
    ("data/lakehouse/gold/dim_card",                    None),
    ("data/lakehouse/gold/dim_date",                    None),
    ("data/lakehouse/gold/dim_channel",                 None),
    # Gold ML + monitoring
    ("data/lakehouse/gold/ml_fraud_label",              None),
    ("data/lakehouse/gold/ml_fraud_training",           ["customer_id", "event_timestamp"]),
    ("data/lakehouse/gold/agg_feature_health_daily",    None),
    ("data/lakehouse/gold/feature_drift_alerts",        None),
    # Silver — large incremental tables
    ("data/lakehouse/silver/stg_transactions",          ["account_id"]),
    ("data/lakehouse/silver/stg_events",                ["account_id"]),
    ("data/lakehouse/silver/stg_customers",             None),
    ("data/lakehouse/silver/stg_accounts",              None),
    ("data/lakehouse/silver/stg_cards",                 None),
    ("data/lakehouse/silver/stg_merchants",             None),
    ("data/lakehouse/silver/stg_fraud_labels",          None),
    # Features
    ("data/lakehouse/features/feat_customer_30d",       ["customer_id"]),
    ("data/lakehouse/features/feat_stream_1h",          ["customer_id"]),
    ("data/lakehouse/features/feat_unified",            ["customer_id"]),
    # Bronze — append-only, small files accumulate quickly
    ("data/lakehouse/bronze/raw_transactions",          None),
]

VACUUM_RETAIN_HOURS = 168  # 7 days — Delta Lake minimum; go lower only with
                            # spark.databricks.delta.retentionDurationCheck.enabled=false


def _optimize(spark, path: str, z_cols: list[str] | None) -> float:
    t0 = time.time()
    if z_cols:
        spark.sql(f"OPTIMIZE delta.`{path}` ZORDER BY ({', '.join(z_cols)})")
    else:
        spark.sql(f"OPTIMIZE delta.`{path}`")
    return time.time() - t0


def _vacuum(spark, path: str) -> float:
    t0 = time.time()
    spark.sql(f"VACUUM delta.`{path}` RETAIN {VACUUM_RETAIN_HOURS} HOURS")
    return time.time() - t0


def run(do_vacuum: bool = False) -> None:
    spark = get_spark("finguard.gold.maintenance")
    # Suppress verbose VACUUM file-listing output
    spark.conf.set("spark.databricks.delta.vacuum.logging.enabled", "false")

    optimize_total = vacuum_total = 0.0
    optimize_skipped = vacuum_skipped = 0

    print(f"\n{'='*60}")
    print(f"[MAINT] Starting maintenance  (vacuum={do_vacuum})")
    print(f"{'='*60}")

    for path, z_cols in TABLES:
        abs_path = str(Path(path).resolve())

        if not Path(path).exists():
            print(f"[MAINT] SKIP  {path}  (not found)")
            optimize_skipped += 1
            continue

        label = path.split("/")[-1]

        # OPTIMIZE (daily)
        try:
            elapsed = _optimize(spark, abs_path, z_cols)
            z_note  = f" ZORDER({', '.join(z_cols)})" if z_cols else ""
            print(f"[MAINT] OPTIMIZE{z_note:30s}  {label:<40s}  {elapsed:.1f}s")
            optimize_total += elapsed
        except Exception as exc:
            print(f"[MAINT] OPTIMIZE FAILED  {label}  — {exc}")

        # VACUUM (weekly — only when flag is set)
        if do_vacuum:
            try:
                elapsed = _vacuum(spark, abs_path)
                print(f"[MAINT] VACUUM   RETAIN {VACUUM_RETAIN_HOURS}h              {label:<40s}  {elapsed:.1f}s")
                vacuum_total += elapsed
            except Exception as exc:
                print(f"[MAINT] VACUUM FAILED  {label}  — {exc}")

    print(f"\n{'='*60}")
    total_tables = len(TABLES) - optimize_skipped
    print(f"[MAINT] Done — {total_tables} tables processed, {optimize_skipped} skipped")
    print(f"[MAINT] OPTIMIZE total: {optimize_total:.1f}s")
    if do_vacuum:
        print(f"[MAINT] VACUUM   total: {vacuum_total:.1f}s")
    print(f"{'='*60}\n")

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Delta Lake maintenance")
    parser.add_argument("--vacuum", action="store_true", help="Also run VACUUM (weekly)")
    args = parser.parse_args()
    run(do_vacuum=args.vacuum)
