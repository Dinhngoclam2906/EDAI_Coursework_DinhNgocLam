"""
Data quality checks for FinGuard pipelines.
Each check raises DataQualityError on failure — acts as a hard circuit breaker.
"""

import json
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

VOLUME_HISTORY_DIR = Path("data/quality/volume_history")


class DataQualityError(Exception):
    pass


def check_not_empty(df: DataFrame, table: str) -> None:
    count = df.count()
    if count == 0:
        raise DataQualityError(f"[QC] {table}: table is empty — pipeline blocked")
    print(f"[QC] {table}: {count:,} rows — OK")


def check_no_nulls(df: DataFrame, table: str, columns: list[str]) -> None:
    for col in columns:
        null_count = df.filter(F.col(col).isNull()).count()
        if null_count > 0:
            raise DataQualityError(f"[QC] {table}.{col}: {null_count:,} unexpected NULLs — pipeline blocked")
    print(f"[QC] {table}: null checks passed for {columns}")


def check_unique(df: DataFrame, table: str, key_col: str) -> None:
    total = df.count()
    unique = df.select(key_col).distinct().count()
    dupes = total - unique
    if dupes > 0:
        raise DataQualityError(f"[QC] {table}.{key_col}: {dupes:,} duplicate keys found — pipeline blocked")
    print(f"[QC] {table}.{key_col}: uniqueness OK ({unique:,} rows)")


def check_referential(
    fact_df: DataFrame,
    dim_df: DataFrame,
    fact_col: str,
    dim_col: str,
    table: str,
) -> None:
    orphans = fact_df.join(dim_df.select(dim_col), fact_df[fact_col] == dim_df[dim_col], "left_anti").count()
    if orphans > 0:
        raise DataQualityError(f"[QC] {table}.{fact_col}: {orphans:,} orphan keys — pipeline blocked")
    print(f"[QC] {table}.{fact_col} -> referential integrity OK")


def check_amount_range(
    df: DataFrame,
    table: str,
    col: str = "amount",
    min_val: float = 5_000,
    max_val: float = 50_000_000,
    max_violation_rate: float = 0.05,
) -> None:
    total = df.count()
    out_of_range = df.filter((F.col(col) < min_val) | (F.col(col) > max_val)).count()
    violation_rate = out_of_range / total if total > 0 else 0
    if violation_rate > max_violation_rate:
        raise DataQualityError(
            f"[QC] {table}.{col}: {violation_rate:.1%} rows out of range [{min_val:,.0f}, {max_val:,.0f}] "
            f"exceeds threshold {max_violation_rate:.0%} — pipeline blocked"
        )
    if out_of_range > 0:
        print(f"[QC] {table}.{col}: {out_of_range:,} rows out of range ({violation_rate:.1%}) — within tolerance")
    else:
        print(f"[QC] {table}.{col}: amount range OK")


def check_volume_drop(
    df: DataFrame,
    table: str,
    max_drop_rate: float = 0.50,
) -> None:
    """Raises if current row count drops more than max_drop_rate vs prior run."""
    current = df.count()
    VOLUME_HISTORY_DIR.mkdir(parents=True, exist_ok=True)
    history_path = VOLUME_HISTORY_DIR / f"{table.replace('.', '_')}.json"

    if history_path.exists():
        prior = json.loads(history_path.read_text(encoding="utf-8-sig"))["row_count"]
        if prior > 0:
            drop_rate = (prior - current) / prior
            if drop_rate > max_drop_rate:
                raise DataQualityError(
                    f"[QC] {table}: row count dropped {drop_rate:.1%} "
                    f"({prior:,} → {current:,}) exceeds threshold {max_drop_rate:.0%} — pipeline blocked"
                )
            print(f"[QC] {table}: volume OK ({prior:,} → {current:,}, {drop_rate:+.1%})")
        else:
            print(f"[QC] {table}: volume OK ({current:,} rows, first run)")
    else:
        print(f"[QC] {table}: volume OK ({current:,} rows, first run)")

    history_path.write_text(json.dumps({"row_count": current}), encoding="utf-8")


def check_fraud_rate(
    df: DataFrame,
    table: str,
    is_fraud_col: str = "is_fraud",
    min_rate: float = 0.001,
    max_rate: float = 0.10,
) -> None:
    """Raises if fraud rate falls outside expected range — signals label pipeline failure or data issue."""
    total = df.count()
    if total == 0:
        return
    fraud_count = df.filter(F.col(is_fraud_col) == 1).count()
    rate = fraud_count / total
    if rate < min_rate:
        raise DataQualityError(
            f"[QC] {table}: fraud rate {rate:.3%} is below minimum {min_rate:.3%} "
            f"— possible label pipeline failure or data loss — pipeline blocked"
        )
    if rate > max_rate:
        raise DataQualityError(
            f"[QC] {table}: fraud rate {rate:.3%} exceeds maximum {max_rate:.3%} "
            f"— possible data contamination — pipeline blocked"
        )
    print(f"[QC] {table}: fraud rate OK ({rate:.3%}, {fraud_count:,} fraud / {total:,} total)")


def check_volume(df: DataFrame, table: str, expected_min: int) -> None:
    count = df.count()
    if count < expected_min:
        raise DataQualityError(
            f"[QC] {table}: only {count:,} rows, expected >= {expected_min:,} — pipeline blocked"
        )
    print(f"[QC] {table}: volume OK ({count:,} rows)")
