"""
Gold — dim_date.

Static date dimension covering the full data range.
Pre-computes calendar attributes including is_salary_day (1st and 15th of month).

Usage:
    uv run python -m pipelines.gold.dim_date
"""

from datetime import date, timedelta
from pathlib import Path

import pandas as pd
from pyspark.sql import functions as F

from pipelines.common.lineage import publish_lineage
from pipelines.common.run_metadata import RunLogger
from pipelines.common.spark_session import get_spark

GOLD_PATH = "data/lakehouse/gold/dim_date"
START_DATE = date(2024, 1, 1)
END_DATE   = date(2027, 12, 31)

DAY_NAMES = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]


def run() -> None:
    # dim_date is static — skip rebuild if it already exists.
    if Path(GOLD_PATH).exists():
        print(f"[GOLD] dim_date: already built, skipping")
        return

    spark = get_spark("finguard.gold.dim_date")
    logger = RunLogger("gold.dim_date")

    try:
        records = []
        current = START_DATE
        while current <= END_DATE:
            records.append({
                "date_key":      int(current.strftime("%Y%m%d")),
                "calendar_date": current.isoformat(),
                "day_of_week":   current.isoweekday(),
                "day_name":      DAY_NAMES[current.weekday()],
                "month":         current.month,
                "quarter":       (current.month - 1) // 3 + 1,
                "year":          current.year,
                "is_weekend":    current.isoweekday() >= 6,
                "is_salary_day": current.day in (1, 15),
            })
            current += timedelta(days=1)

        df = spark.createDataFrame(pd.DataFrame(records))

        df.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .save(GOLD_PATH)

        print(f"[GOLD] dim_date: {df.count():,} rows")
        publish_lineage("gold_dim_date", [], ["gold/dim_date"])
        logger.finish("success", 0, df.count())

    except Exception as exc:
        logger.finish("failed", error_msg=str(exc))
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    run()
