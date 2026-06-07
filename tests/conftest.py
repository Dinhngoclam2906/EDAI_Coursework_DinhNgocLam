"""
Shared pytest fixtures for FinGuard test suite.
"""

from unittest.mock import MagicMock

import pytest
from pyspark.sql import DataFrame


def make_df(total: int, filter_count: int = 0, distinct_count: int = None) -> MagicMock:
    """
    Factory for a mock Spark DataFrame.

    Args:
        total:          return value of df.count()
        filter_count:   return value of df.filter(...).count()  — used by null/range/fraud checks
        distinct_count: return value of df.select(...).distinct().count() — used by uniqueness check;
                        defaults to total (no duplicates)
    """
    df = MagicMock(spec=DataFrame)
    df.count.return_value = total
    df.filter.return_value.count.return_value = filter_count
    df.select.return_value.distinct.return_value.count.return_value = (
        distinct_count if distinct_count is not None else total
    )
    return df
