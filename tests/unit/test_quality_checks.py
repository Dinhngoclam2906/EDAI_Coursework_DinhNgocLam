"""
Unit tests for pipelines/common/quality_checks.py

Why these tests matter:
  Quality gates are the only automated defence between raw source data and the
  Gold tables used for ML training. A bug in check_fraud_rate (e.g. wrong
  threshold direction) could allow a label-pipeline failure to go undetected,
  producing a fraud model trained on 0% fraud — silent, catastrophic.

  All checks raise DataQualityError on failure (hard circuit breaker). These
  tests verify both the raise conditions and the pass conditions, including
  domain-specific thresholds (VND amount range, 0.1–10% fraud rate).

DataFrames are mocked via the make_df() factory in conftest.py — Spark is not
started for any of these tests.
"""

import json
from unittest.mock import MagicMock

import pytest

import pipelines.common.quality_checks as qc_module
from pipelines.common.quality_checks import (
    DataQualityError,
    check_amount_range,
    check_fraud_rate,
    check_no_nulls,
    check_not_empty,
    check_unique,
    check_volume_drop,
)
from tests.conftest import make_df


class _ColExpr:
    """
    Minimal stand-in for a PySpark Column expression.

    Cannot subclass MagicMock here: MagicMock's metaclass re-sets __lt__,
    __gt__, etc. on every subclass to return NotImplemented (see
    unittest.mock._return_values), which raises TypeError in check_amount_range.
    A plain class keeps our operator overrides intact.
    """
    def __lt__(self, other): return _ColExpr()
    def __gt__(self, other): return _ColExpr()
    def __le__(self, other): return _ColExpr()
    def __ge__(self, other): return _ColExpr()
    def __eq__(self, other): return _ColExpr()   # F.col(...) == 1
    def __hash__(self): return id(self)
    def __or__(self, other): return _ColExpr()
    def __and__(self, other): return _ColExpr()
    def __getattr__(self, name):                  # .isNull(), .isin(), etc.
        return lambda *a, **kw: _ColExpr()


@pytest.fixture(autouse=True)
def patch_spark_functions(monkeypatch):
    """
    Patch pyspark.sql.functions (F) at the quality_checks module level.
    F.col() in PySpark 4.x asserts an active SparkContext — even when the
    DataFrame itself is mocked. _ColExpr intercepts column expressions so
    df.filter(col_expr) resolves via the pre-configured mock return value.
    """
    mock_F = MagicMock()
    mock_F.col.side_effect = lambda name: _ColExpr()
    monkeypatch.setattr("pipelines.common.quality_checks.F", mock_F)


# ── check_not_empty ───────────────────────────────────────────────────────────

class TestCheckNotEmpty:

    def test_raises_on_empty_dataframe(self):
        """Empty table must block the pipeline — never write zero rows to Gold."""
        df = make_df(total=0)
        with pytest.raises(DataQualityError, match="table is empty"):
            check_not_empty(df, "fact_transaction")

    def test_passes_on_non_empty_dataframe(self):
        """Normal case — 500k rows should not raise."""
        check_not_empty(make_df(total=500_000), "fact_transaction")


# ── check_no_nulls ────────────────────────────────────────────────────────────

class TestCheckNoNulls:

    def test_raises_when_critical_column_has_nulls(self):
        """NULL transaction_id breaks all downstream joins — must block."""
        df = make_df(total=1000, filter_count=5)
        with pytest.raises(DataQualityError, match="unexpected NULLs"):
            check_no_nulls(df, "fact_transaction", ["transaction_id"])

    def test_passes_when_no_nulls_present(self):
        """No NULLs — all critical columns clean."""
        df = make_df(total=1000, filter_count=0)
        check_no_nulls(df, "fact_transaction", ["transaction_id", "amount"])


# ── check_unique ──────────────────────────────────────────────────────────────

class TestCheckUnique:

    def test_raises_when_duplicate_keys_exist(self):
        """Duplicate transaction_id in Gold breaks fact table grain."""
        df = make_df(total=1000, distinct_count=995)
        with pytest.raises(DataQualityError, match="duplicate keys"):
            check_unique(df, "stg_transactions", "transaction_id")

    def test_passes_when_all_keys_unique(self):
        """After dedup, all 1000 rows should be distinct."""
        df = make_df(total=1000, distinct_count=1000)
        check_unique(df, "stg_transactions", "transaction_id")


# ── check_amount_range ────────────────────────────────────────────────────────

class TestCheckAmountRange:
    """
    Domain rule: Vietnamese banking transactions must be between
    5,000 VND (minimum ATM withdrawal) and 50,000,000 VND.
    Violation rate above 5% signals a systemic data problem.
    """

    def test_raises_when_violation_rate_exceeds_threshold(self):
        """6% of rows out of range — pipeline blocked."""
        df = make_df(total=1000, filter_count=60)   # 6% > 5% threshold
        with pytest.raises(DataQualityError, match="out of range"):
            check_amount_range(df, "stg_transactions")

    def test_passes_when_violations_within_tolerance(self):
        """3% violations — within the 5% tolerance, pipeline continues."""
        df = make_df(total=1000, filter_count=30)   # 3% < 5%
        check_amount_range(df, "stg_transactions")

    def test_passes_when_no_violations(self):
        """All amounts in range — expected normal state."""
        df = make_df(total=1000, filter_count=0)
        check_amount_range(df, "stg_transactions")


# ── check_volume_drop ─────────────────────────────────────────────────────────

class TestCheckVolumeDrop:
    """
    Compares current batch size against prior run stored in volume_history/.
    A >50% drop signals upstream data loss (source pipeline failure, partition
    deletion, CDC gap). First run has no history — should always pass.
    """

    def test_passes_on_first_run_no_history(self, tmp_path, monkeypatch):
        """No prior run recorded — cannot compute a drop. Should pass silently."""
        monkeypatch.setattr(qc_module, "VOLUME_HISTORY_DIR", tmp_path)
        check_volume_drop(make_df(total=500_000), "fact_transaction")

    def test_passes_when_volume_is_stable(self, tmp_path, monkeypatch):
        """Prior: 1000, current: 950 — 5% drop, well within 50% threshold."""
        monkeypatch.setattr(qc_module, "VOLUME_HISTORY_DIR", tmp_path)
        (tmp_path / "fact_transaction.json").write_text(
            json.dumps({"row_count": 1000}), encoding="utf-8"
        )
        check_volume_drop(make_df(total=950), "fact_transaction")

    def test_raises_when_volume_drops_severely(self, tmp_path, monkeypatch):
        """Prior: 1000, current: 400 — 60% drop signals upstream failure."""
        monkeypatch.setattr(qc_module, "VOLUME_HISTORY_DIR", tmp_path)
        (tmp_path / "fact_transaction.json").write_text(
            json.dumps({"row_count": 1000}), encoding="utf-8"
        )
        with pytest.raises(DataQualityError, match="row count dropped"):
            check_volume_drop(make_df(total=400), "fact_transaction")

    def test_records_current_count_for_next_run(self, tmp_path, monkeypatch):
        """After a successful run, current count must be persisted for the next check."""
        monkeypatch.setattr(qc_module, "VOLUME_HISTORY_DIR", tmp_path)
        check_volume_drop(make_df(total=12_000), "fact_fraud_label")
        saved = json.loads((tmp_path / "fact_fraud_label.json").read_text(encoding="utf-8"))
        assert saved["row_count"] == 12_000


# ── check_fraud_rate ──────────────────────────────────────────────────────────

class TestCheckFraudRate:
    """
    Most critical quality gate for a fraud detection platform.

    The expected fraud rate is 0.1% – 10%:
      - Below 0.1%: label pipeline has likely failed — model trains on 0% fraud
                    and learns to predict everything as legitimate. Silent failure.
      - Above 10%:  data contamination — fraudulent rows from another source
                    have been mixed in, inflating the fraud signal.

    Both conditions must hard-block the pipeline.
    """

    def test_raises_when_fraud_rate_is_zero(self):
        """
        0% fraud rate means all fraud labels are missing — the label pipeline
        has failed. Must block immediately; training on this data is catastrophic.
        """
        df = make_df(total=100_000, filter_count=0)
        with pytest.raises(DataQualityError, match="below minimum"):
            check_fraud_rate(df, "fact_transaction")

    def test_raises_when_fraud_rate_below_minimum(self):
        """0.05% fraud — just below the 0.1% minimum threshold."""
        df = make_df(total=100_000, filter_count=50)   # 0.05%
        with pytest.raises(DataQualityError, match="below minimum"):
            check_fraud_rate(df, "fact_transaction")

    def test_raises_when_fraud_rate_exceeds_maximum(self):
        """15% fraud rate — far above the 10% contamination threshold."""
        df = make_df(total=100_000, filter_count=15_000)   # 15%
        with pytest.raises(DataQualityError, match="exceeds maximum"):
            check_fraud_rate(df, "fact_transaction")

    def test_passes_for_normal_fraud_rate(self):
        """1.5% fraud — the expected rate from the data generator config."""
        df = make_df(total=100_000, filter_count=1_500)   # 1.5%
        check_fraud_rate(df, "fact_transaction")

    def test_passes_when_dataframe_is_empty(self):
        """
        Empty batch (e.g. 0 new Silver rows) should not raise — the not_empty
        check handles this case upstream. Fraud rate is undefined on 0 rows.
        """
        df = make_df(total=0, filter_count=0)
        check_fraud_rate(df, "fact_transaction")

    @pytest.mark.parametrize("fraud_count,total,should_raise", [
        (0,       100_000, True),    # 0.000% — label failure
        (99,      100_000, True),    # 0.099% — just below minimum
        (100,     100_000, False),   # 0.100% — exactly at minimum, passes
        (1_500,   100_000, False),   # 1.500% — normal expected rate
        (10_000,  100_000, False),   # 10.00% — exactly at maximum, passes
        (10_001,  100_000, True),    # 10.001% — just above maximum
        (15_000,  100_000, True),    # 15.00% — contamination
    ])
    def test_boundary_thresholds(self, fraud_count, total, should_raise):
        """Boundary sweep — verifies exact threshold edges are handled correctly."""
        df = make_df(total=total, filter_count=fraud_count)
        if should_raise:
            with pytest.raises(DataQualityError):
                check_fraud_rate(df, "fact_transaction")
        else:
            check_fraud_rate(df, "fact_transaction")
