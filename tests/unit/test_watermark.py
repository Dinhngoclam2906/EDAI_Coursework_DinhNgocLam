"""
Unit tests for pipelines/common/watermark.py

Why these tests matter:
  Watermarks are the only mechanism preventing data from being reprocessed or
  silently dropped on every pipeline run. A broken watermark means either
  reprocessing 500k rows every 30 minutes, or missing late-arriving fraud labels.
  The BOM encoding bug (fixed in this codebase) is a concrete example of the
  kind of failure these tests guard against.
"""

import json

import pytest

import pipelines.common.watermark as wm_module
from pipelines.common.watermark import get_watermark, set_watermark


class TestGetWatermark:
    """Reading watermark values from disk."""

    def test_returns_default_when_no_file_exists(self, tmp_path, monkeypatch):
        """New pipeline with no history should start from epoch."""
        monkeypatch.setattr(wm_module, "WATERMARK_DIR", tmp_path)
        assert get_watermark("brand_new_pipeline") == "1970-01-01T00:00:00"

    def test_returns_custom_default_when_no_file_exists(self, tmp_path, monkeypatch):
        """Pipelines that default to a specific backfill date."""
        monkeypatch.setattr(wm_module, "WATERMARK_DIR", tmp_path)
        assert get_watermark("pipeline", default="2026-01-01") == "2026-01-01"

    def test_reads_stored_timestamp(self, tmp_path, monkeypatch):
        """Normal read after a previous successful run."""
        monkeypatch.setattr(wm_module, "WATERMARK_DIR", tmp_path)
        (tmp_path / "silver.stg_transactions.json").write_text(
            '{"last_processed": "2026-05-27 10:04:58", "updated_at": "2026-05-27T10:05:00+00:00"}',
            encoding="utf-8",
        )
        assert get_watermark("silver.stg_transactions") == "2026-05-27 10:04:58"

    def test_handles_utf8_bom_file(self, tmp_path, monkeypatch):
        """
        Windows tools (PowerShell Set-Content) write UTF-8 BOM by default.
        json.loads() raises JSONDecodeError on BOM without utf-8-sig encoding.
        This test guards against that regression.
        """
        monkeypatch.setattr(wm_module, "WATERMARK_DIR", tmp_path)
        content = '{"last_processed": "2026-03-01", "updated_at": "..."}'
        (tmp_path / "gold.fact_transaction.json").write_bytes(
            b"\xef\xbb\xbf" + content.encode("utf-8")
        )
        assert get_watermark("gold.fact_transaction") == "2026-03-01"


class TestSetWatermark:
    """Writing watermark values to disk."""

    def test_creates_file_with_correct_value(self, tmp_path, monkeypatch):
        """Basic write — value must be readable back."""
        monkeypatch.setattr(wm_module, "WATERMARK_DIR", tmp_path)
        set_watermark("gold.fact_transaction", "2026-05-28 11:00:00")
        data = json.loads((tmp_path / "gold.fact_transaction.json").read_text(encoding="utf-8"))
        assert data["last_processed"] == "2026-05-28 11:00:00"

    def test_creates_parent_directory_if_missing(self, tmp_path, monkeypatch):
        """Pipelines run before the watermarks dir is created."""
        nested = tmp_path / "data" / "watermarks"
        monkeypatch.setattr(wm_module, "WATERMARK_DIR", nested)
        set_watermark("test", "2026-01-01")
        assert (nested / "test.json").exists()

    def test_written_file_has_no_bom(self, tmp_path, monkeypatch):
        """Files written by set_watermark must be plain UTF-8, not UTF-8 BOM."""
        monkeypatch.setattr(wm_module, "WATERMARK_DIR", tmp_path)
        set_watermark("pipeline", "2026-01-01")
        raw_bytes = (tmp_path / "pipeline.json").read_bytes()
        assert not raw_bytes.startswith(b"\xef\xbb\xbf"), "BOM found — will break json.loads on cp1252 systems"

    def test_roundtrip_preserves_value(self, tmp_path, monkeypatch):
        """set then get must return the exact same string."""
        monkeypatch.setattr(wm_module, "WATERMARK_DIR", tmp_path)
        ts = "2026-05-28 11:34:46.683834"
        set_watermark("pipeline_a", ts)
        assert get_watermark("pipeline_a") == ts

    def test_overwrites_previous_watermark(self, tmp_path, monkeypatch):
        """Each successful run advances the watermark — old value must be replaced."""
        monkeypatch.setattr(wm_module, "WATERMARK_DIR", tmp_path)
        set_watermark("pipeline_a", "2026-01-01")
        set_watermark("pipeline_a", "2026-05-28")
        assert get_watermark("pipeline_a") == "2026-05-28"
