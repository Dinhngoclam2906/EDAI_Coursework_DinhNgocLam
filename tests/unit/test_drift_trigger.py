"""
Unit tests for pipelines/gold/drift_trigger.py — the _write_request function.

Why these tests matter:
  data/retraining_requests.jsonl is the contract between the data engineering
  layer (Sections 01-03) and the ML training pipeline (Section 04.1). If the
  format is wrong — missing fields, wrong status value, not valid JSON — the
  ML pipeline will fail when it tries to consume these records.

  These tests verify the file I/O behaviour and the record format, ensuring
  the contract is upheld regardless of what Spark reads upstream.
"""

import json
from pathlib import Path

import pytest

from pipelines.gold.drift_trigger import _write_request

REQUIRED_FIELDS = {
    "request_id",
    "triggered_at",
    "trigger_type",
    "drift_type",
    "feature_name",
    "alert_date",
    "psi_value",
    "recommended_action",
    "status",
}


def make_request(drift_type: str = "concept", status: str = "pending") -> dict:
    return {
        "request_id":         "test-uuid-1234",
        "triggered_at":       "2026-05-28T05:34:09+00:00",
        "trigger_type":       "drift_alert",
        "drift_type":         drift_type,
        "feature_name":       "f_fraud_rate",
        "alert_date":         "2026-04-18",
        "psi_value":          0.05,
        "recommended_action": "Investigate new fraud pattern; consider model retraining.",
        "status":             status,
    }


class TestWriteRequest:

    def test_creates_file_on_first_write(self, tmp_path, monkeypatch):
        """File must be created when it doesn't exist yet."""
        output_file = tmp_path / "retraining_requests.jsonl"
        monkeypatch.setattr("pipelines.gold.drift_trigger.REQUESTS_FILE", output_file)
        _write_request(make_request())
        assert output_file.exists()

    def test_appends_on_subsequent_writes(self, tmp_path, monkeypatch):
        """Each drift event is a separate line — must not overwrite previous requests."""
        output_file = tmp_path / "retraining_requests.jsonl"
        monkeypatch.setattr("pipelines.gold.drift_trigger.REQUESTS_FILE", output_file)
        _write_request(make_request())
        _write_request(make_request())
        _write_request(make_request())
        lines = [l for l in output_file.read_text(encoding="utf-8").splitlines() if l.strip()]
        assert len(lines) == 3

    def test_each_line_is_valid_json(self, tmp_path, monkeypatch):
        """ML pipeline reads this file line-by-line; each line must parse."""
        output_file = tmp_path / "retraining_requests.jsonl"
        monkeypatch.setattr("pipelines.gold.drift_trigger.REQUESTS_FILE", output_file)
        _write_request(make_request())
        for line in output_file.read_text(encoding="utf-8").splitlines():
            if line.strip():
                json.loads(line)  # raises if not valid JSON

    def test_written_record_contains_all_required_fields(self, tmp_path, monkeypatch):
        """Section 04.1 ML pipeline expects these fields — all must be present."""
        output_file = tmp_path / "retraining_requests.jsonl"
        monkeypatch.setattr("pipelines.gold.drift_trigger.REQUESTS_FILE", output_file)
        _write_request(make_request())
        record = json.loads(output_file.read_text(encoding="utf-8").strip())
        assert REQUIRED_FIELDS.issubset(record.keys()), (
            f"Missing fields: {REQUIRED_FIELDS - record.keys()}"
        )

    def test_concept_drift_produces_pending_status(self, tmp_path, monkeypatch):
        """
        Concept drift (fraud pattern changed) must have status=pending so the
        ML pipeline picks it up for retraining. monitor status would be ignored.
        """
        output_file = tmp_path / "retraining_requests.jsonl"
        monkeypatch.setattr("pipelines.gold.drift_trigger.REQUESTS_FILE", output_file)
        _write_request(make_request(drift_type="concept", status="pending"))
        record = json.loads(output_file.read_text(encoding="utf-8").strip())
        assert record["status"] == "pending"
        assert record["drift_type"] == "concept"

    def test_covariate_drift_produces_monitor_status(self, tmp_path, monkeypatch):
        """
        Covariate drift (amount distribution shifted) is lower priority —
        status=monitor means the ML pipeline logs it but does not retrain.
        """
        output_file = tmp_path / "retraining_requests.jsonl"
        monkeypatch.setattr("pipelines.gold.drift_trigger.REQUESTS_FILE", output_file)
        _write_request(make_request(drift_type="covariate", status="monitor"))
        record = json.loads(output_file.read_text(encoding="utf-8").strip())
        assert record["status"] == "monitor"
        assert record["drift_type"] == "covariate"
