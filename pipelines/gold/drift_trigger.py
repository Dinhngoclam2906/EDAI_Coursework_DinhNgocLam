"""
Drift trigger — reads feature_drift_alerts and emits retraining requests.

Logic:
  - Reads feature_drift_alerts Delta table
  - Compares alert_dates against last-processed watermark
  - New concept drift alerts  -> write retraining request (status=pending)
  - New covariate drift alerts -> write monitoring recommendation (status=monitor)
  - Updates watermark so the same alert is never processed twice

Retraining requests land in data/retraining_requests.jsonl — a file queue
the future ML pipeline (Section 04.1) can consume.

Usage:
    uv run python -m pipelines.gold.drift_trigger
"""

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import functions as F

from pipelines.common.spark_session import get_spark
from pipelines.common.watermark import get_watermark, set_watermark

GOLD_BASE          = "data/lakehouse/gold"
REQUESTS_FILE      = Path("data/retraining_requests.jsonl")
WATERMARK_KEY      = "drift_trigger.last_alert_date"


def _write_request(record: dict) -> None:
    REQUESTS_FILE.parent.mkdir(parents=True, exist_ok=True)
    with REQUESTS_FILE.open("a") as f:
        f.write(json.dumps(record) + "\n")


def run() -> None:
    spark = get_spark("finguard.gold.drift_trigger")

    try:
        alerts_path = f"{GOLD_BASE}/feature_drift_alerts"

        from delta.tables import DeltaTable
        if not DeltaTable.isDeltaTable(spark, alerts_path):
            print("[DRIFT] feature_drift_alerts table not found — run monitoring pipeline first")
            return

        last_date = get_watermark(WATERMARK_KEY, default="1970-01-01")

        new_alerts = (
            spark.read.format("delta").load(alerts_path)
            .filter(F.col("alert_date") > last_date)
            .orderBy("alert_date", "feature_name")
        )

        rows = new_alerts.collect()

        if not rows:
            print(f"[DRIFT] No new alerts since {last_date} — no action needed")
            return

        now_iso      = datetime.now(timezone.utc).isoformat()
        max_date     = max(str(r["alert_date"]) for r in rows)
        concept_n    = sum(1 for r in rows if r["drift_type"] == "concept")
        covariate_n  = sum(1 for r in rows if r["drift_type"] == "covariate")

        print(f"\n[DRIFT] {'='*55}")
        print(f"[DRIFT] {len(rows)} new alert(s) since {last_date}")
        print(f"[DRIFT]   concept drift:   {concept_n} feature(s)  -> RETRAIN")
        print(f"[DRIFT]   covariate drift: {covariate_n} feature(s)  -> MONITOR")
        print(f"[DRIFT] {'='*55}")

        for row in rows:
            alert_date  = str(row["alert_date"])
            feature     = row["feature_name"]
            psi         = float(row["psi_value"])
            drift_type  = row["drift_type"]
            action      = row["recommended_action"]

            if drift_type == "concept":
                status = "pending"
                label  = "RETRAIN  "
            else:
                status = "monitor"
                label  = "MONITOR  "

            print(f"[DRIFT]   [{label}] {alert_date}  {feature:<25s}  PSI={psi:.3f}  ({drift_type})")
            print(f"[DRIFT]            -> {action}")

            record = {
                "request_id":         str(uuid.uuid4()),
                "triggered_at":       now_iso,
                "trigger_type":       "drift_alert",
                "drift_type":         drift_type,
                "feature_name":       feature,
                "alert_date":         alert_date,
                "psi_value":          round(psi, 4),
                "recommended_action": action,
                "status":             status,
            }
            _write_request(record)

        set_watermark(WATERMARK_KEY, max_date)

        if concept_n > 0:
            print(f"\n[DRIFT] {concept_n} retraining request(s) written to {REQUESTS_FILE}")
            print(f"[DRIFT] -> Concept drift detected: model recall will degrade without retraining")
        if covariate_n > 0:
            print(f"[DRIFT] {covariate_n} monitoring recommendation(s) written to {REQUESTS_FILE}")
            print(f"[DRIFT] -> Covariate drift detected: feature z-scores may be miscalibrated")

        print(f"[DRIFT] Watermark updated to {max_date}\n")

    finally:
        spark.stop()


if __name__ == "__main__":
    run()
