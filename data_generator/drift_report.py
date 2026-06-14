"""
Drift validation report generator.

Reads generated transaction + fraud_label parquet data,
computes daily feature statistics and PSI vs pre-drift baseline,
writes data/drift_validation_report.csv.

Usage:
    uv run python -m data_generator.drift_report
"""

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

from data_generator.config import load_config

PSI_ALERT_THRESHOLD = 0.04  # calibrated for binary/proportion features — matches monitoring.py
N_BINS = 10


def _psi(baseline: np.ndarray, current: np.ndarray, n_bins: int = N_BINS) -> float:
    """Population Stability Index between baseline and current distributions."""
    min_val = min(baseline.min(), current.min())
    max_val = max(baseline.max(), current.max()) + 1e-9
    bins = np.linspace(min_val, max_val, n_bins + 1)

    base_pct = np.histogram(baseline, bins=bins)[0] / max(len(baseline), 1)
    curr_pct = np.histogram(current,  bins=bins)[0] / max(len(current),  1)

    # Avoid log(0)
    base_pct = np.where(base_pct == 0, 1e-6, base_pct)
    curr_pct = np.where(curr_pct == 0, 1e-6, curr_pct)

    psi = np.sum((curr_pct - base_pct) * np.log(curr_pct / base_pct))
    return float(round(psi, 4))


def generate_report(
    data_dir: str = "data",
    config_path: str = "data_generator/config.yaml",
) -> pd.DataFrame:
    config  = load_config(config_path)
    base    = Path(data_dir) / "offline"

    tx = pd.read_parquet(base / "transactions")
    tx["event_timestamp"] = pd.to_datetime(tx["event_timestamp"])
    tx["event_date"]      = tx["event_timestamp"].dt.date

    fl = pd.read_parquet(base / "fraud_labels")
    tx = tx.merge(fl[["transaction_id", "fraud_type"]].rename(columns={"fraud_type": "confirmed_fraud_type"}),
                  on="transaction_id", how="left")

    drift_start = pd.to_datetime(config.drift_start_date).date()

    # Baseline: 30 days before drift_start
    baseline_end   = drift_start
    baseline_start = pd.Timestamp(drift_start) - pd.Timedelta(days=30)
    baseline_tx    = tx[(tx["event_date"] >= baseline_start.date()) & (tx["event_date"] < baseline_end)]

    features_to_track = {
        "amount":    "f_avg_amount_30d",
        "is_fraud":  "f_fraud_rate",
    }

    rows = []
    for event_date, daily_tx in tx.groupby("event_date"):
        for raw_col, feat_name in features_to_track.items():
            daily_vals    = daily_tx[raw_col].astype(float).dropna().values
            baseline_vals = baseline_tx[raw_col].astype(float).dropna().values

            if len(daily_vals) == 0 or len(baseline_vals) == 0:
                continue

            mean_val = float(np.mean(daily_vals))
            std_val  = float(np.std(daily_vals))
            psi_val  = _psi(baseline_vals, daily_vals)

            period = "pre_drift" if event_date < drift_start else "post_drift"
            alert  = psi_val > PSI_ALERT_THRESHOLD and period == "post_drift"

            rows.append({
                "date":           event_date.isoformat(),
                "feature_name":   feat_name,
                "mean":           round(mean_val, 4),
                "stddev":         round(std_val, 4),
                "psi_vs_baseline": psi_val,
                "period":         period,
                "drift_status":   "alert" if psi_val > PSI_ALERT_THRESHOLD
                                  else ("detected" if psi_val > 0.10
                                  else ("baseline" if period == "pre_drift" else "stable")),
            })

    report_df = pd.DataFrame(rows).sort_values(["feature_name", "date"])

    out_path = Path(data_dir) / "drift_validation_report.csv"
    report_df.to_csv(out_path, index=False)
    print(f"Drift report saved -> {out_path}  ({len(report_df)} rows)")

    # Summary
    alerts = report_df[report_df["drift_status"] == "alert"]
    print(f"\n{'='*50}")
    print(f"Drift alerts triggered: {len(alerts)}")
    if not alerts.empty:
        print(alerts[["date", "feature_name", "psi_vs_baseline"]].to_string(index=False))

    return report_df


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", default="data")
    parser.add_argument("--config",   default="data_generator/config.yaml")
    args = parser.parse_args()
    generate_report(args.data_dir, args.config)
