"""
Generates a markdown quality report from the offline generated data.

Usage:
  uv run python -m data_generator.quality_report
"""

import argparse
from pathlib import Path

import pandas as pd

from data_generator.config import load_config


def generate_report(
    data_dir: str = "data",
    config_path: str = "data_generator/config.yaml",
) -> str:
    config = load_config(config_path)
    base = Path(data_dir) / "offline"

    customers    = pd.read_parquet(base / "customers")
    accounts     = pd.read_parquet(base / "accounts")
    cards        = pd.read_parquet(base / "cards")
    merchants    = pd.read_parquet(base / "merchants")
    transactions = pd.read_parquet(base / "transactions")
    fraud_labels = pd.read_parquet(base / "fraud_labels")

    lines: list[str] = ["# FinGuard — Data Quality Report\n"]

    # 1. Row counts
    lines += [
        "## 1. Row Counts\n",
        "| Table | Rows |",
        "|---|---|",
    ]
    for name, df in [
        ("customers", customers), ("accounts", accounts), ("cards", cards),
        ("merchants", merchants), ("transactions", transactions), ("fraud_labels", fraud_labels),
    ]:
        lines.append(f"| {name} | {len(df):,} |")
    lines.append("")

    # 2. Geographic skew
    lines += ["## 2. Geographic Skew (merchant city)\n"]
    merged = transactions.merge(merchants[["merchant_id", "city"]].rename(columns={"city": "merchant_city"}), on="merchant_id", how="left")
    city_dist = merged["merchant_city"].value_counts(normalize=True).head(10)
    lines += ["| City | % Transactions |", "|---|---|"]
    for city, pct in city_dist.items():
        lines.append(f"| {city} | {pct * 100:.1f}% |")
    lines.append("")

    # 3. Fraud rate
    lines += ["## 3. Fraud Rate\n"]
    approved = transactions[transactions["status"] == "approved"]
    actual_rate = len(fraud_labels) / len(approved) if len(approved) else 0
    lines += [
        f"- Configured rate : {config.fraud_rate * 100:.1f}%",
        f"- Actual rate     : {actual_rate * 100:.2f}%",
        f"- Fraud cases     : {len(fraud_labels):,}",
        "",
    ]

    # 4. Cardinality
    lines += [
        "## 4. Cardinality\n",
        "| Column | Unique Values |",
        "|---|---|",
        f"| transaction_id | {transactions['transaction_id'].nunique():,} |",
        f"| account_id     | {transactions['account_id'].nunique():,} |",
        f"| card_id        | {transactions['card_id'].nunique():,} |",
        f"| merchant_id    | {transactions['merchant_id'].nunique():,} |",
        "",
    ]

    # 5. Schema evolution
    lines += ["## 5. Schema Evolution — device_fingerprint\n"]
    null_rate = transactions["device_fingerprint"].isna().mean()
    lines += [
        f"- Null rate : {null_rate * 100:.1f}%",
        f"- Expected  : transactions before {config.schema_change_date} have NULL fingerprint",
        "",
    ]

    # 6. Duplicates
    lines += ["## 6. Offline Duplicates\n"]
    total_rows  = len(transactions)
    unique_rows = transactions["transaction_id"].nunique()
    dup_rate    = (total_rows - unique_rows) / total_rows
    lines += [
        f"- Total rows           : {total_rows:,}",
        f"- Unique transaction_id: {unique_rows:,}",
        f"- Duplicate rate       : {dup_rate * 100:.2f}%  (configured: {config.duplicate_rate_offline * 100:.1f}%)",
        "",
    ]

    # 7. Fraud type distribution
    lines += [
        "## 7. Fraud Type Distribution\n",
        "| Fraud Type | Count | % |",
        "|---|---|---|",
    ]
    for ftype, cnt in fraud_labels["fraud_type"].value_counts().items():
        lines.append(f"| {ftype} | {cnt:,} | {cnt / len(fraud_labels) * 100:.1f}% |")
    lines.append("")

    report = "\n".join(lines)

    out = Path(data_dir) / "quality_report.md"
    out.write_text(report)
    print(f"Report saved -> {out}")
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate data quality report")
    parser.add_argument("--data-dir", default="data")
    parser.add_argument("--config", default="data_generator/config.yaml")
    args = parser.parse_args()
    print(generate_report(args.data_dir, args.config))
