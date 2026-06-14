import argparse
from pathlib import Path

import numpy as np
import pandas as pd
from faker import Faker

from data_generator.config import load_config
from data_generator.offline import customers, accounts, cards, merchants, transactions, fraud_labels


def save_parquet(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False, engine="pyarrow")


def run(config_path: str = "data_generator/config.yaml") -> dict:
    config = load_config(config_path)
    rng = np.random.default_rng(config.random_seed)
    fake = Faker()
    Faker.seed(config.random_seed)

    base = Path(config.output_dir) / "offline"

    print("Generating customers...")
    customers_df = customers.generate(config, rng, fake)
    save_parquet(customers_df, base / "customers" / "customers.parquet")
    print(f"  {len(customers_df):,} rows")

    print("Generating accounts...")
    accounts_df = accounts.generate(config, rng, customers_df)
    save_parquet(accounts_df, base / "accounts" / "accounts.parquet")
    print(f"  {len(accounts_df):,} rows")

    print("Generating cards...")
    cards_df = cards.generate(config, rng, accounts_df)
    save_parquet(cards_df, base / "cards" / "cards.parquet")
    print(f"  {len(cards_df):,} rows")

    print("Generating merchants...")
    merchants_df = merchants.generate(config, rng, fake)
    save_parquet(merchants_df, base / "merchants" / "merchants.parquet")
    print(f"  {len(merchants_df):,} rows")

    print("Generating transactions (this may take a moment)...")
    transactions_df = transactions.generate(config, rng, accounts_df, cards_df, merchants_df)
    transactions_df.to_parquet(
        base / "transactions",
        partition_cols=["transaction_date"],
        index=False,
        engine="pyarrow",
    )
    print(f"  {len(transactions_df):,} rows  (partitioned by transaction_date)")

    print("Generating fraud labels...")
    fraud_labels_df = fraud_labels.generate(config, rng, transactions_df)
    save_parquet(fraud_labels_df, base / "fraud_labels" / "fraud_labels.parquet")
    actual_rate = len(fraud_labels_df) / len(transactions_df) * 100
    print(f"  {len(fraud_labels_df):,} rows  ({actual_rate:.2f}% fraud rate)")

    print("\nOffline generation complete.")
    return {
        "customers": customers_df,
        "accounts": accounts_df,
        "cards": cards_df,
        "merchants": merchants_df,
        "transactions": transactions_df,
        "fraud_labels": fraud_labels_df,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate offline banking data")
    parser.add_argument("--config", default="data_generator/config.yaml")
    args = parser.parse_args()
    run(args.config)
