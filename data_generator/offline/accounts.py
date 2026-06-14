import numpy as np
import pandas as pd
from datetime import datetime, timedelta

from data_generator.config import Config


def generate(config: Config, rng: np.random.Generator, customers_df: pd.DataFrame) -> pd.DataFrame:
    n = config.n_accounts
    customer_ids = customers_df["customer_id"].values

    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=config.days_history + 365)
    opened_offsets = rng.integers(0, (end_date - start_date).days, size=n)
    opened_dates = [start_date + timedelta(days=int(d)) for d in opened_offsets]

    # Balance in VND: log-normal, median ~270k, clipped to realistic range
    balances = np.round(rng.lognormal(mean=14.0, sigma=1.8, size=n), -3)
    balances = np.clip(balances, 0, 500_000_000)

    return pd.DataFrame({
        "account_id":   [f"ACCT-{i:06d}" for i in range(1, n + 1)],
        "customer_id":  rng.choice(customer_ids, size=n, replace=True).tolist(),
        "account_type": rng.choice(["savings", "checking", "credit"], size=n, p=[0.45, 0.40, 0.15]).tolist(),
        "balance":      balances.tolist(),
        "currency":     ["VND"] * n,
        "status":       rng.choice(["active", "frozen", "closed"], size=n, p=[0.90, 0.07, 0.03]).tolist(),
        "opened_ts":    [d.isoformat() for d in opened_dates],
        "created_ts":   [d.isoformat() for d in opened_dates],
    })
