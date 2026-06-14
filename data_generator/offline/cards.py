import numpy as np
import pandas as pd
from datetime import datetime, timedelta

from data_generator.config import Config


def generate(config: Config, rng: np.random.Generator, accounts_df: pd.DataFrame) -> pd.DataFrame:
    n = config.n_cards
    account_ids = accounts_df["account_id"].values

    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=config.days_history + 365)
    issued_offsets = rng.integers(0, (end_date - start_date).days, size=n)
    issued_dates = [start_date + timedelta(days=int(d)) for d in issued_offsets]

    # Expiry: 2–5 years after issue
    expiry_offsets = rng.integers(730, 1825, size=n)
    expiry_dates = [issued_dates[i] + timedelta(days=int(expiry_offsets[i])) for i in range(n)]

    last_four = rng.integers(1000, 9999, size=n)

    return pd.DataFrame({
        "card_id":            [f"CARD-{i:06d}" for i in range(1, n + 1)],
        "account_id":         rng.choice(account_ids, size=n, replace=True).tolist(),
        "card_type":          rng.choice(["debit", "credit"], size=n, p=[0.65, 0.35]).tolist(),
        "masked_card_number": [f"****-****-****-{last_four[i]}" for i in range(n)],
        "expiry_date":        [d.strftime("%m/%Y") for d in expiry_dates],
        "is_active":          rng.choice([True, False], size=n, p=[0.92, 0.08]).tolist(),
        "issued_ts":          [d.isoformat() for d in issued_dates],
        "created_ts":         [d.isoformat() for d in issued_dates],
    })
