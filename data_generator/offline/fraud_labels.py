import numpy as np
import pandas as pd
from datetime import datetime, timedelta  # noqa: F401

from data_generator.config import Config

_FRAUD_TYPES = ["card_not_present", "account_takeover", "identity_theft", "probing"]
_PRE_DRIFT_WEIGHTS  = np.array([0.40, 0.30, 0.15, 0.15], dtype=float)


def generate(config: Config, rng: np.random.Generator, transactions_df: pd.DataFrame) -> pd.DataFrame:
    fraud_txns = transactions_df[transactions_df["is_fraud"]].drop_duplicates("transaction_id")
    n = len(fraud_txns)

    if n == 0:
        return pd.DataFrame(columns=["label_id", "transaction_id", "is_fraud", "fraud_type", "confirmed_ts", "created_ts"])

    # Post-drift: probing weight rises; others share the remainder equally
    probing_w  = config.probing_weight_post_drift if config.drift_enabled and config.scenario_A_probing_surge else 0.15
    remainder  = (1.0 - probing_w) / 3
    post_drift_weights = np.array([remainder, remainder, remainder, probing_w], dtype=float)

    drift_start_dt = datetime.fromisoformat(config.drift_start_date)
    is_post_drift  = pd.to_datetime(fraud_txns["event_timestamp"]) >= drift_start_dt

    pre_w  = _PRE_DRIFT_WEIGHTS / _PRE_DRIFT_WEIGHTS.sum()
    post_w = post_drift_weights / post_drift_weights.sum()

    fraud_types = []
    for post in is_post_drift:
        w = post_w if post else pre_w
        fraud_types.append(str(rng.choice(_FRAUD_TYPES, p=w)))

    # Confirmation lag: 1 hour – 3 days after the transaction
    confirm_delays = rng.integers(3_600, 259_200, size=n)
    event_times = pd.to_datetime(fraud_txns["event_timestamp"].values)
    confirmed_dates = [
        event_times[i].to_pydatetime() + timedelta(seconds=int(confirm_delays[i]))
        for i in range(n)
    ]

    return pd.DataFrame({
        "label_id":       [f"LBL-{i:07d}" for i in range(1, n + 1)],
        "transaction_id": fraud_txns["transaction_id"].values.tolist(),
        "is_fraud":       [True] * n,
        "fraud_type":     fraud_types,
        "confirmed_ts":   [d.isoformat() for d in confirmed_dates],
        "created_ts":     [d.isoformat() for d in confirmed_dates],
    })
