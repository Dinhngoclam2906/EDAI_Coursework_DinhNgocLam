import numpy as np
import pandas as pd
from datetime import datetime, timedelta

from data_generator.config import Config

_DRIFT_RAMP_DAYS = 30  # gradual drift ramps over this many days


def _drift_factor(ts: datetime, drift_start: datetime, mode: str) -> float:
    """Returns a factor in [0, 1] representing how much drift has been applied."""
    if ts < drift_start:
        return 0.0
    if mode == "abrupt":
        return 1.0
    days_since = (ts - drift_start).days
    return min(days_since / _DRIFT_RAMP_DAYS, 1.0)


def generate(
    config: Config,
    rng: np.random.Generator,
    accounts_df: pd.DataFrame,
    cards_df: pd.DataFrame,
    merchants_df: pd.DataFrame,
) -> pd.DataFrame:
    n = config.n_transactions
    schema_change_dt = datetime.fromisoformat(config.schema_change_date)
    drift_start_dt   = datetime.fromisoformat(config.drift_start_date)

    end_date   = datetime.utcnow()
    start_date = end_date - timedelta(days=config.days_history)

    # --- Account & card pools (active only) ---
    active_accounts = accounts_df[accounts_df["status"] == "active"]["account_id"].values
    active_cards    = cards_df[cards_df["is_active"]].copy()

    acct_to_cards: dict[str, list[str]] = (
        active_cards.groupby("account_id")["card_id"].apply(list).to_dict()
    )
    accounts_with_cards = [a for a in active_accounts if a in acct_to_cards]

    sampled_accounts = rng.choice(accounts_with_cards, size=n, replace=True)
    sampled_cards    = [str(rng.choice(acct_to_cards[acct])) for acct in sampled_accounts]

    # --- Merchant sampling with geographic skew ---
    top_city_weights  = {c.name: c.weight for c in config.skew_cities if c.name != "Other"}
    other_weight      = next((c.weight for c in config.skew_cities if c.name == "Other"), 0.20)
    n_other_merchants = max(len(merchants_df) - int(merchants_df["city"].isin(top_city_weights).sum()), 1)

    def merchant_weight(city: str) -> float:
        if city in top_city_weights:
            return top_city_weights[city] / max((merchants_df["city"] == city).sum(), 1)
        return other_weight / n_other_merchants

    raw_weights = merchants_df["city"].map(merchant_weight).values.astype(float)
    raw_weights /= raw_weights.sum()
    sampled_merchants = rng.choice(merchants_df["merchant_id"].values, size=n, p=raw_weights, replace=True)

    # --- Timestamps ---
    total_seconds    = int((end_date - start_date).total_seconds())
    tx_offsets       = rng.integers(0, total_seconds, size=n)
    event_timestamps = [start_date + timedelta(seconds=int(s)) for s in tx_offsets]

    delays            = rng.integers(1, 31, size=n)
    created_timestamps = [event_timestamps[i] + timedelta(seconds=int(delays[i])) for i in range(n)]

    # --- Base amounts ---
    amounts = np.round(rng.lognormal(mean=12.5, sigma=1.5, size=n), -3)
    amounts = np.clip(amounts, 5_000, 50_000_000).astype(float)

    # --- Channels & statuses ---
    channels = rng.choice(["online", "pos", "atm"], size=n, p=[0.50, 0.40, 0.10]).tolist()
    statuses = rng.choice(["approved", "declined"],  size=n, p=[0.95, 0.05]).tolist()

    # --- Drift factors per transaction ---
    drift_factors = np.array([
        _drift_factor(ts, drift_start_dt, config.drift_mode) if config.drift_enabled else 0.0
        for ts in event_timestamps
    ])

    # --- Scenario B: Amount Drift (covariate) ---
    if config.drift_enabled and config.scenario_B_amount_drift:
        approved_mask_np = np.array(statuses) == "approved"
        multipliers = 1.0 + drift_factors * (config.amount_multiplier - 1.0)
        amounts = np.where(approved_mask_np, amounts * multipliers, amounts)
        amounts = np.clip(amounts, 5_000, 50_000_000)

    amounts = np.round(amounts, -3)

    # --- Fraud flag ---
    is_fraud      = np.zeros(n, dtype=bool)
    approved_mask = np.array(statuses) == "approved"
    n_approved    = approved_mask.sum()

    if config.drift_enabled and config.scenario_A_probing_surge:
        # Per-transaction effective fraud rate with drift ramp
        approved_indices = np.where(approved_mask)[0]
        for idx in approved_indices:
            effective_rate = config.fraud_rate * (
                1.0 + drift_factors[idx] * (config.fraud_multiplier - 1.0)
            )
            is_fraud[idx] = rng.random() < effective_rate
    else:
        is_fraud[approved_mask] = rng.random(n_approved) < config.fraud_rate

    # --- Schema evolution: device_fingerprint null before schema_change_date ---
    fp_ints      = rng.integers(100_000, 999_999, size=n)
    fingerprints = [
        f"fp-{fp_ints[i]}" if event_timestamps[i] >= schema_change_dt else None
        for i in range(n)
    ]

    # --- is_pre_drift flag ---
    is_pre_drift = [ts < drift_start_dt for ts in event_timestamps]

    df = pd.DataFrame({
        "transaction_id":     [f"TXN-{i:08d}" for i in range(1, n + 1)],
        "account_id":         sampled_accounts.tolist(),
        "card_id":            sampled_cards,
        "merchant_id":        sampled_merchants.tolist(),
        "amount":             amounts.tolist(),
        "currency":           ["VND"] * n,
        "channel":            channels,
        "status":             statuses,
        "device_fingerprint": fingerprints,
        "is_fraud":           is_fraud.tolist(),
        "is_pre_drift":       is_pre_drift,
        "event_timestamp":    [ts.isoformat() for ts in event_timestamps],
        "created_ts":         [ts.isoformat() for ts in created_timestamps],
        "transaction_date":   [ts.date().isoformat() for ts in event_timestamps],
    })

    # --- Scenario A: Shift fraud amounts to small values (probing) ---
    if config.drift_enabled and config.scenario_A_probing_surge:
        post_fraud_mask = df["is_fraud"] & ~df["is_pre_drift"]
        n_post_fraud = post_fraud_mask.sum()
        if n_post_fraud > 0:
            # 55% of post-drift fraud is probing (very small amounts)
            probing_mask_arr = rng.random(n_post_fraud) < config.probing_weight_post_drift
            post_fraud_idx = df.index[post_fraud_mask]
            probing_amounts = np.round(rng.uniform(5_000, 50_000, size=probing_mask_arr.sum()), -3)
            df.loc[post_fraud_idx[probing_mask_arr], "amount"] = probing_amounts

    # --- Inject duplicates ---
    n_dupes = int(n * config.duplicate_rate_offline)
    if n_dupes > 0:
        dupe_idx = rng.choice(n, size=n_dupes, replace=False)
        dupes = df.iloc[dupe_idx].copy()
        dup_extra_delay = rng.integers(1, 10, size=n_dupes)
        dupes["created_ts"] = [
            (datetime.fromisoformat(dupes["created_ts"].iloc[j]) + timedelta(seconds=int(dup_extra_delay[j]))).isoformat()
            for j in range(n_dupes)
        ]
        df = pd.concat([df, dupes], ignore_index=True)

    return df
