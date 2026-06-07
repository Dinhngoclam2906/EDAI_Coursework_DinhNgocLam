import numpy as np
import pandas as pd
from faker import Faker
from datetime import datetime, timedelta

from data_generator.config import Config

OTHER_CITIES = ["Can Tho", "Hue", "Bien Hoa", "Nha Trang", "Vung Tau", "Hai Phong", "Buon Ma Thuot"]


def generate(config: Config, rng: np.random.Generator, fake: Faker) -> pd.DataFrame:
    n = config.n_customers

    cities = [c.name for c in config.skew_cities]
    weights = np.array([c.weight for c in config.skew_cities], dtype=float)
    weights /= weights.sum()

    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=config.days_history + 365)
    signup_offsets = rng.integers(0, (end_date - start_date).days, size=n)
    signup_dates = [start_date + timedelta(days=int(d)) for d in signup_offsets]

    city_indices = rng.choice(len(cities), size=n, p=weights)
    city_col = [
        rng.choice(OTHER_CITIES) if cities[i] == "Other" else cities[i]
        for i in city_indices
    ]

    return pd.DataFrame({
        "customer_id":   [f"CUST-{i:06d}" for i in range(1, n + 1)],
        "full_name":     [fake.name() for _ in range(n)],
        "date_of_birth": [fake.date_of_birth(minimum_age=18, maximum_age=70).isoformat() for _ in range(n)],
        "city":          city_col,
        "country":       ["Vietnam"] * n,
        "kyc_status":    rng.choice(["verified", "pending", "rejected"], size=n, p=[0.85, 0.12, 0.03]).tolist(),
        "risk_tier":     rng.choice(["low", "medium", "high"], size=n, p=[0.70, 0.25, 0.05]).tolist(),
        "signup_ts":     [d.isoformat() for d in signup_dates],
        "created_ts":    [d.isoformat() for d in signup_dates],
    })
