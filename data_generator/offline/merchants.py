import numpy as np
import pandas as pd
from faker import Faker
from datetime import datetime, timedelta

from data_generator.config import Config

OTHER_CITIES = ["Can Tho", "Hue", "Bien Hoa", "Nha Trang", "Vung Tau", "Hai Phong"]

MCC_CATEGORIES = {
    "5411": "Grocery",
    "5812": "Restaurant",
    "5814": "Fast Food",
    "5912": "Pharmacy",
    "5732": "Electronics",
    "5541": "Gas Station",
    "5311": "Department Store",
    "7011": "Hotel",
    "4111": "Transportation",
    "6011": "ATM / Cash",
    "5999": "Miscellaneous",
    "4814": "Telecom",
}

_MCC_CODES = list(MCC_CATEGORIES.keys())
_MCC_WEIGHTS = np.array([0.15, 0.15, 0.10, 0.08, 0.12, 0.08, 0.08, 0.05, 0.05, 0.05, 0.05, 0.04], dtype=float)
_MCC_WEIGHTS /= _MCC_WEIGHTS.sum()


def generate(config: Config, rng: np.random.Generator, fake: Faker) -> pd.DataFrame:
    n = config.n_merchants

    cities = [c.name for c in config.skew_cities]
    weights = np.array([c.weight for c in config.skew_cities], dtype=float)
    weights /= weights.sum()

    # 5% of merchants are foreign (enables cross-border transactions)
    countries = rng.choice(
        ["Vietnam", "Singapore", "Thailand", "Japan", "USA"],
        size=n, p=[0.95, 0.02, 0.01, 0.01, 0.01],
    ).tolist()

    city_indices = rng.choice(len(cities), size=n, p=weights)
    city_col = []
    for i, ci in enumerate(city_indices):
        if countries[i] != "Vietnam":
            city_col.append(fake.city())
        elif cities[ci] == "Other":
            city_col.append(str(rng.choice(OTHER_CITIES)))
        else:
            city_col.append(cities[ci])

    chosen_mcc = rng.choice(_MCC_CODES, size=n, p=_MCC_WEIGHTS).tolist()

    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=config.days_history + 730)
    created_offsets = rng.integers(0, (end_date - start_date).days, size=n)
    created_dates = [start_date + timedelta(days=int(d)) for d in created_offsets]

    return pd.DataFrame({
        "merchant_id": [f"MERCH-{i:05d}" for i in range(1, n + 1)],
        "name":        [fake.company() for _ in range(n)],
        "mcc_code":    chosen_mcc,
        "category":    [MCC_CATEGORIES[m] for m in chosen_mcc],
        "city":        city_col,
        "country":     countries,
        "is_active":   rng.choice([True, False], size=n, p=[0.95, 0.05]).tolist(),
        "created_ts":  [d.isoformat() for d in created_dates],
    })
