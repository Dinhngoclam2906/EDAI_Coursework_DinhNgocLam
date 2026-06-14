import yaml
from pathlib import Path
from pydantic import BaseModel
from typing import List


class CityWeight(BaseModel):
    name: str
    weight: float


class Config(BaseModel):
    n_customers: int
    n_accounts: int
    n_cards: int
    n_merchants: int
    n_transactions: int
    days_history: int
    fraud_rate: float
    skew_cities: List[CityWeight]
    schema_change_date: str
    duplicate_rate_offline: float
    base_events_per_min: int
    burst_multiplier: int
    burst_days: List[int]
    burst_window_start: str
    burst_window_duration_mins: int
    late_arrival_rate: float
    late_delay_min_max: List[int]
    duplicate_rate_stream: float
    random_seed: int
    output_dir: str

    # Section 03 — drift
    drift_enabled: bool = False
    drift_start_date: str = "2099-01-01"
    drift_mode: str = "gradual"
    scenario_A_probing_surge: bool = False
    fraud_multiplier: float = 1.0
    probing_weight_post_drift: float = 0.55
    scenario_B_amount_drift: bool = False
    amount_multiplier: float = 1.0


def load_config(path: str | Path = "data_generator/config.yaml") -> Config:
    with open(path) as f:
        return Config(**yaml.safe_load(f))
