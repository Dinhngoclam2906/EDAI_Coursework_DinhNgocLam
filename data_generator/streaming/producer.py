"""
Streaming event producer for FinGuard.

Modes:
  --kafka          Send events to Kafka (default: write to JSONL file)
  --duration N     Run for N seconds (default: 60)
  --fast           Ignore rate limits, emit as fast as possible

Usage:
  uv run python -m data_generator.streaming.producer
  uv run python -m data_generator.streaming.producer --kafka --duration 300
"""

import argparse
import json
import time
import uuid
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np

from data_generator.config import Config, load_config

_EVENT_TYPES = [
    "transaction_initiated",
    "transaction_approved",
    "transaction_declined",
    "login_attempt",
    "card_blocked",
    "otp_requested",
]
_EVENT_WEIGHTS = np.array([0.30, 0.25, 0.05, 0.25, 0.05, 0.10], dtype=float)
_EVENT_WEIGHTS /= _EVENT_WEIGHTS.sum()

_OTHER_CITIES = ["Can Tho", "Hue", "Bien Hoa", "Nha Trang", "Vung Tau", "Hai Phong"]


def _is_burst_window(config: Config) -> bool:
    now = datetime.utcnow()
    if now.day not in config.burst_days:
        return False
    h, m = map(int, config.burst_window_start.split(":"))
    burst_start = now.replace(hour=h, minute=m, second=0, microsecond=0)
    burst_end = burst_start + timedelta(minutes=config.burst_window_duration_mins)
    return burst_start <= now <= burst_end


def _make_event(
    event_type: str,
    rng: np.random.Generator,
    config: Config,
    account_ids: list[str],
    card_ids: list[str],
    merchant_ids: list[str],
) -> dict:
    now = datetime.utcnow()

    # Late arrival: created_ts is on time; event_timestamp is in the past
    is_late = rng.random() < config.late_arrival_rate
    event_ts = (
        now - timedelta(seconds=int(rng.integers(*config.late_delay_min_max)))
        if is_late
        else now
    )

    is_tx = event_type.startswith("transaction")

    all_city_names   = [c.name for c in config.skew_cities]
    all_city_weights = np.array([c.weight for c in config.skew_cities], dtype=float)
    all_city_weights /= all_city_weights.sum()
    raw_choice = str(rng.choice(all_city_names, p=all_city_weights))
    city_choice = str(rng.choice(_OTHER_CITIES)) if raw_choice == "Other" else raw_choice

    amount = None
    if is_tx:
        amount = float(np.clip(round(float(rng.lognormal(12.5, 1.5)), -3), 5_000, 50_000_000))

    return {
        "event_id":        str(uuid.uuid4()),
        "event_type":      event_type,
        "event_timestamp": event_ts.isoformat(),
        "created_ts":      now.isoformat(),
        "account_id":      str(rng.choice(account_ids)),
        "card_id":         str(rng.choice(card_ids)) if is_tx or event_type == "card_blocked" else None,
        "merchant_id":     str(rng.choice(merchant_ids)) if is_tx else None,
        "amount":          amount,
        "currency":        "VND" if amount else None,
        "channel":         str(rng.choice(["online", "pos", "atm"], p=[0.50, 0.40, 0.10])) if is_tx else None,
        "device_type":     str(rng.choice(["mobile", "desktop", "atm_machine"], p=[0.60, 0.30, 0.10])),
        "ip_address":      f"hash-{rng.integers(100_000, 999_999)}",
        "city":            city_choice,
        "is_cross_border": bool(rng.random() < 0.03),
    }


def produce(
    config_path: str = "data_generator/config.yaml",
    use_kafka: bool = False,
    output_file: str | None = None,
    duration_seconds: int = 60,
    fast_mode: bool = False,
    account_ids: list[str] | None = None,
    card_ids: list[str] | None = None,
    merchant_ids: list[str] | None = None,
) -> None:
    config = load_config(config_path)
    rng = np.random.default_rng(config.random_seed + 999)

    # Fallback ID pools (used when offline data is not yet generated)
    if account_ids is None:
        account_ids = [f"ACCT-{i:06d}" for i in range(1, 1001)]
    if card_ids is None:
        card_ids = [f"CARD-{i:06d}" for i in range(1, 1001)]
    if merchant_ids is None:
        merchant_ids = [f"MERCH-{i:05d}" for i in range(1, 201)]

    # --- Kafka setup ---
    producer = None
    if use_kafka:
        try:
            from kafka import KafkaProducer  # type: ignore
            producer = KafkaProducer(
                bootstrap_servers=["localhost:9092"],
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            print("Connected to Kafka at localhost:9092")
        except Exception as exc:
            print(f"Kafka unavailable ({exc}), falling back to file output.")
            use_kafka = False

    # --- File output fallback ---
    out_path = Path(output_file) if output_file else Path(config.output_dir) / "streaming" / "events.jsonl"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    start = time.time()
    total_emitted = 0

    print(f"Producer started  |  duration={duration_seconds}s  |  kafka={use_kafka}  |  fast={fast_mode}")

    with open(out_path, "a") as f:
        while time.time() - start < duration_seconds:
            burst = _is_burst_window(config)
            rate_per_min = config.base_events_per_min * (config.burst_multiplier if burst else 1)
            sleep_secs = 0.0 if fast_mode else 60.0 / rate_per_min

            event_type = str(rng.choice(_EVENT_TYPES, p=_EVENT_WEIGHTS))
            event = _make_event(event_type, rng, config, account_ids, card_ids, merchant_ids)

            events_to_emit = [event]

            # Streaming duplicates: re-emit same event with a later created_ts
            if rng.random() < config.duplicate_rate_stream:
                dup = event.copy()
                dup["created_ts"] = (
                    datetime.utcnow() + timedelta(seconds=int(rng.integers(60, 300)))
                ).isoformat()
                events_to_emit.append(dup)

            for e in events_to_emit:
                if use_kafka and producer:
                    producer.send("banking.events", e)
                else:
                    f.write(json.dumps(e) + "\n")
                total_emitted += 1

            if sleep_secs > 0:
                time.sleep(sleep_secs)

    if producer:
        producer.flush()
        producer.close()

    dest = "Kafka topic banking.events" if use_kafka else str(out_path)
    print(f"Done. Emitted {total_emitted:,} events to {dest}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Emit streaming banking events")
    parser.add_argument("--config", default="data_generator/config.yaml")
    parser.add_argument("--kafka", action="store_true", help="Send to Kafka (default: write to file)")
    parser.add_argument("--output", default=None, help="Output JSONL file path")
    parser.add_argument("--duration", type=int, default=60, help="Run duration in seconds")
    parser.add_argument("--fast", action="store_true", help="Emit as fast as possible (ignore rate limits)")
    args = parser.parse_args()

    produce(
        config_path=args.config,
        use_kafka=args.kafka,
        output_file=args.output,
        duration_seconds=args.duration,
        fast_mode=args.fast,
    )
