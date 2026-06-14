# Section 01 — Data Generator Design
# FinGuard: Banking & Fraud Detection Platform

## 1. Domain Overview

This project simulates a retail banking platform operating in Vietnam. The generator produces:

- **Offline historical data** — transactional and reference tables (Parquet)
- **Streaming real-time events** — banking activity events (JSON via Kafka)

The goal is to support downstream ingestion, transformation, and feature engineering pipelines while intentionally injecting realistic data quality and processing challenges that reflect production banking systems.

---

## 2. Offline Dataset Design

### 2.1 Offline Tables

| Table | Grain | Key Columns |
|---|---|---|
| `customers` | One per customer | `customer_id`, `full_name`, `date_of_birth`, `city`, `country`, `kyc_status`, `risk_tier`, `signup_ts`, `created_ts` |
| `accounts` | One per account | `account_id`, `customer_id`, `account_type`, `balance`, `currency`, `status`, `opened_ts`, `created_ts` |
| `cards` | One per card | `card_id`, `account_id`, `card_type`, `masked_card_number`, `expiry_date`, `is_active`, `issued_ts`, `created_ts` |
| `merchants` | One per merchant | `merchant_id`, `name`, `mcc_code`, `category`, `city`, `country`, `is_active`, `created_ts` |
| `transactions` | One per transaction | `transaction_id`, `account_id`, `card_id`, `merchant_id`, `amount`, `currency`, `channel`, `status`, `event_timestamp`, `created_ts` |
| `fraud_labels` | One per confirmed fraud | `label_id`, `transaction_id`, `is_fraud`, `fraud_type`, `confirmed_ts`, `created_ts` |

**Column notes:**
- `event_timestamp` — when the transaction actually occurred (event time)
- `created_ts` — when the record was written into the system (processing time)
- `account_type` — `savings`, `checking`, or `credit`
- `card_type` — `debit` or `credit`
- `channel` — `online`, `pos` (point-of-sale), or `atm`
- `kyc_status` — `verified`, `pending`, or `rejected`
- `risk_tier` — `low`, `medium`, or `high`
- `fraud_type` — `card_not_present`, `account_takeover`, `identity_theft`, `probing`

### 2.2 Offline Data Problems

**Compulsory:**
- **Geographic skew**: 80% of transactions originate from top 3 cities — Ho Chi Minh City (50%), Hanoi (20%), Da Nang (10%).
- **Class imbalance**: ~1.5% of transactions are fraudulent — mirrors real-world fraud rates.
- **High cardinality**: `transaction_id`, `account_id`, `card_id` are all unique identifiers.
- **Schema evolution**: `device_fingerprint` column is absent in the first 60 days of data (old partitions), added from day 61 onwards to simulate a system upgrade.

**Optional chosen:**
- **Duplicates**: 1% duplicate rate in `transactions` — same `transaction_id` appears twice, simulating double-submission bugs in POS terminals.

**Output:** Parquet files partitioned by `transaction_date`.

---

## 3. Streaming Dataset Design

### 3.1 Event Stream Schema

Single unified Kafka topic (`banking.events`) with an `event_type` field.

| Column | Type | Description |
|---|---|---|
| `event_id` | string | Unique event identifier |
| `event_type` | string | `transaction_initiated`, `transaction_approved`, `transaction_declined`, `login_attempt`, `card_blocked`, `card_unblocked`, `otp_requested` |
| `event_timestamp` | timestamp | When the event occurred |
| `created_ts` | timestamp | When the event was written to Kafka |
| `account_id` | string | Associated account (nullable for non-account events) |
| `card_id` | string | Associated card (nullable) |
| `merchant_id` | string | Associated merchant (nullable) |
| `amount` | float | Transaction amount (nullable, only for transaction events) |
| `currency` | string | `VND`, `USD` |
| `channel` | string | `online`, `pos`, `atm` |
| `device_type` | string | `mobile`, `desktop`, `atm_machine` |
| `ip_address` | string | Hashed/masked IP |
| `city` | string | Event location city |
| `is_cross_border` | boolean | True if merchant country ≠ customer country |

### 3.2 Streaming Data Problems

**Compulsory:**
- **Burst traffic**: Baseline 200 events/min → spikes to 4000 events/min on the 1st and 15th of each month (salary days) in 30-minute windows at 09:00.
- **Late arrivals**: 10% of cross-border events have `created_ts` delayed 10–900 seconds after `event_timestamp`.

**Optional chosen:**
- **Duplicates**: 1.5% of streaming events are duplicated (same `event_id` re-emitted within 1–5 minutes), simulating Kafka at-least-once delivery.

**Output:** JSON messages on Kafka topic `banking.events`.

---

## 4. Feature Engineering Preview

Features to be built on top of generated data (fully implemented in Section 02):

**Offline features (stable, batch-computed):**
- `f_customer_total_tx_30d` — total transaction count in last 30 days
- `f_customer_avg_tx_amount_30d` — average transaction amount in last 30 days
- `f_customer_distinct_merchants_30d` — unique merchants visited
- `f_customer_fraud_rate_90d` — historical fraud rate for this customer
- `f_merchant_fraud_rate_90d` — fraud rate at this merchant

**Streaming features (real-time, window-computed):**
- `f_tx_velocity_1h` — number of transactions in the last hour
- `f_amount_zscore_1h` — z-score of current amount vs. 1-hour rolling mean
- `f_foreign_merchant_flag` — whether merchant country differs from customer home country
- `f_unusual_hour_flag` — whether transaction occurs between 00:00–05:00

---

## 5. Generator Configuration

```yaml
# customers & accounts
n_customers: 50000
n_accounts: 60000       # some customers have multiple accounts
n_cards: 75000          # some accounts have multiple cards
n_merchants: 8000
days_history: 180

# fraud settings
fraud_rate: 0.015       # 1.5% of transactions are fraudulent

# geographic skew
skew_cities:
  - name: "Ho Chi Minh City"
    weight: 0.50
  - name: "Hanoi"
    weight: 0.20
  - name: "Da Nang"
    weight: 0.10
  - name: "other"
    weight: 0.20

# schema evolution
schema_change_date: "2026-02-15"   # ~90 days into the 180-day history window   # device_fingerprint added after this date

# offline duplicates
duplicate_rate_offline: 0.01

# streaming config
base_events_per_min: 200
burst_multiplier: 20               # 200 * 20 = 4000 events/min on burst
burst_days: [1, 15]                # 1st and 15th of each month
burst_window: "09:00-09:30"
late_arrival_rate: 0.10
late_delay_min_max: [10, 900]      # seconds
duplicate_rate_stream: 0.015

# reproducibility
random_seed: 42
```

---

## 6. Deliverables

1. **Generator code** — `data_generator/offline/generate.py`, runnable with a single command (`uv run python data_generator/offline/generate.py`)
2. **Offline data output** — Parquet files partitioned by `transaction_date` under `data/offline/` ✓
3. **Streaming events** — `data/streaming/events.jsonl` produced by `data_generator/streaming/`; Bronze ingestion reads from this file (Kafka producer deferred — requires live Kafka)
4. **Quality report** — `data/drift_validation_report.csv` covering feature health metrics per date ✓
5. **Write-up** — optional problem choices (duplicates) and feature design rationale documented in Section 4

---

## 7. Implementation Notes

- Use `random_seed: 42` throughout for full reproducibility
- Deduplication keys: `transaction_id` (offline), `event_id + created_ts` (streaming)
- All timestamps in ISO 8601 format, timezone-aware (UTC)
- `event_timestamp` always precedes or equals `created_ts`
- Cross-border transactions: customer city in Vietnam, merchant city outside Vietnam
- Fraud pattern: probing attacks (many small amounts < 50,000 VND) cluster in burst windows — sets up the drift scenario for Section 03
