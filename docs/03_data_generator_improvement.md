# Section 03 — Data Generator Improvement & Drift
# FinGuard: Banking & Fraud Detection Platform

## 1. Objective

Extend the Section 01 generator with realistic drift scenarios to:

- Simulate how feature distributions change over time
- Test feature store monitoring and drift detection in the Gold layer
- Create a Gold label table for ML training (`ml_fraud_label`)
- Join label + feature tables to produce the training dataset (`ml_fraud_training`)
- Demonstrate the difference between covariate drift and concept drift

---

## 2. What is Feature Drift?

**Covariate drift:** Input feature distributions change over time, but the relationship between features and the label stays the same.
> Example: average transaction amount increases across all customers (salary season), but the fraud rate per amount band stays constant.

**Concept drift:** The statistical relationship between features and the label changes over time.
> Example: fraudsters switch from large-amount card-not-present fraud to high-frequency small-amount probing attacks. The same feature values now mean different things for fraud prediction.

Both types degrade ML model performance if undetected.

---

## 3. Drift Scenarios

### Scenario A: Probing Attack Surge ⭐ (Primary — Concept Drift)

**What changes after `drift_start_date`:**
- Fraud rate increases from 1.5% → 4.5% (3× spike)
- Fraud type distribution shifts: `probing` rises from 15% → 55% of all fraud cases
- Probing transactions are characteristically small: amount < 50,000 VND

**How to inject:**
- After `drift_start_date`, apply `fraud_multiplier` to approved transactions
- For fraud cases in the post-drift window, shift `fraud_type_weights` toward `probing`
- Probing fraud forces `amount` to be sampled from a low-value distribution (5,000–50,000 VND)

**Features affected:**
- `f_avg_amount_30d` decreases (many small probing transactions)
- `f_tx_velocity_1h` increases (probing = many rapid small transactions)
- `f_fraud_rate_90d` increases (historical fraud rate rises)

**Why this matters for ML:**
The model trained on pre-drift data associates fraud with large amounts and high-value merchants. Post-drift, fraud is small-amount and high-frequency — the feature-to-label relationship changes. Without retraining, recall drops significantly.

---

### Scenario B: Transaction Amount Drift (Secondary — Covariate Drift)

**What changes after `drift_start_date`:**
- Average transaction amount increases by 20% (Tết holiday / salary bonus season)
- All approved transactions get amount multiplied by `amount_multiplier`

**How to inject:**
- Multiply amount by `amount_multiplier` for transactions after `drift_start_date`

**Features affected:**
- `f_avg_amount_30d` increases (higher average across all customers)
- `f_total_amount_1h` increases

**Why this matters for ML:**
Feature value ranges shift — z-score features become miscalibrated. The fraud-to-amount relationship is preserved but the model may misclassify more edge cases due to out-of-distribution input values.

---

## 4. Drift Configuration Parameters

Added to `data_generator/config.yaml`:

```yaml
drift_enabled: true
drift_start_date: "2026-03-01"
drift_mode: "gradual"          # gradual | abrupt

# Scenario A: Probing Attack Surge
scenario_A_probing_surge: true
fraud_multiplier: 3.0          # fraud rate: 1.5% → 4.5%
probing_weight_post_drift: 0.55  # probing fraud type rises to 55%

# Scenario B: Amount Drift
scenario_B_amount_drift: true
amount_multiplier: 1.20        # 20% amount increase post-drift
```

**Gradual vs abrupt:**
- `abrupt`: drift applies instantly on `drift_start_date`
- `gradual`: drift ramps linearly from 0% to 100% over 30 days after `drift_start_date`

---

## 5. Gold Monitoring Tables

### `agg_feature_health_daily`

Tracks daily feature statistics and PSI vs pre-drift baseline.

| Column | Description |
|---|---|
| `monitoring_date` | Date of the snapshot |
| `feature_name` | Name of the feature |
| `mean_value` | Daily mean of the feature |
| `stddev_value` | Daily standard deviation |
| `psi_vs_baseline` | PSI computed against pre-drift 30-day baseline |
| `alert_flag` | True if PSI > alert threshold (0.04 for proportion features) |

**PSI thresholds:**
- PSI < 0.10: No significant change (standard continuous-feature threshold)
- PSI 0.10–0.15: Monitor closely
- PSI > 0.15: Alert — for continuous features with many bins

**Note on binary/proportion features:** Standard PSI thresholds assume continuous features. For binary proportion features like `f_fraud_rate`, a 3× fraud spike (1.5% → 4.5%) produces PSI ≈ 0.047 — mathematically bounded because ~98% of transactions are non-fraud regardless. The implementation uses `PSI_ALERT_THRESHOLD = 0.04`, calibrated to detect the post-drift fraud surge while avoiding false alarms in stable periods.

### `feature_drift_alerts`

Records triggered alerts when PSI > 0.04 (threshold calibrated for binary/proportion features — see Section 5 note).

| Column | Description |
|---|---|
| `alert_date` | When the alert fired |
| `feature_name` | Affected feature |
| `psi_value` | PSI that triggered the alert |
| `drift_type` | `covariate` or `concept` |
| `recommended_action` | Human-readable recommendation |

---

## 6. Label & Training Tables

### `ml_fraud_label`

Label-only table (no features) in Gold zone.

| Column | Description |
|---|---|
| `transaction_id` | Business key |
| `customer_id` | Customer identifier |
| `event_timestamp` | Transaction time (used for point-in-time join) |
| `created_ts` | When the label was confirmed |
| `label` | 1 = fraud, 0 = not fraud |

**Label logic:**
- `label = 1` if `transaction_id` appears in `fact_fraud_label`
- `label = 0` for all approved transactions not in `fact_fraud_label`
- Declined transactions excluded (no confirmed label)
- `event_timestamp` = transaction event time (not label confirmation time)

### `ml_fraud_training`

Training-ready table: label + features joined with point-in-time correctness.

| Column | Source |
|---|---|
| `transaction_id` | ml_fraud_label |
| `customer_id` | ml_fraud_label |
| `event_timestamp` | ml_fraud_label |
| `label` | ml_fraud_label |
| `f_total_tx_30d` | feat_customer_30d (as-of event_timestamp) |
| `f_avg_amount_30d` | feat_customer_30d |
| `f_distinct_merchants_30d` | feat_customer_30d |
| `f_declined_rate_30d` | feat_customer_30d |
| `f_fraud_rate_90d` | feat_customer_30d |
| `f_tx_velocity_1h` | feat_stream_1h |
| `f_total_amount_1h` | feat_stream_1h |
| `f_foreign_flag` | feat_stream_1h |
| `f_unusual_hour_flag` | feat_stream_1h |
| `is_pre_drift` | 1 if event_timestamp < drift_start_date |

**Point-in-time correctness:**
Features joined at the latest snapshot where `feature.event_timestamp <= label.event_timestamp` to prevent data leakage.

---

## 7. Deliverables

- [x] Design document: `docs/03_data_generator_improvement.md`
- [x] Extended generator with drift injection: `data_generator/offline/drift_report.py` — generates PSI metrics per feature per date
- [x] Drift validation report: `data/drift_validation_report.csv` — daily feature stats and PSI vs pre-drift baseline
- [x] Gold monitoring tables: `delta.gold.agg_feature_health_daily` (daily PSI per feature), `delta.gold.feature_drift_alerts` (PSI > 0.04 alerts with drift type and recommended action; threshold calibrated for binary proportion features — see Section 5 note)
- [x] Drift trigger: `pipelines/gold/drift_trigger.py` — reads `feature_drift_alerts`, concept drift triggers retraining request (status=pending), covariate drift writes monitoring recommendation; watermarked so alerts are processed exactly once; requests land in `data/retraining_requests.jsonl` for Section 04.1 ML pipeline to consume
- [x] Gold label table: `delta.gold.ml_fraud_label` — transaction-level fraud labels with `event_timestamp` for point-in-time joins
- [x] Gold training table: `delta.gold.ml_fraud_training` — labels joined with `feat_customer_30d` features, `is_pre_drift` flag for drift-aware training
