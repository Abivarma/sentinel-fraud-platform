# Data Dictionary — Sentinel Fraud Platform

## IEEE-CIS Transaction Dataset

| Field | Type | Description |
|---|---|---|
| TransactionID | Long | Unique transaction identifier |
| isFraud | Integer | Label: 1=fraud, 0=legitimate (train only) |
| TransactionDT | Long | Seconds from reference date (2017-11-30) |
| TransactionAmt | Double | Transaction amount in USD |
| ProductCD | String | Product code: W, H, C, S, R |
| card1-card6 | Various | Card-related features (masked) |
| addr1, addr2 | Double | Billing/shipping address codes |
| dist1, dist2 | Double | Distance features |
| P_emaildomain | String | Purchaser email domain |
| R_emaildomain | String | Recipient email domain |
| C1-C14 | Double | Count features (Vesta engineered) |
| D1-D15 | Double | Timedelta features (Vesta engineered) |
| M1-M9 | String | Match features: T/F/M (masked) |
| V1-V339 | Double | Vesta-engineered features (intentionally opaque) |

**Note on TransactionDT:** This is seconds elapsed since 2017-11-30. Convert: `ref_date + timedelta(seconds=TransactionDT)`.

**Note on V-columns:** 339 features engineered by Vesta Corporation. High null rates (some > 90%). Cast to DoubleType; impute nulls with per-column median.

## PaySim Dataset

| Field | Type | Description |
|---|---|---|
| step | Integer | Hour of simulation (1 step = 1 hour) |
| type | String | PAYMENT, TRANSFER, CASH_OUT, DEBIT, CASH_IN |
| amount | Double | Transaction amount |
| nameOrig | String | Origin account (C=customer, M=merchant) |
| oldbalanceOrg | Double | Origin balance before transaction |
| newbalanceOrig | Double | Origin balance after transaction |
| nameDest | String | Destination account |
| oldbalanceDest | Double | Destination balance before |
| newbalanceDest | Double | Destination balance after |
| isFraud | Integer | Fraud label (1=fraud) |
| isFlaggedFraud | Integer | System flag for large transfers (> 200k) |

## Silver Schema (conformed)

| Field | Type | Description |
|---|---|---|
| transaction_id | String | Unified transaction ID |
| customer_id | String | Customer entity key |
| merchant_id | String | Merchant entity key |
| transaction_ts | Timestamp | UTC transaction time |
| amount_usd | Double | Amount in USD |
| currency | String | ISO 4217 currency code |
| channel | String | web, mobile, atm, pos |
| merchant_category | String | MCC category |
| card_type | String | credit, debit, prepaid |
| is_fraud | Integer | Fraud label (null for unlabelled streaming) |
| data_source | String | ieee_cis, paysim, synthetic |
| _ingest_ts | Timestamp | Bronze ingestion time |
| _silver_ts | Timestamp | Silver cleaning time |

## Gold Feature Tables

### customer_features
Entity: `customer_id`, timestamp: `feature_date`

| Feature | Description |
|---|---|
| tx_count_{1d,7d,30d} | Rolling transaction counts |
| tx_amount_sum_{1d,7d,30d} | Rolling amount sums |
| tx_amount_{mean,std}_7d | Amount statistics |
| unique_merchants_7d | Distinct merchants in 7 days |
| fraud_rate_30d | Historical fraud rate |
| avg_tx_hour_7d | Average transaction hour |
| weekend_ratio_7d | Weekend transaction ratio |

### merchant_features
Entity: `merchant_id`, timestamp: `feature_date`

| Feature | Description |
|---|---|
| tx_count_{1d,7d} | Rolling transaction counts |
| fraud_{count,rate}_30d | Fraud statistics |
| avg_amount_7d | Average transaction amount |
| unique_customers_7d | Distinct customers |

### transaction_features
Entity: `transaction_id`, timestamp: `transaction_ts`

| Feature | Description |
|---|---|
| hour_of_day, day_of_week | Temporal features |
| is_weekend, is_night | Binary temporal flags |
| amount_log1p | Log-transformed amount |
| amount_bucket | Categorical bucket |
| amount_zscore | Standardised amount |
| customer_tx_count_{1h,6h,24h} | Customer velocity |
| amount_vs_customer_mean_ratio | Amount anomaly ratio |
| is_new_merchant | Never seen this merchant |
| is_round_amount | Amount divisible by 100 |
| is_velocity_spike | > 5 txns in last 1h |
