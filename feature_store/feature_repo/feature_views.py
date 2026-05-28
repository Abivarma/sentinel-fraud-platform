"""
Feast FeatureView definitions.
These define the feature contract shared by offline training and online serving.
Changing a feature here affects BOTH paths simultaneously.
"""

from datetime import timedelta

from feast import Feature, FeatureView, ValueType

from entities import customer, merchant, transaction
from data_sources import customer_features_source, merchant_features_source, transaction_features_source

customer_feature_view = FeatureView(
    name="customer_features",
    entities=[customer],
    ttl=timedelta(days=7),
    features=[
        Feature(name="tx_count_1d", dtype=ValueType.INT64),
        Feature(name="tx_count_7d", dtype=ValueType.INT64),
        Feature(name="tx_count_30d", dtype=ValueType.INT64),
        Feature(name="tx_amount_sum_1d", dtype=ValueType.DOUBLE),
        Feature(name="tx_amount_sum_7d", dtype=ValueType.DOUBLE),
        Feature(name="tx_amount_sum_30d", dtype=ValueType.DOUBLE),
        Feature(name="tx_amount_mean_7d", dtype=ValueType.DOUBLE),
        Feature(name="tx_amount_std_7d", dtype=ValueType.DOUBLE),
        Feature(name="unique_merchants_7d", dtype=ValueType.INT64),
        Feature(name="fraud_rate_30d", dtype=ValueType.DOUBLE),
        Feature(name="avg_tx_hour_7d", dtype=ValueType.DOUBLE),
        Feature(name="weekend_ratio_7d", dtype=ValueType.DOUBLE),
    ],
    source=customer_features_source,
    tags={"owner": "data-engineering", "domain": "fraud", "phase": "06"},
)

merchant_feature_view = FeatureView(
    name="merchant_features",
    entities=[merchant],
    ttl=timedelta(days=7),
    features=[
        Feature(name="tx_count_1d", dtype=ValueType.INT64),
        Feature(name="tx_count_7d", dtype=ValueType.INT64),
        Feature(name="fraud_count_30d", dtype=ValueType.INT64),
        Feature(name="fraud_rate_30d", dtype=ValueType.DOUBLE),
        Feature(name="avg_amount_7d", dtype=ValueType.DOUBLE),
        Feature(name="unique_customers_7d", dtype=ValueType.INT64),
    ],
    source=merchant_features_source,
    tags={"owner": "data-engineering", "domain": "fraud", "phase": "06"},
)

transaction_feature_view = FeatureView(
    name="transaction_features",
    entities=[transaction],
    ttl=timedelta(hours=24),
    features=[
        Feature(name="hour_of_day", dtype=ValueType.INT32),
        Feature(name="day_of_week", dtype=ValueType.INT32),
        Feature(name="is_weekend", dtype=ValueType.INT32),
        Feature(name="is_night", dtype=ValueType.INT32),
        Feature(name="amount_log1p", dtype=ValueType.DOUBLE),
        Feature(name="amount_bucket", dtype=ValueType.STRING),
        Feature(name="amount_zscore", dtype=ValueType.DOUBLE),
        Feature(name="customer_tx_count_1h", dtype=ValueType.INT64),
        Feature(name="customer_tx_count_6h", dtype=ValueType.INT64),
        Feature(name="customer_tx_count_24h", dtype=ValueType.INT64),
        Feature(name="amount_vs_customer_mean_ratio", dtype=ValueType.DOUBLE),
        Feature(name="is_new_merchant", dtype=ValueType.INT32),
        Feature(name="is_round_amount", dtype=ValueType.INT32),
        Feature(name="is_velocity_spike", dtype=ValueType.INT32),
    ],
    source=transaction_features_source,
    tags={"owner": "data-engineering", "domain": "fraud", "phase": "06"},
)
