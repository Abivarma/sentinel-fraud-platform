"""
Feast FeatureService definitions.
- training_feature_service: all features for training dataset generation
- serving_feature_service: minimal latency subset for real-time inference
"""

from feast import FeatureService

from feature_views import customer_feature_view, merchant_feature_view, transaction_feature_view

training_feature_service = FeatureService(
    name="training_feature_service",
    features=[
        customer_feature_view,
        merchant_feature_view,
        transaction_feature_view,
    ],
    description="Full feature set for model training. Use with feast.get_historical_features().",
    tags={"use_case": "training"},
)

serving_feature_service = FeatureService(
    name="serving_feature_service",
    features=[
        customer_feature_view[["tx_count_1d", "tx_count_7d", "tx_amount_mean_7d", "fraud_rate_30d"]],
        merchant_feature_view[["fraud_rate_30d", "avg_amount_7d"]],
        transaction_feature_view[[
            "hour_of_day", "is_night", "amount_log1p", "amount_zscore",
            "customer_tx_count_1h", "is_velocity_spike", "is_round_amount",
        ]],
    ],
    description="Minimal latency feature set for online inference. Use with feast.get_online_features().",
    tags={"use_case": "serving"},
)
