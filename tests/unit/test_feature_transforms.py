"""Unit tests for the shared feature_transforms module."""

import sys
from pathlib import Path
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "pipelines" / "spark"))

import pyspark.sql.functions as F


def test_compute_temporal_features_hour(spark):
    from shared.feature_transforms import compute_temporal_features
    from datetime import datetime

    rows = [(datetime(2024, 1, 15, 14, 30, 0),)]
    df = spark.createDataFrame(rows, ["transaction_ts"])
    result = compute_temporal_features(df, "transaction_ts")

    row = result.collect()[0]
    assert row["hour_of_day"] == 14
    assert row["day_of_week"] == 2  # Monday (2024-01-15 is Monday, dayofweek=2)
    assert row["is_weekend"] == 0
    assert row["is_night"] == 0


def test_compute_temporal_features_night(spark):
    from shared.feature_transforms import compute_temporal_features
    from datetime import datetime

    rows = [(datetime(2024, 1, 15, 23, 0, 0),)]
    df = spark.createDataFrame(rows, ["transaction_ts"])
    result = compute_temporal_features(df, "transaction_ts")
    assert result.collect()[0]["is_night"] == 1


def test_compute_temporal_features_weekend(spark):
    from shared.feature_transforms import compute_temporal_features
    from datetime import datetime

    rows = [(datetime(2024, 1, 14, 12, 0, 0),)]  # Sunday
    df = spark.createDataFrame(rows, ["transaction_ts"])
    result = compute_temporal_features(df, "transaction_ts")
    assert result.collect()[0]["is_weekend"] == 1


def test_compute_amount_features_buckets(spark):
    from shared.feature_transforms import compute_amount_features

    rows = [(5.0,), (50.0,), (500.0,), (5000.0,)]
    df = spark.createDataFrame(rows, ["amount_usd"])
    result = compute_amount_features(df, "amount_usd").collect()

    assert result[0]["amount_bucket"] == "0-10"
    assert result[1]["amount_bucket"] == "10-100"
    assert result[2]["amount_bucket"] == "100-1000"
    assert result[3]["amount_bucket"] == "1000+"


def test_compute_amount_features_log1p_nonnegative(spark):
    from shared.feature_transforms import compute_amount_features

    rows = [(-100.0,), (0.0,), (100.0,)]
    df = spark.createDataFrame(rows, ["amount_usd"])
    result = compute_amount_features(df, "amount_usd").collect()

    for row in result:
        assert row["amount_log1p"] >= 0.0, "log1p should clamp negatives to 0"


def test_is_round_amount(spark):
    from shared.feature_transforms import compute_amount_features

    rows = [(100.0,), (100.50,), (200.0,)]
    df = spark.createDataFrame(rows, ["amount_usd"])
    result = compute_amount_features(df, "amount_usd").collect()

    assert result[0]["is_round_amount"] == 1
    assert result[1]["is_round_amount"] == 0
    assert result[2]["is_round_amount"] == 1


def test_velocity_spike_flag(spark):
    from shared.feature_transforms import flag_high_risk_patterns

    rows = [(3,), (6,), (10,)]
    df = spark.createDataFrame(rows, ["customer_tx_count_1h"])
    result = flag_high_risk_patterns(df).collect()

    assert result[0]["is_velocity_spike"] == 0
    assert result[1]["is_velocity_spike"] == 1
    assert result[2]["is_velocity_spike"] == 1


def test_streaming_transforms_no_aggregation(spark):
    """Verify streaming transforms don't require full dataset (no .count() or .agg())."""
    from shared.feature_transforms import apply_all_streaming_transforms
    from datetime import datetime

    rows = [
        ("TX001", "C001", "M001", datetime(2024, 1, 15, 14, 0, 0), 150.0),
    ]
    df = spark.createDataFrame(rows, ["transaction_id", "customer_id", "merchant_id", "transaction_ts", "amount_usd"])
    result = apply_all_streaming_transforms(df, global_amount_mean=100.0, global_amount_std=50.0)

    row = result.collect()[0]
    assert row["hour_of_day"] == 14
    assert row["amount_log1p"] > 0
    assert row["amount_zscore"] is not None
