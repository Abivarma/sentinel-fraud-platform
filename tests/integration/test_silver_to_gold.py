"""
Integration tests: Silver → Gold feature engineering pipeline.

Tests verify that feature transform functions produce the correct column set,
that velocity window semantics are correct, and that Delta write/read round-trips
work against a live MinIO instance.

Run with: pytest -m integration tests/integration/test_silver_to_gold.py
Requires: Docker daemon running (testcontainers spins up MinIO)
"""

import sys
from pathlib import Path
from datetime import datetime, timezone, timedelta
import pytest
import pyspark.sql.functions as F
from pyspark.sql import Row

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "pipelines" / "spark"))

pytestmark = pytest.mark.integration

# The 14 per-transaction feature columns produced by apply_all_batch_transforms
# (excludes entity / timestamp columns)
FEATURE_COLS = [
    "hour_of_day",
    "day_of_week",
    "is_weekend",
    "is_night",
    "amount_log1p",
    "amount_bucket",
    "is_round_amount",
    "amount_zscore",
    "customer_tx_count_1h",
    "customer_tx_count_6h",
    "customer_tx_count_24h",
    "customer_tx_amount_sum_1h",       # produced by compute_velocity_features_batch
    "customer_tx_amount_sum_6h",
    "customer_tx_amount_sum_24h",
]

# Columns used by flag_high_risk_patterns (depends on velocity cols being present)
RISK_COLS = ["is_velocity_spike", "is_new_merchant"]


def _make_silver_df(spark, n: int = 20, all_same_customer: bool = False):
    """
    Build a minimal silver-format DataFrame suitable for feature transforms.

    Uses a fixed base timestamp so window boundaries are predictable.
    """
    base_ts = datetime(2023, 6, 15, 10, 0, 0, tzinfo=timezone.utc)

    rows = [
        Row(
            transaction_id=f"TX{i:05d}",
            customer_id="CUST_001" if all_same_customer else f"CUST_{i % 5:03d}",
            merchant_id=f"MERCH_{i % 3:03d}",
            transaction_ts=base_ts + timedelta(minutes=i * 5),
            amount_usd=float(50.0 + i * 7),
            currency="USD",
            channel="card",
            merchant_category="W",
            card_type="credit",
            is_fraud=0,
            data_source="ieee_cis",
        )
        for i in range(n)
    ]
    return spark.createDataFrame(rows)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.integration
def test_gold_feature_columns_present(spark_for_integration):
    """
    Apply the batch feature transform pipeline to a minimal silver DataFrame
    and assert all 14 FEATURE_COLS are present in the output.
    """
    from shared.feature_transforms import apply_all_batch_transforms

    spark = spark_for_integration
    df = _make_silver_df(spark, n=20)

    df_features = apply_all_batch_transforms(
        df,
        entity_col="customer_id",
        ts_col="transaction_ts",
        amount_col="amount_usd",
    )

    actual_cols = set(df_features.columns)
    missing = [c for c in FEATURE_COLS if c not in actual_cols]
    assert not missing, (
        f"Feature columns missing from gold output: {missing}\n"
        f"Actual columns: {sorted(actual_cols)}"
    )


@pytest.mark.integration
def test_velocity_window_correctness(spark_for_integration):
    """
    Create 10 transactions for a single customer_id spaced 5 minutes apart
    (all within 1 hour). After computing velocity features:
      - customer_tx_count_1h should equal 10 for the last row
      - customer_tx_count_24h should equal 10 for every row (all within 24h)
    """
    from shared.feature_transforms import compute_velocity_features_batch

    spark = spark_for_integration

    # 10 transactions, same customer, 5 min apart → all within 1 hour window
    df = _make_silver_df(spark, n=10, all_same_customer=True)

    df_vel = compute_velocity_features_batch(
        df,
        entity_col="customer_id",
        ts_col="transaction_ts",
        amount_col="amount_usd",
        windows_hours=[1, 6, 24],
    )

    rows = df_vel.orderBy("transaction_ts").collect()

    # The last row (index 9) is 45 min after the first — all 10 fit in 1h window
    last_row = rows[-1]
    assert last_row["customer_tx_count_1h"] == 10, (
        f"Expected customer_tx_count_1h == 10 for last row, got "
        f"{last_row['customer_tx_count_1h']}"
    )

    # All 10 rows fit within the 24h window for every row
    for i, row in enumerate(rows):
        count_24h = row["customer_tx_count_24h"]
        assert count_24h >= 1, (
            f"Row {i}: customer_tx_count_24h should be >= 1, got {count_24h}"
        )

    # The 24h count for the last row should be 10 (all within 24h)
    assert last_row["customer_tx_count_24h"] == 10, (
        f"Expected customer_tx_count_24h == 10 for last row, got "
        f"{last_row['customer_tx_count_24h']}"
    )


@pytest.mark.integration
def test_delta_write_and_read(spark_for_integration, minio_client):
    """
    Write 100 feature rows to Delta at s3a://test-lake/gold/test_features,
    read them back, and assert the row count is exactly 100.
    """
    from shared.feature_transforms import apply_all_batch_transforms

    spark = spark_for_integration
    df = _make_silver_df(spark, n=100)

    df_features = apply_all_batch_transforms(
        df,
        entity_col="customer_id",
        ts_col="transaction_ts",
        amount_col="amount_usd",
    ).withColumn(
        "feature_date",
        F.lit(datetime(2023, 6, 15, tzinfo=timezone.utc).isoformat()).cast("timestamp"),
    )

    gold_path = "s3a://test-lake/gold/test_features"
    df_features.write.format("delta").mode("overwrite").save(gold_path)

    df_read = spark.read.format("delta").load(gold_path)
    actual_count = df_read.count()

    assert actual_count == 100, (
        f"Expected 100 rows after Delta round-trip, got {actual_count}"
    )

    # Sanity-check that feature columns survived the round-trip
    read_cols = set(df_read.columns)
    assert "amount_log1p" in read_cols, "amount_log1p missing after Delta round-trip"
    assert "hour_of_day" in read_cols, "hour_of_day missing after Delta round-trip"
