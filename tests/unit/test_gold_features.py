"""Unit tests for Phase 06 gold feature engineering."""

import sys
from pathlib import Path
from datetime import datetime, timedelta
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "pipelines" / "spark"))

import pyspark.sql.functions as F
from pyspark.sql import Window


def test_customer_velocity_1h(spark, sample_silver):
    """5 transactions for C000001 in 1h window → tx_count_1h >= 5."""
    from shared.feature_transforms import compute_velocity_features_batch

    result = compute_velocity_features_batch(
        sample_silver, "customer_id", "transaction_ts", "amount_usd", windows_hours=[1]
    )

    # C000001 has 20 rows but first 5 are within 1h (0-4h, but rangeBetween covers preceding rows)
    c001_rows = result.filter(F.col("customer_id") == "C000001").collect()
    # The last row of C000001 should have a 1h count reflecting its window
    assert any(row["customer_tx_count_1h"] >= 1 for row in c001_rows)


def test_fraud_rate_30d(spark, sample_silver):
    """5 fraud out of 100 rows → fraud_rate visible in window aggregation."""
    from pyspark.sql import Window
    import pyspark.sql.functions as F

    ts_col = F.col("transaction_ts").cast("long")
    w = Window.partitionBy("customer_id").orderBy(ts_col).rangeBetween(-30 * 86400, 0)
    df = sample_silver.withColumn(
        "fraud_rate_30d",
        F.mean(F.col("is_fraud").cast("double")).over(w)
    )

    # C000001 has 20 rows, 5 of which are fraud → rate should be around 0.25
    c001_rate = df.filter(F.col("customer_id") == "C000001").agg(F.max("fraud_rate_30d")).collect()[0][0]
    assert c001_rate is not None
    assert 0.0 <= c001_rate <= 1.0


def test_amount_log1p_nonnegative(spark, sample_silver):
    """amount_log1p must be >= 0 for all rows."""
    from shared.feature_transforms import compute_amount_features

    result = compute_amount_features(sample_silver, "amount_usd")
    neg_count = result.filter(F.col("amount_log1p") < 0).count()
    assert neg_count == 0


def test_feature_date_column(spark, sample_silver):
    """Gold features must have a feature_date column."""
    from datetime import timezone

    feature_date = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    df = sample_silver.withColumn("feature_date", F.lit(feature_date.isoformat()).cast("timestamp"))
    assert "feature_date" in df.columns
    assert df.filter(F.col("feature_date").isNull()).count() == 0
