"""
Shared feature transformation library.

CRITICAL: This module is imported by BOTH:
  - phase_06_gold_features.py  (batch — training data)
  - phase_05_streaming_ingest.py  (streaming — inference)

Any change here affects both paths simultaneously.
This is the architectural guarantee against training-serving feature skew.
"""

from typing import List, Optional

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.types import IntegerType, LongType


def compute_amount_features(df: DataFrame, amount_col: str = "amount_usd") -> DataFrame:
    """Amount-based features. Safe for batch and streaming (no aggregation)."""
    return (
        df
        .withColumn("amount_log1p", F.log1p(F.greatest(F.col(amount_col), F.lit(0.0))))
        .withColumn(
            "amount_bucket",
            F.when(F.col(amount_col) <= 10, "0-10")
             .when(F.col(amount_col) <= 100, "10-100")
             .when(F.col(amount_col) <= 1000, "100-1000")
             .otherwise("1000+"),
        )
        .withColumn("is_round_amount", (F.col(amount_col) % 100 == 0).cast(IntegerType()))
    )


def compute_amount_zscore(df: DataFrame, amount_col: str = "amount_usd") -> DataFrame:
    """Z-score requiring a full pass — batch only."""
    stats = df.agg(
        F.mean(amount_col).alias("mean"),
        F.stddev(amount_col).alias("std"),
    ).collect()[0]
    mean_val = stats["mean"] or 0.0
    std_val = max(stats["std"] or 1.0, 1e-6)
    return df.withColumn("amount_zscore", (F.col(amount_col) - F.lit(mean_val)) / F.lit(std_val))


def compute_amount_zscore_streaming(
    df: DataFrame,
    global_mean: float,
    global_std: float,
    amount_col: str = "amount_usd",
) -> DataFrame:
    """Streaming-safe z-score using pre-computed global statistics."""
    return df.withColumn(
        "amount_zscore",
        (F.col(amount_col) - F.lit(global_mean)) / F.lit(max(global_std, 1e-6)),
    )


def compute_temporal_features(df: DataFrame, ts_col: str = "transaction_ts") -> DataFrame:
    """Time-based features from TimestampType column. No aggregation — works in streaming."""
    return (
        df
        .withColumn("hour_of_day", F.hour(F.col(ts_col)).cast(IntegerType()))
        .withColumn("day_of_week", F.dayofweek(F.col(ts_col)).cast(IntegerType()))
        .withColumn(
            "is_weekend",
            F.when(F.dayofweek(F.col(ts_col)).isin([1, 7]), F.lit(1)).otherwise(F.lit(0)),
        )
        .withColumn(
            "is_night",
            F.when(
                (F.hour(F.col(ts_col)) >= 22) | (F.hour(F.col(ts_col)) < 6),
                F.lit(1),
            ).otherwise(F.lit(0)),
        )
    )


def compute_velocity_features_batch(
    df: DataFrame,
    entity_col: str,
    ts_col: str,
    amount_col: str,
    windows_hours: List[int] = [1, 6, 24],
) -> DataFrame:
    """
    Rolling velocity features using Spark Window functions.
    Batch only — requires full dataset. For streaming, use compute_velocity_features_streaming.
    """
    for hours in windows_hours:
        seconds = hours * 3600
        w = (
            Window.partitionBy(entity_col)
            .orderBy(F.col(ts_col).cast("long"))
            .rangeBetween(-seconds, 0)
        )
        suffix = f"{hours}h"
        df = (
            df
            .withColumn(f"customer_tx_count_{suffix}", F.count("*").over(w).cast(LongType()))
            .withColumn(f"customer_tx_amount_sum_{suffix}", F.sum(F.col(amount_col)).over(w))
        )
    return df


def compute_velocity_features_streaming(
    df: DataFrame,
    redis_lookup_udf=None,
) -> DataFrame:
    """
    Streaming-safe velocity using Redis counters (via UDF).
    Gracefully degrades to zeros if Redis unavailable.
    """
    if redis_lookup_udf is not None:
        for window in ["1h", "6h", "24h"]:
            df = df.withColumn(
                f"customer_tx_count_{window}",
                redis_lookup_udf(F.col("customer_id"), F.lit(f"count_{window}")),
            )
    else:
        for window in ["1h", "6h", "24h"]:
            df = df.withColumn(f"customer_tx_count_{window}", F.lit(0).cast(LongType()))
    return df


def flag_high_risk_patterns(
    df: DataFrame,
    merchant_history_set: Optional[set] = None,
) -> DataFrame:
    """Flag high-risk transaction patterns. Works in batch and streaming."""
    velocity_col = "customer_tx_count_1h"
    if velocity_col in df.columns:
        df = df.withColumn(
            "is_velocity_spike",
            F.when(F.col(velocity_col) > 5, F.lit(1)).otherwise(F.lit(0)),
        )
    else:
        df = df.withColumn("is_velocity_spike", F.lit(0).cast(IntegerType()))

    if merchant_history_set is not None and "customer_id" in df.columns:
        spark = SparkSession.getActiveSession()
        if spark is not None:
            hist_df = spark.createDataFrame(
                list(merchant_history_set), ["customer_id", "merchant_id"]
            ).withColumn("_seen", F.lit(1))
            df = (
                df.join(F.broadcast(hist_df), on=["customer_id", "merchant_id"], how="left")
                .withColumn(
                    "is_new_merchant",
                    F.when(F.col("_seen").isNull(), F.lit(1)).otherwise(F.lit(0)),
                )
                .drop("_seen")
            )
    else:
        df = df.withColumn("is_new_merchant", F.lit(0).cast(IntegerType()))

    return df


def apply_all_batch_transforms(
    df: DataFrame,
    entity_col: str = "customer_id",
    ts_col: str = "transaction_ts",
    amount_col: str = "amount_usd",
) -> DataFrame:
    """Apply all batch-compatible transforms. Used by phase_06_gold_features.py."""
    df = compute_temporal_features(df, ts_col)
    df = compute_amount_features(df, amount_col)
    df = compute_amount_zscore(df, amount_col)
    if entity_col in df.columns:
        df = compute_velocity_features_batch(df, entity_col, ts_col, amount_col)
    df = flag_high_risk_patterns(df)
    return df


def apply_all_streaming_transforms(
    df: DataFrame,
    global_amount_mean: float = 100.0,
    global_amount_std: float = 200.0,
    ts_col: str = "transaction_ts",
    amount_col: str = "amount_usd",
) -> DataFrame:
    """Apply all streaming-safe transforms. Used by phase_05_streaming_ingest.py."""
    df = compute_temporal_features(df, ts_col)
    df = compute_amount_features(df, amount_col)
    df = compute_amount_zscore_streaming(df, global_amount_mean, global_amount_std, amount_col)
    df = compute_velocity_features_streaming(df)
    df = flag_high_risk_patterns(df)
    return df
