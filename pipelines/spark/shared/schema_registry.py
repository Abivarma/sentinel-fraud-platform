"""
Canonical StructType schemas for all datasets.
All PySpark jobs import from here. Never use inferSchema=True in production jobs.
"""

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def _v_columns() -> list:
    """V1-V339: Vesta-engineered features (intentionally opaque, all DoubleType)."""
    return [StructField(f"V{i}", DoubleType(), nullable=True) for i in range(1, 340)]


def _c_columns() -> list:
    return [StructField(f"C{i}", DoubleType(), nullable=True) for i in range(1, 15)]


def _d_columns() -> list:
    return [StructField(f"D{i}", DoubleType(), nullable=True) for i in range(1, 16)]


def _m_columns() -> list:
    return [StructField(f"M{i}", StringType(), nullable=True) for i in range(1, 10)]


IEEE_CIS_TRANSACTION_SCHEMA = StructType(
    [
        StructField("TransactionID", LongType(), nullable=False),
        StructField("isFraud", IntegerType(), nullable=True),
        StructField("TransactionDT", LongType(), nullable=False),
        StructField("TransactionAmt", DoubleType(), nullable=False),
        StructField("ProductCD", StringType(), nullable=True),
        StructField("card1", IntegerType(), nullable=True),
        StructField("card2", DoubleType(), nullable=True),
        StructField("card3", DoubleType(), nullable=True),
        StructField("card4", StringType(), nullable=True),
        StructField("card5", DoubleType(), nullable=True),
        StructField("card6", StringType(), nullable=True),
        StructField("addr1", DoubleType(), nullable=True),
        StructField("addr2", DoubleType(), nullable=True),
        StructField("dist1", DoubleType(), nullable=True),
        StructField("dist2", DoubleType(), nullable=True),
        StructField("P_emaildomain", StringType(), nullable=True),
        StructField("R_emaildomain", StringType(), nullable=True),
    ]
    + _c_columns()
    + _d_columns()
    + _m_columns()
    + _v_columns()
)

IEEE_CIS_IDENTITY_SCHEMA = StructType(
    [StructField("TransactionID", LongType(), nullable=False)]
    + [StructField(f"id_{i:02d}", StringType(), nullable=True) for i in range(1, 39)]
    + [
        StructField("DeviceType", StringType(), nullable=True),
        StructField("DeviceInfo", StringType(), nullable=True),
    ]
)

PAYSIM_SCHEMA = StructType([
    StructField("step", IntegerType(), nullable=False),
    StructField("type", StringType(), nullable=False),
    StructField("amount", DoubleType(), nullable=False),
    StructField("nameOrig", StringType(), nullable=False),
    StructField("oldbalanceOrg", DoubleType(), nullable=False),
    StructField("newbalanceOrig", DoubleType(), nullable=False),
    StructField("nameDest", StringType(), nullable=False),
    StructField("oldbalanceDest", DoubleType(), nullable=False),
    StructField("newbalanceDest", DoubleType(), nullable=False),
    StructField("isFraud", IntegerType(), nullable=False),
    StructField("isFlaggedFraud", IntegerType(), nullable=False),
])

SYNTHETIC_STREAM_SCHEMA = StructType([
    StructField("transaction_id", StringType(), nullable=False),
    StructField("customer_id", StringType(), nullable=False),
    StructField("merchant_id", StringType(), nullable=False),
    StructField("amount", DoubleType(), nullable=False),
    StructField("currency", StringType(), nullable=True),
    StructField("channel", StringType(), nullable=True),
    StructField("merchant_category", StringType(), nullable=True),
    StructField("transaction_ts", LongType(), nullable=False),
    StructField("producer_ts", LongType(), nullable=False),
    StructField("lat", DoubleType(), nullable=True),
    StructField("lon", DoubleType(), nullable=True),
    StructField("device_fingerprint", StringType(), nullable=True),
    StructField("is_fraud", IntegerType(), nullable=True),
])

SILVER_TRANSACTION_SCHEMA = StructType([
    StructField("transaction_id", StringType(), nullable=False),
    StructField("customer_id", StringType(), nullable=True),
    StructField("merchant_id", StringType(), nullable=True),
    StructField("transaction_ts", TimestampType(), nullable=False),
    StructField("amount_usd", DoubleType(), nullable=False),
    StructField("currency", StringType(), nullable=True),
    StructField("channel", StringType(), nullable=True),
    StructField("merchant_category", StringType(), nullable=True),
    StructField("card_type", StringType(), nullable=True),
    StructField("is_fraud", IntegerType(), nullable=True),
    StructField("data_source", StringType(), nullable=False),
    StructField("_ingest_ts", TimestampType(), nullable=False),
    StructField("_silver_ts", TimestampType(), nullable=False),
])

GOLD_CUSTOMER_FEATURE_SCHEMA = StructType([
    StructField("customer_id", StringType(), nullable=False),
    StructField("feature_date", TimestampType(), nullable=False),
    StructField("tx_count_1d", LongType(), nullable=True),
    StructField("tx_count_7d", LongType(), nullable=True),
    StructField("tx_count_30d", LongType(), nullable=True),
    StructField("tx_amount_sum_1d", DoubleType(), nullable=True),
    StructField("tx_amount_sum_7d", DoubleType(), nullable=True),
    StructField("tx_amount_sum_30d", DoubleType(), nullable=True),
    StructField("tx_amount_mean_7d", DoubleType(), nullable=True),
    StructField("tx_amount_std_7d", DoubleType(), nullable=True),
    StructField("unique_merchants_7d", LongType(), nullable=True),
    StructField("fraud_rate_30d", DoubleType(), nullable=True),
    StructField("avg_tx_hour_7d", DoubleType(), nullable=True),
    StructField("weekend_ratio_7d", DoubleType(), nullable=True),
])

GOLD_MERCHANT_FEATURE_SCHEMA = StructType([
    StructField("merchant_id", StringType(), nullable=False),
    StructField("feature_date", TimestampType(), nullable=False),
    StructField("tx_count_1d", LongType(), nullable=True),
    StructField("tx_count_7d", LongType(), nullable=True),
    StructField("fraud_count_30d", LongType(), nullable=True),
    StructField("fraud_rate_30d", DoubleType(), nullable=True),
    StructField("avg_amount_7d", DoubleType(), nullable=True),
    StructField("unique_customers_7d", LongType(), nullable=True),
])

GOLD_TRANSACTION_FEATURE_SCHEMA = StructType([
    StructField("transaction_id", StringType(), nullable=False),
    StructField("transaction_ts", TimestampType(), nullable=False),
    StructField("feature_date", TimestampType(), nullable=False),
    StructField("hour_of_day", IntegerType(), nullable=True),
    StructField("day_of_week", IntegerType(), nullable=True),
    StructField("is_weekend", IntegerType(), nullable=True),
    StructField("is_night", IntegerType(), nullable=True),
    StructField("amount_log1p", DoubleType(), nullable=True),
    StructField("amount_bucket", StringType(), nullable=True),
    StructField("amount_zscore", DoubleType(), nullable=True),
    StructField("customer_tx_count_1h", LongType(), nullable=True),
    StructField("customer_tx_count_6h", LongType(), nullable=True),
    StructField("customer_tx_count_24h", LongType(), nullable=True),
    StructField("amount_vs_customer_mean_ratio", DoubleType(), nullable=True),
    StructField("is_new_merchant", IntegerType(), nullable=True),
    StructField("is_round_amount", IntegerType(), nullable=True),
    StructField("is_velocity_spike", IntegerType(), nullable=True),
])
