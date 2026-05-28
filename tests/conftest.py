"""
Shared pytest fixtures for Sentinel unit and integration tests.
SparkSession uses local[*] master — no Docker required for unit tests.
"""

import os
import pytest
from datetime import datetime, timezone

# Force local mode for tests
os.environ.setdefault("SPARK_MASTER", "local[*]")
os.environ.setdefault("MINIO_ENDPOINT", "http://localhost:9000")
os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testtest")


@pytest.fixture(scope="session")
def spark():
    """Session-scoped local SparkSession with Delta support."""
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).parent.parent / "pipelines" / "spark"))

    from shared.spark_session import get_local_spark
    spark = get_local_spark("sentinel-test", delta=False)
    yield spark
    spark.stop()


@pytest.fixture
def sample_ieee_transactions(spark):
    """100-row IEEE-CIS fixture with known fraud rate and null patterns."""
    import pyspark.sql.functions as F
    from datetime import timedelta

    ref_ts = 1512000000  # 2017-11-30 reference

    rows = []
    for i in range(100):
        is_fraud = 1 if i < 10 else 0  # 10% fraud rate
        rows.append((
            i + 1,          # TransactionID
            is_fraud,       # isFraud
            i * 3600,       # TransactionDT
            float(10 + i * 5),  # TransactionAmt
            "W",            # ProductCD
            str(1000 + i),  # card1
        ))

    schema = "TransactionID long, isFraud int, TransactionDT long, TransactionAmt double, ProductCD string, card1 string"
    df = spark.createDataFrame(rows, schema)
    return df


@pytest.fixture
def sample_paysim(spark):
    """50-row PaySim fixture."""
    rows = []
    for i in range(50):
        is_fraud = 1 if i < 2 else 0  # 4% fraud rate
        rows.append((
            i + 1,          # step
            "PAYMENT",      # type
            float(100 + i * 10),  # amount
            f"C{i:06d}",    # nameOrig
            float(1000),    # oldbalanceOrg
            float(900 + i), # newbalanceOrig
            f"M{i:04d}",    # nameDest
            float(500),     # oldbalanceDest
            float(600 + i), # newbalanceDest
            is_fraud,       # isFraud
            0,              # isFlaggedFraud
        ))

    schema = (
        "step int, type string, amount double, nameOrig string, "
        "oldbalanceOrg double, newbalanceOrig double, nameDest string, "
        "oldbalanceDest double, newbalanceDest double, isFraud int, isFlaggedFraud int"
    )
    return spark.createDataFrame(rows, schema)


@pytest.fixture
def sample_silver(spark):
    """Silver transaction fixture for feature engineering tests."""
    from datetime import datetime, timedelta
    import pyspark.sql.functions as F

    base_ts = datetime(2024, 1, 1, 12, 0, 0)
    rows = []
    for i in range(100):
        ts = base_ts + timedelta(hours=i)
        # Same customer for first 20 rows (for velocity tests)
        customer_id = "C000001" if i < 20 else f"C{i:06d}"
        # New merchant for last 10 rows (for new_merchant test)
        merchant_id = f"M{i:04d}"
        rows.append((
            f"TX{i:06d}",
            customer_id,
            merchant_id,
            ts,
            float(10 + i * 3),  # amount_usd
            1 if i < 5 else 0,  # is_fraud (5%)
            "ieee_cis",
        ))

    schema = "transaction_id string, customer_id string, merchant_id string, transaction_ts timestamp, amount_usd double, is_fraud int, data_source string"
    return spark.createDataFrame(rows, schema)
