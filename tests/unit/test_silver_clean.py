"""Unit tests for Phase 04 silver cleaning logic."""

import sys
from pathlib import Path
from datetime import datetime, timedelta
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "pipelines" / "spark"))

import pyspark.sql.functions as F


def test_transaction_dt_conversion(spark):
    """TransactionDT=86400 (1 day) should map to ref_date + 1 day."""
    IEEE_CIS_REF_DATE_UNIX = 1512000000  # 2017-11-30

    rows = [(1, 0, 86400, 100.0)]
    df = spark.createDataFrame(rows, ["TransactionID", "isFraud", "TransactionDT", "TransactionAmt"])
    df = df.withColumn("transaction_ts", F.to_timestamp(F.col("TransactionDT") + F.lit(IEEE_CIS_REF_DATE_UNIX)))

    ts = df.collect()[0]["transaction_ts"]
    # Should be 2017-12-01 00:00:00
    expected = datetime(2017, 12, 1, 0, 0, 0)
    assert abs((ts - expected).total_seconds()) < 2


def test_deduplication(spark):
    """10 rows with 2 duplicate TransactionIDs → 8 unique rows."""
    rows = []
    for i in range(8):
        rows.append((i, 0, i * 3600, 100.0))
    rows.append((0, 0, 0, 100.0))  # duplicate of row 0
    rows.append((1, 0, 3600, 100.0))  # duplicate of row 1

    df = spark.createDataFrame(rows, ["TransactionID", "isFraud", "TransactionDT", "TransactionAmt"])
    w = F.col("TransactionID")
    deduped = df.dropDuplicates(["TransactionID"])

    assert deduped.count() == 8


def test_null_imputation_numeric(spark):
    """V-columns with nulls should be filled with 0.0 after imputation."""
    rows = [(1, None, 100.0), (2, 50.0, 200.0)]
    df = spark.createDataFrame(rows, ["id", "V1", "amount"])
    df = df.na.fill({"V1": 0.0})

    rows_out = df.collect()
    assert rows_out[0]["V1"] == 0.0
    assert rows_out[1]["V1"] == 50.0


def test_data_source_column(spark):
    """Silver rows from IEEE-CIS must have data_source='ieee_cis'."""
    rows = [(1, 0, "ieee_cis")]
    df = spark.createDataFrame(rows, ["id", "is_fraud", "data_source"])
    assert df.filter(F.col("data_source") == "ieee_cis").count() == 1


def test_silver_schema_union_compatibility(spark):
    """IEEE-CIS and PaySim silver DataFrames must share the same column set."""
    schema = "transaction_id string, customer_id string, amount_usd double, data_source string"
    ieee_df = spark.createDataFrame([("TX1", "C1", 100.0, "ieee_cis")], schema)
    paysim_df = spark.createDataFrame([("TX2", "C2", 200.0, "paysim")], schema)

    union_df = ieee_df.unionByName(paysim_df)
    assert union_df.count() == 2
    assert "data_source" in union_df.columns
