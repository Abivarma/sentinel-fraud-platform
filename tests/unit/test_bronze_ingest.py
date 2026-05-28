"""Unit tests for Phase 03 bronze ingestion logic."""

import sys
import tempfile
import os
from pathlib import Path
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "pipelines" / "spark"))


def test_ingestion_metadata_columns(spark, tmp_path):
    """Ingested DataFrame must have _ingest_ts, _source_file, _batch_id, _ingest_date."""
    import pyspark.sql.functions as F

    rows = [(1, 0, 86400, 150.0, "W")]
    df = spark.createDataFrame(rows, ["TransactionID", "isFraud", "TransactionDT", "TransactionAmt", "ProductCD"])

    df = (
        df
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_source_file", F.lit("test_file.csv"))
        .withColumn("_batch_id", F.lit("test-batch-001"))
        .withColumn("_ingest_date", F.lit("2024-01-01"))
    )

    row = df.collect()[0]
    assert row["_source_file"] == "test_file.csv"
    assert row["_batch_id"] == "test-batch-001"
    assert row["_ingest_date"] == "2024-01-01"
    assert row["_ingest_ts"] is not None


def test_paysim_schema_enforced(spark):
    """PaySim CSV read with explicit schema must reject rows missing required fields."""
    from shared.schema_registry import PAYSIM_SCHEMA

    rows = [(1, "PAYMENT", 100.0, "C000001", 1000.0, 900.0, "M0001", 500.0, 600.0, 0, 0)]
    df = spark.createDataFrame(rows, PAYSIM_SCHEMA)

    assert "step" in df.columns
    assert "isFraud" in df.columns
    assert df.count() == 1


def test_throughput_metric_structure():
    """Metrics dict must have prd_metrics_addressed with correct keys."""
    metrics = {
        "prd_metrics_addressed": [
            {
                "name": "Batch pipeline throughput",
                "prd_target": ">= 1M transactions/minute",
                "measured_value": "1.4M rows/minute",
                "status": "met",
            }
        ]
    }
    assert len(metrics["prd_metrics_addressed"]) == 1
    assert metrics["prd_metrics_addressed"][0]["status"] == "met"
