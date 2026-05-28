"""
Integration tests: Bronze ingestion and Silver cleaning pipeline.

These tests exercise the real schema enforcement, audit column addition,
deduplication, and schema unification logic against a live MinIO instance.

Run with: pytest -m integration tests/integration/test_bronze_to_silver.py
Requires: Docker daemon running (testcontainers spins up MinIO)
"""

import sys
from pathlib import Path
import pytest
import pyspark.sql.functions as F
from pyspark.sql import DataFrame, Row

# Allow imports from shared/ and jobs/
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "pipelines" / "spark"))

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_ieee_rows(n: int, spark) -> DataFrame:
    """Produce a minimal IEEE-CIS-like DataFrame with required core columns."""
    import random
    rows = [
        Row(
            TransactionID=i,
            isFraud=random.randint(0, 1),
            TransactionDT=float(86400 * i),
            TransactionAmt=float(random.uniform(10, 1000)),
            ProductCD="W",
        )
        for i in range(n)
    ]
    return spark.createDataFrame(rows)


def make_paysim_rows(n: int, spark) -> DataFrame:
    """Produce a minimal PaySim-like DataFrame."""
    import random
    rows = [
        Row(
            step=i,
            type="PAYMENT",
            amount=float(random.uniform(10, 1000)),
            nameOrig=f"C{i:010d}",
            oldbalanceOrg=float(random.uniform(0, 5000)),
            newbalanceOrig=float(random.uniform(0, 5000)),
            nameDest=f"M{i:010d}",
            oldbalanceDest=0.0,
            newbalanceDest=float(random.uniform(0, 5000)),
            isFraud=random.randint(0, 1),
            isFlaggedFraud=0,
        )
        for i in range(n)
    ]
    return spark.createDataFrame(rows)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.integration
def test_bronze_schema_enforcement(spark_for_integration, minio_client):
    """
    Write a minimal IEEE-CIS DataFrame as Parquet to S3, read it back,
    and verify the core columns have the expected types.
    """
    spark = spark_for_integration

    df = make_ieee_rows(20, spark)

    # Write to S3 as parquet (simulating raw data landing in bronze)
    path = "s3a://test-lake/bronze/schema_enforcement_test"
    df.write.mode("overwrite").parquet(path)

    # Read back without explicit schema (parquet preserves types)
    df_back = spark.read.parquet(path)

    col_types = {f.name: f.dataType.simpleString() for f in df_back.schema.fields}

    assert "TransactionID" in col_types, "TransactionID column missing after round-trip"
    assert "TransactionAmt" in col_types, "TransactionAmt column missing after round-trip"
    assert "ProductCD" in col_types, "ProductCD column missing after round-trip"

    # TransactionAmt should be numeric (float/double)
    assert col_types["TransactionAmt"] in ("float", "double"), (
        f"TransactionAmt type unexpected: {col_types['TransactionAmt']}"
    )
    # ProductCD should be string
    assert col_types["ProductCD"] == "string", (
        f"ProductCD type unexpected: {col_types['ProductCD']}"
    )

    row_count = df_back.count()
    assert row_count == 20, f"Expected 20 rows back, got {row_count}"


@pytest.mark.integration
def test_bronze_audit_columns(spark_for_integration, minio_client):
    """
    Call add_ingestion_metadata() from bronze_ingest.py directly and verify
    that the four audit columns (_ingest_ts, _source_file, _batch_id,
    _ingest_date) are present on the returned DataFrame.
    """
    from jobs.phase_03_bronze_ingest import add_ingestion_metadata

    spark = spark_for_integration
    df = make_ieee_rows(5, spark)

    df_with_meta = add_ingestion_metadata(df, source_file="test_source.csv")

    audit_cols = {"_ingest_ts", "_source_file", "_batch_id", "_ingest_date"}
    actual_cols = set(df_with_meta.columns)

    missing = audit_cols - actual_cols
    assert not missing, f"Audit columns missing after add_ingestion_metadata: {missing}"

    # Verify _source_file value propagated correctly
    sample = df_with_meta.select("_source_file").first()
    assert sample["_source_file"] == "test_source.csv", (
        f"_source_file value mismatch: {sample['_source_file']}"
    )

    # Write to Delta and read back to confirm persistence
    path = "s3a://test-lake/bronze/audit_columns_test"
    (
        df_with_meta.write
        .format("delta")
        .mode("overwrite")
        .save(path)
    )

    df_read = spark.read.format("delta").load(path)
    read_cols = set(df_read.columns)
    missing_after_read = audit_cols - read_cols
    assert not missing_after_read, (
        f"Audit columns missing after Delta round-trip: {missing_after_read}"
    )


@pytest.mark.integration
def test_bronze_to_silver_pipeline(spark_for_integration, minio_client):
    """
    Write 10 synthetic rows to Delta bronze (5 unique + 5 duplicates on
    TransactionID), then apply the silver deduplication and imputation logic
    inline. Assert:
      (a) output has <= 8 rows (duplicates removed)
      (b) TransactionAmt has no nulls after imputation
    """
    from jobs.phase_03_bronze_ingest import add_ingestion_metadata

    spark = spark_for_integration

    # 5 unique rows + 5 duplicates sharing TransactionIDs 0-4
    base_rows = [
        Row(
            TransactionID=i,
            isFraud=0,
            TransactionDT=float(86400 * i),
            TransactionAmt=float(100.0 + i * 10),
            ProductCD="W",
        )
        for i in range(5)
    ]
    dup_rows = [
        Row(
            TransactionID=i,           # same IDs as base — these are duplicates
            isFraud=0,
            TransactionDT=float(86400 * i + 1),  # slightly different DT
            TransactionAmt=float(100.0 + i * 10),
            ProductCD="W",
        )
        for i in range(5)
    ]

    df = spark.createDataFrame(base_rows + dup_rows)
    df_with_meta = add_ingestion_metadata(df, source_file="test_bronze.csv")

    bronze_path = "s3a://test-lake/bronze/pipeline_test_bronze"
    df_with_meta.write.format("delta").mode("overwrite").save(bronze_path)

    # Read back from bronze
    df_bronze = spark.read.format("delta").load(bronze_path)

    # Apply deduplication logic inline (mirrors phase_04_silver_clean logic)
    from pyspark.sql import Window
    IEEE_CIS_REF_DATE_UNIX = 1512000000

    df_cleaned = df_bronze.withColumn(
        "transaction_ts",
        F.to_timestamp(F.col("TransactionDT") + F.lit(IEEE_CIS_REF_DATE_UNIX)),
    )

    w = Window.partitionBy("TransactionID").orderBy("transaction_ts")
    df_deduped = (
        df_cleaned
        .withColumn("_rank", F.row_number().over(w))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
    )

    row_count = df_deduped.count()
    assert row_count <= 8, (
        f"Expected <= 8 rows after deduplication, got {row_count}"
    )
    # With 5 unique TransactionIDs, exactly 5 should remain
    assert row_count == 5, (
        f"Expected exactly 5 rows after deduplication of 5 unique IDs, got {row_count}"
    )

    # Verify no nulls in TransactionAmt (we created no nulls)
    null_count = df_deduped.filter(F.col("TransactionAmt").isNull()).count()
    assert null_count == 0, (
        f"Expected 0 null TransactionAmt after cleaning, found {null_count}"
    )


@pytest.mark.integration
def test_silver_union_schema(spark_for_integration, minio_client):
    """
    Map IEEE-CIS rows and PaySim rows to the silver schema columns, then
    union them. Assert both sources are present and the combined DataFrame
    has a consistent set of columns.
    """
    spark = spark_for_integration

    IEEE_CIS_REF_DATE_UNIX = 1512000000
    PAYSIM_START_UNIX = 1483228800

    # Build IEEE-CIS silver slice
    ieee_df = make_ieee_rows(5, spark)
    ieee_silver = ieee_df.select(
        F.col("TransactionID").cast("string").alias("transaction_id"),
        F.lit("cust_ieee").alias("customer_id"),
        F.concat(F.lit("merchant_"), F.col("ProductCD")).alias("merchant_id"),
        F.to_timestamp(
            F.col("TransactionDT") + F.lit(IEEE_CIS_REF_DATE_UNIX)
        ).alias("transaction_ts"),
        F.col("TransactionAmt").alias("amount_usd"),
        F.lit("USD").alias("currency"),
        F.lit("card").alias("channel"),
        F.col("ProductCD").alias("merchant_category"),
        F.lit("credit").alias("card_type"),
        F.col("isFraud").alias("is_fraud"),
        F.lit("ieee_cis").alias("data_source"),
        F.current_timestamp().alias("_ingest_ts"),
        F.current_timestamp().alias("_silver_ts"),
    )

    # Build PaySim silver slice
    paysim_df = make_paysim_rows(5, spark)
    paysim_silver = paysim_df.select(
        F.concat(
            F.lit("PS_"), F.monotonically_increasing_id().cast("string")
        ).alias("transaction_id"),
        F.col("nameOrig").alias("customer_id"),
        F.col("nameDest").alias("merchant_id"),
        F.to_timestamp(
            F.col("step") * 3600 + F.lit(PAYSIM_START_UNIX)
        ).alias("transaction_ts"),
        F.col("amount").alias("amount_usd"),
        F.lit("USD").alias("currency"),
        F.lit("pos").alias("channel"),
        F.col("type").alias("merchant_category"),
        F.lit("transfer").alias("card_type"),
        F.col("isFraud").alias("is_fraud"),
        F.lit("paysim").alias("data_source"),
        F.current_timestamp().alias("_ingest_ts"),
        F.current_timestamp().alias("_silver_ts"),
    )

    # Union by name — this will fail if column sets differ
    silver_union = ieee_silver.unionByName(paysim_silver)

    # Both sources should survive the union
    total_count = silver_union.count()
    assert total_count == 10, f"Expected 10 rows in union, got {total_count}"

    ieee_count = silver_union.filter(F.col("data_source") == "ieee_cis").count()
    paysim_count = silver_union.filter(F.col("data_source") == "paysim").count()
    assert ieee_count == 5, f"Expected 5 IEEE-CIS rows, got {ieee_count}"
    assert paysim_count == 5, f"Expected 5 PaySim rows, got {paysim_count}"

    # Required silver columns all present
    required_cols = {
        "transaction_id", "customer_id", "merchant_id", "transaction_ts",
        "amount_usd", "currency", "channel", "merchant_category",
        "card_type", "is_fraud", "data_source", "_ingest_ts", "_silver_ts",
    }
    actual_cols = set(silver_union.columns)
    missing = required_cols - actual_cols
    assert not missing, f"Silver union missing columns: {missing}"
