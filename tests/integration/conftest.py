"""
Integration test fixtures using testcontainers.
Requires Docker to be running (used in CI/CD and local integration testing).
"""
import pytest
import os
import boto3
from testcontainers.minio import MinioContainer


@pytest.fixture(scope="session")
def minio_container():
    """Start a real MinIO container for the test session."""
    with MinioContainer() as minio:
        yield minio


@pytest.fixture(scope="session")
def minio_client(minio_container):
    """Boto3 S3 client pointing at the test MinIO instance."""
    client = boto3.client(
        "s3",
        endpoint_url=minio_container.get_connection_url(),
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
        region_name="us-east-1",
    )
    # Create test bucket
    client.create_bucket(Bucket="test-lake")
    return client


@pytest.fixture(scope="session")
def spark_for_integration(minio_container):
    """SparkSession configured for test MinIO."""
    from pyspark.sql import SparkSession

    endpoint = minio_container.get_connection_url()

    spark = (
        SparkSession.builder
        .appName("sentinel-integration-tests")
        .master("local[2]")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore")
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.jars.packages",
            "io.delta:delta-spark_2.12:3.2.0,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262",
        )
        .getOrCreate()
    )
    yield spark
    spark.stop()
