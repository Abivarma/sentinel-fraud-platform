"""
PySpark session factory for all Sentinel jobs.
Reads all credentials from environment variables — never hardcoded.
"""

import os
from typing import Optional

from pyspark.sql import SparkSession


DELTA_VERSION = "3.2.0"
HADOOP_AWS_VERSION = "3.3.4"
AWS_SDK_VERSION = "1.12.262"
KAFKA_SPARK_VERSION = "3.5.1"


def get_spark(
    app_name: str,
    delta: bool = True,
    streaming: bool = False,
    local: bool = False,
    extra_conf: Optional[dict] = None,
) -> SparkSession:
    """
    Create or get an existing SparkSession with Delta + S3A configuration.

    Args:
        app_name: Application name shown in Spark UI
        delta: Enable Delta Lake extensions
        streaming: Add Kafka package for Structured Streaming
        local: Use local[*] master (unit tests)
        extra_conf: Additional Spark config overrides
    """
    endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
    access_key = os.environ.get("AWS_ACCESS_KEY_ID", "sentinel")
    secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "")

    if local:
        master = "local[*]"
    else:
        master = os.environ.get("SPARK_MASTER", "spark://spark-master:7077")

    packages = [
        f"io.delta:delta-spark_2.12:{DELTA_VERSION}",
        f"org.apache.hadoop:hadoop-aws:{HADOOP_AWS_VERSION}",
        f"com.amazonaws:aws-java-sdk-bundle:{AWS_SDK_VERSION}",
    ]
    if streaming:
        packages.append(f"org.apache.spark:spark-sql-kafka-0-10_2.12:{KAFKA_SPARK_VERSION}")

    builder = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.jars.packages", ",".join(packages))
    )

    if delta:
        builder = (
            builder
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore")
        )

    builder = (
        builder
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    )

    shuffle_partitions = "4" if local else os.environ.get("SPARK_SHUFFLE_PARTITIONS", "8")
    builder = builder.config("spark.sql.shuffle.partitions", shuffle_partitions)

    if extra_conf:
        for k, v in extra_conf.items():
            builder = builder.config(k, v)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def get_local_spark(app_name: str, delta: bool = True) -> SparkSession:
    """Convenience wrapper for local/test SparkSessions."""
    return get_spark(app_name=app_name, delta=delta, local=True)
