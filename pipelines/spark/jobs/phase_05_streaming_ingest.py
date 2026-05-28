"""
Phase 05 — Streaming Ingestion

Spark Structured Streaming job that:
1. Reads from Kafka topic transactions.raw
2. Parses JSON using SYNTHETIC_STREAM_SCHEMA
3. Applies shared feature transforms (same as batch — prevents skew)
4. Writes enriched events to silver/transactions/streaming Delta table
5. Records latency metrics to silver/streaming_metrics Delta table

Runs in parallel with Phase 04 (silver batch cleaning).
Exactly-once semantics via Delta + checkpoint.
"""

import os
import sys
import time
from pathlib import Path

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

sys.path.insert(0, str(Path(__file__).parent.parent))

from shared.spark_session import get_spark
from shared.schema_registry import SYNTHETIC_STREAM_SCHEMA
from shared.s3_utils import SILVER_STREAMING, SILVER_STREAMING_METRICS, CHECKPOINTS_STREAMING_INGEST
from shared.feature_transforms import apply_all_streaming_transforms

KAFKA_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC_RAW", "transactions.raw")
TRIGGER_INTERVAL = os.environ.get("STREAMING_TRIGGER_INTERVAL", "10 seconds")

# Pre-computed global stats from Phase 02 EDA
# These are broadcast to streaming job to avoid training-serving skew in z-score
GLOBAL_AMOUNT_MEAN = 100.0
GLOBAL_AMOUNT_STD = 250.0


def process_batch(batch_df, batch_id: int):
    """Process each micro-batch: add latency metrics."""
    if batch_df.count() == 0:
        return

    # Compute latency for this batch
    now_ms = int(time.time() * 1000)
    batch_df = batch_df.withColumn(
        "latency_ms",
        F.lit(now_ms) - F.col("producer_ts"),
    )

    # Write enriched events
    (
        batch_df.drop("producer_ts")
        .write.format("delta")
        .mode("append")
        .save(SILVER_STREAMING)
    )

    # Write latency metrics
    metrics_df = batch_df.select(
        F.lit(batch_id).alias("batch_id"),
        F.col("latency_ms"),
        F.current_timestamp().alias("recorded_at"),
    )
    (
        metrics_df.write.format("delta")
        .mode("append")
        .save(SILVER_STREAMING_METRICS)
    )


def main():
    print("=" * 60)
    print("Phase 05 — Streaming Ingestion (Spark Structured Streaming)")
    print("=" * 60)

    spark = get_spark("sentinel-streaming-ingest", delta=True, streaming=True)

    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", 10000)
        .option("kafka.group.id", "sentinel-streaming-ingest")
        .load()
    )

    # Parse JSON value using explicit schema — never inferSchema on stream
    parsed = (
        raw_stream
        .select(F.from_json(F.col("value").cast("string"), SYNTHETIC_STREAM_SCHEMA).alias("data"))
        .select("data.*")
        .filter(F.col("transaction_id").isNotNull())
    )

    # Convert Unix ms timestamp to TimestampType
    enriched = parsed.withColumn(
        "transaction_ts",
        F.to_timestamp(F.col("transaction_ts") / 1000),
    ).withColumnRenamed("amount", "amount_usd")

    # Apply shared feature transforms (same code as batch — prevents skew)
    enriched = apply_all_streaming_transforms(
        enriched,
        global_amount_mean=GLOBAL_AMOUNT_MEAN,
        global_amount_std=GLOBAL_AMOUNT_STD,
    )

    query = (
        enriched
        .writeStream
        .foreachBatch(process_batch)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .option("checkpointLocation", CHECKPOINTS_STREAMING_INGEST)
        .start()
    )

    print(f"Streaming query started. Trigger: {TRIGGER_INTERVAL}")
    print(f"Output: {SILVER_STREAMING}")
    print(f"Checkpoint: {CHECKPOINTS_STREAMING_INGEST}")
    print("Press Ctrl+C to stop.")

    query.awaitTermination()


if __name__ == "__main__":
    main()
