"""S3/MinIO path constants and utilities for the Sentinel platform."""

import os

BUCKET = os.environ.get("S3_BUCKET", "lake")


def s3_path(*parts: str) -> str:
    return "s3a://" + "/".join([BUCKET] + list(parts))


BRONZE_TRANSACTIONS_IEEE = s3_path("bronze", "transactions", "ieee_cis")
BRONZE_TRANSACTIONS_PAYSIM = s3_path("bronze", "transactions", "paysim")
BRONZE_IDENTITY_IEEE = s3_path("bronze", "identity", "ieee_cis")

SILVER_TRANSACTIONS = s3_path("silver", "transactions")
SILVER_STREAMING = s3_path("silver", "transactions", "streaming")
SILVER_STREAMING_METRICS = s3_path("silver", "streaming_metrics")

GOLD_CUSTOMER_FEATURES = s3_path("gold", "customer_features")
GOLD_MERCHANT_FEATURES = s3_path("gold", "merchant_features")
GOLD_TRANSACTION_FEATURES = s3_path("gold", "transaction_features")
GOLD_FRAUD_LABELS = s3_path("gold", "fraud_labels")

CHECKPOINTS_STREAMING_INGEST = s3_path("checkpoints", "streaming_ingest")
CHECKPOINTS_STREAMING_INFERENCE = s3_path("checkpoints", "streaming_inference")

FEAST_REGISTRY = s3_path("feast", "registry.db")
