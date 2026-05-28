"""
Feast data source definitions.
All sources point to gold Delta tables on MinIO via SparkSource.
"""

import os

from feast.infra.offline_stores.contrib.spark_offline_store.spark_source import SparkSource

BUCKET = os.environ.get("S3_BUCKET", "lake")


def s3(path: str) -> str:
    return f"s3a://{BUCKET}/{path}"


customer_features_source = SparkSource(
    name="customer_features_source",
    path=s3("gold/customer_features"),
    file_format="delta",
    timestamp_field="feature_date",
    description="Customer-level rolling features from gold Delta table",
)

merchant_features_source = SparkSource(
    name="merchant_features_source",
    path=s3("gold/merchant_features"),
    file_format="delta",
    timestamp_field="feature_date",
    description="Merchant-level rolling features from gold Delta table",
)

transaction_features_source = SparkSource(
    name="transaction_features_source",
    path=s3("gold/transaction_features"),
    file_format="delta",
    timestamp_field="feature_date",
    description="Transaction-level point-in-time features from gold Delta table",
)
