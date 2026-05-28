"""
Bootstrap MinIO: create lake bucket and folder structure.
Run after `make infra-up`.
"""

import os
import sys
from pathlib import Path

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError


def get_client():
    endpoint = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
    access_key = os.environ.get("AWS_ACCESS_KEY_ID", "sentinel")
    secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
    if not secret_key:
        print("AWS_SECRET_ACCESS_KEY not set. Copy .env.example to .env and fill values.")
        sys.exit(1)
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def ensure_bucket(client, bucket: str):
    try:
        client.head_bucket(Bucket=bucket)
        print(f"  Bucket '{bucket}' already exists")
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            client.create_bucket(Bucket=bucket)
            print(f"  Created bucket '{bucket}'")
        else:
            raise


def touch(client, bucket: str, prefix: str):
    key = f"{prefix}/.keep"
    try:
        client.head_object(Bucket=bucket, Key=key)
    except ClientError:
        client.put_object(Bucket=bucket, Key=key, Body=b"")


def main():
    print("Sentinel Fraud Platform — MinIO Bootstrap\n")
    client = get_client()
    bucket = os.environ.get("S3_BUCKET", "lake")

    ensure_bucket(client, bucket)
    ensure_bucket(client, "mlflow")

    prefixes = [
        "bronze/transactions/ieee_cis",
        "bronze/transactions/paysim",
        "bronze/identity/ieee_cis",
        "silver/transactions",
        "silver/transactions/streaming",
        "silver/streaming_metrics",
        "gold/customer_features",
        "gold/merchant_features",
        "gold/transaction_features",
        "gold/fraud_labels",
        "feast",
        "checkpoints/streaming_ingest",
        "checkpoints/streaming_inference",
        "spark-warehouse",
        "schemas",
    ]

    print(f"Creating folder structure in s3://{bucket}/...")
    for prefix in prefixes:
        touch(client, bucket, prefix)
        print(f"  OK  {prefix}/")

    schemas_dir = Path(__file__).parent.parent / "data" / "schemas"
    if schemas_dir.exists():
        for schema_file in schemas_dir.glob("*.json"):
            client.upload_file(str(schema_file), bucket, f"schemas/{schema_file.name}")
            print(f"  Uploaded schema: {schema_file.name}")

    print(f"\nMinIO bootstrap complete. Browse at http://localhost:9001")


if __name__ == "__main__":
    main()
