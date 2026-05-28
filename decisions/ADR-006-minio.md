# ADR-006 — Object Storage: MinIO

**Date:** 2024-01-01  
**Status:** Accepted  
**PRD reference:** §4.2, §6 Decision #6

## Context

Need S3-compatible local object storage for Delta Lake files, MLflow artifacts, and Feast registry. Zero cloud cost.

## Decision

MinIO RELEASE.2024-11-07T00-52-20Z.

## Why S3-compatible matters

The entire platform uses `s3a://` URIs and Hadoop S3A filesystem. Swapping MinIO for AWS S3 / GCS / ADLS requires changing one environment variable. All Spark jobs, Feast configs, and MLflow tracking use `s3a://lake/...` paths — identical in local dev and production.

## Alternatives considered

| Alternative | Why rejected |
|---|---|
| Local filesystem only | Not S3-compatible; code changes for production |
| LocalStack | S3-compatible but heavier; MinIO is purpose-built |
| Cloud S3 | Not free; cloud dependency for local dev |

## Consequences

- MinIO console at `http://localhost:9001` for browsing lake contents.
- `minio-init` creates the `lake` bucket at startup.
- **Interview moment:** "Everything uses `s3a://` URIs. Swapping MinIO for S3 is one environment variable — that's what cloud-portable actually means."
