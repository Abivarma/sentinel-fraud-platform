# ADR-005 — Table Format: Delta Lake

**Date:** 2024-01-01  
**Status:** Accepted  
**PRD reference:** §4.2, §6 Decision #5

## Context

Need a table format for the bronze/silver/gold lakehouse providing ACID, schema enforcement, time travel, and efficient upserts.

## Decision

Delta Lake 3.2.0 (via `delta-spark` Python package).

## Alternatives considered

| Alternative | Why rejected |
|---|---|
| Plain Parquet | No ACID; no time travel; no schema evolution |
| Apache Iceberg | Close second. Rejected because Delta has tighter Spark integration and is the Databricks standard. In a multi-engine shop, Iceberg would be preferred. |
| Apache Hudi | Strong upsert support but more complex; less interview-common |
| ORC | Older; poor Python ecosystem |

## Delta vs Iceberg — the honest trade-off

Iceberg has stronger multi-engine support (Spark + Trino + Snowflake + Flink). Delta is tighter for Spark-only shops (Databricks). This project uses Spark exclusively, so Delta wins. In an interview, demonstrate awareness of both.

## Consequences

- All lakehouse layers are Delta tables on MinIO.
- `S3SingleDriverLogStore` required for Delta on S3-compatible storage — limits to one concurrent writer per table (acceptable for local dev).
- Time travel: `spark.read.format("delta").option("versionAsOf", 0).load(path)` for re-runs.
- **Interview moment:** "Delta Lake gives me ACID and time travel on S3. Any phase can be re-run from a known snapshot — that's what makes reproducibility a first-class property, not an afterthought."
