# ADR-002 — Batch Engine: Apache PySpark

**Date:** 2024-01-01  
**Status:** Accepted  
**PRD reference:** §5, §6 Decision #2

## Context

Need a batch processing engine for ingesting 6M+ transactions, computing feature aggregations with multi-window joins, and building training datasets. Must scale beyond single-machine memory and support streaming parity.

## Decision

Apache PySpark 3.5.1.

## Alternatives considered

| Alternative | Why rejected |
|---|---|
| pandas | Memory-bound; 6M rows with 394 features exceeds typical RAM |
| Dask | Better than pandas at scale but no Structured Streaming; breaks parity story |
| DuckDB | Excellent analytics but no streaming engine; requires two separate codebases |
| Polars | Fast but no Structured Streaming |
| Ray | Strong for ML but weaker DE ecosystem |

## Consequences

- **Training-serving parity:** Same `feature_transforms.py` used by batch (Phase 06) and streaming (Phase 05). This is the single most important technical talking point.
- **Production mapping:** Databricks, EMR, Dataproc all run PySpark. Zero translation needed.
- **Interview moment:** "I chose PySpark because it's the only OSS engine that runs the same transformation code in both batch and micro-batch streaming — that eliminates training-serving skew by construction."
