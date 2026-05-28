# ADR-003 — Streaming Engine: Spark Structured Streaming

**Date:** 2024-01-01  
**Status:** Accepted  
**PRD reference:** §5.2, §6 Decision #3

## Context

Need a streaming engine that applies the same feature transformations as the batch pipeline, enabling true training-serving parity.

## Decision

Spark Structured Streaming (part of PySpark 3.5.1).

## Alternatives considered

| Alternative | Why rejected |
|---|---|
| Apache Flink | Excellent but separate codebase from batch; breaks the skew story |
| Kafka Streams | JVM only; no native Delta integration |
| Faust (Python Kafka) | No native Spark integration |
| AWS Kinesis Data Analytics | Cloud-only; not free |

## Consequences

- `feature_transforms.py` imported by both batch and streaming jobs — one code path.
- Exactly-once semantics via checkpoint-based offset management + Delta Lake sink.
- `trigger(processingTime="10 seconds")` balances p95 < 2s latency target with throughput.
- **Interview moment:** "Structured Streaming lets me write one feature library and use it in batch and streaming. Flink would be faster but would require two separate feature implementations."
