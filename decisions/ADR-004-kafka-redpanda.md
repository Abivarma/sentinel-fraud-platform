# ADR-004 — Message Bus: Kafka / Redpanda

**Date:** 2024-01-01  
**Status:** Accepted  
**PRD reference:** §5, §6 Decision #4

## Context

Need a durable, partitioned message bus for transaction events at 500+ TPS locally. Must be deployable in Docker without heavy infrastructure.

## Decision

Kafka protocol with Redpanda v23.3.21 for local development.  
Production: maps to Confluent Cloud, AWS MSK, or self-managed Kafka.

## Redpanda vs full Apache Kafka

| Property | Apache Kafka | Redpanda |
|---|---|---|
| External dependency | ZooKeeper or KRaft | None — single binary |
| Local Docker containers | 2-3 minimum | 1 |
| Kafka API compatibility | 100% | 100% |
| Performance | Standard | ~10x faster writes |
| Production use | LinkedIn, Uber | DoorDash, Cloudflare |
| Code portability | — | Zero code changes |

## Alternatives considered

| Alternative | Why rejected |
|---|---|
| RabbitMQ | AMQP not Kafka; no consumer group replay |
| AWS Kinesis | Cloud-only |
| Apache Pulsar | Weaker Spark integration |
| Redis Streams | No partitioned consumer groups at scale |

## Consequences

- `confluent-kafka` Python client — works identically on Redpanda and Confluent Cloud.
- Topics: `transactions.raw`, `transactions.enriched`, `transactions.scored` (6 partitions each).
- **Interview moment:** "I run Redpanda locally — it's the Kafka API without ZooKeeper overhead. Same client code runs against Confluent Cloud in production."
