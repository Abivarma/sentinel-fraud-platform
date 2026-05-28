# ADR-009 — Online Feature Store Backend: Redis

**Date:** 2024-01-01  
**Status:** Accepted  
**PRD reference:** §5, §6 Decision #9

## Context

Online feature store needs sub-millisecond read latency during real-time transaction scoring.

## Decision

Redis 7.2 (Alpine).

## Alternatives considered

| Alternative | Why rejected |
|---|---|
| DynamoDB | AWS-only; not free locally |
| Cassandra | Higher latency for point lookups; complex ops |
| Bigtable | GCP-only |
| In-memory dict | Not persistent; not distributed |
| SQLite | Not designed for sub-ms key-value; no TTL |

## Configuration

- `maxmemory 2gb`, `allkeys-lru` eviction — handles cache pressure gracefully
- `save 60 1000` — RDB persistence for restart recovery
- Password-protected; credentials from env vars
- Feast key format: `{project}:{feature_view}:{entity_key}`

## Production mapping

Redis → Redis Enterprise (managed) or AWS ElastiCache for Redis. Zero code changes.

## Consequences

- Feature lookup latency: < 1ms on local Docker network
- TTL on feature views prevents stale features after materialisation gap
- **Interview moment:** "Redis gives sub-millisecond online feature lookup. Combined with Feast materialisation, features served at inference are consistent with what the model trained on."
