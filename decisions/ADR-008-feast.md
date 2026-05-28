# ADR-008 — Feature Store: Feast

**Date:** 2024-01-01  
**Status:** Accepted  
**PRD reference:** §5, §6 Decision #8

## Context

Need a feature store providing: (1) offline store for point-in-time correct training retrieval, (2) online store for low-latency inference, (3) single feature definition for both, (4) materialisation pipeline.

## Decision

Feast 0.40.1 with SparkOfflineStore (Delta Lake on MinIO) and RedisOnlineStore.

## How Feast solves training-serving skew

1. Feature views defined once in Python (`feature_views.py`)
2. Offline retrieval (training): `feast.get_historical_features()` — queries Delta with point-in-time correctness
3. Online retrieval (serving): `feast.get_online_features()` — queries Redis in < 1ms
4. Materialisation: `feast materialize-incremental` copies offline → online

Same definitions → same features. Skew is measured and asserted < 1%.

## Alternatives considered

| Alternative | Why rejected |
|---|---|
| Tecton | Enterprise SaaS; not free |
| Hopsworks | Open source but heavier deployment |
| Custom Redis + Parquet | Reinventing what Feast provides |
| Databricks Feature Store | Tied to Databricks platform |
| Vertex AI Feature Store | GCP-only |

## Consequences

- `feature_store.yaml` is the single configuration file.
- `training_feature_service` and `serving_feature_service` are separate bundles.
- Skew < 1% target from PRD §2.1 validated in `notebooks/phase_07_feature_validation.ipynb`.
- **Interview moment:** "I define features once and serve them in offline training and online inference via Feast. The skew measurement proves consistency within 1% — that's the actual hard problem in ML production."
