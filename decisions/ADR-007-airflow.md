# ADR-007 — Orchestration: Apache Airflow

**Date:** 2024-01-01  
**Status:** Accepted  
**PRD reference:** §5, §6 Decision #7

## Context

Need a workflow orchestrator for batch pipeline DAGs with scheduling, retries, and a UI.

## Decision

Apache Airflow 2.9.3.

## Alternatives considered

| Alternative | Why rejected |
|---|---|
| Prefect 2 | Good product but smaller enterprise footprint |
| Dagster | Excellent observability; steeper learning curve; less interview-common |
| Luigi | Older; no UI parity; less active |
| Temporal | Strong for long-running; lower DE interview recognition |
| Cron + scripts | No retry, UI, or dependency management |

## Why Airflow wins for interviews

Every major bank, fintech, and data-heavy tech company runs Airflow or a managed version (Astronomer, Google Cloud Composer, Amazon MWAA). Airflow knowledge transfers directly.

Key patterns demonstrated:
- `SparkSubmitOperator` for PySpark jobs
- `ExternalTaskSensor` for cross-DAG dependencies
- `@task` decorator (TaskFlow API — modern Airflow pattern)
- `LocalExecutor` locally; trivially upgrades to `CeleryExecutor` or `KubernetesExecutor`

## Consequences

- `LocalExecutor` backed by PostgreSQL in Docker Compose.
- **Interview moment:** "Phase 06 gold features uses `ExternalTaskSensor` to wait for both Phase 04 (silver batch) and Phase 05 (streaming infra) — that's cross-DAG dependency management, the canonical Airflow pattern."
