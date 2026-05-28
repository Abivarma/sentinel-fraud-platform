# Architecture — Sentinel Fraud Platform

## Layer diagram

```
┌─────────────────────────────────────────────────────────────────┐
│  INTERACTION LAYER                                              │
│  FastAPI (REST) + minimal HTML dashboard                        │
└───────────────────────────┬─────────────────────────────────────┘
                            │
┌───────────────────────────▼─────────────────────────────────────┐
│  AI ENGINEERING LAYER                                           │
│  ┌──────────────────────────┐  ┌──────────────────────────────┐ │
│  │ Classical ML             │  │ Agentic Investigator         │ │
│  │ XGBoost + IsoForest      │  │ LangGraph: retriever →       │ │
│  │ ensemble (MLflow)        │  │   feature-explainer →        │ │
│  │ RAGAS eval               │  │   similar-case finder →      │ │
│  │ Guardrails: drift+range  │  │   report writer              │ │
│  └──────────────────────────┘  └──────────────────────────────┘ │
└───────────────────────────┬─────────────────────────────────────┘
                            │
┌───────────────────────────▼─────────────────────────────────────┐
│  FEATURE STORE (Feast)                                          │
│  Offline: Delta Lake on MinIO  │  Online: Redis                 │
└───────────────────────────┬─────────────────────────────────────┘
                            │
┌───────────────────────────▼─────────────────────────────────────┐
│  DATA ENGINEERING LAYER                                         │
│  Batch (PySpark + Airflow)        Streaming (Kafka + Spark SS)  │
│  Ingest → Bronze → Silver →       Kafka topic: transactions.raw │
│  Gold → Feast offline store       SS: parse → enrich → score   │
│  Daily retraining DAG             Online feature update         │
│  Storage: Delta Lake on MinIO                                   │
└─────────────────────────────────────────────────────────────────┘
  Observability: Langfuse | MLflow | Prometheus + Grafana | Airflow UI
```

## Key architectural invariant: training-serving parity

`pipelines/spark/shared/feature_transforms.py` is imported by:
- `phase_06_gold_features.py` — batch feature computation for training
- `phase_05_streaming_ingest.py` — streaming feature computation for inference

The same Python code runs in both paths. This eliminates the #1 production ML failure mode.

## Component decisions

See `decisions/` for full ADRs:

| Component | Choice | ADR |
|---|---|---|
| Batch engine | PySpark 3.5.1 | ADR-002 |
| Streaming engine | Spark Structured Streaming | ADR-003 |
| Message bus | Kafka / Redpanda | ADR-004 |
| Table format | Delta Lake 3.2.0 | ADR-005 |
| Object storage | MinIO (S3-compatible) | ADR-006 |
| Orchestration | Apache Airflow 2.9.3 | ADR-007 |
| Feature store | Feast 0.40.1 | ADR-008 |
| Online store | Redis 7.2 | ADR-009 |
