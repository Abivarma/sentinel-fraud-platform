# Sentinel Fraud Platform

**Real-Time Financial Transaction Fraud & Anomaly Intelligence Platform**

A production-grade data + ML platform built with the same open-source stack used at Netflix, Uber, Stripe, and Databricks. Covers the full ML lifecycle — streaming ingestion, feature engineering, supervised classification, unsupervised anomaly detection, LLM-powered explanations, and real-time serving — all running locally in Docker Compose.

---

## Architecture

```
Raw Data (IEEE-CIS + PaySim)
        │
        ▼
┌──────────────────┐     ┌─────────────────────┐
│  Bronze Layer    │     │  Streaming Layer     │
│  (Delta / MinIO) │     │  Redpanda → Spark    │
│  7M rows, audit  │     │  p95 latency: 847ms  │
└────────┬─────────┘     └──────────┬──────────┘
         │                          │
         ▼                          ▼
┌──────────────────────────────────────────────┐
│              Silver Layer                    │
│  Cleaned, unified schema · 354 imputations  │
│  6.95M rows · TransactionDT decoded         │
└────────────────────┬─────────────────────────┘
                     │
                     ▼
┌──────────────────────────────────────────────┐
│              Gold Layer                      │
│  14 engineered features · Velocity windows  │
│  Shared batch + streaming code (no skew)    │
└──────┬───────────────────────────────────────┘
       │
       ├──────────────────────────┐
       ▼                          ▼
┌─────────────┐          ┌──────────────────┐
│  Feast      │          │  XGBoost         │
│  Feature    │          │  ROC-AUC 0.9945  │
│  Store      │          │  Recall 99.1%    │
│  (Redis)    │          └────────┬─────────┘
└─────────────┘                   │
                                  ├──────────────────┐
                                  ▼                  ▼
                         ┌──────────────┐  ┌────────────────┐
                         │  Isolation   │  │  Groq LLM      │
                         │  Forest      │  │  Explanations  │
                         │  (novel      │  │  519ms avg     │
                         │  anomalies)  │  └───────┬────────┘
                         └──────────────┘          │
                                                   ▼
                                        ┌─────────────────────┐
                                        │  FastAPI Serving    │
                                        │  /score · /batch    │
                                        │  Prometheus metrics │
                                        └─────────────────────┘
```

---

## Tech Stack — Interview Mapping

| Tool | Version | Used at Enterprise Scale |
|------|---------|--------------------------|
| **Apache Spark (PySpark)** | 3.5.1 | Netflix, Uber, LinkedIn, Databricks |
| **Delta Lake** | 3.2.0 | Databricks, Netflix, Comcast |
| **Apache Kafka (Redpanda)** | v23.3.21 | LinkedIn, Stripe, Uber |
| **Feast Feature Store** | 0.40.1 | Gojek, DoorDash, Twitter |
| **Redis** | 7.2 | Used at nearly every company |
| **MinIO** | 2024-11-07 | S3-compatible — swap for AWS/GCS/ADLS with zero code change |
| **Apache Airflow** | 2.9.3 | Airbnb (created it), LinkedIn, Twitter |
| **MLflow** | 2.13.2 | Databricks-backed, industry standard |
| **XGBoost** | 2.0.3 | Industry standard for tabular fraud detection |
| **FastAPI** | 0.111.0 | High-performance ML serving |
| **Prometheus + Grafana** | v2.52.0 / 10.4.3 | Standard observability stack |

> **The "local Databricks" pitch:** PySpark + Delta Lake + MLflow mirrors Databricks exactly. Swap MinIO for S3 and the Docker Spark cluster for Databricks — the job code is unchanged.

---

## PRD Targets — All Met

| Metric | Target | Achieved | Phase |
|--------|--------|----------|-------|
| Batch throughput | ≥ 1M txn/min | **6.3M rows/min** | [Phase 03](evidence/phase_03/metrics.json) |
| Streaming E2E latency p95 | < 2000ms | **847ms** | [Phase 05](evidence/phase_05/metrics.json) |
| Feature freshness | < 30s | **18s** (incremental) | [Phase 07](evidence/phase_07/metrics.json) |
| Training-serving skew | < 1% | **0.23%** (max across 14 features) | [Phase 07](evidence/phase_07/metrics.json) |
| Model quality | > 0.99 ROC-AUC | **0.9945** | [Phase 08](evidence/phase_08/metrics.json) |
| Fraud recall @ t=0.3 | > 95% | **99.1%** | [Phase 08](evidence/phase_08/metrics.json) |
| LLM explanation latency | < 2s | **519ms avg** | [Phase 10](evidence/phase_10/metrics.json) |
| API serving latency | < 1s | **~12ms** (model) + ~500ms (Groq) | [Phase 11](evidence/phase_11/metrics.json) |

---

## Notebooks

| Notebook | Description | Key Findings |
|----------|-------------|--------------|
| [Phase 02 — EDA](notebooks/phase_02_eda.ipynb) | IEEE-CIS + PaySim exploratory analysis | Class imbalance (0.415%), peak fraud at 3am, 163/339 V-columns >50% null |
| [Phase 07 — Feature Validation](notebooks/phase_07_feature_validation.ipynb) | Offline vs online feature skew check | Max skew 0.23% across 14 features (PRD target: <1%) |

---

## Phase Walk-Through

### Data Layer

| Phase | Script | What it does |
|-------|--------|--------------|
| [00](evidence/phase_00/summary.md) | `scripts/generate_evidence.py --phase 00` | Problem identification, fraud landscape |
| [01](evidence/phase_01/summary.md) | `scripts/generate_evidence.py --phase 01` | Solution architecture, tool selection |
| [02](evidence/phase_02/alignment_check.md) | `notebooks/phase_02_eda.ipynb` | Exploratory data analysis |
| [03](evidence/phase_03/alignment_check.md) | `pipelines/spark/jobs/phase_03_bronze_ingest.py` | Raw → Bronze Delta tables |
| [04](evidence/phase_04/alignment_check.md) | `pipelines/spark/jobs/phase_04_silver_clean.py` | Bronze → Silver (clean + unify) |
| [05](evidence/phase_05/alignment_check.md) | `pipelines/spark/jobs/phase_05_streaming_ingest.py` | Kafka → Delta streaming |
| [06](evidence/phase_06/alignment_check.md) | `pipelines/spark/jobs/phase_06_gold_features.py` | Silver → Gold features |
| [07](evidence/phase_07/alignment_check.md) | `feature_store/materialize.py` | Feast apply + materialize |

### ML + GenAI Layer

| Phase | Script | What it does |
|-------|--------|--------------|
| [08](evidence/phase_08/alignment_check.md) | `ml/training/phase_08_train_xgboost.py` | XGBoost fraud classifier |
| [09](evidence/phase_09/alignment_check.md) | `ml/training/phase_09_isolation_forest.py` | Isolation Forest anomaly detection |
| [10](evidence/phase_10/alignment_check.md) | `genai/explainer/phase_10_explain_flagged.py` | LLM explanations via Groq |

### Serving + Observability

| Phase | Code | What it does |
|-------|------|--------------|
| [11](evidence/phase_11/alignment_check.md) | `serving/api/main.py` | FastAPI real-time scoring API |
| [12](evidence/phase_12/alignment_check.md) | `observability/` | Prometheus + Grafana dashboards |

---

## Architecture Decision Records

All major tool choices are documented with problem statement, alternatives considered, and rationale:

| ADR | Decision |
|-----|----------|
| [ADR-001](decisions/ADR-001-domain-choice-finance.md) | Domain: Financial fraud detection |
| [ADR-002](decisions/ADR-002-pyspark-batch-engine.md) | Batch engine: Apache Spark |
| [ADR-003](decisions/ADR-003-spark-structured-streaming.md) | Streaming: Spark Structured Streaming |
| [ADR-004](decisions/ADR-004-kafka-redpanda.md) | Message bus: Redpanda (Kafka protocol) |
| [ADR-005](decisions/ADR-005-delta-lake.md) | Table format: Delta Lake |
| [ADR-006](decisions/ADR-006-minio.md) | Object store: MinIO (S3-compatible) |
| [ADR-007](decisions/ADR-007-airflow.md) | Orchestration: Apache Airflow |
| [ADR-008](decisions/ADR-008-feast.md) | Feature store: Feast |
| [ADR-009](decisions/ADR-009-redis.md) | Online store: Redis |

---

## Quick Start

```bash
# 1. Bootstrap
make setup            # install deps, copy .env.example → .env
# Edit .env: fill KAGGLE_KEY, HF_TOKEN, GROQ_API_KEY
make download-data    # downloads IEEE-CIS (~500MB) + PaySim (~470MB)

# 2. Start infrastructure
make infra-up         # MinIO, Spark, Redpanda, Redis, MLflow

# 3. Run data pipeline (~25 min total)
make data-layer       # phases 03 → 04 → 05/06 (parallel) → 07

# 4. Train models
make ml-layer         # phase-08 (XGBoost) + phase-09 (IsolationForest) + phase-10 (LLM)

# 5. Serve + observe
make serve                          # FastAPI on :8000 (dev mode)
make infra-observability            # Prometheus :9090, Grafana :3000
```

### Test the API

```bash
curl -X POST http://localhost:8000/score/ \
  -H "Content-Type: application/json" \
  -d '{
    "transaction_id": "tx-test-001",
    "amount": 999.99,
    "hour_of_day": 3,
    "day_of_week": 6,
    "is_weekend": 1,
    "is_night": 1,
    "amount_log1p": 6.908,
    "amount_zscore": 4.2,
    "is_round_amount": 0,
    "customer_tx_count_1h": 12.0,
    "customer_tx_count_24h": 35.0,
    "customer_tx_count_7d": 80.0,
    "customer_tx_count_6h": 18.0,
    "fraud_rate_30d": 0.12,
    "is_velocity_spike": 1,
    "amount_vs_mean_ratio": 5.1
  }'
```

---

## Service URLs

| Service | URL | Credentials |
|---------|-----|-------------|
| MinIO Console | http://localhost:9001 | `sentinel` / see `.env` |
| Spark UI | http://localhost:8080 | — |
| MLflow | http://localhost:5000 | — |
| Airflow | http://localhost:8081 | `admin` / `admin` |
| Fraud API | http://localhost:8000 | — |
| API Docs | http://localhost:8000/docs | — |
| Prometheus | http://localhost:9090 | — |
| Grafana | http://localhost:3000 | `admin` / `sentinel123` |

---

## Tests

```bash
make test-unit         # 57 unit tests (no Docker needed)
make test-integration  # 15 integration tests (needs make infra-up)
make test-all          # both
```

Test coverage: schema validation · feature transforms · bronze/silver/gold pipeline · Isolation Forest · FastAPI scoring API · Feast config · streaming schema

---

## Repository Structure

```
sentinel-fraud-platform/
├── decisions/          # 9 Architecture Decision Records
├── docs/               # architecture.md · data_dictionary.md · runbook.md
├── evidence/           # phase_00 … phase_12 (metrics.json, DONE, charts/)
├── notebooks/
│   ├── phase_02_eda.ipynb                  # EDA: imbalance, temporal, V-columns
│   └── phase_07_feature_validation.ipynb  # offline vs online skew check
├── pipelines/
│   ├── spark/jobs/     # Bronze · Silver · Gold · Streaming Spark jobs
│   ├── spark/shared/   # feature_transforms.py (shared batch+stream)
│   └── airflow/dags/   # DAG per phase with ExternalTaskSensor
├── ml/
│   ├── training/       # phase_08_train_xgboost.py · phase_09_isolation_forest.py
│   └── models/         # xgboost_fraud_v1.json
├── genai/explainer/    # Groq LLM explanation pipeline
├── serving/api/        # FastAPI app · predictor · explainer client
├── feature_store/      # Feast feature repo (entities, views, services)
├── streaming/          # Kafka producer (Faker) · latency probe
├── observability/      # Prometheus config · Grafana dashboard JSON
├── tests/
│   ├── unit/           # 57 tests — no external deps
│   └── integration/    # 15 tests — testcontainers (MinIO)
└── docker-compose.yml  # All 13 services
```

---

## Key Design Decisions

### Training-Serving Skew Prevention
`pipelines/spark/shared/feature_transforms.py` is imported by **both** the batch Gold job and the streaming ingest job. The same Python functions produce training features and live inference features — eliminating the #1 ML production failure mode.

### Two-Signal Fraud Detection
- **XGBoost** (supervised): catches known fraud patterns from labeled data (ROC-AUC 0.9945)
- **Isolation Forest** (unsupervised): flags statistical outliers — catches *novel* attack vectors the labeled data hasn't seen

### Explainability by Design
Every high-risk transaction (score > 0.3) gets a natural-language explanation via Groq (llama-3.3-70b-versatile) with automatic rule-based fallback if the API is unavailable.

---

## Docs

- [Architecture Overview](docs/architecture.md)
- [Data Dictionary](docs/data_dictionary.md)
- [Operations Runbook](docs/runbook.md)
- [Data Layer Summary](evidence/data_layer_summary.md)
