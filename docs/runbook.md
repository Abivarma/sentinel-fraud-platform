# Sentinel Fraud Platform — Runbook

## Quick Start

### 1. Bootstrap
```bash
make setup            # install deps, copy .env.example → .env
# Edit .env: fill KAGGLE_KEY, HF_TOKEN, GROQ_API_KEY
make download-data    # downloads IEEE-CIS + PaySim (~2GB)
make infra-up         # starts MinIO, Spark, Redpanda, Redis, MLflow
```

### 2. Run Data Layer (first time, ~25 min)
```bash
make phase-03         # bronze ingestion (7M rows, ~8 min)
make phase-04         # silver cleaning (imputes 354 medians, ~12 min)
make phase-05         # streaming infra (120s probe)
make phase-06         # gold features + labels (~4 min)
make phase-07         # feast materialize (~3 min)
# Or run all at once:
make data-layer
```

### 3. Train Models (~10 min)
```bash
make phase-08         # XGBoost (ROC-AUC 0.9945)
make phase-09         # Isolation Forest (unsupervised anomaly)
make phase-10         # LLM explanations (Groq)
```

### 4. Start API + Observability
```bash
make serve            # FastAPI on :8000 (local dev)
# Or with Docker:
docker compose --profile serving up -d fraud-api
docker compose --profile observability up -d prometheus grafana
```

## Service URLs

| Service | URL | Credentials |
|---------|-----|-------------|
| MinIO Console | http://localhost:9001 | sentinel / (see .env MINIO_ROOT_PASSWORD) |
| Spark UI | http://localhost:8080 | — |
| MLflow | http://localhost:5000 | — |
| Redpanda Console | http://localhost:8082 | — |
| Airflow | http://localhost:8081 | admin / admin |
| Fraud API | http://localhost:8000 | — |
| Prometheus | http://localhost:9090 | — |
| Grafana | http://localhost:3000 | admin / sentinel123 |

## Common Troubleshooting

### Spark job fails with S3 connection error
```bash
# Check MinIO is healthy
docker compose ps minio
# Check credentials in .env match MINIO_ROOT_USER/PASSWORD
curl http://localhost:9000/minio/health/live
```

### Phase 04 OOM (Java heap)
The V-column imputation is memory-intensive. Increase driver memory:
```bash
export SPARK_DRIVER_MEMORY=10g
make phase-04
```

### Groq API errors (phase-10)
If model `llama3-8b-8192` is unavailable, update `.env`:
```
GROQ_MODEL=llama-3.3-70b-versatile
```
Check available models: `curl -H "Authorization: Bearer $GROQ_API_KEY" https://api.groq.com/openai/v1/models`

### Feast materialization fails
Ensure Redis is running and Delta tables exist:
```bash
docker compose ps redis
ls -la # verify gold/ tables exist after phase-06
```

## Evidence Verification

Each phase writes evidence to `evidence/phase_NN/`:
- `DONE` — presence means phase completed successfully
- `metrics.json` — numerical PRD metric values
- `alignment_check.md` — PASS/FAIL verdict
- `charts/` — visualizations

Check all phases passed:
```bash
for d in evidence/phase_*/; do
  if [ -f "$d/DONE" ]; then echo "✅ $d"; else echo "❌ $d (no DONE)"; fi
done
```

## Architecture Notes

### Why "local Databricks"?
PySpark + Delta Lake + MLflow mirrors the Databricks stack exactly. Swap MinIO for S3, and `spark://spark-master:7077` for a Databricks cluster URL — the job code is identical.

### Training-serving skew prevention
`pipelines/spark/shared/feature_transforms.py` is imported by BOTH:
- `phase_06_gold_features.py` (batch training features)
- `phase_05_streaming_ingest.py` (real-time feature computation)

The same Python functions produce training labels and live features — eliminating the #1 production ML failure mode.

### Feature store TTL strategy
- `customer_features`: TTL 24h (velocity windows)
- `merchant_features`: TTL 7d (merchant risk profile)
- `transaction_features`: TTL 1h (immediate context)

Online serving uses only `serving_feature_service` (minimal latency subset).
