.DEFAULT_GOAL := help
SHELL := /bin/bash

.PHONY: help
help:
	@echo "Sentinel Fraud Platform"
	@echo ""
	@echo "Bootstrap:"
	@echo "  make setup            Install dev deps, copy .env"
	@echo "  make download-data    Download IEEE-CIS + PaySim datasets"
	@echo ""
	@echo "Infrastructure:"
	@echo "  make infra-up         Start MinIO, Spark, Redpanda, Redis, MLflow"
	@echo "  make infra-airflow    Start Airflow stack"
	@echo "  make infra-streaming  Start synthetic producer"
	@echo "  make infra-down       Stop everything + remove volumes"
	@echo "  make infra-status     Show service health"
	@echo ""
	@echo "Phases:"
	@echo "  make phase-00  through  make phase-07"
	@echo "  make data-layer        Run all phases 00-07 in dependency order"
	@echo ""
	@echo "Tests:"
	@echo "  make test-unit         Unit tests"
	@echo "  make test-integration  Integration tests (needs infra-up)"
	@echo "  make test-all          All tests"
	@echo ""
	@echo "Quality:"
	@echo "  make lint  |  make type-check  |  make format"

# ─── Bootstrap ────────────────────────────────────────────────────────────────
.PHONY: setup
setup:
	@cp -n .env.example .env 2>/dev/null && echo "Created .env from .env.example" || echo ".env already exists"
	pip install -r requirements/dev.txt -q
	pip install -r requirements/spark.txt -q
	@echo "Setup complete. Edit .env with your credentials before running phases."

.PHONY: download-data
download-data:
	python scripts/download_datasets.py

# ─── Infrastructure ────────────────────────────────────────────────────────────
.PHONY: infra-up
infra-up:
	@set -a && source .env && set +a && \
	docker compose up -d minio postgres redpanda redis spark-master spark-worker mlflow
	@echo "Waiting for init containers..."
	docker compose up minio-init redpanda-init
	@echo ""
	@echo "Services ready:"
	@echo "  MinIO console: http://localhost:9001  (sentinel / value from .env)"
	@echo "  Spark UI:      http://localhost:8080"
	@echo "  MLflow:        http://localhost:5000"
	@echo "  Redpanda:      localhost:19092 (Kafka API)"

.PHONY: infra-airflow
infra-airflow:
	@set -a && source .env && set +a && \
	docker compose up -d airflow-init
	@echo "Waiting for Airflow DB migration..."
	@docker compose wait airflow-init 2>/dev/null || sleep 30
	@set -a && source .env && set +a && \
	docker compose up -d airflow-webserver airflow-scheduler
	@echo "Airflow ready at http://localhost:8081  (admin / admin)"

.PHONY: infra-streaming
infra-streaming:
	@set -a && source .env && set +a && \
	docker compose --profile streaming up -d synthetic-producer

.PHONY: infra-down
infra-down:
	docker compose --profile streaming down --volumes --remove-orphans

.PHONY: infra-status
infra-status:
	docker compose ps

# ─── Phase execution ─────────────────────────────────────────────────────────
SPARK_PKGS = io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4

define spark-submit
	@set -a && source .env && set +a && \
	docker compose exec spark-master spark-submit \
		--master spark://spark-master:7077 \
		--packages $(SPARK_PKGS) \
		--conf spark.hadoop.fs.s3a.endpoint=$${MINIO_ENDPOINT} \
		--conf spark.hadoop.fs.s3a.access.key=$${AWS_ACCESS_KEY_ID} \
		--conf spark.hadoop.fs.s3a.secret.key=$${AWS_SECRET_ACCESS_KEY} \
		$(1)
endef

.PHONY: phase-00
phase-00:
	@echo "=== Phase 00: Problem Identification ==="
	python scripts/generate_evidence.py --phase 00

.PHONY: phase-01
phase-01:
	@echo "=== Phase 01: Solution Landscape ==="
	python scripts/generate_evidence.py --phase 01

.PHONY: phase-02
phase-02:
	@echo "=== Phase 02: EDA Notebook ==="
	jupyter nbconvert --to notebook --execute \
		--ExecutePreprocessor.timeout=600 \
		notebooks/phase_02_eda.ipynb \
		--output notebooks/phase_02_eda_executed.ipynb
	python scripts/generate_evidence.py --phase 02

.PHONY: phase-03
phase-03:
	@echo "=== Phase 03: Bronze Ingestion ==="
	$(call spark-submit,/opt/spark/jobs/jobs/phase_03_bronze_ingest.py)
	python scripts/generate_evidence.py --phase 03

.PHONY: phase-04
phase-04:
	@echo "=== Phase 04: Silver Cleaning ==="
	$(call spark-submit,/opt/spark/jobs/jobs/phase_04_silver_clean.py)
	python scripts/generate_evidence.py --phase 04

.PHONY: phase-05
phase-05: infra-streaming
	@echo "=== Phase 05: Streaming Infrastructure ==="
	@set -a && source .env && set +a && \
	docker compose exec -d spark-master spark-submit \
		--master spark://spark-master:7077 \
		--packages $(SPARK_PKGS),org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
		--conf spark.hadoop.fs.s3a.endpoint=$${MINIO_ENDPOINT} \
		--conf spark.hadoop.fs.s3a.access.key=$${AWS_ACCESS_KEY_ID} \
		--conf spark.hadoop.fs.s3a.secret.key=$${AWS_SECRET_ACCESS_KEY} \
		/opt/spark/jobs/jobs/phase_05_streaming_ingest.py
	@echo "Streaming job running. Collecting metrics for $${STREAMING_DURATION_SECONDS:-120}s..."
	@sleep $${STREAMING_DURATION_SECONDS:-120}
	python streaming/consumers/latency_probe.py --duration 60 --output evidence/phase_05/metrics.json
	python scripts/generate_evidence.py --phase 05

.PHONY: phase-06
phase-06:
	@echo "=== Phase 06: Gold Feature Engineering ==="
	$(call spark-submit,/opt/spark/jobs/jobs/phase_06_gold_features.py)
	$(call spark-submit,/opt/spark/jobs/jobs/phase_06_gold_labels.py)
	python scripts/generate_evidence.py --phase 06

.PHONY: phase-07
phase-07:
	@echo "=== Phase 07: Feature Store Materialisation ==="
	pip install -r requirements/feast.txt -q
	cd feature_store && python materialize.py
	python scripts/generate_evidence.py --phase 07

.PHONY: data-layer
data-layer: phase-00 phase-01 phase-02 phase-03
	@echo "Running phases 04 and 05 in parallel..."
	$(MAKE) -j2 phase-04 phase-05
	$(MAKE) phase-06
	$(MAKE) phase-07
	@echo "=== Data layer complete ==="

# ─── ML / GenAI Phases ────────────────────────────────────────────────────────
VENV_PYTHON = /opt/sentinel-venv/bin/python

.PHONY: phase-08
phase-08:
	@echo "=== Phase 08: XGBoost Fraud Classifier ==="
	@set -a && source .env && set +a && \
	$(VENV_PYTHON) ml/training/phase_08_train_xgboost.py
	python scripts/generate_evidence.py --phase 08

.PHONY: phase-10
phase-10:
	@echo "=== Phase 10: LLM Fraud Explanations ==="
	@set -a && source .env && set +a && \
	cd genai/explainer && $(VENV_PYTHON) phase_10_explain_flagged.py
	python scripts/generate_evidence.py --phase 10

.PHONY: ml-layer
ml-layer: phase-08 phase-10
	@echo "=== ML + GenAI layer complete ==="

# ─── Tests ────────────────────────────────────────────────────────────────────
.PHONY: test-unit
test-unit:
	pytest tests/unit/ -v --tb=short \
		--cov=pipelines --cov=streaming --cov=feature_store \
		--cov-report=term-missing \
		--junitxml=evidence/test_results_unit.xml

.PHONY: test-integration
test-integration:
	pytest tests/integration/ -v --tb=short \
		--junitxml=evidence/test_results_integration.xml

.PHONY: test-all
test-all: test-unit test-integration

# ─── Code quality ─────────────────────────────────────────────────────────────
.PHONY: lint
lint:
	ruff check pipelines/ streaming/ feature_store/ tests/ scripts/

.PHONY: type-check
type-check:
	mypy pipelines/spark/shared/ pipelines/spark/jobs/ --ignore-missing-imports

.PHONY: format
format:
	black pipelines/ streaming/ feature_store/ tests/ scripts/

.PHONY: clean
clean:
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete 2>/dev/null || true
	rm -rf .pytest_cache .mypy_cache
