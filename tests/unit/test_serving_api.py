"""
Unit tests for Phase 11: FastAPI real-time fraud scoring API.
Uses FastAPI TestClient with mocked predictor — no real model or Groq needed.
"""
import pytest
from unittest.mock import patch, MagicMock
from fastapi.testclient import TestClient


SAMPLE_TX = {
    "transaction_id": "tx-001",
    "amount": 500.0,
    "hour_of_day": 2,
    "day_of_week": 5,
    "is_weekend": 1,
    "is_night": 1,
    "amount_log1p": 6.215,
    "amount_zscore": 3.1,
    "is_round_amount": 1,
    "customer_tx_count_1h": 8.0,
    "customer_tx_count_6h": 12.0,
    "customer_tx_count_24h": 20.0,
    "customer_tx_count_7d": 40.0,
    "fraud_rate_30d": 0.08,
    "is_velocity_spike": 1,
    "amount_vs_mean_ratio": 3.2,
}


@pytest.fixture
def mock_predictor():
    """Mock FraudPredictor to avoid loading real XGBoost model."""
    with patch("serving.api.routers.score.get_predictor") as mock_get:
        pred = MagicMock()
        pred.predict.return_value = 0.85
        pred.get_risk_level.return_value = "critical"
        pred.get_top_risk_factors.return_value = [
            "High transaction velocity (8 tx in 1h)",
            "Night-time transaction (02:00h)",
            "Round amount ($500.00)",
        ]
        mock_get.return_value = pred
        yield pred


@pytest.fixture
def mock_health_predictor():
    """Mock predictor for health endpoint."""
    with patch("serving.api.routers.health.get_predictor") as mock_get:
        pred = MagicMock()
        mock_get.return_value = pred
        yield pred


@pytest.fixture
def client(mock_predictor, mock_health_predictor):
    """TestClient with mocked dependencies and suppressed lifespan model load."""
    with patch("serving.api.main.get_predictor"):
        from serving.api.main import app
        with TestClient(app) as c:
            yield c


# ─── Health endpoint ─────────────────────────────────────────────────────────

def test_health_returns_200(client):
    resp = client.get("/health")
    assert resp.status_code == 200


def test_health_schema(client):
    resp = client.get("/health")
    data = resp.json()
    assert "status" in data
    assert "model_loaded" in data
    assert "groq_available" in data
    assert "version" in data


def test_health_model_loaded(client):
    resp = client.get("/health")
    data = resp.json()
    # With mock_health_predictor returning without exception, model_loaded=True
    assert data["model_loaded"] is True


# ─── Score endpoint ───────────────────────────────────────────────────────────

def test_score_returns_200(client):
    resp = client.post("/score/", json=SAMPLE_TX)
    assert resp.status_code == 200


def test_score_response_schema(client):
    resp = client.post("/score/", json=SAMPLE_TX)
    data = resp.json()
    assert "transaction_id" in data
    assert "fraud_score" in data
    assert "risk_level" in data
    assert "top_risk_factors" in data
    assert "latency_ms" in data
    assert "model_version" in data


def test_score_fraud_score_in_range(client):
    resp = client.post("/score/", json=SAMPLE_TX)
    data = resp.json()
    assert 0.0 <= data["fraud_score"] <= 1.0


def test_score_risk_level_critical(client, mock_predictor):
    """fraud_score=0.85 → critical risk level."""
    mock_predictor.predict.return_value = 0.85
    mock_predictor.get_risk_level.return_value = "critical"
    resp = client.post("/score/", json=SAMPLE_TX)
    data = resp.json()
    assert data["risk_level"] == "critical"
    assert data["fraud_score"] == pytest.approx(0.85, abs=1e-4)


def test_score_risk_level_low(client, mock_predictor):
    """fraud_score=0.1 → low risk level."""
    mock_predictor.predict.return_value = 0.1
    mock_predictor.get_risk_level.return_value = "low"
    mock_predictor.get_top_risk_factors.return_value = ["Anomalous feature combination (score=0.10)"]
    resp = client.post("/score/", json={**SAMPLE_TX, "is_velocity_spike": 0, "is_night": 0})
    data = resp.json()
    assert data["risk_level"] == "low"


def test_score_risk_level_medium(client, mock_predictor):
    """fraud_score=0.45 → medium risk level."""
    mock_predictor.predict.return_value = 0.45
    mock_predictor.get_risk_level.return_value = "medium"
    mock_predictor.get_top_risk_factors.return_value = ["Amount 3.2x above baseline"]
    resp = client.post("/score/", json=SAMPLE_TX)
    data = resp.json()
    assert data["risk_level"] == "medium"


def test_score_risk_level_high(client, mock_predictor):
    """fraud_score=0.7 → high risk level."""
    mock_predictor.predict.return_value = 0.7
    mock_predictor.get_risk_level.return_value = "high"
    mock_predictor.get_top_risk_factors.return_value = ["Amount 3.2x above baseline", "Night-time transaction (02:00h)"]
    resp = client.post("/score/", json=SAMPLE_TX)
    data = resp.json()
    assert data["risk_level"] == "high"


def test_score_top_risk_factors_list(client):
    resp = client.post("/score/", json=SAMPLE_TX)
    data = resp.json()
    assert isinstance(data["top_risk_factors"], list)
    assert len(data["top_risk_factors"]) >= 1


def test_score_transaction_id_echoed(client):
    resp = client.post("/score/", json=SAMPLE_TX)
    data = resp.json()
    assert data["transaction_id"] == "tx-001"


def test_score_model_version(client):
    resp = client.post("/score/", json=SAMPLE_TX)
    data = resp.json()
    assert data["model_version"] == "xgboost_v1"


def test_score_no_explanation_low_score(client, mock_predictor):
    """Scores <= 0.3 should not trigger explanation even when include_explanation=True."""
    mock_predictor.predict.return_value = 0.2
    mock_predictor.get_risk_level.return_value = "low"
    mock_predictor.get_top_risk_factors.return_value = ["Anomalous feature combination (score=0.20)"]
    resp = client.post("/score/?include_explanation=true", json=SAMPLE_TX)
    data = resp.json()
    # explanation should be None when score <= 0.3 (no Groq call made)
    assert data["explanation"] is None


def test_score_invalid_amount(client):
    """amount must be > 0."""
    bad_tx = {**SAMPLE_TX, "amount": -10.0}
    resp = client.post("/score/", json=bad_tx)
    assert resp.status_code == 422


def test_score_invalid_hour(client):
    """hour_of_day must be 0-23."""
    bad_tx = {**SAMPLE_TX, "hour_of_day": 25}
    resp = client.post("/score/", json=bad_tx)
    assert resp.status_code == 422


def test_score_invalid_day_of_week(client):
    """day_of_week must be 0-6."""
    bad_tx = {**SAMPLE_TX, "day_of_week": 7}
    resp = client.post("/score/", json=bad_tx)
    assert resp.status_code == 422


# ─── Batch endpoint ───────────────────────────────────────────────────────────

def test_batch_score_returns_200(client):
    payload = {"transactions": [SAMPLE_TX, {**SAMPLE_TX, "transaction_id": "tx-002"}]}
    resp = client.post("/score/batch", json=payload)
    assert resp.status_code == 200


def test_batch_score_result_count(client):
    payload = {"transactions": [SAMPLE_TX, {**SAMPLE_TX, "transaction_id": "tx-002"}]}
    resp = client.post("/score/batch", json=payload)
    data = resp.json()
    assert len(data["results"]) == 2


def test_batch_score_fraud_count(client, mock_predictor):
    """Both transactions score 0.85 > 0.5 → fraud_count = 2."""
    mock_predictor.predict.return_value = 0.85
    payload = {"transactions": [SAMPLE_TX, {**SAMPLE_TX, "transaction_id": "tx-002"}]}
    resp = client.post("/score/batch", json=payload)
    data = resp.json()
    assert data["fraud_count"] == 2


def test_batch_score_no_fraud(client, mock_predictor):
    """Both transactions score 0.1 → fraud_count = 0."""
    mock_predictor.predict.return_value = 0.1
    mock_predictor.get_risk_level.return_value = "low"
    mock_predictor.get_top_risk_factors.return_value = ["Anomalous feature combination (score=0.10)"]
    payload = {"transactions": [SAMPLE_TX, {**SAMPLE_TX, "transaction_id": "tx-002"}]}
    resp = client.post("/score/batch", json=payload)
    data = resp.json()
    assert data["fraud_count"] == 0


def test_batch_score_total_latency_present(client):
    payload = {"transactions": [SAMPLE_TX]}
    resp = client.post("/score/batch", json=payload)
    data = resp.json()
    assert "total_latency_ms" in data
    assert data["total_latency_ms"] >= 0.0


def test_batch_score_each_result_has_schema(client):
    payload = {"transactions": [SAMPLE_TX]}
    resp = client.post("/score/batch", json=payload)
    data = resp.json()
    result = data["results"][0]
    assert "transaction_id" in result
    assert "fraud_score" in result
    assert "risk_level" in result
    assert "top_risk_factors" in result


def test_batch_empty_returns_zero_fraud(client):
    """Empty batch → results=[], fraud_count=0."""
    payload = {"transactions": []}
    resp = client.post("/score/batch", json=payload)
    assert resp.status_code == 200
    data = resp.json()
    assert data["results"] == []
    assert data["fraud_count"] == 0


# ─── Prometheus metrics endpoint ──────────────────────────────────────────────

def test_prometheus_metrics_endpoint(client):
    resp = client.get("/metrics-prometheus")
    assert resp.status_code == 200
    # Should contain prometheus text format markers
    assert b"sentinel_api" in resp.content or b"#" in resp.content


def test_metrics_stub_endpoint(client):
    resp = client.get("/metrics")
    assert resp.status_code == 200
    data = resp.json()
    assert "note" in data
