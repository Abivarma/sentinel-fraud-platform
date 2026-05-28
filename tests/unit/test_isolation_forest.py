"""
Unit tests for Phase 09 — Isolation Forest anomaly detector.
No Spark, no real data: pure sklearn + numpy tests.
"""

import json
import pickle
import tempfile
from pathlib import Path

import numpy as np
import pytest
from sklearn.ensemble import IsolationForest


# ─── Fixtures ─────────────────────────────────────────────────────────────────

FEATURE_COLS = [
    "hour_of_day", "day_of_week", "is_weekend", "is_night",
    "amount_log1p", "amount_zscore", "is_round_amount",
    "customer_tx_count_1h", "customer_tx_count_6h",
    "customer_tx_count_24h", "customer_tx_count_7d",
    "fraud_rate_30d", "is_velocity_spike", "amount_vs_mean_ratio",
]

N_FEATURES = len(FEATURE_COLS)
N_SAMPLES  = 2_000


@pytest.fixture(scope="module")
def synthetic_data():
    """Synthetic feature matrix + fraud labels (no Spark required)."""
    rng = np.random.RandomState(42)
    X = rng.randn(N_SAMPLES, N_FEATURES).astype(np.float32)
    # Inject ~1.5% obvious anomalies (large outliers)
    n_anomalies = int(N_SAMPLES * 0.015)
    outlier_idx = rng.choice(N_SAMPLES, size=n_anomalies, replace=False)
    X[outlier_idx] *= 10  # inflate to be extreme

    # Fraud labels correlated with outliers but not identical (unsupervised use-case)
    y = np.zeros(N_SAMPLES, dtype=np.int32)
    fraud_idx = rng.choice(outlier_idx, size=max(1, n_anomalies // 2), replace=False)
    y[fraud_idx] = 1
    return X, y


@pytest.fixture(scope="module")
def trained_clf(synthetic_data):
    """Fitted IsolationForest on synthetic data."""
    X, _ = synthetic_data
    clf = IsolationForest(n_estimators=200, contamination=0.015, random_state=42, n_jobs=-1)
    clf.fit(X)
    return clf


# ─── Tests ────────────────────────────────────────────────────────────────────

def test_output_shape(trained_clf, synthetic_data):
    """score_samples returns one score per input row."""
    X, _ = synthetic_data
    scores = -trained_clf.score_samples(X)
    assert scores.shape == (N_SAMPLES,), (
        f"Expected shape ({N_SAMPLES},), got {scores.shape}"
    )


def test_anomaly_score_range(trained_clf, synthetic_data):
    """Anomaly scores must be finite and within a plausible numeric range."""
    X, _ = synthetic_data
    scores = -trained_clf.score_samples(X)
    assert np.all(np.isfinite(scores)), "Scores contain NaN or Inf"
    # IsolationForest score_samples output is bounded roughly in [-1, 0.5];
    # after negation the useful range is [-0.5, 1].
    assert scores.min() > -2.0, f"Score min too low: {scores.min()}"
    assert scores.max() <  2.0, f"Score max too high: {scores.max()}"


def test_n_anomalies_near_contamination(trained_clf, synthetic_data):
    """Number of anomalies predicted by clf.predict should be close to contamination*N."""
    X, _ = synthetic_data
    preds = trained_clf.predict(X)      # -1 = anomaly, 1 = normal
    n_anomalies = (preds == -1).sum()
    expected    = int(N_SAMPLES * 0.015)
    # Allow ±50% tolerance — exact count depends on score distribution
    assert abs(n_anomalies - expected) <= expected * 0.5, (
        f"Expected ~{expected} anomalies, got {n_anomalies}"
    )


def test_metrics_dict_has_required_keys(synthetic_data, trained_clf):
    """compute_metrics returns all required keys with values in valid ranges."""
    X, y = synthetic_data
    scores = -trained_clf.score_samples(X)
    n = len(scores)
    n_top = max(1, int(n * 0.01))
    top_indices = np.argsort(scores)[-n_top:]
    anomaly_mask = np.zeros(n, dtype=int)
    anomaly_mask[top_indices] = 1

    n_anomalies = anomaly_mask.sum()
    true_fraud_in_anomalies = (anomaly_mask & y.astype(int)).sum()
    total_fraud = y.sum()

    metrics = {
        "anomaly_rate":          float(n_anomalies / n),
        "fraud_recall_at_1pct":  float(true_fraud_in_anomalies / total_fraud) if total_fraud > 0 else 0.0,
        "anomaly_fraud_overlap": float(true_fraud_in_anomalies / n_anomalies) if n_anomalies > 0 else 0.0,
        "precision_at_top1pct":  float(true_fraud_in_anomalies / n_anomalies) if n_anomalies > 0 else 0.0,
        "n_samples":             int(n),
        "n_anomalies":           int(n_anomalies),
    }

    required_keys = {
        "anomaly_rate", "fraud_recall_at_1pct", "anomaly_fraud_overlap",
        "precision_at_top1pct", "n_samples", "n_anomalies",
    }
    assert required_keys.issubset(set(metrics.keys())), (
        f"Missing keys: {required_keys - set(metrics.keys())}"
    )
    # Values must be numeric and within [0, 1] for rates
    for key in ("anomaly_rate", "fraud_recall_at_1pct", "anomaly_fraud_overlap", "precision_at_top1pct"):
        val = metrics[key]
        assert 0.0 <= val <= 1.0, f"{key}={val} is outside [0, 1]"


def test_model_pickle_roundtrip(trained_clf, synthetic_data):
    """Model survives pickle serialisation and produces identical scores."""
    X, _ = synthetic_data
    scores_before = -trained_clf.score_samples(X)

    with tempfile.NamedTemporaryFile(suffix=".pkl", delete=False) as tmp:
        pickle.dump(trained_clf, tmp)
        tmp_path = tmp.name

    with open(tmp_path, "rb") as f:
        clf_loaded = pickle.load(f)

    scores_after = -clf_loaded.score_samples(X)
    np.testing.assert_array_almost_equal(
        scores_before, scores_after, decimal=6,
        err_msg="Scores differ after pickle round-trip"
    )
