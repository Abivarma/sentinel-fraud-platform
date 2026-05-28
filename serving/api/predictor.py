import xgboost as xgb
import numpy as np
from pathlib import Path
from .models import RiskLevel

MODEL_PATH = Path(__file__).parent.parent.parent / "ml" / "models" / "xgboost_fraud_v1.json"

FEATURE_COLS = [
    "hour_of_day",
    "day_of_week",
    "is_weekend",
    "is_night",
    "amount_log1p",
    "amount_zscore",
    "is_round_amount",
    "customer_tx_count_1h",
    "customer_tx_count_6h",
    "customer_tx_count_24h",
    "customer_tx_count_7d",
    "fraud_rate_30d",
    "is_velocity_spike",
    "amount_vs_mean_ratio",
]


class FraudPredictor:
    def __init__(self):
        self._booster = None

    def _load(self):
        if self._booster is None:
            booster = xgb.Booster()
            booster.load_model(str(MODEL_PATH))
            self._booster = booster

    def predict(self, features: dict) -> float:
        """Convert feature dict to xgb.DMatrix and return fraud probability (0-1)."""
        self._load()
        row = np.array([[features.get(col, 0.0) for col in FEATURE_COLS]], dtype=np.float32)
        dmat = xgb.DMatrix(row, feature_names=FEATURE_COLS)
        prob = self._booster.predict(dmat)[0]
        return float(prob)

    def predict_batch(self, features_list: list[dict]) -> list[float]:
        """Predict fraud probability for a batch of feature dicts."""
        self._load()
        rows = np.array(
            [[feat.get(col, 0.0) for col in FEATURE_COLS] for feat in features_list],
            dtype=np.float32,
        )
        dmat = xgb.DMatrix(rows, feature_names=FEATURE_COLS)
        probs = self._booster.predict(dmat)
        return [float(p) for p in probs]

    def get_risk_level(self, score: float) -> RiskLevel:
        """Map fraud score to risk level."""
        if score < 0.3:
            return RiskLevel.LOW
        elif score < 0.6:
            return RiskLevel.MEDIUM
        elif score < 0.85:
            return RiskLevel.HIGH
        else:
            return RiskLevel.CRITICAL

    def get_top_risk_factors(self, features: dict, score: float) -> list[str]:
        """Return up to 3 human-readable risk factor strings based on feature values."""
        factors = []

        # Velocity signals
        tx_1h = features.get("customer_tx_count_1h", 0.0)
        tx_24h = features.get("customer_tx_count_24h", 0.0)
        if features.get("is_velocity_spike", 0):
            factors.append(f"High transaction velocity ({int(tx_1h)} tx in 1h)")
        elif tx_1h >= 5:
            factors.append(f"Elevated 1h velocity ({int(tx_1h)} tx in 1h)")

        # Time-of-day
        if features.get("is_night", 0):
            hour = int(features.get("hour_of_day", 0))
            factors.append(f"Night-time transaction ({hour:02d}:00h)")

        # Round amount
        if features.get("is_round_amount", 0):
            # Reconstruct approximate amount from log1p
            amount_log1p = features.get("amount_log1p", 0.0)
            approx_amount = np.expm1(amount_log1p)
            factors.append(f"Round amount (${approx_amount:.2f})")

        # Amount vs baseline
        ratio = features.get("amount_vs_mean_ratio", 1.0)
        if ratio >= 2.0:
            factors.append(f"Amount {ratio:.1f}x above baseline")

        # Fraud rate history
        fraud_rate = features.get("fraud_rate_30d", 0.0)
        if fraud_rate > 0.05:
            factors.append(f"High historical fraud rate ({fraud_rate:.1%} over 30d)")

        # Amount z-score
        zscore = features.get("amount_zscore", 0.0)
        if abs(zscore) > 2.5:
            factors.append(f"Unusual amount (z-score={zscore:.1f})")

        # Ensure at least one factor returned
        if not factors:
            factors.append(f"Anomalous feature combination (score={score:.2f})")

        return factors[:3]


# Module-level singleton — loaded lazily on first call
_predictor_instance: FraudPredictor | None = None


def get_predictor() -> FraudPredictor:
    global _predictor_instance
    if _predictor_instance is None:
        _predictor_instance = FraudPredictor()
        _predictor_instance._load()
    return _predictor_instance


predictor = FraudPredictor()
