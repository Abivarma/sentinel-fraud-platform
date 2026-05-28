from pydantic import BaseModel, Field
from typing import Optional
from enum import Enum


class ScoreRequest(BaseModel):
    transaction_id: str
    amount: float = Field(gt=0, description="Transaction amount in USD")
    hour_of_day: int = Field(ge=0, le=23)
    day_of_week: int = Field(ge=0, le=6)
    is_weekend: int = Field(ge=0, le=1)
    is_night: int = Field(ge=0, le=1)
    amount_log1p: float
    amount_zscore: float
    is_round_amount: int = Field(ge=0, le=1)
    customer_tx_count_1h: float = 0.0
    customer_tx_count_6h: float = 0.0
    customer_tx_count_24h: float = 0.0
    customer_tx_count_7d: float = 0.0
    fraud_rate_30d: float = 0.0
    is_velocity_spike: int = 0
    amount_vs_mean_ratio: float = 1.0


class RiskLevel(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class ScoreResponse(BaseModel):
    transaction_id: str
    fraud_score: float = Field(ge=0.0, le=1.0, description="Probability of fraud 0-1")
    risk_level: RiskLevel
    explanation: Optional[str] = None
    top_risk_factors: list[str]
    latency_ms: float
    model_version: str = "xgboost_v1"
    explain_latency_ms: Optional[float] = None


class BatchScoreRequest(BaseModel):
    transactions: list[ScoreRequest]
    include_explanation: bool = False


class BatchScoreResponse(BaseModel):
    results: list[ScoreResponse]
    total_latency_ms: float
    fraud_count: int


class HealthResponse(BaseModel):
    status: str
    model_loaded: bool
    groq_available: bool
    version: str = "1.0.0"
