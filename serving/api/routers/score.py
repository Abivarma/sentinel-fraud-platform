from fastapi import APIRouter, HTTPException
import time
from ..models import ScoreRequest, ScoreResponse, BatchScoreRequest, BatchScoreResponse, RiskLevel
from ..predictor import FraudPredictor, get_predictor
from ..explainer_client import explain

router = APIRouter(prefix="/score", tags=["scoring"])


@router.post("/", response_model=ScoreResponse)
async def score_transaction(req: ScoreRequest, include_explanation: bool = True):
    t0 = time.time()
    pred = get_predictor()
    features = req.model_dump(exclude={"transaction_id", "amount"})
    fraud_score = pred.predict(features)
    risk_level = pred.get_risk_level(fraud_score)
    top_factors = pred.get_top_risk_factors(features, fraud_score)

    explanation = None
    explain_ms = None
    if include_explanation and fraud_score > 0.3:
        explanation, explain_ms = explain(req, fraud_score, risk_level, top_factors)

    total_ms = (time.time() - t0) * 1000
    return ScoreResponse(
        transaction_id=req.transaction_id,
        fraud_score=float(fraud_score),
        risk_level=risk_level,
        explanation=explanation,
        top_risk_factors=top_factors,
        latency_ms=total_ms,
        explain_latency_ms=explain_ms,
    )


@router.post("/batch", response_model=BatchScoreResponse)
async def score_batch(req: BatchScoreRequest):
    t0 = time.time()
    pred = get_predictor()
    results = []
    fraud_count = 0
    for tx in req.transactions:
        features = tx.model_dump(exclude={"transaction_id", "amount"})
        score = pred.predict(features)
        risk = pred.get_risk_level(score)
        factors = pred.get_top_risk_factors(features, score)
        explanation, explain_ms = None, None
        if req.include_explanation and score > 0.3:
            explanation, explain_ms = explain(tx, score, risk, factors)
        if score > 0.5:
            fraud_count += 1
        results.append(
            ScoreResponse(
                transaction_id=tx.transaction_id,
                fraud_score=float(score),
                risk_level=risk,
                explanation=explanation,
                top_risk_factors=factors,
                latency_ms=0.0,  # individual not tracked in batch
                explain_latency_ms=explain_ms,
            )
        )
    return BatchScoreResponse(
        results=results,
        total_latency_ms=(time.time() - t0) * 1000,
        fraud_count=fraud_count,
    )
