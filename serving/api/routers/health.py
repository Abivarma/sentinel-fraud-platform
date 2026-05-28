from fastapi import APIRouter
import os
from ..models import HealthResponse

router = APIRouter(tags=["ops"])


@router.get("/health", response_model=HealthResponse)
async def health():
    try:
        from ..predictor import get_predictor
        get_predictor()
        model_ok = True
    except Exception:
        model_ok = False
    groq_ok = bool(os.environ.get("GROQ_API_KEY"))
    return HealthResponse(
        status="ok" if model_ok else "degraded",
        model_loaded=model_ok,
        groq_available=groq_ok,
    )


@router.get("/metrics")
async def metrics():
    """Prometheus-format metrics endpoint."""
    # In production this would use prometheus_client
    return {"note": "use /metrics-prometheus for Prometheus format"}
