"""
Sentinel Fraud Platform — Real-Time Scoring API
Phase 11: FastAPI serving layer for XGBoost + LLM explanations.
"""
import os
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
from starlette.responses import Response

from .routers import score, health
from .predictor import get_predictor

REQUEST_COUNT = Counter(
    "sentinel_api_requests_total",
    "Total API requests",
    ["method", "endpoint", "status"],
)
REQUEST_LATENCY = Histogram(
    "sentinel_api_request_duration_seconds",
    "Request latency",
    ["endpoint"],
    buckets=[0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0],
)
FRAUD_SCORE_HIST = Histogram(
    "sentinel_fraud_score",
    "Fraud score distribution",
    buckets=[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0],
)
FRAUD_ALERTS = Counter("sentinel_fraud_alerts_total", "Transactions scored above 0.5")


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        get_predictor()
        print("Model loaded at startup")
    except Exception as e:
        print(f"Model load failed: {e}")
    yield


app = FastAPI(
    title="Sentinel Fraud Scoring API",
    description="Real-time fraud detection with XGBoost + LLM explanations",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def metrics_middleware(request: Request, call_next):
    t0 = time.time()
    response = await call_next(request)
    latency = time.time() - t0
    endpoint = request.url.path
    REQUEST_COUNT.labels(
        method=request.method,
        endpoint=endpoint,
        status=response.status_code,
    ).inc()
    REQUEST_LATENCY.labels(endpoint=endpoint).observe(latency)
    return response


@app.get("/metrics-prometheus")
async def prometheus_metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


app.include_router(health.router)
app.include_router(score.router)
