"""Write phase metrics to the evidence store cache."""

import json
import time
from pathlib import Path

METRICS_CACHE_DIR = Path("/tmp/sentinel_metrics")


def write_phase_metrics(phase: str, metrics: dict):
    """
    Write metrics to a well-known temp path.
    scripts/generate_evidence.py reads this and writes to evidence/phase_NN/metrics.json.
    """
    METRICS_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    path = METRICS_CACHE_DIR / f"phase_{phase}.json"
    metrics["phase"] = phase
    metrics["written_at"] = time.time()
    with open(path, "w") as f:
        json.dump(metrics, f, indent=2)


def read_phase_metrics(phase: str) -> dict:
    path = METRICS_CACHE_DIR / f"phase_{phase}.json"
    if path.exists():
        with open(path) as f:
            return json.load(f)
    return {}
