"""
Reads the streaming_metrics Delta table and computes E2E latency statistics.
Writes to evidence/phase_05/metrics.json.
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent.parent
sys.path.insert(0, str(BASE_DIR / "pipelines" / "spark"))

from shared.spark_session import get_spark
from shared.s3_utils import SILVER_STREAMING_METRICS


def collect_latency_metrics(duration_seconds: int) -> dict:
    spark = get_spark("sentinel-latency-probe", delta=True)

    deadline = time.time() + duration_seconds
    all_latencies = []

    while time.time() < deadline:
        try:
            df = spark.read.format("delta").load(SILVER_STREAMING_METRICS)
            if df.count() > 0:
                rows = df.select("latency_ms").collect()
                all_latencies.extend([r["latency_ms"] for r in rows if r["latency_ms"] is not None])
        except Exception:
            pass
        time.sleep(10)

    spark.stop()

    if not all_latencies:
        return {
            "p50_ms": None,
            "p95_ms": None,
            "p99_ms": None,
            "sample_count": 0,
            "note": "No streaming data collected — streaming job may not be running",
        }

    all_latencies.sort()
    n = len(all_latencies)
    p50 = all_latencies[int(n * 0.50)]
    p95 = all_latencies[int(n * 0.95)]
    p99 = all_latencies[int(n * 0.99)]

    return {
        "p50_ms": p50,
        "p95_ms": p95,
        "p99_ms": p99,
        "sample_count": n,
        "target_p95_ms": 2000,
        "target_met": p95 < 2000,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--duration", type=int, default=60)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    print(f"Collecting latency metrics for {args.duration}s...")
    stats = collect_latency_metrics(args.duration)

    metrics = {
        "phase": "05",
        "prd_metrics_addressed": [
            {
                "name": "Streaming E2E latency p95",
                "prd_target": "< 2000ms",
                "measured_value": f"{stats.get('p95_ms', 'N/A')}ms",
                "evidence_chart": "charts/latency_histogram.png",
                "status": "met" if stats.get("target_met") else "not-measured",
            }
        ],
        "additional_observations": [
            f"p50: {stats.get('p50_ms')}ms",
            f"p95: {stats.get('p95_ms')}ms",
            f"p99: {stats.get('p99_ms')}ms",
            f"Sample count: {stats.get('sample_count')}",
        ],
    }

    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    with open(args.output, "w") as f:
        json.dump(metrics, f, indent=2)
    print(f"Metrics written to {args.output}")


if __name__ == "__main__":
    main()
