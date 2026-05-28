"""
Integration tests: Streaming pipeline schema validation and latency metrics.

These tests do NOT require a real Kafka/Redpanda container. They validate:
  - Synthetic stream event schema parsing
  - Latency percentile calculation logic extracted from latency_probe.py

Run with: pytest -m integration tests/integration/test_streaming_pipeline.py
"""

import sys
import json
from pathlib import Path
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "pipelines" / "spark"))

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_stream_event(**overrides) -> dict:
    """
    Build a valid SYNTHETIC_STREAM_SCHEMA event as a Python dict.
    All non-nullable fields are populated; nullable fields default to None.
    """
    base = {
        "transaction_id": "TX_STREAM_001",
        "customer_id": "CUST_001",
        "merchant_id": "MERCH_001",
        "amount": 250.75,
        "currency": "USD",
        "channel": "mobile",
        "merchant_category": "retail",
        "transaction_ts": 1686822000,       # unix epoch seconds
        "producer_ts": 1686822001,
        "lat": 37.7749,
        "lon": -122.4194,
        "device_fingerprint": "fp_abc123",
        "is_fraud": 0,
    }
    base.update(overrides)
    return base


def _compute_latency_percentiles(pairs: list) -> dict:
    """
    Standalone percentile calculator that mirrors the logic in
    streaming/consumers/latency_probe.py::collect_latency_metrics.

    pairs: list of (produce_ts_ms, consume_ts_ms) where both are epoch-ms.
    Returns dict with p50_ms, p95_ms, p99_ms, sample_count keys.
    """
    latencies = sorted(abs(consume - produce) for produce, consume in pairs)
    n = len(latencies)
    if n == 0:
        return {"p50_ms": None, "p95_ms": None, "p99_ms": None, "sample_count": 0}

    p50 = latencies[int(n * 0.50)]
    p95 = latencies[int(n * 0.95)]
    p99 = latencies[int(n * 0.99)]

    return {
        "p50_ms": p50,
        "p95_ms": p95,
        "p99_ms": p99,
        "sample_count": n,
        "target_p95_ms": 2000,
        "target_met": p95 < 2000,
    }


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.integration
def test_streaming_schema_validation():
    """
    Parse a synthetic SYNTHETIC_STREAM_SCHEMA event and assert that all
    required (non-nullable) fields are present and have the correct types.
    """
    from shared.schema_registry import SYNTHETIC_STREAM_SCHEMA

    event = _make_stream_event()
    event_bytes = json.dumps(event).encode("utf-8")

    # Deserialize
    parsed = json.loads(event_bytes.decode("utf-8"))

    # Identify required (non-nullable) fields from the schema
    required_fields = {
        field.name: field.dataType.simpleString()
        for field in SYNTHETIC_STREAM_SCHEMA.fields
        if not field.nullable
    }

    missing = [name for name in required_fields if name not in parsed]
    assert not missing, f"Required stream fields missing from event: {missing}"

    # Type spot-checks
    assert isinstance(parsed["transaction_id"], str), (
        f"transaction_id should be str, got {type(parsed['transaction_id'])}"
    )
    assert isinstance(parsed["customer_id"], str), (
        f"customer_id should be str, got {type(parsed['customer_id'])}"
    )
    assert isinstance(parsed["amount"], (int, float)), (
        f"amount should be numeric, got {type(parsed['amount'])}"
    )
    assert isinstance(parsed["transaction_ts"], int), (
        f"transaction_ts should be int (unix epoch), got {type(parsed['transaction_ts'])}"
    )
    assert isinstance(parsed["producer_ts"], int), (
        f"producer_ts should be int (unix epoch), got {type(parsed['producer_ts'])}"
    )

    # Nullable fields may be present or absent
    assert "currency" in parsed       # nullable but provided in our test event
    assert "is_fraud" in parsed


@pytest.mark.integration
def test_streaming_schema_optional_fields_nullable():
    """
    Verify that events missing nullable fields still satisfy the required
    field contract defined in SYNTHETIC_STREAM_SCHEMA.
    """
    from shared.schema_registry import SYNTHETIC_STREAM_SCHEMA

    # Create event without any nullable fields
    event = _make_stream_event(
        currency=None,
        channel=None,
        merchant_category=None,
        lat=None,
        lon=None,
        device_fingerprint=None,
        is_fraud=None,
    )
    event_bytes = json.dumps(event).encode("utf-8")
    parsed = json.loads(event_bytes.decode("utf-8"))

    required_fields = [
        field.name
        for field in SYNTHETIC_STREAM_SCHEMA.fields
        if not field.nullable
    ]

    for name in required_fields:
        assert name in parsed and parsed[name] is not None, (
            f"Required field '{name}' is missing or null in event"
        )


@pytest.mark.integration
def test_latency_probe_output_format():
    """
    Call the standalone percentile calculator with a realistic set of
    (produce_ts, consume_ts) pairs and assert that the output contains
    p50_ms, p95_ms, p99_ms, and sample_count keys with sensible values.
    """
    # 100 synthetic latency pairs: consume_ts is produce_ts + some latency in ms
    import random
    random.seed(42)

    base_ts = 1_700_000_000_000   # ms
    pairs = [
        (base_ts + i * 100, base_ts + i * 100 + int(random.gauss(300, 50)))
        for i in range(100)
    ]

    result = _compute_latency_percentiles(pairs)

    # Required output keys
    for key in ("p50_ms", "p95_ms", "p99_ms", "sample_count"):
        assert key in result, f"Key '{key}' missing from latency output"

    assert result["sample_count"] == 100, (
        f"Expected sample_count == 100, got {result['sample_count']}"
    )

    # Percentile ordering: p50 <= p95 <= p99
    assert result["p50_ms"] <= result["p95_ms"], (
        f"p50 ({result['p50_ms']}) should be <= p95 ({result['p95_ms']})"
    )
    assert result["p95_ms"] <= result["p99_ms"], (
        f"p95 ({result['p95_ms']}) should be <= p99 ({result['p99_ms']})"
    )

    # With gaussian(mean=300, std=50) latencies all values should be > 0
    assert result["p50_ms"] > 0, "p50_ms should be positive"

    # PRD target: p95 < 2000 ms — our synthetic data is well under that
    assert result.get("target_met") is True, (
        f"target_met should be True for synthetic low-latency data, "
        f"got p95={result['p95_ms']}ms"
    )


@pytest.mark.integration
def test_latency_probe_empty_input():
    """
    Verify the percentile calculator handles an empty input list gracefully,
    matching the behavior in latency_probe.py when no streaming data exists.
    """
    result = _compute_latency_percentiles([])

    assert result["p50_ms"] is None
    assert result["p95_ms"] is None
    assert result["p99_ms"] is None
    assert result["sample_count"] == 0
