"""Unit tests for schema_registry — verifies all schemas are correctly defined."""

import sys
from pathlib import Path
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "pipelines" / "spark"))


def test_ieee_cis_transaction_schema_has_v_columns():
    from shared.schema_registry import IEEE_CIS_TRANSACTION_SCHEMA
    field_names = [f.name for f in IEEE_CIS_TRANSACTION_SCHEMA.fields]
    assert "V1" in field_names
    assert "V339" in field_names
    assert "TransactionID" in field_names
    assert "isFraud" in field_names


def test_ieee_cis_transaction_schema_v_count():
    from shared.schema_registry import IEEE_CIS_TRANSACTION_SCHEMA
    v_cols = [f for f in IEEE_CIS_TRANSACTION_SCHEMA.fields if f.name.startswith("V")]
    assert len(v_cols) == 339


def test_paysim_schema_required_fields():
    from shared.schema_registry import PAYSIM_SCHEMA
    field_names = [f.name for f in PAYSIM_SCHEMA.fields]
    for required in ["step", "type", "amount", "nameOrig", "nameDest", "isFraud"]:
        assert required in field_names


def test_synthetic_stream_schema_has_producer_ts():
    from shared.schema_registry import SYNTHETIC_STREAM_SCHEMA
    field_names = [f.name for f in SYNTHETIC_STREAM_SCHEMA.fields]
    assert "producer_ts" in field_names
    assert "transaction_id" in field_names


def test_silver_schema_has_data_source():
    from shared.schema_registry import SILVER_TRANSACTION_SCHEMA
    field_names = [f.name for f in SILVER_TRANSACTION_SCHEMA.fields]
    assert "data_source" in field_names
    assert "transaction_ts" in field_names
    assert "amount_usd" in field_names


def test_gold_customer_schema_has_fraud_rate():
    from shared.schema_registry import GOLD_CUSTOMER_FEATURE_SCHEMA
    field_names = [f.name for f in GOLD_CUSTOMER_FEATURE_SCHEMA.fields]
    assert "fraud_rate_30d" in field_names
    assert "customer_id" in field_names
    assert "feature_date" in field_names
