"""Feast entity definitions for the Sentinel Fraud Platform."""

from feast import Entity, ValueType

customer = Entity(
    name="customer_id",
    value_type=ValueType.STRING,
    description="Customer entity — maps to card1 (IEEE-CIS) or nameOrig (PaySim)",
    tags={"owner": "data-engineering", "domain": "fraud"},
)

merchant = Entity(
    name="merchant_id",
    value_type=ValueType.STRING,
    description="Merchant entity — maps to ProductCD-derived ID (IEEE-CIS) or nameDest (PaySim)",
    tags={"owner": "data-engineering", "domain": "fraud"},
)

transaction = Entity(
    name="transaction_id",
    value_type=ValueType.STRING,
    description="Transaction entity — unique per transaction",
    tags={"owner": "data-engineering", "domain": "fraud"},
)
