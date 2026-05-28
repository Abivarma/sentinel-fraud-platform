"""JSON schema for synthetic transaction events."""

TRANSACTION_SCHEMA = {
    "type": "object",
    "required": ["transaction_id", "customer_id", "merchant_id", "amount", "transaction_ts", "producer_ts"],
    "properties": {
        "transaction_id": {"type": "string"},
        "customer_id": {"type": "string"},
        "merchant_id": {"type": "string"},
        "amount": {"type": "number", "minimum": 0},
        "currency": {"type": "string"},
        "channel": {"type": "string", "enum": ["web", "mobile", "pos", "atm", "transfer", "other"]},
        "merchant_category": {"type": "string"},
        "transaction_ts": {"type": "integer"},
        "producer_ts": {"type": "integer"},
        "lat": {"type": ["number", "null"]},
        "lon": {"type": ["number", "null"]},
        "device_fingerprint": {"type": ["string", "null"]},
        "is_fraud": {"type": ["integer", "null"]},
    },
}
