"""
Synthetic transaction event producer.
Generates realistic fraud/legitimate transaction events and publishes to Kafka.
Configurable rate and fraud injection rate via environment variables.
"""

import json
import os
import random
import signal
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer, KafkaException
from faker import Faker
from prometheus_client import Counter, Histogram, start_http_server

fake = Faker()
Faker.seed(42)

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_RAW = os.environ.get("KAFKA_TOPIC_RAW", "transactions.raw")
RATE_PER_SECOND = int(os.environ.get("PRODUCER_RATE_PER_SECOND", "500"))
FRAUD_RATE = float(os.environ.get("PRODUCER_FRAUD_RATE", "0.015"))
METRICS_PORT = int(os.environ.get("PRODUCER_METRICS_PORT", "8000"))

# Prometheus metrics
tx_counter = Counter("transactions_produced_total", "Total transactions produced")
fraud_counter = Counter("fraud_injected_total", "Fraud transactions injected")
error_counter = Counter("produce_errors_total", "Kafka produce errors")
produce_latency = Histogram("produce_latency_seconds", "Time to produce one message")

MERCHANT_CATEGORIES = ["grocery", "gas_station", "restaurant", "online", "travel", "entertainment", "healthcare"]
CHANNELS = ["web", "mobile", "pos", "atm"]
CURRENCIES = ["USD", "EUR", "GBP", "CAD"]

# Pre-generated customer and merchant pools for realistic repeat behaviour
CUSTOMERS = [f"C{i:06d}" for i in range(1, 10001)]
MERCHANTS = [f"M{i:04d}" for i in range(1, 1001)]


def generate_transaction(is_fraud: bool) -> dict:
    customer_id = random.choice(CUSTOMERS)
    merchant_id = random.choice(MERCHANTS)
    now_ms = int(time.time() * 1000)

    if is_fraud:
        # Fraud patterns: unusual hour, round amounts, velocity
        hour = random.choice([1, 2, 3, 23, 0])
        amount = round(random.choice([random.uniform(500, 5000), round(random.uniform(100, 5000) / 100) * 100]), 2)
        channel = random.choice(["web", "atm"])
    else:
        hour = random.randint(8, 22)
        # Log-normal amount distribution matching real fraud datasets
        amount = round(max(0.01, random.lognormvariate(3.5, 1.5)), 2)
        channel = random.choice(CHANNELS)

    lat = round(random.uniform(25.0, 49.0), 6)
    lon = round(random.uniform(-125.0, -67.0), 6)

    return {
        "transaction_id": str(uuid.uuid4()),
        "customer_id": customer_id,
        "merchant_id": merchant_id,
        "amount": amount,
        "currency": random.choice(CURRENCIES),
        "channel": channel,
        "merchant_category": random.choice(MERCHANT_CATEGORIES),
        "transaction_ts": now_ms,
        "producer_ts": now_ms,
        "lat": lat,
        "lon": lon,
        "device_fingerprint": fake.md5(),
        "is_fraud": int(is_fraud),
    }


def delivery_report(err, msg):
    if err is not None:
        error_counter.inc()


def main():
    print(f"Starting synthetic producer: {RATE_PER_SECOND} TPS, {FRAUD_RATE:.1%} fraud rate")
    print(f"Broker: {BOOTSTRAP_SERVERS}, Topic: {TOPIC_RAW}")

    start_http_server(METRICS_PORT)
    print(f"Prometheus metrics at http://0.0.0.0:{METRICS_PORT}/metrics")

    producer = Producer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "queue.buffering.max.messages": 100000,
        "queue.buffering.max.ms": 50,
        "batch.num.messages": 1000,
        "compression.type": "snappy",
    })

    running = True
    def handle_sigterm(sig, frame):
        nonlocal running
        print("Received SIGTERM — shutting down gracefully")
        running = False

    signal.signal(signal.SIGTERM, handle_sigterm)
    signal.signal(signal.SIGINT, handle_sigterm)

    interval = 1.0 / RATE_PER_SECOND
    tx_count = 0

    while running:
        loop_start = time.time()
        is_fraud = random.random() < FRAUD_RATE
        tx = generate_transaction(is_fraud)

        payload = json.dumps(tx).encode("utf-8")
        with produce_latency.time():
            producer.produce(
                topic=TOPIC_RAW,
                key=tx["customer_id"].encode("utf-8"),
                value=payload,
                callback=delivery_report,
            )

        tx_counter.inc()
        if is_fraud:
            fraud_counter.inc()

        tx_count += 1
        if tx_count % 1000 == 0:
            producer.poll(0)
            print(f"Produced {tx_count:,} transactions ({fraud_counter._value.get():.0f} fraud)")

        elapsed = time.time() - loop_start
        sleep_time = interval - elapsed
        if sleep_time > 0:
            time.sleep(sleep_time)

    producer.flush(30)
    print(f"Producer stopped. Total: {tx_count:,} transactions")


if __name__ == "__main__":
    main()
