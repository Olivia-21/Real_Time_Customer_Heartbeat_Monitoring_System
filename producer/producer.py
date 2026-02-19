"""
producer.py – Synthetic Heart-Rate Data Generator + Kafka Producer
===================================================================
Production-ready, fault-tolerant Kafka producer that:
  • Generates realistic heart-rate readings for N synthetic customers.
  • Uses exponential back-off retry on Kafka delivery failures.
  • Serialises messages as JSON with schema version for forward compatibility.
  • Gracefully handles SIGTERM / SIGINT for clean container shutdown.
  • Emits structured logs to stdout (friendly for log aggregators).
"""

import json
import logging
import os
import random
import signal
import sys
import time
from datetime import datetime, timezone

from confluent_kafka import Producer, KafkaException

# ──────────────────────────────────────────────
# Logging
# ──────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
    stream=sys.stdout,
)
log = logging.getLogger("heartbeat.producer")

# ──────────────────────────────────────────────
# Configuration (from environment)
# ──────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC             = os.getenv("KAFKA_TOPIC", "heartbeat")
NUM_CUSTOMERS           = int(os.getenv("NUM_CUSTOMERS", "20"))
INTERVAL_SECONDS        = float(os.getenv("INTERVAL_SECONDS", "1"))
SCHEMA_VERSION          = "1.0"

# Realistic heart-rate distribution parameters
HEART_RATE_MEAN   = 75
HEART_RATE_STDDEV = 15
HEART_RATE_MIN    = 30
HEART_RATE_MAX    = 220


# ──────────────────────────────────────────────
# Customer pool (deterministic so IDs are stable)
# ──────────────────────────────────────────────
def build_customer_pool(n: int) -> list[str]:
    return [f"cust_{i:04d}" for i in range(1, n + 1)]


# ──────────────────────────────────────────────
# Data generation
# ──────────────────────────────────────────────
def generate_reading(customer_id: str) -> dict:
    """
    Generate a single heart-rate reading for a customer.
    Occasionally (~3 % chance) injects anomalous values to test the consumer.
    """
    if random.random() < 0.03:
        # Inject an anomaly
        heart_rate = random.choice(
            [random.randint(20, 39), random.randint(181, 220)]
        )
    else:
        heart_rate = int(
            max(HEART_RATE_MIN,
                min(HEART_RATE_MAX,
                    random.gauss(HEART_RATE_MEAN, HEART_RATE_STDDEV)))
        )

    return {
        "schema_version": SCHEMA_VERSION,
        "customer_id": customer_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "heart_rate": heart_rate,
    }


# ──────────────────────────────────────────────
# Delivery callback
# ──────────────────────────────────────────────
def delivery_report(err, msg):
    if err is not None:
        log.error(
            "Delivery failed | topic=%s partition=%s offset=%s error=%s",
            msg.topic(), msg.partition(), msg.offset(), err,
        )
    else:
        log.debug(
            "Delivered | topic=%s partition=%s offset=%s",
            msg.topic(), msg.partition(), msg.offset(),
        )


# ──────────────────────────────────────────────
# Kafka producer factory with retry
# ──────────────────────────────────────────────
def create_producer(max_retries: int = 10) -> Producer:
    conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        # Idempotent producer – exactly-once delivery guarantees
        "enable.idempotence": True,
        "acks": "all",
        "retries": 5,
        "retry.backoff.ms": 500,
        "linger.ms": 10,            # small batching window
        "compression.type": "lz4",  # reduce network overhead
        "client.id": "heartbeat-producer",
    }
    for attempt in range(1, max_retries + 1):
        try:
            producer = Producer(conf)
            log.info("Kafka producer connected | brokers=%s", KAFKA_BOOTSTRAP_SERVERS)
            return producer
        except KafkaException as exc:
            wait = min(2 ** attempt, 60)
            log.warning(
                "Kafka not ready (attempt %d/%d) – retrying in %ds | error=%s",
                attempt, max_retries, wait, exc,
            )
            time.sleep(wait)
    log.critical("Could not connect to Kafka after %d attempts. Exiting.", max_retries)
    sys.exit(1)


# ──────────────────────────────────────────────
# Graceful shutdown
# ──────────────────────────────────────────────
_running = True


def _handle_signal(signum, _frame):
    global _running
    log.info("Signal %s received – initiating graceful shutdown.", signal.Signals(signum).name)
    _running = False


signal.signal(signal.SIGTERM, _handle_signal)
signal.signal(signal.SIGINT, _handle_signal)


# ──────────────────────────────────────────────
# Main loop
# ──────────────────────────────────────────────
def main():
    customers = build_customer_pool(NUM_CUSTOMERS)
    log.info(
        "Producer starting | customers=%d topic=%s interval=%.2fs",
        NUM_CUSTOMERS, KAFKA_TOPIC, INTERVAL_SECONDS,
    )
    producer = create_producer()

    messages_sent = 0

    while _running:
        batch_start = time.monotonic()

        for customer_id in customers:
            if not _running:
                break
            reading = generate_reading(customer_id)
            try:
                producer.produce(
                    topic=KAFKA_TOPIC,
                    key=customer_id.encode("utf-8"),
                    value=json.dumps(reading).encode("utf-8"),
                    on_delivery=delivery_report,
                )
                messages_sent += 1
            except BufferError:
                log.warning("Producer buffer full – flushing before retrying.")
                producer.flush(timeout=10)
                producer.produce(
                    topic=KAFKA_TOPIC,
                    key=customer_id.encode("utf-8"),
                    value=json.dumps(reading).encode("utf-8"),
                    on_delivery=delivery_report,
                )

        # Poll to trigger delivery callbacks
        producer.poll(0)

        elapsed = time.monotonic() - batch_start
        sleep_time = max(0, INTERVAL_SECONDS - elapsed)
        if _running:
            time.sleep(sleep_time)

        if messages_sent % (NUM_CUSTOMERS * 60) == 0 and messages_sent > 0:
            log.info("Heartbeat | messages_sent_total=%d", messages_sent)

    log.info("Flushing remaining messages…")
    producer.flush(timeout=30)
    log.info("Producer shut down cleanly | total_messages_sent=%d", messages_sent)


if __name__ == "__main__":
    main()
