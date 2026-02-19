"""
consumer.py – Kafka Consumer + Anomaly Detector + PostgreSQL Writer
===================================================================
Production-ready, fault-tolerant Kafka consumer that:
  • Reads heart-rate messages from a Kafka topic.
  • Validates and classifies anomalies (bradycardia / tachycardia).
  • Upserts customer records and writes readings to PostgreSQL.
  • Uses manual offset commits (at-least-once delivery guarantee).
  • Reconnects to both Kafka and PostgreSQL on transient failures.
  • Logs pipeline metrics to the pipeline_log table every 60 s.
  • Handles SIGTERM / SIGINT for clean container shutdown.
"""

import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras
from confluent_kafka import Consumer, KafkaError, KafkaException

# ──────────────────────────────────────────────
# Logging
# ──────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
    stream=sys.stdout,
)
log = logging.getLogger("heartbeat.consumer")

# ──────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC             = os.getenv("KAFKA_TOPIC", "heartbeat")
KAFKA_GROUP_ID          = os.getenv("KAFKA_GROUP_ID", "heartbeat_consumer_group")

POSTGRES_HOST     = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT     = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB       = os.getenv("POSTGRES_DB", "heartbeat_db")
POSTGRES_USER     = os.getenv("POSTGRES_USER", "heartbeat_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "heartbeat_pass")

HEART_RATE_MIN    = int(os.getenv("HEART_RATE_MIN", "40"))
HEART_RATE_MAX    = int(os.getenv("HEART_RATE_MAX", "180"))
POLL_TIMEOUT_SEC  = 1.0
LOG_INTERVAL_SEC  = 60.0

# ──────────────────────────────────────────────
# Graceful shutdown flag
# ──────────────────────────────────────────────
_running = True


def _handle_signal(signum, _frame):
    global _running
    log.info("Signal %s received – initiating graceful shutdown.", signal.Signals(signum).name)
    _running = False


signal.signal(signal.SIGTERM, _handle_signal)
signal.signal(signal.SIGINT, _handle_signal)


# ──────────────────────────────────────────────
# PostgreSQL helpers
# ──────────────────────────────────────────────
def get_db_connection(max_retries: int = 10) -> psycopg2.extensions.connection:
    """Connect to PostgreSQL with exponential back-off."""
    for attempt in range(1, max_retries + 1):
        try:
            conn = psycopg2.connect(
                host=POSTGRES_HOST,
                port=POSTGRES_PORT,
                dbname=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD,
                connect_timeout=10,
                application_name="heartbeat-consumer",
            )
            conn.autocommit = False
            log.info("PostgreSQL connected | host=%s db=%s", POSTGRES_HOST, POSTGRES_DB)
            return conn
        except psycopg2.OperationalError as exc:
            wait = min(2 ** attempt, 60)
            log.warning(
                "PostgreSQL not ready (attempt %d/%d) – retrying in %ds | error=%s",
                attempt, max_retries, wait, exc,
            )
            time.sleep(wait)
    log.critical("Could not connect to PostgreSQL after %d attempts. Exiting.", max_retries)
    sys.exit(1)


def ensure_customer(cur, customer_id: str) -> None:
    """Idempotent upsert of a customer record."""
    cur.execute(
        """
        INSERT INTO customers (customer_id)
        VALUES (%s)
        ON CONFLICT (customer_id) DO NOTHING;
        """,
        (customer_id,),
    )


def insert_reading(
    cur,
    customer_id: str,
    recorded_at: str,
    heart_rate: int,
    is_anomaly: bool,
    anomaly_reason: str | None,
) -> None:
    """Insert a validated heart-rate reading."""
    cur.execute(
        """
        INSERT INTO heart_rate_readings
            (customer_id, recorded_at, heart_rate, is_anomaly, anomaly_reason)
        VALUES (%s, %s, %s, %s, %s);
        """,
        (customer_id, recorded_at, heart_rate, is_anomaly, anomaly_reason),
    )


def log_pipeline_metrics(conn, read: int, valid: int, anomaly: int, invalid: int) -> None:
    """Write batch metrics to the pipeline_log table."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pipeline_log
                    (messages_read, messages_valid, messages_anomaly, messages_invalid)
                VALUES (%s, %s, %s, %s);
                """,
                (read, valid, anomaly, invalid),
            )
        conn.commit()
    except Exception as exc:
        log.warning("Failed to write pipeline_log: %s", exc)
        conn.rollback()


# ──────────────────────────────────────────────
# Anomaly detection
# ──────────────────────────────────────────────
def classify_anomaly(heart_rate: int) -> tuple[bool, str | None]:
    """
    Returns (is_anomaly, reason).
    Bradycardia  < HEART_RATE_MIN
    Tachycardia  > HEART_RATE_MAX
    """
    if heart_rate < HEART_RATE_MIN:
        return True, f"bradycardia (bpm={heart_rate} < threshold={HEART_RATE_MIN})"
    if heart_rate > HEART_RATE_MAX:
        return True, f"tachycardia (bpm={heart_rate} > threshold={HEART_RATE_MAX})"
    return False, None


# ──────────────────────────────────────────────
# Message validation
# ──────────────────────────────────────────────
REQUIRED_FIELDS = {"customer_id", "timestamp", "heart_rate"}


def validate_message(data: dict) -> tuple[bool, str]:
    """Returns (is_valid, error_reason)."""
    missing = REQUIRED_FIELDS - data.keys()
    if missing:
        return False, f"missing fields: {missing}"
    try:
        hr = int(data["heart_rate"])
    except (ValueError, TypeError):
        return False, f"heart_rate is not numeric: {data.get('heart_rate')}"
    if hr <= 0 or hr >= 300:
        return False, f"heart_rate physiologically impossible: {hr}"
    return True, ""


# ──────────────────────────────────────────────
# Kafka consumer factory
# ──────────────────────────────────────────────
def create_consumer(max_retries: int = 10) -> Consumer:
    conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": KAFKA_GROUP_ID,
        # Earliest: consumer processes from the start when no committed offset exists
        "auto.offset.reset": "earliest",
        # Manual commit for at-least-once delivery
        "enable.auto.commit": False,
        "max.poll.interval.ms": 300_000,
        "session.timeout.ms": 30_000,
        "client.id": "heartbeat-consumer",
    }
    for attempt in range(1, max_retries + 1):
        try:
            consumer = Consumer(conf)
            consumer.subscribe([KAFKA_TOPIC])
            log.info(
                "Kafka consumer subscribed | brokers=%s topic=%s group=%s",
                KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, KAFKA_GROUP_ID,
            )
            return consumer
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
# Main processing loop
# ──────────────────────────────────────────────
def process_message(msg, conn) -> tuple[str, str]:
    """
    Parse, validate, classify, and persist one Kafka message.
    Returns one of: 'valid', 'anomaly', 'invalid', 'error'.
    """
    try:
        data = json.loads(msg.value().decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        log.warning("Malformed JSON | error=%s payload=%r", exc, msg.value()[:200])
        return "invalid", "malformed_json"

    is_valid, err = validate_message(data)
    if not is_valid:
        log.warning("Invalid message | reason=%s", err)
        return "invalid", err

    customer_id    = data["customer_id"]
    recorded_at    = data["timestamp"]
    heart_rate     = int(data["heart_rate"])
    is_anomaly, reason = classify_anomaly(heart_rate)

    if is_anomaly:
        log.warning(
            "ANOMALY detected | customer=%s bpm=%d reason=%s",
            customer_id, heart_rate, reason,
        )

    try:
        with conn.cursor() as cur:
            ensure_customer(cur, customer_id)
            insert_reading(cur, customer_id, recorded_at, heart_rate, is_anomaly, reason)
        conn.commit()
    except psycopg2.IntegrityError as exc:
        conn.rollback()
        log.warning("DB integrity error (skipping) | error=%s", exc)
        return "error", str(exc)
    except psycopg2.OperationalError as exc:
        conn.rollback()
        log.error("DB operational error (will reconnect) | error=%s", exc)
        raise  # Bubble up to trigger reconnect

    return ("anomaly" if is_anomaly else "valid"), ""


def main():
    log.info("Consumer starting up…")
    consumer = create_consumer()
    conn     = get_db_connection()

    # Rolling counters for the current log window
    counts = {"read": 0, "valid": 0, "anomaly": 0, "invalid": 0}
    last_log_time = time.monotonic()

    try:
        while _running:
            msg = consumer.poll(timeout=POLL_TIMEOUT_SEC)

            if msg is None:
                pass  # No new messages – heartbeat timeout
            elif msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    log.debug(
                        "End of partition | topic=%s partition=%d offset=%d",
                        msg.topic(), msg.partition(), msg.offset(),
                    )
                else:
                    log.error("Kafka error: %s", msg.error())
            else:
                counts["read"] += 1
                try:
                    outcome, _ = process_message(msg, conn)
                    counts[outcome] = counts.get(outcome, 0) + 1
                    # Commit offset after successful write (at-least-once)
                    consumer.commit(message=msg, asynchronous=False)
                except psycopg2.OperationalError:
                    log.warning("Attempting PostgreSQL reconnect…")
                    try:
                        conn.close()
                    except Exception:
                        pass
                    conn = get_db_connection()

            # Periodic metrics flush
            now = time.monotonic()
            if now - last_log_time >= LOG_INTERVAL_SEC:
                log.info(
                    "Pipeline metrics | read=%d valid=%d anomaly=%d invalid=%d",
                    counts["read"], counts["valid"], counts["anomaly"], counts["invalid"],
                )
                log_pipeline_metrics(
                    conn,
                    counts["read"], counts["valid"], counts["anomaly"], counts["invalid"],
                )
                counts = {"read": 0, "valid": 0, "anomaly": 0, "invalid": 0}
                last_log_time = now

    finally:
        log.info("Closing consumer and database connection…")
        consumer.close()
        try:
            conn.close()
        except Exception:
            pass
        log.info("Consumer shut down cleanly.")


if __name__ == "__main__":
    main()
