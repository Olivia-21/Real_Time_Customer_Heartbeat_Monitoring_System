"""
test_pipeline.py – Integration & Unit Tests
============================================
Tests cover:
  - Data generation logic (producer)
  - Message validation (consumer)
  - Anomaly classification (consumer)
  - Database schema (requires a running PostgreSQL instance,
    skipped automatically when DB is not available)
Run with:  pytest tests/ -v
"""

import json
import sys
import os
import importlib.util
import pytest

# ──────────────────────────────────────────────
# Load modules without running their __main__
# ──────────────────────────────────────────────
def _load_module(name: str, path: str):
    spec = importlib.util.spec_from_file_location(name, path)
    mod  = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

producer_mod = _load_module("producer", os.path.join(BASE, "producer", "producer.py"))
consumer_mod = _load_module("consumer", os.path.join(BASE, "consumer", "consumer.py"))


# ══════════════════════════════════════════════
# Producer  tests
# ══════════════════════════════════════════════

class TestBuildCustomerPool:
    def test_count(self):
        pool = producer_mod.build_customer_pool(10)
        assert len(pool) == 10

    def test_format(self):
        pool = producer_mod.build_customer_pool(5)
        assert pool[0] == "cust_0001"
        assert pool[4] == "cust_0005"

    def test_deterministic(self):
        assert producer_mod.build_customer_pool(3) == producer_mod.build_customer_pool(3)


class TestGenerateReading:
    def test_fields_present(self):
        reading = producer_mod.generate_reading("cust_0001")
        assert {"schema_version", "customer_id", "timestamp", "heart_rate"} <= reading.keys()

    def test_customer_id_preserved(self):
        reading = producer_mod.generate_reading("cust_9999")
        assert reading["customer_id"] == "cust_9999"

    def test_heart_rate_in_range(self):
        # Run many times to ensure even injected anomalies stay in physical bounds
        for _ in range(500):
            r = producer_mod.generate_reading("cust_0001")
            assert 20 <= r["heart_rate"] <= 220, f"Out of range: {r['heart_rate']}"

    def test_schema_version(self):
        reading = producer_mod.generate_reading("cust_0001")
        assert reading["schema_version"] == "1.0"

    def test_timestamp_is_iso(self):
        from datetime import datetime
        reading = producer_mod.generate_reading("cust_0001")
        # Should parse without error
        datetime.fromisoformat(reading["timestamp"])


# ══════════════════════════════════════════════
# Consumer – validation tests
# ══════════════════════════════════════════════

class TestValidateMessage:
    def _valid(self):
        return {"customer_id": "cust_0001", "timestamp": "2025-01-01T00:00:00Z", "heart_rate": 75}

    def test_valid_message(self):
        ok, err = consumer_mod.validate_message(self._valid())
        assert ok
        assert err == ""

    def test_missing_customer_id(self):
        data = self._valid()
        del data["customer_id"]
        ok, err = consumer_mod.validate_message(data)
        assert not ok
        assert "customer_id" in err

    def test_missing_timestamp(self):
        data = self._valid()
        del data["timestamp"]
        ok, err = consumer_mod.validate_message(data)
        assert not ok

    def test_missing_heart_rate(self):
        data = self._valid()
        del data["heart_rate"]
        ok, err = consumer_mod.validate_message(data)
        assert not ok

    def test_non_numeric_heart_rate(self):
        data = self._valid()
        data["heart_rate"] = "abc"
        ok, err = consumer_mod.validate_message(data)
        assert not ok
        assert "not numeric" in err

    def test_impossible_heart_rate_zero(self):
        data = self._valid()
        data["heart_rate"] = 0
        ok, _ = consumer_mod.validate_message(data)
        assert not ok

    def test_impossible_heart_rate_300(self):
        data = self._valid()
        data["heart_rate"] = 300
        ok, _ = consumer_mod.validate_message(data)
        assert not ok


# ══════════════════════════════════════════════
# Consumer – anomaly classification tests
# ══════════════════════════════════════════════

class TestClassifyAnomaly:
    """Tests use the default thresholds set in the module environment."""

    def test_normal_reading(self):
        is_anomaly, reason = consumer_mod.classify_anomaly(72)
        assert not is_anomaly
        assert reason is None

    def test_bradycardia(self):
        is_anomaly, reason = consumer_mod.classify_anomaly(30)
        assert is_anomaly
        assert "bradycardia" in reason

    def test_tachycardia(self):
        is_anomaly, reason = consumer_mod.classify_anomaly(210)
        assert is_anomaly
        assert "tachycardia" in reason

    def test_boundary_min_normal(self):
        """Heart rate == HEART_RATE_MIN should be considered normal."""
        threshold = consumer_mod.HEART_RATE_MIN
        is_anomaly, _ = consumer_mod.classify_anomaly(threshold)
        assert not is_anomaly

    def test_boundary_max_normal(self):
        """Heart rate == HEART_RATE_MAX should be considered normal."""
        threshold = consumer_mod.HEART_RATE_MAX
        is_anomaly, _ = consumer_mod.classify_anomaly(threshold)
        assert not is_anomaly

    def test_one_below_min(self):
        threshold = consumer_mod.HEART_RATE_MIN
        is_anomaly, _ = consumer_mod.classify_anomaly(threshold - 1)
        assert is_anomaly

    def test_one_above_max(self):
        threshold = consumer_mod.HEART_RATE_MAX
        is_anomaly, _ = consumer_mod.classify_anomaly(threshold + 1)
        assert is_anomaly


# ══════════════════════════════════════════════
# Database integration tests
# (skipped automatically if PostgreSQL is not reachable)
# ══════════════════════════════════════════════

def _db_available() -> bool:
    try:
        import psycopg2
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            dbname=os.getenv("POSTGRES_DB", "heartbeat_db"),
            user=os.getenv("POSTGRES_USER", "heartbeat_user"),
            password=os.getenv("POSTGRES_PASSWORD", "heartbeat_pass"),
            connect_timeout=3,
        )
        conn.close()
        return True
    except Exception:
        return False


@pytest.mark.skipif(not _db_available(), reason="PostgreSQL not reachable")
class TestDatabaseIntegration:
    @pytest.fixture(autouse=True)
    def db(self):
        import psycopg2
        self.conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            dbname=os.getenv("POSTGRES_DB", "heartbeat_db"),
            user=os.getenv("POSTGRES_USER", "heartbeat_user"),
            password=os.getenv("POSTGRES_PASSWORD", "heartbeat_pass"),
        )
        self.conn.autocommit = False
        yield
        self.conn.rollback()  # Rollback after each test – no persistent state
        self.conn.close()

    def test_customer_upsert_idempotent(self):
        with self.conn.cursor() as cur:
            consumer_mod.ensure_customer(cur, "test_cust_001")
            consumer_mod.ensure_customer(cur, "test_cust_001")  # second call must not fail

    def test_insert_reading(self):
        with self.conn.cursor() as cur:
            consumer_mod.ensure_customer(cur, "test_cust_002")
            consumer_mod.insert_reading(
                cur, "test_cust_002", "2025-01-01T12:00:00Z", 72, False, None
            )
            cur.execute(
                "SELECT heart_rate FROM heart_rate_readings WHERE customer_id = %s",
                ("test_cust_002",),
            )
            row = cur.fetchone()
        assert row is not None
        assert row[0] == 72

    def test_schema_tables_exist(self):
        with self.conn.cursor() as cur:
            for table in ("customers", "heart_rate_readings", "pipeline_log"):
                cur.execute(
                    "SELECT 1 FROM information_schema.tables WHERE table_name = %s",
                    (table,),
                )
                assert cur.fetchone() is not None, f"Table '{table}' not found"
