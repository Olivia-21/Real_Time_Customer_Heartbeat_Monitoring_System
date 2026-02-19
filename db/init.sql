-- ============================================================
-- PostgreSQL Schema – Customer Heartbeat Monitoring
-- Idempotent: safe to run multiple times (IF NOT EXISTS)
-- ============================================================

-- Extension for UUID generation (PostgreSQL ≥ 13 uses gen_random_uuid())
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- ─────────────────────────────────────────────────────────
-- customers lookup table
-- ─────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS customers (
    customer_id   VARCHAR(50)  PRIMARY KEY,
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- ─────────────────────────────────────────────────────────
-- heart_rate_readings – main time-series table
-- ─────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS heart_rate_readings (
    id               BIGSERIAL    PRIMARY KEY,
    customer_id      VARCHAR(50)  NOT NULL REFERENCES customers(customer_id),
    recorded_at      TIMESTAMPTZ  NOT NULL,
    heart_rate       SMALLINT     NOT NULL CHECK (heart_rate > 0 AND heart_rate < 300),
    is_anomaly       BOOLEAN      NOT NULL DEFAULT FALSE,
    anomaly_reason   TEXT,
    inserted_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- Efficient time-series range queries
CREATE INDEX IF NOT EXISTS idx_heart_rate_customer_time
    ON heart_rate_readings (customer_id, recorded_at DESC);

-- Fast anomaly lookups
CREATE INDEX IF NOT EXISTS idx_heart_rate_anomaly
    ON heart_rate_readings (is_anomaly, recorded_at DESC)
    WHERE is_anomaly = TRUE;

-- ─────────────────────────────────────────────────────────
-- pipeline_log – tracks consumer batch metrics
-- ─────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS pipeline_log (
    id               BIGSERIAL    PRIMARY KEY,
    logged_at        TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    messages_read    INT          NOT NULL DEFAULT 0,
    messages_valid   INT          NOT NULL DEFAULT 0,
    messages_anomaly INT          NOT NULL DEFAULT 0,
    messages_invalid INT          NOT NULL DEFAULT 0
);
