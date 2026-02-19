#  Real-Time Customer Heartbeat Monitoring System

A production-ready, fault-tolerant data pipeline that **simulates**, **streams**, **validates**, **stores**, and **visualises** real-time heart-rate sensor data using **Apache Kafka**, **PostgreSQL**, and **Streamlit** — all orchestrated with **Docker Compose**.

---

##  System Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│                         Docker Network: heartbeat_net                │
│                                                                      │
│  ┌────────────┐  JSON msgs  ┌────────┐  subscribe  ┌─────────────┐  │
│  │  Producer  │ ──────────► │ Kafka  │ ───────────► │  Consumer   │  │
│  │ (generator)│             │ broker │             │  (validator) │  │
│  └────────────┘             └────────┘             └──────┬──────┘  │
│                               ▲                           │ INSERT  │
│                        ┌──────┘                    ┌──────▼──────┐  │
│                        │ Zookeeper                 │ PostgreSQL  │  │
│                        └──────────────────────────►│  heartbeat  │  │
│                                                    │    _db      │  │
│                                                    └──────┬──────┘  │
│                                                           │ SELECT  │
│                                                    ┌──────▼──────┐  │
│                                                    │  Streamlit  │  │
│                                                    │  Dashboard  │  │
│                                                    └─────────────┘  │
└──────────────────────────────────────────────────────────────────────┘
              External port 5432          External port 8501
```

### Component Summary

| Component | Image / Language | Role |
|-----------|-----------------|------|
| **Zookeeper** | `confluentinc/cp-zookeeper:7.6.0` | Kafka cluster coordinator |
| **Kafka** | `confluentinc/cp-kafka:7.6.0` | Message broker / topic store |
| **PostgreSQL** | `postgres:16-alpine` | Persistent time-series storage |
| **Producer** | Python 3.12 + `confluent-kafka` | Synthetic data generator |
| **Consumer** | Python 3.12 + `confluent-kafka` + `psycopg2` | Validator, anomaly detector, DB writer |
| **Dashboard** | Python 3.12 + Streamlit | Real-time visualisation |

---

##  Quick Start

### Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (with Docker Compose v2)
- Git (optional)

### 1 – Clone / open the project

```bash
cd DEM10_Lab
```

### 2 – Build and start all services

```bash
docker compose up --build -d
```

> First build downloads images and installs Python packages — takes ~2–3 minutes.

### 3 – Verify all containers are healthy

```bash
docker compose ps
```

All containers should show `healthy` or `running`.

### 4 – Open the dashboard

Navigate to **http://localhost:8501** in your browser.

### 5 – Watch live logs

```bash
# All services
docker compose logs -f

# Individual services
docker compose logs -f producer
docker compose logs -f consumer
```

### 6 – Stop everything

```bash
docker compose down
```

To also remove persistent volumes (wipes PostgreSQL data):

```bash
docker compose down -v
```

---

##  PostgreSQL Schema

```sql
-- Time-series readings table
heart_rate_readings (
    id               BIGSERIAL PRIMARY KEY,
    customer_id      VARCHAR(50) NOT NULL,
    recorded_at      TIMESTAMPTZ NOT NULL,
    heart_rate       SMALLINT   NOT NULL,
    is_anomaly       BOOLEAN    NOT NULL,
    anomaly_reason   TEXT,
    inserted_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
)

-- Indexes
idx_heart_rate_customer_time  ON (customer_id, recorded_at DESC)
idx_heart_rate_anomaly        ON (is_anomaly, recorded_at DESC) WHERE is_anomaly = TRUE
```

### Useful Queries

```sql
-- Latest reading per customer
SELECT DISTINCT ON (customer_id)
       customer_id, heart_rate, is_anomaly, recorded_at
FROM heart_rate_readings
ORDER BY customer_id, recorded_at DESC;

-- All anomalies in the last hour
SELECT * FROM heart_rate_readings
WHERE is_anomaly = TRUE
  AND recorded_at >= NOW() - INTERVAL '1 hour'
ORDER BY recorded_at DESC;

-- Readings per minute (throughput)
SELECT date_trunc('minute', recorded_at) AS minute,
       COUNT(*) AS readings
FROM heart_rate_readings
GROUP BY minute
ORDER BY minute DESC
LIMIT 30;
```

### Connect directly

```bash
docker exec -it postgres psql -U heartbeat_user -d heartbeat_db
```

---

##  Configuration

All services are configured via environment variables in `docker-compose.yml`.

| Variable | Default | Description |
|----------|---------|-------------|
| `NUM_CUSTOMERS` | `20` | Number of synthetic customers |
| `INTERVAL_SECONDS` | `1` | Seconds between producer batches |
| `HEART_RATE_MIN` | `40` | BPM below which an anomaly is flagged |
| `HEART_RATE_MAX` | `180` | BPM above which an anomaly is flagged |
| `KAFKA_TOPIC` | `heartbeat` | Kafka topic name |
| `KAFKA_GROUP_ID` | `heartbeat_consumer_group` | Consumer group ID |

---

##  Production Design Decisions

| Property | Implementation |
|----------|---------------|
| **Idempotency** | `ON CONFLICT DO NOTHING` customer upsert; Kafka idempotent producer. Schema uses `IF NOT EXISTS`. |
| **Fault Tolerance** | Exponential back-off retry on both Kafka and PostgreSQL connections. Consumer auto-reconnects on DB operational errors. |
| **Durability** | PostgreSQL data persisted in a named Docker volume. Kafka `acks=all` ensures broker acknowledgment. |
| **At-least-once delivery** | Manual Kafka offset commit only after successful DB write. |
| **Observability** | Structured logs (timestamp \| level \| logger \| message). Pipeline metrics table updated every 60 s. |
| **Security** | Non-root user in every container. Credentials via environment variables (never hardcoded). |
| **Graceful shutdown** | SIGTERM/SIGINT handlers flush in-flight messages before exit. |

---

##  Running Tests

Install test dependencies locally:

```bash
pip install pytest confluent-kafka psycopg2-binary
```

Run the test suite:

```bash
pytest tests/ -v
```

Integration tests (DB connectivity) are **automatically skipped** if PostgreSQL is not reachable. To run them with a live DB:

```bash
docker compose up -d postgres
pytest tests/ -v --tb=short
```

---

## 📊 Dashboard Features

- **KPI cards** — Total readings, unique customers, anomaly rate %, last reading timestamp
- **Rolling line chart** — 5-minute heart-rate window for the top 6 most active customers
- **Anomaly table** — Latest bradycardia / tachycardia events with reasons
- **Throughput bar chart** — Messages processed per minute (from `pipeline_log`)
- **Per-customer status table** — Latest BPM and status for every customer
- **Auto-refresh** — Reruns every 5 seconds automatically

---

## 📁 Project Structure

```
DEM10_Lab/
├── docker-compose.yml        # Service orchestration
├── db/
│   └── init.sql              # Idempotent PostgreSQL schema
├── producer/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── producer.py           # Data generator + Kafka producer
├── consumer/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── consumer.py           # Kafka consumer + validator + DB writer
├── dashboard/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── app.py                # Streamlit real-time dashboard
├── tests/
│   └── test_pipeline.py      # Unit + integration tests
└── README.md
```

---

##  Troubleshooting

| Problem | Solution |
|---------|----------|
| Kafka container doesn't become healthy | Wait an extra 30 s; Kafka takes time to elect a leader. Run `docker compose restart kafka`. |
| Consumer keeps reconnecting | Check `docker compose logs consumer`. Usually Kafka isn't ready yet — it will retry with back-off. |
| Dashboard shows "Waiting for data" | Ensure the producer is running: `docker compose ps producer`. |
| PostgreSQL port 5432 conflict | Change `ports: - "5432:5432"` to e.g. `"5433:5432"` in `docker-compose.yml`. |
