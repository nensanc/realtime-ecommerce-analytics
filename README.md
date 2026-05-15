# Real-Time E-Commerce Analytics Platform

> End-to-end streaming data platform: **Kafka** for ingestion, **PySpark Structured Streaming** for processing, **PostgreSQL** for storage, **Streamlit** for the real-time dashboard. Includes a multi-rule fraud detection system with stateless, windowed, and UDF-based stateful detection.

![Status](https://img.shields.io/badge/status-Complete-success)
![Python](https://img.shields.io/badge/python-3.12-blue)
![Kafka](https://img.shields.io/badge/kafka-3.6-black)
![Spark](https://img.shields.io/badge/spark-3.5-orange)
![PostgreSQL](https://img.shields.io/badge/postgres-15-blue)
![Streamlit](https://img.shields.io/badge/streamlit-1.30-red)
![License](https://img.shields.io/badge/license-MIT-green)

---

## 🎯 Overview

A complete streaming analytics pipeline running locally on Docker:

- Ingests **10–1000 transactions per second** via Kafka
- Computes **windowed sales analytics** (1-min tumbling windows, idempotent UPSERTs)
- Detects **fraud in real time** with three concurrent rules
- Fans out alerts to **both PostgreSQL (audit) and Kafka (real-time consumers)**
- Visualizes everything on a **Streamlit dashboard** with auto-refresh

See [`PROGRESS.md`](PROGRESS.md) for the detailed sprint-by-sprint log.

---

## 🏗️ Architecture

```
┌─────────────────────────┐
│  Transaction Generator  │   Python + Faker, 10–1000 TPS
│  - 100 products         │   2% anomalous locations
│  - 1000 users           │
└───────────┬─────────────┘
            │  key = user_id
            ▼
   [ Kafka topic: transactions ]   3 partitions, gzip, JSON
            │
            ├──────────────────────────────────┐
            ▼                                  ▼
┌──────────────────────────┐    ┌────────────────────────────────┐
│  Sales Aggregator        │    │  Fraud Detector                │
│  (process_transactions)  │    │  (fraud_detector)              │
│  - 1-min tumbling        │    │  - Rule 1 high-value           │
│  - per-category + overall│    │  - Rule 2 rapid purchases      │
│  - foreachBatch UPSERT   │    │  - Rule 3 geographic imposs.   │
└────────────┬─────────────┘    │  - dual sink fan-out           │
             │                  └─────────┬──────────────┬───────┘
             ▼                            ▼              ▼
       ┌─────────────────────────────────────────┐  ┌─────────────────┐
       │      PostgreSQL 15                      │  │   Kafka topic   │
       │      sales_metrics  +  fraud_events     │  │   fraud-alerts  │
       └─────────────────┬───────────────────────┘  └─────────────────┘
                         │
                         ▼
                ┌────────────────────┐
                │ Streamlit Dashboard│
                │ http://localhost:8501
                │  - 4 KPI cards    │
                │  - Sales line chart│
                │  - Category bars  │
                │  - Fraud table    │
                └────────────────────┘
```

---

## 🛠️ Tech Stack

| Layer              | Technology                                |
|--------------------|-------------------------------------------|
| Message broker     | Apache Kafka 3.6 + Zookeeper 3.8          |
| Kafka client       | `kafka-python-ng` 2.2.3                   |
| Stream processing  | PySpark 3.5 (Structured Streaming)        |
| Storage            | PostgreSQL 15 (with `psycopg2`)           |
| Dashboard          | Streamlit 1.30 + Plotly 5.18              |
| Auto-refresh       | `streamlit-autorefresh` 1.0.1             |
| Data generation    | Faker 22.0                                |
| Infrastructure     | Docker Compose                            |
| Language           | Python 3.12 + Java 17                     |

---

## 🚀 Quick Start

### Prerequisites

- Docker & Docker Compose
- Python 3.12+
- OpenJDK 17 (Spark requirement)
- ~6 GB RAM available

### 1. Clone & configure

```bash
git clone https://github.com/nensanc/realtime-ecommerce-analytics.git
cd realtime-ecommerce-analytics
cp .env.example .env
```

### 2. Start infrastructure

```bash
docker compose up -d
```

| Service     | Port  | Purpose                    |
|-------------|-------|----------------------------|
| Kafka       | 9092  | Event broker               |
| Zookeeper   | 2181  | Kafka coordination         |
| Kafka UI    | 8080  | Visual broker management   |
| PostgreSQL  | 5432  | Aggregated metrics + fraud |
| Redis       | 6379  | Real-time cache (reserved) |

```bash
docker compose ps   # all 5 should be (healthy)
```

### 3. Set up Python environment

```bash
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

### 4. Run the full pipeline (four terminals)

```bash
# Terminal 1 — generator
source venv/bin/activate
python -m streaming.producers.transaction_generator

# Terminal 2 — sales aggregation
source venv/bin/activate
python -m spark.streaming.process_transactions

# Terminal 3 — fraud detection
source venv/bin/activate
python -m spark.streaming.fraud_detector

# Terminal 4 — dashboard (PYTHONPATH=. is required)
source venv/bin/activate
PYTHONPATH=. streamlit run dashboard/app.py
```

⏳ First Spark startup takes ~60s (downloads Kafka & Postgres JARs). Subsequent runs are ~10s.

### 5. Open the dashboard

**http://localhost:8501**

You should see:

- **4 KPI cards** (sales, orders, users, fraud) with hour-over-hour deltas
- **Line chart** of sales per minute over the last hour
- **Bar chart** of sales by category, color-coded
- **Fraud alerts table** with score-based color coding (green/yellow/red)
- **Sidebar** with auto-refresh interval, time window selector, max alerts slider

The dashboard auto-refreshes every 5 seconds by default — watch the numbers grow live.

---

## 🔍 Fraud detection rules

Three rules run in parallel as **independent streaming queries**, all writing to the same `fraud_events` table and `fraud-alerts` Kafka topic.

### Rule 1 — High-value (stateless)

Flags any single transaction whose amount exceeds `FRAUD_THRESHOLD` (default `$1000`).

**Score:** `0.5 + (amount - threshold) / (threshold * 4) * 0.5`, capped at 1.0.

### Rule 2 — Rapid purchases (stateful, windowed)

Flags users with **≥3 orders inside a 60-second tumbling window**. Catches card-testing and bulk-buy attacks.

**Score:** `0.4 + order_count * 0.08`, capped at 1.0.

### Rule 3 — Geographic impossibility (stateful, windowed + UDF)

Flags users whose orders span **two or more cities >500 km apart within a 60-second window**. Catches stolen-account scenarios where the thief is in a different country.

Uses `collect_list()` of coordinates per user-window, then a **Haversine UDF** computes the max pairwise distance.

**Score:** `0.5 + distance_km / 10000 * 0.5`, capped at 1.0.

---

## 🧪 Tuning for fraud demos

Rule 2 (rapid purchases) only fires reliably when many users overlap in time. With the default `NUM_USERS=1000` at 10 TPS, collisions are rare. To force it for a visible demo:

```bash
sed -i 's/NUM_USERS=1000/NUM_USERS=50/' .env
# Restart the generator afterwards
```

Revert when done:

```bash
sed -i 's/NUM_USERS=50/NUM_USERS=1000/' .env
```

---

## 🧪 Smoke tests

Each component has its own self-test for isolated verification.

| Test                                              | What it verifies                       |
|---------------------------------------------------|----------------------------------------|
| `python streaming/smoke_test.py produce/consume`  | Python ↔ Kafka                         |
| `python -m streaming.producers.config`            | Producer factory                       |
| `python -m streaming.producers.catalog`           | Catalog + user pool                    |
| `python -m spark.streaming.spark_session`         | Spark session boots                    |
| `python -m spark.streaming.schemas`               | Transaction schema parses              |
| `python -m spark.streaming.postgres_writer`       | Postgres connection                    |
| `python -m spark.streaming.fraud_writer`          | fraud_events writer                    |
| `python -m spark.streaming.kafka_writer`          | fraud-alerts publisher                 |

---

## 📁 Project Structure

```
realtime-ecommerce-analytics/
├── streaming/                            # Kafka producer side
│   ├── smoke_test.py
│   └── producers/
│       ├── config.py
│       ├── catalog.py
│       └── transaction_generator.py
├── spark/                                # Spark streaming side
│   └── streaming/
│       ├── spark_session.py              # Session factory
│       ├── schemas.py                    # Typed schema
│       ├── postgres_writer.py            # sales_metrics UPSERT
│       ├── process_transactions.py       # Sales aggregation job
│       ├── fraud_writer.py               # fraud_events UPSERT
│       ├── kafka_writer.py               # fraud-alerts publisher
│       └── fraud_detector.py             # 3-rule fraud detector
├── dashboard/                            # Streamlit web UI
│   ├── app.py
│   ├── utils/data.py
│   └── components/
│       ├── kpi_cards.py
│       ├── sales_chart.py
│       ├── category_chart.py
│       └── fraud_table.py
├── database/
│   ├── postgres/init_schema.sql
│   └── redis/keys_structure.md
├── data/                                 # checkpoints, logs, output (gitignored)
├── tests/
├── docs/
├── docker-compose.yml
├── requirements.txt
├── .env.example
├── PROGRESS.md
└── README.md
```

---

## 📋 Sprint progress

| Sprint | Description                                | Status         |
|--------|--------------------------------------------|----------------|
| 0      | Infrastructure setup                       | ✅ Complete    |
| 1      | Transaction generator (Kafka producer)     | ✅ Complete    |
| 2      | PySpark streaming → PostgreSQL             | ✅ Complete    |
| 3      | Fraud detection (3 rules)                  | ✅ Complete    |
| 4      | Streamlit dashboard                        | ✅ Complete    |

See [`PROGRESS.md`](PROGRESS.md) for sprint-by-sprint detail.

---

## 📊 Data schemas

### Transaction event (Kafka `transactions` topic)

```json
{
  "transaction_id": "550e8400-...",
  "timestamp": "2026-05-14T20:55:01.123456+00:00",
  "user_id": 23,
  "user_name": "John Doe",
  "product_id": 678,
  "product_name": "Laptop PRO-1234",
  "category": "Electronics",
  "quantity": 1,
  "unit_price": 1299.99,
  "total_amount": 1299.99,
  "payment_method": "credit_card",
  "location": {
    "country": "Colombia", "city": "Rionegro",
    "latitude": 6.1471, "longitude": -75.3736
  },
  "session_id": "sess_a1b2c3d4",
  "device_type": "mobile"
}
```

### Fraud alert (Kafka `fraud-alerts` topic + `fraud_events` table)

```json
{
  "alert_id": "uuid",
  "alert_type": "high_value | rapid_purchases | geographic_impossibility",
  "fraud_score": 0.85,
  "reason": "High-value order: $5420.00 exceeds threshold $1000.00",
  "user_id": 12345,
  "transaction_id": "550e8400-...",
  "amount": 5420.0,
  "location": { "country": "Colombia", "city": "Bogotá", "latitude": 4.711, "longitude": -74.0721 },
  "detected_at": "2026-05-14T20:55:18.234Z"
}
```

---

## 🧠 Key Design Decisions

| Decision | Rationale |
|---|---|
| `acks=all` + retries in producer | Durability over throughput — no data loss on broker failure |
| Key by `user_id` | Same user → same partition → ordered events per user (essential for stateful fraud) |
| Explicit Spark schema (not inference) | Mandatory for streaming JSON; documents the data contract |
| `foreachBatch` + `psycopg2` UPSERT | Spark's JDBC writer can't UPSERT; canonical workaround |
| Unique constraints with `NULLS NOT DISTINCT` | Idempotent writes on stream restart — no duplicate rows |
| Dual sink via `persist()` | Compute fraud detection once, write to Postgres + Kafka — no recomputation |
| Per-query checkpoint directories | Shared checkpoint paths corrupt state — strict isolation |
| Windowed groupBy for stateful rules | Simpler, idiomatic; documented tradeoff vs `flatMapGroupsWithState` |
| Haversine via Python UDF | Standard pattern; UDF return type explicit (`DoubleType()`) |
| `DoubleType` for money | Acceptable tradeoff for portfolio; would switch to `DecimalType(12,2)` in fintech |
| `-Duser.timezone=UTC` on JVM | **Critical** — without this, Spark JDBC writes timestamps in JVM local TZ, breaking time-range queries |
| Streamlit `@st.cache_data(ttl=5)` | Fresh enough for real-time, light on DB |
| Sidebar config | Clean UX; the dashboard's main canvas stays focused on data |

---

## 🛑 Stopping the stack

```bash
docker compose down       # keep data
docker compose down -v    # wipe volumes (clean slate)
```

> **Important:** always run `docker compose down` before laptop shutdown. Otherwise Zookeeper may keep stale ephemeral broker registrations and Kafka won't start next time.

---

## 👤 Author

**Martin Sanchez** — Senior Data Engineer
Built as a portfolio project to demonstrate end-to-end streaming data engineering skills.

---

## 📜 License

MIT
