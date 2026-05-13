
# Real-Time E-Commerce Analytics Platform

> Streaming data platform that processes e-commerce transactions in real-time using **Apache Kafka** and **PySpark Structured Streaming**, with **PostgreSQL** as the durable analytics store.

![Status](https://img.shields.io/badge/status-Sprint%202%20Complete-success)
![Python](https://img.shields.io/badge/python-3.12-blue)
![Kafka](https://img.shields.io/badge/kafka-3.6-black)
![Spark](https://img.shields.io/badge/spark-3.5-orange)
![PostgreSQL](https://img.shields.io/badge/postgres-15-blue)
![License](https://img.shields.io/badge/license-MIT-green)

---

## 🎯 Overview

A production-grade streaming pipeline that:

- Ingests **10–1000 transactions per second** via Kafka
- Processes events with **PySpark Structured Streaming** — windowed aggregations, watermarks, idempotent UPSERTs
- Persists **1-minute windowed metrics** to PostgreSQL with full deduplication on retry
- Detects **fraud in real time** using windowed aggregations *(Sprint 3)*
- Visualizes everything on a **Streamlit dashboard** with ~5s end-to-end latency *(Sprint 4)*

**Current status:** Sprint 2 complete — full Kafka → Spark → Postgres pipeline working end-to-end.
See [`PROGRESS.md`](PROGRESS.md) for the live status dashboard.

---

## 🏗️ Architecture

```
┌─────────────────────────┐
│  Transaction Generator  │   Python + Faker, 10–1000 TPS
│  - 100 products         │
│  - 1000 users           │
└───────────┬─────────────┘
            │  key = user_id
            ▼
   [ Kafka topic: transactions ]   3 partitions, gzip, JSON
            │
            ▼
┌──────────────────────────────────────────┐
│  PySpark Structured Streaming            │
│  - JSON parse with explicit schema       │
│  - 5-min watermark on event time         │
│  - 1-min tumbling windows                │
│  - Two parallel queries:                 │
│      • Overall sales metrics             │
│      • Per-category breakdown            │
│  - foreachBatch → psycopg2 UPSERT        │
└────────────────────┬─────────────────────┘
                     │
                     ▼
            ┌────────────────────┐
            │   PostgreSQL 15    │
            │   sales_metrics    │   UNIQUE (window_start, category)
            └────────────────────┘   NULLS NOT DISTINCT
                     │
                     ▼
              Streamlit Dashboard
              (Sprint 4 — planned)
```

---

## 🛠️ Tech Stack

| Layer              | Technology                                |
|--------------------|-------------------------------------------|
| Message broker     | Apache Kafka 3.6 + Zookeeper 3.8          |
| Kafka client       | `kafka-python-ng` 2.2.3                   |
| Stream processing  | PySpark 3.5 (Structured Streaming)        |
| Storage            | PostgreSQL 15 (with `psycopg2`)           |
| Cache              | Redis 7                                   |
| Dashboard          | Streamlit + Plotly *(planned)*            |
| Data generation    | Faker 22.0                                |
| Infrastructure     | Docker Compose                            |
| Language           | Python 3.12 + Java 17 (for the JVM)       |

> **Note on the Kafka client:** uses [`kafka-python-ng`](https://github.com/wbarnha/kafka-python-ng), a community-maintained fork of the (now unmaintained) `kafka-python` library, for Python 3.12 compatibility.

---

## 🚀 Quick Start

### Prerequisites

- Docker & Docker Compose
- Python 3.12+
- OpenJDK 17 (Spark requirement)
- ~6 GB RAM available for containers + JVM

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

This starts:

| Service     | Port  | Purpose                    |
|-------------|-------|----------------------------|
| Kafka       | 9092  | Event broker               |
| Zookeeper   | 2181  | Kafka coordination         |
| Kafka UI    | 8080  | Visual broker management   |
| PostgreSQL  | 5432  | Aggregated metrics         |
| Redis       | 6379  | Real-time cache            |

Verify everything is healthy:

```bash
docker compose ps
```

All five services should report `Up (healthy)`.

### 3. Set up Python environment

```bash
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

### 4. Run the full pipeline (two terminals)

**Terminal 1 — transaction generator:**

```bash
source venv/bin/activate
python -m streaming.producers.transaction_generator
```

**Terminal 2 — Spark streaming aggregator:**

```bash
source venv/bin/activate
python -m spark.streaming.process_transactions
```

⏳ First Spark startup takes ~60s (downloads Kafka & Postgres JARs). Subsequent runs are ~10s.

You should see Spark logging per micro-batch:

```
batch_id=0 (overall): upserted 1 row(s)
batch_id=0 (category): upserted 5 row(s)
batch_id=1 (overall): upserted 1 row(s)
batch_id=1 (category): upserted 5 row(s)
```

### 5. Query the results

```bash
docker exec -it postgres psql -U ecommerce_user -d ecommerce -c "
SELECT window_start, category, total_sales, order_count, avg_order_value
FROM sales_metrics
ORDER BY window_start DESC, category NULLS FIRST
LIMIT 12;
"
```

Sample output (after ~2 minutes):

```
    window_start     |  category   | total_sales | order_count | avg_order_value
---------------------+-------------+-------------+-------------+-----------------
 2026-05-13 16:09:00 |             |   122225.84 |         286 |          427.36
 2026-05-13 16:09:00 | Books       |     7070.86 |          60 |          117.85
 2026-05-13 16:09:00 | Clothing    |     8524.48 |          40 |          213.11
 2026-05-13 16:09:00 | Electronics |    73598.57 |          60 |         1226.64
 2026-05-13 16:09:00 | Home        |    22041.06 |          60 |          367.35
 2026-05-13 16:09:00 | Sports      |    10990.87 |          66 |          166.53
```

**Verify idempotency** (categories sum exactly to overall):

```
$7,070.86 + $8,524.48 + $73,598.57 + $22,041.06 + $10,990.87 = $122,225.84  ✓
```

---

## 🧪 Smoke tests

Each component has its own self-test for isolated verification.

| Test                                              | What it verifies                       |
|---------------------------------------------------|----------------------------------------|
| `python streaming/smoke_test.py produce`          | Python → Kafka connectivity            |
| `python streaming/smoke_test.py consume`          | Kafka → Python connectivity            |
| `python -m streaming.producers.config`            | Producer factory works (3 msgs)        |
| `python -m streaming.producers.catalog`           | Catalog + user pool generation         |
| `python -m spark.streaming.spark_session`         | Spark session boots, runs trivial query|
| `python -m spark.streaming.schemas`               | Transaction schema parses correctly    |
| `python -m spark.streaming.postgres_writer`       | Postgres connection works              |

---

## 📁 Project Structure

```
realtime-ecommerce-analytics/
├── streaming/                         # Kafka producer side
│   ├── smoke_test.py
│   └── producers/
│       ├── config.py                  # Producer factory
│       ├── catalog.py                 # Products + users
│       └── transaction_generator.py   # Main generator
├── spark/                             # Spark streaming side
│   ├── streaming/
│   │   ├── spark_session.py           # Session factory
│   │   ├── schemas.py                 # Typed schema
│   │   ├── postgres_writer.py         # UPSERT writer
│   │   └── process_transactions.py    # Aggregation job
│   └── batch/                         # (planned)
├── dashboard/                         # Sprint 4 (planned)
├── database/
│   ├── postgres/init_schema.sql
│   └── redis/keys_structure.md
├── data/
│   ├── checkpoints/                   # Spark state (gitignored)
│   ├── logs/
│   └── output/
├── tests/
├── docs/
├── docker-compose.yml
├── requirements.txt
├── .env.example
├── PROGRESS.md
└── README.md
```

---

## 📋 Sprint Progress

| Sprint | Description                                | Status         |
|--------|--------------------------------------------|----------------|
| 0      | Infrastructure setup                       | ✅ Complete    |
| 1      | Transaction generator (Kafka producer)     | ✅ Complete    |
| 2      | PySpark streaming → PostgreSQL             | ✅ Complete    |
| 3      | Fraud detection                            | 🔜 Next        |
| 4      | Streamlit dashboard                        | ⏳ Planned     |

See [`PROGRESS.md`](PROGRESS.md) for live status and [`docs/SPRINT_PROGRESS.md`](docs/SPRINT_PROGRESS.md) for the detailed sprint log.

---

## 📊 Transaction Event Schema

Each message sent to the `transactions` topic:

```json
{
  "transaction_id": "550e8400-e29b-41d4-a716-446655440000",
  "timestamp": "2026-05-13T16:09:01.123456+00:00",
  "user_id": 12345,
  "user_name": "John Doe",
  "product_id": 678,
  "product_name": "Laptop PRO-1234",
  "category": "Electronics",
  "quantity": 1,
  "unit_price": 1299.99,
  "total_amount": 1299.99,
  "payment_method": "credit_card",
  "location": {
    "country": "Colombia",
    "city": "Rionegro",
    "latitude": 6.1471,
    "longitude": -75.3736
  },
  "session_id": "sess_a1b2c3d4",
  "device_type": "mobile"
}
```

**Partitioning:** messages are keyed by `user_id`, so all events from the same user always land on the same partition. This guarantees per-user ordering for stateful processing in Sprint 3.

---

## 🧠 Key Design Decisions

| Decision | Rationale |
|---|---|
| `acks=all` + retries in producer | Durability over throughput — no data loss on broker failure |
| Key by `user_id` | Same user → same partition → ordered events per user (essential for fraud detection) |
| Explicit Spark schema (not inference) | Mandatory for streaming JSON; also documents the data contract |
| 5-min watermark + 1-min windows | Bounded state, real-time enough for analytics |
| `foreachBatch` + `psycopg2` UPSERT | Spark's JDBC writer can't UPSERT; this is the canonical workaround |
| `(window_start, category) NULLS NOT DISTINCT` | Idempotent writes on stream restart — no duplicate rows |
| `DoubleType` for money (not `DecimalType`) | Acceptable tradeoff for a portfolio project; documented in code |
| Per-query checkpoint directories | Shared checkpoint paths corrupt state — strict isolation |

---

## 🛑 Stopping the stack

```bash
# Stop containers but keep data
docker compose down

# Stop AND wipe volumes (clean slate)
docker compose down -v
```

> **Important:** always run `docker compose down` before laptop shutdown. Otherwise Zookeeper may end up with stale ephemeral broker registrations, preventing Kafka from starting next time.

---

## 👤 Author

**Martin Sanchez** — Senior Data Engineer
Built as a portfolio project to demonstrate streaming data engineering skills end-to-end.

---

## 📜 License

MIT
