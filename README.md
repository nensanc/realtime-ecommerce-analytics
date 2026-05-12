# Real-Time E-Commerce Analytics Platform

> Streaming data platform that processes e-commerce transactions in real-time using **Apache Kafka** and **PySpark Structured Streaming**.

![Status](https://img.shields.io/badge/status-Sprint%201%20Complete-success)
![Python](https://img.shields.io/badge/python-3.12-blue)
![Kafka](https://img.shields.io/badge/kafka-3.6-black)
![Spark](https://img.shields.io/badge/spark-3.5-orange)
![License](https://img.shields.io/badge/license-MIT-green)

---

## 🎯 Overview

A production-grade streaming pipeline that:

- Ingests **10–1000 transactions per second** via Kafka
- Processes events with **PySpark Structured Streaming** *(Sprint 2)*
- Detects **fraud in real time** using windowed aggregations *(Sprint 3)*
- Tracks **inventory levels** and triggers low-stock alerts *(Sprint 3)*
- Visualizes everything on a **Streamlit dashboard** with ~5s end-to-end latency *(Sprint 4)*

**Current status:** Sprint 1 complete — Kafka producer pipeline fully working.
See [`PROGRESS.md`](PROGRESS.md) for the live status dashboard.

---

## 🏗️ Architecture

```
┌─────────────────────────┐
│  Transaction Generator  │  (Python + Faker, 10–1000 TPS)
│  - 100 products         │
│  - 1000 users           │
└───────────┬─────────────┘
            │ key = user_id
            ▼
   [ Kafka topic: transactions ]   3 partitions, gzip, JSON
            │
            ▼
┌─────────────────────────┐
│  PySpark Streaming      │   (Sprint 2 — not yet built)
│  - Sales metrics        │
│  - Fraud detection      │
│  - Inventory tracking   │
└──────┬────────┬─────────┘
       │        │
       ▼        ▼
  PostgreSQL  Redis    →  Streamlit Dashboard
  (history)   (cache)     (Sprint 4)
```

---

## 🛠️ Tech Stack

| Layer              | Technology                                |
|--------------------|-------------------------------------------|
| Message broker     | Apache Kafka 3.6 + Zookeeper 3.8          |
| Kafka client       | `kafka-python-ng` 2.2.3                   |
| Stream processing  | PySpark 3.5 (Structured Streaming)        |
| Storage            | PostgreSQL 15                             |
| Cache              | Redis 7                                   |
| Dashboard          | Streamlit + Plotly                        |
| Data generation    | Faker 22.0                                |
| Infrastructure     | Docker Compose                            |
| Language           | Python 3.12                               |

> **Note on the Kafka client:** This project uses [`kafka-python-ng`](https://github.com/dpkp/kafka-python/issues/2412), a community-maintained fork of the (now unmaintained) `kafka-python` library, for compatibility with Python 3.12.

---

## 🚀 Quick Start

### Prerequisites

- Docker & Docker Compose
- Python 3.12+
- ~4 GB RAM available for containers

### 1. Clone & configure

```bash
git clone <repo-url>
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
| Kafka UI    | 8080  | Visual management          |
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

### 4. Run the transaction generator

```bash
python -m streaming.producers.transaction_generator
```

You should see something like:

```
[INFO] __main__: Building catalog (100 products)...
[INFO] __main__: Building user pool (1000 users)...
[INFO] __main__: ✓ Kafka producer ready
[INFO] __main__: ▶ Generating transactions → topic='transactions' rate=10 TPS
[INFO] __main__: Sent=50 errors=0 elapsed=5.0s actual_tps=9.9
...
```

Stop with `Ctrl+C` — the producer flushes pending messages before exiting.

### 5. Watch the data flow

Open **Kafka UI** at [http://localhost:8080](http://localhost:8080):

- Topics → `transactions` → Messages tab
- You'll see realistic e-commerce events with full transaction details

---

## 🧪 Smoke tests

Verify the core integrations without running the full generator.

### Kafka connectivity

```bash
python streaming/smoke_test.py produce   # send 1 message
python streaming/smoke_test.py consume   # read all messages back
```

### Producer self-test

```bash
python -m streaming.producers.config
```

### Catalog & user pool

```bash
python -m streaming.producers.catalog
```

### PostgreSQL

```bash
docker exec -it postgres psql -U ecommerce_user -d ecommerce -c "\dt"
```

Expected tables: `sales_metrics`, `fraud_events`, `inventory_status`.

### Redis

```bash
docker exec -it redis redis-cli PING
# → PONG
```

---

## 📁 Project Structure

```
realtime-ecommerce-analytics/
├── streaming/                  # Kafka producers & consumers
│   ├── smoke_test.py           # Connectivity smoke test
│   └── producers/
│       ├── config.py           # Reusable producer factory
│       ├── catalog.py          # Products + users
│       └── transaction_generator.py
├── spark/                      # PySpark streaming jobs (Sprint 2)
├── dashboard/                  # Streamlit app (Sprint 4)
├── database/
│   ├── postgres/init_schema.sql
│   └── redis/keys_structure.md
├── data/                       # checkpoints, logs (gitignored)
├── tests/
├── docs/
├── docker-compose.yml
├── requirements.txt
├── .env.example
├── PROGRESS.md                 # Live status dashboard
└── README.md
```

---

## 📋 Sprint Progress

| Sprint | Description                  | Status         |
|--------|------------------------------|----------------|
| 0      | Infrastructure setup         | ✅ Complete    |
| 1      | Transaction generator        | ✅ Complete    |
| 2      | PySpark streaming jobs       | 🔜 Next        |
| 3      | Fraud detection              | ⏳ Planned     |
| 4      | Streamlit dashboard          | ⏳ Planned     |

See [`PROGRESS.md`](PROGRESS.md) for current details and [`docs/SPRINT_PROGRESS.md`](docs/SPRINT_PROGRESS.md) for the detailed sprint log.

---

## 📊 Transaction Event Schema

Each message sent to the `transactions` topic follows this shape:

```json
{
  "transaction_id": "550e8400-e29b-41d4-a716-446655440000",
  "timestamp": "2026-05-12T20:30:45.123456+00:00",
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

## 🛑 Stopping the stack

```bash
# Stop containers but keep data
docker compose down

# Stop AND wipe volumes (clean slate)
docker compose down -v
```

---

## 👤 Author

**Martin Sanchez** — Senior Data Engineer
Built as a portfolio project to demonstrate streaming data engineering skills.

---

## 📜 License

MIT
