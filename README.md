# Real-Time E-Commerce Analytics Platform

> Streaming data platform that processes e-commerce transactions in real-time using **Apache Kafka** and **PySpark Structured Streaming**, with **PostgreSQL** as the durable analytics store and **multi-rule fraud detection** running in parallel.

![Status](https://img.shields.io/badge/status-Sprint%203%20Complete-success)
![Python](https://img.shields.io/badge/python-3.12-blue)
![Kafka](https://img.shields.io/badge/kafka-3.6-black)
![Spark](https://img.shields.io/badge/spark-3.5-orange)
![PostgreSQL](https://img.shields.io/badge/postgres-15-blue)
![License](https://img.shields.io/badge/license-MIT-green)

---

## 🎯 Overview

A production-grade streaming pipeline that:

- Ingests **10–1000 transactions per second** via Kafka
- Computes **windowed sales analytics** (1-min tumbling windows, idempotent UPSERTs)
- Detects **fraud in real time** with three concurrent rules — including stateful detection with custom UDFs
- Fans out alerts to **both PostgreSQL (audit) and Kafka (real-time consumers)** in one pass
- Visualizes everything on a **Streamlit dashboard** *(Sprint 4)*

**Current status:** Sprint 3 complete — fraud detection with three rules in production.
See [`PROGRESS.md`](PROGRESS.md) for the live status dashboard.

---

## 🏗️ Architecture

```
┌─────────────────────────┐
│  Transaction Generator  │   Python + Faker, 10–1000 TPS
│  - 100 products         │   ~2% anomalous locations (for fraud testing)
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
│  - 1-min tumbling        │    │  - Rule 1 high-value (stateless)│
│  - per-category + overall│    │  - Rule 2 rapid purchases       │
│  - foreachBatch UPSERT   │    │  - Rule 3 geographic imposs.    │
└────────────┬─────────────┘    │  - dual sink fan-out            │
             │                  └─────────┬──────────────┬───────┘
             ▼                            ▼              ▼
       ┌─────────────────────────────────────────┐  ┌─────────────────┐
       │      PostgreSQL 15                      │  │   Kafka topic   │
       │      sales_metrics  +  fraud_events     │  │   fraud-alerts  │
       └─────────────────────────────────────────┘  └─────────────────┘
                            │                                │
                            ▼                                ▼
                              Streamlit Dashboard (Sprint 4 — planned)
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

---

## 🔍 Fraud detection rules

Three rules run in parallel as **independent streaming queries**, all writing to the same `fraud_events` table and `fraud-alerts` Kafka topic:

### Rule 1 — High-value (stateless)

Flags any single transaction whose amount exceeds `FRAUD_THRESHOLD` (default `$1000`).

```python
.filter(col("total_amount") > FRAUD_THRESHOLD)
```

**Score:** `0.5 + (amount - threshold) / (threshold * 4) * 0.5`, capped at 1.0.

### Rule 2 — Rapid purchases (stateful, windowed)

Flags users with **≥3 orders inside a 60-second tumbling window**. Catches card-testing and bulk-buy attacks.

```python
.groupBy(window(timestamp, "1 minute"), user_id)
.agg(count("*").alias("order_count"))
.filter(col("order_count") >= 3)
```

**Score:** `0.4 + order_count * 0.08`, capped at 1.0.

### Rule 3 — Geographic impossibility (stateful, windowed + UDF)

Flags users whose orders span **two or more cities >500 km apart within a 60-second window**. Catches stolen-account scenarios where the thief is in a different country.

Uses `collect_list()` of coordinates per user-window, then a **Haversine UDF** computes the max pairwise distance.

**Score:** `0.5 + distance_km / 10000 * 0.5`, capped at 1.0.

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

| Service     | Port  | Purpose                    |
|-------------|-------|----------------------------|
| Kafka       | 9092  | Event broker               |
| Zookeeper   | 2181  | Kafka coordination         |
| Kafka UI    | 8080  | Visual broker management   |
| PostgreSQL  | 5432  | Aggregated metrics + fraud |
| Redis       | 6379  | Real-time cache            |

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

### 4. Run the full pipeline (three terminals)

**Terminal 1 — generator:**

```bash
source venv/bin/activate
python -m streaming.producers.transaction_generator
```

**Terminal 2 — sales aggregation:**

```bash
source venv/bin/activate
python -m spark.streaming.process_transactions
```

**Terminal 3 — fraud detection:**

```bash
source venv/bin/activate
python -m spark.streaming.fraud_detector
```

⏳ First Spark startup takes ~60s. Subsequent runs are ~10s.

### 5. Query the results

**Sales metrics:**

```bash
docker exec -it postgres psql -U ecommerce_user -d ecommerce -c "
SELECT window_start, category, total_sales, order_count
FROM sales_metrics
ORDER BY window_start DESC, category NULLS FIRST
LIMIT 12;
"
```

**Fraud detections:**

```bash
docker exec -it postgres psql -U ecommerce_user -d ecommerce -c "
SELECT alert_type, COUNT(*) AS detections,
       ROUND(AVG(fraud_score)::numeric, 3) AS avg_score
FROM fraud_events
WHERE detected_at > NOW() - INTERVAL '5 minutes'
GROUP BY alert_type
ORDER BY alert_type;
"
```

Sample output:

```
        alert_type        | detections | avg_score
--------------------------+------------+-----------
 geographic_impossibility |          1 |     0.650
 high_value               |        206 |     0.630
 rapid_purchases          |        250 |     0.979
```

---

## 🧪 Tuning for fraud demos

Rule 2 (rapid purchases) only fires reliably when many users overlap in time. With the default `NUM_USERS=1000` at 10 TPS, collisions are rare. To force it for a visible demo:

```bash
# Reduce user pool — restart the generator afterwards
sed -i 's/NUM_USERS=1000/NUM_USERS=50/' .env
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
│       ├── spark_session.py
│       ├── schemas.py
│       ├── postgres_writer.py            # sales_metrics UPSERT
│       ├── process_transactions.py       # Sales aggregation job
│       ├── fraud_writer.py               # fraud_events UPSERT
│       ├── kafka_writer.py               # fraud-alerts publisher
│       └── fraud_detector.py             # 3-rule fraud detector
├── dashboard/                            # Sprint 4 (planned)
├── database/
│   ├── postgres/init_schema.sql
│   └── redis/keys_structure.md
├── data/
│   ├── checkpoints/                      # Spark state (gitignored)
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
| 3      | Fraud detection (3 rules)                  | ✅ Complete    |
| 4      | Streamlit dashboard                        | 🔜 Next        |

See [`PROGRESS.md`](PROGRESS.md) for full sprint detail.

---

## 📊 Schemas

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
    "country": "Colombia",
    "city": "Rionegro",
    "latitude": 6.1471,
    "longitude": -75.3736
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
| Key by `user_id` | Same user → same partition → ordered events per user (essential for stateful fraud detection) |
| Explicit Spark schema (not inference) | Mandatory for streaming JSON; also documents the data contract |
| 30-sec watermark (dev) | Aggressive — fast feedback. Production would use 1–5 min based on observed event lag |
| `foreachBatch` + `psycopg2` UPSERT | Spark's JDBC writer can't UPSERT; canonical workaround |
| Unique constraints with `NULLS NOT DISTINCT` | Idempotent writes on stream restart — no duplicate rows |
| Dual sink via `persist()` | Compute fraud detection once, write to Postgres + Kafka — no recomputation |
| Per-query checkpoint directories | Shared checkpoint paths corrupt state — strict isolation |
| Windowed groupBy for stateful rules (not `flatMapGroupsWithState`) | Simpler, idiomatic; misses cross-window bursts (documented tradeoff) |
| Haversine via Python UDF | Standard pattern; UDF return type explicit (`DoubleType()`) |
| `DoubleType` for money | Acceptable tradeoff for portfolio; would switch to `DecimalType(12,2)` in fintech |

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
Built as a portfolio project to demonstrate streaming data engineering skills end-to-end.

---

## 📜 License

MIT
