# Project Progress

> Quick status dashboard. For detailed sprint logs see [`docs/SPRINT_PROGRESS.md`](docs/SPRINT_PROGRESS.md).

**Last updated:** May 12, 2026
**Current phase:** Sprint 1 complete ✅ → Sprint 2 next

---

## 📍 Where I am

```
Sprint 0  ✅  Infrastructure (Docker stack, schema, .env)
Sprint 1  ✅  Transaction generator (Kafka producer + catalog + users)
Sprint 2  🔜  PySpark Structured Streaming
Sprint 3  ⏳  Fraud detection
Sprint 4  ⏳  Streamlit dashboard
```

---

## ✅ Sprint 0 — Infrastructure (DONE)

- Docker Compose stack: Kafka, Zookeeper, Kafka UI, PostgreSQL, Redis
- Healthchecks on all services; Kafka waits for Zookeeper via `depends_on`
- PostgreSQL schema initialized: `sales_metrics`, `fraud_events`, `inventory_status` + `recent_sales_summary` view
- Redis key-structure documented with TTL conventions
- `.env` / `.env.example` for centralized configuration
- `.gitignore` covering Python, Spark checkpoints, secrets, logs, IDE noise

---

## ✅ Sprint 1 — Transaction Generator (DONE)

### What's working end-to-end

1. **Reusable Kafka producer** (`streaming/producers/config.py`)
   - Production-grade settings: `acks=all`, retries, gzip compression, batching
   - Centralized config + JSON serialization + structured logging
   - Library logs quieted to WARNING (only app logs surface at INFO)
2. **Static reference data** (`streaming/producers/catalog.py`)
   - 100 products across 5 categories (Electronics, Clothing, Books, Home, Sports)
   - 1000 users: 70% Colombia, 30% international across 7 countries
   - Deterministic seed (42) for reproducible debugging
3. **Transaction generator** (`streaming/producers/transaction_generator.py`)
   - Continuous stream of realistic order events
   - Configurable rate via `TRANSACTION_RATE` env var (default 10 TPS)
   - Keyed by `user_id` → same user always lands on the same partition
   - Async sends with success/error callbacks
   - Graceful shutdown on SIGINT/SIGTERM (flushes in-flight messages)
4. **End-to-end validated**: Python → Kafka → visible in Kafka UI

### Verified metrics

| Metric | Target | Actual |
|---|---|---|
| Throughput | 10 TPS | 9.9 TPS (within 1%) |
| Errors over 500+ messages | 0 | 0 |
| Producer connection time | <500ms | ~100ms |
| Catalog + users build | <1s | <100ms |

### Lessons captured

- `kafka-python` is unmaintained → use `kafka-python-ng` (drop-in fork) for Python 3.12 compatibility
- Don't name local packages after installed libraries (renamed `kafka/` → `streaming/`)
- Pure-Python Kafka client lacks `enable_idempotence` and `delivery_timeout_ms` (Java/librdkafka-only); documented in code
- Quiet noisy library loggers — keep only WARNING+ from `kafka.*`
- Deterministic seeding makes streaming bugs reproducible

---

## 🔜 Sprint 2 — PySpark Structured Streaming (NEXT)

**Goal:** consume from `transactions`, compute windowed aggregations, persist to PostgreSQL.

### Planned tasks

- [ ] Add `pyspark==3.5.0` and `psycopg2-binary` to `requirements.txt`
- [ ] Spark session bootstrap with Kafka package
- [ ] Read stream from `transactions` topic
- [ ] Define schema for the JSON payload
- [ ] Compute sales metrics:
  - Total sales per 1-minute tumbling window
  - Order count, avg order value, unique customers
  - Per-category breakdown
- [ ] Write to PostgreSQL (`sales_metrics` table) via JDBC `foreachBatch`
- [ ] Configure checkpointing (`data/checkpoints/sales_metrics/`)
- [ ] Add watermarks for late-arriving data (5 min)
- [ ] Verify with: SQL query against `sales_metrics` shows live data

---

## ⏳ Sprint 3 — Fraud Detection (PLANNED)

- High-value transaction rule (> `FRAUD_THRESHOLD`)
- Rapid repeated purchases (stateful with `mapGroupsWithState`)
- Unusual location detection
- Publish alerts to `fraud-alerts` Kafka topic
- Persist events to `fraud_events` table

---

## ⏳ Sprint 4 — Streamlit Dashboard (PLANNED)

- Real-time sales chart (Plotly)
- Alerts panel reading from Redis
- Inventory grid
- Auto-refresh every 5 seconds
- KPI cards (TPS, avg order value, active users)

---

## 🚀 How to run what's built so far

```bash
# 1. Start infrastructure
docker compose up -d

# 2. Activate venv
source venv/bin/activate

# 3. Run the transaction generator
python -m streaming.producers.transaction_generator

# 4. Watch the data flow in Kafka UI
# http://localhost:8080 → Topics → transactions
```

Stop the generator with `Ctrl+C` — it will flush pending messages and exit cleanly.

---

## 📊 Project structure (current)

```
realtime-ecommerce-analytics/
├── streaming/
│   ├── __init__.py
│   ├── smoke_test.py                  # Sprint 1: connectivity check
│   ├── producers/
│   │   ├── __init__.py
│   │   ├── config.py                  # Sprint 1: producer factory
│   │   ├── catalog.py                 # Sprint 1: products + users
│   │   └── transaction_generator.py   # Sprint 1: main generator
│   └── consumers/
│       └── __init__.py
├── spark/                             # Sprint 2 (empty)
├── dashboard/                         # Sprint 4 (empty)
├── database/
│   ├── postgres/init_schema.sql
│   └── redis/keys_structure.md
├── data/                              # checkpoints, logs, output (gitignored)
├── tests/
├── docs/
├── docker-compose.yml
├── requirements.txt
├── .env / .env.example
├── .gitignore
├── PROGRESS.md
└── README.md
```
