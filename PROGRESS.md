# Project Progress

> Quick status dashboard. For detailed sprint logs see [`docs/SPRINT_PROGRESS.md`](docs/SPRINT_PROGRESS.md).

**Last updated:** May 13, 2026
**Current phase:** Sprint 2 complete ✅ → Sprint 3 next

---

## 📍 Where I am

```
Sprint 0  ✅  Infrastructure (Docker stack, schema, .env)
Sprint 1  ✅  Transaction generator (Kafka producer + catalog + users)
Sprint 2  ✅  PySpark Structured Streaming → PostgreSQL
Sprint 3  🔜  Fraud detection
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

## ✅ Sprint 2 — PySpark Structured Streaming (DONE)

### What's working end-to-end

1. **Reusable Spark session factory** (`spark/streaming/spark_session.py`)
   - Auto-downloads Kafka connector + Postgres JDBC driver via Maven coordinates
   - Tuned for local development: 8 shuffle partitions, UTC timezone, adaptive execution
   - Library log level set to WARN (Spark is catastrophically verbose at INFO)
2. **Explicit transaction schema** (`spark/streaming/schemas.py`)
   - `StructType` with 14 top-level fields and a nested `location` struct
   - Strict nullability on critical fields (IDs, money, timestamp), lenient on names
   - Documented choice of `DoubleType` over `DecimalType` (acceptable tradeoff for portfolio)
3. **PostgreSQL UPSERT writer** (`spark/streaming/postgres_writer.py`)
   - `psycopg2` + `execute_values` for bulk UPSERT inside `foreachBatch`
   - `ON CONFLICT ... DO UPDATE` against `(window_start, category)` unique constraint
   - `NULLS NOT DISTINCT` (Postgres 15+) so NULL-category overall rows are also deduplicated
   - Context-managed connection (commit on success, rollback on error)
4. **Streaming aggregation job** (`spark/streaming/process_transactions.py`)
   - Reads `transactions` topic with `startingOffsets=latest`
   - 5-minute watermark on event-time timestamps to bound state
   - 1-minute tumbling windows with two parallel queries:
     - **Overall metrics** (category=NULL): total_sales, order_count, avg_order_value, unique_customers
     - **Per-category breakdown** (category populated): category_sales, category_orders
   - 10-second trigger interval, `outputMode("update")`
   - Separate checkpoint directories per query

### Verified end-to-end

Sample query against `sales_metrics` after running both terminals for ~2 minutes:

```
    window_start     |  category   | total_sales | order_count | avg_order_value | unique_customers
---------------------+-------------+-------------+-------------+-----------------+------------------
 2026-05-13 16:09:00 |             |   122225.84 |         286 |          427.36 |              242
 2026-05-13 16:09:00 | Books       |     7070.86 |          60 |          117.85 |                0
 2026-05-13 16:09:00 | Clothing    |     8524.48 |          40 |          213.11 |                0
 2026-05-13 16:09:00 | Electronics |    73598.57 |          60 |         1226.64 |                0
 2026-05-13 16:09:00 | Home        |    22041.06 |          60 |          367.35 |                0
 2026-05-13 16:09:00 | Sports      |    10990.87 |          66 |          166.53 |                0
```

**Sanity checks that pass:**

- Categories sum exactly to overall (`$7,070.86 + $8,524.48 + $73,598.57 + $22,041.06 + $10,990.87 = $122,225.84` ✓)
- Order counts sum exactly (`60 + 40 + 60 + 60 + 66 = 286` ✓)
- `unique_customers` = 242 out of 286 orders ≈ 85% unique (HyperLogLog estimate, plausible)
- Electronics drives revenue ($73K) while having the same order count as Books/Home — high-ticket behavior matches realistic e-commerce

### Lessons captured

- Structured Streaming's 3-layer Kafka parse: binary → string → typed via `from_json` + schema
- Explicit schemas are **mandatory** for streaming JSON sources (no inference possible)
- Tumbling windows + watermarks are the foundation of streaming analytics — without watermark, state grows forever
- `foreachBatch` is the escape hatch for non-native sinks (JDBC writer can't UPSERT)
- Never `.collect()` raw stream; only post-aggregation results (tiny by definition)
- Each streaming query needs its **own** checkpoint directory — shared paths corrupt state
- `outputMode("update")` pairs perfectly with UPSERT sinks; `complete` is OOM-prone, `append` adds latency
- PySpark's API isn't 1:1 with Scala (e.g., `StructType.treeString` doesn't exist in Python)
- `approx_count_distinct` (HyperLogLog) is the right unique-count primitive for streaming aggregations

---

## 🔜 Sprint 3 — Fraud Detection (NEXT)

**Goal:** identify suspicious transactions in real time and publish alerts.

### Planned tasks

- [ ] High-value transaction rule: any single order over `FRAUD_THRESHOLD` ($1000 default)
- [ ] Rapid-purchase pattern: ≥3 orders from same user in <60 seconds (stateful)
- [ ] Geographic impossibility: same user, two cities >500km apart, within 5 minutes
- [ ] Publish alerts to Kafka `fraud-alerts` topic
- [ ] Persist events to `fraud_events` table with `fraud_score` (0.0-1.0) and `reason`
- [ ] Unit tests for the rules

---

## ⏳ Sprint 4 — Streamlit Dashboard (PLANNED)

- Real-time sales chart from `sales_metrics` (Plotly)
- Alerts panel reading from Redis cache
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

# Terminal 1 — Generator
python -m streaming.producers.transaction_generator

# Terminal 2 — Spark streaming → Postgres
python -m spark.streaming.process_transactions

# Terminal 3 — Inspect the data being written
docker exec -it postgres psql -U ecommerce_user -d ecommerce -c "
SELECT window_start, category, total_sales, order_count
FROM sales_metrics
ORDER BY window_start DESC, category NULLS FIRST
LIMIT 10;
"
```

Stop all terminals with `Ctrl+C`. Before laptop shutdown, run `docker compose down` to avoid stale Zookeeper state on next boot.

---

## 📊 Project structure (current)

```
realtime-ecommerce-analytics/
├── streaming/                         # Sprint 1: Kafka producer side
│   ├── __init__.py
│   ├── smoke_test.py
│   ├── producers/
│   │   ├── __init__.py
│   │   ├── config.py
│   │   ├── catalog.py
│   │   └── transaction_generator.py
│   └── consumers/
│       └── __init__.py
├── spark/                             # Sprint 2: Spark streaming side
│   ├── __init__.py
│   ├── streaming/
│   │   ├── __init__.py
│   │   ├── spark_session.py           # Session factory
│   │   ├── schemas.py                 # Typed transaction schema
│   │   ├── postgres_writer.py         # UPSERT writer (foreachBatch)
│   │   └── process_transactions.py    # Main streaming job
│   └── batch/                         # (reserved for batch jobs)
├── dashboard/                         # Sprint 4 (empty)
├── database/
│   ├── postgres/init_schema.sql
│   └── redis/keys_structure.md
├── data/
│   ├── checkpoints/                   # Spark streaming state (gitignored)
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
