# Project Progress

> Quick status dashboard. For detailed sprint logs see [`docs/SPRINT_PROGRESS.md`](docs/SPRINT_PROGRESS.md).

**Last updated:** May 14, 2026
**Current phase:** Sprint 3 complete ✅ → Sprint 4 next

---

## 📍 Where I am

```
Sprint 0  ✅  Infrastructure (Docker stack, schema, .env)
Sprint 1  ✅  Transaction generator (Kafka producer + catalog + users)
Sprint 2  ✅  PySpark Structured Streaming → PostgreSQL
Sprint 3  ✅  Fraud detection (3 rules: stateless + 2 stateful)
Sprint 4  🔜  Streamlit dashboard
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
2. **Static reference data** (`streaming/producers/catalog.py`)
   - 100 products across 5 categories, 1000 users (70% Colombia, 30% intl)
   - Deterministic seed (42) for reproducible debugging
3. **Transaction generator** (`streaming/producers/transaction_generator.py`)
   - Configurable rate, keyed by `user_id`, async sends, graceful shutdown
   - Sprint 3 update: 2% probability of anomalous location (to seed geographic fraud)

### Verified

| Metric | Target | Actual |
|---|---|---|
| Throughput | 10 TPS | 9.9 TPS |
| Errors over 500+ msgs | 0 | 0 |

### Lessons captured

- `kafka-python` is unmaintained → use `kafka-python-ng` for Python 3.12
- Don't name local packages after installed libraries (renamed `kafka/` → `streaming/`)
- Pure-Python Kafka client lacks `enable_idempotence` / `delivery_timeout_ms`
- Quiet noisy library loggers (WARNING+ from `kafka.*`)
- Deterministic seeding makes streaming bugs reproducible

---

## ✅ Sprint 2 — PySpark Structured Streaming (DONE)

### What's working end-to-end

1. **Reusable Spark session factory** (`spark/streaming/spark_session.py`)
   - Auto-downloads Kafka + Postgres JDBC JARs via Maven coordinates
2. **Explicit transaction schema** (`spark/streaming/schemas.py`)
   - 14 fields + nested `location` struct
3. **PostgreSQL UPSERT writer** (`spark/streaming/postgres_writer.py`)
   - `psycopg2.execute_values` + `ON CONFLICT ... DO UPDATE`
   - Constraint `(window_start, category) NULLS NOT DISTINCT` for idempotency
4. **Streaming aggregation job** (`spark/streaming/process_transactions.py`)
   - 1-min tumbling windows + 5-min watermark
   - Two parallel queries: overall + per-category

### Verified end-to-end

- Categories sum exactly to overall (`$122,225.84` across 5 categories ✓)
- Order counts sum exactly (`286 = 60+40+60+60+66` ✓)
- UPSERT idempotent on replay

### Lessons captured

- Structured Streaming's 3-layer Kafka parse: binary → string → typed via `from_json`
- Explicit schemas are mandatory for streaming JSON
- Watermark bounds state; no watermark = OOM eventually
- `foreachBatch` is the escape hatch for non-native sinks
- Per-query checkpoint directories — shared paths corrupt state
- `outputMode("update")` pairs perfectly with UPSERT sinks
- `approx_count_distinct` (HyperLogLog) for streaming uniqueness

---

## ✅ Sprint 3 — Fraud Detection (DONE)

### What's working end-to-end

**Three fraud rules, all running in parallel as independent streaming queries:**

| # | Rule | Type | How it works |
|---|---|---|---|
| 1 | **High-value** | Stateless | Filter `total_amount > FRAUD_THRESHOLD` ($1000) |
| 2 | **Rapid purchases** | Stateful (windowed) | `groupBy(window, user_id) + count >= 3` per 60s |
| 3 | **Geographic impossibility** | Stateful (windowed + UDF) | `collect_list(coords)` + Haversine UDF, flag >500 km within 60s |

**Dual-sink fan-out pattern:**
Each detected fraud row goes to **both** sinks in a single `foreachBatch`:
- **PostgreSQL** `fraud_events` table (audit trail, dashboard queries)
- **Kafka** `fraud-alerts` topic (real-time consumers)

`DataFrame.persist()` ensures the upstream is computed once, then read twice.

### Components built

1. **`spark/streaming/kafka_writer.py`** — Kafka alert publisher via `foreachBatch`
2. **`spark/streaming/fraud_writer.py`** — Postgres UPSERT writer for `fraud_events`
3. **`spark/streaming/fraud_detector.py`** — Three streaming queries (~290 lines)
4. **Schema upgrade** — `fraud_events`: added `alert_type` column + unique constraint `(transaction_id, alert_type)`
5. **Generator anomaly injection** — 2% chance of mismatched location (to seed Rule 3)

### Fraud scoring formulas

| Rule | Score formula | Range |
|---|---|---|
| `high_value` | `0.5 + (amount - threshold) / (threshold * 4) * 0.5` | 0.50 → 1.00 |
| `rapid_purchases` | `0.4 + order_count * 0.08` | 0.56 → 1.00 (capped at 8+ orders) |
| `geographic_impossibility` | `0.5 + distance_km / 10000 * 0.5` | 0.55 → 1.00 |

### Verified end-to-end (5-min snapshot with NUM_USERS=50)

```
        alert_type        | detections | avg_score
--------------------------+------------+-----------
 geographic_impossibility |          1 |     0.650
 high_value               |        206 |     0.630
 rapid_purchases          |        250 |     0.979
```

**Sample geographic_impossibility alert:**

```
 user_id | fraud_score |                                reason
---------+-------------+----------------------------------------------------------------------
      49 |        0.65 | Geographic impossibility: user 49 in cities 3016 km apart within 60s
```

(Haversine math verified: `0.5 + 3016/10000 * 0.5 = 0.651` → rounds to `0.65` ✓)

### Lessons captured

- **Stateless rules** are trivial filters; **stateful rules** require `groupBy(window, key) + watermark`
- `outputMode("append")` on windowed aggregations only emits **after watermark passes window end** — first alerts can lag minutes
- `lag()` window function doesn't work in streaming `groupBy(user_id)` (Spark restriction) — windowed aggregation + UDF is the pragmatic workaround
- Fan-out sinks: one `foreachBatch` + `persist()` >> two separate `writeStream` queries
- UDFs registered with explicit return type (`DoubleType()`) for streaming compatibility
- Watermark tuning matters: 5min for production safety vs 30s for fast feedback during dev
- Composite unique constraints `(transaction_id, alert_type)` allow the same transaction to be flagged by multiple rules without duplicates per rule

---

## 🔜 Sprint 4 — Streamlit Dashboard (NEXT)

**Goal:** real-time UI on top of the data we now have in PostgreSQL.

### Planned tasks

- [ ] `streamlit==1.30.0` + `plotly==5.18.0` deps
- [ ] Live sales chart from `sales_metrics`
- [ ] Fraud feed from `fraud_events` (with score color coding)
- [ ] KPI cards: current TPS, avg order value, alert count
- [ ] Per-category breakdown chart
- [ ] Auto-refresh every 5 seconds
- [ ] (Stretch) consume directly from `fraud-alerts` Kafka topic for sub-second latency

---

## 🚀 How to run what's built so far

```bash
# 1. Start infrastructure
docker compose up -d

# 2. Activate venv
source venv/bin/activate

# Terminal 1 — Generator
python -m streaming.producers.transaction_generator

# Terminal 2 — Sales aggregation
python -m spark.streaming.process_transactions

# Terminal 3 — Fraud detection (3 rules running in parallel)
python -m spark.streaming.fraud_detector

# Terminal 4 — Inspect
docker exec -it postgres psql -U ecommerce_user -d ecommerce -c "
SELECT alert_type, COUNT(*), ROUND(AVG(fraud_score)::numeric, 3) AS avg_score
FROM fraud_events
WHERE detected_at > NOW() - INTERVAL '5 minutes'
GROUP BY alert_type
ORDER BY alert_type;
"
```

### 🧪 Tuning for fraud demos

`Rule 2` (rapid purchases) only fires reliably when there's enough user collision. With the default `NUM_USERS=1000` at 10 TPS, collisions are rare. To make Rule 2 fire heavily for demonstration:

```bash
# Temporarily reduce user pool
sed -i 's/NUM_USERS=1000/NUM_USERS=50/' .env
# Restart the generator
```

Revert when done:

```bash
sed -i 's/NUM_USERS=50/NUM_USERS=1000/' .env
```

Stop everything with `Ctrl+C`. Before laptop shutdown, run `docker compose down` to avoid stale Zookeeper state on next boot.

---

## 📊 Project structure (current)

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
│       ├── postgres_writer.py            # sales_metrics UPSERT (Sprint 2)
│       ├── process_transactions.py       # Sales aggregation (Sprint 2)
│       ├── fraud_writer.py               # fraud_events UPSERT (Sprint 3)
│       ├── kafka_writer.py               # fraud-alerts publisher (Sprint 3)
│       └── fraud_detector.py             # 3-rule fraud detector (Sprint 3)
├── dashboard/                            # Sprint 4 (empty)
├── database/
│   ├── postgres/init_schema.sql
│   └── redis/keys_structure.md
├── data/
│   ├── checkpoints/                      # 5 dirs: 2 sales + 3 fraud (gitignored)
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
