# Project Progress

> Live status dashboard. For detailed sprint logs see [`docs/SPRINT_PROGRESS.md`](docs/SPRINT_PROGRESS.md).

**Last updated:** May 15, 2026
**Current phase:** Sprint 4 complete ✅ — **PROJECT COMPLETE** 🎉

---

## 📍 Final state

```
Sprint 0  ✅  Infrastructure (Docker stack, schema, .env)
Sprint 1  ✅  Transaction generator (Kafka producer + catalog + users)
Sprint 2  ✅  PySpark Structured Streaming → PostgreSQL
Sprint 3  ✅  Fraud detection (3 rules: stateless + 2 stateful)
Sprint 4  ✅  Streamlit dashboard (4 KPIs + 2 charts + alerts table)
```

---

## ✅ Sprint 0 — Infrastructure (DONE)

- Docker Compose stack: Kafka, Zookeeper, Kafka UI, PostgreSQL, Redis
- Healthchecks on all services
- PostgreSQL schema initialized: `sales_metrics`, `fraud_events`, `inventory_status`
- Redis key-structure documented
- `.env` / `.env.example` for centralized configuration

---

## ✅ Sprint 1 — Transaction Generator (DONE)

- Reusable Kafka producer (`acks=all`, retries, gzip)
- 100 products across 5 categories, 1000 users (70% Colombia, 30% intl)
- Configurable rate, keyed by `user_id`
- 2% anomalous location seeding (for Rule 3 fraud)

**Verified:** 9.9 TPS sustained, 0 errors over 500+ messages.

---

## ✅ Sprint 2 — PySpark Structured Streaming (DONE)

- Reusable Spark session factory + auto-loaded Kafka/Postgres JARs
- Explicit transaction schema (14 fields + nested location struct)
- `psycopg2` + `execute_values` UPSERT inside `foreachBatch`
- `UNIQUE (window_start, category) NULLS NOT DISTINCT` for idempotency
- 1-min tumbling windows + 5-min watermark
- Two parallel queries: overall + per-category

**Verified end-to-end:** Categories sum exactly to overall ($122,225.84 across 5 cats ✓).

---

## ✅ Sprint 3 — Fraud Detection (DONE)

Three concurrent fraud rules, all dual-sinked to PostgreSQL `fraud_events` and Kafka `fraud-alerts`:

| Rule | Type | How |
|---|---|---|
| **High-value** | Stateless | `total_amount > FRAUD_THRESHOLD` |
| **Rapid purchases** | Stateful (windowed) | `groupBy(window, user) + count >= 3` per 60s |
| **Geographic impossibility** | Stateful (windowed + UDF) | Haversine distance > 500 km within 60s |

**Verified:** All 3 rules firing, with realistic scores 0.5–1.0. Sample geographic detection: "user 49 in cities 3016 km apart within 60s" (score 0.65, math: `0.5 + 3016/10000 * 0.5 = 0.65` ✓).

---

## ✅ Sprint 4 — Streamlit Dashboard (DONE)

Real-time web dashboard reading from PostgreSQL, auto-refreshing every N seconds.

### Components built

1. **`dashboard/app.py`** — Main layout, sidebar config, auto-refresh
2. **`dashboard/utils/data.py`** — 4 cached SQL queries (5s TTL)
3. **`dashboard/components/kpi_cards.py`** — 4 metric cards (sales, orders, users, fraud)
4. **`dashboard/components/sales_chart.py`** — Plotly line chart (sales over time)
5. **`dashboard/components/category_chart.py`** — Plotly horizontal bar chart (by category)
6. **`dashboard/components/fraud_table.py`** — Color-coded fraud alerts table

### Features

- **4 KPI cards** with current-hour values + % delta vs previous hour
- **Line chart** of per-minute sales with interactive tooltips
- **Bar chart** of sales-by-category with consistent color palette
- **Fraud table** with score-based color coding (green/yellow/red)
- **Sidebar config**: refresh interval, time window (1h/2h/6h/24h), max alerts
- **Auto-refresh** every 2-60s (configurable)
- **2-column layout** for charts, full-width for KPIs and table

### Sample live screenshot

```
💰 Sales (1h)    🧾 Orders (1h)    👥 Users (1h)    🚨 Fraud (1h)
$2,153,687       5,728             4,481             142
↑ 8.3% prev      ↑ 5.1% prev       (proxy)           ↑ 12.4% prev

📈 Sales over time (last hour)        📊 Sales by category (last hour)
  Line chart, ~$200K/min steady       Electronics  $1,025,041
                                       Sports       $355,364
                                       Home         $341,678
                                       Clothing     $272,589
                                       Books        $159,016

🚨 Recent fraud alerts (last hour)
21:02:11  💰 High value      471  $1,075.35   0.51 (green)   ...
21:02:11  💰 High value      726  $3,324.66   0.79 (yellow)  ...
21:01:51  ⚡ Rapid purchases   89  $129.50    0.96 (red)     ...
```

### Lessons captured

- Streamlit's `@st.cache_data(ttl=5)` is the right pattern for dashboards over streaming DBs — fresh enough, light on the DB
- Per-query cache decorator beats a single big query: each chart caches independently
- `applymap` styling on DataFrame columns for color-coded scores
- `streamlit-autorefresh` community package > manual JavaScript hacks
- Sidebar for config = clean UX without cluttering the main canvas
- **Critical fix found during dev:** Spark's `spark.sql.session.timeZone=UTC` doesn't propagate to JDBC writes. Must also set `-Duser.timezone=UTC` via `spark.driver.extraJavaOptions` and `spark.executor.extraJavaOptions`. Otherwise timestamps drift by the JVM's local-time offset.
- **Operational lesson:** checkpoints persist across restarts intentionally. When the schema, code logic, or watermark changes, wipe the affected checkpoint directory before relaunch.

---

## 🚀 How to run the full system

```bash
# 1. Infrastructure
docker compose up -d

# 2. Python env
source venv/bin/activate

# Terminal 1 — Generator
python -m streaming.producers.transaction_generator

# Terminal 2 — Sales aggregation
python -m spark.streaming.process_transactions

# Terminal 3 — Fraud detection
python -m spark.streaming.fraud_detector

# Terminal 4 — Dashboard (note PYTHONPATH=.)
PYTHONPATH=. streamlit run dashboard/app.py
```

Open **http://localhost:8501** for the dashboard, **http://localhost:8080** for Kafka UI.

Stop with `Ctrl+C` in each terminal. Before laptop shutdown: `docker compose down`.

---

## 📊 Final project structure

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
│       ├── spark_session.py              # Session factory (UTC TZ enforced)
│       ├── schemas.py                    # Typed schema
│       ├── postgres_writer.py            # sales_metrics UPSERT
│       ├── process_transactions.py       # Sales aggregation job
│       ├── fraud_writer.py               # fraud_events UPSERT
│       ├── kafka_writer.py               # fraud-alerts publisher
│       └── fraud_detector.py             # 3-rule fraud detector
├── dashboard/                            # Streamlit web UI
│   ├── app.py                            # Main layout + auto-refresh
│   ├── utils/
│   │   └── data.py                       # SQL queries (cached)
│   └── components/
│       ├── kpi_cards.py
│       ├── sales_chart.py
│       ├── category_chart.py
│       └── fraud_table.py
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

## 🎓 What this portfolio piece demonstrates

| Skill | Where it shows |
|---|---|
| **Event streaming with Kafka** | Producer with `acks=all`, key-based partitioning, idempotent sends |
| **Spark Structured Streaming** | Schemas, watermarks, windowed aggs, `foreachBatch`, stateful + stateless rules |
| **PostgreSQL operational design** | UPSERT idempotency, `NULLS NOT DISTINCT`, composite unique constraints |
| **Real-time UX** | Streamlit dashboard with cached queries, auto-refresh, interactive sidebar |
| **System design judgment** | Documented tradeoffs (Double vs Decimal, windowed vs `flatMapGroupsWithState`, JDBC TZ bug) |
| **Production operations** | Healthchecks, healthcheck-aware `depends_on`, per-query checkpoints, log noise reduction |
| **Reproducibility** | Deterministic seed, pinned deps, Docker Compose, clear smoke tests per component |
