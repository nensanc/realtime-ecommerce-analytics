-- ============================================================
-- Real-Time E-Commerce Analytics - PostgreSQL Schema
-- Runs automatically on first container start
-- ============================================================

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ============================================
-- sales_metrics: aggregated per time window
-- ============================================
CREATE TABLE IF NOT EXISTS sales_metrics (
    id                SERIAL PRIMARY KEY,
    window_start      TIMESTAMP NOT NULL,
    window_end        TIMESTAMP NOT NULL,
    total_sales       DECIMAL(12, 2) NOT NULL DEFAULT 0,
    order_count       INTEGER NOT NULL DEFAULT 0,
    avg_order_value   DECIMAL(10, 2) NOT NULL DEFAULT 0,
    unique_customers  INTEGER NOT NULL DEFAULT 0,
    category          VARCHAR(100),
    created_at        TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    -- Idempotency key for streaming UPSERT (one row per window+category).
    -- NULLS NOT DISTINCT treats NULL category as a real value for uniqueness.
    CONSTRAINT uq_sales_metrics_window_category
        UNIQUE NULLS NOT DISTINCT (window_start, category)
);

CREATE INDEX IF NOT EXISTS idx_sales_metrics_window
    ON sales_metrics (window_start DESC, window_end DESC);

CREATE INDEX IF NOT EXISTS idx_sales_metrics_category
    ON sales_metrics (category, window_start DESC);

-- ============================================
-- fraud_events
-- ============================================
CREATE TABLE IF NOT EXISTS fraud_events (
    id               SERIAL PRIMARY KEY,
    transaction_id   UUID NOT NULL,
    alert_type       VARCHAR(50) NOT NULL,
    user_id          INTEGER NOT NULL,
    amount           DECIMAL(12, 2) NOT NULL,
    fraud_score      DECIMAL(3, 2) NOT NULL CHECK (fraud_score BETWEEN 0 AND 1),
    reason           TEXT NOT NULL,
    location_city    VARCHAR(100),
    location_country VARCHAR(100),
    detected_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    -- Idempotency: the same transaction can be flagged once per rule type,
    -- but not twice by the same rule on replay.
    CONSTRAINT uq_fraud_events_tx_type
        UNIQUE (transaction_id, alert_type)
);

CREATE INDEX IF NOT EXISTS idx_fraud_events_user
    ON fraud_events (user_id, detected_at DESC);

CREATE INDEX IF NOT EXISTS idx_fraud_events_detected_at
    ON fraud_events (detected_at DESC);

-- ============================================
-- inventory_status
-- ============================================
CREATE TABLE IF NOT EXISTS inventory_status (
    id                SERIAL PRIMARY KEY,
    product_id        INTEGER NOT NULL UNIQUE,
    product_name      VARCHAR(255) NOT NULL,
    category          VARCHAR(100),
    stock_level       INTEGER NOT NULL DEFAULT 0,
    sales_last_hour   INTEGER NOT NULL DEFAULT 0,
    alert_triggered   BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at        TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_inventory_alert
    ON inventory_status (alert_triggered, stock_level);

-- ============================================
-- Convenience view for the dashboard
-- ============================================
CREATE OR REPLACE VIEW recent_sales_summary AS
SELECT
    DATE_TRUNC('minute', window_start) AS minute,
    SUM(total_sales)                   AS total_sales,
    SUM(order_count)                   AS total_orders,
    AVG(avg_order_value)               AS avg_order_value,
    SUM(unique_customers)              AS unique_customers
FROM sales_metrics
WHERE window_start >= CURRENT_TIMESTAMP - INTERVAL '1 hour'
GROUP BY DATE_TRUNC('minute', window_start)
ORDER BY minute DESC;

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO ecommerce_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO ecommerce_user;

DO $$
BEGIN
    RAISE NOTICE '✓ E-commerce analytics schema initialized successfully';
END $$;
