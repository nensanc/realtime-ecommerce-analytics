"""
PostgreSQL UPSERT writer for streaming aggregates.

This module is the bridge between Spark Structured Streaming
(in-memory aggregates) and PostgreSQL (durable storage).

Design notes:
    - Spark's JDBC writer can't UPSERT. We use psycopg2 inside
      foreachBatch with `INSERT ... ON CONFLICT ... DO UPDATE`.
    - We open ONE connection per micro-batch. Connection pooling
      isn't worth the complexity at our throughput.
    - We collect() to the driver, but only AFTER aggregation —
      the result set is tiny (handfuls of rows per batch).
    - The unique constraint (window_start, category) NULLS NOT
      DISTINCT is what makes the UPSERT correct.
"""

from __future__ import annotations

import logging
import os
from contextlib import contextmanager
from typing import Iterator

import psycopg2
from psycopg2.extras import execute_values
from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------
# Connection config from .env
# ---------------------------------------------------------------------
PG_CONFIG = {
    "host":     os.getenv("POSTGRES_HOST",     "localhost"),
    "port":     os.getenv("POSTGRES_PORT",     "5432"),
    "dbname":   os.getenv("POSTGRES_DB",       "ecommerce"),
    "user":     os.getenv("POSTGRES_USER",     "ecommerce_user"),
    "password": os.getenv("POSTGRES_PASSWORD", "ecommerce_pass"),
}


@contextmanager
def pg_connection() -> Iterator[psycopg2.extensions.connection]:
    """Context-managed Postgres connection (commits on exit, rolls back on error)."""
    conn = psycopg2.connect(**PG_CONFIG)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ---------------------------------------------------------------------
# SQL — the centerpiece
# ---------------------------------------------------------------------
# Why ON CONFLICT ... DO UPDATE:
#   Streaming queries restart, replay batches, recompute partial windows.
#   The SAME (window_start, category) key may appear in many batches with
#   different running totals. We want the LATEST values to win.
UPSERT_SQL = """
INSERT INTO sales_metrics
    (window_start, window_end, total_sales, order_count,
     avg_order_value, unique_customers, category)
VALUES %s
ON CONFLICT ON CONSTRAINT uq_sales_metrics_window_category
DO UPDATE SET
    window_end       = EXCLUDED.window_end,
    total_sales      = EXCLUDED.total_sales,
    order_count      = EXCLUDED.order_count,
    avg_order_value  = EXCLUDED.avg_order_value,
    unique_customers = EXCLUDED.unique_customers,
    created_at       = CURRENT_TIMESTAMP
"""


# ---------------------------------------------------------------------
# foreachBatch handlers
# ---------------------------------------------------------------------
def write_overall_metrics(batch_df: DataFrame, batch_id: int) -> None:
    """
    Writer for the overall-metrics stream (category = NULL).

    Called by Spark per micro-batch. The DataFrame here is a regular
    (non-streaming) DataFrame; we can freely .collect() it.
    """
    rows = batch_df.collect()
    if not rows:
        logger.debug("batch_id=%d (overall): no rows", batch_id)
        return

    # Adapt Spark Row objects → list of tuples in the SQL's column order
    payload = [
        (
            r["window_start"],
            r["window_end"],
            float(r["total_sales"]),
            int(r["order_count"]),
            float(r["avg_order_value"]),
            int(r["unique_customers"]),
            None,  # category is NULL for the overall row
        )
        for r in rows
    ]

    with pg_connection() as conn, conn.cursor() as cur:
        execute_values(cur, UPSERT_SQL, payload)

    logger.info(
        "batch_id=%d (overall): upserted %d row(s)", batch_id, len(payload),
    )


def write_category_metrics(batch_df: DataFrame, batch_id: int) -> None:
    """
    Writer for the per-category stream.

    Note: the category breakdown DataFrame doesn't have window_end,
    avg_order_value, or unique_customers — only category_sales and
    category_orders. We map those into the appropriate columns and
    derive window_end as window_start + 1 minute.
    """
    rows = batch_df.collect()
    if not rows:
        logger.debug("batch_id=%d (category): no rows", batch_id)
        return

    from datetime import timedelta

    payload = [
        (
            r["window_start"],
            r["window_start"] + timedelta(minutes=1),  # derived window_end
            float(r["category_sales"]),
            int(r["category_orders"]),
            # avg_order_value: derive from sales / count to avoid extra agg
            float(r["category_sales"]) / max(int(r["category_orders"]), 1),
            0,                          # unique_customers not tracked here
            r["category"],
        )
        for r in rows
    ]

    with pg_connection() as conn, conn.cursor() as cur:
        execute_values(cur, UPSERT_SQL, payload)

    logger.info(
        "batch_id=%d (category): upserted %d row(s)", batch_id, len(payload),
    )


# ---------------------------------------------------------------------
# Self-test: verify connection without Spark
# ---------------------------------------------------------------------
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    logger.info("Testing Postgres connection with config: %s",
                {k: v for k, v in PG_CONFIG.items() if k != "password"})

    with pg_connection() as conn, conn.cursor() as cur:
        cur.execute("SELECT version();")
        version = cur.fetchone()[0]
        logger.info("✓ Connected. Postgres version: %s", version.split(",")[0])

        cur.execute("SELECT count(*) FROM sales_metrics;")
        count = cur.fetchone()[0]
        logger.info("✓ sales_metrics row count: %d", count)
