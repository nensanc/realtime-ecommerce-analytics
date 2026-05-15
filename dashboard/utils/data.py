"""
Data access layer for the dashboard.

All SQL queries live here. The UI layer (app.py + components/) imports
functions from this module — never executes raw SQL itself.
"""

from __future__ import annotations

import os
from typing import Any

import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

load_dotenv()


# ---------------------------------------------------------------------
# Engine (cached across reruns)
# ---------------------------------------------------------------------
@st.cache_resource
def get_engine() -> Engine:
    url = (
        f"postgresql+psycopg2://"
        f"{os.getenv('POSTGRES_USER', 'ecommerce_user')}:"
        f"{os.getenv('POSTGRES_PASSWORD', 'ecommerce_pass')}@"
        f"{os.getenv('POSTGRES_HOST', 'localhost')}:"
        f"{os.getenv('POSTGRES_PORT', '5432')}/"
        f"{os.getenv('POSTGRES_DB', 'ecommerce')}"
    )
    return create_engine(url, pool_pre_ping=True)


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------
def _pct_change(current: float | None, previous: float | None) -> float | None:
    if current is None or previous is None or previous == 0:
        return None
    return (current - previous) / previous * 100


# ---------------------------------------------------------------------
# KPI queries
# ---------------------------------------------------------------------
@st.cache_data(ttl=5)
def get_kpis() -> dict[str, Any]:
    engine = get_engine()

    sales_orders_sql = """
        SELECT
            SUM(CASE WHEN window_start > NOW() - INTERVAL '1 hour'
                     THEN total_sales END) AS sales_current,
            SUM(CASE WHEN window_start BETWEEN NOW() - INTERVAL '2 hours'
                                           AND NOW() - INTERVAL '1 hour'
                     THEN total_sales END) AS sales_previous,
            SUM(CASE WHEN window_start > NOW() - INTERVAL '1 hour'
                     THEN order_count END) AS orders_current,
            SUM(CASE WHEN window_start BETWEEN NOW() - INTERVAL '2 hours'
                                           AND NOW() - INTERVAL '1 hour'
                     THEN order_count END) AS orders_previous,
            SUM(CASE WHEN window_start > NOW() - INTERVAL '1 hour'
                     THEN unique_customers END) AS users_current
        FROM sales_metrics
        WHERE category IS NULL
          AND window_start > NOW() - INTERVAL '2 hours'
    """

    fraud_sql = """
        SELECT
            COUNT(*) FILTER (WHERE detected_at > NOW() - INTERVAL '1 hour')
                AS fraud_current,
            COUNT(*) FILTER (WHERE detected_at BETWEEN NOW() - INTERVAL '2 hours'
                                                  AND NOW() - INTERVAL '1 hour')
                AS fraud_previous
        FROM fraud_events
    """

    with engine.connect() as conn:
        sales = pd.read_sql(sales_orders_sql, conn).iloc[0].to_dict()
        fraud = pd.read_sql(fraud_sql, conn).iloc[0].to_dict()

    return {
        "sales": {
            "current":   float(sales["sales_current"]   or 0),
            "previous":  float(sales["sales_previous"]  or 0),
            "delta_pct": _pct_change(sales["sales_current"], sales["sales_previous"]),
        },
        "orders": {
            "current":   int(sales["orders_current"]   or 0),
            "previous":  int(sales["orders_previous"]  or 0),
            "delta_pct": _pct_change(sales["orders_current"], sales["orders_previous"]),
        },
        "users": {
            "current":   int(sales["users_current"] or 0),
            "previous":  None,
            "delta_pct": None,
        },
        "fraud": {
            "current":   int(fraud["fraud_current"]   or 0),
            "previous":  int(fraud["fraud_previous"]  or 0),
            "delta_pct": _pct_change(fraud["fraud_current"], fraud["fraud_previous"]),
        },
    }


# ---------------------------------------------------------------------
# Sales over time
# ---------------------------------------------------------------------
@st.cache_data(ttl=5)
def get_sales_over_time(hours: int = 1) -> pd.DataFrame:
    """
    Return per-minute sales for the last `hours` hours.

    Output DataFrame columns:
        window_start (datetime), total_sales (float), order_count (int)
    """
    engine = get_engine()
    sql = text("""
        SELECT
            window_start,
            total_sales,
            order_count
        FROM sales_metrics
        WHERE category IS NULL
          AND window_start > NOW() - (INTERVAL '1 hour' * :hours)
        ORDER BY window_start
    """)
    with engine.connect() as conn:
        return pd.read_sql(sql, conn, params={"hours": hours})


# ---------------------------------------------------------------------
# Sales by category
# ---------------------------------------------------------------------
@st.cache_data(ttl=5)
def get_sales_by_category(hours: int = 1) -> pd.DataFrame:
    """
    Return total sales aggregated by category for the last `hours` hours.

    Output DataFrame columns:
        category (str), total_sales (float), order_count (int)
    Sorted by total_sales DESC.
    """
    engine = get_engine()
    sql = text("""
        SELECT
            category,
            SUM(total_sales) AS total_sales,
            SUM(order_count) AS order_count
        FROM sales_metrics
        WHERE category IS NOT NULL
          AND window_start > NOW() - (INTERVAL '1 hour' * :hours)
        GROUP BY category
        ORDER BY total_sales DESC
    """)
    with engine.connect() as conn:
        return pd.read_sql(sql, conn, params={"hours": hours})


# ---------------------------------------------------------------------
# Recent fraud alerts
# ---------------------------------------------------------------------
@st.cache_data(ttl=5)
def get_recent_fraud_alerts(limit: int = 20, hours: int = 1) -> pd.DataFrame:
    """
    Return the most recent fraud detections within the last `hours` hours.

    Output DataFrame columns:
        detected_at, alert_type, user_id, amount, fraud_score,
        reason, location_city, location_country
    """
    engine = get_engine()
    sql = text("""
        SELECT
            detected_at,
            alert_type,
            user_id,
            amount,
            fraud_score,
            reason,
            location_city,
            location_country
        FROM fraud_events
        WHERE detected_at > NOW() - (INTERVAL '1 hour' * :hours)
        ORDER BY detected_at DESC
        LIMIT :limit
    """)
    with engine.connect() as conn:
        return pd.read_sql(sql, conn, params={"hours": hours, "limit": limit})
