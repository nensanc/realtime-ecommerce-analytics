"""
Real-Time E-Commerce Analytics Dashboard.

Run:
    PYTHONPATH=. streamlit run dashboard/app.py
Open:
    http://localhost:8501
"""

from __future__ import annotations

from datetime import datetime, timezone

import streamlit as st
from streamlit_autorefresh import st_autorefresh

from components.kpi_cards import render_kpis
from components.sales_chart import render_sales_chart
from components.category_chart import render_category_chart
from components.fraud_table import render_fraud_table
from utils.data import (
    get_engine,
    get_kpis,
    get_sales_over_time,
    get_sales_by_category,
    get_recent_fraud_alerts,
)


# ---------------------------------------------------------------------
# Page configuration
# ---------------------------------------------------------------------
st.set_page_config(
    page_title="E-Commerce Analytics",
    page_icon="🛒",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ---------------------------------------------------------------------
# Sidebar — configuration
# ---------------------------------------------------------------------
with st.sidebar:
    st.title("⚙️ Settings")

    refresh_seconds = st.slider(
        "Auto-refresh interval (seconds)",
        min_value=2,
        max_value=60,
        value=5,
        step=1,
        help="How often the dashboard re-queries Postgres. Lower = fresher data, but more DB load.",
    )

    time_window_hours = st.selectbox(
        "Time window",
        options=[1, 2, 6, 24],
        index=0,
        format_func=lambda h: f"Last {h} hour" if h == 1 else f"Last {h} hours",
    )

    fraud_limit = st.slider(
        "Max fraud alerts shown",
        min_value=10,
        max_value=100,
        value=20,
        step=10,
    )

    st.divider()
    st.caption("**Streaming pipeline**")
    st.caption("`transactions` → Kafka → Spark → Postgres")
    st.caption("`fraud-alerts` → Kafka → Spark → Postgres")

# Auto-refresh — triggers a script rerun on a timer
st_autorefresh(interval=refresh_seconds * 1000, key="dashboard_refresh")


# ---------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------
st.title("🛒 Real-Time E-Commerce Analytics")

try:
    get_engine().connect().close()
except Exception as e:
    st.error(f"✗ Database unreachable: {e}")
    st.stop()

st.caption(
    f"Last refresh: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC"
    f"  ·  Auto-refresh every {refresh_seconds}s"
)

# ---------------------------------------------------------------------
# KPI cards
# ---------------------------------------------------------------------
kpis = get_kpis()
render_kpis(kpis)

st.divider()

# ---------------------------------------------------------------------
# Charts row — sales over time + by category
# ---------------------------------------------------------------------
left, right = st.columns([3, 2])

with left:
    st.subheader(f"📈 Sales over time (last {time_window_hours}h)")
    sales_df = get_sales_over_time(hours=time_window_hours)
    render_sales_chart(sales_df)

with right:
    st.subheader(f"📊 Sales by category (last {time_window_hours}h)")
    category_df = get_sales_by_category(hours=time_window_hours)
    render_category_chart(category_df)

st.divider()

# ---------------------------------------------------------------------
# Fraud alerts table
# ---------------------------------------------------------------------
st.subheader(f"🚨 Recent fraud alerts (last {time_window_hours}h)")
fraud_df = get_recent_fraud_alerts(limit=fraud_limit, hours=time_window_hours)
render_fraud_table(fraud_df)

# ---------------------------------------------------------------------
# Footer
# ---------------------------------------------------------------------
st.divider()
st.caption(
    "Built with Apache Kafka · PySpark Structured Streaming · PostgreSQL · Streamlit"
    "  ·  [GitHub](https://github.com/nensanc/realtime-ecommerce-analytics)"
)
