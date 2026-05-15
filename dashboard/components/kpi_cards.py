"""
KPI cards component — top row of the dashboard.

Renders four metric cards with current values and % deltas vs the
previous hour. Pure layout code; SQL lives in dashboard/utils/data.py.
"""

from __future__ import annotations

from typing import Any

import streamlit as st


def _format_delta(delta_pct: float | None) -> str | None:
    """Format a % change for Streamlit's metric() delta arg."""
    if delta_pct is None:
        return None
    sign = "+" if delta_pct >= 0 else ""
    return f"{sign}{delta_pct:.1f}% vs prev hour"


def render_kpis(kpis: dict[str, Any]) -> None:
    """Draw four KPI cards as a single row of columns."""
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric(
            label="💰 Sales (1h)",
            value=f"${kpis['sales']['current']:,.0f}",
            delta=_format_delta(kpis["sales"]["delta_pct"]),
        )

    with col2:
        st.metric(
            label="🧾 Orders (1h)",
            value=f"{kpis['orders']['current']:,}",
            delta=_format_delta(kpis["orders"]["delta_pct"]),
        )

    with col3:
        st.metric(
            label="👥 Active users (1h)",
            value=f"{kpis['users']['current']:,}",
            # No delta — unique counts aren't comparable across hours via SUM
            help="Approximate unique customer count from streaming HyperLogLog estimates",
        )

    with col4:
        # Red color for fraud growth = visual alarm; green when going down
        delta_color = "inverse"  # higher fraud = bad = red
        st.metric(
            label="🚨 Fraud alerts (1h)",
            value=f"{kpis['fraud']['current']:,}",
            delta=_format_delta(kpis["fraud"]["delta_pct"]),
            delta_color=delta_color,
        )
