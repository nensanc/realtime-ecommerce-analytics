"""
Sales-over-time line chart component.
"""

from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go
import streamlit as st


def render_sales_chart(df: pd.DataFrame) -> None:
    if df.empty:
        st.info("No sales data in the last hour. Start the streaming pipeline to see live data.")
        return

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=df["window_start"],
            y=df["total_sales"],
            mode="lines+markers",
            name="Sales",
            line=dict(color="#1f77b4", width=2),
            marker=dict(size=6),
            hovertemplate=(
                "<b>%{x|%H:%M}</b><br>"
                "Sales: $%{y:,.2f}<br>"
                "<extra></extra>"
            ),
        )
    )
    fig.update_layout(
        height=350,
        margin=dict(l=0, r=0, t=10, b=0),
        xaxis_title=None,
        yaxis_title="Sales ($)",
        hovermode="x unified",
        showlegend=False,
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
    )
    fig.update_xaxes(showgrid=False, tickformat="%H:%M")
    fig.update_yaxes(gridcolor="rgba(128,128,128,0.2)", tickformat="$,.0f")

    st.plotly_chart(fig, use_container_width=True)
