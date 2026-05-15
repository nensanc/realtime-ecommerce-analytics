"""
Per-category bar chart component.

Renders a Plotly horizontal bar chart showing total sales per category
for the selected time range.
"""

from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go
import streamlit as st


# Color palette — one per category (consistent across runs)
CATEGORY_COLORS = {
    "Electronics": "#1f77b4",   # blue
    "Clothing":    "#ff7f0e",   # orange
    "Books":       "#2ca02c",   # green
    "Home":        "#d62728",   # red
    "Sports":      "#9467bd",   # purple
}


def render_category_chart(df: pd.DataFrame) -> None:
    """Draw the per-category sales chart."""
    if df.empty:
        st.info("No category data in the last hour.")
        return

    # Sort ascending so the highest bar appears at top in horizontal layout
    df = df.sort_values("total_sales", ascending=True)

    colors = [CATEGORY_COLORS.get(cat, "#7f7f7f") for cat in df["category"]]

    fig = go.Figure(
        go.Bar(
            x=df["total_sales"],
            y=df["category"],
            orientation="h",
            marker=dict(color=colors),
            text=df["total_sales"].apply(lambda v: f"${v:,.0f}"),
            textposition="outside",
            hovertemplate=(
                "<b>%{y}</b><br>"
                "Sales: $%{x:,.2f}<br>"
                "Orders: %{customdata:,}<br>"
                "<extra></extra>"
            ),
            customdata=df["order_count"],
        )
    )

    fig.update_layout(
        height=300,
        margin=dict(l=0, r=40, t=10, b=0),
        xaxis_title="Sales ($)",
        yaxis_title=None,
        showlegend=False,
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
    )
    fig.update_xaxes(
        gridcolor="rgba(128,128,128,0.2)",
        tickformat="$,.0f",
    )
    fig.update_yaxes(showgrid=False)

    st.plotly_chart(fig, use_container_width=True)
