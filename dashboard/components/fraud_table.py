"""
Recent fraud alerts table component.

Renders a styled DataFrame with color-coded fraud scores and a
human-friendly view of the alert details.
"""

from __future__ import annotations

import pandas as pd
import streamlit as st


# Map alert_type to a friendlier label + emoji
ALERT_TYPE_DISPLAY = {
    "high_value":              "💰 High value",
    "rapid_purchases":         "⚡ Rapid purchases",
    "geographic_impossibility": "🌍 Geo impossible",
}


def _score_color(score: float) -> str:
    """Return a hex color based on fraud_score severity."""
    if score >= 0.80:
        return "#d32f2f"   # red — high concern
    if score >= 0.65:
        return "#f9a825"   # yellow — medium
    return "#388e3c"       # green — low


def render_fraud_table(df: pd.DataFrame) -> None:
    """Draw the recent-fraud table."""
    if df.empty:
        st.info("No fraud alerts in the last hour. Start the fraud detector to see live alerts.")
        return

    # Build a display copy without mutating the input
    display = df.copy()

    # Pretty alert type
    display["Alert"] = display["alert_type"].map(ALERT_TYPE_DISPLAY).fillna(display["alert_type"])

    # Format detected_at as HH:MM:SS (UTC)
    display["When"] = pd.to_datetime(display["detected_at"]).dt.strftime("%H:%M:%S")

    # Format money
    display["Amount"] = display["amount"].apply(lambda v: f"${float(v):,.2f}")

    # Location compact
    display["Location"] = display.apply(
        lambda r: f"{r['location_city']}, {r['location_country']}"
        if pd.notna(r["location_city"]) else "—",
        axis=1,
    )

    # Score with two decimals
    display["Score"] = display["fraud_score"].apply(lambda v: f"{float(v):.2f}")

    # Truncate reason to keep the row readable
    display["Reason"] = display["reason"].apply(
        lambda r: r if len(r) <= 60 else r[:57] + "..."
    )

    # Final column order
    columns = ["When", "Alert", "user_id", "Amount", "Score", "Location", "Reason"]
    display = display[columns].rename(columns={"user_id": "User"})

    # Color the Score column based on severity
    def color_score(val: str) -> str:
        try:
            score = float(val)
        except (ValueError, TypeError):
            return ""
        color = _score_color(score)
        return f"background-color: {color}; color: white; font-weight: bold;"

    styled = display.style.applymap(color_score, subset=["Score"])

    st.dataframe(
        styled,
        use_container_width=True,
        hide_index=True,
        height=420,
    )
