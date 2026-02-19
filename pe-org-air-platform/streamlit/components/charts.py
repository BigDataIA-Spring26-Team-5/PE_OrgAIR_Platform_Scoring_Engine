"""
components/charts.py — Reusable Plotly chart builders for the dashboard.
"""

import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from typing import Dict, List, Optional


SECTOR_COLORS = {
    "Technology": "#6366f1",
    "Financial Services": "#0ea5e9",
    "Retail": "#10b981",
    "Manufacturing": "#f59e0b",
}

DIMENSION_ORDER = [
    "Data Infrastructure", "AI Governance", "Technology Stack",
    "Talent & Skills", "Leadership Vision", "Use Case Portfolio",
    "Culture & Change",
]

# Bright, highly distinguishable colors for radar
BRIGHT_COLORS = {
    "NVDA": "#22c55e",   # bright green
    "JPM":  "#3b82f6",   # bright blue
    "WMT":  "#f97316",   # bright orange
    "GE":   "#a855f7",   # bright purple
    "DG":   "#ef4444",   # bright red
}


def portfolio_bar_chart(df: pd.DataFrame) -> go.Figure:
    """Horizontal bar chart: Org-AI-R scores with expected range bands."""
    fig = go.Figure()

    for _, row in df.iterrows():
        fig.add_shape(
            type="rect", x0=row["Expected Low"], x1=row["Expected High"],
            y0=row["Ticker"], y1=row["Ticker"],
            fillcolor="rgba(59,130,246,0.12)", line=dict(width=0),
            layer="below",
        )

    colors = [SECTOR_COLORS.get(row["Sector"], "#6b7280") for _, row in df.iterrows()]
    fig.add_trace(go.Bar(
        x=df["Org-AI-R"], y=df["Ticker"], orientation="h",
        marker_color=colors, text=df["Org-AI-R"].round(1),
        textposition="outside", textfont=dict(size=14, color="#1e293b"),
    ))

    fig.update_layout(
        title="Portfolio Org-AI-R Scores",
        xaxis=dict(title="Org-AI-R Score", range=[0, 105]),
        yaxis=dict(autorange="reversed"),
        height=350, margin=dict(l=80, r=40, t=50, b=40),
        showlegend=False, plot_bgcolor="white",
    )
    return fig


def radar_chart(dims_df: pd.DataFrame) -> go.Figure:
    """Spider/radar chart comparing all companies across 7 dimensions."""
    fig = go.Figure()

    for _, row in dims_df.iterrows():
        ticker = row["Ticker"]
        values = [row.get(d, 0) for d in DIMENSION_ORDER]
        values.append(values[0])
        categories = DIMENSION_ORDER + [DIMENSION_ORDER[0]]
        color = BRIGHT_COLORS.get(ticker, "#6b7280")

        fig.add_trace(go.Scatterpolar(
            r=values, theta=categories, name=ticker,
            fill="toself", opacity=0.2,
            line=dict(color=color, width=3),
            fillcolor=color,
        ))

    fig.update_layout(
        polar=dict(radialaxis=dict(visible=True, range=[0, 100])),
        title="7-Dimension Comparison (All Companies)",
        height=500, margin=dict(t=60, b=40),
        legend=dict(font=dict(size=14)),
    )
    return fig


def dimension_bar_chart(dims: Dict[str, float], ticker: str) -> go.Figure:
    """Vertical bar chart for one company's 7 dimension scores with rubric bands."""
    labels = list(dims.keys())
    scores = list(dims.values())

    fig = go.Figure()
    band_colors = [
        (0, 20, "rgba(239,68,68,0.08)"),
        (20, 40, "rgba(249,115,22,0.08)"),
        (40, 60, "rgba(234,179,8,0.08)"),
        (60, 80, "rgba(34,197,94,0.08)"),
        (80, 100, "rgba(59,130,246,0.08)"),
    ]
    for y0, y1, color in band_colors:
        fig.add_hrect(y0=y0, y1=y1, fillcolor=color, line_width=0, layer="below")

    fig.add_trace(go.Bar(
        x=labels, y=scores, marker_color="#6366f1",
        text=[f"{s:.0f}" for s in scores], textposition="outside",
    ))

    fig.update_layout(
        title=f"{ticker} — 7 Dimension Scores",
        yaxis=dict(title="Score", range=[0, 105]),
        xaxis=dict(tickangle=-30),
        height=400, margin=dict(t=50, b=80),
        showlegend=False, plot_bgcolor="white",
    )
    return fig


def waterfall_chart(result: Dict, ticker: str) -> go.Figure:
    """Waterfall showing VR → HR blend → Synergy → Org-AI-R."""
    formula = result.get("formula", {})
    if not isinstance(formula, dict):
        formula = {}
    orgair = result.get("org_air_score", 0)
    wb = formula.get("weighted_base", 0)
    sc = formula.get("synergy_contribution", 0)
    vr_w = formula.get("vr_weighted", 0)
    hr_w = formula.get("hr_weighted", 0)

    fig = go.Figure(go.Waterfall(
        name="", orientation="v",
        x=["V^R (×0.6)", "H^R (×0.4)", "× (1−β)", "Synergy (×β)", "Org-AI-R"],
        y=[vr_w, hr_w, wb - vr_w - hr_w, sc, 0],
        measure=["relative", "relative", "relative", "relative", "total"],
        text=[f"{vr_w:.1f}", f"{hr_w:.1f}", f"×0.88", f"+{sc:.1f}", f"{orgair:.1f}"],
        textposition="outside",
        connector=dict(line=dict(color="rgb(63, 63, 63)")),
    ))

    fig.update_layout(
        title=f"{ticker} — Org-AI-R Formula Waterfall",
        yaxis=dict(title="Score Contribution"),
        height=400, margin=dict(t=50, b=40),
        showlegend=False,
    )
    return fig


def signal_comparison_chart(signals_df: pd.DataFrame) -> go.Figure:
    """Grouped bar chart: 4 CS2 signals × 5 companies."""
    if signals_df.empty:
        return go.Figure()

    signal_cols = {
        "TECHNOLOGY_HIRING_SCORE": "Tech Hiring",
        "INNOVATION_ACTIVITY_SCORE": "Innovation",
        "DIGITAL_PRESENCE_SCORE": "Digital Presence",
        "LEADERSHIP_SIGNALS_SCORE": "Leadership",
    }

    fig = go.Figure()
    for col, label in signal_cols.items():
        if col in signals_df.columns:
            fig.add_trace(go.Bar(
                name=label, x=signals_df["TICKER"], y=signals_df[col],
            ))

    fig.update_layout(
        barmode="group", title="CS2 External Signals by Company",
        yaxis=dict(title="Score (0-100)", range=[0, 105]),
        height=400, margin=dict(t=50, b=40),
    )
    return fig