"""
CS3 Report Generator Service
app/services/report_generator.py

Generates markdown scoring reports per company from Snowflake data.
"""

import logging
from typing import Dict, List, Optional, Any
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

# Company name mapping
COMPANY_NAMES = {
    "CAT": "Caterpillar Inc.", "DE": "Deere & Company",
    "UNH": "UnitedHealth Group", "HCA": "HCA Healthcare",
    "ADP": "Automatic Data Processing", "PAYX": "Paychex Inc.",
    "WMT": "Walmart Inc.", "TGT": "Target Corporation",
    "JPM": "JPMorgan Chase & Co.", "GS": "Goldman Sachs Group",
    "NVDA": "NVIDIA Corporation", "GE": "General Electric Company",
    "DG": "Dollar General Corporation",
}

SECTOR_MAP = {
    "CAT": "Industrials", "DE": "Industrials",
    "UNH": "Healthcare", "HCA": "Healthcare",
    "ADP": "Business Services", "PAYX": "Business Services",
    "WMT": "Consumer Retail", "TGT": "Consumer Retail",
    "JPM": "Financial Services", "GS": "Financial Services",
    "NVDA": "Technology", "GE": "Manufacturing",
    "DG": "Consumer Retail",
}

# Dimension display names
DIM_DISPLAY = {
    "data_infrastructure": "Data Infrastructure",
    "ai_governance": "AI Governance",
    "technology_stack": "Technology Stack",
    "talent_skills": "Talent & Skills",
    "leadership_vision": "Leadership Vision",
    "use_case_portfolio": "Use Case Portfolio",
    "culture_change": "Culture & Change",
}

# Short column headers for matrix
DIM_SHORT = {
    "data_infrastructure": "Data",
    "ai_governance": "Gov",
    "technology_stack": "Tech",
    "talent_skills": "Talent",
    "leadership_vision": "Lead",
    "use_case_portfolio": "Use",
    "culture_change": "Culture",
}

# Rubric level labels
def _level_label(score: float) -> str:
    if score >= 80: return "Excellent"
    if score >= 60: return "Good"
    if score >= 40: return "Adequate"
    if score >= 20: return "Developing"
    return "Nascent"


def generate_company_report(
    ticker: str,
    matrix_rows: List[Dict],
    dimension_rows: List[Dict],
) -> str:
    """Generate markdown report for a single company."""

    name = COMPANY_NAMES.get(ticker, ticker)
    sector = SECTOR_MAP.get(ticker, "Unknown")
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    lines = []
    lines.append(f"# {ticker} — CS3 Scoring Report")
    lines.append(f"")
    lines.append(f"> **{name}** | Sector: {sector} | Scored: {now}")
    lines.append(f"")
    lines.append(f"---")
    lines.append(f"")

    # ── Section 1: Mapping Matrix ──
    lines.append(f"## Mapping Matrix")
    lines.append(f"")

    dim_keys = ["data_infrastructure", "ai_governance", "technology_stack",
                 "talent_skills", "leadership_vision", "use_case_portfolio", "culture_change"]

    # Header
    header = "| Source | Score | " + " | ".join(DIM_SHORT[d] for d in dim_keys) + " |"
    separator = "|:---|---:|" + "|".join(["---:" for _ in dim_keys]) + "|"
    lines.append(header)
    lines.append(separator)

    # Filter out glassdoor/board rows (null scores)
    active_rows = [r for r in matrix_rows if r.get("raw_score") is not None]
    for row in active_rows:
        source = row.get("source", "")
        score = row.get("raw_score")
        score_str = f"{_to_float(score):.1f}" if score is not None else "—"

        # Find primary dimension for this source (highest weight)
        weights = {}
        for d in dim_keys:
            w = row.get(d)
            if w is not None:
                weights[d] = _to_float(w)

        primary_dim = max(weights, key=weights.get) if weights else None

        cells = []
        for d in dim_keys:
            w = row.get(d)
            if w is not None:
                w_str = f"{_to_float(w):.2f}"
                if d == primary_dim:
                    w_str = f"**{w_str}**"
                cells.append(w_str)
            else:
                cells.append("—")

        # Friendly source name
        source_display = _source_display(source)
        lines.append(f"| {source_display} | {score_str} | " + " | ".join(cells) + " |")

    lines.append(f"")

    # ── Section 2: SEC Rubric Scores ──
    sec_rows = [r for r in matrix_rows
                if r.get("source", "").startswith("sec_") and r.get("raw_score") is not None]

    if sec_rows:
        lines.append(f"## SEC Rubric Scores")
        lines.append(f"")
        lines.append(f"| Section | Score | Level | Confidence |")
        lines.append(f"|:---|---:|:---|---:|")

        for row in sec_rows:
            source = row.get("source", "")
            score = _to_float(row.get("raw_score", 0))
            conf = _to_float(row.get("confidence", 0))
            level = _level_label(score)
            sec_display = _sec_display(source)
            lines.append(f"| {sec_display} | {score:.1f} | {level} | {conf:.3f} |")

        lines.append(f"")

    # ── Section 3: Dimension Scores ──
    lines.append(f"## Dimension Scores")
    lines.append(f"")
    lines.append(f"| Dimension | Score | Level | Conf | Sources |")
    lines.append(f"|:---|---:|:---|---:|:---|")

    for row in dimension_rows:
        dim = row.get("dimension", "")
        score = _to_float(row.get("score", 0))
        conf = _to_float(row.get("confidence", 0))
        sources = row.get("sources", "")
        src_count = row.get("source_count", 0)
        level = _level_label(score)
        dim_name = DIM_DISPLAY.get(dim, dim)

        lines.append(f"| {dim_name} | {score:.1f} | {level} | {conf:.3f} | {sources} |")

    lines.append(f"")

    # ── Section 4: Key Findings ──
    lines.append(f"## Key Findings")
    lines.append(f"")

    # Sort dimensions by score
    sorted_dims = sorted(dimension_rows, key=lambda r: _to_float(r.get("score", 0)), reverse=True)

    if sorted_dims:
        top = sorted_dims[0]
        bottom = sorted_dims[-1]
        top_name = DIM_DISPLAY.get(top["dimension"], top["dimension"])
        bot_name = DIM_DISPLAY.get(bottom["dimension"], bottom["dimension"])
        top_score = _to_float(top.get("score", 0))
        bot_score = _to_float(bottom.get("score", 0))

        lines.append(f"**Strongest:** {top_name} ({top_score:.1f}) — {_level_label(top_score)}")
        lines.append(f"")
        lines.append(f"**Weakest:** {bot_name} ({bot_score:.1f}) — {_level_label(bot_score)}")
        lines.append(f"")

        # Average
        avg = sum(_to_float(r.get("score", 0)) for r in dimension_rows) / max(len(dimension_rows), 1)
        lines.append(f"**Average Dimension Score:** {avg:.1f}")
        lines.append(f"")

    lines.append(f"---")
    lines.append(f"")

    return "\n".join(lines)


def generate_portfolio_summary(
    all_dimension_scores: List[Dict],
) -> str:
    """Generate a portfolio comparison markdown."""

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # Group by ticker
    companies: Dict[str, List[Dict]] = {}
    for row in all_dimension_scores:
        t = row.get("ticker", "")
        if t not in companies:
            companies[t] = []
        companies[t].append(row)

    lines = []
    lines.append(f"# CS3 Portfolio Scoring Summary")
    lines.append(f"")
    lines.append(f"> Generated: {now} | Companies: {len(companies)} | Pipeline: CS3 Tasks 5.0a + 5.0b")
    lines.append(f"")
    lines.append(f"---")
    lines.append(f"")

    dim_keys = ["data_infrastructure", "ai_governance", "technology_stack",
                 "talent_skills", "leadership_vision", "use_case_portfolio", "culture_change"]

    # ── Comparison Table ──
    lines.append(f"## Dimension Scores by Company")
    lines.append(f"")

    header = "| Ticker | Sector | " + " | ".join(DIM_SHORT[d] for d in dim_keys) + " | Avg |"
    sep = "|:---|:---|" + "|".join(["---:" for _ in dim_keys]) + "|---:|"
    lines.append(header)
    lines.append(sep)

    ticker_avgs = []
    for ticker in sorted(companies.keys()):
        dims = companies[ticker]
        sector = SECTOR_MAP.get(ticker, "—")
        dim_lookup = {r["dimension"]: _to_float(r.get("score", 0)) for r in dims}

        cells = []
        for d in dim_keys:
            s = dim_lookup.get(d, 50.0)
            cells.append(f"{s:.0f}")

        avg = sum(dim_lookup.get(d, 50.0) for d in dim_keys) / 7
        ticker_avgs.append((ticker, avg))

        lines.append(f"| **{ticker}** | {sector} | " + " | ".join(cells) + f" | **{avg:.0f}** |")

    lines.append(f"")

    # ── Rankings ──
    lines.append(f"## Rankings (by Average Score)")
    lines.append(f"")
    lines.append(f"| Rank | Ticker | Company | Sector | Avg Score | Level |")
    lines.append(f"|---:|:---|:---|:---|---:|:---|")

    ticker_avgs.sort(key=lambda x: x[1], reverse=True)
    for i, (ticker, avg) in enumerate(ticker_avgs, 1):
        name = COMPANY_NAMES.get(ticker, ticker)
        sector = SECTOR_MAP.get(ticker, "—")
        level = _level_label(avg)
        lines.append(f"| {i} | **{ticker}** | {name} | {sector} | {avg:.1f} | {level} |")

    lines.append(f"")

    # ── Per-Dimension Leaders ──
    lines.append(f"## Dimension Leaders")
    lines.append(f"")
    lines.append(f"| Dimension | Leader | Score | Laggard | Score |")
    lines.append(f"|:---|:---|---:|:---|---:|")

    for d in dim_keys:
        scores = []
        for ticker, dims in companies.items():
            for row in dims:
                if row.get("dimension") == d:
                    scores.append((ticker, _to_float(row.get("score", 0))))
        if scores:
            scores.sort(key=lambda x: x[1], reverse=True)
            leader = scores[0]
            laggard = scores[-1]
            dim_name = DIM_DISPLAY.get(d, d)
            lines.append(f"| {dim_name} | **{leader[0]}** | {leader[1]:.0f} | {laggard[0]} | {laggard[1]:.0f} |")

    lines.append(f"")
    lines.append(f"---")
    lines.append(f"")
    lines.append(f"*Generated by CS3 Scoring Engine (Tasks 5.0a + 5.0b) | PE Org-AI-R Platform*")

    return "\n".join(lines)


# ── Helpers ──

def _to_float(val) -> float:
    if val is None:
        return 0.0
    try:
        return float(val)
    except (ValueError, TypeError):
        return 0.0

def _source_display(source: str) -> str:
    return {
        "technology_hiring": "technology_hiring",
        "innovation_activity": "innovation_activity",
        "digital_presence": "digital_presence",
        "leadership_signals": "leadership_signals",
        "sec_item_1": "sec_item_1 (Business)",
        "sec_item_1a": "sec_item_1a (Risk)",
        "sec_item_7": "sec_item_7 (MD&A)",
    }.get(source, source)

def _sec_display(source: str) -> str:
    return {
        "sec_item_1": "Item 1 — Business",
        "sec_item_1a": "Item 1A — Risk Factors",
        "sec_item_7": "Item 7 — MD&A",
    }.get(source, source)