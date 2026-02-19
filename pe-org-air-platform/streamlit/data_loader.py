"""
data_loader.py — Centralized data fetching for Streamlit dashboard.
"""

import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional
import pandas as pd
import requests
import streamlit as st

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
API_BASE = os.getenv("FASTAPI_URL", "http://localhost:8000")
RESULTS_DIR = Path(__file__).parent.parent / "results"

CS3_TICKERS = ["NVDA", "JPM", "WMT", "GE", "DG"]
COMPANY_NAMES = {
    "NVDA": "NVIDIA Corporation", "JPM": "JPMorgan Chase",
    "WMT": "Walmart Inc.", "GE": "GE Aerospace", "DG": "Dollar General",
}
SECTORS = {
    "NVDA": "Technology", "JPM": "Financial Services",
    "WMT": "Retail", "GE": "Manufacturing", "DG": "Retail",
}
EXPECTED_RANGES = {
    "NVDA": (85, 95), "JPM": (65, 75), "WMT": (55, 65),
    "GE": (45, 55), "DG": (35, 45),
}
DIMENSION_LABELS = {
    "data_infrastructure": "Data Infrastructure",
    "ai_governance": "AI Governance",
    "technology_stack": "Technology Stack",
    "talent_skills": "Talent & Skills",
    "leadership_vision": "Leadership Vision",
    "use_case_portfolio": "Use Case Portfolio",
    "culture_change": "Culture & Change",
}


# ---------------------------------------------------------------------------
# API helpers
# ---------------------------------------------------------------------------
def _api(path: str, timeout: int = 30) -> Optional[Dict]:
    try:
        r = requests.get(f"{API_BASE}{path}", timeout=timeout)
        return r.json() if r.status_code == 200 else None
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Snowflake direct
# ---------------------------------------------------------------------------
def _get_snowflake_conn():
    import snowflake.connector
    from dotenv import load_dotenv
    project_root = Path(__file__).parent.parent
    load_dotenv(project_root / ".env")
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
    )


@st.cache_data(ttl=300)
def get_table_counts() -> Dict[str, int]:
    tables = [
        "COMPANIES", "INDUSTRIES", "ASSESSMENTS", "DIMENSION_SCORES",
        "DOCUMENTS", "DOCUMENT_CHUNKS", "EXTERNAL_SIGNALS",
        "COMPANY_SIGNAL_SUMMARIES", "SCORING",
        "SIGNAL_DIMENSION_MAPPING", "EVIDENCE_DIMENSION_SCORES",
    ]
    counts = {}
    try:
        conn = _get_snowflake_conn()
        cur = conn.cursor()
        for t in tables:
            try:
                cur.execute(f"SELECT COUNT(*) FROM {t}")
                counts[t] = cur.fetchone()[0]
            except Exception:
                counts[t] = 0
        cur.close()
        conn.close()
    except Exception:
        counts = {t: -1 for t in tables}
    return counts


# ---------------------------------------------------------------------------
# Results JSON
# ---------------------------------------------------------------------------
@st.cache_data(ttl=120)
def load_result(ticker: str) -> Optional[Dict]:
    path = RESULTS_DIR / f"{ticker.lower()}.json"
    if path.exists():
        return json.loads(path.read_text())
    return None


@st.cache_data(ttl=120)
def load_all_results() -> Dict[str, Dict]:
    results = {}
    for t in CS3_TICKERS:
        r = load_result(t)
        if r:
            results[t] = r
    return results


# ---------------------------------------------------------------------------
# DataFrame builders
# ---------------------------------------------------------------------------
def build_portfolio_df() -> pd.DataFrame:
    results = load_all_results()
    if not results:
        return pd.DataFrame()
    rows = []
    for ticker in CS3_TICKERS:
        r = results.get(ticker)
        if not r:
            continue
        exp = EXPECTED_RANGES.get(ticker, (0, 100))
        score = r.get("org_air_score", 0)
        rows.append({
            "Ticker": ticker,
            "Company": COMPANY_NAMES.get(ticker, ticker),
            "Sector": SECTORS.get(ticker, ""),
            "Org-AI-R": score,
            "V^R": r.get("vr_score", 0),
            "H^R": r.get("hr_score", 0),
            "Synergy": r.get("synergy_score", 0),
            "TC": r.get("talent_concentration", 0),
            "PF": r.get("position_factor", 0),
            "Expected Low": exp[0],
            "Expected High": exp[1],
            "In Range": "✅" if exp[0] <= score <= exp[1] else "⚠️",
        })
    return pd.DataFrame(rows)


def build_dimensions_df() -> pd.DataFrame:
    results = load_all_results()
    if not results:
        return pd.DataFrame()
    rows = []
    for ticker in CS3_TICKERS:
        r = results.get(ticker)
        if not r or not r.get("dimension_scores"):
            continue
        dims = r["dimension_scores"]
        if not isinstance(dims, dict):
            continue
        row = {"Ticker": ticker, "Company": COMPANY_NAMES.get(ticker, ticker)}
        for dim_key, label in DIMENSION_LABELS.items():
            row[label] = dims.get(dim_key, 0)
        rows.append(row)
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Signal summaries
# ---------------------------------------------------------------------------
@st.cache_data(ttl=300)
def get_signal_summaries() -> pd.DataFrame:
    try:
        conn = _get_snowflake_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT ticker, technology_hiring_score, innovation_activity_score,
                   digital_presence_score, leadership_signals_score, composite_score,
                   signal_count
            FROM company_signal_summaries
            WHERE ticker IN ('NVDA','JPM','WMT','GE','DG')
            ORDER BY composite_score DESC
        """)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return pd.DataFrame(rows, columns=cols)
    except Exception:
        return pd.DataFrame()


# ---------------------------------------------------------------------------
# Document stats
# ---------------------------------------------------------------------------
@st.cache_data(ttl=300)
def get_document_stats() -> pd.DataFrame:
    try:
        conn = _get_snowflake_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT d.ticker, d.filing_type, COUNT(*) as doc_count,
                   COALESCE(SUM(d.word_count), 0) as total_words,
                   COALESCE(SUM(d.chunk_count), 0) as total_chunks
            FROM documents d
            WHERE d.ticker IN ('NVDA','JPM','WMT','GE','DG')
            GROUP BY d.ticker, d.filing_type
            ORDER BY d.ticker, d.filing_type
        """)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return pd.DataFrame(rows, columns=cols)
    except Exception:
        return pd.DataFrame()


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------
@st.cache_data(ttl=60)
def check_health() -> Optional[Dict]:
    return _api("/health")