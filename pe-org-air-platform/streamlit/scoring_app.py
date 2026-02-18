# streamlit/scoring_app.py
# CS3 Scoring Engine UI — PE OrgAIR Platform

from __future__ import annotations
import json
import math
import os
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests
import streamlit as st

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# =====================================================================
# Page config (must be first Streamlit call)
# =====================================================================

st.set_page_config(
    page_title="CS3 Scoring Engine",
    layout="wide",
    page_icon="🧮",
)

# =====================================================================
# Constants
# =====================================================================

CS3_PORTFOLIO = ["NVDA", "JPM", "WMT", "GE", "DG"]

COMPANY_SECTORS = {
    "NVDA": "Technology",
    "JPM": "Financial Services",
    "WMT": "Retail",
    "GE": "Manufacturing",
    "DG": "Retail",
}

EXPECTED_ORGAIR_RANGES: Dict[str, Tuple[float, float]] = {
    "NVDA": (85.0, 95.0),
    "JPM":  (65.0, 75.0),
    "WMT":  (55.0, 65.0),
    "GE":   (45.0, 55.0),
    "DG":   (35.0, 45.0),
}

EXPECTED_VR_RANGES: Dict[str, Tuple[float, float]] = {
    "NVDA": (80.0, 100.0),
    "JPM":  (60.0, 80.0),
    "WMT":  (50.0, 70.0),
    "GE":   (40.0, 60.0),
    "DG":   (30.0, 50.0),
}

EXPECTED_PF_RANGES: Dict[str, Tuple[float, float]] = {
    "NVDA": (0.7,  1.0),
    "JPM":  (0.3,  0.7),
    "WMT":  (0.1,  0.5),
    "GE":   (-0.2, 0.2),
    "DG":   (-0.5, -0.1),
}

EXPECTED_HR_RANGES: Dict[str, Tuple[float, float]] = {
    "NVDA": (82.9, 86.3),
    "JPM":  (71.1, 75.1),
    "WMT":  (55.8, 59.1),
    "GE":   (50.4, 53.6),
    "DG":   (50.9, 54.2),
}

# Formula constants (mirror backend exactly)
ORGAIR_ALPHA = 0.60
ORGAIR_BETA  = 0.12
HR_DELTA     = 0.15
SB_BASE_R    = 0.70
SB_SIGMA     = 15.0
SB_Z95       = 1.96

DEFAULT_FASTAPI_URL = os.getenv("FASTAPI_URL", "http://localhost:8000")

# =====================================================================
# Session state init
# =====================================================================

if "base_url" not in st.session_state:
    st.session_state["base_url"] = DEFAULT_FASTAPI_URL
if "selected_ticker" not in st.session_state:
    st.session_state["selected_ticker"] = "NVDA"
if "portfolio_results" not in st.session_state:
    st.session_state["portfolio_results"] = None

# =====================================================================
# Helper functions — API
# =====================================================================

def api_url(base: str, path: str) -> str:
    return f"{base.rstrip('/')}/{path.lstrip('/')}"


def safe_json(resp: requests.Response) -> Dict[str, Any]:
    try:
        return resp.json()
    except Exception:
        return {"_error": resp.text, "_status": resp.status_code}


def post(url: str, timeout_s: int = 300) -> Dict[str, Any]:
    resp = requests.post(url, timeout=timeout_s)
    if resp.status_code >= 400:
        raise RuntimeError(
            f"POST {url} failed ({resp.status_code}): "
            f"{safe_json(resp).get('detail', safe_json(resp))}"
        )
    return safe_json(resp)


def get(url: str, params: Optional[Dict[str, Any]] = None, timeout_s: int = 30) -> Dict[str, Any]:
    resp = requests.get(url, params=params, timeout=timeout_s)
    if resp.status_code >= 400:
        raise RuntimeError(
            f"GET {url} failed ({resp.status_code}): "
            f"{safe_json(resp).get('detail', safe_json(resp))}"
        )
    return safe_json(resp)


def render_kpis(items: List[Tuple[str, Any]], num_cols: int = 0) -> None:
    n = num_cols if num_cols > 0 else len(items)
    cols = st.columns(n)
    for i, (label, value) in enumerate(items):
        cols[i % n].metric(label, value)


def show_json(title: str, data: Any, expanded: bool = False) -> None:
    with st.expander(title, expanded=expanded):
        st.code(json.dumps(data, indent=2, default=str), language="json")


# =====================================================================
# Helper functions — Pure-Python math (Math Explorer; no API calls)
# =====================================================================

def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def compute_synergy_local(vr: float, hr: float, alignment: Optional[float] = None,
                          timing_factor: float = 1.0) -> Dict[str, float]:
    if alignment is None:
        alignment = 1.0 - abs(vr - hr) / 100.0
    timing_factor = _clamp(timing_factor, 0.8, 1.2)
    synergy = (vr * hr / 100.0) * alignment * timing_factor
    return {
        "synergy": _clamp(synergy, 0.0, 100.0),
        "alignment": alignment,
        "timing_factor": timing_factor,
        "raw": synergy,
    }


def compute_orgair_local(vr: float, hr: float, synergy: float) -> Dict[str, float]:
    alpha, beta = ORGAIR_ALPHA, ORGAIR_BETA
    vr_weighted = alpha * vr
    hr_weighted = (1.0 - alpha) * hr
    weighted_base = vr_weighted + hr_weighted
    synergy_contribution = beta * synergy
    org_air = (1.0 - beta) * weighted_base + synergy_contribution
    return {
        "org_air": _clamp(org_air, 0.0, 100.0),
        "weighted_base": weighted_base,
        "synergy_contribution": synergy_contribution,
        "vr_weighted": vr_weighted,
        "hr_weighted": hr_weighted,
        "alpha": alpha,
        "beta": beta,
    }


def compute_ci_local(score: float, n: int) -> Dict[str, float]:
    r = SB_BASE_R
    rho = (n * r) / (1.0 + (n - 1) * r)
    sem = SB_SIGMA * math.sqrt(1.0 - rho)
    margin = SB_Z95 * sem
    return {
        "reliability": rho,
        "sem": sem,
        "margin": margin,
        "ci_lower": _clamp(score - margin, 0.0, 100.0),
        "ci_upper": _clamp(score + margin, 0.0, 100.0),
    }


# =====================================================================
# Sidebar
# =====================================================================

with st.sidebar:
    st.title("CS3 Scoring Engine")
    st.caption("PE OrgAIR Platform — Lab 5 & Lab 6")
    st.divider()

    base_url = st.text_input(
        "FastAPI Base URL",
        value=st.session_state["base_url"],
        help="e.g. http://localhost:8000",
    )
    st.session_state["base_url"] = base_url

    if st.button("Test Connection"):
        try:
            data = get(api_url(base_url, "/health"), timeout_s=10)
            st.success(f"Connected — status: {data.get('status', 'ok')}")
        except Exception as e:
            st.error(f"Cannot reach FastAPI: {e}")

    st.divider()
    st.markdown("**Quick Links**")
    st.markdown(f"- [FastAPI Docs]({base_url}/docs)")
    st.markdown(f"- [Redoc]({base_url}/redoc)")
    st.divider()
    st.caption("Run alongside `app.py` on port 8501.\nThis app runs on port 8502.")

# =====================================================================
# Main tabs
# =====================================================================

tab_overview, tab_health, tab_single, tab_portfolio, tab_math = st.tabs([
    "Overview",
    "System Health",
    "Single Company",
    "Portfolio Dashboard",
    "Math Explorer",
])

# =====================================================================
# Tab 1 — Overview
# =====================================================================

with tab_overview:
    st.title("CS3 PE OrgAIR Scoring Engine")
    st.markdown(
        "This dashboard exposes the full **CS3 scoring pipeline** built in Lab 5 & Lab 6. "
        "It computes Talent Concentration (TC), Vulnerability Risk (V^R), Position Factor (PF), "
        "Human Readiness (H^R), Synergy, and the composite **Org-AI-R** score with "
        "Spearman-Brown 95% confidence intervals for each of the five portfolio companies "
        "(NVDA, JPM, WMT, GE, DG)."
    )

    st.divider()

    col_left, col_right = st.columns(2)

    with col_left:
        with st.expander("Lab 5 — 7-Dimension Rubric Scoring", expanded=False):
            st.markdown(
                "Each dimension is scored 0–100 from evidence collected in CS1/CS2.\n\n"
                "| # | Dimension |\n|---|---|\n"
                "| 1 | AI Strategy & Vision |\n"
                "| 2 | Talent & Workforce AI Readiness |\n"
                "| 3 | Data Infrastructure & AI Ops |\n"
                "| 4 | Financial Investment in AI |\n"
                "| 5 | AI Ethics & Governance |\n"
                "| 6 | AI Product & Revenue Impact |\n"
                "| 7 | Partnerships & Ecosystem |\n"
            )

        with st.expander("Talent Concentration (TC)", expanded=False):
            st.markdown(
                "**TC = leadership_ratio × team_size_factor × skill_concentration × individual_factor**\n\n"
                "- leadership_ratio: senior AI staff / total AI staff\n"
                "- team_size_factor: normalized team size signal\n"
                "- skill_concentration: Herfindahl index of skill types\n"
                "- individual_factor: Glassdoor individual-mention signal"
            )

        with st.expander("Vulnerability Risk (V^R)", expanded=False):
            st.latex(r"V^R = \text{weighted\_dim\_score} \times (1 - \text{talent\_risk\_adj})")
            st.markdown("Weighted average of 7-dimension scores, adjusted for talent concentration risk.")

        with st.expander("Position Factor (PF)", expanded=False):
            st.latex(r"PF = w_{VR} \cdot \frac{V^R - \mu_{sector}}{100} + w_{MC} \cdot (MC_{pctile} - 0.5)")
            st.markdown("Composite of VR vs sector average and market-cap percentile. Range: [−1, +1].")

    with col_right:
        with st.expander("Human Readiness (H^R)", expanded=False):
            st.latex(r"H^R = H^R_{base} \times (1 + \delta \times PF) \quad \delta = 0.15")
            st.markdown("Sector-specific base score amplified/dampened by Position Factor.")

        with st.expander("Synergy", expanded=False):
            st.latex(r"\text{Synergy} = \frac{V^R \times H^R}{100} \times \text{Alignment} \times \text{TimingFactor}")
            st.markdown(
                "- Alignment = 1 − |V^R − H^R| / 100 (auto-computed if not overridden)\n"
                "- TimingFactor ∈ [0.8, 1.2]\n"
                "- Result clamped to [0, 100]"
            )

        with st.expander("Org-AI-R (composite score)", expanded=False):
            st.latex(
                r"\text{Org-AI-R} = (1-\beta)\,[\alpha\,V^R + (1-\alpha)\,H^R] + \beta\,\text{Synergy}"
            )
            st.markdown(
                f"Fixed params: α = {ORGAIR_ALPHA}, β = {ORGAIR_BETA}. Result clamped to [0, 100]."
            )

        with st.expander("Spearman-Brown 95% CI", expanded=False):
            st.latex(r"\rho_{SB} = \frac{n \cdot r}{1 + (n-1)\,r} \quad r=0.70")
            st.latex(r"SEM = 15 \cdot \sqrt{1 - \rho_{SB}}")
            st.latex(r"CI_{95\%} = \text{score} \pm 1.96 \times SEM")

    st.divider()
    st.subheader("Expected Ranges — CS3 Table 5")

    range_rows = []
    for ticker in CS3_PORTFOLIO:
        vr_lo, vr_hi = EXPECTED_VR_RANGES[ticker]
        pf_lo, pf_hi = EXPECTED_PF_RANGES[ticker]
        hr_lo, hr_hi = EXPECTED_HR_RANGES[ticker]
        air_lo, air_hi = EXPECTED_ORGAIR_RANGES[ticker]
        range_rows.append({
            "Ticker": ticker,
            "Sector": COMPANY_SECTORS[ticker],
            "Expected V^R": f"{vr_lo:.0f} – {vr_hi:.0f}",
            "Expected PF":  f"{pf_lo:.1f} – {pf_hi:.1f}",
            "Expected H^R": f"{hr_lo:.1f} – {hr_hi:.1f}",
            "Expected Org-AI-R": f"{air_lo:.0f} – {air_hi:.0f}",
        })
    st.dataframe(pd.DataFrame(range_rows), use_container_width=True, hide_index=True)

    st.divider()
    st.subheader("How to Use This Dashboard")

    st.info(
        "**System Health** — verify that FastAPI, Snowflake, S3, and Redis are reachable "
        "before running any scoring pipeline."
    )
    st.info(
        "**Single Company** — select a ticker and choose *Compute (POST)* to run the full "
        "pipeline end-to-end, or *Load Saved (GET)* to read the last stored result from Snowflake. "
        "Run subtabs in order: Dimensions → TC + V^R → PF → H^R → Org-AI-R."
    )
    st.info(
        "**Portfolio Dashboard** — click *Compute Full Portfolio* to score all 5 companies "
        "in sequence and see a comparative table + charts. Results are cached in the browser "
        "session until you refresh."
    )
    st.info(
        "**Math Explorer** — interactive sliders drive the formulas locally (no API call). "
        "Use this to understand sensitivity and formula behavior without requiring a live server."
    )

# =====================================================================
# Tab 2 — System Health
# =====================================================================

with tab_health:
    st.header("System Health")
    base = st.session_state["base_url"]

    if st.button("Run Health Check", type="primary"):
        try:
            with st.spinner("Checking all services..."):
                data = get(api_url(base, "/health"), timeout_s=30)

            overall = data.get("status", "unknown")
            snowflake_ok = data.get("snowflake") == "ok"
            s3_ok = data.get("s3") == "ok"
            redis_ok = data.get("redis") == "ok"

            render_kpis([
                ("Overall",   overall.upper()),
                ("Snowflake", "ok" if snowflake_ok else data.get("snowflake", "?")),
                ("S3",        "ok" if s3_ok else data.get("s3", "?")),
                ("Redis",     "ok" if redis_ok else data.get("redis", "?")),
            ])

            if overall in ("ok", "healthy"):
                st.success("All services appear healthy.")
            else:
                st.warning(f"Degraded status: {overall}")

            show_json("Raw /health Response", data)

        except requests.exceptions.ConnectionError:
            st.error(f"Cannot reach FastAPI at {base}. Is the server running?")
        except requests.exceptions.Timeout:
            st.error("Health check timed out.")
        except RuntimeError as e:
            st.error(f"API error: {e}")
        except Exception as e:
            st.error(f"Unexpected error: {e}")

    st.divider()
    st.subheader("Individual Service Checks")

    c1, c2, c3 = st.columns(3)

    for col, svc in zip([c1, c2, c3], ["snowflake", "s3", "redis"]):
        with col:
            if st.button(f"Check {svc.capitalize()}"):
                try:
                    with st.spinner(f"Checking {svc}..."):
                        data = get(api_url(base, f"/health/{svc}"), timeout_s=30)
                    status = data.get("status", "unknown")
                    if status == "ok":
                        st.success(f"{svc.capitalize()}: {status}")
                    else:
                        st.warning(f"{svc.capitalize()}: {status}")
                    show_json(f"Raw /health/{svc}", data)
                except requests.exceptions.ConnectionError:
                    st.error(f"Cannot reach FastAPI at {base}.")
                except RuntimeError as e:
                    st.error(f"API error: {e}")
                except Exception as e:
                    st.error(f"Unexpected error: {e}")

# =====================================================================
# Tab 3 — Single Company Analysis
# =====================================================================

with tab_single:
    st.header("Single Company Analysis")
    base = st.session_state["base_url"]

    ctrl_col1, ctrl_col2 = st.columns([2, 2])
    with ctrl_col1:
        ticker = st.selectbox(
            "Select Company",
            CS3_PORTFOLIO,
            index=CS3_PORTFOLIO.index(st.session_state["selected_ticker"]),
        )
        st.session_state["selected_ticker"] = ticker

    with ctrl_col2:
        run_mode = st.radio(
            "Run Mode",
            ["Compute (POST)", "Load Saved (GET)"],
            horizontal=True,
        )

    sector = COMPANY_SECTORS[ticker]
    air_lo, air_hi = EXPECTED_ORGAIR_RANGES[ticker]
    st.caption(f"**{ticker}** | Sector: {sector} | Expected Org-AI-R: {air_lo:.0f} – {air_hi:.0f}")

    st.divider()

    sub1, sub2, sub3, sub4, sub5 = st.tabs([
        "1. Dimensions",
        "2. TC + V^R",
        "3. Position Factor",
        "4. H^R",
        "5. Org-AI-R",
    ])

    # --- Subtab 1: Dimensions ---
    with sub1:
        st.subheader(f"7-Dimension Scores — {ticker}")
        if st.button("Run Dimensions", key="btn_dim"):
            try:
                if run_mode == "Compute (POST)":
                    url = api_url(base, f"/api/v1/scoring/{ticker}")
                    with st.spinner(f"Scoring {ticker} dimensions (POST)..."):
                        data = post(url, timeout_s=300)
                else:
                    url = api_url(base, f"/api/v1/scoring/{ticker}/dimensions")
                    with st.spinner(f"Loading {ticker} dimensions (GET)..."):
                        data = get(url, timeout_s=30)

                # Normalise: POST returns ScoringResponse, GET returns DimensionScoresResponse
                dim_scores = data.get("dimension_scores") or data.get("scores") or []

                if dim_scores:
                    dims_scored = len(dim_scores)
                    # Some rows may have evidence_count
                    total_evidence = sum(d.get("evidence_count", 0) for d in dim_scores)
                    max_evidence = dims_scored * 10  # rough upper bound
                    cov_pct = round(total_evidence / max_evidence * 100, 1) if max_evidence else 0

                    render_kpis([
                        ("Dimensions Scored", f"{dims_scored} / 7"),
                        ("Total Evidence Items", total_evidence),
                        ("Coverage %", f"{cov_pct}%"),
                    ])

                    df = pd.DataFrame(dim_scores)
                    st.dataframe(df, use_container_width=True, hide_index=True)
                else:
                    st.warning("No dimension scores returned. Check that the pipeline has data for this ticker.")

                show_json("Raw Response", data)

            except requests.exceptions.ConnectionError:
                st.error(f"Cannot reach FastAPI at {base}. Is the server running?")
            except requests.exceptions.Timeout:
                st.error("Request timed out.")
            except RuntimeError as e:
                st.error(f"API error: {e}")
            except Exception as e:
                st.error(f"Unexpected error: {e}")

    # --- Subtab 2: TC + V^R ---
    with sub2:
        st.subheader(f"Talent Concentration + Vulnerability Risk — {ticker}")
        if st.button("Run TC + V^R", key="btn_tcvr"):
            try:
                if run_mode == "Compute (POST)":
                    url = api_url(base, f"/api/v1/scoring/tc-vr/{ticker}")
                    with st.spinner(f"Computing TC + V^R for {ticker}..."):
                        data = post(url, timeout_s=300)
                else:
                    url = api_url(base, f"/api/v1/scoring/tc-vr/{ticker}")
                    with st.spinner(f"Loading TC + V^R for {ticker}..."):
                        data = get(url, timeout_s=30)

                tc  = data.get("talent_concentration")
                vr_result = data.get("vr_result") or {}
                vr  = vr_result.get("vr_score")
                val = data.get("validation") or {}
                tc_ok = val.get("tc_in_range", None)
                vr_ok = val.get("vr_in_range", None)

                render_kpis([
                    ("TC",            f"{tc:.4f}" if tc is not None else "—"),
                    ("V^R Score",     f"{vr:.2f}" if vr is not None else "—"),
                    ("TC Validation", f"{'In Range' if tc_ok else 'Out of Range'}" if tc_ok is not None else "—"),
                    ("V^R Validation",f"{'In Range' if vr_ok else 'Out of Range'}" if vr_ok is not None else "—"),
                ])

                if tc_ok is True:
                    st.success(f"TC in range — expected {val.get('tc_expected', '')}")
                elif tc_ok is False:
                    st.warning(f"TC out of range — expected {val.get('tc_expected', '')}")

                if vr_ok is True:
                    st.success(f"V^R in range — expected {val.get('vr_expected', '')}")
                elif vr_ok is False:
                    st.warning(f"V^R out of range — expected {val.get('vr_expected', '')}")

                tc_bd = data.get("tc_breakdown") or {}
                if tc_bd:
                    with st.expander("TC Breakdown"):
                        st.json({
                            "leadership_ratio":   tc_bd.get("leadership_ratio"),
                            "team_size_factor":   tc_bd.get("team_size_factor"),
                            "skill_concentration":tc_bd.get("skill_concentration"),
                            "individual_factor":  tc_bd.get("individual_factor"),
                        })

                job = data.get("job_analysis") or {}
                if job:
                    with st.expander("Job Analysis"):
                        st.json(job)

                with st.expander("V^R Breakdown"):
                    st.json({
                        "weighted_dim_score": vr_result.get("weighted_dim_score"),
                        "talent_risk_adj":    vr_result.get("talent_risk_adj"),
                        "vr_score":           vr_result.get("vr_score"),
                    })

                show_json("Raw Response", data)

            except requests.exceptions.ConnectionError:
                st.error(f"Cannot reach FastAPI at {base}. Is the server running?")
            except requests.exceptions.Timeout:
                st.error("Request timed out. Pipeline may still be running server-side.")
            except RuntimeError as e:
                st.error(f"API error: {e}")
            except Exception as e:
                st.error(f"Unexpected error: {e}")

    # --- Subtab 3: Position Factor ---
    with sub3:
        st.subheader(f"Position Factor — {ticker}")
        if st.button("Run Position Factor", key="btn_pf"):
            try:
                if run_mode == "Compute (POST)":
                    url = api_url(base, f"/api/v1/scoring/pf/{ticker}")
                    with st.spinner(f"Computing PF for {ticker}..."):
                        data = post(url, timeout_s=300)
                else:
                    url = api_url(base, f"/api/v1/scoring/pf/{ticker}")
                    with st.spinner(f"Loading PF for {ticker}..."):
                        data = get(url, timeout_s=30)

                pf   = data.get("position_factor")
                bd   = data.get("pf_breakdown") or {}
                val  = data.get("validation") or {}

                # Interpretation
                if pf is not None:
                    if pf >= 0.7:
                        interp = "Dominant"
                    elif pf >= 0.3:
                        interp = "Strong"
                    elif pf >= -0.2:
                        interp = "Average"
                    else:
                        interp = "Laggard"
                else:
                    interp = "—"

                render_kpis([
                    ("Position Factor",    f"{pf:.4f}" if pf is not None else "—"),
                    ("V^R Used",           f"{bd.get('vr_score', '—'):.2f}" if bd.get('vr_score') is not None else "—"),
                    ("Sector Avg V^R",     f"{bd.get('sector_avg_vr', '—'):.2f}" if bd.get('sector_avg_vr') is not None else "—"),
                    ("MCap Percentile",    f"{bd.get('market_cap_percentile', '—'):.2f}" if bd.get('market_cap_percentile') is not None else "—"),
                    ("Interpretation",     interp),
                ])

                pf_ok = val.get("pf_in_range")
                if pf_ok is True:
                    st.success(f"PF in range — expected {val.get('pf_expected', '')}")
                elif pf_ok is False:
                    st.warning(f"PF out of range — expected {val.get('pf_expected', '')}")

                if bd:
                    with st.expander("PF Formula Step-by-Step"):
                        st.code(
                            f"vr_score          = {bd.get('vr_score')}\n"
                            f"sector_avg_vr     = {bd.get('sector_avg_vr')}\n"
                            f"vr_diff           = {bd.get('vr_diff')}\n"
                            f"vr_component      = {bd.get('vr_component')}\n"
                            f"market_cap_pctile = {bd.get('market_cap_percentile')}\n"
                            f"mcap_component    = {bd.get('mcap_component')}\n"
                            f"position_factor   = {bd.get('position_factor')}",
                            language="text",
                        )

                show_json("Raw Response", data)

            except requests.exceptions.ConnectionError:
                st.error(f"Cannot reach FastAPI at {base}. Is the server running?")
            except requests.exceptions.Timeout:
                st.error("Request timed out. Pipeline may still be running server-side.")
            except RuntimeError as e:
                st.error(f"API error: {e}")
            except Exception as e:
                st.error(f"Unexpected error: {e}")

    # --- Subtab 4: H^R ---
    with sub4:
        st.subheader(f"Human Readiness (H^R) — {ticker}")
        if st.button("Run H^R", key="btn_hr"):
            try:
                if run_mode == "Compute (POST)":
                    url = api_url(base, f"/api/v1/scoring/hr/{ticker}")
                    with st.spinner(f"Computing H^R for {ticker}..."):
                        data = post(url, timeout_s=300)
                else:
                    url = api_url(base, f"/api/v1/scoring/hr/{ticker}")
                    with st.spinner(f"Loading H^R for {ticker}..."):
                        data = get(url, timeout_s=30)

                hr    = data.get("hr_score")
                bd    = data.get("hr_breakdown") or {}
                val   = data.get("validation") or {}

                render_kpis([
                    ("H^R Score",        f"{hr:.2f}" if hr is not None else "—"),
                    ("HR Base",          f"{bd.get('hr_base', '—')}"),
                    ("Position Factor",  f"{bd.get('position_factor', '—')}"),
                    ("Adjustment",       f"{bd.get('position_adjustment', '—')}"),
                    ("Interpretation",   bd.get("interpretation", "—")),
                    ("Validation",       val.get("status", "—")),
                ])

                hr_ok = val.get("hr_in_range")
                if hr_ok is True:
                    st.success(f"H^R in range — expected {val.get('hr_expected', '')}")
                elif hr_ok is False:
                    st.warning(f"H^R out of range — expected {val.get('hr_expected', '')}")

                if hr is not None:
                    st.progress(hr / 100.0, text=f"Readiness: {hr:.1f} / 100")

                hr_base = bd.get("hr_base")
                pf_val  = bd.get("position_factor")
                if hr_base is not None and pf_val is not None:
                    with st.expander("H^R Formula Trace"):
                        st.code(
                            f"H^R = HR_base × (1 + δ × PF)\n"
                            f"    = {hr_base} × (1 + {HR_DELTA} × {pf_val})\n"
                            f"    = {hr_base} × (1 + {HR_DELTA * pf_val:.4f})\n"
                            f"    = {hr_base} × {1 + HR_DELTA * pf_val:.4f}\n"
                            f"    = {hr:.4f}" if hr is not None else "    = ?",
                            language="text",
                        )

                show_json("Raw Response", data)

            except requests.exceptions.ConnectionError:
                st.error(f"Cannot reach FastAPI at {base}. Is the server running?")
            except requests.exceptions.Timeout:
                st.error("Request timed out. Pipeline may still be running server-side.")
            except RuntimeError as e:
                st.error(f"API error: {e}")
            except Exception as e:
                st.error(f"Unexpected error: {e}")

    # --- Subtab 5: Org-AI-R ---
    with sub5:
        st.subheader(f"Org-AI-R — {ticker}")
        if st.button("Run Org-AI-R", key="btn_orgair"):
            try:
                if run_mode == "Compute (POST)":
                    url = api_url(base, f"/api/v1/scoring/orgair/{ticker}")
                    with st.spinner(f"Computing Org-AI-R for {ticker} (full pipeline)..."):
                        data = post(url, timeout_s=300)
                else:
                    url = api_url(base, f"/api/v1/scoring/orgair/{ticker}")
                    with st.spinner(f"Loading Org-AI-R for {ticker}..."):
                        data = get(url, timeout_s=30)

                org_air = data.get("org_air_score") or data.get("org_air")
                bd      = data.get("breakdown") or {}
                val     = data.get("validation") or {}

                vr_score  = bd.get("vr_score")
                hr_score  = bd.get("hr_score")
                syn_score = bd.get("synergy_score")
                val_status = val.get("status", "—")

                render_kpis([
                    ("Org-AI-R",   f"{org_air:.2f}" if org_air is not None else "—"),
                    ("V^R",        f"{vr_score:.2f}" if vr_score is not None else "—"),
                    ("H^R",        f"{hr_score:.2f}" if hr_score is not None else "—"),
                    ("Synergy",    f"{syn_score:.2f}" if syn_score is not None else "—"),
                    ("Validation", val_status),
                ])

                # CI section
                vr_ci  = bd.get("vr_ci")  or {}
                hr_ci  = bd.get("hr_ci")  or {}
                air_ci = bd.get("orgair_ci") or {}

                if vr_ci or hr_ci or air_ci:
                    st.subheader("95% Confidence Intervals")
                    ci_cols = st.columns(3)
                    for ci_col, ci_data, label in [
                        (ci_cols[0], vr_ci,  "V^R CI"),
                        (ci_cols[1], hr_ci,  "H^R CI"),
                        (ci_cols[2], air_ci, "Org-AI-R CI"),
                    ]:
                        lo = ci_data.get("ci_lower")
                        hi = ci_data.get("ci_upper")
                        sem = ci_data.get("sem")
                        if lo is not None and hi is not None and sem is not None:
                            margin = SB_Z95 * sem
                            ci_col.metric(
                                label,
                                f"[{lo:.1f}, {hi:.1f}]",
                                delta=f"±{margin:.2f}",
                            )

                # Validation
                in_range = val.get("orgair_in_range")
                if in_range is True:
                    st.success(f"Org-AI-R in expected range — {val.get('orgair_expected', '')}")
                elif in_range is False:
                    st.warning(f"Org-AI-R out of expected range — expected {val.get('orgair_expected', '')}")

                # Formula decomposition
                with st.expander("Org-AI-R Formula Decomposition"):
                    alpha = bd.get("alpha", ORGAIR_ALPHA)
                    beta  = bd.get("beta",  ORGAIR_BETA)
                    st.code(
                        f"alpha = {alpha},  beta = {beta}\n\n"
                        f"V^R weighted  = α × V^R   = {alpha} × {vr_score or '?'}"
                        + (f" = {alpha * vr_score:.4f}" if vr_score else "") + "\n"
                        f"H^R weighted  = (1−α) × H^R = {1-alpha} × {hr_score or '?'}"
                        + (f" = {(1-alpha) * hr_score:.4f}" if hr_score else "") + "\n"
                        f"weighted_base = {bd.get('weighted_base', '?')}\n\n"
                        f"Synergy contribution = β × Synergy = {beta} × {syn_score or '?'}"
                        + (f" = {beta * syn_score:.4f}" if syn_score else "") + "\n\n"
                        f"Org-AI-R = (1−β) × weighted_base + β × Synergy\n"
                        f"         = {1-beta} × {bd.get('weighted_base', '?')}"
                        + (f" + {beta} × {syn_score:.4f}" if syn_score else "") + "\n"
                        f"         = {org_air:.4f}" if org_air else "         = ?",
                        language="text",
                    )

                # Synergy sub-section
                syn_details = bd
                if syn_details:
                    with st.expander("Synergy Details"):
                        st.code(
                            f"Synergy = (V^R × H^R / 100) × Alignment × TimingFactor\n"
                            f"        = ({vr_score or '?'} × {hr_score or '?'} / 100) × Alignment × 1.0\n"
                            + (
                                f"        = {vr_score * hr_score / 100:.4f} × Alignment\n"
                                f"        ≈ {syn_score:.4f}"
                                if vr_score and hr_score and syn_score else ""
                            ),
                            language="text",
                        )

                show_json("Raw Response", data)

            except requests.exceptions.ConnectionError:
                st.error(f"Cannot reach FastAPI at {base}. Is the server running?")
            except requests.exceptions.Timeout:
                st.error("Request timed out. Pipeline may still be running server-side.")
            except RuntimeError as e:
                st.error(f"API error: {e}")
            except Exception as e:
                st.error(f"Unexpected error: {e}")

# =====================================================================
# Tab 4 — Portfolio Dashboard
# =====================================================================

with tab_portfolio:
    st.header("Portfolio Dashboard — All 5 Companies")
    base = st.session_state["base_url"]

    ctrl_a, ctrl_b = st.columns(2)
    with ctrl_a:
        run_portfolio = st.button("Compute Full Portfolio (POST)", type="primary")
    with ctrl_b:
        load_portfolio = st.button("Load Saved Portfolio (GET)")

    if run_portfolio:
        try:
            url = api_url(base, "/api/v1/scoring/orgair/portfolio")
            with st.spinner("Computing Org-AI-R for all 5 companies — this may take several minutes..."):
                data = post(url, timeout_s=600)
            st.session_state["portfolio_results"] = data
            scored  = data.get("companies_scored", 0)
            failed  = data.get("companies_failed", 0)
            dur     = data.get("duration_seconds", 0)
            if failed == 0:
                st.success(f"Portfolio scored: {scored}/5 companies in {dur:.1f}s")
            else:
                st.warning(f"Partial results: {scored} scored, {failed} failed — {dur:.1f}s")
        except requests.exceptions.ConnectionError:
            st.error(f"Cannot reach FastAPI at {base}. Is the server running?")
        except requests.exceptions.Timeout:
            st.error("Portfolio computation timed out (>10 min). Check server logs.")
        except RuntimeError as e:
            st.error(f"API error: {e}")
        except Exception as e:
            st.error(f"Unexpected error: {e}")

    if load_portfolio:
        try:
            url = api_url(base, "/api/v1/scoring/orgair/portfolio")
            with st.spinner("Loading saved portfolio from Snowflake..."):
                data = get(url, timeout_s=30)
            st.session_state["portfolio_results"] = data
            st.info(data.get("message", "Portfolio loaded from Snowflake."))
        except requests.exceptions.ConnectionError:
            st.error(f"Cannot reach FastAPI at {base}. Is the server running?")
        except RuntimeError as e:
            st.error(f"API error: {e}")
        except Exception as e:
            st.error(f"Unexpected error: {e}")

    portfolio = st.session_state.get("portfolio_results")

    if portfolio:
        results  = portfolio.get("results", [])
        summary  = portfolio.get("summary_table", [])

        # If GET response (results field contains OrgAIRScoringRecord objects with just org_air)
        # handle gracefully
        has_breakdown = any(
            r.get("breakdown") or r.get("org_air_score") for r in results
        ) if results else bool(summary)

        # ---- A. Summary Table ----
        st.subheader("A. Summary Table")
        if summary:
            df_sum = pd.DataFrame(summary)
            # Add display columns
            df_display = df_sum.copy()
            df_display.insert(1, "Sector", [COMPANY_SECTORS.get(t, "—") for t in df_display.get("ticker", pd.Series()).tolist()])

            # Expected range column
            def _range_str(row):
                t = row.get("ticker", "")
                if t in EXPECTED_ORGAIR_RANGES:
                    lo, hi = EXPECTED_ORGAIR_RANGES[t]
                    return f"{lo:.0f} – {hi:.0f}"
                return "—"

            def _in_range(row):
                t = row.get("ticker", "")
                score = row.get("org_air_score")
                if t in EXPECTED_ORGAIR_RANGES and score is not None:
                    lo, hi = EXPECTED_ORGAIR_RANGES[t]
                    return "Yes" if lo <= score <= hi else "No"
                val = row.get("orgair_in_expected_range")
                if val is True:
                    return "Yes"
                if val is False:
                    return "No"
                return "—"

            df_display["Expected Range"] = [_range_str(r) for r in summary]
            df_display["In Range"]       = [_in_range(r)  for r in summary]
            st.dataframe(df_display, use_container_width=True, hide_index=True)
        elif results:
            # Minimal table from GET results
            rows = []
            for r in results:
                rows.append({
                    "Ticker": r.get("ticker", ""),
                    "Org-AI-R": r.get("org_air") or r.get("org_air_score"),
                    "Scored At": r.get("scored_at") or r.get("updated_at"),
                })
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

        # ---- B. Charts ----
        if summary and any(r.get("org_air_score") is not None for r in summary):
            st.subheader("B. Score Charts")
            chart_cols = st.columns(2)

            chart_data = {
                row["ticker"]: {
                    "Org-AI-R":  row.get("org_air_score"),
                    "V^R":       row.get("vr_score"),
                    "H^R":       row.get("hr_score"),
                }
                for row in summary if row.get("ticker")
            }

            with chart_cols[0]:
                st.markdown("**Org-AI-R by Company**")
                orgair_series = {t: d["Org-AI-R"] for t, d in chart_data.items() if d["Org-AI-R"] is not None}
                if orgair_series:
                    df_air = pd.DataFrame(
                        {"Org-AI-R": list(orgair_series.values())},
                        index=list(orgair_series.keys()),
                    )
                    st.bar_chart(df_air)
                    range_caption = " | ".join(
                        f"{t}: {lo:.0f}–{hi:.0f}" for t, (lo, hi) in EXPECTED_ORGAIR_RANGES.items()
                    )
                    st.caption(f"Expected ranges — {range_caption}")

            with chart_cols[1]:
                st.markdown("**V^R vs H^R by Company**")
                vr_vals  = {t: d["V^R"]  for t, d in chart_data.items() if d["V^R"]  is not None}
                hr_vals  = {t: d["H^R"]  for t, d in chart_data.items() if d["H^R"]  is not None}
                tickers_common = [t for t in vr_vals if t in hr_vals]
                if tickers_common:
                    df_vr_hr = pd.DataFrame(
                        {"V^R":  [vr_vals[t]  for t in tickers_common],
                         "H^R":  [hr_vals[t]  for t in tickers_common]},
                        index=tickers_common,
                    )
                    st.bar_chart(df_vr_hr)

            # ---- C. Synergy chart ----
            syn_vals = {row["ticker"]: row.get("synergy_score") for row in summary if row.get("synergy_score") is not None}
            if syn_vals:
                st.subheader("C. Synergy Scores")
                df_syn = pd.DataFrame({"Synergy": list(syn_vals.values())}, index=list(syn_vals.keys()))
                st.bar_chart(df_syn)

        # ---- D. Validation scorecard ----
        in_range_count = sum(
            1 for r in summary
            if r.get("orgair_in_expected_range") is True
            or (
                r.get("org_air_score") is not None
                and r.get("ticker") in EXPECTED_ORGAIR_RANGES
                and EXPECTED_ORGAIR_RANGES[r["ticker"]][0] <= r["org_air_score"] <= EXPECTED_ORGAIR_RANGES[r["ticker"]][1]
            )
        )
        st.subheader("D. Validation Scorecard")
        st.metric("Companies in Expected Org-AI-R Range", f"{in_range_count} / 5")

        # ---- E. Per-company expanders ----
        if results and has_breakdown:
            st.subheader("E. Per-Company Breakdown")
            for r in results:
                t = r.get("ticker", "?")
                air = r.get("org_air_score")
                status = r.get("status", "?")
                with st.expander(f"{t} — Org-AI-R: {air:.2f if air else '—'}" if air else f"{t} — {status}"):
                    bd  = r.get("breakdown") or {}
                    val = r.get("validation") or {}

                    if bd:
                        render_kpis([
                            ("V^R",    f"{bd.get('vr_score', '—'):.2f}" if bd.get('vr_score') is not None else "—"),
                            ("H^R",    f"{bd.get('hr_score', '—'):.2f}" if bd.get('hr_score') is not None else "—"),
                            ("Synergy",f"{bd.get('synergy_score', '—'):.2f}" if bd.get('synergy_score') is not None else "—"),
                            ("Org-AI-R",f"{air:.2f}" if air is not None else "—"),
                            ("Status", val.get("status", "—")),
                        ])

                    show_json("Full JSON", r)

        # ---- F. Other pipeline stages (nested sub-tabs) ----
        st.subheader("F. Other Pipeline Stages")
        fsub1, fsub2, fsub3, fsub4 = st.tabs([
            "Dimensions Summary",
            "TC + V^R Summary",
            "Position Factor Summary",
            "H^R Summary",
        ])

        with fsub1:
            if st.button("Load Dimensions Summary", key="port_dim"):
                try:
                    url = api_url(base, "/api/v1/scoring/summary")
                    with st.spinner("Loading dimensions summary..."):
                        data = get(url, timeout_s=30)
                    companies = data.get("companies", [])
                    if companies:
                        st.dataframe(pd.DataFrame(companies), use_container_width=True, hide_index=True)
                    else:
                        show_json("Raw Response", data)
                except Exception as e:
                    st.error(f"Error: {e}")

        with fsub2:
            if st.button("Load TC + V^R Summary", key="port_tcvr"):
                try:
                    url = api_url(base, "/api/v1/scoring/tc-vr/portfolio")
                    with st.spinner("Loading TC + V^R portfolio..."):
                        data = get(url, timeout_s=30)
                    summary_tbl = data.get("summary_table", [])
                    if summary_tbl:
                        st.dataframe(pd.DataFrame(summary_tbl), use_container_width=True, hide_index=True)
                    else:
                        show_json("Raw Response", data)
                except Exception as e:
                    st.error(f"Error: {e}")

        with fsub3:
            if st.button("Load PF Summary", key="port_pf"):
                try:
                    url = api_url(base, "/api/v1/scoring/pf/portfolio")
                    with st.spinner("Loading PF portfolio..."):
                        data = get(url, timeout_s=30)
                    summary_tbl = data.get("summary_table", [])
                    if summary_tbl:
                        st.dataframe(pd.DataFrame(summary_tbl), use_container_width=True, hide_index=True)
                    else:
                        show_json("Raw Response", data)
                except Exception as e:
                    st.error(f"Error: {e}")

        with fsub4:
            if st.button("Load H^R Summary", key="port_hr"):
                try:
                    url = api_url(base, "/api/v1/scoring/hr/portfolio")
                    with st.spinner("Loading H^R portfolio..."):
                        data = get(url, timeout_s=30)
                    summary_tbl = data.get("summary_table", [])
                    if summary_tbl:
                        st.dataframe(pd.DataFrame(summary_tbl), use_container_width=True, hide_index=True)
                    else:
                        show_json("Raw Response", data)
                except Exception as e:
                    st.error(f"Error: {e}")

        show_json("Raw Portfolio Response", portfolio)

    else:
        st.info("Click **Compute Full Portfolio** (POST) to score all 5 companies, or **Load Saved Portfolio** (GET) to read from Snowflake.")

# =====================================================================
# Tab 5 — Math Explorer (100% local, no API)
# =====================================================================

with tab_math:
    st.header("Math Explorer")
    st.caption("All computations are local — no API call required. Adjust sliders to explore formula sensitivity.")

    st.subheader("Inputs")
    mx_col1, mx_col2 = st.columns(2)

    with mx_col1:
        mx_vr  = st.slider("V^R Score",        0.0, 100.0, 75.0, step=0.5)
        mx_hr_base = st.slider("H^R Base Score", 0.0, 100.0, 68.0, step=0.5)
        mx_pf  = st.slider("Position Factor",  -1.0, 1.0,   0.50, step=0.01)
        mx_tf  = st.slider("Timing Factor",     0.8, 1.2,   1.00, step=0.05)

    with mx_col2:
        mx_n   = st.slider("Evidence Count",    1,    20,    7,    step=1)
        mx_override_align = st.checkbox("Override Alignment?", value=False)
        if mx_override_align:
            mx_align = st.slider("Alignment",   0.0, 1.0,   0.80, step=0.01)
        else:
            mx_align = None

    # --- Live computations ---
    mx_hr_score = mx_hr_base * (1.0 + HR_DELTA * mx_pf)
    mx_hr_score = _clamp(mx_hr_score, 0.0, 100.0)

    mx_syn  = compute_synergy_local(mx_vr, mx_hr_score, mx_align, mx_tf)
    mx_air  = compute_orgair_local(mx_vr, mx_hr_score, mx_syn["synergy"])
    mx_ci   = compute_ci_local(mx_air["org_air"], mx_n)
    mx_vr_ci = compute_ci_local(mx_vr, mx_n)
    mx_hr_ci = compute_ci_local(mx_hr_score, mx_n)

    # --- 4-column KPI row ---
    st.divider()
    st.subheader("Live Output")
    render_kpis([
        ("H^R Score",    f"{mx_hr_score:.2f}"),
        ("Synergy",      f"{mx_syn['synergy']:.2f}"),
        ("Org-AI-R",     f"{mx_air['org_air']:.2f}"),
        ("Reliability ρ",f"{mx_ci['reliability']:.4f}"),
    ])

    ci_c1, ci_c2, ci_c3 = st.columns(3)
    ci_c1.metric("V^R 95% CI", f"[{mx_vr_ci['ci_lower']:.1f}, {mx_vr_ci['ci_upper']:.1f}]", delta=f"±{mx_vr_ci['margin']:.2f}")
    ci_c2.metric("H^R 95% CI", f"[{mx_hr_ci['ci_lower']:.1f}, {mx_hr_ci['ci_upper']:.1f}]", delta=f"±{mx_hr_ci['margin']:.2f}")
    ci_c3.metric("Org-AI-R 95% CI", f"[{mx_ci['ci_lower']:.1f}, {mx_ci['ci_upper']:.1f}]", delta=f"±{mx_ci['margin']:.2f}")

    # --- Formula traces ---
    st.divider()
    st.subheader("Formula Traces")

    with st.expander("H^R Trace"):
        st.code(
            f"H^R = HR_base × (1 + δ × PF)\n"
            f"    = {mx_hr_base} × (1 + {HR_DELTA} × {mx_pf:.2f})\n"
            f"    = {mx_hr_base} × {1 + HR_DELTA * mx_pf:.4f}\n"
            f"    = {mx_hr_score:.4f}",
            language="text",
        )

    with st.expander("Synergy Trace"):
        align_auto = 1.0 - abs(mx_vr - mx_hr_score) / 100.0
        eff_align = mx_align if mx_align is not None else align_auto
        st.code(
            f"Alignment = 1 − |V^R − H^R| / 100  (auto)\n"
            f"          = 1 − |{mx_vr:.2f} − {mx_hr_score:.2f}| / 100\n"
            f"          = {align_auto:.4f}"
            + (f"  →  overridden to {mx_align:.4f}" if mx_align is not None else "") + "\n\n"
            f"TimingFactor = {mx_tf:.2f}  (clamped to [0.8, 1.2])\n\n"
            f"Synergy = (V^R × H^R / 100) × Alignment × TimingFactor\n"
            f"        = ({mx_vr:.2f} × {mx_hr_score:.2f} / 100) × {eff_align:.4f} × {mx_tf:.2f}\n"
            f"        = {mx_vr * mx_hr_score / 100:.4f} × {eff_align:.4f} × {mx_tf:.2f}\n"
            f"        = {mx_syn['synergy']:.4f}",
            language="text",
        )

    with st.expander("Org-AI-R Trace"):
        a = ORGAIR_ALPHA
        b = ORGAIR_BETA
        st.code(
            f"α = {a},  β = {b}\n\n"
            f"V^R weighted  = α × V^R = {a} × {mx_vr:.2f} = {mx_air['vr_weighted']:.4f}\n"
            f"H^R weighted  = (1−α) × H^R = {1-a} × {mx_hr_score:.2f} = {mx_air['hr_weighted']:.4f}\n"
            f"weighted_base = {mx_air['vr_weighted']:.4f} + {mx_air['hr_weighted']:.4f} = {mx_air['weighted_base']:.4f}\n\n"
            f"Synergy contribution = β × Synergy = {b} × {mx_syn['synergy']:.4f} = {mx_air['synergy_contribution']:.4f}\n\n"
            f"Org-AI-R = (1−β) × weighted_base + β × Synergy\n"
            f"         = {1-b} × {mx_air['weighted_base']:.4f} + {b} × {mx_syn['synergy']:.4f}\n"
            f"         = {(1-b)*mx_air['weighted_base']:.4f} + {mx_air['synergy_contribution']:.4f}\n"
            f"         = {mx_air['org_air']:.4f}",
            language="text",
        )

    with st.expander("Spearman-Brown CI Trace"):
        st.code(
            f"n = {mx_n},  r (base reliability) = {SB_BASE_R},  σ = {SB_SIGMA},  z_95 = {SB_Z95}\n\n"
            f"ρ_SB = (n × r) / (1 + (n−1) × r)\n"
            f"     = ({mx_n} × {SB_BASE_R}) / (1 + ({mx_n}−1) × {SB_BASE_R})\n"
            f"     = {mx_n * SB_BASE_R:.3f} / {1 + (mx_n - 1) * SB_BASE_R:.3f}\n"
            f"     = {mx_ci['reliability']:.4f}\n\n"
            f"SEM  = σ × √(1 − ρ_SB)\n"
            f"     = {SB_SIGMA} × √(1 − {mx_ci['reliability']:.4f})\n"
            f"     = {SB_SIGMA} × {math.sqrt(1 - mx_ci['reliability']):.4f}\n"
            f"     = {mx_ci['sem']:.4f}\n\n"
            f"Margin = z_95 × SEM = {SB_Z95} × {mx_ci['sem']:.4f} = {mx_ci['margin']:.4f}\n\n"
            f"Org-AI-R CI_95% = [{mx_air['org_air']:.2f} ± {mx_ci['margin']:.2f}]\n"
            f"                = [{mx_ci['ci_lower']:.2f}, {mx_ci['ci_upper']:.2f}]",
            language="text",
        )

    # --- Sensitivity Analysis ---
    st.divider()
    st.subheader("Sensitivity Analysis — Org-AI-R vs V^R")
    st.caption(f"H^R Base = {mx_hr_base}, PF = {mx_pf:.2f}, TF = {mx_tf}, n = {mx_n}. All other inputs held fixed.")

    vr_range = list(range(0, 105, 5))
    sensitivity_rows = []
    for vr_val in vr_range:
        hr_val = _clamp(mx_hr_base * (1.0 + HR_DELTA * mx_pf), 0.0, 100.0)
        syn_val = compute_synergy_local(float(vr_val), hr_val, mx_align, mx_tf)
        air_val = compute_orgair_local(float(vr_val), hr_val, syn_val["synergy"])
        sensitivity_rows.append({"V^R": vr_val, "Org-AI-R": air_val["org_air"]})

    df_sens = pd.DataFrame(sensitivity_rows).set_index("V^R")
    st.line_chart(df_sens)
