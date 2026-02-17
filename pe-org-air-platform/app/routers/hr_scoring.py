"""
routers/hr_scoring.py — CS3 Task 6.1 Endpoints

Endpoints:
  POST /api/v1/scoring/hr/{ticker}        — Compute H^R for one company
  POST /api/v1/scoring/hr/portfolio       — Compute H^R for all 5 companies
  POST /api/v1/scoring/hr/portfolio/report — Download report as .md

Register in main.py:
    from app.routers.hr_scoring import router as hr_router
    app.include_router(hr_router)
"""

from fastapi import APIRouter
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from datetime import datetime, timezone
from decimal import Decimal
import logging
import time
from fastapi.responses import StreamingResponse
import io

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/scoring", tags=["CS3 H^R (Human Readiness)"])

# The 5 CS3 portfolio companies
CS3_PORTFOLIO = ["NVDA", "JPM", "WMT", "GE", "DG"]

# Sector assignments
COMPANY_SECTORS = {
    "NVDA": "technology",
    "JPM": "financial_services",
    "WMT": "retail",
    "GE": "manufacturing",
    "DG": "retail",
}

# Expected HR ranges (calculated from HR_base × (1 + 0.15 × PF_range))
EXPECTED_HR_RANGES = {
    "NVDA": (82.9, 86.3),   # HR_base=75, PF=0.7-1.0
    "JPM": (71.1, 75.1),    # HR_base=68, PF=0.3-0.7
    "WMT": (55.8, 59.1),    # HR_base=55, PF=0.1-0.5
    "GE": (50.4, 53.6),     # HR_base=52, PF=-0.2-0.2
    "DG": (50.9, 54.2),     # HR_base=55, PF=-0.5--0.1
}


# =====================================================================
# Response Models
# =====================================================================

class HRValidation(BaseModel):
    """Validation against expected ranges."""
    hr_in_range: bool
    hr_expected: str
    status: str  # "✅", "⚠️", or "—"


class HRBreakdown(BaseModel):
    """H^R calculation breakdown."""
    hr_score: float
    hr_base: float
    position_factor: float
    position_adjustment: float
    sector: str
    interpretation: str


class HRResponse(BaseModel):
    """Single company H^R response."""
    ticker: str
    status: str  # "success" or "failed"
    
    # H^R outputs
    hr_score: Optional[float] = None
    hr_breakdown: Optional[HRBreakdown] = None
    
    # Validation
    validation: Optional[HRValidation] = None
    
    # Metadata
    duration_seconds: Optional[float] = None
    error: Optional[str] = None
    scored_at: Optional[str] = None


class PortfolioHRResponse(BaseModel):
    """Portfolio H^R response."""
    status: str
    companies_scored: int
    companies_failed: int
    results: List[HRResponse]
    summary_table: List[Dict[str, Any]]
    duration_seconds: float


# =====================================================================
# Helper: Compute H^R
# =====================================================================

def _compute_hr(ticker: str) -> HRResponse:
    """
    Core logic:
    1. Get Position Factor from PF endpoint
    2. Get sector
    3. Calculate H^R
    4. Validate against expected range
    """
    start = time.time()
    ticker = ticker.upper()
    
    try:
        logger.info("=" * 60)
        logger.info(f"🌐 H^R CALCULATION: {ticker}")
        logger.info("=" * 60)
        
        # ---- 1. Get Position Factor from PF endpoint ----
        from app.routers.position_factor import _compute_position_factor
        pf_result = _compute_position_factor(ticker)
        
        if pf_result.status != "success":
            raise ValueError(f"Position Factor calculation failed: {pf_result.error}")
        
        position_factor = pf_result.position_factor
        logger.info(f"[{ticker}] Position Factor from PF endpoint: {position_factor:.4f}")
        
        # ---- 2. Get sector ----
        sector = COMPANY_SECTORS.get(ticker)
        if sector is None:
            raise ValueError(f"No sector defined for {ticker}")
        
        logger.info(f"[{ticker}] Sector: {sector}")
        
        # ---- 3. Calculate H^R ----
        from app.scoring.hr_calculator import HRCalculator
        hr_calc = HRCalculator()
        
        hr_result = hr_calc.calculate(
            sector=sector,
            position_factor=position_factor
        )
        
        # Get interpretation
        interpretation = hr_calc.interpret_hr_score(float(hr_result.hr_score))
        
        logger.info(f"[{ticker}] H^R Breakdown:")
        logger.info(f"  Sector             = {sector}")
        logger.info(f"  HR Base            = {float(hr_result.hr_base):.2f}")
        logger.info(f"  Position Factor    = {position_factor:.4f}")
        logger.info(f"  Position Adj (δ×PF)= {float(hr_result.position_adjustment):.4f}")
        logger.info(f"  ───────────────────────────────────────")
        logger.info(f"  H^R Score          = {float(hr_result.hr_score):.2f}")
        logger.info(f"  Interpretation     = {interpretation}")
        
        # ---- 4. Validate against expected range ----
        validation = None
        if ticker in EXPECTED_HR_RANGES:
            exp_lo, exp_hi = EXPECTED_HR_RANGES[ticker]
            hr_ok = exp_lo <= float(hr_result.hr_score) <= exp_hi
            status = "✅" if hr_ok else "⚠️"
            
            validation = HRValidation(
                hr_in_range=hr_ok,
                hr_expected=f"{exp_lo:.1f} to {exp_hi:.1f}",
                status=status
            )
            
            logger.info(f"[{ticker}] Validation (CS3 Expected Range):")
            logger.info(f"  H^R = {float(hr_result.hr_score):.2f}  expected [{exp_lo:.1f}, {exp_hi:.1f}]  {status}")
        
        logger.info("=" * 60)
        
        return HRResponse(
            ticker=ticker,
            status="success",
            hr_score=float(hr_result.hr_score),
            hr_breakdown=HRBreakdown(
                hr_score=float(hr_result.hr_score),
                hr_base=float(hr_result.hr_base),
                position_factor=position_factor,
                position_adjustment=float(hr_result.position_adjustment),
                sector=sector,
                interpretation=interpretation,
            ),
            validation=validation,
            duration_seconds=round(time.time() - start, 2),
            scored_at=datetime.now(timezone.utc).isoformat(),
        )
        
    except Exception as e:
        logger.error(f"H^R calculation failed for {ticker}: {e}", exc_info=True)
        return HRResponse(
            ticker=ticker,
            status="failed",
            error=str(e),
            duration_seconds=round(time.time() - start, 2),
        )


# =====================================================================
# POST /api/v1/scoring/hr/portfolio — Calculate H^R for all 5 companies
# =====================================================================

@router.post(
    "/hr/portfolio",
    response_model=PortfolioHRResponse,
    summary="Calculate H^R for all 5 CS3 portfolio companies",
    description="""
    Runs Task 6.1 (H^R calculation) for all 5 companies: NVDA, JPM, WMT, GE, DG.
    
    Pipeline for each company:
    1. Get Position Factor from PF endpoint (Task 6.0a)
    2. Get sector baseline H^R
    3. Calculate H^R = HR_base × (1 + 0.15 × PF)
    4. Validate against expected ranges
    
    Returns individual breakdowns + summary comparison table.
    """,
)
async def score_portfolio_hr():
    """Calculate H^R for all 5 companies."""
    start = time.time()
    
    logger.info("=" * 70)
    logger.info("🚀 H^R PORTFOLIO SCORING — 5 COMPANIES")
    logger.info("=" * 70)
    
    results = []
    scored = 0
    failed = 0
    
    for ticker in CS3_PORTFOLIO:
        result = _compute_hr(ticker)
        results.append(result)
        if result.status == "success":
            scored += 1
        else:
            failed += 1
    
    # Build summary table
    summary = []
    logger.info("")
    logger.info("=" * 70)
    logger.info("📊 H^R SUMMARY TABLE")
    logger.info("=" * 70)
    logger.info(f"{'Ticker':<8} {'Sector':<20} {'HR Base':>9} {'PF':>8} "
                f"{'Adj':>8} {'H^R':>8} {'Range':>15} {'✓':>3}")
    logger.info("-" * 70)
    
    for r in results:
        if r.status == "success" and r.hr_breakdown:
            b = r.hr_breakdown
            val_status = r.validation.status if r.validation else "—"
            range_str = r.validation.hr_expected if r.validation else "—"
            
            logger.info(
                f"{r.ticker:<8} {b.sector:<20} {b.hr_base:>9.2f} {b.position_factor:>8.4f} "
                f"{b.position_adjustment:>8.4f} {b.hr_score:>8.2f} {range_str:>15} {val_status:>3}"
            )
            
            summary.append({
                "ticker": r.ticker,
                "sector": b.sector,
                "hr_base": b.hr_base,
                "position_factor": b.position_factor,
                "position_adjustment": b.position_adjustment,
                "hr_score": b.hr_score,
                "interpretation": b.interpretation,
                "hr_in_expected_range": r.validation.hr_in_range if r.validation else None,
            })
        else:
            logger.info(f"{r.ticker:<8} FAILED: {r.error}")
            summary.append({"ticker": r.ticker, "status": "failed", "error": r.error})
    
    logger.info("-" * 70)
    
    # Count validations
    hr_pass = sum(1 for r in results if r.validation and r.validation.hr_in_range)
    hr_total = sum(1 for r in results if r.validation)
    
    logger.info(f"Scored: {scored}  Failed: {failed}")
    logger.info(f"H^R Validation: {hr_pass}/{hr_total} within expected range")
    logger.info(f"Duration: {time.time() - start:.2f}s")
    logger.info("=" * 70)
    
    return PortfolioHRResponse(
        status="success" if failed == 0 else "partial",
        companies_scored=scored,
        companies_failed=failed,
        results=results,
        summary_table=summary,
        duration_seconds=round(time.time() - start, 2),
    )


# =====================================================================
# POST /api/v1/scoring/hr/{ticker} — Calculate H^R for one company
# =====================================================================

@router.post(
    "/hr/{ticker}",
    response_model=HRResponse,
    summary="Calculate H^R for one company",
    description="""
    Runs Task 6.1 (H^R calculation) for a single ticker.
    
    Pipeline:
    1. Get Position Factor from PF endpoint (Task 6.0a)
    2. Get sector baseline H^R
    3. Calculate H^R = HR_base × (1 + 0.15 × PF)
    4. Validate against expected range
    
    H^R interpretation:
    - 75-100: Highly Ready
    - 60-75: Moderately Ready
    - 45-60: Developing
    - 0-45: Not Ready
    """,
)
async def score_hr(ticker: str):
    """Calculate H^R for one company."""
    return _compute_hr(ticker.upper())


# =====================================================================
# POST /api/v1/scoring/hr/portfolio/report — Download MD Report
# =====================================================================

def _generate_hr_report(portfolio: PortfolioHRResponse) -> str:
    """Generate a markdown report from H^R results."""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    
    lines = []
    lines.append("# H^R (Human Readiness) Scoring — CS3 Portfolio Report")
    lines.append("")
    lines.append(f"**Generated:** {now}")
    lines.append(f"**Companies:** {portfolio.companies_scored} scored, {portfolio.companies_failed} failed")
    lines.append(f"**Duration:** {portfolio.duration_seconds}s")
    lines.append("")
    
    # ---- Summary Table ----
    lines.append("## Portfolio Summary Table")
    lines.append("")
    lines.append("| Ticker | Sector | HR Base | PF | Adj (δ×PF) | H^R | Expected Range | Status |")
    lines.append("|--------|--------|---------|-----|-----------|------|----------------|--------|")
    
    hr_pass = 0
    hr_total = 0
    
    for r in portfolio.results:
        if r.status != "success":
            lines.append(f"| {r.ticker} | — | — | — | — | — | — | ❌ |")
            continue
        
        b = r.hr_breakdown
        val = r.validation
        
        if val:
            hr_total += 1
            if val.hr_in_range:
                hr_pass += 1
        
        status = val.status if val else "—"
        range_str = val.hr_expected if val else "—"
        
        lines.append(
            f"| {r.ticker} | {b.sector} | {b.hr_base:.1f} | {b.position_factor:.4f} "
            f"| {b.position_adjustment:.4f} | {b.hr_score:.2f} | {range_str} | {status} |"
        )
    
    lines.append("")
    
    # ---- Scorecard ----
    lines.append("## Validation Scorecard")
    lines.append("")
    lines.append(f"- **H^R:** {hr_pass}/{hr_total} ✅ within expected range")
    lines.append("")
    
    # ---- Interpretation ----
    lines.append("## H^R Interpretation by Company")
    lines.append("")
    
    for r in portfolio.results:
        if r.status == "success" and r.hr_breakdown:
            b = r.hr_breakdown
            lines.append(f"### {r.ticker} — H^R = {b.hr_score:.2f}")
            lines.append(f"**{b.interpretation}**")
            lines.append("")
            lines.append(f"- Sector: {b.sector}")
            lines.append(f"- Base readiness: {b.hr_base:.1f}")
            lines.append(f"- Position adjustment: {b.position_adjustment:+.4f} (δ×PF = 0.15 × {b.position_factor:.4f})")
            lines.append("")
    
    # ---- Ordering ----
    scored = [r for r in portfolio.results if r.status == "success"]
    scored_sorted = sorted(scored, key=lambda r: r.hr_score or 0, reverse=True)
    ordering = " > ".join(f"{r.ticker} ({r.hr_score:.1f})" for r in scored_sorted)
    lines.append(f"**Relative ordering:** {ordering}")
    lines.append("")
    
    # ---- Footer ----
    lines.append("---")
    lines.append("")
    lines.append("*Report generated by CS3 H^R Scoring Pipeline*")
    lines.append(f"*Formula: H^R = HR_base × (1 + δ × PF), where δ = 0.15*")
    
    return "\n".join(lines)


@router.post(
    "/hr/portfolio/report",
    summary="Generate & download H^R portfolio report as .md file",
    description="""
    Calculates H^R for all 5 CS3 companies, then generates a
    downloadable Markdown report with:
    - Summary table with validation
    - Validation scorecard
    - Interpretation by company
    - Relative ordering
    
    Returns a downloadable `.md` file.
    """,
    responses={
        200: {
            "content": {"text/markdown": {}},
            "description": "Downloadable Markdown report file",
        }
    },
)
async def download_hr_report():
    """Calculate H^R for all 5 companies, generate MD report, return as downloadable file."""
    start = time.time()
    
    logger.info("📝 Generating downloadable H^R Portfolio Report")
    
    # Run portfolio scoring
    results = []
    scored = 0
    failed = 0
    
    for ticker in CS3_PORTFOLIO:
        result = _compute_hr(ticker)
        results.append(result)
        if result.status == "success":
            scored += 1
        else:
            failed += 1
    
    portfolio = PortfolioHRResponse(
        status="success" if failed == 0 else "partial",
        companies_scored=scored,
        companies_failed=failed,
        results=results,
        summary_table=[],
        duration_seconds=round(time.time() - start, 2),
    )
    
    # Generate markdown content
    md_content = _generate_hr_report(portfolio)
    
    # Build filename with timestamp
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filename = f"cs3_hr_report_{ts}.md"
    
    # Stream as downloadable file
    buffer = io.BytesIO(md_content.encode("utf-8"))
    buffer.seek(0)
    
    logger.info(f"📝 H^R report ready — {len(md_content)} chars, file={filename}")
    
    return StreamingResponse(
        content=buffer,
        media_type="text/markdown",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "Content-Length": str(len(md_content.encode("utf-8"))),
        },
    )