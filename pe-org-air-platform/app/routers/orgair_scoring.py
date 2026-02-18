"""
routers/orgair_scoring.py — CS3 Task 6.4 Endpoints

Endpoints:
  POST /api/v1/scoring/orgair/{ticker}        — Compute Org-AI-R for one company
  POST /api/v1/scoring/orgair/portfolio       — Compute Org-AI-R for all 5 CS3 companies
  GET  /api/v1/scoring/orgair/portfolio       — Read portfolio from Snowflake
  GET  /api/v1/scoring/orgair/{ticker}        — Read one from Snowflake

Register in main.py:
    from app.routers.orgair_scoring import router as orgair_router
    app.include_router(orgair_router)
"""

from fastapi import APIRouter
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from datetime import datetime, timezone
from decimal import Decimal
import logging
import time

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/scoring", tags=["CS3 Org-AI-R"])

# The 5 CS3 portfolio companies
CS3_PORTFOLIO = ["NVDA", "JPM", "WMT", "GE", "DG"]

# Sector assignments (matches hr_scoring.py)
COMPANY_SECTORS = {
    "NVDA": "technology",
    "JPM": "financial_services",
    "WMT": "retail",
    "GE": "manufacturing",
    "DG": "retail",
}

# Expected OrgAIR ranges from case study Table 5
EXPECTED_ORGAIR_RANGES = {
    "NVDA": (85.0, 95.0),
    "JPM":  (65.0, 75.0),
    "WMT":  (55.0, 65.0),
    "GE":   (45.0, 55.0),
    "DG":   (35.0, 45.0),
}


# =====================================================================
# Response Models
# =====================================================================

class CIBreakdown(BaseModel):
    ci_lower: float
    ci_upper: float
    sem: float
    reliability: float
    score_type: str


class OrgAIRBreakdown(BaseModel):
    org_air_score: float
    vr_score: float
    hr_score: float
    synergy_score: float
    weighted_base: float
    synergy_contribution: float
    vr_weighted: float
    hr_weighted: float
    alpha: float
    beta: float
    vr_ci: Optional[CIBreakdown] = None
    hr_ci: Optional[CIBreakdown] = None
    orgair_ci: Optional[CIBreakdown] = None


class OrgAIRValidation(BaseModel):
    orgair_in_range: bool
    orgair_expected: str
    status: str  # "✅", "⚠️", or "—"


class OrgAIRResponse(BaseModel):
    ticker: str
    status: str  # "success" or "failed"

    # Outputs
    org_air_score: Optional[float] = None
    breakdown: Optional[OrgAIRBreakdown] = None

    # Validation
    validation: Optional[OrgAIRValidation] = None

    # Metadata
    duration_seconds: Optional[float] = None
    error: Optional[str] = None
    scored_at: Optional[str] = None


class PortfolioOrgAIRResponse(BaseModel):
    status: str
    companies_scored: int
    companies_failed: int
    results: List[OrgAIRResponse]
    summary_table: List[Dict[str, Any]]
    duration_seconds: float


# =====================================================================
# Helper: Compute Org-AI-R
# =====================================================================

def _compute_orgair(ticker: str) -> OrgAIRResponse:
    """
    Core logic:
    1. Get V^R score from tc_vr_scoring pipeline
    2. Get H^R score from hr_scoring pipeline
    3. Compute Synergy
    4. Compute Org-AI-R
    5. Compute CIs for VR, HR, Org-AI-R
    6. Validate against expected range
    """
    start = time.time()
    ticker = ticker.upper()

    try:
        logger.info("=" * 60)
        logger.info(f"Org-AI-R CALCULATION: {ticker}")
        logger.info("=" * 60)

        # ---- 1. Get V^R ----
        from app.routers.tc_vr_scoring import _compute_tc_vr
        vr_response = _compute_tc_vr(ticker)

        if vr_response.status != "success":
            raise ValueError(f"V^R calculation failed: {vr_response.error}")

        vr_score = vr_response.vr_result.vr_score if vr_response.vr_result else None
        if vr_score is None:
            raise ValueError("V^R score missing from TC+VR response")

        logger.info(f"[{ticker}] V^R = {vr_score:.2f}")

        # ---- 2. Get H^R ----
        from app.routers.hr_scoring import _compute_hr
        hr_response = _compute_hr(ticker)

        if hr_response.status != "success":
            raise ValueError(f"H^R calculation failed: {hr_response.error}")

        hr_score = hr_response.hr_score
        logger.info(f"[{ticker}] H^R = {hr_score:.2f}")

        # ---- 3. Synergy ----
        from app.scoring.synergy_calculator import SynergyCalculator
        synergy_result = SynergyCalculator().calculate(vr_score, hr_score)
        synergy_score = float(synergy_result.synergy_score)
        logger.info(f"[{ticker}] Synergy = {synergy_score:.2f}")

        # ---- 4. Org-AI-R ----
        from app.scoring.orgair_calculator import OrgAIRCalculator
        orgair_result = OrgAIRCalculator().calculate(vr_score, hr_score, synergy_score)
        org_air_score = float(orgair_result.org_air_score)
        logger.info(f"[{ticker}] Org-AI-R = {org_air_score:.2f}")

        # ---- 5. Confidence Intervals ----
        from app.scoring.confidence_calculator import ConfidenceCalculator
        ci_calc = ConfidenceCalculator()

        vr_ci_result = ci_calc.calculate(vr_score, 7, "vr")
        hr_ci_result = ci_calc.calculate(hr_score, 7, "hr")
        orgair_ci_result = ci_calc.calculate(org_air_score, 7, "org_air")

        vr_ci = CIBreakdown(
            ci_lower=float(vr_ci_result.ci_lower),
            ci_upper=float(vr_ci_result.ci_upper),
            sem=float(vr_ci_result.sem),
            reliability=float(vr_ci_result.reliability),
            score_type="vr",
        )
        hr_ci = CIBreakdown(
            ci_lower=float(hr_ci_result.ci_lower),
            ci_upper=float(hr_ci_result.ci_upper),
            sem=float(hr_ci_result.sem),
            reliability=float(hr_ci_result.reliability),
            score_type="hr",
        )
        orgair_ci = CIBreakdown(
            ci_lower=float(orgair_ci_result.ci_lower),
            ci_upper=float(orgair_ci_result.ci_upper),
            sem=float(orgair_ci_result.sem),
            reliability=float(orgair_ci_result.reliability),
            score_type="org_air",
        )

        logger.info(
            f"[{ticker}] Org-AI-R CI = [{float(orgair_ci_result.ci_lower):.2f}, "
            f"{float(orgair_ci_result.ci_upper):.2f}]  reliability={float(orgair_ci_result.reliability):.4f}"
        )

        # ---- 6. Validate ----
        validation = None
        if ticker in EXPECTED_ORGAIR_RANGES:
            exp_lo, exp_hi = EXPECTED_ORGAIR_RANGES[ticker]
            in_range = exp_lo <= org_air_score <= exp_hi
            status = "✅" if in_range else "⚠️"
            validation = OrgAIRValidation(
                orgair_in_range=in_range,
                orgair_expected=f"{exp_lo:.1f} to {exp_hi:.1f}",
                status=status,
            )
            logger.info(
                f"[{ticker}] Validation: Org-AI-R={org_air_score:.2f} "
                f"expected [{exp_lo:.1f}, {exp_hi:.1f}]  {status}"
            )

        breakdown = OrgAIRBreakdown(
            org_air_score=org_air_score,
            vr_score=vr_score,
            hr_score=hr_score,
            synergy_score=synergy_score,
            weighted_base=float(orgair_result.weighted_base),
            synergy_contribution=float(orgair_result.synergy_contribution),
            vr_weighted=float(orgair_result.vr_weighted),
            hr_weighted=float(orgair_result.hr_weighted),
            alpha=float(orgair_result.alpha),
            beta=float(orgair_result.beta),
            vr_ci=vr_ci,
            hr_ci=hr_ci,
            orgair_ci=orgair_ci,
        )

        logger.info("=" * 60)

        return OrgAIRResponse(
            ticker=ticker,
            status="success",
            org_air_score=org_air_score,
            breakdown=breakdown,
            validation=validation,
            duration_seconds=round(time.time() - start, 2),
            scored_at=datetime.now(timezone.utc).isoformat(),
        )

    except Exception as e:
        logger.error(f"Org-AI-R calculation failed for {ticker}: {e}", exc_info=True)
        return OrgAIRResponse(
            ticker=ticker,
            status="failed",
            error=str(e),
            duration_seconds=round(time.time() - start, 2),
        )


# =====================================================================
# POST /api/v1/scoring/orgair/portfolio — Calculate Org-AI-R for all 5
# =====================================================================

@router.post(
    "/orgair/portfolio",
    response_model=PortfolioOrgAIRResponse,
    summary="Calculate Org-AI-R for all 5 CS3 portfolio companies",
    description="""
    Runs the full Org-AI-R scoring pipeline for all 5 portfolio companies.

    Pipeline for each company:
    1. Get V^R (from TC+VR pipeline)
    2. Get H^R (from HR pipeline)
    3. Compute Synergy = (VR × HR / 100) × Alignment × TimingFactor
    4. Compute Org-AI-R = (1−β) × [α×VR + (1−α)×HR] + β×Synergy
    5. Compute SEM-based 95% CI for VR, HR, Org-AI-R
    6. Validate against expected ranges (Table 5)

    Returns individual breakdowns + summary comparison table.
    """,
)
async def score_portfolio_orgair():
    """Calculate Org-AI-R for all 5 companies."""
    start = time.time()

    logger.info("=" * 70)
    logger.info("Org-AI-R PORTFOLIO SCORING — 5 COMPANIES")
    logger.info("=" * 70)

    results = []
    scored = 0
    failed = 0

    for ticker in CS3_PORTFOLIO:
        result = _compute_orgair(ticker)
        results.append(result)
        if result.status == "success":
            scored += 1
            _save_orgair_result(result)
        else:
            failed += 1

    # Build summary table
    summary = []
    logger.info("")
    logger.info("=" * 70)
    logger.info("Org-AI-R SUMMARY TABLE")
    logger.info("=" * 70)
    logger.info(
        f"{'Ticker':<8} {'V^R':>8} {'H^R':>8} {'Synergy':>9} {'Org-AI-R':>10} "
        f"{'Range':>15} {'✓':>3}"
    )
    logger.info("-" * 70)

    for r in results:
        if r.status == "success" and r.breakdown:
            b = r.breakdown
            val_status = r.validation.status if r.validation else "—"
            range_str = r.validation.orgair_expected if r.validation else "—"

            logger.info(
                f"{r.ticker:<8} {b.vr_score:>8.2f} {b.hr_score:>8.2f} "
                f"{b.synergy_score:>9.2f} {b.org_air_score:>10.2f} "
                f"{range_str:>15} {val_status:>3}"
            )

            summary.append({
                "ticker": r.ticker,
                "vr_score": b.vr_score,
                "hr_score": b.hr_score,
                "synergy_score": b.synergy_score,
                "org_air_score": b.org_air_score,
                "weighted_base": b.weighted_base,
                "synergy_contribution": b.synergy_contribution,
                "orgair_in_expected_range": r.validation.orgair_in_range if r.validation else None,
            })
        else:
            logger.info(f"{r.ticker:<8} FAILED: {r.error}")
            summary.append({"ticker": r.ticker, "status": "failed", "error": r.error})

    logger.info("-" * 70)

    orgair_pass = sum(1 for r in results if r.validation and r.validation.orgair_in_range)
    orgair_total = sum(1 for r in results if r.validation)

    logger.info(f"Scored: {scored}  Failed: {failed}")
    logger.info(f"Org-AI-R Validation: {orgair_pass}/{orgair_total} within expected range")
    logger.info(f"Duration: {time.time() - start:.2f}s")
    logger.info("=" * 70)

    return PortfolioOrgAIRResponse(
        status="success" if failed == 0 else "partial",
        companies_scored=scored,
        companies_failed=failed,
        results=results,
        summary_table=summary,
        duration_seconds=round(time.time() - start, 2),
    )


# =====================================================================
# POST /api/v1/scoring/orgair/{ticker} — Calculate Org-AI-R for one company
# =====================================================================

@router.post(
    "/orgair/{ticker}",
    response_model=OrgAIRResponse,
    summary="Calculate Org-AI-R for one company",
    description="""
    Runs the full Org-AI-R scoring pipeline for a single ticker.

    Pipeline:
    1. Get V^R (from TC+VR pipeline)
    2. Get H^R (from HR pipeline)
    3. Compute Synergy = (VR × HR / 100) × Alignment × TimingFactor
    4. Compute Org-AI-R = (1−β) × [α×VR + (1−α)×HR] + β×Synergy
       where α=0.60, β=0.12
    5. Compute SEM-based 95% CI
    6. Validate against expected range

    Saves result to S3 + Snowflake.
    """,
)
async def score_orgair(ticker: str):
    """Calculate Org-AI-R for one company. Saves to S3 + Snowflake SCORING table."""
    result = _compute_orgair(ticker.upper())
    if result.status == "success":
        _save_orgair_result(result)
    return result


# =====================================================================
# Snowflake + S3 persistence helpers
# =====================================================================

def _save_orgair_result(result: OrgAIRResponse) -> None:
    """Save Org-AI-R result to S3 JSON and upsert into Snowflake SCORING table."""
    ticker = result.ticker
    try:
        from app.services.s3_storage import get_s3_service
        s3 = get_s3_service()
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        s3_key = f"scoring/orgair/{ticker}/{ts}.json"
        s3.upload_json(result.model_dump(), s3_key)
        logger.info(f"[{ticker}] Org-AI-R result saved to S3: {s3_key}")
    except Exception as e:
        logger.warning(f"[{ticker}] S3 save failed (non-fatal): {e}")

    try:
        _upsert_scoring_orgair(ticker, org_air=result.org_air_score)
        logger.info(f"[{ticker}] SCORING table upserted: org_air={result.org_air_score}")
    except Exception as e:
        logger.warning(f"[{ticker}] Snowflake SCORING upsert failed (non-fatal): {e}")


def _upsert_scoring_orgair(ticker: str, org_air: Optional[float]) -> None:
    """MERGE INTO SCORING — updates only the org_air column, preserving existing columns."""
    from app.services.snowflake import get_snowflake_connection
    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        sql = """
            MERGE INTO SCORING AS tgt
            USING (SELECT %s AS ticker) AS src
            ON tgt.ticker = src.ticker
            WHEN MATCHED THEN UPDATE SET
                org_air    = %s,
                updated_at = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN INSERT
                (ticker, org_air, scored_at, updated_at)
            VALUES
                (%s, %s, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())
        """
        cursor.execute(sql, [ticker, org_air, ticker, org_air])
        conn.commit()
        cursor.close()
    finally:
        conn.close()


# =====================================================================
# Response models for GET endpoints
# =====================================================================

class OrgAIRScoringRecord(BaseModel):
    """Org-AI-R score stored in the SCORING table for one company."""
    ticker: str
    org_air: Optional[float] = None
    scored_at: Optional[str] = None
    updated_at: Optional[str] = None


class PortfolioOrgAIRScoringResponse(BaseModel):
    """SCORING table org_air rows for all CS3 portfolio companies."""
    status: str
    results: List[OrgAIRScoringRecord]
    message: Optional[str] = None


def _fetch_orgair_row(ticker: str) -> Optional[OrgAIRScoringRecord]:
    """Read org_air column from the Snowflake SCORING table for one ticker."""
    from app.services.snowflake import get_snowflake_connection
    from snowflake.connector import DictCursor
    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor(DictCursor)
        cursor.execute(
            "SELECT ticker, org_air, scored_at, updated_at FROM SCORING WHERE ticker = %s",
            [ticker.upper()],
        )
        row = cursor.fetchone()
        cursor.close()
        if not row:
            return None
        return OrgAIRScoringRecord(
            ticker=row["TICKER"],
            org_air=row.get("ORG_AIR"),
            scored_at=str(row["SCORED_AT"]) if row.get("SCORED_AT") else None,
            updated_at=str(row["UPDATED_AT"]) if row.get("UPDATED_AT") else None,
        )
    finally:
        conn.close()


# =====================================================================
# GET /api/v1/scoring/orgair/portfolio — Read all 5 from Snowflake
# =====================================================================

@router.get(
    "/orgair/portfolio",
    response_model=PortfolioOrgAIRScoringResponse,
    summary="Get last computed Org-AI-R for all 5 CS3 companies (from Snowflake)",
    description="""
    Reads the latest Org-AI-R scores for all 5 CS3 portfolio companies from the
    Snowflake SCORING table. No computation is performed.

    Use POST /orgair/portfolio to (re)compute and refresh the stored scores.
    """,
)
async def get_portfolio_orgair():
    """Return last stored Org-AI-R for all 5 portfolio companies."""
    results = []
    for ticker in CS3_PORTFOLIO:
        try:
            row = _fetch_orgair_row(ticker)
            results.append(row if row else OrgAIRScoringRecord(ticker=ticker))
        except Exception as e:
            logger.warning(f"[{ticker}] Failed to fetch SCORING row: {e}")
            results.append(OrgAIRScoringRecord(ticker=ticker))

    scored = sum(1 for r in results if r.org_air is not None)
    return PortfolioOrgAIRScoringResponse(
        status="ok",
        results=results,
        message=f"{scored}/{len(CS3_PORTFOLIO)} companies have stored Org-AI-R scores",
    )


# =====================================================================
# GET /api/v1/scoring/orgair/{ticker} — Read one from Snowflake
# =====================================================================

@router.get(
    "/orgair/{ticker}",
    response_model=OrgAIRScoringRecord,
    summary="Get last computed Org-AI-R for one company (from Snowflake)",
    description="""
    Reads the latest Org-AI-R score for a single ticker from the Snowflake SCORING table.
    No computation is performed.

    Use POST /orgair/{ticker} to (re)compute and refresh the stored score.
    """,
)
async def get_orgair(ticker: str):
    """Return last stored Org-AI-R for one company."""
    from fastapi import HTTPException
    row = _fetch_orgair_row(ticker.upper())
    if not row:
        raise HTTPException(
            status_code=404,
            detail=f"No scoring record found for {ticker.upper()}. Run POST /orgair/{ticker} first.",
        )
    return row
