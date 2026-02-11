"""
Scoring Service — CS3 Pipeline Orchestrator
app/services/scoring_service.py

Orchestrates the full Task 5.0a + 5.0b pipeline for a single company:

  1. Read CS2 signal scores from company_signal_summaries (Snowflake)
  2. Read SEC section chunk text from S3 (via document_chunks metadata in Snowflake)
  3. Run RubricScorer on SEC section text → 3 EvidenceScores
  4. Combine 4 CS2 signal scores + 3 SEC rubric scores → 7 EvidenceScores
  5. Run EvidenceMapper → 7 DimensionScores
  6. Persist mapping matrix + dimension scores to Snowflake
  7. Return full result dict
"""

import json
import logging
from decimal import Decimal
from typing import Dict, List, Optional, Any
from datetime import datetime, timezone

from app.scoring.evidence_mapper import (
    EvidenceMapper, EvidenceScore, SignalSource, Dimension,
)
from app.scoring.rubric_scorer import RubricScorer
from app.repositories.scoring_repository import get_scoring_repository
from app.repositories.signal_repository import get_signal_repository
from app.repositories.company_repository import CompanyRepository
from app.services.snowflake import get_snowflake_connection

logger = logging.getLogger(__name__)


class ScoringService:
    """
    Orchestrates the CS3 scoring pipeline.

    Reads from:
      - company_signal_summaries (4 CS2 signal scores)
      - document_chunks + S3 (SEC section text for rubric scoring)

    Writes to:
      - signal_dimension_mapping (Table 1 matrix per ticker)
      - evidence_dimension_scores (7 aggregated dimension scores per ticker)
    """

    # SEC section names as they appear in document_chunks.section column
    SEC_SECTION_MAP = {
        "sec_item_1": ["business", "item_1_business", "item_1"],
        "sec_item_1a": ["risk_factors", "item_1a_risk_factors", "item_1a"],
        "sec_item_7": ["mda", "item_7_mda", "item_7"],
    }

    # Which rubric dimension each SEC section should be scored against
    SEC_RUBRIC_MAP = {
        "sec_item_1": "use_case_portfolio",     # Business → Use Case (primary)
        "sec_item_1a": "ai_governance",          # Risk Factors → Governance (primary)
        "sec_item_7": "leadership_vision",       # MD&A → Leadership (primary)
    }

    def __init__(self):
        self.mapper = EvidenceMapper()
        self.rubric_scorer = RubricScorer()
        self.scoring_repo = get_scoring_repository()
        self.signal_repo = get_signal_repository()
        self.company_repo = CompanyRepository()
        self.conn = get_snowflake_connection()

        # Cache: s3_key → list of parsed chunk dicts (avoids re-downloading)
        self._s3_chunk_cache: Dict[str, List[Dict]] = {}

    def score_company(self, ticker: str) -> Dict[str, Any]:
        """Full scoring pipeline for a company."""
        ticker = ticker.upper()
        self._s3_chunk_cache.clear()  # Fresh cache per company run

        logger.info(f"{'='*60}")
        logger.info(f"🎯 CS3 SCORING PIPELINE: {ticker}")
        logger.info(f"{'='*60}")

        # Step 0: Validate company exists
        company = self.company_repo.get_by_ticker(ticker)
        if not company:
            raise ValueError(f"Company not found for ticker: {ticker}")
        company_id = str(company["id"])

        # Step 1: Fetch CS2 signal scores
        logger.info(f"📊 Step 1: Fetching CS2 signal scores...")
        cs2_evidence = self._fetch_cs2_signals(company_id, ticker)
        logger.info(f"   Found {len(cs2_evidence)} CS2 signal scores")

        # Step 2: Fetch SEC section text and score via rubric
        logger.info(f"📄 Step 2: Fetching SEC sections & rubric scoring...")
        sec_evidence, sec_details = self._fetch_and_score_sec_sections(ticker)
        logger.info(f"   Found {len(sec_evidence)} SEC section scores")

        # Step 3: Combine all evidence
        all_evidence = cs2_evidence + sec_evidence
        logger.info(f"📋 Step 3: Total evidence sources = {len(all_evidence)}")

        # Step 4: Map to dimensions
        logger.info(f"🔄 Step 4: Mapping evidence to 7 dimensions...")
        dim_scores = self.mapper.map_evidence_to_dimensions(all_evidence)

        # Step 5: Build outputs
        mapping_matrix = self.mapper.build_mapping_matrix(all_evidence, ticker)
        dimension_summary = self.mapper.build_dimension_summary(all_evidence, ticker)
        coverage = self.mapper.get_coverage_report(all_evidence)

        # Step 6: Persist to Snowflake
        logger.info(f"💾 Step 5: Persisting to Snowflake...")
        persisted = False
        try:
            self.scoring_repo.upsert_mapping_matrix(mapping_matrix)
            self.scoring_repo.upsert_dimension_scores(dimension_summary)
            persisted = True
            logger.info(f"   ✅ Persisted {len(mapping_matrix)} mapping rows + {len(dimension_summary)} dimension scores")
        except Exception as e:
            logger.error(f"   ❌ Persistence failed: {e}")

        # Build result
        result = {
            "ticker": ticker,
            "company_id": company_id,
            "scored_at": datetime.now(timezone.utc).isoformat(),
            "mapping_matrix": mapping_matrix,
            "dimension_scores": dimension_summary,
            "coverage": {dim.value: info for dim, info in coverage.items()},
            "evidence_sources": {
                "cs2_signals": len(cs2_evidence),
                "sec_sections": len(sec_evidence),
                "sec_details": sec_details,
                "total": len(all_evidence),
            },
            "persisted": persisted,
        }

        # Log summary
        logger.info(f"\n{'─'*60}")
        logger.info(f"📊 DIMENSION SCORES FOR {ticker}:")
        logger.info(f"{'─'*60}")
        for row in dimension_summary:
            logger.info(
                f"   {row['dimension']:25s} | Score: {row['score']:6.2f} | "
                f"Conf: {row['confidence']:.3f} | Sources: {row['sources']}"
            )
        logger.info(f"{'─'*60}")
        logger.info(f"✅ Scoring complete for {ticker}")

        return result

    def score_all_companies(self) -> List[Dict[str, Any]]:
        """Score all companies that have CS2 signal data."""
        summaries = self.signal_repo.get_all_summaries()
        results = []
        for summary in summaries:
            ticker = summary.get("ticker")
            if not ticker:
                continue
            try:
                result = self.score_company(ticker)
                results.append(result)
            except Exception as e:
                logger.error(f"Failed to score {ticker}: {e}")
                results.append({"ticker": ticker, "error": str(e), "persisted": False})
        return results

    # ------------------------------------------------------------------
    # Private: Fetch CS2 signal scores
    # ------------------------------------------------------------------

    def _fetch_cs2_signals(self, company_id: str, ticker: str) -> List[EvidenceScore]:
        """Read the 4 CS2 signal scores from company_signal_summaries."""
        summary = self.signal_repo.get_summary_by_ticker(ticker)
        if not summary:
            logger.warning(f"   ⚠️  No signal summary found for {ticker}")
            return []

        evidence = []
        signal_map = {
            "technology_hiring_score": SignalSource.TECHNOLOGY_HIRING,
            "innovation_activity_score": SignalSource.INNOVATION_ACTIVITY,
            "digital_presence_score": SignalSource.DIGITAL_PRESENCE,
            "leadership_signals_score": SignalSource.LEADERSHIP_SIGNALS,
        }

        # Get signal count per category
        signals = self.signal_repo.get_signals_by_company(company_id)
        category_counts = {}
        for s in signals:
            cat = s.get("category", "")
            category_counts[cat] = category_counts.get(cat, 0) + 1

        for score_key, source_enum in signal_map.items():
            score_val = summary.get(score_key)
            if score_val is not None:
                ev_count = category_counts.get(source_enum.value, 1)
                evidence.append(EvidenceScore(
                    source=source_enum,
                    raw_score=Decimal(str(round(float(score_val), 2))),
                    confidence=Decimal("0.85"),
                    evidence_count=ev_count,
                    metadata={"from": "company_signal_summaries"},
                ))
                logger.info(f"   ✅ {source_enum.value}: {score_val:.1f}")
            else:
                logger.info(f"   ⚠️  {source_enum.value}: no score")

        return evidence

    # ------------------------------------------------------------------
    # Private: Fetch SEC section text & score via rubric
    # ------------------------------------------------------------------

    def _fetch_and_score_sec_sections(
        self, ticker: str
    ) -> tuple[List[EvidenceScore], Dict[str, Any]]:
        """
        1. Find 10-K chunk metadata in Snowflake (section + s3_key)
        2. Download unique S3 chunk files (each = JSON array of ALL chunks for that filing)
        3. Filter chunks by section name, concatenate text
        4. Run rubric scorer on concatenated section text
        """
        evidence = []
        details = {}

        for signal_source_key, section_names in self.SEC_SECTION_MAP.items():
            section_text = self._get_section_text(ticker, section_names)

            if not section_text:
                logger.info(f"   ⚠️  {signal_source_key}: no section text found")
                details[signal_source_key] = {"found": False, "word_count": 0}
                continue

            word_count = len(section_text.split())
            logger.info(f"   📄 {signal_source_key}: {word_count} words")

            # Score via rubric
            rubric_dimension = self.SEC_RUBRIC_MAP[signal_source_key]
            rubric_result = self.rubric_scorer.score_dimension(
                dimension=rubric_dimension,
                evidence_text=section_text,
            )

            source_enum = SignalSource(signal_source_key)
            evidence.append(EvidenceScore(
                source=source_enum,
                raw_score=rubric_result.score,
                confidence=rubric_result.confidence,
                evidence_count=1,
                metadata={
                    "rubric_dimension": rubric_dimension,
                    "rubric_level": rubric_result.level.label,
                    "matched_keywords": rubric_result.matched_keywords[:10],
                    "word_count": word_count,
                    "rationale": rubric_result.rationale,
                },
            ))

            details[signal_source_key] = {
                "found": True,
                "word_count": word_count,
                "rubric_score": float(rubric_result.score),
                "rubric_level": rubric_result.level.label,
                "rubric_confidence": float(rubric_result.confidence),
                "matched_keywords": rubric_result.matched_keywords[:10],
            }

            logger.info(
                f"   ✅ {signal_source_key} → rubric [{rubric_dimension}] = "
                f"{rubric_result.score} ({rubric_result.level.label})"
            )

        return evidence, details

    def _get_section_text(self, ticker: str, section_names: List[str]) -> Optional[str]:
        """
        Get concatenated section text for a ticker from S3 chunk files.

        Your chunk storage structure:
          - document_chunks table has: s3_key, section, chunk_index
          - All chunks for one filing share the SAME s3_key
            e.g. sec/chunks/JPM/10-K/2025-02-14_chunks.json
          - That S3 file is a JSON array of ALL chunks for that filing
          - Each chunk dict has: document_id, chunk_index, content, section, ...

        So we:
          1. Query Snowflake for unique s3_keys where section matches
          2. Download each S3 file ONCE (cached)
          3. Filter the chunks inside by section name
          4. Concatenate content
        """
        # Step 1: Find which S3 files contain our target sections
        placeholders = ", ".join(["%s"] * len(section_names))
        sql = f"""
        SELECT DISTINCT dc.s3_key
        FROM document_chunks dc
        JOIN documents d ON dc.document_id = d.id
        WHERE d.ticker = %s
        AND d.filing_type = '10-K'
        AND LOWER(dc.section) IN ({placeholders})
        AND d.status IN ('chunked', 'indexed', 'parsed')
        AND dc.s3_key IS NOT NULL
        """
        params = [ticker.upper()] + [s.lower() for s in section_names]

        cur = self.conn.cursor()
        try:
            cur.execute(sql, params)
            s3_keys = [row[0] for row in cur.fetchall() if row[0]]
        finally:
            cur.close()

        if not s3_keys:
            return None

        logger.info(f"   📦 Found {len(s3_keys)} S3 file(s) with target sections")

        # Step 2 + 3: Download each file, filter by section
        section_names_lower = {s.lower() for s in section_names}
        text_parts = []

        for s3_key in s3_keys:
            chunks = self._load_chunks_from_s3(s3_key)
            if not chunks:
                continue

            # Filter chunks whose section matches
            for chunk in chunks:
                chunk_section = (chunk.get("section") or "").lower()
                if chunk_section in section_names_lower:
                    content = chunk.get("content", "")
                    if content and content.strip():
                        text_parts.append(content)

        if text_parts:
            combined = "\n\n".join(text_parts)
            logger.info(f"   📝 Extracted {len(text_parts)} matching chunks, {len(combined.split())} total words")
            return combined

        return None

    def _load_chunks_from_s3(self, s3_key: str) -> List[Dict]:
        """
        Download a chunks JSON file from S3 and return parsed list of chunk dicts.

        File format (your existing structure):
          [
            {"document_id": "...", "chunk_index": 0, "content": "...", "section": "business", ...},
            {"document_id": "...", "chunk_index": 1, "content": "...", "section": "risk_factors", ...},
            ...
          ]

        Uses s3.get_file() which returns bytes or None.
        Results are cached to avoid re-downloading the same file multiple times.
        """
        # Check cache first
        if s3_key in self._s3_chunk_cache:
            return self._s3_chunk_cache[s3_key]

        try:
            from app.services.s3_storage import get_s3_service
            s3 = get_s3_service()

            # get_file() returns bytes or None
            data = s3.get_file(s3_key)
            if data is None:
                logger.warning(f"   ⚠️  S3 file not found: {s3_key}")
                self._s3_chunk_cache[s3_key] = []
                return []

            # Decode bytes → string
            text = data.decode("utf-8") if isinstance(data, bytes) else str(data)

            # Parse JSON
            parsed = json.loads(text)

            # Normalize structure
            if isinstance(parsed, list):
                chunks = parsed
            elif isinstance(parsed, dict):
                # Handle wrapper formats: {"chunks": [...]} or single chunk
                if "chunks" in parsed:
                    chunks = parsed["chunks"]
                elif "content" in parsed:
                    chunks = [parsed]
                else:
                    # Unknown dict shape — try to treat values as chunks
                    chunks = []
            else:
                chunks = []

            logger.info(f"   📦 Loaded {len(chunks)} chunks from S3: {s3_key}")
            self._s3_chunk_cache[s3_key] = chunks
            return chunks

        except json.JSONDecodeError as e:
            logger.warning(f"   ⚠️  JSON parse failed for {s3_key}: {e}")
            self._s3_chunk_cache[s3_key] = []
            return []
        except Exception as e:
            logger.warning(f"   ⚠️  S3 load failed for {s3_key}: {e}")
            self._s3_chunk_cache[s3_key] = []
            return []


# Singleton
_service: Optional[ScoringService] = None

def get_scoring_service() -> ScoringService:
    global _service
    if _service is None:
        _service = ScoringService()
    return _service