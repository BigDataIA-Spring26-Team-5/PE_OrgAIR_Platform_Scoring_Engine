# app/scoring/talent_concentration.py
import json
import logging
import re
from dataclasses import dataclass
from decimal import Decimal
from typing import List, Optional, Set

logger = logging.getLogger(__name__)

# AI awareness keywords (mirrors glassdoor_collector.py AI_AWARENESS_KEYWORDS)
_AI_KEYWORDS = frozenset([
    "ai", "machine learning", "automation", "deep learning", "nlp",
    "llm", "generative ai", "data science", "neural network",
    "artificial intelligence", "ml", "large language model",
    "computer vision", "predictive analytics",
])


@dataclass
class JobAnalysis:
    """Analysis of job postings for talent concentration."""
    total_ai_jobs: int
    senior_ai_jobs: int    # Principal, Staff, Director, VP level
    mid_ai_jobs: int       # Senior, Lead level
    entry_ai_jobs: int     # Junior, Associate, entry level
    unique_skills: Set[str]


@dataclass
class GlassdoorReview:
    """Single employee review loaded from the S3 raw snapshot."""
    review_id: str
    rating: float
    title: str
    pros: str
    cons: str
    advice_to_management: Optional[str]
    is_current_employee: bool
    job_title: str
    review_date: Optional[str]
    source: str


class TalentConcentrationCalculator:
    def __init__(self):
        pass

    # ------------------------------------------------------------------ #
    # Job-posting analysis (unchanged)                                     #
    # ------------------------------------------------------------------ #

    def analyze_job_postings(self, postings: List[dict]) -> JobAnalysis:
        """Analyze job postings to extract AI talent concentration signals."""
        senior_keywords = {"principal", "staff", "director", "vp", "head", "chief"}
        mid_keywords = {"senior", "lead", "manager"}
        entry_keywords = {"junior", "associate", "entry", "intern"}

        total_ai_jobs = 0
        senior_ai_jobs = 0
        mid_ai_jobs = 0
        entry_ai_jobs = 0
        unique_skills: Set[str] = set()

        for posting in postings:
            if not posting.get("is_ai_role", False):
                continue
            total_ai_jobs += 1
            title_lower = posting.get("title", "").lower()
            title_words = set(title_lower.split())
            if title_words & senior_keywords:
                senior_ai_jobs += 1
            elif title_words & mid_keywords:
                mid_ai_jobs += 1
            elif title_words & entry_keywords:
                entry_ai_jobs += 1
            unique_skills.update(posting.get("ai_skills_found", []))

        return JobAnalysis(
            total_ai_jobs=total_ai_jobs,
            senior_ai_jobs=senior_ai_jobs,
            mid_ai_jobs=mid_ai_jobs,
            entry_ai_jobs=entry_ai_jobs,
            unique_skills=unique_skills,
        )

    # ------------------------------------------------------------------ #
    # Glassdoor S3 loader                                                  #
    # ------------------------------------------------------------------ #

    @staticmethod
    def load_glassdoor_reviews(
        ticker: str,
        s3_service=None,
    ) -> List[GlassdoorReview]:
        """Load individual reviews from the latest raw S3 snapshot for *ticker*.

        S3 path: glassdoor_signals/raw/{ticker}/{timestamp}_raw.json
        The wrapper JSON has shape: {"reviews": [...], "review_count": N, ...}
        Files sort lexicographically by timestamp; the last key is the most recent.
        Returns an empty list if no data exists or the download fails.
        """
        from app.services.s3_storage import get_s3_service

        svc = s3_service or get_s3_service()
        prefix = f"glassdoor_signals/raw/{ticker.upper()}/"
        keys = svc.list_files(prefix)
        if not keys:
            logger.warning("No Glassdoor raw snapshot found in S3 for %s", ticker)
            return []

        latest_key = sorted(keys)[-1]
        raw = svc.get_file(latest_key)
        if raw is None:
            logger.error("Failed to download Glassdoor snapshot: %s", latest_key)
            return []

        try:
            wrapper = json.loads(raw)
        except json.JSONDecodeError:
            logger.error("Invalid JSON in Glassdoor snapshot: %s", latest_key)
            return []

        reviews = []
        for item in wrapper.get("reviews", []):
            reviews.append(GlassdoorReview(
                review_id=item.get("review_id", ""),
                rating=float(item.get("rating") or 0.0),
                title=item.get("title") or "",
                pros=item.get("pros") or "",
                cons=item.get("cons") or "",
                advice_to_management=item.get("advice_to_management"),
                is_current_employee=bool(item.get("is_current_employee", False)),
                job_title=item.get("job_title") or "",
                review_date=item.get("review_date"),
                source=item.get("source", "unknown"),
            ))

        logger.info("Loaded %d Glassdoor reviews for %s from %s", len(reviews), ticker, latest_key)
        return reviews

    # ------------------------------------------------------------------ #
    # AI-mention counter                                                   #
    # ------------------------------------------------------------------ #

    @staticmethod
    def count_ai_mentions(reviews: List[GlassdoorReview]) -> tuple:
        """Return (ai_mention_count, total_review_count).

        A review counts as an AI mention if any AI awareness keyword appears
        in the combined text of pros, cons, advice_to_management, title, or job_title.
        Whole-word matching is used for short terms ('ai', 'ml', 'nlp', 'llm').
        """
        whole_word_terms = frozenset(["ai", "ml", "nlp", "llm"])
        ai_mention_count = 0

        for review in reviews:
            text = " ".join(filter(None, [
                review.pros,
                review.cons,
                review.advice_to_management or "",
                review.title,
                review.job_title,
            ])).lower()

            matched = False
            for kw in _AI_KEYWORDS:
                if kw in whole_word_terms:
                    if re.search(rf"\b{re.escape(kw)}\b", text):
                        matched = True
                        break
                else:
                    if kw in text:
                        matched = True
                        break
            if matched:
                ai_mention_count += 1

        return ai_mention_count, len(reviews)

    # ------------------------------------------------------------------ #
    # TC score                                                             #
    # ------------------------------------------------------------------ #

    def calculate_tc(
        self,
        job_analysis: JobAnalysis,
        glassdoor_reviews: Optional[List[GlassdoorReview]] = None,
    ) -> Decimal:
        """Calculate Talent Concentration (TC) score as a Decimal in [0, 1].

        Formula (weights unchanged from original):
            TC = 0.40 × leadership_ratio      (senior AI jobs / total AI jobs)
               + 0.30 × team_size_factor      (inverse-sqrt of team size)
               + 0.20 × skill_concentration   (1 - unique_skills / 15)
               + 0.10 × individual_factor     (fraction of reviews mentioning AI)

        individual_factor falls back to 0.5 (neutral) when no reviews are available.
        """
        total = job_analysis.total_ai_jobs
        senior = job_analysis.senior_ai_jobs

        leadership_ratio = senior / total if total > 0 else 0.5
        team_size_factor = min(1.0, 1.0 / (total ** 0.5 + 0.1))
        skill_concentration = max(0.0, 1.0 - len(job_analysis.unique_skills) / 15)

        if glassdoor_reviews:
            ai_mentions, total_reviews = self.count_ai_mentions(glassdoor_reviews)
            individual_factor = ai_mentions / total_reviews if total_reviews > 0 else 0.5
        else:
            individual_factor = 0.5

        tc = (
            0.4 * leadership_ratio
            + 0.3 * team_size_factor
            + 0.2 * skill_concentration
            + 0.1 * individual_factor
        )

        return Decimal(str(max(0.0, min(1.0, tc)))).quantize(Decimal("0.0001"))
