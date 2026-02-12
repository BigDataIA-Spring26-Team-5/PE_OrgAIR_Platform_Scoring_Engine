# app/scoring/talent_concentration.py
from dataclasses import dataclass
from decimal import Decimal
from typing import List, Set

@dataclass
class JobAnalysis:
    """Analysis of job postings for talent concentration."""
    total_ai_jobs: int
    senior_ai_jobs: int # Principal, Staff, Director, VP level
    mid_ai_jobs: int # Senior, Lead level
    entry_ai_jobs: int # Junior, Associate, entry level
    unique_skills: Set[str] # Distinct skills required

class TalentConcentrationCalculator:
    def __init__(self):
        pass

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

    def calculate_tc(
        self,
        job_analysis: JobAnalysis,
        glassdoor_individual_mentions: int,
        glassdoor_review_count: int,
    ) -> Decimal:
        """Calculate Talent Concentration (TC) score as a Decimal in [0, 1]."""
        total = job_analysis.total_ai_jobs
        senior = job_analysis.senior_ai_jobs

        leadership_ratio = senior / total if total > 0 else 0.5
        team_size_factor = min(1.0, 1.0 / (total ** 0.5 + 0.1))
        skill_concentration = max(0.0, 1.0 - len(job_analysis.unique_skills) / 15)
        individual_factor = (
            min(1.0, glassdoor_individual_mentions / glassdoor_review_count)
            if glassdoor_review_count > 0
            else 0.5
        )

        tc = (
            0.4 * leadership_ratio
            + 0.3 * team_size_factor
            + 0.2 * skill_concentration
            + 0.1 * individual_factor
        )

        return Decimal(str(max(0.0, min(1.0, tc)))).quantize(Decimal("0.0001"))
