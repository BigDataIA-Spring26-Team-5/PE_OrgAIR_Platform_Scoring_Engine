# tests/test_property_based.py
"""
Property-Based Tests — Task 5.3 (CS3)

8 Hypothesis tests with max_examples=500, covering:
  - 5 VRCalculator properties
  - 3 EvidenceMapper properties
"""

from decimal import Decimal

import pytest
from hypothesis import assume, given, settings
from hypothesis import strategies as st

from app.scoring.evidence_mapper import (
    Dimension,
    DimensionScore,
    EvidenceMapper,
    EvidenceScore,
    SignalSource,
)
from app.scoring.vr_calculator import VRCalculator, VRResult
from app.scoring.synergy_calculator import SynergyCalculator
from app.scoring.confidence_calculator import ConfidenceCalculator
from app.scoring.orgair_calculator import OrgAIRCalculator

# ---------------------------------------------------------------------------
# Shared strategies
# ---------------------------------------------------------------------------

ALL_DIMS = [d.value for d in Dimension]  # 7 strings


dim_score_st = st.floats(
    min_value=0.0, max_value=100.0, allow_nan=False, allow_infinity=False
)
tc_st = st.floats(
    min_value=0.0, max_value=1.0, allow_nan=False, allow_infinity=False
)


@st.composite
def dim_scores_dict(draw):
    """Draw a dict mapping each dimension name → float score in [0, 100]."""
    return {d: draw(dim_score_st) for d in ALL_DIMS}


@st.composite
def evidence_score_st(draw):
    """Draw a random EvidenceScore with valid fields."""
    return EvidenceScore(
        source=draw(st.sampled_from(list(SignalSource))),
        raw_score=Decimal(
            str(
                draw(
                    st.floats(
                        min_value=0.0,
                        max_value=100.0,
                        allow_nan=False,
                        allow_infinity=False,
                    )
                )
            )
        ),
        confidence=Decimal(
            str(
                draw(
                    st.floats(
                        min_value=0.01,
                        max_value=1.0,
                        allow_nan=False,
                        allow_infinity=False,
                    )
                )
            )
        ),
        evidence_count=draw(st.integers(min_value=1, max_value=100)),
    )


# ---------------------------------------------------------------------------
# VR Property Tests
# ---------------------------------------------------------------------------


class TestVRPropertyBased:
    """5 property tests for VRCalculator."""

    @given(dim_scores_dict(), tc_st)
    @settings(max_examples=500)
    def test_vr_always_bounded(self, scores, tc):
        """V^R score is always in [0, 100] for any valid inputs."""
        result = VRCalculator().calculate(scores, tc)
        assert Decimal("0") <= result.vr_score <= Decimal("100")

    @given(
        dim_scores_dict(),
        tc_st,
        st.floats(
            min_value=1.0, max_value=50.0, allow_nan=False, allow_infinity=False
        ),
    )
    @settings(max_examples=500)
    def test_higher_scores_increase_vr(self, scores, tc, delta):
        """Boosting every dimension score by delta (capped at 100) does not decrease VR."""
        calc = VRCalculator()
        result_original = calc.calculate(scores, tc)
        boosted = {k: min(100.0, v + delta) for k, v in scores.items()}
        result_boosted = calc.calculate(boosted, tc)
        assert result_boosted.vr_score >= result_original.vr_score

    @given(
        dim_scores_dict(),
        st.floats(
            min_value=0.0, max_value=0.5, allow_nan=False, allow_infinity=False
        ),
        st.floats(
            min_value=0.5, max_value=1.0, allow_nan=False, allow_infinity=False
        ),
    )
    @settings(max_examples=500)
    def test_talent_concentration_penalty(self, scores, low_tc, high_tc):
        """Higher TC value produces a lower or equal VR score (penalty is monotone)."""
        calc = VRCalculator()
        result_low = calc.calculate(scores, low_tc)
        result_high = calc.calculate(scores, high_tc)
        assert result_high.vr_score <= result_low.vr_score

    @given(
        st.floats(
            min_value=0.0, max_value=100.0, allow_nan=False, allow_infinity=False
        ),
        tc_st,
    )
    @settings(max_examples=500)
    def test_uniform_dimensions_no_cv_penalty(self, uniform_val, tc):
        """When all dimension scores are equal, CV = 0 so cv_penalty >= 0.99."""
        scores = {d: uniform_val for d in ALL_DIMS}
        result = VRCalculator().calculate(scores, tc)
        assert result.cv_penalty >= Decimal("0.99")

    @given(dim_scores_dict(), tc_st)
    @settings(max_examples=500)
    def test_deterministic(self, scores, tc):
        """Calling calculate() twice with identical args yields the same vr_score."""
        calc = VRCalculator()
        result1 = calc.calculate(scores, tc)
        result2 = calc.calculate(scores, tc)
        assert result1.vr_score == result2.vr_score


# ---------------------------------------------------------------------------
# Evidence Mapper Property Tests
# ---------------------------------------------------------------------------


class TestEvidenceMapperPropertyBased:
    """3 property tests for EvidenceMapper."""

    @given(st.lists(evidence_score_st(), min_size=1, max_size=9))
    @settings(max_examples=500)
    def test_all_dimensions_returned(self, raw_evidence):
        """map_evidence_to_dimensions always returns exactly 7 keys, one per Dimension."""
        # Deduplicate by source so each SignalSource appears at most once
        seen: set = set()
        evidence = []
        for ev in raw_evidence:
            if ev.source not in seen:
                seen.add(ev.source)
                evidence.append(ev)

        mapper = EvidenceMapper()
        result = mapper.map_evidence_to_dimensions(evidence)

        assert len(result) == 7
        assert set(result.keys()) == set(Dimension)

    @given(st.just([]))
    @settings(max_examples=500)
    def test_missing_evidence_defaults_to_50(self, empty_list):
        """With no evidence, every dimension score defaults to 50.0."""
        mapper = EvidenceMapper()
        result = mapper.map_evidence_to_dimensions(empty_list)
        for dim, ds in result.items():
            assert ds.score == Decimal("50.0"), (
                f"{dim.value} expected Decimal('50.0'), got {ds.score!r}"
            )

    @given(
        st.lists(
            st.sampled_from(list(SignalSource)),
            min_size=1,
            max_size=2,
            unique=True,
        ),
        st.lists(
            st.sampled_from(list(SignalSource)),
            min_size=5,
            max_size=9,
            unique=True,
        ),
    )
    @settings(max_examples=500)
    def test_more_evidence_higher_confidence(self, small_sources, large_sources):
        """5-9 unique sources yield average dimension confidence >= 1-2 unique sources.

        Uses fixed raw_score=70 and confidence=0.8 to isolate coverage behavior.
        The large set is forced to be a superset of small to guarantee the property.
        """
        # Ensure large is a proper superset of small (coverage can only increase)
        large_set = list({*large_sources, *small_sources})
        assume(len(large_set) > len(small_sources))

        def make_scores(sources):
            return [
                EvidenceScore(
                    source=s,
                    raw_score=Decimal("70"),
                    confidence=Decimal("0.8"),
                    evidence_count=5,
                )
                for s in sources
            ]

        mapper = EvidenceMapper()
        small_result = mapper.map_evidence_to_dimensions(make_scores(small_sources))
        large_result = mapper.map_evidence_to_dimensions(make_scores(large_set))

        avg_small = sum(float(ds.confidence) for ds in small_result.values()) / len(
            small_result
        )
        avg_large = sum(float(ds.confidence) for ds in large_result.values()) / len(
            large_result
        )

        assert avg_large >= avg_small


# ---------------------------------------------------------------------------
# Shared strategies for new calculators
# ---------------------------------------------------------------------------

score_0_100 = st.floats(
    min_value=0.0, max_value=100.0, allow_nan=False, allow_infinity=False
)
alignment_st = st.floats(
    min_value=0.0, max_value=1.0, allow_nan=False, allow_infinity=False
)
timing_st = st.floats(
    min_value=0.5, max_value=1.5, allow_nan=False, allow_infinity=False
)
evidence_count_st = st.integers(min_value=1, max_value=50)


# ---------------------------------------------------------------------------
# Synergy Property Tests
# ---------------------------------------------------------------------------


class TestSynergyPropertyBased:
    """3 property tests for SynergyCalculator."""

    @given(score_0_100, score_0_100, alignment_st, timing_st)
    @settings(max_examples=500)
    def test_synergy_always_bounded(self, vr, hr, alignment, timing):
        """Synergy score is always in [0, 100] for any valid inputs."""
        result = SynergyCalculator().calculate(vr, hr, alignment=alignment, timing_factor=timing)
        assert Decimal("0") <= result.synergy_score <= Decimal("100")

    @given(score_0_100, alignment_st, timing_st)
    @settings(max_examples=500)
    def test_synergy_zero_when_either_is_zero(self, nonzero, alignment, timing):
        """Synergy is 0 when VR=0 or HR=0."""
        calc = SynergyCalculator()
        result_vr_zero = calc.calculate(0.0, nonzero, alignment=alignment, timing_factor=timing)
        result_hr_zero = calc.calculate(nonzero, 0.0, alignment=alignment, timing_factor=timing)
        assert result_vr_zero.synergy_score == Decimal("0")
        assert result_hr_zero.synergy_score == Decimal("0")

    @given(score_0_100, score_0_100, alignment_st, timing_st)
    @settings(max_examples=500)
    def test_synergy_deterministic(self, vr, hr, alignment, timing):
        """Same inputs always produce the same Synergy score."""
        calc = SynergyCalculator()
        r1 = calc.calculate(vr, hr, alignment=alignment, timing_factor=timing)
        r2 = calc.calculate(vr, hr, alignment=alignment, timing_factor=timing)
        assert r1.synergy_score == r2.synergy_score


# ---------------------------------------------------------------------------
# Confidence Interval Property Tests
# ---------------------------------------------------------------------------


class TestConfidencePropertyBased:
    """3 property tests for ConfidenceCalculator."""

    @given(score_0_100, evidence_count_st, st.sampled_from(["vr", "hr", "org_air"]))
    @settings(max_examples=500)
    def test_ci_always_valid_range(self, score, n, score_type):
        """CI bounds are valid: ci_lower <= ci_upper, both within [0, 100]."""
        result = ConfidenceCalculator().calculate(score, n, score_type)
        assert result.ci_lower <= result.ci_upper
        assert Decimal("0") <= result.ci_lower <= Decimal("100")
        assert Decimal("0") <= result.ci_upper <= Decimal("100")

    @given(score_0_100, st.sampled_from(["vr", "hr", "org_air"]))
    @settings(max_examples=500)
    def test_more_evidence_higher_reliability(self, score, score_type):
        """Larger evidence count yields equal or higher reliability (ρ)."""
        calc = ConfidenceCalculator()
        small = calc.calculate(score, 1, score_type)
        large = calc.calculate(score, 20, score_type)
        assert large.reliability >= small.reliability

    @given(score_0_100, evidence_count_st, st.sampled_from(["vr", "hr", "org_air"]))
    @settings(max_examples=500)
    def test_confidence_deterministic(self, score, n, score_type):
        """Same inputs always produce the same CI."""
        calc = ConfidenceCalculator()
        r1 = calc.calculate(score, n, score_type)
        r2 = calc.calculate(score, n, score_type)
        assert r1.ci_lower == r2.ci_lower
        assert r1.ci_upper == r2.ci_upper


# ---------------------------------------------------------------------------
# Org-AI-R Property Tests
# ---------------------------------------------------------------------------


class TestOrgAIRPropertyBased:
    """3 property tests for OrgAIRCalculator."""

    @given(score_0_100, score_0_100, score_0_100)
    @settings(max_examples=500)
    def test_orgair_always_bounded(self, vr, hr, synergy):
        """Org-AI-R score is always in [0, 100] for any valid inputs."""
        result = OrgAIRCalculator().calculate(vr, hr, synergy_score=synergy)
        assert Decimal("0") <= result.org_air_score <= Decimal("100")

    @given(
        st.floats(min_value=0.0, max_value=80.0, allow_nan=False, allow_infinity=False),
        st.floats(min_value=10.0, max_value=20.0, allow_nan=False, allow_infinity=False),
        score_0_100,
        score_0_100,
    )
    @settings(max_examples=500)
    def test_orgair_monotone_with_vr(self, vr_low, delta, hr, synergy):
        """Increasing VR (with HR and synergy fixed) does not decrease Org-AI-R."""
        vr_high = min(100.0, vr_low + delta)
        calc = OrgAIRCalculator()
        result_low = calc.calculate(vr_low, hr, synergy_score=synergy)
        result_high = calc.calculate(vr_high, hr, synergy_score=synergy)
        assert result_high.org_air_score >= result_low.org_air_score

    @given(score_0_100, score_0_100, score_0_100)
    @settings(max_examples=500)
    def test_orgair_deterministic(self, vr, hr, synergy):
        """Same inputs always produce the same Org-AI-R score."""
        calc = OrgAIRCalculator()
        r1 = calc.calculate(vr, hr, synergy_score=synergy)
        r2 = calc.calculate(vr, hr, synergy_score=synergy)
        assert r1.org_air_score == r2.org_air_score
