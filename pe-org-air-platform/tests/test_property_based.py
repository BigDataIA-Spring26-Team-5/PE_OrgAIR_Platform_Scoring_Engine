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
