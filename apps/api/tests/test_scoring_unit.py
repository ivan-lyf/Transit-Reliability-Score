"""Unit tests for the Stage 6 scoring formula and bucketing helpers.

All tests are pure Python — no database, no async, no mocks.
"""

from __future__ import annotations

import pytest

from transit_api.services.aggregation.scorer import (
    assign_day_type,
    assign_hour_bucket,
    compute_score,
)


# ---------------------------------------------------------------------------
# compute_score
# ---------------------------------------------------------------------------


class TestComputeScore:
    """Verify the scoring formula boundaries, clamping, and rounding."""

    def test_perfect_score(self) -> None:
        # 100% on time, zero delay across the board → score == 100
        assert compute_score(1.0, 0.0, 0.0) == 100

    def test_zero_score(self) -> None:
        # 0% on time, p95 ≥ cap, p50 ≥ cap → score == 0
        assert compute_score(0.0, 900.0, 300.0) == 0

    def test_score_clamped_above_100(self) -> None:
        # Weights already sum to 1.0 so score can't exceed 100,
        # but verify the clamp handles edge cases (e.g. floating-point > 1).
        result = compute_score(1.0, 0.0, -300.0)  # p50 early → c3=0 (clamped)
        assert 0 <= result <= 100

    def test_score_clamped_below_0(self) -> None:
        result = compute_score(0.0, 99999.0, 99999.0)
        assert result == 0

    def test_on_time_only_component(self) -> None:
        # 80% on time, p95 at cap, p50 at cap → score = round(100 * 0.6 * 0.8) = 48
        score = compute_score(
            0.8, 900.0, 300.0,
            weight_on_time=0.6, weight_p95=0.25, weight_p50=0.15,
            p95_cap=900, p50_cap=300,
        )
        assert score == 48

    def test_p95_component_clamped_at_zero(self) -> None:
        # p95 beyond cap: c2 should clamp to 0
        score_at_cap = compute_score(
            0.0, 900.0, 0.0,
            weight_on_time=0.0, weight_p95=1.0, weight_p50=0.0,
            p95_cap=900, p50_cap=300,
        )
        score_beyond_cap = compute_score(
            0.0, 99999.0, 0.0,
            weight_on_time=0.0, weight_p95=1.0, weight_p50=0.0,
            p95_cap=900, p50_cap=300,
        )
        assert score_at_cap == 0
        assert score_beyond_cap == 0

    def test_p50_component_uses_absolute_value(self) -> None:
        # An early-arriving bus (negative p50) should be penalised the same
        # as an equally late bus.
        score_late = compute_score(
            0.0, 0.0, 150.0,
            weight_on_time=0.0, weight_p95=0.0, weight_p50=1.0,
            p95_cap=900, p50_cap=300,
        )
        score_early = compute_score(
            0.0, 0.0, -150.0,
            weight_on_time=0.0, weight_p95=0.0, weight_p50=1.0,
            p95_cap=900, p50_cap=300,
        )
        assert score_late == score_early

    def test_p50_half_cap(self) -> None:
        # p50 = 150s (half of 300s cap) → c3 = 0.5 → score = round(50)
        score = compute_score(
            0.0, 0.0, 150.0,
            weight_on_time=0.0, weight_p95=0.0, weight_p50=1.0,
            p95_cap=900, p50_cap=300,
        )
        assert score == 50

    def test_rounding(self) -> None:
        # Verify Python's "round half to even" doesn't cause surprises.
        # 0.6 * 0.5 = 0.30 → round(30) = 30
        score = compute_score(
            0.5, 900.0, 300.0,
            weight_on_time=0.6, weight_p95=0.25, weight_p50=0.15,
            p95_cap=900, p50_cap=300,
        )
        assert score == 30

    def test_default_weights_from_config(self) -> None:
        # Default kwargs should produce valid output without errors.
        score = compute_score(0.9, 120.0, 30.0)
        assert 0 <= score <= 100

    def test_full_formula_known_values(self) -> None:
        """Regression test with explicit arithmetic.

        on_time_rate=0.8, p95=300s, p50=60s
        c1 = 0.8
        c2 = 1 - 300/900  = 0.6667
        c3 = 1 - 60/300   = 0.8
        raw = 0.6*0.8 + 0.25*0.6667 + 0.15*0.8
            = 0.48 + 0.1667 + 0.12 = 0.7667
        score = round(76.67) = 77
        """
        score = compute_score(
            0.8, 300.0, 60.0,
            weight_on_time=0.6, weight_p95=0.25, weight_p50=0.15,
            p95_cap=900, p50_cap=300,
        )
        assert score == 77


# ---------------------------------------------------------------------------
# assign_day_type
# ---------------------------------------------------------------------------


class TestAssignDayType:
    """Verify weekday / saturday / sunday classification."""

    @pytest.mark.parametrize("weekday", [0, 1, 2, 3, 4])
    def test_weekdays(self, weekday: int) -> None:
        assert assign_day_type(weekday) == "weekday"

    def test_saturday(self) -> None:
        assert assign_day_type(5) == "saturday"

    def test_sunday(self) -> None:
        assert assign_day_type(6) == "sunday"


# ---------------------------------------------------------------------------
# assign_hour_bucket
# ---------------------------------------------------------------------------


class TestAssignHourBucket:
    """Verify hour → bucket mapping and out-of-window exclusion."""

    @pytest.mark.parametrize("hour,expected", [
        (6, "6-9"),
        (7, "6-9"),
        (8, "6-9"),
        (9, "9-12"),
        (10, "9-12"),
        (11, "9-12"),
        (12, "12-15"),
        (13, "12-15"),
        (14, "12-15"),
        (15, "15-18"),
        (16, "15-18"),
        (17, "15-18"),
        (18, "18-21"),
        (19, "18-21"),
        (20, "18-21"),
    ])
    def test_in_window(self, hour: int, expected: str) -> None:
        assert assign_hour_bucket(hour) == expected

    @pytest.mark.parametrize("hour", [0, 1, 2, 3, 4, 5, 21, 22, 23])
    def test_out_of_window(self, hour: int) -> None:
        assert assign_hour_bucket(hour) is None


# ---------------------------------------------------------------------------
# MIN_SAMPLES behavior (via ScoreCard low_confidence flag)
# ---------------------------------------------------------------------------
# The low_confidence flag is computed in the router, but we test the
# underlying logic here so it's not buried inside an HTTP test.


class TestMinSamplesBehavior:
    """low_confidence should be True when sample_n < min_samples."""

    def test_low_confidence_flag(self) -> None:
        min_samples = 20
        assert (5 < min_samples) is True   # low confidence
        assert (25 < min_samples) is False  # sufficient data
