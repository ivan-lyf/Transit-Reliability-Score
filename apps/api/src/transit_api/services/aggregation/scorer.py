"""Pure-Python scoring formula and bucketing helpers for Stage 6.

All functions here are stateless and free of I/O so they can be unit-tested
without a database or settings object.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Score formula
# ---------------------------------------------------------------------------

# Default weight / cap values match the config defaults.  Pass explicit values
# in tests to avoid depending on the settings singleton.
_DEFAULT_WEIGHT_ON_TIME = 0.6
_DEFAULT_WEIGHT_P95 = 0.25
_DEFAULT_WEIGHT_P50 = 0.15
_DEFAULT_P95_CAP = 900  # seconds (15 min)
_DEFAULT_P50_CAP = 300  # seconds (5 min)


def compute_score(
    on_time_rate: float,
    p95_delay_sec: float,
    p50_delay_sec: float,
    *,
    weight_on_time: float = _DEFAULT_WEIGHT_ON_TIME,
    weight_p95: float = _DEFAULT_WEIGHT_P95,
    weight_p50: float = _DEFAULT_WEIGHT_P50,
    p95_cap: float = _DEFAULT_P95_CAP,
    p50_cap: float = _DEFAULT_P50_CAP,
) -> int:
    """Compute a reliability score in [0, 100].

    Formula
    -------
    c1 = on_time_rate                                 (0–1)
    c2 = clamp(1 - p95_delay_sec / p95_cap, 0, 1)
    c3 = clamp(1 - |p50_delay_sec| / p50_cap, 0, 1)
    raw = weight_on_time * c1 + weight_p95 * c2 + weight_p50 * c3
    score = round(100 * raw), clamped to [0, 100]

    Args:
        on_time_rate: Fraction of arrivals within ±on_time_threshold_sec.
        p95_delay_sec: 95th-percentile delay in seconds (positive = late).
        p50_delay_sec: Median delay in seconds (may be negative for early).
        weight_on_time: Weight for on-time-rate component.
        weight_p95: Weight for p95 component.
        weight_p50: Weight for p50 component.
        p95_cap: Delay at which the p95 component collapses to 0 (seconds).
        p50_cap: |delay| at which the p50 component collapses to 0 (seconds).

    Returns:
        Integer score in [0, 100].
    """
    c1 = float(on_time_rate)
    c2 = max(0.0, min(1.0, 1.0 - float(p95_delay_sec) / p95_cap))
    c3 = max(0.0, min(1.0, 1.0 - abs(float(p50_delay_sec)) / p50_cap))
    raw = weight_on_time * c1 + weight_p95 * c2 + weight_p50 * c3
    return max(0, min(100, round(100.0 * raw)))


# ---------------------------------------------------------------------------
# Day-type bucketing
# ---------------------------------------------------------------------------

_DOW_TO_DAY_TYPE: dict[int, str] = {
    0: "weekday",  # Monday
    1: "weekday",  # Tuesday
    2: "weekday",  # Wednesday
    3: "weekday",  # Thursday
    4: "weekday",  # Friday
    5: "saturday",
    6: "sunday",
}


def assign_day_type(weekday: int) -> str:
    """Map Python weekday integer (0=Mon … 6=Sun) to day_type label.

    Returns one of 'weekday', 'saturday', 'sunday'.
    """
    return _DOW_TO_DAY_TYPE[weekday]


# ---------------------------------------------------------------------------
# Hour-bucket bucketing
# ---------------------------------------------------------------------------

_HOUR_BUCKETS: list[tuple[int, int, str]] = [
    (6, 8, "6-9"),
    (9, 11, "9-12"),
    (12, 14, "12-15"),
    (15, 17, "15-18"),
    (18, 20, "18-21"),
]


def assign_hour_bucket(hour: int) -> str | None:
    """Map an hour-of-day (0–23) to an hour_bucket label.

    Returns None for hours outside the five service windows (before 6 AM or
    after 8 PM), which are excluded from aggregation.
    """
    for lo, hi, label in _HOUR_BUCKETS:
        if lo <= hour <= hi:
            return label
    return None
