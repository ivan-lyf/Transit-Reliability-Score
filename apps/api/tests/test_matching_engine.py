"""Unit tests for Stage 5 matching engine core logic."""

from datetime import date, datetime, timedelta, timezone

import pytest

from transit_api.services.matching.engine import (
    MatchingReport,
    _classify_match,
    compute_delay_sec,
    compute_observed_ts,
    compute_scheduled_ts,
    compute_service_date,
    dedup_rt_updates,
)


class TestComputeServiceDate:
    """Tests for service date derivation."""

    def test_normal_daytime(self) -> None:
        """Normal daytime trip: service_date == feed_ts date."""
        feed_ts = datetime(2026, 2, 6, 14, 30, 0, tzinfo=timezone.utc)
        sched_sec = 52200  # 14:30:00
        result = compute_service_date(feed_ts, sched_sec)
        assert result == date(2026, 2, 6)

    def test_overnight_trip_after_midnight(self) -> None:
        """Overnight trip (sched_arrival_sec >= 86400): service_date is previous day."""
        # Trip scheduled for 25:30:00 = 1:30 AM next day
        feed_ts = datetime(2026, 2, 7, 1, 30, 0, tzinfo=timezone.utc)
        sched_sec = 91800  # 25:30:00
        result = compute_service_date(feed_ts, sched_sec)
        assert result == date(2026, 2, 6)

    def test_exactly_midnight(self) -> None:
        """Arrival at exactly midnight (86400 seconds)."""
        feed_ts = datetime(2026, 2, 7, 0, 0, 0, tzinfo=timezone.utc)
        sched_sec = 86400  # 24:00:00
        result = compute_service_date(feed_ts, sched_sec)
        assert result == date(2026, 2, 6)

    def test_just_before_midnight(self) -> None:
        """Arrival just before midnight: normal service date."""
        feed_ts = datetime(2026, 2, 6, 23, 59, 59, tzinfo=timezone.utc)
        sched_sec = 86399  # 23:59:59
        result = compute_service_date(feed_ts, sched_sec)
        assert result == date(2026, 2, 6)

    def test_very_late_night_26xx(self) -> None:
        """Trip at 26:30:00 (2:30 AM next day)."""
        feed_ts = datetime(2026, 2, 7, 2, 30, 0, tzinfo=timezone.utc)
        sched_sec = 95400  # 26:30:00
        result = compute_service_date(feed_ts, sched_sec)
        assert result == date(2026, 2, 6)


class TestComputeScheduledTs:
    """Tests for scheduled timestamp computation."""

    def test_normal_time(self) -> None:
        """8:30:00 on 2026-02-06."""
        svc_date = date(2026, 2, 6)
        sched_sec = 30600  # 8:30:00
        result = compute_scheduled_ts(svc_date, sched_sec)
        expected = datetime(2026, 2, 6, 8, 30, 0, tzinfo=timezone.utc)
        assert result == expected

    def test_overnight_time(self) -> None:
        """25:30:00 on service date 2026-02-06 -> 2026-02-07 01:30 UTC."""
        svc_date = date(2026, 2, 6)
        sched_sec = 91800  # 25:30:00
        result = compute_scheduled_ts(svc_date, sched_sec)
        expected = datetime(2026, 2, 7, 1, 30, 0, tzinfo=timezone.utc)
        assert result == expected

    def test_midnight_exact(self) -> None:
        """24:00:00 on service date 2026-02-06 -> 2026-02-07 00:00 UTC."""
        svc_date = date(2026, 2, 6)
        sched_sec = 86400
        result = compute_scheduled_ts(svc_date, sched_sec)
        expected = datetime(2026, 2, 7, 0, 0, 0, tzinfo=timezone.utc)
        assert result == expected


class TestComputeObservedTs:
    """Tests for observed timestamp computation."""

    def test_arrival_time_epoch_preferred(self) -> None:
        """arrival_time (unix epoch) takes priority."""
        epoch = 1738800000  # some unix epoch
        expected = datetime.fromtimestamp(epoch, tz=timezone.utc)
        result = compute_observed_ts(
            arrival_time_epoch=epoch,
            arrival_delay=60,
            scheduled_ts=datetime(2026, 2, 6, 8, 0, 0, tzinfo=timezone.utc),
            feed_ts=datetime(2026, 2, 6, 8, 1, 0, tzinfo=timezone.utc),
        )
        assert result == expected

    def test_arrival_delay_fallback(self) -> None:
        """If arrival_time is None, use scheduled_ts + arrival_delay."""
        scheduled_ts = datetime(2026, 2, 6, 8, 0, 0, tzinfo=timezone.utc)
        result = compute_observed_ts(
            arrival_time_epoch=None,
            arrival_delay=120,
            scheduled_ts=scheduled_ts,
            feed_ts=datetime(2026, 2, 6, 8, 5, 0, tzinfo=timezone.utc),
        )
        assert result == scheduled_ts + timedelta(seconds=120)

    def test_arrival_delay_negative(self) -> None:
        """Negative delay (early arrival)."""
        scheduled_ts = datetime(2026, 2, 6, 8, 0, 0, tzinfo=timezone.utc)
        result = compute_observed_ts(
            arrival_time_epoch=None,
            arrival_delay=-30,
            scheduled_ts=scheduled_ts,
            feed_ts=datetime(2026, 2, 6, 7, 59, 0, tzinfo=timezone.utc),
        )
        assert result == scheduled_ts - timedelta(seconds=30)

    def test_feed_ts_last_resort(self) -> None:
        """If both arrival_time and arrival_delay are None, use feed_ts."""
        feed_ts = datetime(2026, 2, 6, 8, 5, 0, tzinfo=timezone.utc)
        result = compute_observed_ts(
            arrival_time_epoch=None,
            arrival_delay=None,
            scheduled_ts=datetime(2026, 2, 6, 8, 0, 0, tzinfo=timezone.utc),
            feed_ts=feed_ts,
        )
        assert result == feed_ts

    def test_arrival_time_zero_treated_as_missing(self) -> None:
        """arrival_time=0 should be treated as missing."""
        scheduled_ts = datetime(2026, 2, 6, 8, 0, 0, tzinfo=timezone.utc)
        result = compute_observed_ts(
            arrival_time_epoch=0,
            arrival_delay=60,
            scheduled_ts=scheduled_ts,
            feed_ts=datetime(2026, 2, 6, 8, 5, 0, tzinfo=timezone.utc),
        )
        assert result == scheduled_ts + timedelta(seconds=60)


class TestComputeDelaySec:
    """Tests for delay computation."""

    def test_positive_delay(self) -> None:
        """Late arrival: positive delay_sec."""
        observed = datetime(2026, 2, 6, 8, 5, 0, tzinfo=timezone.utc)
        scheduled = datetime(2026, 2, 6, 8, 0, 0, tzinfo=timezone.utc)
        assert compute_delay_sec(observed, scheduled) == 300

    def test_negative_delay(self) -> None:
        """Early arrival: negative delay_sec."""
        observed = datetime(2026, 2, 6, 7, 58, 0, tzinfo=timezone.utc)
        scheduled = datetime(2026, 2, 6, 8, 0, 0, tzinfo=timezone.utc)
        assert compute_delay_sec(observed, scheduled) == -120

    def test_on_time(self) -> None:
        """Exactly on time: zero delay."""
        ts = datetime(2026, 2, 6, 8, 0, 0, tzinfo=timezone.utc)
        assert compute_delay_sec(ts, ts) == 0


class TestDedupRtUpdates:
    """Tests for RT update deduplication."""

    def test_no_duplicates(self) -> None:
        """No duplicates: all rows preserved."""
        rows = [
            {
                "trip_id": "T1",
                "stop_id": "S1",
                "stop_sequence": 1,
                "feed_timestamp": datetime(2026, 2, 6, 8, 0, tzinfo=timezone.utc),
            },
            {
                "trip_id": "T1",
                "stop_id": "S2",
                "stop_sequence": 2,
                "feed_timestamp": datetime(2026, 2, 6, 8, 0, tzinfo=timezone.utc),
            },
        ]
        deduped, count = dedup_rt_updates(rows)
        assert len(deduped) == 2
        assert count == 0

    def test_keeps_latest_feed_timestamp(self) -> None:
        """Duplicate key: keep the row with latest feed_timestamp."""
        rows = [
            {
                "trip_id": "T1",
                "stop_id": "S1",
                "stop_sequence": 1,
                "feed_timestamp": datetime(2026, 2, 6, 8, 0, tzinfo=timezone.utc),
                "value": "old",
            },
            {
                "trip_id": "T1",
                "stop_id": "S1",
                "stop_sequence": 1,
                "feed_timestamp": datetime(2026, 2, 6, 8, 5, tzinfo=timezone.utc),
                "value": "new",
            },
        ]
        deduped, count = dedup_rt_updates(rows)
        assert len(deduped) == 1
        assert count == 1
        assert deduped[0]["value"] == "new"

    def test_three_duplicates(self) -> None:
        """Three entries for same key: only latest survives."""
        ts1 = datetime(2026, 2, 6, 8, 0, tzinfo=timezone.utc)
        ts2 = datetime(2026, 2, 6, 8, 5, tzinfo=timezone.utc)
        ts3 = datetime(2026, 2, 6, 8, 10, tzinfo=timezone.utc)
        rows = [
            {
                "trip_id": "T1",
                "stop_id": "S1",
                "stop_sequence": 1,
                "feed_timestamp": ts1,
                "value": "first",
            },
            {
                "trip_id": "T1",
                "stop_id": "S1",
                "stop_sequence": 1,
                "feed_timestamp": ts3,
                "value": "third",
            },
            {
                "trip_id": "T1",
                "stop_id": "S1",
                "stop_sequence": 1,
                "feed_timestamp": ts2,
                "value": "second",
            },
        ]
        deduped, count = dedup_rt_updates(rows)
        assert len(deduped) == 1
        assert count == 2
        assert deduped[0]["value"] == "third"

    def test_different_stop_sequences_not_deduped(self) -> None:
        """Different stop_sequence values are different keys."""
        ts = datetime(2026, 2, 6, 8, 0, tzinfo=timezone.utc)
        rows = [
            {"trip_id": "T1", "stop_id": "S1", "stop_sequence": 1, "feed_timestamp": ts},
            {"trip_id": "T1", "stop_id": "S1", "stop_sequence": 2, "feed_timestamp": ts},
        ]
        deduped, count = dedup_rt_updates(rows)
        assert len(deduped) == 2
        assert count == 0

    def test_empty_input(self) -> None:
        """Empty input returns empty result."""
        deduped, count = dedup_rt_updates([])
        assert len(deduped) == 0
        assert count == 0


class TestClassifyMatch:
    """Tests for match classification."""

    def test_zero_candidates_unmatched(self) -> None:
        status, confidence = _classify_match(0, False)
        assert status == "unmatched"
        assert confidence == 0.0

    def test_one_candidate_matched(self) -> None:
        status, confidence = _classify_match(1, False)
        assert status == "matched"
        assert confidence == 1.0

    def test_multiple_candidates_ambiguous(self) -> None:
        status, confidence = _classify_match(3, False)
        assert status == "ambiguous"
        assert confidence == pytest.approx(1.0 / 3, abs=0.001)

    def test_strict_mode_rejects_ambiguous(self) -> None:
        status, confidence = _classify_match(3, True)
        assert status == "unmatched"
        assert confidence == 0.0

    def test_strict_mode_allows_single(self) -> None:
        status, confidence = _classify_match(1, True)
        assert status == "matched"
        assert confidence == 1.0


class TestMatchingReport:
    """Tests for the MatchingReport dataclass."""

    def test_default_values(self) -> None:
        report = MatchingReport()
        assert report.scanned_count == 0
        assert report.matched_count == 0
        assert report.run_id  # non-empty UUID

    def test_to_dict(self) -> None:
        report = MatchingReport(scanned_count=10, matched_count=8)
        d = report.to_dict()
        assert d["scanned_count"] == 10
        assert d["matched_count"] == 8
        assert "run_id" in d
