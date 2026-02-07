# Stage 5: Schedule-to-Observation Matching - Implementation Plan

## Current-State Findings

### Data Sources
- **Static schedules** (`stop_times` table): Composite PK `(trip_id, stop_id, stop_sequence)`, with `sched_arrival_sec` (seconds from midnight, supports >86400 for overnight trips).
- **RT observations** (`rt_trip_updates` table): Raw GTFS-RT trip updates with `trip_id`, `stop_id`, `stop_sequence`, `arrival_delay`, `arrival_time` (unix epoch), `feed_timestamp`. Dedup constraint on `(trip_id, stop_id, feed_timestamp)`.
- **Existing `rt_observations`** table: Already has `delay_sec` but is a simpler model without `service_date`, `match_status`, or `match_confidence`. Stage 5 creates a richer `matched_arrivals` table.

### Migration Chain
`001 -> 151e83983aaf -> 002 -> 003`

### Architecture Patterns
- Async SQLAlchemy + asyncpg, raw SQL for bulk ops
- Pydantic v2 BaseSettings for config, `@lru_cache` singleton
- structlog for logging
- Admin router pattern for trigger endpoints

---

## Schema Changes

### New Table: `matched_arrivals`

| Column | Type | Constraints |
|--------|------|-------------|
| id | Integer | PK, autoincrement |
| trip_id | String(128) | NOT NULL |
| stop_id | String(64) | NOT NULL |
| stop_sequence | Integer | NOT NULL |
| service_date | Date | NOT NULL |
| scheduled_ts | DateTime(tz) | NOT NULL |
| observed_ts | DateTime(tz) | NOT NULL |
| delay_sec | Integer | NOT NULL |
| match_status | String(16) | NOT NULL (matched/unmatched/ambiguous) |
| match_confidence | Float | NOT NULL, default=1.0 |
| source_feed_ts | DateTime(tz) | NOT NULL |
| rt_trip_update_id | Integer | nullable (FK optional) |
| created_at | DateTime(tz) | NOT NULL, server_default=now() |

**Indexes:**
- `ix_matched_trip_stop_date (trip_id, stop_id, service_date)` - primary lookup
- `ix_matched_stop_observed (stop_id, observed_ts)` - scoring queries
- `ix_matched_date_trip (service_date, trip_id)` - date-range queries
- `uq_matched_arrival_key (trip_id, stop_id, stop_sequence, service_date)` UNIQUE - idempotency

**Idempotency key:** `(trip_id, stop_id, stop_sequence, service_date)` - one matched result per scheduled stop per service day.

---

## Matching Algorithm

### Input
- Unmatched `rt_trip_updates` rows (those with `schedule_relationship = 'SCHEDULED'` and having `arrival_time` or `arrival_delay`)
- Joined against `stop_times` on `(trip_id, stop_id)`, with `stop_sequence` as tiebreaker

### Steps

1. **Fetch RT candidates**: Query `rt_trip_updates` not yet matched, within configurable window
2. **Deduplicate RT updates**: For each `(trip_id, stop_id, stop_sequence)`, keep the latest `feed_timestamp` (most recent update wins)
3. **Join to schedule**: Match `rt_trip_updates.(trip_id, stop_id)` to `stop_times.(trip_id, stop_id)`
4. **Resolve stop_sequence**: If `stop_sequence` available, use exact match. Otherwise match on `(trip_id, stop_id)` and pick the candidate with lowest `stop_sequence` (deterministic tiebreaker)
5. **Compute service_date**: Derive from `feed_timestamp`, handling midnight rollover (if `sched_arrival_sec >= 86400`, the service_date is the previous calendar day)
6. **Compute scheduled_ts**: `service_date midnight (UTC) + sched_arrival_sec`
7. **Compute observed_ts**: Use `arrival_time` (unix epoch) if available, else `feed_timestamp + arrival_delay` offset from scheduled
8. **Compute delay_sec**: `observed_ts - scheduled_ts` in seconds
9. **Classify match_status**: `matched` if single candidate, `ambiguous` if multiple candidates, `unmatched` if no schedule match
10. **Persist**: INSERT ... ON CONFLICT (idempotency key) DO UPDATE with latest values

### Edge Cases

| Case | Handling |
|------|----------|
| GTFS times >24:00:00 | `sched_arrival_sec` already stored as int >86400; service_date adjusted to prior day |
| Missing stop_id/trip_id | Skip with warning, increment error_count |
| Duplicate RT updates | Dedup: keep latest feed_timestamp per (trip_id, stop_id, stop_sequence) |
| Out-of-order updates | Latest feed_timestamp wins during dedup |
| Multiple schedule candidates | Mark as ambiguous; use lowest stop_sequence as tiebreaker |
| Midnight boundary | Service date derived from feed_timestamp with overnight rollback |
| Canceled trips | `schedule_relationship != 'SCHEDULED'` are skipped |

---

## Config Knobs

| Variable | Default | Description |
|----------|---------|-------------|
| MATCH_WINDOW_MINUTES | 90 | How far back to scan RT updates |
| MATCH_MAX_CANDIDATES | 5 | Max schedule candidates per RT update |
| MATCH_BATCH_SIZE | 1000 | DB batch size for inserts |
| MATCH_STRICT_MODE | false | If true, reject ambiguous matches |

---

## Trigger Interface

### Admin Endpoint
`POST /admin/matching/run` -> `MatchingRunResponse`

### Response Schema
```json
{
  "run_id": "uuid",
  "started_at": "iso",
  "ended_at": "iso",
  "duration_ms": 1234,
  "scanned_count": 5000,
  "matched_count": 4200,
  "unmatched_count": 600,
  "ambiguous_count": 100,
  "deduped_count": 80,
  "error_count": 20
}
```

---

## Test Plan

1. **Unit tests** (`test_matching_engine.py`):
   - `test_compute_service_date_normal` / `_overnight`
   - `test_compute_scheduled_ts`
   - `test_compute_delay_sec`
   - `test_dedup_rt_updates`
   - `test_match_exact_stop_sequence`
   - `test_match_tiebreak_lowest_sequence`
   - `test_skip_canceled_trips`
   - `test_skip_missing_keys`
   - `test_24xx_time_handling`
   - `test_midnight_boundary`

2. **Integration tests** (`test_matching_integration.py`):
   - Seed schedules + RT updates, run matching, verify persisted rows
   - Verify delay_sec correctness
   - Verify unmatched/ambiguous rows
   - Verify idempotent rerun (no duplicate rows)

3. **Endpoint tests** (`test_matching_api.py`):
   - `POST /admin/matching/run` success path
   - Summary response schema validation
   - Error handling

4. **Full suite**: All existing tests pass without regression
