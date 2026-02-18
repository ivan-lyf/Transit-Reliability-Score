# Transit Reliability Score API

FastAPI backend for the Transit Reliability Score application.

## Setup

Requires Python 3.12+.

```bash
# Create virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -e ".[dev]"
```

## Running

```bash
# Development
uvicorn transit_api.main:app --reload --host 0.0.0.0 --port 8000

# Production
uvicorn transit_api.main:app --host 0.0.0.0 --port 8000
```

## Static GTFS Import (Stage 3)

Environment variables:
- `STATIC_GTFS_URL` (alias: `GTFS_STATIC_URL`)
- `IMPORT_BATCH_SIZE` (default: 1000)
- `GTFS_IMPORT_STRICT` (default: false)

Operator commands:
```bash
# Apply migrations (includes Stage 3 import log + stop_times uniqueness)
alembic upgrade head

# Trigger import from remote feed (default source)
curl -X POST http://localhost:8000/admin/import/static-gtfs \
  -H "Content-Type: application/json" \
  -d '{"source_type":"remote","source":"","dry_run":false,"skip_if_unchanged":true}'

# Trigger import from a local ZIP fixture
curl -X POST http://localhost:8000/admin/import/static-gtfs \
  -H "Content-Type: application/json" \
  -d '{"source_type":"local","source":"/absolute/path/to/gtfs.zip","dry_run":true}'
```

## Testing

```bash
# Run tests
pytest

# Run with coverage
pytest --cov=transit_api --cov-report=term-missing
```

## Linting

```bash
# Check
ruff check src tests
ruff format --check src tests

# Fix
ruff check --fix src tests
ruff format src tests
```

## Type Checking

```bash
mypy src
```

## Reliability Scoring (Stage 6)

### Scoring formula

Each `(stop_id, route_id, day_type, hour_bucket)` bucket is assigned a score
in **[0, 100]**:

```
c1  = on_time_rate                         (fraction of arrivals within ±120 s)
c2  = clamp(1 − p95_delay_sec / 900, 0, 1)
c3  = clamp(1 − |p50_delay_sec| / 300, 0, 1)
raw = 0.60 × c1 + 0.25 × c2 + 0.15 × c3
score = round(100 × raw), clamped to [0, 100]
```

Weights and caps are configurable via environment variables
(`WEIGHT_ON_TIME_RATE`, `WEIGHT_P95_COMPONENT`, `WEIGHT_P50_COMPONENT`,
`P95_MAX_DELAY_SEC`, `P50_MAX_DELAY_SEC`).

### Day-type bucketing

| `service_date` DOW (PostgreSQL) | `day_type` |
|---------------------------------|------------|
| 1–5 (Mon–Fri)                   | `weekday`  |
| 6 (Saturday)                    | `saturday` |
| 0 (Sunday)                      | `sunday`   |

### Hour-bucket mapping (service timezone: America/Vancouver)

| Local hours | `hour_bucket` |
|-------------|---------------|
| 06–08       | `6-9`         |
| 09–11       | `9-12`        |
| 12–14       | `12-15`       |
| 15–17       | `15-18`       |
| 18–20       | `18-21`       |

Arrivals outside these windows are excluded from aggregation.

### Environment variables (Stage 6)

| Variable             | Default              | Description                                     |
|----------------------|----------------------|-------------------------------------------------|
| `AGG_LOOKBACK_DAYS`  | `14`                 | Days of `matched_arrivals` to include per run   |
| `MIN_SAMPLES`        | `20`                 | Below this, `low_confidence=true` in responses  |
| `AGG_BATCH_SIZE`     | `1000`               | UPSERT batch size                               |
| `SERVICE_TIMEZONE`   | `America/Vancouver`  | IANA timezone for hour-bucket assignment        |
| `ON_TIME_THRESHOLD_SEC` | `120`           | ±seconds window for on-time classification      |
| `P95_MAX_DELAY_SEC`  | `900`                | Delay cap for p95 component (score → 0 at cap)  |
| `P50_MAX_DELAY_SEC`  | `300`                | |delay| cap for p50 component                   |

### Running the aggregation job

**Via the API (recommended):**
```bash
# Dry run — compute but do not write to score_agg
curl -X POST http://localhost:8000/admin/agg/run \
  -H "Content-Type: application/json" \
  -d '{"dry_run": true}'

# Live run with default lookback (AGG_LOOKBACK_DAYS)
curl -X POST http://localhost:8000/admin/agg/run \
  -H "Content-Type: application/json" \
  -d '{"dry_run": false}'

# Override lookback window
curl -X POST http://localhost:8000/admin/agg/run \
  -H "Content-Type: application/json" \
  -d '{"lookback_days": 7, "dry_run": false}'
```

**Check the last run:**
```bash
curl http://localhost:8000/meta/last-agg
```

**Score endpoints:**
```bash
# Score card for one bucket
curl "http://localhost:8000/scores?stop_id=S1&route_id=R1&day_type=weekday&hour_bucket=9-12"

# Risky stops near a location
curl "http://localhost:8000/scores/nearby-risky?lat=49.28&lon=-123.12&day_type=weekday&hour_bucket=9-12"

# 7-day trend for a stop+route
curl "http://localhost:8000/scores/trend?stop_id=S1&route_id=R1&days=7"
```

### Database migration (Stage 6)

The Stage 6 migration adds the `agg_run_log` table:

```bash
alembic upgrade head
```

### Running integration tests

Integration tests require a live PostgreSQL instance:

```bash
export RUN_INTEGRATION_TESTS=1
export DATABASE_URL=postgresql+asyncpg://user:pass@host:5432/testdb
pytest tests/test_aggregation_integration.py -v
```

> **Warning:** Integration tests drop and recreate all tables. Use a
> dedicated test database, not production.

---

## Public API (Stage 7)

### Endpoint reference

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/stops/nearby` | Stops within radius, ordered by distance |
| `GET` | `/stops/{stop_id}/routes` | Routes serving a stop |
| `GET` | `/scores` | Score card for one bucket |
| `GET` | `/scores/nearby-risky` | Riskiest stops near a location (worst route per stop) |
| `GET` | `/scores/trend` | Daily score series for a stop+route |
| `GET` | `/meta/last-ingest` | Last GTFS-RT ingest status per feed |
| `GET` | `/meta/last-agg` | Last aggregation run summary |

### Day-type and hour-bucket conventions

`day_type` values stored in `score_agg`:

| `day_type` | Days of week |
|------------|--------------|
| `weekday`  | Monday–Friday |
| `saturday` | Saturday |
| `sunday`   | Sunday |

`hour_bucket` values: `6-9`, `9-12`, `12-15`, `15-18`, `18-21`
(local hours in `SERVICE_TIMEZONE`, default `America/Vancouver`).

Arrivals outside these windows are excluded from aggregation.
`/scores/nearby-risky` and `/scores/trend` accept optional `day_type` /
`hour_bucket` parameters; if omitted they default to the **current local
time** in the service timezone.

### Staleness

`/meta/last-ingest` marks a feed as `is_fresh=false` when
`now − last_success_at > STALE_FEED_THRESHOLD_SEC` (default 120 s).

### Example requests

```bash
# Stops within 500 m of a point
curl "http://localhost:8000/stops/nearby?lat=49.2827&lon=-123.1207&radius_km=0.5"

# Routes serving a stop
curl "http://localhost:8000/stops/55846/routes"

# Score card for a specific bucket
curl "http://localhost:8000/scores?stop_id=55846&route_id=099&day_type=weekday&hour_bucket=9-12"

# Riskiest stops nearby (explicit bucket)
curl "http://localhost:8000/scores/nearby-risky?lat=49.28&lon=-123.12&day_type=weekday&hour_bucket=9-12"

# Riskiest stops nearby (smart defaults — bucket from current local time)
curl "http://localhost:8000/scores/nearby-risky?lat=49.28&lon=-123.12"

# 7-day daily trend
curl "http://localhost:8000/scores/trend?stop_id=55846&route_id=099&days=7"

# Ingest health
curl "http://localhost:8000/meta/last-ingest"
```

### Running Stage 7 integration tests

```bash
export RUN_INTEGRATION_TESTS=1
export DATABASE_URL=postgresql+asyncpg://user:pass@host:5432/testdb
pytest tests/test_public_api_integration.py -v
```

> **Warning:** Integration tests drop and recreate all tables. Use a
> dedicated test database, not production.
