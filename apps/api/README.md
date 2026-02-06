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
