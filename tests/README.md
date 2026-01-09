# Tests

Test suite covering API response assembly, ingestion orchestration, percentile logic, and token handling.

- [Tests](#tests)
  - [Running Tests](#running-tests)
  - [Test Categories](#test-categories)
    - [Unit Tests](#unit-tests)
    - [Integration Tests](#integration-tests)
    - [Local E2E Tests](#local-e2e-tests)
  - [Environment Setup](#environment-setup)
    - [Unit Tests Only](#unit-tests-only)
    - [Integration Tests](#integration-tests-1)
  - [What Tests Assume](#what-tests-assume)
  - [Environment Variables](#environment-variables)
  - [Mocking Patterns](#mocking-patterns)
    - [GitHub Client](#github-client)
    - [Redis Cache](#redis-cache)
    - [Celery Task Wrapper](#celery-task-wrapper)


## Running Tests

```bash
# Full suite
make test

# or (without Make)
uv run --extra dev pytest -q

# Verbose output
uv run --extra dev pytest -v

# Single file
uv run --extra dev pytest tests/test_percentiles.py -v
```

## Test Categories

### Unit Tests

Pure function tests requiring no external services.

| Area | Test File | Coverage |
|------|-----------|----------|
| Percentile logic | `test_percentiles.py` | Lookup, inversion, clamping, binning |
| Baseline resolution | `test_baseline_resolution.py` | Baseline selection priority |
| API schemas | `test_api_schemas.py` | Request defaults and validation |
| Config validation | `test_config_validation.py` | Environment variable validation |
| Pipeline window | `test_pipeline_window_resolution.py` | Window resolution logic |
| Pipeline LRU cache | `test_pipeline_lru_cache.py` | Language cache behavior |
| User locking | `test_user_lock.py`, `test_pipeline_user_lock.py` | Per-user sync lock |
| Token store | `test_token_store.py` | Token reference management |
| Throttle semaphore | `test_throttle_semaphore.py` | Rate limit semaphore |
| GitHub client | `test_github_client_graphql_retry.py` | GraphQL retry logic |
| Worker job status mapping | `test_derive_terminal_job_fields.py` | Pipeline result → terminal job status |

### Integration Tests

Tests requiring Postgres via `DATABASE_URL`.

| Area | Test File | Coverage |
|------|-----------|----------|
| Jobs endpoint | `test_jobs_endpoint.py` | Job status endpoint |
| Metrics cache | `test_metrics_cache.py` | Cache read/write behavior |
| Pipeline integration | `test_pipeline_ingest_and_compute_integration.py` | Full pipeline with mocked GitHub |
| Worker task | `test_worker_task_wrapper.py` | Celery task execution |
| Worker skipped status | `test_worker_skipped_job_status.py` | Locked pipeline → SKIPPED sync_jobs status |

### Local E2E Tests

These tests exercise the full stack end-to-end against a running API + worker and the real GitHub API.

They are disabled by default and require:

- A running stack (`docker compose up -d --build`)
- Real GitHub OAuth tokens (PATs or OAuth App tokens) used as request inputs
- A local `DATABASE_URL` pointing at the same Postgres used by the stack (tests query `sync_jobs` and clear `github_sync_state`)
- A locally produced and stored file of valid outputs loaded via `WAVE_METRICS_VALID_OUTPUTS_PATH`. This data fixture file must have this structure (key: OAuth token; value: response from `/api/v1/metrics`):

```json
{
  "<github_token>": {
    "user_id": "string (UUID)",
    "github_login": "string",
    "github_created_at": "string (ISO 8601 datetime)",
    "metrics_window_start": "string (ISO 8601 datetime)",
    "metrics_window_end": "string (ISO 8601 datetime)",
    "metrics_baseline_dates": {
      "start": "string (YYYY-MM-DD)",
      "end": "string (YYYY-MM-DD)"
    },
    "metrics": {
      "<metric_name>": {
        "value": "number | null",
        "percentile": "number | null",
        "bin": "string | null",
        "description": "string"
      }
    },
    "lifetime_language_profile": [
      {
        "language": "string",
        "pct": "number"
      }
    ],
    "computed_at": "string (ISO 8601 datetime)"
  },
  "<github_token>": {
    // and so on
  }
}
```

**Note:** The file `tests/data_fixtures/valid_outputs_by_token.json` is gitignored and must be created locally.

Enable and run:

```bash
export WAVE_METRICS_E2E=1
export WAVE_METRICS_BASE_URL=http://localhost:8000
export WAVE_METRICS_VALID_OUTPUTS_PATH="tests/data_fixtures/valid_outputs_by_token.json"
uv run --extra dev pytest -v -q -m e2e
```

Or via Make:

```bash
make test-e2e E2E_BASE_URL=http://localhost:8000 E2E_VALID_OUTPUTS_PATH=tests/data_fixtures/valid_outputs_by_token.json
```

To stream progress logs while the E2E tests run:

```bash
make test-e2e-verbose E2E_BASE_URL=http://localhost:8000 E2E_VALID_OUTPUTS_PATH=tests/data_fixtures/valid_outputs_by_token.json
```

**E2E environment variables:**

| Variable | Default | Description |
|----------|---------|-------------|
| `WAVE_METRICS_E2E` | `0` | Set to `1` to enable E2E tests |
| `WAVE_METRICS_API_AUTH_TOKEN` | (empty) | API auth token; preferred over `API_AUTH_TOKEN` for E2E |
| `WAVE_METRICS_BASE_URL` | `http://localhost:8000` | API base URL |
| `WAVE_METRICS_VALID_OUTPUTS_PATH` | (empty) | Path to expected outputs JSON |
| `WAVE_METRICS_E2E_LOG` | `0` | Set to `1` for verbose progress logs |
| `WAVE_METRICS_E2E_TIMEOUT_SECONDS` | `1800` | Max wait per job |
| `WAVE_METRICS_E2E_POLL_SECONDS` | `3` | Polling interval |

**Tuning (optional):**

- `WAVE_METRICS_E2E_INT_ABS_TOL` (default `10`)
- `WAVE_METRICS_E2E_PERCENTILE_ABS_TOL` (default `1.0`)
- `WAVE_METRICS_E2E_FLOAT_REL_TOL` (default `0.05`)
- `WAVE_METRICS_E2E_FLOAT_ABS_TOL_SMALL` (default `0.05`)
- `WAVE_METRICS_E2E_FLOAT_ABS_SWITCH` (default `0.2`)
- `WAVE_METRICS_E2E_RATE_ABS_TOL` (default `0.05`)
- `WAVE_METRICS_E2E_LATENCY_REL_TOL` (default `0.15`)

## Environment Setup

### Unit Tests Only

```bash
uv run --extra dev pytest tests/test_percentiles.py -v
```

### Integration Tests

```bash
# Start Postgres
docker compose up -d postgres

# Run tests
export DATABASE_URL=postgresql+psycopg2://postgres:postgres@localhost:5432/wave-metrics
export RUN_DB_MIGRATIONS=1
uv run --extra dev pytest -v
```

## What Tests Assume

- No network access required; all GitHub calls monkeypatched
- Integration tests run `apply_pending_migrations()` in setup
- Cache behavior validated with FakeRedis
- Tests are isolated; each clears relevant tables

## Environment Variables

Some modules read from environment at import time. The `conftest.py` autouse fixture clears these for determinism:

- `API_AUTH_TOKEN`
- `POPULATION_BASELINE_ID`

## Mocking Patterns

### GitHub Client

```python
def test_with_mocked_github(monkeypatch):
    def _fake_fetch_user_login(token):
        return "test-user", {"databaseId": 123, "createdAt": "2020-01-01T00:00:00Z"}

    monkeypatch.setattr("services.shared.pipeline.fetch_user_login", _fake_fetch_user_login)
```

Patch at import location in `pipeline.py`, not definition in `github_client.py`.

### Redis Cache

```python
import services.shared.caching as caching_module

class FakeRedis:
    def __init__(self):
        self._store = {}

    def get(self, key):
        return self._store.get(key)

    def setex(self, key, ttl, value):
        self._store[key] = value

def test_caching(monkeypatch):
    fake = FakeRedis()
    monkeypatch.setattr(caching_module, "_redis", fake)
    monkeypatch.setattr(caching_module, "get_redis", lambda: fake)
```

### Celery Task Wrapper

The Celery task wrapper is tested without running Celery by calling the function directly and monkeypatching:

- `services.worker.app.tasks.lease_github_token_ref`
- `services.worker.app.tasks.finalize_github_token_ref`
- `services.worker.app.tasks.db_session`

If a test needs the Celery task id to be set (for job status writes), use `sync_and_compute.apply(..., task_id=...)` instead of calling it directly.

```python
from services.worker.app.tasks import sync_and_compute

result = sync_and_compute.apply(args=[user_id, token_ref], kwargs={"backfill_days": 30}, task_id=str(job_id)).get()
assert result["status"] in {"ok", "locked"}
```
