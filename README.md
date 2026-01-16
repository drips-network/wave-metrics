```
┬ ┬┏─┐ ┬ ┬┏─┐  ┌┬┐┏━┐┌┳┐┬─┓┬┏─┐┌━┓
┃╻│┣━┤ ┃┌┘├┤   ┃│┃┃┤  ┃ ├┳┛┃│  └━┐
┗┻┘┴ ┴ ┗┘ ┗─┘  ┴ ┴┗━┘ ┴ ┴┗─┴┗─┘┗━┘
```

This service helps maintainers participating in Drips Wave evaluate contributors by measuring their collaborative activity in public GitHub projects. The service ingests each contributor's public repository activity via GitHub's GraphQL API, computes rolling 3-year metrics and percentile ranks based on population-level measures taken across all GitHub users (via GitHub Archive), and serves results through a cached REST API.

- [Architecture](#architecture)
- [Quick Start](#quick-start)
- [API Endpoints](#api-endpoints)
- [Metrics](#metrics)
- [Configuration](#configuration)
- [Baseline (population\_cdfs)](#baseline-population_cdfs)
- [Testing](#testing)

## Architecture

```
services/
├── api/                    # FastAPI server
│   └── app/
│       ├── main.py         # /api/v1/metrics, /api/v1/sync, /api/v1/jobs/, /health, ...
│       ├── schemas.py      # Pydantic models
│       └── security.py     # Bearer token auth
├── worker/                 # Celery background jobs
│   └── app/
│       ├── main.py         # Celery app factory
│       └── tasks.py        # sync_and_compute
└── shared/                 # Core logic
    ├── pipeline.py         # Ingestion + compute orchestration
    ├── github_client.py    # GitHub GraphQL helpers
    ├── percentiles.py      # Percentile ranks lookup + binning
    ├── throttle.py         # Redis rate-limit coordination
    └── ...
```

**Components:**

- **API** (FastAPI): Serves metrics reads and sync job submissions
- **Worker** (Celery): Executes GitHub ingestion and metrics computation
- **Postgres**: Normalized pull request data, serving tables, percentile threshold table
- **Redis**: Celery broker, metrics cache, rate-limit coordination

**Data Flow:**

1. `POST /api/v1/sync` enqueues `sync_and_compute` task with token reference
2. Worker ingests PRs via GitHub GraphQL → normalized tables
3. Worker computes metrics from Postgres + GitHub API (`contributionsCollection`)
4. Worker looks up percentile ranks
5. Worker writes to `contributor_metrics`, invalidates cache
6. `GET /api/v1/metrics` reads from serving tables (cached in Redis)

## Quick Start

```bash
# Start services (includes migrations + baseline load)
make up

# Or, if starting manually:
docker compose up -d --build
make load-baseline   # Required before first sync

# Verify health
curl http://localhost:8000/health
curl http://localhost:8000/version

# Trigger sync (add Authorization header if API_AUTH_TOKEN is set)
curl -X POST http://localhost:8000/api/v1/sync \
  -H 'Content-Type: application/json' \
  -H "Authorization: Bearer $API_AUTH_TOKEN" \
  -d '{"github_token":"ghp_xxx"}'

# For bulk refresh runs, route to the `bulk` queue with:
#   -d '{"github_token":"ghp_xxx","queue":"bulk"}'
# and ensure a worker is consuming the `bulk` queue (e.g. `docker compose up -d worker_bulk`).

# Poll job (unauthenticated)
curl http://localhost:8000/api/v1/jobs/<job_id>

# Read metrics (after job completes)
curl -H "Authorization: Bearer $API_AUTH_TOKEN" \
  "http://localhost:8000/api/v1/metrics?user_id=<uuid>"

```

Auth is controlled by `API_AUTH_TOKEN`. In production it should be set; for local development it can be empty to disable auth.

Token refs stored in Redis are encrypted; `TOKEN_REF_KEYS_JSON` and `TOKEN_REF_ACTIVE_KEY_ID` are required (local dev defaults are in `.env.example`).


## API Endpoints

| Endpoint | Purpose |
|----------|---------|
| `GET /health` | Liveness check |
| `POST /api/v1/sync` | Enqueue GitHub ingestion job |
| `GET /api/v1/jobs/{job_id}` | Poll job status (unauthenticated) |
| `GET /api/v1/metrics` | Retrieve metrics by user_id |
| `GET /api/v1/metrics/by-login` | Retrieve metrics by github_login |

For request/response schemas, auth details, and error codes, see [`services/api/README.md`](services/api/README.md).


## Metrics

Nine metrics comparing individual contributors against GitHub-wide population:

| Metric | Description | Gate |
|--------|-------------|------|
| `total_opened_prs` | PRs opened with ≥1 comment or review | ≥1 PR |
| `total_merged_prs` | Merged PRs | ≥1 PR |
| `pr_merge_rate` | PRs Merged / PRs opened ratio (0..1) | ≥20 PRs opened |
| `pr_drop_rate` | PRs closed without merge ratio (0..1; excludes drafts) | ≥20 non-draft PRs opened |
| `avg_merge_latency_hours` | Mean hours for PRs to merge | ≥20 PRs merged |
| `oss_reviews` | PR reviews submitted | ≥10 total activity† |
| `oss_issues_opened` | Issues opened | ≥10 total activity† |
| `oss_composite` | 0.40 × `oss_reviews` + 0.35 × `total_opened_prs` + 0.25 × `oss_issues_opened` | ≥10 total activity† |

†Total activity = `oss_reviews` + `total_opened_prs` + `oss_issues_opened`

**PR eligibility:** Only PRs with comment_count ≥ 1 OR review_count ≥ 1 are counted. This filters out self-merges and auto-merged dependency bumps.

**Window:** Per-contributor metrics use a rolling 1096-day (~3-year) window. Population baselines use a fixed window (currently 2022-10-01 to 2025-09-30; for more details, see [`population_data/README.md`](population_data/README.md#why-the-baseline-is-frozen)).

**Percentile Bins:**

| Range | Label |
|-------|-------|
| 0–24 | Very Low |
| 25–49 | Low |
| 50–74 | Medium |
| 75–89 | High |
| 90–98 | Very High |
| 99+ | Extremely High |

Display percentiles are clamped to a maximum of `99.9`, so `100.0` will never appear.
Percentiles are the raw percentile rank of the raw value against the population CDF. Some metrics are
lower-is-better, which is exposed in the API response for client-side interpretation.

**PR Eligibility:** Only PRs with `comment_count ≥ 1 OR review_count ≥ 1` are counted. This filters out self-merges and auto-merged dependency bumps.

**Window:** Per-contributor metrics use a rolling 1096-day (~3-year) window. Population-level data (for percentile ranks) uses a fixed baseline (currently 2022-10-01 to 2025-09-30).

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | `postgresql+psycopg2://...localhost:5432/wave-metrics` | Postgres connection |
| `REDIS_URL` | `redis://localhost:6379/0` | Redis for cache, queue, throttling |
| `API_AUTH_TOKEN` | (empty) | Bearer token for API authentication; required in production, empty disables auth for local dev |
| `POPULATION_BASELINE_ID` | (empty) | Pins baseline for percentile lookup |

For the full list of configuration variables (GitHub throttling, token refs, database pool tuning, etc.), see [`services/api/README.md`](services/api/README.md#configuration)

## Baseline (population_cdfs)

Percentiles require population-level thresholds loaded into Postgres (`population_cdfs`). The repo ships a default baseline CSV (whose production logic is described in [`population_data/README.md`](population_data/README.md)) at `population_data/output/population_cdfs.csv`.

The baseline dataset can be loaded into Postgres and verified with these targets:

```bash
make load-baseline
make verify-baseline
```

If you set `POPULATION_BASELINE_ID`, it must match a loaded `baseline_id` (e.g. the `BASELINE_ID` used by `make load-baseline`).

## Testing

```bash
# Install dev dependencies
make dev-install

# Run tests
make test

# or (without Make)
uv run --extra dev pytest -q
```

Integration tests require Postgres. Start it with `docker compose up -d postgres` (or `make up`) before running the test suite.
