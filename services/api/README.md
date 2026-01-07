# API

FastAPI server exposing HTTP endpoints for reading contributor metrics and triggering GitHub sync jobs. Delegates ingestion and compute to Celery workers via the shared pipeline.

For pipeline architecture and metrics computation, see [`services/shared/README.md`](../shared/README.md).

- [API](#api)
  - [Endpoints](#endpoints)
    - [GET /health](#get-health)
    - [GET /version](#get-version)
    - [GET /api/v1/metrics](#get-apiv1metrics)
    - [GET /api/v1/metrics/by-login](#get-apiv1metricsby-login)
    - [POST /api/v1/sync](#post-apiv1sync)
    - [GET /api/v1/jobs/{job\_id}](#get-apiv1jobsjob_id)
  - [API Contract](#api-contract)
    - [Response Shape Stability](#response-shape-stability)
    - [Behavioral Guarantees](#behavioral-guarantees)
    - [Rate Limiting](#rate-limiting)
  - [Authentication](#authentication)
  - [Configuration](#configuration)
  - [Startup Behavior](#startup-behavior)
  - [Caching](#caching)
  - [Local Development](#local-development)

## Endpoints

### GET /health

Verifies Postgres and Redis connectivity.

```json
{
  "status": "healthy",
  "timestamp": "2025-01-15T12:00:00Z",
  "database": true,
  "redis": true
}
```

Returns `"degraded"` when either dependency is unreachable. Use for load balancer health checks.

HTTP status:
- `200` when healthy
- `503` when degraded

Railway deploy note:
- Set the service health check path to `/health` and enable rolling/wait-for-healthy deploys so a misconfigured release doesn't replace the previous healthy deployment

When `HEALTH_CHECK_BROKER=1`, the response also includes:

```json
{
  "broker": true,
  "broker_workers": 2
}
```

Returns `"degraded"` when any dependency (including broker, when enabled) is unreachable.

### GET /version

```json
{"version": "0.1.0"}
```

### GET /api/v1/metrics
Retrieves precomputed metrics for a contributor.

**Parameters:**

| Name | Required | Description |
|------|----------|-------------|
| `user_id` | Yes | Internal user UUID |

**Response:**
```json
{
  "user_id": "a1b2c3d4-...",
  "github_login": "octocat",
  "github_created_at": "2012-03-15T08:30:00Z",
  "metrics_window_start": "2022-12-16T00:00:00Z",
  "metrics_window_end": "2025-12-16T00:00:00Z",
  "metrics_baseline_dates": {"start": "2022-10-01", "end": "2025-09-30"},
  "metrics": {
    "total_opened_prs": {"value": 408, "percentile": 97.2, "bin": "Very High", "description": "..."},
    "oss_composite": {"value": 93.2, "percentile": 95.7, "bin": "Very High", "description": "..."}
  },
  "lifetime_language_profile": [{"language": "Python", "pct": 48.3}],
  "computed_at": "2025-12-16T14:30:00Z"
}
```

Percentiles may be `null` when gated by threshold rules (e.g., `pr_merge_rate` requires ≥20 PRs).

**Errors:**

| Status | Condition |
|--------|-----------|
| 401 | Auth enabled and token missing/invalid |
| 404 | No metrics found for user |
| 500 | Database or internal error |

### GET /api/v1/metrics/by-login

Resolves `user_id` by `github_login` (case-insensitive) and returns the exact same response shape as `/api/v1/metrics`.

**Parameters:**

| Name | Required | Description |
|------|----------|-------------|
| `github_login` | Yes | GitHub login |

**Errors:**

| Status | Condition |
|--------|-----------|
| 401 | Auth enabled and token missing/invalid |
| 404 | No user found for login |
| 500 | Database or internal error |

### POST /api/v1/sync

Enqueues a GitHub ingestion and metrics computation job.

**Request:**

```json
{
  "user_id": "optional-uuid",
  "github_login": "optional-login",
  "github_token": "optional-if-X-GitHub-Token-header-set",
  "backfill_days": 30
}
```

| Field | Required | Description |
|-------|----------|-------------|
| `user_id` | No | Internal UUID; must already exist unless paired with `github_login` (reservation flow) |
| `github_login` | No | GitHub login (lowercased/trimmed) |
| `github_token` | Yes (or `X-GitHub-Token`) | OAuth token for ingestion and metrics |
| `backfill_days` | No | Lookback window in days; `null` enables incremental mode |
| `queue` | No | Celery queue selection; must be `default` or `bulk` (allowlisted), defaults to `default` when omitted, and can't access scheduled lanes (`daily`/`backfill`) |

**Resolution precedence:**

1. If `user_id` is provided and exists in `users`: use it (and validate `github_login` if also provided)
2. If `user_id` is provided but does not exist in `users`:
   - If `github_login` is provided: treat as a login-reservation request using that `user_id`
   - Else: reject with 400
3. Else if `github_login` is provided: resolve or create a reservation in `github_login_aliases` without GitHub calls
4. Else (token-only): call GitHub once, upsert `users` by stable `github_user_id`, and confirm `github_login_aliases`

**Response:**

```json
{
  "job_id": "d290f1ee-6c54-4b01-90e6-d701748f0851",
  "status": "enqueued",
  "user_id": "canonical-internal-uuid",
  "github_login": "octocat"
}
```

**Reservation note (github_login-only resolution):**

When `github_login` is provided and the login has not been confirmed yet, `/api/v1/sync` may return a reserved `user_id` (from `github_login_aliases`) before the worker has verified the GitHub identity. After the job reaches `COMPLETED`, clients should treat `GET /api/v1/jobs/{job_id}` as authoritative for the final canonical `user_id` and use that `user_id` for metrics reads and long-term storage.

Recommended client flow:

1. `POST /api/v1/sync` → store `job_id`
2. Poll `GET /api/v1/jobs/{job_id}` until `COMPLETED`, `FAILED`, or `SKIPPED` (this endpoint is unauthenticated)
3. Read metrics using `GET /api/v1/metrics?user_id=<user_id from jobs>`

**Errors:**

| Status | Condition |
|--------|-----------|
| 400 | Invalid/missing token or invalid identity inputs |
| 401 | Auth enabled and token missing/invalid |
| 500 | Database or internal error |

### GET /api/v1/jobs/{job_id}

Returns job status and timestamps.

This endpoint is intentionally **unauthenticated** even when `API_AUTH_TOKEN` is set.

```json
{
  "job_id": "d290f1ee-6c54-4b01-90e6-d701748f0851",
  "status": "PENDING",
  "user_id": "canonical-internal-uuid",
  "github_login": "octocat",
  "created_at": "2025-01-01T00:00:00Z",
  "started_at": null,
  "completed_at": null,
  "error_message": null
}
```

**Status values:** `PENDING`, `RUNNING`, `COMPLETED`, `FAILED`, `SKIPPED` (terminal; did not run due to per-user lock)

**Stale detection:** if a job is `RUNNING` for longer than `RUNNING_JOB_STALE_SECONDS`, the API marks it `FAILED` and sets `stale_marked_at` and `error_message`

## API Contract

These guarantees hold for clients. Implementation details may change; these will not without a version bump.

### Response Shape Stability

**GET /api/v1/metrics:** returns `MetricsResponse` (stable shape).

**POST /api/v1/sync:** response includes:

| Field | Presence | Type |
|-------|----------|------|
| `job_id` | Always | string (Celery task UUID) |
| `status` | Always | `"enqueued"` on 200 |
| `user_id` | Always | string (UUID) |
| `github_login` | Always | string |

### Behavioral Guarantees

1. **Read-only metrics endpoints:** `/api/v1/metrics` and `/api/v1/metrics/by-login` never trigger computation
2. **Idempotent writes:** ingestion uses upsert semantics for normalized tables
3. **Cache coherence:** cache invalidated after successful sync; reads within TTL may return stale data
4. **Auth boundary:** when `API_AUTH_TOKEN` is configured, `/sync` and metrics endpoints require `Authorization: Bearer <token>`
5. **Jobs endpoint exception:** `/api/v1/jobs/{job_id}` is intentionally unauthenticated
6. **No hidden job id:** `job_id` is the Celery task UUID

### Rate Limiting

The API does not enforce rate limits on callers. However:

- Each sync consumes GitHub API quota for the provided token
- Rate limits coordinated per-token across workers via Redis
- Single token: 5,000 requests/hour; heavy backfills may exhaust this

**Typical consumption:** Even contributors with extensive histories (100+ PRs over 3 years) typically require fewer than 70 GitHub API calls per sync. Light users may need only 10-20 calls. The 5,000/hour limit is rarely a concern for individual syncs.

## Authentication

Controlled by `API_AUTH_TOKEN` environment variable.

- **Disabled (empty):** all requests allowed
- **Enabled (non-empty):** requests must include `Authorization: Bearer <token>`

Exception: `GET /api/v1/jobs/{job_id}` is always unauthenticated.

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | `postgresql+psycopg2://...localhost:5432/wave-metrics` | Postgres connection |
| `REDIS_URL` | `redis://localhost:6379/0` | Redis for cache and Celery |
| `SERVICE_VERSION` | `0.1.0` | Reported by `/version` |
| `PORT` | `8000` | HTTP server port |
| `API_BIND_HOST` | `0.0.0.0` | Uvicorn bind host; set to `::` when environment (e.g. Railway networking) requires IPv6 listening |
| `RUN_DB_MIGRATIONS` | `1` | Run migrations on startup |
| `CACHE_TTL_SECONDS` | `180` | Metrics cache TTL |
| `API_AUTH_TOKEN` | (empty) | Bearer token for API authentication; required in production, empty disables auth for local dev |
| `RUNNING_JOB_STALE_SECONDS` | `3600` | Mark RUNNING jobs stale after this many seconds |
| `GITHUB_LOGIN_RESERVATION_TTL_SECONDS` | `86400` | TTL for unconfirmed github_login reservations |
| `POPULATION_BASELINE_ID` | (empty) | Pins baseline for percentile lookup |
| `TOKEN_REF_KEYS_JSON` | (required) | JSON object mapping key IDs to base64-encoded 32-byte keys (token_ref encryption) |
| `TOKEN_REF_ACTIVE_KEY_ID` | (required) | Active key ID within `TOKEN_REF_KEYS_JSON` |
| `TOKEN_REF_TTL_SECONDS_NORMAL` | `900` | Token ref TTL (seconds) for normal runs |
| `TOKEN_REF_TTL_SECONDS_BULK` | `86400` | Token ref TTL (seconds) for bulk runs |
| `GH_REDIS_PREFIX` | `gh:` | Redis key prefix for rate-limit keys |
| `GH_BACKOFF_BASE_SECONDS` | `5` | Initial backoff for GitHub retries |
| `GH_BACKOFF_CAP_SECONDS` | `60` | Maximum backoff for GitHub retries |
| `GH_SEMAPHORE_TTL_SECONDS` | `300` | Semaphore key TTL |
| `GH_SEMAPHORE_ACQUIRE_TIMEOUT_SECONDS` | `60` | Max wait for semaphore |
| `GH_COOLDOWN_MAX_WAIT_SECONDS` | `600` | Cap on secondary cooldown waits |
| `HEALTH_CHECK_BROKER` | `0` | Enable broker connectivity check in `/health` |
| `HEALTH_CHECK_BROKER_TIMEOUT_SECONDS` | `2` | Timeout for broker ping |

## Startup Behavior

On application startup:

1. If `RUN_DB_MIGRATIONS=1`: runs Alembic migrations (`services/alembic.ini`), applying `schema.sql` and `materialized_views.sql`
2. Creates Celery client configured against `REDIS_URL`
3. Begins serving HTTP requests

For production with multiple replicas, run migrations via a single replica or dedicated job.

## Caching

Metrics responses cached in Redis with key pattern `metrics:{user_id}`.

- **TTL:** Controlled by `CACHE_TTL_SECONDS` (default 180s)
- **Invalidation:** Occurs in worker after successful sync
- **Failure behavior:** Cache write failures serve from DB; no error raised

See `services/shared/caching.py` for cache behavior and error handling.

## Local Development

```bash
# Start full stack
make up

# Test endpoints
curl http://localhost:8000/health
curl http://localhost:8000/version

# Trigger sync
curl -X POST http://localhost:8000/api/v1/sync \
  -H 'Content-Type: application/json' \
  -H "Authorization: Bearer $API_AUTH_TOKEN" \
  -d '{"github_token":"ghp_xxx"}'

# Poll job (unauthenticated)
curl http://localhost:8000/api/v1/jobs/<job_id>

# Read metrics
curl -H "Authorization: Bearer $API_AUTH_TOKEN" \
  "http://localhost:8000/api/v1/metrics?user_id=<uuid>"

# Read metrics by login
curl -H "Authorization: Bearer $API_AUTH_TOKEN" \
  "http://localhost:8000/api/v1/metrics/by-login?github_login=octocat"
```
