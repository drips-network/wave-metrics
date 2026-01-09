# Worker

Celery application executing GitHub ingestion and metrics computation jobs. Consumes tasks from Redis and delegates to the shared pipeline.

For pipeline details, see [`services/shared/README.md`](../shared/README.md).

- [Worker](#worker)
  - [Architecture](#architecture)
  - [Queues](#queues)
  - [Task: sync\_and\_compute](#task-sync_and_compute)
  - [Task Contract](#task-contract)
    - [Invariants](#invariants)
    - [Celery Settings](#celery-settings)
  - [Login Mismatch](#login-mismatch)
  - [Failure Semantics](#failure-semantics)
    - [During Ingestion](#during-ingestion)
    - [During Computation](#during-computation)
    - [Infrastructure Failures](#infrastructure-failures)
  - [Configuration](#configuration)
    - [Required](#required)
    - [Token Refs](#token-refs)
    - [GitHub Throttling](#github-throttling)
    - [Operational](#operational)
  - [Scaling](#scaling)
    - [Horizontal Scaling](#horizontal-scaling)
    - [Rate Limit Coordination](#rate-limit-coordination)
    - [Database Connections](#database-connections)
  - [Graceful Shutdown](#graceful-shutdown)
    - [Warm Shutdown](#warm-shutdown)
    - [Cold Shutdown (Iff Emergency)](#cold-shutdown-iff-emergency)
  - [Monitoring](#monitoring)
    - [Queue Health](#queue-health)
    - [Run Metadata Queries](#run-metadata-queries)
    - [Key Metrics](#key-metrics)
  - [Local Development](#local-development)


## Architecture

```
┌─────────────┐         ┌──────────────┐         ┌──────────────┐
│   Redis     │ ──────► │   Celery     │ ──────► │   Postgres   │
│  (Broker)   │         │   Worker     │         │   (Primary)  │
└─────────────┘         └──────────────┘         └──────────────┘
                               │
                               ▼
                        GitHub GraphQL API
                       (via Redis throttling)
```

## Queues

Worker traffic can be split between `default` and `bulk` queues so large refreshes do not starve API-triggered syncs.

API-triggered syncs can be routed to the `bulk` queue via `/api/v1/sync` request body `queue="bulk"`.

| Queue | Purpose | Tasks |
|-------|---------|-------|
| `default` | API-triggered syncs | `sync_and_compute` |
| `bulk` | Bulk API-triggered syncs | `sync_and_compute` |

## Task: sync_and_compute

The primary task that orchestrates GitHub ingestion and metrics computation.

**Signature:**

```python
sync_and_compute(user_id, token_ref, backfill_days=None, triggered_by=None)
```

| Argument | Type | Description |
|----------|------|-------------|
| `user_id` | str | Internal user UUID |
| `token_ref` | str | Short-lived token reference UUID (stored in Redis) |
| `backfill_days` | int/null | Lookback window; `null` for incremental mode |
| `triggered_by` | str | Origin label like `api`, `manual` |

**Return:**

```python
{
    "status": "ok",
    "user": "octocat",
    "user_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
    "sync_run_id": "11111111-2222-3333-4444-555555555555",
    "compute_run_id": "66666666-7777-8888-9999-000000000000",
    "window_start": "2025-01-01T00:00:00+00:00",
    "window_end": "2025-01-31T00:00:00+00:00",
    "baseline_id": "2025-09-30_v1",
    "computed_at": "2025-01-31T00:00:00+00:00"
}
```

If another sync is already running for the same user, the task returns:

```python
{"status": "locked", "user": "octocat", "user_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890"}
```

**Execution Flow:**

1. Lease token reference from Redis
2. Resolve GitHub login via `fetch_user_login()`
3. Ensure user exists in `users` table
4. Resolve ingestion window (explicit, backfill, or incremental)
5. Iterate PRs via windowed GraphQL search
6. Upsert repositories, languages, and PRs
7. Compute rolling 3-year metrics with percentiles
8. Compute lifetime language profile
9. Invalidate Redis cache (`metrics:{user_id}`)
10. Record completion in run metadata tables

## Task Contract

### Invariants

1. **Idempotency:** Running identical arguments twice is safe. Domain writes use `INSERT ... ON CONFLICT DO UPDATE`. Run metadata appends a new row per execution.

2. **Token ref leasing:** Token refs are leased per Celery task id. On success, the token ref is deleted; on failure, the token is retained so `acks_late` redelivery can retry. Deterministic terminal failures (such as login mismatch) also delete the token ref to prevent retries.

3. **Atomic run metadata:** Metadata is written via separate transactions. Main transaction failures still record `FAILED` status.

4. **Window-based ingestion:** PRs are fetched within explicit bounds. Overlapping windows are safe.

5. **Cache invalidation on success only:** Redis cache is deleted only after successful computation.

6. **Rate limit coordination:** All GitHub calls coordinate per-token limits via Redis.

### Celery Settings

| Setting | Value | Implication |
|---------|-------|-------------|
| `broker` | Redis | Tasks in Redis lists |
| `backend` | Redis | Results available via AsyncResult |
| `task_default_queue` | `default` | Default queue for tasks; workers should consume the queues they serve (e.g. `-Q default`) |
| `acks_late` | `True` | Task acknowledged on completion; worker crash triggers redelivery |
| `task_routes` | (configured) | Routes `sync_and_compute`→`default` |

## Login Mismatch

When `github_login` is provided in the sync request, the worker verifies it matches the token owner:

1. Fetches viewer identity via `fetch_user_login()`
2. Compares normalized logins (lowercase, trimmed)
3. On mismatch:
   - Marks job as `FAILED` with error message
   - Finalizes the token ref as successful (deterministic terminal failure, no retry)
   - Returns `{"status": "failed", "error": "github_login does not match token owner ..."}`

This is a terminal failure because the token cannot be used for the requested user.

## Failure Semantics

### During Ingestion

Ingestion flushes and commits in batches via `db_session()`, so failures mid-run can leave partial progress in normalized tables. This is safe because all writes are idempotent and can be safely re-run.

| Failure | Database State | Run Metadata | Task State |
|---------|---------------|--------------|------------|
| GitHub 401 (bad token) before first flush | No domain writes | `FAILED` | Failed, no retry |
| GitHub 401 (bad token) mid-run | Partial PR/repo rows (safe to re-run via upserts) | `FAILED` | Failed, no retry |
| GitHub 403/429 (rate limit) | Partial possible if mid-flush | Not yet recorded | Retrying internally |
| GitHub 5xx | Partial possible if mid-flush | `FAILED` | Failed, no retry |
| DB connection error | Depends on failure point | May fail to record | Failed |

### During Computation

| Failure | Database State | Run Metadata | Cache |
|---------|---------------|--------------|-------|
| Baseline not found | Ingested data committed | `FAILED` | Not invalidated |
| Metrics upsert fails | Ingested data committed | `FAILED` | Not invalidated |
| Cache invalidation fails | All data committed | `SUCCESS` | Stale until TTL |

### Infrastructure Failures

**Redis unavailable:** Worker cannot fetch tasks; in-flight tasks may fail on cache/throttle operations.

**Postgres unavailable:** Task fails on first DB operation; run metadata cannot be recorded.

## Configuration

### Required

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | `postgresql+psycopg2://...localhost:5432/wave-metrics` | Postgres |
| `REDIS_URL` | `redis://localhost:6379/0` | Broker, backend, throttling |
| `TOKEN_REF_KEYS_JSON` | (required) | JSON object mapping key IDs to base64-encoded 32-byte keys (token_ref encryption) |
| `TOKEN_REF_ACTIVE_KEY_ID` | (required) | Active key ID within `TOKEN_REF_KEYS_JSON` |

### Token Refs

| Variable | Default | Description |
|----------|---------|-------------|
| `TOKEN_REF_KEY_PREFIX` | `gh:token_ref:` | Redis key prefix for token refs |
| `TOKEN_REF_TTL_SECONDS_NORMAL` | `900` | Token ref TTL (seconds) for normal runs |
| `TOKEN_REF_TTL_SECONDS_BULK` | `86400` | Token ref TTL (seconds) for bulk runs |

### GitHub Throttling

| Variable | Default | Description |
|----------|---------|-------------|
| `GH_MAX_RETRIES` | `2` | Max retries per request on rate limit |
| `GH_CONCURRENCY_PER_TOKEN` | `2` | Concurrent requests per token |
| `GH_BACKOFF_BASE_SECONDS` | `5` | Initial backoff |
| `GH_BACKOFF_CAP_SECONDS` | `60` | Maximum backoff |
| `GH_REDIS_PREFIX` | `gh:` | Redis key prefix |
| `GH_SEMAPHORE_TTL_SECONDS` | `300` | Semaphore key TTL |
| `GH_SEMAPHORE_ACQUIRE_TIMEOUT_SECONDS` | `60` | Max wait for semaphore |
| `GH_COOLDOWN_MAX_WAIT_SECONDS` | `600` | Cap on cooldown waits |

### Operational

| Variable | Value | Description |
|----------|-------|-------------|
| `RUN_DB_MIGRATIONS` | `0` | Should be `0` for workers |

## Scaling

### Horizontal Scaling

Run multiple worker containers. Each is independent; Redis handles task distribution.

```yaml
# docker-compose.override.yml
services:
  worker:
    deploy:
      replicas: 4
```

### Rate Limit Coordination

Workers processing the same token coordinate via Redis:

| Key | Purpose |
|-----|---------|
| `gh:budget:{hash}` | Remaining requests, reset epoch |
| `gh:sem:{hash}` | Concurrency semaphore |
| `gh:cooldown:{hash}` | Secondary rate limit cooldown |

Workers processing different tokens don't interfere.

### Database Connections

Default SQLAlchemy pooling uses SQLAlchemy defaults (`pool_size=5`, `max_overflow=10`). You can override via `DB_POOL_SIZE`,
`DB_MAX_OVERFLOW`, `DB_POOL_TIMEOUT_SECONDS`, and `DB_POOL_RECYCLE_SECONDS`. For high concurrency, we may consider PgBouncer.

## Graceful Shutdown

### Warm Shutdown

```bash
kill -TERM <worker_pid>
```

Behavior:

- Stops fetching new tasks immediately
- In-flight tasks continue to completion
- Process exits after all tasks complete

### Cold Shutdown (Iff Emergency)

```bash
kill -9 <worker_pid>
```

Consequences:

- In-flight tasks terminated immediately
- With `acks_late=True`: tasks are redelivered (may re-run idempotent work)
- Run metadata may show `RUNNING` permanently

## Monitoring

### Queue Health

```bash
# Queue depth
redis-cli LLEN default
redis-cli LLEN bulk

# Active tasks
celery -A services.worker.app.main:celery_app inspect active

# Worker stats
celery -A services.worker.app.main:celery_app inspect stats
```

### Run Metadata Queries

```sql
-- Recent failed ingestions
SELECT user_id, error_message, finished_at
FROM github_sync_runs
WHERE status = 'FAILED'
ORDER BY finished_at DESC LIMIT 20;

-- Stuck runs
SELECT user_id, started_at
FROM github_sync_runs
WHERE status = 'RUNNING'
  AND started_at < NOW() - INTERVAL '1 hour';
```

### Key Metrics

| Metric | Healthy Range | Action |
|--------|---------------|--------|
| Queue depth | < 100 sustained | Add workers |
| Task success rate | > 95% | Check error_message |
| Task duration p95 | < 5 min | Check rate limits |

## Local Development

```bash
# Start full stack
make up

# Watch worker logs
docker compose logs -f worker
docker compose logs -f worker_bulk

# Trigger sync
curl -X POST http://localhost:8000/api/v1/sync \
  -H 'Content-Type: application/json' \
  -H "Authorization: Bearer $API_AUTH_TOKEN" \
  -d '{"github_token":"ghp_xxx"}'

# Poll job (unauthenticated)
curl http://localhost:8000/api/v1/jobs/<job_id>
```

**Standalone (without Docker):**

```bash
export DATABASE_URL=postgresql+psycopg2://postgres:postgres@localhost:5432/wave-metrics
export REDIS_URL=redis://localhost:6379/0
export RUN_DB_MIGRATIONS=0

celery -A services.worker.app.main:celery_app worker \
    --loglevel=info \
    --concurrency=2 \
    -Q default
```
