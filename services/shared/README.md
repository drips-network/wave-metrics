# Shared Modules

Core logic for GitHub ingestion, metrics computation, percentile lookup, and rate limiting. Both the API and worker services import from this module. External orchestrators should call pipeline functions directly.

- [Shared Modules](#shared-modules)
  - [Module Overview](#module-overview)
  - [Pipeline Functions](#pipeline-functions)
    - [ingest\_user\_github\_activity](#ingest_user_github_activity)
    - [compute\_user\_metrics\_and\_language\_profile](#compute_user_metrics_and_language_profile)
    - [ingest\_and\_compute\_user](#ingest_and_compute_user)
    - [user\_needs\_recompute](#user_needs_recompute)
  - [Guarantees](#guarantees)
    - [Data Integrity](#data-integrity)
    - [Computation Semantics](#computation-semantics)
    - [Rate Limiting](#rate-limiting)
    - [Failure Handling](#failure-handling)
  - [Database Schema](#database-schema)
    - [Normalized Tables](#normalized-tables)
    - [Job Tracking](#job-tracking)
    - [Token Storage](#token-storage)
    - [Serving Tables](#serving-tables)
    - [Population Tables](#population-tables)
    - [Views](#views)
    - [Run Metadata](#run-metadata)
  - [GitHub Client](#github-client)
    - [Window Splitting](#window-splitting)
  - [Rate Limiting](#rate-limiting-1)
    - [Rate Limit Types](#rate-limit-types)
    - [Redis Keys](#redis-keys)
  - [Percentiles Logic](#percentiles-logic)
    - [Baseline Resolution](#baseline-resolution)
    - [Percentile Lookup](#percentile-lookup)
    - [Display Percentile](#display-percentile)
    - [Categorical Bins](#categorical-bins)
  - [Configuration](#configuration)
  - [Usage from Orchestrators](#usage-from-orchestrators)
    - [Incremental Daily Sync](#incremental-daily-sync)
    - [Selective Recompute](#selective-recompute)
    - [Partitioned Backfill](#partitioned-backfill)

## Module Overview

```
services/shared/
├── config.py              # Environment configuration
├── database.py            # SQLAlchemy engine, sessions, migrations
├── schema.sql             # Authoritative schema snapshot
├── materialized_views.sql # View definitions (v_eligible_prs)
├── persistence/alembic/   # Alembic migration runner (schema snapshot revisions)
├── pipeline.py            # Ingestion and compute orchestration
├── github_client.py       # GitHub GraphQL API helpers
├── throttle.py            # Rate limit coordination via Redis
├── metric_computations.py # Pure computation functions
├── metric_definitions.py  # Metric names, descriptions, invert flags
├── percentiles.py         # Baseline resolution, percentile lookup, bins
├── caching.py             # Redis cache helpers
├── diagnostics.py         # Parity/debug helpers (see diagnostics.md)
├── diagnostics.md         # How to run diagnostics tools
├── token_store.py         # Short-lived GitHub token references
├── token_vault.py         # Persistent encrypted GitHub tokens (optional)
├── locks.py               # Per-user sync locking via Redis
├── login_aliases.py       # GitHub login alias confirmation
└── users.py               # User enumeration for batch jobs
```

## Pipeline Functions

The Wave Metrics pipeline is built with three levels of abstraction to accommodate different regular use cases.

### ingest_user_github_activity

Pulls GitHub data into normalized Postgres tables without computing metrics.

```python
def ingest_user_github_activity(
    user_id,           # Optional; generated if None
    github_token,      # OAuth token with read:user scope
    since=None,        # Explicit window start (datetime, UTC)
    until=None,        # Explicit window end (datetime, UTC)
    backfill_days=None,# Lookback from now; ignored if since provided
    partition_key=None,# Orchestration label for run metadata
    triggered_by=None, # Origin label: 'api', 'scheduler', 'manual'
) -> dict
```

**Window Resolution:**

| Priority | Condition | Window |
|----------|-----------|--------|
| 1 | `since` provided | `[since, until or now]` |
| 2 | `backfill_days` provided | `[now - backfill_days, until or now]` |
| 3 | Incremental | `[watermark - 1d, now]` or `[now - 1096d, now]` if no watermark |

**Returns:**

```python
{
    "user_id": "a1b2c3d4-...",
    "github_login": "octocat",
    "window_start": datetime(2025, 1, 1, tzinfo=UTC),
    "window_end": datetime(2025, 1, 31, tzinfo=UTC),
    "pr_count": 42,
    "metadata_ok": True,
}
```

### compute_user_metrics_and_language_profile

Computes rolling 3-year metrics, percentiles, and lifetime language profile. Requires GitHub token for OSS Activity Score components via `contributionsCollection`.

```python
def compute_user_metrics_and_language_profile(
    user_id,          # Required
    github_token,     # Required
    partition_key=None,
    triggered_by=None,
) -> dict
```

**Returns:**

```python
{
    "user_id": "a1b2c3d4-...",
    "github_login": "octocat",
    "baseline_id": "2025-09-30_v1",
    "metrics_window_start": datetime(...),
    "metrics_window_end": datetime(...),
    "computed_at": datetime(...),
    "metadata_ok": True,
}
```

### ingest_and_compute_user

Full orchestration: ingestion + compute + cache invalidation.

```python
def ingest_and_compute_user(
    user_id,
    github_token=None,
    backfill_days=None,
    since=None,
    until=None,
    partition_key=None,
    triggered_by=None,
    persist_github_token=False,
) -> dict
```

**Execution:**

1. `ingest_user_github_activity()` → GitHub to normalized tables
2. `compute_user_metrics_and_language_profile()` → serving tables
3. `invalidate_metrics()` → clear Redis cache

### user_needs_recompute

Helper for batch jobs to detect stale metrics.

```python
def user_needs_recompute(session, user_id) -> bool
```

Returns `True` when:

- `contributor_metrics` row missing, OR
- `github_sync_state.last_pr_updated_at > contributor_metrics.computed_at`

## Guarantees

### Data Integrity

1. **Idempotency:** All writes use `INSERT ... ON CONFLICT DO UPDATE`. Re-running with identical inputs is safe. Run metadata tables are append-only.

2. **Window-based ingestion:** PRs are fetched within explicit time bounds. Overlapping windows are safe; PRs are deduplicated by `github_pr_id`.

3. **Referential consistency:** Serving tables reference valid `users` rows.

### Computation Semantics

4. **Eligibility:** PR metrics are computed from PRs where `comment_count >= 1 OR review_count >= 1`, projected via `v_eligible_prs` view.

5. **Rolling window:** Computation uses a 1096-day window ending at "now". PRs are included based on `created_at` (not `updated_at`). Window bounds are stored in `contributor_metrics`.

6. **Rate semantics:** `pr_merge_rate` and `pr_drop_rate` are ratios (0..1). `pr_drop_rate` excludes drafts.

7. **Baseline-driven percentiles:** Looked up from `population_cdfs`, selected by `POPULATION_BASELINE_ID` or latest baseline.

8. **Language weighting:** Percentages are weighted by PR size (`additions + deletions`). The language profile is measured across the user's lifetime (not windowed), as there is no population-level measure appropriate for comparison (which would motivate a 3-year constraint).

### Rate Limiting

9. **Per-token isolation:** Each token has independent rate budgets.

10. **Cross-worker coordination:** Workers using same token coordinate via Redis semaphore.

### Failure Handling

11. **Run metadata survives failures:** Written via separate transactions using `ENGINE.begin()`.

12. **Partial ingestion progress:** Ingestion flushes in batches; failures may leave partial progress. This is safe because writes are idempotent.

13. **Partial compute failure:** Ingested data is committed; failure gets recorded in `metric_compute_runs`.

## Database Schema

### Normalized Tables

**users:** Internal UUID ↔ GitHub identity

| Column | Type | Description |
|--------|------|-------------|
| `id` | UUID PK | Internal identifier |
| `github_login` | TEXT UNIQUE | GitHub username |
| `github_user_id` | BIGINT UNIQUE | GitHub database ID |
| `github_created_at` | TIMESTAMPTZ | Account creation |

**github_login_aliases:** Login reservation/alias surface (may exist before a canonical users row)

| Column | Type | Description |
|--------|------|-------------|
| `github_login` | TEXT PK | Lowercased login (reservation key) |
| `user_id` | UUID | Associated internal user ID |
| `expires_at` | TIMESTAMPTZ | Expiry for unconfirmed reservations |
| `confirmed_at` | TIMESTAMPTZ | When alias was confirmed via GitHub |
| `github_user_id` | BIGINT | GitHub database ID (after confirmation) |

**pull_requests:** PR data with eligibility columns

| Column | Type | Description |
|--------|------|-------------|
| `github_pr_id` | BIGINT UNIQUE | Dedup key |
| `author_user_id` | FK | References users |
| `state` | TEXT | OPEN, MERGED, CLOSED |
| `comment_count` | INTEGER | For eligibility check |
| `review_count` | INTEGER | For eligibility check |

### Job Tracking

**sync_jobs:** Job status keyed by Celery task UUID

| Column | Type | Description |
|--------|------|-------------|
| `job_id` | UUID PK | Celery task UUID |
| `user_id` | UUID | Target user |
| `github_login` | TEXT | GitHub login at enqueue time |
| `backfill_days` | INTEGER | Optional lookback window |
| `triggered_by` | TEXT | Origin label like `api`, `scheduler.daily`, `scheduler.backfill` |
| `partition_key` | TEXT | Optional grouping key for scheduler ticks and backfill runs |
| `status` | TEXT | PENDING, RUNNING, COMPLETED, FAILED, SKIPPED (terminal; did not run due to per-user lock) |
| `error_message` | TEXT | Error details if failed (queryable prefix conventions like `missing_token`, `token_invalid`, `enqueue_failed`) |
| `sync_run_id` | UUID | Linked github_sync_runs row |
| `compute_run_id` | UUID | Linked metric_compute_runs row |

### Token Storage

**user_tokens:** Encrypted token storage for scheduled refresh capability

| Column | Type | Description |
|--------|------|-------------|
| `user_id` | UUID | FK to users (PK with provider) |
| `provider` | TEXT | Token provider (e.g., "github") |
| `encryption_key_id` | TEXT | Key ID used for encryption |
| `ciphertext` | BYTEA | Encrypted token |
| `token_fingerprint` | TEXT | SHA256 prefix for identification |
| `invalidated_at` | TIMESTAMPTZ | When marked invalid |

### Serving Tables

**contributor_metrics:** Precomputed metrics per user

| Column | Type | Notes |
|--------|------|-------|
| `user_id` | UUID PK | FK to users |
| `metrics_window_start/end` | TIMESTAMPTZ | Rolling 3-year bounds |
| `baseline_id` | TEXT | Baseline for percentile lookup |
| `*_percentile` | NUMERIC(4,1) | Display percentiles (may be NULL) |

**contributor_language_profile:** Lifetime language distribution

| Column | Type | Notes |
|--------|------|-------|
| `user_id` | UUID | FK to users |
| `language` | TEXT | Language name |
| `pct` | NUMERIC | 0-100, PR-weighted |

### Population Tables

**population_cdfs:** Percentile thresholds by baseline

| Column | Type |
|--------|------|
| `baseline_id` | TEXT |
| `metric_name` | TEXT |
| `percentile` | NUMERIC(4,1) |
| `threshold_value` | NUMERIC |

**baseline_metadata:** Baseline version info (optional)

### Views

**v_eligible_prs:** Plain view projecting PR eligibility. No refresh is required.

### Run Metadata

**github_sync_runs:** Ingestion run history

| Column | Type |
|--------|------|
| `id` | UUID PK |
| `user_id` | FK |
| `status` | TEXT (PENDING/RUNNING/SUCCESS/FAILED) |
| `window_start/end` | TIMESTAMPTZ |
| `error_message` | TEXT |

**metric_compute_runs:** Computation run history (same structure plus `baseline_id`)

## GitHub Client

`github_client.py` provides GraphQL helpers routed through Redis-coordinated throttling.

| Function | Purpose |
|----------|---------|
| `fetch_user_login(token)` | Returns `(login, viewer_metadata)` |
| `graphql_query(token, query, variables)` | Low-level GraphQL request with retry/error handling |
| `iter_user_prs_windowed(token, login, since, until)` | Yields PRs with window splitting |
| `fetch_repo_languages(token, owner, name)` | Returns `[{language, bytes}]` |
| `fetch_oss_activity_counts_3y(token, login, end)` | OSS reviews + issues via 3×1-year calls |

### Window Splitting

For users with extensive history:

1. Query `issueCount` for proposed window
2. If count ≥ 1000 and window > 1 day: split in half, recurse
3. Otherwise: paginate through results

## Rate Limiting

`throttle.py` implements Redis-coordinated rate limiting.

### Rate Limit Types

| Type | Detection | Behavior |
|------|-----------|----------|
| Primary | `X-RateLimit-Remaining = 0` | Block until reset |
| Secondary | 403/429 with `Retry-After` | Honor header |
| Abuse | 403/429 without header | Exponential backoff |

### Redis Keys

All keys use `SHA256(token)[:32]` to avoid storing raw tokens. The longer prefix makes collisions negligibly likely.

| Key | Purpose | TTL |
|-----|---------|-----|
| `gh:budget:{hash}` | Remaining requests, reset epoch | Until reset + 5s |
| `gh:cooldown:{hash}` | Secondary rate limit cooldown | From Retry-After (capped by `GH_COOLDOWN_MAX_WAIT_SECONDS`) |
| `gh:sem:{hash}` | Concurrency semaphore | `GH_SEMAPHORE_TTL_SECONDS` (default 300s), refreshed on acquire |

## Percentiles Logic

### Baseline Resolution

1. `POPULATION_BASELINE_ID` env var if set
2. Latest `baseline_metadata` row by `baseline_end_date DESC`
3. Fallback: latest `population_cdfs` row

### Percentile Lookup

```
raw_percentile = MAX(percentile) WHERE threshold_value <= value
```

### Display Percentile

1. Invert for metrics where lower is better (`pr_drop_rate`, `avg_merge_latency_hours`)
2. Clamp to `[0.0, 99.9]` (so 100.0 is never returned)

This avoids claiming a "perfect" 100th percentile when percentiles are sampled at 0.1 granularity

### Categorical Bins

| Percentile | Bin Label |
|------------|-----|
| 0–24 | Very Low |
| 25–49 | Low |
| 50–74 | Medium |
| 75–89 | High |
| 90–98 | Very High |
| 99+ | Exceptional |

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | `...localhost:5432/wave-metrics` | Postgres DSN |
| `REDIS_URL` | `redis://localhost:6379/0` | Redis for cache + throttling |
| `RUN_DB_MIGRATIONS` | `1` | Run migrations on startup |
| `CACHE_TTL_SECONDS` | `180` | Metrics cache TTL |
| `GH_MAX_RETRIES` | `2` | Max retries per GitHub request |
| `GH_CONCURRENCY_PER_TOKEN` | `2` | Concurrent requests per token |
| `GH_BACKOFF_BASE_SECONDS` | `5` | Initial backoff |
| `GH_BACKOFF_CAP_SECONDS` | `60` | Maximum backoff |
| `GH_REDIS_PREFIX` | `gh:` | Redis key prefix for rate-limit keys |
| `GH_SEMAPHORE_TTL_SECONDS` | `300` | Semaphore key TTL |
| `GH_SEMAPHORE_ACQUIRE_TIMEOUT_SECONDS` | `60` | Max wait for semaphore |
| `GH_COOLDOWN_MAX_WAIT_SECONDS` | `600` | Cap on secondary cooldown waits |
| `TOKEN_REF_KEY_PREFIX` | `gh:token_ref:` | Redis key prefix for token refs |
| `TOKEN_REF_KEYS_JSON` | (required) | JSON object mapping key IDs to base64-encoded 32-byte keys (token_ref encryption) |
| `TOKEN_REF_ACTIVE_KEY_ID` | (required) | Active key ID within `TOKEN_REF_KEYS_JSON` |
| `TOKEN_REF_TTL_SECONDS_NORMAL` | `900` | Token ref TTL (seconds) for normal runs |
| `TOKEN_REF_TTL_SECONDS_BULK` | `86400` | Token ref TTL (seconds) for bulk runs |
| `USER_LOCK_TTL_SECONDS` | `900` | Per-user sync lock TTL |
| `USER_LOCK_WAIT_TIMEOUT_SECONDS` | `0` | Wait timeout for user lock |
| `REPO_LANGUAGE_CACHE_MAX_REPOS` | `500` | LRU cache size for repo languages |
| `RUNNING_JOB_STALE_SECONDS` | `3600` | Mark RUNNING jobs stale after this |
| `GITHUB_LOGIN_RESERVATION_TTL_SECONDS` | `86400` | TTL for unconfirmed login reservations |
| `POPULATION_BASELINE_ID` | (empty) | Pins baseline selection |
| `TOKEN_VAULT_KEYS_JSON` | (empty) | JSON object mapping key IDs to base64-encoded 32-byte keys |
| `TOKEN_VAULT_ACTIVE_KEY_ID` | (empty) | Active key ID within `TOKEN_VAULT_KEYS_JSON` |

## Usage from Orchestrators

### Incremental Daily Sync

```python
from services.shared.pipeline import ingest_and_compute_user
from services.shared.users import list_tracked_users

for user_id in list_tracked_users():
    result = ingest_and_compute_user(
        user_id=str(user_id),
        github_token=None,
        backfill_days=None,
        triggered_by="scheduler",
    )
    status = (result or {}).get("status")
    if status == "missing_token":
        print(f"No token for user {user_id}, skipping")
    elif status == "token_invalid":
        print(f"Invalid token for user {user_id}, skipping")
    elif status == "locked":
        print(f"User {user_id} locked, skipping")
```

### Selective Recompute

```python
from services.shared.pipeline import ingest_and_compute_user, user_needs_recompute
from services.shared.database import db_session
from services.shared.users import list_tracked_users

with db_session() as session:
    for user_id in list_tracked_users():
        if not user_needs_recompute(session, user_id):
            continue
        result = ingest_and_compute_user(
            user_id=str(user_id),
            github_token=None,
            triggered_by="scheduler",
        )
        status = (result or {}).get("status")
        if status in {"missing_token", "token_invalid", "locked"}:
            continue
```

### Partitioned Backfill

```python
from datetime import datetime, timedelta, timezone
from services.shared.pipeline import ingest_and_compute_user

# Example: processing a single day's partition
partition_date = datetime(2025, 1, 15, tzinfo=timezone.utc)
for user_id in user_ids_for_partition:
    result = ingest_and_compute_user(
        user_id=str(user_id),
        github_token=None,
        since=partition_date,
        until=partition_date + timedelta(days=1),
        partition_key="2025-01-15",
        triggered_by="scheduler",
    )
    status = (result or {}).get("status")
    if status in {"missing_token", "token_invalid", "locked"}:
        continue
```
