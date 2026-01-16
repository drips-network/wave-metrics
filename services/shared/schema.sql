-- =============================================================================
-- Wave Metrics Database Schema
-- =============================================================================
--
-- Migration Safety:
--   This schema is applied via Alembic (see `services/alembic.ini`) from
--   `services/shared/database.py`, under a PostgreSQL advisory lock
--
-- Idempotency Requirement:
--   This file is executed as a schema snapshot. Keep it idempotent so it can be
--   re-applied safely on partially initialized databases (CREATE TABLE IF NOT EXISTS,
--   CREATE INDEX IF NOT EXISTS, ALTER TABLE ... IF EXISTS, etc)
--
--
-- =============================================================================

-- Core users mapping to GitHub identities
CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY,
    github_login TEXT NOT NULL UNIQUE,
    github_user_id BIGINT NOT NULL UNIQUE,
    github_created_at TIMESTAMPTZ,
    avatar_url TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Enforce case-insensitive uniqueness for GitHub logins
CREATE UNIQUE INDEX IF NOT EXISTS idx_users_github_login_lower
ON users (LOWER(github_login));

-- Login-only reservation/alias surface (may exist before a canonical users row)
CREATE TABLE IF NOT EXISTS github_login_aliases (
    github_login TEXT PRIMARY KEY,
    user_id UUID NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ,
    confirmed_at TIMESTAMPTZ,
    github_user_id BIGINT
);

DO $$
BEGIN
    ALTER TABLE github_login_aliases
    ADD CONSTRAINT chk_github_login_aliases_login_lower
    CHECK (github_login = LOWER(github_login));
EXCEPTION
    WHEN duplicate_object THEN
        NULL;
END $$;

CREATE INDEX IF NOT EXISTS idx_github_login_aliases_expires_at
ON github_login_aliases (expires_at);

CREATE INDEX IF NOT EXISTS idx_github_login_aliases_user_id
ON github_login_aliases (user_id);

-- Job tracking keyed by Celery task UUID
CREATE TABLE IF NOT EXISTS sync_jobs (
    job_id UUID PRIMARY KEY,
    user_id UUID NOT NULL,
    github_login TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('PENDING', 'RUNNING', 'COMPLETED', 'FAILED', 'SKIPPED')),
    error_message TEXT,
    backfill_days INTEGER,
    triggered_by TEXT,
    partition_key TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    stale_marked_at TIMESTAMPTZ,
    sync_run_id UUID,
    compute_run_id UUID,
    baseline_id TEXT,
    window_start TIMESTAMPTZ,
    window_end TIMESTAMPTZ
);

ALTER TABLE sync_jobs ADD COLUMN IF NOT EXISTS partition_key TEXT;

CREATE INDEX IF NOT EXISTS idx_sync_jobs_user_created_at
ON sync_jobs (user_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_sync_jobs_status_created_at
ON sync_jobs (status, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_sync_jobs_triggered_partition_created_at
ON sync_jobs (triggered_by, partition_key, created_at DESC);

-- Repositories
CREATE TABLE IF NOT EXISTS repositories (
    id BIGSERIAL PRIMARY KEY,
    github_repo_id BIGINT NOT NULL UNIQUE,
    owner_login TEXT NOT NULL,
    name TEXT NOT NULL,
    is_private BOOLEAN NOT NULL DEFAULT FALSE,
    default_branch TEXT,
    pushed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(owner_login, name)
);

CREATE INDEX IF NOT EXISTS idx_repositories_owner_name ON repositories(owner_login, name);

-- Pull requests authored by users
CREATE TABLE IF NOT EXISTS pull_requests (
    id BIGSERIAL PRIMARY KEY,
    github_pr_id BIGINT NOT NULL UNIQUE,
    repository_id BIGINT REFERENCES repositories(id) ON DELETE CASCADE,
    author_user_id UUID REFERENCES users(id) ON DELETE SET NULL,
    number INTEGER NOT NULL,
    state TEXT NOT NULL,
    is_draft BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL,
    merged_at TIMESTAMPTZ,
    closed_at TIMESTAMPTZ,
    comment_count INTEGER NOT NULL DEFAULT 0,
    review_count INTEGER NOT NULL DEFAULT 0,
    additions INTEGER,
    deletions INTEGER,
    changed_files INTEGER,
    updated_at TIMESTAMPTZ,
    url TEXT
);

CREATE INDEX IF NOT EXISTS idx_pull_requests_author_time
ON pull_requests (author_user_id, created_at DESC);

-- Ensure eligibility columns exist on existing databases
ALTER TABLE pull_requests ADD COLUMN IF NOT EXISTS comment_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE pull_requests ADD COLUMN IF NOT EXISTS review_count INTEGER NOT NULL DEFAULT 0;

-- Repo languages
CREATE TABLE IF NOT EXISTS repository_languages (
    repository_id BIGINT REFERENCES repositories(id) ON DELETE CASCADE,
    language TEXT NOT NULL,
    bytes BIGINT NOT NULL,
    PRIMARY KEY (repository_id, language)
);

-- Population baselines (CDF thresholds for percentile lookup)
CREATE TABLE IF NOT EXISTS population_cdfs (
    metric_name TEXT NOT NULL,
    percentile NUMERIC(4, 1) NOT NULL,
    threshold_value NUMERIC NOT NULL,
    baseline_id TEXT NOT NULL,
    baseline_start_date DATE NOT NULL,
    baseline_end_date DATE NOT NULL,
    computed_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (baseline_id, metric_name, percentile)
);

CREATE INDEX IF NOT EXISTS idx_population_cdfs_lookup
ON population_cdfs (baseline_id, metric_name, threshold_value);

-- Baseline metadata (optional; simplifies baseline resolution)
CREATE TABLE IF NOT EXISTS baseline_metadata (
    baseline_id TEXT PRIMARY KEY,
    baseline_start_date DATE NOT NULL,
    baseline_end_date DATE NOT NULL,
    computed_at TIMESTAMPTZ NOT NULL,
    notes TEXT
);

-- Serving tables
CREATE TABLE IF NOT EXISTS contributor_metrics (
    user_id UUID PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,

    github_login TEXT NOT NULL,
    metrics_window_start TIMESTAMPTZ NOT NULL,
    metrics_window_end TIMESTAMPTZ NOT NULL,

    baseline_id TEXT NOT NULL,
    baseline_start_date DATE NOT NULL,
    baseline_end_date DATE NOT NULL,

    -- Raw metric values
    total_opened_prs INTEGER NOT NULL DEFAULT 0,
    total_merged_prs INTEGER NOT NULL DEFAULT 0,
    closed_without_merge_prs INTEGER NOT NULL DEFAULT 0,
    pr_merge_rate NUMERIC(10, 6),
    pr_drop_rate NUMERIC(10, 6),
    avg_merge_latency_hours NUMERIC(12, 3),

    oss_prs_opened INTEGER NOT NULL DEFAULT 0,
    oss_reviews INTEGER NOT NULL DEFAULT 0,
    oss_issues_opened INTEGER NOT NULL DEFAULT 0,
    oss_composite_raw NUMERIC(6, 2),

    -- Display percentiles (NULL when gated; clamped with no direction adjustment)
    total_opened_prs_percentile NUMERIC(4, 1),
    total_merged_prs_percentile NUMERIC(4, 1),
    pr_merge_rate_percentile NUMERIC(4, 1),
    pr_drop_rate_percentile NUMERIC(4, 1),
    avg_merge_latency_hours_percentile NUMERIC(4, 1),
    oss_prs_opened_percentile NUMERIC(4, 1),
    oss_reviews_percentile NUMERIC(4, 1),
    oss_issues_opened_percentile NUMERIC(4, 1),
    oss_composite_percentile NUMERIC(4, 1),

    computed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_contributor_metrics_computed_at
ON contributor_metrics (computed_at DESC);

CREATE TABLE IF NOT EXISTS contributor_language_profile (
    user_id UUID REFERENCES users(id) ON DELETE CASCADE,
    language TEXT NOT NULL,
    pct NUMERIC(6, 2) NOT NULL,
    computed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (user_id, language)
);

CREATE INDEX IF NOT EXISTS idx_contributor_language_profile_user
ON contributor_language_profile (user_id);

-- Run metadata tables (append-only)
CREATE TABLE IF NOT EXISTS github_sync_runs (
    id UUID PRIMARY KEY,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    partition_key TEXT,
    window_start TIMESTAMPTZ,
    window_end TIMESTAMPTZ,
    backfill_days INTEGER,
    triggered_by TEXT,
    status TEXT NOT NULL,
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_github_sync_runs_user_partition
ON github_sync_runs (user_id, partition_key);

CREATE TABLE IF NOT EXISTS metric_compute_runs (
    id UUID PRIMARY KEY,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    partition_key TEXT,
    window_start TIMESTAMPTZ,
    window_end TIMESTAMPTZ,
    backfill_days INTEGER,
    triggered_by TEXT,
    status TEXT NOT NULL,
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    baseline_id TEXT
);

CREATE INDEX IF NOT EXISTS idx_metric_compute_runs_user_partition
ON metric_compute_runs (user_id, partition_key);

ALTER TABLE metric_compute_runs ADD COLUMN IF NOT EXISTS baseline_id TEXT;
ALTER TABLE metric_compute_runs DROP COLUMN IF EXISTS wave_id;

-- Sync state
CREATE TABLE IF NOT EXISTS github_sync_state (
    user_id UUID REFERENCES users(id) ON DELETE CASCADE,
    last_pr_updated_at TIMESTAMPTZ,
    PRIMARY KEY (user_id)
);

-- Cleanup legacy tables
DROP TABLE IF EXISTS user_tokens;
