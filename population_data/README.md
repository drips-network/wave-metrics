# Population-Level Metrics Pipeline

This BigQuery pipeline generates population-level **percentile thresholds** from [GH Archive](https://www.gharchive.org/). These thresholds enable the percentile lookups in wave-metrics: when the service computes a contributor's `pr_merge_rate`, for example, it determines their percentile rank by comparing against thresholds derived from ~4 million GitHub users over a 3-year window.

**These scripts are not automatically executed.** This section of the repo serves to document the methods that produced the current percentile thresholds table and that would serve as a starting point if the baseline ever needs to be regenerated.

- [Population-Level Metrics Pipeline](#population-level-metrics-pipeline)
  - [Files](#files)
  - [Why the Baseline Is Frozen](#why-the-baseline-is-frozen)
  - [Data Flow](#data-flow)
  - [PR Eligibility](#pr-eligibility)
  - [Metrics](#metrics)
  - [Using the Shipped Baseline Locally](#using-the-shipped-baseline-locally)
  - [Configuration](#configuration)
  - [Cost Estimate (Full 3-Year Range)](#cost-estimate-full-3-year-range)
  - [Output Schema](#output-schema)
  - [Loading into Postgres](#loading-into-postgres)
  - [Design Decisions](#design-decisions)
  - [Validation](#validation)


## Files

| File | Purpose |
|------|---------|
| `pipeline.bqsql` | Main pipeline: extracts events from GH Archive, builds PR aggregates, computes per-user metrics, generates percentile thresholds |
| `validations.bqsql` | Post-run validation queries: invariant checks, distribution summaries, monotonicity verification |
| `load_population_cdfs.sh` | Shell script to load baseline CSV into Postgres via docker compose |
| `output/population_cdfs.csv` | Shipped baseline CSV (2022-10-01 to 2025-09-30) |

## Why the Baseline Is Frozen

The current baseline window is **2022-10-01 to 2025-09-30**. As of now, it cannot be extended because [GitHub trimmed Events API payloads in October 2025](https://github.blog/changelog/2025-08-08-upcoming-changes-to-github-events-api-payloads/), removing `payload.pull_request.user.login` (the PR author) from `PullRequestEvent` and `PullRequestReviewEvent`. This prevents author attribution for approximinately a third of the PRs (those with reviews but no comments) that this pipeline measures.

By contrast, the per-contributor metrics computed via GraphQL are unaffected. The GraphQL API returns complete PR data including author, merge status, and timestamps. Only population-level baseline regeneration from GitHub Archive is blocked beyond the month of September 2025.

## Data Flow

```
GH Archive (githubarchive.day.*, githubarchive.month.*)
    │
    ├─► Query A: PushEvent counts (~172 GB, no payload parsing)
    │       └─► stg_oss_event_counts_monthly (PushEvent rows)
    │
    └─► Query B: Payload extraction (~14.9 TB, 4 event types)
            └─► raw_extracted_events
                    │
                    ├─► stg_oss_event_counts_monthly (Reviews, Issues)
                    ├─► stg_pr_comment_snapshots_monthly
                    ├─► stg_pr_review_snapshots_monthly
                    └─► stg_pr_terminal_events
                            │
                            └─► pr_agg (deduplicated PRs with outcomes)
                                    │
                                    └─► user_metrics_baseline (per-user aggregates)
                                            │
                                            └─► population_cdfs (1001 percentile thresholds × 10 metrics)
                                                    │
                                                    └─► Export CSV → Load into wave-metrics Postgres
```

## PR Eligibility

The pipeline includes only PRs that received at least one comment OR one review during the baseline window. The wave-metrics service's eligibility filter (`comment_count >= 1 OR review_count >= 1`) is aligned with this. This condition ensures that the PR metrics reflect meaningful OSS collaboration signals rather than self-merges on personal projects or auto-merged dependency bumps.

PRs are deduplicated on `(repo_id, pr_number)` only (not including author) to handle cases where the author field differs between `IssueCommentEvent` and `PullRequestReviewEvent` payloads for the same PR.

## Metrics

All percentile thresholds are computed against **PR-active users only** (`total_opened_prs >= 1`), excluding the ~89% of GitHub accounts that never open PRs. Across this 2022-10-01 to 2025-09-30 baseline window, that is 3.9 million PR-active users (out of a total of 34.2 million users).

| Metric | Additional Filter | Notes |
|--------|-------------------|-------|
| `total_opened_prs` | — | Commented/reviewed PRs opened |
| `total_merged_prs` | — | Commented/reviewed PRs merged |
| `pr_merge_rate` | — | `total_merged_prs` / `total_opened_prs` |
| `pr_drop_rate` | — | PRs closed-without-merge / PRs opened (excludes drafts in numerator and denominator) |
| `avg_merge_latency_hours` | `total_merged_prs >= 1` | Mean hours from open to merge |
| `oss_prs_opened` | — | Same as `total_opened_prs` (minor storage redundancy chosen for semantic clarity, since this goes with the other `oss_`-prefixed metrics for calculation of `oss_composite`) |
| `oss_reviews` | — | PullRequestReviewEvent count |
| `oss_issues_opened` | — | IssuesEvent count |
| `oss_composite` | — | Weighted average: 0.40×reviews + 0.35×prs + 0.25×issues |
| `oss_pushes` | — | PushEvent count (warehoused, not used in current metrics) |

**Note:** wave-metrics currently serves 9 metrics; `oss_pushes` thresholds are warehoused for potential future use but not exposed in the API.

## Using the Shipped Baseline Locally

The repository ships a baseline CSV at `population_data/output/population_cdfs.csv`. Load it with:

```bash
make load-baseline
```

Verify the load:

```bash
make verify-baseline
```

**Overrides:**

| Variable | Default | Description |
|----------|---------|-------------|
| `BASELINE_ID` | `2025-09-30_v1` | Baseline identifier |
| `CSV` | `./population_data/output/population_cdfs.csv` | Path to CSV file |

Example with custom baseline:

```bash
make load-baseline BASELINE_ID=2025-09-30_v2 CSV=./my_custom_baseline.csv
```

## Configuration

Edit parameters at the top of `pipeline.bqsql`:

```sql
DECLARE p_baseline_start_date DATE DEFAULT DATE '2022-10-01';
DECLARE p_baseline_end_date DATE DEFAULT DATE '2025-09-30';
DECLARE p_baseline_id STRING DEFAULT '2025-09-30_v1';
```

The pipeline handles multi-year ranges automatically, switching between daily tables (for partial years at boundaries) and monthly tables (for complete middle years) to minimize scan costs.

## Cost Estimate (Full 3-Year Range)

| Query | Bytes Scanned | Est. (On-Demand) Cost |
|-------|---------------|-----------|
| Query A: PushEvent | ~172 GB | ~$1 |
| Query B: Payload extraction | ~14.9 TB | ~$91 |
| Downstream queries | ~10 GB | ~$0.06 |
| **Total** | **~15.1 TB** | **~$93** |

## Output Schema

The `population_cdfs` table stores **percentile thresholds** (quantiles) sampled at 0.1% granularity. Each row maps a percentile rank to the metric value at that threshold; at query time, we compute `raw_percentile = MAX(percentile) WHERE threshold_value <= value` for the contributor's metric value.

| Column | Type | Description |
|--------|------|-------------|
| `metric_name` | string | Metric identifier |
| `percentile` | float | 0.0 to 100.0 in 0.1 increments (1001 points) |
| `threshold_value` | float | Raw metric value at this percentile |
| `baseline_id` | string | Version identifier |
| `baseline_start_date` | date | Baseline window start |
| `baseline_end_date` | date | Baseline window end |

## Loading into Postgres

The pipeline includes an optional GCS export stage (Section 10, commented out). After export:

```bash
gsutil cp 'gs://bucket-name/wave-metrics/population_cdfs_*.csv' ./
cat population_cdfs_*.csv | head -1 > population_cdfs.csv
cat population_cdfs_*.csv | grep -v metric_name >> population_cdfs.csv
psql -c "DELETE FROM population_cdfs WHERE baseline_id = '2025-09-30_v1'"
psql -c "\copy population_cdfs FROM 'population_cdfs.csv' WITH CSV HEADER"
```

## Design Decisions

**Terminal state logic:** A PR is marked `is_closed_final_unmerged` only if its *last observed* action is `closed` and it was never merged. This correctly handles reopen cycles (opened → closed → reopened → still open ≠ dropped).

**Timestamp semantics:** `closed_at` uses MAX (final close), `merged_at` uses MIN (first/only merge, for latency calculation).

**Bot filtering:** Bots are excluded from all contributor activity counts but retained in PR terminal events (bots legitimately merge/close PRs via CI/CD).

**Merge detection:** Handles both modern `action='merged'` events and historical `action='closed'` with `merged_flag=true`.

## Validation

Run `validations.bqsql` after pipeline execution to verify:

- Invariants: merged ≤ opened, dropped ≤ opened, rates in [0,1], latency ≥ 0
- No PR is both merged AND dropped
- No duplicate PRs after deduplication
- No bot authors in PR population
- Threshold monotonicity (values non-decreasing across percentiles)
- 1001 points per metric with percentiles 0.0–100.0
