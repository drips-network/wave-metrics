# Scheduler

This service enqueues Celery work (it does not run ingestion/compute itself).

For worker task behavior and status semantics, see [`services/worker/README.md`](../worker/README.md).

- [Scheduler](#scheduler)
  - [Commands](#commands)
  - [Required Environment Variables](#required-environment-variables)
  - [Parameters](#parameters)
  - [Operational Notes](#operational-notes)
  - [CLI Options](#cli-options)

## Commands

Daily refresh:

```bash
python -m services.scheduler.app.main daily
```

Equivalent Make target (runs inside the `api` container, so a separate scheduler container isn't needed locally):

```bash
make scheduler-daily
```

Backfill subset:

```bash
python -m services.scheduler.app.main backfill \
  --backfill-days 30 \
  --github-logins octocat,calliope \
  --partition-key backfill:2025-12-24:30d:wave1 \
  --confirm
```

The `--confirm` flag is an operator safeguard: if the scheduler resolves more than `BACKFILL_MAX_USERS` users, it refuses to enqueue the
backfill unless `--confirm` is passed (or `BACKFILL_CONFIRM=1`). The threshold can be adjusted with `--backfill-max-users`.

Equivalent Make target:

```bash
make scheduler-backfill SCHEDULER_BACKFILL_ARGS='--backfill-days 30 --github-logins octocat,calliope --partition-key backfill:2025-12-24:30d:wave1 --confirm'
```

Backfill by user IDs:

```bash
python -m services.scheduler.app.main backfill --backfill-days 30 --user-ids <uuid1>,<uuid2> --confirm
```

## Required Environment Variables

- `DATABASE_URL`
- `REDIS_URL`

The scheduler doesn't require token vault keys. Only workers need `TOKEN_VAULT_KEYS_JSON` and `TOKEN_VAULT_ACTIVE_KEY_ID`.

## Parameters

- `LOG_LEVEL` (default `INFO`)
- `ENQUEUE_BATCH_SIZE` (default `200`)
- `ENQUEUE_SLEEP_SECONDS` (default `0.2`)
- `MAX_ENQUEUED_JOBS` (default `10000`)

Backfill safeguards:

- `BACKFILL_MAX_USERS` (default `50`)
- `BACKFILL_CONFIRM=1` (alternative to `--confirm`)

Global duplicate-run locks (optional overrides):

- `SCHEDULER_DAILY_LOCK_ID`
- `SCHEDULER_BACKFILL_LOCK_ID`

## Operational Notes

The scheduler inserts `sync_jobs` rows before enqueueing Celery tasks. If the scheduler exits after inserts but before enqueue completes, those jobs will remain `PENDING`.

This is not fatal (re-running the scheduler will enqueue a new set of jobs), but it can confuse completeness queries unless keying by `partition_key`.

To diagnose a partially enqueued tick, look for old `PENDING` rows for the partition:

```sql
SELECT COUNT(*) AS pending_old
FROM sync_jobs
WHERE triggered_by = 'scheduler.daily'
  AND partition_key = 'daily:YYYY-MM-DD'
  AND status = 'PENDING'
  AND created_at < CURRENT_TIMESTAMP - INTERVAL '10 minutes';
```

## CLI Options

Daily:

- `--enqueue-batch-size`, `--enqueue-sleep-seconds`, `--max-enqueued-jobs`

Backfill:

- `--backfill-days`
- `--user-ids`, `--github-logins`
- `--allow-missing`
- `--partition-key`, `--campaign`
- `--backfill-max-users`, `--confirm` (`--confirm` required when selected users exceed `--backfill-max-users` / `BACKFILL_MAX_USERS`)
- `--enqueue-batch-size`, `--enqueue-sleep-seconds`, `--max-enqueued-jobs`
