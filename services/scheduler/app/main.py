import argparse
import copy
import datetime
import logging
import os
import time
import uuid
from contextlib import contextmanager

from celery import Celery
from sqlalchemy import bindparam, text

from services.shared.celery_config import CELERY_DEFAULT_QUEUE, CELERY_TASK_ROUTES
from services.shared.config import REDIS_URL, validate_config
from services.shared.database import ENGINE, db_session


logger = logging.getLogger("scheduler")

DEFAULT_SCHEDULER_DAILY_LOCK_ID = 0x7761766501  # 0x77617665 is "wave" in ASCII
DEFAULT_SCHEDULER_BACKFILL_LOCK_ID = 0x7761766502
DEFAULT_BACKFILL_MAX_USERS = 50
DEFAULT_ENQUEUE_BATCH_SIZE = 200
DEFAULT_ENQUEUE_SLEEP_SECONDS = 0.2
DEFAULT_MAX_ENQUEUED_JOBS = 10_000


def _env_int(key, default):
    raw = os.getenv(key)
    if raw is None or str(raw).strip() == "":
        return default
    return int(str(raw).strip())


def _env_float(key, default):
    raw = os.getenv(key)
    if raw is None or str(raw).strip() == "":
        return default
    return float(str(raw).strip())


def _utc_date_string():
    return str(datetime.datetime.now(datetime.timezone.utc).date())


def _create_celery_client():
    app = Celery(
        "wave-metrics-scheduler",
        broker=REDIS_URL,
    )
    app.conf.task_default_queue = CELERY_DEFAULT_QUEUE
    app.conf.task_routes = copy.deepcopy(CELERY_TASK_ROUTES)
    return app


def _truncate_error_message(value, max_chars=500):
    normalized = str(value or "").strip()
    if len(normalized) > max_chars:
        return normalized[:max_chars]
    return normalized


@contextmanager
def _scheduler_run_lock(lock_id, mode_label):
    """
    Acquire a global scheduler lock for the duration of a run

    Uses PostgreSQL advisory locks when available. For non-Postgres dialects
    (e.g., SQLite in certain test contexts), locking is skipped

    Args:
        lock_id (int): Advisory lock ID
        mode_label (str): Mode label for logging ('daily' or 'backfill')

    Yields:
        bool whether the lock is held (or skipped and treated as held)
    """
    if ENGINE.dialect.name != "postgresql":
        logger.warning("Scheduler lock skipped (non-Postgres dialect)", extra={"mode": mode_label})
        yield True
        return

    connection = ENGINE.connect()
    acquired = False
    body_exception = None
    try:
        acquired = bool(
            connection.execute(
                text("SELECT pg_try_advisory_lock(:lock_id)"),
                {"lock_id": int(lock_id)},
            ).scalar()
        )
        if not acquired:
            yield False
            return

        yield True
    except Exception as exc:
        body_exception = exc
        raise
    finally:
        try:
            if acquired:
                connection.execute(text("SELECT pg_advisory_unlock(:lock_id)"), {"lock_id": int(lock_id)})
        except Exception:
            logger.exception(
                "Failed to release scheduler advisory lock",
                extra={"lock_id": lock_id, "mode": mode_label},
            )
            if body_exception is None:
                raise
        finally:
            connection.close()


def _insert_sync_jobs(job_rows):
    insert_stmt = text(
        "INSERT INTO sync_jobs ("
        "job_id, user_id, github_login, status, error_message, backfill_days, triggered_by, partition_key"
        ") VALUES ("
        ":job_id, :user_id, :github_login, :status, :error_message, :backfill_days, :triggered_by, :partition_key"
        ")"
    )

    with db_session() as session:
        session.execute(insert_stmt, job_rows)


def _mark_enqueue_failed(job_id, detail):
    message = _truncate_error_message(f"enqueue_failed: {detail}")
    last_exc = None
    for attempt in range(3):
        try:
            with db_session() as session:
                session.execute(
                    text(
                        "UPDATE sync_jobs "
                        "SET status='FAILED', completed_at=CURRENT_TIMESTAMP, error_message=:error_message "
                        "WHERE job_id=:job_id AND stale_marked_at IS NULL"
                    ),
                    {"job_id": job_id, "error_message": message},
                )
            return
        except Exception as exc:
            last_exc = exc
            if attempt < 2:
                time.sleep(0.2)
    raise last_exc


def _enqueue_jobs(celery_app, task_name, task_args_builder, users, triggered_by, partition_key, backfill_days):
    """
    Insert sync_jobs rows and enqueue Celery tasks for a batch
    """
    jobs_inserted = 0
    jobs_enqueued = 0
    enqueue_failures = 0

    job_rows = []
    enqueue_specs = []
    for user_id, github_login in users:
        job_id = str(uuid.uuid4())
        job_rows.append(
            {
                "job_id": job_id,
                "user_id": str(user_id),
                "github_login": str(github_login),
                "status": "PENDING",
                "error_message": None,
                "backfill_days": backfill_days,
                "triggered_by": triggered_by,
                "partition_key": partition_key,
            }
        )
        enqueue_specs.append((job_id, task_args_builder(user_id)))

    if not job_rows:
        return jobs_inserted, jobs_enqueued, enqueue_failures

    _insert_sync_jobs(job_rows)
    jobs_inserted += len(job_rows)

    for job_id, (args, kwargs) in enqueue_specs:
        try:
            celery_app.send_task(task_name, args=args, kwargs=kwargs, task_id=job_id)
            jobs_enqueued += 1
        except Exception as exc:
            enqueue_failures += 1
            logger.exception(
                "Celery enqueue failed",
                extra={"job_id": job_id, "task_name": task_name, "error": type(exc).__name__},
            )
            try:
                _mark_enqueue_failed(job_id, f"{type(exc).__name__}: {str(exc)}")
            except Exception as mark_exc:
                logger.exception(
                    "Failed to mark enqueue failure in sync_jobs",
                    extra={"job_id": job_id, "task_name": task_name},
                )
                raise RuntimeError(f"Failed to mark enqueue failure job_id={job_id}") from mark_exc

    return jobs_inserted, jobs_enqueued, enqueue_failures


def _parse_csv_list(raw):
    if not raw:
        return []
    return [part.strip() for part in str(raw).split(",") if part.strip()]


def _resolve_backfill_users(user_ids, github_logins, allow_missing=False):
    """
    Resolve explicit backfill selectors into (user_id, github_login) tuples
    """
    normalized_logins = [
        str(login).strip().lower() for login in (github_logins or []) if str(login).strip()
    ]
    normalized_user_ids = [str(user_id).strip() for user_id in (user_ids or []) if str(user_id).strip()]

    resolved = {}

    with db_session() as session:
        if normalized_user_ids:
            stmt = text("SELECT id, github_login FROM users WHERE id IN :ids").bindparams(
                bindparam("ids", expanding=True)
            )
            for row in session.execute(stmt, {"ids": normalized_user_ids}).fetchall():
                resolved[str(row[0])] = str(row[1])

        if normalized_logins:
            stmt = text("SELECT id, github_login FROM users WHERE LOWER(github_login) IN :logins").bindparams(
                bindparam("logins", expanding=True)
            )
            for row in session.execute(stmt, {"logins": normalized_logins}).fetchall():
                resolved[str(row[0])] = str(row[1])

    resolved_user_ids = set(resolved.keys())
    resolved_logins_lower = {str(login).strip().lower() for login in resolved.values() if str(login).strip()}

    missing_user_ids = sorted(set(normalized_user_ids) - resolved_user_ids)
    missing_logins = sorted(set(normalized_logins) - resolved_logins_lower)

    if missing_user_ids or missing_logins:
        missing_user_ids_preview = missing_user_ids[:20]
        missing_logins_preview = missing_logins[:20]
        logger.warning(
            "Backfill selectors did not resolve to users",
            extra={
                "missing_user_ids": missing_user_ids_preview,
                "missing_user_ids_count": len(missing_user_ids),
                "missing_github_logins": missing_logins_preview,
                "missing_github_logins_count": len(missing_logins),
            },
        )
        if not allow_missing:
            parts = []
            if missing_user_ids:
                parts.append(f"missing user_ids={missing_user_ids}")
            if missing_logins:
                parts.append(f"missing github_logins={missing_logins}")
            details = "; ".join(parts)
            raise ValueError(
                f"Backfill selectors did not resolve to users ({details}). "
                "Pass --allow-missing to proceed"
            )

    return sorted(resolved.items(), key=lambda item: item[0])


def run_daily(args):
    """
    Enqueue one incremental refresh per user
    """
    started = time.monotonic()
    utc_day = _utc_date_string()
    partition_key = f"daily:{utc_day}"
    triggered_by = "scheduler.daily"

    enqueue_batch_size = int(args.enqueue_batch_size)
    enqueue_sleep_seconds = float(args.enqueue_sleep_seconds)
    max_enqueued_jobs = int(args.max_enqueued_jobs)

    celery_app = _create_celery_client()

    lock_id = _env_int("SCHEDULER_DAILY_LOCK_ID", DEFAULT_SCHEDULER_DAILY_LOCK_ID)

    users_total_selected = 0
    jobs_inserted = 0
    jobs_enqueued = 0
    enqueue_failures = 0

    with _scheduler_run_lock(lock_id, mode_label="daily") as lock_held:
        if not lock_held:
            logger.warning("Scheduler daily lock already held; exiting", extra={"lock_id": lock_id, "mode": "daily"})
            return

        logger.info(
            "Scheduler daily started",
            extra={
                "mode": "daily",
                "partition_key": partition_key,
            },
        )

        pending_batch = []
        page_size = max(enqueue_batch_size, 100)
        last_user_id = None

        while True:
            with db_session() as session:
                rows = session.execute(
                    text(
                        "SELECT id, github_login "
                        "FROM users "
                        "WHERE (:last_user_id IS NULL OR id > :last_user_id) "
                        "ORDER BY id "
                        "LIMIT :limit"
                    ),
                    {"limit": page_size, "last_user_id": last_user_id},
                ).fetchall()

            if not rows:
                break
            last_user_id = rows[-1][0]

            for user_id, github_login in rows:
                pending_batch.append((str(user_id), str(github_login)))
                users_total_selected += 1
                if users_total_selected > max_enqueued_jobs:
                    raise RuntimeError(f"MAX_ENQUEUED_JOBS exceeded ({max_enqueued_jobs})")

                if len(pending_batch) >= enqueue_batch_size:
                    inserted, enqueued, failures = _enqueue_jobs(
                        celery_app,
                        task_name="refresh_daily",
                        task_args_builder=lambda uid: (
                            [uid],
                            {"partition_key": partition_key, "triggered_by": triggered_by},
                        ),
                        users=pending_batch,
                        triggered_by=triggered_by,
                        partition_key=partition_key,
                        backfill_days=None,
                    )
                    jobs_inserted += inserted
                    jobs_enqueued += enqueued
                    enqueue_failures += failures

                    logger.info(
                        "Scheduler daily batch finished",
                        extra={
                            "mode": "daily",
                            "partition_key": partition_key,
                            "batch_size": inserted,
                            "jobs_inserted": jobs_inserted,
                            "jobs_enqueued": jobs_enqueued,
                            "enqueue_failures": enqueue_failures,
                        },
                    )

                    pending_batch = []
                    if enqueue_sleep_seconds > 0:
                        time.sleep(enqueue_sleep_seconds)

        if pending_batch:
            inserted, enqueued, failures = _enqueue_jobs(
                celery_app,
                task_name="refresh_daily",
                task_args_builder=lambda uid: (
                    [uid],
                    {"partition_key": partition_key, "triggered_by": triggered_by},
                ),
                users=pending_batch,
                triggered_by=triggered_by,
                partition_key=partition_key,
                backfill_days=None,
            )
            jobs_inserted += inserted
            jobs_enqueued += enqueued
            enqueue_failures += failures

            logger.info(
                "Scheduler daily batch finished",
                extra={
                    "mode": "daily",
                    "partition_key": partition_key,
                    "batch_size": inserted,
                    "jobs_inserted": jobs_inserted,
                    "jobs_enqueued": jobs_enqueued,
                    "enqueue_failures": enqueue_failures,
                },
            )

        logger.info(
            "Scheduler daily finished",
            extra={
                "mode": "daily",
                "partition_key": partition_key,
                "users_total_selected": users_total_selected,
                "jobs_inserted": jobs_inserted,
                "jobs_enqueued": jobs_enqueued,
                "enqueue_failures": enqueue_failures,
                "duration_seconds": round(time.monotonic() - started, 3),
            },
        )


def run_backfill(args):
    """
    Enqueue backfill tasks for an explicit user subset
    """
    started = time.monotonic()
    utc_day = _utc_date_string()
    triggered_by = "scheduler.backfill"

    backfill_days = int(args.backfill_days)
    if backfill_days < 1:
        raise ValueError("backfill_days must be >= 1")

    campaign = str(args.campaign or "").strip()
    if args.partition_key:
        partition_key = str(args.partition_key).strip()
    else:
        suffix = f":{campaign}" if campaign else ""
        partition_key = f"backfill:{utc_day}:{backfill_days}d{suffix}"

    enqueue_batch_size = int(args.enqueue_batch_size)
    enqueue_sleep_seconds = float(args.enqueue_sleep_seconds)
    max_enqueued_jobs = int(args.max_enqueued_jobs)

    raw_user_ids = _parse_csv_list(args.user_ids)
    raw_github_logins = _parse_csv_list(args.github_logins)

    users = _resolve_backfill_users(raw_user_ids, raw_github_logins, allow_missing=bool(args.allow_missing))
    users_total_selected = len(users)

    if users_total_selected == 0:
        raise ValueError("No users selected (provide --user-ids and/or --github-logins)")

    if users_total_selected > max_enqueued_jobs:
        raise RuntimeError(f"MAX_ENQUEUED_JOBS exceeded ({max_enqueued_jobs})")

    backfill_max_users = int(args.backfill_max_users)
    confirmed = bool(args.confirm) or (str(os.getenv("BACKFILL_CONFIRM", "")).strip() == "1")
    if not confirmed and users_total_selected > backfill_max_users:
        raise RuntimeError(
            f"Backfill requires confirmation for {users_total_selected} users "
            f"(max without confirm is {backfill_max_users})"
        )

    celery_app = _create_celery_client()

    lock_id = _env_int("SCHEDULER_BACKFILL_LOCK_ID", DEFAULT_SCHEDULER_BACKFILL_LOCK_ID)

    jobs_inserted = 0
    jobs_enqueued = 0
    enqueue_failures = 0

    with _scheduler_run_lock(lock_id, mode_label="backfill") as lock_held:
        if not lock_held:
            logger.warning(
                "Scheduler backfill lock already held; exiting",
                extra={"lock_id": lock_id, "mode": "backfill"},
            )
            return

        logger.info(
            "Scheduler backfill started",
            extra={
                "mode": "backfill",
                "partition_key": partition_key,
                "backfill_days": backfill_days,
                "users_total_selected": users_total_selected,
            },
        )

        for i in range(0, len(users), enqueue_batch_size):
            batch = users[i : i + enqueue_batch_size]
            inserted, enqueued, failures = _enqueue_jobs(
                celery_app,
                task_name="backfill_user",
                task_args_builder=lambda uid: (
                    [uid, backfill_days],
                    {"partition_key": partition_key, "triggered_by": triggered_by},
                ),
                users=batch,
                triggered_by=triggered_by,
                partition_key=partition_key,
                backfill_days=backfill_days,
            )
            jobs_inserted += inserted
            jobs_enqueued += enqueued
            enqueue_failures += failures

            logger.info(
                "Scheduler backfill batch finished",
                extra={
                    "mode": "backfill",
                    "partition_key": partition_key,
                    "backfill_days": backfill_days,
                    "batch_size": inserted,
                    "jobs_inserted": jobs_inserted,
                    "jobs_enqueued": jobs_enqueued,
                    "enqueue_failures": enqueue_failures,
                },
            )

            if enqueue_sleep_seconds > 0 and (i + enqueue_batch_size) < len(users):
                time.sleep(enqueue_sleep_seconds)

        logger.info(
            "Scheduler backfill finished",
            extra={
                "mode": "backfill",
                "partition_key": partition_key,
                "backfill_days": backfill_days,
                "users_total_selected": users_total_selected,
                "jobs_inserted": jobs_inserted,
                "jobs_enqueued": jobs_enqueued,
                "enqueue_failures": enqueue_failures,
                "duration_seconds": round(time.monotonic() - started, 3),
            },
        )


def build_arg_parser():
    parser = argparse.ArgumentParser(prog="wave-metrics-scheduler")
    subparsers = parser.add_subparsers(dest="mode", required=True)

    daily = subparsers.add_parser("daily", help="Enqueue one daily refresh per user")
    daily.add_argument(
        "--enqueue-batch-size",
        default=_env_int("ENQUEUE_BATCH_SIZE", DEFAULT_ENQUEUE_BATCH_SIZE),
    )
    daily.add_argument(
        "--enqueue-sleep-seconds",
        default=_env_float("ENQUEUE_SLEEP_SECONDS", DEFAULT_ENQUEUE_SLEEP_SECONDS),
    )
    daily.add_argument(
        "--max-enqueued-jobs",
        default=_env_int("MAX_ENQUEUED_JOBS", DEFAULT_MAX_ENQUEUED_JOBS),
    )

    backfill = subparsers.add_parser("backfill", help="Enqueue backfill tasks for a subset of users")
    backfill.add_argument("--backfill-days", required=True)
    backfill.add_argument("--user-ids", default="")
    backfill.add_argument("--github-logins", default="")
    backfill.add_argument("--partition-key", default="")
    backfill.add_argument("--campaign", default="")
    backfill.add_argument("--confirm", action="store_true")
    backfill.add_argument(
        "--allow-missing",
        action="store_true",
        help="Proceed even if some provided --user-ids/--github-logins don't resolve to users",
    )
    backfill.add_argument(
        "--backfill-max-users",
        default=_env_int("BACKFILL_MAX_USERS", DEFAULT_BACKFILL_MAX_USERS),
    )
    backfill.add_argument(
        "--enqueue-batch-size",
        default=_env_int("ENQUEUE_BATCH_SIZE", DEFAULT_ENQUEUE_BATCH_SIZE),
    )
    backfill.add_argument(
        "--enqueue-sleep-seconds",
        default=_env_float("ENQUEUE_SLEEP_SECONDS", DEFAULT_ENQUEUE_SLEEP_SECONDS),
    )
    backfill.add_argument(
        "--max-enqueued-jobs",
        default=_env_int("MAX_ENQUEUED_JOBS", DEFAULT_MAX_ENQUEUED_JOBS),
    )

    return parser


def main():
    logging.basicConfig(level=str(os.getenv("LOG_LEVEL", "INFO")).upper())
    validate_config()

    parser = build_arg_parser()
    args = parser.parse_args()

    if args.mode == "daily":
        run_daily(args)
        return

    if args.mode == "backfill":
        run_backfill(args)
        return

    raise ValueError(f"Unknown mode: {args.mode}")


if __name__ == "__main__":
    main()
