import logging
import time

from celery import shared_task
from sqlalchemy import text

from services.shared import config as shared_config
from services.shared.database import db_session
from services.shared.github_client import fetch_user_login
from services.shared.pipeline import ingest_and_compute_user
from services.shared.token_store import finalize_github_token_ref, lease_github_token_ref


logger = logging.getLogger("worker.tasks")

_MAX_ERROR_MESSAGE_CHARS = 500


def _truncate_error_message(value):
    """
    Truncate error messages to a safe length for storage/logging

    Args:
        value (str): Error message

    Returns:
        str truncated message
    """
    normalized = str(value or "").strip()
    if len(normalized) > _MAX_ERROR_MESSAGE_CHARS:
        return normalized[:_MAX_ERROR_MESSAGE_CHARS]
    return normalized


def _derive_terminal_job_fields_from_pipeline_result(result):
    """
    Map pipeline result dict to sync_jobs terminal status + error_message

    Pipeline status values and their job status mappings:
        - 'ok'           -> COMPLETED, None
        - 'locked'       -> SKIPPED, "locked"
        - 'missing_token'-> FAILED, "missing_token[: details]"
        - 'token_invalid'-> FAILED, "token_invalid[: details]"
        - empty/missing  -> FAILED, "unexpected_pipeline_status:<empty>"
        - unexpected     -> FAILED, "unexpected_pipeline_status:<value>"
        - non-dict       -> FAILED, "unexpected_pipeline_status:non_dict"

    Args:
        result (Any): Pipeline result

    Returns:
        Tuple of (job_status, error_message)
    """
    if not isinstance(result, dict):
        return "FAILED", _truncate_error_message("unexpected_pipeline_status:non_dict")

    pipeline_status = str(result.get("status") or "").strip().lower()
    if pipeline_status == "ok":
        return "COMPLETED", None

    if pipeline_status == "locked":
        return "SKIPPED", "locked"

    if pipeline_status in {"missing_token", "token_invalid"}:
        error = str(result.get("error") or "").strip()
        if not error:
            return "FAILED", pipeline_status
        return "FAILED", _truncate_error_message(f"{pipeline_status}: {error}")

    if not pipeline_status:
        return "FAILED", "unexpected_pipeline_status:<empty>"

    error = str(result.get("error") or "").strip()
    base = f"unexpected_pipeline_status:{pipeline_status}"
    if not error:
        return "FAILED", _truncate_error_message(base)
    return "FAILED", _truncate_error_message(f"{base}: {error}")


def _mark_job_running(job_id, task_id, user_id, task_name):
    if not job_id:
        return

    try:
        with db_session() as session:
            updated = session.execute(
                text(
                    "UPDATE sync_jobs "
                    "SET status='RUNNING', started_at=COALESCE(started_at, CURRENT_TIMESTAMP) "
                    "WHERE job_id=:job_id AND status='PENDING' AND stale_marked_at IS NULL"
                ),
                {"job_id": job_id},
            )
            if (updated.rowcount or 0) == 0:
                logger.warning(
                    "%s: missing or non-PENDING job row when marking RUNNING",
                    task_name,
                    extra={"celery_task_id": task_id, "job_id": job_id, "user_id": user_id},
                )
    except Exception:
        logger.exception(
            "%s: failed to mark job RUNNING",
            task_name,
            extra={"celery_task_id": task_id, "job_id": job_id, "user_id": user_id},
        )
        raise


def _finalize_job_from_pipeline_result(job_id, task_id, user_id, task_name, result):
    if not job_id:
        return

    job_status, job_error_message = _derive_terminal_job_fields_from_pipeline_result(result)

    canonical_user_id = user_id
    sync_run_id = None
    compute_run_id = None
    baseline_id = None
    window_start = None
    window_end = None

    if isinstance(result, dict):
        canonical_user_id = result.get("user_id") or user_id
        sync_run_id = result.get("sync_run_id")
        compute_run_id = result.get("compute_run_id")
        baseline_id = result.get("baseline_id")
        window_start = result.get("window_start")
        window_end = result.get("window_end")

    try:
        with db_session() as session:
            updated = session.execute(
                text(
                    "UPDATE sync_jobs "
                    "SET status=:status, "
                    "started_at=COALESCE(started_at, CURRENT_TIMESTAMP), "
                    "completed_at=CURRENT_TIMESTAMP, "
                    "error_message=:error_message, "
                    "user_id=:user_id, "
                    "sync_run_id=:sync_run_id, compute_run_id=:compute_run_id, baseline_id=:baseline_id, "
                    "window_start=:window_start, window_end=:window_end "
                    "WHERE job_id=:job_id AND stale_marked_at IS NULL"
                ),
                {
                    "job_id": job_id,
                    "user_id": canonical_user_id,
                    "sync_run_id": sync_run_id,
                    "compute_run_id": compute_run_id,
                    "baseline_id": baseline_id,
                    "window_start": window_start,
                    "window_end": window_end,
                    "status": job_status,
                    "error_message": job_error_message,
                },
            )
            if (updated.rowcount or 0) == 0:
                logger.warning(
                    "%s: missing job row when marking terminal status",
                    task_name,
                    extra={
                        "celery_task_id": task_id,
                        "job_id": job_id,
                        "user_id": user_id,
                        "status": job_status,
                    },
                )
    except Exception:
        logger.exception(
            "%s: failed to mark job terminal status",
            task_name,
            extra={
                "celery_task_id": task_id,
                "job_id": job_id,
                "user_id": user_id,
                "status": job_status,
                "error_message": job_error_message,
            },
        )
        raise


def _best_effort_mark_job_failed(job_id, task_id, user_id, task_name, error):
    if not job_id:
        return

    error_message = _truncate_error_message(f"{type(error).__name__}: {str(error)}")

    try:
        with db_session() as session:
            updated = session.execute(
                text(
                    "UPDATE sync_jobs "
                    "SET status='FAILED', "
                    "started_at=COALESCE(started_at, CURRENT_TIMESTAMP), "
                    "completed_at=CURRENT_TIMESTAMP, "
                    "error_message=:error "
                    "WHERE job_id=:job_id AND stale_marked_at IS NULL"
                ),
                {"job_id": job_id, "error": error_message},
            )
            if (updated.rowcount or 0) == 0:
                logger.warning(
                    "%s: missing job row when marking FAILED",
                    task_name,
                    extra={"celery_task_id": task_id, "job_id": job_id, "user_id": user_id},
                )
    except Exception:
        logger.exception(
            "%s: failed to mark job FAILED",
            task_name,
            extra={"celery_task_id": task_id, "job_id": job_id, "user_id": user_id},
        )


@shared_task(name="sync_and_compute", bind=True)
def sync_and_compute(self, user_id, token_ref, backfill_days=None, triggered_by=None):
    """
    Celery wrapper delegating to the shared ingestion + compute pipeline

    Args:
        user_id (str): Internal user UUID
        token_ref (str): Short-lived GitHub token reference UUID
        backfill_days (int): Optional lookback window
        triggered_by (str): Optional origin label such as 'api' or 'worker'

    Returns:
        dict with summary of the run
    """
    started = time.monotonic()
    task_id = getattr(getattr(self, "request", None), "id", None)
    job_id = task_id
    lease_id = task_id or token_ref

    finalize_success = False
    task_name = "sync_and_compute"
    token_ref_leased = False

    logger.info(
        "sync_and_compute started",
        extra={
            "celery_task_id": task_id,
            "user_id": user_id,
            "token_ref": token_ref,
            "triggered_by": triggered_by,
        },
    )

    result = None
    github_token = None
    requested_login = None
    pipeline_status = None
    pipeline_error = None
    sync_run_id = None
    compute_run_id = None
    baseline_id = None
    window_start = None
    window_end = None
    primary_exception = None

    try:
        try:
            _mark_job_running(job_id, task_id=task_id, user_id=user_id, task_name=task_name)

            with db_session() as session:
                github_token = lease_github_token_ref(
                    session,
                    token_ref,
                    expected_user_id=user_id,
                    lease_id=lease_id,
                )
                token_ref_leased = True

                if job_id:
                    row = session.execute(
                        text("SELECT github_login FROM sync_jobs WHERE job_id=:job_id"),
                        {"job_id": job_id},
                    ).fetchone()
                    if row and row[0]:
                        requested_login = str(row[0])
        except Exception as exc:
            primary_exception = exc
            logger.exception(
                "sync_and_compute failed to prepare",
                extra={
                    "celery_task_id": task_id,
                    "job_id": job_id,
                    "user_id": user_id,
                    "error": type(exc).__name__,
                },
            )
            _best_effort_mark_job_failed(job_id, task_id=task_id, user_id=user_id, task_name=task_name, error=exc)
            raise

        try:
            if requested_login:
                viewer_login, _viewer = fetch_user_login(github_token)
                normalized_requested = str(requested_login).strip().lower()
                normalized_viewer = str(viewer_login or "").strip().lower()

                if normalized_viewer and normalized_requested != normalized_viewer:
                    error_msg = (
                        "github_login does not match token owner "
                        f"(expected {normalized_requested}, got {normalized_viewer})"
                    )

                    _best_effort_mark_job_failed(
                        job_id,
                        task_id=task_id,
                        user_id=user_id,
                        task_name=task_name,
                        error=RuntimeError(error_msg),
                    )

                    finalize_success = True
                    github_token = None
                    logger.info(
                        "sync_and_compute finished (login mismatch)",
                        extra={"celery_task_id": task_id, "job_id": job_id, "user_id": user_id},
                    )
                    return {"status": "failed", "error": error_msg}

            result = ingest_and_compute_user(
                user_id=user_id,
                github_token=github_token,
                backfill_days=backfill_days,
                partition_key=None,
                triggered_by=triggered_by or "api",
                persist_github_token=shared_config.TOKEN_VAULT_ENABLED,
            )

            if isinstance(result, dict):
                pipeline_status = str(result.get("status") or "").strip().lower()
                pipeline_error = result.get("error")
                sync_run_id = result.get("sync_run_id")
                compute_run_id = result.get("compute_run_id")
                baseline_id = result.get("baseline_id")
                window_start = result.get("window_start")
                window_end = result.get("window_end")

            _finalize_job_from_pipeline_result(
                job_id,
                task_id=task_id,
                user_id=user_id,
                task_name=task_name,
                result=result,
            )
            job_status, job_error_message = _derive_terminal_job_fields_from_pipeline_result(result)

            if pipeline_status == "locked":
                logger.info(
                    "sync_and_compute finished: per-user lock held",
                    extra={"celery_task_id": task_id, "job_id": job_id, "user_id": user_id},
                )
            elif job_status != "COMPLETED":
                logger.warning(
                    "sync_and_compute finished with non-ok pipeline status",
                    extra={
                        "celery_task_id": task_id,
                        "job_id": job_id,
                        "user_id": user_id,
                        "pipeline_status": pipeline_status,
                        "pipeline_error": _truncate_error_message(pipeline_error) if pipeline_error else None,
                        "status": job_status,
                        "error_message": job_error_message,
                    },
                )

            finalize_success = True
            return result
        except Exception as exc:
            primary_exception = exc
            _best_effort_mark_job_failed(job_id, task_id=task_id, user_id=user_id, task_name=task_name, error=exc)
            raise
    finally:
        finalize_exception = None
        if token_ref_leased:
            try:
                finalize_github_token_ref(None, token_ref, lease_id=lease_id, success=finalize_success)
            except Exception as exc:
                finalize_exception = exc
                logger.exception(
                    "sync_and_compute failed to finalize token_ref",
                    extra={
                        "celery_task_id": task_id,
                        "job_id": job_id,
                        "user_id": user_id,
                        "token_ref": token_ref,
                        "lease_id": lease_id,
                    },
                )

        github_token = None
        logger.info(
            "sync_and_compute finished",
            extra={
                "celery_task_id": task_id,
                "job_id": job_id,
                "user_id": user_id,
                "github_login": (result or {}).get("user") if isinstance(result, dict) else None,
                "task_name": task_name,
                "triggered_by": triggered_by or "api",
                "partition_key": None,
                "backfill_days": backfill_days,
                "pipeline_status": pipeline_status,
                "pipeline_error": _truncate_error_message(pipeline_error) if pipeline_error else None,
                "sync_run_id": sync_run_id,
                "compute_run_id": compute_run_id,
                "baseline_id": baseline_id,
                "window_start": window_start,
                "window_end": window_end,
                "duration_seconds": round(time.monotonic() - started, 3),
            },
        )

        if finalize_exception is not None and primary_exception is None:
            raise finalize_exception


@shared_task(bind=True, name="refresh_daily")
def refresh_daily(self, user_id, partition_key=None, triggered_by=None):
    """
    Enqueue-safe daily refresh task using vault-sourced tokens

    Args:
        user_id (str): Internal user UUID
        partition_key (str): Optional grouping key (e.g. 'daily:YYYY-MM-DD')
        triggered_by (str): Optional origin label such as 'scheduler.daily'

    Returns:
        dict with summary of the run
    """
    started = time.monotonic()
    task_id = getattr(getattr(self, "request", None), "id", None)
    job_id = task_id
    task_name = "refresh_daily"

    result = None
    pipeline_status = None
    pipeline_error = None
    sync_run_id = None
    compute_run_id = None
    baseline_id = None
    window_start = None
    window_end = None

    logger.info(
        "refresh_daily started",
        extra={
            "celery_task_id": task_id,
            "job_id": job_id,
            "user_id": user_id,
            "partition_key": partition_key,
            "triggered_by": triggered_by,
        },
    )

    try:
        _mark_job_running(job_id, task_id=task_id, user_id=user_id, task_name=task_name)
        result = ingest_and_compute_user(
            user_id=user_id,
            github_token=None,
            backfill_days=None,
            partition_key=partition_key,
            triggered_by=triggered_by,
            persist_github_token=False,
        )
        if isinstance(result, dict):
            pipeline_status = str(result.get("status") or "").strip().lower()
            pipeline_error = result.get("error")
            sync_run_id = result.get("sync_run_id")
            compute_run_id = result.get("compute_run_id")
            baseline_id = result.get("baseline_id")
            window_start = result.get("window_start")
            window_end = result.get("window_end")
        _finalize_job_from_pipeline_result(
            job_id,
            task_id=task_id,
            user_id=user_id,
            task_name=task_name,
            result=result,
        )
        return result
    except Exception as exc:
        _best_effort_mark_job_failed(job_id, task_id=task_id, user_id=user_id, task_name=task_name, error=exc)
        raise
    finally:
        logger.info(
            "refresh_daily finished",
            extra={
                "celery_task_id": task_id,
                "job_id": job_id,
                "user_id": user_id,
                "github_login": (result or {}).get("user") if isinstance(result, dict) else None,
                "task_name": task_name,
                "triggered_by": triggered_by,
                "partition_key": partition_key,
                "backfill_days": None,
                "pipeline_status": pipeline_status,
                "pipeline_error": _truncate_error_message(pipeline_error) if pipeline_error else None,
                "sync_run_id": sync_run_id,
                "compute_run_id": compute_run_id,
                "baseline_id": baseline_id,
                "window_start": window_start,
                "window_end": window_end,
                "duration_seconds": round(time.monotonic() - started, 3),
            },
        )


@shared_task(bind=True, name="backfill_user")
def backfill_user(self, user_id, backfill_days, partition_key=None, triggered_by=None):
    """
    Enqueue-safe backfill task using vault-sourced tokens

    Args:
        user_id (str): Internal user UUID
        backfill_days (int): Lookback days for ingestion window
        partition_key (str): Optional grouping key for the campaign
        triggered_by (str): Optional origin label such as 'scheduler.backfill'

    Returns:
        dict with summary of the run
    """
    started = time.monotonic()
    task_id = getattr(getattr(self, "request", None), "id", None)
    job_id = task_id
    task_name = "backfill_user"

    result = None
    pipeline_status = None
    pipeline_error = None
    sync_run_id = None
    compute_run_id = None
    baseline_id = None
    window_start = None
    window_end = None

    logger.info(
        "backfill_user started",
        extra={
            "celery_task_id": task_id,
            "job_id": job_id,
            "user_id": user_id,
            "backfill_days": backfill_days,
            "partition_key": partition_key,
            "triggered_by": triggered_by,
        },
    )

    try:
        _mark_job_running(job_id, task_id=task_id, user_id=user_id, task_name=task_name)
        result = ingest_and_compute_user(
            user_id=user_id,
            github_token=None,
            backfill_days=backfill_days,
            partition_key=partition_key,
            triggered_by=triggered_by,
            persist_github_token=False,
        )
        if isinstance(result, dict):
            pipeline_status = str(result.get("status") or "").strip().lower()
            pipeline_error = result.get("error")
            sync_run_id = result.get("sync_run_id")
            compute_run_id = result.get("compute_run_id")
            baseline_id = result.get("baseline_id")
            window_start = result.get("window_start")
            window_end = result.get("window_end")
        _finalize_job_from_pipeline_result(
            job_id,
            task_id=task_id,
            user_id=user_id,
            task_name=task_name,
            result=result,
        )
        return result
    except Exception as exc:
        _best_effort_mark_job_failed(job_id, task_id=task_id, user_id=user_id, task_name=task_name, error=exc)
        raise
    finally:
        logger.info(
            "backfill_user finished",
            extra={
                "celery_task_id": task_id,
                "job_id": job_id,
                "user_id": user_id,
                "github_login": (result or {}).get("user") if isinstance(result, dict) else None,
                "task_name": task_name,
                "triggered_by": triggered_by,
                "partition_key": partition_key,
                "backfill_days": backfill_days,
                "pipeline_status": pipeline_status,
                "pipeline_error": _truncate_error_message(pipeline_error) if pipeline_error else None,
                "sync_run_id": sync_run_id,
                "compute_run_id": compute_run_id,
                "baseline_id": baseline_id,
                "window_start": window_start,
                "window_end": window_end,
                "duration_seconds": round(time.monotonic() - started, 3),
            },
        )
