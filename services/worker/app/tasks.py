import logging

from celery import shared_task
from sqlalchemy import text

from services.shared import config as shared_config
from services.shared.database import db_session
from services.shared.github_client import fetch_user_login
from services.shared.pipeline import ingest_and_compute_user
from services.shared.token_store import delete_github_token_ref, finalize_github_token_ref, lease_github_token_ref


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
        - 'locked'       -> SKIPPED, "Skipped: per-user lock held"
        - 'missing_token'/'token_invalid'/'failed' -> FAILED, error from result (or a fallback)
        - empty/missing  -> FAILED, "Pipeline returned an empty status"
        - unexpected     -> FAILED, "Unexpected pipeline status=..."
        - non-dict       -> FAILED, "Pipeline returned a non-dict result"

    Args:
        result (Any): Pipeline result

    Returns:
        Tuple of (job_status, error_message)
    """
    if not isinstance(result, dict):
        return "FAILED", _truncate_error_message("Pipeline returned a non-dict result")

    pipeline_status = str(result.get("status") or "").strip().lower()
    if pipeline_status == "ok":
        return "COMPLETED", None

    if pipeline_status == "locked":
        return "SKIPPED", "Skipped: per-user lock held"

    if pipeline_status in {"missing_token", "token_invalid", "failed"}:
        error = result.get("error") or f"Pipeline returned status={pipeline_status}"
        return "FAILED", _truncate_error_message(error)

    if not pipeline_status:
        return "FAILED", _truncate_error_message("Pipeline returned an empty status")

    return "FAILED", _truncate_error_message(f"Unexpected pipeline status={pipeline_status}")


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
    task_id = getattr(getattr(self, "request", None), "id", None)
    job_id = task_id
    lease_id = task_id or token_ref

    finalize_called = False
    finalize_success = False

    logger.info(
        "sync_and_compute started",
        extra={
            "celery_task_id": task_id,
            "user_id": user_id,
            "token_ref": token_ref,
            "triggered_by": triggered_by,
        },
    )

    def _best_effort_mark_job_failed(error) -> None:
        if not job_id:
            return

        error_message = _truncate_error_message(f"{type(error).__name__}: {str(error)}")

        try:
            with db_session() as session:
                updated = session.execute(
                    text(
                        "UPDATE sync_jobs "
                        "SET status='FAILED', completed_at=NOW(), error_message=:error "
                        "WHERE job_id=:job_id AND stale_marked_at IS NULL"
                    ),
                    {"job_id": job_id, "error": error_message},
                )
                if (updated.rowcount or 0) == 0:
                    logger.warning(
                        "sync_and_compute: missing job row when marking FAILED (prepare error)",
                        extra={"celery_task_id": task_id, "job_id": job_id, "user_id": user_id},
                    )
        except Exception:
            logger.exception(
                "sync_and_compute: failed to mark job FAILED (prepare error)",
                extra={"celery_task_id": task_id, "job_id": job_id, "user_id": user_id},
            )

    try:
        with db_session() as session:
            if job_id:
                updated = session.execute(
                    text(
                        "UPDATE sync_jobs "
                        "SET status='RUNNING', started_at=COALESCE(started_at, NOW()) "
                        "WHERE job_id=:job_id AND status='PENDING' AND stale_marked_at IS NULL"
                    ),
                    {"job_id": job_id},
                )
                if (updated.rowcount or 0) == 0:
                    logger.warning(
                        "sync_and_compute: missing or non-PENDING job row when marking RUNNING",
                        extra={"celery_task_id": task_id, "job_id": job_id, "user_id": user_id},
                    )

            github_token = lease_github_token_ref(
                session,
                token_ref,
                expected_user_id=user_id,
                lease_id=lease_id,
            )

            requested_login = None
            if job_id:
                row = session.execute(
                    text("SELECT github_login FROM sync_jobs WHERE job_id=:job_id"),
                    {"job_id": job_id},
                ).fetchone()
                if row and row[0]:
                    requested_login = str(row[0])
    except Exception as exc:
        logger.exception(
            "sync_and_compute failed to prepare",
            extra={"celery_task_id": task_id, "job_id": job_id, "user_id": user_id, "error": type(exc).__name__},
        )
        _best_effort_mark_job_failed(exc)
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

                _best_effort_mark_job_failed(RuntimeError(error_msg))

                try:
                    delete_github_token_ref(None, token_ref)
                except Exception:
                    logger.exception(
                        "sync_and_compute: failed to delete token_ref after login mismatch",
                        extra={"celery_task_id": task_id, "job_id": job_id,
                               "user_id": user_id, "token_ref": token_ref},
                    )

                finalize_called = True
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

        job_status, job_error_message = _derive_terminal_job_fields_from_pipeline_result(result)
        pipeline_status = None
        pipeline_error = None
        if isinstance(result, dict):
            pipeline_status = str(result.get("status") or "").strip().lower()
            pipeline_error = result.get("error")

        if job_id:
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
                            "SET status=:status, completed_at=NOW(), error_message=:error_message, "
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
                            "sync_and_compute: missing job row when marking terminal status",
                            extra={
                                "celery_task_id": task_id,
                                "job_id": job_id,
                                "user_id": user_id,
                                "status": job_status,
                            },
                        )
            except Exception:
                logger.exception(
                    "sync_and_compute: failed to mark job terminal status",
                    extra={
                        "celery_task_id": task_id,
                        "job_id": job_id,
                        "user_id": user_id,
                        "status": job_status,
                        "error_message": job_error_message,
                    },
                )

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
        if job_id:
            error_message = _truncate_error_message(f"{type(exc).__name__}: {str(exc)}")
            try:
                with db_session() as session:
                    updated = session.execute(
                        text(
                            "UPDATE sync_jobs "
                            "SET status='FAILED', completed_at=NOW(), error_message=:error "
                            "WHERE job_id=:job_id AND stale_marked_at IS NULL"
                        ),
                        {"job_id": job_id, "error": error_message},
                    )
                    if (updated.rowcount or 0) == 0:
                        logger.warning(
                            "sync_and_compute: missing job row when marking FAILED",
                            extra={"celery_task_id": task_id, "job_id": job_id, "user_id": user_id},
                        )
            except Exception:
                logger.exception(
                    "sync_and_compute: failed to mark job FAILED",
                    extra={"celery_task_id": task_id, "job_id": job_id, "user_id": user_id},
                )
        raise
    finally:
        if not finalize_called:
            finalize_github_token_ref(None, token_ref, lease_id=lease_id, success=finalize_success)

        github_token = None
        logger.info(
            "sync_and_compute finished",
            extra={
                "celery_task_id": task_id,
                "job_id": job_id,
                "user_id": user_id,
            },
        )
