import logging
import os
import re
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from celery import Celery
from kombu.exceptions import OperationalError
from fastapi import FastAPI, Depends, Header, HTTPException, Query
from fastapi.responses import JSONResponse
from sqlalchemy import text

from services.shared.caching import get_cached_metrics, set_cached_metrics, get_redis
from services.shared import config as shared_config
from services.shared.config import (
    GITHUB_LOGIN_RESERVATION_TTL_SECONDS,
    HEALTH_CHECK_BROKER,
    HEALTH_CHECK_BROKER_TIMEOUT_SECONDS,
    REDIS_URL,
    RUNNING_JOB_STALE_SECONDS,
    SERVICE_VERSION,
    validate_config,
)
from services.shared.database import get_session, db_session, apply_pending_migrations
from services.shared.github_client import fetch_user_login
from services.shared.metric_definitions import get_metric_description
from services.shared.percentiles import percentile_bin
from services.shared.token_store import create_github_token_ref, delete_github_token_ref

from .schemas import MetricsResponse, SyncRequest
from .security import verify_api_auth_token

logger = logging.getLogger("api")

_GITHUB_LOGIN_RE = re.compile(r"^[a-z0-9-]{1,39}$")


def _as_float_or_none(value):
    """
    Convert numeric or None to float

    Args:
        value: Value to convert

    Returns:
        float or None
    """
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        logger.warning("_as_float_or_none failed value=%r error=%s", value, type(exc).__name__)
        return None


def _as_int_or_none(value):
    """
    Convert integer-like or None to int

    Args:
        value: Value to convert

    Returns:
        int or None
    """
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        logger.warning("_as_int_or_none failed value=%r error=%s", value, type(exc).__name__)
        return None


def _to_iso8601_z(dt: Optional[datetime]) -> Optional[str]:
    """
    Render datetime as ISO-8601 with a trailing Z when in UTC

    Args:
        dt (datetime): Datetime or None

    Returns:
        str or None
    """
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _normalize_github_login(value: Optional[str]) -> str:
    """
    Normalize a GitHub login

    Args:
        value (str): Raw GitHub login

    Returns:
        str normalized login (lowercased, trimmed) or "" when missing
    """
    if value is None:
        return ""
    return str(value).strip().lower()


def _require_valid_github_login(value: Optional[str]) -> str:
    normalized = _normalize_github_login(value)
    if not normalized:
        raise HTTPException(status_code=400, detail="github_login must not be empty")

    if not _GITHUB_LOGIN_RE.match(normalized):
        raise HTTPException(status_code=400, detail="github_login is invalid")

    return normalized


def _get_metrics_payload_by_user_id(user_id_str: str, db) -> Dict[str, Any]:
    cache_key = f"metrics:{user_id_str}"
    cached = get_cached_metrics(cache_key)
    if cached:
        return cached

    row = db.execute(
        text(
            "SELECT"
            " u.github_login,"
            " u.github_created_at,"
            " cm.metrics_window_start,"
            " cm.metrics_window_end,"
            " cm.baseline_start_date,"
            " cm.baseline_end_date,"
            " cm.total_opened_prs,"
            " cm.total_merged_prs,"
            " cm.pr_merge_rate,"
            " cm.pr_drop_rate,"
            " cm.avg_merge_latency_hours,"
            " cm.oss_prs_opened,"
            " cm.oss_reviews,"
            " cm.oss_issues_opened,"
            " cm.oss_composite_raw,"
            " cm.total_opened_prs_percentile,"
            " cm.total_merged_prs_percentile,"
            " cm.pr_merge_rate_percentile,"
            " cm.pr_drop_rate_percentile,"
            " cm.avg_merge_latency_hours_percentile,"
            " cm.oss_prs_opened_percentile,"
            " cm.oss_reviews_percentile,"
            " cm.oss_issues_opened_percentile,"
            " cm.oss_composite_percentile,"
            " cm.computed_at"
            " FROM users u"
            " JOIN contributor_metrics cm ON cm.user_id = u.id"
            " WHERE u.id = :user_id"
        ),
        {"user_id": user_id_str},
    ).mappings().fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Metrics not found")

    langs = db.execute(
        text(
            "SELECT language, pct"
            " FROM contributor_language_profile"
            " WHERE user_id = :user_id"
            " ORDER BY pct DESC"
        ),
        {"user_id": user_id_str},
    ).mappings().fetchall()

    github_login = str(row["github_login"])
    github_created_at = _to_iso8601_z(row["github_created_at"])
    metrics_window_start = _to_iso8601_z(row["metrics_window_start"])
    metrics_window_end = _to_iso8601_z(row["metrics_window_end"])
    baseline_start = str(row["baseline_start_date"])
    baseline_end = str(row["baseline_end_date"])

    total_opened_prs = _as_int_or_none(row["total_opened_prs"])
    total_merged_prs = _as_int_or_none(row["total_merged_prs"])
    pr_merge_rate = _as_float_or_none(row["pr_merge_rate"])
    pr_drop_rate = _as_float_or_none(row["pr_drop_rate"])
    avg_merge_latency_hours = _as_float_or_none(row["avg_merge_latency_hours"])
    oss_prs_opened = _as_int_or_none(row["oss_prs_opened"])
    oss_reviews = _as_int_or_none(row["oss_reviews"])
    oss_issues_opened = _as_int_or_none(row["oss_issues_opened"])
    oss_composite_raw = _as_float_or_none(row["oss_composite_raw"])

    def _metric_entry(metric_name, value, percentile_value):
        p = _as_float_or_none(percentile_value)
        return {
            "value": value,
            "percentile": p,
            "bin": percentile_bin(p) if p is not None else None,
            "description": get_metric_description(metric_name),
        }

    payload = {
        "user_id": user_id_str,
        "github_login": github_login,
        "github_created_at": github_created_at,
        "metrics_window_start": metrics_window_start,
        "metrics_window_end": metrics_window_end,
        "metrics_baseline_dates": {"start": baseline_start, "end": baseline_end},
        "metrics": {
            "total_opened_prs": _metric_entry(
                "total_opened_prs",
                total_opened_prs,
                row["total_opened_prs_percentile"],
            ),
            "total_merged_prs": _metric_entry(
                "total_merged_prs",
                total_merged_prs,
                row["total_merged_prs_percentile"],
            ),
            "pr_merge_rate": _metric_entry(
                "pr_merge_rate",
                pr_merge_rate,
                row["pr_merge_rate_percentile"],
            ),
            "pr_drop_rate": _metric_entry(
                "pr_drop_rate",
                pr_drop_rate,
                row["pr_drop_rate_percentile"],
            ),
            "avg_merge_latency_hours": _metric_entry(
                "avg_merge_latency_hours",
                avg_merge_latency_hours,
                row["avg_merge_latency_hours_percentile"],
            ),
            "oss_prs_opened": _metric_entry(
                "oss_prs_opened",
                oss_prs_opened,
                row["oss_prs_opened_percentile"],
            ),
            "oss_reviews": _metric_entry(
                "oss_reviews",
                oss_reviews,
                row["oss_reviews_percentile"],
            ),
            "oss_issues_opened": _metric_entry(
                "oss_issues_opened",
                oss_issues_opened,
                row["oss_issues_opened_percentile"],
            ),
            "oss_composite": _metric_entry(
                "oss_composite",
                oss_composite_raw,
                row["oss_composite_percentile"],
            ),
        },
        "lifetime_language_profile": [
            {
                "language": str(language_row["language"]),
                "pct": _as_float_or_none(language_row["pct"]) or 0.0,
            }
            for language_row in langs
        ],
        "computed_at": _to_iso8601_z(row["computed_at"]) or metrics_window_end,
    }

    set_cached_metrics(cache_key, payload)
    return payload


def _resolve_effective_backfill_days(user_id: str, explicit_backfill_days: Optional[int], session) -> Optional[int]:
    if explicit_backfill_days is not None:
        return explicit_backfill_days

    row = session.execute(
        text(
            "SELECT gss.last_pr_updated_at "
            "FROM users u "
            "LEFT JOIN github_sync_state gss ON gss.user_id = u.id "
            "WHERE u.id = :user_id"
        ),
        {"user_id": user_id},
    ).fetchone()

    if not row:
        return 1096

    last_pr_updated_at = row[0]
    return None if last_pr_updated_at is not None else 1096


def _resolve_sync_user_identity(body: SyncRequest, github_token: str, session) -> tuple[str, str]:
    """
    Resolve the canonical user identity for a /sync request

    Resolution precedence:
        1) If user_id is provided: treat as authoritative only if it exists in users
           - If it does not exist and github_login is provided, treat as a login-reservation request using that user_id
           - If it does not exist and github_login is not provided, fail fast with 400
        2) Else if github_login is provided: resolve without GitHub calls
           - If users.github_login matches (case-insensitive), use that users.id
           - Else if github_login_aliases has an active reservation/alias, use its user_id
           - Else create a new reservation row and use its user_id
        3) Else (token-only): call GitHub once, upsert users by github_user_id, and confirm github_login_aliases

    Returns:
        Tuple of (user_id, github_login)
    """
    requested_user_id = str(body.user_id) if body.user_id is not None else None
    requested_login = _normalize_github_login(body.github_login)

    if requested_user_id:
        row = session.execute(
            text("SELECT id, github_login FROM users WHERE id = :user_id"),
            {"user_id": requested_user_id},
        ).fetchone()

        if row:
            canonical_user_id = str(row[0])
            canonical_login = _normalize_github_login(row[1])

            if requested_login and requested_login != canonical_login:
                raise HTTPException(status_code=400, detail="user_id and github_login mismatch")

            return canonical_user_id, canonical_login

        if not requested_login:
            raise HTTPException(status_code=400, detail="user_id not found; provide github_login or omit user_id")

        expires_sql = "NOW() + (:ttl_seconds * INTERVAL '1 second')"

        existing = session.execute(
            text(
                "SELECT user_id, confirmed_at, expires_at "
                "FROM github_login_aliases "
                "WHERE github_login = :login"
            ),
            {"login": requested_login},
        ).fetchone()

        if existing:
            existing_user_id = str(existing[0])
            confirmed_at = existing[1]
            expires_at = existing[2]
            is_active = bool(confirmed_at is not None or expires_at is None or expires_at > datetime.now(timezone.utc))

            if is_active and existing_user_id != requested_user_id:
                raise HTTPException(status_code=400, detail="user_id and github_login mismatch")

            session.execute(
                text(
                    "UPDATE github_login_aliases "
                    f"SET user_id = :user_id, expires_at = {expires_sql}, updated_at = NOW() "
                    "WHERE github_login = :login"
                ),
                {"login": requested_login, "user_id": requested_user_id, "ttl_seconds": GITHUB_LOGIN_RESERVATION_TTL_SECONDS},  # noqa
            )
            return requested_user_id, requested_login

        session.execute(
            text(
                "INSERT INTO github_login_aliases (github_login, user_id, expires_at) "
                f"VALUES (:login, :user_id, {expires_sql}) "
                "ON CONFLICT (github_login) DO NOTHING"
            ),
            {"login": requested_login, "user_id": requested_user_id, "ttl_seconds": GITHUB_LOGIN_RESERVATION_TTL_SECONDS},  # noqa
        )

        existing = session.execute(
            text(
                "SELECT user_id "
                "FROM github_login_aliases "
                "WHERE github_login = :login "
                "  AND (confirmed_at IS NOT NULL OR expires_at IS NULL OR expires_at > NOW())"
            ),
            {"login": requested_login},
        ).fetchone()

        if not existing or str(existing[0]) != requested_user_id:
            raise HTTPException(status_code=400, detail="user_id and github_login mismatch")

        return requested_user_id, requested_login

    if requested_login:
        row = session.execute(
            text("SELECT id, github_login FROM users WHERE LOWER(github_login) = :login LIMIT 1"),
            {"login": requested_login},
        ).fetchone()
        if row:
            return str(row[0]), _normalize_github_login(row[1])

        alias_row = session.execute(
            text(
                "SELECT user_id "
                "FROM github_login_aliases "
                "WHERE github_login = :login "
                "  AND (confirmed_at IS NOT NULL OR expires_at IS NULL OR expires_at > NOW())"
            ),
            {"login": requested_login},
        ).fetchone()

        if alias_row:
            return str(alias_row[0]), requested_login

        desired_user_id = str(uuid.uuid4())
        expires_sql = "NOW() + (:ttl_seconds * INTERVAL '1 second')"

        session.execute(
            text(
                "INSERT INTO github_login_aliases (github_login, user_id, expires_at) "
                f"VALUES (:login, :user_id, {expires_sql}) "
                "ON CONFLICT (github_login) DO NOTHING"
            ),
            {"login": requested_login, "user_id": desired_user_id, "ttl_seconds": GITHUB_LOGIN_RESERVATION_TTL_SECONDS},  # noqa
        )

        alias_row = session.execute(
            text(
                "SELECT user_id, confirmed_at, expires_at "
                "FROM github_login_aliases "
                "WHERE github_login = :login"
            ),
            {"login": requested_login},
        ).fetchone()

        if not alias_row:
            raise HTTPException(status_code=500, detail="Failed to reserve github_login")

        alias_user_id = str(alias_row[0])
        confirmed_at = alias_row[1]
        expires_at = alias_row[2]
        is_active = bool(confirmed_at is not None or expires_at is None or expires_at > datetime.now(timezone.utc))

        if is_active:
            return alias_user_id, requested_login

        updated = session.execute(
            text(
                "UPDATE github_login_aliases "
                f"SET user_id = :user_id, expires_at = {expires_sql}, updated_at = NOW() "
                "WHERE github_login = :login "
                "  AND confirmed_at IS NULL "
                "  AND expires_at IS NOT NULL "
                "  AND expires_at <= NOW()"
            ),
            {"login": requested_login, "user_id": desired_user_id, "ttl_seconds": GITHUB_LOGIN_RESERVATION_TTL_SECONDS},  # noqa
        )

        if (updated.rowcount or 0) > 0:
            return desired_user_id, requested_login

        alias_row = session.execute(
            text(
                "SELECT user_id "
                "FROM github_login_aliases "
                "WHERE github_login = :login "
                "  AND (confirmed_at IS NOT NULL OR expires_at IS NULL OR expires_at > NOW())"
            ),
            {"login": requested_login},
        ).fetchone()
        if not alias_row:
            raise HTTPException(status_code=500, detail="Failed to reserve github_login")

        return str(alias_row[0]), requested_login

    raw_login, viewer = fetch_user_login(github_token)
    viewer_login = _normalize_github_login(raw_login)
    if not viewer_login:
        raise HTTPException(status_code=502, detail="Failed to resolve GitHub login")

    if not _GITHUB_LOGIN_RE.match(viewer_login):
        raise HTTPException(status_code=502, detail="GitHub returned an invalid login")

    stable_github_user_id = int((viewer or {}).get("databaseId") or 0)
    if stable_github_user_id <= 0:
        raise HTTPException(status_code=502, detail="GitHub returned an invalid user id")

    candidate_user_id = str(uuid.uuid4())

    row = session.execute(
        text(
            "INSERT INTO users(id, github_login, github_user_id, github_created_at, avatar_url) "
            "VALUES (:id, :login, :gh_uid, :gcreated, :avatar) "
            "ON CONFLICT (github_user_id) DO UPDATE SET "
            "github_login=EXCLUDED.github_login, "
            "github_created_at=COALESCE(users.github_created_at, EXCLUDED.github_created_at), "
            "avatar_url=EXCLUDED.avatar_url "
            "RETURNING id, github_login"
        ),
        {
            "id": candidate_user_id,
            "login": viewer_login,
            "gh_uid": stable_github_user_id,
            "gcreated": (viewer or {}).get("createdAt"),
            "avatar": (viewer or {}).get("avatarUrl"),
        },
    ).fetchone()

    if not row or not row[0]:
        raise HTTPException(status_code=500, detail="Failed to upsert user")

    canonical_user_id = str(row[0])
    canonical_login = _normalize_github_login(row[1]) or viewer_login

    session.execute(
        text(
            "INSERT INTO github_login_aliases (github_login, user_id, confirmed_at, expires_at, github_user_id, updated_at) "  # noqa
            "VALUES (:login, :user_id, NOW(), NULL, :gh_uid, NOW()) "
            "ON CONFLICT (github_login) DO UPDATE SET "
            "user_id=EXCLUDED.user_id, "
            "updated_at=NOW(), "
            "confirmed_at=NOW(), "
            "expires_at=NULL, "
            "github_user_id=EXCLUDED.github_user_id"
        ),
        {"login": canonical_login, "user_id": canonical_user_id, "gh_uid": stable_github_user_id},
    )

    return canonical_user_id, canonical_login


@asynccontextmanager
async def lifespan(_app: FastAPI):
    validate_config()
    apply_pending_migrations()
    yield


app = FastAPI(
    title="Wave Metrics Service",
    version=SERVICE_VERSION,
    lifespan=lifespan,
)

celery_client = Celery("wave-metrics-api", broker=REDIS_URL, backend=REDIS_URL)
# Ensure API and worker agree on the Celery queue name
celery_client.conf.task_default_queue = "default"


@app.get("/health")
def health() -> Dict[str, Any]:
    """
    Health check: verifies DB and Redis connectivity

    Returns:
        Dict with status, timestamp, and per-dependency booleans
    """
    database_ok = False
    redis_ok = False
    broker_ok = None
    broker_workers = None

    # Check Postgres
    try:
        with db_session() as session:
            session.execute(text("SELECT 1"))
            database_ok = True
    except Exception as exc:
        logger.error("health: database check failed error=%s msg=%s", type(exc).__name__, str(exc))
        database_ok = False

    # Check Redis
    try:
        r = get_redis()
        redis_ok = bool(r.ping())
    except Exception as exc:
        logger.error("health: redis check failed error=%s msg=%s", type(exc).__name__, str(exc))
        redis_ok = False

    if HEALTH_CHECK_BROKER:
        try:
            replies = celery_client.control.ping(timeout=HEALTH_CHECK_BROKER_TIMEOUT_SECONDS)
            broker_ok = True
            broker_workers = len(replies or [])
        except (OperationalError, TimeoutError, OSError, ConnectionError) as exc:
            logger.error("health: broker ping failed error=%s msg=%s", type(exc).__name__, str(exc))
            broker_ok = False
            broker_workers = None

    healthy = bool(database_ok and redis_ok and (True if broker_ok is None else broker_ok))
    status = "healthy" if healthy else "degraded"

    payload = {
        "status": status,
        "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "database": database_ok,
        "redis": redis_ok,
    }

    if broker_ok is not None:
        payload["broker"] = broker_ok
        payload["broker_workers"] = broker_workers

    return JSONResponse(status_code=200 if healthy else 503, content=payload)


@app.get("/version")
def version():
    return {"version": app.version}


@app.get("/api/v1/metrics", response_model=MetricsResponse)
def get_metrics(
    user_id: uuid.UUID = Query(...),
    _=Depends(verify_api_auth_token),
    db=Depends(get_session),
) -> MetricsResponse:
    """
    Retrieve contributor metrics for a user

    Args:
        user_id (str): User UUID

    Returns:
        Metrics payload
    """
    return _get_metrics_payload_by_user_id(str(user_id), db)


@app.get("/api/v1/metrics/by-login", response_model=MetricsResponse)
def get_metrics_by_login(
    github_login: str = Query(...),
    _=Depends(verify_api_auth_token),
    db=Depends(get_session),
) -> MetricsResponse:
    """
    Retrieve metrics by GitHub login

    Args:
        github_login (str): GitHub login

    Returns:
        MetricsResponse payload
    """
    normalized_login = _require_valid_github_login(github_login)

    row = db.execute(
        text("SELECT id FROM users WHERE LOWER(github_login) = :login LIMIT 1"),
        {"login": normalized_login},
    ).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="User not found")

    return _get_metrics_payload_by_user_id(str(row[0]), db)


@app.post("/api/v1/sync")
def sync_github(
    body: SyncRequest,
    x_github_token: Optional[str] = Header(default=None, alias="X-GitHub-Token"),
    _=Depends(verify_api_auth_token),
) -> Dict[str, Any]:
    """
    Enqueue a sync job for a user

    Args:
        body (dict): Payload with user_id, github_login, github_token

    Returns:
        dict with accepted job info
    """
    token = x_github_token or (body.github_token.get_secret_value() if body.github_token else "")
    if not token:
        raise HTTPException(status_code=400, detail="github_token is required")

    queue_name = body.queue or "default"
    is_bulk = queue_name == "bulk"
    token_ref_ttl_seconds = (
        shared_config.TOKEN_REF_TTL_SECONDS_BULK
        if is_bulk
        else shared_config.TOKEN_REF_TTL_SECONDS_NORMAL
    )
    triggered_by = "api.bulk" if is_bulk else "api"

    try:
        with db_session() as session:
            user_id, github_login = _resolve_sync_user_identity(body, token, session)
            effective_backfill_days = _resolve_effective_backfill_days(user_id, body.backfill_days, session)

            job_id = str(uuid.uuid4())
            session.execute(
                text(
                    "INSERT INTO sync_jobs (job_id, user_id, github_login, status, backfill_days, triggered_by) "
                    "VALUES (:job_id, :user_id, :github_login, 'PENDING', :backfill_days, :triggered_by)"
                ),
                {
                    "job_id": job_id,
                    "user_id": user_id,
                    "github_login": github_login,
                    "backfill_days": effective_backfill_days,
                    "triggered_by": triggered_by,
                },
            )
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("sync: failed to create job", extra={"error": type(exc).__name__})
        raise HTTPException(status_code=500, detail="Failed to create job") from exc

    token_ref = None
    try:
        token_ref = create_github_token_ref(None, user_id, token, ttl_seconds=token_ref_ttl_seconds)

        celery_client.send_task(
            "sync_and_compute",
            args=[user_id, token_ref, effective_backfill_days, triggered_by],
            task_id=job_id,
            queue=queue_name,
        )
    except Exception as exc:
        if token_ref:
            try:
                delete_github_token_ref(None, token_ref)
            except Exception:
                logger.exception(
                    "sync: failed to cleanup token_ref after enqueue failure",
                    extra={"user_id": user_id, "token_ref": token_ref},
                )

        error_message = f"{type(exc).__name__}: {str(exc)}".strip()
        if len(error_message) > 500:
            error_message = error_message[:500]

        try:
            with db_session() as session:
                session.execute(
                    text(
                        "UPDATE sync_jobs "
                        "SET status='FAILED', completed_at=NOW(), error_message=:error "
                        "WHERE job_id=:job_id AND stale_marked_at IS NULL"
                    ),
                    {"job_id": job_id, "error": error_message},
                )
        except Exception:
            logger.exception(
                "sync: failed to mark job FAILED after enqueue failure",
                extra={"job_id": job_id, "user_id": user_id},
            )

        logger.exception("sync: enqueue failed", extra={"job_id": job_id, "user_id": user_id})
        raise HTTPException(status_code=500, detail="Failed to enqueue job") from exc

    logger.info(
        "sync: enqueued sync_and_compute job_id=%s user_id=%s github_login=%s backfill_days=%s triggered_by=%s queue=%s",  # noqa
        job_id,
        user_id,
        github_login,
        effective_backfill_days,
        triggered_by,
        queue_name,
    )

    return {"job_id": job_id, "status": "enqueued", "user_id": user_id, "github_login": github_login}


@app.get("/api/v1/jobs/{job_id}")
def get_job(job_id: uuid.UUID, db=Depends(get_session)) -> Dict[str, Any]:
    """
    Retrieve a job record by job_id

    This endpoint is intentionally unauthenticated.

    Args:
        job_id (uuid.UUID): Job id (Celery task id)

    Returns:
        Dict job status payload
    """
    job_id_str = str(job_id)

    row = db.execute(
        text(
            "SELECT job_id, status, user_id, github_login, created_at, started_at, completed_at, stale_marked_at, error_message "  # noqa
            "FROM sync_jobs "
            "WHERE job_id = :job_id"
        ),
        {"job_id": job_id_str},
    ).mappings().fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="Job not found")

    if row["status"] == "RUNNING" and row["stale_marked_at"] is None:
        now = datetime.now(timezone.utc)
        started_or_created = row["started_at"] or row["created_at"]
        if started_or_created is not None and started_or_created.tzinfo is None:
            started_or_created = started_or_created.replace(tzinfo=timezone.utc)

        if started_or_created is not None:
            age_seconds = (now - started_or_created).total_seconds()
            if age_seconds > float(RUNNING_JOB_STALE_SECONDS):
                error_message = (
                    "Marked FAILED: job stale; age_seconds="
                    f"{int(age_seconds)} exceeded RUNNING_JOB_STALE_SECONDS="
                    f"{int(RUNNING_JOB_STALE_SECONDS)}"
                )
                db.execute(
                    text(
                        "UPDATE sync_jobs "
                        "SET status='FAILED', stale_marked_at=NOW(), completed_at=NOW(), error_message=:error "
                        "WHERE job_id=:job_id AND status='RUNNING' AND stale_marked_at IS NULL"
                    ),
                    {"job_id": job_id_str, "error": error_message},
                )
                row = db.execute(
                    text(
                        "SELECT job_id, status, user_id, github_login, created_at, started_at, completed_at, error_message "  # noqa
                        "FROM sync_jobs "
                        "WHERE job_id = :job_id"
                    ),
                    {"job_id": job_id_str},
                ).mappings().fetchone() or row

    return {
        "job_id": str(row["job_id"]),
        "status": str(row["status"]),
        "user_id": str(row["user_id"]),
        "github_login": str(row["github_login"]),
        "created_at": _to_iso8601_z(row["created_at"]),
        "started_at": _to_iso8601_z(row["started_at"]),
        "completed_at": _to_iso8601_z(row["completed_at"]),
        "error_message": row["error_message"],
    }


if __name__ == "__main__":
    import uvicorn

    port = int(os.getenv("PORT", "8000"))
    host = str(os.getenv("API_BIND_HOST", "0.0.0.0")).strip() or "0.0.0.0"

    uvicorn.run(app, host=host, port=port)
