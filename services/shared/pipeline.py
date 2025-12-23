import logging
import uuid
from collections import OrderedDict
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Tuple

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from redis.exceptions import RedisError

from services.shared.caching import invalidate_metrics
from services.shared.config import REPO_LANGUAGE_CACHE_MAX_REPOS, USER_LOCK_TTL_SECONDS, USER_LOCK_WAIT_TIMEOUT_SECONDS
from services.shared import config as shared_config
from services.shared import token_vault
from services.shared.locks import acquire_user_lock, release_user_lock
from services.shared.database import ENGINE, db_session
from services.shared.login_aliases import confirm_login_alias
from services.shared.github_client import (
    fetch_oss_activity_counts_3y,
    fetch_repo_languages,
    fetch_user_login,
    iter_user_prs_windowed,
)
from services.shared.percentiles import (
    compute_oss_composite_raw,
    lookup_raw_percentile,
    resolve_baseline,
    to_display_percentile,
)


logger = logging.getLogger("pipeline")

METRICS_WINDOW_DAYS = 1096


def _validate_uuid_string(value, field_name) -> Optional[str]:
    """
    Validate a UUID string boundary

    Args:
        value: Value to validate (str or None)
        field_name (str): Name used in error messages

    Returns:
        str or None

    Raises:
        ValueError: When value is not a valid UUID
    """
    if value is None:
        return None

    try:
        return str(uuid.UUID(str(value)))
    except ValueError as exc:
        raise ValueError(f"{field_name} must be a valid UUID") from exc


def _require_uuid_string(value, field_name) -> str:
    """
    Validate a required UUID string boundary

    Args:
        value: Value to validate (str)
        field_name (str): Name used in error messages

    Returns:
        str normalized UUID

    Raises:
        ValueError: When value is missing or not a valid UUID
    """
    normalized = _validate_uuid_string(value, field_name)
    if normalized is None:
        raise ValueError(f"{field_name} must be a valid UUID")
    return normalized


def _ensure_user(session, desired_user_id, github_login, github_user_id, github_created_at, avatar_url) -> str:
    """
    Upsert the user row using github_user_id as the stable identity

    Args:
        session: DB session
        desired_user_id (str): Optional desired user id
        github_login (str): Current GitHub login
        github_user_id (int): Stable GitHub databaseId
        github_created_at (str): GitHub createdAt
        avatar_url (str): Avatar URL

    Returns:
        str internal user UUID

    Raises:
        ValueError: When github_user_id is missing or invalid
    """
    stable_github_user_id = int(github_user_id or 0)
    if stable_github_user_id <= 0:
        raise ValueError("github_user_id is required to upsert a user safely")

    candidate_user_id = desired_user_id or str(uuid.uuid4())

    normalized_login = str(github_login or "").strip().lower()

    row = session.execute(
        text(
            "INSERT INTO users(id, github_login, github_user_id, github_created_at, avatar_url) "
            "VALUES (:id, :login, :gh_uid, :gcreated, :avatar) "
            "ON CONFLICT (github_user_id) DO UPDATE SET "
            "github_login=EXCLUDED.github_login, "
            "github_created_at=COALESCE(users.github_created_at, EXCLUDED.github_created_at), "
            "avatar_url=EXCLUDED.avatar_url "
            "RETURNING id"
        ),
        {
            "id": candidate_user_id,
            "login": normalized_login,
            "gh_uid": stable_github_user_id,
            "gcreated": github_created_at,
            "avatar": avatar_url,
        },
    ).fetchone()

    if not row or not row[0]:
        raise RuntimeError("Failed to upsert user row")

    return str(row[0])


def _upsert_repository(session, github_repo_id, owner_login, name, is_private) -> int:
    """
    Upsert repository row using github_repo_id as the stable identity

    Args:
        session: DB session
        github_repo_id (int): Stable GitHub repository databaseId
        owner_login (str): Current owner login
        name (str): Current repository name
        is_private (bool): Privacy

    Returns:
        int repository id (pk)

    Raises:
        ValueError: When github_repo_id is missing or invalid
    """
    stable_github_repo_id = int(github_repo_id or 0)
    if stable_github_repo_id <= 0:
        raise ValueError("github_repo_id is required to upsert a repository safely")

    row = session.execute(
        text(
            "INSERT INTO repositories (github_repo_id, owner_login, name, is_private) "
            "VALUES (:rid, :owner, :name, :priv) "
            "ON CONFLICT (github_repo_id) DO UPDATE SET "
            "owner_login=EXCLUDED.owner_login, "
            "name=EXCLUDED.name, "
            "is_private=EXCLUDED.is_private "
            "RETURNING id"
        ),
        {
            "rid": stable_github_repo_id,
            "owner": owner_login,
            "name": name,
            "priv": bool(is_private),
        },
    ).fetchone()

    if not row or row[0] is None:
        raise RuntimeError("Failed to upsert repository row")

    return int(row[0])


def _upsert_repo_languages(session, repository_id, languages) -> None:
    """
    Upsert repository language rows

    Args:
        session: DB session
        repository_id (int): Repo pk
        languages (list): List of {language, bytes}

    Returns:
        None
    """
    for item in languages:
        session.execute(
            text(
                "INSERT INTO repository_languages (repository_id, language, bytes)"
                " VALUES (:rid,:lang,:bytes)"
                " ON CONFLICT (repository_id, language) DO UPDATE SET bytes=EXCLUDED.bytes"
            ),
            {"rid": repository_id, "lang": item.get("language"), "bytes": int(item.get("bytes") or 0)},
        )


def _upsert_pull_request(session, pr, repository_id, author_user_id) -> None:
    """
    Upsert pull request row

    Args:
        session: DB session
        pr (dict): PR node
        repository_id (int): Repo pk
        author_user_id (str): User UUID

    Returns:
        None
    """
    gid = int(pr.get("databaseId") or 0)
    if not gid:
        gid = repository_id * 10_000_000 + int(pr.get("number") or 0)

    comment_count = int(((pr.get("comments") or {}).get("totalCount") or 0))
    review_count = int(((pr.get("reviews") or {}).get("totalCount") or 0))

    session.execute(
        text(
            "INSERT INTO pull_requests (github_pr_id, repository_id, author_user_id, number, state, is_draft,"
            " created_at, merged_at, closed_at, comment_count, review_count, additions, deletions, changed_files,"
            " updated_at, url)"
            " VALUES (:gid,:rid,:uid,:number,:state,:draft,:created,:merged,:closed,:cmt,:rev,:add,:del,:files,:updated,:url)"  # noqa
            " ON CONFLICT (github_pr_id) DO UPDATE SET"
            " repository_id=EXCLUDED.repository_id, author_user_id=EXCLUDED.author_user_id,"
            " state=EXCLUDED.state, is_draft=EXCLUDED.is_draft, merged_at=EXCLUDED.merged_at,"
            " closed_at=EXCLUDED.closed_at, comment_count=EXCLUDED.comment_count, review_count=EXCLUDED.review_count,"
            " additions=EXCLUDED.additions, deletions=EXCLUDED.deletions,"
            " changed_files=EXCLUDED.changed_files, updated_at=EXCLUDED.updated_at, url=EXCLUDED.url"
        ),
        {
            "gid": gid,
            "rid": repository_id,
            "uid": author_user_id,
            "number": int(pr.get("number") or 0),
            "state": "MERGED" if pr.get("merged") else pr.get("state") or "OPEN",
            "draft": bool(pr.get("isDraft") or False),
            "created": pr.get("createdAt"),
            "merged": pr.get("mergedAt"),
            "closed": pr.get("closedAt"),
            "cmt": comment_count,
            "rev": review_count,
            "add": int(pr.get("additions") or 0),
            "del": int(pr.get("deletions") or 0),
            "files": int(pr.get("changedFiles") or 0),
            "updated": pr.get("updatedAt"),
            "url": pr.get("url"),
        },
    )


def _normalize_to_utc(dt) -> Optional[datetime]:
    """
    Normalize a datetime to timezone-aware UTC

    Args:
        dt: Datetime or None

    Returns:
        Datetime in UTC or None
    """
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _resolve_ingestion_window(session, user_id, since, until, backfill_days) -> Tuple[datetime, datetime]:
    """
    Choose [window_start, window_end] for GitHub search

    Priority:
        1. Explicit since / until when provided
        2. backfill_days relative to now when since is None
        3. Incremental based on github_sync_state and default lookback
    """
    now = datetime.now(timezone.utc)
    if since is not None:
        window_start = _normalize_to_utc(since)
        window_end = _normalize_to_utc(until) if until is not None else now
        return window_start, window_end

    window_end = _normalize_to_utc(until) if until is not None else now

    if backfill_days is not None:
        window_start = window_end - timedelta(days=int(backfill_days))
        return window_start, window_end

    row = session.execute(
        text("SELECT last_pr_updated_at FROM github_sync_state WHERE user_id=:u"),
        {"u": user_id},
    ).fetchone()

    default_lookback_days = METRICS_WINDOW_DAYS
    if row and row[0]:
        watermark = row[0]
        if watermark.tzinfo is None:
            watermark = watermark.replace(tzinfo=timezone.utc)
        window_start = watermark - timedelta(days=1)
    else:
        window_start = window_end - timedelta(days=default_lookback_days)
    return window_start, window_end


def _create_github_sync_run(user_id, partition_key, triggered_by):
    """
    Create a github_sync_runs row when the table exists

    Returns:
        Tuple of (run_id, success)
    """
    try:
        run_id = str(uuid.uuid4())
        with ENGINE.begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO github_sync_runs "
                    "(id, user_id, partition_key, triggered_by, status) "
                    "VALUES (:id, :user_id, :partition_key, :triggered_by, 'PENDING')"
                ),
                {
                    "id": run_id,
                    "user_id": user_id,
                    "partition_key": partition_key,
                    "triggered_by": triggered_by,
                },
            )
        return run_id, True
    except SQLAlchemyError as exc:
        logger.error(
            "github_sync_runs insert failed",
            extra={"user_id": user_id, "partition_key": partition_key, "triggered_by": triggered_by},
            exc_info=True,
        )
        return None, False


def _mark_github_sync_run_started(run_id):
    """
    Mark a github_sync_runs row as RUNNING

    Returns:
        bool success
    """
    if not run_id:
        return False
    try:
        with ENGINE.begin() as conn:
            conn.execute(
                text(
                    "UPDATE github_sync_runs "
                    "SET status='RUNNING', started_at=:started_at "
                    "WHERE id=:id"
                ),
                {"id": run_id, "started_at": datetime.now(timezone.utc)},
            )
        return True
    except SQLAlchemyError as exc:
        logger.error("github_sync_runs start update failed", extra={"run_id": run_id}, exc_info=True)
        return False


def _mark_github_sync_run_finished(run_id, status, window_start, window_end, backfill_days, error_message):
    """
    Mark a github_sync_runs row as finished

    Args:
        run_id (str): Run id or None
        status (str): SUCCESS or FAILED
        window_start (datetime): Window start
        window_end (datetime): Window end
        backfill_days (int): Optional backfill days
        error_message (str): Optional error message

    Returns:
        bool success
    """
    if not run_id:
        return False
    try:
        with ENGINE.begin() as conn:
            conn.execute(
                text(
                    "UPDATE github_sync_runs "
                    "SET status=:status, window_start=:window_start, window_end=:window_end,"
                    " backfill_days=:backfill_days, finished_at=:finished_at, error_message=:error_message "
                    "WHERE id=:id"
                ),
                {
                    "id": run_id,
                    "status": status,
                    "window_start": window_start,
                    "window_end": window_end,
                    "backfill_days": backfill_days,
                    "finished_at": datetime.now(timezone.utc),
                    "error_message": (error_message or "")[:500] if error_message else None,
                },
            )
        return True
    except SQLAlchemyError as exc:
        logger.error("github_sync_runs finish update failed",
                     extra={"run_id": run_id, "status": status}, exc_info=True)
        return False


def _create_metric_compute_run(user_id, baseline_id, partition_key, triggered_by):
    """
    Create a metric_compute_runs row when the table exists

    Returns:
        Tuple of (run_id, success)
    """
    try:
        run_id = str(uuid.uuid4())
        with ENGINE.begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO metric_compute_runs "
                    "(id, user_id, baseline_id, partition_key, triggered_by, status) "
                    "VALUES (:id, :user_id, :baseline_id, :partition_key, :triggered_by, 'PENDING')"
                ),
                {
                    "id": run_id,
                    "user_id": user_id,
                    "baseline_id": baseline_id,
                    "partition_key": partition_key,
                    "triggered_by": triggered_by,
                },
            )
        return run_id, True
    except SQLAlchemyError as exc:
        logger.error(
            "metric_compute_runs insert failed",
            extra={"user_id": user_id, "baseline_id": baseline_id,
                   "partition_key": partition_key, "triggered_by": triggered_by},
            exc_info=True,
        )
        return None, False


def _update_metric_compute_run_baseline(run_id, baseline_id):
    """
    Update baseline_id on a metric_compute_runs row

    Args:
        run_id (str): Run id or None
        baseline_id (str): Baseline id

    Returns:
        bool success
    """
    if not run_id:
        return False
    try:
        with ENGINE.begin() as conn:
            conn.execute(
                text("UPDATE metric_compute_runs SET baseline_id=:baseline_id WHERE id=:id"),
                {"id": run_id, "baseline_id": baseline_id},
            )
        return True
    except SQLAlchemyError as exc:
        logger.error(
            "metric_compute_runs baseline update failed",
            extra={"run_id": run_id, "baseline_id": baseline_id},
            exc_info=True,
        )
        return False


def _mark_metric_compute_run_started(run_id):
    """
    Mark a metric_compute_runs row as RUNNING

    Returns:
        bool success
    """
    if not run_id:
        return False
    try:
        with ENGINE.begin() as conn:
            conn.execute(
                text(
                    "UPDATE metric_compute_runs "
                    "SET status='RUNNING', started_at=:started_at "
                    "WHERE id=:id"
                ),
                {"id": run_id, "started_at": datetime.now(timezone.utc)},
            )
        return True
    except SQLAlchemyError as exc:
        logger.error("metric_compute_runs start update failed", extra={"run_id": run_id}, exc_info=True)
        return False


def _mark_metric_compute_run_finished(run_id, status, window_start, window_end, error_message):
    """
    Mark a metric_compute_runs row as finished

    Args:
        run_id (str): Run id or None
        status (str): SUCCESS or FAILED
        window_start (datetime): Window start
        window_end (datetime): Window end
        error_message (str): Optional error message

    Returns:
        bool success
    """
    if not run_id:
        return False
    try:
        with ENGINE.begin() as conn:
            conn.execute(
                text(
                    "UPDATE metric_compute_runs "
                    "SET status=:status, window_start=:window_start, window_end=:window_end, "
                    "finished_at=:finished_at, error_message=:error_message "
                    "WHERE id=:id"
                ),
                {
                    "id": run_id,
                    "status": status,
                    "window_start": window_start,
                    "window_end": window_end,
                    "finished_at": datetime.now(timezone.utc),
                    "error_message": (error_message or "")[:500] if error_message else None,
                },
            )
        return True
    except SQLAlchemyError as exc:
        logger.error("metric_compute_runs finish update failed",
                     extra={"run_id": run_id, "status": status}, exc_info=True)
        return False


def ingest_user_github_activity(
    user_id,
    github_token,
    since=None,
    until=None,
    backfill_days=None,
    partition_key=None,
    triggered_by=None,
) -> Dict[str, Any]:
    """
    Ingest GitHub activity for a single user into normalized tables

    Args:
        user_id (str): Internal user UUID (may be None; a new user id will be generated when absent)
        github_token (str): OAuth token with read:user scope
        since (datetime): Optional lower bound for PR updated_at (UTC)
        until (datetime): Optional upper bound for PR updated_at (UTC); defaults to now when omitted
        backfill_days (int): Optional lookback window used when since is not provided
        partition_key (str): Optional partition label, e.g. '2025-01-01' or '2025-01'
        triggered_by (str): Optional origin label, e.g. 'api', 'scheduler', 'manual'

    Returns:
        dict with keys:
            user_id (str): Internal user UUID
            github_login (str): GitHub login
            window_start (datetime): Effective window start (UTC)
            window_end (datetime): Effective window end (UTC)
            pr_count (int): Number of PRs ingested in this run
    """
    user_id = _validate_uuid_string(user_id, "user_id")

    login, viewer = fetch_user_login(github_token)
    pr_count = 0

    internal_user_id = None
    window_start = None
    window_end = None
    run_id = None
    metadata_ok = True

    logger.info(
        "ingest_user_github_activity started",
        extra={"user_id": user_id, "login": login, "run_id": run_id, "triggered_by": triggered_by},
    )

    try:
        # Ensure user + resolve ingestion window first (commits user if newly created)
        with db_session() as session:
            internal_user_id = _ensure_user(
                session,
                user_id,
                login,
                viewer.get("databaseId"),
                viewer.get("createdAt"),
                viewer.get("avatarUrl"),
            )
            confirm_login_alias(session, login, internal_user_id, viewer.get("databaseId"))
            window_start, window_end = _resolve_ingestion_window(
                session,
                internal_user_id,
                since,
                until,
                backfill_days,
            )

        run_id, created_ok = _create_github_sync_run(
            internal_user_id,
            partition_key,
            triggered_by,
        )
        metadata_ok = bool(created_ok)
        metadata_ok = _mark_github_sync_run_started(run_id) and metadata_ok

        max_cached_repos = max(1, int(REPO_LANGUAGE_CACHE_MAX_REPOS))
        repo_lang_cache: "OrderedDict[int, Any]" = OrderedDict()
        repo_pk_cache: "OrderedDict[int, int]" = OrderedDict()

        def _lru_get(cache, key, default=None):
            if key in cache:
                cache.move_to_end(key)
                return cache[key]
            return default

        def _lru_set(cache, key, value):
            cache[key] = value
            cache.move_to_end(key)
            while len(cache) > max_cached_repos:
                cache.popitem(last=False)

        batch_repo_languages = {}
        batch: list = []
        batch_size = 50

        def _flush(items, repo_languages_by_id) -> None:
            if not items:
                return
            with db_session() as session:
                for pr_node in items:
                    repo = pr_node.get("repository") or {}
                    owner = (repo.get("owner") or {}).get("login")
                    name = repo.get("name")
                    github_repo_id = int(repo.get("databaseId") or 0)
                    repository_pk = _lru_get(repo_pk_cache, github_repo_id)
                    if repository_pk is None:
                        repository_pk = _upsert_repository(
                            session,
                            github_repo_id,
                            owner,
                            name,
                            repo.get("isPrivate"),
                        )
                        _lru_set(repo_pk_cache, github_repo_id, repository_pk)

                    langs = repo_languages_by_id.get(github_repo_id)
                    if langs is None:
                        langs = _lru_get(repo_lang_cache, github_repo_id) or []
                    _upsert_repo_languages(session, repository_pk, langs)
                    _upsert_pull_request(session, pr_node, repository_pk, internal_user_id)

        # Network phase: fetch PRs and repo languages without holding DB sessions open
        for pr in iter_user_prs_windowed(github_token, login, window_start, window_end):
            repo = pr.get("repository") or {}
            owner = (repo.get("owner") or {}).get("login")
            name = repo.get("name")
            github_repo_id = int(repo.get("databaseId") or 0)

            if github_repo_id:
                langs = _lru_get(repo_lang_cache, github_repo_id)
                if langs is None:
                    langs = fetch_repo_languages(github_token, owner, name)
                    _lru_set(repo_lang_cache, github_repo_id, langs)
                batch_repo_languages[github_repo_id] = langs or []

            batch.append(pr)
            pr_count += 1
            if len(batch) >= batch_size:
                _flush(batch, batch_repo_languages)
                batch = []
                batch_repo_languages = {}

        _flush(batch, batch_repo_languages)

        with db_session() as session:
            session.execute(
                text(
                    "INSERT INTO github_sync_state(user_id, last_pr_updated_at)"
                    " VALUES (:u, :ts)"
                    " ON CONFLICT (user_id) DO UPDATE SET "
                    "last_pr_updated_at = CASE "
                    "  WHEN github_sync_state.last_pr_updated_at IS NULL THEN EXCLUDED.last_pr_updated_at "
                    "  WHEN github_sync_state.last_pr_updated_at < EXCLUDED.last_pr_updated_at THEN EXCLUDED.last_pr_updated_at "  # noqa
                    "  ELSE github_sync_state.last_pr_updated_at "
                    "END"
                ),
                {"u": internal_user_id, "ts": window_end},
            )

        metadata_ok = _mark_github_sync_run_finished(
            run_id,
            "SUCCESS",
            window_start,
            window_end,
            backfill_days,
            error_message=None,
        ) and metadata_ok

        logger.info(
            "ingest_user_github_activity completed",
            extra={
                "user_id": internal_user_id,
                "login": login,
                "run_id": run_id,
                "pr_count": pr_count,
                "window_start": window_start.isoformat() if window_start else None,
                "window_end": window_end.isoformat() if window_end else None,
            },
        )
    except Exception as exc:
        logger.exception(
            "ingest_user_github_activity failed",
            extra={
                "user_id": internal_user_id or user_id,
                "login": login,
                "run_id": run_id,
                "pr_count": pr_count,
                "window_start": window_start.isoformat() if window_start else None,
                "window_end": window_end.isoformat() if window_end else None,
                "since": since.isoformat() if since else None,
                "until": until.isoformat() if until else None,
                "backfill_days": backfill_days,
            },
        )
        metadata_ok = _mark_github_sync_run_finished(
            run_id,
            "FAILED",
            window_start,
            window_end,
            backfill_days,
            error_message=str(exc),
        ) and metadata_ok
        raise

    return {
        "user_id": internal_user_id,
        "github_login": login,
        "run_id": run_id,
        "window_start": window_start,
        "window_end": window_end,
        "pr_count": pr_count,
        "metadata_ok": metadata_ok,
    }


def _fetch_pr_metrics_3y(session, user_id, window_start, window_end) -> Dict[str, Any]:
    """
    Compute rolling-window eligible PR aggregates per spec

    Args:
        session: DB session
        user_id (str): User UUID
        window_start (datetime): Inclusive lower bound
        window_end (datetime): Exclusive upper bound

    Returns:
        dict with total_opened_prs, total_opened_prs_non_draft, total_merged_prs,
        closed_without_merge_prs, avg_merge_latency_hours
    """
    row = session.execute(
        text("""
            WITH eligible AS (
                SELECT
                    state,
                    is_draft,
                    created_at,
                    merged_at
                FROM v_eligible_prs
                WHERE user_id = :user_id
                  AND is_eligible = TRUE
                  AND created_at >= :window_start
                  AND created_at < :window_end
            )
            SELECT
                COUNT(*) AS total_opened_prs,
                COUNT(*) FILTER (WHERE is_draft = FALSE) AS total_opened_prs_non_draft,
                COUNT(*) FILTER (WHERE state = 'MERGED') AS total_merged_prs,
                COUNT(*) FILTER (WHERE state = 'CLOSED' AND is_draft = FALSE) AS closed_without_merge_prs,
                AVG(EXTRACT(EPOCH FROM (merged_at - created_at)) / 3600.0) FILTER (
                    WHERE state = 'MERGED'
                      AND merged_at IS NOT NULL
                      AND merged_at >= created_at
                ) AS avg_merge_latency_hours
            FROM eligible
        """),
        {"user_id": user_id, "window_start": window_start, "window_end": window_end},
    ).fetchone()

    if not row:
        return {
            "total_opened_prs": 0,
            "total_opened_prs_non_draft": 0,
            "total_merged_prs": 0,
            "closed_without_merge_prs": 0,
            "avg_merge_latency_hours": None,
        }

    return {
        "total_opened_prs": int(row[0] or 0),
        "total_opened_prs_non_draft": int(row[1] or 0),
        "total_merged_prs": int(row[2] or 0),
        "closed_without_merge_prs": int(row[3] or 0),
        "avg_merge_latency_hours": float(row[4]) if row[4] is not None else None,
    }


def _compute_lifetime_language_profile(session, user_id) -> Dict[str, float]:
    """
    Compute PR-weighted lifetime language points for a user

    Args:
        session: DB session
        user_id (str): User UUID

    Returns:
        dict language -> points
    """
    rows = session.execute(
        text("""
            WITH pr_weights AS (
                SELECT
                    p.repository_id,
                    CASE
                        WHEN COALESCE(p.additions, 0) + COALESCE(p.deletions, 0) > 0
                            THEN COALESCE(p.additions, 0) + COALESCE(p.deletions, 0)
                        ELSE GREATEST(COALESCE(p.changed_files, 0), 1)
                    END AS w
                FROM pull_requests p
                WHERE p.author_user_id = :user_id
            ),
            repo_lang_norm AS (
                SELECT
                    rl.repository_id,
                    rl.language,
                    rl.bytes::DECIMAL / NULLIF(SUM(rl.bytes) OVER (PARTITION BY rl.repository_id), 0) AS pct
                FROM repository_languages rl
            )
            SELECT
                rl.language,
                SUM(pw.w * rl.pct) AS lang_points
            FROM pr_weights pw
            JOIN repo_lang_norm rl ON rl.repository_id = pw.repository_id
            GROUP BY rl.language
        """),
        {"user_id": user_id},
    ).fetchall()

    points_by_language: Dict[str, float] = {}
    for row in rows:
        points_by_language[str(row[0])] = float(row[1] or 0.0)
    return points_by_language


def compute_user_metrics_and_language_profile(
    user_id,
    github_token,
    partition_key=None,
    triggered_by=None,
) -> Dict[str, Any]:
    """
    Compute rolling 3-year metrics + percentiles and lifetime language profile

    Requires github_token for OSS counts via contributionsCollection

    Args:
        user_id (str): Internal user UUID
        github_token (str): OAuth token
        partition_key (str): Optional partition label
        triggered_by (str): Optional origin label

    Returns:
        dict summary
    """
    user_id = _require_uuid_string(user_id, "user_id")

    computed_at = datetime.now(timezone.utc)
    metrics_window_end = computed_at
    metrics_window_start = metrics_window_end - timedelta(days=METRICS_WINDOW_DAYS)

    with db_session() as session:
        user_row = session.execute(
            text("SELECT github_login FROM users WHERE id = :u"),
            {"u": user_id},
        ).fetchone()
        if not user_row or not user_row[0]:
            raise ValueError(f"User '{user_id}' not found")
        github_login = str(user_row[0])


    baseline = None
    baseline_id = None
    baseline_start_date = None
    baseline_end_date = None
    pr_metrics = None
    points_by_language = None

    run_id, created_ok = _create_metric_compute_run(
        user_id,
        None,
        partition_key,
        triggered_by,
    )
    metadata_ok = bool(created_ok)
    metadata_ok = _mark_metric_compute_run_started(run_id) and metadata_ok

    logger.info(
        "compute_user_metrics_and_language_profile started",
        extra={"user_id": user_id, "run_id": run_id, "baseline_id": baseline_id, "triggered_by": triggered_by},
    )

    try:
        with db_session() as session:
            baseline = resolve_baseline(session)
            baseline_id = baseline["baseline_id"]
            baseline_start_date = baseline["baseline_start_date"]
            baseline_end_date = baseline["baseline_end_date"]

            pr_metrics = _fetch_pr_metrics_3y(session, user_id, metrics_window_start, metrics_window_end)
            points_by_language = _compute_lifetime_language_profile(session, user_id)

        metadata_ok = _update_metric_compute_run_baseline(run_id, baseline_id) and metadata_ok

        oss_counts = fetch_oss_activity_counts_3y(github_token, github_login, metrics_window_end)
        oss_reviews = int(oss_counts.get("oss_reviews") or 0)
        oss_issues_opened = int(oss_counts.get("oss_issues_opened") or 0)

        total_opened_prs = pr_metrics["total_opened_prs"]
        total_opened_prs_non_draft = pr_metrics["total_opened_prs_non_draft"]
        total_merged_prs = pr_metrics["total_merged_prs"]
        closed_without_merge_prs = pr_metrics["closed_without_merge_prs"]
        avg_merge_latency_hours = pr_metrics["avg_merge_latency_hours"]

        pr_merge_rate = None if total_opened_prs == 0 else float(total_merged_prs) / float(total_opened_prs)
        pr_drop_rate = None if total_opened_prs_non_draft == 0 else \
            float(closed_without_merge_prs) / float(total_opened_prs_non_draft)

        oss_prs_opened = int(total_opened_prs)
        oss_activity_total = oss_prs_opened + oss_reviews + oss_issues_opened

        with db_session() as session:
            percentiles = {
                "total_opened_prs_percentile": None,
                "total_merged_prs_percentile": None,
                "pr_merge_rate_percentile": None,
                "pr_drop_rate_percentile": None,
                "avg_merge_latency_hours_percentile": None,
                "oss_prs_opened_percentile": None,
                "oss_reviews_percentile": None,
                "oss_issues_opened_percentile": None,
                "oss_composite_percentile": None,
            }

            oss_composite_raw = None

            if total_opened_prs > 0:
                raw = lookup_raw_percentile(session, baseline_id, "total_opened_prs", total_opened_prs)
                percentiles["total_opened_prs_percentile"] = to_display_percentile("total_opened_prs", raw)

                raw = lookup_raw_percentile(session, baseline_id, "total_merged_prs", total_merged_prs)
                percentiles["total_merged_prs_percentile"] = to_display_percentile("total_merged_prs", raw)

                if total_opened_prs >= 20 and pr_merge_rate is not None:
                    raw = lookup_raw_percentile(session, baseline_id, "pr_merge_rate", pr_merge_rate)
                    percentiles["pr_merge_rate_percentile"] = to_display_percentile("pr_merge_rate", raw)

                if total_opened_prs_non_draft >= 20 and pr_drop_rate is not None:
                    raw = lookup_raw_percentile(session, baseline_id, "pr_drop_rate", pr_drop_rate)
                    percentiles["pr_drop_rate_percentile"] = to_display_percentile("pr_drop_rate", raw)

                if total_merged_prs >= 20 and avg_merge_latency_hours is not None:
                    raw = lookup_raw_percentile(session, baseline_id, "avg_merge_latency_hours",
                                                avg_merge_latency_hours)
                    percentiles["avg_merge_latency_hours_percentile"] = to_display_percentile(
                        "avg_merge_latency_hours",
                        raw,
                    )

                if oss_activity_total >= 10:
                    p_prs = lookup_raw_percentile(session, baseline_id, "oss_prs_opened", oss_prs_opened)
                    p_reviews = lookup_raw_percentile(session, baseline_id, "oss_reviews", oss_reviews)
                    p_issues = lookup_raw_percentile(session, baseline_id, "oss_issues_opened", oss_issues_opened)

                    percentiles["oss_prs_opened_percentile"] = to_display_percentile("oss_prs_opened", p_prs)
                    percentiles["oss_reviews_percentile"] = to_display_percentile("oss_reviews", p_reviews)
                    percentiles["oss_issues_opened_percentile"] = to_display_percentile("oss_issues_opened", p_issues)

                    oss_composite_raw = compute_oss_composite_raw(p_reviews, p_prs, p_issues)
                    raw = lookup_raw_percentile(session, baseline_id, "oss_composite", oss_composite_raw)
                    percentiles["oss_composite_percentile"] = to_display_percentile("oss_composite", raw)

            session.execute(
                text(
                    "INSERT INTO contributor_metrics ("
                    " user_id, github_login, metrics_window_start, metrics_window_end,"
                    " baseline_id, baseline_start_date, baseline_end_date,"
                    " total_opened_prs, total_merged_prs, closed_without_merge_prs,"
                    " pr_merge_rate, pr_drop_rate, avg_merge_latency_hours,"
                    " oss_prs_opened, oss_reviews, oss_issues_opened, oss_composite_raw,"
                    " total_opened_prs_percentile, total_merged_prs_percentile, pr_merge_rate_percentile,"
                    " pr_drop_rate_percentile, avg_merge_latency_hours_percentile,"
                    " oss_prs_opened_percentile, oss_reviews_percentile, oss_issues_opened_percentile,"
                    " oss_composite_percentile, computed_at"
                    ") VALUES ("
                    " :user_id, :github_login, :metrics_window_start, :metrics_window_end,"
                    " :baseline_id, :baseline_start_date, :baseline_end_date,"
                    " :total_opened_prs, :total_merged_prs, :closed_without_merge_prs,"
                    " :pr_merge_rate, :pr_drop_rate, :avg_merge_latency_hours,"
                    " :oss_prs_opened, :oss_reviews, :oss_issues_opened, :oss_composite_raw,"
                    " :total_opened_prs_percentile, :total_merged_prs_percentile, :pr_merge_rate_percentile,"
                    " :pr_drop_rate_percentile, :avg_merge_latency_hours_percentile,"
                    " :oss_prs_opened_percentile, :oss_reviews_percentile, :oss_issues_opened_percentile,"
                    " :oss_composite_percentile, :computed_at"
                    ") ON CONFLICT (user_id) DO UPDATE SET"
                    " github_login=EXCLUDED.github_login,"
                    " metrics_window_start=EXCLUDED.metrics_window_start,"
                    " metrics_window_end=EXCLUDED.metrics_window_end,"
                    " baseline_id=EXCLUDED.baseline_id,"
                    " baseline_start_date=EXCLUDED.baseline_start_date,"
                    " baseline_end_date=EXCLUDED.baseline_end_date,"
                    " total_opened_prs=EXCLUDED.total_opened_prs,"
                    " total_merged_prs=EXCLUDED.total_merged_prs,"
                    " closed_without_merge_prs=EXCLUDED.closed_without_merge_prs,"
                    " pr_merge_rate=EXCLUDED.pr_merge_rate,"
                    " pr_drop_rate=EXCLUDED.pr_drop_rate,"
                    " avg_merge_latency_hours=EXCLUDED.avg_merge_latency_hours,"
                    " oss_prs_opened=EXCLUDED.oss_prs_opened,"
                    " oss_reviews=EXCLUDED.oss_reviews,"
                    " oss_issues_opened=EXCLUDED.oss_issues_opened,"
                    " oss_composite_raw=EXCLUDED.oss_composite_raw,"
                    " total_opened_prs_percentile=EXCLUDED.total_opened_prs_percentile,"
                    " total_merged_prs_percentile=EXCLUDED.total_merged_prs_percentile,"
                    " pr_merge_rate_percentile=EXCLUDED.pr_merge_rate_percentile,"
                    " pr_drop_rate_percentile=EXCLUDED.pr_drop_rate_percentile,"
                    " avg_merge_latency_hours_percentile=EXCLUDED.avg_merge_latency_hours_percentile,"
                    " oss_prs_opened_percentile=EXCLUDED.oss_prs_opened_percentile,"
                    " oss_reviews_percentile=EXCLUDED.oss_reviews_percentile,"
                    " oss_issues_opened_percentile=EXCLUDED.oss_issues_opened_percentile,"
                    " oss_composite_percentile=EXCLUDED.oss_composite_percentile,"
                    " computed_at=:computed_at"
                ),
                {
                    "user_id": user_id,
                    "github_login": github_login,
                    "metrics_window_start": metrics_window_start,
                    "metrics_window_end": metrics_window_end,
                    "baseline_id": baseline_id,
                    "baseline_start_date": baseline_start_date,
                    "baseline_end_date": baseline_end_date,
                    "total_opened_prs": total_opened_prs,
                    "total_merged_prs": total_merged_prs,
                    "closed_without_merge_prs": closed_without_merge_prs,
                    "pr_merge_rate": pr_merge_rate,
                    "pr_drop_rate": pr_drop_rate,
                    "avg_merge_latency_hours": avg_merge_latency_hours,
                    "oss_prs_opened": oss_prs_opened,
                    "oss_reviews": oss_reviews,
                    "oss_issues_opened": oss_issues_opened,
                    "oss_composite_raw": oss_composite_raw,
                    "total_opened_prs_percentile": percentiles["total_opened_prs_percentile"],
                    "total_merged_prs_percentile": percentiles["total_merged_prs_percentile"],
                    "pr_merge_rate_percentile": percentiles["pr_merge_rate_percentile"],
                    "pr_drop_rate_percentile": percentiles["pr_drop_rate_percentile"],
                    "avg_merge_latency_hours_percentile": percentiles["avg_merge_latency_hours_percentile"],
                    "oss_prs_opened_percentile": percentiles["oss_prs_opened_percentile"],
                    "oss_reviews_percentile": percentiles["oss_reviews_percentile"],
                    "oss_issues_opened_percentile": percentiles["oss_issues_opened_percentile"],
                    "oss_composite_percentile": percentiles["oss_composite_percentile"],
                    "computed_at": computed_at,
                },
            )

            total_points = sum(points_by_language.values()) or 0.0
            session.execute(
                text("DELETE FROM contributor_language_profile WHERE user_id = :u"),
                {"u": user_id},
            )
            for language, points in points_by_language.items():
                pct = 0.0 if total_points <= 0 else 100.0 * float(points) / float(total_points)
                session.execute(
                    text(
                        "INSERT INTO contributor_language_profile (user_id, language, pct, computed_at)"
                        " VALUES (:u, :lang, :pct, :computed_at)"
                        " ON CONFLICT (user_id, language) DO UPDATE SET pct=EXCLUDED.pct, computed_at=:computed_at"
                    ),
                    {"u": user_id, "lang": language, "pct": pct, "computed_at": computed_at},
                )

        metadata_ok = _mark_metric_compute_run_finished(
            run_id,
            "SUCCESS",
            metrics_window_start,
            metrics_window_end,
            error_message=None,
        ) and metadata_ok

        logger.info(
            "compute_user_metrics_and_language_profile completed",
            extra={"user_id": user_id, "run_id": run_id, "baseline_id": baseline_id},
        )

    except Exception as exc:
        logger.exception(
            "compute_user_metrics_and_language_profile failed",
            extra={"user_id": user_id, "run_id": run_id, "error": type(exc).__name__},
        )
        metadata_ok = _mark_metric_compute_run_finished(
            run_id,
            "FAILED",
            metrics_window_start,
            metrics_window_end,
            error_message=str(exc),
        ) and metadata_ok
        raise

    return {
        "user_id": user_id,
        "github_login": github_login,
        "run_id": run_id,
        "metrics_window_start": metrics_window_start,
        "metrics_window_end": metrics_window_end,
        "baseline_id": baseline_id,
        "baseline_start_date": baseline_start_date,
        "baseline_end_date": baseline_end_date,
        "total_opened_prs": total_opened_prs,
        "total_merged_prs": total_merged_prs,
        "computed_at": computed_at,
        "metadata_ok": metadata_ok,
    }


def user_needs_recompute(session, user_id):
    """
    Determine whether serving metrics should be recomputed

    Criteria:
        - contributor_metrics row missing, OR
        - github_sync_state.last_pr_updated_at > contributor_metrics.computed_at

    Args:
        session: DB session
        user_id (str): User UUID

    Returns:
        bool
    """
    row = session.execute(
        text("""
            SELECT
                cm.computed_at,
                gss.last_pr_updated_at
            FROM users u
            LEFT JOIN contributor_metrics cm ON cm.user_id = u.id
            LEFT JOIN github_sync_state gss ON gss.user_id = u.id
            WHERE u.id = :user_id
        """),
        {"user_id": user_id},
    ).fetchone()

    if row is None:
        return True

    computed_at = row[0]
    last_updated_at = row[1]

    if computed_at is None:
        return True

    if last_updated_at is None:
        return False

    return last_updated_at > computed_at


def ingest_and_compute_user(
    user_id,
    github_token=None,
    backfill_days=None,
    since=None,
    until=None,
    partition_key=None,
    triggered_by=None,
    persist_github_token=False,
) -> Dict[str, Any]:
    """
    Orchestrate ingestion from GitHub and compute metrics for a single user

    Args:
        user_id (str): Internal user UUID (optional when github_token is provided)
        github_token (str): Optional OAuth token (when omitted, user_id is required and token resolves from token vault)
        backfill_days (int): Optional lookback window (default METRICS_WINDOW_DAYS for new users)
        since (datetime): Optional explicit window start
        until (datetime): Optional explicit window end
        partition_key (str): Optional partition label
        triggered_by (str): Optional origin label
        persist_github_token (bool): When true, persist github_token to the token vault when enabled

    Returns:
        dict summary

        Success:
            status='ok' and includes run ids for correlation

        Locked:
            status='locked' when another sync is already running for the user

        Missing token:
            status='missing_token' when no token is provided and none is stored

        Invalid token:
            status='token_invalid' when a vault-sourced token is invalidated or returns 401
    """
    token_resolved_from_vault = False

    if not github_token:
        if not user_id:
            raise ValueError("user_id is required when github_token is not provided")
        user_id = _require_uuid_string(user_id, "user_id")

        if not shared_config.TOKEN_VAULT_ENABLED:
            with db_session() as session:
                row = session.execute(
                    text("SELECT github_login FROM users WHERE id = :u"),
                    {"u": user_id},
                ).fetchone()
            github_login = str(row[0]) if row else None
            return {
                "status": "missing_token",
                "user_id": user_id,
                "user": github_login,
                "error": "Token vault not enabled and no token provided",
            }

        with db_session() as session:
            try:
                github_token = token_vault.get_github_token(session, user_id)
                token_resolved_from_vault = True
            except ValueError as exc:
                row = session.execute(
                    text("SELECT github_login FROM users WHERE id = :u"),
                    {"u": user_id},
                ).fetchone()
                github_login = str(row[0]) if row else None
                error_msg = str(exc)
                logger.info(
                    "Token vault lookup failed: user_id=%s error=%s",
                    user_id,
                    error_msg,
                )
                if error_msg.lower().startswith("github token is invalidated"):
                    return {
                        "status": "token_invalid",
                        "user_id": user_id,
                        "user": github_login,
                        "error": error_msg,
                    }
                return {
                    "status": "missing_token",
                    "user_id": user_id,
                    "user": github_login,
                    "error": error_msg,
                }
    else:
        user_id = _validate_uuid_string(user_id, "user_id")

    logger.info(
        "ingest_and_compute_user started",
        extra={"user_id": user_id, "triggered_by": triggered_by},
    )

    try:
        login, viewer = fetch_user_login(github_token)
    except PermissionError:
        if token_resolved_from_vault:
            with db_session() as session:
                try:
                    token_vault.mark_github_token_invalid(session, user_id, reason="401 unauthorized")
                except ValueError as exc:
                    logger.warning(
                        "Failed to mark token invalid after 401: user_id=%s error=%s",
                        user_id,
                        str(exc),
                    )
                row = session.execute(
                    text("SELECT github_login FROM users WHERE id = :u"),
                    {"u": user_id},
                ).fetchone()
            github_login = str(row[0]) if row else None
            return {
                "status": "token_invalid",
                "user_id": user_id,
                "user": github_login,
                "error": "GitHub token unauthorized",
            }
        raise

    with db_session() as session:
        internal_user_id = _ensure_user(
            session,
            user_id,
            login,
            viewer.get("databaseId"),
            viewer.get("createdAt"),
            viewer.get("avatarUrl"),
        )
        confirm_login_alias(session, login, internal_user_id, viewer.get("databaseId"))

        if token_resolved_from_vault and shared_config.TOKEN_VAULT_ENABLED:
            token_vault.upsert_github_token(
                session,
                internal_user_id,
                github_token,
                verified_at=datetime.now(timezone.utc),
                stored_by=triggered_by,
            )

        if persist_github_token and shared_config.TOKEN_VAULT_ENABLED:
            token_vault.upsert_github_token(
                session,
                internal_user_id,
                github_token,
                verified_at=datetime.now(timezone.utc),
                stored_by=triggered_by,
            )

    lock_value = None
    try:
        lock_value = acquire_user_lock(
            internal_user_id,
            ttl_seconds=USER_LOCK_TTL_SECONDS,
            wait_timeout_seconds=USER_LOCK_WAIT_TIMEOUT_SECONDS,
        )
    except TimeoutError:
        logger.info(
            "ingest_and_compute_user skipped: per-user lock held",
            extra={"user_id": internal_user_id, "login": login, "triggered_by": triggered_by},
        )
        return {"status": "locked", "user": login, "user_id": internal_user_id}
    except RedisError as exc:
        logger.exception(
            "ingest_and_compute_user failed to acquire per-user lock",
            extra={"user_id": internal_user_id, "login": login, "error": type(exc).__name__},
        )
        raise RuntimeError("Failed to acquire per-user lock") from exc

    try:
        try:
            ingest_summary = ingest_user_github_activity(
                user_id=internal_user_id,
                github_token=github_token,
                since=since,
                until=until,
                backfill_days=backfill_days,
                partition_key=partition_key,
                triggered_by=triggered_by,
            )

            compute_summary = compute_user_metrics_and_language_profile(
                user_id=internal_user_id,
                github_token=github_token,
                partition_key=partition_key,
                triggered_by=triggered_by,
            )
        except PermissionError:
            if token_resolved_from_vault:
                with db_session() as session:
                    try:
                        token_vault.mark_github_token_invalid(session, internal_user_id, reason="401 unauthorized")
                    except ValueError as exc:
                        logger.warning(
                            "Failed to mark token invalid after 401: user_id=%s error=%s",
                            internal_user_id,
                            str(exc),
                        )

                return {
                    "status": "token_invalid",
                    "user_id": internal_user_id,
                    "user": login,
                    "error": "GitHub token unauthorized",
                }
            raise

        invalidate_metrics(internal_user_id)

        logger.info(
            "ingest_and_compute_user completed",
            extra={
                "user_id": internal_user_id,
                "login": login,
                "sync_run_id": ingest_summary.get("run_id"),
                "compute_run_id": compute_summary.get("run_id"),
                "pr_count": ingest_summary["pr_count"],
            },
        )

        return {
            "status": "ok",
            "user": login,
            "user_id": internal_user_id,
            "sync_run_id": ingest_summary.get("run_id"),
            "compute_run_id": compute_summary.get("run_id"),
            "window_start": ingest_summary["window_start"],
            "window_end": ingest_summary["window_end"],
            "baseline_id": compute_summary["baseline_id"],
            "computed_at": compute_summary["computed_at"],
        }
    finally:
        if lock_value is not None:
            release_user_lock(internal_user_id, lock_value)
