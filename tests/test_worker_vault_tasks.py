import uuid
from datetime import datetime, timezone

from sqlalchemy import text

from services.shared.database import apply_pending_migrations, db_session
from services.worker.app.tasks import backfill_user, refresh_daily


def _truncate_sync_jobs_table() -> None:
    with db_session() as session:
        session.execute(text("TRUNCATE sync_jobs"))


def _insert_pending_job(job_id, user_id, github_login, backfill_days=None, partition_key=None, triggered_by=None):
    with db_session() as session:
        session.execute(
            text(
                "INSERT INTO sync_jobs(job_id, user_id, github_login, status, backfill_days, partition_key, triggered_by) "
                "VALUES (:job_id, :user_id, :github_login, 'PENDING', :backfill_days, :partition_key, :triggered_by)"
            ),
            {
                "job_id": str(job_id),
                "user_id": str(user_id),
                "github_login": str(github_login),
                "backfill_days": backfill_days,
                "partition_key": partition_key,
                "triggered_by": triggered_by,
            },
        )


def test_refresh_daily_ok_updates_job_fields_expected(monkeypatch):
    apply_pending_migrations()
    _truncate_sync_jobs_table()

    user_id = str(uuid.uuid4())
    job_id = uuid.uuid4()
    github_login = "calliope"
    partition_key = "daily:2025-12-24"
    triggered_by = "scheduler.daily"

    _insert_pending_job(
        job_id,
        user_id=user_id,
        github_login=github_login,
        backfill_days=None,
        partition_key=partition_key,
        triggered_by=triggered_by,
    )

    captured = {}

    def _fake_ingest_and_compute_user(**kwargs):
        captured.update(kwargs)
        window_start = datetime(2025, 12, 1, tzinfo=timezone.utc)
        window_end = datetime(2025, 12, 24, tzinfo=timezone.utc)
        return {
            "status": "ok",
            "user_id": user_id,
            "user": github_login,
            "sync_run_id": str(uuid.uuid4()),
            "compute_run_id": str(uuid.uuid4()),
            "baseline_id": "baseline-1",
            "window_start": window_start,
            "window_end": window_end,
        }

    monkeypatch.setattr("services.worker.app.tasks.ingest_and_compute_user", _fake_ingest_and_compute_user)

    result = refresh_daily.apply(
        args=[user_id],
        kwargs={"partition_key": partition_key, "triggered_by": triggered_by},
        task_id=str(job_id),
    ).get()
    assert result["status"] == "ok"

    assert captured["github_token"] is None
    assert captured["partition_key"] == partition_key
    assert captured["triggered_by"] == triggered_by
    assert captured["backfill_days"] is None

    with db_session() as session:
        row = session.execute(
            text(
                "SELECT status, error_message, started_at, completed_at, sync_run_id, compute_run_id, baseline_id "
                "FROM sync_jobs WHERE job_id=:job_id"
            ),
            {"job_id": str(job_id)},
        ).fetchone()

    assert row is not None
    assert row[0] == "COMPLETED"
    assert row[1] is None
    assert row[2] is not None
    assert row[3] is not None
    assert row[4] is not None
    assert row[5] is not None
    assert row[6] == "baseline-1"


def test_backfill_user_locked_sets_skipped_expected(monkeypatch):
    apply_pending_migrations()
    _truncate_sync_jobs_table()

    user_id = str(uuid.uuid4())
    job_id = uuid.uuid4()
    github_login = "calliope"
    partition_key = "backfill:2025-12-24:30d:wave1"
    triggered_by = "scheduler.backfill"

    _insert_pending_job(
        job_id,
        user_id=user_id,
        github_login=github_login,
        backfill_days=30,
        partition_key=partition_key,
        triggered_by=triggered_by,
    )

    def _fake_ingest_and_compute_user(**kwargs):
        _ = kwargs
        return {"status": "locked", "user_id": user_id, "user": github_login}

    monkeypatch.setattr("services.worker.app.tasks.ingest_and_compute_user", _fake_ingest_and_compute_user)

    result = backfill_user.apply(
        args=[user_id, 30],
        kwargs={"partition_key": partition_key, "triggered_by": triggered_by},
        task_id=str(job_id),
    ).get()
    assert result["status"] == "locked"

    with db_session() as session:
        row = session.execute(
            text("SELECT status, error_message FROM sync_jobs WHERE job_id=:job_id"),
            {"job_id": str(job_id)},
        ).fetchone()

    assert row is not None
    assert row[0] == "SKIPPED"
    assert str(row[1] or "") == "locked"
