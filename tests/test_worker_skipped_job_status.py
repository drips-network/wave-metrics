import uuid

from sqlalchemy import text

from services.shared.database import apply_pending_migrations, db_session
from services.worker.app.tasks import sync_and_compute


def _truncate_sync_jobs_table() -> None:
    with db_session() as session:
        session.execute(text("TRUNCATE sync_jobs"))


def test_sync_and_compute_locked_pipeline_sets_skipped_job_status_expected(monkeypatch):
    apply_pending_migrations()
    _truncate_sync_jobs_table()

    user_id = str(uuid.uuid4())
    job_id = uuid.uuid4()
    github_login = "octocat"

    with db_session() as session:
        session.execute(
            text(
                "INSERT INTO sync_jobs(job_id, user_id, github_login, status) "
                "VALUES (:job_id, :user_id, :github_login, 'PENDING')"
            ),
            {"job_id": str(job_id), "user_id": user_id, "github_login": github_login},
        )

    def _fake_lease(_session, token_ref, expected_user_id=None, lease_id=None, lease_ttl_seconds=None):
        _ = _session
        _ = token_ref
        _ = expected_user_id
        _ = lease_id
        _ = lease_ttl_seconds
        return "token-abc"

    def _fake_finalize(_session, token_ref, lease_id=None, success=False):
        _ = _session
        _ = token_ref
        _ = lease_id
        _ = success
        return None

    def _fake_fetch_user_login(_github_token):
        _ = _github_token
        return github_login, {"login": github_login}

    def _fake_ingest_and_compute_user(**kwargs):
        _ = kwargs
        return {"status": "locked", "user_id": user_id, "user": github_login}

    monkeypatch.setattr("services.worker.app.tasks.lease_github_token_ref", _fake_lease)
    monkeypatch.setattr("services.worker.app.tasks.finalize_github_token_ref", _fake_finalize)
    monkeypatch.setattr("services.worker.app.tasks.fetch_user_login", _fake_fetch_user_login)
    monkeypatch.setattr("services.worker.app.tasks.ingest_and_compute_user", _fake_ingest_and_compute_user)

    result = sync_and_compute.apply(
        args=[user_id, "token-ref-xyz"],
        kwargs={"backfill_days": 30},
        task_id=str(job_id),
    ).get()
    assert result["status"] == "locked"

    with db_session() as session:
        row = session.execute(
            text("SELECT status, error_message, started_at, completed_at FROM sync_jobs WHERE job_id=:job_id"),
            {"job_id": str(job_id)},
        ).fetchone()

    assert row is not None
    assert row[0] == "SKIPPED"
    assert str(row[1] or "") == "locked"
    assert row[2] is not None
    assert row[3] is not None
