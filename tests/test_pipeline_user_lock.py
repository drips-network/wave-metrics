from datetime import datetime, timezone

import pytest
from sqlalchemy import text

from services.shared.database import apply_pending_migrations, db_session
from services.shared.pipeline import ingest_and_compute_user


def test_ingest_and_compute_user_returns_locked_when_lock_held_expected(monkeypatch):
    apply_pending_migrations()

    with db_session() as session:
        session.execute(text("DELETE FROM users"))

    def _fake_fetch_user_login(_token):
        viewer = {
            "databaseId": 999,
            "createdAt": datetime(2020, 1, 1, tzinfo=timezone.utc),
            "avatarUrl": "https://example.com/avatar",
        }
        return "locked-user", viewer

    def _fake_acquire_user_lock(_user_id, ttl_seconds=900, wait_timeout_seconds=0):
        _ = ttl_seconds
        _ = wait_timeout_seconds
        raise TimeoutError("lock held")

    def _should_not_be_called(*_args, **_kwargs):
        raise AssertionError("ingest_user_github_activity should not be called when locked")

    monkeypatch.setattr("services.shared.pipeline.fetch_user_login", _fake_fetch_user_login)
    monkeypatch.setattr("services.shared.pipeline.acquire_user_lock", _fake_acquire_user_lock)
    monkeypatch.setattr("services.shared.pipeline.ingest_user_github_activity", _should_not_be_called)

    result = ingest_and_compute_user(
        user_id=None,
        github_token="dummy-token",
        backfill_days=30,
        partition_key="test",
        triggered_by="test",
    )

    assert result["status"] == "locked"
    assert result["user"] == "locked-user"
    assert isinstance(result["user_id"], str)


def test_ingest_and_compute_user_releases_lock_on_exception_expected(monkeypatch):
    apply_pending_migrations()

    with db_session() as session:
        session.execute(text("TRUNCATE github_sync_runs, metric_compute_runs, github_sync_state CASCADE"))
        session.execute(text("TRUNCATE pull_requests, repository_languages, repositories, users CASCADE"))

    def _fake_fetch_user_login(_token):
        viewer = {
            "databaseId": 999,
            "createdAt": datetime(2020, 1, 1, tzinfo=timezone.utc),
            "avatarUrl": "https://example.com/avatar",
        }
        return "exc-user", viewer

    def _fake_acquire_user_lock(_user_id, ttl_seconds=900, wait_timeout_seconds=0):
        _ = ttl_seconds
        _ = wait_timeout_seconds
        return "lock-1"

    released = {}

    def _fake_release_user_lock(user_id, lock_value):
        released["user_id"] = user_id
        released["lock_value"] = lock_value

    def _fake_ingest_user_github_activity(*_args, **_kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr("services.shared.pipeline.fetch_user_login", _fake_fetch_user_login)
    monkeypatch.setattr("services.shared.pipeline.acquire_user_lock", _fake_acquire_user_lock)
    monkeypatch.setattr("services.shared.pipeline.release_user_lock", _fake_release_user_lock)
    monkeypatch.setattr("services.shared.pipeline.ingest_user_github_activity", _fake_ingest_user_github_activity)

    with pytest.raises(RuntimeError):
        _ = ingest_and_compute_user(
            user_id=None,
            github_token="dummy-token",
            backfill_days=30,
            partition_key="test",
            triggered_by="test",
        )

    assert released["lock_value"] == "lock-1"

    with db_session() as session:
        user_row = session.execute(text("SELECT id FROM users WHERE github_login = 'exc-user'")).fetchone()
        assert user_row is not None
        assert released["user_id"] == str(user_row[0])
