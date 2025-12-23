from services.worker.app.tasks import sync_and_compute


def test_sync_and_compute_celery_wrapper_delegates_to_pipeline(monkeypatch):
    """
    Ensure Celery task passes arguments through to ingest_and_compute_user
    """
    captured = {}
    lease_called = {}
    finalize_called = {}

    def _fake_ingest_and_compute_user(
        user_id,
        github_token,
        backfill_days=None,
        since=None,
        until=None,
        partition_key=None,
        triggered_by=None,
        persist_github_token=False,
    ):
        captured["user_id"] = user_id
        captured["github_token"] = github_token
        captured["backfill_days"] = backfill_days
        captured["partition_key"] = partition_key
        captured["triggered_by"] = triggered_by
        captured["persist_github_token"] = persist_github_token
        return {"status": "ok", "user_id": user_id}

    monkeypatch.setattr("services.worker.app.tasks.ingest_and_compute_user", _fake_ingest_and_compute_user)

    def _fake_db_session():
        class _CM:
            def __enter__(self):
                return object()

            def __exit__(self, exc_type, exc, tb):
                _ = exc_type
                _ = exc
                _ = tb
                return False

        return _CM()

    def _fake_lease(_session, token_ref, expected_user_id=None, lease_id=None, lease_ttl_seconds=None):
        _ = lease_ttl_seconds
        lease_called["token_ref"] = token_ref
        lease_called["expected_user_id"] = expected_user_id
        lease_called["lease_id"] = lease_id
        return "token-abc"

    def _fake_finalize(_session, token_ref, lease_id=None, success=False):
        finalize_called["token_ref"] = token_ref
        finalize_called["lease_id"] = lease_id
        finalize_called["success"] = success
        return None

    monkeypatch.setattr("services.worker.app.tasks.db_session", _fake_db_session)
    monkeypatch.setattr("services.worker.app.tasks.lease_github_token_ref", _fake_lease)
    monkeypatch.setattr("services.worker.app.tasks.finalize_github_token_ref", _fake_finalize)

    result = sync_and_compute("user-123", "token-ref-xyz", backfill_days=30)

    assert captured["user_id"] == "user-123"
    assert captured["github_token"] == "token-abc"
    assert captured["backfill_days"] == 30
    assert captured["partition_key"] is None
    assert captured["triggered_by"] == "api"
    assert captured["persist_github_token"] is False

    assert lease_called["token_ref"] == "token-ref-xyz"
    assert lease_called["expected_user_id"] == "user-123"
    assert lease_called["lease_id"] == "token-ref-xyz"

    assert finalize_called["token_ref"] == "token-ref-xyz"
    assert finalize_called["lease_id"] == "token-ref-xyz"
    assert finalize_called["success"] is True

    assert result["status"] == "ok"
    assert result["user_id"] == "user-123"
