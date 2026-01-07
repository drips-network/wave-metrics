from datetime import datetime, timedelta, timezone
import uuid

from fastapi.testclient import TestClient
from sqlalchemy import text

from services.api.app import main as api_main
from services.api.app.main import app
from services.shared.database import apply_pending_migrations, db_session


def _insert_user(user_id: str, github_login: str, github_user_id: int) -> None:
    with db_session() as session:
        session.execute(
            text(
                "INSERT INTO users(id, github_login, github_user_id, github_created_at, avatar_url) "
                "VALUES (:id, :login, :gh_uid, NOW(), '')"
            ),
            {"id": user_id, "login": github_login, "gh_uid": github_user_id},
        )


def _ensure_schema():
    apply_pending_migrations()


def _clear_tables():
    with db_session() as session:
        session.execute(
            text(
                "TRUNCATE github_login_aliases, sync_jobs, github_sync_state, users CASCADE"
            )
        )


def test_sync_token_only_returns_canonical_ids_expected(monkeypatch):
    _ensure_schema()
    _clear_tables()

    sent = {}

    def _fake_send_task(name, args=None, task_id=None, **kwargs):
        sent["queue"] = kwargs.get("queue")
        sent["name"] = name
        sent["args"] = args or []
        sent["task_id"] = task_id
        return object()

    def _fake_create_token_ref(_session, user_id, token, ttl_seconds=None):
        sent["token_user_id"] = user_id
        sent["token"] = token
        sent["ttl_seconds"] = ttl_seconds
        return "token-ref-123"

    def _fake_delete_token_ref(_session, token_ref):
        sent["deleted_token_ref"] = token_ref
        return None

    def _fake_fetch_user_login(_token):
        now = datetime(2020, 1, 1, tzinfo=timezone.utc)
        return "OctoCat", {"login": "OctoCat", "databaseId": 123, "createdAt": now, "avatarUrl": ""}

    monkeypatch.setattr(api_main.celery_client, "send_task", _fake_send_task)
    monkeypatch.setattr(api_main, "create_github_token_ref", _fake_create_token_ref)
    monkeypatch.setattr(api_main, "delete_github_token_ref", _fake_delete_token_ref)
    monkeypatch.setattr(api_main, "fetch_user_login", _fake_fetch_user_login)

    client = TestClient(app)
    resp = client.post("/api/v1/sync", json={"github_token": "dummy"})
    assert resp.status_code == 200

    payload = resp.json()
    assert payload["status"] == "enqueued"
    assert payload["github_login"] == "octocat"
    uuid.UUID(payload["job_id"])
    uuid.UUID(payload["user_id"])

    assert sent["task_id"] == payload["job_id"]
    assert sent["args"][0] == payload["user_id"]
    assert sent["args"][1] == "token-ref-123"
    assert sent["args"][3] == "api"
    assert sent["queue"] == "default"
    assert sent["ttl_seconds"] == api_main.shared_config.TOKEN_REF_TTL_SECONDS_NORMAL

    with db_session() as session:
        job_row = session.execute(
            text("SELECT status, user_id, github_login FROM sync_jobs WHERE job_id=:job_id"),
            {"job_id": payload["job_id"]},
        ).fetchone()
        assert job_row is not None
        assert job_row[0] == "PENDING"
        assert str(job_row[1]) == payload["user_id"]
        assert job_row[2] == "octocat"

        alias_row = session.execute(
            text(
                "SELECT user_id, github_user_id, confirmed_at, expires_at "
                "FROM github_login_aliases WHERE github_login=:login"
            ),
            {"login": "octocat"},
        ).fetchone()
        assert alias_row is not None
        assert str(alias_row[0]) == payload["user_id"]
        assert int(alias_row[1]) == 123
        assert alias_row[2] is not None
        assert alias_row[3] is None


def test_sync_login_only_does_not_call_github_expected(monkeypatch):
    _ensure_schema()
    _clear_tables()

    sent = {}

    def _fake_send_task(name, args=None, task_id=None, **kwargs):
        sent["queue"] = kwargs.get("queue")
        sent["name"] = name
        sent["args"] = args or []
        sent["task_id"] = task_id
        return object()

    def _fake_create_token_ref(_session, user_id, token, ttl_seconds=None):
        sent["token_user_id"] = user_id
        sent["token"] = token
        sent["ttl_seconds"] = ttl_seconds
        return "token-ref-123"

    def _fake_fetch_user_login(_token):
        raise AssertionError("fetch_user_login should not be called for login-provided requests")

    monkeypatch.setattr(api_main.celery_client, "send_task", _fake_send_task)
    monkeypatch.setattr(api_main, "create_github_token_ref", _fake_create_token_ref)
    monkeypatch.setattr(api_main, "fetch_user_login", _fake_fetch_user_login)

    client = TestClient(app)
    resp = client.post("/api/v1/sync", json={"github_login": "Some-Login", "github_token": "dummy"})
    assert resp.status_code == 200

    payload = resp.json()
    assert payload["status"] == "enqueued"
    assert payload["github_login"] == "some-login"
    uuid.UUID(payload["job_id"])
    uuid.UUID(payload["user_id"])
    assert sent["args"][3] == "api"
    assert sent["queue"] == "default"
    assert sent["ttl_seconds"] == api_main.shared_config.TOKEN_REF_TTL_SECONDS_NORMAL


def test_sync_defaults_to_default_queue_expected(monkeypatch):
    _ensure_schema()
    _clear_tables()

    sent = {}

    def _fake_send_task(name, args=None, task_id=None, **kwargs):
        sent["queue"] = kwargs.get("queue")
        sent["args"] = args or []
        return object()

    def _fake_create_token_ref(_session, user_id, token, ttl_seconds=None):
        sent["ttl_seconds"] = ttl_seconds
        return "token-ref-123"

    def _fake_fetch_user_login(_token):
        now = datetime(2020, 1, 1, tzinfo=timezone.utc)
        return "OctoCat", {"login": "OctoCat", "databaseId": 123, "createdAt": now, "avatarUrl": ""}

    monkeypatch.setattr(api_main.celery_client, "send_task", _fake_send_task)
    monkeypatch.setattr(api_main, "create_github_token_ref", _fake_create_token_ref)
    monkeypatch.setattr(api_main, "fetch_user_login", _fake_fetch_user_login)

    client = TestClient(app)
    resp = client.post("/api/v1/sync", json={"github_token": "dummy"})
    assert resp.status_code == 200

    assert sent["queue"] == "default"
    assert sent["ttl_seconds"] == api_main.shared_config.TOKEN_REF_TTL_SECONDS_NORMAL
    assert sent["args"][3] == "api"


def test_sync_body_queue_bulk_routes_bulk_queue_and_uses_bulk_ttl_expected(monkeypatch):
    _ensure_schema()
    _clear_tables()

    sent = {}

    def _fake_send_task(name, args=None, task_id=None, **kwargs):
        sent["name"] = name
        sent["args"] = args or []
        sent["task_id"] = task_id
        sent["queue"] = kwargs.get("queue")
        return object()

    def _fake_create_token_ref(_session, user_id, token, ttl_seconds=None):
        sent["token_user_id"] = user_id
        sent["token"] = token
        sent["ttl_seconds"] = ttl_seconds
        return "token-ref-123"

    def _fake_fetch_user_login(_token):
        now = datetime(2020, 1, 1, tzinfo=timezone.utc)
        return "OctoCat", {"login": "OctoCat", "databaseId": 123, "createdAt": now, "avatarUrl": ""}

    monkeypatch.setattr(api_main.celery_client, "send_task", _fake_send_task)
    monkeypatch.setattr(api_main, "create_github_token_ref", _fake_create_token_ref)
    monkeypatch.setattr(api_main, "fetch_user_login", _fake_fetch_user_login)

    client = TestClient(app)
    resp = client.post(
        "/api/v1/sync",
        json={"github_token": "dummy", "queue": "bulk"},
    )
    assert resp.status_code == 200

    payload = resp.json()
    assert payload["status"] == "enqueued"
    assert sent["queue"] == "bulk"
    assert sent["ttl_seconds"] == api_main.shared_config.TOKEN_REF_TTL_SECONDS_BULK
    assert sent["args"][3] == "api.bulk"


def test_jobs_endpoint_returns_404_for_unknown_job_expected():
    _ensure_schema()
    _clear_tables()

    client = TestClient(app)
    resp = client.get(f"/api/v1/jobs/{uuid.uuid4()}")
    assert resp.status_code == 404


def test_sync_rejects_unknown_user_id_without_github_login_expected(monkeypatch):
    _ensure_schema()
    _clear_tables()

    def _fake_send_task(name, args=None, task_id=None, **kwargs):
        raise AssertionError("send_task should not be called for invalid requests")

    monkeypatch.setattr(api_main.celery_client, "send_task", _fake_send_task)

    client = TestClient(app)
    resp = client.post("/api/v1/sync", json={"user_id": str(uuid.uuid4()), "github_token": "dummy"})
    assert resp.status_code == 400
    assert "user_id not found" in resp.json()["detail"]


def test_sync_rejects_user_id_and_github_login_mismatch_expected(monkeypatch):
    _ensure_schema()
    _clear_tables()

    existing_user_id = str(uuid.uuid4())
    _insert_user(existing_user_id, "octocat", github_user_id=123)

    def _fake_send_task(name, args=None, task_id=None, **kwargs):
        raise AssertionError("send_task should not be called for invalid requests")

    monkeypatch.setattr(api_main.celery_client, "send_task", _fake_send_task)

    client = TestClient(app)
    resp = client.post(
        "/api/v1/sync",
        json={"user_id": existing_user_id, "github_login": "different", "github_token": "dummy"},
    )
    assert resp.status_code == 400
    assert resp.json()["detail"] == "user_id and github_login mismatch"


def test_jobs_endpoint_marks_stale_running_failed_expected():
    _ensure_schema()
    _clear_tables()

    job_id = str(uuid.uuid4())
    user_id = str(uuid.uuid4())
    started_at = datetime.now(timezone.utc) - timedelta(hours=2)

    with db_session() as session:
        session.execute(
            text(
                "INSERT INTO sync_jobs (job_id, user_id, github_login, status, created_at, started_at) "
                "VALUES (:job_id, :user_id, :login, 'RUNNING', :created_at, :started_at)"
            ),
            {
                "job_id": job_id,
                "user_id": user_id,
                "login": "octocat",
                "created_at": started_at,
                "started_at": started_at,
            },
        )

    client = TestClient(app)
    resp = client.get(f"/api/v1/jobs/{job_id}")
    assert resp.status_code == 200

    payload = resp.json()
    assert payload["status"] == "FAILED"
    assert "stale" in (payload.get("error_message") or "").lower()
    assert payload["completed_at"] is not None

    with db_session() as session:
        row = session.execute(
            text("SELECT status, stale_marked_at, completed_at FROM sync_jobs WHERE job_id=:job_id"),
            {"job_id": job_id},
        ).fetchone()
        assert row is not None
        assert row[0] == "FAILED"
        assert row[1] is not None
        assert row[2] is not None
