from datetime import datetime, timezone
import uuid

from fastapi.testclient import TestClient
from sqlalchemy import text

from services.api.app.main import app
from services.shared.database import apply_pending_migrations, db_session
from services.shared import caching as caching_module


def _ensure_schema():
    """
    Apply migrations once for integration tests
    """
    apply_pending_migrations()


def _clear_tables():
    """
    Truncate tables used by API metrics tests to avoid cross-test interference
    """
    with db_session() as session:
        session.execute(text("TRUNCATE contributor_language_profile, contributor_metrics, users CASCADE"))


def _insert_metrics_fixture(user_id):
    """
    Insert minimal contributor_metrics and contributor_language_profile rows
    """
    now = datetime(2025, 1, 1, tzinfo=timezone.utc)

    with db_session() as session:
        github_user_id = uuid.UUID(user_id).int & ((1 << 63) - 1)
        session.execute(
            text(
                "INSERT INTO users(id, github_login, github_user_id, github_created_at, avatar_url)"
                " VALUES (:id, :login, :github_user_id, :created, '')"
                " ON CONFLICT (github_login) DO NOTHING"
            ),
            {"id": user_id, "login": "login", "github_user_id": github_user_id, "created": now},
        )

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
                " :u, :login, :ws, :we,"
                " :bid, :bs, :be,"
                " 10, 8, 1,"
                " 0.8, 0.1, 12.5,"
                " 10, 2, 1, 50.0,"
                " 10.0, 20.0, NULL,"
                " NULL, NULL,"
                " 30.0, 40.0, 50.0,"
                " 60.0, :computed"
                ")"
            ),
            {
                "u": user_id,
                "login": "login",
                "ws": now,
                "we": now,
                "bid": "baseline-1",
                "bs": now.date(),
                "be": now.date(),
                "computed": now,
            },
        )

        session.execute(
            text(
                "INSERT INTO contributor_language_profile (user_id, language, pct, computed_at)"
                " VALUES (:u, 'Python', 80.0, :computed)"
            ),
            {"u": user_id, "computed": now},
        )


def test_metrics_endpoint_invalid_user_id_returns_422_expected():
    client = TestClient(app)
    resp = client.get("/api/v1/metrics", params={"user_id": "not-a-uuid"})
    assert resp.status_code == 422


def test_metrics_endpoint_uses_single_cache_key(monkeypatch):
    _ensure_schema()
    _clear_tables()

    user_id = str(uuid.uuid4())
    _insert_metrics_fixture(user_id)

    class FakeRedis:
        def __init__(self):
            self._store = {}

        def get(self, key):
            return self._store.get(key)

        def setex(self, key, ttl, value):
            _ = ttl
            self._store[key] = value

        def delete(self, key):
            self._store.pop(key, None)

        def ping(self):
            return True

    fake = FakeRedis()

    def _fake_get_redis():
        return fake

    monkeypatch.setattr(caching_module, "_redis", fake)
    monkeypatch.setattr(caching_module, "get_redis", _fake_get_redis)

    client = TestClient(app)
    resp1 = client.get("/api/v1/metrics", params={"user_id": user_id})
    assert resp1.status_code == 200

    resp2 = client.get("/api/v1/metrics", params={"user_id": user_id})
    assert resp2.status_code == 200

    key = f"metrics:{user_id}"
    assert fake.get(key) is not None


def test_metrics_by_login_returns_404_for_unknown_login_expected():
    _ensure_schema()
    _clear_tables()

    client = TestClient(app)
    resp = client.get("/api/v1/metrics/by-login", params={"github_login": "missing"})
    assert resp.status_code == 404


def test_metrics_by_login_populates_metrics_cache_key_expected(monkeypatch):
    _ensure_schema()
    _clear_tables()

    user_id = str(uuid.uuid4())
    _insert_metrics_fixture(user_id)

    class FakeRedis:
        def __init__(self):
            self._store = {}

        def get(self, key):
            return self._store.get(key)

        def setex(self, key, ttl, value):
            _ = ttl
            self._store[key] = value

        def delete(self, key):
            self._store.pop(key, None)

        def ping(self):
            return True

    fake = FakeRedis()

    def _fake_get_redis():
        return fake

    monkeypatch.setattr(caching_module, "_redis", fake)
    monkeypatch.setattr(caching_module, "get_redis", _fake_get_redis)

    client = TestClient(app)
    resp = client.get("/api/v1/metrics/by-login", params={"github_login": "login"})
    assert resp.status_code == 200
    payload = resp.json()
    assert payload["user_id"] == user_id

    key = f"metrics:{user_id}"
    assert fake.get(key) is not None

