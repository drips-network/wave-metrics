from datetime import datetime, timezone

from sqlalchemy import text

from services.shared.database import apply_pending_migrations, db_session
from services.shared.pipeline import ingest_user_github_activity


def test_ingest_repo_language_cache_eviction_refetches_expected(monkeypatch):
    apply_pending_migrations()

    with db_session() as session:
        session.execute(text("TRUNCATE github_sync_runs, github_sync_state CASCADE"))
        session.execute(text("TRUNCATE pull_requests, repository_languages, repositories, users CASCADE"))

    def _fake_fetch_user_login(_token):
        viewer = {
            "databaseId": 123,
            "createdAt": datetime(2020, 1, 1, tzinfo=timezone.utc),
            "avatarUrl": "https://example.com/avatar",
        }
        return "lru-user", viewer

    def _fake_iter_user_prs_windowed(_token, _login, since_dt, until_dt, max_window_days=180):
        _ = max_window_days
        yield {
            "databaseId": 1,
            "number": 1,
            "url": "https://example.com/pr/1",
            "state": "MERGED",
            "isDraft": False,
            "createdAt": since_dt.isoformat(),
            "mergedAt": until_dt.isoformat(),
            "closedAt": until_dt.isoformat(),
            "additions": 1,
            "deletions": 1,
            "changedFiles": 1,
            "merged": True,
            "updatedAt": until_dt.isoformat(),
            "comments": {"totalCount": 0},
            "reviews": {"totalCount": 0},
            "repository": {
                "databaseId": 100,
                "name": "repo-a",
                "isPrivate": False,
                "owner": {"login": "owner"},
            },
        }
        yield {
            "databaseId": 2,
            "number": 2,
            "url": "https://example.com/pr/2",
            "state": "MERGED",
            "isDraft": False,
            "createdAt": since_dt.isoformat(),
            "mergedAt": until_dt.isoformat(),
            "closedAt": until_dt.isoformat(),
            "additions": 1,
            "deletions": 1,
            "changedFiles": 1,
            "merged": True,
            "updatedAt": until_dt.isoformat(),
            "comments": {"totalCount": 0},
            "reviews": {"totalCount": 0},
            "repository": {
                "databaseId": 200,
                "name": "repo-b",
                "isPrivate": False,
                "owner": {"login": "owner"},
            },
        }
        yield {
            "databaseId": 3,
            "number": 3,
            "url": "https://example.com/pr/3",
            "state": "MERGED",
            "isDraft": False,
            "createdAt": since_dt.isoformat(),
            "mergedAt": until_dt.isoformat(),
            "closedAt": until_dt.isoformat(),
            "additions": 1,
            "deletions": 1,
            "changedFiles": 1,
            "merged": True,
            "updatedAt": until_dt.isoformat(),
            "comments": {"totalCount": 0},
            "reviews": {"totalCount": 0},
            "repository": {
                "databaseId": 100,
                "name": "repo-a",
                "isPrivate": False,
                "owner": {"login": "owner"},
            },
        }

    fetch_calls = {"repos": []}

    def _fake_fetch_repo_languages(_token, _owner, name):
        fetch_calls["repos"].append(name)
        return [{"language": "Python", "bytes": 1000}]

    def _fake_upsert_repository(_session, github_repo_id, _owner_login, _name, _is_private):
        return int(github_repo_id)

    def _noop(*_args, **_kwargs):
        return None

    monkeypatch.setattr("services.shared.pipeline.REPO_LANGUAGE_CACHE_MAX_REPOS", 1)
    monkeypatch.setattr("services.shared.pipeline.fetch_user_login", _fake_fetch_user_login)
    monkeypatch.setattr("services.shared.pipeline.iter_user_prs_windowed", _fake_iter_user_prs_windowed)
    monkeypatch.setattr("services.shared.pipeline.fetch_repo_languages", _fake_fetch_repo_languages)
    monkeypatch.setattr("services.shared.pipeline._upsert_repository", _fake_upsert_repository)
    monkeypatch.setattr("services.shared.pipeline._upsert_repo_languages", _noop)
    monkeypatch.setattr("services.shared.pipeline._upsert_pull_request", _noop)

    _ = ingest_user_github_activity(
        user_id=None,
        github_token="dummy-token",
        since=None,
        until=None,
        backfill_days=30,
        partition_key="lru",
        triggered_by="test",
    )

    assert fetch_calls["repos"] == ["repo-a", "repo-b", "repo-a"]


def test_ingest_repo_languages_persist_even_if_evicted_before_flush_expected(monkeypatch):
    apply_pending_migrations()

    with db_session() as session:
        session.execute(text("TRUNCATE github_sync_runs, github_sync_state CASCADE"))
        session.execute(text("TRUNCATE pull_requests, repository_languages, repositories, users CASCADE"))

    def _fake_fetch_user_login(_token):
        viewer = {
            "databaseId": 456,
            "createdAt": datetime(2020, 1, 1, tzinfo=timezone.utc),
            "avatarUrl": "https://example.com/avatar",
        }
        return "lru-user-2", viewer

    def _fake_iter_user_prs_windowed(_token, _login, since_dt, until_dt, max_window_days=180):
        _ = max_window_days
        yield {
            "databaseId": 10,
            "number": 10,
            "url": "https://example.com/pr/10",
            "state": "MERGED",
            "isDraft": False,
            "createdAt": since_dt.isoformat(),
            "mergedAt": until_dt.isoformat(),
            "closedAt": until_dt.isoformat(),
            "additions": 1,
            "deletions": 1,
            "changedFiles": 1,
            "merged": True,
            "updatedAt": until_dt.isoformat(),
            "comments": {"totalCount": 0},
            "reviews": {"totalCount": 0},
            "repository": {
                "databaseId": 100,
                "name": "repo-a",
                "isPrivate": False,
                "owner": {"login": "owner"},
            },
        }
        yield {
            "databaseId": 20,
            "number": 20,
            "url": "https://example.com/pr/20",
            "state": "MERGED",
            "isDraft": False,
            "createdAt": since_dt.isoformat(),
            "mergedAt": until_dt.isoformat(),
            "closedAt": until_dt.isoformat(),
            "additions": 1,
            "deletions": 1,
            "changedFiles": 1,
            "merged": True,
            "updatedAt": until_dt.isoformat(),
            "comments": {"totalCount": 0},
            "reviews": {"totalCount": 0},
            "repository": {
                "databaseId": 200,
                "name": "repo-b",
                "isPrivate": False,
                "owner": {"login": "owner"},
            },
        }

    def _fake_fetch_repo_languages(_token, _owner, name):
        if name == "repo-a":
            return [{"language": "Python", "bytes": 1000}]
        if name == "repo-b":
            return [{"language": "Go", "bytes": 2000}]
        raise AssertionError(f"unexpected repo name: {name}")

    monkeypatch.setattr("services.shared.pipeline.REPO_LANGUAGE_CACHE_MAX_REPOS", 1)
    monkeypatch.setattr("services.shared.pipeline.fetch_user_login", _fake_fetch_user_login)
    monkeypatch.setattr("services.shared.pipeline.iter_user_prs_windowed", _fake_iter_user_prs_windowed)
    monkeypatch.setattr("services.shared.pipeline.fetch_repo_languages", _fake_fetch_repo_languages)

    _ = ingest_user_github_activity(
        user_id=None,
        github_token="dummy-token",
        since=None,
        until=None,
        backfill_days=30,
        partition_key="lru",
        triggered_by="test",
    )

    with db_session() as session:
        rows = session.execute(
            text(
                "SELECT r.github_repo_id, rl.language "
                "FROM repositories r "
                "JOIN repository_languages rl ON rl.repository_id = r.id "
                "WHERE r.github_repo_id IN (100, 200)"
            )
        ).fetchall()

    pairs = {(int(row[0]), str(row[1])) for row in rows}
    assert (100, "Python") in pairs
    assert (200, "Go") in pairs
