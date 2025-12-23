from datetime import datetime, timedelta, timezone

from sqlalchemy import text

from services.shared.database import apply_pending_migrations, db_session
from services.shared.pipeline import (
    compute_user_metrics_and_language_profile,
    ingest_and_compute_user,
    ingest_user_github_activity,
)
from services.shared.users import list_tracked_users

from services.shared import percentiles


def _clear_tables():
    """
    Clear tables used in integration tests to avoid cross-test interference

    Delete only test baseline rows
    """
    with db_session() as session:
        session.execute(text("TRUNCATE github_sync_runs, metric_compute_runs, github_sync_state CASCADE"))
        session.execute(
            text(
                "TRUNCATE contributor_language_profile, contributor_metrics, "
                "pull_requests, repository_languages, repositories, users CASCADE"
            )
        )
        session.execute(text("DELETE FROM population_cdfs WHERE baseline_id LIKE 'baseline-test%'"))
        session.execute(text("DELETE FROM baseline_metadata WHERE baseline_id LIKE 'baseline-test%'"))


def _seed_baseline(baseline_id="baseline-test"):
    """
    Seed baseline_metadata for baseline resolution during compute

    Also pins baseline resolution to the seeded baseline id, so these tests do
    not depend on any local `.env` POPULATION_BASELINE_ID.
    """
    percentiles.POPULATION_BASELINE_ID = str(baseline_id)
    now = datetime(2025, 1, 1, tzinfo=timezone.utc)
    with db_session() as session:
        session.execute(
            text(
                "INSERT INTO baseline_metadata (baseline_id, baseline_start_date, baseline_end_date, computed_at, notes)"  # noqa
                " VALUES (:bid, :bs, :be, :computed, :notes)"
                " ON CONFLICT (baseline_id) DO UPDATE SET computed_at=EXCLUDED.computed_at"
            ),
            {
                "bid": baseline_id,
                "bs": now.date(),
                "be": now.date(),
                "computed": now,
                "notes": "test",
            },
        )


def _insert_user_and_repo(user_id, github_login="login"):
    with db_session() as session:
        session.execute(
            text(
                "INSERT INTO users(id, github_login, github_user_id, github_created_at, avatar_url) "
                "VALUES (:id, :login, 1, NOW(), '')"
            ),
            {"id": user_id, "login": github_login},
        )
        session.execute(
            text(
                "INSERT INTO repositories (github_repo_id, owner_login, name, is_private) "
                "VALUES (100, 'owner', 'repo', FALSE) "
                "ON CONFLICT (github_repo_id) DO NOTHING"
            )
        )


def _insert_eligible_prs(user_id, count, state="MERGED"):
    now = datetime.now(timezone.utc)
    with db_session() as session:
        repo_row = session.execute(text("SELECT id FROM repositories WHERE github_repo_id = 100")).fetchone()
        repo_id = int(repo_row[0])
        for i in range(count):
            session.execute(
                text(
                    "INSERT INTO pull_requests (github_pr_id, repository_id, author_user_id, number, state, is_draft, "
                    "created_at, merged_at, closed_at, comment_count, review_count, additions, deletions, changed_files, "  # noqa
                    "updated_at, url) "
                    "VALUES (:gid, :rid, :uid, :number, :state, FALSE, :created, :merged, :closed, 1, 0, 1, 1, 1, :updated, :url)"  # noqa
                ),
                {
                    "gid": 10_000 + i,
                    "rid": repo_id,
                    "uid": user_id,
                    "number": i + 1,
                    "state": state,
                    "created": now - timedelta(days=2),
                    "merged": now - timedelta(days=1) if state == "MERGED" else None,
                    "closed": now - timedelta(days=1) if state != "OPEN" else None,
                    "updated": now - timedelta(hours=1),
                    "url": f"https://example.com/pr/{10_000 + i}",
                },
            )


def test_list_tracked_users_reflects_inserted_users():
    apply_pending_migrations()
    _clear_tables()

    user_id = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
    with db_session() as session:
        session.execute(
            text(
                "INSERT INTO users(id, github_login, github_user_id, github_created_at, avatar_url)"
                " VALUES (:id, 'login', 1, NOW(), '')"
            ),
            {"id": user_id},
        )

    users = list_tracked_users()
    assert str(users[0]) == user_id


def test_ingest_user_github_activity_updates_sync_state_and_metadata(monkeypatch):
    apply_pending_migrations()
    _clear_tables()

    def _fake_fetch_user_login(token):
        _ = token
        viewer = {
            "databaseId": 999,
            "createdAt": datetime(2020, 1, 1, tzinfo=timezone.utc),
            "avatarUrl": "https://example.com/avatar",
        }
        return "ingest-user", viewer

    def _fake_iter_user_prs_windowed(token, login, since_dt, until_dt, max_window_days=180):
        _ = token
        _ = login
        _ = max_window_days
        yield {
            "databaseId": 400,
            "number": 1,
            "url": "https://example.com/pr/400",
            "state": "MERGED",
            "isDraft": False,
            "createdAt": (since_dt + timedelta(days=1)).isoformat(),
            "mergedAt": until_dt.isoformat(),
            "closedAt": until_dt.isoformat(),
            "additions": 10,
            "deletions": 5,
            "changedFiles": 1,
            "merged": True,
            "updatedAt": until_dt.isoformat(),
            "comments": {"totalCount": 1},
            "reviews": {"totalCount": 0},
            "repository": {
                "databaseId": 500,
                "name": "repo",
                "isPrivate": False,
                "owner": {"login": "owner"},
            },
        }

    def _fake_fetch_repo_languages(token, owner, name):
        _ = token
        _ = owner
        _ = name
        return [{"language": "Python", "bytes": 1000}]

    monkeypatch.setattr("services.shared.pipeline.fetch_user_login", _fake_fetch_user_login)
    monkeypatch.setattr("services.shared.pipeline.iter_user_prs_windowed", _fake_iter_user_prs_windowed)
    monkeypatch.setattr("services.shared.pipeline.fetch_repo_languages", _fake_fetch_repo_languages)

    summary = ingest_user_github_activity(
        user_id=None,
        github_token="dummy-token",
        since=None,
        until=None,
        backfill_days=30,
        partition_key="ingest-metadata",
        triggered_by="test",
    )

    internal_user_id = summary["user_id"]

    with db_session() as session:
        state_row = session.execute(
            text("SELECT last_pr_updated_at FROM github_sync_state WHERE user_id=:u"),
            {"u": internal_user_id},
        ).fetchone()
        assert state_row is not None
        assert state_row[0] == summary["window_end"]

        run_row = session.execute(
            text(
                "SELECT user_id, partition_key, status, window_start, window_end "
                "FROM github_sync_runs WHERE user_id=:u AND partition_key=:pk"
            ),
            {"u": internal_user_id, "pk": "ingest-metadata"},
        ).fetchone()
        assert run_row is not None
        assert str(run_row[0]) == internal_user_id
        assert run_row[1] == "ingest-metadata"
        assert run_row[2] == "SUCCESS"
        assert run_row[3] == summary["window_start"]
        assert run_row[4] == summary["window_end"]


def test_github_sync_state_last_pr_updated_at_is_monotonic_expected(monkeypatch):
    apply_pending_migrations()
    _clear_tables()

    def _fake_fetch_user_login(token):
        _ = token
        viewer = {
            "databaseId": 999,
            "createdAt": datetime(2020, 1, 1, tzinfo=timezone.utc),
            "avatarUrl": "https://example.com/avatar",
        }
        return "monotonic-user", viewer

    def _fake_iter_user_prs_windowed(_token, _login, _since_dt, _until_dt, max_window_days=180):
        _ = max_window_days
        yield from ()

    monkeypatch.setattr("services.shared.pipeline.fetch_user_login", _fake_fetch_user_login)
    monkeypatch.setattr("services.shared.pipeline.iter_user_prs_windowed", _fake_iter_user_prs_windowed)

    later_until = datetime(2025, 1, 10, tzinfo=timezone.utc)
    earlier_until = datetime(2025, 1, 5, tzinfo=timezone.utc)

    first = ingest_user_github_activity(
        user_id=None,
        github_token="dummy-token",
        since=datetime(2025, 1, 1, tzinfo=timezone.utc),
        until=later_until,
        backfill_days=None,
        partition_key="monotonic-1",
        triggered_by="test",
    )

    _ = ingest_user_github_activity(
        user_id=first["user_id"],
        github_token="dummy-token",
        since=datetime(2025, 1, 1, tzinfo=timezone.utc),
        until=earlier_until,
        backfill_days=None,
        partition_key="monotonic-2",
        triggered_by="test",
    )

    with db_session() as session:
        state_row = session.execute(
            text("SELECT last_pr_updated_at FROM github_sync_state WHERE user_id=:u"),
            {"u": first["user_id"]},
        ).fetchone()
        assert state_row is not None
        assert state_row[0] == first["window_end"]


def test_ingest_and_compute_user_end_to_end_with_mocked_github(monkeypatch):
    apply_pending_migrations()
    _clear_tables()
    _seed_baseline()

    def _fake_fetch_user_login(token):
        _ = token
        viewer = {
            "databaseId": 123,
            "createdAt": datetime(2020, 1, 1, tzinfo=timezone.utc),
            "avatarUrl": "https://example.com/avatar",
        }
        return "fake-login", viewer

    def _fake_iter_user_prs_windowed(token, login, since_dt, until_dt, max_window_days=180):
        _ = token
        _ = login
        _ = max_window_days
        yield {
            "databaseId": 200,
            "number": 1,
            "url": "https://example.com/pr/200",
            "state": "MERGED",
            "isDraft": False,
            "createdAt": (since_dt + timedelta(days=1)).isoformat(),
            "mergedAt": until_dt.isoformat(),
            "closedAt": until_dt.isoformat(),
            "additions": 5,
            "deletions": 3,
            "changedFiles": 1,
            "merged": True,
            "updatedAt": until_dt.isoformat(),
            "comments": {"totalCount": 1},
            "reviews": {"totalCount": 0},
            "repository": {
                "databaseId": 300,
                "name": "repo",
                "isPrivate": False,
                "owner": {"login": "owner"},
            },
        }

    def _fake_fetch_repo_languages(token, owner, name):
        _ = token
        _ = owner
        _ = name
        return [{"language": "Python", "bytes": 1000}]

    def _fake_fetch_oss_activity_counts_3y(token, login, window_end):
        _ = token
        _ = login
        _ = window_end
        return {"oss_reviews": 2, "oss_issues_opened": 1}

    monkeypatch.setattr("services.shared.pipeline.fetch_user_login", _fake_fetch_user_login)
    monkeypatch.setattr("services.shared.pipeline.iter_user_prs_windowed", _fake_iter_user_prs_windowed)
    monkeypatch.setattr("services.shared.pipeline.fetch_repo_languages", _fake_fetch_repo_languages)
    monkeypatch.setattr("services.shared.pipeline.fetch_oss_activity_counts_3y", _fake_fetch_oss_activity_counts_3y)
    monkeypatch.setattr("services.shared.pipeline.acquire_user_lock", lambda *_args, **_kwargs: "lock")
    monkeypatch.setattr("services.shared.pipeline.release_user_lock", lambda *_args, **_kwargs: None)

    result = ingest_and_compute_user(
        user_id=None,
        github_token="dummy-token",
        backfill_days=30,
        partition_key="integration-ingest-compute",
        triggered_by="test",
    )

    assert result["status"] == "ok"
    internal_user_id = result["user_id"]
    assert internal_user_id is not None
    assert result["window_start"] <= result["window_end"]

    with db_session() as session:
        state_row = session.execute(
            text("SELECT last_pr_updated_at FROM github_sync_state WHERE user_id=:u"),
            {"u": internal_user_id},
        ).fetchone()
        assert state_row is not None
        assert state_row[0] == result["window_end"]

        metrics_row = session.execute(
            text(
                "SELECT total_opened_prs, total_merged_prs, oss_reviews, oss_issues_opened "
                "FROM contributor_metrics WHERE user_id=:u"
            ),
            {"u": internal_user_id},
        ).fetchone()
        assert metrics_row is not None
        assert int(metrics_row[0]) == 1
        assert int(metrics_row[1]) == 1
        assert int(metrics_row[2]) == 2
        assert int(metrics_row[3]) == 1

        lang_row = session.execute(
            text(
                "SELECT language, pct FROM contributor_language_profile "
                "WHERE user_id=:u ORDER BY pct DESC"
            ),
            {"u": internal_user_id},
        ).fetchone()
        assert lang_row is not None
        assert lang_row[0] == "Python"
        assert float(lang_row[1]) == 100.0


def test_ingest_user_github_activity_records_failed_run_metadata(monkeypatch):
    apply_pending_migrations()
    _clear_tables()

    def _fake_fetch_user_login(token):
        _ = token
        viewer = {
            "databaseId": 999,
            "createdAt": datetime(2020, 1, 1, tzinfo=timezone.utc),
            "avatarUrl": "https://example.com/avatar",
        }
        return "ingest-fail-user", viewer

    def _fake_iter_user_prs_windowed(_token, _login, _since_dt, _until_dt, max_window_days=180):
        _ = max_window_days
        raise RuntimeError("boom")

    monkeypatch.setattr("services.shared.pipeline.fetch_user_login", _fake_fetch_user_login)
    monkeypatch.setattr("services.shared.pipeline.iter_user_prs_windowed", _fake_iter_user_prs_windowed)

    try:
        ingest_user_github_activity(
            user_id=None,
            github_token="dummy-token",
            since=None,
            until=None,
            backfill_days=30,
            partition_key="ingest-failure",
            triggered_by="test",
        )
        assert False, "expected ingest to raise"
    except RuntimeError:
        pass

    with db_session() as session:
        run_row = session.execute(
            text(
                "SELECT status FROM github_sync_runs "
                "WHERE partition_key = :pk "
                "ORDER BY created_at DESC LIMIT 1"
            ),
            {"pk": "ingest-failure"},
        ).fetchone()
        assert run_row is not None
        assert run_row[0] == "FAILED"


def test_compute_gates_all_percentiles_when_pr_inactive(monkeypatch):
    apply_pending_migrations()
    _clear_tables()
    _seed_baseline()

    user_id = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
    _insert_user_and_repo(user_id, github_login="login")

    def _fake_fetch_oss_activity_counts_3y(_token, _login, _window_end):
        return {"oss_reviews": 100, "oss_issues_opened": 100}

    monkeypatch.setattr("services.shared.pipeline.fetch_oss_activity_counts_3y", _fake_fetch_oss_activity_counts_3y)

    compute_user_metrics_and_language_profile(
        user_id=user_id,
        github_token="dummy-token",
        partition_key="compute-gating",
        triggered_by="test",
    )

    with db_session() as session:
        row = session.execute(
            text(
                "SELECT "
                "total_opened_prs_percentile, total_merged_prs_percentile, pr_merge_rate_percentile, "
                "pr_drop_rate_percentile, avg_merge_latency_hours_percentile, "
                "oss_prs_opened_percentile, oss_reviews_percentile, oss_issues_opened_percentile, "
                "oss_composite_percentile "
                "FROM contributor_metrics WHERE user_id = :u"
            ),
            {"u": user_id},
        ).fetchone()
        assert row is not None
        assert all(value is None for value in row)


def test_compute_gates_rate_and_latency_percentiles_under_20_prs(monkeypatch):
    apply_pending_migrations()
    _clear_tables()
    _seed_baseline()

    user_id = "cccccccc-cccc-cccc-cccc-cccccccccccc"
    _insert_user_and_repo(user_id, github_login="login")
    _insert_eligible_prs(user_id, 10, state="MERGED")

    def _fake_fetch_oss_activity_counts_3y(_token, _login, _window_end):
        return {"oss_reviews": 0, "oss_issues_opened": 0}

    monkeypatch.setattr("services.shared.pipeline.fetch_oss_activity_counts_3y", _fake_fetch_oss_activity_counts_3y)

    compute_user_metrics_and_language_profile(
        user_id=user_id,
        github_token="dummy-token",
        partition_key="compute-gating",
        triggered_by="test",
    )

    with db_session() as session:
        row = session.execute(
            text(
                "SELECT total_opened_prs_percentile, total_merged_prs_percentile, "
                "pr_merge_rate_percentile, pr_drop_rate_percentile, avg_merge_latency_hours_percentile "
                "FROM contributor_metrics WHERE user_id = :u"
            ),
            {"u": user_id},
        ).fetchone()
        assert row is not None
        assert row[0] is not None
        assert row[1] is not None
        assert row[2] is None
        assert row[3] is None
        assert row[4] is None


def test_compute_gates_oss_percentiles_under_10_total_activity(monkeypatch):
    apply_pending_migrations()
    _clear_tables()
    _seed_baseline()

    user_id = "dddddddd-dddd-dddd-dddd-dddddddddddd"
    _insert_user_and_repo(user_id, github_login="login")
    _insert_eligible_prs(user_id, 5, state="MERGED")

    def _fake_fetch_oss_activity_counts_3y(_token, _login, _window_end):
        return {"oss_reviews": 2, "oss_issues_opened": 2}

    monkeypatch.setattr("services.shared.pipeline.fetch_oss_activity_counts_3y", _fake_fetch_oss_activity_counts_3y)

    compute_user_metrics_and_language_profile(
        user_id=user_id,
        github_token="dummy-token",
        partition_key="compute-gating",
        triggered_by="test",
    )

    with db_session() as session:
        row = session.execute(
            text(
                "SELECT oss_prs_opened_percentile, oss_reviews_percentile, oss_issues_opened_percentile, "
                "oss_composite_percentile "
                "FROM contributor_metrics WHERE user_id = :u"
            ),
            {"u": user_id},
        ).fetchone()
        assert row is not None
        assert all(value is None for value in row)
