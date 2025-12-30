from services.scheduler.app import main as scheduler_main


def test_scheduler_parse_csv_list_trims_expected():
    assert scheduler_main._parse_csv_list("") == []
    assert scheduler_main._parse_csv_list("a") == ["a"]
    assert scheduler_main._parse_csv_list(" a, b , ,c ") == ["a", "b", "c"]


def test_scheduler_run_lock_skips_on_non_postgres_expected(monkeypatch):
    class _Dialect:
        name = "sqlite"

    class _Engine:
        dialect = _Dialect()

    monkeypatch.setattr(scheduler_main, "ENGINE", _Engine())
    with scheduler_main._scheduler_run_lock(123, mode_label="daily") as lock_held:
        assert lock_held is True


def test_scheduler_resolve_backfill_users_case_insensitive_dedupes_expected(monkeypatch):
    first_user_id = "11111111-1111-1111-1111-111111111111"
    second_user_id = "22222222-2222-2222-2222-222222222222"

    class _Result:
        def __init__(self, rows):
            self._rows = rows

        def fetchall(self):
            return self._rows

    class _Session:
        def execute(self, stmt, params):
            sql = str(getattr(stmt, "text", stmt))
            if "LOWER(github_login)" in sql:
                logins = set(params.get("logins") or [])
                rows = []
                if "octocat" in logins:
                    rows.append((first_user_id, "octocat"))
                if "calliope" in logins:
                    rows.append((second_user_id, "calliope"))
                return _Result(rows)

            ids = set(params.get("ids") or [])
            rows = []
            if first_user_id in ids:
                rows.append((first_user_id, "octocat"))
            return _Result(rows)

    def _fake_db_session():
        class _CM:
            def __enter__(self):
                return _Session()

            def __exit__(self, exc_type, exc, tb):
                _ = exc_type
                _ = exc
                _ = tb
                return False

        return _CM()

    monkeypatch.setattr(scheduler_main, "db_session", _fake_db_session)

    users = scheduler_main._resolve_backfill_users(
        user_ids=[first_user_id],
        github_logins=["OCTOCAT", "calliope"],
    )
    assert users == [(first_user_id, "octocat"), (second_user_id, "calliope")]


def test_scheduler_resolve_backfill_users_missing_selectors_raise_expected(monkeypatch):
    resolved_user_id = "11111111-1111-1111-1111-111111111111"

    class _Result:
        def __init__(self, rows):
            self._rows = rows

        def fetchall(self):
            return self._rows

    class _Session:
        def execute(self, stmt, params):
            sql = str(getattr(stmt, "text", stmt))
            if "LOWER(github_login)" in sql:
                logins = set(params.get("logins") or [])
                rows = []
                if "octocat" in logins:
                    rows.append((resolved_user_id, "octocat"))
                return _Result(rows)

            ids = set(params.get("ids") or [])
            rows = []
            if resolved_user_id in ids:
                rows.append((resolved_user_id, "octocat"))
            return _Result(rows)

    def _fake_db_session():
        class _CM:
            def __enter__(self):
                return _Session()

            def __exit__(self, exc_type, exc, tb):
                _ = exc_type
                _ = exc
                _ = tb
                return False

        return _CM()

    monkeypatch.setattr(scheduler_main, "db_session", _fake_db_session)

    missing_user_id = "22222222-2222-2222-2222-222222222222"

    try:
        scheduler_main._resolve_backfill_users(
            user_ids=[resolved_user_id, missing_user_id],
            github_logins=["octocat", "missing-login"],
        )
        assert False, "Expected ValueError"
    except ValueError as exc:
        assert "did not resolve" in str(exc).lower()

    users = scheduler_main._resolve_backfill_users(
        user_ids=[resolved_user_id, missing_user_id],
        github_logins=["octocat", "missing-login"],
        allow_missing=True,
    )
    assert users == [(resolved_user_id, "octocat")]
