from datetime import datetime, timezone

from services.shared.pipeline import user_needs_recompute


class _FakeSession:
    def __init__(self, row):
        self._row = row

    def execute(self, _statement, _params=None):
        class _Result:
            def __init__(self, row):
                self._row = row

            def fetchone(self):
                return self._row

        return _Result(self._row)


def test_user_needs_recompute_returns_true_when_user_row_missing():
    session = _FakeSession(None)
    assert user_needs_recompute(session, "user-id") is True


def test_user_needs_recompute_returns_true_when_metrics_missing():
    session = _FakeSession((None, datetime(2025, 1, 1, tzinfo=timezone.utc)))
    assert user_needs_recompute(session, "user-id") is True


def test_user_needs_recompute_returns_false_when_no_sync_state():
    session = _FakeSession((datetime(2025, 1, 2, tzinfo=timezone.utc), None))
    assert user_needs_recompute(session, "user-id") is False


def test_user_needs_recompute_returns_true_when_sync_newer_than_metrics():
    computed_at = datetime(2025, 1, 1, tzinfo=timezone.utc)
    last_updated_at = datetime(2025, 1, 2, tzinfo=timezone.utc)
    session = _FakeSession((computed_at, last_updated_at))
    assert user_needs_recompute(session, "user-id") is True


def test_user_needs_recompute_returns_false_when_metrics_newer_than_sync():
    computed_at = datetime(2025, 1, 2, tzinfo=timezone.utc)
    last_updated_at = datetime(2025, 1, 1, tzinfo=timezone.utc)
    session = _FakeSession((computed_at, last_updated_at))
    assert user_needs_recompute(session, "user-id") is False

