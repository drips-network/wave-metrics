from datetime import datetime, timedelta, timezone

from sqlalchemy import text

from services.shared.pipeline import _resolve_ingestion_window


class _FakeSession:
    """
    Minimal stub session for _resolve_ingestion_window tests

    Args:
        row_value: Value to return for last_pr_updated_at or None for no row
    """

    def __init__(self, row_value):
        self._row_value = row_value

    def execute(self, statement, params=None):
        """
        Return an object with fetchone() yielding the configured row value
        """
        _ = statement  # unused in tests
        _ = params

        class _Result:
            def __init__(self, value):
                self._value = value

            def fetchone(self):
                if self._value is None:
                    return None
                return (self._value,)

        return _Result(self._row_value)


def _dt(days_offset):
    """
    Helper to produce a UTC datetime offset from an arbitrary anchor
    """
    anchor = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    return anchor + timedelta(days=days_offset)


def test_resolve_ingestion_window_explicit_since_until_uses_arguments_over_watermark(monkeypatch):
    # Prepare a fake session that would otherwise return a watermark
    watermark = _dt(-10)
    session = _FakeSession(watermark)

    # Make now deterministic
    fixed_now = _dt(0)

    def _fake_now(_tz=None):
        _ = _tz
        return fixed_now

    monkeypatch.setattr("services.shared.pipeline.datetime",
                        type("D", (), {"now": staticmethod(_fake_now), "timezone": timezone}))

    since = _dt(-30)
    until = _dt(-5)

    window_start, window_end = _resolve_ingestion_window(session, "user-id", since, until, backfill_days=None)

    assert window_start == since
    assert window_end == until


def test_resolve_ingestion_window_backfill_days_ignores_watermark(monkeypatch):
    watermark = _dt(-10)
    session = _FakeSession(watermark)

    fixed_now = _dt(0)

    def _fake_now(_tz=None):
        _ = _tz
        return fixed_now

    monkeypatch.setattr("services.shared.pipeline.datetime",
                        type("D", (), {"now": staticmethod(_fake_now), "timezone": timezone}))

    backfill_days = 7
    window_start, window_end = _resolve_ingestion_window(session, "user-id",
                                                         since=None, until=None, backfill_days=backfill_days)

    assert window_end == fixed_now
    assert window_start == fixed_now - timedelta(days=backfill_days)


def test_resolve_ingestion_window_incremental_with_watermark(monkeypatch):
    watermark = _dt(-20)
    session = _FakeSession(watermark)

    fixed_now = _dt(0)

    def _fake_now(_tz=None):
        _ = _tz
        return fixed_now

    monkeypatch.setattr("services.shared.pipeline.datetime",
                        type("D", (), {"now": staticmethod(_fake_now), "timezone": timezone}))

    window_start, window_end = _resolve_ingestion_window(session, "user-id",
                                                         since=None, until=None, backfill_days=None)

    assert window_end == fixed_now
    # One-day overlap relative to watermark
    assert window_start == watermark - timedelta(days=1)


def test_resolve_ingestion_window_incremental_without_watermark(monkeypatch):
    session = _FakeSession(None)

    fixed_now = _dt(0)

    def _fake_now(_tz=None):
        _ = _tz
        return fixed_now

    monkeypatch.setattr("services.shared.pipeline.datetime",
                        type("D", (), {"now": staticmethod(_fake_now), "timezone": timezone}))

    window_start, window_end = _resolve_ingestion_window(session, "user-id",
                                                         since=None, until=None, backfill_days=None)

    assert window_end == fixed_now
    # Default lookback ensures a new user gets enough history for 3-year metrics
    assert window_start == fixed_now - timedelta(days=1096)
