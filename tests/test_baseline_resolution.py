import pytest

from services.shared.percentiles import resolve_baseline


class _FakeSession:
    def __init__(self, rows_by_key):
        self._rows_by_key = rows_by_key

    def execute(self, statement, params=None):
        sql = str(statement)
        params = params or {}

        key = None
        if "FROM baseline_metadata WHERE baseline_id" in sql:
            key = ("baseline_by_id", params.get("bid"))
        elif "FROM population_cdfs WHERE baseline_id" in sql:
            key = ("cdfs_by_id", params.get("bid"))
        elif "FROM baseline_metadata" in sql:
            key = ("baseline_latest", None)
        elif "FROM population_cdfs" in sql:
            key = ("cdfs_latest", None)

        row = self._rows_by_key.get(key)

        class _Result:
            def __init__(self, row_value):
                self._row_value = row_value

            def fetchone(self):
                return self._row_value

        return _Result(row)


def test_resolve_baseline_prefers_env_var_when_present(monkeypatch):
    monkeypatch.setattr("services.shared.percentiles.POPULATION_BASELINE_ID", "baseline-1")
    session = _FakeSession(
        {
            ("baseline_by_id", "baseline-1"): ("baseline-1", "2022-01-01", "2023-01-01"),
        }
    )
    resolved = resolve_baseline(session)
    assert resolved["baseline_id"] == "baseline-1"
    assert resolved["baseline_start_date"] == "2022-01-01"
    assert resolved["baseline_end_date"] == "2023-01-01"


def test_resolve_baseline_env_var_falls_back_to_population_cdfs(monkeypatch):
    monkeypatch.setattr("services.shared.percentiles.POPULATION_BASELINE_ID", "baseline-2")
    session = _FakeSession(
        {
            ("baseline_by_id", "baseline-2"): None,
            ("cdfs_by_id", "baseline-2"): ("baseline-2", "2022-02-01", "2023-02-01"),
        }
    )
    resolved = resolve_baseline(session)
    assert resolved["baseline_id"] == "baseline-2"
    assert resolved["baseline_start_date"] == "2022-02-01"
    assert resolved["baseline_end_date"] == "2023-02-01"


def test_resolve_baseline_env_var_missing_raises(monkeypatch):
    monkeypatch.setattr("services.shared.percentiles.POPULATION_BASELINE_ID", "missing")
    session = _FakeSession(
        {
            ("baseline_by_id", "missing"): None,
            ("cdfs_by_id", "missing"): None,
        }
    )
    with pytest.raises(ValueError):
        resolve_baseline(session)


def test_resolve_baseline_uses_latest_baseline_metadata(monkeypatch):
    monkeypatch.setattr("services.shared.percentiles.POPULATION_BASELINE_ID", "")
    session = _FakeSession(
        {
            ("baseline_latest", None): ("baseline-latest", "2022-03-01", "2023-03-01"),
        }
    )
    resolved = resolve_baseline(session)
    assert resolved["baseline_id"] == "baseline-latest"


def test_resolve_baseline_falls_back_to_population_cdfs_when_metadata_missing(monkeypatch):
    monkeypatch.setattr("services.shared.percentiles.POPULATION_BASELINE_ID", "")
    session = _FakeSession(
        {
            ("baseline_latest", None): None,
            ("cdfs_latest", None): ("baseline-cdfs", "2022-04-01", "2023-04-01"),
        }
    )
    resolved = resolve_baseline(session)
    assert resolved["baseline_id"] == "baseline-cdfs"

