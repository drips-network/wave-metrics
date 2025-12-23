import pytest

from services.shared import github_client as github_client_module


class _FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def json(self):
        return self._payload


@pytest.mark.parametrize("code", ["INTERNAL", "RATE_LIMITED", "ABUSE_DETECTED"])
def test_graphql_query_retries_transient_errors_expected(monkeypatch, code):
    calls = {"send": 0, "sleep": []}

    payloads = [
        {
            "errors": [
                {
                    "message": "temporary",
                    "extensions": {"code": code},
                }
            ]
        },
        {"data": {"ok": 1}},
    ]

    def _fake_send_github_graphql(_github_token, _payload, timeout=None):
        _ = timeout
        calls["send"] += 1
        return _FakeResponse(payloads.pop(0))

    monkeypatch.setattr(github_client_module, "GH_MAX_RETRIES", 2)
    monkeypatch.setattr(github_client_module, "GH_BACKOFF_BASE_SECONDS", 0)
    monkeypatch.setattr(github_client_module, "GH_BACKOFF_CAP_SECONDS", 0)
    monkeypatch.setattr(github_client_module, "send_github_graphql", _fake_send_github_graphql)
    monkeypatch.setattr(github_client_module.random, "uniform", lambda _a, _b: 0.0)
    monkeypatch.setattr(github_client_module.time, "sleep", lambda s: calls["sleep"].append(s))

    data = github_client_module.graphql_query("token", "query{ viewer { login } }", {})
    assert data == {"ok": 1}
    assert calls["send"] == 2
    assert len(calls["sleep"]) == 1


def test_graphql_query_does_not_retry_non_transient_errors_expected(monkeypatch):
    calls = {"send": 0, "sleep": 0}

    def _fake_send_github_graphql(_github_token, _payload, timeout=None):
        _ = timeout
        calls["send"] += 1
        return _FakeResponse(
            {
                "errors": [
                    {
                        "message": "bad",
                        "extensions": {"code": "SOME_OTHER"},
                    }
                ]
            }
        )

    monkeypatch.setattr(github_client_module, "GH_MAX_RETRIES", 3)
    monkeypatch.setattr(github_client_module, "send_github_graphql", _fake_send_github_graphql)

    def _sleep(_s):
        calls["sleep"] += 1

    monkeypatch.setattr(github_client_module.time, "sleep", _sleep)

    with pytest.raises(github_client_module.GitHubAPIError):
        _ = github_client_module.graphql_query("token", "query{ viewer { login } }", {})

    assert calls["send"] == 1
    assert calls["sleep"] == 0


def test_graphql_query_raises_after_exhausting_transient_retries_expected(monkeypatch):
    calls = {"send": 0, "sleep": 0}

    def _fake_send_github_graphql(_github_token, _payload, timeout=None):
        _ = timeout
        calls["send"] += 1
        return _FakeResponse(
            {
                "errors": [
                    {
                        "message": "temporary",
                        "extensions": {"code": "INTERNAL"},
                    }
                ]
            }
        )

    monkeypatch.setattr(github_client_module, "GH_MAX_RETRIES", 1)
    monkeypatch.setattr(github_client_module, "GH_BACKOFF_BASE_SECONDS", 0)
    monkeypatch.setattr(github_client_module, "GH_BACKOFF_CAP_SECONDS", 0)
    monkeypatch.setattr(github_client_module, "send_github_graphql", _fake_send_github_graphql)

    def _sleep(_s):
        calls["sleep"] += 1

    monkeypatch.setattr(github_client_module.time, "sleep", _sleep)

    with pytest.raises(github_client_module.GitHubAPIError):
        _ = github_client_module.graphql_query("token", "query{ viewer { login } }", {})

    assert calls["send"] == 2
    assert calls["sleep"] == 1


def test_compute_graphql_error_backoff_seconds_caps_expected(monkeypatch):
    monkeypatch.setattr(github_client_module, "GH_BACKOFF_BASE_SECONDS", 5)
    monkeypatch.setattr(github_client_module, "GH_BACKOFF_CAP_SECONDS", 6)
    monkeypatch.setattr(github_client_module.random, "uniform", lambda _a, _b: 0.0)

    wait = github_client_module._compute_graphql_error_backoff_seconds(attempt=10)
    assert wait == 6.0
