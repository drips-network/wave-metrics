from contextlib import contextmanager

import httpx
import pytest

from services.shared import throttle


class FakeRedis:
    def __init__(self, time_module):
        self._time = time_module
        self._values = {}
        self._expire_at = {}

    def _is_expired(self, key):
        expire_at = self._expire_at.get(key)
        if expire_at is None:
            return False
        return self._time.time() >= expire_at

    def _ensure_not_expired(self, key):
        if self._is_expired(key):
            self._values.pop(key, None)
            self._expire_at.pop(key, None)

    def exists(self, key):
        self._ensure_not_expired(key)
        return 1 if key in self._values else 0

    def ttl(self, key):
        self._ensure_not_expired(key)
        if key not in self._values:
            return -2
        expire_at = self._expire_at.get(key)
        if expire_at is None:
            return -1
        return max(0, int(expire_at - self._time.time()))

    def expire(self, key, seconds):
        self._ensure_not_expired(key)
        if key not in self._values:
            return 0
        self._expire_at[key] = self._time.time() + int(seconds)
        return 1

    def setex(self, key, seconds, value):
        self._values[key] = value
        self._expire_at[key] = self._time.time() + int(seconds)
        return True

    def incr(self, key):
        self._ensure_not_expired(key)
        value = int(self._values.get(key, 0)) + 1
        self._values[key] = value
        return value

    def decr(self, key):
        self._ensure_not_expired(key)
        value = int(self._values.get(key, 0)) - 1
        self._values[key] = value
        return value

    def delete(self, key):
        self._values.pop(key, None)
        self._expire_at.pop(key, None)
        return 1

    def hgetall(self, key):
        self._ensure_not_expired(key)
        value = self._values.get(key)
        return value if isinstance(value, dict) else {}

    def hset(self, key, mapping):
        self._ensure_not_expired(key)
        value = self._values.get(key)
        if not isinstance(value, dict):
            value = {}
        value.update(mapping)
        self._values[key] = value
        return True

    def eval(self, script, numkeys, *args):
        _ = numkeys

        if script == throttle._ACQUIRE_SEMAPHORE_LUA:
            sem_key = args[0]
            limit = int(args[1])
            ttl_seconds = int(args[2])

            value = int(self.incr(sem_key))
            self.expire(sem_key, ttl_seconds)

            if value <= limit:
                return value

            self.decr(sem_key)
            return 0

        if script == throttle._RELEASE_SEMAPHORE_LUA:
            sem_key = args[0]
            value = int(self.decr(sem_key))
            if value <= 0:
                self.delete(sem_key)
            return value

        raise NotImplementedError("FakeRedis.eval does not support this script")


@pytest.fixture()
def redis_client():
    return FakeRedis(throttle.time)


def test_token_hash_length_expected(redis_client, monkeypatch):
    monkeypatch.setattr(throttle, "get_redis", lambda: redis_client)

    token_hash = throttle._token_hash("ghp_example")
    assert len(token_hash) == 32


def test_acquire_token_semaphore_times_out_expected(redis_client, monkeypatch):
    monkeypatch.setattr(throttle, "get_redis", lambda: redis_client)

    current_time = {"t": 0.0}

    def fake_time():
        current_time["t"] += 0.6
        return current_time["t"]

    monkeypatch.setattr(throttle.time, "time", fake_time)
    monkeypatch.setattr(throttle.time, "sleep", lambda _seconds: None)

    with pytest.raises(TimeoutError):
        throttle._acquire_token_semaphore(
            redis_client,
            token_hash="abc123",
            limit=0,
            timeout_seconds=1,
        )


def test_send_github_graphql_releases_semaphore_on_no_response_expected(redis_client, monkeypatch):
    monkeypatch.setattr(throttle, "get_redis", lambda: redis_client)

    @contextmanager
    def fake_http_client(timeout=throttle.DEFAULT_GITHUB_TIMEOUT):
        class _Client:
            def post(self, *_args, **_kwargs):
                raise RuntimeError("network boom")

        yield _Client()

    monkeypatch.setattr(throttle, "_http_client", fake_http_client)

    token_hash = throttle._token_hash("ghp_example")
    sem_key = throttle._sem_key(token_hash)

    with pytest.raises(RuntimeError):
        throttle.send_github_graphql("ghp_example", {"query": "{ viewer { login } }"})

    assert redis_client.exists(sem_key) == 0


def test_parse_retry_after_http_date_expected(monkeypatch):
    original_datetime = throttle.datetime
    fixed_now = original_datetime(2025, 1, 1, 0, 0, 0, tzinfo=throttle.timezone.utc)

    class _FakeDatetime(original_datetime):
        @classmethod
        def now(cls, tz=None):
            _ = tz
            return fixed_now

    monkeypatch.setattr(throttle, "datetime", _FakeDatetime)

    seconds = throttle._parse_retry_after("Wed, 01 Jan 2025 00:00:10 GMT")
    assert seconds == 10


def test_wait_for_secondary_cooldown_repairs_missing_ttl_expected(monkeypatch):
    calls = {"expire": []}

    class _Redis:
        def __init__(self):
            self._exists_calls = 0

        def exists(self, _key):
            self._exists_calls += 1
            return 1 if self._exists_calls == 1 else 0

        def ttl(self, _key):
            return -1

        def expire(self, _key, seconds):
            calls["expire"].append(int(seconds))
            return 1

    monkeypatch.setattr(throttle.time, "sleep", lambda _seconds: None)

    throttle._wait_for_secondary_cooldown(_Redis(), "abc123")
    assert calls["expire"] == [5]


def test_wait_for_secondary_cooldown_caps_large_ttl_expected(monkeypatch):
    calls = {"expire": []}

    class _Redis:
        def __init__(self):
            self._exists_calls = 0

        def exists(self, _key):
            self._exists_calls += 1
            return 1 if self._exists_calls == 1 else 0

        def ttl(self, _key):
            return throttle.GH_COOLDOWN_MAX_WAIT_SECONDS + 123

        def expire(self, _key, seconds):
            calls["expire"].append(int(seconds))
            return 1

    monkeypatch.setattr(throttle.time, "sleep", lambda _seconds: None)

    throttle._wait_for_secondary_cooldown(_Redis(), "abc123")
    assert calls["expire"] == [int(throttle.GH_COOLDOWN_MAX_WAIT_SECONDS)]


def test_begin_request_waits_for_budget_reset_expected(monkeypatch):
    sleep_calls = []

    class _Redis:
        def hgetall(self, _key):
            return {
                b"remaining": b"0",
                b"reset_epoch": b"1010",
            }

    monkeypatch.setattr(throttle, "get_redis", lambda: _Redis())
    monkeypatch.setattr(throttle, "_wait_for_secondary_cooldown", lambda _redis, _token_hash: None)
    monkeypatch.setattr(throttle, "_acquire_token_semaphore", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(throttle.time, "time", lambda: 1000.0)
    monkeypatch.setattr(throttle.time, "sleep", lambda seconds: sleep_calls.append(seconds))

    _token_hash, _redis = throttle.begin_request("ghp_example")

    assert sleep_calls == [11]


def test_send_github_graphql_retries_on_retry_after_expected(monkeypatch):
    req = httpx.Request("POST", throttle.GITHUB_GRAPHQL_URL)

    responses = [
        httpx.Response(429, headers={"Retry-After": "1"}, request=req),
        httpx.Response(200, request=req),
    ]
    sleep_calls = []

    @contextmanager
    def fake_http_client(timeout=throttle.DEFAULT_GITHUB_TIMEOUT):
        _ = timeout

        class _Client:
            def post(self, *_args, **_kwargs):
                return responses.pop(0)

        yield _Client()

    monkeypatch.setattr(throttle, "begin_request", lambda *_args, **_kwargs: ("abc123", None))
    monkeypatch.setattr(throttle, "end_request", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(throttle, "_http_client", fake_http_client)
    monkeypatch.setattr(throttle.time, "sleep", lambda seconds: sleep_calls.append(seconds))

    _ = throttle.send_github_graphql("ghp_example", {"query": "query{viewer{login}}"})
    assert sleep_calls == [1]


def test_send_github_graphql_unauthorized_raises_expected(monkeypatch):
    req = httpx.Request("POST", throttle.GITHUB_GRAPHQL_URL)

    @contextmanager
    def fake_http_client(timeout=throttle.DEFAULT_GITHUB_TIMEOUT):
        _ = timeout

        class _Client:
            def post(self, *_args, **_kwargs):
                return httpx.Response(401, request=req)

        yield _Client()

    monkeypatch.setattr(throttle, "begin_request", lambda *_args, **_kwargs: ("abc123", None))
    monkeypatch.setattr(throttle, "end_request", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(throttle, "_http_client", fake_http_client)

    with pytest.raises(PermissionError):
        throttle.send_github_graphql("ghp_example", {"query": "query{viewer{login}}"})


def test_send_github_graphql_backoff_without_retry_after_expected(monkeypatch):
    req = httpx.Request("POST", throttle.GITHUB_GRAPHQL_URL)

    responses = [
        httpx.Response(403, request=req),
        httpx.Response(200, request=req),
    ]
    sleep_calls = []

    @contextmanager
    def fake_http_client(timeout=throttle.DEFAULT_GITHUB_TIMEOUT):
        _ = timeout

        class _Client:
            def post(self, *_args, **_kwargs):
                return responses.pop(0)

        yield _Client()

    monkeypatch.setattr(throttle, "begin_request", lambda *_args, **_kwargs: ("abc123", None))
    monkeypatch.setattr(throttle, "end_request", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(throttle, "_http_client", fake_http_client)
    monkeypatch.setattr(throttle.random, "random", lambda: 0.0)
    monkeypatch.setattr(throttle.time, "sleep", lambda seconds: sleep_calls.append(seconds))

    _ = throttle.send_github_graphql("ghp_example", {"query": "query{viewer{login}}"})

    assert sleep_calls == [throttle.GH_BACKOFF_BASE_SECONDS]
