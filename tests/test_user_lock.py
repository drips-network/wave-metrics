import pytest

from services.shared import locks as locks_module


class _FakeRedis:
    def __init__(self):
        self._values = {}

    def set(self, key, value, nx=False, ex=None):
        _ = ex
        if nx and key in self._values:
            return False
        self._values[key] = value
        return True

    def eval(self, _script, _numkeys, key, expected_value):
        if self._values.get(key) == expected_value:
            del self._values[key]
            return 1
        return 0

    def get(self, key):
        return self._values.get(key)


def test_acquire_user_lock_returns_lock_value_expected(monkeypatch):
    fake_redis = _FakeRedis()
    monkeypatch.setattr(locks_module, "get_redis", lambda: fake_redis)

    lock_value = locks_module.acquire_user_lock("user-1", ttl_seconds=10, wait_timeout_seconds=0)
    assert isinstance(lock_value, str)
    assert fake_redis.get(locks_module.user_lock_key("user-1")) == lock_value


def test_acquire_user_lock_timeout_when_already_locked_expected(monkeypatch):
    fake_redis = _FakeRedis()
    monkeypatch.setattr(locks_module, "get_redis", lambda: fake_redis)

    _ = locks_module.acquire_user_lock("user-1", ttl_seconds=10, wait_timeout_seconds=0)

    with pytest.raises(TimeoutError):
        _ = locks_module.acquire_user_lock("user-1", ttl_seconds=10, wait_timeout_seconds=0)


def test_acquire_user_lock_waits_and_succeeds_with_positive_timeout_expected(monkeypatch):
    class _FlakyRedis(_FakeRedis):
        def __init__(self):
            super().__init__()
            self.calls = 0

        def set(self, key, value, nx=False, ex=None):
            self.calls += 1
            if self.calls == 1:
                return False
            return super().set(key, value, nx=nx, ex=ex)

    fake_redis = _FlakyRedis()
    monkeypatch.setattr(locks_module, "get_redis", lambda: fake_redis)
    monkeypatch.setattr(locks_module.time, "sleep", lambda _s: None)
    monkeypatch.setattr(locks_module.time, "monotonic", lambda: 0.0)

    lock_value = locks_module.acquire_user_lock("user-1", ttl_seconds=10, wait_timeout_seconds=5)
    assert isinstance(lock_value, str)
    assert fake_redis.calls >= 2


def test_release_user_lock_only_releases_matching_value_expected(monkeypatch):
    fake_redis = _FakeRedis()
    monkeypatch.setattr(locks_module, "get_redis", lambda: fake_redis)

    lock_value = locks_module.acquire_user_lock("user-1", ttl_seconds=10, wait_timeout_seconds=0)

    locks_module.release_user_lock("user-1", "wrong-value")
    assert fake_redis.get(locks_module.user_lock_key("user-1")) == lock_value

    locks_module.release_user_lock("user-1", lock_value)
    assert fake_redis.get(locks_module.user_lock_key("user-1")) is None
