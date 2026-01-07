import json
import uuid

import pytest

from services.shared import token_store


class FakeRedis:
    def __init__(self):
        self._values = {}
        self._ttls = {}

    def setex(self, key, _ttl_seconds, value):
        self._values[key] = value
        self._ttls[key] = int(_ttl_seconds)
        return True

    def set(self, key, value, nx=False, ex=None):
        if nx and key in self._values:
            return False
        self._values[key] = value
        if ex is not None:
            self._ttls[key] = int(ex)
        return True

    def delete(self, key):
        self._values.pop(key, None)
        self._ttls.pop(key, None)
        return 1

    def get(self, key):
        return self._values.get(key)

    def ttl(self, key):
        if key not in self._values:
            return -2
        return self._ttls.get(key, -1)

    def expire(self, key, seconds):
        if key not in self._values:
            return 0
        self._ttls[key] = int(seconds)
        return 1

    def eval(self, _script, _numkeys, key):
        value = self._values.get(key)
        if value is None:
            return None
        self._values.pop(key, None)
        self._ttls.pop(key, None)
        return value


@pytest.fixture()
def redis_client():
    return FakeRedis()


def test_create_and_consume_github_token_ref_happy_path_expected(redis_client, monkeypatch):
    monkeypatch.setattr(token_store, "get_redis", lambda: redis_client)

    token_ref = token_store.create_github_token_ref(
        None,
        user_id="user-123",
        github_token="token-abc",
        ttl_seconds=60,
    )

    token = token_store.consume_github_token_ref(None, token_ref, expected_user_id="user-123")
    assert token == "token-abc"

    with pytest.raises(ValueError):
        token_store.consume_github_token_ref(None, token_ref, expected_user_id="user-123")


def test_create_github_token_ref_stores_encrypted_payload_expected(redis_client, monkeypatch):
    monkeypatch.setattr(token_store, "get_redis", lambda: redis_client)

    token_ref = token_store.create_github_token_ref(
        None,
        user_id="user-123",
        github_token="token-abc",
        ttl_seconds=60,
    )

    raw_payload = redis_client.get(token_store._token_ref_key(token_ref))
    payload = json.loads(raw_payload)

    assert payload.get("github_token") != "token-abc"
    assert (payload.get("enc") or {}).get("alg") == token_store.TOKEN_REF_ENCRYPTION_ALG


def test_decode_token_ref_payload_rejects_missing_enc_expected(redis_client, monkeypatch):
    monkeypatch.setattr(token_store, "get_redis", lambda: redis_client)

    token_ref = "missing-enc"
    redis_client._values[token_store._token_ref_key(token_ref)] = json.dumps(
        {"user_id": "user-123", "github_token": "plaintext-token"}
    )

    with pytest.raises(ValueError) as excinfo:
        token_store.consume_github_token_ref(None, token_ref, expected_user_id="user-123")

    assert str(excinfo.value) == "token_ref payload could not be decoded"


def test_token_store_user_id_normalizes_uuid_expected(redis_client, monkeypatch):
    monkeypatch.setattr(token_store, "get_redis", lambda: redis_client)

    user_id = uuid.uuid4()
    token_ref = token_store.create_github_token_ref(
        None,
        user_id=user_id,
        github_token="token-abc",
        ttl_seconds=60,
    )

    token = token_store.consume_github_token_ref(None, token_ref, expected_user_id=user_id)
    assert token == "token-abc"


def test_decode_token_ref_payload_rejects_non_object_expected(redis_client, monkeypatch):
    monkeypatch.setattr(token_store, "get_redis", lambda: redis_client)

    token_ref = "non-object"
    redis_client._values[token_store._token_ref_key(token_ref)] = json.dumps([])

    with pytest.raises(ValueError) as excinfo:
        token_store.consume_github_token_ref(None, token_ref)

    assert str(excinfo.value) == "token_ref payload could not be decoded"


def test_decode_token_ref_payload_rejects_non_object_enc_expected(redis_client, monkeypatch):
    monkeypatch.setattr(token_store, "get_redis", lambda: redis_client)

    token_ref = "bad-enc"
    redis_client._values[token_store._token_ref_key(token_ref)] = json.dumps(
        {"user_id": "user-123", "github_token": "ciphertext", "enc": "nope"}
    )

    with pytest.raises(ValueError) as excinfo:
        token_store.consume_github_token_ref(None, token_ref)

    assert str(excinfo.value) == "token_ref payload could not be decoded"


def test_lease_extends_token_ttl_to_lease_ttl_expected(redis_client, monkeypatch):
    monkeypatch.setattr(token_store, "get_redis", lambda: redis_client)

    token_ref = token_store.create_github_token_ref(
        None,
        user_id="user-123",
        github_token="token-lease",
        ttl_seconds=5,
    )

    token_key = token_store._token_ref_key(token_ref)
    assert redis_client.ttl(token_key) == 5

    token = token_store.lease_github_token_ref(
        None,
        token_ref,
        expected_user_id="user-123",
        lease_id="task-1",
        lease_ttl_seconds=60,
    )
    assert token == "token-lease"
    assert redis_client.ttl(token_key) == 60


def test_consume_github_token_ref_not_found_raises_expected(redis_client, monkeypatch):
    monkeypatch.setattr(token_store, "get_redis", lambda: redis_client)

    with pytest.raises(ValueError):
        token_store.consume_github_token_ref(None, "missing")


def test_consume_github_token_ref_expired_raises_expected(redis_client, monkeypatch):
    monkeypatch.setattr(token_store, "get_redis", lambda: redis_client)

    token_ref = token_store.create_github_token_ref(None, user_id="user-123", github_token="expired", ttl_seconds=-1)

    with pytest.raises(ValueError):
        token_store.consume_github_token_ref(None, token_ref, expected_user_id="user-123")


def test_delete_github_token_ref_is_idempotent_expected(redis_client, monkeypatch):
    monkeypatch.setattr(token_store, "get_redis", lambda: redis_client)

    token_ref = token_store.create_github_token_ref(None, user_id="user-123", github_token="token-del", ttl_seconds=60)

    token_store.delete_github_token_ref(None, token_ref)
    token_store.delete_github_token_ref(None, token_ref)

    with pytest.raises(ValueError):
        token_store.consume_github_token_ref(None, token_ref)


def test_consume_github_token_ref_user_binding_mismatch_raises_expected(redis_client, monkeypatch):
    monkeypatch.setattr(token_store, "get_redis", lambda: redis_client)

    token_ref = token_store.create_github_token_ref(None, user_id="user-aaa", github_token="token-abc", ttl_seconds=60)

    with pytest.raises(ValueError):
        token_store.consume_github_token_ref(None, token_ref, expected_user_id="user-bbb")


def test_consume_github_token_ref_payload_decode_error_raises_expected(redis_client, monkeypatch):
    monkeypatch.setattr(token_store, "get_redis", lambda: redis_client)

    token_ref = "bad-json"
    redis_client._values[token_store._token_ref_key(token_ref)] = "not-json"

    with pytest.raises(ValueError):
        token_store.consume_github_token_ref(None, token_ref)


def test_consume_github_token_ref_payload_missing_token_raises_expected(redis_client, monkeypatch):
    monkeypatch.setattr(token_store, "get_redis", lambda: redis_client)

    token_ref = "missing-token"
    redis_client._values[token_store._token_ref_key(token_ref)] = json.dumps({"user_id": "user-123"})

    with pytest.raises(ValueError):
        token_store.consume_github_token_ref(None, token_ref)


def test_lease_github_token_ref_allows_same_lease_id_replay_expected(redis_client, monkeypatch):
    monkeypatch.setattr(token_store, "get_redis", lambda: redis_client)

    token_ref = token_store.create_github_token_ref(None, user_id="user-123",
                                                    github_token="token-lease", ttl_seconds=60)

    token = token_store.lease_github_token_ref(None, token_ref, expected_user_id="user-123", lease_id="task-1")
    assert token == "token-lease"

    token = token_store.lease_github_token_ref(None, token_ref, expected_user_id="user-123", lease_id="task-1")
    assert token == "token-lease"

    with pytest.raises(ValueError):
        token_store.lease_github_token_ref(None, token_ref, expected_user_id="user-123", lease_id="task-2")


def test_finalize_github_token_ref_deletes_only_on_success_expected(redis_client, monkeypatch):
    monkeypatch.setattr(token_store, "get_redis", lambda: redis_client)

    token_ref = token_store.create_github_token_ref(None, user_id="user-123",
                                                    github_token="token-lease", ttl_seconds=60)

    token = token_store.lease_github_token_ref(None, token_ref, expected_user_id="user-123", lease_id="task-1")
    assert token == "token-lease"

    token_store.finalize_github_token_ref(None, token_ref, lease_id="task-1", success=False)

    token = token_store.lease_github_token_ref(None, token_ref, expected_user_id="user-123", lease_id="task-1")
    assert token == "token-lease"

    token_store.finalize_github_token_ref(None, token_ref, lease_id="task-1", success=True)

    with pytest.raises(ValueError):
        token_store.lease_github_token_ref(None, token_ref, expected_user_id="user-123", lease_id="task-1")


def test_lease_github_token_ref_race_retries_when_lease_key_missing_expected(redis_client, monkeypatch):
    monkeypatch.setattr(token_store, "get_redis", lambda: redis_client)

    token_ref = token_store.create_github_token_ref(None, user_id="user-123",
                                                    github_token="token-lease", ttl_seconds=60)
    lease_key = token_store._token_ref_lease_key(token_ref)

    set_calls = {"count": 0}
    real_set = redis_client.set

    def _flaky_set(key, value, nx=False, ex=None):
        set_calls["count"] += 1

        if key == lease_key and nx and set_calls["count"] == 1:
            redis_client.delete(lease_key)
            return False

        return real_set(key, value, nx=nx, ex=ex)

    monkeypatch.setattr(redis_client, "set", _flaky_set)

    token = token_store.lease_github_token_ref(None, token_ref, expected_user_id="user-123", lease_id="task-1")
    assert token == "token-lease"


def test_lease_github_token_ref_deletes_lease_when_payload_missing_expected(redis_client, monkeypatch):
    monkeypatch.setattr(token_store, "get_redis", lambda: redis_client)

    token_ref = token_store.create_github_token_ref(None, user_id="user-123",
                                                    github_token="token-lease", ttl_seconds=60)

    lease_key = token_store._token_ref_lease_key(token_ref)
    redis_client.setex(lease_key, 60, "task-1")

    redis_client.delete(token_store._token_ref_key(token_ref))

    with pytest.raises(ValueError):
        token_store.lease_github_token_ref(None, token_ref, expected_user_id="user-123", lease_id="task-2")

    assert redis_client.get(lease_key) is None


def test_cleanup_expired_token_refs_noop_expected(redis_client, monkeypatch):
    monkeypatch.setattr(token_store, "get_redis", lambda: redis_client)

    _ = token_store.create_github_token_ref(None, user_id="user-123", github_token="expired", ttl_seconds=-1)

    assert token_store.cleanup_expired_token_refs(None) == 0
