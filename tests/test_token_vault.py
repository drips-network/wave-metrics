import base64
import json
import uuid
from datetime import datetime, timezone

import pytest
from sqlalchemy import text

from services.shared.database import apply_pending_migrations, db_session
from services.shared import token_vault


TEST_KEY_1 = base64.b64encode(b"\x01" * 32).decode("utf-8")
TEST_KEY_2 = base64.b64encode(b"\x02" * 32).decode("utf-8")


@pytest.fixture
def vault_enabled(monkeypatch):
    keyring = {"k1": TEST_KEY_1, "k2": TEST_KEY_2}
    monkeypatch.setattr("services.shared.config.TOKEN_VAULT_KEYS_JSON", json.dumps(keyring))
    monkeypatch.setattr("services.shared.config.TOKEN_VAULT_ACTIVE_KEY_ID", "k1")
    monkeypatch.setattr("services.shared.config.TOKEN_VAULT_ENABLED", True)


def _create_user(session, user_id):
    github_login = f"login-{str(user_id)[:8]}"
    github_user_id = uuid.UUID(str(user_id)).int & ((1 << 63) - 1)

    session.execute(
        text(
            "INSERT INTO users(id, github_login, github_user_id, github_created_at, avatar_url) "
            "VALUES (:id, :login, :github_user_id, NOW(), '')"
        ),
        {"id": str(user_id), "login": github_login, "github_user_id": github_user_id},
    )
    return github_login, github_user_id


def test_upsert_and_get_github_token_roundtrip(vault_enabled):
    _ = vault_enabled
    apply_pending_migrations()

    user_id = str(uuid.uuid4())
    github_token = "gho_test_token"

    with db_session() as session:
        _create_user(session, user_id)
        token_vault.upsert_github_token(session, user_id, github_token, stored_by="test")

    with db_session() as session:
        loaded = token_vault.get_github_token(session, user_id)

    assert loaded == github_token


def test_decrypt_with_wrong_user_id_raises(vault_enabled):
    _ = vault_enabled
    apply_pending_migrations()

    user_id = str(uuid.uuid4())
    other_user_id = str(uuid.uuid4())
    github_token = "gho_test_token"

    with db_session() as session:
        _create_user(session, user_id)
        token_vault.upsert_github_token(session, user_id, github_token, stored_by="test")

        row = session.execute(
            text(
                "SELECT encryption_key_id, nonce, ciphertext "
                "FROM user_tokens WHERE user_id=:u AND provider='github'"
            ),
            {"u": user_id},
        ).fetchone()

    assert row is not None
    stored_key_id, nonce, ciphertext = row

    _active_key_id, keyring = token_vault._load_keyring()
    key_bytes = keyring[str(stored_key_id)]

    with pytest.raises(ValueError):
        _ = token_vault._decrypt_with_key(key_bytes, nonce, ciphertext, other_user_id, "github")


def test_rotate_on_read_updates_key_id(vault_enabled, monkeypatch):
    _ = vault_enabled
    apply_pending_migrations()

    user_id = str(uuid.uuid4())
    github_token = "gho_test_token"

    with db_session() as session:
        _create_user(session, user_id)
        token_vault.upsert_github_token(session, user_id, github_token, stored_by="test")

    monkeypatch.setattr("services.shared.config.TOKEN_VAULT_ACTIVE_KEY_ID", "k2")

    with db_session() as session:
        loaded = token_vault.get_github_token(session, user_id, rotate_to_active=True)
        row = session.execute(
            text(
                "SELECT encryption_key_id "
                "FROM user_tokens WHERE user_id=:u AND provider='github'"
            ),
            {"u": user_id},
        ).fetchone()

    assert loaded == github_token
    assert row is not None
    assert str(row[0]) == "k2"


def test_reencrypt_tokens_to_active_key_updates_rows(vault_enabled, monkeypatch):
    _ = vault_enabled
    apply_pending_migrations()

    user_id = str(uuid.uuid4())
    github_token = "gho_test_token"

    with db_session() as session:
        _create_user(session, user_id)
        token_vault.upsert_github_token(session, user_id, github_token, stored_by="test")

    monkeypatch.setattr("services.shared.config.TOKEN_VAULT_ACTIVE_KEY_ID", "k2")

    with db_session() as session:
        updated = token_vault.reencrypt_tokens_to_active_key(session, provider="github", batch_size=10)
        row = session.execute(
            text(
                "SELECT encryption_key_id "
                "FROM user_tokens WHERE user_id=:u AND provider='github'"
            ),
            {"u": user_id},
        ).fetchone()

        loaded = token_vault.get_github_token(session, user_id, rotate_to_active=False)

    assert updated >= 1
    assert row is not None
    assert str(row[0]) == "k2"
    assert loaded == github_token


def test_upsert_skips_reencrypt_when_fingerprint_matches(vault_enabled, monkeypatch):
    _ = vault_enabled
    apply_pending_migrations()

    user_id = str(uuid.uuid4())
    github_token = "gho_test_token"

    with db_session() as session:
        _create_user(session, user_id)
        token_vault.upsert_github_token(session, user_id, github_token, stored_by="test")

        original = session.execute(
            text(
                "SELECT encryption_key_id, nonce, ciphertext "
                "FROM user_tokens WHERE user_id=:u AND provider='github'"
            ),
            {"u": user_id},
        ).fetchone()

    assert original is not None
    original_key_id, original_nonce, original_ciphertext = original

    monkeypatch.setattr("services.shared.config.TOKEN_VAULT_ACTIVE_KEY_ID", "k2")

    with db_session() as session:
        token_vault.upsert_github_token(session, user_id, github_token, stored_by="test")
        updated_row = session.execute(
            text(
                "SELECT encryption_key_id, nonce, ciphertext "
                "FROM user_tokens WHERE user_id=:u AND provider='github'"
            ),
            {"u": user_id},
        ).fetchone()

    assert updated_row is not None
    updated_key_id, updated_nonce, updated_ciphertext = updated_row

    assert str(original_key_id) == str(updated_key_id)
    assert bytes(original_nonce) == bytes(updated_nonce)
    assert bytes(original_ciphertext) == bytes(updated_ciphertext)


def test_get_token_with_missing_key_raises(vault_enabled, monkeypatch):
    _ = vault_enabled
    apply_pending_migrations()

    user_id = str(uuid.uuid4())
    github_token = "gho_test_token"

    with db_session() as session:
        _create_user(session, user_id)
        token_vault.upsert_github_token(session, user_id, github_token, stored_by="test")

    keyring = {"k2": TEST_KEY_2}
    monkeypatch.setattr("services.shared.config.TOKEN_VAULT_KEYS_JSON", json.dumps(keyring))
    monkeypatch.setattr("services.shared.config.TOKEN_VAULT_ACTIVE_KEY_ID", "k2")
    monkeypatch.setattr("services.shared.config.TOKEN_VAULT_ENABLED", True)

    with db_session() as session:
        with pytest.raises(ValueError) as excinfo:
            _ = token_vault.get_github_token(session, user_id)

    assert "not present" in str(excinfo.value).lower()


def test_get_invalidated_token_raises(vault_enabled):
    _ = vault_enabled
    apply_pending_migrations()

    user_id = str(uuid.uuid4())
    github_token = "gho_test_token"

    with db_session() as session:
        _create_user(session, user_id)
        token_vault.upsert_github_token(session, user_id, github_token, stored_by="test")
        token_vault.mark_github_token_invalid(session, user_id, "401 unauthorized")

    with db_session() as session:
        with pytest.raises(ValueError) as excinfo:
            _ = token_vault.get_github_token(session, user_id)

    assert "invalidat" in str(excinfo.value).lower()


def test_upsert_requires_vault_enabled_even_when_token_unchanged(vault_enabled, monkeypatch):
    _ = vault_enabled
    apply_pending_migrations()

    user_id = str(uuid.uuid4())
    github_token = "gho_test_token"

    with db_session() as session:
        _create_user(session, user_id)
        token_vault.upsert_github_token(session, user_id, github_token, stored_by="test")

    monkeypatch.setattr("services.shared.config.TOKEN_VAULT_KEYS_JSON", "")
    monkeypatch.setattr("services.shared.config.TOKEN_VAULT_ACTIVE_KEY_ID", "")
    monkeypatch.setattr("services.shared.config.TOKEN_VAULT_ENABLED", False)

    with db_session() as session:
        with pytest.raises(ValueError) as excinfo:
            token_vault.upsert_github_token(session, user_id, github_token, stored_by="test")

    assert "not enabled" in str(excinfo.value).lower()


def test_pipeline_vault_token_updates_last_verified_at(vault_enabled, monkeypatch):
    _ = vault_enabled
    apply_pending_migrations()

    from services.shared import pipeline

    user_id = str(uuid.uuid4())
    github_token = "gho_test_token"

    with db_session() as session:
        github_login, github_user_id = _create_user(session, user_id)
        token_vault.upsert_github_token(session, user_id, github_token, stored_by="test")
        before = session.execute(
            text(
                "SELECT last_verified_at "
                "FROM user_tokens WHERE user_id=:u AND provider='github'"
            ),
            {"u": user_id},
        ).fetchone()

    assert before is not None
    assert before[0] is None

    def _fake_fetch_user_login(_token):
        viewer = {
            "databaseId": github_user_id,
            "createdAt": datetime(2020, 1, 1, tzinfo=timezone.utc),
            "avatarUrl": "https://example.com/avatar",
        }
        return github_login, viewer

    def _fake_ingest_user_github_activity(*_args, **_kwargs):
        now = datetime.now(timezone.utc)
        return {"run_id": str(uuid.uuid4()), "pr_count": 0, "window_start": now, "window_end": now}

    def _fake_compute_user_metrics_and_language_profile(*_args, **_kwargs):
        now = datetime.now(timezone.utc)
        return {"run_id": str(uuid.uuid4()), "baseline_id": "b1", "computed_at": now}

    monkeypatch.setattr("services.shared.pipeline.fetch_user_login", _fake_fetch_user_login)
    monkeypatch.setattr("services.shared.pipeline.acquire_user_lock", lambda *_args, **_kwargs: "lock")
    monkeypatch.setattr("services.shared.pipeline.release_user_lock", lambda *_args, **_kwargs: None)
    monkeypatch.setattr("services.shared.pipeline.ingest_user_github_activity", _fake_ingest_user_github_activity)
    monkeypatch.setattr(
        "services.shared.pipeline.compute_user_metrics_and_language_profile",
        _fake_compute_user_metrics_and_language_profile,
    )
    monkeypatch.setattr("services.shared.pipeline.invalidate_metrics", lambda *_args, **_kwargs: None)

    result = pipeline.ingest_and_compute_user(
        user_id=user_id,
        github_token=None,
        backfill_days=30,
        partition_key="test",
        triggered_by="test",
    )

    assert result["status"] == "ok"

    with db_session() as session:
        after = session.execute(
            text(
                "SELECT last_verified_at "
                "FROM user_tokens WHERE user_id=:u AND provider='github'"
            ),
            {"u": user_id},
        ).fetchone()

    assert after is not None
    assert after[0] is not None
