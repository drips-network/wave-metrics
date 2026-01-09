import base64
import json

import pytest

from services.shared import config as config_module


def test_validate_config_raises_on_invalid_values_expected(monkeypatch):
    monkeypatch.setattr(config_module, "GH_CONCURRENCY_PER_TOKEN", 0)

    with pytest.raises(ValueError) as excinfo:
        config_module.validate_config()

    assert "GH_CONCURRENCY_PER_TOKEN" in str(excinfo.value)


def test_validate_config_requires_token_ref_keys_expected(monkeypatch):
    monkeypatch.setattr(config_module, "TOKEN_REF_KEYS_JSON", "", raising=False)
    monkeypatch.setattr(config_module, "TOKEN_REF_ACTIVE_KEY_ID", "", raising=False)

    with pytest.raises(ValueError) as excinfo:
        config_module.validate_config()

    message = str(excinfo.value)
    assert "TOKEN_REF_KEYS_JSON is required" in message
    assert "TOKEN_REF_ACTIVE_KEY_ID is required" in message


def test_validate_config_rejects_short_token_ref_key_expected(monkeypatch):
    short_key = base64.b64encode(b"\x00" * 16).decode("ascii")
    monkeypatch.setattr(
        config_module,
        "TOKEN_REF_KEYS_JSON",
        json.dumps({"short": short_key}),
        raising=False,
    )
    monkeypatch.setattr(config_module, "TOKEN_REF_ACTIVE_KEY_ID", "short", raising=False)

    with pytest.raises(ValueError) as excinfo:
        config_module.validate_config()

    assert "must be 32 bytes" in str(excinfo.value)


def test_validate_config_passes_with_valid_values_expected(monkeypatch):
    monkeypatch.setattr(config_module, "GH_CONCURRENCY_PER_TOKEN", 1)
    monkeypatch.setattr(config_module, "GH_SEMAPHORE_TTL_SECONDS", 300)
    monkeypatch.setattr(config_module, "GH_SEMAPHORE_ACQUIRE_TIMEOUT_SECONDS", 60)
    monkeypatch.setattr(config_module, "REPO_LANGUAGE_CACHE_MAX_REPOS", 10)

    valid_key = base64.b64encode(b"\x01" * 32).decode("ascii")
    monkeypatch.setattr(config_module, "TOKEN_REF_KEYS_JSON", json.dumps({"test": valid_key}), raising=False)
    monkeypatch.setattr(config_module, "TOKEN_REF_ACTIVE_KEY_ID", "test", raising=False)

    config_module.validate_config()
