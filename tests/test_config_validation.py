import pytest

from services.shared import config as config_module


def test_validate_config_raises_on_invalid_values_expected(monkeypatch):
    monkeypatch.setattr(config_module, "GH_CONCURRENCY_PER_TOKEN", 0)

    with pytest.raises(ValueError) as excinfo:
        config_module.validate_config()

    assert "GH_CONCURRENCY_PER_TOKEN" in str(excinfo.value)


def test_validate_config_passes_with_valid_values_expected(monkeypatch):
    monkeypatch.setattr(config_module, "GH_CONCURRENCY_PER_TOKEN", 1)
    monkeypatch.setattr(config_module, "GH_SEMAPHORE_TTL_SECONDS", 300)
    monkeypatch.setattr(config_module, "GH_SEMAPHORE_ACQUIRE_TIMEOUT_SECONDS", 60)
    monkeypatch.setattr(config_module, "REPO_LANGUAGE_CACHE_MAX_REPOS", 10)

    config_module.validate_config()
