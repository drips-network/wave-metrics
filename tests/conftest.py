import base64
import json
import sys
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


@pytest.fixture(autouse=True)
def _isolate_test_env(monkeypatch):
    """
    Ensure tests don't depend on developer shell env vars

    In particular:
    - API auth is disabled unless a test explicitly enables it
    - Baseline selection uses test-seeded baselines unless a test explicitly pins one
    """
    monkeypatch.setattr("services.api.app.security.API_AUTH_TOKEN", "", raising=False)
    monkeypatch.setattr("services.shared.percentiles.POPULATION_BASELINE_ID", "", raising=False)

    monkeypatch.setattr("services.shared.config.TOKEN_VAULT_KEYS_JSON", "", raising=False)
    monkeypatch.setattr("services.shared.config.TOKEN_VAULT_ACTIVE_KEY_ID", "", raising=False)
    monkeypatch.setattr("services.shared.config.TOKEN_VAULT_ENABLED", False, raising=False)

    test_key_b64 = base64.b64encode(b"\x00" * 32).decode("ascii")
    monkeypatch.setattr(
        "services.shared.config.TOKEN_REF_KEYS_JSON",
        json.dumps({"test": test_key_b64}),
        raising=False,
    )
    monkeypatch.setattr("services.shared.config.TOKEN_REF_ACTIVE_KEY_ID", "test", raising=False)
    monkeypatch.setattr("services.shared.config.TOKEN_REF_TTL_SECONDS_NORMAL", 900, raising=False)
    monkeypatch.setattr("services.shared.config.TOKEN_REF_TTL_SECONDS_BULK", 86400, raising=False)
    monkeypatch.setattr("services.shared.token_store._TOKEN_REF_KEYRING_CACHE", None, raising=False)
