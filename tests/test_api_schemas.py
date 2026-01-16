import uuid

import pytest
from pydantic import ValidationError

from services.api.app.schemas import MetricEntry, SyncRequest


def test_sync_request_backfill_days_defaults_to_none():
    req = SyncRequest(user_id=str(uuid.uuid4()), github_token="token-abc")
    assert req.backfill_days is None


def test_sync_request_accepts_github_login_expected():
    req = SyncRequest(github_login=" OctoCat ", github_token="token-abc")
    assert req.github_login == "octocat"


def test_sync_request_rejects_empty_github_login_expected():
    with pytest.raises(ValidationError):
        SyncRequest(github_login="   ", github_token="token-abc")


def test_metric_entry_lower_is_better_defaults_false_expected():
    entry = MetricEntry(value=1, percentile=50.0, bin="High", description="x")
    assert entry.lower_is_better is False
