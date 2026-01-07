import re
import uuid
from typing import List, Literal, Optional, Union

from pydantic import BaseModel, Field, SecretStr, field_validator

_YYYY_MM_DD_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_GITHUB_LOGIN_RE = re.compile(r"^[a-z0-9-]{1,39}$")
_METRIC_BIN = Literal["Very Low", "Low", "Medium", "High", "Very High", "Exceptional"]


class BaselineDates(BaseModel):
    start: str  # YYYY-MM-DD
    end: str  # YYYY-MM-DD

    @field_validator("start", "end")
    @classmethod
    def _validate_yyyy_mm_dd(cls, value: str) -> str:
        if not _YYYY_MM_DD_RE.match(value):
            raise ValueError("must be YYYY-MM-DD")
        return value


class MetricEntry(BaseModel):
    value: Optional[Union[int, float]] = None
    percentile: Optional[float] = Field(default=None, ge=0.0, le=100.0)
    bin: Optional[_METRIC_BIN] = None
    description: str


class LanguageShare(BaseModel):
    language: str
    pct: float


class MetricsPayload(BaseModel):
    total_opened_prs: MetricEntry
    total_merged_prs: MetricEntry
    pr_merge_rate: MetricEntry
    pr_drop_rate: MetricEntry
    avg_merge_latency_hours: MetricEntry
    oss_prs_opened: MetricEntry
    oss_reviews: MetricEntry
    oss_issues_opened: MetricEntry
    oss_composite: MetricEntry


class MetricsResponse(BaseModel):
    user_id: str
    github_login: str
    github_created_at: Optional[str] = None
    metrics_window_start: str
    metrics_window_end: str
    metrics_baseline_dates: BaselineDates
    metrics: MetricsPayload
    lifetime_language_profile: List[LanguageShare]
    computed_at: str


class SyncRequest(BaseModel):
    user_id: Optional[str] = None
    github_login: Optional[str] = None
    github_token: Optional[SecretStr] = None
    backfill_days: Optional[int] = Field(default=None, ge=1)
    queue: Optional[str] = None

    @field_validator("user_id")
    @classmethod
    def _validate_user_id_uuid(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        uuid.UUID(value)
        return value

    @field_validator("github_login")
    @classmethod
    def _validate_github_login(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None

        normalized = value.strip().lower()
        if not normalized:
            raise ValueError("github_login must not be empty")

        if not _GITHUB_LOGIN_RE.match(normalized):
            raise ValueError("github_login is invalid")

        return normalized

    @field_validator("queue")
    @classmethod
    def _validate_queue(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None

        normalized = value.strip().lower()
        if not normalized:
            return None

        if normalized not in {"default", "bulk"}:
            raise ValueError("queue must be 'default' or 'bulk'")

        return normalized
