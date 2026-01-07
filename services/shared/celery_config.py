"""
Celery routing configuration shared across scheduler and workers
"""

CELERY_DEFAULT_QUEUE = "default"

CELERY_QUEUE_NAMES = (
    "default",
    "bulk",
    "daily",
    "backfill",
)

CELERY_TASK_ROUTES = {
    "sync_and_compute": {"queue": "default"},
    "refresh_daily": {"queue": "daily"},
    "backfill_user": {"queue": "backfill"},
}
