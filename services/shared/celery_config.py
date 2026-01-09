"""
Celery routing configuration shared across workers
"""

CELERY_DEFAULT_QUEUE = "default"

CELERY_QUEUE_NAMES = (
    "default",
    "bulk",
)

CELERY_TASK_ROUTES = {
    "sync_and_compute": {"queue": "default"},
}
