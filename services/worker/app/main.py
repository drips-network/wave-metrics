import copy

from celery import Celery
from kombu import Queue

from services.shared.celery_config import (
    CELERY_DEFAULT_QUEUE,
    CELERY_QUEUE_NAMES,
    CELERY_TASK_ROUTES,
)
from services.shared.config import REDIS_URL, validate_config


def create_celery():
    """
    Create a Celery instance configured with Redis

    Args:
        None

    Returns:
        Celery app
    """
    validate_config()

    app = Celery(
        "wave-metrics-worker",
        broker=REDIS_URL,
        backend=REDIS_URL,
        include=["services.worker.app.tasks"],
    )
    # Default queue is "default"; task_routes send daily/backfill to dedicated queues
    app.conf.task_default_queue = CELERY_DEFAULT_QUEUE

    app.conf.task_queues = tuple(Queue(name) for name in CELERY_QUEUE_NAMES)

    app.conf.task_routes = copy.deepcopy(CELERY_TASK_ROUTES)

    # Ensure tasks are redelivered if the worker crashes mid-run
    app.conf.task_acks_late = True
    app.conf.task_reject_on_worker_lost = True

    return app


celery_app = create_celery()


if __name__ == "__main__":
    celery_app.start()
