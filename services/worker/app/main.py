from celery import Celery

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
    # Route all tasks to the "default" queue, which the worker is configured to consume
    app.conf.task_default_queue = "default"

    # Ensure tasks are redelivered if the worker crashes mid-run
    app.conf.task_acks_late = True
    app.conf.task_reject_on_worker_lost = True

    return app


celery_app = create_celery()


if __name__ == "__main__":
    celery_app.start()
