import logging
import time
import uuid

from redis.exceptions import RedisError

from services.shared.caching import get_redis


logger = logging.getLogger("locks")


_RELEASE_LOCK_LUA = """
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("DEL", KEYS[1])
end
return 0
"""


def user_lock_key(user_id):
    return f"wm:lock:user:{user_id}"


def acquire_user_lock(user_id, ttl_seconds=900, wait_timeout_seconds=0):
    """
    Acquire a per-user distributed lock

    Args:
        user_id (str): Internal user UUID
        ttl_seconds (int): Lock TTL for crash safety
        wait_timeout_seconds (int): Max time to wait; 0 means fail fast

    Returns:
        str lock_value used for release

    Raises:
        TimeoutError: When lock cannot be acquired within wait_timeout_seconds
    """
    redis_client = get_redis()
    lock_key = user_lock_key(user_id)
    lock_value = str(uuid.uuid4())

    wait_timeout_seconds = max(0, int(wait_timeout_seconds))
    ttl_seconds = max(1, int(ttl_seconds))

    deadline = time.monotonic() + float(wait_timeout_seconds)

    while True:
        acquired = redis_client.set(lock_key, lock_value, nx=True, ex=ttl_seconds)
        if acquired:
            return lock_value

        if time.monotonic() >= deadline:
            raise TimeoutError(f"User lock already held for user_id={user_id}")

        time.sleep(0.2)


def release_user_lock(user_id, lock_value):
    """
    Release a per-user distributed lock

    Never raises; best-effort cleanup
    """
    try:
        redis_client = get_redis()
        redis_client.eval(_RELEASE_LOCK_LUA, 1, user_lock_key(user_id), lock_value)
    except RedisError as exc:
        logger.error(
            "Failed to release user lock",
            extra={"user_id": user_id, "error": type(exc).__name__},
            exc_info=True,
        )
        return
