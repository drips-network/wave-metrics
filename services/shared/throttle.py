import hashlib
import logging
import random
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

import httpx


from services.shared.caching import get_redis
from services.shared.config import (
    GH_BACKOFF_BASE_SECONDS,
    GH_BACKOFF_CAP_SECONDS,
    GH_CONCURRENCY_PER_TOKEN,
    GH_COOLDOWN_MAX_WAIT_SECONDS,
    GH_MAX_RETRIES,
    GH_REDIS_PREFIX,
    GH_SEMAPHORE_ACQUIRE_TIMEOUT_SECONDS,
    GH_SEMAPHORE_TTL_SECONDS,
)

GITHUB_GRAPHQL_URL = "https://api.github.com/graphql"
DEFAULT_GITHUB_TIMEOUT = 30.0
SEMAPHORE_ROUTE_KEY = "graphql"

logger = logging.getLogger("throttle")

_ACQUIRE_SEMAPHORE_LUA = """
local val = redis.call('INCR', KEYS[1])
redis.call('EXPIRE', KEYS[1], tonumber(ARGV[2]))
if val <= tonumber(ARGV[1]) then
  return val
end
redis.call('DECR', KEYS[1])
return 0
"""

_RELEASE_SEMAPHORE_LUA = """
local val = redis.call('DECR', KEYS[1])
if val <= 0 then
  redis.call('DEL', KEYS[1])
end
return val
"""


def _token_hash(token):
    """
    Stable hash of the token used for Redis keys

    Uses a longer prefix to make collisions negligibly likely

    Args:
        token (str): OAuth token

    Returns:
        str token hash prefix
    """
    return hashlib.sha256((token or "").encode("utf-8")).hexdigest()[:32]


def _budget_key(token_hash):
    return f"{GH_REDIS_PREFIX}budget:{token_hash}"


def _cooldown_key(token_hash):
    return f"{GH_REDIS_PREFIX}cooldown:{token_hash}"


def _sem_key(token_hash):
    return f"{GH_REDIS_PREFIX}sem:{token_hash}"


def _semaphore_ttl_seconds():
    return max(30, int(GH_SEMAPHORE_TTL_SECONDS))


def _semaphore_acquire_timeout_seconds():
    return max(1, int(GH_SEMAPHORE_ACQUIRE_TIMEOUT_SECONDS))


def _parse_retry_after(value):
    if not value:
        return 0

    try:
        return max(0, int(value))
    except (TypeError, ValueError):
        pass

    try:
        dt = parsedate_to_datetime(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        return max(0, int((dt - now).total_seconds()))
    except (TypeError, ValueError, OverflowError):
        return 0


def _acquire_token_semaphore(redis_client, token_hash, limit, timeout_seconds):
    """
    Acquire a per-token concurrency permit using a counter semaphore

    Args:
        redis_client: Redis client
        token_hash (str): Token hash used in key names
        limit (int): Maximum concurrent requests per token
        timeout_seconds (int): Maximum time to wait for a permit

    Raises:
        TimeoutError: When a permit cannot be acquired within the timeout
    """
    sem_key = _sem_key(token_hash)
    ttl_seconds = _semaphore_ttl_seconds()
    deadline = time.time() + float(timeout_seconds)

    while True:
        if time.time() > deadline:
            raise TimeoutError(
                f"Timed out acquiring GitHub semaphore for token={token_hash[:6]} timeout={timeout_seconds}s"
            )

        try:
            value = int(redis_client.eval(_ACQUIRE_SEMAPHORE_LUA, 1, sem_key, int(limit), int(ttl_seconds)))
        except Exception as exc:
            raise RuntimeError("Redis unavailable for GitHub throttling") from exc

        if value > 0:
            return

        time.sleep(0.2)


def _release_token_semaphore(redis_client, token_hash):
    """
    Release a per-token concurrency permit

    Never raises; best-effort cleanup

    Args:
        redis_client: Redis client
        token_hash (str): Token hash used in key names

    Returns:
        None
    """
    try:
        sem_key = _sem_key(token_hash)
        redis_client.eval(_RELEASE_SEMAPHORE_LUA, 1, sem_key)
    except Exception as exc:
        logger.warning(
            "throttle: failed to release semaphore token=%s error=%s",
            token_hash[:6],
            type(exc).__name__,
        )
        return


def _wait_for_secondary_cooldown(redis_client, token_hash):
    cd_key = _cooldown_key(token_hash)

    while redis_client.exists(cd_key):
        ttl_seconds = redis_client.ttl(cd_key)

        if ttl_seconds == -2:
            return

        if ttl_seconds in (None, -1):
            logger.warning(
                "throttle: cooldown key missing TTL; repairing (token=%s)",
                token_hash[:6],
            )
            redis_client.expire(cd_key, min(5, GH_COOLDOWN_MAX_WAIT_SECONDS))
            ttl_seconds = redis_client.ttl(cd_key)

        if ttl_seconds is not None and ttl_seconds > GH_COOLDOWN_MAX_WAIT_SECONDS:
            logger.warning(
                "throttle: cooldown TTL too large; capping to %ss (token=%s)",
                GH_COOLDOWN_MAX_WAIT_SECONDS,
                token_hash[:6],
            )
            redis_client.expire(cd_key, GH_COOLDOWN_MAX_WAIT_SECONDS)
            ttl_seconds = GH_COOLDOWN_MAX_WAIT_SECONDS

        sleep_seconds = 1 if ttl_seconds is None or ttl_seconds < 1 else min(5, ttl_seconds)
        logger.info(
            "throttle: secondary cooldown active for %s, ttl=%s",
            token_hash[:6],
            ttl_seconds,
        )
        time.sleep(sleep_seconds)


def begin_request(github_token, route_key=SEMAPHORE_ROUTE_KEY):
    """
    Block if cooldown or budget reset required; acquire per-token semaphore

    Args:
        github_token (str): OAuth token
        route_key (str): Optional route key for future granularity

    Returns:
        str token hash (for end_request)
    """
    token_hash = _token_hash(github_token)
    redis_client = get_redis()

    _wait_for_secondary_cooldown(redis_client, token_hash)

    b_key = _budget_key(token_hash)
    budget = redis_client.hgetall(b_key) or {}
    try:
        remaining = int(budget.get(b"remaining", b"-1"))
        reset_epoch = int(budget.get(b"reset_epoch", b"0"))
    except (TypeError, ValueError):
        remaining, reset_epoch = -1, 0

    now_epoch = int(time.time())
    if remaining == 0 and reset_epoch > now_epoch:
        wait_seconds = max(0, reset_epoch - now_epoch) + 1
        logger.info("throttle: waiting for primary reset %ss (token=%s)", wait_seconds, token_hash[:6])
        time.sleep(wait_seconds)

    _acquire_token_semaphore(
        redis_client,
        token_hash,
        limit=GH_CONCURRENCY_PER_TOKEN,
        timeout_seconds=_semaphore_acquire_timeout_seconds(),
    )
    return token_hash, redis_client


def end_request(redis_client, route_key, response, token_hash):
    """
    Update budget and cooldown, and release semaphore

    Never raises; errors are logged but do not mask upstream failures

    Args:
        route_key (str): Route key
        response (httpx.Response): HTTP response
        token_hash (str): Token hash returned from begin_request

    Returns:
        None
    """
    if redis_client is None:
        logger.warning("throttle: end_request missing redis_client token=%s", token_hash[:6])
        return

    try:
        remaining_hdr = response.headers.get("X-RateLimit-Remaining")
        reset_hdr = response.headers.get("X-RateLimit-Reset")
        if remaining_hdr is not None and reset_hdr is not None:
            try:
                remaining = int(remaining_hdr)
                reset_epoch = int(reset_hdr)
                ttl = max(0, reset_epoch - int(time.time())) + 5
                redis_client.hset(
                    _budget_key(token_hash),
                    mapping={"remaining": remaining, "reset_epoch": reset_epoch},
                )
                redis_client.expire(_budget_key(token_hash), ttl)
            except Exception as exc:
                logger.warning(
                    "throttle: failed to update budget token=%s error=%s",
                    token_hash[:6],
                    type(exc).__name__,
                )

        if response.status_code in (403, 429):
            retry_after = response.headers.get("Retry-After")
            seconds = _parse_retry_after(retry_after)
            if seconds > 0:
                try:
                    redis_client.setex(_cooldown_key(token_hash), seconds, 1)
                    logger.info("throttle: applying Retry-After cooldown %ss token=%s", seconds, token_hash[:6])
                except Exception as exc:
                    logger.warning(
                        "throttle: failed to set cooldown token=%s error=%s",
                        token_hash[:6],
                        type(exc).__name__,
                    )
    finally:
        _release_token_semaphore(redis_client, token_hash)


@contextmanager
def _http_client(timeout=DEFAULT_GITHUB_TIMEOUT):
    with httpx.Client(timeout=timeout) as client:
        yield client


def send_github_graphql(github_token, json_payload, timeout=DEFAULT_GITHUB_TIMEOUT):
    """
    Send a GraphQL request with Redis-coordinated throttling and retries

    Args:
        github_token (str): OAuth token
        json_payload (dict): JSON payload with {query, variables}
        timeout (float): HTTP client timeout per request

    Returns:
        httpx.Response
    """
    route_key = SEMAPHORE_ROUTE_KEY
    attempt = 0

    while True:
        token_hash, redis_client = begin_request(github_token, route_key)
        headers = {"Authorization": f"bearer {github_token}"}
        response = None

        try:
            with _http_client(timeout=timeout) as client:
                response = client.post(GITHUB_GRAPHQL_URL, headers=headers, json=json_payload)
        finally:
            if response is not None:
                end_request(redis_client, route_key, response, token_hash)
            else:
                try:
                    _release_token_semaphore(redis_client, token_hash)
                except Exception as exc:
                    logger.warning(
                        "throttle: cleanup release failed token=%s error=%s",
                        token_hash[:6],
                        type(exc).__name__,
                    )

        if response.status_code == 401:
            raise PermissionError("GitHub token unauthorized")

        if response.status_code in (403, 429):
            retry_after = response.headers.get("Retry-After")
            wait_seconds = _parse_retry_after(retry_after)
            if wait_seconds > 0:
                logger.info(
                    "throttle: waiting Retry-After %ss then retry (route=%s, token=%s, attempt=%s)",
                    wait_seconds,
                    route_key,
                    token_hash[:6],
                    attempt + 1,
                )
                time.sleep(wait_seconds)
                attempt += 1
                if attempt > GH_MAX_RETRIES:
                    response.raise_for_status()
                continue

            sleep_seconds = min(GH_BACKOFF_CAP_SECONDS, GH_BACKOFF_BASE_SECONDS * (2 ** attempt))
            sleep_seconds += random.random()
            logger.warning(
                "throttle: backing off %0.1fs (attempt=%s, route=%s, token=%s)",
                sleep_seconds,
                attempt + 1,
                route_key,
                token_hash[:6],
            )
            time.sleep(sleep_seconds)
            attempt += 1
            if attempt > GH_MAX_RETRIES:
                response.raise_for_status()
            continue

        if response.status_code >= 400:
            logger.error(
                "throttle: GitHub HTTP error %s for route=%s token=%s; raising",
                response.status_code,
                route_key,
                token_hash[:6],
            )
        response.raise_for_status()

        remaining_hdr = response.headers.get("X-RateLimit-Remaining", "?")
        logger.debug(
            "throttle: request completed (route=%s, token=%s, remaining=%s, attempts=%s)",
            route_key,
            token_hash[:6],
            remaining_hdr,
            attempt + 1,
        )
        return response
