"""Caching helpers for contributor metrics.

Key pattern:
    metrics:{user_id}
"""

import json
import logging
from typing import Any, Dict, Optional

import redis
from redis import Redis

from .config import REDIS_URL, CACHE_TTL_SECONDS

_redis: Optional[Redis] = None
logger = logging.getLogger("caching")


def get_redis() -> Redis:
    """
    Return a singleton Redis client

    Args:
        None

    Returns:
        redis.Redis client
    """
    global _redis
    if _redis is None:
        _redis = redis.Redis.from_url(REDIS_URL)
    return _redis


def get_cached_metrics(key) -> Optional[Dict[str, Any]]:
    """
    Retrieve cached metrics JSON

    Args:
        key (str): Cache key

    Returns:
        dict or None
    """
    try:
        client = get_redis()
        raw = client.get(key)
    except Exception as exc:
        logger.warning("get_cached_metrics redis error=%s", type(exc).__name__)
        return None

    if not raw:
        return None

    try:
        return json.loads(raw)
    except (TypeError, json.JSONDecodeError) as exc:
        logger.warning("get_cached_metrics decode error=%s", type(exc).__name__)
        return None


def set_cached_metrics(key, payload) -> None:
    """
    Cache metrics payload with TTL

    Args:
        key (str): Cache key
        payload (dict): Serializable payload

    Returns:
        None
    """
    try:
        client = get_redis()
        client.setex(key, CACHE_TTL_SECONDS, json.dumps(payload, default=str))
    except Exception as exc:
        logger.warning("set_cached_metrics redis error=%s", type(exc).__name__)
        return


def invalidate_metrics(user_id) -> None:
    """
    Invalidate metrics cache entry for a user

    Args:
        user_id (str): UUID

    Returns:
        None
    """
    try:
        client = get_redis()
        client.delete(f"metrics:{user_id}")
    except Exception as exc:
        logger.warning("invalidate_metrics redis error=%s", type(exc).__name__)
        return
