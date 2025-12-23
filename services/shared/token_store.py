import json
import logging
import uuid

from services.shared.caching import get_redis
from services.shared.config import TOKEN_REF_KEY_PREFIX


logger = logging.getLogger("token_store")

DEFAULT_TOKEN_REF_TTL_SECONDS = 15 * 60

_CONSUME_TOKEN_REF_LUA = """
local val = redis.call('GET', KEYS[1])
if not val then
  return nil
end
redis.call('DEL', KEYS[1])
return val
"""


def _token_ref_key(token_ref):
    return f"{TOKEN_REF_KEY_PREFIX}{token_ref}"


def _token_ref_lease_key(token_ref):
    return f"{TOKEN_REF_KEY_PREFIX}{token_ref}:lease"


def _encode_token_ref_payload(user_id, github_token):
    return json.dumps({"user_id": user_id, "github_token": github_token})


def _decode_token_ref_payload(raw_payload):
    payload_text = raw_payload.decode("utf-8") if hasattr(raw_payload, "decode") else str(raw_payload)

    try:
        payload = json.loads(payload_text)
    except (TypeError, json.JSONDecodeError) as exc:
        raise ValueError("token_ref payload could not be decoded") from exc

    github_token = payload.get("github_token")
    user_id = payload.get("user_id")

    if not github_token:
        raise ValueError("token_ref payload missing github_token")

    return user_id, github_token


def create_github_token_ref(session, user_id, github_token, ttl_seconds=None):
    """
    Create a short-lived Redis token reference

    Note:
        Token refs store the raw GitHub token in Redis without encryption
        This is acceptable only because they are short-lived (TTL) and are
        intended to be used by at most one Celery task via a lease

    Args:
        session: Unused (preserved for call-site compatibility)
        user_id (str): Optional internal user UUID
        github_token (str): GitHub OAuth token
        ttl_seconds (int): Optional TTL override

    Returns:
        str token_ref UUID
    """
    _ = session

    if not github_token:
        raise ValueError("github_token is required")

    token_ref = str(uuid.uuid4())
    ttl = int(ttl_seconds or DEFAULT_TOKEN_REF_TTL_SECONDS)

    if ttl <= 0:
        return token_ref

    redis_client = get_redis()
    redis_client.setex(
        _token_ref_key(token_ref),
        ttl,
        _encode_token_ref_payload(user_id, github_token),
    )
    return token_ref


def consume_github_token_ref(session, token_ref, expected_user_id=None):
    """
    Consume a token reference and return the token

    Consume is atomic: token returned once, then removed

    Prefer `lease_github_token_ref` when Celery `acks_late` is enabled

    Args:
        session: Unused (preserved for call-site compatibility)
        token_ref (str): Token ref UUID
        expected_user_id (str): Optional user id binding check

    Returns:
        str GitHub OAuth token
    """
    _ = session

    if not token_ref:
        raise ValueError("token_ref is required")

    redis_client = get_redis()
    raw_payload = redis_client.eval(_CONSUME_TOKEN_REF_LUA, 1, _token_ref_key(token_ref))

    if not raw_payload:
        raise ValueError("token_ref not found or expired")

    stored_user_id, github_token = _decode_token_ref_payload(raw_payload)

    if expected_user_id is not None and stored_user_id != expected_user_id:
        raise ValueError("token_ref invalid")

    return str(github_token)


def lease_github_token_ref(session, token_ref, expected_user_id=None, lease_id=None, lease_ttl_seconds=None):
    """
    Lease a token reference and return the token

    This is replay-safe for Celery `acks_late`: if the worker crashes after
    leasing the token but before completing the task, redelivery of the same
    Celery task id can re-lease and re-fetch the same token.

    Args:
        session: Unused (preserved for call-site compatibility)
        token_ref (str): Token ref UUID
        expected_user_id (str): Optional user id binding check
        lease_id (str): Required to enable leasing; typically the Celery task id
        lease_ttl_seconds (int): Optional lease TTL override

    Returns:
        str GitHub OAuth token

    Raises:
        ValueError: When token_ref is missing/expired or leased by a different id
    """
    _ = session

    if not token_ref:
        raise ValueError("token_ref is required")

    if not lease_id:
        raise ValueError("lease_id is required")

    ttl = int(lease_ttl_seconds or DEFAULT_TOKEN_REF_TTL_SECONDS)

    redis_client = get_redis()
    lease_key = _token_ref_lease_key(token_ref)
    token_key = _token_ref_key(token_ref)

    claimed = redis_client.set(lease_key, str(lease_id), nx=True, ex=ttl)
    if not claimed:
        raw_existing = redis_client.get(lease_key)

        if raw_existing is None:
            claimed = redis_client.set(lease_key, str(lease_id), nx=True, ex=ttl)
            if not claimed:
                raise ValueError("token_ref is already leased")
        else:
            existing = raw_existing.decode("utf-8") if hasattr(raw_existing, "decode") else str(raw_existing or "")
            if existing != str(lease_id):
                raw_payload = redis_client.get(token_key)
                if not raw_payload:
                    try:
                        redis_client.delete(lease_key)
                    except Exception:
                        pass
                    raise ValueError("token_ref not found or expired")
                raise ValueError("token_ref is already leased")

    raw_payload = redis_client.get(token_key)
    if not raw_payload:
        try:
            redis_client.delete(lease_key)
        except Exception:
            pass
        raise ValueError("token_ref not found or expired")

    stored_user_id, github_token = _decode_token_ref_payload(raw_payload)

    if expected_user_id is not None and stored_user_id != expected_user_id:
        raise ValueError("token_ref invalid")

    return str(github_token)


def finalize_github_token_ref(session, token_ref, lease_id=None, success=False):
    """
    Finalize a leased token reference

    On success, deletes both the token and lease keys. On failure, leaves the
    token in place so the same lease_id can retry.

    Args:
        session: Unused (preserved for call-site compatibility)
        token_ref (str): Token ref UUID
        lease_id (str): Lease id used during `lease_github_token_ref`
        success (bool): Whether the consumer completed successfully

    Returns:
        None
    """
    _ = session

    if not token_ref:
        return

    if not lease_id:
        return

    if not success:
        return

    try:
        redis_client = get_redis()
        redis_client.delete(_token_ref_key(token_ref))
    except Exception as exc:
        logger.warning(
            "finalize_github_token_ref failed to delete token key token_ref=%s lease_id=%s error=%s",
            token_ref,
            lease_id,
            type(exc).__name__,
        )
    try:
        redis_client = get_redis()
        redis_client.delete(_token_ref_lease_key(token_ref))
    except Exception as exc:
        logger.warning(
            "finalize_github_token_ref failed to delete lease key token_ref=%s lease_id=%s error=%s",
            token_ref,
            lease_id,
            type(exc).__name__,
        )
    return


def delete_github_token_ref(session, token_ref):
    """
    Delete a token reference when present

    Args:
        session: Unused (preserved for call-site compatibility)
        token_ref (str): Token ref UUID

    Returns:
        None
    """
    _ = session

    if not token_ref:
        return

    try:
        redis_client = get_redis()
        redis_client.delete(_token_ref_key(token_ref))
        redis_client.delete(_token_ref_lease_key(token_ref))
    except Exception as exc:
        logger.warning(
            "delete_github_token_ref failed token_ref=%s error=%s",
            token_ref,
            type(exc).__name__,
        )
        return


def cleanup_expired_token_refs(session):
    """
    Delete expired token references

    Redis-backed token refs expire automatically; this is a no-op

    Args:
        session: Unused

    Returns:
        int number of rows deleted
    """
    _ = session
    return 0
