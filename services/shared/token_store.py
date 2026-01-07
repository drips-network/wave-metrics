import base64
import json
import logging
import os
import uuid

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from services.shared import config as shared_config
from services.shared.caching import get_redis


logger = logging.getLogger("token_store")

TOKEN_REF_ENCRYPTION_ALG = "aes-256-gcm-v1"
TOKEN_REF_NONCE_BYTES = 12

DEFAULT_TOKEN_REF_TTL_SECONDS = int(shared_config.TOKEN_REF_TTL_SECONDS_NORMAL)

_TOKEN_REF_KEYRING_CACHE = None

_CONSUME_TOKEN_REF_LUA = """
local val = redis.call('GET', KEYS[1])
if not val then
  return nil
end
redis.call('DEL', KEYS[1])
return val
"""


def _token_ref_key(token_ref):
    return f"{shared_config.TOKEN_REF_KEY_PREFIX}{token_ref}"


def _token_ref_lease_key(token_ref):
    return f"{shared_config.TOKEN_REF_KEY_PREFIX}{token_ref}:lease"


def _load_token_ref_keyring():
    global _TOKEN_REF_KEYRING_CACHE
    if _TOKEN_REF_KEYRING_CACHE is not None:
        return _TOKEN_REF_KEYRING_CACHE

    raw_json = str(shared_config.TOKEN_REF_KEYS_JSON or "").strip()
    if not raw_json:
        raise ValueError("TOKEN_REF_KEYS_JSON is required")

    try:
        keyring_raw = json.loads(raw_json)
    except json.JSONDecodeError as exc:
        raise ValueError("TOKEN_REF_KEYS_JSON is not valid JSON") from exc

    if not isinstance(keyring_raw, dict):
        raise ValueError("TOKEN_REF_KEYS_JSON must be a JSON object mapping key IDs to base64 keys")

    decoded = {}
    for key_id, key_b64 in keyring_raw.items():
        key_id_str = str(key_id or "").strip()
        if not key_id_str:
            raise ValueError("TOKEN_REF_KEYS_JSON contains an empty key id")
        try:
            key_bytes = base64.b64decode(str(key_b64), validate=True)
        except Exception as exc:
            raise ValueError(f"Token ref key '{key_id_str}' is not valid base64") from exc

        if len(key_bytes) != 32:
            raise ValueError(f"Token ref key '{key_id_str}' must be 32 bytes (got {len(key_bytes)})")

        decoded[key_id_str] = key_bytes

    _TOKEN_REF_KEYRING_CACHE = decoded
    return decoded


def _active_token_ref_key_id():
    return str(shared_config.TOKEN_REF_ACTIVE_KEY_ID or "").strip()


def _normalize_user_id(value):
    if value is None:
        return None

    normalized = str(value).strip()
    return normalized or None


def _aad_bytes(user_id, token_ref):
    normalized_user_id = str(_normalize_user_id(user_id) or "")
    normalized_ref = str(token_ref or "")
    return f"github|{normalized_user_id}|{normalized_ref}".encode("utf-8")


def _encrypt_github_token(token_text, user_id, token_ref):
    key_id = _active_token_ref_key_id()
    if not key_id:
        raise ValueError("TOKEN_REF_ACTIVE_KEY_ID is required")

    key_bytes = _load_token_ref_keyring().get(key_id)
    if not key_bytes:
        raise ValueError(f"Active token ref key '{key_id}' not present in keyring")

    nonce = os.urandom(TOKEN_REF_NONCE_BYTES)
    aad = _aad_bytes(user_id, token_ref)
    ciphertext = AESGCM(key_bytes).encrypt(nonce, str(token_text).encode("utf-8"), aad)
    return {
        "alg": TOKEN_REF_ENCRYPTION_ALG,
        "kid": key_id,
        "nonce": base64.b64encode(nonce).decode("ascii"),
        "ciphertext": base64.b64encode(ciphertext).decode("ascii"),
    }


def _decrypt_github_token(enc_meta, ciphertext_b64, user_id, token_ref):
    try:
        key_id = str((enc_meta or {}).get("kid") or "").strip()
        key_bytes = _load_token_ref_keyring().get(key_id)
        if not key_bytes:
            raise ValueError("missing key")

        nonce_b = base64.b64decode(str((enc_meta or {}).get("nonce") or ""), validate=True)
        ciphertext_b = base64.b64decode(str(ciphertext_b64 or ""), validate=True)
        aad = _aad_bytes(user_id, token_ref)

        token_bytes = AESGCM(key_bytes).decrypt(nonce_b, ciphertext_b, aad)
        return token_bytes.decode("utf-8")
    except InvalidTag as exc:
        raise ValueError("token_ref payload could not be decoded") from exc
    except (TypeError, ValueError) as exc:
        raise ValueError("token_ref payload could not be decoded") from exc
    except Exception as exc:
        raise ValueError("token_ref payload could not be decoded") from exc


def _encode_token_ref_payload(user_id, github_token, token_ref):
    normalized_user_id = _normalize_user_id(user_id)
    enc = _encrypt_github_token(github_token, normalized_user_id, token_ref)
    return json.dumps(
        {
            "user_id": normalized_user_id,
            "github_token": enc["ciphertext"],
            "enc": {
                "alg": enc["alg"],
                "kid": enc["kid"],
                "nonce": enc["nonce"],
            },
        }
    )


def _decode_token_ref_payload(raw_payload, expected_user_id=None, token_ref=None):
    payload_text = raw_payload.decode("utf-8") if hasattr(raw_payload, "decode") else str(raw_payload)

    try:
        payload = json.loads(payload_text)
    except (TypeError, json.JSONDecodeError) as exc:
        raise ValueError("token_ref payload could not be decoded") from exc

    if not isinstance(payload, dict):
        raise ValueError("token_ref payload could not be decoded")

    token_field = payload.get("github_token")
    stored_user_id = _normalize_user_id(payload.get("user_id"))

    if not token_field:
        raise ValueError("token_ref payload missing github_token")

    if expected_user_id is not None and stored_user_id != _normalize_user_id(expected_user_id):
        raise ValueError("token_ref invalid")

    enc_meta = payload.get("enc")
    if not isinstance(enc_meta, dict):
        raise ValueError("token_ref payload could not be decoded")

    alg = str(enc_meta.get("alg") or "").strip()
    if alg != TOKEN_REF_ENCRYPTION_ALG:
        raise ValueError("token_ref payload could not be decoded")

    github_token = _decrypt_github_token(enc_meta, token_field, stored_user_id, token_ref)

    return stored_user_id, github_token


def create_github_token_ref(session, user_id, github_token, ttl_seconds=None):
    """
    Create a short-lived Redis token reference

    Note:
        Token refs store an encrypted GitHub token in Redis and are intended to
        be used by at most one Celery task via a lease

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
    normalized_user_id = _normalize_user_id(user_id)

    if ttl <= 0:
        return token_ref

    redis_client = get_redis()
    redis_client.setex(
        _token_ref_key(token_ref),
        ttl,
        _encode_token_ref_payload(normalized_user_id, github_token, token_ref),
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

    stored_user_id, github_token = _decode_token_ref_payload(
        raw_payload,
        expected_user_id=expected_user_id,
        token_ref=token_ref,
    )

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
                    except Exception as exc:
                        logger.warning(
                            "lease_github_token_ref: cleanup lease_key failed token_ref=%s lease_id=%s error=%s",
                            token_ref,
                            lease_id,
                            type(exc).__name__,
                        )
                    raise ValueError("token_ref not found or expired")
                raise ValueError("token_ref is already leased")

    raw_payload = redis_client.get(token_key)
    if not raw_payload:
        try:
            redis_client.delete(lease_key)
        except Exception as exc:
            logger.warning(
                "lease_github_token_ref: cleanup lease_key failed token_ref=%s lease_id=%s error=%s",
                token_ref,
                lease_id,
                type(exc).__name__,
            )
        raise ValueError("token_ref not found or expired")

    desired_lease_ttl = ttl
    try:
        token_ttl = int(redis_client.ttl(token_key))
        if token_ttl > desired_lease_ttl:
            desired_lease_ttl = token_ttl
    except Exception as exc:
        logger.warning(
            "lease_github_token_ref: token_key TTL read failed token_ref=%s lease_id=%s error=%s",
            token_ref,
            lease_id,
            type(exc).__name__,
        )

    try:
        if desired_lease_ttl > 0:
            redis_client.expire(lease_key, desired_lease_ttl)
    except Exception as exc:
        logger.warning(
            "lease_github_token_ref: lease_key TTL extend failed token_ref=%s lease_id=%s error=%s",
            token_ref,
            lease_id,
            type(exc).__name__,
        )

    try:
        if desired_lease_ttl > 0:
            redis_client.expire(token_key, desired_lease_ttl)
    except Exception as exc:
        logger.warning(
            "lease_github_token_ref: token_key TTL extend failed token_ref=%s lease_id=%s error=%s",
            token_ref,
            lease_id,
            type(exc).__name__,
        )

    stored_user_id, github_token = _decode_token_ref_payload(
        raw_payload,
        expected_user_id=expected_user_id,
        token_ref=token_ref,
    )

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
