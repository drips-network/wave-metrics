import base64
import hashlib
import json
import os
import uuid

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from sqlalchemy import text

from services.shared import config as shared_config


ENCRYPTION_ALG = "aes-256-gcm-v1"
GITHUB_PROVIDER = "github"
NONCE_BYTES = 12
MAX_INVALIDATION_REASON_CHARS = 500


def _require_uuid_string(value, field_name):
    """
    Normalize and validate a required UUID string boundary

    Args:
        value: Value to validate (str)
        field_name (str): Name used in error messages

    Returns:
        str normalized UUID

    Raises:
        ValueError: When value is missing or not a valid UUID
    """
    if not value:
        raise ValueError(f"{field_name} must be a valid UUID")

    try:
        return str(uuid.UUID(str(value)))
    except ValueError as exc:
        raise ValueError(f"{field_name} must be a valid UUID") from exc


def _aad_bytes(user_id, provider) -> bytes:
    """
    Build stable AAD bytes for token encryption

    Contract:
        AAD format is permanently defined as:
            f"{canonical_user_id}|{provider}".encode("utf-8")

        canonical_user_id is str(uuid.UUID(user_id))
        provider is lowercase (e.g. "github")

        Do not change this format. If it changes, existing ciphertext becomes undecryptable
    """
    canonical_user_id = str(uuid.UUID(str(user_id)))
    normalized_provider = str(provider or "").strip().lower()
    return f"{canonical_user_id}|{normalized_provider}".encode("utf-8")


def _fingerprint_token(token_text):
    return hashlib.sha256(str(token_text).encode("utf-8")).hexdigest()


def _load_keyring():
    """
    Load and decode the token vault keyring

    Returns:
        Tuple of (active_key_id, keyring_bytes)

    Raises:
        ValueError: When vault is not enabled or keyring is invalid
    """
    if not shared_config.TOKEN_VAULT_ENABLED:
        raise ValueError("Token vault is not enabled")

    active_key_id = str(shared_config.TOKEN_VAULT_ACTIVE_KEY_ID or "").strip()
    if not active_key_id:
        raise ValueError("TOKEN_VAULT_ACTIVE_KEY_ID is required")

    raw_json = str(shared_config.TOKEN_VAULT_KEYS_JSON or "").strip()
    if not raw_json:
        raise ValueError("TOKEN_VAULT_KEYS_JSON is required")

    try:
        keyring_raw = json.loads(raw_json)
    except json.JSONDecodeError as exc:
        raise ValueError("TOKEN_VAULT_KEYS_JSON is not valid JSON") from exc

    if not isinstance(keyring_raw, dict):
        raise ValueError("TOKEN_VAULT_KEYS_JSON must be a JSON object mapping key IDs to base64 keys")

    keyring = {}
    for key_id, key_b64 in keyring_raw.items():
        if not key_id:
            raise ValueError("TOKEN_VAULT_KEYS_JSON contains an empty key id")
        try:
            key_bytes = base64.b64decode(str(key_b64), validate=True)
        except Exception as exc:
            raise ValueError(f"Token vault key '{key_id}' is not valid base64") from exc

        if len(key_bytes) != 32:
            raise ValueError(f"Token vault key '{key_id}' must be 32 bytes (got {len(key_bytes)})")
        keyring[str(key_id)] = key_bytes

    if active_key_id not in keyring:
        raise ValueError(f"Active token vault key '{active_key_id}' not present in keyring")

    return active_key_id, keyring


def _encrypt_with_key(key_bytes, token_text, user_id, provider):
    token_bytes = str(token_text).encode("utf-8")
    nonce = os.urandom(NONCE_BYTES)
    aad = _aad_bytes(user_id, provider)
    ciphertext = AESGCM(key_bytes).encrypt(nonce, token_bytes, aad)
    return nonce, ciphertext


def _decrypt_with_key(key_bytes, nonce, ciphertext, user_id, provider):
    aad = _aad_bytes(user_id, provider)
    try:
        plaintext = AESGCM(key_bytes).decrypt(bytes(nonce), bytes(ciphertext), aad)
    except InvalidTag as exc:
        raise ValueError("Token ciphertext authentication failed") from exc
    except (TypeError, ValueError) as exc:
        raise ValueError("Token ciphertext could not be decrypted") from exc

    try:
        return plaintext.decode("utf-8")
    except Exception as exc:
        raise ValueError("Token plaintext is not valid UTF-8") from exc


def upsert_github_token(session, user_id, github_token, verified_at=None, stored_by=None) -> None:
    """
    Encrypt and upsert a GitHub token for a user

    Args:
        session: SQLAlchemy session
        user_id (str): Internal user UUID
        github_token (str): Raw GitHub token
        verified_at (datetime): Optional verification timestamp
        stored_by (str): Optional label like 'api', 'worker', 'scheduler'

    Raises:
        ValueError: When vault is not enabled or user_id is invalid
        RuntimeError: When encryption or DB write fails
    """
    if not github_token:
        raise ValueError("github_token is required")

    canonical_user_id = _require_uuid_string(user_id, "user_id")
    provider = GITHUB_PROVIDER

    active_key_id, keyring = _load_keyring()

    token_fingerprint = _fingerprint_token(github_token)

    try:
        row = session.execute(
            text(
                "SELECT token_fingerprint "
                "FROM user_tokens "
                "WHERE user_id = :u AND provider = :p"
            ),
            {"u": canonical_user_id, "p": provider},
        ).fetchone()

        if row and str(row[0] or "") == token_fingerprint:
            session.execute(
                text(
                    "UPDATE user_tokens SET "
                    "stored_by = COALESCE(:stored_by, stored_by), "
                    "last_verified_at = COALESCE(:verified_at, last_verified_at), "
                    "invalidated_at = NULL, "
                    "invalidated_reason = NULL, "
                    "updated_at = NOW() "
                    "WHERE user_id = :u AND provider = :p"
                ),
                {
                    "stored_by": stored_by,
                    "verified_at": verified_at,
                    "u": canonical_user_id,
                    "p": provider,
                },
            )
            return

        nonce, ciphertext = _encrypt_with_key(keyring[active_key_id], github_token, canonical_user_id, provider)

        session.execute(
            text(
                "INSERT INTO user_tokens ("
                "user_id, provider, encryption_key_id, encryption_alg, nonce, ciphertext, "
                "token_fingerprint, stored_by, last_verified_at"
                ") VALUES ("
                ":u, :p, :kid, :alg, :nonce, :ciphertext, :fp, :stored_by, :verified_at"
                ") ON CONFLICT (user_id, provider) DO UPDATE SET "
                "encryption_key_id = EXCLUDED.encryption_key_id, "
                "encryption_alg = EXCLUDED.encryption_alg, "
                "nonce = EXCLUDED.nonce, "
                "ciphertext = EXCLUDED.ciphertext, "
                "token_fingerprint = EXCLUDED.token_fingerprint, "
                "stored_by = COALESCE(EXCLUDED.stored_by, user_tokens.stored_by), "
                "last_verified_at = COALESCE(EXCLUDED.last_verified_at, user_tokens.last_verified_at), "
                "invalidated_at = NULL, "
                "invalidated_reason = NULL, "
                "updated_at = NOW()"
            ),
            {
                "u": canonical_user_id,
                "p": provider,
                "kid": active_key_id,
                "alg": ENCRYPTION_ALG,
                "nonce": nonce,
                "ciphertext": ciphertext,
                "fp": token_fingerprint,
                "stored_by": stored_by,
                "verified_at": verified_at,
            },
        )
    except ValueError:
        raise
    except Exception as exc:
        raise RuntimeError("Failed to upsert GitHub token") from exc


def get_github_token(session, user_id, rotate_to_active=True, touch_last_used=True) -> str:
    """
    Fetch and decrypt the persisted GitHub token for a user

    Args:
        session: SQLAlchemy session
        user_id (str): Internal user UUID
        rotate_to_active (bool): Re-encrypt with active key when stored key is old
        touch_last_used (bool): Update last_used_at on successful read

    Returns:
        str GitHub token

    Raises:
        ValueError: When token is missing, invalidated, vault not configured,
            or key_id not in keyring
    """
    canonical_user_id = _require_uuid_string(user_id, "user_id")
    provider = GITHUB_PROVIDER

    active_key_id, keyring = _load_keyring()

    row = session.execute(
        text(
            "SELECT encryption_key_id, nonce, ciphertext, invalidated_at, invalidated_reason "
            "FROM user_tokens WHERE user_id = :u AND provider = :p"
        ),
        {"u": canonical_user_id, "p": provider},
    ).fetchone()

    if not row:
        raise ValueError("GitHub token not found")

    stored_key_id, nonce, ciphertext, invalidated_at, invalidated_reason = row

    if invalidated_at is not None:
        reason_text = str(invalidated_reason or "").strip()
        reason_suffix = f": {reason_text}" if reason_text else ""
        raise ValueError(f"GitHub token is invalidated{reason_suffix}")

    stored_key_id = str(stored_key_id or "").strip()
    if stored_key_id not in keyring:
        raise ValueError(f"Token vault key '{stored_key_id}' not present in keyring")

    token_text = _decrypt_with_key(keyring[stored_key_id], nonce, ciphertext, canonical_user_id, provider)

    if rotate_to_active and stored_key_id != active_key_id:
        new_nonce, new_ciphertext = _encrypt_with_key(
            keyring[active_key_id],
            token_text,
            canonical_user_id,
            provider,
        )
        session.execute(
            text(
                "UPDATE user_tokens SET "
                "encryption_key_id = :kid, "
                "encryption_alg = :alg, "
                "nonce = :nonce, "
                "ciphertext = :ciphertext, "
                "token_fingerprint = :fp, "
                "updated_at = NOW() "
                "WHERE user_id = :u AND provider = :p"
            ),
            {
                "kid": active_key_id,
                "alg": ENCRYPTION_ALG,
                "nonce": new_nonce,
                "ciphertext": new_ciphertext,
                "fp": _fingerprint_token(token_text),
                "u": canonical_user_id,
                "p": provider,
            },
        )

    if touch_last_used:
        session.execute(
            text(
                "UPDATE user_tokens SET last_used_at = NOW() "
                "WHERE user_id = :u AND provider = :p"
            ),
            {"u": canonical_user_id, "p": provider},
        )

    return token_text


def mark_github_token_invalid(session, user_id, reason) -> None:
    """
    Mark a user's GitHub token invalid to avoid repeated scheduled failures

    Args:
        session: SQLAlchemy session
        user_id (str): Internal user UUID
        reason (str): Human-readable reason (truncated to 500 chars)

    Returns:
        None
    """
    canonical_user_id = _require_uuid_string(user_id, "user_id")
    provider = GITHUB_PROVIDER

    reason_text = str(reason or "").strip()
    if len(reason_text) > MAX_INVALIDATION_REASON_CHARS:
        reason_text = reason_text[:MAX_INVALIDATION_REASON_CHARS]

    result = session.execute(
        text(
            "UPDATE user_tokens SET "
            "invalidated_at = NOW(), "
            "invalidated_reason = :reason, "
            "updated_at = NOW() "
            "WHERE user_id = :u AND provider = :p"
        ),
        {"reason": reason_text, "u": canonical_user_id, "p": provider},
    )

    if not getattr(result, "rowcount", 0):
        raise ValueError("GitHub token not found")


def reencrypt_tokens_to_active_key(session, provider, batch_size=500) -> int:
    """
    Re-encrypt a batch of tokens to the active key

    Args:
        session: SQLAlchemy session
        provider (str): Token provider (e.g., "github")
        batch_size (int): Number of rows per batch

    Returns:
        int number of rows updated
    """
    provider_norm = str(provider or "").strip().lower()
    if not provider_norm:
        raise ValueError("provider is required")

    active_key_id, keyring = _load_keyring()

    updated = 0

    while True:
        rows = session.execute(
            text(
                "SELECT user_id, encryption_key_id, nonce, ciphertext "
                "FROM user_tokens "
                "WHERE provider = :p AND encryption_key_id <> :kid "
                "ORDER BY updated_at ASC "
                "LIMIT :limit"
            ),
            {"p": provider_norm, "kid": active_key_id, "limit": int(batch_size)},
        ).fetchall()

        if not rows:
            break

        for user_id_val, stored_key_id, nonce, ciphertext in rows:
            stored_key_id = str(stored_key_id or "").strip()
            if stored_key_id not in keyring:
                raise ValueError(f"Token vault key '{stored_key_id}' not present in keyring")

            token_text = _decrypt_with_key(keyring[stored_key_id], nonce, ciphertext, str(user_id_val), provider_norm)
            new_nonce, new_ciphertext = _encrypt_with_key(
                keyring[active_key_id],
                token_text,
                str(user_id_val),
                provider_norm,
            )
            session.execute(
                text(
                    "UPDATE user_tokens SET "
                    "encryption_key_id = :kid, "
                    "encryption_alg = :alg, "
                    "nonce = :nonce, "
                    "ciphertext = :ciphertext, "
                    "token_fingerprint = :fp, "
                    "updated_at = NOW() "
                    "WHERE user_id = :u AND provider = :p"
                ),
                {
                    "kid": active_key_id,
                    "alg": ENCRYPTION_ALG,
                    "nonce": new_nonce,
                    "ciphertext": new_ciphertext,
                    "fp": _fingerprint_token(token_text),
                    "u": str(user_id_val),
                    "p": provider_norm,
                },
            )
            updated += 1

    return updated
