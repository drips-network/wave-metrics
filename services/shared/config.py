import os
import socket
from urllib.parse import urlparse, urlunparse


def env_or(key, default):
    """
    Read environment variable with a default

    Args:
        key (str): Environment variable name
        default (str): Default value when not set

    Returns:
        str value
    """
    return os.getenv(key, default)


def env_int_or_none(key, default=None):
    """
    Read an optional integer environment variable

    Args:
        key (str): Environment variable name
        default (int): Value to use when env var is unset/blank

    Returns:
        int or None

    Raises:
        ValueError: When the env var is set but not an integer
    """
    value = os.getenv(key)
    if value is None or value == "":
        return default
    return int(value)


def _running_in_docker() -> bool:
    return os.path.exists("/.dockerenv")


def _hostname_resolves(hostname) -> bool:
    """
    Check whether a hostname resolves via DNS

    Args:
        hostname (str): Hostname to resolve

    Returns:
        bool
    """
    try:
        socket.getaddrinfo(hostname, None)
        return True
    except socket.gaierror:
        return False


def _normalize_local_service_url(url, docker_hostname, localhost_hostname="localhost"):
    """
    Normalize Docker service hostnames for host-run processes

    Many local dev setups use `docker compose` with service hostnames like
    `postgres` / `redis` in `.env` for containers, but also run Python locally
    against published ports on `localhost`

    Args:
        url (str): URL string
        docker_hostname (str): Docker service hostname to replace
        localhost_hostname (str): Replacement hostname

    Returns:
        str normalized URL
    """
    if not url:
        return url

    if _running_in_docker() and _hostname_resolves(docker_hostname):
        return url

    parsed = urlparse(url)
    hostname = (parsed.hostname or "").lower()
    if hostname != str(docker_hostname).lower():
        return url

    netloc = parsed.netloc
    if not netloc:
        return url

    userinfo = ""
    hostport = netloc
    if "@" in netloc:
        userinfo, hostport = netloc.rsplit("@", 1)

    host, sep, port = hostport.partition(":")
    if host.lower() != str(docker_hostname).lower():
        return url

    new_hostport = f"{localhost_hostname}{sep}{port}" if sep else str(localhost_hostname)
    new_netloc = f"{userinfo}@{new_hostport}" if userinfo else new_hostport

    updated = parsed._replace(netloc=new_netloc)
    return urlunparse(updated)


def validate_config():
    """
    Validate critical configuration values

    Raises:
        ValueError: When configuration is invalid
    """
    errors = []

    if not DATABASE_URL:
        errors.append("DATABASE_URL is required")
    if not REDIS_URL:
        errors.append("REDIS_URL is required")

    if CACHE_TTL_SECONDS < 0:
        errors.append("CACHE_TTL_SECONDS must be >= 0")

    if DB_POOL_SIZE is not None and DB_POOL_SIZE < 1:
        errors.append("DB_POOL_SIZE must be >= 1")
    if DB_MAX_OVERFLOW is not None and DB_MAX_OVERFLOW < 0:
        errors.append("DB_MAX_OVERFLOW must be >= 0")
    if DB_POOL_TIMEOUT_SECONDS is not None and DB_POOL_TIMEOUT_SECONDS < 1:
        errors.append("DB_POOL_TIMEOUT_SECONDS must be >= 1")
    if DB_POOL_RECYCLE_SECONDS is not None and DB_POOL_RECYCLE_SECONDS < 0:
        errors.append("DB_POOL_RECYCLE_SECONDS must be >= 0")

    if GH_MAX_RETRIES < 0:
        errors.append("GH_MAX_RETRIES must be >= 0")
    if GH_CONCURRENCY_PER_TOKEN < 1:
        errors.append("GH_CONCURRENCY_PER_TOKEN must be >= 1")
    if GH_BACKOFF_BASE_SECONDS < 0 or GH_BACKOFF_CAP_SECONDS < 0:
        errors.append("GH_BACKOFF_BASE_SECONDS and GH_BACKOFF_CAP_SECONDS must be >= 0")

    if GH_SEMAPHORE_TTL_SECONDS < 30:
        errors.append("GH_SEMAPHORE_TTL_SECONDS must be >= 30")
    if GH_SEMAPHORE_ACQUIRE_TIMEOUT_SECONDS < 1:
        errors.append("GH_SEMAPHORE_ACQUIRE_TIMEOUT_SECONDS must be >= 1")
    if GH_COOLDOWN_MAX_WAIT_SECONDS < 0:
        errors.append("GH_COOLDOWN_MAX_WAIT_SECONDS must be >= 0")

    if USER_LOCK_TTL_SECONDS < 1:
        errors.append("USER_LOCK_TTL_SECONDS must be >= 1")
    if USER_LOCK_WAIT_TIMEOUT_SECONDS < 0:
        errors.append("USER_LOCK_WAIT_TIMEOUT_SECONDS must be >= 0")

    if REPO_LANGUAGE_CACHE_MAX_REPOS < 1:
        errors.append("REPO_LANGUAGE_CACHE_MAX_REPOS must be >= 1")

    if HEALTH_CHECK_BROKER_TIMEOUT_SECONDS < 1:
        errors.append("HEALTH_CHECK_BROKER_TIMEOUT_SECONDS must be >= 1")

    if RUNNING_JOB_STALE_SECONDS < 1:
        errors.append("RUNNING_JOB_STALE_SECONDS must be >= 1")
    if GITHUB_LOGIN_RESERVATION_TTL_SECONDS < 60:
        errors.append("GITHUB_LOGIN_RESERVATION_TTL_SECONDS must be >= 60")

    if TOKEN_REF_TTL_SECONDS_NORMAL < 1:
        errors.append("TOKEN_REF_TTL_SECONDS_NORMAL must be >= 1")
    if TOKEN_REF_TTL_SECONDS_BULK < 1:
        errors.append("TOKEN_REF_TTL_SECONDS_BULK must be >= 1")

    token_ref_keys_json = str(TOKEN_REF_KEYS_JSON or "").strip()
    token_ref_active_key_id = str(TOKEN_REF_ACTIVE_KEY_ID or "").strip()

    if not token_ref_keys_json:
        errors.append("TOKEN_REF_KEYS_JSON is required")
    if not token_ref_active_key_id:
        errors.append("TOKEN_REF_ACTIVE_KEY_ID is required")
    if token_ref_keys_json and token_ref_active_key_id:
        try:
            import base64
            import json

            keyring = json.loads(token_ref_keys_json)
            if not isinstance(keyring, dict):
                errors.append("TOKEN_REF_KEYS_JSON must be a JSON object mapping key IDs to base64 keys")
            else:
                if token_ref_active_key_id not in keyring:
                    errors.append(f"TOKEN_REF_ACTIVE_KEY_ID '{token_ref_active_key_id}' not in keyring")

                for key_id, key_b64 in keyring.items():
                    if not str(key_id or "").strip():
                        errors.append("TOKEN_REF_KEYS_JSON contains an empty key id")
                        continue
                    try:
                        key_bytes = base64.b64decode(str(key_b64), validate=True)
                    except Exception as exc:
                        errors.append(f"Token ref key '{key_id}' is not valid base64: {exc}")
                        continue

                    if len(key_bytes) != 32:
                        errors.append(f"Token ref key '{key_id}' must be 32 bytes (got {len(key_bytes)})")
        except json.JSONDecodeError as exc:
            errors.append(f"TOKEN_REF_KEYS_JSON is not valid JSON: {exc}")
        except Exception as exc:
            errors.append(f"TOKEN_REF_KEYS_JSON validation failed: {exc}")

    if TOKEN_VAULT_KEYS_JSON or TOKEN_VAULT_ACTIVE_KEY_ID:
        if not TOKEN_VAULT_KEYS_JSON:
            errors.append("TOKEN_VAULT_ACTIVE_KEY_ID requires TOKEN_VAULT_KEYS_JSON")
        if not TOKEN_VAULT_ACTIVE_KEY_ID:
            errors.append("TOKEN_VAULT_KEYS_JSON requires TOKEN_VAULT_ACTIVE_KEY_ID")
        if TOKEN_VAULT_KEYS_JSON and TOKEN_VAULT_ACTIVE_KEY_ID:
            try:
                import base64
                import json

                keyring = json.loads(TOKEN_VAULT_KEYS_JSON)
                if not isinstance(keyring, dict):
                    errors.append("TOKEN_VAULT_KEYS_JSON must be a JSON object mapping key IDs to base64 keys")
                else:
                    if TOKEN_VAULT_ACTIVE_KEY_ID not in keyring:
                        errors.append(
                            f"TOKEN_VAULT_ACTIVE_KEY_ID '{TOKEN_VAULT_ACTIVE_KEY_ID}' not in keyring"
                        )
                    for key_id, key_b64 in keyring.items():
                        try:
                            key_bytes = base64.b64decode(str(key_b64), validate=True)
                        except Exception as exc:
                            errors.append(f"Token vault key '{key_id}' is not valid base64: {exc}")
                            continue

                        if len(key_bytes) != 32:
                            errors.append(
                                f"Token vault key '{key_id}' must be 32 bytes (got {len(key_bytes)})"
                            )
            except json.JSONDecodeError as exc:
                errors.append(f"TOKEN_VAULT_KEYS_JSON is not valid JSON: {exc}")
            except Exception as exc:
                errors.append(f"TOKEN_VAULT_KEYS_JSON validation failed: {exc}")

    if errors:
        raise ValueError("Invalid configuration: " + "; ".join(errors))


_RAW_DATABASE_URL = env_or("DATABASE_URL", "postgresql+psycopg2://postgres:postgres@localhost:5432/wave-metrics")
DATABASE_URL = _normalize_local_service_url(_RAW_DATABASE_URL, docker_hostname="postgres")

_RAW_REDIS_URL = env_or("REDIS_URL", "redis://localhost:6379/0")
REDIS_URL = _normalize_local_service_url(_RAW_REDIS_URL, docker_hostname="redis")

SERVICE_VERSION = env_or("SERVICE_VERSION", "0.1.0")
RUN_DB_MIGRATIONS = env_or("RUN_DB_MIGRATIONS", "1") == "1"
CACHE_TTL_SECONDS = int(env_or("CACHE_TTL_SECONDS", "180"))
API_AUTH_TOKEN = env_or("API_AUTH_TOKEN", "")
POPULATION_BASELINE_ID = env_or("POPULATION_BASELINE_ID", "")

# Token ref encryption configuration
TOKEN_REF_KEYS_JSON = env_or("TOKEN_REF_KEYS_JSON", "")
TOKEN_REF_ACTIVE_KEY_ID = env_or("TOKEN_REF_ACTIVE_KEY_ID", "")
TOKEN_REF_TTL_SECONDS_NORMAL = int(env_or("TOKEN_REF_TTL_SECONDS_NORMAL", "900"))
TOKEN_REF_TTL_SECONDS_BULK = int(env_or("TOKEN_REF_TTL_SECONDS_BULK", "86400"))

# Token vault configuration
TOKEN_VAULT_KEYS_JSON = env_or("TOKEN_VAULT_KEYS_JSON", "")
TOKEN_VAULT_ACTIVE_KEY_ID = env_or("TOKEN_VAULT_ACTIVE_KEY_ID", "")
TOKEN_VAULT_ENABLED = bool(TOKEN_VAULT_KEYS_JSON and TOKEN_VAULT_ACTIVE_KEY_ID)

# DB pool knobs (optional; when unset, SQLAlchemy defaults apply)
DB_POOL_SIZE = env_int_or_none("DB_POOL_SIZE")
DB_MAX_OVERFLOW = env_int_or_none("DB_MAX_OVERFLOW")
DB_POOL_TIMEOUT_SECONDS = env_int_or_none("DB_POOL_TIMEOUT_SECONDS")
DB_POOL_RECYCLE_SECONDS = env_int_or_none("DB_POOL_RECYCLE_SECONDS")

# GitHub throttling knobs
GH_MAX_RETRIES = int(env_or("GH_MAX_RETRIES", "2"))
GH_CONCURRENCY_PER_TOKEN = int(env_or("GH_CONCURRENCY_PER_TOKEN", "2"))
GH_BACKOFF_BASE_SECONDS = int(env_or("GH_BACKOFF_BASE_SECONDS", "5"))
GH_BACKOFF_CAP_SECONDS = int(env_or("GH_BACKOFF_CAP_SECONDS", "60"))
GH_REDIS_PREFIX = env_or("GH_REDIS_PREFIX", "gh:")
GH_SEMAPHORE_TTL_SECONDS = int(env_or("GH_SEMAPHORE_TTL_SECONDS", "300"))
GH_SEMAPHORE_ACQUIRE_TIMEOUT_SECONDS = int(env_or("GH_SEMAPHORE_ACQUIRE_TIMEOUT_SECONDS", "60"))
GH_COOLDOWN_MAX_WAIT_SECONDS = int(env_or("GH_COOLDOWN_MAX_WAIT_SECONDS", "600"))

# GitHub token ref storage (Redis)
TOKEN_REF_KEY_PREFIX = env_or("TOKEN_REF_KEY_PREFIX", f"{GH_REDIS_PREFIX}token_ref:")

# Per-user sync lock
USER_LOCK_TTL_SECONDS = int(env_or("USER_LOCK_TTL_SECONDS", "900"))
USER_LOCK_WAIT_TIMEOUT_SECONDS = int(env_or("USER_LOCK_WAIT_TIMEOUT_SECONDS", "0"))

# Ingestion cache knobs
REPO_LANGUAGE_CACHE_MAX_REPOS = int(env_or("REPO_LANGUAGE_CACHE_MAX_REPOS", "500"))

# Jobs + login reservation knobs
RUNNING_JOB_STALE_SECONDS = int(env_or("RUNNING_JOB_STALE_SECONDS", "3600"))
GITHUB_LOGIN_RESERVATION_TTL_SECONDS = int(env_or("GITHUB_LOGIN_RESERVATION_TTL_SECONDS", "86400"))

# Health check knobs
HEALTH_CHECK_BROKER = env_or("HEALTH_CHECK_BROKER", "0") == "1"
HEALTH_CHECK_BROKER_TIMEOUT_SECONDS = int(env_or("HEALTH_CHECK_BROKER_TIMEOUT_SECONDS", "2"))
