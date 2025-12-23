import logging
import os
from contextlib import contextmanager
from typing import Generator, Iterator

from alembic import command
from alembic.config import Config as AlembicConfig
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker

from .config import (
    DATABASE_URL,
    DB_MAX_OVERFLOW,
    DB_POOL_RECYCLE_SECONDS,
    DB_POOL_SIZE,
    DB_POOL_TIMEOUT_SECONDS,
    RUN_DB_MIGRATIONS,
)


logger = logging.getLogger("database")

# Advisory lock ID for migration coordination across replicas
# Using a fixed hash to ensure all workers contend on the same lock
MIGRATION_LOCK_ID = 0x77617665  # 'wave' in hex

_engine_kwargs = {"pool_pre_ping": True, "future": True}
if DB_POOL_SIZE is not None:
    _engine_kwargs["pool_size"] = int(DB_POOL_SIZE)
if DB_MAX_OVERFLOW is not None:
    _engine_kwargs["max_overflow"] = int(DB_MAX_OVERFLOW)
if DB_POOL_TIMEOUT_SECONDS is not None:
    _engine_kwargs["pool_timeout"] = int(DB_POOL_TIMEOUT_SECONDS)
if DB_POOL_RECYCLE_SECONDS is not None:
    _engine_kwargs["pool_recycle"] = int(DB_POOL_RECYCLE_SECONDS)

ENGINE = create_engine(DATABASE_URL, **_engine_kwargs)
SessionLocal = sessionmaker(bind=ENGINE, autocommit=False, autoflush=False, future=True)

_MIGRATIONS_APPLIED = False


def _build_alembic_config() -> AlembicConfig:
    """
    Build Alembic config with absolute paths

    Returns:
        Alembic Config
    """
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    services_dir = os.path.join(project_root, "services")
    alembic_ini_path = os.path.join(services_dir, "alembic.ini")

    config = AlembicConfig(alembic_ini_path)
    config.set_main_option(
        "script_location",
        os.path.join(services_dir, "shared", "persistence", "alembic"),
    )
    config.set_main_option("sqlalchemy.url", DATABASE_URL)
    return config


def run_migrations_if_needed() -> None:
    """
    Apply Alembic migrations when enabled

    Uses PostgreSQL advisory locks to ensure only one replica runs migrations
    at a time, preventing race conditions in multi-replica deployments

    Args:
        None

    Returns:
        None
    """
    global _MIGRATIONS_APPLIED

    if not RUN_DB_MIGRATIONS:
        logger.debug("RUN_DB_MIGRATIONS is disabled; skipping migrations")
        return

    if _MIGRATIONS_APPLIED:
        logger.debug("Migrations already applied in this process")
        return

    logger.info("Applying database migrations")
    alembic_config = _build_alembic_config()

    try:
        with ENGINE.begin() as conn:
            if ENGINE.dialect.name == "postgresql":
                logger.debug("Acquiring transaction advisory lock %s", MIGRATION_LOCK_ID)
                conn.execute(
                    text("SELECT pg_advisory_xact_lock(:lock_id)"),
                    {"lock_id": MIGRATION_LOCK_ID},
                )

            command.upgrade(alembic_config, "head")

        _MIGRATIONS_APPLIED = True
        logger.info("Database migrations completed successfully")
    except Exception:
        logger.exception("Database migration failed")
        raise


def apply_pending_migrations() -> None:
    """
    Apply database migrations when enabled

    Safe for multi-replica deployments: uses PostgreSQL advisory locks to
    serialize migration execution across concurrent workers

    Args:
        None

    Returns:
        None
    """
    run_migrations_if_needed()


def refresh_metric_views() -> None:
    """
    No-op: metrics eligibility is implemented as a plain view

    Retained for API compatibility with external orchestrators

    Args:
        None

    Returns:
        None
    """
    return


def get_session() -> Generator[Session, None, None]:
    """
    FastAPI dependency that yields a database session

    Args:
        None

    Returns:
        Generator yielding SQLAlchemy Session
    """
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


@contextmanager
def db_session() -> Iterator[Session]:
    """
    Context manager for imperative code paths (workers, scripts)

    Args:
        None

    Returns:
        SQLAlchemy Session
    """
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
