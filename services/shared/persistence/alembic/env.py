"""
Alembic environment configuration

Wave Metrics uses SQL-file-first migrations:
- `services/shared/schema.sql` is the authoritative schema snapshot
- Alembic tracks applied revisions and runs `schema.sql` under version control
"""

import os
import sys
from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool


config = context.config


def _database_url_from_env():
    database_url = os.environ.get("DATABASE_URL")
    if database_url:
        return database_url

    pg_user = os.environ.get("PGUSER")
    pg_host = os.environ.get("PGHOST")
    if pg_host and pg_user:
        pg_pass = os.environ.get("PGPASSWORD", "")
        pg_port = os.environ.get("PGPORT", "5432")
        pg_db = os.environ.get("PGDATABASE", "postgres")
        return f"postgresql+psycopg2://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}"

    return "postgresql+psycopg2://postgres:postgres@postgres:5432/wave-metrics"


def _configure_sys_path():
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../.."))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)


def run_migrations_offline():
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online():
    connectable = engine_from_config(
        config.get_section(config.config_ini_section),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(connection=connection)

        with context.begin_transaction():
            context.run_migrations()


_configure_sys_path()
existing_url = config.get_main_option("sqlalchemy.url")
if not existing_url:
    config.set_main_option("sqlalchemy.url", _database_url_from_env())

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
