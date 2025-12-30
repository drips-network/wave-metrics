"""
Add partition_key to sync_jobs

Revision ID: 003
Revises: 002
Create Date: 2025-12-30
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


revision = "003"
down_revision = "002"
branch_labels = None
depends_on = None


def _column_exists(connection, table_name, column_name) -> bool:
    inspector = inspect(connection)
    columns = inspector.get_columns(table_name)
    return any(str(col.get("name")) == str(column_name) for col in columns)


def _table_exists(connection, table_name) -> bool:
    inspector = inspect(connection)
    return str(table_name) in {str(name) for name in inspector.get_table_names()}


def _index_exists(connection, table_name, index_name) -> bool:
    inspector = inspect(connection)
    indexes = inspector.get_indexes(table_name)
    return any(str(idx.get("name")) == str(index_name) for idx in indexes)


def upgrade():
    connection = op.get_bind()

    if connection.dialect.name == "postgresql":
        op.execute(
            """
            DO $$
            BEGIN
                IF to_regclass('sync_jobs') IS NULL THEN
                    RETURN;
                END IF;

                IF NOT EXISTS (
                    SELECT 1
                    FROM information_schema.columns
                    WHERE table_schema = current_schema()
                      AND table_name = 'sync_jobs'
                      AND column_name = 'partition_key'
                ) THEN
                    ALTER TABLE sync_jobs ADD COLUMN partition_key TEXT;
                END IF;
            END $$;
            """
        )
        op.execute(
            "CREATE INDEX IF NOT EXISTS idx_sync_jobs_triggered_partition_created_at "
            "ON sync_jobs (triggered_by, partition_key, created_at DESC)"
        )
        return

    if not _table_exists(connection, "sync_jobs"):
        return

    if not _column_exists(connection, "sync_jobs", "partition_key"):
        op.add_column("sync_jobs", sa.Column("partition_key", sa.Text(), nullable=True))

    if not _index_exists(connection, "sync_jobs", "idx_sync_jobs_triggered_partition_created_at"):
        op.create_index(
            "idx_sync_jobs_triggered_partition_created_at",
            "sync_jobs",
            ["triggered_by", "partition_key", "created_at"],
            unique=False,
        )


def downgrade():
    raise RuntimeError("Downgrades are not supported for wave-metrics")
