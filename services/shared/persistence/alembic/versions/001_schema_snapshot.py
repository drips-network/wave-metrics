"""
Schema snapshot (authoritative SQL)

Revision ID: 001
Revises:
Create Date: 2025-12-19
"""

from pathlib import Path

from alembic import op


revision = "001"
down_revision = None
branch_labels = None
depends_on = None


def _read_sql(relative_path):
    base_dir = Path(__file__).resolve().parents[3]
    sql_path = base_dir / relative_path
    return sql_path.read_text(encoding="utf-8")


def upgrade():
    connection = op.get_bind()
    connection.exec_driver_sql(_read_sql("schema.sql"))
    connection.exec_driver_sql(_read_sql("materialized_views.sql"))


def downgrade():
    raise RuntimeError("Downgrades are not supported for wave-metrics")
