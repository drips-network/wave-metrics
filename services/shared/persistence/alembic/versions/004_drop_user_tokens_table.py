"""
Drop user_tokens table

Revision ID: 004
Revises: 003
Create Date: 2026-01-07
"""

from alembic import op


revision = "004"
down_revision = "003"
branch_labels = None
depends_on = None


def upgrade():
    # Schema snapshot already drops user_tokens for new databases; keep migration for existing installs
    op.execute("DROP TABLE IF EXISTS user_tokens")


def downgrade():
    raise RuntimeError("Downgrades are not supported for wave-metrics")
