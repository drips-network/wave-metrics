"""
Allow SKIPPED in sync_jobs.status

Revision ID: 002
Revises: 001
Create Date: 2025-12-20
"""

from alembic import op


revision = "002"
down_revision = "001"
branch_labels = None
depends_on = None


def upgrade():
    op.execute(
        """
        DO $$
        DECLARE
            constraint_name text;
        BEGIN
            IF to_regclass('sync_jobs') IS NULL THEN
                RETURN;
            END IF;

            FOR constraint_name IN
                SELECT con.conname
                FROM pg_constraint con
                JOIN pg_class rel ON rel.oid = con.conrelid
                JOIN pg_namespace nsp ON nsp.oid = rel.relnamespace
                WHERE rel.relname = 'sync_jobs'
                  AND nsp.nspname = current_schema()
                  AND con.contype = 'c'
                  AND pg_get_constraintdef(con.oid) ILIKE '%status%'
                  AND pg_get_constraintdef(con.oid) ILIKE '%PENDING%'
            LOOP
                EXECUTE format(
                    'ALTER TABLE %I.sync_jobs DROP CONSTRAINT IF EXISTS %I',
                    current_schema(),
                    constraint_name
                );
            END LOOP;

            ALTER TABLE sync_jobs DROP CONSTRAINT IF EXISTS sync_jobs_status_check;

            ALTER TABLE sync_jobs
                ADD CONSTRAINT sync_jobs_status_check
                CHECK (status IN ('PENDING', 'RUNNING', 'COMPLETED', 'FAILED', 'SKIPPED'));
        END $$;
        """
    )


def downgrade():
    raise RuntimeError("Downgrades are not supported for wave-metrics")

