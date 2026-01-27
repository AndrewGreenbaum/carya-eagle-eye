"""Add last_heartbeat column to scan_jobs for stuck job detection

Revision ID: 20260127_scan_job_heartbeat
Revises: 20260126_composite_indexes
Create Date: 2026-01-27

FIX 2026-01: Enables detection of silently crashed scan jobs.

The heartbeat pattern works as follows:
1. Running scans update `last_heartbeat` every 30 seconds
2. StuckScanMonitor checks for jobs where status='running' AND last_heartbeat > 2 minutes old
3. Stuck jobs are marked as 'failed' with descriptive error message

This catches failure modes where the exception handler itself fails:
- OOM kills (SIGKILL)
- DB pool exhaustion preventing status update
- Process crash/segfault
- Railway container restarts
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import TIMESTAMP

# revision identifiers, used by Alembic.
revision = '20260127_scan_job_heartbeat'
down_revision = '20260126_composite_indexes'
branch_labels = None
depends_on = None


def upgrade():
    # Add last_heartbeat column with timezone support
    # Default to NOW() so existing running jobs get a heartbeat
    op.add_column(
        'scan_jobs',
        sa.Column(
            'last_heartbeat',
            TIMESTAMP(timezone=True),
            nullable=True,
            server_default=sa.func.now()
        )
    )

    # Index for stuck job detection query:
    # SELECT * FROM scan_jobs WHERE status = 'running' AND last_heartbeat < NOW() - INTERVAL '2 minutes'
    op.create_index(
        'idx_scan_jobs_stuck_detection',
        'scan_jobs',
        ['status', 'last_heartbeat'],
        postgresql_using='btree'
    )


def downgrade():
    op.drop_index('idx_scan_jobs_stuck_detection', table_name='scan_jobs')
    op.drop_column('scan_jobs', 'last_heartbeat')
