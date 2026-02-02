"""add_brave_api_usage_tracking

Revision ID: b4a64bf65e15
Revises: 20260127_scan_job_heartbeat
Create Date: 2026-02-02 11:25:42.477153

"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'b4a64bf65e15'
down_revision: Union[str, None] = '20260127_scan_job_heartbeat'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Create brave_api_usage table for cost monitoring
    op.create_table(
        'brave_api_usage',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('usage_date', sa.Date(), nullable=False),
        sa.Column('query_type', sa.String(length=50), nullable=False),
        sa.Column('query_count', sa.Integer(), nullable=False),
        sa.Column('estimated_cost', sa.Float(), nullable=False),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint('id')
    )

    # Add index on usage_date for efficient daily/weekly queries
    op.create_index('ix_brave_api_usage_usage_date', 'brave_api_usage', ['usage_date'])


def downgrade() -> None:
    op.drop_index('ix_brave_api_usage_usage_date', table_name='brave_api_usage')
    op.drop_table('brave_api_usage')
