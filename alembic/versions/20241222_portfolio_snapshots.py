"""Add portfolio_snapshots table for persistent diff detection

Revision ID: 20241222_portfolio_snapshots
Revises: 20241222_dedup_indexes
Create Date: 2024-12-22

Fixes: Portfolio diff stealth detection was broken on Railway because
snapshots were stored in ephemeral /tmp/ directory. After every deploy,
all companies appeared as "new" (false positives).

This migration creates a database table to store snapshots persistently.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '20241222_portfolio_snapshots'
down_revision: Union[str, None] = '20241222_feedback'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Create portfolio_snapshots table
    op.create_table(
        'portfolio_snapshots',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('fund_slug', sa.String(), nullable=False),
        sa.Column('companies_json', sa.Text(), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint('id')
    )

    # Unique index on fund_slug (one snapshot per fund)
    op.create_index(
        'idx_portfolio_snapshots_fund_slug',
        'portfolio_snapshots',
        ['fund_slug'],
        unique=True
    )


def downgrade() -> None:
    op.drop_index('idx_portfolio_snapshots_fund_slug', table_name='portfolio_snapshots')
    op.drop_table('portfolio_snapshots')
