"""Add enterprise AI classification fields to deals

Revision ID: 20241218_enterprise_ai
Revises: 20241217_perf_indexes
Create Date: 2024-12-18

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '20241218_enterprise_ai'
down_revision: Union[str, None] = '20241217_perf_indexes'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add enterprise AI classification fields to deals table
    op.add_column('deals', sa.Column('enterprise_category', sa.String(), nullable=True))
    op.add_column('deals', sa.Column('is_enterprise_ai', sa.Boolean(), nullable=False, server_default='false'))
    op.add_column('deals', sa.Column('verification_snippet', sa.Text(), nullable=True))
    op.add_column('deals', sa.Column('lead_partner_name', sa.String(), nullable=True))

    # Add index for enterprise AI filtering (common query pattern)
    op.create_index(
        'idx_deals_is_enterprise_ai',
        'deals',
        ['is_enterprise_ai'],
        if_not_exists=True
    )
    op.create_index(
        'idx_deals_enterprise_category',
        'deals',
        ['enterprise_category'],
        if_not_exists=True
    )
    # Composite index for filtering lead + enterprise deals
    op.create_index(
        'idx_deals_enterprise_lead',
        'deals',
        ['is_enterprise_ai', 'is_lead_confirmed', sa.text('created_at DESC')],
        if_not_exists=True
    )


def downgrade() -> None:
    # Remove indexes
    op.drop_index('idx_deals_enterprise_lead', table_name='deals', if_exists=True)
    op.drop_index('idx_deals_enterprise_category', table_name='deals', if_exists=True)
    op.drop_index('idx_deals_is_enterprise_ai', table_name='deals', if_exists=True)

    # Remove columns
    op.drop_column('deals', 'lead_partner_name')
    op.drop_column('deals', 'verification_snippet')
    op.drop_column('deals', 'is_enterprise_ai')
    op.drop_column('deals', 'enterprise_category')
