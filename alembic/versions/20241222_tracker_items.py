"""Add tracker_items table for CRM pipeline management

Revision ID: 20241222_tracker_items
Revises: 20241220_stealth_indexes
Create Date: 2024-12-22

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '20241222_tracker_items'
down_revision: Union[str, None] = '20241220_stealth_indexes'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Create tracker_items table for CRM/Kanban pipeline
    op.create_table(
        'tracker_items',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('company_name', sa.String(255), nullable=False),
        sa.Column('round_type', sa.String(50), nullable=True),
        sa.Column('amount', sa.String(100), nullable=True),
        sa.Column('lead_investor', sa.String(255), nullable=True),
        sa.Column('website', sa.String(500), nullable=True),
        sa.Column('status', sa.String(50), nullable=False, server_default='watching'),
        sa.Column('notes', sa.Text(), nullable=True),
        sa.Column('last_contact_date', sa.Date(), nullable=True),
        sa.Column('next_step', sa.Text(), nullable=True),
        sa.Column('position', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('deal_id', sa.Integer(), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        sa.ForeignKeyConstraint(['deal_id'], ['deals.id'], ondelete='SET NULL'),
    )

    # Index on status for filtering by Kanban column
    op.create_index(
        'idx_tracker_items_status',
        'tracker_items',
        ['status'],
        if_not_exists=True
    )

    # Composite index for ordering within each column (status + position)
    op.create_index(
        'idx_tracker_items_status_position',
        'tracker_items',
        ['status', 'position'],
        if_not_exists=True
    )

    # Index on deal_id for lookups and preventing duplicates
    op.create_index(
        'idx_tracker_items_deal_id',
        'tracker_items',
        ['deal_id'],
        if_not_exists=True
    )

    # Index on updated_at for recent activity queries
    op.create_index(
        'idx_tracker_items_updated_at',
        'tracker_items',
        [sa.text('updated_at DESC')],
        if_not_exists=True
    )


def downgrade() -> None:
    op.drop_index('idx_tracker_items_updated_at', table_name='tracker_items', if_exists=True)
    op.drop_index('idx_tracker_items_deal_id', table_name='tracker_items', if_exists=True)
    op.drop_index('idx_tracker_items_status_position', table_name='tracker_items', if_exists=True)
    op.drop_index('idx_tracker_items_status', table_name='tracker_items', if_exists=True)
    op.drop_table('tracker_items')
