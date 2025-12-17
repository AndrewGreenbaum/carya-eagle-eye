"""Add feedback table for user flags and suggestions

Revision ID: 20241222_feedback
Revises: 20241222_dedup_indexes
Create Date: 2024-12-22

Replaces Google Sheets integration with PostgreSQL storage.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '20241222_feedback'
down_revision: Union[str, None] = '20241222_dedup_indexes'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Create feedback table for flags and suggestions
    op.create_table(
        'feedback',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('feedback_type', sa.String(20), nullable=False),  # 'flag' or 'suggestion'
        sa.Column('company_name', sa.String(255), nullable=False),
        sa.Column('deal_id', sa.Integer(), nullable=True),
        sa.Column('reason', sa.Text(), nullable=True),
        sa.Column('suggestion_type', sa.String(50), nullable=True),  # 'missing_company', 'error', 'other'
        sa.Column('source_url', sa.Text(), nullable=True),
        sa.Column('reporter_email', sa.String(255), nullable=True),
        sa.Column('reviewed', sa.Boolean(), nullable=False, server_default='false'),
        sa.Column('reviewed_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        sa.ForeignKeyConstraint(['deal_id'], ['deals.id'], ondelete='SET NULL'),
    )

    # Index on feedback_type for filtering
    op.create_index(
        'idx_feedback_type',
        'feedback',
        ['feedback_type'],
        if_not_exists=True
    )

    # Index on reviewed for filtering unreviewed items
    op.create_index(
        'idx_feedback_reviewed',
        'feedback',
        ['reviewed'],
        if_not_exists=True
    )

    # Index on created_at for sorting by date
    op.create_index(
        'idx_feedback_created_at',
        'feedback',
        [sa.text('created_at DESC')],
        if_not_exists=True
    )


def downgrade() -> None:
    op.drop_index('idx_feedback_created_at', table_name='feedback', if_exists=True)
    op.drop_index('idx_feedback_reviewed', table_name='feedback', if_exists=True)
    op.drop_index('idx_feedback_type', table_name='feedback', if_exists=True)
    op.drop_table('feedback')
