"""Add tracker_columns table for configurable Kanban columns

Revision ID: 20260102_tracker_columns
Revises: 20241225_add_scan_jobs
Create Date: 2026-01-02
"""
from typing import Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '20260102_tracker_columns'
down_revision: Union[str, None] = '20241225_add_scan_jobs'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Create tracker_columns table for configurable Kanban columns
    op.create_table(
        'tracker_columns',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('slug', sa.String(50), nullable=False),
        sa.Column('display_name', sa.String(100), nullable=False),
        sa.Column('color', sa.String(20), nullable=False, server_default='slate'),
        sa.Column('position', sa.Integer(), nullable=False),
        sa.Column('is_active', sa.Boolean(), nullable=False, server_default='true'),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint('id')
    )

    # Unique constraint on slug
    op.create_index('ix_tracker_columns_slug', 'tracker_columns', ['slug'], unique=True)

    # Index on position for ordering
    op.create_index('ix_tracker_columns_position', 'tracker_columns', ['position'], unique=False)

    # Seed with the 6 default columns
    op.execute("""
        INSERT INTO tracker_columns (slug, display_name, color, position) VALUES
        ('watching', 'Watching', 'slate', 0),
        ('reached_out', 'Reached Out', 'blue', 1),
        ('in_conversation', 'In Conversation', 'amber', 2),
        ('closing_spv', 'Closing SPV', 'emerald', 3),
        ('spv_complete', 'SPV Complete', 'green', 4),
        ('spv_rejected', 'SPV Rejected', 'red', 5)
    """)


def downgrade() -> None:
    op.drop_index('ix_tracker_columns_position', table_name='tracker_columns')
    op.drop_index('ix_tracker_columns_slug', table_name='tracker_columns')
    op.drop_table('tracker_columns')
