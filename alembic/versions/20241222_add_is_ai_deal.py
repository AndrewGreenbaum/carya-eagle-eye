"""Add is_ai_deal field for tracking all AI deals (enterprise + consumer)

Revision ID: 20241222_is_ai_deal
Revises: 20241222_token_usage
Create Date: 2024-12-22

Adds is_ai_deal boolean field to deals table to track ALL AI deals,
not just enterprise AI. This enables filtering by:
- All AI deals (is_ai_deal=True)
- Enterprise AI only (is_enterprise_ai=True)
- Consumer AI (is_ai_deal=True AND is_enterprise_ai=False)
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '20241222_is_ai_deal'
down_revision: Union[str, None] = '20241222_token_usage'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add is_ai_deal column
    op.add_column('deals', sa.Column('is_ai_deal', sa.Boolean(), nullable=False, server_default='false'))

    # Add index for is_ai_deal filtering
    op.create_index(
        'idx_deals_is_ai_deal',
        'deals',
        ['is_ai_deal'],
        if_not_exists=True
    )

    # Backfill: Set is_ai_deal=True for all existing enterprise AI deals
    # (Consumer AI deals from before this migration will remain False until re-extracted)
    op.execute("""
        UPDATE deals
        SET is_ai_deal = TRUE
        WHERE is_enterprise_ai = TRUE
    """)


def downgrade() -> None:
    # Remove index
    op.drop_index('idx_deals_is_ai_deal', table_name='deals', if_exists=True)

    # Remove column
    op.drop_column('deals', 'is_ai_deal')
