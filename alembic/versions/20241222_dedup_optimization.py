"""Add index for deal duplicate detection optimization

Revision ID: 20241222_dedup_indexes
Revises: 20241222_tracker_items
Create Date: 2024-12-22

Fixes: find_duplicate_deal() queries by round_type + date range
without a composite index, causing slow scans.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '20241222_dedup_indexes'
down_revision: Union[str, None] = '20241222_tracker_items'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Composite index for find_duplicate_deal() date range queries
    # Query pattern: WHERE round_type = ? AND announced_date BETWEEN ? AND ?
    op.create_index(
        'idx_deals_announced_date_round',
        'deals',
        ['announced_date', 'round_type'],
        if_not_exists=True
    )

    # Add index on articles.deal_id for JOIN performance
    # Used when linking articles to existing deals
    op.create_index(
        'idx_articles_deal_id',
        'articles',
        ['deal_id'],
        if_not_exists=True
    )


def downgrade() -> None:
    op.drop_index('idx_articles_deal_id', table_name='articles', if_exists=True)
    op.drop_index('idx_deals_announced_date_round', table_name='deals', if_exists=True)
