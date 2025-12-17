"""Add performance indexes for query optimization

Revision ID: 20241217_perf_indexes
Revises: 8119cb1b5f82
Create Date: 2024-12-17

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '20241217_perf_indexes'
down_revision: Union[str, None] = '8119cb1b5f82'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Performance indexes for common query patterns

    # Deal queries - frequently filtered by round_type and is_lead_confirmed
    op.create_index(
        'idx_deals_created_at_desc',
        'deals',
        [sa.text('created_at DESC')],
        if_not_exists=True
    )
    op.create_index(
        'idx_deals_round_type',
        'deals',
        ['round_type'],
        if_not_exists=True
    )
    op.create_index(
        'idx_deals_is_lead_confirmed',
        'deals',
        ['is_lead_confirmed'],
        if_not_exists=True
    )
    op.create_index(
        'idx_deals_composite',
        'deals',
        ['round_type', 'is_lead_confirmed', sa.text('created_at DESC')],
        if_not_exists=True
    )

    # DealInvestor - join table frequently filtered by fund_id and is_lead
    op.create_index(
        'idx_deal_investors_fund_id',
        'deal_investors',
        ['fund_id'],
        if_not_exists=True
    )
    op.create_index(
        'idx_deal_investors_is_lead',
        'deal_investors',
        ['is_lead'],
        if_not_exists=True
    )
    op.create_index(
        'idx_deal_investors_composite',
        'deal_investors',
        ['fund_id', 'is_lead', 'deal_id'],
        if_not_exists=True
    )

    # Articles - deduplication by URL
    op.create_index(
        'idx_articles_url_unique',
        'articles',
        ['url'],
        unique=True,
        if_not_exists=True
    )
    op.create_index(
        'idx_articles_source_fund',
        'articles',
        ['source_fund_slug'],
        if_not_exists=True
    )

    # Portfolio companies - name lookups
    op.create_index(
        'idx_portfolio_companies_name_lower',
        'portfolio_companies',
        [sa.text('lower(name)')],
        if_not_exists=True
    )

    # Funds - slug lookups (should be unique)
    op.create_index(
        'idx_funds_slug_unique',
        'funds',
        ['slug'],
        unique=True,
        if_not_exists=True
    )


def downgrade() -> None:
    # Remove indexes in reverse order
    op.drop_index('idx_funds_slug_unique', table_name='funds', if_exists=True)
    op.drop_index('idx_portfolio_companies_name_lower', table_name='portfolio_companies', if_exists=True)
    op.drop_index('idx_articles_source_fund', table_name='articles', if_exists=True)
    op.drop_index('idx_articles_url_unique', table_name='articles', if_exists=True)
    op.drop_index('idx_deal_investors_composite', table_name='deal_investors', if_exists=True)
    op.drop_index('idx_deal_investors_is_lead', table_name='deal_investors', if_exists=True)
    op.drop_index('idx_deal_investors_fund_id', table_name='deal_investors', if_exists=True)
    op.drop_index('idx_deals_composite', table_name='deals', if_exists=True)
    op.drop_index('idx_deals_is_lead_confirmed', table_name='deals', if_exists=True)
    op.drop_index('idx_deals_round_type', table_name='deals', if_exists=True)
    op.drop_index('idx_deals_created_at_desc', table_name='deals', if_exists=True)
