"""Add token_usage table for tracking Claude API token consumption

Revision ID: 20241222_token_usage
Revises: 20241222_portfolio_snapshots
Create Date: 2024-12-22

Tracks input/output tokens per Claude API call to enable:
- Cost breakdown by source (brave_search, techcrunch, a16z, etc.)
- Daily/weekly/monthly usage trends
- Identifying which sources consume the most tokens
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '20241222_token_usage'
down_revision: Union[str, None] = '20241222_portfolio_snapshots'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Create token_usage table
    op.create_table(
        'token_usage',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('timestamp', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column('source_name', sa.String(100), nullable=False),
        sa.Column('scan_id', sa.String(100), nullable=True),
        sa.Column('article_url', sa.String(2000), nullable=True),
        sa.Column('input_tokens', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('output_tokens', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('cache_read_tokens', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('cache_write_tokens', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('model', sa.String(100), nullable=False, server_default='claude-3-5-haiku-20241022'),
        sa.Column('estimated_cost_usd', sa.Float(), nullable=False, server_default='0.0'),
        sa.PrimaryKeyConstraint('id')
    )

    # Index on timestamp for date range queries
    op.create_index(
        'idx_token_usage_timestamp',
        'token_usage',
        [sa.text('timestamp DESC')],
        if_not_exists=True
    )

    # Index on source_name for filtering by source
    op.create_index(
        'idx_token_usage_source',
        'token_usage',
        ['source_name'],
        if_not_exists=True
    )

    # Composite index for common query: source + timestamp
    op.create_index(
        'idx_token_usage_source_timestamp',
        'token_usage',
        ['source_name', sa.text('timestamp DESC')],
        if_not_exists=True
    )


def downgrade() -> None:
    op.drop_index('idx_token_usage_source_timestamp', table_name='token_usage', if_exists=True)
    op.drop_index('idx_token_usage_source', table_name='token_usage', if_exists=True)
    op.drop_index('idx_token_usage_timestamp', table_name='token_usage', if_exists=True)
    op.drop_table('token_usage')
