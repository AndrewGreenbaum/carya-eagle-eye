"""Add stealth_signals table for pre-funding detection

Revision ID: 20260103_stealth_signals
Revises: 20260103_amount_review
Create Date: 2026-01-03

Stores pre-funding signals from early detection scrapers:
- hackernews: Launch HN posts
- ycombinator: Demo Day companies
- github_trending: Trending dev tools
- linkedin_jobs: Stealth startup hiring
- delaware_corps: New tech incorporations

These sources previously produced 0 deals but wasted ~$44/month in LLM calls.
Now they go through rule-based scoring instead of Claude extraction.
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

# revision identifiers, used by Alembic.
revision = '20260103_stealth_signals'
down_revision = '20260103_amount_review'
branch_labels = None
depends_on = None


def upgrade():
    # Create stealth_signals table
    op.create_table(
        'stealth_signals',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('company_name', sa.String(200), nullable=False),
        sa.Column('source', sa.String(50), nullable=False),
        sa.Column('source_url', sa.Text(), nullable=False),
        sa.Column('score', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('signals', JSONB(), nullable=False, server_default='{}'),
        sa.Column('metadata_json', JSONB(), nullable=False, server_default='{}'),
        sa.Column('spotted_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column('dismissed', sa.Boolean(), nullable=False, server_default='false'),
        sa.Column('converted_deal_id', sa.Integer(), sa.ForeignKey('deals.id'), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint('id')
    )

    # Indexes for common queries
    op.create_index('ix_stealth_signals_source', 'stealth_signals', ['source'])
    op.create_index('ix_stealth_signals_score', 'stealth_signals', ['score'])
    op.create_index('ix_stealth_signals_spotted', 'stealth_signals', ['spotted_at'])
    op.create_index('ix_stealth_signals_dismissed', 'stealth_signals', ['dismissed'])

    # Unique constraint to prevent duplicate company+source entries
    op.create_index(
        'ix_stealth_signals_company_source',
        'stealth_signals',
        ['company_name', 'source'],
        unique=True
    )


def downgrade():
    op.drop_index('ix_stealth_signals_company_source', table_name='stealth_signals')
    op.drop_index('ix_stealth_signals_dismissed', table_name='stealth_signals')
    op.drop_index('ix_stealth_signals_spotted', table_name='stealth_signals')
    op.drop_index('ix_stealth_signals_score', table_name='stealth_signals')
    op.drop_index('ix_stealth_signals_source', table_name='stealth_signals')
    op.drop_table('stealth_signals')
