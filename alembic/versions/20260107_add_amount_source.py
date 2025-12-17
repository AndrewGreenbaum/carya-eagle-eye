"""Add amount_source column to deals table

Revision ID: 20260107_amount_source
Revises: 20260103_stealth_signals
Create Date: 2026-01-07

Tracks the source of deal amount for priority handling:
- sec_form_d: Official SEC Form D filing (highest priority)
- article: Extracted from news article by LLM
- crunchbase: Imported from Crunchbase Pro CSV

SEC Form D amounts are exact legal filings and should override
LLM-extracted approximations (e.g., $47,500,000 vs "$50M").
"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '20260107_amount_source'
down_revision = '20260103_stealth_signals'
branch_labels = None
depends_on = None


def upgrade():
    # Add amount_source column to track where amount came from
    op.add_column('deals', sa.Column(
        'amount_source',
        sa.String(),
        nullable=True
    ))


def downgrade():
    op.drop_column('deals', 'amount_source')
