"""Add unique constraint on portfolio_companies.name to prevent duplicates

Revision ID: 20260102_company_name_unique
Revises: 20260102_tracker_columns
Create Date: 2026-01-02

This fixes the race condition where concurrent article processing created
duplicate portfolio_companies records. Example: 4 articles about Knight FinTech
created 4 separate companies and 4 separate deals.

The fix:
1. Add UNIQUE constraint on LOWER(name) for case-insensitive uniqueness
2. Update get_or_create_company() to use INSERT ... ON CONFLICT (in storage.py)
"""
from typing import Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '20260102_company_name_unique'
down_revision: Union[str, None] = '20260102_tracker_columns'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Add unique index on LOWER(name) for case-insensitive uniqueness
    # This prevents "Knight FinTech" and "knight fintech" from being separate companies
    op.create_index(
        'uq_portfolio_companies_name_lower',
        'portfolio_companies',
        [sa.text('LOWER(name)')],
        unique=True
    )


def downgrade() -> None:
    op.drop_index('uq_portfolio_companies_name_lower', table_name='portfolio_companies')
