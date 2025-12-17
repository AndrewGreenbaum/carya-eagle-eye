"""Add company links and founders fields

Revision ID: 20241218_company_links
Revises: 20241218_enterprise_ai
Create Date: 2024-12-18

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '20241218_company_links'
down_revision: Union[str, None] = '20241218_enterprise_ai'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add linkedin_url to portfolio_companies table
    op.add_column('portfolio_companies', sa.Column('linkedin_url', sa.String(), nullable=True))

    # Add founders_json to deals table (stores founder info as JSON)
    op.add_column('deals', sa.Column('founders_json', sa.Text(), nullable=True))


def downgrade() -> None:
    # Remove columns
    op.drop_column('deals', 'founders_json')
    op.drop_column('portfolio_companies', 'linkedin_url')
