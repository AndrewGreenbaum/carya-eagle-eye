"""Add indexes for stealth_detections table

Revision ID: 20241220_stealth_indexes
Revises: 20241218_company_links
Create Date: 2024-12-20

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '20241220_stealth_indexes'
down_revision: Union[str, None] = '20241218_company_links'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Index on detected_url for deduplication checks
    op.create_index(
        'idx_stealth_detections_url',
        'stealth_detections',
        ['detected_url'],
        unique=True,
        if_not_exists=True
    )

    # Index on fund_slug for filtering by fund
    op.create_index(
        'idx_stealth_detections_fund_slug',
        'stealth_detections',
        ['fund_slug'],
        if_not_exists=True
    )

    # Index on company_name for lookups
    op.create_index(
        'idx_stealth_detections_company_name',
        'stealth_detections',
        ['company_name'],
        if_not_exists=True
    )

    # Composite index for common query pattern (fund + confirmation status)
    op.create_index(
        'idx_stealth_detections_composite',
        'stealth_detections',
        ['fund_slug', 'is_confirmed', sa.text('detected_at DESC')],
        if_not_exists=True
    )


def downgrade() -> None:
    op.drop_index('idx_stealth_detections_composite', table_name='stealth_detections', if_exists=True)
    op.drop_index('idx_stealth_detections_company_name', table_name='stealth_detections', if_exists=True)
    op.drop_index('idx_stealth_detections_fund_slug', table_name='stealth_detections', if_exists=True)
    op.drop_index('idx_stealth_detections_url', table_name='stealth_detections', if_exists=True)
