"""Add lead_evidence_weak column to deals table

Revision ID: 20241225_lead_evidence_weak
Revises: 20241224_verification
Create Date: 2025-12-25
"""
from alembic import op
import sqlalchemy as sa
from typing import Union


# revision identifiers, used by Alembic.
revision: str = '20241225_lead_evidence_weak'
down_revision: Union[str, None] = '20241224_verification'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Add lead_evidence_weak column to deals table
    # True if snippet lacks "led by" language but Claude determined lead status
    op.add_column('deals', sa.Column('lead_evidence_weak', sa.Boolean(), nullable=False, server_default='false'))


def downgrade() -> None:
    op.drop_column('deals', 'lead_evidence_weak')
