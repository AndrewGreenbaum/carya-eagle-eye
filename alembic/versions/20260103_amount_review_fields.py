"""Add amount_needs_review and amount_review_reason columns to deals table

Revision ID: 20260103_amount_review
Revises: 20260103_bigint_amount
Create Date: 2026-01-03

Catches suspicious amounts (e.g., Series A >$100M, market size confusion).
See Agency $150M -> $20M bug fix.
"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '20260103_amount_review'
down_revision = '20260103_bigint_amount'
branch_labels = None
depends_on = None


def upgrade():
    # Add amount_needs_review: True if amount seems suspicious
    op.add_column('deals', sa.Column(
        'amount_needs_review',
        sa.Boolean(),
        nullable=False,
        server_default='false'
    ))

    # Add amount_review_reason: Why the amount needs review
    op.add_column('deals', sa.Column(
        'amount_review_reason',
        sa.String(),
        nullable=True
    ))


def downgrade():
    op.drop_column('deals', 'amount_review_reason')
    op.drop_column('deals', 'amount_needs_review')
