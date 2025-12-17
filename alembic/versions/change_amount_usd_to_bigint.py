"""Change amount_usd from INTEGER to BIGINT for large deals (>$2.1B)

Revision ID: bigint_amount
Revises: add_stealth_detection_indexes
Create Date: 2026-01-03

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '20260103_bigint_amount'
down_revision = '20260102_company_name_unique'
branch_labels = None
depends_on = None


def upgrade():
    # Change amount_usd from INTEGER (max 2.1B) to BIGINT (max 9.2 quintillion)
    # This allows deals like Replit $2.3B, Stripe $6.5B, etc.
    op.alter_column('deals', 'amount_usd',
                    type_=sa.BigInteger(),
                    existing_type=sa.Integer(),
                    existing_nullable=True)


def downgrade():
    # Revert to INTEGER (will fail if any values > 2.1B exist)
    op.alter_column('deals', 'amount_usd',
                    type_=sa.Integer(),
                    existing_type=sa.BigInteger(),
                    existing_nullable=True)
