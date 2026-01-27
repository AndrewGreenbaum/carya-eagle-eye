"""Add composite indexes for common query patterns

Revision ID: 20260126_composite_indexes
Revises: 20260122_regenerate_dedup_keys
Create Date: 2026-01-26

FIX 2026-01: Adds composite indexes to eliminate full table scans on common queries.

Before: 9 sequential queries in find_duplicate_deal() caused N+1 pattern at scale.
After: Composite indexes allow index-only scans for most duplicate detection queries.

Indexes added:
1. idx_deals_lead_enterprise_created - Main listing filter (is_lead_confirmed, is_enterprise_ai, created_at)
2. idx_deals_round_date - Duplicate detection TIER 2.5 (round_type, announced_date)
3. idx_deals_amount_date_round - Duplicate detection TIER 3, 4 (amount_usd, announced_date, round_type)
4. idx_deals_round_created - Null date duplicate detection (round_type, created_at)
5. idx_deals_company_round_date - Duplicate detection joins (company_id, round_type, announced_date)
"""
from alembic import op

# revision identifiers, used by Alembic.
revision = '20260126_composite_indexes'
down_revision = '20260122_regenerate_dedup_keys'
branch_labels = None
depends_on = None


def upgrade():
    # 1. Main deals listing with filters
    # Query pattern: WHERE is_lead_confirmed = true AND is_enterprise_ai = true ORDER BY created_at DESC
    op.create_index(
        'idx_deals_lead_enterprise_created',
        'deals',
        ['is_lead_confirmed', 'is_enterprise_ai', 'created_at'],
        postgresql_using='btree'
    )

    # 2. Duplicate detection TIER 2.5 - round_type + announced_date range
    # Query pattern: WHERE round_type = ? AND announced_date BETWEEN ? AND ?
    op.create_index(
        'idx_deals_round_date',
        'deals',
        ['round_type', 'announced_date'],
        postgresql_using='btree'
    )

    # 3. Duplicate detection TIER 3, 4 - amount + date + round
    # Query pattern: WHERE amount_usd BETWEEN ? AND ? AND announced_date = ? AND round_type = ?
    op.create_index(
        'idx_deals_amount_date_round',
        'deals',
        ['amount_usd', 'announced_date', 'round_type'],
        postgresql_using='btree'
    )

    # 4. Null date duplicate detection - round_type + created_at
    # Query pattern: WHERE round_type = ? AND announced_date IS NULL AND created_at >= ?
    op.create_index(
        'idx_deals_round_created',
        'deals',
        ['round_type', 'created_at'],
        postgresql_using='btree'
    )

    # 5. Company-based duplicate detection
    # Query pattern: JOIN portfolio_companies ON company_id WHERE round_type = ? AND announced_date = ?
    op.create_index(
        'idx_deals_company_round_date',
        'deals',
        ['company_id', 'round_type', 'announced_date'],
        postgresql_using='btree'
    )


def downgrade():
    op.drop_index('idx_deals_company_round_date', table_name='deals')
    op.drop_index('idx_deals_round_created', table_name='deals')
    op.drop_index('idx_deals_amount_date_round', table_name='deals')
    op.drop_index('idx_deals_round_date', table_name='deals')
    op.drop_index('idx_deals_lead_enterprise_created', table_name='deals')
