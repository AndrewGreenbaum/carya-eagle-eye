"""Add content_hashes table for persistent content deduplication

Revision ID: 20260121_content_hashes
Revises: 20260116_amount_dedup_key
Create Date: 2026-01-21

FIX: Saves ~$10-15/month by preventing re-extraction of syndicated articles
across different scan runs.

Problem: In-memory content hash cache is cleared between runs, so if TechCrunch
publishes an article at 2pm and VentureBeat syndicates it at 6pm (different run),
we'd extract it twice.

Solution: Persistent database cache with 30-day TTL.
- Content hash (SHA256 first 128 bits)
- Content length (to prefer longer articles)
- Expiration date (automatic cleanup)
"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '20260121_content_hashes'
down_revision = '20260116_amount_dedup_key'
branch_labels = None
depends_on = None


def upgrade():
    # Create the content_hashes table
    op.create_table(
        'content_hashes',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('content_hash', sa.String(32), nullable=False),
        sa.Column('content_length', sa.Integer(), nullable=False, default=0),
        sa.Column('source_url', sa.String(2000), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.func.now()),
        sa.Column('expires_at', sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint('id')
    )

    # Create index on content_hash for fast lookup
    op.create_index(
        'idx_content_hashes_hash',
        'content_hashes',
        ['content_hash'],
        unique=False
    )

    # Create index on expires_at for efficient cleanup queries
    op.create_index(
        'idx_content_hashes_expires_at',
        'content_hashes',
        ['expires_at'],
        unique=False
    )

    # Composite index for the common query pattern
    op.create_index(
        'idx_content_hashes_lookup',
        'content_hashes',
        ['content_hash', 'expires_at'],
        unique=False
    )


def downgrade():
    op.drop_index('idx_content_hashes_lookup', table_name='content_hashes')
    op.drop_index('idx_content_hashes_expires_at', table_name='content_hashes')
    op.drop_index('idx_content_hashes_hash', table_name='content_hashes')
    op.drop_table('content_hashes')
