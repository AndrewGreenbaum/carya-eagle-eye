"""Add scan_jobs table and scan_job_id to deals

Revision ID: 20241225_add_scan_jobs
Revises: 20241225_lead_evidence_weak
Create Date: 2025-12-25
"""
from alembic import op
import sqlalchemy as sa
from typing import Union


# revision identifiers, used by Alembic.
revision: str = '20241225_add_scan_jobs'
down_revision: Union[str, None] = '20241225_lead_evidence_weak'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Create scan_jobs table
    op.create_table(
        'scan_jobs',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('job_id', sa.String(), nullable=False),
        sa.Column('started_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('completed_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('duration_seconds', sa.Float(), nullable=True),
        sa.Column('status', sa.String(), nullable=False, server_default='running'),
        sa.Column('error_message', sa.String(), nullable=True),
        sa.Column('total_articles_found', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('total_deals_extracted', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('total_deals_saved', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('total_duplicates_skipped', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('total_errors', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('lead_deals_found', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('enterprise_ai_deals_found', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('source_results_json', sa.String(), nullable=True),
        sa.Column('trigger', sa.String(), nullable=False, server_default='scheduled'),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index('ix_scan_jobs_job_id', 'scan_jobs', ['job_id'], unique=True)
    op.create_index('ix_scan_jobs_status', 'scan_jobs', ['status'], unique=False)

    # Add scan_job_id to deals table
    op.add_column('deals', sa.Column('scan_job_id', sa.Integer(), nullable=True))
    op.create_index('ix_deals_scan_job_id', 'deals', ['scan_job_id'], unique=False)
    op.create_foreign_key('fk_deals_scan_job_id', 'deals', 'scan_jobs', ['scan_job_id'], ['id'])


def downgrade() -> None:
    op.drop_constraint('fk_deals_scan_job_id', 'deals', type_='foreignkey')
    op.drop_index('ix_deals_scan_job_id', table_name='deals')
    op.drop_column('deals', 'scan_job_id')
    op.drop_index('ix_scan_jobs_status', table_name='scan_jobs')
    op.drop_index('ix_scan_jobs_job_id', table_name='scan_jobs')
    op.drop_table('scan_jobs')
