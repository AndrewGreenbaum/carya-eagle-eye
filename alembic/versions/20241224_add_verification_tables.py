"""Add verification tables for date/founder accuracy

Revision ID: 20241224_verification
Revises: 20241222_is_ai_deal
Create Date: 2024-12-24

Adds:
- company_aliases: Track company rebrands (Bedrock Security → Bedrock Data)
- date_sources: Track dates from multiple sources with confidence scores
- founder_validations: Track LinkedIn validation results for founders
- New fields on deals: date_confidence, date_source_count, sec_filing_date, etc.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '20241224_verification'
down_revision: Union[str, None] = '20241222_is_ai_deal'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1. Create company_aliases table
    op.create_table(
        'company_aliases',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('company_id', sa.Integer(), sa.ForeignKey('portfolio_companies.id'), nullable=False),
        sa.Column('alias_name', sa.String(255), nullable=False),
        sa.Column('alias_type', sa.String(50), nullable=False, server_default='rebrand'),
        sa.Column('effective_date', sa.Date(), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now()),
    )
    op.create_index('ix_company_aliases_company_id', 'company_aliases', ['company_id'])
    op.create_index('ix_company_aliases_alias_name', 'company_aliases', [sa.text('LOWER(alias_name)')])
    # Unique constraint on company_id + alias_name to prevent duplicates
    op.create_unique_constraint('uq_company_aliases_company_alias', 'company_aliases', ['company_id', 'alias_name'])

    # 2. Create date_sources table
    op.create_table(
        'date_sources',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('deal_id', sa.Integer(), sa.ForeignKey('deals.id'), nullable=False),
        sa.Column('source_type', sa.String(50), nullable=False),  # sec_form_d, press_release, article_headline, etc.
        sa.Column('source_url', sa.String(2000), nullable=True),
        sa.Column('extracted_date', sa.Date(), nullable=False),
        sa.Column('confidence_score', sa.Float(), nullable=False, server_default='0.5'),
        sa.Column('is_primary', sa.Boolean(), nullable=False, server_default='false'),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now()),
    )
    op.create_index('ix_date_sources_deal_id', 'date_sources', ['deal_id'])

    # 3. Create founder_validations table
    op.create_table(
        'founder_validations',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('deal_id', sa.Integer(), sa.ForeignKey('deals.id'), nullable=False),
        sa.Column('founder_name', sa.String(255), nullable=False),
        sa.Column('extracted_title', sa.String(100), nullable=True),
        sa.Column('linkedin_url', sa.String(500), nullable=True),
        sa.Column('linkedin_current_company', sa.String(255), nullable=True),
        sa.Column('linkedin_current_title', sa.String(255), nullable=True),
        sa.Column('is_match', sa.Boolean(), nullable=True),
        sa.Column('title_is_leadership', sa.Boolean(), nullable=True),
        sa.Column('validated_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('validation_method', sa.String(50), nullable=False, server_default='brave_search'),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now()),
    )
    op.create_index('ix_founder_validations_deal_id', 'founder_validations', ['deal_id'])

    # 4. Add new fields to deals table
    op.add_column('deals', sa.Column('founders_validated', sa.Boolean(), nullable=False, server_default='false'))
    op.add_column('deals', sa.Column('date_confidence', sa.Float(), nullable=False, server_default='0.5'))
    op.add_column('deals', sa.Column('date_source_count', sa.Integer(), nullable=False, server_default='1'))
    op.add_column('deals', sa.Column('sec_filing_date', sa.Date(), nullable=True))
    op.add_column('deals', sa.Column('sec_filing_url', sa.String(500), nullable=True))


def downgrade() -> None:
    # Remove columns from deals
    op.drop_column('deals', 'sec_filing_url')
    op.drop_column('deals', 'sec_filing_date')
    op.drop_column('deals', 'date_source_count')
    op.drop_column('deals', 'date_confidence')
    op.drop_column('deals', 'founders_validated')

    # Drop tables
    op.drop_index('ix_founder_validations_deal_id', table_name='founder_validations')
    op.drop_table('founder_validations')

    op.drop_index('ix_date_sources_deal_id', table_name='date_sources')
    op.drop_table('date_sources')

    op.drop_constraint('uq_company_aliases_company_alias', 'company_aliases', type_='unique')
    op.drop_index('ix_company_aliases_alias_name', table_name='company_aliases')
    op.drop_index('ix_company_aliases_company_id', table_name='company_aliases')
    op.drop_table('company_aliases')
