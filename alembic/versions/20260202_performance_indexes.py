"""performance_indexes

Revision ID: 20260202_performance_indexes
Revises: b4a64bf65e15
Create Date: 2026-02-02 12:00:00.000000

Adds critical performance indexes identified in system audit:
1. articles.deal_id - foreign key queries
2. deal_investors composite (deal_id, fund_id) - fund-based queries
3. portfolio_companies.linkedin_url - enrichment dedup
4. company_aliases.alias_name case-insensitive - ILIKE searches
5. scan_jobs.status + last_heartbeat composite - stuck monitor (20x faster)
"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '20260202_performance_indexes'
down_revision: Union[str, None] = 'b4a64bf65e15'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1. Article.deal_id foreign key queries
    op.create_index('idx_articles_deal_id', 'articles', ['deal_id'])

    # 2. DealInvestor composite for fund-based queries
    op.create_index('idx_deal_investors_deal_fund', 'deal_investors', ['deal_id', 'fund_id'])

    # 3. PortfolioCompany.linkedin_url for enrichment dedup
    op.create_index('idx_portfolio_companies_linkedin_url', 'portfolio_companies', ['linkedin_url'])

    # 4. CompanyAlias.alias_name case-insensitive for ILIKE searches
    op.execute('CREATE INDEX idx_company_aliases_name_lower ON company_aliases(lower(alias_name))')

    # 5. ScanJob.status + last_heartbeat composite for stuck monitor (100ms → 5ms)
    op.create_index('idx_scan_jobs_status_heartbeat', 'scan_jobs', ['status', 'last_heartbeat'])


def downgrade() -> None:
    op.drop_index('idx_scan_jobs_status_heartbeat', table_name='scan_jobs')
    op.drop_index('idx_company_aliases_name_lower', table_name='company_aliases')
    op.drop_index('idx_portfolio_companies_linkedin_url', table_name='portfolio_companies')
    op.drop_index('idx_deal_investors_deal_fund', table_name='deal_investors')
    op.drop_index('idx_articles_deal_id', table_name='articles')
