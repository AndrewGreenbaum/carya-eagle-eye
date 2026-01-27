"""
Backfill Enrichment - One-time script to enrich ALL deals missing website or LinkedIn.

This script runs through ALL deals in the database and attempts to find:
1. Company website (MANDATORY - targeting 95%+ coverage)
2. CEO/Founder LinkedIn (best effort - targeting 50%+ coverage)

Uses multiple fallback search strategies for hard-to-find companies.
"""

import asyncio
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Dict, Optional, Tuple

from sqlalchemy import select, or_, and_

from ..archivist.database import get_session
from ..archivist.models import Deal, PortfolioCompany, DealInvestor, Fund
from ..enrichment.brave_enrichment import (
    BraveEnrichmentClient,
    DealContext,
)
from ..config.settings import settings

logger = logging.getLogger(__name__)


@dataclass
class BackfillStats:
    """Track backfill progress and results."""
    total_processed: int = 0
    websites_found: int = 0
    websites_already_had: int = 0
    linkedins_found: int = 0
    linkedins_already_had: int = 0
    failed_website: int = 0
    failed_linkedin: int = 0


async def get_deals_missing_website() -> List[Dict]:
    """Get all deals where the company is missing a website.

    FIX: Now includes lead investor via subquery to avoid N+1 query problem.
    """
    async with get_session() as session:
        # Subquery to get lead investor for each deal
        lead_investor_subq = (
            select(
                DealInvestor.deal_id,
                DealInvestor.investor_name,
            )
            .where(DealInvestor.is_lead == True)
            .subquery()
        )

        # Join deals with companies that have no website, plus lead investor
        stmt = (
            select(
                Deal.id,
                PortfolioCompany.id.label("company_id"),
                PortfolioCompany.name.label("company_name"),
                PortfolioCompany.website,
                Deal.founders_json,
                Deal.enterprise_category,
                lead_investor_subq.c.investor_name.label("lead_investor"),
            )
            .join(PortfolioCompany, Deal.company_id == PortfolioCompany.id)
            .outerjoin(lead_investor_subq, Deal.id == lead_investor_subq.c.deal_id)
            .where(
                or_(
                    PortfolioCompany.website.is_(None),
                    PortfolioCompany.website == "",
                )
            )
            .order_by(Deal.created_at.desc())
        )

        result = await session.execute(stmt)
        rows = result.fetchall()

        deals = []
        for row in rows:
            # Skip invalid company names
            name = row.company_name or ""
            if name.lower() in ("<unknown>", "unknown", "n/a", "no funding deal"):
                continue

            deals.append({
                "deal_id": row.id,
                "company_id": row.company_id,
                "company_name": name,
                "founders_json": row.founders_json,
                "enterprise_category": row.enterprise_category,
                "lead_investor": row.lead_investor,  # FIX: Include in result
            })

        return deals


async def get_deals_missing_founder_linkedin() -> List[Dict]:
    """Get all deals that have founders but missing LinkedIn URLs.

    FIX: Now includes lead investor via subquery to avoid N+1 query problem.
    """
    async with get_session() as session:
        # Subquery to get lead investor for each deal
        lead_investor_subq = (
            select(
                DealInvestor.deal_id,
                DealInvestor.investor_name,
            )
            .where(DealInvestor.is_lead == True)
            .subquery()
        )

        # Get deals with founders_json that might be missing LinkedIn
        stmt = (
            select(
                Deal.id,
                PortfolioCompany.id.label("company_id"),
                PortfolioCompany.name.label("company_name"),
                Deal.founders_json,
                lead_investor_subq.c.investor_name.label("lead_investor"),
            )
            .join(PortfolioCompany, Deal.company_id == PortfolioCompany.id)
            .outerjoin(lead_investor_subq, Deal.id == lead_investor_subq.c.deal_id)
            .where(Deal.founders_json.isnot(None))
            .order_by(Deal.created_at.desc())
        )

        result = await session.execute(stmt)
        rows = result.fetchall()

        deals = []
        for row in rows:
            # Skip invalid company names
            name = row.company_name or ""
            if name.lower() in ("<unknown>", "unknown", "n/a", "no funding deal"):
                continue

            # Parse founders JSON and check for missing LinkedIn
            try:
                founders = json.loads(row.founders_json) if row.founders_json else []
            except (json.JSONDecodeError, TypeError):
                continue

            # Check if any founder is missing LinkedIn
            founders_missing_linkedin = [
                f for f in founders
                if f.get("name") and not f.get("linkedin_url")
            ]

            if founders_missing_linkedin:
                deals.append({
                    "deal_id": row.id,
                    "company_id": row.company_id,
                    "company_name": name,
                    "founders": founders,
                    "founders_missing_linkedin": founders_missing_linkedin,
                    "lead_investor": row.lead_investor,  # FIX: Include in result
                })

        return deals


async def get_lead_investor_for_deal(deal_id: int) -> Optional[str]:
    """Get the lead investor name for a deal.

    LEGACY: This function is kept for backwards compatibility.
    New code should use the lead_investor field from get_deals_missing_* functions.
    """
    async with get_session() as session:
        stmt = (
            select(DealInvestor.investor_name)
            .where(
                and_(
                    DealInvestor.deal_id == deal_id,
                    DealInvestor.is_lead == True,
                )
            )
            .limit(1)
        )
        result = await session.execute(stmt)
        row = result.scalar_one_or_none()
        return row


async def update_company_website_in_session(
    session,
    company_id: int,
    website: str,
) -> bool:
    """Update company website using an existing session."""
    try:
        stmt = select(PortfolioCompany).where(PortfolioCompany.id == company_id)
        result = await session.execute(stmt)
        company = result.scalar_one_or_none()
        if company:
            company.website = website
            company.updated_at = datetime.now(timezone.utc).replace(tzinfo=None)
            logger.info(f"Updated company {company_id} ({company.name}) website to {website}")
            return True
        else:
            logger.warning(f"Company {company_id} not found")
        return False
    except Exception as e:
        logger.error(f"Error updating company {company_id} website: {e}")
        return False


async def update_deal_founders_in_session(
    session,
    deal_id: int,
    founders: List[Dict],
) -> bool:
    """Update deal founders JSON using an existing session."""
    try:
        stmt = select(Deal).where(Deal.id == deal_id)
        result = await session.execute(stmt)
        deal = result.scalar_one_or_none()
        if deal:
            deal.founders_json = json.dumps(founders)
            logger.info(f"Updated deal {deal_id} founders")
            return True
        else:
            logger.warning(f"Deal {deal_id} not found")
        return False
    except Exception as e:
        logger.error(f"Error updating deal {deal_id} founders: {e}")
        return False


# Keep legacy functions for backwards compatibility
async def update_company_website(company_id: int, website: str) -> bool:
    """Update company website in database."""
    async with get_session() as session:
        return await update_company_website_in_session(session, company_id, website)


async def update_deal_founders(deal_id: int, founders: List[Dict]) -> bool:
    """Update deal founders JSON with LinkedIn URLs."""
    async with get_session() as session:
        return await update_deal_founders_in_session(session, deal_id, founders)


class BackfillEnrichmentClient(BraveEnrichmentClient):
    """Extended enrichment client with multiple fallback strategies."""

    async def find_website_with_fallbacks(
        self,
        company_name: str,
        investor: Optional[str] = None,
        category: Optional[str] = None,
    ) -> Optional[str]:
        """
        Try multiple search strategies to find company website.
        Returns the first valid result found.
        """
        if not company_name or len(company_name) < 2:
            return None

        # Skip invalid names
        invalid_names = {"<unknown>", "unknown", "n/a", "no funding deal", "no specific funding deal"}
        if company_name.lower() in invalid_names:
            return None

        strategies = []

        # Strategy 1: Company + Investor context (most accurate)
        if investor:
            investor_short = investor.split()[0] if investor else ""
            strategies.append(f'"{company_name}" "{investor_short}" startup official website')

        # Strategy 2: Company + Category
        if category:
            category_map = {
                "vertical_saas": "healthcare software",
                "infrastructure": "developer tools API",
                "security": "cybersecurity",
                "agentic": "AI automation agent",
                "data_intelligence": "enterprise data analytics",
            }
            cat_context = category_map.get(category, "startup technology")
            strategies.append(f'"{company_name}" {cat_context} company website')

        # Strategy 3: Company + "official site"
        strategies.append(f'"{company_name}" official site homepage startup')

        # Strategy 4: Just company name + startup
        strategies.append(f'"{company_name}" startup company')

        # Strategy 5: Site-specific search (useful for common names)
        strategies.append(f'"{company_name}" site:crunchbase.com')

        for i, query in enumerate(strategies):
            try:
                results = await self._search(query, count=10)

                for result in results:
                    url = result.get("url", "")

                    # For Crunchbase strategy, extract the actual company URL
                    if "crunchbase.com" in url:
                        # Try to find company website in description
                        desc = result.get("description", "")
                        # Crunchbase often mentions the website
                        # We'll skip Crunchbase URLs themselves but use as signal
                        continue

                    if self._is_valid_startup_website(url, company_name):
                        from urllib.parse import urlparse
                        parsed = urlparse(url)
                        website = f"https://{parsed.netloc}"
                        logger.info(f"Found website for {company_name} using strategy {i+1}: {website}")
                        return website

                # Rate limit between strategies
                await asyncio.sleep(self.rate_limit_delay)

            except Exception as e:
                logger.warning(f"Strategy {i+1} failed for {company_name}: {e}")
                continue

        return None

    async def find_linkedin_with_fallbacks(
        self,
        founder_name: str,
        company_name: str,
        investor: Optional[str] = None,
    ) -> Optional[str]:
        """
        Try multiple search strategies to find founder LinkedIn.
        """
        if not founder_name or len(founder_name) < 3:
            return None

        # Skip generic names
        if founder_name.lower() in ("ceo", "founder", "unknown", "n/a"):
            return None

        strategies = []

        # Strategy 1: Full context with company
        strategies.append(f'site:linkedin.com/in "{founder_name}" "{company_name}"')

        # Strategy 2: Founder + company + title
        strategies.append(f'"{founder_name}" "{company_name}" LinkedIn CEO founder')

        # Strategy 3: With investor context
        if investor:
            investor_short = investor.split()[0] if investor else ""
            strategies.append(f'"{founder_name}" "{investor_short}" LinkedIn')

        # Strategy 4: Just founder name + title
        strategies.append(f'"{founder_name}" CEO founder LinkedIn profile')

        # Strategy 5: Broadest search
        strategies.append(f'"{founder_name}" LinkedIn')

        for i, query in enumerate(strategies):
            try:
                results = await self._search(query, count=5)

                for result in results:
                    url = result.get("url", "")
                    title = result.get("title", "").lower()

                    # Must be a LinkedIn profile URL
                    linkedin_url = self._extract_linkedin_url(url)
                    if linkedin_url:
                        # Verify founder name appears in result
                        founder_first = founder_name.split()[0].lower()
                        if founder_first in title:
                            logger.info(f"Found LinkedIn for {founder_name} using strategy {i+1}: {linkedin_url}")
                            return linkedin_url

                # Rate limit between strategies
                await asyncio.sleep(self.rate_limit_delay)

            except Exception as e:
                logger.warning(f"LinkedIn strategy {i+1} failed for {founder_name}: {e}")
                continue

        return None


async def backfill_websites(
    limit: Optional[int] = None,
    dry_run: bool = False,
) -> BackfillStats:
    """
    Backfill ALL deals missing website.

    Args:
        limit: Optional limit on number of deals to process (None = all)
        dry_run: If True, don't save to database (just log what would happen)

    Returns:
        BackfillStats with results
    """
    stats = BackfillStats()

    logger.info("Starting website backfill...")
    deals = await get_deals_missing_website()

    if limit:
        deals = deals[:limit]

    logger.info(f"Found {len(deals)} deals missing website")

    if not deals:
        return stats

    # Use a single session for all updates
    async with get_session() as session:
        async with BackfillEnrichmentClient() as client:
            for i, deal in enumerate(deals):
                company_name = deal["company_name"]
                company_id = deal["company_id"]
                deal_id = deal["deal_id"]
                category = deal.get("enterprise_category")

                # FIX: Use lead investor from query result (avoids N+1 query problem)
                investor = deal.get("lead_investor")

                logger.info(f"[{i+1}/{len(deals)}] Enriching {company_name}...")

                try:
                    website = await client.find_website_with_fallbacks(
                        company_name=company_name,
                        investor=investor,
                        category=category,
                    )

                    if website:
                        stats.websites_found += 1
                        if not dry_run:
                            success = await update_company_website_in_session(
                                session, company_id, website
                            )
                            if not success:
                                logger.error(f"  -> Failed to save website for {company_name}")
                        logger.info(f"  -> Found: {website}")
                    else:
                        stats.failed_website += 1
                        logger.warning(f"  -> Not found")

                except Exception as e:
                    stats.failed_website += 1
                    logger.error(f"  -> Error: {e}")

                stats.total_processed += 1

                # Rate limit
                await asyncio.sleep(0.5)

                # Commit every 10 updates to avoid losing all progress
                if stats.total_processed % 10 == 0:
                    await session.commit()
                    logger.info(f"  -> Committed {stats.total_processed} updates")

            # FIX: Final commit for any remaining uncommitted changes
            if stats.total_processed % 10 != 0:
                await session.commit()
                logger.info(f"  -> Final commit ({stats.total_processed} total updates)")

    logger.info(f"Website backfill complete: {stats.websites_found} found, {stats.failed_website} failed")
    return stats


async def backfill_linkedin(
    limit: Optional[int] = None,
    dry_run: bool = False,
) -> BackfillStats:
    """
    Backfill ALL deals missing founder LinkedIn.

    Args:
        limit: Optional limit on number of deals to process (None = all)
        dry_run: If True, don't save to database (just log what would happen)

    Returns:
        BackfillStats with results
    """
    stats = BackfillStats()

    logger.info("Starting LinkedIn backfill...")
    deals = await get_deals_missing_founder_linkedin()

    if limit:
        deals = deals[:limit]

    logger.info(f"Found {len(deals)} deals with founders missing LinkedIn")

    if not deals:
        return stats

    # Use a single session for all updates
    async with get_session() as session:
        async with BackfillEnrichmentClient() as client:
            for i, deal in enumerate(deals):
                company_name = deal["company_name"]
                deal_id = deal["deal_id"]
                founders = deal["founders"]
                founders_missing = deal["founders_missing_linkedin"]

                # FIX: Use lead investor from query result (avoids N+1 query problem)
                investor = deal.get("lead_investor")

                logger.info(f"[{i+1}/{len(deals)}] Enriching LinkedIn for {company_name} ({len(founders_missing)} founders)...")

                updated_founders = list(founders)  # Copy
                founders_enriched = 0

                for founder in founders_missing:
                    founder_name = founder.get("name", "")

                    try:
                        linkedin_url = await client.find_linkedin_with_fallbacks(
                            founder_name=founder_name,
                            company_name=company_name,
                            investor=investor,
                        )

                        if linkedin_url:
                            # Update the founder in the list
                            for f in updated_founders:
                                if f.get("name") == founder_name:
                                    f["linkedin_url"] = linkedin_url
                                    founders_enriched += 1
                                    break
                            logger.info(f"  -> {founder_name}: {linkedin_url}")
                        else:
                            logger.warning(f"  -> {founder_name}: Not found")

                    except Exception as e:
                        logger.error(f"  -> {founder_name}: Error - {e}")

                    # Rate limit between founders
                    await asyncio.sleep(0.3)

                if founders_enriched > 0:
                    stats.linkedins_found += founders_enriched
                    if not dry_run:
                        await update_deal_founders_in_session(session, deal_id, updated_founders)
                else:
                    stats.failed_linkedin += 1

                stats.total_processed += 1

                # Commit every 10 updates
                if stats.total_processed % 10 == 0:
                    await session.commit()
                    logger.info(f"  -> Committed {stats.total_processed} updates")

            # FIX: Final commit for any remaining uncommitted changes
            if stats.total_processed % 10 != 0:
                await session.commit()
                logger.info(f"  -> Final commit ({stats.total_processed} total updates)")

    logger.info(f"LinkedIn backfill complete: {stats.linkedins_found} found, {stats.failed_linkedin} deals with no results")
    return stats


async def backfill_all(
    limit: Optional[int] = None,
    dry_run: bool = False,
) -> Tuple[BackfillStats, BackfillStats]:
    """
    Run full backfill for both websites and LinkedIn.

    Args:
        limit: Optional limit per category (None = all)
        dry_run: If True, don't save to database

    Returns:
        Tuple of (website_stats, linkedin_stats)
    """
    logger.info("=" * 60)
    logger.info("STARTING FULL ENRICHMENT BACKFILL")
    logger.info("=" * 60)

    # Phase 1: Websites (MANDATORY)
    logger.info("\n--- PHASE 1: WEBSITE ENRICHMENT ---")
    website_stats = await backfill_websites(limit=limit, dry_run=dry_run)

    # Phase 2: LinkedIn (best effort)
    logger.info("\n--- PHASE 2: LINKEDIN ENRICHMENT ---")
    linkedin_stats = await backfill_linkedin(limit=limit, dry_run=dry_run)

    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("BACKFILL COMPLETE")
    logger.info("=" * 60)
    logger.info(f"Websites: {website_stats.websites_found} found, {website_stats.failed_website} failed")
    logger.info(f"LinkedIn: {linkedin_stats.linkedins_found} found, {linkedin_stats.failed_linkedin} deals with no results")

    return website_stats, linkedin_stats


async def get_enrichment_coverage() -> Dict:
    """Get current enrichment coverage statistics.

    FIX (2026-01): Added with_both and missing_both metrics for better coverage tracking.
    """
    async with get_session() as session:
        # Total deals with company
        total_stmt = select(Deal.id).join(PortfolioCompany)
        total_result = await session.execute(total_stmt)
        total_deals = len(total_result.fetchall())

        # Get all deals with company info for comprehensive analysis
        all_deals_stmt = (
            select(Deal.id, Deal.founders_json, PortfolioCompany.website)
            .join(PortfolioCompany)
        )
        all_deals_result = await session.execute(all_deals_stmt)
        all_deals = all_deals_result.fetchall()

        # Calculate metrics
        with_website = 0
        with_founders = 0
        with_linkedin = 0
        with_both = 0
        missing_both = 0

        for row in all_deals:
            has_website = bool(row.website and row.website.strip())
            has_linkedin = False
            has_founders = bool(row.founders_json)

            if has_website:
                with_website += 1

            if has_founders:
                with_founders += 1
                try:
                    founders = json.loads(row.founders_json)
                    if any(f.get("linkedin_url") for f in founders):
                        has_linkedin = True
                        with_linkedin += 1
                except (json.JSONDecodeError, TypeError):
                    pass

            # Track deals with both website AND founder LinkedIn
            if has_website and has_linkedin:
                with_both += 1
            # Track deals missing both website AND founder LinkedIn
            elif not has_website and not has_linkedin:
                missing_both += 1

        return {
            "total_deals": total_deals,
            "with_website": with_website,
            "website_percentage": round(with_website / total_deals * 100, 1) if total_deals else 0,
            "with_founders": with_founders,
            "with_linkedin": with_linkedin,
            "linkedin_percentage": round(with_linkedin / total_deals * 100, 1) if total_deals else 0,
            "missing_website": total_deals - with_website,
            "missing_linkedin": with_founders - with_linkedin,
            "with_both": with_both,
            "missing_both": missing_both,
        }


# CLI entry point for manual runs
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Backfill enrichment for all deals")
    parser.add_argument("--limit", type=int, help="Limit number of deals to process")
    parser.add_argument("--dry-run", action="store_true", help="Don't save to database")
    parser.add_argument("--websites-only", action="store_true", help="Only backfill websites")
    parser.add_argument("--linkedin-only", action="store_true", help="Only backfill LinkedIn")
    parser.add_argument("--stats", action="store_true", help="Just show coverage stats")

    args = parser.parse_args()

    async def main():
        if args.stats:
            stats = await get_enrichment_coverage()
            print("\nEnrichment Coverage:")
            print(f"  Total Deals: {stats['total_deals']}")
            print(f"  With Website: {stats['with_website']} ({stats['website_percentage']}%)")
            print(f"  With LinkedIn: {stats['with_linkedin']} ({stats['linkedin_percentage']}%)")
            print(f"\n  Missing Website: {stats['missing_website']}")
            print(f"  Missing LinkedIn: {stats['missing_linkedin']}")
            return

        if args.websites_only:
            await backfill_websites(limit=args.limit, dry_run=args.dry_run)
        elif args.linkedin_only:
            await backfill_linkedin(limit=args.limit, dry_run=args.dry_run)
        else:
            await backfill_all(limit=args.limit, dry_run=args.dry_run)

    asyncio.run(main())
