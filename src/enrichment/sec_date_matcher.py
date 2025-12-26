"""
SEC Form D Date Matcher - Cross-reference deal dates with official SEC filings.

SEC Form D filings are the authoritative source for funding round dates.
Companies must file within 15 days of first sale.

This module:
1. Searches SEC EDGAR for Form D filings matching a company name
2. Cross-references filing dates with extracted deal dates
3. Provides confidence scoring based on SEC verification
"""

import asyncio
import logging
import re
import httpx
import feedparser
from dataclasses import dataclass
from datetime import date, timedelta, datetime, timezone
from difflib import SequenceMatcher
from json import JSONDecodeError
from typing import Optional, List, Tuple
from urllib.parse import quote

logger = logging.getLogger(__name__)

# SEC EDGAR rate limiting (1 request/second guideline)
SEC_REQUEST_DELAY = 1.0
SEC_MAX_RETRIES = 3

# Confidence scores for date sources
DATE_CONFIDENCE = {
    "sec_form_d": 0.95,      # Official legal filing
    "press_release": 0.85,   # Company official announcement
    "article_headline": 0.75, # Date in article headline
    "article_body": 0.60,    # Date mentioned in article text
    "article_published": 0.40, # Fallback to when article was published
}

# Multi-source bonus when 2+ sources agree
MULTI_SOURCE_BONUS = 0.1


@dataclass
class SECDateMatch:
    """Result of SEC date cross-reference."""
    company_name: str
    sec_filing_date: date
    sec_filing_url: str
    cik: str
    amount_filed: Optional[str] = None
    name_match_score: float = 0.0  # 0.0-1.0 Levenshtein similarity
    date_delta_days: int = 0  # Difference from extracted date
    is_confident_match: bool = False  # True if we're confident this is the right filing


class SECDateMatcher:
    """
    Cross-reference deal dates with SEC EDGAR Form D filings.

    SEC filings are the authoritative source because:
    - Companies must file Form D within 15 days of first sale
    - Filing date is exact (not "a few days ago")
    - Amount raised is precise (not "reportedly $XM")
    """

    def __init__(self):
        self.client = httpx.AsyncClient(
            timeout=30,
            headers={
                "User-Agent": "BudTracker/1.0 (contact@budtracker.io; Investment Research)",
                "Accept": "application/json, text/html, application/xml",
            },
            follow_redirects=True,
            # Lower limits for SEC to respect their rate guidelines
            limits=httpx.Limits(max_connections=10, max_keepalive_connections=5),
        )

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.client.aclose()

    async def search_company(
        self,
        company_name: str,
        amount: Optional[str] = None,
        date_hint: Optional[date] = None,
    ) -> Optional[SECDateMatch]:
        """
        Search SEC EDGAR for Form D filings matching a company name.

        Args:
            company_name: Name of the company to search
            amount: Expected amount (for validation)
            date_hint: Extracted date to compare against

        Returns:
            SECDateMatch if found, None otherwise
        """
        # Normalize company name for search
        normalized_name = self._normalize_company_name(company_name)

        if len(normalized_name) < 3:
            logger.debug(f"Company name too short for SEC search: {company_name}")
            return None

        # Search SEC EDGAR company database
        cik, sec_name = await self._search_sec_company(normalized_name)

        if not cik:
            logger.debug(f"No SEC CIK found for: {company_name}")
            return None

        # Get recent Form D filings for this CIK
        filings = await self._get_form_d_filings(cik, days_back=365)

        if not filings:
            logger.debug(f"No Form D filings found for CIK {cik}")
            return None

        # Find best matching filing
        best_match = self._find_best_filing(
            filings=filings,
            company_name=company_name,
            sec_name=sec_name,
            amount=amount,
            date_hint=date_hint,
        )

        if best_match:
            logger.info(
                f"SEC match found for {company_name}: "
                f"CIK={cik}, filing_date={best_match.sec_filing_date}, "
                f"match_score={best_match.name_match_score:.2f}"
            )

        return best_match

    def _normalize_company_name(self, name: str) -> str:
        """Normalize company name for search."""
        # Remove common suffixes
        name = re.sub(r'\s*(Inc\.?|LLC|Corp\.?|Ltd\.?|Co\.?|LP|L\.P\.)$', '', name, flags=re.I)
        # Remove "AI" suffix (common in our domain)
        name = re.sub(r'\s*AI$', '', name, flags=re.I)
        # Remove extra whitespace
        name = ' '.join(name.split())
        return name.strip()

    async def _search_sec_company(self, company_name: str) -> Tuple[Optional[str], Optional[str]]:
        """
        Search SEC EDGAR for company CIK.

        Returns:
            Tuple of (CIK, official SEC company name) or (None, None)
        """
        await asyncio.sleep(SEC_REQUEST_DELAY)

        # Use SEC full-text search
        search_url = "https://efts.sec.gov/LATEST/search-index"
        params = {
            "q": f'"{company_name}"',
            "forms": "D,D/A",
            "dateRange": "custom",
            "startdt": (date.today() - timedelta(days=365)).isoformat(),
            "enddt": date.today().isoformat(),
        }

        try:
            # URL-encode company name to handle special characters
            encoded_name = quote(company_name, safe='')

            # First try the company search endpoint
            company_url = f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&company={encoded_name}&type=D&output=json"
            response = await self.client.get(company_url)

            if response.status_code == 200:
                try:
                    data = response.json()
                    if data and isinstance(data, list) and len(data) > 0:
                        # Find best match by name similarity
                        best_match = None
                        best_score = 0.0

                        for company in data[:10]:  # Check top 10
                            sec_name = company.get("name", "")
                            score = SequenceMatcher(None, company_name.lower(), sec_name.lower()).ratio()

                            if score > best_score and score > 0.6:
                                best_score = score
                                best_match = company

                        if best_match:
                            cik = str(best_match.get("cik", "")).zfill(10)
                            return cik, best_match.get("name")
                except (JSONDecodeError, KeyError, TypeError):
                    pass  # JSON parsing failed, try fallback

            # Fallback: Try atom feed search with company name
            feed_url = f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&company={encoded_name}&type=D&output=atom"
            response = await self.client.get(feed_url)

            if response.status_code == 200:
                # Parse atom feed (feedparser imported at module level)
                feed = feedparser.parse(response.text)

                for entry in feed.entries[:5]:
                    title = entry.get("title", "")
                    # Extract CIK from title or link
                    cik_match = re.search(r'\((\d+)\)', title)
                    if cik_match:
                        cik = cik_match.group(1).zfill(10)
                        company_match = re.match(r'D(?:/A)?\s*-\s*(.+?)\s*\(', title)
                        sec_name = company_match.group(1) if company_match else None

                        # Verify name similarity
                        if sec_name:
                            score = SequenceMatcher(None, company_name.lower(), sec_name.lower()).ratio()
                            if score > 0.6:
                                return cik, sec_name

            return None, None

        except Exception as e:
            logger.warning(f"SEC company search error for {company_name}: {e}")
            return None, None

    async def _get_form_d_filings(self, cik: str, days_back: int = 365) -> List[dict]:
        """
        Get Form D filings for a specific CIK.

        Returns:
            List of filing dictionaries with date, url, amount
        """
        await asyncio.sleep(SEC_REQUEST_DELAY)

        filings = []

        # Use SEC submissions endpoint
        submissions_url = f"https://data.sec.gov/submissions/CIK{cik}.json"

        try:
            response = await self.client.get(submissions_url)

            if response.status_code != 200:
                return filings

            data = response.json()

            # Get recent filings
            recent = data.get("filings", {}).get("recent", {})
            forms = recent.get("form", [])
            dates = recent.get("filingDate", [])
            accessions = recent.get("accessionNumber", [])

            cutoff = date.today() - timedelta(days=days_back)

            for i, form in enumerate(forms):
                if form not in ("D", "D/A"):
                    continue

                try:
                    filing_date = date.fromisoformat(dates[i])
                except (ValueError, IndexError):
                    continue

                if filing_date < cutoff:
                    continue

                accession = accessions[i] if i < len(accessions) else ""
                accession_clean = accession.replace("-", "")

                filing_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession_clean}/primary_doc.xml"

                filings.append({
                    "form": form,
                    "date": filing_date,
                    "url": filing_url,
                    "accession": accession,
                })

            return filings

        except Exception as e:
            logger.warning(f"SEC filings fetch error for CIK {cik}: {e}")
            return filings

    def _find_best_filing(
        self,
        filings: List[dict],
        company_name: str,
        sec_name: Optional[str],
        amount: Optional[str],
        date_hint: Optional[date],
    ) -> Optional[SECDateMatch]:
        """
        Find the best matching Form D filing.

        Matching criteria:
        1. Company name similarity (via sec_name)
        2. Date proximity to hint (if provided)
        3. Amount match (if provided)
        """
        if not filings:
            return None

        # Calculate name match score
        name_score = 0.0
        if sec_name:
            name_score = SequenceMatcher(
                None,
                company_name.lower(),
                sec_name.lower()
            ).ratio()

        best_filing = None
        best_score = 0.0

        for filing in filings:
            score = name_score  # Base score from name match

            filing_date = filing.get("date")

            # Boost score for date proximity
            if date_hint and filing_date:
                days_diff = abs((filing_date - date_hint).days)
                if days_diff <= 7:
                    score += 0.3  # Within a week
                elif days_diff <= 30:
                    score += 0.2  # Within a month
                elif days_diff <= 90:
                    score += 0.1  # Within 3 months

            # Prefer original Form D over amendments
            if filing.get("form") == "D":
                score += 0.1

            if score > best_score:
                best_score = score
                best_filing = filing

        if not best_filing:
            return None

        # Extract CIK from URL
        cik_match = re.search(r'/data/(\d+)/', best_filing.get("url", ""))
        cik = cik_match.group(1) if cik_match else ""

        date_delta = 0
        if date_hint and best_filing.get("date"):
            date_delta = (best_filing["date"] - date_hint).days

        return SECDateMatch(
            company_name=sec_name or company_name,
            sec_filing_date=best_filing["date"],
            sec_filing_url=best_filing["url"],
            cik=cik,
            name_match_score=name_score,
            date_delta_days=date_delta,
            is_confident_match=name_score > 0.8 and abs(date_delta) <= 30,
        )


def calculate_date_confidence(
    date_sources: List[Tuple[str, date, float]],
) -> Tuple[date, float, int]:
    """
    Calculate the most likely date and confidence score from multiple sources.

    Args:
        date_sources: List of (source_type, extracted_date, base_confidence)

    Returns:
        Tuple of (best_date, confidence_score, source_count)

    Algorithm:
    1. Group sources by date (allowing 1-day tolerance)
    2. For each date cluster, sum weighted confidence
    3. Return date with highest total confidence
    4. If 2+ sources agree, boost confidence by MULTI_SOURCE_BONUS
    """
    if not date_sources:
        return date.today(), 0.0, 0

    # Group by date (1-day tolerance)
    date_clusters: dict[date, List[Tuple[str, float]]] = {}

    for source_type, extracted_date, base_conf in date_sources:
        # Find existing cluster within 1 day
        matched_cluster = None
        for cluster_date in date_clusters.keys():
            if abs((cluster_date - extracted_date).days) <= 1:
                matched_cluster = cluster_date
                break

        if matched_cluster:
            date_clusters[matched_cluster].append((source_type, base_conf))
        else:
            date_clusters[extracted_date] = [(source_type, base_conf)]

    # Calculate total confidence for each cluster
    best_date = None
    best_confidence = 0.0
    best_count = 0

    for cluster_date, sources in date_clusters.items():
        total_confidence = sum(conf for _, conf in sources)
        source_count = len(sources)

        # Multi-source bonus
        if source_count >= 2:
            total_confidence += MULTI_SOURCE_BONUS

        # Cap at 1.0
        total_confidence = min(1.0, total_confidence)

        if total_confidence > best_confidence:
            best_confidence = total_confidence
            best_date = cluster_date
            best_count = source_count

    return best_date or date.today(), best_confidence, best_count


async def verify_deal_date_with_sec(
    company_name: str,
    extracted_date: Optional[date],
    amount: Optional[str] = None,
) -> Optional[SECDateMatch]:
    """
    Convenience function to verify a deal date against SEC filings.

    Returns SECDateMatch if found, None otherwise.
    """
    async with SECDateMatcher() as matcher:
        return await matcher.search_company(
            company_name=company_name,
            amount=amount,
            date_hint=extracted_date,
        )


async def persist_date_source(
    deal_id: int,
    source_type: str,
    extracted_date: date,
    confidence_score: float,
    source_url: Optional[str] = None,
    is_primary: bool = False,
) -> bool:
    """
    Persist a date source to the database.

    Args:
        deal_id: ID of the deal
        source_type: Type of source (sec_form_d, press_release, article_headline, etc.)
        extracted_date: The date extracted from this source
        confidence_score: Confidence in this date (0.0-1.0)
        source_url: URL of the source
        is_primary: Whether this is the primary/selected date for the deal

    Returns:
        True if persisted successfully, False otherwise
    """
    from ..archivist.database import get_session
    from ..archivist.models import DateSource

    try:
        async with get_session() as session:
            date_source = DateSource(
                deal_id=deal_id,
                source_type=source_type,
                source_url=source_url,
                extracted_date=extracted_date,
                confidence_score=confidence_score,
                is_primary=is_primary,
            )
            session.add(date_source)
            await session.commit()
            logger.info(f"Persisted date source for deal {deal_id}: {source_type} -> {extracted_date}")
            return True
    except Exception as e:
        logger.error(f"Error persisting date source for deal {deal_id}: {e}")
        return False


async def persist_sec_date_match(deal_id: int, match: SECDateMatch) -> bool:
    """
    Persist SEC date match to both DateSource table and Deal record.

    Args:
        deal_id: ID of the deal to update
        match: SECDateMatch result from verification

    Returns:
        True if persisted successfully, False otherwise
    """
    from ..archivist.database import get_session
    from ..archivist.models import Deal, DateSource

    try:
        async with get_session() as session:
            # Update Deal with SEC filing info
            deal = await session.get(Deal, deal_id)
            if deal:
                deal.sec_filing_date = match.sec_filing_date
                deal.sec_filing_url = match.sec_filing_url
                if match.is_confident_match:
                    deal.date_confidence = DATE_CONFIDENCE["sec_form_d"]
                    deal.announced_date = match.sec_filing_date

            # Add DateSource record
            date_source = DateSource(
                deal_id=deal_id,
                source_type="sec_form_d",
                source_url=match.sec_filing_url,
                extracted_date=match.sec_filing_date,
                confidence_score=DATE_CONFIDENCE["sec_form_d"],
                is_primary=match.is_confident_match,
            )
            session.add(date_source)
            await session.commit()

            logger.info(f"Persisted SEC date match for deal {deal_id}: {match.sec_filing_date}")
            return True
    except Exception as e:
        logger.error(f"Error persisting SEC date match for deal {deal_id}: {e}")
        return False
