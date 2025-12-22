"""
Founders Fund Scraper.

Source: https://foundersfund.com/ (homepage + portfolio)
Format: PLAYWRIGHT (JS hydration required)

NOTE: Founders Fund does NOT have a /news/ page. The site has:
- Homepage with featured content
- /portfolio/ page with company logos
- Blog posts at paths like /2023/08/diversity-myth-30-years-later/

This scraper monitors the homepage for blog post links and the
portfolio page for new company additions (stealth detection).
"""

import logging
from datetime import date
from typing import List, Optional
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

from ..playwright_scraper import PlaywrightScraper
from ..base_scraper import RawArticle, NormalizedArticle
from ...config.funds import FUND_REGISTRY


class FoundersFundScraper(PlaywrightScraper):
    """
    Scraper for Founders Fund portfolio and blog content.

    Uses Playwright to render JavaScript-heavy pages.
    Scrapes homepage for blog links and portfolio for company detection.
    """

    def __init__(self):
        super().__init__(FUND_REGISTRY["founders_fund"])
        self.base_url = "https://foundersfund.com"

    async def fetch(self, url: Optional[str] = None) -> str:
        """Fetch homepage which has links to blog posts."""
        target_url = url or self.base_url

        html = await self.fetch_rendered(
            target_url,
            wait_selector="a[href*='/20'], [class*='post'], [class*='article']",
            wait_ms=5000
        )
        return html

    async def parse(self, html: str) -> List[RawArticle]:
        """Parse homepage for blog post links (paths like /2023/08/...)."""
        soup = BeautifulSoup(html, "lxml")
        articles = []
        seen_urls = set()

        # Find all links that look like blog posts (year/month pattern)
        for link in soup.select("a[href]"):
            href = link.get("href", "")

            # Blog posts have paths like /2023/08/title-slug/
            if not href:
                continue

            # Make absolute URL
            if href.startswith("/"):
                url = f"{self.base_url}{href}"
            elif href.startswith("http"):
                url = href
            else:
                continue

            # Must be a Founders Fund blog post
            if "foundersfund.com" not in url:
                continue

            # Check for year/month pattern in URL (stricter: months 01-12 only)
            import re
            if not re.search(r'/(20[0-2][0-9])/(0[1-9]|1[0-2])/', url):
                continue

            if url in seen_urls:
                continue
            seen_urls.add(url)

            # Get title from link text or nearby heading
            title = link.get_text(strip=True)
            if not title or len(title) < 10:
                # Try to find a heading nearby
                parent = link.find_parent(["div", "article", "section"])
                if parent:
                    heading = parent.select_one("h1, h2, h3, h4")
                    if heading:
                        title = heading.get_text(strip=True)

            if not title or len(title) < 10:
                # Use URL slug as title
                slug = url.rstrip("/").split("/")[-1]
                title = slug.replace("-", " ").title()

            articles.append(RawArticle(
                url=url,
                title=title,
                html=str(link.parent) if link.parent else str(link),
                published_date=self._extract_date_from_url(url),
            ))

        return articles

    def _extract_date_from_url(self, url: str) -> Optional[date]:
        """Extract date from URL like /2023/08/title/."""
        import re
        match = re.search(r'/(\d{4})/(\d{2})/', url)
        if match:
            try:
                year = int(match.group(1))
                month = int(match.group(2))
                return date(year, month, 1)
            except (ValueError, TypeError):
                pass
        return None

    async def normalize(self, raw: RawArticle) -> NormalizedArticle:
        """Fetch and extract content from individual blog post."""
        # OPTIMIZATION: Check if card HTML already has sufficient content
        # Skip expensive Playwright fetch if we already have 1500+ chars
        card_text = self._extract_text(raw.html)
        if len(card_text) >= 1500:
            return NormalizedArticle(
                url=raw.url,
                title=raw.title,
                text=card_text,
                published_date=raw.published_date,
            )

        try:
            full_html = await self.fetch_rendered(
                raw.url,
                wait_selector="article, .post-content, .entry-content, main, .content",
                wait_ms=2000  # Reduced from 3000ms - content loads fast
            )
            soup = BeautifulSoup(full_html, "lxml")

            # Find main content
            content_el = soup.select_one(
                "article, .post-content, .entry-content, [class*='Content'], main, .content"
            )

            if content_el:
                # Remove non-content elements
                for el in content_el.select("nav, header, footer, .share, script, style, .sidebar"):
                    el.decompose()
                text = content_el.get_text(separator="\n", strip=True)
            else:
                text = self._extract_text(full_html)

        except Exception as e:
            logger.warning(f"Error fetching {raw.url}: {e}")
            text = card_text  # Fall back to card text on error

        return NormalizedArticle(
            url=raw.url,
            title=raw.title,
            text=text or card_text,
            published_date=raw.published_date,
        )

    async def fetch_portfolio(self) -> List[str]:
        """
        Fetch current portfolio companies for stealth detection.

        Returns list of company names currently on portfolio page.
        """
        try:
            html = await self.fetch_rendered(
                f"{self.base_url}/portfolio/",
                wait_selector="[class*='company'], [class*='portfolio'], [class*='grid'], img[alt]",
                wait_ms=5000
            )
            soup = BeautifulSoup(html, "lxml")

            companies = []

            # Try multiple selectors for company names
            for el in soup.select("[class*='company'], [class*='portfolio'], [class*='Company']"):
                name_el = el.select_one("h3, h4, .name, [class*='name'], [class*='Name']")
                if name_el:
                    name = name_el.get_text(strip=True)
                    if name and len(name) > 1:
                        companies.append(name)

            # Also check image alt text (common pattern for portfolio pages)
            for img in soup.select("img[alt]"):
                alt = img.get("alt", "").strip()
                if alt and len(alt) > 1 and len(alt) < 50:
                    # Skip generic alt text
                    if alt.lower() not in ("logo", "image", "company", "portfolio"):
                        companies.append(alt)

            return list(set(companies))  # Dedupe
        except Exception as e:
            logger.warning(f"Error fetching portfolio: {e}")
            return []


def create_scraper() -> FoundersFundScraper:
    return FoundersFundScraper()
