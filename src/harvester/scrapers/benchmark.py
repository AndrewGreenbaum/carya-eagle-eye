"""
Benchmark Capital Scraper.

Source: GlobeNewswire/PRNewswire RSS + Brave Search
Format: EXTERNAL

Benchmark doesn't have a public news feed - we search news aggregators.
Must include Partner names in article. Filter NOT 'International', NOT 'Electronics'.
"""

import logging
from datetime import datetime, timedelta
from typing import List, Optional
import feedparser
from bs4 import BeautifulSoup

from ..base_scraper import BaseScraper, RawArticle, NormalizedArticle
from ...config.funds import FUND_REGISTRY

logger = logging.getLogger(__name__)


class BenchmarkScraper(BaseScraper):
    """
    Scraper for Benchmark Capital via news aggregators.

    Searches GlobeNewswire and PRNewswire RSS feeds for Benchmark mentions.
    Filters out "Benchmark International" and "Benchmark Electronics".
    """

    # Benchmark partners for verification
    PARTNERS = [
        "Bill Gurley",
        "Peter Fenton",
        "Matt Cohler",
        "Eric Vishria",
        "Sarah Tavel",
        "Chetan Puttagunta",
        "Miles Grimshaw",
        "Victor Lazarte",
    ]

    def __init__(self):
        super().__init__(FUND_REGISTRY["benchmark"])
        self.partner_names = self.PARTNERS

        # RSS feeds to search
        self.rss_feeds = [
            # GlobeNewswire - funding/venture capital news
            "https://www.globenewswire.com/RssFeed/subjectcode/17-Funding%2FVenture%20Capital/feedTitle/GlobeNewswire%20-%20Funding%2FVenture%20Capital",
            # PRNewswire - venture capital feed
            "https://www.prnewswire.com/rss/financial-services-latest-news/financial-services-latest-news-list.rss",
        ]

    async def fetch(self, url: Optional[str] = None) -> str:
        """Fetch and combine RSS feeds, then search for Benchmark mentions."""
        if url:
            response = await self.client.get(url, follow_redirects=True)
            response.raise_for_status()
            return response.text

        # Collect all RSS items
        all_items = []

        for feed_url in self.rss_feeds:
            try:
                response = await self.client.get(
                    feed_url,
                    follow_redirects=True,
                    timeout=30.0
                )
                if response.status_code == 200:
                    feed = feedparser.parse(response.text)
                    # Check for malformed feed
                    if feed.bozo and feed.bozo_exception:
                        logger.warning(f"Malformed RSS feed {feed_url}: {feed.bozo_exception}")
                    all_items.extend(feed.entries)
            except Exception as e:
                logger.warning(f"Error fetching {feed_url}: {e}")
                continue

        # Convert to a pseudo-HTML format for parsing
        html_parts = ["<html><body>"]
        for item in all_items:
            title = item.get("title", "")
            link = item.get("link", "")
            summary = item.get("summary", "")
            published = item.get("published", "")

            # Only include if mentions Benchmark (case-insensitive)
            combined_text = f"{title} {summary}".lower()
            if "benchmark" in combined_text:
                # Skip false positives
                if self._should_exclude(combined_text):
                    continue

                html_parts.append(f"""
                <article>
                    <h3><a href="{link}">{title}</a></h3>
                    <time datetime="{published}">{published}</time>
                    <p>{summary}</p>
                </article>
                """)

        html_parts.append("</body></html>")
        return "\n".join(html_parts)

    async def parse(self, html: str) -> List[RawArticle]:
        """Parse RSS-derived HTML for Benchmark mentions."""
        soup = BeautifulSoup(html, "lxml")
        articles = []
        seen_urls = set()

        for article in soup.select("article"):
            title_el = article.select_one("h3")
            if not title_el:
                continue
            title = title_el.get_text(strip=True)

            # Skip excluded content
            if self._should_exclude(title):
                continue

            # Get link
            link_el = article.select_one("a[href]")
            if not link_el:
                continue
            url = link_el.get("href", "")

            if not url or url in seen_urls:
                continue
            seen_urls.add(url)

            # Get date
            date_el = article.select_one("time")
            pub_date = None
            if date_el:
                date_str = date_el.get("datetime") or date_el.get_text(strip=True)
                pub_date = self._parse_date(date_str)

            # Get summary
            summary_el = article.select_one("p")
            summary = summary_el.get_text(strip=True) if summary_el else ""

            # Check if this is likely a funding announcement
            combined = f"{title} {summary}".lower()
            funding_keywords = ["funding", "investment", "raises", "series", "million", "led by"]
            if not any(kw in combined for kw in funding_keywords):
                continue

            articles.append(RawArticle(
                url=url,
                title=title,
                html=str(article),
                published_date=pub_date,
                tags=["benchmark", "press_release"],
            ))

        return articles

    async def normalize(self, raw: RawArticle) -> NormalizedArticle:
        """Fetch full article and verify Benchmark mention."""
        try:
            full_html = await self.fetch(raw.url)
            text = self._extract_text(full_html)

            # Verify this is actually about Benchmark Capital
            if not self._verify_benchmark_mention(text):
                text = f"[UNVERIFIED] {text}"

            # Flag if a partner is mentioned
            partner_mentioned = self._find_partner(text)
            if partner_mentioned:
                text = f"[PARTNER: {partner_mentioned}]\n{text}"

        except Exception:
            text = self._extract_text(raw.html)

        return NormalizedArticle(
            url=raw.url,
            title=raw.title,
            text=text,
            published_date=raw.published_date,
            tags=raw.tags,
        )

    def _should_exclude(self, text: str) -> bool:
        """Check if text contains excluded keywords."""
        text_lower = text.lower()
        exclude_keywords = [
            "benchmark international",
            "benchmark electronics",
            "benchmark capital management",
            "benchmark mineral",
            "benchmark litigation",
        ]
        return any(kw in text_lower for kw in exclude_keywords)

    def _verify_benchmark_mention(self, text: str) -> bool:
        """Verify the article mentions Benchmark Capital properly."""
        text_lower = text.lower()

        # Must mention Benchmark Capital or a partner name
        valid_mentions = [
            "benchmark capital",
            "benchmark partners",
            "benchmark led",
            "led by benchmark",
        ]
        if any(m in text_lower for m in valid_mentions):
            return True

        # Check for partner names
        for partner in self.partner_names:
            if partner.lower() in text_lower:
                return True

        return False

    def _find_partner(self, text: str) -> Optional[str]:
        """Find which Benchmark partner is mentioned."""
        text_lower = text.lower()
        for partner in self.partner_names:
            if partner.lower() in text_lower:
                return partner
        return None


def create_scraper() -> BenchmarkScraper:
    return BenchmarkScraper()
