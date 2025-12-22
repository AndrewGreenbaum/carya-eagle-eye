"""
a16z (Andreessen Horowitz) Scraper.

Source: https://a16z.com/news-content/
Format: HTML (converted from Playwright - simpler HTTP approach)

a16z news page with investment announcements.
"""

import logging
from typing import List, Optional
from bs4 import BeautifulSoup

from ..base_scraper import SimpleHTMLScraper, RawArticle, NormalizedArticle
from ...config.funds import FUND_REGISTRY

logger = logging.getLogger(__name__)

# Verticals to exclude (not Enterprise AI)
EXCLUDED_VERTICALS = {"crypto", "web3", "bio", "games", "gaming", "consumer", "nft", "defi"}


class A16ZScraper(SimpleHTMLScraper):
    """
    Scraper for a16z news and announcements.

    Uses simple HTTP requests - converted from Playwright for Railway compatibility.
    """

    def __init__(self):
        super().__init__(FUND_REGISTRY["a16z"])
        self.base_url = "https://a16z.com"

    async def fetch(self, url: Optional[str] = None) -> str:
        """Fetch HTML with retry logic for transient errors."""
        target_url = url or f"{self.base_url}/news-content/"
        return await self._fetch_with_retry(target_url)

    async def parse(self, html: str) -> List[RawArticle]:
        soup = BeautifulSoup(html, "lxml")
        articles = []
        seen_urls = set()

        # a16z uses various card/article patterns
        selectors = [
            "article",
            ".post-card",
            ".news-item",
            "[class*='PostCard']",
            "[class*='NewsCard']",
            "[class*='article-card']",
            "[class*='card']",
            "[class*='Card']",
            "a[href*='/announcement']",
            "a[href*='/news/']",
        ]

        for selector in selectors:
            for card in soup.select(selector):
                # Extract title
                title_el = card.select_one("h2, h3, h4, .title, [class*='title'], [class*='Title']")
                if not title_el:
                    # If card is an <a> tag, try getting title from its text
                    if card.name == "a":
                        title = card.get_text(strip=True)
                    else:
                        continue
                else:
                    title = title_el.get_text(strip=True)

                if not title or len(title) < 10:
                    continue

                # Extract link
                link_el = card.select_one("a[href]")
                if not link_el:
                    if card.name == "a":
                        link_el = card
                    else:
                        link_el = card.find_parent("a")
                if not link_el:
                    continue

                url = link_el.get("href", "")

                # Make URL absolute
                if url and not url.startswith("http"):
                    url = f"{self.base_url}{url}"

                # Skip non-a16z URLs
                if "a16z.com" not in url:
                    continue

                if url in seen_urls or not url:
                    continue
                seen_urls.add(url)

                # Extract date
                date_el = card.select_one("time, .date, [class*='date'], [class*='Date']")
                pub_date = None
                if date_el:
                    date_str = date_el.get("datetime") or date_el.get_text(strip=True)
                    pub_date = self._parse_date(date_str)

                # Extract tags
                tags = []
                for tag_el in card.select(".tag, .category, [class*='tag'], [class*='category']"):
                    tag_text = tag_el.get_text(strip=True).lower()
                    if tag_text:
                        tags.append(tag_text)

                # Filter out excluded verticals (crypto, bio, games, etc.)
                title_lower = title.lower()
                if any(v in tags or v in title_lower for v in EXCLUDED_VERTICALS):
                    logger.debug(f"Skipping a16z article with excluded vertical: {title[:50]}")
                    continue

                articles.append(RawArticle(
                    url=url,
                    title=title,
                    html=str(card),
                    published_date=pub_date,
                    tags=tags
                ))

        return articles

    async def normalize(self, raw: RawArticle) -> NormalizedArticle:
        author = None
        text = None

        # OPTIMIZATION: Check if card HTML already has sufficient content
        # Skip full fetch if we already have 1500+ chars of text (saves HTTP request)
        card_text = self._extract_text(raw.html)
        if len(card_text) >= 1500:
            text = card_text
        else:
            # Need to fetch full article for complete content
            try:
                full_html = await self.fetch(raw.url)
                soup = BeautifulSoup(full_html, "lxml")

                # Find main content
                content_el = soup.select_one(
                    "article, .post-content, .entry-content, [class*='ArticleContent'], [class*='PostContent'], main"
                )

                if content_el:
                    for el in content_el.select("nav, header, footer, .share, .related, script, style"):
                        el.decompose()
                    text = content_el.get_text(separator="\n", strip=True)
                else:
                    text = self._extract_text(full_html)

                # Extract author
                author_el = soup.select_one(".author, [class*='author'], [class*='Author'], [rel='author']")
                if author_el:
                    author = author_el.get_text(strip=True)

            except Exception as e:
                logger.error(f"Error fetching {raw.url}: {e}", exc_info=True)
                text = card_text  # Fall back to card text on fetch error

        return NormalizedArticle(
            url=raw.url,
            title=raw.title,
            text=text or card_text,
            published_date=raw.published_date,
            author=author,
            tags=raw.tags
        )


def create_scraper() -> A16ZScraper:
    return A16ZScraper()
