"""
GitHub Trending Scraper - Monitor trending repositories for dev tools before funding.

GitHub Trending tracks the most popular repos by stars gained. Many dev tools
appear on trending BEFORE they announce funding - it's an early signal.

Key insight: Dev tools that trend often have VC interest. Track them early.

Source: https://github.com/trending
"""

import asyncio
import logging
import re
import httpx
from dataclasses import dataclass
from datetime import datetime, date, timezone
from typing import List, Optional
from bs4 import BeautifulSoup

from ..base_scraper import NormalizedArticle
from ...config.settings import settings

logger = logging.getLogger(__name__)


# GitHub Trending URLs by time range
GITHUB_TRENDING_URLS = {
    "daily": "https://github.com/trending?since=daily",
    "weekly": "https://github.com/trending?since=weekly",
    "monthly": "https://github.com/trending?since=monthly",
}

# Languages relevant for enterprise dev tools
ENTERPRISE_LANGUAGES = [
    "python", "typescript", "go", "rust", "java", "javascript"
]

# Keywords that indicate enterprise/B2B dev tools
ENTERPRISE_KEYWORDS = [
    # Infrastructure
    "api", "sdk", "cli", "framework", "platform", "infrastructure",
    "database", "queue", "cache", "storage", "cloud",
    # AI/ML
    "ai", "ml", "llm", "gpt", "agent", "embedding", "vector",
    "langchain", "llamaindex", "rag", "transformer",
    # DevOps
    "deploy", "ci", "cd", "kubernetes", "docker", "terraform",
    "monitoring", "observability", "logging", "tracing",
    # Security
    "security", "auth", "identity", "encryption", "compliance",
    # Data
    "analytics", "etl", "pipeline", "warehouse", "lakehouse",
]


@dataclass
class TrendingRepo:
    """A trending GitHub repository."""
    name: str  # org/repo
    url: str
    description: str
    language: Optional[str]
    stars_today: int
    total_stars: int
    forks: int
    built_by: List[str]  # Contributor avatars/names


class GitHubTrendingScraper:
    """
    Scraper for GitHub Trending repositories.

    Monitors trending repos to find dev tools before they announce funding.
    """

    def __init__(self, languages: Optional[List[str]] = None):
        """
        Initialize scraper.

        Args:
            languages: Filter by programming languages. If None, uses all.
        """
        self.languages = languages or ENTERPRISE_LANGUAGES
        self.client = httpx.AsyncClient(
            timeout=30,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
                "Accept": "text/html,application/xhtml+xml",
            }
        )

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.client.aclose()

    async def fetch_trending_page(self, language: Optional[str] = None, since: str = "daily") -> str:
        """Fetch GitHub trending page."""
        try:
            base_url = f"https://github.com/trending"
            if language:
                base_url = f"https://github.com/trending/{language}"

            url = f"{base_url}?since={since}"
            response = await self.client.get(url)
            response.raise_for_status()
            return response.text
        except httpx.HTTPError as e:
            # FIX: Specific exception + logger (not print)
            logger.error(f"HTTP error fetching GitHub trending: {e}")
            return ""
        except Exception as e:
            logger.error(f"Unexpected error fetching GitHub trending: {e}", exc_info=True)
            return ""

    def parse_trending_repos(self, html: str) -> List[TrendingRepo]:
        """Parse trending repositories from HTML."""
        repos = []
        soup = BeautifulSoup(html, 'lxml')

        for article in soup.select('article.Box-row'):
            try:
                # Repository name (org/repo)
                name_el = article.select_one('h2 a')
                if not name_el:
                    continue

                name = name_el.get_text(strip=True).replace('\n', '').replace(' ', '')
                # FIX: Validate href is not empty (prevents invalid "https://github.com" URLs)
                href = name_el.get('href', '').strip()
                if not href:
                    continue
                url = f"https://github.com{href}"

                # Description
                desc_el = article.select_one('p')
                description = desc_el.get_text(strip=True) if desc_el else ''

                # Language
                lang_el = article.select_one('[itemprop="programmingLanguage"]')
                language = lang_el.get_text(strip=True) if lang_el else None

                # Stars today
                stars_today_el = article.select_one('.float-sm-right')
                stars_today = 0
                if stars_today_el:
                    match = re.search(r'(\d+(?:,\d+)*)\s*stars?\s*today', stars_today_el.get_text())
                    if match:
                        stars_today = int(match.group(1).replace(',', ''))

                # Total stars and forks
                star_fork_els = article.select('a.Link--muted')
                total_stars = 0
                forks = 0
                for el in star_fork_els:
                    href = el.get('href', '')
                    text = el.get_text(strip=True).replace(',', '')
                    if '/stargazers' in href:
                        total_stars = int(text) if text.isdigit() else 0
                    elif '/forks' in href:
                        forks = int(text) if text.isdigit() else 0

                # Built by contributors
                built_by = []
                contributor_els = article.select('img.avatar')
                for img in contributor_els[:5]:
                    alt = img.get('alt', '')
                    if alt and alt.startswith('@'):
                        built_by.append(alt[1:])

                repos.append(TrendingRepo(
                    name=name,
                    url=url,
                    description=description,
                    language=language,
                    stars_today=stars_today,
                    total_stars=total_stars,
                    forks=forks,
                    built_by=built_by,
                ))

            except Exception as e:
                # FIX: Use logger instead of print
                logger.warning(f"Error parsing trending repo: {e}", exc_info=True)
                continue

        return repos

    async def fetch_all_trending(self, since: str = "daily") -> List[TrendingRepo]:
        """Fetch trending repos across multiple languages in parallel."""
        all_repos = []
        seen_names = set()

        # Fetch all pages in parallel (general + language-specific)
        async def fetch_and_parse(language: Optional[str] = None) -> List[TrendingRepo]:
            html = await self.fetch_trending_page(language=language, since=since)
            return self.parse_trending_repos(html)

        # Create tasks for parallel fetching
        tasks = [fetch_and_parse(None)]  # General trending
        tasks.extend([fetch_and_parse(lang) for lang in self.languages])

        # Execute all in parallel
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Deduplicate results
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Error fetching trending page: {result}")
                continue
            for repo in result:
                if repo.name not in seen_names:
                    seen_names.add(repo.name)
                    all_repos.append(repo)

        logger.info(f"GitHub Trending: {len(all_repos)} unique repos from {len(self.languages) + 1} pages")
        return all_repos

    def is_enterprise_devtool(self, repo: TrendingRepo) -> bool:
        """Check if repo is an enterprise dev tool."""
        text = f"{repo.name} {repo.description}".lower()
        return any(kw in text for kw in ENTERPRISE_KEYWORDS)

    async def fetch_repo_readme(self, repo: TrendingRepo) -> Optional[str]:
        """Fetch README content for a repository.

        Optimized: Try only the most common patterns (README.md on main/master)
        instead of 8 HTTP requests per repo. Most repos use README.md.
        """
        try:
            # Try README.md on main branch first (most common)
            raw_url = f"https://raw.githubusercontent.com/{repo.name}/main/README.md"
            response = await self.client.get(raw_url)
            if response.status_code == 200:
                return response.text[:5000]  # Truncate

            # Try README.md on master branch (older repos)
            raw_url = f"https://raw.githubusercontent.com/{repo.name}/master/README.md"
            response = await self.client.get(raw_url)
            if response.status_code == 200:
                return response.text[:5000]

            # Fallback: try lowercase readme.md on main (rare but exists)
            raw_url = f"https://raw.githubusercontent.com/{repo.name}/main/readme.md"
            response = await self.client.get(raw_url)
            if response.status_code == 200:
                return response.text[:5000]

            return None
        except httpx.HTTPError as e:
            # FIX: Specific exception + logger
            logger.error(f"HTTP error fetching README for {repo.name}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error fetching README for {repo.name}: {e}", exc_info=True)
            return None

    async def repo_to_article(self, repo: TrendingRepo) -> NormalizedArticle:
        """Convert GitHub repo to NormalizedArticle."""
        # Try to fetch README for more context
        readme = await self.fetch_repo_readme(repo)

        text_parts = [
            f"GitHub Trending: {repo.name}",
            f"Description: {repo.description}",
            f"Language: {repo.language or 'Unknown'}",
            f"Stars Today: {repo.stars_today:,}",
            f"Total Stars: {repo.total_stars:,}",
            f"Forks: {repo.forks:,}",
        ]

        if repo.built_by:
            text_parts.append(f"Top Contributors: {', '.join(repo.built_by)}")

        text_parts.append(f"\nGitHub URL: {repo.url}")

        if readme:
            text_parts.append(f"\n--- README ---\n{readme[:2000]}")

        return NormalizedArticle(
            url=repo.url,
            title=f"GitHub Trending: {repo.name} (+{repo.stars_today} stars) - {repo.description[:80]}",
            text="\n".join(text_parts),
            # FIX: Use None instead of date.today() (corrupts historical data)
            # Trending repos don't have a specific publish date
            published_date=None,
            author=repo.built_by[0] if repo.built_by else None,
            tags=['github', 'trending', repo.language.lower() if repo.language else 'unknown'],
            fund_slug="",  # No specific fund
            # FIX: Use timezone-aware datetime (utcnow() deprecated in Python 3.12+)
            fetched_at=datetime.now(timezone.utc),
        )

    async def scrape_all(
        self,
        since: str = "daily",
        filter_enterprise: bool = True,
        repos: Optional[List[TrendingRepo]] = None
    ) -> List[NormalizedArticle]:
        """
        Full scraping pipeline for GitHub Trending.

        Args:
            since: Time range - "daily", "weekly", or "monthly"
            filter_enterprise: Only include enterprise dev tools
            repos: Optional pre-fetched repos to avoid double HTTP fetching

        Returns:
            List of NormalizedArticle objects.
        """
        # Use pre-fetched repos or fetch new ones
        if repos is None:
            repos = await self.fetch_all_trending(since=since)

        # OPTIMIZATION: Filter BEFORE fetching README (saves 70-210 HTTP requests)
        if filter_enterprise:
            filtered_repos = [r for r in repos if self.is_enterprise_devtool(r)]
            logger.info(f"GitHub Trending: {len(filtered_repos)} enterprise repos (filtered from {len(repos)})")
        else:
            filtered_repos = repos

        # Fetch READMEs in parallel with semaphore (not serial)
        semaphore = asyncio.Semaphore(5)

        async def convert_with_limit(repo: TrendingRepo) -> NormalizedArticle:
            async with semaphore:
                return await self.repo_to_article(repo)

        tasks = [convert_with_limit(repo) for repo in filtered_repos]
        articles = await asyncio.gather(*tasks, return_exceptions=True)

        # Filter out any exceptions
        valid_articles = []
        for article in articles:
            if isinstance(article, Exception):
                logger.error(f"Error converting repo to article: {article}")
                continue
            valid_articles.append(article)

        return valid_articles


# Convenience function
async def run_github_trending_scraper(since: str = "daily") -> List[NormalizedArticle]:
    """Run GitHub Trending scraper and return articles."""
    async with GitHubTrendingScraper() as scraper:
        return await scraper.scrape_all(since=since)
