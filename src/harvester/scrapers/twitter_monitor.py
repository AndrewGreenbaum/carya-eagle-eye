"""
Twitter/X Monitoring Scraper - Monitor VC announcements on Twitter.

Uses Twitter API v2 to monitor:
1. Official VC fund accounts for investment announcements
2. Search for funding news with tracked fund mentions

SETUP:
1. Apply for Twitter Developer account: https://developer.twitter.com/
2. Create a project and app
3. Get Bearer Token
4. Set TWITTER_BEARER_TOKEN environment variable

API LIMITS (Free Tier):
- 1,500 tweets/month read (Tweet caps)
- 1 App environment
- Login with X

OPTIMIZATION:
- Focus on official VC accounts (higher signal)
- Use targeted search queries
- Cache results to avoid duplicate API calls
"""

import asyncio
import logging
import httpx
from dataclasses import dataclass
from datetime import datetime, date, timedelta, timezone

logger = logging.getLogger(__name__)
from typing import List, Optional, Dict, Any

from ..base_scraper import NormalizedArticle
from ...config.settings import settings


# Official Twitter/X handles for tracked VC funds
# Only includes validated, working handles
VC_TWITTER_HANDLES = {
    # Fund accounts
    "benchmark": "benchmark",
    "sequoia": "sequoia",
    "index": "IndexVentures",
    "a16z": "a16z",
    "bessemer": "Bessemer_VP",
    "greylock": "GreylockVC",
    "gv": "GVteam",
    "menlo": "MenloVentures",
    "thrive": "ThriveCapital",
    "accel": "Accel",
    "felicis": "FelicisVC",
    "first_round": "firstround",
}

# Partner accounts who actively tweet about deals
# These are individual GPs who often announce investments before fund accounts
VC_PARTNER_HANDLES = {
    "benchmark_bgurley": "bgurley",         # Bill Gurley - VERY ACTIVE
    "benchmark_sarahtavel": "sarahtavel",   # Sarah Tavel
    "redpoint_ttunguz": "ttunguz",          # Tomasz Tunguz - VERY ACTIVE
    "first_round_joshk": "joshk",           # Josh Kopelman
    "greylock_reidhoffman": "reidhoffman",  # Reid Hoffman
    "sequoia_amysun": "chasingamy",         # Amy Sun
}

# Search queries for funding news
FUNDING_SEARCH_QUERIES = [
    '"led by" (Sequoia OR a16z OR Benchmark OR Greylock) funding',
    '"Series A" OR "Series B" OR "Series C" (led OR investment) venture',
    'enterprise AI funding (led OR raises OR announces)',
]


@dataclass
class Tweet:
    """Parsed tweet data."""
    id: str
    text: str
    author_username: str
    author_name: str
    created_at: datetime
    url: str
    metrics: Dict[str, int]


class TwitterMonitor:
    """
    Monitor Twitter/X for VC funding announcements.

    Uses Twitter API v2 with Bearer Token authentication.
    """

    def __init__(self):
        self.bearer_token = settings.twitter_bearer_token
        self.base_url = "https://api.twitter.com/2"
        self.client = httpx.AsyncClient(
            timeout=30,
            headers={
                "Authorization": f"Bearer {self.bearer_token}",
                "User-Agent": "BudTracker/1.0",
            }
        )
        self._request_count = 0

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.client.aclose()

    def _check_rate_limit(self) -> bool:
        """Check if we're approaching rate limits."""
        # Free tier: 1,500 tweets/month
        # FIX #48: Use configurable limit from settings
        return self._request_count < settings.twitter_requests_per_run

    async def get_user_tweets(
        self,
        username: str,
        max_results: int = 10,
        hours_back: int = 168,  # 1 week default
    ) -> List[Tweet]:
        """
        Get recent tweets from a specific user.

        Args:
            username: Twitter handle (without @)
            max_results: Maximum tweets to fetch (5-100)
            hours_back: Only get tweets from last N hours
        """
        if not self.bearer_token:
            return []

        if not self._check_rate_limit():
            logger.warning("Rate limit reached for this run")
            return []

        try:
            # First, get user ID from username
            user_response = await self.client.get(
                f"{self.base_url}/users/by/username/{username}",
                params={"user.fields": "id,name,username"}
            )

            if user_response.status_code == 401:
                logger.error("Twitter API: Invalid or expired Bearer Token")
                return []

            if user_response.status_code == 429:
                logger.warning("Twitter API: Rate limited")
                return []

            if user_response.status_code != 200:
                # FIX: Log unexpected status codes instead of silent failure
                logger.warning(f"Twitter API: Unexpected status {user_response.status_code} for @{username}")
                return []

            # FIX: Handle JSON parse errors
            try:
                user_data = user_response.json()
            except (ValueError, TypeError) as e:
                logger.error(f"Failed to parse JSON response for @{username}: {e}")
                return []

            if "data" not in user_data:
                # FIX: Log when user not found instead of silent failure
                logger.warning(f"Twitter API: User @{username} not found or suspended")
                return []

            # FIX: Use .get() to prevent KeyError
            user_id = user_data["data"].get("id")
            if not user_id:
                logger.error(f"Missing user ID in response for @{username}")
                return []

            self._request_count += 1

            # Get user's tweets
            # FIX: Use timezone-aware datetime (utcnow() deprecated in Python 3.12+)
            start_time = (datetime.now(timezone.utc) - timedelta(hours=hours_back)).isoformat()

            tweets_response = await self.client.get(
                f"{self.base_url}/users/{user_id}/tweets",
                params={
                    "max_results": min(max_results, 100),
                    "start_time": start_time,
                    "tweet.fields": "created_at,public_metrics,text",
                    "expansions": "author_id",
                    "user.fields": "name,username",
                }
            )

            if tweets_response.status_code != 200:
                # FIX: Log status code instead of silent failure
                logger.warning(f"Twitter API: Failed to fetch tweets for @{username} (status {tweets_response.status_code})")
                return []

            self._request_count += 1

            # FIX: Handle JSON parse errors
            try:
                data = tweets_response.json()
            except (ValueError, TypeError) as e:
                logger.error(f"Failed to parse tweets JSON for @{username}: {e}")
                return []

            tweets = []
            for tweet_data in data.get("data", []):
                # FIX: Validate required fields before accessing
                tweet_id = tweet_data.get("id")
                tweet_text = tweet_data.get("text", "")
                created_at_str = tweet_data.get("created_at")

                if not tweet_id or not created_at_str:
                    logger.warning(f"Skipping tweet with missing id or created_at")
                    continue

                # FIX: Handle fromisoformat errors
                try:
                    created_at = datetime.fromisoformat(
                        created_at_str.replace("Z", "+00:00")
                    )
                except (ValueError, AttributeError) as e:
                    logger.warning(f"Invalid created_at format: {created_at_str}")
                    continue

                tweets.append(Tweet(
                    id=tweet_id,
                    text=tweet_text,
                    author_username=username,
                    author_name=user_data["data"].get("name", username),
                    created_at=created_at,
                    url=f"https://twitter.com/{username}/status/{tweet_id}",
                    metrics=tweet_data.get("public_metrics", {}),
                ))

            return tweets

        # FIX: Handle specific HTTP errors first, then general with exc_info
        except httpx.HTTPError as e:
            logger.error(f"HTTP error fetching tweets for @{username}: {e}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error fetching tweets for @{username}: {e}", exc_info=True)
            return []

    async def search_tweets(
        self,
        query: str,
        max_results: int = 10,
        hours_back: int = 168,
    ) -> List[Tweet]:
        """
        Search for tweets matching a query.

        Args:
            query: Search query (Twitter search syntax)
            max_results: Maximum tweets to fetch
            hours_back: Only get tweets from last N hours
        """
        if not self.bearer_token:
            return []

        if not self._check_rate_limit():
            logger.warning("Rate limit reached for this run")
            return []

        try:
            # FIX: Use timezone-aware datetime (utcnow() deprecated in Python 3.12+)
            start_time = (datetime.now(timezone.utc) - timedelta(hours=hours_back)).isoformat()

            response = await self.client.get(
                f"{self.base_url}/tweets/search/recent",
                params={
                    "query": query,
                    "max_results": min(max_results, 100),
                    "start_time": start_time,
                    "tweet.fields": "created_at,public_metrics,text,author_id",
                    "expansions": "author_id",
                    "user.fields": "name,username",
                }
            )

            if response.status_code == 401:
                logger.error("Twitter API: Invalid or expired Bearer Token")
                return []

            if response.status_code == 429:
                logger.warning("Twitter API: Rate limited")
                return []

            if response.status_code != 200:
                logger.warning(f"Twitter API error: {response.status_code}")
                return []

            self._request_count += 1

            # FIX: Handle JSON parse errors
            try:
                data = response.json()
            except (ValueError, TypeError) as e:
                logger.error(f"Failed to parse search tweets JSON: {e}")
                return []

            # Build user lookup from includes
            # FIX: Use .get() to prevent KeyError
            users = {}
            for user in data.get("includes", {}).get("users", []):
                user_id = user.get("id")
                if not user_id:
                    continue
                users[user_id] = {
                    "username": user.get("username", "unknown"),
                    "name": user.get("name", "Unknown"),
                }

            tweets = []
            for tweet_data in data.get("data", []):
                # FIX: Validate required fields before accessing
                tweet_id = tweet_data.get("id")
                tweet_text = tweet_data.get("text", "")
                created_at_str = tweet_data.get("created_at")

                if not tweet_id or not created_at_str:
                    logger.warning(f"Skipping search tweet with missing id or created_at")
                    continue

                author_id = tweet_data.get("author_id", "")
                author_info = users.get(author_id, {"username": "unknown", "name": "Unknown"})

                # FIX: Handle fromisoformat errors
                try:
                    created_at = datetime.fromisoformat(
                        created_at_str.replace("Z", "+00:00")
                    )
                except (ValueError, AttributeError) as e:
                    logger.warning(f"Invalid created_at format: {created_at_str}")
                    continue

                tweets.append(Tweet(
                    id=tweet_id,
                    text=tweet_text,
                    author_username=author_info["username"],
                    author_name=author_info["name"],
                    created_at=created_at,
                    url=f"https://twitter.com/{author_info['username']}/status/{tweet_id}",
                    metrics=tweet_data.get("public_metrics", {}),
                ))

            return tweets

        # FIX: Handle specific HTTP errors first, then general with exc_info
        except httpx.HTTPError as e:
            logger.error(f"HTTP error searching tweets: {e}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error searching tweets: {e}", exc_info=True)
            return []

    def _is_funding_related(self, text: str) -> bool:
        """Check if tweet is likely about funding."""
        text_lower = text.lower()
        funding_keywords = [
            "funding", "raised", "series", "seed", "investment",
            "led by", "announces", "closes", "million", "venture",
            "portfolio", "backed", "round"
        ]
        return sum(1 for kw in funding_keywords if kw in text_lower) >= 2

    async def monitor_vc_accounts(self, hours_back: int = 168) -> List[Tweet]:
        """
        Monitor all tracked VC Twitter accounts + key partner accounts.

        Returns funding-related tweets only.
        """
        all_tweets = []

        # Monitor fund accounts
        for fund_slug, handle in VC_TWITTER_HANDLES.items():
            if not self._check_rate_limit():
                break

            tweets = await self.get_user_tweets(
                username=handle,
                max_results=20,
                hours_back=hours_back,
            )

            # Filter for funding-related tweets
            for tweet in tweets:
                if self._is_funding_related(tweet.text):
                    all_tweets.append(tweet)

            await asyncio.sleep(1)  # Rate limiting

        # Monitor individual partner accounts (often announce deals before fund accounts)
        for partner_key, handle in VC_PARTNER_HANDLES.items():
            if not self._check_rate_limit():
                break

            tweets = await self.get_user_tweets(
                username=handle,
                max_results=10,  # Fewer tweets per partner
                hours_back=hours_back,
            )

            # Filter for funding-related tweets
            for tweet in tweets:
                if self._is_funding_related(tweet.text):
                    all_tweets.append(tweet)

            await asyncio.sleep(1)  # Rate limiting

        return all_tweets

    async def search_funding_news(self, hours_back: int = 168) -> List[Tweet]:
        """
        Search for funding news tweets.

        Uses predefined search queries to find funding announcements.
        """
        all_tweets = []
        seen_ids = set()

        for query in FUNDING_SEARCH_QUERIES:
            if not self._check_rate_limit():
                break

            tweets = await self.search_tweets(
                query=query,
                max_results=20,
                hours_back=hours_back,
            )

            for tweet in tweets:
                if tweet.id not in seen_ids:
                    seen_ids.add(tweet.id)
                    all_tweets.append(tweet)

            await asyncio.sleep(1)

        return all_tweets

    async def scrape_all(self, hours_back: int = 168) -> List[NormalizedArticle]:
        """
        Full Twitter monitoring pipeline.

        Combines VC account monitoring and search results.
        Returns normalized articles ready for extraction.
        """
        # Reset request counter for this run (fixes bug where counter never resets)
        self._request_count = 0

        if not self.bearer_token:
            logger.warning("Twitter API: TWITTER_BEARER_TOKEN not configured")
            logger.info("Get one at: https://developer.twitter.com/")
            return []

        articles = []
        seen_ids = set()

        # Monitor VC accounts
        vc_tweets = await self.monitor_vc_accounts(hours_back)

        # Search for funding news
        search_tweets = await self.search_funding_news(hours_back)

        # Combine and deduplicate
        all_tweets = vc_tweets + search_tweets

        for tweet in all_tweets:
            if tweet.id in seen_ids:
                continue
            seen_ids.add(tweet.id)

            # FIX: Only add "..." if text is actually truncated
            text_preview = tweet.text[:100] if tweet.text else ""
            title_suffix = "..." if len(tweet.text) > 100 else ""

            # Create normalized article from tweet
            articles.append(NormalizedArticle(
                url=tweet.url,
                title=f"@{tweet.author_username}: {text_preview}{title_suffix}",
                text=tweet.text,
                published_date=tweet.created_at.date(),
                author=tweet.author_name,
                tags=['twitter', tweet.author_username],
                fund_slug=None,
                # FIX: Use timezone-aware datetime (utcnow() deprecated in Python 3.12+)
                fetched_at=datetime.now(timezone.utc),
            ))

        logger.info(f"Twitter: Found {len(articles)} funding-related tweets (used {self._request_count} API calls)")
        return articles


def get_twitter_setup_instructions() -> str:
    """Return instructions for setting up Twitter API."""
    return """
=== TWITTER API SETUP INSTRUCTIONS ===

1. Go to https://developer.twitter.com/

2. Sign up for a Developer account (free tier available)

3. Create a Project:
   - Name: "BudTracker" or similar
   - Use case: "Academic research" or "Building a tool"

4. Create an App within the project

5. Go to "Keys and tokens" section

6. Generate Bearer Token (under "Authentication Tokens")

7. Copy the Bearer Token

8. Add to Railway environment variable:
   TWITTER_BEARER_TOKEN=your_bearer_token_here

FREE TIER LIMITS:
- 1,500 tweets/month read
- Rate limited per 15-minute window
- Recent search only (last 7 days)

The scraper is optimized to stay within free tier limits.
"""


# Convenience function
async def run_twitter_monitor(hours_back: int = 168) -> List[NormalizedArticle]:
    """Run Twitter monitor and return articles."""
    async with TwitterMonitor() as monitor:
        return await monitor.scrape_all(hours_back)
