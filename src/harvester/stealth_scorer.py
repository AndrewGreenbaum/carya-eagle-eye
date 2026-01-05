"""
Rule-based scoring for pre-funding signals.

Scores articles from early detection scrapers on a 0-100 scale.
No LLM calls - pure keyword and pattern matching.

Sources:
- hackernews: Launch HN posts, high-engagement discussions
- ycombinator: Demo Day batch companies
- github: Trending dev tools and AI repos
- linkedin: Stealth startup hiring signals
- delaware: New tech company incorporations
"""

import re
import logging
from typing import Tuple, Dict, Any, Optional
from dataclasses import dataclass

from .base_scraper import NormalizedArticle

logger = logging.getLogger(__name__)


# AI/ML keywords for bonus scoring
AI_KEYWORDS = [
    'ai', 'artificial intelligence', 'machine learning', 'ml', 'llm', 'gpt',
    'neural', 'deep learning', 'nlp', 'transformer', 'generative', 'agent',
    'langchain', 'openai', 'anthropic', 'claude', 'chatgpt', 'copilot',
]

# Enterprise/infrastructure keywords
ENTERPRISE_KEYWORDS = [
    'enterprise', 'b2b', 'saas', 'api', 'sdk', 'infrastructure',
    'database', 'cloud', 'devops', 'mlops', 'security', 'compliance',
    'observability', 'monitoring', 'analytics', 'data pipeline',
]

# Funding-related keywords (signals company may be raising)
FUNDING_KEYWORDS = [
    'series a', 'series b', 'seed', 'raised', 'funding', 'backed by',
    'venture', 'investor', 'capital', 'valuation',
]


@dataclass
class ScoredSignal:
    """Result of scoring an article."""
    company_name: str
    score: int  # 0-100
    signals: Dict[str, Any]
    metadata: Dict[str, Any]


def _has_keywords(text: str, keywords: list) -> bool:
    """Check if text contains any of the keywords."""
    text_lower = text.lower()
    return any(kw in text_lower for kw in keywords)


def _count_keywords(text: str, keywords: list) -> int:
    """Count how many keywords appear in text."""
    text_lower = text.lower()
    return sum(1 for kw in keywords if kw in text_lower)


def _extract_company_name_generic(article: NormalizedArticle) -> str:
    """Extract company name from article title (generic fallback)."""
    title = article.title

    # Try common patterns
    patterns = [
        r'^(?:GitHub Trending: )?([A-Za-z0-9_-]+/[A-Za-z0-9_-]+)',  # GitHub repos
        r'^(?:YC [A-Z]\d+: )?(.+?)(?:\s*[-–—:]\s*.+)?$',  # YC companies
        r'^(?:Delaware Filing: )?(.+?)(?:\s*[-–—:]\s*.+)?$',  # Delaware
        r'^(?:Stealth Signal: )?(.+?)(?:\s*[-–—:]\s*.+)?$',  # LinkedIn
        r'^(.+?)(?:\s*[-–—:]\s*.+)?$',  # Generic: take first part before dash
    ]

    for pattern in patterns:
        match = re.match(pattern, title)
        if match:
            name = match.group(1).strip()
            if name and len(name) > 1:
                return name

    return title.split(' - ')[0].strip() or article.title[:100]


def score_hackernews(article: NormalizedArticle) -> ScoredSignal:
    """
    Score a Hacker News article.

    High signals:
    - Launch HN posts (+30)
    - Show HN posts (+20)
    - High score mentions (+15-25)
    - AI/ML keywords (+15)
    - Enterprise keywords (+10)
    """
    score = 0
    signals = {}
    metadata = {}

    title = article.title.lower()
    text = article.text.lower()
    full_text = f"{title} {text}"

    # Story type from tags
    tags = [t.lower() for t in article.tags]

    # Launch HN is highest signal
    if 'launch_hn' in tags or 'launch hn' in title:
        score += 30
        signals['launch_hn'] = True
    elif 'show_hn' in tags or 'show hn' in title:
        score += 20
        signals['show_hn'] = True

    # Extract score from text (e.g., "Score: 150")
    score_match = re.search(r'score[:\s]+(\d+)', text)
    if score_match:
        hn_score = int(score_match.group(1))
        metadata['hn_score'] = hn_score
        if hn_score >= 200:
            score += 25
            signals['viral'] = True
        elif hn_score >= 100:
            score += 20
            signals['high_score'] = True
        elif hn_score >= 50:
            score += 10
            signals['good_score'] = True

    # Extract comments from text
    comments_match = re.search(r'comments?[:\s]+(\d+)', text)
    if comments_match:
        comments = int(comments_match.group(1))
        metadata['comments'] = comments
        if comments >= 100:
            score += 10
            signals['high_engagement'] = True

    # AI/ML keywords
    ai_count = _count_keywords(full_text, AI_KEYWORDS)
    if ai_count >= 2:
        score += 15
        signals['ai_keywords'] = ai_count
    elif ai_count == 1:
        score += 8

    # Enterprise keywords
    if _has_keywords(full_text, ENTERPRISE_KEYWORDS):
        score += 10
        signals['enterprise'] = True

    # Funding mentions (might be announcing)
    if _has_keywords(full_text, FUNDING_KEYWORDS):
        score += 5
        signals['funding_mention'] = True

    # Extract company name from title
    company_name = _extract_company_name_generic(article)

    return ScoredSignal(
        company_name=company_name,
        score=min(score, 100),
        signals=signals,
        metadata=metadata,
    )


def score_ycombinator(article: NormalizedArticle) -> ScoredSignal:
    """
    Score a Y Combinator batch company.

    High signals:
    - YC batch company (+40 base)
    - AI vertical (+20)
    - B2B/Enterprise (+15)
    - Recent batch (+10)
    """
    score = 40  # Base score for being a YC company
    signals = {'yc_company': True}
    metadata = {}

    title = article.title
    text = article.text.lower()
    full_text = f"{title.lower()} {text}"

    # Extract batch from title (e.g., "YC W24: Company Name")
    batch_match = re.search(r'YC\s*([WS]\d{2})', title, re.IGNORECASE)
    if batch_match:
        batch = batch_match.group(1).upper()
        signals['yc_batch'] = batch
        metadata['batch'] = batch

        # Recent batches get bonus (W24, S24, etc.)
        year_match = re.search(r'([WS])(\d{2})', batch)
        if year_match:
            year = int(year_match.group(2))
            if year >= 24:  # 2024+
                score += 10
                signals['recent_batch'] = True

    # AI/ML keywords
    ai_count = _count_keywords(full_text, AI_KEYWORDS)
    if ai_count >= 2:
        score += 20
        signals['ai_company'] = True
    elif ai_count == 1:
        score += 10

    # Enterprise/B2B keywords
    if _has_keywords(full_text, ENTERPRISE_KEYWORDS):
        score += 15
        signals['b2b'] = True

    # Extract industry from text
    industry_match = re.search(r'industry[:\s]+([^,\n]+)', text)
    if industry_match:
        industry = industry_match.group(1).strip()
        metadata['industry'] = industry

    # Extract company name from title
    company_match = re.match(r'YC\s*[WS]\d+:\s*(.+?)(?:\s*[-–—]\s*.+)?$', title)
    if company_match:
        company_name = company_match.group(1).strip()
    else:
        company_name = _extract_company_name_generic(article)

    return ScoredSignal(
        company_name=company_name,
        score=min(score, 100),
        signals=signals,
        metadata=metadata,
    )


def score_github(article: NormalizedArticle) -> ScoredSignal:
    """
    Score a GitHub trending repository.

    High signals:
    - High stars/day (+20-30)
    - AI/ML repo (+20)
    - Infrastructure/DevTools (+15)
    - Active development (+10)
    """
    score = 0
    signals = {}
    metadata = {}

    title = article.title
    text = article.text.lower()
    full_text = f"{title.lower()} {text}"

    # Extract stars from title (e.g., "GitHub Trending: repo (+150 stars)")
    stars_match = re.search(r'\+(\d+)\s*stars?', title, re.IGNORECASE)
    if stars_match:
        stars_today = int(stars_match.group(1))
        metadata['stars_today'] = stars_today
        if stars_today >= 200:
            score += 30
            signals['viral'] = True
        elif stars_today >= 100:
            score += 25
            signals['high_stars'] = True
        elif stars_today >= 50:
            score += 15
            signals['trending'] = True
        else:
            score += 5

    # Extract total stars from text
    total_match = re.search(r'total\s*stars?[:\s]+(\d+)', text)
    if total_match:
        total_stars = int(total_match.group(1))
        metadata['total_stars'] = total_stars
        if total_stars >= 10000:
            score += 10
            signals['established'] = True

    # Extract language from text
    lang_match = re.search(r'language[:\s]+(\w+)', text)
    if lang_match:
        metadata['language'] = lang_match.group(1)

    # AI/ML repo
    ai_count = _count_keywords(full_text, AI_KEYWORDS)
    if ai_count >= 2:
        score += 20
        signals['ai_repo'] = True
    elif ai_count == 1:
        score += 10

    # Infrastructure/DevTools
    if _has_keywords(full_text, ENTERPRISE_KEYWORDS):
        score += 15
        signals['infrastructure'] = True

    # Extract repo name from title
    repo_match = re.match(r'GitHub Trending:\s*([^(]+)', title)
    if repo_match:
        company_name = repo_match.group(1).strip()
    else:
        company_name = _extract_company_name_generic(article)

    return ScoredSignal(
        company_name=company_name,
        score=min(score, 100),
        signals=signals,
        metadata=metadata,
    )


def score_linkedin(article: NormalizedArticle) -> ScoredSignal:
    """
    Score a LinkedIn jobs stealth signal.

    High signals:
    - Stealth mode indicator (+40)
    - Fund-backed mention (+30)
    - Founding team hiring (+20)
    - AI/ML roles (+15)
    """
    score = 0
    signals = {}
    metadata = {}

    title = article.title
    text = article.text.lower()
    full_text = f"{title.lower()} {text}"
    tags = [t.lower() for t in article.tags]

    # Stealth mode is the primary signal
    if 'stealth' in tags or 'stealth' in full_text:
        score += 40
        signals['stealth_mode'] = True

    # Fund-backed mention
    fund_match = re.search(r'fund[:\s]+([a-z0-9_]+)', text)
    if fund_match or 'fund:' in ' '.join(tags):
        score += 30
        signals['fund_backed'] = True
        # Extract fund name from tags
        for tag in tags:
            if tag.startswith('fund:'):
                metadata['matched_fund'] = tag.replace('fund:', '')
                break

    # Founding team roles
    founding_roles = ['founding', 'co-founder', 'cto', 'ceo', 'head of', 'vp of', 'director of']
    if any(role in full_text for role in founding_roles):
        score += 20
        signals['founding_team'] = True

    # AI/ML hiring
    ai_count = _count_keywords(full_text, AI_KEYWORDS)
    if ai_count >= 1:
        score += 15
        signals['ai_hiring'] = True

    # Engineering roles
    if any(role in full_text for role in ['engineer', 'developer', 'architect']):
        score += 10
        signals['engineering_hire'] = True

    # Extract company name from title
    company_match = re.match(r'Stealth Signal:\s*([^-]+)', title)
    if company_match:
        company_name = company_match.group(1).strip()
    else:
        company_name = _extract_company_name_generic(article)

    return ScoredSignal(
        company_name=company_name,
        score=min(score, 100),
        signals=signals,
        metadata=metadata,
    )


def score_delaware(article: NormalizedArticle) -> ScoredSignal:
    """
    Score a Delaware incorporation signal.

    High signals:
    - Startup-friendly agent (+35)
    - Tech name pattern (+25)
    - Recent formation (+15)
    - AI in name (+15)
    """
    score = 0
    signals = {}
    metadata = {}

    title = article.title
    text = article.text.lower()
    full_text = f"{title.lower()} {text}"
    tags = [t.lower() for t in article.tags]

    # Startup-friendly registered agent
    startup_agents = [
        'stripe atlas', 'clerky', 'legalzoom', 'rocket lawyer',
        'harvard business services', 'northwest registered', 'incorp services'
    ]
    if 'startup_agent' in tags or any(agent in text for agent in startup_agents):
        score += 35
        signals['startup_agent'] = True

    # Tech name pattern
    tech_patterns = [
        r'\b(ai|ml|labs?|tech|technologies|systems|software|platform|cloud|data|analytics|cyber|security)\b',
        r'\b(automation|robotics|bio|health|med|fin|dev|ops|infrastructure)\b',
        r'\b(intelligence|agent|bot|quantum|neural|deep|machine)\b',
    ]
    if 'tech_name' in tags:
        score += 25
        signals['tech_name'] = True
    else:
        for pattern in tech_patterns:
            if re.search(pattern, full_text, re.IGNORECASE):
                score += 25
                signals['tech_name'] = True
                break

    # Recent formation
    if 'formation date' in text:
        # Check if within last 30 days (rough heuristic)
        if any(word in text for word in ['today', 'yesterday', 'this week', 'last week', 'days ago']):
            score += 15
            signals['recent_formation'] = True

    # AI in company name
    ai_name_count = _count_keywords(title, ['ai', 'ml', 'intelligence', 'neural', 'gpt', 'llm'])
    if ai_name_count >= 1:
        score += 15
        signals['ai_name'] = True

    # Extract company name from title
    company_match = re.match(r'Delaware Filing:\s*(.+)', title)
    if company_match:
        company_name = company_match.group(1).strip()
    else:
        company_name = _extract_company_name_generic(article)

    # Extract entity type from text
    entity_match = re.search(r'type[:\s]+(corporation|llc|lp|inc)', text)
    if entity_match:
        metadata['entity_type'] = entity_match.group(1).upper()

    return ScoredSignal(
        company_name=company_name,
        score=min(score, 100),
        signals=signals,
        metadata=metadata,
    )


# Map source names to scoring functions
SCORERS = {
    'hackernews': score_hackernews,
    'ycombinator': score_ycombinator,
    'github_trending': score_github,
    'github': score_github,  # Alias
    'linkedin_jobs': score_linkedin,
    'linkedin': score_linkedin,  # Alias
    'delaware_corps': score_delaware,
    'delaware': score_delaware,  # Alias
}


def score_article(article: NormalizedArticle, source: str) -> Optional[ScoredSignal]:
    """
    Score an article from a given source.

    Args:
        article: NormalizedArticle to score
        source: Source name (hackernews, ycombinator, github_trending, linkedin_jobs, delaware_corps)

    Returns:
        ScoredSignal with company name, score, signals, and metadata
        Returns None if source not recognized
    """
    scorer = SCORERS.get(source)
    if not scorer:
        logger.warning(f"Unknown source for scoring: {source}")
        return None

    try:
        return scorer(article)
    except Exception as e:
        logger.error(f"Error scoring article from {source}: {e}")
        return None
