"""
Fund Registry - Configuration for 18 elite VC firms.

Each fund has specific ingestion targets, disambiguation rules, and extraction hints.
"""

from dataclasses import dataclass, field
from typing import List, Optional
from enum import Enum


class ScraperType(Enum):
    """Type of scraper needed for the fund."""
    HTML = "html"           # Simple HTML parsing
    RSS = "rss"             # RSS feed
    PLAYWRIGHT = "playwright"  # JavaScript hydration required
    EXTERNAL = "external"    # PRN/Business Wire aggregators
    DOM_DIFF = "dom_diff"    # Sitemap/portfolio page diffing


@dataclass
class FundConfig:
    """Configuration for a single VC fund."""
    name: str
    slug: str
    ingestion_url: str
    scraper_type: ScraperType

    # Disambiguation
    negative_keywords: List[str] = field(default_factory=list)
    required_keywords: List[str] = field(default_factory=list)

    # Extraction hints
    partner_names: List[str] = field(default_factory=list)
    extraction_notes: str = ""

    # Special handling
    stealth_monitor_path: Optional[str] = None  # Path to monitor for stealth additions
    vertical_filter: Optional[List[str]] = None  # Filter by vertical (crypto, bio, etc.)


# The 18 Elite VC Funds Registry
FUND_REGISTRY: dict[str, FundConfig] = {
    "founders_fund": FundConfig(
        name="Founders Fund",
        slug="founders_fund",
        ingestion_url="https://foundersfund.com/",
        scraper_type=ScraperType.PLAYWRIGHT,
        stealth_monitor_path="/portfolio/",
        extraction_notes="Homepage + blog posts (/YYYY/MM/ pattern). No /news/ page exists.",
        # UPDATED 2026-01: Current partners + notable former
        partner_names=[
            # Current
            "Peter Thiel", "Napoleon Ta", "Trae Stephens", "Lauren Gross",
            "Scott Nolan", "John Luttig", "Delian Asparouhov", "Joey Krug",
            "Matias Van Thienen", "Amin Mirzadegan", "Sean Liu",
            # Former (still newsworthy)
            "Keith Rabois", "Brian Singerman"
        ]
    ),

    "benchmark": FundConfig(
        name="Benchmark",
        slug="benchmark",
        ingestion_url="external:prn,businesswire",
        scraper_type=ScraperType.EXTERNAL,
        negative_keywords=["International", "Electronics", "Benchmark Capital Management"],
        extraction_notes="Must include Partner names in article. Filter NOT 'International', NOT 'Electronics'.",
        # UPDATED 2026-01: Current GPs + former partners still referenced in deals
        partner_names=[
            # Current GPs
            "Chetan Puttagunta", "Peter Fenton", "Eric Vishria",
            # Former (still newsworthy)
            "Bill Gurley", "Sarah Tavel", "Miles Grimshaw", "Victor Lazarte"
        ]
    ),

    "sequoia": FundConfig(
        name="Sequoia Capital",
        slug="sequoia",
        ingestion_url="https://sequoiacap.com/stories",
        scraper_type=ScraperType.HTML,
        negative_keywords=["HongShan", "Peak XV", "Sequoia India", "Sequoia China"],
        extraction_notes="Distinguish from HongShan/Peak XV. Parse Author byline for Partner."
    ),

    "khosla": FundConfig(
        name="Khosla Ventures",
        slug="khosla",
        ingestion_url="https://www.khoslaventures.com/posts/rss.xml",
        scraper_type=ScraperType.RSS,
        extraction_notes="RSS feed of blog posts; deep-tech focus.",
        # UPDATED 2026-01: Managing Directors + key investors
        partner_names=[
            # Managing Directors
            "Vinod Khosla", "Samir Kaul", "Sven Strohband", "David Weiden", "Keith Rabois",
            # Key Investors
            "Ethan Choi", "Kanu Gulati", "Alex Morgan", "Jai Sajnani"
        ]
    ),

    "index": FundConfig(
        name="Index Ventures",
        slug="index",
        ingestion_url="https://indexventures.com/perspectives",
        scraper_type=ScraperType.HTML,
        extraction_notes="Distinguish 'Double Down' (follow-on) from new Leads."
    ),

    "a16z": FundConfig(
        name="a16z",
        slug="a16z",
        ingestion_url="https://a16z.com/news-content/",
        scraper_type=ScraperType.HTML,
        vertical_filter=["crypto", "bio", "games", "fintech", "enterprise"],
        extraction_notes="Filter by vertical (Crypto, Bio, Games). RSS available."
    ),

    "insight": FundConfig(
        name="Insight Partners",
        slug="insight",
        ingestion_url="https://insightpartners.com/about-us/media/",
        scraper_type=ScraperType.HTML,
        extraction_notes="Filter 'ScaleUp' vs. standard rounds. Growth-stage focus."
    ),

    "bessemer": FundConfig(
        name="Bessemer Venture Partners",
        slug="bessemer",
        ingestion_url="https://bvp.com/news",
        scraper_type=ScraperType.HTML,
        negative_keywords=["BVP Forge"],
        extraction_notes="Flag 'BVP Forge' as PE/Buyout, not venture."
    ),

    "redpoint": FundConfig(
        name="Redpoint",
        slug="redpoint",
        ingestion_url="https://www.redpoint.com/content-hub/",
        scraper_type=ScraperType.PLAYWRIGHT,  # Gatsby/React requires JS rendering
        required_keywords=["Funding News"],
        extraction_notes="MUST use www. prefix. Filter by 'Funding News' tag. Gatsby/React site - requires Playwright."
    ),

    "greylock": FundConfig(
        name="Greylock",
        slug="greylock",
        ingestion_url="https://greylock.com/portfolio-news/",
        scraper_type=ScraperType.PLAYWRIGHT,
        extraction_notes="Portfolio news page with inline content (no individual URLs). Requires Playwright.",
        # UPDATED 2026-01: Current partners + venture partners
        partner_names=[
            "Reid Hoffman", "Asheem Chandna", "Jerry Chen", "Saam Motamedi",
            "Seth Rosenberg", "Mike Duboe", "David Sze", "Josh Elman",
            "Corinne Riley", "Christine Kim", "Mor Chen", "Shreya Shekhar"
        ]
    ),

    "gv": FundConfig(
        name="GV (Google Ventures)",
        slug="gv",
        ingestion_url="https://gv.com/news/",
        scraper_type=ScraperType.HTML,
        negative_keywords=["Visionary Holdings", "GV Ticker", "NYSE:GV"],
        extraction_notes="CRITICAL: Exclude 'Visionary Holdings' (Ticker: GV).",
        # UPDATED 2026-01: Current GPs
        partner_names=[
            "M.G. Siegler", "Crystal Huang", "Terri Burns", "Krishna Yeshwant",
            "Frédérique Dame", "David Krane", "Tyson Clark", "Brendan Bulik-Sullivan",
            "Sangeen Zeb", "Issi Rozen"
        ]
    ),

    "menlo": FundConfig(
        name="Menlo Ventures",
        slug="menlo",
        ingestion_url="https://menlovc.com/perspective/",
        scraper_type=ScraperType.HTML,
        extraction_notes="Perspective/blog page; focus on AI investments."
    ),

    "usv": FundConfig(
        name="Union Square Ventures",
        slug="usv",
        ingestion_url="https://usv.com/blog",
        scraper_type=ScraperType.HTML,
        extraction_notes="NLP must read body for 'We are leading...' phrasing."
    ),

    "thrive": FundConfig(
        name="Thrive Capital",
        slug="thrive",
        ingestion_url="external:prn,businesswire,techcrunch",
        scraper_type=ScraperType.EXTERNAL,
        negative_keywords=["Thrive IT Services", "Thrive Global", "Thrive Market"],
        extraction_notes="CRITICAL: Exclude 'Thrive IT Services' or 'Thrive Global'.",
        # UPDATED 2026-01: Current team (~9 investors)
        partner_names=[
            "Josh Kushner", "Kareem Zaki", "Nabil Mallick", "Belen Mella",
            "Gaurav Ahuja", "Joe Kahn", "Kyle Yuan"
        ]
    ),

    "accel": FundConfig(
        name="Accel",
        slug="accel",
        ingestion_url="https://accel.com/news",
        scraper_type=ScraperType.HTML,
        extraction_notes="Identify London vs. US vs. India teams."
    ),

    "felicis": FundConfig(
        name="Felicis Ventures",
        slug="felicis",
        ingestion_url="https://felicis.com/insights",
        scraper_type=ScraperType.HTML,
        required_keywords=["Welcome to the family", "portfolio"],
        extraction_notes="Look for 'Welcome to the family' keywords."
    ),

    "general_catalyst": FundConfig(
        name="General Catalyst",
        slug="general_catalyst",
        ingestion_url="https://www.generalcatalyst.com/stories",
        scraper_type=ScraperType.HTML,
        extraction_notes="Stories page for news; watch for 'Hatching/Co-creation' (Incubations)."
    ),

    "first_round": FundConfig(
        name="First Round Capital",
        slug="first_round",
        ingestion_url="https://review.firstround.com",
        scraper_type=ScraperType.HTML,
        extraction_notes="Monitor 'Community' page/Twitter for deal news.",
        # UPDATED 2026-01: Current partners
        partner_names=[
            "Josh Kopelman", "Todd Jackson", "Brett Berson", "Bill Trenchard",
            "Meka Asonye", "Liz Wessel", "Hayley Barna"
        ]
    ),
}


def get_fund(slug: str) -> FundConfig:
    """Get fund configuration by slug."""
    if slug not in FUND_REGISTRY:
        raise ValueError(f"Unknown fund: {slug}")
    return FUND_REGISTRY[slug]


def get_all_funds() -> List[FundConfig]:
    """Get all fund configurations."""
    return list(FUND_REGISTRY.values())


def get_funds_by_scraper_type(scraper_type: ScraperType) -> List[FundConfig]:
    """Get funds that use a specific scraper type."""
    return [f for f in FUND_REGISTRY.values() if f.scraper_type == scraper_type]


# =============================================================================
# EXTERNAL-ONLY FUNDS (Single source of truth)
# =============================================================================
# Funds without working website scrapers - deals come from external news sources
# with shorter/less reliable content. These need more lenient processing.

def get_external_only_fund_slugs() -> frozenset[str]:
    """
    Get slugs of funds that are 'external-only' (no working website scraper).

    External-only funds need more lenient processing because:
    - Content comes from news sources, not fund websites
    - Articles may be shorter or less detailed
    - Headlines may be the only content available

    This is the SINGLE SOURCE OF TRUTH for external-only classification.
    Do not hardcode this list elsewhere - import and use this function.

    Returns:
        frozenset of fund slugs that are external-only
    """
    # Funds with ScraperType.EXTERNAL are definitively external-only
    external_type_funds = {
        f.slug for f in FUND_REGISTRY.values()
        if f.scraper_type == ScraperType.EXTERNAL
    }

    # Additional funds with low website scraper coverage
    # These have scrapers but coverage is unreliable, so external sources are primary
    low_coverage_funds = {
        "greylock",      # Playwright scraper exists but coverage is low
        "gv",            # HTML scraper exists but external sources more reliable
        "redpoint",      # Playwright required, often fails
        "founders_fund", # Playwright, homepage blog is sparse
        "first_round",   # No portfolio announcements page
        "khosla",        # RSS exists but rarely updated
    }

    return frozenset(external_type_funds | low_coverage_funds)


# Pre-computed at module load for performance
EXTERNAL_ONLY_FUNDS: frozenset[str] = get_external_only_fund_slugs()
