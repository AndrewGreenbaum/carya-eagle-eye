"""Fund-specific scraper implementations."""

# Original 5 scrapers
from .a16z import A16ZScraper
from .sequoia import SequoiaScraper
from .benchmark import BenchmarkScraper
from .founders_fund import FoundersFundScraper
from .thrive import ThriveScraper

# Additional 13 scrapers
from .khosla import KhoslaScraper
from .index_ventures import IndexVenturesScraper
from .insight import InsightScraper
from .bessemer import BessemerScraper
from .redpoint import RedpointScraper
from .greylock import GreylockScraper
from .gv import GVScraper
from .menlo import MenloScraper
from .usv import USVScraper
from .accel import AccelScraper
from .felicis import FelicisScraper
from .general_catalyst import GeneralCatalystScraper
from .first_round import FirstRoundScraper

# Data sources
from .sec_edgar import SECEdgarScraper, run_sec_edgar_scraper
from .brave_search import BraveSearchScraper, run_brave_search_scraper
from .firecrawl_scraper import FirecrawlScraper, scrape_with_firecrawl, run_firecrawl_scraper
from .google_alerts import GoogleAlertsScraper, run_google_alerts_scraper, get_alert_setup_instructions
from .twitter_monitor import TwitterMonitor, run_twitter_monitor, get_twitter_setup_instructions

# RSS Feeds
from .techcrunch_rss import TechCrunchScraper, run_techcrunch_scraper
from .fortune_term_sheet import FortuneTermSheetScraper, run_fortune_scraper

# Community/Early Signal Sources
from .ycombinator import YCombinatorScraper, run_ycombinator_scraper
from .github_trending import GitHubTrendingScraper, run_github_trending_scraper
from .hackernews import HackerNewsScraper, run_hackernews_scraper

# Stealth Detection
from .linkedin_jobs import LinkedInJobsScraper, run_linkedin_jobs_scraper
from .portfolio_diff import PortfolioDiffScraper, run_portfolio_diff_scraper
from .uspto_trademarks import USPTOTrademarkScraper, run_uspto_trademark_scraper
from .delaware_corps import DelawareCorpsScraper, run_delaware_corps_scraper

# Cross-Reference (SEC + Delaware)
from .sec_delaware_crossref import (
    HighPriorityLead,
    get_high_priority_leads,
    run_crossref_scan,
    PRIORITY_HIGH,
    PRIORITY_STRONG,
    PRIORITY_WATCH,
)

# Additional News Sources
from .tech_funding_news import TechFundingNewsScraper, run_tech_funding_news_scraper
from .ventureburn import VentureburnScraper, run_ventureburn_scraper
from .crunchbase_news import CrunchbaseNewsScraper, run_crunchbase_news_scraper
from .venturebeat import VentureBeatScraper, run_venturebeat_scraper
from .axios_prorata import AxiosProRataScraper, run_axios_prorata_scraper
from .strictlyvc import StrictlyVCScraper, run_strictlyvc_scraper

# PR Wire RSS (PRNewswire, GlobeNewswire, BusinessWire)
from .prwire_rss import PRWireRSSScraper, scrape_prwire_feeds

# Google News RSS (external-only fund coverage)
from .google_news_rss import GoogleNewsRSSScraper, run_google_news_scraper

__all__ = [
    # Original 5
    "A16ZScraper",
    "SequoiaScraper",
    "BenchmarkScraper",
    "FoundersFundScraper",
    "ThriveScraper",
    # Additional 13
    "KhoslaScraper",
    "IndexVenturesScraper",
    "InsightScraper",
    "BessemerScraper",
    "RedpointScraper",
    "GreylockScraper",
    "GVScraper",
    "MenloScraper",
    "USVScraper",
    "AccelScraper",
    "FelicisScraper",
    "GeneralCatalystScraper",
    "FirstRoundScraper",
    # Data sources
    "SECEdgarScraper",
    "run_sec_edgar_scraper",
    "BraveSearchScraper",
    "run_brave_search_scraper",
    "FirecrawlScraper",
    "scrape_with_firecrawl",
    "run_firecrawl_scraper",
    # Social/Alerts
    "GoogleAlertsScraper",
    "run_google_alerts_scraper",
    "get_alert_setup_instructions",
    "TwitterMonitor",
    "run_twitter_monitor",
    "get_twitter_setup_instructions",
    # RSS Feeds
    "TechCrunchScraper",
    "run_techcrunch_scraper",
    "FortuneTermSheetScraper",
    "run_fortune_scraper",
    # Community/Early Signal Sources
    "YCombinatorScraper",
    "run_ycombinator_scraper",
    "GitHubTrendingScraper",
    "run_github_trending_scraper",
    "HackerNewsScraper",
    "run_hackernews_scraper",
    # Stealth Detection
    "LinkedInJobsScraper",
    "run_linkedin_jobs_scraper",
    "PortfolioDiffScraper",
    "run_portfolio_diff_scraper",
    "USPTOTrademarkScraper",
    "run_uspto_trademark_scraper",
    "DelawareCorpsScraper",
    "run_delaware_corps_scraper",
    # Cross-Reference (SEC + Delaware)
    "HighPriorityLead",
    "get_high_priority_leads",
    "run_crossref_scan",
    "PRIORITY_HIGH",
    "PRIORITY_STRONG",
    "PRIORITY_WATCH",
    # Additional News Sources
    "TechFundingNewsScraper",
    "run_tech_funding_news_scraper",
    "VentureburnScraper",
    "run_ventureburn_scraper",
    "CrunchbaseNewsScraper",
    "run_crunchbase_news_scraper",
    "VentureBeatScraper",
    "run_venturebeat_scraper",
    "AxiosProRataScraper",
    "run_axios_prorata_scraper",
    "StrictlyVCScraper",
    "run_strictlyvc_scraper",
    # PR Wire RSS
    "PRWireRSSScraper",
    "scrape_prwire_feeds",
    # Google News RSS
    "GoogleNewsRSSScraper",
    "run_google_news_scraper",
]
