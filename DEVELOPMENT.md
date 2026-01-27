# Carya Eagle Eye

Deal intelligence: **lead investments** from 18 VC firms in **Enterprise AI** startups.

## URLs & Costs
- Frontend: https://carya-eagle-eye.up.railway.app
- Backend: https://bud-tracker-backend-production.up.railway.app
- ~$70/mo (~$28 LLM + $30 Brave + $10 Railway)

## Architecture
```
src/main.py              # FastAPI
src/analyst/extractor.py # Claude extraction
src/archivist/storage.py # Deduplication
src/harvester/scrapers/  # 40+ scrapers
src/harvester/base_scraper.py # Shared retry logic, health alerts
src/enrichment/          # Website + LinkedIn
src/scheduler/jobs.py    # Cron jobs
src/common/url_utils.py  # URL validation
src/common/http_client.py # Shared HTTP client config
frontend/                # React + Vite + Tailwind
```

## 18 Tracked Funds
| Fund | Slug | Critical Notes |
|------|------|----------------|
| a16z | `a16z` | - |
| Accel | `accel` | Exclude "Accel Entertainment" |
| Benchmark | `benchmark` | EXTERNAL ONLY |
| Bessemer | `bessemer` | Flag BVP Forge as PE |
| Felicis | `felicis` | - |
| First Round | `first_round` | EXTERNAL ONLY |
| Founders Fund | `founders_fund` | EXTERNAL, Playwright |
| General Catalyst | `general_catalyst` | - |
| Greylock | `greylock` | EXTERNAL, Playwright |
| GV | `gv` | **CRITICAL: Exclude NYSE:GV ticker** |
| Index | `index` | - |
| Insight | `insight` | Growth-stage |
| Khosla | `khosla` | EXTERNAL ONLY |
| Menlo | `menlo` | - |
| Redpoint | `redpoint` | EXTERNAL, Playwright |
| Sequoia | `sequoia` | **US ONLY - exclude HongShan/Peak XV** |
| Thrive | `thrive` | **CRITICAL: Exclude Thrive IT/Global/Market** |
| USV | `usv` | - |

**External-only (8):** benchmark, first_round, founders_fund, greylock, gv, khosla, redpoint, thrive → Brave + Google News RSS

## Classification
**Enterprise AI:** Infrastructure, Security, Vertical SaaS (B2B), Agentic, Data Intelligence
**Exclude:** Consumer AI, gaming, crypto/Web3, social/dating

**Lead phrases:** "led by", "leads", "co-led", "headed by", "spearheaded"
**NOT lead:** "backed by", "participated in", "invested in", "joined"

## Key Endpoints
```
POST /scrapers/run/{fund_slug}
POST /scrapers/all-sources?days=7
POST /scrapers/sec-edgar?hours=24
POST /scrapers/google-news?days_back=30
GET  /deals?is_lead=true&is_enterprise_ai=true&fund_slug=a16z
POST /enrichment/deals?limit=50&offset=0&skip_enriched=true
GET  /health | /scheduler/status | /scans
```
**API Key:** `X-API-Key: dev-key`

## Database
**Tables:** deals, funds, articles, tracker_items, scan_jobs, token_usage, portfolio_companies, content_hashes

**Key fields:** `amount_usd` (BIGINT), `amount_source` (sec_form_d|crunchbase|article), `is_enterprise_ai`, `is_lead_confirmed`, `lead_evidence_weak`, `enterprise_category`, `verification_snippet`, `founders_json`, `dedup_key`, `amount_dedup_key`

**content_hashes table** (2026-01): Persistent content dedup cache
- `content_hash` - SHA256 first 128 bits
- `content_length` - For preferring longer articles
- `expires_at` - 30-day TTL, auto-cleanup on scan start

## LLM Pipeline
```
Article → Source Filter → Title Filter → Content Dedup → Keyword Filter → Crypto Filter → Haiku → Post-Processing → [Sonnet re-extract?] → Save
```

**Pre-extraction filters** (2026-01):
- `should_skip_by_source()` - Source-specific rules (Crunchbase /reviews/, YC /apply/, LinkedIn /jobs/)
- `check_global_content_seen()` - Content hash dedup (in-memory + persistent DB cache, 30-day TTL)
- `is_non_announcement_title()` - Skip year-in-review, interviews, job postings
- `is_likely_funding_from_title()` - Require funding signals in title

**Post-processing (13 steps):** *(all tracked in extraction_filter_stats)*
1. `_validate_round_type()` - correct invalid round types to UNKNOWN
2. `_validate_confidence_score()` - clamp confidence to [0, 1], reset NaN/Inf to 0
3. `_validate_company_in_text()` - reject hallucinated company names
4. `_validate_startup_not_fund()` - reject LP/fund structures (SEC Form D false positives)
5. `_validate_founders_in_text()` - remove hallucinated founder names
6. `_validate_investors_in_text()` - remove hallucinated investors (word boundary matching)
7. `_verify_tracked_fund()` - downgrade weak evidence to LIKELY_LEAD
8. `_is_crypto_deal()` → NOT_AI (threshold=4, AI signals protect legitimate deals)
9. `_is_consumer_ai_deal()` → is_enterprise_ai=False
10. `_is_consumer_fintech_deal()` → NOT_AI
11. `_looks_like_article_title()` - reject titles as names
12. `_is_background_mention()` - reject non-headline companies (proportional threshold)
13. `_validate_deal_amount()` - flag suspicious amounts (updated 2025/2026 thresholds)

**Fund structure rejection** (2026-01): Catches SEC Form D filings for LP structures:
- Names ending with "Fund I/II/.../XVI" (roman numerals)
- Names ending with "Fund 1/2/.../N" (numbers)
- Names ending with ", LP" / ", LLC" / ", LLP"
- Fund codes like "SP-1216 Fund I", "AU-0707 Fund III"
- Safe: "Fundrise", "GoFundMe", "FundBox" (Fund not at end)

**Confidence thresholds:** Default=0.50, External=0.40
**Hybrid re-extract (2026-01):**
- Internal sources: 0.45-0.65 confidence + weak evidence → try Sonnet
- External sources: 0.35-0.65 confidence + weak evidence → try Sonnet (lower threshold for headlines)
- High-conf path: >0.65 + weak evidence + tracked lead → try Sonnet
**Confidence penalties:** Hallucinated founders (-0.03 each, max -0.10), investors (-0.05 each, max -0.15), weak evidence (-0.08)
**Early exit scores:** No funding keywords=0.05, Crypto=0.15 (differentiated for debugging)

**Amount priority:** SEC Form D > Crunchbase > Article
**Valuation vs funding:** "$50M at $500M valuation" → amount=$50M (NOT $500M)

## Deduplication (`storage.py:find_duplicate_deal`)
| Tier | Match Criteria |
|------|----------------|
| 0 | Same company + round + date ±3 days (race condition) |
| 4 | 60% prefix overlap + amount ±5% + date + round (name variations) |
| 3 | Same amount ±10% + company + date ±30 days (cross-round) |
| 2.5 | Same company + round + date ±30 days + amount sanity check |
| 2 | Same company + exact date |
| 1 | Same company + round + amount ±15% (SQL-filtered by round_type) |

**2026-01 Fixes (batch 1):**
- TIER 2.5 extended from ±7 to ±30 days to catch duplicates like Emergent/Khosla where dates differed by 19 days
- Date update logic: Now prefers **higher confidence dates** over earlier dates (fixes Emergent bug where wrong Jan 1 date was kept instead of correct Jan 20)
- Placeholder date detection: Jan 1 and quarter-start dates (Apr 1, Jul 1, Oct 1) are flagged as likely extraction errors

**2026-01 Fixes (batch 2):**
- TIER 4: Requires 60% prefix overlap (not just 3 chars) to prevent "MEQ Probe" vs "MEQ Consulting" false matches
- TIER 2.5: Amount sanity check - blocks matches with >5x amount ratio UNLESS larger amount ≥$500M (valuation confusion)
- TIER 2.5: Null-date handling - checks recent deals (30 days by created_at) when incoming date is null
- TIER 1: SQL-level round_type filter to avoid LIMIT 200 missing duplicates
- TIER 1: Tighter date window (60 days) when one/both amounts are missing

**2026-01 Fixes (batch 3 - storage & scheduler):**
- **Advisory lock fail-fast:** `save_deal()` now raises exception if advisory lock fails (was silent continue)
- **scalar_one() race handling:** `get_or_create_company()` and `save_stealth_detection()` use `scalar_one_or_none()` with retry
- **HTTP client race fix:** `_get_article_fetch_client()` now acquires lock BEFORE first None check
- **SEC rate limiting:** Token bucket pattern instead of sleep-inside-semaphore (3x faster)
- **Exception counting:** `asyncio.gather` exceptions now increment `stats["errors"]`
- **Content hash conservative:** Returns True (skip) on DB error to save Claude API costs
- **Empty name fallback:** `company_names_match()` falls back to case-insensitive comparison when both normalize to empty
- **Amount precision:** `format_sec_amount()` uses Decimal for exact representation
- **Parallel alerts:** Alert sending uses `asyncio.gather` instead of serial loop
- **GoogleNews cleanup warning:** `__del__` warns if HTTP clients not properly closed

**Two dedup keys:**
- `dedup_key = MD5(name|round|date_bucket)` - exact match
- `amount_dedup_key = MD5(name|amt_bucket|date_bucket)` - catches LLM round inconsistency (≥$250K threshold)

**Company matching:** Uses consolidated `COMPANY_NAME_SUFFIXES` list:
- Legal: Inc, LLC, Ltd, Corp, Co, Corporation, Incorporated, Limited, Company
- Tech: Labs, Lab, Tech, AI, Health, Cloud, ML, Ops, Dev, HQ, App, IO

## Enrichment
```
POST /enrichment/deals?limit=50&offset=0&skip_enriched=false&force_update=true
```
**LinkedIn queries:** `site:linkedin.com/in "{founder}" "{company}"` → fallback searches
**Website:** LLM extraction → Brave fallback

## URL Validation (`url_utils.py`)
- `is_valid_url()` - rejects placeholders (Unknown, N/A, pending)
- `is_valid_linkedin_profile()` - requires `/in/` + 3-char username
- `is_valid_linkedin_company()` - requires `/company/`
- `sanitize_url()` - www→https, http→https

## LinkedIn Enrichment (`brave_enrichment.py`)
- **Non-person slug rejection:** Rejects `/in/` URLs ending in: postings, jobs, careers, hiring, official, team, admin, support, contact, sales, recruiting, talent, openings
- **Name matching:** Requires BOTH first AND last name to match (not just one)
- **CEO fallback validation:** Cross-validates against existing founders to prevent mismatches
- **Cache:** 30-day LinkedIn result caching with `asyncio.Lock` for thread safety
- **URL regex:** Anchored to prevent false matches (e.g., `notlinkedin.com`)
- **Website verification:** Checks final URL domain after redirects (rejects redirect to linkedin.com, crunchbase.com, etc.)
- **URL validation:** All URLs sanitized and validated before DB persistence (`sanitize_url()` + `is_valid_website_url()`)

## Frontend
**Stack:** React + Vite + Tailwind
**Mobile:** Hidden stats cards, card layout for deals, drawer sidebar
**Breakpoints:** sm:640px, md:768px, lg:1024px
**Touch targets:** 44px minimum

**Date parsing (timezone fix):**
```javascript
// WRONG: new Date("2026-01-05") - UTC midnight shifts day
// RIGHT: split and construct local
const [y,m,d] = "2026-01-05".split("-").map(Number);
new Date(y, m-1, d);
```

**Filters:** AI master toggle → Enterprise AI sub-toggle → defaults both ON + Lead ON

## Development

**IMPORTANT: Always use the Python 3.11 virtual environment for all Python commands:**
```bash
source venv/bin/activate  # REQUIRED before running any Python/pytest commands
```

```bash
# Local
source venv/bin/activate && python -m uvicorn src.main:app --reload --port 8000
cd frontend && npm run dev

# Tests (requires venv activation)
source venv/bin/activate && python -m pytest tests/ -v

# Deploy
railway up --service bud-tracker-backend
cd frontend && npm run build && railway up --service bud-tracker-frontend

# Migrations
source venv/bin/activate && python -m alembic upgrade head
```

## Crunchbase Import
```bash
python3 scripts/process_crunchbase_csv.py ~/Downloads/export.csv
python3 scripts/process_crunchbase_csv.py ~/Downloads/export.csv --dry-run
```

## Scraper Reliability (`base_scraper.py`, `techcrunch_rss.py`, `brave_search.py`, `extractor.py`)
- **Retry logic:** 3 retries with exponential backoff + jitter (avoids thundering herd)
- **Jitter:** `delay * random.uniform(0.9, 1.1)` on all retry delays (scrapers + Claude API calls)
- **4xx handling:** Fail fast (no retry) - article doesn't exist or paywall
- **5xx handling:** Retry with backoff, log error after exhaustion
- **Health alerts:** `SCRAPER_HEALTH_ALERT` logged when scrapers return 0 results
- **Shared retry:** `BaseScraper._fetch_with_retry()` used by all HTML scrapers
- **Error logging:** All exceptions logged with `exc_info=True` for stack traces
- **Cache:** 12-hour TTL with automatic cleanup every 100 operations
- **Selector logging:** Debug logs when all CSS selectors fail (helps detect site changes)
- **Title validation:** Skip articles with empty/whitespace titles
- **Date parsing:** Debug logs for failed date parsing attempts

**Centralized settings** (`settings.py`):
```python
request_timeout = 30           # Main scraper timeout
article_fetch_timeout = 15     # Article fetch timeout
article_rate_limit_delay = 0.3 # Delay between article fetches
max_concurrent_articles = 5    # Parallel article fetches
max_concurrent_feeds = 5       # Parallel feed fetches
```

**Shared HTTP client** (`common/http_client.py`):
```python
USER_AGENT_BOT = "BudTracker/1.0 (Investment Research Bot)"
USER_AGENT_BROWSER = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)..."
create_scraper_client(user_agent, timeout, ...)
```

## Monitoring (`jobs.py`, `extractor.py`)
**Health metrics** in `_health_metrics` response field:
- `per_source_stats`: {source: {articles, deals, leads, enterprise_ai}}
- `confidence_bands`: {below_threshold, borderline, medium, high}
- `rejection_stats`: {confidence_too_low, not_new_announcement, invalid_company_name}
- `extraction_filter_stats`: {crypto_filtered, consumer_ai_filtered, lead_evidence_downgraded, ...}

**Log markers** (grep in Railway):
- `SCRAPER_HEALTH_ALERT` - scrapers returning 0 results
- `CONFIDENCE_BANDS` - confidence score distribution per source
- `REJECTION_STATS` - why deals weren't saved
- `EXTRACTION_FILTER_STATS` - post-processing filter activity
- `HYBRID_RESULT` / `HYBRID_RESULT_HIGH_CONF` - Sonnet re-extraction outcomes
- `HYBRID_FAILED` / `HYBRID_FAILED_HIGH_CONF` - Sonnet re-extraction errors (falls back to Haiku)
- `INVALID_CONFIDENCE` - NaN/Inf confidence from LLM (serious parsing failure)

**Cost optimizations** (2026-01):
- Persistent content dedup: ~$10-15/mo savings (cross-run syndication catch)
- Source-specific filtering: ~$5-10/mo savings (skip non-funding pages)
- Tighter Sonnet threshold: ~$2-3/mo savings (0.45 for internal sources)
- Compiled crypto regex: Minor CPU savings (single regex vs 35 string searches)

## Troubleshooting
| Issue | Fix |
|-------|-----|
| Scraper 0 results | Check logs for `SCRAPER_HEALTH_ALERT`, selectors may have changed |
| DB connection | `alembic current` then `upgrade head` |
| Frontend not loading | CORS, `.env.production`, rebuild |
| Brave 422 | Query too long |
| Dates off by 1 | Use date component parsing |
| Content hash table missing | Run `alembic upgrade head` (migration: `20260121_content_hashes`) |

## Critical Rules
1. **GV:** NEVER confuse with NYSE:GV stock ticker
2. **Thrive:** EXCLUDE Thrive IT Services, Thrive Global, Thrive Market
3. **Sequoia:** US ONLY - exclude HongShan, Peak XV, India, China
4. **Lead evidence:** Weak evidence → LIKELY_LEAD, not rejected
5. **Dates:** Reject >365 days old
6. **Crypto/Consumer:** Auto-reclassified, not Enterprise AI
