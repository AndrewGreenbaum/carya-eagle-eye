/**
 * Bud Tracker API Client
 *
 * Comprehensive API client for all backend endpoints.
 * Supports caching, error handling, and TypeScript types.
 */

import type {
  Deal,
  Fund,
  Founder,
  PaginatedDeals,
  DealFilters,
  HealthResponse,
  ScraperStatus,
  SchedulerStatusResponse,
  StealthDetectionsResponse,
  StealthSignal,
  StealthSignalsResponse,
  StealthSignalStats,
  StealthSignalSource,
  ScrapeResponse,
  SECEdgarResponse,
  BraveSearchResponse,
  NewsAggregatorResponse,
  FirecrawlResponse,
  AllSourcesResponse,
  ExtractionRequest,
  ExtractionResponse,
  InvestorRole,
  InvestmentStage,
  EnterpriseCategory,
} from '../types';

// ============================================
// Configuration
// ============================================

const API_BASE = import.meta.env.VITE_API_BASE || '/api';
const API_KEY = import.meta.env.VITE_API_KEY || 'dev-key';

// ============================================
// Cache Infrastructure
// ============================================

interface CacheEntry<T> {
  data: T;
  timestamp: number;
}

const cache = new Map<string, CacheEntry<unknown>>();
const CACHE_TTL = 300000; // 5 minutes

function getCached<T>(key: string): T | null {
  const entry = cache.get(key);
  if (entry && Date.now() - entry.timestamp < CACHE_TTL) {
    return entry.data as T;
  }
  cache.delete(key);
  return null;
}

function setCache<T>(key: string, data: T): void {
  cache.set(key, { data, timestamp: Date.now() });
}

export function invalidateCache(prefix?: string): void {
  if (prefix) {
    for (const key of cache.keys()) {
      if (key.startsWith(prefix)) {
        cache.delete(key);
      }
    }
  } else {
    cache.clear();
  }
}

// ============================================
// Timeout & Error Handling
// ============================================

const DEFAULT_TIMEOUT_MS = 30000; // 30s - matches backend

export class ApiTimeoutError extends Error {
  constructor(message = 'Request timed out') {
    super(message);
    this.name = 'ApiTimeoutError';
  }
}

async function fetchWithTimeout(
  url: string,
  options: RequestInit = {},
  timeoutMs = DEFAULT_TIMEOUT_MS
): Promise<Response> {
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), timeoutMs);

  try {
    return await fetch(url, { ...options, signal: controller.signal });
  } catch (error) {
    if (error instanceof Error && error.name === 'AbortError') {
      throw new ApiTimeoutError(`Request timed out after ${timeoutMs / 1000}s`);
    }
    throw error;
  } finally {
    clearTimeout(timeoutId);
  }
}

// ============================================
// Request Helpers
// ============================================

async function apiGet<T>(path: string, useCache = true): Promise<T> {
  const cacheKey = `GET:${path}`;

  if (useCache) {
    const cached = getCached<T>(cacheKey);
    if (cached) return cached;
  }

  const response = await fetchWithTimeout(`${API_BASE}${path}`, {
    headers: {
      'X-API-Key': API_KEY,
    },
  });

  if (!response.ok) {
    const error = await response.json().catch(() => ({ detail: response.statusText }));
    throw new Error(error.detail || `API Error: ${response.status}`);
  }

  const data = await response.json();
  if (useCache) setCache(cacheKey, data);
  return data;
}

async function apiPost<T>(path: string, body?: unknown): Promise<T> {
  const response = await fetchWithTimeout(`${API_BASE}${path}`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'X-API-Key': API_KEY,
    },
    body: body ? JSON.stringify(body) : undefined,
  });

  if (!response.ok) {
    const error = await response.json().catch(() => ({ detail: response.statusText }));
    throw new Error(error.detail || `API Error: ${response.status}`);
  }

  return response.json();
}

async function apiPut<T>(path: string, body?: unknown): Promise<T> {
  const response = await fetchWithTimeout(`${API_BASE}${path}`, {
    method: 'PUT',
    headers: {
      'Content-Type': 'application/json',
      'X-API-Key': API_KEY,
    },
    body: body ? JSON.stringify(body) : undefined,
  });

  if (!response.ok) {
    const error = await response.json().catch(() => ({ detail: response.statusText }));
    throw new Error(error.detail || `API Error: ${response.status}`);
  }

  return response.json();
}

async function apiDelete(path: string): Promise<void> {
  const response = await fetchWithTimeout(`${API_BASE}${path}`, {
    method: 'DELETE',
    headers: {
      'X-API-Key': API_KEY,
    },
  });

  if (!response.ok) {
    const error = await response.json().catch(() => ({ detail: response.statusText }));
    throw new Error(error.detail || `API Error: ${response.status}`);
  }
}

// ============================================
// Response Mappers (snake_case -> camelCase)
// ============================================

interface FounderApiResponse {
  name: string;
  title?: string;
  linkedin_url?: string;
}

interface DealApiResponse {
  id: string;
  startup_name: string;
  investor_roles: string[];
  investment_stage: string;
  amount_invested: string;
  date: string;
  next_steps?: string;
  enterprise_category?: string;
  is_enterprise_ai: boolean;
  is_ai_deal: boolean;
  lead_investor?: string;
  lead_partner?: string;
  verification_snippet?: string;
  lead_evidence_weak?: boolean;
  // Company links
  company_website?: string;
  company_linkedin?: string;
  // Source tracking
  source_url?: string;
  source_name?: string;
  // Founders
  founders?: FounderApiResponse[];
}

interface PaginatedDealsApiResponse {
  deals: DealApiResponse[];
  total: number;
  limit: number;
  offset: number;
  has_more: boolean;
}

function mapDeal(api: DealApiResponse): Deal {
  // Map founders from snake_case to camelCase
  const founders: Founder[] | undefined = api.founders?.map((f) => ({
    name: f.name,
    title: f.title,
    linkedinUrl: f.linkedin_url,
  }));

  return {
    id: api.id,
    startupName: api.startup_name,
    investorRoles: api.investor_roles as InvestorRole[],
    investmentStage: api.investment_stage as InvestmentStage,
    amountInvested: api.amount_invested,
    date: api.date,
    nextSteps: api.next_steps,
    enterpriseCategory: api.enterprise_category as EnterpriseCategory | undefined,
    isEnterpriseAi: api.is_enterprise_ai ?? false,
    isAiDeal: api.is_ai_deal ?? api.is_enterprise_ai ?? false,  // Fallback for backwards compat
    leadInvestor: api.lead_investor,
    leadPartner: api.lead_partner,
    verificationSnippet: api.verification_snippet,
    leadEvidenceWeak: api.lead_evidence_weak ?? false,
    // Company links
    companyWebsite: api.company_website,
    companyLinkedin: api.company_linkedin,
    // Source tracking
    sourceUrl: api.source_url,
    sourceName: api.source_name,
    // Founders
    founders,
  };
}

// ============================================
// Health & Status Endpoints
// ============================================

export async function fetchHealth(): Promise<HealthResponse> {
  const data = await apiGet<{
    status: string;
    timestamp: string;
    tracked_funds: number;
    implemented_scrapers: number;
  }>('/health');

  return {
    status: data.status as 'healthy' | 'unhealthy',
    timestamp: data.timestamp,
    trackedFunds: data.tracked_funds,
    implementedScrapers: data.implemented_scrapers,
  };
}

export async function fetchScraperStatus(): Promise<ScraperStatus> {
  const data = await apiGet<{
    implemented: string[];
    not_implemented: string[];
    total_funds: number;
  }>('/scrapers/status');

  return {
    implemented: data.implemented,
    unimplemented: data.not_implemented,
    totalFunds: data.total_funds,
  };
}

export async function fetchSchedulerStatus(): Promise<SchedulerStatusResponse> {
  const data = await apiGet<{
    status: string;
    jobs: Array<{ id: string; name: string; next_run?: string }>;
  }>('/scheduler/status', false);

  // Extract next_run from the first job (scheduled_scrape)
  const nextRun = data.jobs.find((j) => j.id === 'scheduled_scrape')?.next_run;

  return {
    status: data.status as SchedulerStatusResponse['status'],
    jobs: data.jobs.map((j) => ({
      id: j.id,
      name: j.name,
      nextRun: j.next_run,
    })),
    next_run: nextRun,
  };
}

// ============================================
// Funds Endpoints
// ============================================

export async function fetchFunds(): Promise<Fund[]> {
  const data = await apiGet<Array<{
    slug: string;
    name: string;
    ingestion_url: string;
    scraper_type: string;
  }>>('/funds');

  return data.map((f) => ({
    slug: f.slug,
    name: f.name,
    ingestionUrl: f.ingestion_url,
    scraperType: f.scraper_type as Fund['scraperType'],
  }));
}

export async function fetchFund(slug: string): Promise<Fund> {
  const data = await apiGet<{
    slug: string;
    name: string;
    ingestion_url: string;
    scraper_type: string;
  }>(`/funds/${slug}`);

  return {
    slug: data.slug,
    name: data.name,
    ingestionUrl: data.ingestion_url,
    scraperType: data.scraper_type as Fund['scraperType'],
  };
}

// ============================================
// Deals Endpoints
// ============================================

export async function fetchDeals(filters: DealFilters = {}): Promise<PaginatedDeals> {
  const params = new URLSearchParams();
  params.set('limit', (filters.limit ?? 50).toString());
  params.set('offset', (filters.offset ?? 0).toString());
  if (filters.stage) params.set('stage', filters.stage);
  if (filters.is_lead !== undefined) params.set('is_lead', filters.is_lead.toString());
  if (filters.fund_slug) params.set('fund_slug', filters.fund_slug);
  if (filters.is_ai_deal !== undefined) params.set('is_ai_deal', filters.is_ai_deal.toString());
  if (filters.is_enterprise_ai !== undefined) params.set('is_enterprise_ai', filters.is_enterprise_ai.toString());
  if (filters.enterprise_category) params.set('enterprise_category', filters.enterprise_category);
  if (filters.search) params.set('search', filters.search);
  if (filters.sort_direction) params.set('sort_direction', filters.sort_direction);

  const data = await apiGet<PaginatedDealsApiResponse>(`/deals?${params.toString()}`, false); // Don't cache when searching

  return {
    deals: data.deals.map(mapDeal),
    total: data.total,
    limit: data.limit,
    offset: data.offset,
    hasMore: data.has_more,
  };
}

export function getExportUrl(filters: Omit<DealFilters, 'limit' | 'offset'> = {}): string {
  const params = new URLSearchParams();
  if (filters.stage) params.set('stage', filters.stage);
  if (filters.is_lead !== undefined) params.set('is_lead', filters.is_lead.toString());
  if (filters.fund_slug) params.set('fund_slug', filters.fund_slug);
  if (filters.is_ai_deal !== undefined) params.set('is_ai_deal', filters.is_ai_deal.toString());
  if (filters.is_enterprise_ai !== undefined) params.set('is_enterprise_ai', filters.is_enterprise_ai.toString());
  if (filters.enterprise_category) params.set('enterprise_category', filters.enterprise_category);
  if (filters.search) params.set('search', filters.search);

  const queryString = params.toString();
  return `${API_BASE}/deals/export${queryString ? '?' + queryString : ''}`;
}

export interface UpdateDealRequest {
  companyName?: string;
  website?: string;
  linkedinUrl?: string;
  roundType?: string;
  amount?: string;
  announcedDate?: string; // ISO format: YYYY-MM-DD
  isLeadConfirmed?: boolean;
  leadPartnerName?: string;
  enterpriseCategory?: string;
  isEnterpriseAi?: boolean;
  founders?: Array<{ name: string; title?: string; linkedinUrl?: string }>;
}

export async function updateDeal(dealId: string, updates: UpdateDealRequest): Promise<Deal> {
  // Convert camelCase to snake_case for API
  const body: Record<string, unknown> = {};
  if (updates.companyName !== undefined) body.company_name = updates.companyName;
  if (updates.website !== undefined) body.website = updates.website;
  if (updates.linkedinUrl !== undefined) body.linkedin_url = updates.linkedinUrl;
  if (updates.roundType !== undefined) body.round_type = updates.roundType;
  if (updates.amount !== undefined) body.amount = updates.amount;
  if (updates.announcedDate !== undefined) body.announced_date = updates.announcedDate;
  if (updates.isLeadConfirmed !== undefined) body.is_lead_confirmed = updates.isLeadConfirmed;
  if (updates.leadPartnerName !== undefined) body.lead_partner_name = updates.leadPartnerName;
  if (updates.enterpriseCategory !== undefined) body.enterprise_category = updates.enterpriseCategory;
  if (updates.isEnterpriseAi !== undefined) body.is_enterprise_ai = updates.isEnterpriseAi;
  if (updates.founders !== undefined) {
    body.founders = updates.founders.map((f) => ({
      name: f.name,
      title: f.title,
      linkedin_url: f.linkedinUrl,
    }));
  }

  const data = await apiPut<DealApiResponse>(`/deals/${dealId}`, body);
  invalidateCache('GET:/deals');
  return mapDeal(data);
}

// ============================================
// Stealth Detection Endpoints
// ============================================

export async function fetchStealthDetections(
  fundSlug?: string,
  limit = 50,
  offset = 0
): Promise<StealthDetectionsResponse> {
  const params = new URLSearchParams();
  params.set('limit', limit.toString());
  params.set('offset', offset.toString());
  if (fundSlug) params.set('fund_slug', fundSlug);

  const data = await apiGet<{
    detections: Array<{
      id: number;
      fund_slug: string;
      detected_url: string;
      detected_at: string;
      company_name?: string;
      is_confirmed: boolean;
      notes?: string;
    }>;
    total: number;
  }>(`/stealth-detections?${params.toString()}`);

  return {
    detections: data.detections.map((d) => ({
      id: d.id,
      fundSlug: d.fund_slug,
      detectedUrl: d.detected_url,
      detectedAt: d.detected_at,
      companyName: d.company_name,
      isConfirmed: d.is_confirmed,
      notes: d.notes,
    })),
    total: data.total,
  };
}

// ============================================
// Stealth Signals (Pre-Funding Detection) Endpoints
// ============================================

interface StealthSignalApiResponse {
  id: number;
  company_name: string;
  source: string;
  source_url: string;
  score: number;
  signals: Record<string, unknown>;
  metadata: Record<string, unknown>;
  spotted_at: string;
  dismissed: boolean;
  converted_deal_id?: number;
  created_at: string;
}

interface StealthSignalsApiResponse {
  signals: StealthSignalApiResponse[];
  total: number;
}

interface StealthSignalStatsApiResponse {
  total: number;
  by_source: Record<string, number>;
  avg_score: number;
  converted: number;
}

function mapStealthSignal(api: StealthSignalApiResponse): StealthSignal {
  return {
    id: api.id,
    companyName: api.company_name,
    source: api.source as StealthSignalSource,
    sourceUrl: api.source_url,
    score: api.score,
    signals: api.signals,
    metadata: api.metadata,
    spottedAt: api.spotted_at,
    dismissed: api.dismissed,
    convertedDealId: api.converted_deal_id,
    createdAt: api.created_at,
  };
}

export async function fetchStealthSignals(
  options: {
    source?: StealthSignalSource;
    minScore?: number;
    includeDismissed?: boolean;
    limit?: number;
    offset?: number;
  } = {}
): Promise<StealthSignalsResponse> {
  const params = new URLSearchParams();
  if (options.source) params.set('source', options.source);
  if (options.minScore !== undefined) params.set('min_score', options.minScore.toString());
  if (options.includeDismissed) params.set('include_dismissed', 'true');
  params.set('limit', (options.limit ?? 100).toString());
  params.set('offset', (options.offset ?? 0).toString());

  const data = await apiGet<StealthSignalsApiResponse>(
    `/stealth-signals?${params.toString()}`,
    false // Don't cache - data changes frequently
  );

  return {
    signals: data.signals.map(mapStealthSignal),
    total: data.total,
  };
}

export async function fetchStealthSignalStats(
  includeDismissed = false
): Promise<StealthSignalStats> {
  const params = new URLSearchParams();
  if (includeDismissed) params.set('include_dismissed', 'true');

  const data = await apiGet<StealthSignalStatsApiResponse>(
    `/stealth-signals/stats?${params.toString()}`,
    false
  );

  return {
    total: data.total,
    bySource: data.by_source,
    avgScore: data.avg_score,
    converted: data.converted,
  };
}

export async function dismissStealthSignal(signalId: number): Promise<void> {
  await apiPost(`/stealth-signals/${signalId}/dismiss`);
  invalidateCache('GET:/stealth-signals');
}

export async function undismissStealthSignal(signalId: number): Promise<void> {
  await apiPost(`/stealth-signals/${signalId}/undismiss`);
  invalidateCache('GET:/stealth-signals');
}

export async function linkStealthSignalToDeal(
  signalId: number,
  dealId: number
): Promise<void> {
  await apiPost(`/stealth-signals/${signalId}/link/${dealId}`);
  invalidateCache('GET:/stealth-signals');
}

// ============================================
// Extraction Endpoints
// ============================================

export async function extractDeal(request: ExtractionRequest): Promise<ExtractionResponse> {
  const data = await apiPost<{
    startup_name: string;
    round_label: string;
    amount?: string;
    tracked_fund_is_lead: boolean;
    tracked_fund_name?: string;
    tracked_fund_role?: string;
    confidence_score: number;
    reasoning_summary: string;
  }>('/extract', {
    text: request.text,
    source_url: request.sourceUrl,
    fund_slug: request.fundSlug,
  });

  return {
    startupName: data.startup_name,
    roundLabel: data.round_label,
    amount: data.amount,
    trackedFundIsLead: data.tracked_fund_is_lead,
    trackedFundName: data.tracked_fund_name,
    trackedFundRole: data.tracked_fund_role,
    confidenceScore: data.confidence_score,
    reasoningSummary: data.reasoning_summary,
  };
}

export async function extractDealBatch(
  requests: ExtractionRequest[]
): Promise<ExtractionResponse[]> {
  const data = await apiPost<Array<{
    startup_name: string;
    round_label: string;
    amount?: string;
    tracked_fund_is_lead: boolean;
    tracked_fund_name?: string;
    tracked_fund_role?: string;
    confidence_score: number;
    reasoning_summary: string;
  }>>('/extract/batch', {
    articles: requests.map((r) => ({
      text: r.text,
      source_url: r.sourceUrl,
      fund_slug: r.fundSlug,
    })),
  });

  return data.map((d) => ({
    startupName: d.startup_name,
    roundLabel: d.round_label,
    amount: d.amount,
    trackedFundIsLead: d.tracked_fund_is_lead,
    trackedFundName: d.tracked_fund_name,
    trackedFundRole: d.tracked_fund_role,
    confidenceScore: d.confidence_score,
    reasoningSummary: d.reasoning_summary,
  }));
}

// ============================================
// Scraper Endpoints
// ============================================

export async function runScraper(fundSlug: string): Promise<ScrapeResponse> {
  const data = await apiPost<{
    fund_slug: string;
    articles_found: number;
    articles_skipped_duplicate: number;
    deals_extracted: number;
    deals_saved: number;
    errors: string[];
    duration_seconds: number;
  }>(`/scrapers/run/${fundSlug}`);

  invalidateCache('GET:/deals');

  return {
    fundSlug: data.fund_slug,
    articlesFound: data.articles_found,
    articlesSkippedDuplicate: data.articles_skipped_duplicate,
    dealsExtracted: data.deals_extracted,
    dealsSaved: data.deals_saved,
    errors: data.errors,
    durationSeconds: data.duration_seconds,
  };
}

export async function runScrapersBatch(
  fundSlugs: string[],
  parallel = false
): Promise<ScrapeResponse[]> {
  const data = await apiPost<Array<{
    fund_slug: string;
    articles_found: number;
    articles_skipped_duplicate: number;
    deals_extracted: number;
    deals_saved: number;
    errors: string[];
    duration_seconds: number;
  }>>('/scrapers/run', { fund_slugs: fundSlugs, parallel });

  invalidateCache('GET:/deals');

  return data.map((d) => ({
    fundSlug: d.fund_slug,
    articlesFound: d.articles_found,
    articlesSkippedDuplicate: d.articles_skipped_duplicate,
    dealsExtracted: d.deals_extracted,
    dealsSaved: d.deals_saved,
    errors: d.errors,
    durationSeconds: d.duration_seconds,
  }));
}

export async function runAllScrapers(parallel = true): Promise<ScrapeResponse[]> {
  const data = await apiPost<Array<{
    fund_slug: string;
    articles_found: number;
    articles_skipped_duplicate: number;
    deals_extracted: number;
    deals_saved: number;
    errors: string[];
    duration_seconds: number;
  }>>(`/scrapers/run-all?parallel=${parallel}`);

  invalidateCache('GET:/deals');

  return data.map((d) => ({
    fundSlug: d.fund_slug,
    articlesFound: d.articles_found,
    articlesSkippedDuplicate: d.articles_skipped_duplicate,
    dealsExtracted: d.deals_extracted,
    dealsSaved: d.deals_saved,
    errors: d.errors,
    durationSeconds: d.duration_seconds,
  }));
}

// ============================================
// Data Source Endpoints
// ============================================

export async function runSECEdgar(hours = 24): Promise<SECEdgarResponse> {
  const data = await apiPost<{
    filings_found: number;
    filings_with_tracked_funds: number;
    articles_generated: number;
  }>(`/scrapers/sec-edgar?hours=${hours}`);

  invalidateCache('GET:/deals');

  return {
    filingsFound: data.filings_found,
    filingsWithTrackedFunds: data.filings_with_tracked_funds,
    articlesGenerated: data.articles_generated,
  };
}

export async function runBraveSearch(freshness = 'pw'): Promise<BraveSearchResponse> {
  const data = await apiPost<{
    queries_executed: number;
    results_found: number;
    articles_generated: number;
  }>(`/scrapers/brave-search?freshness=${freshness}`);

  invalidateCache('GET:/deals');

  return {
    queriesExecuted: data.queries_executed,
    resultsFound: data.results_found,
    articlesGenerated: data.articles_generated,
  };
}

export async function runNewsAggregator(days = 30): Promise<NewsAggregatorResponse> {
  const data = await apiPost<{
    articles_fetched: number;
    articles_processed: number;
    enterprise_ai_deals: number;
    lead_deals: number;
    deals_saved: number;
    skipped_consumer: number;
    skipped_participant: number;
    errors: number;
  }>(`/scrapers/news?days=${days}`);

  invalidateCache('GET:/deals');

  return {
    articlesFetched: data.articles_fetched,
    articlesProcessed: data.articles_processed,
    enterpriseAiDeals: data.enterprise_ai_deals,
    leadDeals: data.lead_deals,
    dealsSaved: data.deals_saved,
    skippedConsumer: data.skipped_consumer,
    skippedParticipant: data.skipped_participant,
    errors: data.errors,
  };
}

export async function runFirecrawl(urls: string[]): Promise<FirecrawlResponse> {
  const data = await apiPost<{
    urls_submitted: number;
    urls_scraped: number;
    articles_generated: number;
  }>('/scrapers/firecrawl', { urls });

  return {
    urlsSubmitted: data.urls_submitted,
    urlsScraped: data.urls_scraped,
    articlesGenerated: data.articles_generated,
  };
}

export async function runAllSources(days = 7): Promise<AllSourcesResponse> {
  const data = await apiPost<{
    sec_edgar: {
      filings_found: number;
      filings_with_tracked_funds: number;
      articles_generated: number;
    };
    brave_search?: {
      queries_executed: number;
      results_found: number;
      articles_generated: number;
    };
    newsapi: {
      articles_fetched: number;
      articles_processed: number;
      enterprise_ai_deals: number;
      lead_deals: number;
      deals_saved: number;
      skipped_consumer: number;
      skipped_participant: number;
      errors: number;
    };
  }>(`/scrapers/all-sources?days=${days}`);

  invalidateCache('GET:/deals');

  return {
    secEdgar: {
      filingsFound: data.sec_edgar.filings_found,
      filingsWithTrackedFunds: data.sec_edgar.filings_with_tracked_funds,
      articlesGenerated: data.sec_edgar.articles_generated,
    },
    braveSearch: data.brave_search
      ? {
          queriesExecuted: data.brave_search.queries_executed,
          resultsFound: data.brave_search.results_found,
          articlesGenerated: data.brave_search.articles_generated,
        }
      : undefined,
    newsapi: {
      articlesFetched: data.newsapi.articles_fetched,
      articlesProcessed: data.newsapi.articles_processed,
      enterpriseAiDeals: data.newsapi.enterprise_ai_deals,
      leadDeals: data.newsapi.lead_deals,
      dealsSaved: data.newsapi.deals_saved,
      skippedConsumer: data.newsapi.skipped_consumer,
      skippedParticipant: data.newsapi.skipped_participant,
      errors: data.newsapi.errors,
    },
  };
}

export async function runBackfill(months = 12, dryRun = false): Promise<NewsAggregatorResponse> {
  const data = await apiPost<{
    articles_fetched: number;
    articles_processed: number;
    enterprise_ai_deals: number;
    lead_deals: number;
    deals_saved: number;
    skipped_consumer: number;
    skipped_participant: number;
    errors: number;
  }>(`/backfill?months=${months}&dry_run=${dryRun}`);

  if (!dryRun) invalidateCache('GET:/deals');

  return {
    articlesFetched: data.articles_fetched,
    articlesProcessed: data.articles_processed,
    enterpriseAiDeals: data.enterprise_ai_deals,
    leadDeals: data.lead_deals,
    dealsSaved: data.deals_saved,
    skippedConsumer: data.skipped_consumer,
    skippedParticipant: data.skipped_participant,
    errors: data.errors,
  };
}

// ============================================
// Tracker CRM Endpoints
// ============================================

import type {
  TrackerItem,
  TrackerItemsResponse,
  TrackerStatus,
  TrackerStats,
  TrackerColumn,
  TrackerColumnsResponse,
} from '../types';

// ============================================
// Tracker Column API Types & Functions
// ============================================

interface TrackerColumnApiResponse {
  id: number;
  slug: string;
  display_name: string;
  color: string;
  position: number;
  is_active: boolean;
}

interface TrackerColumnsApiResponse {
  columns: TrackerColumnApiResponse[];
  item_counts: Record<string, number>;
}

function mapTrackerColumn(api: TrackerColumnApiResponse): TrackerColumn {
  return {
    id: api.id,
    slug: api.slug,
    displayName: api.display_name,
    color: api.color,
    position: api.position,
    isActive: api.is_active,
  };
}

export async function fetchTrackerColumns(): Promise<TrackerColumnsResponse> {
  const data = await apiGet<TrackerColumnsApiResponse>('/tracker/columns', false);
  return {
    columns: data.columns.map(mapTrackerColumn),
    itemCounts: data.item_counts,
  };
}

export async function createTrackerColumn(column: {
  displayName: string;
  color?: string;
  slug?: string;
}): Promise<TrackerColumn> {
  const data = await apiPost<TrackerColumnApiResponse>('/tracker/columns', {
    display_name: column.displayName,
    color: column.color || 'slate',
    slug: column.slug,
  });
  invalidateCache('GET:/tracker/columns');
  return mapTrackerColumn(data);
}

export async function updateTrackerColumn(
  columnId: number,
  updates: Partial<{ displayName: string; color: string }>
): Promise<TrackerColumn> {
  const data = await apiPut<TrackerColumnApiResponse>(`/tracker/columns/${columnId}`, {
    display_name: updates.displayName,
    color: updates.color,
  });
  invalidateCache('GET:/tracker/columns');
  return mapTrackerColumn(data);
}

export async function moveTrackerColumn(
  columnId: number,
  position: number
): Promise<TrackerColumn> {
  const data = await apiPut<TrackerColumnApiResponse>(`/tracker/columns/${columnId}/move`, {
    position,
  });
  invalidateCache('GET:/tracker/columns');
  return mapTrackerColumn(data);
}

export async function deleteTrackerColumn(columnId: number): Promise<void> {
  await apiDelete(`/tracker/columns/${columnId}`);
  invalidateCache('GET:/tracker/columns');
  invalidateCache('GET:/tracker');
}

// ============================================
// Tracker Item API Types & Functions
// ============================================

interface TrackerItemApiResponse {
  id: number;
  company_name: string;
  round_type?: string;
  amount?: string;
  lead_investor?: string;
  website?: string;
  status: string;
  notes?: string;
  last_contact_date?: string;
  next_step?: string;
  position: number;
  deal_id?: number;
  created_at: string;
  updated_at: string;
}

interface TrackerItemsApiResponse {
  items: TrackerItemApiResponse[];
  total: number;
  stats: Record<string, number>;
}

function mapTrackerItem(api: TrackerItemApiResponse): TrackerItem {
  return {
    id: api.id,
    companyName: api.company_name,
    roundType: api.round_type,
    amount: api.amount,
    leadInvestor: api.lead_investor,
    website: api.website,
    status: api.status as TrackerStatus,
    notes: api.notes,
    lastContactDate: api.last_contact_date,
    nextStep: api.next_step,
    position: api.position,
    dealId: api.deal_id,
    createdAt: api.created_at,
    updatedAt: api.updated_at,
  };
}

export async function fetchTrackerItems(status?: TrackerStatus): Promise<TrackerItemsResponse> {
  const params = new URLSearchParams();
  if (status) params.set('status', status);

  const data = await apiGet<TrackerItemsApiResponse>(
    `/tracker${params.toString() ? '?' + params.toString() : ''}`,
    false // Don't cache tracker items
  );

  return {
    items: data.items.map(mapTrackerItem),
    total: data.total,
    stats: data.stats as TrackerStats, // Backend always includes 'total'
  };
}

export async function fetchTrackerItem(itemId: number): Promise<TrackerItem> {
  const data = await apiGet<TrackerItemApiResponse>(`/tracker/${itemId}`, false);
  return mapTrackerItem(data);
}

export async function createTrackerItem(item: {
  companyName: string;
  roundType?: string;
  amount?: string;
  leadInvestor?: string;
  website?: string;
  notes?: string;
  status?: TrackerStatus;
}): Promise<TrackerItem> {
  const data = await apiPost<TrackerItemApiResponse>('/tracker', {
    company_name: item.companyName,
    round_type: item.roundType,
    amount: item.amount,
    lead_investor: item.leadInvestor,
    website: item.website,
    notes: item.notes,
    status: item.status || 'watching',
  });
  invalidateCache('GET:/tracker');
  return mapTrackerItem(data);
}

interface BulkCreateResponse {
  created: TrackerItemApiResponse[];
  count: number;
}

export async function bulkCreateTrackerItems(
  companyNames: string[],
  status: TrackerStatus = 'watching'
): Promise<{ created: TrackerItem[]; count: number }> {
  const data = await apiPost<BulkCreateResponse>('/tracker/bulk', {
    company_names: companyNames,
    status,
  });
  invalidateCache('GET:/tracker');
  return {
    created: data.created.map(mapTrackerItem),
    count: data.count,
  };
}

export async function updateTrackerItem(
  itemId: number,
  updates: Partial<{
    companyName: string;
    roundType: string;
    amount: string;
    leadInvestor: string;
    website: string;
    notes: string;
    status: TrackerStatus;
    lastContactDate: string;
    nextStep: string;
  }>
): Promise<TrackerItem> {
  const data = await apiPut<TrackerItemApiResponse>(`/tracker/${itemId}`, {
    company_name: updates.companyName,
    round_type: updates.roundType,
    amount: updates.amount,
    lead_investor: updates.leadInvestor,
    website: updates.website,
    notes: updates.notes,
    status: updates.status,
    last_contact_date: updates.lastContactDate,
    next_step: updates.nextStep,
  });
  invalidateCache('GET:/tracker');
  return mapTrackerItem(data);
}

export async function moveTrackerItem(
  itemId: number,
  status: TrackerStatus,
  position: number
): Promise<TrackerItem> {
  const data = await apiPost<TrackerItemApiResponse>(`/tracker/${itemId}/move`, {
    status,
    position,
  });
  invalidateCache('GET:/tracker');
  return mapTrackerItem(data);
}

export async function deleteTrackerItem(itemId: number): Promise<void> {
  await apiDelete(`/tracker/${itemId}`);
  invalidateCache('GET:/tracker');
}

export async function addDealToTracker(
  dealId: number,
  status: TrackerStatus = 'watching'
): Promise<TrackerItem> {
  const data = await apiPost<TrackerItemApiResponse>('/tracker/from-deal', {
    deal_id: dealId,
    status,
  });
  invalidateCache('GET:/tracker');
  return mapTrackerItem(data);
}

// ============================================
// Feedback & Flagging
// ============================================

interface FeedbackResponse {
  success: boolean;
  timestamp: string;
  company_name: string;
  message: string;
}

export async function flagDeal(
  dealId: number | null,
  companyName: string,
  reason?: string,
  sourceUrl?: string
): Promise<{ success: boolean; message: string }> {
  const data = await apiPost<FeedbackResponse>('/feedback/flag', {
    deal_id: dealId,
    company_name: companyName,
    reason: reason || null,
    source_url: sourceUrl || null,
  });
  return {
    success: data.success,
    message: data.message,
  };
}

export async function submitFeedback(
  companyName: string,
  details?: string,
  suggestionType: 'missing_company' | 'error' | 'other' = 'missing_company'
): Promise<{ success: boolean; message: string }> {
  const data = await apiPost<FeedbackResponse>('/feedback/suggestion', {
    company_name: companyName,
    details: details || null,
    suggestion_type: suggestionType,
  });
  return {
    success: data.success,
    message: data.message,
  };
}

// ============================================
// Cache Management
// ============================================

export async function clearServerCache(): Promise<void> {
  await apiPost('/cache/clear');
  invalidateCache();
}

// ============================================
// Legacy Compatibility
// ============================================

export async function fetchDealsLegacy(): Promise<Deal[]> {
  const result = await fetchDeals({ limit: 200 });
  return result.deals;
}

// Re-export for backwards compatibility
export type { DealFilters as FetchDealsOptions };

// ============================================
// Token Usage Endpoints
// ============================================

export interface TokenUsageResponse {
  totalTokens: number;
  totalInputTokens: number;
  totalOutputTokens: number;
  totalCacheReadTokens: number;
  totalCacheWriteTokens: number;
  totalCostUsd: number;
  totalCalls: number;
  period: string;
  startDate: string;
  endDate: string;
}

export async function fetchTokenUsage(days = 7): Promise<TokenUsageResponse> {
  const data = await apiGet<{
    total_tokens: number;
    total_input_tokens: number;
    total_output_tokens: number;
    total_cache_read_tokens: number;
    total_cache_write_tokens: number;
    total_cost_usd: number;
    total_calls: number;
    period: string;
    start_date: string;
    end_date: string;
  }>(`/usage/tokens?days=${days}`, false);

  return {
    totalTokens: data.total_tokens,
    totalInputTokens: data.total_input_tokens,
    totalOutputTokens: data.total_output_tokens,
    totalCacheReadTokens: data.total_cache_read_tokens,
    totalCacheWriteTokens: data.total_cache_write_tokens,
    totalCostUsd: data.total_cost_usd,
    totalCalls: data.total_calls,
    period: data.period,
    startDate: data.start_date,
    endDate: data.end_date,
  };
}
