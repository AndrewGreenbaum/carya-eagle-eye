// ============================================
// Core Enums & Types
// ============================================

export type InvestorRole = 'lead' | 'non_lead' | 'stealth';

export type InvestmentStage =
  | 'pre_seed'
  | 'seed'
  | 'series_a'
  | 'series_b'
  | 'series_c'
  | 'series_d'
  | 'growth'
  | 'exit'
  | 'unknown';

export type EnterpriseCategory =
  | 'infrastructure'
  | 'security'
  | 'vertical_saas'
  | 'agentic'
  | 'data_intelligence'
  // Consumer AI categories
  | 'consumer_ai'
  | 'gaming_ai'
  | 'social_ai'
  // Non-AI (specific categories)
  | 'crypto'
  | 'fintech'
  | 'healthcare'
  | 'hardware'
  | 'saas'
  | 'other'
  // Legacy (backwards compat)
  | 'not_ai';

export type ScraperType = 'html' | 'rss' | 'playwright' | 'external' | 'dom_diff';

export type ViewType = 'dashboard' | 'stealth' | 'settings' | 'scrapers' | 'tracker' | 'scans' | 'prefunding';

export type ServiceStatus = 'live' | 'idle' | 'error' | 'processing';

// ============================================
// Deal Types
// ============================================

export interface Founder {
  name: string;
  linkedinUrl?: string;
  title?: string;
}

export interface Deal {
  id: string;
  startupName: string;
  investorRoles: InvestorRole[];
  investmentStage: InvestmentStage;
  amountInvested: string;
  date: string;
  nextSteps?: string;
  // AI classification fields
  enterpriseCategory?: EnterpriseCategory;
  isEnterpriseAi: boolean;
  isAiDeal: boolean;
  leadInvestor?: string;
  leadPartner?: string;
  verificationSnippet?: string;
  leadEvidenceWeak?: boolean; // True if snippet lacks "led by" but Claude determined lead
  // Source tracking
  sourceUrl?: string;
  sourceName?: string;
  // Company info
  companyWebsite?: string;
  companyLinkedin?: string;
  founders?: Founder[];
}

export type SortDirection = 'asc' | 'desc';

export interface PaginatedDeals {
  deals: Deal[];
  total: number;
  limit: number;
  offset: number;
  hasMore: boolean;
}

// API uses snake_case, so we need to handle both
export interface DealFilters {
  fund_slug?: string;
  stage?: InvestmentStage;
  is_lead?: boolean;
  is_ai_deal?: boolean;
  is_enterprise_ai?: boolean;
  enterprise_category?: EnterpriseCategory;
  search?: string;
  sort_direction?: SortDirection;
  limit?: number;
  offset?: number;
}

// ============================================
// Fund Types
// ============================================

export interface Fund {
  slug: string;
  name: string;
  ingestionUrl: string;
  scraperType: ScraperType;
  // Extended fields from registry
  partnerNames?: string[];
  negativeKeywords?: string[];
  extractionNotes?: string;
  stealthMonitorPath?: string;
  // Runtime status
  lastScraped?: string;
  isActive?: boolean;
}

// ============================================
// Stealth Detection Types
// ============================================

export interface StealthDetection {
  id: number;
  fundSlug: string;
  detectedUrl: string;
  detectedAt: string;
  companyName?: string;
  isConfirmed: boolean;
  notes?: string;
}

export interface StealthDetectionsResponse {
  detections: StealthDetection[];
  total: number;
}

// ============================================
// Pre-Funding Stealth Signals Types
// ============================================

export type StealthSignalSource =
  | 'hackernews'
  | 'ycombinator'
  | 'github'
  | 'github_trending'
  | 'linkedin'
  | 'linkedin_jobs'
  | 'delaware'
  | 'delaware_corps';

export interface StealthSignal {
  id: number;
  companyName: string;
  source: StealthSignalSource;
  sourceUrl: string;
  score: number; // 0-100 certainty score
  signals: Record<string, unknown>; // Source-specific signals
  metadata: Record<string, unknown>; // Source-specific metadata
  spottedAt: string;
  dismissed: boolean;
  convertedDealId?: number;
  createdAt: string;
}

export interface StealthSignalsResponse {
  signals: StealthSignal[];
  total: number;
}

export interface StealthSignalStats {
  total: number;
  bySource: Record<string, number>;
  avgScore: number;
  converted: number;
}

// Source display labels for stealth signals
export const STEALTH_SIGNAL_SOURCE_LABELS: Record<StealthSignalSource, string> = {
  hackernews: 'Hacker News',
  ycombinator: 'Y Combinator',
  github: 'GitHub',
  github_trending: 'GitHub Trending',
  linkedin: 'LinkedIn',
  linkedin_jobs: 'LinkedIn Jobs',
  delaware: 'Delaware Corps',
  delaware_corps: 'Delaware Corps',
};

// Source icon colors (for UI display)
export const STEALTH_SIGNAL_SOURCE_COLORS: Record<StealthSignalSource, string> = {
  hackernews: 'orange',
  ycombinator: 'orange',
  github: 'slate',
  github_trending: 'slate',
  linkedin: 'blue',
  linkedin_jobs: 'blue',
  delaware: 'emerald',
  delaware_corps: 'emerald',
};

// ============================================
// Health & Status Types
// ============================================

export interface HealthResponse {
  status: 'healthy' | 'unhealthy';
  timestamp: string;
  trackedFunds: number;
  implementedScrapers: number;
}

export interface ScraperStatus {
  implemented: string[];
  unimplemented: string[];
  totalFunds: number;
}

export interface ScraperStatusResponse {
  implemented: string[];
  notImplemented: string[];
  totalFunds: number;
}

export interface SchedulerJob {
  id: string;
  name: string;
  nextRun?: string;
  next_run_time?: string;
}

export interface SchedulerStatusResponse {
  status: 'running' | 'stopped' | 'not_initialized';
  jobs: SchedulerJob[];
  next_run?: string;
}

// ============================================
// Scraping Result Types
// ============================================

export interface ScrapeResponse {
  fundSlug: string;
  articlesFound: number;
  articlesSkippedDuplicate: number;
  dealsExtracted: number;
  dealsSaved: number;
  errors: string[];
  durationSeconds: number;
}

export interface SECEdgarResponse {
  filingsFound: number;
  filingsWithTrackedFunds: number;
  articlesGenerated: number;
}

export interface BraveSearchResponse {
  queriesExecuted: number;
  resultsFound: number;
  articlesGenerated: number;
}

export interface NewsAggregatorResponse {
  articlesFetched: number;
  articlesProcessed: number;
  enterpriseAiDeals: number;
  leadDeals: number;
  dealsSaved: number;
  skippedConsumer: number;
  skippedParticipant: number;
  errors: number;
}

export interface FirecrawlResponse {
  urlsSubmitted: number;
  urlsScraped: number;
  articlesGenerated: number;
}

export interface AllSourcesResponse {
  secEdgar: SECEdgarResponse;
  braveSearch?: BraveSearchResponse;
  newsapi: NewsAggregatorResponse;
}

// ============================================
// Extraction Types
// ============================================

export interface ExtractionRequest {
  text: string;
  sourceUrl?: string;
  fundSlug?: string;
}

export interface ExtractionResponse {
  startupName: string;
  roundLabel: string;
  amount?: string;
  trackedFundIsLead: boolean;
  trackedFundName?: string;
  trackedFundRole?: string;
  confidenceScore: number;
  reasoningSummary: string;
}

// ============================================
// UI State Types
// ============================================

export interface ServiceStatusInfo {
  status: ServiceStatus;
  lastRun?: string;
}

export interface SystemStatus {
  secEdgar: ServiceStatusInfo;
  scrapers: ServiceStatusInfo;
  claude: ServiceStatusInfo;
  nextScrape?: string;
}

export interface DashboardStats {
  newDeals24h: number;
  newDealsTrend: number; // +/- from previous day
  enterpriseAiRatio: number; // percentage
  tokensUsed: string; // e.g., "8.4M"
  claudeCalls: number;
  verificationRate: number; // percentage
}

export interface FilterState {
  aiDealsOnly: boolean;        // Master toggle: AI Companies Only (when OFF, shows all deals incl non-AI)
  enterpriseAiOnly: boolean;   // Sub-filter: Enterprise AI Only (only applies when aiDealsOnly=true)
  leadOnly: boolean;
  showRejected: boolean;
  selectedFund?: string;
  selectedCategory?: EnterpriseCategory;
  searchQuery?: string;
  sortDirection: SortDirection;
}

export interface AppState {
  view: ViewType;
  filters: FilterState;
  deals: Deal[];
  funds: Fund[];
  isLoading: boolean;
  error?: string;
  selectedDeal?: Deal;
  pagination: {
    total: number;
    limit: number;
    offset: number;
    hasMore: boolean;
  };
  systemStatus: SystemStatus;
  stats: DashboardStats;
}

// ============================================
// Notification Types (for Slack/Discord)
// ============================================

export interface NotificationSettings {
  slackWebhookUrl?: string;
  discordWebhookUrl?: string;
  enableLeadAlerts: boolean;
  enableDailySummary: boolean;
  enableErrorAlerts: boolean;
}

// ============================================
// Cost Tracking Types
// ============================================

export interface CostTracker {
  currentMonth: number; // USD
  monthlyLimit: number;
  lastUpdated?: string;
  breakdown?: {
    anthropic: number;
    newsapi: number;
    braveSearch: number;
    firecrawl: number;
  };
}

// ============================================
// Utility Types
// ============================================

export type LoadingState = 'idle' | 'loading' | 'success' | 'error';

export interface ApiError {
  detail: string;
  status: number;
}

// Category display helpers
export const CATEGORY_LABELS: Record<EnterpriseCategory, string> = {
  // Enterprise AI
  infrastructure: 'Infrastructure',
  security: 'Security',
  vertical_saas: 'Vertical SaaS',
  agentic: 'Agentic',
  data_intelligence: 'Data Intelligence',
  // Consumer AI
  consumer_ai: 'Consumer AI',
  gaming_ai: 'Gaming AI',
  social_ai: 'Social AI',
  // Non-AI (specific categories)
  crypto: 'Crypto',
  fintech: 'Fintech',
  healthcare: 'Healthcare',
  hardware: 'Hardware',
  saas: 'SaaS',
  other: 'Other',
  // Legacy
  not_ai: 'Other',
};

export const STAGE_LABELS: Record<InvestmentStage, string> = {
  pre_seed: 'Pre-Seed',
  seed: 'Seed',
  series_a: 'Series A',
  series_b: 'Series B',
  series_c: 'Series C',
  series_d: 'Series D',
  growth: 'Growth',
  exit: 'Exit',
  unknown: 'Unknown',
};

// ============================================
// Tracker CRM Types
// ============================================

// TrackerStatus is now a string to support dynamic columns
export type TrackerStatus = string;

// Configurable column type
export interface TrackerColumn {
  id: number;
  slug: string;
  displayName: string;
  color: string;
  position: number;
  isActive: boolean;
}

export interface TrackerColumnsResponse {
  columns: TrackerColumn[];
  itemCounts: Record<string, number>;
}

export interface TrackerItem {
  id: number;
  companyName: string;
  roundType?: string;
  amount?: string;
  leadInvestor?: string;
  website?: string;
  status: TrackerStatus;
  notes?: string;
  lastContactDate?: string;
  nextStep?: string;
  position: number;
  dealId?: number;
  createdAt: string;
  updatedAt: string;
}

export interface TrackerStats {
  total: number;
  [key: string]: number; // Dynamic columns
}

export interface TrackerItemsResponse {
  items: TrackerItem[];
  total: number;
  stats: TrackerStats;
}

// Available colors for columns
export const TRACKER_COLORS = [
  'slate', 'blue', 'amber', 'emerald', 'green', 'red', 'purple', 'pink', 'cyan', 'orange',
] as const;

export type TrackerColor = typeof TRACKER_COLORS[number];

// Color class mappings for Tailwind
export const TRACKER_COLOR_CLASSES: Record<string, { dot: string; bg: string; border: string }> = {
  slate: { dot: 'bg-slate-500', bg: 'bg-slate-500/20', border: 'border-slate-500/30' },
  blue: { dot: 'bg-blue-500', bg: 'bg-blue-500/20', border: 'border-blue-500/30' },
  amber: { dot: 'bg-amber-500', bg: 'bg-amber-500/20', border: 'border-amber-500/30' },
  emerald: { dot: 'bg-emerald-500', bg: 'bg-emerald-500/20', border: 'border-emerald-500/30' },
  green: { dot: 'bg-green-500', bg: 'bg-green-500/20', border: 'border-green-500/30' },
  red: { dot: 'bg-red-500', bg: 'bg-red-500/20', border: 'border-red-500/30' },
  purple: { dot: 'bg-purple-500', bg: 'bg-purple-500/20', border: 'border-purple-500/30' },
  pink: { dot: 'bg-pink-500', bg: 'bg-pink-500/20', border: 'border-pink-500/30' },
  cyan: { dot: 'bg-cyan-500', bg: 'bg-cyan-500/20', border: 'border-cyan-500/30' },
  orange: { dot: 'bg-orange-500', bg: 'bg-orange-500/20', border: 'border-orange-500/30' },
};

// Legacy constants for backward compatibility
export const TRACKER_STATUS_LABELS: Record<string, string> = {
  watching: 'Watching',
  reached_out: 'Reached Out',
  in_conversation: 'In Conversation',
  closing_spv: 'Closing SPV',
  spv_complete: 'SPV Complete',
  spv_rejected: 'SPV Rejected',
};

export const TRACKER_STATUS_COLORS: Record<string, string> = {
  watching: 'slate',
  reached_out: 'blue',
  in_conversation: 'amber',
  closing_spv: 'emerald',
  spv_complete: 'green',
  spv_rejected: 'red',
};

// The 18 tracked funds (from backend registry)
export const TRACKED_FUNDS = [
  { slug: 'a16z', name: 'a16z' },
  { slug: 'sequoia', name: 'Sequoia Capital' },
  { slug: 'benchmark', name: 'Benchmark' },
  { slug: 'founders_fund', name: 'Founders Fund' },
  { slug: 'thrive', name: 'Thrive Capital' },
  { slug: 'greylock', name: 'Greylock' },
  { slug: 'khosla', name: 'Khosla Ventures' },
  { slug: 'index', name: 'Index Ventures' },
  { slug: 'insight', name: 'Insight Partners' },
  { slug: 'bessemer', name: 'Bessemer VP' },
  { slug: 'redpoint', name: 'Redpoint' },
  { slug: 'gv', name: 'GV' },
  { slug: 'menlo', name: 'Menlo Ventures' },
  { slug: 'usv', name: 'USV' },
  { slug: 'accel', name: 'Accel' },
  { slug: 'felicis', name: 'Felicis' },
  { slug: 'general_catalyst', name: 'General Catalyst' },
  { slug: 'first_round', name: 'First Round' },
] as const;
