import { useState, useEffect, useCallback } from 'react';
import {
  Clock,
  CheckCircle,
  XCircle,
  Loader2,
  ChevronDown,
  ChevronRight,
  ChevronLeft,
  Building2,
  Users,
  TrendingUp,
  AlertTriangle,
  RefreshCw,
  Info,
  Filter,
  Zap,
  Eye,
  Ban,
} from 'lucide-react';

// Source configuration - reflects current scraper settings
const SOURCE_CONFIG = {
  // Sources that bypass title filter (funding-focused)
  bypassFilter: [
    { name: 'sec_edgar', label: 'SEC Edgar', reason: 'Official filings' },
    { name: 'brave_search', label: 'Brave Search', reason: 'Targeted queries' },
    { name: 'tech_funding_news', label: 'Tech Funding News', reason: 'Funding-focused site' },
    { name: 'ventureburn', label: 'Ventureburn', reason: 'Funding-focused site' },
    { name: 'crunchbase_news', label: 'Crunchbase News', reason: 'Funding database' },
    { name: 'linkedin_jobs', label: 'LinkedIn Jobs', reason: 'Stealth detector' },
    { name: 'google_news', label: 'Google News', reason: 'Fund-specific queries' },
  ],
  // Early signal sources (0 deals expected - they watch pre-funding)
  earlySignals: [
    { name: 'ycombinator', label: 'Y Combinator', purpose: 'Demo Day companies' },
    { name: 'github_trending', label: 'GitHub Trending', purpose: 'Dev tools pre-funding' },
    { name: 'hackernews', label: 'Hacker News', purpose: 'Launch posts' },
  ],
  // Sources with title filter (general news)
  withFilter: [
    { name: 'techcrunch', label: 'TechCrunch' },
    { name: 'google_alerts', label: 'Google Alerts' },
    { name: 'prwire', label: 'PR Wire' },
  ],
  // Disabled sources
  disabled: [
    { name: 'venturebeat', label: 'VentureBeat', reason: 'RSS blocked Dec 2025' },
    { name: 'axios_prorata', label: 'Axios Pro Rata', reason: 'RSS 404 Dec 2025' },
    { name: 'strictlyvc', label: 'StrictlyVC', reason: 'RSS dead since 2020' },
  ],
};

const API_BASE = import.meta.env.VITE_API_BASE || 'http://localhost:8000';
const API_KEY = import.meta.env.VITE_API_KEY || 'dev-key';

interface ScanSourceStats {
  articles_found: number;
  deals_extracted: number;
  deals_saved: number;
  duplicates_skipped: number;
  errors: number;
  error_message?: string;
}

interface ScanDeal {
  id: string;
  startup_name: string;
  round_type: string;
  amount?: string;
  lead_investor?: string;
  is_lead: boolean;
  is_enterprise_ai: boolean;
  enterprise_category?: string;
  source_name?: string;
  founders?: Array<{
    name: string;
    title?: string;
    linkedin_url?: string;
  }>;
}

interface Scan {
  id: number;
  job_id: string;
  started_at: string;
  completed_at?: string;
  duration_seconds?: number;
  status: 'running' | 'success' | 'failed';
  trigger: string;
  total_articles_found: number;
  total_deals_extracted: number;
  total_deals_saved: number;
  total_duplicates_skipped: number;
  total_errors: number;
  lead_deals_found: number;
  enterprise_ai_deals_found: number;
  error_message?: string;
  source_results?: Record<string, ScanSourceStats>;
  deals?: ScanDeal[];
}

interface ScanListResponse {
  scans: Scan[];
  total_count: number;
  page: number;
  limit: number;
}

function formatDuration(seconds?: number): string {
  if (!seconds) return '-';
  if (seconds < 60) return `${Math.round(seconds)}s`;
  const mins = Math.floor(seconds / 60);
  const secs = Math.round(seconds % 60);
  return `${mins}m ${secs}s`;
}

function formatDate(isoString: string): string {
  // For full ISO timestamps with time (e.g., "2026-01-05T14:30:00Z"),
  // JavaScript handles timezone correctly. For date-only strings,
  // parse components to avoid timezone shift bug.
  let date: Date;
  if (isoString.includes('T')) {
    date = new Date(isoString);
  } else {
    const parts = isoString.split('-');
    if (parts.length >= 3) {
      const year = parseInt(parts[0], 10);
      const month = parseInt(parts[1], 10) - 1;
      const day = parseInt(parts[2], 10);
      date = new Date(year, month, day);
    } else {
      date = new Date(isoString);
    }
  }
  return date.toLocaleString('en-US', {
    month: 'short',
    day: 'numeric',
    hour: 'numeric',
    minute: '2-digit',
    hour12: true,
  });
}

function StatusBadge({ status }: { status: string }) {
  const config = {
    running: { icon: Loader2, color: 'bg-blue-500/20 text-blue-400', animate: true },
    success: { icon: CheckCircle, color: 'bg-emerald-500/20 text-emerald-400', animate: false },
    failed: { icon: XCircle, color: 'bg-red-500/20 text-red-400', animate: false },
  }[status] || { icon: Clock, color: 'bg-slate-500/20 text-slate-400', animate: false };

  const Icon = config.icon;

  return (
    <span className={`inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-medium ${config.color}`}>
      <Icon className={`w-3.5 h-3.5 ${config.animate ? 'animate-spin' : ''}`} />
      {status}
    </span>
  );
}

// Get source category info
function getSourceCategory(name: string): { category: string; icon: typeof Zap; color: string } {
  const bypassNames = SOURCE_CONFIG.bypassFilter.map(s => s.name);
  const earlyNames = SOURCE_CONFIG.earlySignals.map(s => s.name);
  const filterNames = SOURCE_CONFIG.withFilter.map(s => s.name);

  if (bypassNames.includes(name)) {
    return { category: 'Funding-focused', icon: Zap, color: 'text-emerald-400' };
  }
  if (earlyNames.includes(name)) {
    return { category: 'Early signal', icon: Eye, color: 'text-blue-400' };
  }
  if (filterNames.includes(name)) {
    return { category: 'Filtered', icon: Filter, color: 'text-amber-400' };
  }
  // Fund scrapers (a16z, sequoia, etc.)
  return { category: 'Fund scraper', icon: Building2, color: 'text-purple-400' };
}

function DataSourcesPanel({ isOpen, onToggle }: { isOpen: boolean; onToggle: () => void }) {
  return (
    <div className="mb-6 bg-slate-900/50 border border-slate-700/50 rounded-lg overflow-hidden">
      <button
        onClick={onToggle}
        className="w-full p-4 flex items-center justify-between hover:bg-slate-800/30 transition-colors"
      >
        <div className="flex items-center gap-2">
          <Info className="w-5 h-5 text-slate-400" />
          <span className="font-medium">Data Sources Configuration</span>
        </div>
        {isOpen ? (
          <ChevronDown className="w-5 h-5 text-slate-400" />
        ) : (
          <ChevronRight className="w-5 h-5 text-slate-400" />
        )}
      </button>

      {isOpen && (
        <div className="border-t border-slate-700/50 p-4 space-y-4">
          {/* Bypass Filter Sources */}
          <div>
            <div className="flex items-center gap-2 mb-2">
              <Zap className="w-4 h-4 text-emerald-400" />
              <span className="text-sm font-medium text-emerald-400">Bypass Title Filter</span>
              <span className="text-xs text-slate-500">(All articles sent to Claude)</span>
            </div>
            <div className="flex flex-wrap gap-2">
              {SOURCE_CONFIG.bypassFilter.map(s => (
                <span
                  key={s.name}
                  className="px-2 py-1 bg-emerald-900/20 border border-emerald-700/30 rounded text-xs"
                  title={s.reason}
                >
                  {s.label}
                </span>
              ))}
            </div>
          </div>

          {/* Early Signal Sources */}
          <div>
            <div className="flex items-center gap-2 mb-2">
              <Eye className="w-4 h-4 text-blue-400" />
              <span className="text-sm font-medium text-blue-400">Early Signals</span>
              <span className="text-xs text-slate-500">(Pre-funding watchlist, 0 deals expected)</span>
            </div>
            <div className="flex flex-wrap gap-2">
              {SOURCE_CONFIG.earlySignals.map(s => (
                <span
                  key={s.name}
                  className="px-2 py-1 bg-blue-900/20 border border-blue-700/30 rounded text-xs"
                  title={s.purpose}
                >
                  {s.label}
                </span>
              ))}
            </div>
          </div>

          {/* Filtered Sources */}
          <div>
            <div className="flex items-center gap-2 mb-2">
              <Filter className="w-4 h-4 text-amber-400" />
              <span className="text-sm font-medium text-amber-400">With Title Filter</span>
              <span className="text-xs text-slate-500">(General news, filtered before Claude)</span>
            </div>
            <div className="flex flex-wrap gap-2">
              {SOURCE_CONFIG.withFilter.map(s => (
                <span
                  key={s.name}
                  className="px-2 py-1 bg-amber-900/20 border border-amber-700/30 rounded text-xs"
                >
                  {s.label}
                </span>
              ))}
            </div>
          </div>

          {/* Disabled Sources */}
          <div>
            <div className="flex items-center gap-2 mb-2">
              <Ban className="w-4 h-4 text-red-400" />
              <span className="text-sm font-medium text-red-400">Disabled</span>
              <span className="text-xs text-slate-500">(RSS feeds broken/deprecated)</span>
            </div>
            <div className="flex flex-wrap gap-2">
              {SOURCE_CONFIG.disabled.map(s => (
                <span
                  key={s.name}
                  className="px-2 py-1 bg-red-900/20 border border-red-700/30 rounded text-xs line-through opacity-60"
                  title={s.reason}
                >
                  {s.label}
                </span>
              ))}
            </div>
          </div>

          {/* Fund Scrapers note */}
          <div className="pt-2 border-t border-slate-700/30">
            <div className="flex items-center gap-2">
              <Building2 className="w-4 h-4 text-purple-400" />
              <span className="text-sm text-slate-400">
                <span className="text-purple-400 font-medium">18 Fund Scrapers</span> (a16z, Sequoia, etc.) run separately in Phase 1
              </span>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

function SourceBreakdown({ sources }: { sources: Record<string, ScanSourceStats> }) {
  const sortedSources = Object.entries(sources)
    .filter(([name]) => name !== 'total_deals_saved')
    .sort(([, a], [, b]) => b.deals_saved - a.deals_saved);

  return (
    <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-4 gap-2 mt-3">
      {sortedSources.map(([name, stats]) => {
        const { category, icon: Icon, color } = getSourceCategory(name);
        return (
          <div
            key={name}
            className={`p-2 rounded text-xs ${
              stats.error_message
                ? 'bg-red-900/20 border border-red-700/30'
                : stats.deals_saved > 0
                ? 'bg-emerald-900/20 border border-emerald-700/30'
                : 'bg-slate-800/50 border border-slate-700/30'
            }`}
          >
            <div className="flex items-center gap-1.5">
              <Icon className={`w-3 h-3 ${color} flex-shrink-0`} />
              <span className="font-medium text-white truncate">{name.replace(/_/g, ' ')}</span>
            </div>
            {stats.error_message ? (
              <div className="text-red-400 truncate mt-1" title={stats.error_message}>
                Error
              </div>
            ) : (
              <div className="text-slate-400 mt-1">
                {stats.articles_found} articles → {stats.deals_saved} deals
              </div>
            )}
            <div className={`text-[10px] ${color} mt-0.5 opacity-70`}>{category}</div>
          </div>
        );
      })}
    </div>
  );
}

function DealCard({ deal }: { deal: ScanDeal }) {
  return (
    <div className="p-3 bg-slate-800/50 rounded-lg border border-slate-700/50 hover:border-slate-600/50 transition-colors">
      <div className="flex items-start justify-between gap-2">
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2">
            <Building2 className="w-4 h-4 text-slate-500 flex-shrink-0" />
            <span className="font-medium text-white truncate">{deal.startup_name}</span>
          </div>
          <div className="flex items-center gap-2 mt-1 text-sm text-slate-400">
            <span className="capitalize">{deal.round_type.replace(/_/g, ' ')}</span>
            {deal.amount && <span>• {deal.amount}</span>}
          </div>
        </div>
        <div className="flex flex-col items-end gap-1">
          {deal.is_lead && (
            <span className="px-2 py-0.5 bg-purple-500/20 text-purple-300 text-xs rounded-full">
              Lead
            </span>
          )}
          {deal.is_enterprise_ai && (
            <span className="px-2 py-0.5 bg-emerald-500/20 text-emerald-300 text-xs rounded-full">
              Enterprise AI
            </span>
          )}
        </div>
      </div>
      {deal.lead_investor && (
        <div className="mt-2 text-sm text-slate-400 flex items-center gap-1.5">
          <TrendingUp className="w-3.5 h-3.5" />
          <span>{deal.lead_investor}</span>
        </div>
      )}
      {deal.founders && deal.founders.length > 0 && (
        <div className="mt-2 text-sm text-slate-400 flex items-center gap-1.5">
          <Users className="w-3.5 h-3.5" />
          <span>{deal.founders.map(f => f.name).join(', ')}</span>
        </div>
      )}
    </div>
  );
}

function ScanRow({ scan, isExpanded, onToggle }: { scan: Scan; isExpanded: boolean; onToggle: () => void }) {
  const [details, setDetails] = useState<Scan | null>(null);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    if (isExpanded && !details) {
      setLoading(true);
      fetch(`${API_BASE}/scans/${scan.id}`, {
        headers: { 'X-API-Key': API_KEY },
      })
        .then(res => res.json())
        .then(data => setDetails(data))
        .catch(console.error)
        .finally(() => setLoading(false));
    }
  }, [isExpanded, scan.id, details]);

  return (
    <div className="border border-slate-700/50 rounded-lg overflow-hidden">
      {/* Header row */}
      <button
        onClick={onToggle}
        className="w-full p-4 flex items-center gap-4 hover:bg-slate-800/50 transition-colors text-left"
      >
        <div className="flex-shrink-0">
          {isExpanded ? (
            <ChevronDown className="w-5 h-5 text-slate-400" />
          ) : (
            <ChevronRight className="w-5 h-5 text-slate-400" />
          )}
        </div>

        <div className="flex-1 min-w-0 grid grid-cols-2 md:grid-cols-4 lg:grid-cols-6 gap-4 items-center">
          {/* Date */}
          <div>
            <div className="text-sm font-medium text-white">{formatDate(scan.started_at)}</div>
            <div className="text-xs text-slate-500">{scan.job_id}</div>
          </div>

          {/* Status */}
          <div>
            <StatusBadge status={scan.status} />
          </div>

          {/* Duration */}
          <div className="hidden md:block">
            <div className="text-sm text-slate-300">{formatDuration(scan.duration_seconds)}</div>
            <div className="text-xs text-slate-500">{scan.trigger}</div>
          </div>

          {/* Articles */}
          <div className="hidden lg:block text-center">
            <div className="text-sm font-medium text-white">{scan.total_articles_found}</div>
            <div className="text-xs text-slate-500">articles</div>
          </div>

          {/* Deals Saved */}
          <div className="text-center">
            <div className={`text-sm font-medium ${scan.total_deals_saved > 0 ? 'text-emerald-400' : 'text-slate-400'}`}>
              {scan.total_deals_saved}
            </div>
            <div className="text-xs text-slate-500">deals</div>
          </div>

          {/* Errors */}
          <div className="hidden lg:block text-center">
            <div className={`text-sm font-medium ${scan.total_errors > 0 ? 'text-amber-400' : 'text-slate-400'}`}>
              {scan.total_errors}
            </div>
            <div className="text-xs text-slate-500">errors</div>
          </div>
        </div>
      </button>

      {/* Expanded content */}
      {isExpanded && (
        <div className="border-t border-slate-700/50 p-4 bg-slate-900/50">
          {loading ? (
            <div className="flex items-center justify-center py-8">
              <Loader2 className="w-6 h-6 animate-spin text-slate-400" />
            </div>
          ) : details ? (
            <div className="space-y-4">
              {/* Error message if failed */}
              {details.error_message && (
                <div className="p-3 bg-red-900/20 border border-red-700/30 rounded-lg flex items-start gap-2">
                  <AlertTriangle className="w-5 h-5 text-red-400 flex-shrink-0 mt-0.5" />
                  <div className="text-sm text-red-300">{details.error_message}</div>
                </div>
              )}

              {/* Source breakdown */}
              {details.source_results && Object.keys(details.source_results).length > 0 && (
                <div>
                  <h4 className="text-sm font-medium text-slate-300 mb-2">Source Breakdown</h4>
                  <SourceBreakdown sources={details.source_results} />
                </div>
              )}

              {/* Deals found */}
              {details.deals && details.deals.length > 0 && (
                <div>
                  <h4 className="text-sm font-medium text-slate-300 mb-2">
                    Deals Found ({details.deals.length})
                  </h4>
                  <div className="grid gap-2 md:grid-cols-2 lg:grid-cols-3">
                    {details.deals.map(deal => (
                      <DealCard key={deal.id} deal={deal} />
                    ))}
                  </div>
                </div>
              )}

              {/* No deals message */}
              {(!details.deals || details.deals.length === 0) && details.status === 'success' && (
                <div className="text-center py-4 text-slate-500">
                  No new deals found in this scan
                </div>
              )}
            </div>
          ) : null}
        </div>
      )}
    </div>
  );
}

interface ScansPageProps {
  onBack?: () => void;
}

export function ScansPage({ onBack }: ScansPageProps = {}) {
  const [scans, setScans] = useState<Scan[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [expandedId, setExpandedId] = useState<number | null>(null);
  const [page, setPage] = useState(1);
  const [totalCount, setTotalCount] = useState(0);
  const [showSourcesPanel, setShowSourcesPanel] = useState(false);
  const limit = 20;

  const fetchScans = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const res = await fetch(`${API_BASE}/scans?page=${page}&limit=${limit}`, {
        headers: { 'X-API-Key': API_KEY },
      });
      if (!res.ok) throw new Error('Failed to fetch scans');
      const data: ScanListResponse = await res.json();
      setScans(data.scans);
      setTotalCount(data.total_count);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load scans');
    } finally {
      setLoading(false);
    }
  }, [page]);

  useEffect(() => {
    fetchScans();
  }, [fetchScans]);

  // Auto-refresh if there's a running scan
  useEffect(() => {
    const hasRunning = scans.some(s => s.status === 'running');
    if (hasRunning) {
      const interval = setInterval(fetchScans, 10000);
      return () => clearInterval(interval);
    }
  }, [scans, fetchScans]);

  const totalPages = Math.ceil(totalCount / limit);

  return (
    <div className="min-h-screen bg-slate-950 text-white">
      {/* Header */}
      <div className="border-b border-slate-800 bg-slate-900/50">
        <div className="max-w-7xl mx-auto px-4 py-6">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-3">
              {onBack && (
                <button
                  onClick={onBack}
                  className="flex items-center gap-1 text-slate-400 hover:text-slate-200 transition-colors"
                >
                  <ChevronLeft className="w-5 h-5" />
                  <span className="text-sm">Deals</span>
                </button>
              )}
              <h1 className="text-2xl font-bold">Scan History</h1>
            </div>
            <button
              onClick={fetchScans}
              disabled={loading}
              className="flex items-center gap-2 px-4 py-2 bg-slate-800 hover:bg-slate-700 rounded-lg transition-colors disabled:opacity-50"
            >
              <RefreshCw className={`w-4 h-4 ${loading ? 'animate-spin' : ''}`} />
              Refresh
            </button>
          </div>
        </div>
      </div>

      {/* Content */}
      <div className="max-w-7xl mx-auto px-4 py-6">
        {error && (
          <div className="mb-4 p-4 bg-red-900/20 border border-red-700/30 rounded-lg text-red-300">
            {error}
          </div>
        )}

        {/* Data Sources Configuration Panel */}
        <DataSourcesPanel
          isOpen={showSourcesPanel}
          onToggle={() => setShowSourcesPanel(!showSourcesPanel)}
        />

        {loading && scans.length === 0 ? (
          <div className="flex items-center justify-center py-12">
            <Loader2 className="w-8 h-8 animate-spin text-slate-400" />
          </div>
        ) : scans.length === 0 ? (
          <div className="text-center py-12 text-slate-500">
            No scans found. Scans will appear here after the scheduler runs.
          </div>
        ) : (
          <>
            <div className="space-y-2">
              {scans.map(scan => (
                <ScanRow
                  key={scan.id}
                  scan={scan}
                  isExpanded={expandedId === scan.id}
                  onToggle={() => setExpandedId(expandedId === scan.id ? null : scan.id)}
                />
              ))}
            </div>

            {/* Pagination */}
            {totalPages > 1 && (
              <div className="mt-6 flex items-center justify-center gap-2">
                <button
                  onClick={() => setPage(p => Math.max(1, p - 1))}
                  disabled={page === 1}
                  className="px-3 py-1.5 bg-slate-800 hover:bg-slate-700 rounded disabled:opacity-50 disabled:cursor-not-allowed"
                >
                  Previous
                </button>
                <span className="text-slate-400 px-4">
                  Page {page} of {totalPages}
                </span>
                <button
                  onClick={() => setPage(p => Math.min(totalPages, p + 1))}
                  disabled={page === totalPages}
                  className="px-3 py-1.5 bg-slate-800 hover:bg-slate-700 rounded disabled:opacity-50 disabled:cursor-not-allowed"
                >
                  Next
                </button>
              </div>
            )}
          </>
        )}
      </div>
    </div>
  );
}
