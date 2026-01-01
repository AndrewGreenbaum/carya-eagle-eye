/**
 * ScraperControl - Manual scraper control panel
 *
 * Features:
 * - Run individual fund scrapers
 * - Run data source scrapers (SEC EDGAR, Brave, NewsAPI, Firecrawl)
 * - Run all scrapers at once
 * - View scraper status and last run times
 * - Backfill historical data
 *
 * Command Center dark theme
 */

import { useState, useEffect } from 'react';
import {
  Play,
  RefreshCw,
  CheckCircle,
  XCircle,
  Clock,
  Zap,
  FileText,
  Search,
  Newspaper,
  Globe,
  Loader2,
  AlertCircle,
  ChevronDown,
  ChevronUp,
} from 'lucide-react';
import {
  fetchScraperStatus,
  runScraper,
  runAllScrapers,
  runSECEdgar,
  runBraveSearch,
  runNewsAggregator,
  runFirecrawl,
  runAllSources,
  runBackfill,
} from '../api/deals';
import type { ScraperStatus } from '../types';
import { TRACKED_FUNDS } from '../types';

export function ScraperControl() {
  const [status, setStatus] = useState<ScraperStatus | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [runningScrapers, setRunningScrapers] = useState<Set<string>>(new Set());
  const [scraperErrors, setScraperErrors] = useState<Map<string, string>>(new Map());
  const [showFunds, setShowFunds] = useState(true);
  const [showSources, setShowSources] = useState(true);

  // Clear scraper error after 10 seconds
  const addScraperError = (source: string, message: string) => {
    setScraperErrors((prev) => new Map(prev).set(source, message));
    setTimeout(() => {
      setScraperErrors((prev) => {
        const next = new Map(prev);
        next.delete(source);
        return next;
      });
    }, 10000);
  };

  const loadStatus = async () => {
    try {
      const data = await fetchScraperStatus();
      setStatus(data);
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load scraper status');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    loadStatus();
    const interval = setInterval(loadStatus, 30000); // Refresh every 30s
    return () => clearInterval(interval);
  }, []);

  const handleRunScraper = async (fundSlug: string) => {
    setRunningScrapers((prev) => new Set([...prev, fundSlug]));
    try {
      await runScraper(fundSlug);
      await loadStatus();
    } catch (err) {
      const message = err instanceof Error ? err.message : 'Scraper failed';
      console.error(`Failed to run scraper for ${fundSlug}:`, err);
      addScraperError(fundSlug, message);
    } finally {
      setRunningScrapers((prev) => {
        const next = new Set(prev);
        next.delete(fundSlug);
        return next;
      });
    }
  };

  const handleRunAllScrapers = async () => {
    setRunningScrapers((prev) => new Set([...prev, 'all-funds']));
    try {
      await runAllScrapers();
      await loadStatus();
    } catch (err) {
      const message = err instanceof Error ? err.message : 'Failed to run all scrapers';
      console.error('Failed to run all scrapers:', err);
      addScraperError('all-funds', message);
    } finally {
      setRunningScrapers((prev) => {
        const next = new Set(prev);
        next.delete('all-funds');
        return next;
      });
    }
  };

  const handleRunDataSource = async (source: string) => {
    setRunningScrapers((prev) => new Set([...prev, source]));
    try {
      switch (source) {
        case 'sec-edgar':
          await runSECEdgar(24);
          break;
        case 'brave-search':
          await runBraveSearch('pw');
          break;
        case 'news-aggregator':
          await runNewsAggregator(7);
          break;
        case 'firecrawl':
          await runFirecrawl([]);
          break;
        case 'all-sources':
          await runAllSources(7);
          break;
        case 'backfill':
          await runBackfill(3);
          break;
      }
      await loadStatus();
    } catch (err) {
      const message = err instanceof Error ? err.message : `Failed to run ${source}`;
      console.error(`Failed to run ${source}:`, err);
      addScraperError(source, message);
    } finally {
      setRunningScrapers((prev) => {
        const next = new Set(prev);
        next.delete(source);
        return next;
      });
    }
  };

  if (loading) {
    return (
      <div className="flex-1 flex items-center justify-center bg-[#050506]">
        <div className="flex flex-col items-center gap-3">
          <div className="w-8 h-8 border-2 border-slate-700 border-t-emerald-500 rounded-full animate-spin" />
          <span className="text-slate-500 text-sm">Loading scraper status...</span>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="flex-1 flex items-center justify-center bg-[#050506]">
        <div className="text-center">
          <AlertCircle className="w-12 h-12 mx-auto mb-4 text-slate-600" />
          <p className="text-slate-400">{error}</p>
          <button onClick={loadStatus} className="mt-4 text-sm text-emerald-400 hover:underline">
            Try again
          </button>
        </div>
      </div>
    );
  }

  return (
    <div className="flex-1 flex flex-col overflow-hidden bg-[#050506]">
      {/* Header */}
      <div className="px-6 py-4 border-b border-slate-800 bg-[#0a0a0c] sticky top-0 z-10">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-3">
            <Zap className="w-6 h-6 text-amber-400" />
            <div>
              <h2 className="text-lg font-bold text-white">Scraper Control</h2>
              <p className="text-xs text-slate-500 mt-0.5">
                {status?.implemented?.length || 0} scrapers implemented
              </p>
            </div>
          </div>
          <div className="flex items-center gap-3">
            <button
              onClick={loadStatus}
              className="btn-secondary flex items-center gap-2"
            >
              <RefreshCw className="w-3 h-3" />
              Refresh Status
            </button>
            <button
              onClick={handleRunAllScrapers}
              disabled={runningScrapers.has('all-funds')}
              className="btn-primary flex items-center gap-2"
            >
              {runningScrapers.has('all-funds') ? (
                <Loader2 className="w-4 h-4 animate-spin" />
              ) : (
                <Play className="w-4 h-4" />
              )}
              Run All Scrapers
            </button>
          </div>
        </div>
      </div>

      {/* Error Banner */}
      {scraperErrors.size > 0 && (
        <div className="px-6 py-3 bg-red-900/30 border-b border-red-700/50">
          {Array.from(scraperErrors.entries()).map(([source, message]) => (
            <div key={source} className="flex items-center gap-2 text-sm text-red-300">
              <AlertCircle className="w-4 h-4 flex-shrink-0" />
              <span className="font-medium">{source}:</span>
              <span>{message}</span>
            </div>
          ))}
        </div>
      )}

      {/* Content */}
      <div className="flex-1 overflow-auto p-6 space-y-6">
        {/* Data Sources */}
        <section>
          <button
            onClick={() => setShowSources(!showSources)}
            className="flex items-center gap-2 text-sm font-bold text-white uppercase tracking-wider mb-4 hover:text-emerald-400 transition-colors"
          >
            {showSources ? <ChevronUp className="w-4 h-4" /> : <ChevronDown className="w-4 h-4" />}
            External Data Sources
          </button>

          {showSources && (
            <div className="grid grid-cols-2 gap-4">
              <DataSourceCard
                name="SEC EDGAR"
                description="Form D filings (FREE)"
                icon={<FileText className="w-5 h-5 text-blue-400" />}
                isRunning={runningScrapers.has('sec-edgar')}
                onRun={() => handleRunDataSource('sec-edgar')}
              />
              <DataSourceCard
                name="Brave Search"
                description="News search API ($5/mo)"
                icon={<Search className="w-5 h-5 text-orange-400" />}
                isRunning={runningScrapers.has('brave-search')}
                onRun={() => handleRunDataSource('brave-search')}
              />
              <DataSourceCard
                name="NewsAPI"
                description="News aggregator ($50/mo)"
                icon={<Newspaper className="w-5 h-5 text-purple-400" />}
                isRunning={runningScrapers.has('news-aggregator')}
                onRun={() => handleRunDataSource('news-aggregator')}
              />
              <DataSourceCard
                name="Firecrawl"
                description="JS-heavy PR sites ($19/mo)"
                icon={<Globe className="w-5 h-5 text-red-400" />}
                isRunning={runningScrapers.has('firecrawl')}
                onRun={() => handleRunDataSource('firecrawl')}
              />
              <DataSourceCard
                name="All Sources"
                description="Run all external sources"
                icon={<Zap className="w-5 h-5 text-emerald-400" />}
                isRunning={runningScrapers.has('all-sources')}
                onRun={() => handleRunDataSource('all-sources')}
                primary
              />
              <DataSourceCard
                name="Historical Backfill"
                description="3 months of news data"
                icon={<Clock className="w-5 h-5 text-slate-400" />}
                isRunning={runningScrapers.has('backfill')}
                onRun={() => handleRunDataSource('backfill')}
              />
            </div>
          )}
        </section>

        {/* Fund Scrapers */}
        <section>
          <button
            onClick={() => setShowFunds(!showFunds)}
            className="flex items-center gap-2 text-sm font-bold text-white uppercase tracking-wider mb-4 hover:text-emerald-400 transition-colors"
          >
            {showFunds ? <ChevronUp className="w-4 h-4" /> : <ChevronDown className="w-4 h-4" />}
            Fund Website Scrapers [{TRACKED_FUNDS.length}]
          </button>

          {showFunds && (
            <div className="grid grid-cols-3 gap-3">
              {TRACKED_FUNDS.map((fund) => {
                const isImplemented = status?.implemented?.includes(fund.slug);
                const isRunning = runningScrapers.has(fund.slug);

                return (
                  <FundScraperCard
                    key={fund.slug}
                    name={fund.name}
                    slug={fund.slug}
                    isImplemented={isImplemented}
                    isRunning={isRunning}
                    onRun={() => handleRunScraper(fund.slug)}
                  />
                );
              })}
            </div>
          )}
        </section>
      </div>

      {/* Footer Stats */}
      <div className="px-6 py-3 border-t border-slate-800 bg-[#0a0a0c] text-xs text-slate-600 flex justify-between">
        <span>
          {status?.implemented?.length || 0} implemented / {status?.unimplemented?.length || 0}{' '}
          pending
        </span>
        <span>Last refresh: {new Date().toLocaleTimeString()}</span>
      </div>
    </div>
  );
}

interface DataSourceCardProps {
  name: string;
  description: string;
  icon: React.ReactNode;
  isRunning: boolean;
  onRun: () => void;
  primary?: boolean;
}

function DataSourceCard({
  name,
  description,
  icon,
  isRunning,
  onRun,
  primary,
}: DataSourceCardProps) {
  return (
    <div
      className={`p-4 rounded border transition-all ${
        primary
          ? 'bg-emerald-500/10 border-emerald-500/30 hover:border-emerald-500/50'
          : 'bg-slate-900/50 border-slate-800 hover:border-slate-700'
      }`}
    >
      <div className="flex items-start justify-between mb-3">
        <div className="flex items-center gap-3">
          {icon}
          <div>
            <div className="font-bold text-white">{name}</div>
            <div className="text-xs text-slate-500">{description}</div>
          </div>
        </div>
      </div>
      <button
        onClick={onRun}
        disabled={isRunning}
        className={`w-full flex items-center justify-center gap-2 py-2 rounded text-sm transition-colors ${
          primary
            ? 'bg-emerald-500 text-white hover:bg-emerald-600 disabled:bg-emerald-500/50'
            : 'bg-slate-800 text-slate-300 hover:bg-slate-700 disabled:bg-slate-800/50'
        }`}
      >
        {isRunning ? (
          <>
            <Loader2 className="w-4 h-4 animate-spin" />
            Running...
          </>
        ) : (
          <>
            <Play className="w-4 h-4" />
            Run Now
          </>
        )}
      </button>
    </div>
  );
}

interface FundScraperCardProps {
  name: string;
  slug: string;
  isImplemented?: boolean;
  isRunning: boolean;
  onRun: () => void;
}

function FundScraperCard({
  name,
  slug,
  isImplemented = true,
  isRunning,
  onRun,
}: FundScraperCardProps) {
  return (
    <div
      className={`p-3 rounded border transition-all ${
        isImplemented
          ? 'bg-slate-900/50 border-slate-800 hover:border-slate-700'
          : 'bg-slate-900/20 border-slate-800/50 opacity-50'
      }`}
    >
      <div className="flex items-center justify-between mb-2">
        <div className="flex items-center gap-2">
          {isImplemented ? (
            <CheckCircle className="w-3 h-3 text-emerald-400" />
          ) : (
            <XCircle className="w-3 h-3 text-slate-600" />
          )}
          <span className="text-sm font-medium text-white truncate">{name}</span>
        </div>
      </div>
      <div className="text-[10px] text-slate-600 mb-2 font-mono">{slug}</div>
      <button
        onClick={onRun}
        disabled={!isImplemented || isRunning}
        className="w-full flex items-center justify-center gap-1 py-1.5 text-xs bg-slate-800 text-slate-400 rounded hover:bg-slate-700 hover:text-white disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
      >
        {isRunning ? (
          <>
            <Loader2 className="w-3 h-3 animate-spin" />
            Running
          </>
        ) : (
          <>
            <Play className="w-3 h-3" />
            Run
          </>
        )}
      </button>
    </div>
  );
}

export default ScraperControl;
