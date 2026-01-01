/**
 * Pre-Funding Detection Tab
 *
 * Displays stealth signals from 5 early-detection sources:
 * - Y Combinator (Demo Day batch companies)
 * - GitHub Trending (dev tools before funding)
 * - Hacker News (Launch HN posts)
 * - LinkedIn Jobs (stealth startup hiring)
 * - Delaware Corps (new tech incorporations)
 *
 * These sources previously produced 0 deals but wasted ~$44/month in LLM calls.
 * Now they go through rule-based scoring instead of Claude extraction.
 */

import { useState, useEffect, useCallback, useMemo } from 'react';
import {
  Radar,
  ExternalLink,
  Eye,
  EyeOff,
  Link2,
  RefreshCw,
  ChevronDown,
  AlertCircle,
  Github,
  Linkedin,
  Building2,
  Newspaper,
  Sparkles,
} from 'lucide-react';
import type {
  StealthSignal,
  StealthSignalStats,
  StealthSignalSource,
} from '../types';
import {
  fetchStealthSignals,
  fetchStealthSignalStats,
  dismissStealthSignal,
  undismissStealthSignal,
} from '../api/deals';

// Source configuration
const SOURCE_CONFIG: Record<string, { label: string; icon: typeof Radar; color: string; description: string }> = {
  hackernews: {
    label: 'Hacker News',
    icon: Newspaper,
    color: 'text-orange-400',
    description: 'Launch HN posts',
  },
  ycombinator: {
    label: 'Y Combinator',
    icon: Sparkles,
    color: 'text-orange-500',
    description: 'Demo Day companies',
  },
  github: {
    label: 'GitHub',
    icon: Github,
    color: 'text-slate-300',
    description: 'Trending repos',
  },
  github_trending: {
    label: 'GitHub Trending',
    icon: Github,
    color: 'text-slate-300',
    description: 'Trending dev tools',
  },
  linkedin: {
    label: 'LinkedIn',
    icon: Linkedin,
    color: 'text-blue-400',
    description: 'Stealth hiring',
  },
  linkedin_jobs: {
    label: 'LinkedIn Jobs',
    icon: Linkedin,
    color: 'text-blue-400',
    description: 'Stealth startup hiring',
  },
  delaware: {
    label: 'Delaware',
    icon: Building2,
    color: 'text-emerald-400',
    description: 'New incorporations',
  },
  delaware_corps: {
    label: 'Delaware Corps',
    icon: Building2,
    color: 'text-emerald-400',
    description: 'Tech company filings',
  },
};

// Format relative time
function formatRelativeTime(isoString: string): string {
  const date = new Date(isoString);
  const now = new Date();
  const diffMs = now.getTime() - date.getTime();
  const diffMins = Math.floor(diffMs / 60000);
  const diffHours = Math.floor(diffMs / 3600000);
  const diffDays = Math.floor(diffMs / 86400000);

  if (diffMins < 60) return `${diffMins}m ago`;
  if (diffHours < 24) return `${diffHours}h ago`;
  if (diffDays < 7) return `${diffDays}d ago`;
  return date.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
}

// Score badge component
function ScoreBadge({ score }: { score: number }) {
  const getColor = () => {
    if (score >= 70) return 'bg-emerald-900/50 text-emerald-400 border-emerald-700/50';
    if (score >= 50) return 'bg-amber-900/50 text-amber-400 border-amber-700/50';
    if (score >= 30) return 'bg-slate-800 text-slate-300 border-slate-700';
    return 'bg-slate-900 text-slate-500 border-slate-800';
  };

  return (
    <span className={`px-2 py-0.5 rounded border text-xs font-medium ${getColor()}`}>
      {score}%
    </span>
  );
}

// Signal chips component
function SignalChips({ signals }: { signals: Record<string, unknown> }) {
  const formatKey = (key: string) => {
    return key
      .replace(/_/g, ' ')
      .replace(/\b\w/g, (c) => c.toUpperCase());
  };

  const formatValue = (value: unknown): string => {
    if (typeof value === 'boolean') return '';
    if (typeof value === 'number') return value.toString();
    if (typeof value === 'string') return value;
    return '';
  };

  const entries = Object.entries(signals).slice(0, 4); // Max 4 chips

  return (
    <div className="flex flex-wrap gap-1">
      {entries.map(([key, value]) => (
        <span
          key={key}
          className="px-1.5 py-0.5 bg-slate-800 rounded text-[10px] text-slate-400"
        >
          {formatKey(key)}
          {formatValue(value) && `: ${formatValue(value)}`}
        </span>
      ))}
      {Object.keys(signals).length > 4 && (
        <span className="px-1.5 py-0.5 text-[10px] text-slate-500">
          +{Object.keys(signals).length - 4} more
        </span>
      )}
    </div>
  );
}

// Source icon component
function SourceIcon({ source }: { source: string }) {
  const config = SOURCE_CONFIG[source] || {
    label: source,
    icon: Radar,
    color: 'text-slate-400',
  };
  const Icon = config.icon;

  return (
    <div className="flex items-center gap-1.5">
      <Icon className={`w-3.5 h-3.5 ${config.color}`} />
      <span className="text-xs text-slate-400">{config.label}</span>
    </div>
  );
}

// Signal row component
function SignalRow({
  signal,
  onDismiss,
  onUndismiss,
}: {
  signal: StealthSignal;
  onDismiss: (id: number) => void;
  onUndismiss: (id: number) => void;
}) {
  const [isExpanded, setIsExpanded] = useState(false);

  return (
    <>
      <tr
        className={`border-b border-slate-800/50 hover:bg-slate-900/50 transition-colors cursor-pointer ${
          signal.dismissed ? 'opacity-50' : ''
        }`}
        onClick={() => setIsExpanded(!isExpanded)}
      >
        <td className="px-4 py-3">
          <div className="flex items-center gap-3">
            <ChevronDown
              className={`w-4 h-4 text-slate-500 transition-transform ${
                isExpanded ? 'rotate-0' : '-rotate-90'
              }`}
            />
            <div>
              <div className="font-medium text-white">{signal.companyName}</div>
              <div className="text-xs text-slate-500 mt-0.5">
                {formatRelativeTime(signal.spottedAt)}
              </div>
            </div>
          </div>
        </td>
        <td className="px-4 py-3">
          <SourceIcon source={signal.source} />
        </td>
        <td className="px-4 py-3 text-center">
          <ScoreBadge score={signal.score} />
        </td>
        <td className="px-4 py-3">
          <SignalChips signals={signal.signals} />
        </td>
        <td className="px-4 py-3">
          <div className="flex items-center gap-2 justify-end">
            {signal.convertedDealId && (
              <span className="flex items-center gap-1 px-2 py-0.5 bg-emerald-900/30 text-emerald-400 rounded text-xs">
                <Link2 className="w-3 h-3" />
                Converted
              </span>
            )}
            <a
              href={signal.sourceUrl}
              target="_blank"
              rel="noopener noreferrer"
              className="p-1.5 text-slate-400 hover:text-white hover:bg-slate-800 rounded transition-colors"
              onClick={(e) => e.stopPropagation()}
              title="View source"
            >
              <ExternalLink className="w-4 h-4" />
            </a>
            <button
              onClick={(e) => {
                e.stopPropagation();
                if (signal.dismissed) {
                  onUndismiss(signal.id);
                } else {
                  onDismiss(signal.id);
                }
              }}
              className={`p-1.5 rounded transition-colors ${
                signal.dismissed
                  ? 'text-slate-500 hover:text-white hover:bg-slate-800'
                  : 'text-slate-400 hover:text-amber-400 hover:bg-amber-900/30'
              }`}
              title={signal.dismissed ? 'Restore signal' : 'Dismiss signal'}
            >
              {signal.dismissed ? <Eye className="w-4 h-4" /> : <EyeOff className="w-4 h-4" />}
            </button>
          </div>
        </td>
      </tr>
      {isExpanded && (
        <tr className="bg-slate-900/30">
          <td colSpan={5} className="px-4 py-3">
            <div className="pl-8 space-y-3">
              {/* Metadata */}
              {Object.keys(signal.metadata).length > 0 && (
                <div>
                  <div className="text-xs text-slate-500 mb-1">Metadata</div>
                  <div className="flex flex-wrap gap-2">
                    {Object.entries(signal.metadata).map(([key, value]) => (
                      <span
                        key={key}
                        className="px-2 py-1 bg-slate-800/50 rounded text-xs text-slate-300"
                      >
                        <span className="text-slate-500">{key}:</span>{' '}
                        {typeof value === 'object' ? JSON.stringify(value) : String(value)}
                      </span>
                    ))}
                  </div>
                </div>
              )}
              {/* All signals */}
              <div>
                <div className="text-xs text-slate-500 mb-1">All Signals</div>
                <div className="flex flex-wrap gap-2">
                  {Object.entries(signal.signals).map(([key, value]) => (
                    <span
                      key={key}
                      className="px-2 py-1 bg-purple-900/30 text-purple-300 rounded text-xs"
                    >
                      {key.replace(/_/g, ' ')}
                      {typeof value !== 'boolean' && `: ${value}`}
                    </span>
                  ))}
                </div>
              </div>
              {/* Source URL */}
              <div>
                <div className="text-xs text-slate-500 mb-1">Source</div>
                <a
                  href={signal.sourceUrl}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="text-xs text-blue-400 hover:text-blue-300 break-all"
                >
                  {signal.sourceUrl}
                </a>
              </div>
            </div>
          </td>
        </tr>
      )}
    </>
  );
}

// Main component
export function PreFundingTab() {
  const [signals, setSignals] = useState<StealthSignal[]>([]);
  const [stats, setStats] = useState<StealthSignalStats | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [sourceFilter, setSourceFilter] = useState<StealthSignalSource | ''>('');
  const [includeDismissed, setIncludeDismissed] = useState(false);
  const minScore = 25; // Fixed minimum score threshold
  // Load signals
  const loadSignals = useCallback(async () => {
    try {
      const [signalsData, statsData] = await Promise.all([
        fetchStealthSignals({
          source: sourceFilter || undefined,
          minScore,
          includeDismissed,
          limit: 200,
        }),
        fetchStealthSignalStats(includeDismissed),
      ]);
      setSignals(signalsData.signals);
      setStats(statsData);
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load signals');
    } finally {
      setLoading(false);
    }
  }, [sourceFilter, includeDismissed]);

  // Initial load
  useEffect(() => {
    loadSignals();
  }, [loadSignals]);

  // Dismiss handler
  const handleDismiss = useCallback(async (signalId: number) => {
    try {
      await dismissStealthSignal(signalId);
      setSignals((prev) =>
        prev.map((s) => (s.id === signalId ? { ...s, dismissed: true } : s))
      );
    } catch (err) {
      console.error('Failed to dismiss signal:', err);
    }
  }, []);

  // Undismiss handler
  const handleUndismiss = useCallback(async (signalId: number) => {
    try {
      await undismissStealthSignal(signalId);
      setSignals((prev) =>
        prev.map((s) => (s.id === signalId ? { ...s, dismissed: false } : s))
      );
    } catch (err) {
      console.error('Failed to undismiss signal:', err);
    }
  }, []);

  // Filter displayed signals
  const displayedSignals = useMemo(() => {
    if (includeDismissed) return signals;
    return signals.filter((s) => !s.dismissed);
  }, [signals, includeDismissed]);

  if (loading) {
    return (
      <div className="flex-1 flex items-center justify-center bg-[#050506]">
        <RefreshCw className="w-6 h-6 text-purple-400 animate-spin" />
      </div>
    );
  }

  if (error) {
    return (
      <div className="flex-1 flex items-center justify-center bg-[#050506]">
        <div className="text-center">
          <AlertCircle className="w-8 h-8 text-red-400 mx-auto mb-2" />
          <p className="text-red-400">{error}</p>
          <button
            onClick={loadSignals}
            className="mt-4 px-4 py-2 bg-slate-800 text-white rounded hover:bg-slate-700"
          >
            Retry
          </button>
        </div>
      </div>
    );
  }

  return (
    <div className="flex-1 flex flex-col overflow-hidden bg-[#050506]">
      {/* Header */}
      <div className="px-6 py-4 border-b border-slate-800 bg-[#0a0a0c]">
        <div className="flex items-center gap-3 mb-4">
          <Radar className="w-5 h-5 text-purple-400" />
          <h2 className="text-lg font-bold text-white">Pre-Funding Signals</h2>
          {stats && (
            <span className="text-xs text-slate-500 ml-2">({stats.total})</span>
          )}
        </div>

        {/* Filters */}
        <div className="flex items-center gap-4">
          <select
            value={sourceFilter}
            onChange={(e) => setSourceFilter(e.target.value as StealthSignalSource | '')}
            className="bg-slate-800 border border-slate-700 rounded px-3 py-1.5 text-sm text-white focus:outline-none focus:border-purple-500"
          >
            <option value="">All Sources</option>
            {Object.entries(SOURCE_CONFIG).map(([key, config]) => (
              <option key={key} value={key}>
                {config.label}
              </option>
            ))}
          </select>

          <label className="flex items-center gap-2 cursor-pointer">
            <input
              type="checkbox"
              checked={includeDismissed}
              onChange={(e) => setIncludeDismissed(e.target.checked)}
              className="accent-purple-500"
            />
            <span className="text-xs text-slate-400">Show dismissed</span>
          </label>

          <div className="ml-auto text-xs text-slate-500">
            {displayedSignals.length} signals
          </div>
        </div>
      </div>

      {/* Table */}
      <div className="flex-1 overflow-auto">
        {displayedSignals.length === 0 ? (
          <div className="flex items-center justify-center h-64">
            <div className="text-center">
              <Radar className="w-12 h-12 text-slate-700 mx-auto mb-3" />
              <p className="text-slate-500">No signals found</p>
              <p className="text-xs text-slate-600 mt-1">
                Signals will appear after the next scheduled scan
              </p>
            </div>
          </div>
        ) : (
          <table className="w-full">
            <thead className="sticky top-0 bg-slate-900 z-10">
              <tr className="text-left text-xs text-slate-500 uppercase tracking-wider">
                <th className="px-4 py-3 font-medium">Company</th>
                <th className="px-4 py-3 font-medium">Source</th>
                <th className="px-4 py-3 font-medium text-center">Score</th>
                <th className="px-4 py-3 font-medium">Signals</th>
                <th className="px-4 py-3 font-medium text-right">Actions</th>
              </tr>
            </thead>
            <tbody>
              {displayedSignals.map((signal) => (
                <SignalRow
                  key={signal.id}
                  signal={signal}
                  onDismiss={handleDismiss}
                  onUndismiss={handleUndismiss}
                />
              ))}
            </tbody>
          </table>
        )}
      </div>

      {/* Footer with source icons */}
      <div className="px-6 py-2 border-t border-slate-800/50 bg-[#0a0a0c]">
        <div className="flex items-center gap-4 text-[10px] text-slate-600">
          <div className="flex items-center gap-1">
            <Sparkles className="w-3 h-3 text-orange-400/60" />
            <span>YC</span>
          </div>
          <div className="flex items-center gap-1">
            <Github className="w-3 h-3 text-slate-500" />
            <span>GitHub</span>
          </div>
          <div className="flex items-center gap-1">
            <Newspaper className="w-3 h-3 text-orange-400/60" />
            <span>HN</span>
          </div>
          <div className="flex items-center gap-1">
            <Linkedin className="w-3 h-3 text-blue-400/60" />
            <span>LinkedIn</span>
          </div>
          <div className="flex items-center gap-1">
            <Building2 className="w-3 h-3 text-emerald-400/60" />
            <span>Delaware</span>
          </div>
        </div>
      </div>
    </div>
  );
}

export default PreFundingTab;
