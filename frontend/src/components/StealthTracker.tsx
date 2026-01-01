/**
 * StealthTracker - Monitor portfolio additions without press releases
 *
 * Command Center dark theme
 */

import { useState, useEffect } from 'react';
import { Ghost, ExternalLink, RefreshCw, CheckCircle, AlertCircle, Filter } from 'lucide-react';
import { fetchStealthDetections } from '../api/deals';
import type { StealthDetection } from '../types';
import { TRACKED_FUNDS } from '../types';

export function StealthTracker() {
  const [detections, setDetections] = useState<StealthDetection[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [selectedFund, setSelectedFund] = useState<string | undefined>(undefined);

  const loadDetections = async () => {
    setLoading(true);
    setError(null);
    try {
      const data = await fetchStealthDetections(selectedFund, 100, 0);
      setDetections(data.detections);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    loadDetections();
  }, [selectedFund]);

  const getFundName = (slug: string): string => {
    const fund = TRACKED_FUNDS.find((f) => f.slug === slug);
    return fund?.name || slug;
  };

  return (
    <div className="flex-1 flex flex-col overflow-hidden bg-[#050506]">
      {/* Header */}
      <div className="px-6 py-4 border-b border-slate-800 bg-[#0a0a0c] sticky top-0 z-10">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-3">
            <Ghost className="w-6 h-6 text-purple-400" />
            <div>
              <h2 className="text-lg font-bold text-white">Stealth Tracker</h2>
              <p className="text-xs text-slate-500 mt-0.5">
                Portfolio additions detected without press releases
              </p>
            </div>
          </div>
          <div className="flex items-center gap-3">
            {/* Fund Filter */}
            <div className="relative">
              <select
                value={selectedFund || ''}
                onChange={(e) => setSelectedFund(e.target.value || undefined)}
                className="appearance-none bg-slate-900 border border-slate-700 rounded px-3 py-1.5 pr-8 text-sm text-slate-300 focus:outline-none focus:border-emerald-500"
              >
                <option value="">All Funds</option>
                {TRACKED_FUNDS.map((fund) => (
                  <option key={fund.slug} value={fund.slug}>
                    {fund.name}
                  </option>
                ))}
              </select>
              <Filter className="w-3 h-3 absolute right-2.5 top-1/2 -translate-y-1/2 text-slate-500 pointer-events-none" />
            </div>

            {/* Refresh */}
            <button
              onClick={loadDetections}
              disabled={loading}
              className="btn-secondary flex items-center gap-2"
            >
              <RefreshCw className={`w-3 h-3 ${loading ? 'animate-spin' : ''}`} />
              Refresh
            </button>
          </div>
        </div>
      </div>

      {/* Content */}
      <div className="flex-1 overflow-auto">
        {loading ? (
          <div className="flex items-center justify-center py-20">
            <div className="flex flex-col items-center gap-3">
              <div className="w-8 h-8 border-2 border-slate-700 border-t-emerald-500 rounded-full animate-spin" />
              <span className="text-slate-500 text-sm">Loading detections...</span>
            </div>
          </div>
        ) : error ? (
          <div className="flex items-center justify-center py-20">
            <div className="text-center">
              <AlertCircle className="w-12 h-12 mx-auto mb-4 text-slate-600" />
              <p className="text-slate-400">{error}</p>
              <button
                onClick={loadDetections}
                className="mt-4 text-sm text-emerald-400 hover:underline"
              >
                Try again
              </button>
            </div>
          </div>
        ) : detections.length === 0 ? (
          <div className="flex items-center justify-center py-20">
            <div className="text-center">
              <Ghost className="w-16 h-16 mx-auto mb-4 text-slate-700" />
              <p className="text-slate-400 font-medium">No stealth detections yet</p>
              <p className="text-slate-600 text-sm mt-1">
                Portfolio monitoring will detect new additions automatically
              </p>
            </div>
          </div>
        ) : (
          <div className="divide-y divide-slate-800/50">
            {detections.map((detection) => (
              <DetectionRow
                key={detection.id}
                detection={detection}
                fundName={getFundName(detection.fundSlug)}
              />
            ))}
          </div>
        )}
      </div>

      {/* Footer Stats */}
      <div className="px-6 py-3 border-t border-slate-800 bg-[#0a0a0c] text-xs text-slate-600 flex justify-between">
        <span>{detections.length} detections</span>
        <span>
          {detections.filter((d) => d.isConfirmed).length} confirmed
        </span>
      </div>
    </div>
  );
}

interface DetectionRowProps {
  detection: StealthDetection;
  fundName: string;
}

function DetectionRow({ detection, fundName }: DetectionRowProps) {
  return (
    <div className="px-6 py-4 hover:bg-slate-900/50 transition-colors">
      <div className="flex items-start justify-between gap-4">
        <div className="flex-1 min-w-0">
          {/* Company & Status */}
          <div className="flex items-center gap-3">
            <span className="font-bold text-white">
              {detection.companyName || 'Unknown Company'}
            </span>
            {detection.isConfirmed ? (
              <span className="flex items-center gap-1 px-2 py-0.5 rounded text-xs bg-emerald-500/20 text-emerald-400 border border-emerald-500/30">
                <CheckCircle className="w-3 h-3" />
                Confirmed
              </span>
            ) : (
              <span className="px-2 py-0.5 rounded text-xs bg-amber-500/20 text-amber-400 border border-amber-500/30">
                Pending
              </span>
            )}
          </div>

          {/* Fund & Time */}
          <div className="mt-1 text-sm text-slate-500 flex items-center gap-2">
            <span className="text-purple-400 font-medium">{fundName}</span>
            <span>·</span>
            <span>{formatDate(detection.detectedAt)}</span>
          </div>

          {/* Notes */}
          {detection.notes && (
            <p className="mt-2 text-sm text-slate-400">{detection.notes}</p>
          )}
        </div>

        {/* View Link */}
        <a
          href={detection.detectedUrl}
          target="_blank"
          rel="noopener noreferrer"
          className="flex items-center gap-1 px-3 py-1.5 text-sm text-blue-400 hover:text-blue-300 hover:bg-slate-800 rounded transition-colors"
        >
          <ExternalLink className="w-4 h-4" />
          View
        </a>
      </div>
    </div>
  );
}

function formatDate(dateStr: string): string {
  try {
    // Parse date components to avoid timezone issues with ISO date strings
    // "YYYY-MM-DD" parsed by new Date() is treated as UTC midnight,
    // which can shift to previous day in local timezones behind UTC
    let date: Date;
    const parts = dateStr.split('T')[0].split('-');
    if (parts.length >= 3) {
      const year = parseInt(parts[0], 10);
      const month = parseInt(parts[1], 10) - 1; // JS months are 0-indexed
      const day = parseInt(parts[2], 10);
      date = new Date(year, month, day);
    } else {
      date = new Date(dateStr);
    }

    const now = new Date();
    const diffMs = now.getTime() - date.getTime();
    const diffHours = Math.floor(diffMs / (1000 * 60 * 60));

    if (diffHours < 24) {
      return date.toLocaleTimeString('en-US', {
        hour: '2-digit',
        minute: '2-digit',
      });
    }

    if (diffHours < 48) {
      return 'Yesterday';
    }

    return date.toLocaleDateString('en-US', {
      month: 'short',
      day: 'numeric',
    });
  } catch {
    return dateStr;
  }
}

export default StealthTracker;
