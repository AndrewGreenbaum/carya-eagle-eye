/**
 * NewDeals - Shows unseen deals from the last 30 days
 * Each unseen deal has a slowly flashing blue dot that disappears on hover/keyboard selection
 */

import { useState, useEffect, useCallback } from 'react';

const API_BASE = import.meta.env.VITE_API_URL || 'https://bud-tracker-backend-production.up.railway.app';
const API_KEY = import.meta.env.VITE_API_KEY || 'dev-key';

const SEEN_KEY = 'carya-seen-deals';

interface Deal {
  id: number;
  startup_name: string;
  round_type: string;
  amount_usd?: number;
  lead_investor?: string;
  is_lead: boolean;
  is_enterprise_ai: boolean;
  enterprise_category?: string;
  created_at: string;
}

interface DealsResponse {
  deals: Deal[];
  total: number;
}

function getSeen(): Set<string> {
  try {
    const raw = localStorage.getItem(SEEN_KEY);
    if (raw) return new Set(JSON.parse(raw));
  } catch {}
  return new Set();
}

function saveSeen(seen: Set<string>) {
  localStorage.setItem(SEEN_KEY, JSON.stringify([...seen]));
}

// Filter out false positives - fund names extracted as company names
function isLikelyFundName(name: string): boolean {
  // Pattern: ends with "Fund" followed by optional roman numerals/numbers
  // e.g., "SP-1216 Fund I", "AU-0707 Fund III", "Feld Ventures Fund, LP"
  if (/\bfund\s*(i{1,3}|iv|v|vi{0,3}|[0-9]+)?\s*,?\s*(lp|llc|llp)?$/i.test(name)) return true;
  // Contains "Fund" + LP/LLC at end
  if (/\bfund.*\b(lp|llc|llp)$/i.test(name)) return true;
  // Ends with ", LP" or ", LLC" (typical fund structure)
  if (/,\s*(lp|llc|llp)$/i.test(name)) return true;
  // Looks like a fund code (letters + numbers + "Fund")
  if (/^[A-Z]{2,}-\d+\s+fund/i.test(name)) return true;
  return false;
}

function formatAmount(amount: number | undefined): string {
  if (!amount) return '';
  if (amount >= 1_000_000_000) return `$${(amount / 1_000_000_000).toFixed(1)}B`;
  if (amount >= 1_000_000) return `$${(amount / 1_000_000).toFixed(0)}M`;
  if (amount >= 1_000) return `$${(amount / 1_000).toFixed(0)}K`;
  return `$${amount}`;
}

export function NewDeals() {
  const [deals, setDeals] = useState<Deal[]>([]);
  const [seen, setSeen] = useState<Set<string>>(getSeen);
  const [selectedIdx, setSelectedIdx] = useState(-1);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const loadDeals = useCallback(async () => {
    try {
      const res = await fetch(`${API_BASE}/deals?days=30&limit=200`, {
        headers: { 'X-API-Key': API_KEY },
      });
      if (!res.ok) throw new Error('Failed to fetch deals');
      const data: DealsResponse = await res.json();

      // Filter out seen deals and fund names
      const unseenDeals = (data.deals || [])
        .filter((d: Deal) => !getSeen().has(String(d.id)))
        .filter((d: Deal) => !isLikelyFundName(d.startup_name));

      setDeals(unseenDeals);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load');
    } finally {
      setLoading(false);
    }
  }, []);

  // Initial load + 5-minute polling
  useEffect(() => {
    loadDeals();
    const interval = setInterval(loadDeals, 5 * 60 * 1000);
    return () => clearInterval(interval);
  }, [loadDeals]);

  const markSeen = useCallback((dealId: string) => {
    setSeen((prev) => {
      if (prev.has(dealId)) return prev;
      const next = new Set(prev);
      next.add(dealId);
      saveSeen(next);
      return next;
    });
    // Remove the deal from the list
    setDeals((prev) => prev.filter((d) => String(d.id) !== dealId));
  }, []);

  const markAllSeen = useCallback(() => {
    const allIds = new Set([...seen, ...deals.map((d) => String(d.id))]);
    saveSeen(allIds);
    setSeen(allIds);
    setDeals([]);
  }, [deals, seen]);

  // Keyboard navigation
  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      const tag = (e.target as HTMLElement).tagName;
      if (tag === 'INPUT' || tag === 'TEXTAREA') return;
      if (document.querySelector('[aria-modal="true"]')) return;
      if (e.metaKey || e.ctrlKey || e.altKey) return;

      if (e.key === 'j' || e.key === 'ArrowDown') {
        e.preventDefault();
        setSelectedIdx((prev) => {
          const next = prev < deals.length - 1 ? prev + 1 : prev;
          if (next >= 0 && deals[next]) markSeen(String(deals[next].id));
          return next;
        });
      } else if (e.key === 'k' || e.key === 'ArrowUp') {
        e.preventDefault();
        setSelectedIdx((prev) => {
          const next = prev > 0 ? prev - 1 : 0;
          if (deals[next]) markSeen(String(deals[next].id));
          return next;
        });
      } else if (e.key === 'Escape') {
        setSelectedIdx(-1);
      }
    };
    window.addEventListener('keydown', handleKeyDown);
    return () => window.removeEventListener('keydown', handleKeyDown);
  }, [deals, markSeen]);

  const unseenCount = deals.length;

  return (
    <div className="flex flex-col flex-1 min-h-0 font-sans">
      {/* Header */}
      <div className="flex items-baseline justify-between px-6 sm:px-12 pt-10 sm:pt-12 pb-8 sm:pb-10">
        <h1 className="text-sm font-semibold text-zinc-200 tracking-[-0.02em]">New</h1>
        <div className="flex items-center gap-4">
          {unseenCount > 0 && (
            <button
              onClick={markAllSeen}
              className="text-xs text-zinc-500 hover:text-zinc-300 transition-colors"
            >
              Mark all seen
            </button>
          )}
          <span className="text-xs text-zinc-600">
            {unseenCount > 0 ? `${unseenCount} unseen` : 'All caught up'}
          </span>
        </div>
      </div>

      {error && (
        <div className="mx-6 sm:mx-12 mb-4 text-xs text-red-400">{error}</div>
      )}

      {/* Deals list */}
      <div className="flex-1 min-h-0 overflow-auto px-6 sm:px-12 pb-12">
        {deals.length === 0 && !error && !loading && (
          <div className="text-xs text-zinc-700 pt-4">No unseen deals from the last 30 days.</div>
        )}
        <div className="flex flex-col">
          {deals.map((deal, idx) => {
            const isSelected = selectedIdx === idx;
            return (
              <div
                key={deal.id}
                className={`flex items-center gap-3 py-4 px-2 -mx-1 border-b border-zinc-800/40 rounded-md transition-[background] duration-75 cursor-default ${
                  isSelected ? 'bg-zinc-800/15' : 'hover:bg-zinc-800/15'
                }`}
                onClick={() => markSeen(String(deal.id))}
              >
                {/* Flashing dot */}
                <div className="w-4 shrink-0 flex justify-center">
                  <div
                    className="w-1.5 h-1.5 rounded-full bg-blue-500 animate-pulse"
                    style={{ animationDuration: '3s' }}
                  />
                </div>

                {/* Company info */}
                <div className="flex-1 min-w-0">
                  <div className="flex items-baseline gap-3">
                    <span className="text-[15px] font-semibold text-zinc-50 tracking-[-0.02em] truncate">
                      {deal.startup_name}
                    </span>
                    {deal.amount_usd && (
                      <span className="text-[15px] font-medium tabular-nums text-zinc-400 shrink-0">
                        {formatAmount(deal.amount_usd)}
                      </span>
                    )}
                  </div>
                  <div className="flex items-baseline gap-2 mt-1">
                    {deal.round_type && (
                      <span className="text-xs text-zinc-600">{deal.round_type.replace(/_/g, ' ')}</span>
                    )}
                    {deal.lead_investor && (
                      <>
                        <span className="text-zinc-700">&middot;</span>
                        <span className="text-xs text-zinc-500">{deal.lead_investor}</span>
                      </>
                    )}
                  </div>
                </div>

                {/* Badges */}
                <div className="flex items-center gap-2 shrink-0">
                  {deal.is_lead && (
                    <span className="text-[10px] text-emerald-600 uppercase tracking-wider">Lead</span>
                  )}
                  {deal.is_enterprise_ai && (
                    <span className="text-[10px] text-blue-600 uppercase tracking-wider">Ent</span>
                  )}
                </div>
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
}

export default NewDeals;
