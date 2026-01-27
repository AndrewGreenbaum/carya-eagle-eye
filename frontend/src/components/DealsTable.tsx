/**
 * DealsTable - Main deals table with company links
 *
 * Features:
 * - Company name with source
 * - Stage badge & Lead fund
 * - Enterprise category with icon
 * - Company links (website + CEO LinkedIn icons)
 * - Detection time
 * - Click to open modal
 */

import React, { memo, useRef, useEffect, useState } from 'react';
import { Globe, Linkedin } from 'lucide-react';
import type { Deal, InvestmentStage, PaginatedDeals, SortDirection } from '../types';
import { STAGE_LABELS } from '../types';
import { DealCard } from './DealCard';

interface DealsTableProps {
  deals: Deal[];
  pagination: PaginatedDeals;
  onDealClick: (deal: Deal) => void;
  onExport: () => void;
  onRefresh: () => void;
  onLoadMore: () => void;
  isLoading?: boolean;
  sortDirection: SortDirection;
  onSortChange: (direction: SortDirection) => void;
  showRejected?: boolean;
  modalOpen?: boolean;
  nextScrape?: string;
  onViewScans?: () => void;
}

export function DealsTable({
  deals,
  pagination,
  onDealClick,
  onExport: _onExport,
  onRefresh: _onRefresh,
  onLoadMore,
  isLoading = false,
  sortDirection: _sortDirection,
  onSortChange: _onSortChange,
  showRejected = false,
  modalOpen = false,
  nextScrape,
  onViewScans,
}: DealsTableProps) {
  void _onExport;
  void _onRefresh;
  void _sortDirection;
  void _onSortChange;

  // Countdown timer state
  const [countdown, setCountdown] = useState<string>('');

  // Update countdown every second
  useEffect(() => {
    if (!nextScrape) return;

    const updateCountdown = () => {
      const now = new Date().getTime();
      const target = new Date(nextScrape).getTime();
      const diff = target - now;

      if (diff <= 0) {
        setCountdown('scanning...');
        return;
      }

      const hours = Math.floor(diff / (1000 * 60 * 60));
      const minutes = Math.floor((diff % (1000 * 60 * 60)) / (1000 * 60));
      const seconds = Math.floor((diff % (1000 * 60)) / 1000);

      if (hours > 0) {
        setCountdown(`${hours}h ${minutes}m`);
      } else if (minutes > 0) {
        setCountdown(`${minutes}m ${seconds}s`);
      } else {
        setCountdown(`${seconds}s`);
      }
    };

    updateCountdown();
    const interval = setInterval(updateCountdown, 1000);
    return () => clearInterval(interval);
  }, [nextScrape]);

  const [selectedIndex, setSelectedIndex] = useState<number>(-1);
  const [toast, setToast] = useState<string | null>(null);
  const containerRef = useRef<HTMLDivElement>(null);
  const rowRefs = useRef<(HTMLDivElement | null)[]>([]);
  const sentinelRef = useRef<HTMLDivElement>(null);
  const loadMoreRef = useRef(onLoadMore);
  loadMoreRef.current = onLoadMore;

  // Refs for stable keyboard handler (avoids re-registering on every state change)
  const selectedIndexRef = useRef(selectedIndex);
  const dealsRef = useRef(deals);
  const onDealClickRef = useRef(onDealClick);
  selectedIndexRef.current = selectedIndex;
  dealsRef.current = deals;
  onDealClickRef.current = onDealClick;

  // Suppress hover during keyboard navigation (scrollIntoView moves rows under cursor)
  const isKeyboardNavRef = useRef(false);

  // Track previous modal state and save scroll position
  const prevModalOpenRef = useRef(modalOpen);
  const savedScrollRef = useRef(0);

  // Save scroll position when modal opens, restore when it closes
  useEffect(() => {
    if (modalOpen && !prevModalOpenRef.current) {
      // Modal just opened - save scroll position
      if (containerRef.current) {
        savedScrollRef.current = containerRef.current.scrollTop;
      }
    } else if (!modalOpen && prevModalOpenRef.current) {
      // Modal just closed - restore scroll position and scroll selected row into view
      requestAnimationFrame(() => {
        if (containerRef.current) {
          containerRef.current.scrollTop = savedScrollRef.current;
        }
        if (selectedIndexRef.current >= 0 && rowRefs.current[selectedIndexRef.current]) {
          rowRefs.current[selectedIndexRef.current]?.scrollIntoView({ block: 'nearest', behavior: 'instant' });
        }
      });
    }
    prevModalOpenRef.current = modalOpen;
  }, [modalOpen]);

  // Scroll selected row into view when selection changes
  useEffect(() => {
    if (selectedIndex === 0 && containerRef.current) {
      // First row: scroll to top so header/column labels stay visible
      containerRef.current.scrollTo({ top: 0, behavior: 'smooth' });
    } else if (selectedIndex >= 0 && rowRefs.current[selectedIndex]) {
      rowRefs.current[selectedIndex]?.scrollIntoView({ block: 'nearest', behavior: 'smooth' });
    }
  }, [selectedIndex]);

  // Keyboard navigation - registered once, reads from refs
  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      // Skip if typing in input
      if (e.target instanceof HTMLInputElement || e.target instanceof HTMLTextAreaElement) return;
      // Skip if any modal is open
      if (document.querySelector('[aria-modal="true"]')) return;

      const key = e.key;
      const deals = dealsRef.current;
      const idx = selectedIndexRef.current;

      // Up / W / ArrowUp
      if (key === 'ArrowUp' || key === 'w' || key === 'W') {
        e.preventDefault();
        isKeyboardNavRef.current = true;
        setSelectedIndex(prev => Math.max(0, prev - 1));
        return;
      }

      // Down / S / ArrowDown
      if (key === 'ArrowDown' || key === 's' || key === 'S') {
        e.preventDefault();
        isKeyboardNavRef.current = true;
        setSelectedIndex(prev => Math.min(deals.length - 1, prev + 1));
        return;
      }

      // Escape - do nothing, just keeps selection
      if (key === 'Escape') {
        return;
      }

      // Enter - open selected deal
      if (key === 'Enter' && idx >= 0 && idx < deals.length) {
        e.preventDefault();
        onDealClickRef.current(deals[idx]);
        return;
      }

      // Ctrl+C / Cmd+C - copy deal data
      if ((e.ctrlKey || e.metaKey) && key === 'c' && idx >= 0 && idx < deals.length) {
        const selection = window.getSelection();
        if (!selection || selection.toString().length === 0) {
          e.preventDefault();
          const deal = deals[idx];
          const stage = STAGE_LABELS[deal.investmentStage] || deal.investmentStage;
          const amount = deal.amountInvested ? formatAmount(deal.amountInvested) : '';
          const investor = deal.leadInvestor || '';
          const category = deal.enterpriseCategory && deal.enterpriseCategory !== 'not_ai'
            ? deal.enterpriseCategory.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase())
            : '';
          const date = deal.date || '';
          const row = [deal.startupName, stage, amount, investor, category, date].join('\t');
          navigator.clipboard.writeText(row);
          setToast(`Copied: ${deal.startupName}`);
          setTimeout(() => setToast(null), 2000);
        }
        return;
      }

    };

    window.addEventListener('keydown', handleKeyDown);
    return () => window.removeEventListener('keydown', handleKeyDown);
  }, []); // Empty deps - handler reads from refs, never re-registers

  // Reset selection only when the list is replaced (filter change), not when appended (load more)
  const firstDealIdRef = useRef(deals[0]?.id);
  useEffect(() => {
    if (deals[0]?.id !== firstDealIdRef.current) {
      setSelectedIndex(-1);
      firstDealIdRef.current = deals[0]?.id;
    }
  }, [deals]);

  // Re-enable hover selection when user moves the mouse
  useEffect(() => {
    const handleMouseMove = () => {
      isKeyboardNavRef.current = false;
    };
    window.addEventListener('mousemove', handleMouseMove);
    return () => window.removeEventListener('mousemove', handleMouseMove);
  }, []);

  // Infinite scroll via intersection observer
  useEffect(() => {
    const sentinel = sentinelRef.current;
    if (!sentinel) return;

    const observer = new IntersectionObserver(
      (entries) => {
        if (entries[0].isIntersecting && pagination.hasMore && !isLoading) {
          loadMoreRef.current();
        }
      },
      { threshold: 0.1 }
    );

    observer.observe(sentinel);
    return () => observer.disconnect();
  }, [pagination.hasMore, isLoading]);

  return (
    <>
      {/* Mobile Card View */}
      <div className="md:hidden flex-1 overflow-auto scrollbar-hide p-4 space-y-3">
        {deals.map((deal) => (
          <DealCard
            key={deal.id}
            deal={deal}
            onClick={() => onDealClick(deal)}
            showRejected={showRejected}
          />
        ))}
        {deals.length === 0 && !isLoading && (
          <div className="py-12 text-center text-zinc-600">
            No deals found matching your filters
          </div>
        )}
        <div ref={sentinelRef} className="h-8" />
        {isLoading && deals.length > 0 && (
          <div className="py-4 text-center text-zinc-700 text-xs">Loading...</div>
        )}
      </div>

      {/* Desktop Table View */}
      <div ref={containerRef} className="hidden md:block flex-1 overflow-auto scrollbar-hide font-sans">
        {/* Page Header */}
        <div className="flex items-baseline justify-between px-6 sm:px-10 pt-8 pb-6">
          <div className="flex items-center gap-2">
            <span className="text-sm font-medium text-slate-300 tracking-wide">DEALS</span>
            <span className="text-sm text-slate-500">({pagination.total})</span>
          </div>
          {countdown && (
            <button
              onClick={onViewScans}
              className="flex items-center gap-2 text-xs text-slate-500 hover:text-slate-300 transition-colors cursor-pointer"
            >
              <span>next scan</span>
              <span className="text-slate-400 font-medium tabular-nums">{countdown}</span>
            </button>
          )}
        </div>

        {/* Column Headers */}
        <div className="grid grid-cols-[1.3fr_1fr_0.7fr_100px_100px] gap-6 px-6 sm:px-10 pb-3 border-b border-slate-700/50">
          <span className="text-[11px] uppercase tracking-wider text-slate-500 font-medium">Company</span>
          <span className="text-[11px] uppercase tracking-wider text-slate-500 font-medium">Lead Investor</span>
          <span className="text-[11px] uppercase tracking-wider text-slate-500 font-medium">Raised</span>
          <span className="text-[11px] uppercase tracking-wider text-slate-500 font-medium text-center">Links</span>
          <span className="text-[11px] uppercase tracking-wider text-slate-500 font-medium text-right">Date</span>
        </div>

        <div>
          {deals.map((deal, index) => (
            <DealRow
              key={deal.id}
              deal={deal}
              onClick={() => onDealClick(deal)}
              showRejected={showRejected}
              isSelected={index === selectedIndex}
              onHover={() => { if (!isKeyboardNavRef.current) setSelectedIndex(index); }}
              ref={(el) => { rowRefs.current[index] = el; }}
            />
          ))}
          {deals.length === 0 && !isLoading && (
            <div className="px-6 sm:px-12 py-12 text-center text-zinc-600">
              No deals found matching your filters
            </div>
          )}
        </div>
        <div ref={sentinelRef} className="h-8" />
        {isLoading && deals.length > 0 && (
          <div className="py-4 text-center text-zinc-700 text-xs">Loading...</div>
        )}
      </div>

      {/* Toast notification */}
      {toast && (
        <div className="fixed bottom-6 right-6 bg-zinc-900 border border-zinc-800/50 text-zinc-200 text-xs px-4 py-2.5 rounded-lg shadow-lg z-50 animate-fade-in">
          {toast}
        </div>
      )}
    </>
  );
}

interface DealRowProps {
  deal: Deal;
  onClick: () => void;
  showRejected?: boolean;
  isSelected?: boolean;
  onHover?: () => void;
}

const DealRow = memo(React.forwardRef<HTMLDivElement, DealRowProps>(function DealRow({ deal, onClick, showRejected = false, isSelected = false, onHover }, ref) {
  const isRejected = !deal.investorRoles.includes('lead');
  const shouldDim = isRejected && !showRejected;

  return (
    <div
      ref={ref}
      onClick={onClick}
      onMouseEnter={onHover}
      className={`grid grid-cols-[1.3fr_1fr_0.7fr_100px_100px] gap-6 items-center px-6 sm:px-10 py-5 cursor-pointer border-b border-slate-800/60 transition-colors hover:bg-slate-800/30 ${
        isSelected ? 'bg-slate-800/40' : ''
      } ${shouldDim ? 'opacity-30 hover:opacity-60' : ''}`}
    >
      {/* Company */}
      <div>
        <div className="text-base font-semibold text-white">
          {deal.startupName}
        </div>
        <div className="text-xs text-slate-500 mt-1 flex items-center gap-1">
          <span className="opacity-70">↗</span>
          {deal.sourceName || 'unknown'}
        </div>
      </div>

      {/* Lead Investor */}
      <div>
        <div className="text-sm font-medium text-slate-200">
          {deal.leadInvestor || 'Unknown'}
        </div>
        <div className="mt-1">
          <StageBadge stage={deal.investmentStage} isRejected={isRejected} />
        </div>
      </div>

      {/* Raised */}
      <div>
        {deal.amountInvested && deal.amountInvested !== 'Undisclosed' ? (
          <span className="text-base font-semibold text-emerald-400 tabular-nums">
            {formatAmount(deal.amountInvested)}
          </span>
        ) : (
          <span className="text-slate-600">—</span>
        )}
      </div>

      {/* Links */}
      <CompanyLinks
        website={deal.companyWebsite}
        ceoLinkedin={deal.founders?.find(f => f.linkedinUrl)?.linkedinUrl}
        shouldDim={shouldDim}
      />

      {/* Date */}
      <div className="text-sm text-slate-400 text-right tabular-nums">
        {formatDate(deal.date)}
      </div>
    </div>
  );
}));

function StageBadge({ stage, isRejected }: { stage: InvestmentStage; isRejected: boolean }) {
  if (isRejected) {
    return <span className="text-[10px] font-semibold uppercase px-2 py-0.5 rounded bg-slate-700 text-slate-400">Unknown</span>;
  }

  const stageStyles: Record<string, string> = {
    seed: 'bg-purple-500/20 text-purple-400 border-purple-500/30',
    series_a: 'bg-cyan-500/20 text-cyan-400 border-cyan-500/30',
    series_b: 'bg-blue-500/20 text-blue-400 border-blue-500/30',
    series_c: 'bg-indigo-500/20 text-indigo-400 border-indigo-500/30',
    series_d: 'bg-violet-500/20 text-violet-400 border-violet-500/30',
    series_e: 'bg-fuchsia-500/20 text-fuchsia-400 border-fuchsia-500/30',
    growth: 'bg-amber-500/20 text-amber-400 border-amber-500/30',
    unknown: 'bg-slate-700 text-slate-400 border-slate-600',
  };

  return (
    <span className={`text-[10px] font-semibold uppercase px-2 py-0.5 rounded border ${stageStyles[stage] || stageStyles.unknown}`}>
      {STAGE_LABELS[stage] || stage}
    </span>
  );
}

function formatAmount(amount: string): string {
  return amount
    .replace(/\bmillion\b/gi, 'M')
    .replace(/\bbillion\b/gi, 'B')
    .replace(/(\d)\s+(M|B)\b/g, '$1$2');
}


function CompanyLinks({
  website,
  ceoLinkedin,
  shouldDim,
}: {
  website?: string;
  ceoLinkedin?: string;
  shouldDim: boolean;
}) {
  const hasWebsite = !!website;
  const hasLinkedin = !!ceoLinkedin;

  const handleClick = (e: React.MouseEvent, url: string) => {
    e.stopPropagation();
    window.open(url, '_blank', 'noopener,noreferrer');
  };

  return (
    <div className={`flex items-center justify-center gap-3 ${shouldDim ? 'opacity-50' : ''}`}>
      <button
        onClick={(e) => hasWebsite && handleClick(e, website!)}
        disabled={!hasWebsite}
        className={`transition-colors ${hasWebsite ? 'cursor-pointer' : 'cursor-default'}`}
        title={hasWebsite ? `Visit ${website}` : 'No website'}
      >
        <Globe className={`w-[18px] h-[18px] transition-colors ${
          hasWebsite
            ? 'text-cyan-500 hover:text-cyan-400'
            : 'text-slate-700'
        }`} />
      </button>
      <button
        onClick={(e) => hasLinkedin && handleClick(e, ceoLinkedin!)}
        disabled={!hasLinkedin}
        className={`transition-colors ${hasLinkedin ? 'cursor-pointer' : 'cursor-default'}`}
        title={hasLinkedin ? 'CEO LinkedIn' : 'No LinkedIn'}
      >
        <Linkedin className={`w-[18px] h-[18px] transition-colors ${
          hasLinkedin
            ? 'text-blue-500 hover:text-blue-400'
            : 'text-slate-700'
        }`} />
      </button>
    </div>
  );
}

function formatDate(dateString: string): string {
  if (!dateString) return 'Unknown';

  // Parse date components to avoid timezone issues with ISO date strings
  // "YYYY-MM-DD" parsed by new Date() is treated as UTC midnight,
  // which can shift to previous day in local timezones behind UTC
  let date: Date;
  const parts = dateString.split('T')[0].split('-');
  if (parts.length >= 3) {
    const year = parseInt(parts[0], 10);
    const month = parseInt(parts[1], 10) - 1; // JS months are 0-indexed
    const day = parseInt(parts[2], 10);
    date = new Date(year, month, day);
  } else {
    date = new Date(dateString);
  }

  const now = new Date();
  const diffMs = now.getTime() - date.getTime();
  const diffHours = Math.floor(diffMs / (1000 * 60 * 60));

  if (diffHours < 24) {
    return date.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit' });
  }

  if (diffHours < 48) {
    return 'Yesterday';
  }

  return date.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
}

export default DealsTable;
