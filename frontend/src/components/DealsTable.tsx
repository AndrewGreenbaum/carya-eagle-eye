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

import React, { memo, useRef, useEffect } from 'react';
import {
  Link,
  FileText,
  Globe,
  Cpu,
  Shield,
  Building2,
  Bot,
  Database,
  Bitcoin,
  DollarSign,
  Heart,
  Wrench,
  Cloud,
  HelpCircle,
  List,
  Download,
  RefreshCw,
  ChevronLeft,
  ChevronRight,
  ArrowUp,
  ArrowDown,
  Linkedin,
} from 'lucide-react';
import type { Deal, EnterpriseCategory, InvestmentStage, PaginatedDeals, SortDirection } from '../types';
import { STAGE_LABELS } from '../types';
import { DealCard } from './DealCard';

interface DealsTableProps {
  deals: Deal[];
  pagination: PaginatedDeals;
  onDealClick: (deal: Deal) => void;
  onExport: () => void;
  onRefresh: () => void;
  onPageChange: (offset: number) => void;
  isLoading?: boolean;
  sortDirection: SortDirection;
  onSortChange: (direction: SortDirection) => void;
  showRejected?: boolean;
  selectedIndex?: number;
  scrollTrigger?: number;
}

export function DealsTable({
  deals,
  pagination,
  onDealClick,
  onExport,
  onRefresh,
  onPageChange,
  isLoading = false,
  sortDirection,
  onSortChange,
  showRejected = false,
  selectedIndex = -1,
  scrollTrigger = 0,
}: DealsTableProps) {
  const currentPage = Math.floor(pagination.offset / pagination.limit) + 1;
  const totalPages = Math.ceil(pagination.total / pagination.limit);

  // Refs for scrolling to selected row
  const rowRefs = useRef<Map<number, HTMLElement>>(new Map());

  // Scroll to selected row when scrollTrigger changes (e.g., modal closes)
  useEffect(() => {
    if (scrollTrigger > 0 && selectedIndex >= 0) {
      // Small delay to ensure scroll happens after modal's focus restoration
      const timer = setTimeout(() => {
        const row = rowRefs.current.get(selectedIndex);
        if (row) {
          row.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
        }
      }, 100);
      return () => clearTimeout(timer);
    }
  }, [scrollTrigger, selectedIndex]);

  return (
    <>
      {/* Header */}
      <div className="px-4 sm:px-6 py-3 border-b border-slate-800 bg-[#0a0a0c] flex flex-col sm:flex-row justify-between items-start sm:items-center gap-2 sm:gap-0">
        <h2 className="text-xs sm:text-sm font-bold text-white uppercase tracking-widest flex items-center gap-2">
          <List className="w-4 h-4" />
          <span className="hidden sm:inline">Latest Extractions</span>
          <span className="sm:hidden">Deals</span>
          <span className="text-slate-500 font-normal">({pagination.total})</span>
        </h2>
        <div className="flex gap-2 w-full sm:w-auto">
          <button
            onClick={onExport}
            className="btn-primary flex items-center justify-center gap-1.5 flex-1 sm:flex-none"
            aria-label="Export deals to CSV"
          >
            <Download className="w-3 h-3" />
            <span className="hidden sm:inline">EXPORT CSV</span>
            <span className="sm:hidden">EXPORT</span>
          </button>
          <button
            onClick={onRefresh}
            disabled={isLoading}
            className="btn-secondary flex items-center justify-center gap-1.5 flex-1 sm:flex-none"
            aria-label={isLoading ? 'Refreshing deals...' : 'Refresh deals'}
          >
            <RefreshCw className={`w-3 h-3 ${isLoading ? 'animate-spin' : ''}`} />
            <span className="hidden sm:inline">REFRESH</span>
            <span className="sm:hidden">{isLoading ? '...' : 'REFRESH'}</span>
          </button>
        </div>
      </div>

      {/* Mobile Card View */}
      <div className="md:hidden flex-1 overflow-auto scrollbar-hide p-4 space-y-3">
        {deals.map((deal, index) => (
          <DealCard
            key={deal.id}
            deal={deal}
            onClick={() => onDealClick(deal)}
            showRejected={showRejected}
            isSelected={index === selectedIndex}
            cardRef={(el) => {
              if (el) rowRefs.current.set(index, el);
              else rowRefs.current.delete(index);
            }}
          />
        ))}
        {deals.length === 0 && (
          <div className="py-12 text-center text-slate-500">
            {isLoading ? 'Loading deals...' : 'No deals found matching your filters'}
          </div>
        )}
      </div>

      {/* Desktop Table View */}
      <div className="hidden md:block flex-1 overflow-auto scrollbar-hide">
        <table className="w-full text-left border-collapse">
          <thead className="bg-[#0a0a0c] sticky top-0 z-10 text-[10px] uppercase font-bold text-slate-500">
            <tr>
              <th scope="col" className="px-6 py-3 border-b border-slate-800">Company / Source</th>
              <th scope="col" className="px-6 py-3 border-b border-slate-800">Stage & Fund</th>
              <th scope="col" className="px-6 py-3 border-b border-slate-800">AI Category</th>
              <th scope="col" className="px-6 py-3 border-b border-slate-800 w-24 text-center">
                Links
              </th>
              <th
                scope="col"
                aria-sort={sortDirection === 'desc' ? 'descending' : 'ascending'}
                className="px-6 py-3 border-b border-slate-800 text-right cursor-pointer hover:bg-slate-800/50 transition-colors select-none"
                onClick={() => onSortChange(sortDirection === 'desc' ? 'asc' : 'desc')}
                onKeyDown={(e) => {
                  if (e.key === 'Enter' || e.key === ' ') {
                    e.preventDefault();
                    onSortChange(sortDirection === 'desc' ? 'asc' : 'desc');
                  }
                }}
                tabIndex={0}
                role="button"
              >
                <span className="flex items-center justify-end gap-1">
                  Announced
                  {sortDirection === 'desc' ? (
                    <ArrowDown className="w-3 h-3 text-emerald-400" aria-hidden="true" />
                  ) : (
                    <ArrowUp className="w-3 h-3 text-emerald-400" aria-hidden="true" />
                  )}
                </span>
              </th>
            </tr>
          </thead>
          <tbody className="text-xs divide-y divide-slate-800/50">
            {deals.map((deal, index) => (
              <DealRow
                key={deal.id}
                deal={deal}
                onClick={() => onDealClick(deal)}
                showRejected={showRejected}
                isSelected={index === selectedIndex}
                rowRef={(el) => {
                  if (el) rowRefs.current.set(index, el);
                  else rowRefs.current.delete(index);
                }}
              />
            ))}
            {deals.length === 0 && (
              <tr>
                <td colSpan={5} className="px-6 py-12 text-center text-slate-500">
                  {isLoading ? 'Loading deals...' : 'No deals found matching your filters'}
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>

      {/* Pagination */}
      {totalPages > 1 && (
        <div className="px-4 sm:px-6 py-3 border-t border-slate-800 bg-[#0a0a0c] flex flex-col sm:flex-row justify-between items-center gap-2">
          <div className="text-xs text-slate-500 order-2 sm:order-1">
            <span className="hidden sm:inline">Showing </span>
            {pagination.offset + 1}-{Math.min(pagination.offset + pagination.limit, pagination.total)}
            <span className="hidden sm:inline"> of</span>
            <span className="sm:hidden">/</span> {pagination.total}
          </div>
          <div className="flex gap-2 order-1 sm:order-2 w-full sm:w-auto justify-between sm:justify-end">
            <button
              onClick={() => onPageChange(Math.max(0, pagination.offset - pagination.limit))}
              disabled={pagination.offset === 0}
              className="btn-secondary flex items-center justify-center gap-1 disabled:opacity-50 min-w-[44px] min-h-[44px] sm:min-w-0 sm:min-h-0"
            >
              <ChevronLeft className="w-4 h-4 sm:w-3 sm:h-3" />
              <span className="hidden sm:inline">Previous</span>
            </button>
            <span className="px-3 py-1.5 text-xs text-slate-400 flex items-center">
              {currentPage}/{totalPages}
            </span>
            <button
              onClick={() => onPageChange(pagination.offset + pagination.limit)}
              disabled={!pagination.hasMore}
              className="btn-secondary flex items-center justify-center gap-1 disabled:opacity-50 min-w-[44px] min-h-[44px] sm:min-w-0 sm:min-h-0"
            >
              <span className="hidden sm:inline">Next</span>
              <ChevronRight className="w-4 h-4 sm:w-3 sm:h-3" />
            </button>
          </div>
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
  rowRef?: (el: HTMLTableRowElement | null) => void;
}

const DealRow = memo(function DealRow({ deal, onClick, showRejected = false, isSelected = false, rowRef }: DealRowProps) {
  // A deal is "rejected" only if not led by a tracked fund
  // AI classification is informational, not a rejection criteria
  const isRejected = !deal.investorRoles.includes('lead');
  // When showRejected is enabled, show all deals with full color (no greying out)
  const shouldDim = isRejected && !showRejected;

  return (
    <tr
      ref={rowRef}
      onClick={onClick}
      className={`deal-row cursor-pointer group ${
        shouldDim ? 'opacity-50 grayscale hover:grayscale-0 hover:opacity-100' : ''
      } ${isSelected ? 'bg-slate-800/50 ring-1 ring-inset ring-blue-500/30' : ''}`}
    >
      {/* Company / Source */}
      <td className="px-6 py-4">
        <div
          className={`text-sm font-bold group-hover:underline underline-offset-4 cursor-pointer ${
            isRejected ? 'text-slate-400 group-hover:text-white' : 'text-emerald-400'
          }`}
        >
          {deal.startupName}
        </div>
        <div className="text-[10px] text-slate-500 mt-0.5 flex items-center gap-1">
          <SourceIcon source={deal.sourceName} />
          {deal.sourceName || 'Unknown Source'}
        </div>
      </td>

      {/* Stage & Fund */}
      <td className="px-6 py-4">
        <div className="flex items-center gap-2 mb-1">
          <StageBadge stage={deal.investmentStage} isRejected={isRejected} />
        </div>
        <div className={`font-bold ${isRejected ? 'text-slate-400' : 'text-white'}`}>
          {deal.leadInvestor || 'Unknown'}
        </div>
      </td>

      {/* AI Category */}
      <td className="px-6 py-4">
        <CategoryBadge category={deal.enterpriseCategory} shouldDim={shouldDim} isRejected={isRejected} />
      </td>

      {/* Company Links */}
      <td className="px-6 py-4">
        <CompanyLinks
          website={deal.companyWebsite}
          ceoLinkedin={deal.founders?.find(f => f.linkedinUrl)?.linkedinUrl}
          shouldDim={shouldDim}
        />
      </td>

      {/* Detected Time */}
      <td className="px-6 py-4 text-right text-slate-500 font-mono">
        {formatDate(deal.date)}
      </td>
    </tr>
  );
});

function SourceIcon({ source }: { source?: string }) {
  if (!source) return <Link className="w-3 h-3" />;

  const lowerSource = source.toLowerCase();
  if (lowerSource.includes('sec') || lowerSource.includes('form d')) {
    return <FileText className="w-3 h-3" />;
  }
  if (lowerSource.includes('blog') || lowerSource.includes('sequoia') || lowerSource.includes('a16z')) {
    return <Globe className="w-3 h-3" />;
  }
  return <Link className="w-3 h-3" />;
}

function StageBadge({ stage, isRejected }: { stage: InvestmentStage; isRejected: boolean }) {
  if (isRejected) {
    return (
      <span className="stage-badge bg-slate-800 text-slate-400 border-slate-700">REJECTED</span>
    );
  }

  return <span className={`stage-badge stage-${stage}`}>{STAGE_LABELS[stage]?.toUpperCase() || stage.toUpperCase()}</span>;
}

const CATEGORY_ICONS: Record<EnterpriseCategory, React.ReactNode> = {
  // Enterprise AI
  infrastructure: <Cpu className="w-3 h-3 text-slate-400" />,
  security: <Shield className="w-3 h-3 text-slate-400" />,
  vertical_saas: <Building2 className="w-3 h-3 text-slate-400" />,
  agentic: <Bot className="w-3 h-3 text-slate-400" />,
  data_intelligence: <Database className="w-3 h-3 text-slate-400" />,
  // Consumer AI
  consumer_ai: <Bot className="w-3 h-3 text-blue-400" />,
  gaming_ai: <Bot className="w-3 h-3 text-purple-400" />,
  social_ai: <Bot className="w-3 h-3 text-pink-400" />,
  // Non-AI (specific categories)
  crypto: <Bitcoin className="w-3 h-3 text-orange-400" />,
  fintech: <DollarSign className="w-3 h-3 text-emerald-400" />,
  healthcare: <Heart className="w-3 h-3 text-rose-400" />,
  hardware: <Wrench className="w-3 h-3 text-gray-400" />,
  saas: <Cloud className="w-3 h-3 text-sky-400" />,
  other: <HelpCircle className="w-3 h-3 text-slate-500" />,
  // Legacy
  not_ai: <HelpCircle className="w-3 h-3 text-slate-500" />,
};

function CategoryBadge({
  category,
  shouldDim,
  isRejected,
}: {
  category?: EnterpriseCategory;
  shouldDim: boolean;
  isRejected?: boolean;
}) {
  const cat = category || 'other';

  // Check if it's a consumer AI category
  const isConsumerAi = cat === 'consumer_ai' || cat === 'gaming_ai' || cat === 'social_ai';
  const isNonAi = ['crypto', 'fintech', 'healthcare', 'hardware', 'saas', 'other', 'not_ai'].includes(cat);

  // Non-AI categories: only show specific label if NOT rejected (is a lead)
  // Rejected non-AI deals just show "other"
  if (isNonAi) {
    const displayCat = (isRejected || cat === 'not_ai') ? 'other' : cat;
    const icon = CATEGORY_ICONS[displayCat] || CATEGORY_ICONS['other'];
    return (
      <div className={`category-badge category-${displayCat}`}>
        {icon}
        {displayCat.replace('_', ' ')}
      </div>
    );
  }

  const icon = CATEGORY_ICONS[cat] || CATEGORY_ICONS['other'];

  // Consumer AI gets a different style
  if (isConsumerAi) {
    return (
      <div className={`category-badge category-consumer ${shouldDim ? 'opacity-75' : ''}`}>
        {icon}
        {cat.replace('_', ' ')}
      </div>
    );
  }

  // Enterprise AI categories
  return (
    <div className={`category-badge category-${cat} ${shouldDim ? 'opacity-75' : ''}`}>
      {icon}
      {cat.replace('_', ' ')}
    </div>
  );
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
    e.stopPropagation(); // Prevent row click
    window.open(url, '_blank', 'noopener,noreferrer');
  };

  return (
    <div className="flex items-center justify-center gap-3">
      {/* Website Icon */}
      <button
        onClick={(e) => hasWebsite && handleClick(e, website!)}
        disabled={!hasWebsite}
        className={`p-1.5 rounded transition-all ${
          hasWebsite
            ? 'hover:bg-slate-700 text-emerald-400 hover:text-emerald-300 cursor-pointer'
            : 'text-slate-700 cursor-not-allowed'
        } ${shouldDim ? 'opacity-50' : ''}`}
        title={hasWebsite ? `Visit ${website}` : 'No website'}
      >
        <Globe className="w-4 h-4" />
      </button>

      {/* LinkedIn Icon */}
      <button
        onClick={(e) => hasLinkedin && handleClick(e, ceoLinkedin!)}
        disabled={!hasLinkedin}
        className={`p-1.5 rounded transition-all ${
          hasLinkedin
            ? 'hover:bg-slate-700 text-blue-400 hover:text-blue-300 cursor-pointer'
            : 'text-slate-700 cursor-not-allowed'
        } ${shouldDim ? 'opacity-50' : ''}`}
        title={hasLinkedin ? 'CEO LinkedIn' : 'No LinkedIn'}
      >
        <Linkedin className="w-4 h-4" />
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
