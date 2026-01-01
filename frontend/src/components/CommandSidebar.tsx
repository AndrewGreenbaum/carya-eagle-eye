/**
 * CommandSidebar - Left panel with filters, fund list, and cost tracker
 *
 * Features:
 * - Nested filter toggles (Option C structure):
 *   - AI Companies Only (master toggle - when OFF shows all deals incl non-AI)
 *     └─ Enterprise AI Only (sub-filter - only visible when AI is ON)
 *   - Lead Investors
 *   - Show Rejected
 * - Tracked funds list with last scraped times
 * - Category dropdown filter
 * - Cost tracker widget
 */

import { useState, useEffect } from 'react';
import { Home, ChevronDown, ChevronUp, Search, X, Kanban, Mail, Plane } from 'lucide-react';
import type { FilterState, EnterpriseCategory, Fund, ViewType } from '../types';
import { CATEGORY_LABELS, TRACKED_FUNDS } from '../types';

interface CommandSidebarProps {
  filters: FilterState;
  onFiltersChange: (filters: FilterState) => void;
  funds: Fund[];
  currentView: ViewType;
  onViewChange: (view: ViewType) => void;
  onOpenFeedback?: () => void;
  onOpenAircraft?: () => void;
  isMobileOpen?: boolean;
  onMobileClose?: () => void;
}

export function CommandSidebar({
  filters,
  onFiltersChange,
  funds,
  currentView,
  onViewChange,
  onOpenFeedback,
  onOpenAircraft,
  isMobileOpen = false,
  onMobileClose,
}: CommandSidebarProps) {
  const [showCategoryDropdown, setShowCategoryDropdown] = useState(false);

  // Close sidebar when navigating on mobile
  const handleViewChange = (view: ViewType) => {
    onViewChange(view);
    onMobileClose?.();
  };

  // Prevent body scroll when mobile sidebar is open
  useEffect(() => {
    if (isMobileOpen) {
      document.body.style.overflow = 'hidden';
    } else {
      document.body.style.overflow = '';
    }
    return () => {
      document.body.style.overflow = '';
    };
  }, [isMobileOpen]);

  const toggleFilter = (key: keyof FilterState) => {
    if (key === 'selectedFund' || key === 'selectedCategory' || key === 'searchQuery' || key === 'sortDirection') return;

    // Option C nested filter logic:
    // - AI Companies Only = master toggle (when OFF, shows all deals incl non-AI)
    // - Enterprise AI Only = sub-filter (only applies when AI Companies is ON)
    // When turning OFF "AI Companies Only", also turn off "Enterprise AI Only"
    if (key === 'aiDealsOnly' && filters.aiDealsOnly) {
      // Turning OFF AI filter - also turn off Enterprise AI sub-filter
      onFiltersChange({
        ...filters,
        aiDealsOnly: false,
        enterpriseAiOnly: false,
      });
    } else {
      onFiltersChange({
        ...filters,
        [key]: !filters[key],
      });
    }
  };

  const setCategory = (category: EnterpriseCategory | undefined) => {
    onFiltersChange({
      ...filters,
      selectedCategory: category,
    });
    setShowCategoryDropdown(false);
  };

  const setFund = (fundSlug: string | undefined) => {
    onFiltersChange({
      ...filters,
      selectedFund: fundSlug,
    });
  };

  // Merge tracked funds with any additional funds from API
  const displayFunds = TRACKED_FUNDS.map((tf) => {
    const apiFund = funds.find((f) => f.slug === tf.slug);
    return {
      ...tf,
      lastScraped: apiFund?.lastScraped,
      isActive: apiFund?.isActive ?? true,
    };
  });

  return (
    <>
      {/* Mobile overlay backdrop */}
      {isMobileOpen && (
        <div
          className="fixed inset-0 bg-black/60 z-40 md:hidden"
          onClick={onMobileClose}
          aria-hidden="true"
        />
      )}

      {/* Sidebar - fixed on mobile, static on desktop */}
      <aside
        className={`
          w-64 h-full bg-[#0a0a0d] border-r border-slate-800/60 flex flex-col shrink-0
          md:relative md:translate-x-0
          fixed top-0 left-0 z-50 transition-transform duration-300 ease-in-out
          ${isMobileOpen ? 'translate-x-0' : '-translate-x-full md:translate-x-0'}
        `}
      >
        {/* Mobile close button */}
        <div className="md:hidden flex items-center justify-between px-4 py-2 border-b border-slate-800/40">
          <span className="text-sm font-semibold text-slate-300">Menu</span>
          <button
            onClick={onMobileClose}
            className="w-11 h-11 flex items-center justify-center text-slate-400 hover:text-slate-200 hover:bg-slate-800/50 rounded transition-colors"
            aria-label="Close menu"
          >
            <X className="w-5 h-5" />
          </button>
        </div>

        {/* Scrollable content area - entire sidebar scrolls */}
        <div className="flex-1 min-h-0 overflow-y-auto">
          {/* Navigation */}
          <nav className="px-5 py-4 border-b border-slate-800/40">
            <div className="space-y-2">
              <NavItem
                icon={<Home className="w-4 h-4" />}
                label="Dashboard"
                active={currentView === 'dashboard'}
                onClick={() => handleViewChange('dashboard')}
              />
              <NavItem
                icon={<Kanban className="w-4 h-4" />}
                label="Tracker"
                active={currentView === 'tracker'}
                onClick={() => handleViewChange('tracker')}
              />
            </div>
          </nav>

        {/* Search */}
        <div className="px-5 py-4 border-b border-slate-800/40">
          <div className="text-[10px] uppercase text-slate-600 font-semibold mb-3 tracking-widest">
            Search
          </div>
          <div className="relative">
            <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-600" />
            <input
              type="text"
              placeholder="Search companies..."
              value={filters.searchQuery || ''}
              onChange={(e) => onFiltersChange({ ...filters, searchQuery: e.target.value })}
              className="w-full bg-slate-900/50 border border-slate-700/50 rounded pl-8 pr-8 py-2 text-sm text-slate-200 placeholder:text-slate-600 focus:outline-none focus:border-cyan-800/60"
            />
            {filters.searchQuery && (
              <button
                onClick={() => onFiltersChange({ ...filters, searchQuery: '' })}
                className="absolute right-2.5 top-1/2 -translate-y-1/2 text-slate-600 hover:text-slate-300"
              >
                <X className="w-4 h-4" />
              </button>
            )}
          </div>
        </div>

        {/* Filters */}
        <div className="px-5 py-4 border-b border-slate-800/40">
          <div className="text-[10px] uppercase text-slate-600 font-semibold mb-4 tracking-widest">
            Filters
          </div>
          <div className="space-y-3">
            {/* Master toggle: AI Companies Only */}
            <FilterCheckbox
              label="AI Companies Only"
              checked={filters.aiDealsOnly}
              onChange={() => toggleFilter('aiDealsOnly')}
            />
            {/* Sub-filter: Enterprise AI Only (nested, only visible when AI is on) */}
            {filters.aiDealsOnly && (
              <div className="ml-4 border-l border-slate-700/50 pl-3">
                <FilterCheckbox
                  label="Enterprise AI Only"
                  checked={filters.enterpriseAiOnly}
                  onChange={() => toggleFilter('enterpriseAiOnly')}
                />
              </div>
            )}
            <FilterCheckbox
              label="Lead Investors"
              checked={filters.leadOnly}
              onChange={() => toggleFilter('leadOnly')}
            />
            <FilterCheckbox
              label="Show Rejected"
              checked={filters.showRejected}
              onChange={() => toggleFilter('showRejected')}
            />

            {/* Category Dropdown */}
            <div className="mt-4 pt-2">
              <button
                onClick={() => setShowCategoryDropdown(!showCategoryDropdown)}
                className="w-full flex items-center justify-between text-sm text-slate-400 hover:text-slate-200 py-3 md:py-2 px-3 bg-slate-900/50 rounded border border-slate-700/50 min-h-[44px] md:min-h-0"
              >
                <span>
                  {filters.selectedCategory
                    ? CATEGORY_LABELS[filters.selectedCategory]
                    : 'All Categories'}
                </span>
                {showCategoryDropdown ? (
                  <ChevronUp className="w-4 h-4" />
                ) : (
                  <ChevronDown className="w-4 h-4" />
                )}
              </button>

              {showCategoryDropdown && (
                <div className="mt-1 bg-slate-900/80 border border-slate-700/50 rounded overflow-hidden">
                  <button
                    onClick={() => setCategory(undefined)}
                    className={`w-full text-left text-xs px-3 py-3 md:py-2 hover:bg-slate-800/50 min-h-[44px] md:min-h-0 ${
                      !filters.selectedCategory ? 'text-cyan-500' : 'text-slate-400'
                    }`}
                  >
                    All Categories
                  </button>
                  {(Object.keys(CATEGORY_LABELS) as EnterpriseCategory[])
                    .filter((c) => c !== 'not_ai')
                    .map((category) => (
                      <button
                        key={category}
                        onClick={() => setCategory(category)}
                        className={`w-full text-left text-xs px-3 py-3 md:py-2 hover:bg-slate-800/50 min-h-[44px] md:min-h-0 ${
                          filters.selectedCategory === category
                            ? 'text-cyan-500'
                            : 'text-slate-400'
                        }`}
                      >
                        {CATEGORY_LABELS[category]}
                      </button>
                    ))}
                </div>
              )}
            </div>
          </div>
        </div>

        {/* Tracked Funds List */}
        <div className="px-5 py-4">
          <div className="flex items-center justify-between mb-4">
            <div className="text-[10px] uppercase text-slate-600 font-semibold tracking-widest">
              Tracked Funds [{displayFunds.length}]
            </div>
            {filters.selectedFund && (
              <button
                onClick={() => setFund(undefined)}
                className="text-[10px] text-cyan-600 hover:text-cyan-500"
              >
                Clear
              </button>
            )}
          </div>
          <ul className="space-y-1.5 text-xs text-slate-400" role="listbox" aria-label="Tracked funds filter">
            {displayFunds.map((fund) => (
              <li key={fund.slug} role="presentation">
                <button
                  type="button"
                  role="option"
                  aria-selected={fund.slug === filters.selectedFund}
                  onClick={() => setFund(fund.slug === filters.selectedFund ? undefined : fund.slug)}
                  onKeyDown={(e) => {
                    if (e.key === 'Enter' || e.key === ' ') {
                      e.preventDefault();
                      setFund(fund.slug === filters.selectedFund ? undefined : fund.slug);
                    }
                  }}
                  className={`w-full flex items-center justify-between px-3 py-3 md:py-2 rounded text-left transition-colors cursor-pointer min-h-[44px] md:min-h-0 ${
                    fund.slug === filters.selectedFund
                      ? 'bg-cyan-950/40 text-cyan-500 border-l-2 border-cyan-700'
                      : 'hover:bg-slate-800/40 hover:text-slate-200'
                  } ${!fund.isActive ? 'opacity-50' : ''}`}
                >
                  <span>{fund.name}</span>
                  {fund.lastScraped && (
                    <span className="text-[9px] text-slate-600">
                      {formatTimeAgo(fund.lastScraped)}
                    </span>
                  )}
                </button>
              </li>
            ))}
          </ul>
        </div>
      </div>

        {/* Footer Actions - fixed at bottom */}
        <div className="px-5 py-4 border-t border-slate-800/40 flex items-center justify-center gap-3 shrink-0">
          <button
            onClick={onOpenAircraft}
            className="p-2 hover:bg-slate-800/50 text-slate-500 hover:text-cyan-500 rounded transition-colors"
            title="Live Aircraft Tracker"
          >
            <Plane className="w-5 h-5" />
          </button>
          <button
            onClick={onOpenFeedback}
            className="p-2 hover:bg-slate-800/50 text-slate-500 hover:text-slate-300 rounded transition-colors"
            title="Submit feedback"
          >
            <Mail className="w-5 h-5" />
          </button>
        </div>
      </aside>
    </>
  );
}

interface NavItemProps {
  icon: React.ReactNode;
  label: string;
  active?: boolean;
  disabled?: boolean;
  onClick: () => void;
}

function NavItem({ icon, label, active, disabled, onClick }: NavItemProps) {
  return (
    <button
      onClick={onClick}
      disabled={disabled}
      className={`w-full flex items-center gap-3 px-3 py-3 md:py-2.5 rounded text-sm transition-colors min-h-[44px] md:min-h-0 ${
        active
          ? 'bg-cyan-950/40 text-cyan-500 border-l-2 border-cyan-700'
          : disabled
          ? 'text-slate-600 cursor-not-allowed'
          : 'text-slate-400 hover:bg-slate-800/40 hover:text-slate-200'
      }`}
    >
      {icon}
      <span>{label}</span>
      {disabled && (
        <span className="ml-auto text-[9px] text-slate-600 uppercase">Soon</span>
      )}
    </button>
  );
}

interface FilterCheckboxProps {
  label: string;
  checked: boolean;
  onChange: () => void;
}

function FilterCheckbox({ label, checked, onChange }: FilterCheckboxProps) {
  return (
    <label className="flex items-center gap-3 text-sm cursor-pointer hover:text-white group min-h-[44px] md:min-h-0 py-1 md:py-0">
      <div
        className={`filter-checkbox ${checked ? 'checked' : 'unchecked'} w-5 h-5 md:w-4 md:h-4`}
        onClick={onChange}
      >
        {checked && '✓'}
      </div>
      <span className="group-hover:translate-x-1 transition-transform">{label}</span>
    </label>
  );
}

function formatTimeAgo(dateString: string): string {
  const date = new Date(dateString);
  const now = new Date();
  const diffMs = now.getTime() - date.getTime();
  const diffMinutes = Math.floor(diffMs / 60000);

  if (diffMinutes < 1) return 'just now';
  if (diffMinutes < 60) return `${diffMinutes}m ago`;

  const diffHours = Math.floor(diffMinutes / 60);
  if (diffHours < 24) return `${diffHours}h ago`;

  const diffDays = Math.floor(diffHours / 24);
  return `${diffDays}d ago`;
}

export default CommandSidebar;
