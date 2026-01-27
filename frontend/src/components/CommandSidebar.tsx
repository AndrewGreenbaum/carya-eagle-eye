/**
 * CommandSidebar - Minimal left panel with navigation, filters, and funds
 */

import { useState, useEffect } from 'react';
import { X, ChevronDown, ChevronUp, Mail } from 'lucide-react';
import type { FilterState, EnterpriseCategory, Fund, ViewType } from '../types';

const NAV_VIEWS: ViewType[] = ['dashboard', 'tracker', 'new'];
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
  onOpenAircraft: _onOpenAircraft,
  isMobileOpen = false,
  onMobileClose,
}: CommandSidebarProps) {
  void _onOpenAircraft;
  const [showCategoryDropdown, setShowCategoryDropdown] = useState(false);
  const [showFunds, setShowFunds] = useState(false);
  const [showShortcuts, setShowShortcuts] = useState(false);

  // Close shortcuts popover on any outside click
  useEffect(() => {
    if (!showShortcuts) return;
    const close = () => setShowShortcuts(false);
    // Delay to avoid closing immediately from the same click that opened it
    const timer = setTimeout(() => document.addEventListener('click', close), 0);
    return () => {
      clearTimeout(timer);
      document.removeEventListener('click', close);
    };
  }, [showShortcuts]);

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

    if (key === 'aiDealsOnly' && filters.aiDealsOnly) {
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

      <aside
        className={`
          w-64 h-full bg-black border-r border-slate-800/15 flex flex-col shrink-0
          md:relative md:translate-x-0
          fixed top-0 left-0 z-50 transition-transform duration-200 ease-in-out
          ${isMobileOpen ? 'translate-x-0' : '-translate-x-full md:translate-x-0'}
        `}
      >
        {/* Mobile close button */}
        <div className="md:hidden flex items-center justify-between px-6 py-3">
          <span className="text-sm font-medium text-slate-300">Menu</span>
          <button
            onClick={onMobileClose}
            className="w-10 h-10 flex items-center justify-center text-slate-400 hover:text-slate-200 rounded transition-colors"
            aria-label="Close menu"
          >
            <X className="w-5 h-5" />
          </button>
        </div>

        {/* Scrollable content */}
        <div className="flex-1 min-h-0 overflow-y-auto scrollbar-hide px-6">
          {/* Brand */}
          <div className="flex items-center gap-2.5 pt-6 pb-10">
            <div className="w-2 h-2 bg-slate-400 rounded-full animate-pulse shadow-[0_0_6px_rgba(148,163,184,0.6)]" style={{ animationDuration: '4s' }} />
            <span className="text-[11px] font-light tracking-[0.2em] uppercase text-slate-400 font-mono">
              Carya Eagle Eye
            </span>
          </div>

          {/* Navigation */}
          <nav className="flex flex-col gap-1 pb-10">
            {NAV_VIEWS.map((view) => (
              <NavItem
                key={view}
                label={view.charAt(0).toUpperCase() + view.slice(1)}
                active={currentView === view}
                onClick={() => handleViewChange(view)}
              />
            ))}
          </nav>

          {/* Search */}
          <div className="pb-8">
            <div className="relative">
              <input
                type="text"
                placeholder="Search"
                value={filters.searchQuery || ''}
                onChange={(e) => onFiltersChange({ ...filters, searchQuery: e.target.value })}
                className="w-full bg-transparent border-b border-slate-800 py-2 text-sm text-slate-200 placeholder:text-slate-600 focus:outline-none focus:border-slate-500"
              />
              {filters.searchQuery && (
                <button
                  onClick={() => onFiltersChange({ ...filters, searchQuery: '' })}
                  className="absolute right-0 top-1/2 -translate-y-1/2 text-slate-600 hover:text-slate-300"
                >
                  <X className="w-3.5 h-3.5" />
                </button>
              )}
            </div>
          </div>

          {/* Filters */}
          <div className="pb-10">
            <div className="text-[10px] uppercase text-slate-600 font-semibold mb-4 tracking-widest">
              Filters
            </div>
            <div className="space-y-4">
              <FilterCheckbox
                label="AI Companies Only"
                checked={filters.aiDealsOnly}
                onChange={() => toggleFilter('aiDealsOnly')}
              />
              {filters.aiDealsOnly && (
                <div className="ml-6">
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

              {/* Category */}
              <div className="pt-3">
                <button
                  onClick={() => setShowCategoryDropdown(!showCategoryDropdown)}
                  className="flex items-center gap-2 py-1 cursor-pointer min-h-[44px] md:min-h-0 transition-colors"
                >
                  <div className={`w-1 h-1 rounded-full transition-colors ${filters.selectedCategory ? 'bg-emerald-500' : 'bg-transparent'}`} />
                  <span className={`text-xs transition-colors ${filters.selectedCategory ? 'text-white' : 'text-slate-600 hover:text-slate-400'}`}>
                    {filters.selectedCategory
                      ? CATEGORY_LABELS[filters.selectedCategory]
                      : 'All Categories'}
                  </span>
                </button>

                {showCategoryDropdown && (
                  <div className="ml-3 mt-1 space-y-0.5">
                    <button
                      onClick={() => setCategory(undefined)}
                      className={`block text-xs py-1 transition-colors ${
                        !filters.selectedCategory ? 'text-white' : 'text-slate-600 hover:text-slate-400'
                      }`}
                    >
                      All
                    </button>
                    {(Object.keys(CATEGORY_LABELS) as EnterpriseCategory[])
                      .filter((c) => c !== 'not_ai')
                      .map((category) => (
                        <button
                          key={category}
                          onClick={() => setCategory(category)}
                          className={`block text-xs py-1 transition-colors ${
                            filters.selectedCategory === category
                              ? 'text-white'
                              : 'text-slate-600 hover:text-slate-400'
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

          {/* Funds */}
          <div className="pb-6">
            <button
              onClick={() => setShowFunds(!showFunds)}
              className="flex items-center justify-between w-full mb-2"
            >
              <div className="text-[10px] uppercase text-slate-600 font-semibold tracking-widest">
                Funds {filters.selectedFund && `(1)`}
              </div>
              <div className="flex items-center gap-2">
                {filters.selectedFund && (
                  <span
                    onClick={(e) => { e.stopPropagation(); setFund(undefined); }}
                    className="text-[10px] text-cyan-600 hover:text-cyan-500 cursor-pointer"
                  >
                    Clear
                  </span>
                )}
                {showFunds ? (
                  <ChevronUp className="w-3 h-3 text-slate-600" />
                ) : (
                  <ChevronDown className="w-3 h-3 text-slate-600" />
                )}
              </div>
            </button>
            {showFunds && (
              <ul className="space-y-1 text-sm mt-3" role="listbox" aria-label="Tracked funds filter">
                {displayFunds.map((fund) => (
                  <li key={fund.slug} role="presentation">
                    <button
                      type="button"
                      role="option"
                      aria-selected={fund.slug === filters.selectedFund}
                      onClick={() => setFund(fund.slug === filters.selectedFund ? undefined : fund.slug)}
                      className={`w-full text-left px-2 py-1.5 rounded text-sm transition-colors cursor-pointer ${
                        fund.slug === filters.selectedFund
                          ? 'text-cyan-500'
                          : 'text-slate-500 hover:text-slate-300'
                      } ${!fund.isActive ? 'opacity-50' : ''}`}
                    >
                      {fund.name}
                    </button>
                  </li>
                ))}
              </ul>
            )}
          </div>
        </div>

        {/* Bottom status */}
        <div className="px-6 py-4 shrink-0">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-2">
              <div className="w-2 h-2 bg-purple-500 rounded-full" />
              <span className="text-xs text-slate-600">18 sources active</span>
            </div>
            <div className="flex items-center gap-2 relative">
              <button
                onClick={() => setShowShortcuts(!showShortcuts)}
                className="w-1.5 h-1.5 rounded-full bg-zinc-700 hover:bg-zinc-500 transition-colors"
                title="Keyboard shortcuts"
              />
              <button
                onClick={onOpenFeedback}
                className="text-slate-700 hover:text-slate-400 transition-colors"
                title="Submit feedback"
              >
                <Mail className="w-3 h-3" />
              </button>
              {showShortcuts && (
                <div className="absolute bottom-6 right-0 bg-zinc-900 border border-zinc-800 rounded-md px-3 py-2.5 text-[10px] text-slate-400 whitespace-nowrap z-50 shadow-xl">
                  <div className="text-slate-300 font-medium mb-1.5">Shortcuts</div>
                  <div className="space-y-1 font-mono">
                    <div><span className="text-slate-500">/</span> Search</div>
                    <div><span className="text-slate-500">1</span> Dashboard</div>
                    <div><span className="text-slate-500">2</span> Tracker</div>
                    <div><span className="text-slate-500">3</span> New</div>
                    <div className="border-t border-zinc-800 pt-1 mt-1.5 text-slate-500">Dashboard</div>
                    <div><span className="text-slate-500">W/S</span> Up / Down</div>
                    <div><span className="text-slate-500">Enter</span> Open deal</div>
                    <div><span className="text-slate-500">Ctrl+C</span> Copy name</div>
                    <div className="border-t border-zinc-800 pt-1 mt-1.5 text-slate-500">Tracker</div>
                    <div><span className="text-slate-500">J/K</span> Up / Down</div>
                    <div><span className="text-slate-500">H/L</span> Prev / Next col</div>
                    <div><span className="text-slate-500">Enter</span> Open card</div>
                    <div><span className="text-slate-500">N</span> New card</div>
                    <div><span className="text-slate-500">Shift+H/L</span> Move card</div>
                    <div><span className="text-slate-500">Esc</span> Clear</div>
                  </div>
                </div>
              )}
            </div>
          </div>
        </div>
      </aside>
    </>
  );
}

interface NavItemProps {
  label: string;
  active?: boolean;
  onClick: () => void;
}

function NavItem({ label, active, onClick }: NavItemProps) {
  return (
    <button
      onClick={onClick}
      className={`w-full text-left py-2.5 font-mono text-[13px] transition-colors min-h-[44px] md:min-h-0 focus:outline-none ${
        active
          ? 'text-slate-100 font-medium'
          : 'text-slate-700 hover:text-slate-400'
      }`}
    >
      {label}
    </button>
  );
}

interface FilterToggleProps {
  label: string;
  checked: boolean;
  onChange: () => void;
}

function FilterCheckbox({ label, checked, onChange }: FilterToggleProps) {
  return (
    <button
      onClick={onChange}
      className="flex items-center gap-2 py-1 cursor-pointer min-h-[44px] md:min-h-0 transition-colors"
    >
      <div className={`w-1 h-1 rounded-full transition-colors ${checked ? 'bg-emerald-500' : 'bg-transparent'}`} />
      <span className={`text-xs transition-colors ${checked ? 'text-white' : 'text-slate-600 hover:text-slate-400'}`}>
        {label}
      </span>
    </button>
  );
}

export default CommandSidebar;
