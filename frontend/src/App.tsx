/**
 * App.tsx - Main orchestrator for the Bud Tracker Command Center
 *
 * Features:
 * - Dashboard view with stats, filters, and deals table
 * - Stealth Tracker view for silent portfolio additions
 * - Scraper Control view for manual scraper runs
 * - Settings panel for notifications
 * - Live system status indicators
 *
 * Command Center dark theme
 */

import { useState, useEffect, useCallback, useRef } from 'react';
import { ChevronLeft, ChevronRight } from 'lucide-react';
import { CommandHeader } from './components/CommandHeader';
import { CommandSidebar } from './components/CommandSidebar';
import { StatsCards } from './components/StatsCards';
import { DealsTable } from './components/DealsTable';
import { DealModal } from './components/DealModal';
import { FeedbackModal } from './components/FeedbackModal';
import { StealthTracker } from './components/StealthTracker';
import { ScraperControl } from './components/ScraperControl';
import { SettingsPanel } from './components/SettingsPanel';
import { ErrorBoundary } from './components/ErrorBoundary';
import { Tracker } from './components/Tracker';
import { AdminFeedback } from './components/AdminFeedback';
import { TokenUsage } from './components/TokenUsage';
import { MindMapDocs } from './components/MindMapDocs';
import { AircraftTracker } from './components/AircraftTracker';
import { ScansPage } from './components/ScansPage';
import { PreFundingTab } from './components/PreFundingTab';
import {
  fetchDeals,
  fetchFunds,
  fetchHealth,
  fetchScraperStatus,
  fetchSchedulerStatus,
  fetchTokenUsage,
  invalidateCache,
  getExportUrl,
  ApiTimeoutError,
  addDealToTracker,
} from './api/deals';
import type {
  Deal,
  Fund,
  FilterState,
  ViewType,
  SystemStatus,
  DashboardStats,
  PaginatedDeals,
  SortDirection,
} from './types';

const PAGE_SIZE = 50;

// Sample deals fallback
const sampleDeals: Deal[] = [
  {
    id: '1',
    startupName: 'VectorDB Labs',
    investorRoles: ['lead'],
    investmentStage: 'series_a',
    amountInvested: '$45M',
    date: '2025-01-15',
    enterpriseCategory: 'infrastructure',
    isEnterpriseAi: true,
    isAiDeal: true,
    leadInvestor: 'Sequoia Capital',
    leadPartner: 'Roelof Botha',
    verificationSnippet: 'led by Sequoia Capital',
    sourceName: 'TechCrunch',
  },
  {
    id: '2',
    startupName: 'SecureAI',
    investorRoles: ['lead'],
    investmentStage: 'series_b',
    amountInvested: '$75M',
    date: '2025-02-20',
    enterpriseCategory: 'security',
    isEnterpriseAi: true,
    isAiDeal: true,
    leadInvestor: 'a16z',
    verificationSnippet: 'Series B led by Andreessen Horowitz',
    sourceName: 'Fortune',
  },
  {
    id: '3',
    startupName: 'AgentFlow',
    investorRoles: ['lead', 'non_lead'],
    investmentStage: 'seed',
    amountInvested: '$12M',
    date: '2025-03-01',
    enterpriseCategory: 'agentic',
    isEnterpriseAi: true,
    isAiDeal: true,
    leadInvestor: 'Founders Fund',
    leadPartner: 'Keith Rabois',
    verificationSnippet: 'Founders Fund led the round',
    sourceName: 'SEC EDGAR',
  },
];

function App() {
  // Check for usage dashboard URL (no password)
  if (window.location.pathname === '/usage') {
    return <TokenUsage />;
  }

  // Check for admin page URL
  if (window.location.pathname === '/admin') {
    return <AdminFeedback />;
  }

  // Check for claude docs page URL - renders as visual radial mind map
  if (window.location.pathname === '/claude') {
    return <MindMapDocs />;
  }

  // Check for scans page URL
  if (window.location.pathname === '/scans') {
    return <ScansPage />;
  }

  // View state - persist to URL hash for refresh
  const getInitialView = (): ViewType => {
    const hash = window.location.hash.slice(1);
    const validViews: ViewType[] = ['dashboard', 'tracker', 'prefunding', 'stealth', 'scrapers', 'scans', 'settings'];
    return validViews.includes(hash as ViewType) ? (hash as ViewType) : 'dashboard';
  };
  const [currentView, setCurrentView] = useState<ViewType>(getInitialView);
  const [settingsOpen, setSettingsOpen] = useState(false);
  const [sidebarCollapsed, setSidebarCollapsed] = useState(false);
  const [mobileMenuOpen, setMobileMenuOpen] = useState(false);

  // Data state (deals stored in pagination.deals to avoid redundancy)
  const [funds, setFunds] = useState<Fund[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // Pagination state (includes deals array)
  const [pagination, setPagination] = useState<PaginatedDeals>({
    deals: [],
    total: 0,
    limit: PAGE_SIZE,
    offset: 0,
    hasMore: false,
  });

  // Filter state - default to AI Companies + Enterprise AI + Lead only
  // Option C filter structure: AI Companies Only (master) > Enterprise AI Only (sub-filter)
  const [filters, setFilters] = useState<FilterState>({
    aiDealsOnly: true,        // ON by default - AI Companies Only (master toggle)
    enterpriseAiOnly: true,   // ON by default - Enterprise AI Only (sub-filter)
    leadOnly: true,
    showRejected: false,
    selectedFund: undefined,
    selectedCategory: undefined,
    searchQuery: undefined,
    sortDirection: 'desc',
  });

  // Debounced search query (to avoid API calls on every keystroke)
  const [debouncedSearch, setDebouncedSearch] = useState<string | undefined>(undefined);

  // System status
  const [systemStatus, setSystemStatus] = useState<SystemStatus>({
    secEdgar: { status: 'idle', lastRun: undefined },
    scrapers: { status: 'idle', lastRun: undefined },
    claude: { status: 'idle', lastRun: undefined },
    nextScrape: new Date(Date.now() + 3600000).toISOString(),
  });

  // Stats
  const [stats, setStats] = useState<DashboardStats>({
    newDeals24h: 0,
    newDealsTrend: 0,
    enterpriseAiRatio: 0,
    tokensUsed: '0',
    claudeCalls: 0,
    verificationRate: 100,
  });

  // Modal state
  const [selectedDeal, setSelectedDeal] = useState<Deal | null>(null);
  const [feedbackOpen, setFeedbackOpen] = useState(false);
  const [aircraftTrackerOpen, setAircraftTrackerOpen] = useState(false);


  // Keyboard navigation state
  const [selectedDealIdx, setSelectedDealIdx] = useState<number>(-1);
  const [copiedDeal, setCopiedDeal] = useState<Deal | null>(null);
  const [pasteToast, setPasteToast] = useState<string | null>(null);

  // Ref to save scroll position when modal opens
  const savedScrollPos = useRef<number>(0);

  // Debounce search query
  useEffect(() => {
    const timer = setTimeout(() => {
      setDebouncedSearch(filters.searchQuery);
    }, 300);
    return () => clearTimeout(timer);
  }, [filters.searchQuery]);

  // Sync view to URL hash for refresh persistence
  useEffect(() => {
    window.location.hash = currentView;
  }, [currentView]);

  // Load system status
  const loadSystemStatus = useCallback(async () => {
    try {
      const [health, scraperStatus, schedulerStatus] = await Promise.all([
        fetchHealth().catch(() => null),
        fetchScraperStatus().catch(() => null),
        fetchSchedulerStatus().catch(() => null),
      ]);

      if (health || scraperStatus || schedulerStatus) {
        setSystemStatus((prev) => ({
          ...prev,
          secEdgar: { status: 'live', lastRun: new Date().toISOString() },
          scrapers: {
            status: scraperStatus?.implemented?.length ? 'live' : 'idle',
            lastRun: new Date().toISOString(),
          },
          claude: { status: 'live', lastRun: new Date().toISOString() },
          nextScrape: schedulerStatus?.next_run || prev.nextScrape,
        }));
      }
    } catch (err) {
      console.warn('Failed to load system status:', err);
    }
  }, []);

  // Load funds
  const loadFunds = useCallback(async () => {
    try {
      const fundsData = await fetchFunds();
      setFunds(fundsData);
    } catch (err) {
      console.warn('Failed to load funds:', err);
    }
  }, []);

  // Load deals
  const loadDeals = useCallback(
    async (offset = 0) => {
      setLoading(true);
      try {
        // When showRejected is true, don't apply filters - show everything
        // Option C Filter logic (nested):
        // - AI Companies Only ON + Enterprise AI ON → is_enterprise_ai=true (most restrictive)
        // - AI Companies Only ON + Enterprise AI OFF → is_ai_deal=true (all AI incl consumer)
        // - AI Companies Only OFF → no AI filter (all deals including non-AI)
        const result = await fetchDeals({
          limit: PAGE_SIZE,
          offset,
          is_lead: filters.showRejected ? undefined : (filters.leadOnly ? true : undefined),
          is_ai_deal: filters.showRejected ? undefined :
            (filters.aiDealsOnly && !filters.enterpriseAiOnly ? true : undefined),
          is_enterprise_ai: filters.showRejected ? undefined :
            (filters.aiDealsOnly && filters.enterpriseAiOnly ? true : undefined),
          enterprise_category: filters.selectedCategory,
          fund_slug: filters.selectedFund,
          search: debouncedSearch || undefined,
          sort_direction: filters.sortDirection,
        });

        // Use API results directly - no client-side filtering needed
        // The API already filters based on the parameters we sent
        setPagination({
          deals: result.deals,
          total: result.total,
          limit: PAGE_SIZE,
          offset: result.offset,
          hasMore: result.hasMore,
        });

        // Calculate stats from the results
        // Use total count as "companies scanned" - this is the total in the database
        const totalCompaniesScanned = result.total;
        const enterpriseDeals = result.deals.filter((d) => d.isEnterpriseAi).length;
        const ratio = result.deals.length > 0
          ? Math.round((enterpriseDeals / result.deals.length) * 100)
          : 0;

        // Calculate verification rate (deals with verification snippets)
        const verifiedDeals = result.deals.filter((d) => d.verificationSnippet && d.verificationSnippet.trim().length > 0).length;
        const verificationRate = result.deals.length > 0
          ? Math.round((verifiedDeals / result.deals.length) * 100)
          : 0;

        // Fetch real token usage from API (last 7 days)
        let tokensFormatted = '0';
        let claudeApiCalls = 0;
        try {
          const tokenData = await fetchTokenUsage(7);
          const totalTokens = tokenData.totalTokens;
          tokensFormatted = totalTokens > 1000000
            ? `${(totalTokens / 1000000).toFixed(1)}M`
            : totalTokens > 1000
            ? `${(totalTokens / 1000).toFixed(1)}K`
            : totalTokens.toString();
          claudeApiCalls = tokenData.totalCalls;
        } catch (err) {
          console.warn('Failed to fetch token usage:', err);
          tokensFormatted = 'N/A';
        }

        setStats((prev) => ({
          ...prev,
          newDeals24h: totalCompaniesScanned,
          newDealsTrend: 0, // Clear trend since we're showing total now
          enterpriseAiRatio: ratio,
          verificationRate: verificationRate,
          tokensUsed: tokensFormatted,
          claudeCalls: claudeApiCalls,
        }));

        setError(null);
      } catch (err) {
        console.error('Failed to fetch deals:', err);

        if (err instanceof ApiTimeoutError) {
          setError('Request timed out. Server may be busy with scans. Click Retry to refresh.');
        } else {
          setPagination({
            deals: sampleDeals,
            total: sampleDeals.length,
            limit: PAGE_SIZE,
            offset: 0,
            hasMore: false,
          });
          setError('Failed to load deals. Using sample data.');
        }
      } finally {
        setLoading(false);
      }
    },
    [filters.aiDealsOnly, filters.enterpriseAiOnly, filters.leadOnly, filters.showRejected, filters.selectedCategory, filters.selectedFund, filters.sortDirection, debouncedSearch]
  );

  // Initial load and reload on filter change
  useEffect(() => {
    loadDeals(0);
  }, [loadDeals]);

  // Load funds and system status once on mount
  useEffect(() => {
    loadFunds();
    loadSystemStatus();
  }, [loadFunds, loadSystemStatus]);

  // Refresh system status periodically
  useEffect(() => {
    const interval = setInterval(loadSystemStatus, 60000);
    return () => clearInterval(interval);
  }, [loadSystemStatus]);

  // Global keyboard shortcuts
  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      const tag = (e.target as HTMLElement).tagName;
      // Skip if user is typing in an input or textarea
      if (tag === 'INPUT' || tag === 'TEXTAREA') return;
      // Skip if any modal is open (except for modal-specific shortcuts)
      if (selectedDeal || feedbackOpen || aircraftTrackerOpen || settingsOpen) return;

      // View switching (no modifiers)
      if (!e.metaKey && !e.ctrlKey && !e.altKey) {
        if (e.key === '1') {
          e.preventDefault();
          setCurrentView('dashboard');
          return;
        } else if (e.key === '2') {
          e.preventDefault();
          setCurrentView('tracker');
          return;
        }
      }

      // Dashboard navigation (only when on dashboard)
      if (currentView === 'dashboard') {
        if (e.key === 'j' || e.key === 'ArrowDown') {
          e.preventDefault();
          setSelectedDealIdx((prev) => {
            const maxIdx = pagination.deals.length - 1;
            const newIdx = prev < 0 ? 0 : Math.min(prev + 1, maxIdx);
            // Scroll the new row into view (center it to avoid header overlap)
            requestAnimationFrame(() => {
              const rows = document.querySelectorAll('tr.deal-row');
              if (rows[newIdx]) {
                rows[newIdx].scrollIntoView({ behavior: 'smooth', block: 'center' });
              }
            });
            return newIdx;
          });
        } else if (e.key === 'k' || e.key === 'ArrowUp') {
          e.preventDefault();
          setSelectedDealIdx((prev) => {
            const newIdx = Math.max(prev - 1, 0);
            // Scroll the new row into view (center it to avoid header overlap)
            requestAnimationFrame(() => {
              const rows = document.querySelectorAll('tr.deal-row');
              if (rows[newIdx]) {
                rows[newIdx].scrollIntoView({ behavior: 'smooth', block: 'center' });
              }
            });
            return newIdx;
          });
        } else if (e.key === 'Enter' && selectedDealIdx >= 0) {
          e.preventDefault();
          const deal = pagination.deals[selectedDealIdx];
          if (deal) setSelectedDeal(deal);
        } else if (e.key === 'Escape') {
          e.preventDefault();
          setSelectedDealIdx(-1);
        }
      }

      // Ctrl+V to paste copied deal to tracker
      if ((e.ctrlKey || e.metaKey) && e.key === 'v' && copiedDeal) {
        e.preventDefault();
        addDealToTracker(parseInt(copiedDeal.id), 'watching')
          .then(() => {
            setPasteToast(`Added ${copiedDeal.startupName} to tracker`);
            setTimeout(() => setPasteToast(null), 3000);
            setCopiedDeal(null);
          })
          .catch((err) => {
            console.error('Failed to paste deal:', err);
            setPasteToast('Failed to add to tracker');
            setTimeout(() => setPasteToast(null), 3000);
          });
      }
    };

    window.addEventListener('keydown', handleKeyDown);
    return () => window.removeEventListener('keydown', handleKeyDown);
  }, [
    currentView,
    selectedDeal,
    feedbackOpen,
    aircraftTrackerOpen,
    settingsOpen,
    selectedDealIdx,
    pagination.deals,
    copiedDeal,
  ]);

  // Reset selection when deals change (page change, filter change)
  useEffect(() => {
    setSelectedDealIdx(-1);
  }, [pagination.offset, filters]);

  const handleRefresh = () => {
    invalidateCache();
    loadDeals(0);
    loadSystemStatus();
  };

  const handleExport = () => {
    // Use same Option C filter logic as loadDeals
    const url = getExportUrl({
      is_lead: filters.leadOnly ? true : undefined,
      is_ai_deal: filters.aiDealsOnly && !filters.enterpriseAiOnly ? true : undefined,
      is_enterprise_ai: filters.aiDealsOnly && filters.enterpriseAiOnly ? true : undefined,
      enterprise_category: filters.selectedCategory,
      fund_slug: filters.selectedFund,
      search: debouncedSearch || undefined,
    });
    window.open(url, '_blank');
  };

  const handlePageChange = (newOffset: number) => {
    loadDeals(newOffset);
  };

  const handleSortChange = useCallback((direction: SortDirection) => {
    // Update sort direction in filters - this triggers API refetch via useEffect
    setFilters((prev) => ({ ...prev, sortDirection: direction }));
  }, []);

  // Handle deal click - set both selected deal and index
  const handleDealClick = useCallback((deal: Deal) => {
    const idx = pagination.deals.findIndex((d) => d.id === deal.id);
    if (idx !== -1) {
      setSelectedDealIdx(idx);
    }
    // Save scroll position before opening modal
    const tableContainer = document.querySelector('.overflow-auto.scrollbar-hide');
    if (tableContainer) {
      savedScrollPos.current = tableContainer.scrollTop;
    }
    setSelectedDeal(deal);
  }, [pagination.deals]);

  return (
    <div className="h-screen flex flex-col bg-[#050506] text-slate-300 font-['JetBrains_Mono',monospace]">
      {/* Header */}
      <CommandHeader
        systemStatus={systemStatus}
        onSettingsClick={() => setSettingsOpen(true)}
        onPreFundingClick={() => setCurrentView('prefunding')}
        onMenuClick={() => setMobileMenuOpen(true)}
      />

      {/* Main Layout */}
      <div className="flex-1 flex overflow-hidden relative">
        {/* Sidebar - hidden on mobile (shown via drawer), visible on desktop */}
        <div
          className={`hidden md:block h-full transition-all duration-300 ease-in-out shrink-0 ${
            sidebarCollapsed ? 'w-0 overflow-hidden' : 'w-64'
          }`}
        >
          <div className="w-64 h-full overflow-hidden">
            <CommandSidebar
              filters={filters}
              onFiltersChange={setFilters}
              funds={funds}
              currentView={currentView}
              onViewChange={setCurrentView}
              onOpenFeedback={() => setFeedbackOpen(true)}
              onOpenAircraft={() => setAircraftTrackerOpen(true)}
            />
          </div>
        </div>

        {/* Mobile Sidebar Drawer */}
        <div className="md:hidden">
          <CommandSidebar
            filters={filters}
            onFiltersChange={setFilters}
            funds={funds}
            currentView={currentView}
            onViewChange={setCurrentView}
            onOpenFeedback={() => setFeedbackOpen(true)}
            onOpenAircraft={() => setAircraftTrackerOpen(true)}
            isMobileOpen={mobileMenuOpen}
            onMobileClose={() => setMobileMenuOpen(false)}
          />
        </div>

        {/* Sidebar Toggle Button - hidden on mobile, visible on desktop */}
        <button
          onClick={() => setSidebarCollapsed(!sidebarCollapsed)}
          className={`hidden md:flex absolute top-1/2 -translate-y-1/2 z-20 items-center justify-center w-4 h-8 bg-slate-900/80 hover:bg-slate-700 border border-slate-800 hover:border-slate-600 rounded-r text-slate-600 hover:text-slate-300 opacity-40 hover:opacity-100 transition-all duration-300 ${
            sidebarCollapsed ? 'left-0' : 'left-64'
          }`}
          title={sidebarCollapsed ? 'Show sidebar' : 'Hide sidebar'}
        >
          {sidebarCollapsed ? (
            <ChevronRight className="w-3 h-3" />
          ) : (
            <ChevronLeft className="w-3 h-3" />
          )}
        </button>

        {/* Main Content */}
        <main className="flex-1 flex flex-col overflow-hidden">
          {/* Dashboard View */}
          {currentView === 'dashboard' && (
            <>
              {/* Stats Cards */}
              <StatsCards stats={stats} />

              {/* Scan Progress Bar - shows when scan is running */}
              {(() => {
                if (!systemStatus.nextScrape) return null;
                const nextRun = new Date(systemStatus.nextScrape);
                const now = new Date();
                const isRunning = nextRun.getTime() <= now.getTime();
                return isRunning ? (
                  <div className="px-6 py-2 border-b border-slate-800 bg-[#0a0a0c]">
                    <div className="flex items-center gap-3">
                      <span className="text-xs text-emerald-400 font-medium">SCANNING</span>
                      <div className="flex-1 h-1 bg-slate-800 rounded-full overflow-hidden">
                        <div className="h-full w-1/3 bg-gradient-to-r from-emerald-500 via-emerald-400 to-emerald-500 rounded-full animate-scan-bar" />
                      </div>
                    </div>
                  </div>
                ) : null;
              })()}

              {/* Error Banner */}
              {error && (
                <div className="px-6 py-2 border-b border-amber-500/30 bg-amber-500/10 text-amber-400 text-sm flex items-center justify-between">
                  <span>{error}</span>
                  <button
                    onClick={handleRefresh}
                    className="text-amber-300 hover:text-amber-200 underline"
                  >
                    Retry
                  </button>
                </div>
              )}

              {/* Deals Table */}
              <div className="flex-1 flex flex-col overflow-hidden">
                <ErrorBoundary>
                  <DealsTable
                    deals={pagination.deals}
                    pagination={pagination}
                    onDealClick={handleDealClick}
                    onExport={handleExport}
                    onRefresh={handleRefresh}
                    onPageChange={handlePageChange}
                    isLoading={loading}
                    sortDirection={filters.sortDirection}
                    onSortChange={handleSortChange}
                    showRejected={filters.showRejected}
                    selectedIndex={selectedDealIdx}
                  />
                </ErrorBoundary>
              </div>
            </>
          )}

          {/* Stealth Tracker View */}
          {currentView === 'stealth' && (
            <ErrorBoundary>
              <StealthTracker />
            </ErrorBoundary>
          )}

          {/* Scrapers View */}
          {currentView === 'scrapers' && (
            <ErrorBoundary>
              <ScraperControl />
            </ErrorBoundary>
          )}

          {/* Tracker View */}
          {currentView === 'tracker' && (
            <ErrorBoundary>
              <Tracker />
            </ErrorBoundary>
          )}

          {/* Scans View */}
          {currentView === 'scans' && (
            <ErrorBoundary>
              <ScansPage />
            </ErrorBoundary>
          )}

          {/* Pre-Funding Signals View */}
          {currentView === 'prefunding' && (
            <ErrorBoundary>
              <PreFundingTab />
            </ErrorBoundary>
          )}
        </main>
      </div>

      {/* Deal Modal */}
      {selectedDeal && (
        <DealModal
          deal={selectedDeal}
          onClose={() => {
            setSelectedDeal(null);
            // Restore scroll position after modal closes
            requestAnimationFrame(() => {
              const tableContainer = document.querySelector('.overflow-auto.scrollbar-hide');
              if (tableContainer) {
                tableContainer.scrollTop = savedScrollPos.current;
              }
            });
          }}
          onCopy={setCopiedDeal}
        />
      )}

      {/* Paste Toast */}
      {pasteToast && (
        <div className="fixed bottom-4 right-4 bg-slate-800 text-white px-4 py-2 rounded-lg shadow-lg z-50 animate-fade-in">
          {pasteToast}
        </div>
      )}


      {/* Feedback Modal */}
      {feedbackOpen && (
        <FeedbackModal onClose={() => setFeedbackOpen(false)} />
      )}

      {/* Aircraft Tracker Modal */}
      {aircraftTrackerOpen && (
        <AircraftTracker onClose={() => setAircraftTrackerOpen(false)} />
      )}

      {/* Settings Panel */}
      <SettingsPanel isOpen={settingsOpen} onClose={() => setSettingsOpen(false)} />
    </div>
  );
}

export default App;
