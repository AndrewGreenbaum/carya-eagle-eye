/**
 * CommandHeader - Top status bar with system indicators
 *
 * Shows:
 * - Logo with pulsing indicator
 * - SEC EDGAR status
 * - Scrapers status (18/18 ACTIVE)
 * - Claude 3.5 status
 * - Next scrape countdown (live updating every second)
 *
 * Command Center dark theme
 */

import { useState, useEffect } from 'react';
import { Database, Server, Cpu, Settings, Clock, History, Radar, Menu } from 'lucide-react';
import type { SystemStatus } from '../types';

interface CommandHeaderProps {
  systemStatus: SystemStatus;
  onSettingsClick: () => void;
  onPreFundingClick?: () => void;
  onMenuClick?: () => void;
}

export function CommandHeader({
  systemStatus,
  onSettingsClick,
  onPreFundingClick,
  onMenuClick,
}: CommandHeaderProps) {
  // Live countdown state - updates every second
  const [countdown, setCountdown] = useState<string | null>(null);

  // Calculate countdown to next scrape
  const calculateCountdown = (): string | null => {
    if (!systemStatus.nextScrape) return null;

    const nextRun = new Date(systemStatus.nextScrape);
    const now = new Date();
    const diffMs = nextRun.getTime() - now.getTime();

    if (diffMs <= 0) return 'Running...';

    const hours = Math.floor(diffMs / 3600000);
    const minutes = Math.floor((diffMs % 3600000) / 60000);
    const seconds = Math.floor((diffMs % 60000) / 1000);

    if (hours > 0) {
      return `${hours}h ${minutes.toString().padStart(2, '0')}m ${seconds.toString().padStart(2, '0')}s`;
    }
    return `${minutes}m ${seconds.toString().padStart(2, '0')}s`;
  };

  // Update countdown every second
  useEffect(() => {
    // Initial calculation
    setCountdown(calculateCountdown());

    // Update every second
    const interval = setInterval(() => {
      setCountdown(calculateCountdown());
    }, 1000);

    return () => clearInterval(interval);
  }, [systemStatus.nextScrape]);

  const nextScrape = countdown;

  return (
    <header className="h-14 md:h-16 border-b border-slate-800 bg-[#0a0a0c] flex items-center justify-between px-3 md:px-6 shrink-0 z-20">
      {/* Left: Menu button (mobile) + Logo */}
      <div className="flex items-center gap-2 md:gap-4">
        {/* Hamburger menu - mobile only */}
        {onMenuClick && (
          <button
            onClick={onMenuClick}
            className="md:hidden flex items-center justify-center w-10 h-10 -ml-1 text-slate-400 hover:text-slate-200 hover:bg-slate-800/50 rounded transition-colors"
            aria-label="Toggle menu"
          >
            <Menu className="w-5 h-5" />
          </button>
        )}
        <div className="w-2.5 h-2.5 md:w-3 md:h-3 bg-emerald-500 rounded-full pulse-glow" />
        <h1 className="text-base md:text-xl font-bold tracking-tighter bg-gradient-to-r from-amber-300 via-yellow-200 to-amber-400 bg-clip-text text-transparent animate-shimmer bg-[length:200%_100%]">
          Carya Eagle Eye
        </h1>
      </div>

      {/* Status Indicators - hidden on mobile, visible on md+ */}
      <div className="hidden md:flex gap-4 text-xs font-medium">
        {/* SEC EDGAR */}
        <StatusBadge
          icon={<Database className="w-3 h-3 text-blue-400" />}
          label="SEC EDGAR"
          value={systemStatus.secEdgar.status === 'live' ? 'LIVE STREAM' : 'IDLE'}
          valueColor={systemStatus.secEdgar.status === 'live' ? 'text-emerald-400' : 'text-slate-400'}
        />

        {/* Scrapers */}
        <StatusBadge
          icon={<Server className="w-3 h-3 text-purple-400" />}
          label="SCRAPERS"
          value={systemStatus.scrapers.status === 'live' ? '18/18 ACTIVE' : 'IDLE'}
          valueColor={systemStatus.scrapers.status === 'live' ? 'text-white' : 'text-slate-400'}
        />

        {/* Claude Status */}
        <StatusBadge
          icon={<Cpu className="w-3 h-3 text-orange-400" />}
          label="CLAUDE 3.5"
          value={systemStatus.claude.status.toUpperCase()}
          valueColor={
            systemStatus.claude.status === 'processing'
              ? 'text-orange-400'
              : systemStatus.claude.status === 'error'
              ? 'text-red-400'
              : 'text-white'
          }
        />

        {/* Next Scrape Countdown with animated bar */}
        {nextScrape && (
          <div className="flex items-center gap-2 px-3 py-1.5 bg-slate-900 rounded border border-slate-800">
            <Clock className="w-3 h-3 text-slate-400" />
            <span className="text-slate-400">NEXT:</span>
            <span className={`font-mono ${nextScrape === 'Running...' ? 'text-orange-400 animate-pulse' : 'text-emerald-400'}`}>
              {nextScrape}
            </span>
            <div className="w-16 h-1 bg-slate-800 rounded-full overflow-hidden ml-1">
              <div className="h-full w-1/3 bg-emerald-500 rounded-full animate-scan-bar" />
            </div>
          </div>
        )}

        {/* Action Buttons - Desktop */}
        {onPreFundingClick && (
          <button
            onClick={onPreFundingClick}
            className="flex items-center gap-2 px-2 py-1.5 bg-slate-900/50 rounded border border-slate-800/50 hover:border-amber-500/40 transition-colors text-slate-500 hover:text-amber-400"
            title="Pre-Funding Signals"
          >
            <Radar className="w-3 h-3" />
          </button>
        )}

        <button
          onClick={() => window.location.href = '/scans'}
          className="flex items-center gap-2 px-3 py-1.5 bg-slate-900 rounded border border-slate-800 hover:border-cyan-500/50 transition-colors text-slate-400 hover:text-cyan-400"
          title="View scan history"
        >
          <History className="w-3 h-3" />
        </button>

        <button
          onClick={onSettingsClick}
          className="flex items-center gap-2 px-3 py-1.5 bg-slate-900 rounded border border-slate-800 hover:border-emerald-500/50 transition-colors"
          title="Settings"
        >
          <Settings className="w-3 h-3" />
        </button>
      </div>

      {/* Mobile Action Buttons - visible only on mobile */}
      <div className="flex md:hidden items-center gap-1">
        <button
          onClick={onSettingsClick}
          className="flex items-center justify-center w-10 h-10 text-slate-400 hover:text-slate-200 hover:bg-slate-800/50 rounded transition-colors"
          title="Settings"
        >
          <Settings className="w-4 h-4" />
        </button>
      </div>
    </header>
  );
}

interface StatusBadgeProps {
  icon: React.ReactNode;
  label: string;
  value: string;
  valueColor: string;
}

function StatusBadge({ icon, label, value, valueColor }: StatusBadgeProps) {
  return (
    <div className="flex items-center gap-2 px-3 py-1.5 bg-slate-900 rounded border border-slate-800">
      {icon}
      <span className="text-slate-400">{label}:</span>
      <span className={valueColor}>{value}</span>
    </div>
  );
}

export default CommandHeader;
