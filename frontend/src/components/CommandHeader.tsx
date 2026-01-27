/**
 * CommandHeader - Minimal header with logo only (no status badges)
 */

import { Menu } from 'lucide-react';
import type { SystemStatus } from '../types';

interface CommandHeaderProps {
  systemStatus: SystemStatus;
  onSettingsClick: () => void;
  onPreFundingClick?: () => void;
  onMenuClick?: () => void;
}

export function CommandHeader({
  systemStatus: _systemStatus,
  onSettingsClick: _onSettingsClick,
  onPreFundingClick: _onPreFundingClick,
  onMenuClick,
}: CommandHeaderProps) {
  // Keep props for API compatibility
  void _systemStatus;
  void _onSettingsClick;
  void _onPreFundingClick;

  return (
    <header className="h-14 md:h-16 bg-[#0a0a0c] flex items-center px-3 md:px-6 shrink-0 z-20">
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
    </header>
  );
}

export default CommandHeader;
