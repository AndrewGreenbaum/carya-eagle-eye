/**
 * StatsCards - Simple page header
 */

import { memo } from 'react';
import type { DashboardStats } from '../types';

interface StatsCardsProps {
  stats: DashboardStats;
}

export const StatsCards = memo(function StatsCards({ stats }: StatsCardsProps) {
  return (
    <div className="px-6 pt-6 pb-6 bg-[#0a0a0c]">
      <h2 className="text-2xl font-semibold text-white tracking-tight">
        Deals
      </h2>
      <p className="text-sm text-slate-500 mt-1">
        {stats.newDeals24h.toLocaleString()} companies tracked
      </p>
    </div>
  );
});

export default StatsCards;
