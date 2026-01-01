/**
 * StatsCards - 3 metric cards for the dashboard
 *
 * Shows:
 * - New Deals (24h) with trend
 * - Enterprise AI Ratio
 * - Tokens/Claude calls
 */

import { memo } from 'react';
import { Zap, PieChart, Code, TrendingUp, TrendingDown } from 'lucide-react';
import type { DashboardStats } from '../types';

interface StatsCardsProps {
  stats: DashboardStats;
}

export const StatsCards = memo(function StatsCards({ stats }: StatsCardsProps) {
  return (
    <div className="hidden md:grid md:grid-cols-2 lg:grid-cols-3 gap-4 p-6 border-b border-slate-800 bg-[#0a0a0c]">
      {/* Total Deals Scanned */}
      <StatCard
        title="Total Deals"
        value={stats.newDeals24h.toString()}
        icon={<Zap className="w-4 h-4 text-yellow-500" />}
        subtitle="Companies scanned"
      />

      {/* Enterprise AI Ratio */}
      <StatCard
        title="Ent. AI Ratio"
        value={`${stats.enterpriseAiRatio}%`}
        icon={<PieChart className="w-4 h-4 text-blue-500" />}
        subtitle="High signal density"
      />

      {/* Tokens Used */}
      <StatCard
        title="Tokens Used"
        value={stats.tokensUsed}
        icon={<Code className="w-4 h-4 text-purple-500" />}
        subtitle={`~${stats.claudeCalls.toLocaleString()} Claude calls`}
      />
    </div>
  );
});

interface StatCardProps {
  title: string;
  value: string;
  icon: React.ReactNode;
  trend?: number;
  trendLabel?: string;
  subtitle?: string;
  subtitleColor?: string;
}

function StatCard({
  title,
  value,
  icon,
  trend,
  trendLabel,
  subtitle,
  subtitleColor = 'text-slate-400',
}: StatCardProps) {
  return (
    <div className="stats-card group">
      <div className="flex justify-between items-start mb-2">
        <span className="text-[10px] font-bold text-slate-500 uppercase tracking-wider">
          {title}
        </span>
        <div className="group-hover:scale-110 transition-transform">{icon}</div>
      </div>
      <div className="text-3xl font-bold text-white">{value}</div>
      {trend !== undefined && trendLabel && (
        <div
          className={`text-[10px] mt-1 flex items-center gap-1 ${
            trend >= 0 ? 'text-emerald-400' : 'text-red-400'
          }`}
        >
          {trend >= 0 ? (
            <TrendingUp className="w-3 h-3" />
          ) : (
            <TrendingDown className="w-3 h-3" />
          )}
          <span>{trendLabel}</span>
        </div>
      )}
      {subtitle && !trend && (
        <div className={`text-[10px] ${subtitleColor} mt-1`}>{subtitle}</div>
      )}
    </div>
  );
}

export default StatsCards;

// Memoized to prevent re-renders when parent state changes
// Only re-renders when stats prop changes
