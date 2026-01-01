/**
 * TokenUsage.tsx - Token Usage Dashboard
 *
 * Visualizes Claude API token consumption with:
 * - Summary stats (total cost, tokens, calls)
 * - Bar chart: Cost breakdown by source
 * - Line chart: Daily cost trends
 *
 * Access via /usage URL (no password required)
 */

import { useState, useEffect, useCallback } from 'react';
import { RefreshCw, DollarSign, Cpu, MessageSquare, TrendingUp } from 'lucide-react';
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  LineChart,
  Line,
  Legend,
} from 'recharts';

const API_BASE = import.meta.env.VITE_API_BASE || 'https://bud-tracker-backend-production.up.railway.app';
const API_KEY = 'dev-key';

interface TokenUsageBySource {
  source: string;
  input_tokens: number;
  output_tokens: number;
  cache_read_tokens: number;
  cache_write_tokens: number;
  total_tokens: number;
  cost_usd: number;
  call_count: number;
}

interface TokenUsageByDay {
  date: string;
  input_tokens: number;
  output_tokens: number;
  cache_read_tokens: number;
  cache_write_tokens: number;
  total_tokens: number;
  cost_usd: number;
  call_count: number;
}

interface TokenUsageResponse {
  period: string;
  start_date: string;
  end_date: string;
  total_input_tokens: number;
  total_output_tokens: number;
  total_cache_read_tokens: number;
  total_cache_write_tokens: number;
  total_tokens: number;
  total_cost_usd: number;
  total_calls: number;
  by_source: TokenUsageBySource[];
  by_day: TokenUsageByDay[];
}

// Format large numbers
function formatNumber(num: number): string {
  if (num >= 1_000_000) {
    return `${(num / 1_000_000).toFixed(1)}M`;
  }
  if (num >= 1_000) {
    return `${(num / 1_000).toFixed(1)}K`;
  }
  return num.toString();
}

// Format currency
function formatCost(cost: number): string {
  return `$${cost.toFixed(2)}`;
}

export function TokenUsage() {
  const [data, setData] = useState<TokenUsageResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [days, setDays] = useState(7);

  const fetchUsage = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const url = `${API_BASE}/usage/tokens?days=${days}`;
      const res = await fetch(url, {
        headers: { 'X-API-Key': API_KEY },
      });
      if (!res.ok) {
        throw new Error(`HTTP ${res.status}: ${res.statusText}`);
      }
      const json = await res.json();
      setData(json);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load');
    }
    setLoading(false);
  }, [days]);

  useEffect(() => {
    fetchUsage();
  }, [fetchUsage]);

  // Prepare chart data
  const sourceChartData = data?.by_source.map(s => ({
    name: s.source.replace(/_/g, ' '),
    cost: s.cost_usd,
    calls: s.call_count,
  })) || [];

  const dailyChartData = data?.by_day.slice().reverse().map(d => ({
    date: d.date.slice(5), // MM-DD format
    cost: d.cost_usd,
    calls: d.call_count,
  })) || [];

  return (
    <div className="min-h-screen bg-black text-green-400 p-8 font-mono">
      <div className="max-w-6xl mx-auto">
        {/* Header */}
        <div className="flex items-center justify-between mb-8">
          <h1 className="text-2xl font-bold text-green-500">
            TOKEN USAGE DASHBOARD
          </h1>
          <div className="flex gap-4">
            <select
              value={days}
              onChange={(e) => setDays(Number(e.target.value))}
              className="bg-gray-900 border border-green-900 rounded px-3 py-2 text-green-400"
            >
              <option value={7}>Last 7 days</option>
              <option value={14}>Last 14 days</option>
              <option value={30}>Last 30 days</option>
              <option value={60}>Last 60 days</option>
            </select>
            <button
              onClick={fetchUsage}
              className="flex items-center gap-2 px-4 py-2 bg-green-900/30 border border-green-700 rounded hover:bg-green-900/50"
            >
              <RefreshCw className="w-4 h-4" />
              Refresh
            </button>
          </div>
        </div>

        {/* Loading / Error */}
        {loading ? (
          <div className="text-center py-12 text-green-600">Loading usage data...</div>
        ) : error ? (
          <div className="text-center py-12">
            <div className="text-red-400 mb-4">Error: {error}</div>
            <button
              onClick={fetchUsage}
              className="px-4 py-2 bg-green-900/30 border border-green-700 rounded hover:bg-green-900/50"
            >
              Retry
            </button>
          </div>
        ) : data ? (
          <>
            {/* Stats Cards */}
            <div className="grid grid-cols-4 gap-4 mb-8">
              <div className="bg-gray-900 border border-green-900 rounded-lg p-4">
                <div className="flex items-center gap-2 text-green-600 text-sm mb-1">
                  <DollarSign className="w-4 h-4" />
                  Total Cost
                </div>
                <div className="text-3xl font-bold text-green-400">
                  {formatCost(data.total_cost_usd)}
                </div>
              </div>
              <div className="bg-gray-900 border border-green-900 rounded-lg p-4">
                <div className="flex items-center gap-2 text-green-600 text-sm mb-1">
                  <Cpu className="w-4 h-4" />
                  Input Tokens
                </div>
                <div className="text-3xl font-bold text-green-400">
                  {formatNumber(data.total_input_tokens)}
                </div>
              </div>
              <div className="bg-gray-900 border border-green-900 rounded-lg p-4">
                <div className="flex items-center gap-2 text-green-600 text-sm mb-1">
                  <MessageSquare className="w-4 h-4" />
                  Output Tokens
                </div>
                <div className="text-3xl font-bold text-green-400">
                  {formatNumber(data.total_output_tokens)}
                </div>
              </div>
              <div className="bg-gray-900 border border-green-900 rounded-lg p-4">
                <div className="flex items-center gap-2 text-green-600 text-sm mb-1">
                  <TrendingUp className="w-4 h-4" />
                  API Calls
                </div>
                <div className="text-3xl font-bold text-green-400">
                  {formatNumber(data.total_calls)}
                </div>
              </div>
            </div>

            {/* No data message */}
            {data.total_calls === 0 ? (
              <div className="text-center py-12 text-green-600 border border-green-900 rounded-lg bg-gray-900/50">
                <p className="text-lg mb-2">No token usage data yet</p>
                <p className="text-sm text-green-700">
                  Data will appear after the next scheduled scan runs
                </p>
              </div>
            ) : (
              <>
                {/* Cost by Source Chart */}
                <div className="bg-gray-900 border border-green-900 rounded-lg p-6 mb-8">
                  <h2 className="text-lg font-semibold text-green-500 mb-4">COST BY SOURCE</h2>
                  <ResponsiveContainer width="100%" height={300}>
                    <BarChart data={sourceChartData} layout="vertical">
                      <CartesianGrid strokeDasharray="3 3" stroke="#1e3a2f" />
                      <XAxis
                        type="number"
                        stroke="#4ade80"
                        tickFormatter={(v) => `$${v.toFixed(2)}`}
                      />
                      <YAxis
                        type="category"
                        dataKey="name"
                        stroke="#4ade80"
                        width={120}
                        tick={{ fontSize: 12 }}
                      />
                      <Tooltip
                        contentStyle={{
                          backgroundColor: '#0a0a0c',
                          border: '1px solid #166534',
                          borderRadius: '8px',
                        }}
                        labelStyle={{ color: '#4ade80' }}
                        formatter={(value) => [`$${Number(value).toFixed(4)}`, 'Cost']}
                      />
                      <Bar dataKey="cost" fill="#10b981" radius={[0, 4, 4, 0]} />
                    </BarChart>
                  </ResponsiveContainer>
                </div>

                {/* Daily Trend Chart */}
                <div className="bg-gray-900 border border-green-900 rounded-lg p-6 mb-8">
                  <h2 className="text-lg font-semibold text-green-500 mb-4">DAILY TREND</h2>
                  <ResponsiveContainer width="100%" height={300}>
                    <LineChart data={dailyChartData}>
                      <CartesianGrid strokeDasharray="3 3" stroke="#1e3a2f" />
                      <XAxis dataKey="date" stroke="#4ade80" />
                      <YAxis
                        yAxisId="left"
                        stroke="#4ade80"
                        tickFormatter={(v) => `$${v.toFixed(2)}`}
                      />
                      <YAxis
                        yAxisId="right"
                        orientation="right"
                        stroke="#3b82f6"
                      />
                      <Tooltip
                        contentStyle={{
                          backgroundColor: '#0a0a0c',
                          border: '1px solid #166534',
                          borderRadius: '8px',
                        }}
                        labelStyle={{ color: '#4ade80' }}
                        formatter={(value, name) => [
                          name === 'cost' ? `$${Number(value).toFixed(4)}` : value,
                          name === 'cost' ? 'Cost' : 'API Calls'
                        ]}
                      />
                      <Legend />
                      <Line
                        yAxisId="left"
                        type="monotone"
                        dataKey="cost"
                        stroke="#10b981"
                        strokeWidth={2}
                        dot={{ fill: '#10b981' }}
                        name="Cost ($)"
                      />
                      <Line
                        yAxisId="right"
                        type="monotone"
                        dataKey="calls"
                        stroke="#3b82f6"
                        strokeWidth={2}
                        dot={{ fill: '#3b82f6' }}
                        name="API Calls"
                      />
                    </LineChart>
                  </ResponsiveContainer>
                </div>

                {/* Source Breakdown Table */}
                <div className="bg-gray-900 border border-green-900 rounded-lg p-6">
                  <h2 className="text-lg font-semibold text-green-500 mb-4">SOURCE BREAKDOWN</h2>
                  <table className="w-full">
                    <thead>
                      <tr className="text-left text-green-600 border-b border-green-900">
                        <th className="pb-2">Source</th>
                        <th className="pb-2 text-right">Calls</th>
                        <th className="pb-2 text-right">Input</th>
                        <th className="pb-2 text-right">Output</th>
                        <th className="pb-2 text-right">Cost</th>
                      </tr>
                    </thead>
                    <tbody>
                      {data.by_source.map((source) => (
                        <tr key={source.source} className="border-b border-green-900/50">
                          <td className="py-2 text-green-400">{source.source}</td>
                          <td className="py-2 text-right text-green-300">{source.call_count}</td>
                          <td className="py-2 text-right text-green-300">{formatNumber(source.input_tokens)}</td>
                          <td className="py-2 text-right text-green-300">{formatNumber(source.output_tokens)}</td>
                          <td className="py-2 text-right text-green-400 font-semibold">
                            {formatCost(source.cost_usd)}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </>
            )}
          </>
        ) : null}

        {/* Back link */}
        <div className="mt-8 text-center">
          <a href="/" className="text-green-600 hover:text-green-400">
            &larr; Back to Dashboard
          </a>
        </div>
      </div>
    </div>
  );
}
