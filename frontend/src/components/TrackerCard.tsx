/**
 * TrackerCard - A draggable card in the Tracker Kanban
 * Uses @dnd-kit/sortable with distance activation (8px) to prevent click conflicts
 */

import { useSortable } from '@dnd-kit/sortable';
import { CSS } from '@dnd-kit/utilities';
import type { TrackerItem } from '../types';

interface TrackerCardProps {
  item: TrackerItem;
  isDragging?: boolean;
  isSelected?: boolean;
  onClick?: () => void;
}

export function TrackerCard({ item, isDragging, isSelected, onClick }: TrackerCardProps) {
  const {
    attributes,
    listeners,
    setNodeRef,
    transform,
    transition,
    isDragging: isSortableDragging,
  } = useSortable({ id: item.id });

  const style = {
    transform: CSS.Transform.toString(transform),
    transition,
  };

  const isCurrentlyDragging = isDragging || isSortableDragging;

  const displayAmount = formatAmount(item.amount);
  const roundStyle = getRoundStyle(item.roundType);

  return (
    <div
      ref={setNodeRef}
      style={style}
      {...attributes}
      {...listeners}
      role="article"
      aria-label={`${item.companyName}${item.leadInvestor ? `, led by ${item.leadInvestor}` : ''}`}
      className={`
        p-4 mb-3 border border-zinc-700/60 bg-zinc-900/40 cursor-grab rounded-lg select-none
        hover:bg-zinc-800/50 hover:border-zinc-600/60 transition-all duration-100
        ${isCurrentlyDragging ? 'opacity-30 cursor-grabbing' : ''}
        ${isSelected ? 'bg-zinc-800/50 border-zinc-600' : ''}
      `}
      onClick={() => !isCurrentlyDragging && onClick?.()}
    >
      {/* Row 1: Company + Amount */}
      <div className="flex items-baseline justify-between gap-4">
        <h4 className="text-[14px] font-medium text-slate-300 tracking-[-0.01em] truncate">
          {item.companyName}
        </h4>
        {displayAmount && (
          <span className="text-[14px] font-medium tabular-nums shrink-0 text-emerald-400">
            {displayAmount}
          </span>
        )}
      </div>

      {/* Row 2: Round Badge · Investor | Date */}
      <div className="flex items-center justify-between gap-3 mt-2">
        <div className="flex items-center gap-2 min-w-0">
          {item.roundType && (
            <span className={`text-[9px] font-medium uppercase px-1.5 py-0.5 rounded border shrink-0 ${roundStyle}`}>
              {item.roundType}
            </span>
          )}
          {item.leadInvestor && (
            <span className="text-[11px] text-slate-500 truncate">{item.leadInvestor}</span>
          )}
        </div>
        <span className="text-[10px] text-slate-600 font-mono font-light shrink-0">
          {formatAddedDate(item.createdAt)}
        </span>
      </div>

      {/* Next Step */}
      {item.nextStep && (
        <div className="text-[10px] text-amber-400/60 mt-2 truncate">
          {item.nextStep}
        </div>
      )}
    </div>
  );
}

// Parse ISO date string to local Date, avoiding timezone shift bug
// "YYYY-MM-DD" parsed by new Date() is treated as UTC midnight,
// which can shift to previous day in local timezones behind UTC
function parseLocalDate(dateString: string): Date {
  const parts = dateString.split('T')[0].split('-');
  if (parts.length >= 3) {
    const year = parseInt(parts[0], 10);
    const month = parseInt(parts[1], 10) - 1; // JS months are 0-indexed
    const day = parseInt(parts[2], 10);
    return new Date(year, month, day);
  }
  return new Date(dateString);
}


function formatAddedDate(dateString: string): string {
  try {
    const date = parseLocalDate(dateString);
    return date.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
  } catch {
    return '';
  }
}

function getRoundStyle(roundType: string | undefined | null): string {
  if (!roundType) return 'bg-slate-700/50 text-slate-400 border-slate-600/50';

  const normalized = roundType.toLowerCase().replace(/\s+/g, '_');

  const styles: Record<string, string> = {
    seed: 'bg-purple-500/20 text-purple-400 border-purple-500/30',
    'pre-seed': 'bg-pink-500/20 text-pink-400 border-pink-500/30',
    'pre_seed': 'bg-pink-500/20 text-pink-400 border-pink-500/30',
    series_a: 'bg-cyan-500/20 text-cyan-400 border-cyan-500/30',
    'series a': 'bg-cyan-500/20 text-cyan-400 border-cyan-500/30',
    series_b: 'bg-blue-500/20 text-blue-400 border-blue-500/30',
    'series b': 'bg-blue-500/20 text-blue-400 border-blue-500/30',
    series_c: 'bg-indigo-500/20 text-indigo-400 border-indigo-500/30',
    'series c': 'bg-indigo-500/20 text-indigo-400 border-indigo-500/30',
    series_d: 'bg-violet-500/20 text-violet-400 border-violet-500/30',
    'series d': 'bg-violet-500/20 text-violet-400 border-violet-500/30',
    'series d+': 'bg-violet-500/20 text-violet-400 border-violet-500/30',
    series_e: 'bg-fuchsia-500/20 text-fuchsia-400 border-fuchsia-500/30',
    growth: 'bg-amber-500/20 text-amber-400 border-amber-500/30',
  };

  return styles[normalized] || 'bg-slate-700/50 text-slate-400 border-slate-600/50';
}

function formatAmount(raw: string | undefined | null): string {
  if (!raw) return '';
  const trimmed = raw.trim();
  if (!trimmed || /^(N\/A|Unknown|n\/a|unknown)$/i.test(trimmed)) return '';

  // Match optional $, number, optional unit (word or abbreviation), optional modifier like "+"
  const match = trimmed.match(/^\$?([\d,.]+)\s*(billion|million|thousand|B|M|K)?\s*(\+?)$/i);
  if (!match) return trimmed; // Can't parse, return as-is

  const numStr = match[1].replace(/,/g, '');
  const num = parseFloat(numStr);
  if (isNaN(num)) return trimmed;

  const unitRaw = (match[2] || '').toLowerCase();
  const modifier = match[3] || '';

  let suffix = '';
  if (unitRaw === 'b' || unitRaw === 'billion') suffix = 'B';
  else if (unitRaw === 'm' || unitRaw === 'million') suffix = 'M';
  else if (unitRaw === 'k' || unitRaw === 'thousand') suffix = 'K';

  // No unit: raw dollar amount — convert large numbers to shorthand
  if (!suffix) {
    if (num >= 1_000_000_000) return `$${+(num / 1_000_000_000).toFixed(1)}B${modifier}`;
    if (num >= 1_000_000) return `$${+(num / 1_000_000).toFixed(1)}M${modifier}`;
    if (num >= 1_000) return `$${+(num / 1_000).toFixed(0)}K${modifier}`;
    return `$${num}${modifier}`;
  }

  // Format with clean number (strip trailing .0)
  const clean = num % 1 === 0 ? num.toString() : num.toString();
  return `$${clean}${suffix}${modifier}`;
}


export default TrackerCard;
