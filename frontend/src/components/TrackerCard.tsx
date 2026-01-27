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

  return (
    <div
      ref={setNodeRef}
      style={style}
      {...attributes}
      {...listeners}
      role="article"
      aria-label={`${item.companyName}${item.leadInvestor ? `, led by ${item.leadInvestor}` : ''}`}
      className={`
        py-5 px-2 -mx-1 border-b border-zinc-800/40 cursor-grab rounded-md select-none
        first:pt-1 hover:bg-zinc-800/15 transition-[background] duration-75
        ${isCurrentlyDragging ? 'opacity-30 cursor-grabbing' : ''}
        ${isSelected ? 'bg-zinc-800/15' : ''}
      `}
      onClick={() => !isCurrentlyDragging && onClick?.()}
    >
      {/* Row 1: Company + Amount */}
      <div className="flex items-baseline justify-between gap-4">
        <h4 className="text-[15px] font-semibold text-zinc-50 tracking-[-0.02em] truncate">
          {item.companyName}
        </h4>
        {displayAmount && (
          <span className="text-[15px] font-medium tabular-nums shrink-0 text-zinc-400">
            {displayAmount}
          </span>
        )}
      </div>

      {/* Row 2: Round · Investor | Date */}
      <div className="flex items-baseline justify-between gap-4 mt-1.5">
        <span className="text-xs text-zinc-600 truncate">
          {item.roundType && <>{item.roundType}</>}
          {item.roundType && item.leadInvestor && <span className="text-zinc-500"> &middot; </span>}
          {item.leadInvestor && <span className="text-zinc-500">{item.leadInvestor}</span>}
        </span>
        <span className="text-[11px] text-zinc-700 font-mono font-light shrink-0">
          {formatAddedDate(item.createdAt)}
        </span>
      </div>

      {/* Next Step */}
      {item.nextStep && (
        <div className="text-[11px] text-amber-400/60 mt-2 truncate">
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
