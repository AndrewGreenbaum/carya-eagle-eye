/**
 * TrackerColumn - A Kanban column for the Tracker
 *
 * Uses @dnd-kit for drag-and-drop functionality
 * Supports column edit, move, and delete actions
 */

import { useDroppable } from '@dnd-kit/core';
import {
  SortableContext,
  verticalListSortingStrategy,
} from '@dnd-kit/sortable';
import { TrackerCard } from './TrackerCard';
import type { TrackerItem, TrackerColumn as TrackerColumnType } from '../types';

interface TrackerColumnProps {
  column: TrackerColumnType;
  items: TrackerItem[];
  count: number;
  isFirst: boolean;
  isLast: boolean;
  selectedIndex?: number;
  onEditItem: (item: TrackerItem) => void;
  onEditColumn: (column: TrackerColumnType) => void;
  onMoveLeft: (column: TrackerColumnType) => void;
  onMoveRight: (column: TrackerColumnType) => void;
  onDeleteColumn: (column: TrackerColumnType) => void;
}

export function TrackerColumn({
  column,
  items,
  count,
  isFirst,
  isLast,
  selectedIndex,
  onEditItem,
  onEditColumn,
  onMoveLeft,
  onMoveRight,
  onDeleteColumn,
}: TrackerColumnProps) {
  const { setNodeRef, isOver } = useDroppable({
    id: column.slug,
  });

  return (
    <div className="flex flex-col w-[280px] sm:w-[300px] shrink-0 group">
      {/* Column Header */}
      <div className="flex items-baseline gap-2 mb-6 sm:mb-8">
        <h3 className="text-[11px] uppercase tracking-[0.12em] text-zinc-600 font-medium truncate">
          {column.displayName}
        </h3>
        <span className="text-[11px] text-zinc-700">{count}</span>
        {/* Column Actions — revealed on hover */}
        <div className="ml-auto flex items-center gap-1 opacity-0 group-hover:opacity-100 transition-opacity">
          {!isFirst && (
            <button
              onClick={() => onMoveLeft(column)}
              className="text-[11px] text-zinc-800 hover:text-zinc-400 transition-colors p-1 min-w-[44px] min-h-[44px] sm:min-w-0 sm:min-h-0 flex items-center justify-center"
              title="Move left"
            >
              &lsaquo;
            </button>
          )}
          {!isLast && (
            <button
              onClick={() => onMoveRight(column)}
              className="text-[11px] text-zinc-800 hover:text-zinc-400 transition-colors p-1 min-w-[44px] min-h-[44px] sm:min-w-0 sm:min-h-0 flex items-center justify-center"
              title="Move right"
            >
              &rsaquo;
            </button>
          )}
          <button
            onClick={() => onEditColumn(column)}
            className="text-[11px] text-zinc-800 hover:text-zinc-400 transition-colors p-1 tracking-[1px] min-w-[44px] min-h-[44px] sm:min-w-0 sm:min-h-0 flex items-center justify-center"
            title="Edit column"
          >
            &middot;&middot;&middot;
          </button>
          <button
            onClick={() => onDeleteColumn(column)}
            className="text-[11px] text-zinc-800 hover:text-red-400 transition-colors p-1 min-w-[44px] min-h-[44px] sm:min-w-0 sm:min-h-0 flex items-center justify-center"
            title="Delete column"
          >
            &times;
          </button>
        </div>
      </div>

      {/* Column Body */}
      <div
        ref={setNodeRef}
        role="region"
        aria-label={`${column.displayName} column, ${count} items`}
        className={`flex-1 transition-colors ${
          isOver ? 'outline outline-1 outline-emerald-500/20 rounded' : ''
        }`}
      >
        <SortableContext
          items={items.map((i) => i.id)}
          strategy={verticalListSortingStrategy}
        >
          <div className="flex flex-col">
            {items.map((item, idx) => (
              <TrackerCard
                key={item.id}
                item={item}
                isSelected={selectedIndex === idx}
                onClick={() => onEditItem(item)}
              />
            ))}
          </div>
        </SortableContext>

        {/* Empty State */}
        {items.length === 0 && (
          <div className="py-10">
            <span className="text-xs text-zinc-800">Drop here</span>
          </div>
        )}
      </div>
    </div>
  );
}

export default TrackerColumn;
