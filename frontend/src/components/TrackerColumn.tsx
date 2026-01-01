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
import { ChevronLeft, ChevronRight, Pencil, Trash2 } from 'lucide-react';

import { TrackerCard } from './TrackerCard';
import type { TrackerItem, TrackerColumn as TrackerColumnType } from '../types';
import { TRACKER_COLOR_CLASSES } from '../types';

interface TrackerColumnProps {
  column: TrackerColumnType;
  items: TrackerItem[];
  count: number;
  isFirst: boolean;
  isLast: boolean;
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
  onEditItem,
  onEditColumn,
  onMoveLeft,
  onMoveRight,
  onDeleteColumn,
}: TrackerColumnProps) {
  const { setNodeRef, isOver } = useDroppable({
    id: column.slug,
  });

  const colorClasses = TRACKER_COLOR_CLASSES[column.color] || TRACKER_COLOR_CLASSES.slate;

  return (
    <div className="flex flex-col w-64 sm:w-72 shrink-0 group">
      {/* Column Header */}
      <div className="flex items-center gap-2 mb-3 px-1">
        <div className={`w-2 h-2 rounded-full ${colorClasses.dot}`} />
        <h3 className="text-sm font-medium text-white truncate">{column.displayName}</h3>
        <span className="px-2 py-0.5 text-xs bg-slate-800 text-slate-400 rounded-full shrink-0">
          {count}
        </span>
        {/* Column Actions (always visible on mobile, hover on desktop) */}
        <div className="ml-auto flex items-center gap-0.5 sm:opacity-0 sm:group-hover:opacity-100 transition-opacity">
          {!isFirst && (
            <button
              onClick={() => onMoveLeft(column)}
              className="p-2 sm:p-1 text-slate-500 hover:text-white hover:bg-slate-700 rounded transition-colors"
              title="Move left"
            >
              <ChevronLeft className="w-4 h-4 sm:w-3.5 sm:h-3.5" />
            </button>
          )}
          {!isLast && (
            <button
              onClick={() => onMoveRight(column)}
              className="p-2 sm:p-1 text-slate-500 hover:text-white hover:bg-slate-700 rounded transition-colors"
              title="Move right"
            >
              <ChevronRight className="w-4 h-4 sm:w-3.5 sm:h-3.5" />
            </button>
          )}
          <button
            onClick={() => onEditColumn(column)}
            className="p-2 sm:p-1 text-slate-500 hover:text-white hover:bg-slate-700 rounded transition-colors"
            title="Edit column"
          >
            <Pencil className="w-4 h-4 sm:w-3.5 sm:h-3.5" />
          </button>
          <button
            onClick={() => onDeleteColumn(column)}
            className="p-2 sm:p-1 text-slate-500 hover:text-red-400 hover:bg-slate-700 rounded transition-colors"
            title="Delete column"
          >
            <Trash2 className="w-4 h-4 sm:w-3.5 sm:h-3.5" />
          </button>
        </div>
      </div>

      {/* Column Body */}
      <div
        ref={setNodeRef}
        role="region"
        aria-label={`${column.displayName} column, ${count} items`}
        className={`flex-1 rounded-lg p-2 transition-colors min-h-[200px] ${
          isOver ? 'bg-slate-800/50 ring-2 ring-emerald-500/30' : 'bg-slate-900/30'
        }`}
      >
        <SortableContext
          items={items.map((i) => i.id)}
          strategy={verticalListSortingStrategy}
        >
          <div className="space-y-2">
            {items.map((item) => (
              <TrackerCard
                key={item.id}
                item={item}
                onClick={() => onEditItem(item)}
              />
            ))}
          </div>
        </SortableContext>

        {/* Empty State */}
        {items.length === 0 && (
          <div className="flex flex-col items-center justify-center h-24 text-center px-4">
            <span className="text-slate-500 text-sm">No items</span>
            <span className="text-slate-600 text-xs mt-1">
              Drag items here
            </span>
          </div>
        )}
      </div>
    </div>
  );
}

export default TrackerColumn;
