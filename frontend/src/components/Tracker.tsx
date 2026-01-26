/**
 * Tracker - Kanban-style CRM for managing deal pipeline
 *
 * Features:
 * - Configurable columns (add, rename, reorder, delete)
 * - Drag-and-drop between columns
 * - Manual company entry
 * - Links to deals from Dashboard
 */

import { useState, useEffect, useCallback } from 'react';
import {
  DndContext,
  DragOverlay,
  rectIntersection,
  KeyboardSensor,
  PointerSensor,
  useSensor,
  useSensors,
  type DragStartEvent,
  type DragEndEvent,
  type DragOverEvent,
} from '@dnd-kit/core';
import { sortableKeyboardCoordinates } from '@dnd-kit/sortable';
import { Plus, RefreshCw, Upload } from 'lucide-react';

import { TrackerColumn } from './TrackerColumn';
import { TrackerCard } from './TrackerCard';
import { TrackerModal } from './TrackerModal';
import { TrackerColumnModal } from './TrackerColumnModal';
import { TrackerBulkAddModal } from './TrackerBulkAddModal';
import type { TrackerItem, TrackerStatus, TrackerStats, TrackerColumn as TrackerColumnType } from '../types';
import {
  fetchTrackerItems,
  fetchTrackerColumns,
  createTrackerItem,
  bulkCreateTrackerItems,
  updateTrackerItem,
  moveTrackerItem,
  deleteTrackerItem,
  createTrackerColumn,
  updateTrackerColumn,
  moveTrackerColumn,
  deleteTrackerColumn,
} from '../api/deals';

export function Tracker() {
  const [items, setItems] = useState<TrackerItem[]>([]);
  const [columns, setColumns] = useState<TrackerColumnType[]>([]);
  const [stats, setStats] = useState<TrackerStats | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [activeItem, setActiveItem] = useState<TrackerItem | null>(null);
  const [modalItem, setModalItem] = useState<TrackerItem | null>(null);
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [isCreating, setIsCreating] = useState(false);
  const [overColumnId, setOverColumnId] = useState<string | null>(null);
  // Column modal state
  const [isColumnModalOpen, setIsColumnModalOpen] = useState(false);
  const [editingColumn, setEditingColumn] = useState<TrackerColumnType | null>(null);
  // Bulk add modal state
  const [isBulkAddModalOpen, setIsBulkAddModalOpen] = useState(false);

  const sensors = useSensors(
    useSensor(PointerSensor, {
      activationConstraint: {
        distance: 8,
      },
    }),
    useSensor(KeyboardSensor, {
      coordinateGetter: sortableKeyboardCoordinates,
    })
  );

  const loadColumns = useCallback(async () => {
    try {
      const response = await fetchTrackerColumns();
      setColumns(response.columns.sort((a, b) => a.position - b.position));
    } catch (err) {
      console.error('Failed to load columns:', err);
      // Don't set error - columns are secondary, items are primary
    }
  }, []);

  const loadItems = useCallback(async () => {
    try {
      setError(null);
      const response = await fetchTrackerItems();
      setItems(response.items);
      setStats(response.stats);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load tracker items');
    }
  }, []);

  const loadAll = useCallback(async () => {
    await Promise.all([loadColumns(), loadItems()]);
  }, [loadColumns, loadItems]);

  useEffect(() => {
    loadAll();
  }, [loadAll]);

  const getItemsByStatus = (status: TrackerStatus): TrackerItem[] => {
    return items
      .filter((item) => item.status === status)
      .sort((a, b) => a.position - b.position);
  };

  const handleDragStart = (event: DragStartEvent) => {
    const { active } = event;
    const item = items.find((i) => i.id === active.id);
    if (item) {
      setActiveItem(item);
    }
  };

  const handleDragOver = (event: DragOverEvent) => {
    const { over } = event;
    if (!over) {
      setOverColumnId(null);
      return;
    }

    const overId = over.id;
    const columnSlugs = columns.map((c) => c.slug);

    // Check if hovering over a column background directly
    if (columnSlugs.includes(String(overId))) {
      setOverColumnId(String(overId));
      return;
    }

    // Check droppable/sortable data for columnSlug (handles card-drop-* IDs too)
    // This is the most reliable method for cross-column drags
    const columnSlug = over.data?.current?.columnSlug;
    if (columnSlug && columnSlugs.includes(columnSlug)) {
      setOverColumnId(columnSlug);
      return;
    }

    // Fallback: sortable container ID
    const containerId = over.data?.current?.sortable?.containerId;
    if (containerId && columnSlugs.includes(containerId)) {
      setOverColumnId(containerId);
      return;
    }

    // Last fallback: look up item in state
    // Handle both numeric IDs and "card-drop-123" format
    const itemId = String(overId).startsWith('card-drop-')
      ? Number(String(overId).replace('card-drop-', ''))
      : Number(overId);

    const overItem = items.find((i) => i.id === itemId);
    if (overItem) {
      setOverColumnId(overItem.status);
    } else {
      setOverColumnId(null);
    }
  };

  const handleDragCancel = () => {
    setActiveItem(null);
    setOverColumnId(null);
  };

  const handleDragEnd = async (event: DragEndEvent) => {
    const { active, over } = event;
    setActiveItem(null);
    setOverColumnId(null);

    if (!over) return;

    const draggedItem = items.find((i) => i.id === active.id);
    if (!draggedItem) return;

    let targetStatus: TrackerStatus;
    let targetPosition: number;

    const columnSlugs = columns.map((c) => c.slug);

    if (columnSlugs.includes(over.id as string)) {
      // Dropped on a column background - position at end
      targetStatus = over.id as TrackerStatus;
      const columnItems = getItemsByStatus(targetStatus);
      // Position at end (excluding self if same column)
      targetPosition = draggedItem.status === targetStatus
        ? Math.max(0, columnItems.length - 1)
        : columnItems.length;
    } else {
      // Dropped on a card - insert at that card's position
      const overItem = items.find((i) => i.id === over.id);
      if (!overItem) return;

      targetStatus = overItem.status;
      const columnItems = getItemsByStatus(targetStatus);
      const overIndex = columnItems.findIndex((i) => i.id === over.id);

      // Insert at the position of the target card
      targetPosition = overIndex >= 0 ? overIndex : columnItems.length;

      // Adjust for same-column drag from before the target
      if (draggedItem.status === targetStatus) {
        const draggedIndex = columnItems.findIndex((i) => i.id === draggedItem.id);
        if (draggedIndex >= 0 && draggedIndex < targetPosition) {
          targetPosition = targetPosition - 1;
        }
      }
    }

    // Clamp to valid range
    targetPosition = Math.max(0, targetPosition);

    // Skip if no actual change
    if (draggedItem.status === targetStatus && draggedItem.position === targetPosition) {
      return;
    }

    // Store previous state for rollback on error
    const previousItems = [...items];

    // Optimistic update: immediately update UI
    setItems((prev) => {
      // Remove dragged item from current position
      const withoutDragged = prev.filter((item) => item.id !== draggedItem.id);

      // Get items in the target column (excluding dragged item)
      const targetColumnItems = withoutDragged
        .filter((i) => i.status === targetStatus)
        .sort((a, b) => a.position - b.position);

      // Build new target column with correct positions
      const newTargetColumnItems: TrackerItem[] = [];
      let inserted = false;

      for (let i = 0; i < targetColumnItems.length; i++) {
        if (i === targetPosition && !inserted) {
          newTargetColumnItems.push({
            ...draggedItem,
            status: targetStatus,
            position: newTargetColumnItems.length,
          });
          inserted = true;
        }
        newTargetColumnItems.push({
          ...targetColumnItems[i],
          position: newTargetColumnItems.length,
        });
      }

      // If not inserted yet (position is at end), append
      if (!inserted) {
        newTargetColumnItems.push({
          ...draggedItem,
          status: targetStatus,
          position: newTargetColumnItems.length,
        });
      }

      // Combine: other columns unchanged + renumbered target column
      const otherColumnItems = withoutDragged.filter((i) => i.status !== targetStatus);
      return [...otherColumnItems, ...newTargetColumnItems];
    });

    // Sync with backend
    try {
      await moveTrackerItem(draggedItem.id, targetStatus, targetPosition);
    } catch (err) {
      console.error('Failed to move item:', err);
      // Rollback on error
      setItems(previousItems);
      setError('Failed to move item. Please try again.');
      setTimeout(() => setError(null), 3000);
    }
  };

  const handleCreateNew = () => {
    setModalItem(null);
    setIsCreating(true);
    setIsModalOpen(true);
  };

  const handleEditItem = (item: TrackerItem) => {
    setModalItem(item);
    setIsCreating(false);
    setIsModalOpen(true);
  };

  const handleSaveItem = async (data: Partial<TrackerItem>) => {
    try {
      if (isCreating) {
        await createTrackerItem({
          companyName: data.companyName!,
          roundType: data.roundType,
          amount: data.amount,
          leadInvestor: data.leadInvestor,
          website: data.website,
          notes: data.notes,
          status: data.status || 'watching',
        });
      } else if (modalItem) {
        // Update fields (backend ignores status in PUT, must use move endpoint)
        await updateTrackerItem(modalItem.id, {
          companyName: data.companyName,
          roundType: data.roundType,
          amount: data.amount,
          leadInvestor: data.leadInvestor,
          website: data.website,
          notes: data.notes,
          lastContactDate: data.lastContactDate,
          nextStep: data.nextStep,
        });
        // If status changed, move the item to the new column
        if (data.status && data.status !== modalItem.status) {
          const targetItems = items.filter((i) => i.status === data.status);
          await moveTrackerItem(modalItem.id, data.status, targetItems.length);
        }
      }
      setIsModalOpen(false);
      loadItems();
    } catch (err) {
      console.error('Failed to save item:', err);
      setError(err instanceof Error ? err.message : 'Failed to save item');
    }
  };

  const handleDeleteItem = async (itemId: number) => {
    try {
      await deleteTrackerItem(itemId);
      setIsModalOpen(false);
      loadItems();
    } catch (err) {
      console.error('Failed to delete item:', err);
      setError(err instanceof Error ? err.message : 'Failed to delete item');
    }
  };

  // Column management handlers
  const handleAddColumn = () => {
    setEditingColumn(null);
    setIsColumnModalOpen(true);
  };

  const handleEditColumn = (column: TrackerColumnType) => {
    setEditingColumn(column);
    setIsColumnModalOpen(true);
  };

  const handleSaveColumn = async (data: { displayName: string; color: string }) => {
    try {
      if (editingColumn) {
        await updateTrackerColumn(editingColumn.id, data);
      } else {
        await createTrackerColumn(data);
      }
      setIsColumnModalOpen(false);
      setEditingColumn(null);
      loadColumns();
    } catch (err) {
      console.error('Failed to save column:', err);
      setError(err instanceof Error ? err.message : 'Failed to save column');
    }
  };

  const handleMoveColumnLeft = async (column: TrackerColumnType) => {
    if (column.position <= 0) return;
    try {
      await moveTrackerColumn(column.id, column.position - 1);
      loadColumns();
    } catch (err) {
      console.error('Failed to move column:', err);
      setError(err instanceof Error ? err.message : 'Failed to move column');
    }
  };

  const handleMoveColumnRight = async (column: TrackerColumnType) => {
    if (column.position >= columns.length - 1) return;
    try {
      await moveTrackerColumn(column.id, column.position + 1);
      loadColumns();
    } catch (err) {
      console.error('Failed to move column:', err);
      setError(err instanceof Error ? err.message : 'Failed to move column');
    }
  };

  const handleDeleteColumn = async (column: TrackerColumnType) => {
    if (columns.length <= 1) {
      setError('Cannot delete the last column');
      return;
    }
    const itemCount = items.filter((i) => i.status === column.slug).length;
    const confirmed = window.confirm(
      itemCount > 0
        ? `Delete "${column.displayName}"? ${itemCount} item(s) will move to the first column.`
        : `Delete "${column.displayName}"?`
    );
    if (!confirmed) return;
    try {
      await deleteTrackerColumn(column.id);
      loadAll();
    } catch (err) {
      console.error('Failed to delete column:', err);
      setError(err instanceof Error ? err.message : 'Failed to delete column');
    }
  };

  // Bulk add handler
  const handleBulkAdd = async (companyNames: string[]) => {
    const result = await bulkCreateTrackerItems(companyNames, 'watching');
    // Optimistically add items to state without full reload
    setItems((prev) => [...prev, ...result.created]);
    // Update stats
    if (stats) {
      setStats({
        ...stats,
        total: stats.total + result.count,
        watching: (stats.watching || 0) + result.count,
      });
    }
  };

  return (
    <div className="flex flex-col flex-1 min-h-0">
      {/* Header */}
      <div className="flex flex-col sm:flex-row items-start sm:items-center justify-between px-4 sm:px-6 py-4 border-b border-slate-800 gap-3 sm:gap-0">
        <div>
          <h1 className="text-lg sm:text-xl font-bold text-white">Deal Tracker</h1>
          <p className="text-sm text-slate-400">
            {stats?.total || 0} companies in pipeline
          </p>
        </div>
        <div className="flex items-center gap-2 sm:gap-3 w-full sm:w-auto">
          <button
            onClick={loadAll}
            className="p-2.5 sm:p-2 text-slate-400 hover:text-white hover:bg-slate-800 rounded transition-colors min-w-[44px] min-h-[44px] sm:min-w-0 sm:min-h-0 flex items-center justify-center"
            title="Refresh"
          >
            <RefreshCw className="w-4 h-4" />
          </button>
          <button
            onClick={() => setIsBulkAddModalOpen(true)}
            className="flex items-center justify-center gap-2 px-3 sm:px-4 py-2.5 sm:py-2 bg-slate-700 hover:bg-slate-600 text-white rounded font-medium transition-colors flex-1 sm:flex-none min-h-[44px] sm:min-h-0"
          >
            <Upload className="w-4 h-4" />
            <span className="hidden sm:inline">Bulk Add</span>
            <span className="sm:hidden">Bulk</span>
          </button>
          <button
            onClick={handleCreateNew}
            className="flex items-center justify-center gap-2 px-3 sm:px-4 py-2.5 sm:py-2 bg-emerald-600 hover:bg-emerald-500 text-white rounded font-medium transition-colors flex-1 sm:flex-none min-h-[44px] sm:min-h-0"
          >
            <Plus className="w-4 h-4" />
            <span className="hidden sm:inline">Add Company</span>
            <span className="sm:hidden">Add</span>
          </button>
        </div>
      </div>

      {/* Error Banner */}
      {error && (
        <div className="mx-6 mt-4 p-3 bg-red-500/10 border border-red-500/30 rounded text-red-400 text-sm">
          {error}
          <button
            onClick={() => setError(null)}
            className="ml-2 underline hover:no-underline"
          >
            Dismiss
          </button>
        </div>
      )}

      {/* Kanban Board */}
      <div className="flex-1 min-h-0 overflow-auto p-4 sm:p-6">
        <DndContext
          sensors={sensors}
          collisionDetection={rectIntersection}
          onDragStart={handleDragStart}
          onDragOver={handleDragOver}
          onDragEnd={handleDragEnd}
          onDragCancel={handleDragCancel}
        >
          <div className="flex gap-3 sm:gap-4 min-h-full pb-4" style={{ minWidth: 'max-content' }}>
            {columns.map((column) => {
              const columnItems = getItemsByStatus(column.slug);
              return (
                <TrackerColumn
                  key={column.slug}
                  column={column}
                  items={columnItems}
                  count={columnItems.length}
                  isFirst={column.position === 0}
                  isLast={column.position === columns.length - 1}
                  isDropTarget={overColumnId === column.slug && activeItem !== null}
                  onEditItem={handleEditItem}
                  onEditColumn={handleEditColumn}
                  onMoveLeft={handleMoveColumnLeft}
                  onMoveRight={handleMoveColumnRight}
                  onDeleteColumn={handleDeleteColumn}
                />
              );
            })}
            {/* Add Column Button */}
            <button
              onClick={handleAddColumn}
              className="flex flex-col items-center justify-center w-64 sm:w-72 shrink-0 h-32 border-2 border-dashed border-slate-700 rounded-lg text-slate-500 hover:border-slate-600 hover:text-slate-400 transition-colors min-h-[44px]"
            >
              <Plus className="w-6 h-6 mb-1" />
              <span className="text-sm">Add Column</span>
            </button>
          </div>

          <DragOverlay>
            {activeItem ? (
              <TrackerCard item={activeItem} columnSlug={activeItem.status} isDragging />
            ) : null}
          </DragOverlay>
        </DndContext>
      </div>

      {/* Item Modal */}
      {isModalOpen && (
        <TrackerModal
          item={modalItem}
          isCreating={isCreating}
          onClose={() => setIsModalOpen(false)}
          onSave={handleSaveItem}
          onDelete={modalItem ? () => handleDeleteItem(modalItem.id) : undefined}
        />
      )}

      {/* Column Modal */}
      {isColumnModalOpen && (
        <TrackerColumnModal
          column={editingColumn}
          onClose={() => {
            setIsColumnModalOpen(false);
            setEditingColumn(null);
          }}
          onSave={handleSaveColumn}
        />
      )}

      {/* Bulk Add Modal */}
      {isBulkAddModalOpen && (
        <TrackerBulkAddModal
          onClose={() => setIsBulkAddModalOpen(false)}
          onSubmit={handleBulkAdd}
        />
      )}
    </div>
  );
}

export default Tracker;
