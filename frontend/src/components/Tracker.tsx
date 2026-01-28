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
  closestCorners,
  KeyboardSensor,
  PointerSensor,
  useSensor,
  useSensors,
  type DragStartEvent,
  type DragEndEvent,
  type DragOverEvent,
} from '@dnd-kit/core';
import { sortableKeyboardCoordinates } from '@dnd-kit/sortable';
import { Plus } from 'lucide-react';

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
  // Column modal state
  const [isColumnModalOpen, setIsColumnModalOpen] = useState(false);
  const [editingColumn, setEditingColumn] = useState<TrackerColumnType | null>(null);
  // Bulk add modal state
  const [isBulkAddModalOpen, setIsBulkAddModalOpen] = useState(false);

  // Keyboard navigation state
  const [selectedColIdx, setSelectedColIdx] = useState<number>(-1);
  const [selectedCardIdx, setSelectedCardIdx] = useState<number>(-1);

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

  // Keyboard navigation handler
  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      // Skip if typing in an input/textarea
      const tag = (e.target as HTMLElement).tagName;
      if (tag === 'INPUT' || tag === 'TEXTAREA') return;
      // Skip if a modal is open
      if (document.querySelector('[aria-modal="true"]')) return;
      // Skip if meta/ctrl/alt held (except shift combos)
      if (e.metaKey || e.ctrlKey || e.altKey) return;

      const key = e.key;
      const hasShift = e.shiftKey;

      // Helper: get items for a column index
      const getColItems = (colIdx: number) => {
        if (colIdx < 0 || colIdx >= columns.length) return [];
        return items
          .filter((i) => i.status === columns[colIdx].slug)
          .sort((a, b) => a.position - b.position);
      };

      // N: Create new card
      if (key === 'n' || key === 'N') {
        if (!hasShift) {
          e.preventDefault();
          setModalItem(null);
          setIsCreating(true);
          setIsModalOpen(true);
        }
        return;
      }

      // Escape: Clear selection
      if (key === 'Escape') {
        setSelectedColIdx(-1);
        setSelectedCardIdx(-1);
        return;
      }

      // Enter: Open selected card's modal
      if (key === 'Enter') {
        if (selectedColIdx >= 0 && selectedCardIdx >= 0) {
          const colItems = getColItems(selectedColIdx);
          if (selectedCardIdx < colItems.length) {
            e.preventDefault();
            setModalItem(colItems[selectedCardIdx]);
            setIsCreating(false);
            setIsModalOpen(true);
          }
        }
        return;
      }

      // Shift+H/L or Shift+ArrowLeft/Right: Move card between columns
      if (hasShift && (key === 'H' || key === 'ArrowLeft' || key === 'L' || key === 'ArrowRight')) {
        if (selectedColIdx < 0 || selectedCardIdx < 0) return;
        const colItems = getColItems(selectedColIdx);
        if (selectedCardIdx >= colItems.length) return;

        const direction = (key === 'H' || key === 'ArrowLeft') ? -1 : 1;
        const targetColIdx = selectedColIdx + direction;
        if (targetColIdx < 0 || targetColIdx >= columns.length) return;

        e.preventDefault();
        const draggedItem = colItems[selectedCardIdx];
        const targetStatus = columns[targetColIdx].slug as TrackerStatus;
        const targetItems = items
          .filter((i) => i.status === targetStatus)
          .sort((a, b) => a.position - b.position);
        const targetPosition = targetItems.length;

        // Store previous state for rollback
        const previousItems = [...items];

        // Optimistic update
        setItems((prev) => {
          const updated = prev.map((item) =>
            item.id === draggedItem.id
              ? { ...item, status: targetStatus, position: targetPosition }
              : item
          );
          const targetColItems = updated
            .filter((i) => i.status === targetStatus)
            .sort((a, b) => a.position - b.position);
          return updated.map((item) => {
            if (item.status === targetStatus) {
              const idx = targetColItems.findIndex((c) => c.id === item.id);
              return { ...item, position: idx };
            }
            return item;
          });
        });

        // Update selection to follow the card
        setSelectedColIdx(targetColIdx);
        setSelectedCardIdx(targetItems.length); // It's now last in new column

        // Sync with backend
        moveTrackerItem(draggedItem.id, targetStatus, targetPosition).catch(() => {
          setItems(previousItems);
        });
        return;
      }

      // J/ArrowDown: Next card in column
      if (key === 'j' || key === 'ArrowDown') {
        e.preventDefault();
        if (selectedColIdx < 0) {
          // First keypress: select first card in first column
          if (columns.length > 0) {
            setSelectedColIdx(0);
            setSelectedCardIdx(0);
          }
        } else {
          const colItems = getColItems(selectedColIdx);
          if (selectedCardIdx < colItems.length - 1) {
            setSelectedCardIdx(selectedCardIdx + 1);
          }
        }
        return;
      }

      // K/ArrowUp: Previous card in column
      if (key === 'k' || key === 'ArrowUp') {
        e.preventDefault();
        if (selectedColIdx < 0) {
          if (columns.length > 0) {
            setSelectedColIdx(0);
            setSelectedCardIdx(0);
          }
        } else {
          if (selectedCardIdx > 0) {
            setSelectedCardIdx(selectedCardIdx - 1);
          }
        }
        return;
      }

      // H/ArrowLeft: Previous column
      if (key === 'h' || key === 'ArrowLeft') {
        e.preventDefault();
        if (selectedColIdx < 0) {
          if (columns.length > 0) {
            setSelectedColIdx(0);
            setSelectedCardIdx(0);
          }
        } else if (selectedColIdx > 0) {
          setSelectedColIdx(selectedColIdx - 1);
          setSelectedCardIdx(0);
        }
        return;
      }

      // L/ArrowRight: Next column
      if (key === 'l' || key === 'ArrowRight') {
        e.preventDefault();
        if (selectedColIdx < 0) {
          if (columns.length > 0) {
            setSelectedColIdx(0);
            setSelectedCardIdx(0);
          }
        } else if (selectedColIdx < columns.length - 1) {
          setSelectedColIdx(selectedColIdx + 1);
          setSelectedCardIdx(0);
        }
        return;
      }
    };

    window.addEventListener('keydown', handleKeyDown);
    return () => window.removeEventListener('keydown', handleKeyDown);
  }, [columns, items, selectedColIdx, selectedCardIdx, isModalOpen]);

  // Scroll selected card into view
  useEffect(() => {
    if (selectedColIdx < 0 || selectedCardIdx < 0) return;
    if (!columns[selectedColIdx]) return;

    const colItems = items
      .filter((i) => i.status === columns[selectedColIdx].slug)
      .sort((a, b) => a.position - b.position);

    const selectedItem = colItems[selectedCardIdx];
    if (!selectedItem) return;

    // Find the card element by data attribute
    const cardEl = document.querySelector(`[data-tracker-item-id="${selectedItem.id}"]`);
    if (cardEl) {
      cardEl.scrollIntoView({ behavior: 'smooth', block: 'nearest', inline: 'nearest' });
    }
  }, [selectedColIdx, selectedCardIdx, columns, items]);

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

  const handleDragOver = (_event: DragOverEvent) => {
    // Don't update state during drag - causes flickering and race conditions
    // The visual feedback is handled by dnd-kit's DragOverlay
    // State is updated only in handleDragEnd after drop completes
  };

  const handleDragEnd = async (event: DragEndEvent) => {
    const { active, over } = event;
    setActiveItem(null);

    if (!over) return;

    const draggedItem = items.find((i) => i.id === active.id);
    if (!draggedItem) return;

    let targetStatus: TrackerStatus;
    let targetPosition: number;

    const columnSlugs = columns.map((c) => c.slug);
    if (columnSlugs.includes(over.id as string)) {
      // Dropped on a column
      targetStatus = over.id as TrackerStatus;
      const columnItems = getItemsByStatus(targetStatus);
      targetPosition = columnItems.length;
    } else {
      // Dropped on another card
      const overItem = items.find((i) => i.id === over.id);
      if (!overItem) return;

      targetStatus = overItem.status;
      const columnItems = getItemsByStatus(targetStatus);
      const overIndex = columnItems.findIndex((i) => i.id === over.id);
      targetPosition = overIndex >= 0 ? overIndex : columnItems.length;
    }

    // Skip if no actual change
    if (draggedItem.status === targetStatus && draggedItem.position === targetPosition) {
      return;
    }

    // Store previous state for rollback
    const previousItems = [...items];

    // Update local state optimistically (instant feedback)
    setItems((prev) => {
      const updated = prev.map((item) =>
        item.id === draggedItem.id
          ? { ...item, status: targetStatus, position: targetPosition }
          : item
      );
      // Re-sort positions within the column
      const columnItems = updated
        .filter((i) => i.status === targetStatus)
        .sort((a, b) => a.position - b.position);

      return updated.map((item) => {
        if (item.status === targetStatus) {
          const idx = columnItems.findIndex((c) => c.id === item.id);
          return { ...item, position: idx };
        }
        return item;
      });
    });

    // Sync with backend (async, don't block UI)
    try {
      await moveTrackerItem(draggedItem.id, targetStatus, targetPosition);
    } catch (err) {
      console.error('Failed to move item:', err);
      // Rollback to previous state instead of full reload
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
    <div className="flex flex-col flex-1 min-h-0 font-sans">
      {/* Header */}
      <div className="flex items-baseline justify-between px-6 sm:px-12 pt-10 sm:pt-12 pb-8 sm:pb-10">
        <h1 className="text-sm font-semibold text-zinc-200 tracking-[-0.02em]">Pipeline</h1>
        <div className="flex items-baseline gap-6">
          <span className="text-xs text-zinc-600">
            {stats?.total || 0} companies
          </span>
          <button
            onClick={() => setIsBulkAddModalOpen(true)}
            className="text-[11px] text-zinc-700 hover:text-zinc-400 transition-colors min-w-[44px] min-h-[44px] sm:min-w-0 sm:min-h-0 flex items-center justify-center"
          >
            Bulk
          </button>
          <button
            onClick={handleCreateNew}
            className="text-[11px] text-zinc-700 hover:text-zinc-400 transition-colors min-w-[44px] min-h-[44px] sm:min-w-0 sm:min-h-0 flex items-center justify-center"
          >
            <Plus className="w-3.5 h-3.5" />
          </button>
        </div>
      </div>

      {/* Error Banner */}
      {error && (
        <div className="mx-6 sm:mx-12 mb-4 text-xs text-red-400">
          {error}
          <button
            onClick={() => setError(null)}
            className="ml-2 text-zinc-600 hover:text-zinc-400 transition-colors"
          >
            Dismiss
          </button>
        </div>
      )}

      {/* Kanban Board */}
      <div className="flex-1 min-h-0 overflow-auto px-6 sm:px-12 pb-12">
        <DndContext
          sensors={sensors}
          collisionDetection={closestCorners}
          onDragStart={handleDragStart}
          onDragOver={handleDragOver}
          onDragEnd={handleDragEnd}
        >
          <div className="flex gap-10 sm:gap-14 min-h-full pb-4" style={{ minWidth: 'max-content' }}>
            {columns.map((column, colIdx) => {
              const columnItems = getItemsByStatus(column.slug);
              return (
                <TrackerColumn
                  key={column.slug}
                  column={column}
                  items={columnItems}
                  count={columnItems.length}
                  isFirst={column.position === 0}
                  isLast={column.position === columns.length - 1}
                  selectedIndex={selectedColIdx === colIdx ? selectedCardIdx : undefined}
                  onEditItem={handleEditItem}
                  onEditColumn={handleEditColumn}
                  onMoveLeft={handleMoveColumnLeft}
                  onMoveRight={handleMoveColumnRight}
                  onDeleteColumn={handleDeleteColumn}
                />
              );
            })}
            {/* Add Column */}
            <button
              onClick={handleAddColumn}
              className="shrink-0 text-lg font-light text-zinc-800 hover:text-zinc-500 transition-colors pt-0.5 px-3 min-w-[44px] min-h-[44px] flex items-start justify-center"
            >
              +
            </button>
          </div>

          <DragOverlay>
            {activeItem ? (
              <TrackerCard item={activeItem} isDragging />
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
