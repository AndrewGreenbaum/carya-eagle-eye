/**
 * TrackerColumnModal - Modal for creating/editing tracker columns
 */

import { useState, useEffect } from 'react';
import { X } from 'lucide-react';
import type { TrackerColumn } from '../types';

interface TrackerColumnModalProps {
  column: TrackerColumn | null;
  onClose: () => void;
  onSave: (data: { displayName: string; color: string }) => void;
}

export function TrackerColumnModal({
  column,
  onClose,
  onSave,
}: TrackerColumnModalProps) {
  const [displayName, setDisplayName] = useState(column?.displayName || '');
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (column) {
      setDisplayName(column.displayName);
    }
  }, [column]);

  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onClose();
    };
    document.addEventListener('keydown', handleKeyDown);
    return () => document.removeEventListener('keydown', handleKeyDown);
  }, [onClose]);

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    const trimmedName = displayName.trim();
    if (!trimmedName) {
      setError('Column name is required');
      return;
    }
    if (trimmedName.length > 100) {
      setError('Column name must be 100 characters or less');
      return;
    }
    onSave({ displayName: trimmedName, color: column?.color || 'slate' });
  };

  const isEditing = !!column;

  return (
    <>
      {/* Backdrop */}
      <div
        className="fixed inset-0 bg-black/75 backdrop-blur-sm z-40"
        onClick={onClose}
        aria-hidden="true"
      />

      {/* Modal */}
      <div
        role="dialog"
        aria-modal="true"
        className="fixed top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 z-50 w-full max-w-[380px] mx-4 bg-[#0a0a0f] border border-zinc-800/25 rounded-2xl shadow-2xl"
      >
        <button
          onClick={onClose}
          className="absolute top-6 right-7 text-zinc-700 hover:text-zinc-400 transition-colors p-2"
          aria-label="Close"
        >
          <X className="w-4 h-4" />
        </button>

        <form onSubmit={handleSubmit} className="px-10 sm:px-12 py-10">
          <h2 className="text-[15px] font-semibold text-zinc-200 tracking-[-0.02em] mb-10">
            {isEditing ? 'Column' : 'Add Column'}
          </h2>

          <div>
            <label
              htmlFor="displayName"
              className="block text-[10px] uppercase tracking-[0.1em] text-zinc-600 font-medium mb-3"
            >
              Name
            </label>
            <input
              type="text"
              id="displayName"
              value={displayName}
              onChange={(e) => {
                setDisplayName(e.target.value);
                setError(null);
              }}
              placeholder="e.g., Due Diligence"
              className="w-full bg-transparent border-b border-zinc-800 py-2.5 text-sm text-zinc-50 placeholder:text-zinc-800 placeholder:font-light focus:outline-none focus:border-zinc-600 transition-colors"
              autoFocus
            />
            {error && <p className="mt-2 text-xs text-red-400">{error}</p>}
          </div>

          <div className="flex items-center justify-between mt-12">
            {isEditing && (
              <button
                type="button"
                onClick={onClose}
                className="text-xs text-zinc-700 hover:text-red-400 transition-colors py-2"
              >
                Delete column
              </button>
            )}
            <button
              type="submit"
              className={`text-xs font-medium text-emerald-500 hover:text-emerald-400 transition-colors py-2 ${!isEditing ? 'ml-auto' : ''}`}
            >
              Save
            </button>
          </div>
        </form>
      </div>
    </>
  );
}

export default TrackerColumnModal;
