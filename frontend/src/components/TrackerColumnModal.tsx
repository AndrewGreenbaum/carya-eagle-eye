/**
 * TrackerColumnModal - Modal for creating/editing tracker columns
 */

import { useState, useEffect } from 'react';
import { X } from 'lucide-react';
import type { TrackerColumn } from '../types';
import { TRACKER_COLORS, TRACKER_COLOR_CLASSES } from '../types';

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
  const [color, setColor] = useState(column?.color || 'slate');
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (column) {
      setDisplayName(column.displayName);
      setColor(column.color);
    }
  }, [column]);

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
    onSave({ displayName: trimmedName, color });
  };

  const isEditing = !!column;

  return (
    <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
      <div className="bg-slate-900 rounded-lg w-full max-w-md mx-4 shadow-xl">
        {/* Header */}
        <div className="flex items-center justify-between px-6 py-4 border-b border-slate-800">
          <h2 className="text-lg font-semibold text-white">
            {isEditing ? 'Edit Column' : 'Add Column'}
          </h2>
          <button
            onClick={onClose}
            className="text-slate-400 hover:text-white transition-colors"
          >
            <X className="w-5 h-5" />
          </button>
        </div>

        {/* Form */}
        <form onSubmit={handleSubmit} className="p-6 space-y-5">
          {/* Column Name */}
          <div>
            <label
              htmlFor="displayName"
              className="block text-sm font-medium text-slate-300 mb-1.5"
            >
              Column Name
            </label>
            <input
              type="text"
              id="displayName"
              value={displayName}
              onChange={(e) => {
                setDisplayName(e.target.value);
                setError(null);
              }}
              placeholder="e.g., Qualified, Due Diligence"
              className="w-full px-3 py-2 bg-slate-800 border border-slate-700 rounded text-white placeholder-slate-500 focus:outline-none focus:ring-2 focus:ring-emerald-500/50 focus:border-emerald-500"
              autoFocus
            />
            {error && <p className="mt-1.5 text-sm text-red-400">{error}</p>}
          </div>

          {/* Color Picker */}
          <div>
            <label className="block text-sm font-medium text-slate-300 mb-2">
              Column Color
            </label>
            <div className="flex flex-wrap gap-2">
              {TRACKER_COLORS.map((c) => {
                const classes = TRACKER_COLOR_CLASSES[c];
                const isSelected = color === c;
                return (
                  <button
                    key={c}
                    type="button"
                    onClick={() => setColor(c)}
                    className={`w-8 h-8 rounded-full ${classes.dot} transition-all ${
                      isSelected
                        ? 'ring-2 ring-white ring-offset-2 ring-offset-slate-900'
                        : 'hover:scale-110'
                    }`}
                    title={c.charAt(0).toUpperCase() + c.slice(1)}
                    aria-label={`Select ${c} color`}
                  />
                );
              })}
            </div>
          </div>

          {/* Preview */}
          <div>
            <label className="block text-sm font-medium text-slate-300 mb-2">
              Preview
            </label>
            <div className="flex items-center gap-2 px-3 py-2 bg-slate-800 rounded">
              <div
                className={`w-2 h-2 rounded-full ${
                  TRACKER_COLOR_CLASSES[color]?.dot || TRACKER_COLOR_CLASSES.slate.dot
                }`}
              />
              <span className="text-white text-sm">
                {displayName.trim() || 'Column Name'}
              </span>
              <span className="ml-auto px-2 py-0.5 text-xs bg-slate-700 text-slate-400 rounded-full">
                0
              </span>
            </div>
          </div>

          {/* Actions */}
          <div className="flex justify-end gap-3 pt-2">
            <button
              type="button"
              onClick={onClose}
              className="px-4 py-2 text-slate-300 hover:text-white transition-colors"
            >
              Cancel
            </button>
            <button
              type="submit"
              className="px-4 py-2 bg-emerald-600 hover:bg-emerald-500 text-white rounded font-medium transition-colors"
            >
              {isEditing ? 'Save Changes' : 'Add Column'}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}

export default TrackerColumnModal;
