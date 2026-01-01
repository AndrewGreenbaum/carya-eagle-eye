/**
 * TrackerModal - Create/Edit modal for tracker items
 *
 * Features:
 * - Create new companies
 * - Edit existing items
 * - Delete items
 */

import { useState, useEffect, useRef, useCallback } from 'react';
import { X, Trash2, Globe, Calendar, Building2, Target } from 'lucide-react';
import type { TrackerItem, TrackerStatus } from '../types';
import { TRACKER_STATUS_LABELS } from '../types';

interface TrackerModalProps {
  item: TrackerItem | null;
  isCreating: boolean;
  onClose: () => void;
  onSave: (data: Partial<TrackerItem>) => void;
  onDelete?: () => void;
}

export function TrackerModal({
  item,
  isCreating,
  onClose,
  onSave,
  onDelete,
}: TrackerModalProps) {
  const [formData, setFormData] = useState({
    companyName: '',
    roundType: '',
    amount: '',
    leadInvestor: '',
    website: '',
    notes: '',
    status: 'watching' as TrackerStatus,
    lastContactDate: '',
    nextStep: '',
  });
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [showDeleteConfirm, setShowDeleteConfirm] = useState(false);

  const closeButtonRef = useRef<HTMLButtonElement>(null);

  // Populate form when editing
  useEffect(() => {
    if (item) {
      setFormData({
        companyName: item.companyName || '',
        roundType: item.roundType || '',
        amount: item.amount || '',
        leadInvestor: item.leadInvestor || '',
        website: item.website || '',
        notes: item.notes || '',
        status: item.status || 'watching',
        lastContactDate: item.lastContactDate || '',
        nextStep: item.nextStep || '',
      });
    } else {
      setFormData({
        companyName: '',
        roundType: '',
        amount: '',
        leadInvestor: '',
        website: '',
        notes: '',
        status: 'watching',
        lastContactDate: '',
        nextStep: '',
      });
    }
  }, [item]);

  // Focus management
  useEffect(() => {
    closeButtonRef.current?.focus();
    const previouslyFocused = document.activeElement as HTMLElement;
    return () => previouslyFocused?.focus?.();
  }, []);

  // Escape key handler
  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      if (e.key === 'Escape') {
        onClose();
      }
    };
    document.addEventListener('keydown', handleKeyDown);
    return () => document.removeEventListener('keydown', handleKeyDown);
  }, [onClose]);

  const handleBackdropClick = useCallback(
    (e: React.MouseEvent) => {
      if (e.target === e.currentTarget) {
        onClose();
      }
    },
    [onClose]
  );

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!formData.companyName.trim()) return;

    setIsSubmitting(true);
    try {
      await onSave(formData);
    } finally {
      setIsSubmitting(false);
    }
  };

  const handleDelete = async () => {
    if (!onDelete) return;
    setIsSubmitting(true);
    try {
      await onDelete();
    } finally {
      setIsSubmitting(false);
    }
  };

  return (
    <>
      {/* Backdrop */}
      <div
        className="fixed inset-0 bg-black/70 backdrop-blur-sm z-40 animate-fade-in"
        onClick={handleBackdropClick}
        aria-hidden="true"
      />

      {/* Modal */}
      <div
        role="dialog"
        aria-modal="true"
        aria-labelledby="tracker-modal-title"
        className="fixed top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 z-50 w-full max-w-lg mx-4 bg-[#0a0a0c] border border-slate-800 rounded-lg shadow-2xl animate-slide-up"
      >
        {/* Header */}
        <div className="flex items-center justify-between p-4 border-b border-slate-800">
          <h2 id="tracker-modal-title" className="text-lg font-bold text-white">
            {isCreating ? 'Add Company' : 'Edit Company'}
          </h2>
          <button
            ref={closeButtonRef}
            onClick={onClose}
            className="p-2 hover:bg-slate-800 rounded transition-colors text-slate-400 hover:text-white"
            aria-label="Close"
          >
            <X className="w-5 h-5" />
          </button>
        </div>

        {/* Form */}
        <form onSubmit={handleSubmit}>
          <div className="p-4 space-y-4 max-h-[60vh] overflow-y-auto">
            {/* Company Name */}
            <div>
              <label className="block text-xs font-medium text-slate-400 mb-1">
                Company Name <span className="text-red-400">*</span>
              </label>
              <div className="relative">
                <Building2 className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-500" />
                <input
                  type="text"
                  value={formData.companyName}
                  onChange={(e) =>
                    setFormData({ ...formData, companyName: e.target.value })
                  }
                  placeholder="Acme Inc."
                  required
                  className="w-full bg-slate-900 border border-slate-700 rounded pl-10 pr-3 py-2 text-white placeholder:text-slate-500 focus:outline-none focus:border-emerald-500/50"
                />
              </div>
            </div>

            {/* Round Type & Amount */}
            <div className="grid grid-cols-2 gap-3">
              <div>
                <label className="block text-xs font-medium text-slate-400 mb-1">
                  Round Type
                </label>
                <select
                  value={formData.roundType}
                  onChange={(e) =>
                    setFormData({ ...formData, roundType: e.target.value })
                  }
                  className="w-full bg-slate-900 border border-slate-700 rounded px-3 py-2 text-white focus:outline-none focus:border-emerald-500/50"
                >
                  <option value="">Select...</option>
                  <option value="Pre-Seed">Pre-Seed</option>
                  <option value="Seed">Seed</option>
                  <option value="Series A">Series A</option>
                  <option value="Series B">Series B</option>
                  <option value="Series C">Series C</option>
                  <option value="Series D+">Series D+</option>
                  <option value="Growth">Growth</option>
                </select>
              </div>
              <div>
                <label className="block text-xs font-medium text-slate-400 mb-1">
                  Amount
                </label>
                <input
                  type="text"
                  value={formData.amount}
                  onChange={(e) =>
                    setFormData({ ...formData, amount: e.target.value })
                  }
                  placeholder="$10M"
                  className="w-full bg-slate-900 border border-slate-700 rounded px-3 py-2 text-white placeholder:text-slate-500 focus:outline-none focus:border-emerald-500/50"
                />
              </div>
            </div>

            {/* Lead Investor */}
            <div>
              <label className="block text-xs font-medium text-slate-400 mb-1">
                Lead Investor
              </label>
              <input
                type="text"
                value={formData.leadInvestor}
                onChange={(e) =>
                  setFormData({ ...formData, leadInvestor: e.target.value })
                }
                placeholder="Sequoia Capital"
                className="w-full bg-slate-900 border border-slate-700 rounded px-3 py-2 text-white placeholder:text-slate-500 focus:outline-none focus:border-emerald-500/50"
              />
            </div>

            {/* Website */}
            <div>
              <label className="block text-xs font-medium text-slate-400 mb-1">
                Website
              </label>
              <div className="relative">
                <Globe className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-500" />
                <input
                  type="url"
                  value={formData.website}
                  onChange={(e) =>
                    setFormData({ ...formData, website: e.target.value })
                  }
                  placeholder="https://acme.com"
                  className="w-full bg-slate-900 border border-slate-700 rounded pl-10 pr-3 py-2 text-white placeholder:text-slate-500 focus:outline-none focus:border-emerald-500/50"
                />
              </div>
            </div>

            {/* Notes */}
            <div>
              <label className="block text-xs font-medium text-slate-400 mb-1">
                Notes
              </label>
              <textarea
                value={formData.notes}
                onChange={(e) =>
                  setFormData({ ...formData, notes: e.target.value })
                }
                placeholder="Add any notes about this company..."
                rows={3}
                className="w-full bg-slate-900 border border-slate-700 rounded px-3 py-2 text-white placeholder:text-slate-500 focus:outline-none focus:border-emerald-500/50 resize-none"
              />
            </div>

            {/* Pipeline Tracking Section */}
            <div className="pt-3 border-t border-slate-800">
              <h3 className="text-xs font-medium text-slate-500 uppercase mb-3">
                Pipeline Status
              </h3>
            </div>

            {/* Pipeline Status - visible for both create and edit */}
            <div>
              <label className="block text-xs font-medium text-slate-400 mb-1">
                Status
              </label>
              <div className="relative">
                <Target className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-500" />
                <select
                  value={formData.status}
                  onChange={(e) =>
                    setFormData({ ...formData, status: e.target.value as TrackerStatus })
                  }
                  className="w-full bg-slate-900 border border-slate-700 rounded pl-10 pr-3 py-2 text-white focus:outline-none focus:border-emerald-500/50 appearance-none cursor-pointer"
                >
                  {(Object.entries(TRACKER_STATUS_LABELS) as [TrackerStatus, string][]).map(
                    ([value, label]) => (
                      <option key={value} value={value}>
                        {label}
                      </option>
                    )
                  )}
                </select>
              </div>
            </div>

            {/* Additional Pipeline Fields (only for editing) */}
            {!isCreating && (
              <>
                {/* Last Contact Date */}
                <div>
                  <label className="block text-xs font-medium text-slate-400 mb-1">
                    Last Contact Date
                  </label>
                  <div className="relative">
                    <Calendar className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-500" />
                    <input
                      type="date"
                      value={formData.lastContactDate}
                      onChange={(e) =>
                        setFormData({ ...formData, lastContactDate: e.target.value })
                      }
                      className="w-full bg-slate-900 border border-slate-700 rounded pl-10 pr-3 py-2 text-white focus:outline-none focus:border-emerald-500/50"
                    />
                  </div>
                </div>

                {/* Next Step */}
                <div>
                  <label className="block text-xs font-medium text-slate-400 mb-1">
                    Next Step
                  </label>
                  <input
                    type="text"
                    value={formData.nextStep}
                    onChange={(e) =>
                      setFormData({ ...formData, nextStep: e.target.value })
                    }
                    placeholder="Schedule follow-up call"
                    className="w-full bg-slate-900 border border-slate-700 rounded px-3 py-2 text-white placeholder:text-slate-500 focus:outline-none focus:border-emerald-500/50"
                  />
                </div>
              </>
            )}
          </div>

          {/* Footer */}
          <div className="flex items-center justify-between p-4 border-t border-slate-800">
            {/* Delete Button */}
            {!isCreating && onDelete && (
              <div>
                {showDeleteConfirm ? (
                  <div className="flex items-center gap-2">
                    <span className="text-sm text-slate-400">Delete?</span>
                    <button
                      type="button"
                      onClick={handleDelete}
                      disabled={isSubmitting}
                      className="px-3 py-1 text-sm bg-red-600 hover:bg-red-500 text-white rounded transition-colors"
                    >
                      Yes
                    </button>
                    <button
                      type="button"
                      onClick={() => setShowDeleteConfirm(false)}
                      className="px-3 py-1 text-sm bg-slate-700 hover:bg-slate-600 text-white rounded transition-colors"
                    >
                      No
                    </button>
                  </div>
                ) : (
                  <button
                    type="button"
                    onClick={() => setShowDeleteConfirm(true)}
                    className="flex items-center gap-1 px-3 py-2 text-sm text-red-400 hover:text-red-300 hover:bg-red-900/20 rounded transition-colors"
                  >
                    <Trash2 className="w-4 h-4" />
                    Delete
                  </button>
                )}
              </div>
            )}

            {/* Action Buttons */}
            <div className={`flex items-center gap-2 ${isCreating || !onDelete ? 'ml-auto' : ''}`}>
              <button
                type="button"
                onClick={onClose}
                className="px-4 py-2 text-sm text-slate-400 hover:text-white hover:bg-slate-800 rounded transition-colors"
              >
                Cancel
              </button>
              <button
                type="submit"
                disabled={isSubmitting || !formData.companyName.trim()}
                className="px-4 py-2 text-sm bg-emerald-600 hover:bg-emerald-500 disabled:bg-slate-700 disabled:text-slate-500 text-white rounded font-medium transition-colors"
              >
                {isSubmitting ? 'Saving...' : isCreating ? 'Add Company' : 'Save Changes'}
              </button>
            </div>
          </div>
        </form>
      </div>
    </>
  );
}

export default TrackerModal;
