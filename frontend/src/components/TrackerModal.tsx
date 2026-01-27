/**
 * TrackerModal - Create/Edit modal for tracker items
 *
 * Features:
 * - Create new companies
 * - Edit existing items
 * - Delete items
 */

import { useState, useEffect, useRef, useCallback } from 'react';
import { X } from 'lucide-react';
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
        className="fixed inset-0 bg-black/75 backdrop-blur-sm z-40"
        onClick={handleBackdropClick}
        aria-hidden="true"
      />

      {/* Modal */}
      <div
        role="dialog"
        aria-modal="true"
        aria-labelledby="tracker-modal-title"
        className="fixed top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 z-50 w-full max-w-[520px] mx-4 bg-[#0a0a0f] border border-zinc-800/25 rounded-2xl shadow-2xl"
      >
        {/* Close */}
        <button
          ref={closeButtonRef}
          onClick={onClose}
          className="absolute top-6 right-7 text-zinc-700 hover:text-zinc-400 transition-colors p-2"
          aria-label="Close"
        >
          <X className="w-4 h-4" />
        </button>

        {/* Form */}
        <form onSubmit={handleSubmit}>
          <div className="px-10 sm:px-14 pt-12 pb-8 max-h-[70vh] overflow-y-auto">
            <h2 id="tracker-modal-title" className="text-[15px] font-semibold text-zinc-200 tracking-[-0.02em] mb-10">
              {isCreating ? 'Add Company' : (formData.companyName || 'Edit Company')}
            </h2>

            {/* Company Name */}
            <div className="mb-8">
              <label className="block text-[10px] uppercase tracking-[0.1em] text-zinc-600 font-medium mb-3">
                Company
              </label>
              <input
                type="text"
                value={formData.companyName}
                onChange={(e) => setFormData({ ...formData, companyName: e.target.value })}
                placeholder="Acme Inc."
                required
                className="w-full bg-transparent border-b border-zinc-800 py-2.5 text-sm text-zinc-50 placeholder:text-zinc-800 placeholder:font-light focus:outline-none focus:border-zinc-600 transition-colors"
              />
            </div>

            {/* Round & Amount */}
            <div className="grid grid-cols-2 gap-8 mb-8">
              <div>
                <label className="block text-[10px] uppercase tracking-[0.1em] text-zinc-600 font-medium mb-3">
                  Round
                </label>
                <select
                  value={formData.roundType}
                  onChange={(e) => setFormData({ ...formData, roundType: e.target.value })}
                  className="w-full bg-transparent border-b border-zinc-800 py-2.5 text-sm text-zinc-50 focus:outline-none focus:border-zinc-600 appearance-none cursor-pointer transition-colors"
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
                <label className="block text-[10px] uppercase tracking-[0.1em] text-zinc-600 font-medium mb-3">
                  Amount
                </label>
                <input
                  type="text"
                  value={formData.amount}
                  onChange={(e) => setFormData({ ...formData, amount: e.target.value })}
                  placeholder="$10M"
                  className="w-full bg-transparent border-b border-zinc-800 py-2.5 text-sm text-zinc-50 placeholder:text-zinc-800 placeholder:font-light focus:outline-none focus:border-zinc-600 transition-colors"
                />
              </div>
            </div>

            {/* Lead Investor */}
            <div className="mb-8">
              <label className="block text-[10px] uppercase tracking-[0.1em] text-zinc-600 font-medium mb-3">
                Lead Investor
              </label>
              <input
                type="text"
                value={formData.leadInvestor}
                onChange={(e) => setFormData({ ...formData, leadInvestor: e.target.value })}
                placeholder="Sequoia Capital"
                className="w-full bg-transparent border-b border-zinc-800 py-2.5 text-sm text-zinc-50 placeholder:text-zinc-800 placeholder:font-light focus:outline-none focus:border-zinc-600 transition-colors"
              />
            </div>

            {/* Website */}
            <div className="mb-8">
              <label className="block text-[10px] uppercase tracking-[0.1em] text-zinc-600 font-medium mb-3">
                Website
              </label>
              <input
                type="url"
                value={formData.website}
                onChange={(e) => setFormData({ ...formData, website: e.target.value })}
                placeholder="https://"
                className="w-full bg-transparent border-b border-zinc-800 py-2.5 text-sm text-zinc-50 placeholder:text-zinc-800 placeholder:font-light focus:outline-none focus:border-zinc-600 transition-colors"
              />
            </div>

            {/* Notes */}
            <div className="mb-8">
              <label className="block text-[10px] uppercase tracking-[0.1em] text-zinc-600 font-medium mb-3">
                Notes
              </label>
              <textarea
                value={formData.notes}
                onChange={(e) => setFormData({ ...formData, notes: e.target.value })}
                placeholder="Internal notes..."
                rows={2}
                className="w-full bg-transparent border-b border-zinc-800 py-2.5 text-sm text-zinc-50 placeholder:text-zinc-800 placeholder:font-light focus:outline-none focus:border-zinc-600 transition-colors resize-none"
              />
            </div>

            {/* Pipeline Section */}
            <div className="border-t border-zinc-800/15 pt-8 mt-8">
              <div className="mb-8">
                <label className="block text-[10px] uppercase tracking-[0.1em] text-zinc-600 font-medium mb-3">
                  Status
                </label>
                <select
                  value={formData.status}
                  onChange={(e) => setFormData({ ...formData, status: e.target.value as TrackerStatus })}
                  className="w-full bg-transparent border-b border-zinc-800 py-2.5 text-sm text-zinc-50 focus:outline-none focus:border-zinc-600 appearance-none cursor-pointer transition-colors"
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

              {/* Additional fields for editing */}
              {!isCreating && (
                <>
                  <div className="mb-8">
                    <label className="block text-[10px] uppercase tracking-[0.1em] text-zinc-600 font-medium mb-3">
                      Next Step
                    </label>
                    <input
                      type="text"
                      value={formData.nextStep}
                      onChange={(e) => setFormData({ ...formData, nextStep: e.target.value })}
                      placeholder="What's next?"
                      className="w-full bg-transparent border-b border-zinc-800 py-2.5 text-sm text-zinc-50 placeholder:text-zinc-800 placeholder:font-light focus:outline-none focus:border-zinc-600 transition-colors"
                    />
                  </div>

                  <div className="mb-8">
                    <label className="block text-[10px] uppercase tracking-[0.1em] text-zinc-600 font-medium mb-3">
                      Last Contact
                    </label>
                    <input
                      type="date"
                      value={formData.lastContactDate}
                      onChange={(e) => setFormData({ ...formData, lastContactDate: e.target.value })}
                      className="w-full bg-transparent border-b border-zinc-800 py-2.5 text-sm text-zinc-50 focus:outline-none focus:border-zinc-600 transition-colors"
                    />
                  </div>
                </>
              )}
            </div>
          </div>

          {/* Footer */}
          <div className="flex items-center justify-between px-10 sm:px-14 pb-10 pt-2">
            {/* Delete */}
            {!isCreating && onDelete && (
              <div>
                {showDeleteConfirm ? (
                  <div className="flex items-center gap-3">
                    <span className="text-xs text-zinc-500">Delete?</span>
                    <button
                      type="button"
                      onClick={handleDelete}
                      disabled={isSubmitting}
                      className="text-xs text-red-400 hover:text-red-300 transition-colors"
                    >
                      Yes
                    </button>
                    <button
                      type="button"
                      onClick={() => setShowDeleteConfirm(false)}
                      className="text-xs text-zinc-600 hover:text-zinc-400 transition-colors"
                    >
                      No
                    </button>
                  </div>
                ) : (
                  <button
                    type="button"
                    onClick={() => setShowDeleteConfirm(true)}
                    className="text-xs text-zinc-700 hover:text-red-400 transition-colors py-2"
                  >
                    Delete
                  </button>
                )}
              </div>
            )}

            {/* Save */}
            <button
              type="submit"
              disabled={isSubmitting || !formData.companyName.trim()}
              className={`text-xs font-medium text-emerald-500 hover:text-emerald-400 disabled:text-zinc-700 transition-colors py-2 ${isCreating || !onDelete ? 'ml-auto' : ''}`}
            >
              {isSubmitting ? 'Saving...' : 'Save'}
            </button>
          </div>
        </form>
      </div>
    </>
  );
}

export default TrackerModal;
