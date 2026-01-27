/**
 * TrackerBulkAddModal - Modal for bulk adding companies to the tracker
 *
 * Supports comma-separated and newline-separated company names.
 */

import { useState, useMemo, useEffect } from 'react';
import { X } from 'lucide-react';

interface TrackerBulkAddModalProps {
  onClose: () => void;
  onSubmit: (companyNames: string[]) => Promise<void>;
}

export function TrackerBulkAddModal({
  onClose,
  onSubmit,
}: TrackerBulkAddModalProps) {
  const [input, setInput] = useState('');
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onClose();
    };
    document.addEventListener('keydown', handleKeyDown);
    return () => document.removeEventListener('keydown', handleKeyDown);
  }, [onClose]);

  // Parse input into company names (supports comma and newline separation)
  const parsedNames = useMemo(() => {
    if (!input.trim()) return [];

    const names = input
      .split(/[\n,]+/)
      .map((name) => name.trim())
      .filter((name) => name.length > 0);

    // Dedupe (case-insensitive)
    const seen = new Set<string>();
    return names.filter((name) => {
      const lower = name.toLowerCase();
      if (seen.has(lower)) return false;
      seen.add(lower);
      return true;
    });
  }, [input]);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (parsedNames.length === 0) {
      setError('Please enter at least one company name');
      return;
    }

    setIsSubmitting(true);
    setError(null);

    try {
      await onSubmit(parsedNames);
      onClose();
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to add companies');
    } finally {
      setIsSubmitting(false);
    }
  };

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
        className="fixed top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 z-50 w-full max-w-[460px] mx-4 bg-[#0a0a0f] border border-zinc-800/25 rounded-2xl shadow-2xl"
      >
        <button
          onClick={onClose}
          className="absolute top-6 right-7 text-zinc-700 hover:text-zinc-400 transition-colors p-2"
          aria-label="Close"
        >
          <X className="w-4 h-4" />
        </button>

        <form onSubmit={handleSubmit} className="px-10 sm:px-14 py-12">
          <h2 className="text-[15px] font-semibold text-zinc-200 tracking-[-0.02em] mb-10">
            Add Companies
          </h2>

          <div>
            <label
              htmlFor="companies"
              className="block text-[10px] uppercase tracking-[0.1em] text-zinc-600 font-medium mb-3"
            >
              One per line
            </label>
            <textarea
              id="companies"
              value={input}
              onChange={(e) => {
                setInput(e.target.value);
                setError(null);
              }}
              placeholder={"Acme Corp\nTechStartup\nAI Solutions"}
              rows={8}
              className="w-full bg-transparent border-b border-zinc-800 py-2.5 text-[13px] text-zinc-200 font-mono font-light leading-[1.8] placeholder:text-zinc-800 focus:outline-none focus:border-zinc-600 transition-colors resize-none"
              autoFocus
            />
            {parsedNames.length > 0 && (
              <div className="text-[11px] text-zinc-700 mt-3">
                {parsedNames.length} {parsedNames.length === 1 ? 'company' : 'companies'}
              </div>
            )}
          </div>

          {/* Error */}
          {error && (
            <div className="text-xs text-red-400 mt-4">
              {error}
            </div>
          )}

          {/* Actions */}
          <div className="flex justify-end mt-12">
            <button
              type="submit"
              disabled={isSubmitting || parsedNames.length === 0}
              className="text-xs font-medium text-emerald-500 hover:text-emerald-400 disabled:text-zinc-700 transition-colors py-2"
            >
              {isSubmitting ? 'Adding...' : 'Add'}
            </button>
          </div>
        </form>
      </div>
    </>
  );
}

export default TrackerBulkAddModal;
