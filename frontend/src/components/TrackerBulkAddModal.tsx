/**
 * TrackerBulkAddModal - Modal for bulk adding companies to the tracker
 *
 * Supports comma-separated and newline-separated company names.
 */

import { useState, useMemo } from 'react';
import { X, Upload, AlertCircle } from 'lucide-react';

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

  // Parse input into company names (supports comma and newline separation)
  const parsedNames = useMemo(() => {
    if (!input.trim()) return [];

    // Split by newlines first, then by commas
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
    <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
      <div className="bg-slate-900 rounded-lg w-full max-w-lg mx-4 shadow-xl">
        {/* Header */}
        <div className="flex items-center justify-between px-6 py-4 border-b border-slate-800">
          <div>
            <h2 className="text-lg font-semibold text-white">Bulk Add Companies</h2>
            <p className="text-sm text-slate-400 mt-0.5">
              Add multiple companies to Watching
            </p>
          </div>
          <button
            onClick={onClose}
            className="text-slate-400 hover:text-white transition-colors"
          >
            <X className="w-5 h-5" />
          </button>
        </div>

        {/* Form */}
        <form onSubmit={handleSubmit} className="p-6 space-y-4">
          {/* Text Area */}
          <div>
            <label
              htmlFor="companies"
              className="block text-sm font-medium text-slate-300 mb-1.5"
            >
              Company Names
            </label>
            <textarea
              id="companies"
              value={input}
              onChange={(e) => {
                setInput(e.target.value);
                setError(null);
              }}
              placeholder="Enter company names separated by commas or new lines:

Acme Corp
TechStartup Inc, AI Solutions
DataCo"
              rows={8}
              className="w-full px-3 py-2 bg-slate-800 border border-slate-700 rounded text-white placeholder-slate-500 focus:outline-none focus:ring-2 focus:ring-emerald-500/50 focus:border-emerald-500 resize-none font-mono text-sm"
              autoFocus
            />
          </div>

          {/* Preview */}
          {parsedNames.length > 0 && (
            <div className="bg-slate-800/50 rounded-lg p-3">
              <div className="flex items-center justify-between mb-2">
                <span className="text-sm font-medium text-slate-300">
                  Preview ({parsedNames.length} {parsedNames.length === 1 ? 'company' : 'companies'})
                </span>
              </div>
              <div className="flex flex-wrap gap-1.5 max-h-24 overflow-y-auto">
                {parsedNames.map((name, i) => (
                  <span
                    key={i}
                    className="px-2 py-0.5 bg-slate-700 text-slate-300 text-xs rounded"
                  >
                    {name}
                  </span>
                ))}
              </div>
            </div>
          )}

          {/* Error */}
          {error && (
            <div className="flex items-center gap-2 p-3 bg-red-500/10 border border-red-500/30 rounded text-red-400 text-sm">
              <AlertCircle className="w-4 h-4 shrink-0" />
              {error}
            </div>
          )}

          {/* Actions */}
          <div className="flex justify-end gap-3 pt-2">
            <button
              type="button"
              onClick={onClose}
              className="px-4 py-2 text-slate-300 hover:text-white transition-colors"
              disabled={isSubmitting}
            >
              Cancel
            </button>
            <button
              type="submit"
              disabled={isSubmitting || parsedNames.length === 0}
              className="flex items-center gap-2 px-4 py-2 bg-emerald-600 hover:bg-emerald-500 disabled:bg-slate-700 disabled:text-slate-500 text-white rounded font-medium transition-colors"
            >
              <Upload className="w-4 h-4" />
              {isSubmitting
                ? 'Adding...'
                : `Add ${parsedNames.length} ${parsedNames.length === 1 ? 'Company' : 'Companies'}`}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}

export default TrackerBulkAddModal;
