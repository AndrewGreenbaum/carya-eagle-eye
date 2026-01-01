/**
 * FeedbackModal - Submit feedback, report errors, or suggest companies
 *
 * Features:
 * - Two modes: "error" (report a problem) and "suggestion" (suggest a company)
 * - Submits to /feedback/suggestion endpoint
 * - Casual, friendly tone
 */

import { useState, useEffect, useRef, useCallback } from 'react';
import { X, AlertCircle, Lightbulb, Loader2, Check, Send } from 'lucide-react';
import { submitFeedback } from '../api/deals';

interface FeedbackModalProps {
  onClose: () => void;
}

type FeedbackType = 'error' | 'suggestion';

export function FeedbackModal({ onClose }: FeedbackModalProps) {
  const [feedbackType, setFeedbackType] = useState<FeedbackType | null>(null);
  const [companyName, setCompanyName] = useState('');
  const [details, setDetails] = useState('');
  const [status, setStatus] = useState<'idle' | 'loading' | 'success' | 'error'>('idle');
  const [errorMessage, setErrorMessage] = useState('');

  const modalRef = useRef<HTMLDivElement>(null);
  const closeButtonRef = useRef<HTMLButtonElement>(null);

  // Focus management
  useEffect(() => {
    closeButtonRef.current?.focus();
    const previouslyFocused = document.activeElement as HTMLElement;
    return () => {
      previouslyFocused?.focus?.();
    };
  }, []);

  // Handle Escape key
  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      if (e.key === 'Escape') {
        onClose();
      }
    };
    document.addEventListener('keydown', handleKeyDown);
    return () => document.removeEventListener('keydown', handleKeyDown);
  }, [onClose]);

  const handleBackdropClick = useCallback((e: React.MouseEvent) => {
    if (e.target === e.currentTarget) {
      onClose();
    }
  }, [onClose]);

  const handleSubmit = async () => {
    if (!feedbackType || !companyName.trim()) return;

    setStatus('loading');
    setErrorMessage('');

    try {
      await submitFeedback(
        companyName.trim(),
        details.trim() || undefined,
        feedbackType === 'error' ? 'error' : 'missing_company'
      );
      setStatus('success');
      // Auto-close after success
      setTimeout(() => onClose(), 2000);
    } catch (err) {
      console.error('Failed to submit feedback:', err);
      setStatus('error');
      setErrorMessage('Failed to submit. Please try again.');
    }
  };

  const handleBack = () => {
    setFeedbackType(null);
    setCompanyName('');
    setDetails('');
    setStatus('idle');
    setErrorMessage('');
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
        ref={modalRef}
        role="dialog"
        aria-modal="true"
        aria-labelledby="feedback-modal-title"
        className="fixed top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 z-50 w-full max-w-md mx-4 bg-[#0a0a0c] border border-slate-800 rounded-lg shadow-2xl animate-slide-up"
      >
        {/* Header */}
        <div className="flex items-center justify-between p-6 border-b border-slate-800">
          <div>
            <h2 id="feedback-modal-title" className="text-xl font-bold text-white">
              {feedbackType === null
                ? 'How can we help?'
                : feedbackType === 'error'
                ? 'Report an Error'
                : 'Suggest a Company'}
            </h2>
            <p className="text-sm text-slate-400 mt-1">
              {feedbackType === null
                ? "We're not perfect. Let us know how we can improve."
                : feedbackType === 'error'
                ? "Tell us what's wrong and we'll fix it."
                : "Know a company we should be tracking?"}
            </p>
          </div>
          <button
            ref={closeButtonRef}
            onClick={onClose}
            className="p-2 hover:bg-slate-800 rounded transition-colors text-slate-400 hover:text-white"
            aria-label="Close feedback"
          >
            <X className="w-5 h-5" />
          </button>
        </div>

        {/* Content */}
        <div className="p-6">
          {/* Success State */}
          {status === 'success' && (
            <div className="text-center py-8">
              <div className="inline-flex items-center justify-center w-16 h-16 rounded-full bg-emerald-500/20 mb-4">
                <Check className="w-8 h-8 text-emerald-400" />
              </div>
              <h3 className="text-lg font-bold text-white mb-2">Thanks for the feedback!</h3>
              <p className="text-sm text-slate-400">We'll look into it soon.</p>
            </div>
          )}

          {/* Type Selection */}
          {status !== 'success' && feedbackType === null && (
            <div className="space-y-3">
              <button
                onClick={() => setFeedbackType('error')}
                className="w-full flex items-center gap-4 p-4 bg-slate-900 hover:bg-slate-800 border border-slate-800 hover:border-red-600/50 rounded-lg transition-all group"
              >
                <div className="p-3 bg-red-500/10 rounded-lg group-hover:bg-red-500/20">
                  <AlertCircle className="w-6 h-6 text-red-400" />
                </div>
                <div className="text-left">
                  <div className="font-medium text-white">Report an Error</div>
                  <div className="text-sm text-slate-400">
                    Wrong data, duplicate, or incorrect info
                  </div>
                </div>
              </button>

              <button
                onClick={() => setFeedbackType('suggestion')}
                className="w-full flex items-center gap-4 p-4 bg-slate-900 hover:bg-slate-800 border border-slate-800 hover:border-amber-600/50 rounded-lg transition-all group"
              >
                <div className="p-3 bg-amber-500/10 rounded-lg group-hover:bg-amber-500/20">
                  <Lightbulb className="w-6 h-6 text-amber-400" />
                </div>
                <div className="text-left">
                  <div className="font-medium text-white">Suggest a Company</div>
                  <div className="text-sm text-slate-400">
                    Know a company we should be tracking?
                  </div>
                </div>
              </button>
            </div>
          )}

          {/* Form */}
          {status !== 'success' && feedbackType !== null && (
            <div className="space-y-4">
              <div>
                <label className="block text-sm font-medium text-slate-300 mb-1">
                  Company Name *
                </label>
                <input
                  type="text"
                  value={companyName}
                  onChange={(e) => setCompanyName(e.target.value)}
                  placeholder="e.g., Anthropic, OpenAI, Glean"
                  className="w-full bg-slate-900 border border-slate-700 rounded px-3 py-2 text-white placeholder:text-slate-500 focus:outline-none focus:border-emerald-500/50"
                  autoFocus
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-slate-300 mb-1">
                  {feedbackType === 'error' ? "What's wrong?" : 'Details (optional)'}
                </label>
                <textarea
                  value={details}
                  onChange={(e) => setDetails(e.target.value)}
                  placeholder={
                    feedbackType === 'error'
                      ? 'e.g., Wrong funding amount, not Enterprise AI, duplicate entry...'
                      : 'e.g., Just raised Series B, AI infrastructure company...'
                  }
                  rows={3}
                  className="w-full bg-slate-900 border border-slate-700 rounded px-3 py-2 text-white placeholder:text-slate-500 focus:outline-none focus:border-emerald-500/50 resize-none"
                />
              </div>

              {errorMessage && (
                <div className="text-sm text-red-400 bg-red-400/10 px-3 py-2 rounded">
                  {errorMessage}
                </div>
              )}
            </div>
          )}
        </div>

        {/* Footer */}
        {status !== 'success' && (
          <div className="px-6 py-4 border-t border-slate-800 flex justify-between">
            {feedbackType !== null ? (
              <>
                <button
                  onClick={handleBack}
                  className="btn-secondary"
                  disabled={status === 'loading'}
                >
                  Back
                </button>
                <button
                  onClick={handleSubmit}
                  disabled={!companyName.trim() || status === 'loading'}
                  className={`flex items-center gap-2 px-4 py-2 rounded font-medium transition-colors ${
                    status === 'loading'
                      ? 'bg-emerald-600/50 text-white cursor-wait'
                      : !companyName.trim()
                      ? 'bg-slate-700 text-slate-500 cursor-not-allowed'
                      : 'bg-emerald-600 hover:bg-emerald-500 text-white'
                  }`}
                >
                  {status === 'loading' ? (
                    <Loader2 className="w-4 h-4 animate-spin" />
                  ) : (
                    <Send className="w-4 h-4" />
                  )}
                  Submit
                </button>
              </>
            ) : (
              <button onClick={onClose} className="btn-secondary ml-auto">
                Cancel
              </button>
            )}
          </div>
        )}
      </div>
    </>
  );
}

export default FeedbackModal;
