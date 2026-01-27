/**
 * DealModal - Detailed view of a single deal
 *
 * Shows deal details with company links and founder info
 * Command Center dark theme
 *
 * Accessibility features:
 * - Focus trap within modal
 * - Escape key to close
 * - Focus restoration on close
 */

import { memo, useState, useEffect, useRef, useCallback } from 'react';
import {
  X,
  ExternalLink,
  Target,
  Check,
  Loader2,
  Flag,
  Pencil,
} from 'lucide-react';
import type { Deal } from '../types';
import { STAGE_LABELS, CATEGORY_LABELS } from '../types';
import { addDealToTracker, deleteTrackerItem, flagDeal } from '../api/deals';
import { DealEditModal } from './DealEditModal';

interface DealModalProps {
  deal: Deal;
  onClose: () => void;
  onDealUpdated?: (updatedDeal: Deal) => void;
}

export const DealModal = memo(function DealModal({ deal, onClose, onDealUpdated }: DealModalProps) {
  const modalRef = useRef<HTMLDivElement>(null);
  const closeButtonRef = useRef<HTMLButtonElement>(null);
  const [trackingStatus, setTrackingStatus] = useState<'idle' | 'loading' | 'success' | 'already' | 'error'>('idle');
  const [showUndo, setShowUndo] = useState(false);
  const [trackerItemId, setTrackerItemId] = useState<number | null>(null);
  const undoTimeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const [flagStatus, setFlagStatus] = useState<'idle' | 'loading' | 'success' | 'error'>('idle');
  const [isEditModalOpen, setIsEditModalOpen] = useState(false);
  const [currentDeal, setCurrentDeal] = useState(deal);

  // Update currentDeal when deal prop changes
  useEffect(() => {
    setCurrentDeal(deal);
  }, [deal]);

  const handleDealSaved = useCallback((updatedDeal: Deal) => {
    setCurrentDeal(updatedDeal);
    onDealUpdated?.(updatedDeal);
  }, [onDealUpdated]);

  const handleFlag = useCallback(async () => {
    if (flagStatus === 'loading' || flagStatus === 'success') return;

    // Prompt for reason
    const reason = window.prompt(
      `Flag "${currentDeal.startupName}" - What's wrong?\n\n(e.g., "Wrong funding amount", "Not Enterprise AI", "Duplicate entry")`
    );

    // User cancelled
    if (reason === null) return;

    setFlagStatus('loading');
    try {
      await flagDeal(
        currentDeal.id ? parseInt(currentDeal.id) : null,
        currentDeal.startupName,
        reason || undefined,
        currentDeal.sourceUrl || undefined
      );
      setFlagStatus('success');
    } catch (err) {
      console.error('Failed to flag deal:', err);
      setFlagStatus('error');
      setTimeout(() => setFlagStatus('idle'), 3000);
    }
  }, [currentDeal.id, currentDeal.startupName, currentDeal.sourceUrl, flagStatus]);

  const handleAddToTracker = useCallback(async () => {
    if (trackingStatus === 'loading' || trackingStatus === 'success' || trackingStatus === 'already') return;

    setTrackingStatus('loading');
    try {
      const item = await addDealToTracker(parseInt(currentDeal.id), 'watching');
      setTrackerItemId(item.id);
      setTrackingStatus('success');
      setShowUndo(true);
      undoTimeoutRef.current = setTimeout(() => {
        setShowUndo(false);
      }, 5000);
    } catch (err: unknown) {
      console.error('Failed to add to tracker:', err);
      const error = err as { status?: number };
      if (error.status === 400) {
        setTrackingStatus('already');
      } else {
        setTrackingStatus('error');
        setTimeout(() => setTrackingStatus('idle'), 3000);
      }
    }
  }, [currentDeal.id, trackingStatus]);

  const handleUndo = useCallback(async () => {
    if (!trackerItemId) return;
    if (undoTimeoutRef.current) clearTimeout(undoTimeoutRef.current);
    setShowUndo(false);
    try {
      await deleteTrackerItem(trackerItemId);
      setTrackerItemId(null);
      setTrackingStatus('idle');
    } catch (err) {
      console.error('Failed to undo track:', err);
      setTrackingStatus('success');
    }
  }, [trackerItemId]);

  // Cleanup undo timeout
  useEffect(() => {
    return () => {
      if (undoTimeoutRef.current) clearTimeout(undoTimeoutRef.current);
    };
  }, []);

  // Focus the close button when modal opens
  useEffect(() => {
    // Store the previously focused element BEFORE focusing modal
    const previouslyFocused = document.activeElement as HTMLElement;
    closeButtonRef.current?.focus();

    return () => {
      // Restore focus when modal closes
      if (previouslyFocused && document.body.contains(previouslyFocused)) {
        previouslyFocused.focus?.();
      }
    };
  }, []);

  // Handle Escape and Enter keys
  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      if (e.key === 'Escape') {
        onClose();
      }
      if (e.key === 'Enter') {
        e.preventDefault();
        if (showUndo) {
          handleUndo();
        } else {
          handleAddToTracker();
        }
      }
    };

    document.addEventListener('keydown', handleKeyDown);
    return () => document.removeEventListener('keydown', handleKeyDown);
  }, [onClose, showUndo, handleUndo, handleAddToTracker]);

  const handleBackdropClick = useCallback((e: React.MouseEvent) => {
    if (e.target === e.currentTarget) {
      onClose();
    }
  }, [onClose]);

  return (
    <>
      {/* Backdrop */}
      <div
        className="fixed inset-0 bg-black/85 backdrop-blur-[12px] z-40"
        onClick={handleBackdropClick}
        aria-hidden="true"
      />

      {/* Modal */}
      <div
        ref={modalRef}
        role="dialog"
        aria-modal="true"
        aria-labelledby="deal-modal-title"
        className="fixed inset-4 sm:inset-auto sm:top-1/2 sm:left-1/2 sm:-translate-x-1/2 sm:-translate-y-1/2 z-50 w-auto sm:w-[540px] bg-[#0a0a0f] border border-slate-800/20 rounded-xl overflow-y-auto max-h-[calc(100vh-2rem)] sm:max-h-[80vh]"
        style={{ scrollbarWidth: 'none' }}
      >
        <div className="p-8 sm:p-11 relative">
          {/* Top-right actions: Edit, Flag, Close */}
          <div className="absolute top-4 right-4 flex items-center gap-1">
            <button
              onClick={() => setIsEditModalOpen(true)}
              className="w-8 h-8 flex items-center justify-center rounded-md text-slate-700 hover:text-slate-400 transition-colors"
              aria-label="Edit deal"
              title="Edit"
            >
              <Pencil className="w-3.5 h-3.5" aria-hidden="true" />
            </button>
            <button
              onClick={handleFlag}
              disabled={flagStatus === 'loading' || flagStatus === 'success'}
              className={`w-8 h-8 flex items-center justify-center rounded-md transition-colors ${
                flagStatus === 'success'
                  ? 'text-red-400 cursor-default'
                  : flagStatus === 'loading'
                  ? 'text-slate-700 cursor-wait'
                  : 'text-slate-700 hover:text-red-400'
              }`}
              aria-label="Flag deal"
              title={flagStatus === 'success' ? 'Flagged!' : 'Flag for review'}
            >
              {flagStatus === 'loading' ? (
                <Loader2 className="w-3.5 h-3.5 animate-spin" aria-hidden="true" />
              ) : (
                <Flag className="w-3.5 h-3.5" aria-hidden="true" />
              )}
            </button>
            <button
              ref={closeButtonRef}
              onClick={onClose}
              className="w-8 h-8 flex items-center justify-center rounded-md text-slate-700 hover:text-slate-400 transition-colors focus:outline-none"
              aria-label="Close deal details"
            >
              <X className="w-4 h-4" aria-hidden="true" />
            </button>
          </div>

          {/* Header */}
          <div className="flex items-baseline justify-between mb-1.5">
            <h2 id="deal-modal-title" className="text-lg font-semibold text-slate-50 tracking-tight truncate pr-28">
              {currentDeal.startupName}
            </h2>
            <span className="text-base font-semibold text-slate-200 shrink-0">
              {currentDeal.amountInvested ? currentDeal.amountInvested.replace(/\bmillion\b/gi, 'M').replace(/\bbillion\b/gi, 'B').replace(/(\d)\s+(M|B)\b/g, '$1$2') : '—'}
            </span>
          </div>

          {/* Subtitle */}
          <p className="text-[11px] text-slate-600 mb-9">
            {currentDeal.leadInvestor || 'Unknown'} · {STAGE_LABELS[currentDeal.investmentStage]} · {currentDeal.enterpriseCategory
              ? (CATEGORY_LABELS[currentDeal.enterpriseCategory] || currentDeal.enterpriseCategory.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase()))
              : 'Unclassified'}
          </p>

          {/* Info Grid */}
          <div className="grid grid-cols-2 gap-7 mb-9">
            <div className="flex flex-col gap-1.5">
              <span className="text-[10px] uppercase tracking-[0.08em] text-slate-700 font-medium">Lead Investor</span>
              <span className="text-[13px] text-slate-200 font-medium">{currentDeal.leadInvestor || 'Unknown'}</span>
              {currentDeal.leadPartner && (
                <span className="text-[11px] text-slate-500">{currentDeal.leadPartner}</span>
              )}
            </div>
            <div className="flex flex-col gap-1.5">
              <span className="text-[10px] uppercase tracking-[0.08em] text-slate-700 font-medium">Stage</span>
              <span className="text-[13px] text-slate-200 font-medium">{STAGE_LABELS[currentDeal.investmentStage]}</span>
            </div>
            <div className="flex flex-col gap-1.5">
              <span className="text-[10px] uppercase tracking-[0.08em] text-slate-700 font-medium">Category</span>
              <span className="text-[13px] text-slate-200 font-medium">
                {currentDeal.enterpriseCategory
                  ? (CATEGORY_LABELS[currentDeal.enterpriseCategory] || currentDeal.enterpriseCategory.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase()))
                  : 'Unclassified'}
              </span>
            </div>
            <div className="flex flex-col gap-1.5">
              <span className="text-[10px] uppercase tracking-[0.08em] text-slate-700 font-medium">Source</span>
              {currentDeal.sourceUrl ? (
                <a
                  href={currentDeal.sourceUrl}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="text-[13px] text-blue-400 hover:text-blue-300 font-medium transition-colors"
                >
                  {getSourceLabel(currentDeal.sourceUrl)}
                </a>
              ) : (
                <span className="text-[13px] text-slate-200 font-medium">Unknown</span>
              )}
            </div>
            <div className="flex flex-col gap-1.5">
              <span className="text-[10px] uppercase tracking-[0.08em] text-slate-700 font-medium">Announced</span>
              <span className="text-[13px] text-slate-200 font-medium">{formatDate(currentDeal.date)}</span>
            </div>
          </div>

          {/* Founders */}
          {currentDeal.founders && currentDeal.founders.length > 0 && (
            <div className="pt-7 border-t border-slate-800/10">
              <div className="text-[10px] uppercase tracking-[0.08em] text-slate-700 font-medium mb-3.5">
                Founders
              </div>
              {currentDeal.founders.map((founder, index) => (
                <div key={index} className="flex items-center justify-between py-2">
                  <span className="text-[12px] text-slate-200 font-medium">{founder.name}</span>
                  <span className="text-[11px] text-slate-600">{getShortRole(founder.title)}</span>
                </div>
              ))}
            </div>
          )}

          {/* Actions */}
          <div className="flex items-center gap-3 pt-7 mt-7 border-t border-slate-800/10">
            {currentDeal.companyWebsite && (
              <a
                href={currentDeal.companyWebsite}
                target="_blank"
                rel="noopener noreferrer"
                className="flex items-center gap-2 px-4 py-2.5 rounded-md border border-slate-800/20 text-[11px] font-medium text-slate-500 hover:text-slate-200 hover:border-slate-800/40 transition-all"
              >
                <span className="w-1.5 h-1.5 rounded-full bg-cyan-500" />
                Website
              </a>
            )}
            {currentDeal.companyLinkedin && (
              <a
                href={currentDeal.companyLinkedin}
                target="_blank"
                rel="noopener noreferrer"
                className="flex items-center gap-2 px-4 py-2.5 rounded-md border border-slate-800/20 text-[11px] font-medium text-slate-500 hover:text-slate-200 hover:border-slate-800/40 transition-all"
              >
                <span className="w-1.5 h-1.5 rounded-full bg-blue-500" />
                LinkedIn
              </a>
            )}
            {currentDeal.sourceUrl && (
              <a
                href={currentDeal.sourceUrl}
                target="_blank"
                rel="noopener noreferrer"
                className="flex items-center gap-2 px-4 py-2.5 rounded-md border border-slate-800/20 text-[11px] font-medium text-slate-500 hover:text-slate-200 hover:border-slate-800/40 transition-all"
              >
                <ExternalLink className="w-3 h-3" />
                Source
              </a>
            )}
            {/* Track button - becomes Undo for 5s after tracking */}
            <button
              onClick={showUndo ? handleUndo : handleAddToTracker}
              disabled={trackingStatus === 'loading' || (trackingStatus === 'success' && !showUndo) || trackingStatus === 'already'}
              className={`flex items-center gap-2 px-4 py-2.5 rounded-md border text-[11px] font-medium transition-all ml-auto ${
                showUndo
                  ? 'border-slate-800/20 text-slate-400 hover:text-slate-200 hover:border-slate-800/40'
                  : trackingStatus === 'success' || trackingStatus === 'already'
                  ? 'border-emerald-700/30 text-emerald-400 cursor-default'
                  : trackingStatus === 'error'
                  ? 'border-red-700/30 text-red-400 hover:text-red-300'
                  : trackingStatus === 'loading'
                  ? 'border-slate-800/20 text-slate-600 cursor-wait'
                  : 'border-slate-800/20 text-slate-500 hover:text-slate-200 hover:border-slate-800/40'
              }`}
            >
              {trackingStatus === 'loading' ? (
                <Loader2 className="w-3 h-3 animate-spin" />
              ) : showUndo ? (
                <X className="w-3 h-3" />
              ) : trackingStatus === 'success' || trackingStatus === 'already' ? (
                <Check className="w-3 h-3" />
              ) : (
                <Target className="w-3 h-3" />
              )}
              {showUndo
                ? 'Undo'
                : trackingStatus === 'success' || trackingStatus === 'already'
                ? 'Tracked'
                : trackingStatus === 'error'
                ? 'Retry'
                : 'Track'}
            </button>
          </div>
        </div>
      </div>

      {/* Edit Modal */}
      {isEditModalOpen && (
        <DealEditModal
          deal={currentDeal}
          onClose={() => setIsEditModalOpen(false)}
          onSave={handleDealSaved}
        />
      )}
    </>
  );
});

function formatDate(dateString: string): string {
  if (!dateString) return 'Unknown';
  try {
    // Parse date components to avoid timezone issues with ISO date strings
    // "YYYY-MM-DD" parsed by new Date() is treated as UTC midnight,
    // which can shift to previous day in local timezones behind UTC
    const parts = dateString.split('T')[0].split('-');
    if (parts.length >= 3) {
      const year = parseInt(parts[0], 10);
      const month = parseInt(parts[1], 10) - 1; // JS months are 0-indexed
      const day = parseInt(parts[2], 10);
      const date = new Date(year, month, day);
      return date.toLocaleDateString('en-US', {
        year: 'numeric',
        month: 'long',
        day: 'numeric',
      });
    }
    // Fallback for other date formats
    const date = new Date(dateString);
    return date.toLocaleDateString('en-US', {
      year: 'numeric',
      month: 'long',
      day: 'numeric',
    });
  } catch {
    return dateString;
  }
}


function getShortRole(title?: string): string {
  if (!title) return '';
  const lower = title.toLowerCase();
  const roles = ['ceo', 'cto', 'cfo', 'coo', 'cpo', 'cmo', 'ciso'];
  for (const role of roles) {
    if (lower.includes(role)) return role.toUpperCase();
  }
  if (lower.includes('co-founder') || lower.includes('cofounder')) return 'Co-Founder';
  if (lower.includes('founder')) return 'Founder';
  if (lower.includes('president')) return 'President';
  if (lower.includes('partner')) return 'Partner';
  if (lower.includes('director')) return 'Director';
  if (lower.includes('vp') || lower.includes('vice president')) return 'VP';
  return '';
}

function getSourceLabel(url?: string): string {
  if (!url) return 'Unknown';
  try {
    // Google News redirects contain the real URL in the query string
    if (url.includes('google.com/url')) {
      const params = new URL(url).searchParams;
      const realUrl = params.get('url') || params.get('q') || '';
      if (realUrl) {
        const domain = new URL(realUrl).hostname.replace('www.', '');
        return domainToName(domain);
      }
      return 'Google News';
    }
    const domain = new URL(url).hostname.replace('www.', '');
    return domainToName(domain);
  } catch {
    return 'Unknown';
  }
}

function domainToName(domain: string): string {
  const map: Record<string, string> = {
    'techcrunch.com': 'TechCrunch',
    'sec.gov': 'SEC Form D',
    'bloomberg.com': 'Bloomberg',
    'reuters.com': 'Reuters',
    'crunchbase.com': 'Crunchbase',
    'fortune.com': 'Fortune',
    'forbes.com': 'Forbes',
    'wsj.com': 'WSJ',
    'nytimes.com': 'NY Times',
    'venturebeat.com': 'VentureBeat',
    'theinformation.com': 'The Information',
    'businessinsider.com': 'Business Insider',
    'semafor.com': 'Semafor',
    'axios.com': 'Axios',
  };
  return map[domain] || domain;
}

export default DealModal;
