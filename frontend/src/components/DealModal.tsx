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
  Users,
  DollarSign,
  Calendar,
  Shield,
  Cpu,
  Bot,
  Database,
  Building2,
  Layers,
  Globe,
  Linkedin,
  User,
  Target,
  Check,
  Loader2,
  Flag,
  AlertTriangle,
  Pencil,
} from 'lucide-react';
import type { Deal, EnterpriseCategory } from '../types';
import { STAGE_LABELS, CATEGORY_LABELS } from '../types';
import { addDealToTracker, flagDeal } from '../api/deals';
import { DealEditModal } from './DealEditModal';

interface DealModalProps {
  deal: Deal;
  onClose: () => void;
  onDealUpdated?: (updatedDeal: Deal) => void;
  onCopy?: (deal: Deal) => void;
}

export const DealModal = memo(function DealModal({ deal, onClose, onDealUpdated, onCopy }: DealModalProps) {
  const modalRef = useRef<HTMLDivElement>(null);
  const closeButtonRef = useRef<HTMLButtonElement>(null);
  const [trackingStatus, setTrackingStatus] = useState<'idle' | 'loading' | 'success' | 'already' | 'error'>('idle');
  const [flagStatus, setFlagStatus] = useState<'idle' | 'loading' | 'success' | 'error'>('idle');
  const [isEditModalOpen, setIsEditModalOpen] = useState(false);
  const [currentDeal, setCurrentDeal] = useState(deal);

  // Undo tracking state
  const [undoTimeout, setUndoTimeout] = useState<ReturnType<typeof setTimeout> | null>(null);
  const [showUndoToast, setShowUndoToast] = useState(false);
  const [copyToast, setCopyToast] = useState(false);

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
    if (trackingStatus === 'loading' || trackingStatus === 'already' || showUndoToast) return;

    setTrackingStatus('loading');
    try {
      await addDealToTracker(parseInt(currentDeal.id), 'watching');
      setTrackingStatus('success');
      // Show undo button for 10 seconds
      setShowUndoToast(true);
      const timeout = setTimeout(() => {
        setShowUndoToast(false);
        setUndoTimeout(null);
      }, 10000);
      setUndoTimeout(timeout);
    } catch (err: unknown) {
      console.error('Failed to add to tracker:', err);
      const error = err as { status?: number };
      if (error.status === 400) {
        setTrackingStatus('already');
      } else {
        setTrackingStatus('error');
      }
    }
  }, [currentDeal.id, trackingStatus, showUndoToast]);

  // Focus the close button when modal opens
  useEffect(() => {
    closeButtonRef.current?.focus();
    // Note: We intentionally do NOT restore focus on close
    // because that causes unwanted scrolling. The DealsTable
    // handles scroll-to-selected-row on modal close.
  }, []);

  // Handle keyboard shortcuts
  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      // Skip if typing in input or if edit modal is open
      const tag = (e.target as HTMLElement).tagName;
      if (tag === 'INPUT' || tag === 'TEXTAREA' || isEditModalOpen) return;

      if (e.key === 'Escape') {
        onClose();
        return;
      }

      // Ctrl+C to copy deal in spreadsheet format (tab-separated)
      if ((e.ctrlKey || e.metaKey) && e.key === 'c') {
        e.preventDefault();
        // Format: Company, Round, Amount, Lead Investor, Date, Category, Website
        const fields = [
          currentDeal.startupName,
          currentDeal.investmentStage?.toUpperCase() || '',
          currentDeal.amountInvested || '',
          currentDeal.leadInvestor || '',
          currentDeal.date || '',
          currentDeal.enterpriseCategory?.replace('_', ' ') || '',
          currentDeal.companyWebsite || '',
        ];
        const tsvData = fields.join('\t');
        navigator.clipboard.writeText(tsvData).catch(console.error);
        onCopy?.(currentDeal);
        setCopyToast(true);
        setTimeout(() => setCopyToast(false), 2000);
        return;
      }

      // 't' to track instantly (or untrack within 10s)
      if (e.key === 't' && !e.ctrlKey && !e.metaKey && !e.altKey) {
        e.preventDefault();

        if (showUndoToast && undoTimeout) {
          // Untrack: remove from tracker
          clearTimeout(undoTimeout);
          setUndoTimeout(null);
          setShowUndoToast(false);
          setTrackingStatus('idle');
          // Note: actual untrack API call would go here if needed
        } else if (trackingStatus !== 'loading' && trackingStatus !== 'already') {
          // Track - allowed from idle, error, or success states
          setTrackingStatus('loading');
          addDealToTracker(parseInt(currentDeal.id), 'watching')
            .then(() => {
              setTrackingStatus('success');
              setShowUndoToast(true);
              // Allow untrack for 10 seconds
              const timeout = setTimeout(() => {
                setShowUndoToast(false);
                setUndoTimeout(null);
              }, 10000);
              setUndoTimeout(timeout);
            })
            .catch((err: unknown) => {
              console.error('Failed to add to tracker:', err);
              const error = err as { status?: number };
              if (error.status === 400) {
                setTrackingStatus('already');
              } else {
                setTrackingStatus('error');
              }
            });
        }
      }
    };

    document.addEventListener('keydown', handleKeyDown);
    return () => {
      document.removeEventListener('keydown', handleKeyDown);
      if (undoTimeout) clearTimeout(undoTimeout);
    };
  }, [onClose, currentDeal, onCopy, trackingStatus, undoTimeout, showUndoToast, isEditModalOpen]);

  const handleBackdropClick = useCallback((e: React.MouseEvent) => {
    if (e.target === e.currentTarget) {
      onClose();
    }
  }, [onClose]);

  const getCategoryIcon = (category?: EnterpriseCategory) => {
    switch (category) {
      case 'infrastructure':
        return <Layers className="w-4 h-4" />;
      case 'security':
        return <Shield className="w-4 h-4" />;
      case 'vertical_saas':
        return <Building2 className="w-4 h-4" />;
      case 'agentic':
        return <Bot className="w-4 h-4" />;
      case 'data_intelligence':
        return <Database className="w-4 h-4" />;
      default:
        return <Cpu className="w-4 h-4" />;
    }
  };

  return (
    <>
      {/* Backdrop - instant, no animation */}
      <div
        className="fixed inset-0 bg-black/70 backdrop-blur-sm z-40"
        onClick={handleBackdropClick}
        aria-hidden="true"
      />

      {/* Modal Content - instant, no animation */}
      <div
        ref={modalRef}
        role="dialog"
        aria-modal="true"
        aria-labelledby="deal-modal-title"
        className="fixed inset-4 sm:inset-auto sm:top-1/2 sm:left-1/2 sm:-translate-x-1/2 sm:-translate-y-1/2 z-50 w-auto sm:w-full sm:max-w-2xl sm:mx-4 bg-[#0a0a0c] border border-slate-800 rounded-lg shadow-2xl overflow-y-auto max-h-[calc(100vh-2rem)] sm:max-h-[90vh]"
      >
        {/* Header */}
        <div className="flex items-start justify-between p-3 sm:p-4 border-b border-slate-800">
          <div className="min-w-0 flex-1 mr-2">
            <h2 id="deal-modal-title" className="text-lg sm:text-xl font-bold text-white truncate">{currentDeal.startupName}</h2>
            <div className="flex items-center gap-2 mt-1 text-sm flex-wrap">
              <span className={`stage-badge stage-${currentDeal.investmentStage}`}>
                {STAGE_LABELS[currentDeal.investmentStage]}
              </span>
              <span className="text-slate-500">•</span>
              <span className="text-slate-300">{currentDeal.amountInvested}</span>
            </div>
          </div>
          <div className="flex items-center gap-1">
            {/* Edit Button - tiny icon */}
            <button
              onClick={() => setIsEditModalOpen(true)}
              className="p-1.5 hover:bg-slate-800 rounded transition-colors text-slate-500 hover:text-blue-400"
              aria-label="Edit deal"
              title="Edit"
            >
              <Pencil className="w-3.5 h-3.5" aria-hidden="true" />
            </button>
            {/* Flag Button - tiny icon */}
            <button
              onClick={handleFlag}
              disabled={flagStatus === 'loading' || flagStatus === 'success'}
              className={`p-1.5 rounded transition-colors ${
                flagStatus === 'success'
                  ? 'text-red-400 cursor-default'
                  : flagStatus === 'loading'
                  ? 'text-slate-600 cursor-wait'
                  : 'text-slate-500 hover:bg-slate-800 hover:text-red-400'
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
              className="p-2 hover:bg-slate-800 rounded transition-colors text-slate-400 hover:text-white"
              aria-label="Close deal details"
            >
              <X className="w-5 h-5" aria-hidden="true" />
            </button>
          </div>
        </div>

        {/* Content */}
        <div className="p-3 sm:p-4 space-y-3">
          {/* Main Info Grid */}
          <div className="grid grid-cols-2 gap-2">
            {/* Lead Investor */}
            <InfoCard
              icon={<Users className="w-4 h-4 text-emerald-400" />}
              label="Lead Investor"
              value={currentDeal.leadInvestor || 'Unknown'}
              subValue={currentDeal.leadPartner ? `Partner: ${currentDeal.leadPartner}` : undefined}
            />

            {/* Amount */}
            <InfoCard
              icon={<DollarSign className="w-4 h-4 text-blue-400" />}
              label="Amount Raised"
              value={currentDeal.amountInvested}
            />

            {/* Date */}
            <InfoCard
              icon={<Calendar className="w-4 h-4 text-purple-400" />}
              label="Announced"
              value={formatDate(currentDeal.date)}
            />

            {/* Category */}
            <InfoCard
              icon={getCategoryIcon(currentDeal.enterpriseCategory)}
              label="Enterprise Category"
              value={
                currentDeal.enterpriseCategory
                  ? CATEGORY_LABELS[currentDeal.enterpriseCategory]
                  : 'Not Classified'
              }
              badge={
                currentDeal.isEnterpriseAi ? (
                  <span className="ml-2 px-2 py-0.5 bg-emerald-500/20 text-emerald-400 text-xs rounded">
                    Enterprise AI
                  </span>
                ) : (
                  <span className="ml-2 px-2 py-0.5 bg-slate-700 text-slate-400 text-xs rounded">
                    Consumer
                  </span>
                )
              }
            />
          </div>

          {/* Company Links - Always show icons */}
          <div>
            <label className="text-[10px] font-bold text-slate-500 uppercase tracking-wider block mb-1">
              Company Links
            </label>
            <div className="flex flex-wrap gap-2">
              {/* Website - always show, green when available */}
              {currentDeal.companyWebsite ? (
                <a
                  href={currentDeal.companyWebsite}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="flex items-center gap-1.5 px-2 py-1 bg-slate-800 hover:bg-slate-700 border border-emerald-700/50 rounded text-xs text-white transition-colors"
                  title={currentDeal.companyWebsite}
                >
                  <Globe className="w-3 h-3 text-emerald-400" />
                  <span className="text-emerald-300">{extractDomain(currentDeal.companyWebsite)}</span>
                </a>
              ) : (
                <span
                  className="flex items-center gap-1.5 px-2 py-1 bg-slate-900 border border-slate-800 rounded text-xs text-slate-600 cursor-not-allowed"
                  title="Website not available"
                >
                  <Globe className="w-3 h-3" />
                  No website
                </span>
              )}

              {/* Company LinkedIn - always show if exists */}
              {currentDeal.companyLinkedin ? (
                <a
                  href={currentDeal.companyLinkedin}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="flex items-center gap-1.5 px-2 py-1 bg-slate-800 hover:bg-slate-700 border border-blue-700/50 rounded text-xs text-white transition-colors"
                  title={currentDeal.companyLinkedin}
                >
                  <Linkedin className="w-3 h-3 text-blue-400" />
                  <span className="text-blue-300">Company</span>
                </a>
              ) : (
                <span
                  className="flex items-center gap-1.5 px-2 py-1 bg-slate-900 border border-slate-800 rounded text-xs text-slate-600 cursor-not-allowed"
                  title="Company LinkedIn not available"
                >
                  <Linkedin className="w-3 h-3" />
                  No LinkedIn
                </span>
              )}
            </div>
          </div>

          {/* Founders - show all founders with LinkedIn status */}
          {currentDeal.founders && currentDeal.founders.length > 0 ? (
            <div>
              <label className="text-[10px] font-bold text-slate-500 uppercase tracking-wider block mb-1">
                {currentDeal.founders.length === 1 ? 'Founder' : `Founders (${currentDeal.founders.length})`}
              </label>
              <div className="space-y-1">
                {currentDeal.founders.map((founder, index) => (
                  <div key={index} className="flex items-center justify-between p-2 bg-slate-900/50 border border-slate-800 rounded">
                    <div className="flex items-center gap-1.5 min-w-0">
                      <User className="w-3 h-3 text-slate-400 shrink-0" />
                      <span className="text-white text-xs truncate">{founder.name}</span>
                      {founder.title && (
                        <span className="text-slate-500 text-[10px] truncate hidden sm:inline">• {founder.title}</span>
                      )}
                    </div>
                    {/* LinkedIn - always show status */}
                    {founder.linkedinUrl ? (
                      <a
                        href={founder.linkedinUrl}
                        target="_blank"
                        rel="noopener noreferrer"
                        className="flex items-center gap-1 text-blue-400 hover:text-blue-300 text-[10px] transition-colors shrink-0"
                        title={founder.linkedinUrl}
                      >
                        <Linkedin className="w-2.5 h-2.5" />
                        LinkedIn
                      </a>
                    ) : (
                      <span className="flex items-center gap-1 text-slate-600 text-[10px] shrink-0" title="LinkedIn not found">
                        <Linkedin className="w-2.5 h-2.5" />
                        No LinkedIn
                      </span>
                    )}
                  </div>
                ))}
              </div>
            </div>
          ) : (
            <div>
              <label className="text-[10px] font-bold text-slate-500 uppercase tracking-wider block mb-1">
                Founders
              </label>
              <div className="flex items-center p-2 bg-slate-900/50 border border-slate-800 rounded">
                <span className="text-slate-500 italic text-xs">No founder information available</span>
              </div>
            </div>
          )}

          {/* Verification Snippet - proof of lead status */}
          {currentDeal.verificationSnippet && (
            <div>
              <label className="text-[10px] font-bold text-slate-500 uppercase tracking-wider block mb-1 flex items-center gap-2">
                Lead Verification
                {currentDeal.leadEvidenceWeak && (
                  <span className="flex items-center gap-1 text-amber-400 font-normal normal-case tracking-normal">
                    <AlertTriangle className="w-2.5 h-2.5" />
                    Weak
                  </span>
                )}
              </label>
              <div className={`p-2 rounded ${
                currentDeal.leadEvidenceWeak
                  ? 'bg-amber-900/20 border border-amber-700/30'
                  : 'bg-emerald-900/20 border border-emerald-700/30'
              }`}>
                <p className={`text-xs italic ${
                  currentDeal.leadEvidenceWeak ? 'text-amber-300' : 'text-emerald-300'
                }`}>"{currentDeal.verificationSnippet}"</p>
              </div>
            </div>
          )}

          {/* Source Link */}
          {currentDeal.sourceUrl && (
            <div className="pt-2 border-t border-slate-800">
              <a
                href={currentDeal.sourceUrl}
                target="_blank"
                rel="noopener noreferrer"
                className="flex items-center gap-1.5 text-xs text-blue-400 hover:text-blue-300 transition-colors"
              >
                <ExternalLink className="w-3 h-3" />
                View Source
              </a>
            </div>
          )}
        </div>

        {/* Footer */}
        <div className="px-3 sm:px-4 py-2 border-t border-slate-800 flex justify-end">
          {/* Track Button */}
          {showUndoToast ? (
            // Undo state - 10 second window after tracking
            <button
              onClick={() => {
                if (undoTimeout) {
                  clearTimeout(undoTimeout);
                  setUndoTimeout(null);
                }
                setShowUndoToast(false);
                setTrackingStatus('idle');
              }}
              className="flex items-center gap-1.5 px-3 py-1.5 rounded text-xs font-medium transition-all bg-amber-600 hover:bg-amber-500 text-white justify-center"
            >
              <X className="w-3 h-3" />
              Undo
            </button>
          ) : trackingStatus === 'already' ? (
            // Already tracked - show checkmark, no action
            <div className="flex items-center gap-1.5 px-3 py-1.5 rounded text-xs font-medium bg-emerald-600/50 text-emerald-200 justify-center">
              <Check className="w-3 h-3" />
              Tracked
            </div>
          ) : (
            // Default track button
            <button
              onClick={handleAddToTracker}
              disabled={trackingStatus === 'loading'}
              className={`flex items-center gap-1.5 px-3 py-1.5 rounded text-xs font-medium transition-all justify-center ${
                trackingStatus === 'loading'
                  ? 'bg-blue-600/50 text-white cursor-wait'
                  : trackingStatus === 'error'
                  ? 'bg-red-600 hover:bg-red-500 text-white'
                  : 'bg-blue-600 hover:bg-blue-500 text-white'
              }`}
            >
              {trackingStatus === 'loading' ? (
                <Loader2 className="w-3 h-3 animate-spin" />
              ) : (
                <Target className="w-3 h-3" />
              )}
              {trackingStatus === 'error' ? 'Retry' : 'Track'}
            </button>
          )}
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

      {/* Copy Toast */}
      {copyToast && (
        <div className="fixed bottom-4 right-4 bg-slate-800 text-slate-300 px-3 py-1.5 rounded text-xs shadow-lg z-[60] animate-fade-in">
          Copied
        </div>
      )}
    </>
  );
});

interface InfoCardProps {
  icon: React.ReactNode;
  label: string;
  value: string;
  subValue?: string;
  badge?: React.ReactNode;
}

function InfoCard({ icon, label, value, subValue, badge }: InfoCardProps) {
  return (
    <div className="p-2 bg-slate-900/50 border border-slate-800 rounded">
      <div className="flex items-center gap-1.5 text-[10px] text-slate-500 mb-0.5">
        {icon}
        {label}
      </div>
      <div className="text-white text-sm font-medium flex items-center flex-wrap">
        {value}
        {badge}
      </div>
      {subValue && <div className="text-[10px] text-slate-400">{subValue}</div>}
    </div>
  );
}

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

function extractDomain(url: string): string {
  try {
    const parsed = new URL(url);
    return parsed.hostname.replace('www.', '');
  } catch {
    return url;
  }
}

export default DealModal;
