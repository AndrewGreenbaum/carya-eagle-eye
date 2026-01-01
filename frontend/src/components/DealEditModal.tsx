/**
 * DealEditModal - Edit a deal's details
 *
 * Allows editing company info, deal details, and founders
 */

import { useState, useEffect, useRef, useCallback } from 'react';
import {
  X,
  Save,
  Loader2,
  AlertCircle,
  Plus,
  Trash2,
} from 'lucide-react';
import type { Deal, EnterpriseCategory, InvestmentStage, Founder } from '../types';
import { STAGE_LABELS, CATEGORY_LABELS } from '../types';
import { updateDeal, type UpdateDealRequest } from '../api/deals';

interface DealEditModalProps {
  deal: Deal;
  onClose: () => void;
  onSave: (updatedDeal: Deal) => void;
}

const ENTERPRISE_CATEGORIES: EnterpriseCategory[] = [
  'infrastructure',
  'security',
  'vertical_saas',
  'agentic',
  'data_intelligence',
  'consumer_ai',
  'gaming_ai',
  'social_ai',
];

const ROUND_TYPES: InvestmentStage[] = [
  'pre_seed',
  'seed',
  'series_a',
  'series_b',
  'series_c',
  'series_d',
  'growth',
  'exit',
  'unknown',
];

export function DealEditModal({ deal, onClose, onSave }: DealEditModalProps) {
  const modalRef = useRef<HTMLDivElement>(null);
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Form state
  const [companyName, setCompanyName] = useState(deal.startupName);
  const [website, setWebsite] = useState(deal.companyWebsite || '');
  const [linkedinUrl, setLinkedinUrl] = useState(deal.companyLinkedin || '');
  const [roundType, setRoundType] = useState(deal.investmentStage);
  const [amount, setAmount] = useState(deal.amountInvested || '');
  const [announcedDate, setAnnouncedDate] = useState(deal.date || '');
  const [leadPartnerName, setLeadPartnerName] = useState(deal.leadPartner || '');
  const [enterpriseCategory, setEnterpriseCategory] = useState<EnterpriseCategory | ''>(
    deal.enterpriseCategory || ''
  );
  const [isEnterpriseAi, setIsEnterpriseAi] = useState(deal.isEnterpriseAi);
  const [founders, setFounders] = useState<Founder[]>(deal.founders || []);

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

  const handleBackdropClick = useCallback(
    (e: React.MouseEvent) => {
      if (e.target === e.currentTarget) {
        onClose();
      }
    },
    [onClose]
  );

  const handleAddFounder = () => {
    setFounders([...founders, { name: '', title: '', linkedinUrl: '' }]);
  };

  const handleRemoveFounder = (index: number) => {
    setFounders(founders.filter((_, i) => i !== index));
  };

  const handleFounderChange = (
    index: number,
    field: keyof Founder,
    value: string
  ) => {
    const updated = [...founders];
    updated[index] = { ...updated[index], [field]: value };
    setFounders(updated);
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setIsSubmitting(true);
    setError(null);

    try {
      const updates: UpdateDealRequest = {
        companyName: companyName !== deal.startupName ? companyName : undefined,
        // For clearable fields, send empty string to clear, undefined to skip
        website: website !== (deal.companyWebsite || '') ? (website || '') : undefined,
        linkedinUrl: linkedinUrl !== (deal.companyLinkedin || '') ? (linkedinUrl || '') : undefined,
        roundType: roundType !== deal.investmentStage ? roundType : undefined,
        amount: amount !== deal.amountInvested ? amount : undefined,
        announcedDate: announcedDate !== deal.date ? (announcedDate || '') : undefined,
        leadPartnerName: leadPartnerName !== (deal.leadPartner || '') ? (leadPartnerName || '') : undefined,
        enterpriseCategory: enterpriseCategory !== (deal.enterpriseCategory || '')
          ? (enterpriseCategory || '')
          : undefined,
        isEnterpriseAi: isEnterpriseAi !== deal.isEnterpriseAi ? isEnterpriseAi : undefined,
        founders: JSON.stringify(founders) !== JSON.stringify(deal.founders)
          ? founders.filter(f => f.name.trim()).map(f => ({
              name: f.name,
              title: f.title || undefined,
              linkedinUrl: f.linkedinUrl || undefined,
            }))
          : undefined,
      };

      // Only submit if there are actual changes
      const hasChanges = Object.values(updates).some(v => v !== undefined);
      if (!hasChanges) {
        onClose();
        return;
      }

      const updatedDeal = await updateDeal(deal.id, updates);
      onSave(updatedDeal);
      onClose();
    } catch (err) {
      console.error('Failed to update deal:', err);
      setError(err instanceof Error ? err.message : 'Failed to update deal');
    } finally {
      setIsSubmitting(false);
    }
  };

  return (
    <>
      {/* Backdrop */}
      <div
        className="fixed inset-0 bg-black/70 backdrop-blur-sm z-50"
        onClick={handleBackdropClick}
        aria-hidden="true"
      />

      {/* Modal Content */}
      <div
        ref={modalRef}
        role="dialog"
        aria-modal="true"
        aria-labelledby="edit-modal-title"
        className="fixed top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 z-50 w-full max-w-2xl mx-4 bg-[#0a0a0c] border border-slate-800 rounded-lg shadow-2xl max-h-[90vh] overflow-hidden flex flex-col"
      >
        {/* Header */}
        <div className="flex items-center justify-between p-6 border-b border-slate-800 shrink-0">
          <h2 id="edit-modal-title" className="text-xl font-bold text-white">
            Edit Deal
          </h2>
          <button
            onClick={onClose}
            className="p-2 hover:bg-slate-800 rounded transition-colors text-slate-400 hover:text-white"
            aria-label="Close"
          >
            <X className="w-5 h-5" />
          </button>
        </div>

        {/* Form */}
        <form onSubmit={handleSubmit} className="flex-1 overflow-y-auto">
          <div className="p-6 space-y-6">
            {/* Error */}
            {error && (
              <div className="flex items-center gap-2 p-3 bg-red-500/10 border border-red-500/30 rounded text-red-400 text-sm">
                <AlertCircle className="w-4 h-4 shrink-0" />
                {error}
              </div>
            )}

            {/* Company Info Section */}
            <div>
              <h3 className="text-sm font-bold text-slate-400 uppercase tracking-wider mb-4">
                Company Information
              </h3>
              <div className="grid grid-cols-1 gap-4">
                <div>
                  <label className="block text-sm font-medium text-slate-300 mb-1">
                    Company Name *
                  </label>
                  <input
                    type="text"
                    value={companyName}
                    onChange={(e) => setCompanyName(e.target.value)}
                    required
                    className="w-full px-3 py-2 bg-slate-800 border border-slate-700 rounded text-white placeholder-slate-500 focus:outline-none focus:ring-2 focus:ring-blue-500/50 focus:border-blue-500"
                  />
                </div>
                <div className="grid grid-cols-2 gap-4">
                  <div>
                    <label className="block text-sm font-medium text-slate-300 mb-1">
                      Website
                    </label>
                    <input
                      type="url"
                      value={website}
                      onChange={(e) => setWebsite(e.target.value)}
                      placeholder="https://example.com"
                      className="w-full px-3 py-2 bg-slate-800 border border-slate-700 rounded text-white placeholder-slate-500 focus:outline-none focus:ring-2 focus:ring-blue-500/50 focus:border-blue-500"
                    />
                  </div>
                  <div>
                    <label className="block text-sm font-medium text-slate-300 mb-1">
                      Company LinkedIn
                    </label>
                    <input
                      type="url"
                      value={linkedinUrl}
                      onChange={(e) => setLinkedinUrl(e.target.value)}
                      placeholder="https://linkedin.com/company/..."
                      className="w-full px-3 py-2 bg-slate-800 border border-slate-700 rounded text-white placeholder-slate-500 focus:outline-none focus:ring-2 focus:ring-blue-500/50 focus:border-blue-500"
                    />
                  </div>
                </div>
              </div>
            </div>

            {/* Deal Info Section */}
            <div>
              <h3 className="text-sm font-bold text-slate-400 uppercase tracking-wider mb-4">
                Deal Information
              </h3>
              <div className="grid grid-cols-2 gap-4">
                <div>
                  <label className="block text-sm font-medium text-slate-300 mb-1">
                    Round Type
                  </label>
                  <select
                    value={roundType}
                    onChange={(e) => setRoundType(e.target.value as InvestmentStage)}
                    className="w-full px-3 py-2 bg-slate-800 border border-slate-700 rounded text-white focus:outline-none focus:ring-2 focus:ring-blue-500/50 focus:border-blue-500"
                  >
                    {ROUND_TYPES.map((rt) => (
                      <option key={rt} value={rt}>
                        {STAGE_LABELS[rt]}
                      </option>
                    ))}
                  </select>
                </div>
                <div>
                  <label className="block text-sm font-medium text-slate-300 mb-1">
                    Amount
                  </label>
                  <input
                    type="text"
                    value={amount}
                    onChange={(e) => setAmount(e.target.value)}
                    placeholder="$10M"
                    className="w-full px-3 py-2 bg-slate-800 border border-slate-700 rounded text-white placeholder-slate-500 focus:outline-none focus:ring-2 focus:ring-blue-500/50 focus:border-blue-500"
                  />
                </div>
                <div>
                  <label className="block text-sm font-medium text-slate-300 mb-1">
                    Announced Date
                  </label>
                  <input
                    type="date"
                    value={announcedDate}
                    onChange={(e) => setAnnouncedDate(e.target.value)}
                    className="w-full px-3 py-2 bg-slate-800 border border-slate-700 rounded text-white focus:outline-none focus:ring-2 focus:ring-blue-500/50 focus:border-blue-500"
                  />
                </div>
                <div>
                  <label className="block text-sm font-medium text-slate-300 mb-1">
                    Lead Partner
                  </label>
                  <input
                    type="text"
                    value={leadPartnerName}
                    onChange={(e) => setLeadPartnerName(e.target.value)}
                    placeholder="Partner name"
                    className="w-full px-3 py-2 bg-slate-800 border border-slate-700 rounded text-white placeholder-slate-500 focus:outline-none focus:ring-2 focus:ring-blue-500/50 focus:border-blue-500"
                  />
                </div>
              </div>
            </div>

            {/* Classification Section */}
            <div>
              <h3 className="text-sm font-bold text-slate-400 uppercase tracking-wider mb-4">
                Classification
              </h3>
              <div className="grid grid-cols-2 gap-4">
                <div>
                  <label className="block text-sm font-medium text-slate-300 mb-1">
                    Enterprise Category
                  </label>
                  <select
                    value={enterpriseCategory}
                    onChange={(e) =>
                      setEnterpriseCategory(e.target.value as EnterpriseCategory | '')
                    }
                    className="w-full px-3 py-2 bg-slate-800 border border-slate-700 rounded text-white focus:outline-none focus:ring-2 focus:ring-blue-500/50 focus:border-blue-500"
                  >
                    <option value="">Not Classified</option>
                    {ENTERPRISE_CATEGORIES.map((cat) => (
                      <option key={cat} value={cat}>
                        {CATEGORY_LABELS[cat]}
                      </option>
                    ))}
                  </select>
                </div>
                <div className="flex items-center">
                  <label className="flex items-center gap-2 cursor-pointer">
                    <input
                      type="checkbox"
                      checked={isEnterpriseAi}
                      onChange={(e) => setIsEnterpriseAi(e.target.checked)}
                      className="w-4 h-4 rounded bg-slate-800 border-slate-700 text-emerald-500 focus:ring-emerald-500/50"
                    />
                    <span className="text-sm font-medium text-slate-300">
                      Enterprise AI
                    </span>
                  </label>
                </div>
              </div>
            </div>

            {/* Founders Section */}
            <div>
              <div className="flex items-center justify-between mb-4">
                <h3 className="text-sm font-bold text-slate-400 uppercase tracking-wider">
                  Founders
                </h3>
                <button
                  type="button"
                  onClick={handleAddFounder}
                  className="flex items-center gap-1 px-2 py-1 text-xs text-blue-400 hover:text-blue-300 transition-colors"
                >
                  <Plus className="w-3 h-3" />
                  Add Founder
                </button>
              </div>
              {founders.length === 0 ? (
                <p className="text-sm text-slate-500 italic">No founders added</p>
              ) : (
                <div className="space-y-3">
                  {founders.map((founder, index) => (
                    <div
                      key={index}
                      className="p-3 bg-slate-900/50 border border-slate-800 rounded"
                    >
                      <div className="grid grid-cols-12 gap-2">
                        <div className="col-span-4">
                          <input
                            type="text"
                            value={founder.name}
                            onChange={(e) =>
                              handleFounderChange(index, 'name', e.target.value)
                            }
                            placeholder="Name"
                            className="w-full px-2 py-1.5 bg-slate-800 border border-slate-700 rounded text-sm text-white placeholder-slate-500 focus:outline-none focus:ring-1 focus:ring-blue-500/50"
                          />
                        </div>
                        <div className="col-span-3">
                          <input
                            type="text"
                            value={founder.title || ''}
                            onChange={(e) =>
                              handleFounderChange(index, 'title', e.target.value)
                            }
                            placeholder="Title"
                            className="w-full px-2 py-1.5 bg-slate-800 border border-slate-700 rounded text-sm text-white placeholder-slate-500 focus:outline-none focus:ring-1 focus:ring-blue-500/50"
                          />
                        </div>
                        <div className="col-span-4">
                          <input
                            type="url"
                            value={founder.linkedinUrl || ''}
                            onChange={(e) =>
                              handleFounderChange(index, 'linkedinUrl', e.target.value)
                            }
                            placeholder="LinkedIn URL"
                            className="w-full px-2 py-1.5 bg-slate-800 border border-slate-700 rounded text-sm text-white placeholder-slate-500 focus:outline-none focus:ring-1 focus:ring-blue-500/50"
                          />
                        </div>
                        <div className="col-span-1 flex items-center justify-center">
                          <button
                            type="button"
                            onClick={() => handleRemoveFounder(index)}
                            className="p-1 text-slate-500 hover:text-red-400 transition-colors"
                            title="Remove founder"
                          >
                            <Trash2 className="w-4 h-4" />
                          </button>
                        </div>
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </div>
          </div>

          {/* Footer */}
          <div className="px-6 py-4 border-t border-slate-800 flex justify-end gap-3 shrink-0">
            <button
              type="button"
              onClick={onClose}
              disabled={isSubmitting}
              className="px-4 py-2 text-slate-300 hover:text-white transition-colors"
            >
              Cancel
            </button>
            <button
              type="submit"
              disabled={isSubmitting}
              className="flex items-center gap-2 px-4 py-2 bg-blue-600 hover:bg-blue-500 disabled:bg-slate-700 disabled:text-slate-500 text-white rounded font-medium transition-colors"
            >
              {isSubmitting ? (
                <Loader2 className="w-4 h-4 animate-spin" />
              ) : (
                <Save className="w-4 h-4" />
              )}
              {isSubmitting ? 'Saving...' : 'Save Changes'}
            </button>
          </div>
        </form>
      </div>
    </>
  );
}

export default DealEditModal;
