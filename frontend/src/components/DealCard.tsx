/**
 * DealCard - Mobile-friendly card view for deals
 *
 * Displays deal information in a compact card format for mobile devices.
 * Used in DealsTable as an alternative to the table layout on small screens.
 */

import React, { memo } from 'react';
import {
  Globe,
  Linkedin,
  FileText,
  Link,
  Cpu,
  Shield,
  Building2,
  Bot,
  Database,
  XCircle,
} from 'lucide-react';
import type { Deal, EnterpriseCategory, InvestmentStage } from '../types';
import { STAGE_LABELS } from '../types';

interface DealCardProps {
  deal: Deal;
  onClick: () => void;
  showRejected?: boolean;
}

export const DealCard = memo(function DealCard({ deal, onClick, showRejected = false }: DealCardProps) {
  const isRejected = !deal.investorRoles.includes('lead');
  const shouldDim = isRejected && !showRejected;

  const handleLinkClick = (e: React.MouseEvent, url: string) => {
    e.stopPropagation();
    window.open(url, '_blank', 'noopener,noreferrer');
  };

  const ceoLinkedin = deal.founders?.find(f => f.linkedinUrl)?.linkedinUrl;

  return (
    <div
      onClick={onClick}
      className={`bg-slate-900/50 border border-slate-800 rounded-lg p-4 cursor-pointer active:bg-slate-800/50 transition-colors ${
        shouldDim ? 'opacity-60' : ''
      }`}
    >
      {/* Top row: Company name + Amount */}
      <div className="flex items-start justify-between gap-2 mb-2">
        <div className="min-w-0 flex-1">
          <h3 className={`text-base font-bold truncate ${
            isRejected ? 'text-slate-400' : 'text-emerald-400'
          }`}>
            {deal.startupName}
          </h3>
          <div className="text-xs text-slate-500 flex items-center gap-1 mt-0.5">
            <SourceIcon source={deal.sourceName} />
            <span className="truncate">{deal.sourceName || 'Unknown Source'}</span>
          </div>
        </div>
        <div className="text-right shrink-0">
          <div className="text-sm font-bold text-white">{deal.amountInvested?.replace(/\bmillion\b/gi, 'M').replace(/\bbillion\b/gi, 'B').replace(/(\d)\s+(M|B)\b/g, '$1$2')}</div>
          <div className="text-xs text-slate-500">{formatDate(deal.date)}</div>
        </div>
      </div>

      {/* Middle row: Stage + Lead Investor */}
      <div className="flex items-center gap-2 mb-3">
        <StageBadge stage={deal.investmentStage} isRejected={isRejected} />
        <span className="text-xs text-slate-400 truncate">
          {deal.leadInvestor || 'Unknown'}
        </span>
      </div>

      {/* Bottom row: Category + Links */}
      <div className="flex items-center justify-between">
        <CategoryBadge category={deal.enterpriseCategory} shouldDim={shouldDim} />

        <div className="flex items-center gap-2">
          {/* Website */}
          {deal.companyWebsite && (
            <button
              onClick={(e) => handleLinkClick(e, deal.companyWebsite!)}
              className="p-2 rounded-lg bg-slate-800 text-emerald-400 active:bg-slate-700"
              title="Visit website"
            >
              <Globe className="w-4 h-4" />
            </button>
          )}

          {/* LinkedIn */}
          {ceoLinkedin && (
            <button
              onClick={(e) => handleLinkClick(e, ceoLinkedin)}
              className="p-2 rounded-lg bg-slate-800 text-blue-400 active:bg-slate-700"
              title="CEO LinkedIn"
            >
              <Linkedin className="w-4 h-4" />
            </button>
          )}
        </div>
      </div>
    </div>
  );
});

function SourceIcon({ source }: { source?: string }) {
  if (!source) return <Link className="w-3 h-3" />;

  const lowerSource = source.toLowerCase();
  if (lowerSource.includes('sec') || lowerSource.includes('form d')) {
    return <FileText className="w-3 h-3" />;
  }
  if (lowerSource.includes('blog') || lowerSource.includes('sequoia') || lowerSource.includes('a16z')) {
    return <Globe className="w-3 h-3" />;
  }
  return <Link className="w-3 h-3" />;
}

function StageBadge({ stage, isRejected }: { stage: InvestmentStage; isRejected: boolean }) {
  if (isRejected) {
    return (
      <span className="stage-badge bg-slate-800 text-slate-400 border-slate-700 text-[10px]">REJECTED</span>
    );
  }

  return (
    <span className={`stage-badge stage-${stage} text-[10px]`}>
      {STAGE_LABELS[stage]?.toUpperCase() || stage.toUpperCase()}
    </span>
  );
}

const CATEGORY_ICONS: Record<EnterpriseCategory, React.ReactNode> = {
  infrastructure: <Cpu className="w-3 h-3 text-slate-400" />,
  security: <Shield className="w-3 h-3 text-slate-400" />,
  vertical_saas: <Building2 className="w-3 h-3 text-slate-400" />,
  agentic: <Bot className="w-3 h-3 text-slate-400" />,
  data_intelligence: <Database className="w-3 h-3 text-slate-400" />,
  consumer_ai: <Bot className="w-3 h-3 text-blue-400" />,
  gaming_ai: <Bot className="w-3 h-3 text-purple-400" />,
  social_ai: <Bot className="w-3 h-3 text-pink-400" />,
  not_ai: <XCircle className="w-3 h-3 text-red-900" />,
};

function CategoryBadge({
  category,
  shouldDim,
}: {
  category?: EnterpriseCategory;
  shouldDim: boolean;
}) {
  const cat = category || 'not_ai';
  const icon = CATEGORY_ICONS[cat] || CATEGORY_ICONS['not_ai'];
  const isConsumerAi = cat === 'consumer_ai' || cat === 'gaming_ai' || cat === 'social_ai';

  if (cat === 'not_ai') {
    return (
      <div className="category-badge category-not_ai text-[10px]">
        {icon}
        not ai
      </div>
    );
  }

  if (isConsumerAi) {
    return (
      <div className={`category-badge category-consumer text-[10px] ${shouldDim ? 'opacity-75' : ''}`}>
        {icon}
        {cat.replace('_', ' ')}
      </div>
    );
  }

  return (
    <div className={`category-badge category-${cat} text-[10px] ${shouldDim ? 'opacity-75' : ''}`}>
      {icon}
      {cat.replace('_', ' ')}
    </div>
  );
}

function formatDate(dateString: string): string {
  if (!dateString) return 'Unknown';

  const parts = dateString.split('T')[0].split('-');
  let date: Date;
  if (parts.length >= 3) {
    const year = parseInt(parts[0], 10);
    const month = parseInt(parts[1], 10) - 1;
    const day = parseInt(parts[2], 10);
    date = new Date(year, month, day);
  } else {
    date = new Date(dateString);
  }

  const now = new Date();
  const diffMs = now.getTime() - date.getTime();
  const diffHours = Math.floor(diffMs / (1000 * 60 * 60));

  if (diffHours < 24) {
    return 'Today';
  }
  if (diffHours < 48) {
    return 'Yesterday';
  }
  return date.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
}

export default DealCard;
