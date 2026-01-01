/**
 * MindMapDocs.tsx - Interactive Mind Map with Pan/Zoom
 *
 * Modern, sophisticated design with muted colors and full interactivity.
 * Access via /claude URL
 */

import { useState, useEffect, useCallback, useRef, MouseEvent, WheelEvent } from 'react';
import { RefreshCw, ArrowLeft, X, ZoomIn, ZoomOut, Maximize2, Move } from 'lucide-react';

const API_BASE = import.meta.env.VITE_API_BASE || 'https://bud-tracker-backend-production.up.railway.app';

// Palantir/Anduril muted tactical color palette
const COLORS = {
  root: { bg: '#0f1a18', border: '#1e4d47', text: '#4a9089', glow: 'rgba(30, 77, 71, 0.2)' },
  pipeline: { bg: '#0f1318', border: '#1e3a5f', text: '#4a7a9f', glow: 'rgba(30, 58, 95, 0.15)' },
  funds: { bg: '#18140f', border: '#4a3d2a', text: '#8a7a5a', glow: 'rgba(74, 61, 42, 0.15)' },
  classify: { bg: '#18101a', border: '#4a2a4a', text: '#8a5a8a', glow: 'rgba(74, 42, 74, 0.15)' },
  enrich: { bg: '#0f1618', border: '#1e4a5f', text: '#4a8a9f', glow: 'rgba(30, 74, 95, 0.15)' },
  ui: { bg: '#101810', border: '#2a4a2a', text: '#5a8a5a', glow: 'rgba(42, 74, 42, 0.15)' },
  infra: { bg: '#14101a', border: '#3a2a5a', text: '#7a6a9a', glow: 'rgba(58, 42, 90, 0.15)' },
  stealth: { bg: '#121212', border: '#2a2a2a', text: '#6a6a6a', glow: 'rgba(42, 42, 42, 0.15)' },
  docs: { bg: '#0f1614', border: '#1e4a3a', text: '#4a8a7a', glow: 'rgba(30, 74, 58, 0.15)' },
  child: { bg: '#0c0c0e', border: '#252528', text: '#7a7a7f', glow: 'rgba(37, 37, 40, 0.1)' },
};

// Node structure
interface MindMapNode {
  id: string;
  label: string;
  content?: string;
  children?: MindMapNode[];
  colorKey: keyof typeof COLORS;
}

// Curated mind map structure
function createMindMapStructure(sections: Map<string, string>): MindMapNode {
  return {
    id: 'root',
    label: 'Carya Eagle Eye',
    colorKey: 'root',
    children: [
      {
        id: 'pipeline',
        label: 'Data Pipeline',
        colorKey: 'pipeline',
        children: [
          { id: 'scrapers', label: 'Scrapers', content: sections.get('data-sources'), colorKey: 'child' },
          { id: 'llm', label: 'LLM Extraction', content: sections.get('llm-pipeline'), colorKey: 'child' },
          { id: 'dedup', label: 'Deduplication', content: sections.get('deal-deduplication'), colorKey: 'child' },
          { id: 'scheduler', label: 'Scheduler', content: sections.get('scheduler-jobs-py-'), colorKey: 'child' },
        ]
      },
      {
        id: 'funds',
        label: '18 VC Funds',
        colorKey: 'funds',
        children: [
          { id: 'fund-list', label: 'Fund Registry', content: sections.get('the-18-tracked-funds'), colorKey: 'child' },
          { id: 'matching', label: 'Fund Matching', content: sections.get('fund-matching'), colorKey: 'child' },
          { id: 'brave', label: 'Brave Search', content: sections.get('brave-search-queries'), colorKey: 'child' },
          { id: 'gnews', label: 'Google News', content: sections.get('google-news-rss'), colorKey: 'child' },
        ]
      },
      {
        id: 'classify',
        label: 'Classification',
        colorKey: 'classify',
        children: [
          { id: 'enterprise', label: 'Enterprise AI', content: sections.get('classification-rules'), colorKey: 'child' },
          { id: 'lead', label: 'Lead Verification', content: sections.get('classification-rules'), colorKey: 'child' },
          { id: 'validation', label: 'URL Validation', content: sections.get('url-validation-schemas-py-'), colorKey: 'child' },
        ]
      },
      {
        id: 'enrich',
        label: 'Enrichment',
        colorKey: 'enrich',
        children: [
          { id: 'linkedin', label: 'LinkedIn', content: sections.get('linkedin-enrichment'), colorKey: 'child' },
          { id: 'brave-client', label: 'Brave Client', content: sections.get('enrichment-system'), colorKey: 'child' },
          { id: 'dates', label: 'Date Verification', content: sections.get('data-verification'), colorKey: 'child' },
        ]
      },
      {
        id: 'ui',
        label: 'Frontend',
        colorKey: 'ui',
        children: [
          { id: 'dashboard', label: 'Dashboard', content: sections.get('frontend-dashboard'), colorKey: 'child' },
          { id: 'api', label: 'API Endpoints', content: sections.get('key-api-endpoints'), colorKey: 'child' },
        ]
      },
      {
        id: 'infra',
        label: 'Infrastructure',
        colorKey: 'infra',
        children: [
          { id: 'db', label: 'Database', content: sections.get('database-schema'), colorKey: 'child' },
          { id: 'deploy', label: 'Deployment', content: sections.get('github-railway-cli'), colorKey: 'child' },
          { id: 'costs', label: 'API Costs', content: sections.get('api-costs'), colorKey: 'child' },
        ]
      },
      {
        id: 'stealth',
        label: 'Stealth Detection',
        colorKey: 'stealth',
        children: [
          { id: 'portfolio', label: 'Portfolio Diff', content: sections.get('portfolio-diff-scraper'), colorKey: 'child' },
          { id: 'delaware', label: 'Delaware Corps', content: sections.get('delaware-scraper'), colorKey: 'child' },
          { id: 'sec', label: 'SEC EDGAR', content: sections.get('data-sources'), colorKey: 'child' },
        ]
      },
      {
        id: 'docs',
        label: 'Reference',
        colorKey: 'docs',
        children: [
          { id: 'quick', label: 'Quick Reference', content: sections.get('quick-reference'), colorKey: 'child' },
          { id: 'arch', label: 'Architecture', content: sections.get('architecture'), colorKey: 'child' },
          { id: 'trouble', label: 'Troubleshooting', content: sections.get('troubleshooting'), colorKey: 'child' },
        ]
      },
    ]
  };
}

// Parse markdown into sections map
function parseMarkdownSections(content: string): Map<string, string> {
  const sections = new Map<string, string>();
  const lines = content.split('\n');
  let currentSection = '';
  let currentContent: string[] = [];
  let inCodeBlock = false;

  for (const line of lines) {
    if (line.startsWith('```')) {
      inCodeBlock = !inCodeBlock;
      currentContent.push(line);
      continue;
    }
    if (inCodeBlock) {
      currentContent.push(line);
      continue;
    }
    const h2Match = line.match(/^## (.+)$/);
    if (h2Match) {
      if (currentSection) sections.set(currentSection, currentContent.join('\n'));
      currentSection = h2Match[1].toLowerCase().replace(/[^a-z0-9]+/g, '-');
      currentContent = [];
    } else {
      currentContent.push(line);
    }
  }
  if (currentSection) sections.set(currentSection, currentContent.join('\n'));
  return sections;
}

// Render markdown content
function ContentRenderer({ content }: { content: string }) {
  const lines = content.split('\n');
  const elements: JSX.Element[] = [];
  let inCodeBlock = false;
  let codeLines: string[] = [];
  let inTable = false;
  let tableRows: string[][] = [];

  const flushCode = () => {
    if (codeLines.length > 0) {
      elements.push(
        <pre key={elements.length} className="bg-black/60 rounded-lg p-4 overflow-x-auto my-4 text-xs border border-zinc-800">
          <code className="text-emerald-400/90 font-mono">{codeLines.join('\n')}</code>
        </pre>
      );
      codeLines = [];
    }
  };

  const flushTable = () => {
    if (tableRows.length > 1) {
      const headers = tableRows[0];
      const body = tableRows.slice(2);
      elements.push(
        <div key={elements.length} className="overflow-x-auto my-4 rounded-lg border border-zinc-800">
          <table className="min-w-full text-xs">
            <thead>
              <tr className="bg-zinc-900/50">
                {headers.map((h, i) => (
                  <th key={i} className="px-3 py-2 text-left text-zinc-400 font-medium border-b border-zinc-800">
                    {h.trim()}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {body.map((row, i) => (
                <tr key={i} className="border-b border-zinc-800/50 hover:bg-zinc-800/20">
                  {row.map((cell, j) => (
                    <td key={j} className="px-3 py-2 text-zinc-500">{cell.trim()}</td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      );
      tableRows = [];
    }
  };

  for (const line of lines) {
    if (line.startsWith('```')) {
      if (inCodeBlock) { flushCode(); inCodeBlock = false; }
      else { flushTable(); inCodeBlock = true; }
      continue;
    }
    if (inCodeBlock) { codeLines.push(line); continue; }
    if (line.includes('|') && line.trim().startsWith('|')) {
      if (!inTable) inTable = true;
      tableRows.push(line.split('|').slice(1, -1));
      continue;
    } else if (inTable) { flushTable(); inTable = false; }
    if (!line.trim()) continue;
    if (line.startsWith('### ')) {
      elements.push(<h4 key={elements.length} className="text-sm font-semibold text-zinc-200 mt-5 mb-2">{line.slice(4)}</h4>);
      continue;
    }
    if (line.match(/^[-*] /)) {
      elements.push(<li key={elements.length} className="text-zinc-400 ml-4 my-1 text-sm list-disc">{line.slice(2)}</li>);
      continue;
    }
    elements.push(<p key={elements.length} className="text-zinc-400 my-2 text-sm leading-relaxed">{line}</p>);
  }
  flushCode();
  flushTable();
  return <div className="space-y-1">{elements}</div>;
}

// Main component
export function MindMapDocs() {
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [mindMap, setMindMap] = useState<MindMapNode | null>(null);
  const [expandedBranch, setExpandedBranch] = useState<string | null>(null);
  const [selectedNode, setSelectedNode] = useState<MindMapNode | null>(null);

  // Pan and zoom state
  const [zoom, setZoom] = useState(1);
  const [pan, setPan] = useState({ x: 0, y: 0 });
  const [isPanning, setIsPanning] = useState(false);
  const [panStart, setPanStart] = useState({ x: 0, y: 0 });
  const containerRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const fetchDocs = async () => {
      setLoading(true);
      try {
        const res = await fetch(`${API_BASE}/docs/claude`);
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data = await res.json();
        const sections = parseMarkdownSections(data.content);
        setMindMap(createMindMapStructure(sections));
        setError(null);
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Failed to load');
      } finally {
        setLoading(false);
      }
    };
    fetchDocs();
  }, []);

  // Zoom with mouse wheel
  const handleWheel = useCallback((e: WheelEvent) => {
    e.preventDefault();
    const delta = e.deltaY > 0 ? -0.1 : 0.1;
    setZoom(z => Math.max(0.3, Math.min(2.5, z + delta)));
  }, []);

  // Pan with mouse drag
  const handleMouseDown = useCallback((e: MouseEvent) => {
    if (e.button === 0) { // Left click
      setIsPanning(true);
      setPanStart({ x: e.clientX - pan.x, y: e.clientY - pan.y });
    }
  }, [pan]);

  const handleMouseMove = useCallback((e: MouseEvent) => {
    if (isPanning) {
      setPan({ x: e.clientX - panStart.x, y: e.clientY - panStart.y });
    }
  }, [isPanning, panStart]);

  const handleMouseUp = useCallback(() => {
    setIsPanning(false);
  }, []);

  const handleReset = useCallback(() => {
    setZoom(1);
    setPan({ x: 0, y: 0 });
  }, []);

  const handleBranchToggle = useCallback((branchId: string, e: MouseEvent) => {
    e.stopPropagation();
    setExpandedBranch(prev => prev === branchId ? null : branchId);
    setSelectedNode(null);
  }, []);

  const handleChildClick = useCallback((child: MindMapNode, e: MouseEvent) => {
    e.stopPropagation();
    setSelectedNode(child);
  }, []);

  if (loading) {
    return (
      <div className="min-h-screen bg-[#09090b] flex items-center justify-center">
        <RefreshCw className="w-8 h-8 text-zinc-600 animate-spin" />
      </div>
    );
  }

  return (
    <div className="h-screen bg-[#09090b] text-zinc-300 overflow-hidden flex">
      {/* Mind Map Canvas */}
      <div
        ref={containerRef}
        className={`flex-1 relative ${isPanning ? 'cursor-grabbing' : 'cursor-grab'}`}
        onWheel={handleWheel}
        onMouseDown={handleMouseDown}
        onMouseMove={handleMouseMove}
        onMouseUp={handleMouseUp}
        onMouseLeave={handleMouseUp}
      >
        {/* Subtle grid background */}
        <div
          className="absolute inset-0"
          style={{
            backgroundImage: `
              radial-gradient(circle at 50% 50%, rgba(39, 39, 42, 0.3) 0%, transparent 70%),
              linear-gradient(rgba(39, 39, 42, 0.15) 1px, transparent 1px),
              linear-gradient(90deg, rgba(39, 39, 42, 0.15) 1px, transparent 1px)
            `,
            backgroundSize: '100% 100%, 60px 60px, 60px 60px',
          }}
        />

        {/* Header */}
        <header className="absolute top-0 left-0 right-0 z-50 px-4 py-3 flex items-center justify-between">
          <a
            href="/"
            className="flex items-center gap-2 text-zinc-500 hover:text-zinc-300 transition-colors text-sm"
          >
            <ArrowLeft className="w-4 h-4" />
            Dashboard
          </a>

          {/* Zoom controls */}
          <div className="flex items-center gap-1 bg-zinc-900/80 backdrop-blur border border-zinc-800 rounded-lg p-1">
            <button
              onClick={() => setZoom(z => Math.max(0.3, z - 0.2))}
              className="p-1.5 hover:bg-zinc-800 rounded transition-colors"
              title="Zoom out"
            >
              <ZoomOut className="w-4 h-4 text-zinc-400" />
            </button>
            <span className="text-xs text-zinc-500 w-12 text-center font-mono">
              {Math.round(zoom * 100)}%
            </span>
            <button
              onClick={() => setZoom(z => Math.min(2.5, z + 0.2))}
              className="p-1.5 hover:bg-zinc-800 rounded transition-colors"
              title="Zoom in"
            >
              <ZoomIn className="w-4 h-4 text-zinc-400" />
            </button>
            <div className="w-px h-4 bg-zinc-700 mx-1" />
            <button
              onClick={handleReset}
              className="p-1.5 hover:bg-zinc-800 rounded transition-colors"
              title="Reset view"
            >
              <Maximize2 className="w-4 h-4 text-zinc-400" />
            </button>
          </div>

          <div className="flex items-center gap-2 text-xs text-zinc-600">
            <Move className="w-3 h-3" />
            <span>Drag to pan • Scroll to zoom</span>
          </div>
        </header>

        {/* Mind Map Content */}
        {error ? (
          <div className="absolute inset-0 flex items-center justify-center">
            <div className="bg-red-950/50 border border-red-900/50 rounded-lg p-4 text-red-400 text-sm">
              {error}
            </div>
          </div>
        ) : mindMap ? (
          <div
            className="absolute inset-0 flex items-center justify-center transition-transform duration-100"
            style={{
              transform: `translate(${pan.x}px, ${pan.y}px) scale(${zoom})`,
            }}
          >
            {/* Central node */}
            <div className="relative">
              {/* Center glow */}
              <div
                className="absolute inset-0 rounded-2xl blur-2xl"
                style={{ backgroundColor: COLORS.root.glow, transform: 'scale(2)' }}
              />

              <button
                onClick={(e) => { e.stopPropagation(); setExpandedBranch(null); setSelectedNode(null); }}
                className="relative px-8 py-4 rounded-2xl border-2 transition-all duration-300 hover:scale-105"
                style={{
                  backgroundColor: COLORS.root.bg,
                  borderColor: COLORS.root.border,
                  boxShadow: `0 0 40px ${COLORS.root.glow}`,
                }}
              >
                <span
                  className="font-semibold text-lg"
                  style={{ color: COLORS.root.text }}
                >
                  {mindMap.label}
                </span>
              </button>

              {/* Branch nodes */}
              {mindMap.children?.map((branch, i) => {
                const angle = (i / mindMap.children!.length) * Math.PI * 2 - Math.PI / 2;
                const radius = 420;
                const x = Math.cos(angle) * radius;
                const y = Math.sin(angle) * radius;
                const colors = COLORS[branch.colorKey];
                const isExpanded = expandedBranch === branch.id;

                return (
                  <div
                    key={branch.id}
                    className="absolute transition-all duration-500"
                    style={{
                      left: `calc(50% + ${x}px)`,
                      top: `calc(50% + ${y}px)`,
                      transform: 'translate(-50%, -50%)',
                    }}
                  >
                    {/* Branch glow when expanded */}
                    {isExpanded && (
                      <div
                        className="absolute inset-0 rounded-xl blur-xl -z-10"
                        style={{ backgroundColor: colors.glow, transform: 'scale(2.5)' }}
                      />
                    )}

                    <button
                      onClick={(e) => handleBranchToggle(branch.id, e)}
                      className={`
                        relative px-5 py-2.5 rounded-xl border transition-all duration-300
                        hover:scale-105 ${isExpanded ? 'scale-105' : ''}
                      `}
                      style={{
                        backgroundColor: colors.bg,
                        borderColor: isExpanded ? colors.border : `${colors.border}60`,
                        boxShadow: isExpanded ? `0 0 30px ${colors.glow}` : 'none',
                      }}
                    >
                      <span
                        className="font-medium text-sm whitespace-nowrap"
                        style={{ color: isExpanded ? colors.text : `${colors.text}cc` }}
                      >
                        {branch.label}
                      </span>
                    </button>

                    {/* Children */}
                    {isExpanded && branch.children && (
                      <>
                        {branch.children.map((child, ci) => {
                          const childAngle = angle + ((ci - (branch.children!.length - 1) / 2) * 0.4);
                          const childRadius = 190;
                          const cx = Math.cos(childAngle) * childRadius;
                          const cy = Math.sin(childAngle) * childRadius;
                          const isSelected = selectedNode?.id === child.id;
                          const childColors = COLORS[branch.colorKey];

                          return (
                            <button
                              key={child.id}
                              onClick={(e) => handleChildClick(child, e)}
                              className={`
                                absolute px-3 py-1.5 rounded-lg border transition-all duration-300
                                hover:scale-105 ${isSelected ? 'scale-105' : ''}
                              `}
                              style={{
                                left: `calc(50% + ${cx}px)`,
                                top: `calc(50% + ${cy}px)`,
                                transform: 'translate(-50%, -50%)',
                                backgroundColor: isSelected ? childColors.bg : '#111113',
                                borderColor: isSelected ? childColors.border : '#27272a',
                                boxShadow: isSelected ? `0 0 20px ${childColors.glow}` : 'none',
                              }}
                            >
                              <span
                                className="text-xs whitespace-nowrap"
                                style={{ color: isSelected ? childColors.text : '#a1a1aa' }}
                              >
                                {child.label}
                              </span>
                            </button>
                          );
                        })}

                        {/* Connection lines to children */}
                        <svg
                          className="absolute pointer-events-none -z-10"
                          style={{
                            left: '50%',
                            top: '50%',
                            width: '450px',
                            height: '450px',
                            transform: 'translate(-50%, -50%)',
                          }}
                        >
                          {branch.children.map((child, ci) => {
                            const childAngle = angle + ((ci - (branch.children!.length - 1) / 2) * 0.4);
                            const childRadius = 190;
                            const ex = 225 + Math.cos(childAngle) * childRadius;
                            const ey = 225 + Math.sin(childAngle) * childRadius;
                            const mx = 225 + Math.cos(childAngle) * (childRadius * 0.5);
                            const my = 225 + Math.sin(childAngle) * (childRadius * 0.5);

                            return (
                              <path
                                key={child.id}
                                d={`M 225,225 Q ${mx},${my} ${ex},${ey}`}
                                fill="none"
                                stroke={colors.border}
                                strokeWidth="1"
                                strokeOpacity="0.3"
                                strokeDasharray="4 4"
                              />
                            );
                          })}
                        </svg>
                      </>
                    )}
                  </div>
                );
              })}

              {/* Connection lines from center */}
              <svg
                className="absolute pointer-events-none -z-10"
                style={{
                  left: '50%',
                  top: '50%',
                  width: '1000px',
                  height: '1000px',
                  transform: 'translate(-50%, -50%)',
                }}
              >
                {mindMap.children?.map((branch, i) => {
                  const angle = (i / mindMap.children!.length) * Math.PI * 2 - Math.PI / 2;
                  const radius = 420;
                  const ex = 500 + Math.cos(angle) * radius;
                  const ey = 500 + Math.sin(angle) * radius;
                  const mx = 500 + Math.cos(angle) * (radius * 0.4);
                  const my = 500 + Math.sin(angle) * (radius * 0.4);
                  const colors = COLORS[branch.colorKey];
                  const isExpanded = expandedBranch === branch.id;

                  return (
                    <path
                      key={branch.id}
                      d={`M 500,500 Q ${mx},${my} ${ex},${ey}`}
                      fill="none"
                      stroke={isExpanded ? colors.border : '#27272a'}
                      strokeWidth={isExpanded ? 2 : 1}
                      strokeOpacity={isExpanded ? 0.6 : 0.3}
                      className="transition-all duration-300"
                    />
                  );
                })}
              </svg>
            </div>
          </div>
        ) : null}
      </div>

      {/* Detail Panel */}
      <div
        className={`
          w-[480px] bg-[#0c0c0e] border-l border-zinc-800/50
          transform transition-all duration-300 ease-out flex flex-col
          ${selectedNode ? 'translate-x-0' : 'translate-x-full'}
        `}
      >
        {selectedNode && (
          <>
            <div className="flex items-center justify-between px-5 py-4 border-b border-zinc-800/50">
              <h3 className="font-medium text-zinc-200">{selectedNode.label}</h3>
              <button
                onClick={() => setSelectedNode(null)}
                className="p-1.5 hover:bg-zinc-800 rounded-lg transition-colors"
              >
                <X className="w-4 h-4 text-zinc-500" />
              </button>
            </div>
            <div className="flex-1 overflow-y-auto p-5">
              {selectedNode.content ? (
                <ContentRenderer content={selectedNode.content} />
              ) : (
                <p className="text-zinc-600 text-sm italic">No content available</p>
              )}
            </div>
          </>
        )}
      </div>
    </div>
  );
}

export default MindMapDocs;
