/**
 * TreeDocs.tsx - Interactive Mind Map/Tree View of System Documentation
 *
 * Displays CLAUDE.md as a visual tree structure with expandable nodes.
 * Access via /claude URL
 */

import { useState, useEffect, useCallback } from 'react';
import { RefreshCw, BookOpen, ArrowLeft, ChevronDown, ChevronRight, Circle, Minus } from 'lucide-react';

const API_BASE = import.meta.env.VITE_API_BASE || 'https://bud-tracker-backend-production.up.railway.app';

// Tree node structure
interface TreeNode {
  id: string;
  title: string;
  level: number; // 1 = h1 (root), 2 = h2, 3 = h3, 4 = h4
  content: string;
  children: TreeNode[];
  expanded: boolean;
}

// Parse markdown into tree structure
function parseMarkdownToTree(content: string): TreeNode {
  const lines = content.split('\n');
  const root: TreeNode = {
    id: 'root',
    title: 'Carya Eagle Eye',
    level: 0,
    content: '',
    children: [],
    expanded: true,
  };

  let currentH1: TreeNode | null = null;
  let currentH2: TreeNode | null = null;
  let currentH3: TreeNode | null = null;
  let contentBuffer: string[] = [];
  let inCodeBlock = false; // Track code blocks to skip header detection

  const flushContent = (node: TreeNode | null) => {
    if (node && contentBuffer.length > 0) {
      node.content = contentBuffer.join('\n').trim();
      contentBuffer = [];
    }
  };

  for (const line of lines) {
    // Track code blocks - don't parse headers inside them
    if (line.startsWith('```')) {
      inCodeBlock = !inCodeBlock;
      contentBuffer.push(line);
      continue;
    }

    // Inside code block - just add to content, don't parse as headers
    if (inCodeBlock) {
      contentBuffer.push(line);
      continue;
    }

    const h1Match = line.match(/^# (.+)$/);
    const h2Match = line.match(/^## (.+)$/);
    const h3Match = line.match(/^### (.+)$/);
    const h4Match = line.match(/^#### (.+)$/);

    if (h1Match) {
      flushContent(currentH3 || currentH2 || currentH1);
      currentH1 = {
        id: h1Match[1].toLowerCase().replace(/[^a-z0-9]+/g, '-'),
        title: h1Match[1],
        level: 1,
        content: '',
        children: [],
        expanded: true,
      };
      root.children.push(currentH1);
      root.title = h1Match[1]; // Use h1 as root title
      currentH2 = null;
      currentH3 = null;
    } else if (h2Match) {
      flushContent(currentH3 || currentH2);
      currentH2 = {
        id: h2Match[1].toLowerCase().replace(/[^a-z0-9]+/g, '-'),
        title: h2Match[1],
        level: 2,
        content: '',
        children: [],
        expanded: false,
      };
      if (currentH1) {
        currentH1.children.push(currentH2);
      } else {
        root.children.push(currentH2);
      }
      currentH3 = null;
    } else if (h3Match) {
      flushContent(currentH3);
      currentH3 = {
        id: h3Match[1].toLowerCase().replace(/[^a-z0-9]+/g, '-'),
        title: h3Match[1],
        level: 3,
        content: '',
        children: [],
        expanded: false,
      };
      if (currentH2) {
        currentH2.children.push(currentH3);
      } else if (currentH1) {
        currentH1.children.push(currentH3);
      }
    } else if (h4Match) {
      // Add h4 as leaf content, not separate node
      contentBuffer.push(line);
    } else {
      contentBuffer.push(line);
    }
  }

  // Flush remaining content
  flushContent(currentH3 || currentH2 || currentH1);

  return root;
}

// Render inline markdown formatting
function renderInline(text: string): React.ReactNode {
  // Handle inline code
  const parts = text.split(/(`[^`]+`)/g);
  return parts.map((part, i) => {
    if (part.startsWith('`') && part.endsWith('`')) {
      return (
        <code key={i} className="bg-slate-800 text-emerald-400 px-1 py-0.5 rounded text-xs">
          {part.slice(1, -1)}
        </code>
      );
    }
    // Handle bold
    const boldParts = part.split(/(\*\*[^*]+\*\*)/g);
    return boldParts.map((bp, j) => {
      if (bp.startsWith('**') && bp.endsWith('**')) {
        return <strong key={`${i}-${j}`} className="text-white font-semibold">{bp.slice(2, -2)}</strong>;
      }
      return bp;
    });
  });
}

// Compact content preview for tree nodes
function ContentPreview({ content }: { content: string }) {
  const lines = content.split('\n').filter(l => l.trim());
  const preview = lines.slice(0, 3);

  return (
    <div className="mt-2 text-xs text-slate-500 space-y-1">
      {preview.map((line, i) => {
        // Skip code blocks, tables
        if (line.startsWith('```') || line.startsWith('|')) return null;
        // Show bullet points
        if (line.match(/^[-*] /)) {
          return (
            <div key={i} className="flex items-start gap-1">
              <Minus className="w-3 h-3 mt-0.5 text-slate-600" />
              <span className="truncate">{renderInline(line.slice(2))}</span>
            </div>
          );
        }
        // Regular text (truncated)
        if (line.length > 0 && !line.startsWith('#')) {
          return (
            <p key={i} className="truncate">{renderInline(line.slice(0, 60))}{line.length > 60 ? '...' : ''}</p>
          );
        }
        return null;
      })}
      {lines.length > 3 && (
        <p className="text-slate-600 italic">+{lines.length - 3} more lines</p>
      )}
    </div>
  );
}

// Tree node component
interface TreeNodeProps {
  node: TreeNode;
  depth: number;
  onToggle: (id: string) => void;
  isLast: boolean;
  parentLines: boolean[];
}

function TreeNodeComponent({ node, depth, onToggle, isLast, parentLines }: TreeNodeProps) {
  const hasChildren = node.children.length > 0;
  const hasContent = node.content.trim().length > 0;

  // Color based on level
  const getNodeColor = () => {
    switch (node.level) {
      case 0:
      case 1: return 'bg-emerald-500 border-emerald-400';
      case 2: return 'bg-blue-500 border-blue-400';
      case 3: return 'bg-purple-500 border-purple-400';
      default: return 'bg-slate-500 border-slate-400';
    }
  };

  const getBgColor = () => {
    switch (node.level) {
      case 0:
      case 1: return 'bg-emerald-500/10 border-emerald-500/30 hover:bg-emerald-500/20';
      case 2: return 'bg-blue-500/10 border-blue-500/30 hover:bg-blue-500/20';
      case 3: return 'bg-purple-500/10 border-purple-500/30 hover:bg-purple-500/20';
      default: return 'bg-slate-500/10 border-slate-500/30 hover:bg-slate-500/20';
    }
  };

  return (
    <div className="relative">
      {/* Vertical connecting lines from parent */}
      {depth > 0 && (
        <div className="absolute left-0 top-0 bottom-0 flex">
          {parentLines.map((showLine, i) => (
            <div key={i} className="w-6 relative">
              {showLine && (
                <div className="absolute left-3 top-0 bottom-0 w-px bg-slate-700" />
              )}
            </div>
          ))}
          {/* Horizontal connector */}
          <div className="absolute left-3 top-5 w-3 h-px bg-slate-700" style={{ left: `${(depth - 1) * 24 + 12}px` }} />
        </div>
      )}

      {/* Node content */}
      <div
        className="relative"
        style={{ marginLeft: depth * 24 }}
      >
        <button
          onClick={() => (hasChildren || hasContent) && onToggle(node.id)}
          className={`w-full text-left p-3 rounded-lg border transition-all ${getBgColor()} ${
            (hasChildren || hasContent) ? 'cursor-pointer' : 'cursor-default'
          }`}
        >
          <div className="flex items-center gap-2">
            {/* Expand/collapse icon */}
            {(hasChildren || hasContent) ? (
              node.expanded ? (
                <ChevronDown className="w-4 h-4 text-slate-400 flex-shrink-0" />
              ) : (
                <ChevronRight className="w-4 h-4 text-slate-400 flex-shrink-0" />
              )
            ) : (
              <Circle className={`w-2 h-2 ${getNodeColor()} rounded-full flex-shrink-0`} />
            )}

            {/* Node dot */}
            <div className={`w-2 h-2 rounded-full ${getNodeColor()}`} />

            {/* Title */}
            <span className={`font-medium ${
              node.level <= 1 ? 'text-lg text-white' :
              node.level === 2 ? 'text-base text-slate-200' :
              'text-sm text-slate-300'
            }`}>
              {node.title}
            </span>

            {/* Child count badge */}
            {hasChildren && !node.expanded && (
              <span className="ml-auto text-xs px-2 py-0.5 rounded-full bg-slate-700 text-slate-400">
                {node.children.length}
              </span>
            )}
          </div>

          {/* Content preview when collapsed */}
          {hasContent && !node.expanded && !hasChildren && (
            <ContentPreview content={node.content} />
          )}
        </button>

        {/* Expanded content */}
        {node.expanded && hasContent && (
          <div className="mt-2 ml-6 p-3 bg-slate-900/50 rounded-lg border border-slate-800 text-sm">
            <MarkdownContent content={node.content} />
          </div>
        )}

        {/* Child nodes */}
        {node.expanded && hasChildren && (
          <div className="mt-1 space-y-1">
            {node.children.map((child, i) => (
              <TreeNodeComponent
                key={child.id}
                node={child}
                depth={depth + 1}
                onToggle={onToggle}
                isLast={i === node.children.length - 1}
                parentLines={[...parentLines, !isLast]}
              />
            ))}
          </div>
        )}
      </div>
    </div>
  );
}

// Full markdown content renderer for expanded nodes
function MarkdownContent({ content }: { content: string }) {
  const lines = content.split('\n');
  const elements: JSX.Element[] = [];
  let inCodeBlock = false;
  let codeLines: string[] = [];
  let inTable = false;
  let tableRows: string[][] = [];

  const flushCode = () => {
    if (codeLines.length > 0) {
      elements.push(
        <pre key={elements.length} className="bg-slate-900 rounded p-2 overflow-x-auto my-2 text-xs">
          <code className="text-emerald-400">{codeLines.join('\n')}</code>
        </pre>
      );
      codeLines = [];
    }
  };

  const flushTable = () => {
    if (tableRows.length > 0) {
      const headers = tableRows[0];
      const body = tableRows.slice(2);
      elements.push(
        <div key={elements.length} className="overflow-x-auto my-2">
          <table className="min-w-full border border-slate-700 rounded text-xs">
            <thead className="bg-slate-800">
              <tr>
                {headers.map((h, i) => (
                  <th key={i} className="px-2 py-1 text-left text-slate-300 font-medium border-b border-slate-700">
                    {h.trim()}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {body.map((row, i) => (
                <tr key={i} className={i % 2 === 0 ? 'bg-slate-900/50' : 'bg-slate-900'}>
                  {row.map((cell, j) => (
                    <td key={j} className="px-2 py-1 text-slate-400 border-b border-slate-800">
                      {renderInline(cell.trim())}
                    </td>
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
      if (inCodeBlock) {
        flushCode();
        inCodeBlock = false;
      } else {
        flushTable();
        inCodeBlock = true;
      }
      continue;
    }

    if (inCodeBlock) {
      codeLines.push(line);
      continue;
    }

    if (line.includes('|') && line.trim().startsWith('|')) {
      if (!inTable) inTable = true;
      tableRows.push(line.split('|').slice(1, -1));
      continue;
    } else if (inTable) {
      flushTable();
      inTable = false;
    }

    if (!line.trim()) continue;

    if (line.startsWith('>')) {
      elements.push(
        <blockquote key={elements.length} className="border-l-2 border-emerald-500 pl-2 my-2 text-slate-400 italic text-xs">
          {renderInline(line.slice(1).trim())}
        </blockquote>
      );
      continue;
    }

    if (line.match(/^[-*] /)) {
      elements.push(
        <li key={elements.length} className="text-slate-300 ml-3 my-0.5 text-xs list-disc">
          {renderInline(line.slice(2))}
        </li>
      );
      continue;
    }

    if (line.match(/^\d+\. /)) {
      elements.push(
        <li key={elements.length} className="text-slate-300 ml-3 my-0.5 text-xs list-decimal">
          {renderInline(line.replace(/^\d+\. /, ''))}
        </li>
      );
      continue;
    }

    const h4Match = line.match(/^#### (.+)$/);
    if (h4Match) {
      elements.push(
        <h5 key={elements.length} className="text-sm font-medium text-slate-200 mt-3 mb-1">
          {h4Match[1]}
        </h5>
      );
      continue;
    }

    elements.push(
      <p key={elements.length} className="text-slate-300 my-1 text-xs">
        {renderInline(line)}
      </p>
    );
  }

  flushCode();
  flushTable();

  return <div>{elements}</div>;
}

// Main component
export function TreeDocs() {
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [tree, setTree] = useState<TreeNode | null>(null);
  const [updatedAt, setUpdatedAt] = useState<number>(0);

  useEffect(() => {
    const fetchDocs = async () => {
      setLoading(true);
      try {
        const res = await fetch(`${API_BASE}/docs/claude`);
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data = await res.json();
        setUpdatedAt(data.updated_at);
        setTree(parseMarkdownToTree(data.content));
        setError(null);
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Failed to load documentation');
      } finally {
        setLoading(false);
      }
    };
    fetchDocs();
  }, []);

  // Toggle node expansion
  const toggleNode = useCallback((id: string) => {
    setTree(prevTree => {
      if (!prevTree) return prevTree;

      const toggleInTree = (node: TreeNode): TreeNode => {
        if (node.id === id) {
          return { ...node, expanded: !node.expanded };
        }
        return {
          ...node,
          children: node.children.map(toggleInTree),
        };
      };

      return toggleInTree(prevTree);
    });
  }, []);

  // Expand/collapse all
  const setAllExpanded = useCallback((expanded: boolean) => {
    setTree(prevTree => {
      if (!prevTree) return prevTree;

      const setExpanded = (node: TreeNode): TreeNode => ({
        ...node,
        expanded: node.level <= 1 ? true : expanded, // Keep root always expanded
        children: node.children.map(setExpanded),
      });

      return setExpanded(prevTree);
    });
  }, []);

  const formatDate = (timestamp: number) => {
    if (!timestamp) return 'Unknown';
    return new Date(timestamp * 1000).toLocaleDateString('en-US', {
      year: 'numeric',
      month: 'short',
      day: 'numeric',
    });
  };

  if (loading) {
    return (
      <div className="min-h-screen bg-[#050506] flex items-center justify-center">
        <RefreshCw className="w-8 h-8 text-emerald-400 animate-spin" />
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-[#050506] text-slate-300">
      {/* Header */}
      <header className="sticky top-0 z-50 bg-[#0a0a0c] border-b border-slate-800 px-6 py-4">
        <div className="max-w-6xl mx-auto flex items-center justify-between">
          <div className="flex items-center gap-4">
            <a
              href="/"
              className="flex items-center gap-2 text-slate-400 hover:text-white transition-colors"
            >
              <ArrowLeft className="w-5 h-5" />
              <span>Dashboard</span>
            </a>
            <div className="h-6 w-px bg-slate-700" />
            <div className="flex items-center gap-2">
              <BookOpen className="w-6 h-6 text-emerald-400" />
              <h1 className="text-xl font-bold text-white">System Documentation</h1>
            </div>
          </div>
          <div className="flex items-center gap-4">
            <button
              onClick={() => setAllExpanded(true)}
              className="px-3 py-1.5 text-sm bg-slate-800 hover:bg-slate-700 text-slate-300 rounded transition-colors"
            >
              Expand All
            </button>
            <button
              onClick={() => setAllExpanded(false)}
              className="px-3 py-1.5 text-sm bg-slate-800 hover:bg-slate-700 text-slate-300 rounded transition-colors"
            >
              Collapse All
            </button>
            <div className="text-sm text-slate-500">
              Updated: {formatDate(updatedAt)}
            </div>
          </div>
        </div>
      </header>

      {/* Legend */}
      <div className="bg-[#0a0a0c]/80 border-b border-slate-800 px-6 py-2">
        <div className="max-w-6xl mx-auto flex items-center gap-6 text-xs text-slate-400">
          <span className="text-slate-500">Legend:</span>
          <div className="flex items-center gap-1.5">
            <div className="w-3 h-3 rounded-full bg-emerald-500" />
            <span>Root/Overview</span>
          </div>
          <div className="flex items-center gap-1.5">
            <div className="w-3 h-3 rounded-full bg-blue-500" />
            <span>Main Sections</span>
          </div>
          <div className="flex items-center gap-1.5">
            <div className="w-3 h-3 rounded-full bg-purple-500" />
            <span>Subsections</span>
          </div>
        </div>
      </div>

      {/* Tree Content */}
      <main className="max-w-6xl mx-auto px-6 py-8">
        {error ? (
          <div className="bg-red-900/20 border border-red-800 rounded-lg p-4 text-red-400">
            {error}
          </div>
        ) : tree ? (
          <div className="space-y-2">
            {/* Root node (h1) */}
            <div className="p-4 rounded-lg border bg-emerald-500/10 border-emerald-500/30 mb-4">
              <div className="flex items-center gap-3">
                <div className="w-4 h-4 rounded-full bg-emerald-500" />
                <h2 className="text-2xl font-bold text-white">{tree.title}</h2>
              </div>
              <p className="mt-2 text-slate-400 text-sm">
                Autonomous deal intelligence tracking lead investments from 18 elite VC firms in Enterprise AI startups.
              </p>
              <div className="mt-3 flex gap-4 text-xs text-slate-500">
                <span>{tree.children.filter(c => c.level === 1).flatMap(c => c.children).length} sections</span>
                <span>Click nodes to expand</span>
              </div>
            </div>

            {/* H2 sections as tree nodes */}
            {tree.children.map((h1Node) => (
              <div key={h1Node.id} className="space-y-1">
                {h1Node.children.map((node, i) => (
                  <TreeNodeComponent
                    key={node.id}
                    node={node}
                    depth={0}
                    onToggle={toggleNode}
                    isLast={i === h1Node.children.length - 1}
                    parentLines={[]}
                  />
                ))}
              </div>
            ))}
          </div>
        ) : null}
      </main>

      {/* Footer */}
      <footer className="border-t border-slate-800 px-6 py-6 mt-8">
        <div className="max-w-6xl mx-auto text-center text-sm text-slate-500">
          Carya Eagle Eye Documentation Tree View
        </div>
      </footer>
    </div>
  );
}

export default TreeDocs;
