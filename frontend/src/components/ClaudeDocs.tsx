/**
 * ClaudeDocs.tsx - System Documentation Page
 *
 * Displays CLAUDE.md content in a clean, readable format.
 * Access via /claude URL
 */

import { useState, useEffect } from 'react';
import { RefreshCw, BookOpen, ArrowLeft, ExternalLink, ChevronDown, ChevronRight } from 'lucide-react';

const API_BASE = import.meta.env.VITE_API_BASE || 'https://bud-tracker-backend-production.up.railway.app';

interface Section {
  id: string;
  title: string;
  level: number;
  content: string;
}

// Parse markdown into sections (only h1 and h2 create sections, h3 stays as content)
function parseMarkdown(content: string): Section[] {
  const lines = content.split('\n');
  const sections: Section[] = [];
  let currentSection: Section | null = null;
  let currentContent: string[] = [];

  for (const line of lines) {
    const h1Match = line.match(/^# (.+)$/);
    const h2Match = line.match(/^## (.+)$/);

    // Only h1 and h2 create new sections - h3 stays as content
    if (h1Match || h2Match) {
      // Save previous section
      if (currentSection) {
        currentSection.content = currentContent.join('\n');
        sections.push(currentSection);
      }

      const title = h1Match?.[1] || h2Match?.[1] || '';
      const level = h1Match ? 1 : 2;

      currentSection = {
        id: title.toLowerCase().replace(/[^a-z0-9]+/g, '-'),
        title,
        level,
        content: '',
      };
      currentContent = [];
    } else if (currentSection) {
      currentContent.push(line);
    }
  }

  // Save last section
  if (currentSection) {
    currentSection.content = currentContent.join('\n');
    sections.push(currentSection);
  }

  return sections;
}

// Render markdown content with basic formatting
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
        <pre key={elements.length} className="bg-slate-900 rounded-lg p-4 overflow-x-auto my-4 text-sm">
          <code className="text-emerald-400">{codeLines.join('\n')}</code>
        </pre>
      );
      codeLines = [];
    }
  };

  const flushTable = () => {
    if (tableRows.length > 0) {
      const headers = tableRows[0];
      const body = tableRows.slice(2); // Skip header and separator
      elements.push(
        <div key={elements.length} className="overflow-x-auto my-4">
          <table className="min-w-full border border-slate-700 rounded-lg overflow-hidden">
            <thead className="bg-slate-800">
              <tr>
                {headers.map((h, i) => (
                  <th key={i} className="px-4 py-2 text-left text-slate-300 font-medium border-b border-slate-700">
                    {h.trim()}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {body.map((row, i) => (
                <tr key={i} className={i % 2 === 0 ? 'bg-slate-900/50' : 'bg-slate-900'}>
                  {row.map((cell, j) => (
                    <td key={j} className="px-4 py-2 text-slate-400 border-b border-slate-800">
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

  const renderInline = (text: string): React.ReactNode => {
    // Handle inline code
    const parts = text.split(/(`[^`]+`)/g);
    return parts.map((part, i) => {
      if (part.startsWith('`') && part.endsWith('`')) {
        return (
          <code key={i} className="bg-slate-800 text-emerald-400 px-1.5 py-0.5 rounded text-sm">
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
        // Handle links
        const linkMatch = bp.match(/\[([^\]]+)\]\(([^)]+)\)/);
        if (linkMatch) {
          return (
            <a
              key={`${i}-${j}`}
              href={linkMatch[2]}
              target="_blank"
              rel="noopener noreferrer"
              className="text-blue-400 hover:text-blue-300 underline"
            >
              {linkMatch[1]}
            </a>
          );
        }
        return bp;
      });
    });
  };

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];

    // Code blocks
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

    // Tables
    if (line.includes('|') && line.trim().startsWith('|')) {
      if (!inTable) {
        inTable = true;
      }
      const cells = line.split('|').slice(1, -1);
      tableRows.push(cells);
      continue;
    } else if (inTable) {
      flushTable();
      inTable = false;
    }

    // Empty line
    if (!line.trim()) {
      continue;
    }

    // Blockquote
    if (line.startsWith('>')) {
      elements.push(
        <blockquote key={elements.length} className="border-l-4 border-emerald-500 pl-4 my-4 text-slate-400 italic">
          {renderInline(line.slice(1).trim())}
        </blockquote>
      );
      continue;
    }

    // List items
    if (line.match(/^[-*] /)) {
      elements.push(
        <li key={elements.length} className="text-slate-300 ml-4 my-1">
          {renderInline(line.slice(2))}
        </li>
      );
      continue;
    }

    // Numbered list
    if (line.match(/^\d+\. /)) {
      elements.push(
        <li key={elements.length} className="text-slate-300 ml-4 my-1 list-decimal">
          {renderInline(line.replace(/^\d+\. /, ''))}
        </li>
      );
      continue;
    }

    // H3 headers (subheadings within sections)
    const h3Match = line.match(/^### (.+)$/);
    if (h3Match) {
      elements.push(
        <h4 key={elements.length} className="text-lg font-semibold text-white mt-6 mb-2">
          {h3Match[1]}
        </h4>
      );
      continue;
    }

    // H4 headers
    const h4Match = line.match(/^#### (.+)$/);
    if (h4Match) {
      elements.push(
        <h5 key={elements.length} className="text-base font-medium text-slate-200 mt-4 mb-2">
          {h4Match[1]}
        </h5>
      );
      continue;
    }

    // Regular paragraph
    elements.push(
      <p key={elements.length} className="text-slate-300 my-2">
        {renderInline(line)}
      </p>
    );
  }

  flushCode();
  flushTable();

  return <div>{elements}</div>;
}

// Collapsible section component
function SectionCard({ section, defaultOpen = false }: { section: Section; defaultOpen?: boolean }) {
  const [isOpen, setIsOpen] = useState(defaultOpen);

  return (
    <div id={section.id} className="bg-slate-900/50 rounded-lg border border-slate-800 mb-4 overflow-hidden">
      <button
        onClick={() => setIsOpen(!isOpen)}
        className="w-full px-6 py-4 flex items-center justify-between hover:bg-slate-800/50 transition-colors"
      >
        <h3 className={`font-bold text-left ${section.level === 2 ? 'text-lg text-white' : 'text-base text-slate-200'}`}>
          {section.title}
        </h3>
        {isOpen ? (
          <ChevronDown className="w-5 h-5 text-slate-400" />
        ) : (
          <ChevronRight className="w-5 h-5 text-slate-400" />
        )}
      </button>
      {isOpen && (
        <div className="px-6 pb-6 border-t border-slate-800">
          <MarkdownContent content={section.content} />
        </div>
      )}
    </div>
  );
}

export function ClaudeDocs() {
  const [updatedAt, setUpdatedAt] = useState<number>(0);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [sections, setSections] = useState<Section[]>([]);

  useEffect(() => {
    const fetchDocs = async () => {
      setLoading(true);
      try {
        const res = await fetch(`${API_BASE}/docs/claude`);
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data = await res.json();
        setUpdatedAt(data.updated_at);
        setSections(parseMarkdown(data.content));
        setError(null);
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Failed to load documentation');
      } finally {
        setLoading(false);
      }
    };
    fetchDocs();
  }, []);

  const formatDate = (timestamp: number) => {
    if (!timestamp) return 'Unknown';
    return new Date(timestamp * 1000).toLocaleDateString('en-US', {
      year: 'numeric',
      month: 'long',
      day: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
    });
  };

  // Get title from first h1
  const title = sections.find(s => s.level === 1)?.title || 'System Documentation';
  const h2Sections = sections.filter(s => s.level === 2);

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
        <div className="max-w-5xl mx-auto flex items-center justify-between">
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
              <h1 className="text-xl font-bold text-white">{title}</h1>
            </div>
          </div>
          <div className="text-sm text-slate-500">
            Updated: {formatDate(updatedAt)}
          </div>
        </div>
      </header>

      {/* Navigation */}
      <nav className="sticky top-[73px] z-40 bg-[#0a0a0c]/95 backdrop-blur border-b border-slate-800 px-6 py-3">
        <div className="max-w-5xl mx-auto flex flex-wrap gap-2">
          {h2Sections.map((section) => (
            <a
              key={section.id}
              href={`#${section.id}`}
              className="px-3 py-1.5 text-sm bg-slate-800 hover:bg-slate-700 text-slate-300 hover:text-white rounded-lg transition-colors"
            >
              {section.title}
            </a>
          ))}
        </div>
      </nav>

      {/* Content */}
      <main className="max-w-5xl mx-auto px-6 py-8">
        {error ? (
          <div className="bg-red-900/20 border border-red-800 rounded-lg p-4 text-red-400">
            {error}
          </div>
        ) : (
          <div>
            {/* Intro from first section */}
            {sections[0] && sections[0].level === 1 && (
              <div className="mb-8">
                <MarkdownContent content={sections[0].content} />
              </div>
            )}

            {/* H2 Sections as collapsible cards */}
            {h2Sections.map((section, i) => (
              <SectionCard
                key={section.id}
                section={section}
                defaultOpen={i < 3} // First 3 sections open by default
              />
            ))}
          </div>
        )}
      </main>

      {/* Footer */}
      <footer className="border-t border-slate-800 px-6 py-6 mt-8">
        <div className="max-w-5xl mx-auto flex items-center justify-between text-sm text-slate-500">
          <div>
            Carya Eagle Eye &copy; 2025
          </div>
          <a
            href="https://github.com/UMICHLEG/bud-tracker"
            target="_blank"
            rel="noopener noreferrer"
            className="flex items-center gap-1 hover:text-slate-300 transition-colors"
          >
            <ExternalLink className="w-4 h-4" />
            GitHub
          </a>
        </div>
      </footer>
    </div>
  );
}

export default ClaudeDocs;
