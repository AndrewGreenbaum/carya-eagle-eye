/**
 * AdminFeedback.tsx - Admin page to view and manage feedback
 * Access via /admin URL
 */

import { useState, useEffect, useCallback } from 'react';
import { Check, Flag, MessageSquare, Trash2, Undo2 } from 'lucide-react';

const API_BASE = import.meta.env.VITE_API_BASE || 'https://bud-tracker-backend-production.up.railway.app';
const API_KEY = 'dev-key';

interface FeedbackItem {
  id: number;
  feedback_type: 'flag' | 'suggestion';
  company_name: string;
  deal_id: number | null;
  reason: string | null;
  suggestion_type: string | null;
  source_url: string | null;
  reporter_email: string | null;
  reviewed: boolean;
  created_at: string;
}

export function AdminFeedback() {
  const [feedback, setFeedback] = useState<FeedbackItem[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [filter, setFilter] = useState<'all' | 'unreviewed'>('unreviewed');

  const fetchFeedback = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const params = filter === 'unreviewed' ? '?reviewed=false' : '';
      const url = `${API_BASE}/feedback${params}`;
      const res = await fetch(url, {
        headers: { 'X-API-Key': API_KEY },
      });
      if (!res.ok) {
        throw new Error(`HTTP ${res.status}: ${res.statusText}`);
      }
      const data = await res.json();
      setFeedback(data.items || []);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load');
    }
    setLoading(false);
  }, [filter]);

  useEffect(() => {
    fetchFeedback();
  }, [fetchFeedback]);

  const toggleReviewed = async (id: number, currentStatus: boolean) => {
    const newStatus = !currentStatus;
    try {
      const res = await fetch(`${API_BASE}/feedback/${id}/reviewed?reviewed=${newStatus}`, {
        method: 'PATCH',
        headers: { 'X-API-Key': API_KEY },
      });
      if (!res.ok) throw new Error('Failed to update');
      setFeedback(prev => prev.map(f =>
        f.id === id ? { ...f, reviewed: newStatus } : f
      ));
    } catch (err) {
      console.error('Failed to toggle reviewed:', err);
    }
  };

  const deleteFeedback = async (id: number) => {
    if (!confirm('Delete this feedback item permanently?')) return;
    try {
      const res = await fetch(`${API_BASE}/feedback/${id}`, {
        method: 'DELETE',
        headers: { 'X-API-Key': API_KEY },
      });
      if (!res.ok) throw new Error('Failed to delete');
      setFeedback(prev => prev.filter(f => f.id !== id));
    } catch (err) {
      console.error('Failed to delete feedback:', err);
    }
  };

  return (
    <div className="min-h-screen bg-zinc-950 text-zinc-300 p-8 font-mono">
      <div className="max-w-6xl mx-auto">
        {/* Header */}
        <div className="flex items-center justify-between mb-8">
          <h1 className="text-2xl font-bold text-zinc-100">
            FEEDBACK ADMIN
          </h1>
          <select
            value={filter}
            onChange={(e) => setFilter(e.target.value as 'all' | 'unreviewed')}
            className="bg-zinc-900 border border-zinc-700 rounded px-3 py-2 text-zinc-300 focus:outline-none focus:border-zinc-500"
          >
            <option value="unreviewed">Unreviewed Only</option>
            <option value="all">All Feedback</option>
          </select>
        </div>

        {/* Stats */}
        <div className="grid grid-cols-3 gap-4 mb-8">
          <div className="bg-zinc-900 border border-zinc-800 rounded-lg p-4">
            <div className="text-3xl font-bold text-zinc-100">{feedback.length}</div>
            <div className="text-zinc-500 text-sm">Total Items</div>
          </div>
          <div className="bg-zinc-900 border border-zinc-800 rounded-lg p-4">
            <div className="text-3xl font-bold text-red-400">
              {feedback.filter(f => f.feedback_type === 'flag').length}
            </div>
            <div className="text-zinc-500 text-sm">Flags</div>
          </div>
          <div className="bg-zinc-900 border border-zinc-800 rounded-lg p-4">
            <div className="text-3xl font-bold text-blue-400">
              {feedback.filter(f => f.feedback_type === 'suggestion').length}
            </div>
            <div className="text-zinc-500 text-sm">Suggestions</div>
          </div>
        </div>

        {/* Feedback List */}
        {loading ? (
          <div className="text-center py-12 text-zinc-500">Loading feedback...</div>
        ) : error ? (
          <div className="text-center py-12">
            <div className="text-red-400 mb-4">Error: {error}</div>
            <button
              onClick={fetchFeedback}
              className="px-4 py-2 bg-zinc-800 border border-zinc-700 rounded hover:bg-zinc-700"
            >
              Retry
            </button>
          </div>
        ) : feedback.length === 0 ? (
          <div className="text-center py-12 text-zinc-500">
            No feedback to review
          </div>
        ) : (
          <div className="space-y-4">
            {feedback.map((item) => (
              <div
                key={item.id}
                className={`bg-zinc-900 border rounded-lg p-4 ${
                  item.reviewed ? 'border-zinc-800 opacity-50' : 'border-zinc-700'
                }`}
              >
                <div className="flex items-start justify-between">
                  <div className="flex-1">
                    {/* Type badge */}
                    <div className="flex items-center gap-2 mb-2">
                      {item.feedback_type === 'flag' ? (
                        <span className="flex items-center gap-1 px-2 py-1 bg-red-950 text-red-400 text-xs rounded border border-red-900">
                          <Flag className="w-3 h-3" />
                          FLAG
                        </span>
                      ) : (
                        <span className="flex items-center gap-1 px-2 py-1 bg-blue-950 text-blue-400 text-xs rounded border border-blue-900">
                          <MessageSquare className="w-3 h-3" />
                          {item.suggestion_type?.toUpperCase() || 'SUGGESTION'}
                        </span>
                      )}
                      {item.reviewed && (
                        <span className="px-2 py-1 bg-zinc-800 text-zinc-400 text-xs rounded border border-zinc-700">
                          REVIEWED
                        </span>
                      )}
                    </div>

                    {/* Company name */}
                    <h3 className="text-lg font-semibold text-zinc-100 mb-1">
                      {item.company_name}
                    </h3>

                    {/* Reason/details */}
                    {item.reason && (
                      <p className="text-zinc-400 mb-2">{item.reason}</p>
                    )}

                    {/* Metadata */}
                    <div className="flex gap-4 text-xs text-zinc-500">
                      <span>
                        {new Date(item.created_at).toLocaleDateString()} {new Date(item.created_at).toLocaleTimeString()}
                      </span>
                      {item.deal_id && <span>Deal #{item.deal_id}</span>}
                      {item.source_url && (
                        <a
                          href={item.source_url}
                          target="_blank"
                          rel="noopener noreferrer"
                          className="text-blue-400 hover:underline"
                        >
                          Source
                        </a>
                      )}
                      {item.reporter_email && (
                        <span>From: {item.reporter_email}</span>
                      )}
                    </div>
                  </div>

                  {/* Actions */}
                  <div className="flex items-center gap-2 ml-4">
                    {item.reviewed ? (
                      <button
                        onClick={() => toggleReviewed(item.id, true)}
                        className="flex items-center gap-1 px-3 py-2 bg-zinc-800 border border-zinc-700 rounded hover:bg-zinc-700 text-sm transition-colors"
                        title="Undo Review"
                      >
                        <Undo2 className="w-4 h-4" />
                        Undo
                      </button>
                    ) : (
                      <button
                        onClick={() => toggleReviewed(item.id, false)}
                        className="flex items-center gap-1 px-3 py-2 bg-zinc-800 border border-zinc-700 rounded hover:bg-zinc-700 text-sm transition-colors"
                        title="Mark as Reviewed"
                      >
                        <Check className="w-4 h-4" />
                        Reviewed
                      </button>
                    )}
                    <button
                      onClick={() => deleteFeedback(item.id)}
                      className="flex items-center gap-1 px-3 py-2 bg-red-950 border border-red-900 rounded hover:bg-red-900 text-red-400 text-sm transition-colors"
                      title="Delete"
                    >
                      <Trash2 className="w-4 h-4" />
                    </button>
                  </div>
                </div>
              </div>
            ))}
          </div>
        )}

        {/* Back link */}
        <div className="mt-8 text-center">
          <a href="/" className="text-zinc-500 hover:text-zinc-300 transition-colors">
            ← Back to Dashboard
          </a>
        </div>
      </div>
    </div>
  );
}
