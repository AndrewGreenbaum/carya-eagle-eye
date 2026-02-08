/**
 * CaryaChat.tsx - Right sidebar chat for Carya AI assistant
 *
 * Connects to OpenClaw WebSocket gateway for real-time chat with Carya,
 * your VC deal intelligence consigliere.
 */

import { useState, useEffect, useRef, useCallback } from 'react';
import { MessageSquare, X, Send, Loader2 } from 'lucide-react';

interface Message {
  id: string;
  role: 'user' | 'assistant';
  content: string;
  timestamp: Date;
}

const OPENCLAW_URL = 'wss://openclaw-production-b45a.up.railway.app';
const OPENCLAW_TOKEN = '9abaoi6e0si9xh9i0iwupaxmntnvbl5l';

export function CaryaChat() {
  const [isOpen, setIsOpen] = useState(false);
  const [messages, setMessages] = useState<Message[]>([]);
  const [inputValue, setInputValue] = useState('');
  const [isConnected, setIsConnected] = useState(false);
  const [isLoading, setIsLoading] = useState(false);
  const [connectionError, setConnectionError] = useState<string | null>(null);

  const wsRef = useRef<WebSocket | null>(null);
  const messagesEndRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);
  const reconnectTimeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  };

  useEffect(() => {
    scrollToBottom();
  }, [messages]);

  const connect = useCallback(() => {
    if (wsRef.current?.readyState === WebSocket.OPEN) return;

    try {
      const ws = new WebSocket(`${OPENCLAW_URL}/?token=${OPENCLAW_TOKEN}`);

      ws.onopen = () => {
        setIsConnected(true);
        setConnectionError(null);
        console.log('[Carya] Connected to OpenClaw');
      };

      ws.onmessage = (event) => {
        try {
          const data = JSON.parse(event.data);

          // Handle different message types from OpenClaw
          if (data.type === 'agent_response' || data.type === 'message') {
            const content = data.content || data.text || data.message || '';
            if (content) {
              setMessages(prev => [...prev, {
                id: crypto.randomUUID(),
                role: 'assistant',
                content: content,
                timestamp: new Date(),
              }]);
              setIsLoading(false);
            }
          } else if (data.type === 'stream_start') {
            setIsLoading(true);
          } else if (data.type === 'stream_end') {
            setIsLoading(false);
          } else if (data.type === 'error') {
            setConnectionError(data.message || 'An error occurred');
            setIsLoading(false);
          }
        } catch (e) {
          // Handle plain text responses
          if (typeof event.data === 'string' && event.data.trim()) {
            setMessages(prev => [...prev, {
              id: crypto.randomUUID(),
              role: 'assistant',
              content: event.data,
              timestamp: new Date(),
            }]);
            setIsLoading(false);
          }
        }
      };

      ws.onclose = () => {
        setIsConnected(false);
        console.log('[Carya] Disconnected from OpenClaw');

        // Attempt reconnect after 5 seconds
        if (isOpen) {
          reconnectTimeoutRef.current = setTimeout(() => {
            console.log('[Carya] Attempting reconnect...');
            connect();
          }, 5000);
        }
      };

      ws.onerror = (error) => {
        console.error('[Carya] WebSocket error:', error);
        setConnectionError('Connection error. Retrying...');
      };

      wsRef.current = ws;
    } catch (error) {
      console.error('[Carya] Failed to connect:', error);
      setConnectionError('Failed to connect to Carya');
    }
  }, [isOpen]);

  useEffect(() => {
    if (isOpen) {
      connect();
      // Focus input when chat opens
      setTimeout(() => inputRef.current?.focus(), 100);
    }

    return () => {
      if (reconnectTimeoutRef.current) {
        clearTimeout(reconnectTimeoutRef.current);
      }
    };
  }, [isOpen, connect]);

  useEffect(() => {
    // Cleanup on unmount
    return () => {
      if (wsRef.current) {
        wsRef.current.close();
      }
      if (reconnectTimeoutRef.current) {
        clearTimeout(reconnectTimeoutRef.current);
      }
    };
  }, []);

  const sendMessage = () => {
    if (!inputValue.trim() || !wsRef.current || wsRef.current.readyState !== WebSocket.OPEN) {
      return;
    }

    const userMessage: Message = {
      id: crypto.randomUUID(),
      role: 'user',
      content: inputValue.trim(),
      timestamp: new Date(),
    };

    setMessages(prev => [...prev, userMessage]);
    setInputValue('');
    setIsLoading(true);

    // Send message to OpenClaw
    wsRef.current.send(JSON.stringify({
      type: 'message',
      content: inputValue.trim(),
    }));
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      sendMessage();
    }
  };

  return (
    <>
      {/* Toggle tab - left side, shows "Carya" text vertically */}
      {!isOpen && (
        <button
          onClick={() => setIsOpen(true)}
          className="fixed left-0 top-1/2 -translate-y-1/2 z-30 px-1.5 py-4 bg-slate-900/90 hover:bg-slate-800 border border-l-0 border-slate-800 hover:border-slate-700 rounded-r text-slate-500 hover:text-slate-300 transition-all duration-150"
          title="Ask Carya"
        >
          <span className="text-[11px] font-medium tracking-wide" style={{ writingMode: 'vertical-rl' }}>
            Carya
          </span>
        </button>
      )}

      {/* Left Sidebar */}
      <div
        className={`fixed top-0 left-0 h-full w-80 bg-[#050506] border-r border-slate-800 z-20 flex flex-col transition-transform duration-200 ease-in-out ${
          isOpen ? 'translate-x-0' : '-translate-x-full'
        }`}
      >
        {/* Header */}
        <div className="flex items-center justify-between px-4 py-3 border-b border-slate-800">
          <div className="flex items-center gap-3">
            <div className="w-7 h-7 rounded bg-slate-800 flex items-center justify-center">
              <MessageSquare className="w-4 h-4 text-slate-400" />
            </div>
            <div>
              <h3 className="text-sm font-medium text-slate-300">Carya</h3>
              <p className="text-[10px] text-slate-600">
                {isConnected ? (
                  <span className="flex items-center gap-1">
                    <span className="w-1.5 h-1.5 bg-emerald-500 rounded-full" />
                    Online
                  </span>
                ) : (
                  <span className="flex items-center gap-1">
                    <span className="w-1.5 h-1.5 bg-amber-500 rounded-full animate-pulse" />
                    Connecting...
                  </span>
                )}
              </p>
            </div>
          </div>
          <button
            onClick={() => setIsOpen(false)}
            className="p-1.5 text-slate-600 hover:text-slate-400 hover:bg-slate-800 rounded transition-colors"
            title="Close"
          >
            <X className="w-4 h-4" />
          </button>
        </div>

        {/* Messages */}
        <div className="flex-1 overflow-y-auto p-4 space-y-3">
          {messages.length === 0 && (
            <div className="text-center py-6">
              <div className="w-10 h-10 mx-auto mb-3 rounded bg-slate-800/50 flex items-center justify-center">
                <MessageSquare className="w-5 h-5 text-slate-600" />
              </div>
              <p className="text-slate-500 text-xs mb-1">Hey, I'm Carya.</p>
              <p className="text-slate-600 text-[10px]">Your VC deal intelligence partner.</p>
              <div className="mt-4 space-y-2">
                <p className="text-slate-700 text-[10px]">Try asking:</p>
                <div className="flex flex-col gap-1.5">
                  {['System health?', 'Recent a16z deals', "How's my pipeline?"].map((q) => (
                    <button
                      key={q}
                      onClick={() => {
                        setInputValue(q);
                        inputRef.current?.focus();
                      }}
                      className="px-2 py-1.5 text-[11px] bg-slate-900 hover:bg-slate-800 text-slate-500 hover:text-slate-400 rounded border border-slate-800 hover:border-slate-700 transition-colors text-left"
                    >
                      {q}
                    </button>
                  ))}
                </div>
              </div>
            </div>
          )}

          {messages.map((msg) => (
            <div
              key={msg.id}
              className={`flex ${msg.role === 'user' ? 'justify-end' : 'justify-start'}`}
            >
              <div
                className={`max-w-[90%] px-3 py-2 rounded text-xs ${
                  msg.role === 'user'
                    ? 'bg-slate-800 text-slate-300'
                    : 'bg-slate-900 text-slate-400 border border-slate-800'
                }`}
              >
                <p className="whitespace-pre-wrap break-words">{msg.content}</p>
                <p className="text-[9px] mt-1 text-slate-600">
                  {msg.timestamp.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}
                </p>
              </div>
            </div>
          ))}

          {isLoading && (
            <div className="flex justify-start">
              <div className="bg-slate-900 border border-slate-800 px-3 py-2 rounded">
                <Loader2 className="w-3 h-3 text-slate-500 animate-spin" />
              </div>
            </div>
          )}

          {connectionError && (
            <div className="text-center py-2">
              <p className="text-amber-500 text-[10px]">{connectionError}</p>
            </div>
          )}

          <div ref={messagesEndRef} />
        </div>

        {/* Input */}
        <div className="p-3 border-t border-slate-800">
          <div className="flex items-center gap-2">
            <input
              ref={inputRef}
              type="text"
              value={inputValue}
              onChange={(e) => setInputValue(e.target.value)}
              onKeyDown={handleKeyDown}
              placeholder="Ask Carya..."
              disabled={!isConnected}
              className="flex-1 px-3 py-2 bg-slate-900 border border-slate-800 rounded text-xs text-slate-300 placeholder-slate-600 focus:outline-none focus:border-slate-700 disabled:opacity-50 disabled:cursor-not-allowed"
            />
            <button
              onClick={sendMessage}
              disabled={!inputValue.trim() || !isConnected || isLoading}
              className="p-2 bg-slate-800 hover:bg-slate-700 disabled:bg-slate-900 disabled:text-slate-700 disabled:cursor-not-allowed text-slate-400 hover:text-slate-300 rounded transition-colors"
            >
              <Send className="w-3.5 h-3.5" />
            </button>
          </div>
        </div>
      </div>
    </>
  );
}
