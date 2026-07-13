import { useState, useRef, useEffect } from 'react';
import ReactMarkdown from 'react-markdown';
import OCCDiagram from './OCCDiagram';

interface Message {
  role: 'user' | 'agent' | 'system';
  content: string;
  requires_confirmation?: boolean;
  target_table?: string;
  resolved?: boolean;
  show_occ_diagram?: boolean;
  occ_timeline?: { baseline_time: string; commit_time: string; crash_time: string };
}

export default function AICopilot() {
  const [messages, setMessages] = useState<Message[]>([
    { role: 'agent', content: "Hello! I am your AI-powered Lakehouse Copilot. I can analyze health and trigger maintenance. How can I assist you today?" }
  ]);
  const [input, setInput] = useState('');
  const [isAgentThinking, setIsAgentThinking] = useState(false);
  const scrollRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const eventSource = new EventSource("http://127.0.0.1:8001/api/agent-notifications");

    eventSource.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data);
        setMessages(prev => [...prev, {
          role: 'agent',
          content: data.reply,
          requires_confirmation: data.requires_confirmation,
          target_table: data.target_table,
          show_occ_diagram: data.show_occ_diagram,
          occ_timeline: data.occ_timeline
        }]);
      } catch (err) {
        console.error("Failed to parse agent notification", err);
      }
    };

    eventSource.onerror = (error) => console.error("Lost SSE connection", error);
    return () => eventSource.close();
  }, []);

  const sendMessage = async (text: string) => {
    if (!text.trim()) return;
    setMessages(prev => [...prev, { role: 'user', content: text }]);
    setInput('');
    setIsAgentThinking(true);

    try {
      const response = await fetch("http://127.0.0.1:8001/chat", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ message: text })
      });
      const data = await response.json();
      setMessages(prev => [...prev, {
        role: 'agent',
        content: data.reply,
        requires_confirmation: data.requires_confirmation,
        target_table: data.target_table,
        show_occ_diagram: data.show_occ_diagram,
        occ_timeline: data.occ_timeline
      }]);
    } catch (error) {
      setMessages(prev => [...prev, { role: 'agent', content: `Error connecting to backend.` }]);
    } finally {
      setIsAgentThinking(false);
    }
  };

  const handleConfirmation = (isConfirmed: boolean, tableName: string, messageIndex: number) => {
    setMessages(prev => prev.map((m, i) => i === messageIndex ? { ...m, resolved: true } : m));

    if (isConfirmed) {
      setMessages(prev => [
        ...prev,
        { role: 'agent', content: `⏳ Running compaction and snapshot expiration on **${tableName}**... this can take a moment, please wait.` }
      ]);
      sendMessage(`Yes, please proceed with execute_confirmed_maintenance on the ${tableName} table.`);
    } else {
      sendMessage(`No, cancel the maintenance operation on ${tableName}.`);
    }
  };

  useEffect(() => {
    scrollRef.current?.scrollTo(0, scrollRef.current.scrollHeight);
  }, [messages]);

  return (
    <div className="flex flex-col h-[85vh] bg-[#0f172a] rounded-xl overflow-hidden shadow-2xl border border-slate-700 font-sans">
      {/* Header */}
      <div className="bg-slate-800/80 backdrop-blur-sm p-4 border-b border-slate-700 flex justify-between items-center shadow-sm z-10">
        <div className="flex items-center gap-3">
          <div className="w-2.5 h-2.5 rounded-full bg-emerald-500 animate-pulse"></div>
          <span className="text-slate-200 font-semibold tracking-wide text-sm">Lakehouse Copilot</span>
        </div>
        {isAgentThinking && <span className="text-blue-400 text-xs font-medium animate-pulse">Agent is typing...</span>}
      </div>

      {/* Chat Area */}
      <div ref={scrollRef} className="flex-1 overflow-y-auto p-6 space-y-6 scroll-smooth bg-gradient-to-b from-[#0f172a] to-slate-900">
        {messages.map((m, i) => (
          <div key={i} className={`flex flex-col ${m.role === 'user' ? 'items-end' : 'items-start'}`}>
            <div
              className={`max-w-[85%] p-4 rounded-2xl text-sm leading-relaxed shadow-md prose prose-invert prose-sm max-w-none ${m.role === 'user'
                  ? 'bg-blue-600 text-white rounded-br-none'
                  : 'bg-slate-800 text-slate-200 border border-slate-700 rounded-bl-none'
                }`}
            >
              <ReactMarkdown>{m.content}</ReactMarkdown>
            </div>

            {/* Action Buttons */}
            {m.requires_confirmation && m.target_table && !m.resolved && (
              <div className="mt-3 p-4 border border-rose-500/30 bg-rose-950/30 rounded-xl max-w-[85%] backdrop-blur-md shadow-lg ml-2">
                <div className="flex items-center gap-2 mb-3">
                  <span className="text-lg">🛠️</span>
                  <p className="text-sm text-rose-200 font-medium">
                    Maintenance required on: <span className="font-bold text-white">{m.target_table}</span>
                  </p>
                </div>
                <div className="flex gap-3">
                  <button
                    onClick={() => handleConfirmation(true, m.target_table!, i)}
                    className="flex-1 px-4 py-2 bg-rose-600 text-white rounded-lg text-xs font-bold tracking-wide hover:bg-rose-500 transition-all shadow-md hover:shadow-rose-500/20"
                  >
                    Execute Compaction
                  </button>
                  <button
                    onClick={() => handleConfirmation(false, m.target_table!, i)}
                    className="px-4 py-2 bg-slate-700 text-slate-300 rounded-lg text-xs font-bold hover:bg-slate-600 transition-all"
                  >
                    Dismiss
                  </button>
                </div>
              </div>
            )}

            {/* OCC Diagram */}
            {m.show_occ_diagram && m.occ_timeline && (
              <OCCDiagram
                baselineTime={m.occ_timeline.baseline_time}
                commitTime={m.occ_timeline.commit_time}
                crashTime={m.occ_timeline.crash_time}
              />
            )}

          </div>
        ))}
      </div>

      {/* Input Area */}
      <div className="p-4 bg-slate-800/80 backdrop-blur-md border-t border-slate-700">
        <div className="relative">
          <input
            type="text"
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyDown={(e) => e.key === 'Enter' && sendMessage(input)}
            placeholder="Ask a question or request maintenance..."
            className="w-full bg-slate-900 border border-slate-600 rounded-xl pl-4 pr-12 py-3 text-sm text-slate-100 placeholder-slate-400 focus:outline-none focus:border-blue-500 focus:ring-1 focus:ring-blue-500 transition-all shadow-inner"
            disabled={isAgentThinking}
          />
          <button
            onClick={() => sendMessage(input)}
            className="absolute right-2 top-1.5 p-1.5 bg-blue-600 hover:bg-blue-500 rounded-lg text-white transition-colors"
          >
            <svg xmlns="http://www.w3.org/2000/svg" className="h-5 w-5" viewBox="0 0 20 20" fill="currentColor">
              <path d="M10.894 2.553a1 1 0 00-1.788 0l-7 14a1 1 0 001.169 1.409l5-1.429A1 1 0 009 15.571V11a1 1 0 112 0v4.571a1 1 0 00.725.962l5 1.428a1 1 0 001.17-1.408l-7-14z" />
            </svg>
          </button>
        </div>
      </div>
    </div>
  );
}
