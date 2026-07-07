import { useState, useRef, useEffect } from 'react';

interface Message {
  role: 'user' | 'agent' | 'system';
  content: string;
  requires_confirmation?: boolean;
  target_table?: string;
}

export default function AICopilot() {
  const [messages, setMessages] = useState<Message[]>([
    { role: 'agent', content: "Hello! I am your AI-powered Lakehouse Copilot. I can analyze health and trigger maintenance. How can I assist you today?" }
  ]);
  const [input, setInput] = useState('');
  const [isAgentThinking, setIsAgentThinking] = useState(false);
  const scrollRef = useRef<HTMLDivElement>(null);

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
        target_table: data.target_table
      }]);
    } catch (error) {
      setMessages(prev => [...prev, { role: 'agent', content: `Error: Unable to connect to backend agent. ${error}` }]);
    } finally {
      setIsAgentThinking(false);
    }
  };

  // The hidden command sent when a user clicks the UI button
  const handleConfirmation = (isConfirmed: boolean, tableName: string) => {
    if (isConfirmed) {
      sendMessage(`Yes, please proceed with execute_confirmed_maintenance on the ${tableName} table.`);
    } else {
      sendMessage(`No, cancel the maintenance operation on ${tableName}.`);
    }
  };

  useEffect(() => {
    scrollRef.current?.scrollTo(0, scrollRef.current.scrollHeight);
  }, [messages]);

  return (
    <div className="flex flex-col h-[600px] bg-slate-900 rounded-2xl overflow-hidden shadow-2xl border border-slate-800">
      <div className="bg-slate-800 p-4 border-b border-slate-700 flex justify-between items-center">
        <span className="text-slate-300 font-mono text-sm">agent@lakehouse:~$</span>
        {isAgentThinking && <span className="text-blue-400 animate-pulse text-xs font-mono">Agent thinking...</span>}
      </div>

      <div ref={scrollRef} className="flex-1 overflow-y-auto p-6 space-y-4">
        {messages.map((m, i) => (
          <div key={i} className={`flex flex-col ${m.role === 'user' ? 'items-end' : 'items-start'}`}>
            <div className={`max-w-[85%] p-3 rounded-lg text-sm ${
              m.role === 'user' ? 'bg-blue-600 text-white' : 'bg-slate-700 text-slate-200'
            }`}>
              {m.content}
            </div>
            
            {/* The Dynamic HITL UI Component */}
            {m.requires_confirmation && m.target_table && (
              <div className="mt-2 p-3 border border-amber-500/30 bg-amber-900/20 rounded-lg max-w-[85%]">
                <p className="text-sm text-amber-200 font-medium mb-3">
                  ⚠️ Require authorization to run PySpark Compaction on: <span className="font-bold">{m.target_table}</span>
                </p>
                <div className="flex gap-2">
                  <button 
                    onClick={() => handleConfirmation(true, m.target_table!)}
                    className="px-3 py-1.5 bg-amber-600 text-white rounded text-xs font-semibold hover:bg-amber-700 transition-colors"
                  >
                    Confirm & Execute
                  </button>
                  <button 
                    onClick={() => handleConfirmation(false, m.target_table!)}
                    className="px-3 py-1.5 bg-slate-600 text-slate-200 rounded text-xs font-semibold hover:bg-slate-500 transition-colors"
                  >
                    Cancel
                  </button>
                </div>
              </div>
            )}
          </div>
        ))}
      </div>

      <div className="p-4 bg-slate-800 border-t border-slate-700">
        <input 
          type="text"
          value={input}
          onChange={(e) => setInput(e.target.value)}
          onKeyDown={(e) => e.key === 'Enter' && sendMessage(input)}
          placeholder="Ask about table health or request maintenance..."
          className="w-full bg-slate-900 border border-slate-700 rounded-lg px-4 py-2 text-sm text-white placeholder-slate-500 focus:outline-none focus:border-blue-500"
          disabled={isAgentThinking}
        />
      </div>
    </div>
  );
}