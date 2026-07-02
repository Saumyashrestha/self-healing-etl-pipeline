import { useState, useRef, useEffect } from 'react';

interface Message {
  role: 'user' | 'agent';
  content: string;
}

export default function AICopilot() {
  const [messages, setMessages] = useState<Message[]>([
    { role: 'agent', content: "Hello! I am your Lakehouse Copilot. I'm ready to help you analyze and optimize your Iceberg tables. How can I assist you today?" }
  ]);
  const [isAgentThinking, setIsAgentThinking] = useState(false);
  const scrollRef = useRef<HTMLDivElement>(null);

  const quickActions = [
    { label: "Analyze Health", action: "analyze" },
    { label: "Run Compaction", action: "compact" },
    { label: "Clear Snapshots", action: "cleanup" }
  ];

  const handleAction = async (action: string) => {
    setMessages(prev => [...prev, { role: 'user', content: `Execute: ${action}` }]);
    setIsAgentThinking(true);

    // Simulate Agent Delay
    setTimeout(() => {
      setIsAgentThinking(false);
      setMessages(prev => [...prev, { 
        role: 'agent', 
        content: `I have initiated the '${action}' command. Checking Iceberg metadata and preparing Spark job...` 
      }]);
    }, 1500);
  };

  useEffect(() => {
    scrollRef.current?.scrollTo(0, scrollRef.current.scrollHeight);
  }, [messages]);

  return (
    <div className="flex flex-col h-[600px] bg-slate-900 rounded-2xl overflow-hidden shadow-2xl border border-slate-800">
      {/* Header */}
      <div className="bg-slate-800 p-4 border-b border-slate-700 flex justify-between items-center">
        <span className="text-slate-300 font-mono text-sm">copilot_agent@iceberg:~$</span>
        {isAgentThinking && <span className="text-blue-400 animate-pulse text-xs">Agent thinking...</span>}
      </div>

      {/* Chat Feed */}
      <div ref={scrollRef} className="flex-1 overflow-y-auto p-6 space-y-4">
        {messages.map((m, i) => (
          <div key={i} className={`flex ${m.role === 'user' ? 'justify-end' : 'justify-start'}`}>
            <div className={`max-w-[80%] p-3 rounded-lg text-sm ${m.role === 'user' ? 'bg-blue-600 text-white' : 'bg-slate-700 text-slate-200'}`}>
              {m.content}
            </div>
          </div>
        ))}
      </div>

      {/* Prompt Chips */}
      <div className="p-4 bg-slate-800 border-t border-slate-700 flex gap-2">
        {quickActions.map(q => (
          <button 
            key={q.action}
            onClick={() => handleAction(q.action)}
            className="px-3 py-1 bg-slate-700 hover:bg-slate-600 border border-slate-600 rounded-full text-xs text-slate-300 transition-colors"
          >
            {q.label}
          </button>
        ))}
      </div>
    </div>
  );
}