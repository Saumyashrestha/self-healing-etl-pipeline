import { useState } from 'react';
import Dashboard from './components/Dashboard';
import IcebergMetrics from './components/IcebergMetrics';
import AICopilot from './components/AICopilot';

function App() {
  const [activeTab, setActiveTab] = useState('dashboard');

  return (
    <div className="flex h-screen bg-[#F8FAFC] font-sans text-slate-900 overflow-hidden">
      <aside className="w-64 bg-white border-r border-slate-200 p-6 flex flex-col justify-between">
        <div>
          <div className="flex items-center gap-3 mb-10">
            <h1 className="font-bold text-lg tracking-tight">Lakehouse Copilot</h1>
          </div>
          <nav className="space-y-1.5">
            {['Dashboard', 'Iceberg Metrics', 'AI Copilot'].map((item) => (
              <button 
                key={item}
                onClick={() => setActiveTab(item.toLowerCase().replace(' ', '-'))}
                className={`w-full text-left px-4 py-2.5 rounded-md text-sm font-medium ${
                  activeTab === item.toLowerCase().replace(' ', '-') ? 'bg-blue-50 text-blue-700' : 'text-slate-600'
                }`}
              >
                {item}
              </button>
            ))}
          </nav>
        </div>
      </aside>

      <main className="flex-1 p-8 lg:p-10 overflow-y-auto">
        {activeTab === 'dashboard' && <Dashboard />}
        {activeTab === 'iceberg-metrics' && <IcebergMetrics />}
        {activeTab === 'ai-copilot' && <AICopilot />}
      </main>
    </div>
  );
}
export default App;