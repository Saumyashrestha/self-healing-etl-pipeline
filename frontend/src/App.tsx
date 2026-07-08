import { useState } from 'react';
import Dashboard from './components/Dashboard';
import IcebergMetrics from './components/IcebergMetrics';
import AICopilot from './components/AICopilot';
// 1. Add this import:
import OCCVisualizer from './components/OCCVisualizer';

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
            {/* 2. Add 'OCC Simulation' to this array: */}
            {['Dashboard', 'Iceberg Metrics', 'AI Copilot', 'OCC Simulation'].map((item) => (
              <button 
                key={item}
                onClick={() => setActiveTab(item.toLowerCase().replace(' ', '-'))}
                className={`w-full text-left px-4 py-2.5 rounded-md text-sm font-medium ${
                  activeTab === item.toLowerCase().replace(' ', '-') ? 'bg-blue-700 text-white' : 'text-slate-600'
                }`}
              >
                {item}
              </button>
            ))}
          </nav>
        </div>
      </aside>

      <main className="flex-1 p-8 lg:p-10 overflow-y-auto relative">
        <div className={activeTab === 'dashboard' ? 'block' : 'hidden'}>
          <Dashboard />
        </div>
        
        <div className={activeTab === 'iceberg-metrics' ? 'block' : 'hidden'}>
          <IcebergMetrics />
        </div>
        
        <div className={activeTab === 'ai-copilot' ? 'block' : 'hidden'}>
          <AICopilot />
        </div>

        {/* 3. Add this rendering block: */}
        <div className={activeTab === 'occ-simulation' ? 'block' : 'hidden'}>
          <OCCVisualizer />
        </div>
      </main>
    </div>
  );
}
export default App;