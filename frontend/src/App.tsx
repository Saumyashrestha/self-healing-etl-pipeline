import { useEffect, useState } from 'react';
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { SSEClientTransport } from "@modelcontextprotocol/sdk/client/sse.js";
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, ReferenceLine } from 'recharts';

// The shape of our historical data
interface HistoryData {
  batch: number;
  files: number;
  snapshots: number;
}

function App() {
  const [data, setData] = useState<HistoryData[]>([]);
  const [mcpClient, setMcpClient] = useState<Client | null>(null);
  
  // Current Table State
  const [currentFiles, setCurrentFiles] = useState(0);
  const [currentSnapshots, setCurrentSnapshots] = useState(0);
  
  // Alert Threshold
  const FILE_THRESHOLD = 100;
  const isAlertActive = currentFiles > FILE_THRESHOLD;

  useEffect(() => {
    async function init() {
      try {
        const transport = new SSEClientTransport(new URL("http://127.0.0.1:8000/sse"));
        const client = new Client({ name: "Dashboard", version: "1.0.0" }, { capabilities: {} });
        await client.connect(transport);
        setMcpClient(client);

        // Fetch historical data from Python backend
        const result = await client.callTool({ 
          name: "get_table_history", 
          arguments: { table_name: "orders" } 
        });
        
        const contentArray = result.content as { text: string }[];
        const parsedData = JSON.parse(contentArray[0].text);
        
        setData(parsedData);
        
        // Set current metrics based on the most recent batch
        if (parsedData.length > 0) {
          const latest = parsedData[parsedData.length - 1];
          setCurrentFiles(latest.files);
          setCurrentSnapshots(latest.snapshots);
        }
      } catch (e) {
        console.error("Failed to init:", e);
      }
    }
    init();
  }, []);

  const triggerMaintenance = async () => {
    if (!mcpClient) return;
    alert("Triggering maintenance... (This will be hooked up to the LLM agent next!)");
  };

  return (
    <div className="p-8 bg-slate-50 min-h-screen font-sans">
      <header className="mb-8 flex justify-between items-center">
        <div>
          <h1 className="text-3xl font-bold text-slate-800">Lakehouse Health Monitor</h1>
          <p className="text-slate-500 mt-1">Orders Table - Current Status</p>
        </div>
        <button 
          onClick={triggerMaintenance}
          className={`px-4 py-2 rounded-lg font-semibold text-white shadow-sm transition-colors ${
            isAlertActive ? 'bg-red-600 hover:bg-red-700 animate-pulse' : 'bg-slate-800 hover:bg-slate-700'
          }`}
        >
          Force Manual Maintenance
        </button>
      </header>
      
      {/* 1. Current Warehouse Health Metrics (Top Row) */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-4 mb-8">
        
        <div className="bg-white p-5 rounded-xl shadow-sm border border-slate-200">
          <p className="text-sm font-medium text-slate-500 uppercase tracking-wider">Total Snapshots</p>
          <p className="text-3xl font-bold text-slate-800 mt-2">{currentSnapshots}</p>
        </div>
        
        <div className={`p-5 rounded-xl shadow-sm border ${
          isAlertActive ? 'bg-red-50 border-red-200' : 'bg-white border-slate-200'
        }`}>
          <p className={`text-sm font-medium uppercase tracking-wider ${isAlertActive ? 'text-red-700' : 'text-slate-500'}`}>
            Data File Count
          </p>
          <div className="flex items-end gap-2 mt-2">
            <p className={`text-3xl font-bold ${isAlertActive ? 'text-red-700' : 'text-slate-800'}`}>
              {currentFiles}
            </p>
            <p className={`text-sm mb-1 ${isAlertActive ? 'text-red-600' : 'text-slate-400'}`}>
              / {FILE_THRESHOLD} limit
            </p>
          </div>
        </div>

        <div className={`p-5 rounded-xl shadow-sm border ${
          isAlertActive ? 'bg-red-600 border-red-700 text-white' : 'bg-green-100 border-green-200 text-green-800'
        }`}>
          <p className="text-sm font-medium uppercase tracking-wider opacity-80">System Status</p>
          <p className="text-xl font-bold mt-2">
            {isAlertActive ? '⚠️ Critical Bloat' : '✅ Healthy'}
          </p>
        </div>

        <div className="bg-white p-5 rounded-xl shadow-sm border border-slate-200">
          <p className="text-sm font-medium text-slate-500 uppercase tracking-wider">Last Maintenance</p>
          <p className="text-lg font-bold text-slate-800 mt-2">Batch 50</p>
          <p className="text-xs text-slate-400 mt-1">via rewriteDataFiles</p>
        </div>
      </div>

      {/* 2. Charts and Logs (Bottom Row) */}
      <div className="grid grid-cols-1 xl:grid-cols-3 gap-8">
        
        {/* Trend Chart (Takes 2 columns) */}
        <div className="xl:col-span-2 bg-white p-6 rounded-xl shadow-sm border border-slate-200">
          <div className="mb-4">
            <h2 className="text-lg font-bold text-slate-800">Pre vs. Post-Maintenance Delta</h2>
            <p className="text-sm text-slate-500">File accumulation over time and compaction events</p>
          </div>
          <div className="h-80">
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={data} margin={{ top: 10, right: 30, left: 0, bottom: 0 }}>
                <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#e2e8f0" />
                <XAxis dataKey="batch" stroke="#64748b" fontSize={12} tickMargin={10} />
                <YAxis stroke="#64748b" fontSize={12} tickMargin={10} />
                <Tooltip 
                  contentStyle={{ borderRadius: '8px', border: 'none', boxShadow: '0 4px 6px -1px rgb(0 0 0 / 0.1)' }}
                />
                <ReferenceLine x={50} stroke="#ef4444" strokeDasharray="3 3" label={{ position: 'top', value: 'Compaction Triggered', fill: '#ef4444', fontSize: 12 }} />
                <Line 
                  type="monotone" 
                  dataKey="files" 
                  name="Data Files"
                  stroke="#3b82f6" 
                  strokeWidth={3} 
                  dot={{ r: 4, strokeWidth: 2 }}
                  activeDot={{ r: 6 }} 
                />
              </LineChart>
            </ResponsiveContainer>
          </div>
        </div>

        {/* Maintenance Action Log (Takes 1 column) */}
        <div className="bg-white rounded-xl shadow-sm border border-slate-200 overflow-hidden flex flex-col h-full">
          <div className="p-4 border-b border-slate-100 bg-slate-50">
            <h2 className="font-bold text-slate-800">Maintenance History</h2>
          </div>
          <div className="p-4 flex-1 overflow-y-auto">
            <div className="relative pl-4 border-l-2 border-slate-200 space-y-6">
              
              <div className="relative">
                <div className="absolute -left-[21px] bg-green-500 h-3 w-3 rounded-full border-2 border-white"></div>
                <p className="text-xs text-slate-400 font-mono">Batch 51</p>
                <p className="font-semibold text-slate-700 text-sm mt-1">System Healthy</p>
                <p className="text-xs text-slate-500 mt-1">File count reduced to 5.</p>
              </div>

              <div className="relative">
                <div className="absolute -left-[21px] bg-blue-500 h-3 w-3 rounded-full border-2 border-white"></div>
                <p className="text-xs text-slate-400 font-mono">Batch 50</p>
                <p className="font-semibold text-slate-700 text-sm mt-1">Agent ran rewriteDataFiles</p>
                <p className="text-xs text-slate-500 mt-1">Compacted 295 small files.</p>
              </div>

              <div className="relative">
                <div className="absolute -left-[21px] bg-red-500 h-3 w-3 rounded-full border-2 border-white"></div>
                <p className="text-xs text-slate-400 font-mono">Batch 50</p>
                <p className="font-semibold text-slate-700 text-sm mt-1">Threshold Exceeded</p>
                <p className="text-xs text-slate-500 mt-1">Files hit 300. Alert triggered.</p>
              </div>

            </div>
          </div>
        </div>
        
      </div>
    </div>
  );
}

export default App;