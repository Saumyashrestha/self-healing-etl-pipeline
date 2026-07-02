import { useEffect, useState } from 'react';
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { SSEClientTransport } from "@modelcontextprotocol/sdk/client/sse.js";
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, ReferenceLine } from 'recharts';

interface HistoryData {
  batch: number; files: number; snapshots: number; avg_file_size_kb: number; manifests: number;
}

export default function Dashboard() {
  const [data, setData] = useState<HistoryData[]>([]);
  const [audit, setAudit] = useState({
    before: { files: 0, snapshots: 0, avg_file_size_kb: 0, manifests: 0 },
    after: { files: 0, snapshots: 0, avg_file_size_kb: 0, manifests: 0 }
  });
  const [isSimulating, setIsSimulating] = useState(false);
  const [isRefreshing, setIsRefreshing] = useState(false);
  const [simLogs, setSimLogs] = useState<string[]>([
    "System ready. Waiting to trigger incremental load simulation...", "Target table: local.db.orders"
  ]);

  const fetchMetrics = async () => {
    setIsRefreshing(true);
    try {
      const transport = new SSEClientTransport(new URL("http://127.0.0.1:8000/sse"));
      const client = new Client({ name: "Dashboard", version: "1.0.0" }, { capabilities: {} });
      await client.connect(transport);
      const result = await client.callTool({ name: "get_table_history", arguments: { table_name: "orders" } });
      const history: HistoryData[] = JSON.parse((result.content as any)[0].text);
      setData(history);
      if (history.length > 0) {
        setAudit({ before: history.reduce((prev, curr) => (prev.files > curr.files) ? prev : curr), after: history[history.length - 1] });
      }
    } catch (e) { console.error(e); } finally { setIsRefreshing(false); }
  };

  useEffect(() => { fetchMetrics(); }, []);

  const handleSimulation = async () => {
    setIsSimulating(true);
    setSimLogs(prev => [...prev, "> Initiating 50-batch incremental load...", "> Warning: Fragmenting table..."]);
    try {
      const transport = new SSEClientTransport(new URL("http://127.0.0.1:8000/sse"));
      const simClient = new Client({ name: "Dashboard-Sim", version: "1.0.0" }, { capabilities: {} });
      await simClient.connect(transport);
      const result = await simClient.callTool({ name: "run_incremental_load", arguments: { batches: 50 } });
      setSimLogs(prev => [...prev, `> Server: ${(result.content as any)[0].text}`]);
    } catch (e) { setSimLogs(prev => [...prev, `> Error: ${e}`]); } finally { setIsSimulating(false); }
  };

  const currentTotalStorageMB = ((audit.after.files * audit.after.avg_file_size_kb) / 1024).toFixed(2);
  const healthScore = audit.after.files < 10 ? 98 : 45;

  return (
    <div className="animate-in fade-in duration-500 max-w-7xl mx-auto">
      <header className="mb-8 flex justify-between items-end">
        <div>
          <h2 className="text-2xl font-bold text-slate-900">System Dashboard</h2>
          <p className="text-slate-500 text-sm mt-1">Monitoring real-time Iceberg telemetry.</p>
        </div>
        <button onClick={fetchMetrics} disabled={isRefreshing} className="px-4 py-2 bg-white border border-slate-200 rounded-lg text-sm font-medium hover:bg-slate-50">
          {isRefreshing ? 'Syncing...' : 'Refresh Metrics'}
        </button>
      </header>

      {/* METRIC CARDS */}
      <div className="grid grid-cols-12 gap-6 mb-6">
        <div className="col-span-12 md:col-span-4 bg-white p-6 rounded-2xl border shadow-sm flex flex-col justify-center items-center">
          <h3 className="text-xs font-bold text-slate-400 uppercase">Overall Health</h3>
          <div className={`text-6xl font-black ${healthScore > 50 ? 'text-emerald-500' : 'text-red-500'}`}>{healthScore}%</div>
        </div>
        <div className="col-span-12 md:col-span-8 grid grid-cols-3 gap-6">
           <div className="bg-white p-6 rounded-2xl border shadow-sm">
             <h3 className="text-sm font-semibold text-slate-500">Total Storage</h3>
             <p className="text-3xl font-bold">{currentTotalStorageMB} <span className="text-lg text-slate-400">MB</span></p>
           </div>
           <div className="bg-white p-6 rounded-2xl border shadow-sm">
             <h3 className="text-sm font-semibold text-slate-500">Manifests</h3>
             <p className="text-3xl font-bold">{audit.after.manifests}</p>
           </div>
           <div className="bg-white p-6 rounded-2xl border shadow-sm">
             <h3 className="text-sm font-semibold text-slate-500">Data Files</h3>
             <p className="text-3xl font-bold">{audit.after.files}</p>
           </div>
        </div>
      </div>

      {/* ENGINE & ANALYTICS */}
      <div className="grid grid-cols-12 gap-6">
        <div className="col-span-12 lg:col-span-4 bg-white p-6 rounded-2xl border shadow-sm flex flex-col justify-between">
          <h3 className="font-bold text-slate-800">Pipeline Simulation</h3>
          <button onClick={handleSimulation} disabled={isSimulating} className="w-full py-3 mt-4 bg-blue-600 text-white rounded-lg font-bold">
            {isSimulating ? 'Deploying...' : 'Trigger 50-Batch Load'}
          </button>
          <div className="mt-4 bg-[#0F172A] p-4 rounded-lg font-mono text-xs text-emerald-400 h-32 overflow-y-auto">
            {simLogs.map((log, i) => <div key={i}>{log}</div>)}
          </div>
        </div>
        
        <div className="col-span-12 lg:col-span-8 bg-white p-6 rounded-2xl border shadow-sm">
          <ResponsiveContainer width="100%" height={250}>
            <LineChart data={data}>
              <CartesianGrid strokeDasharray="3 3" vertical={false} />
              <XAxis dataKey="batch" />
              <YAxis />
              <Tooltip />
              <ReferenceLine x={50} stroke="#EF4444" strokeDasharray="3 3" />
              <Line type="monotone" dataKey="files" stroke="#2563EB" strokeWidth={3} />
            </LineChart>
          </ResponsiveContainer>
        </div>
      </div>
    </div>
  );
}