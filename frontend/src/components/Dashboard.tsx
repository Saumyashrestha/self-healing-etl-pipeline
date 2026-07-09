import { useEffect, useState } from 'react';
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { SSEClientTransport } from "@modelcontextprotocol/sdk/client/sse.js";
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, ReferenceLine, Legend } from 'recharts';

interface HistoryData {
  batch: number; 
  files: number; 
  snapshots: number; 
  avg_file_size_kb: number; 
  manifests: number;
  delete_files: number | undefined;     
  delete_file_avg_kb: number | undefined;
  health_score: number | undefined;
}

interface AuditShape {
  files: number;
  snapshots: number;
  avg_file_size_kb: number;
  manifests: number;
  delete_files: number | undefined;
  delete_file_avg_kb: number | undefined;
  health_score: number | undefined;
}

const defaultAudit: { before: AuditShape; after: AuditShape } = { 
  before: { files: 0, snapshots: 0, avg_file_size_kb: 0, manifests: 0, delete_files: 0, delete_file_avg_kb: 0, health_score: 100 }, 
  after: { files: 0, snapshots: 0, avg_file_size_kb: 0, manifests: 0, delete_files: 0, delete_file_avg_kb: 0, health_score: 100 }  
};

// Accept the setActiveTab prop from App.tsx
export default function Dashboard({ setActiveTab }: { setActiveTab: (tab: string) => void }) {
  const [ordersData, setOrdersData] = useState<HistoryData[]>([]);
  const [ordersAudit, setOrdersAudit] = useState(defaultAudit);
  
  const [itemsData, setItemsData] = useState<HistoryData[]>([]);
  const [itemsAudit, setItemsAudit] = useState(defaultAudit);

  const [isSimulating, setIsSimulating] = useState(false);
  const [isRefreshing, setIsRefreshing] = useState(false);
  const [simLogs, setSimLogs] = useState<string[]>([
    "System ready. Waiting to trigger incremental load simulation...", "Target tables: local.db.orders & local.db.order_items"
  ]);

  // --- NEW: Proactive Agent Alert State ---
  const [activeAlert, setActiveAlert] = useState<string | null>(null);

  // --- NEW: Listen for alerts from the backend ---
  useEffect(() => {
    const eventSource = new EventSource("http://127.0.0.1:8001/api/agent-notifications");
    
    eventSource.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data);
        setActiveAlert(`Proactive Alert: ${data.target_table} requires attention. Click to view in chat.`);
      } catch (err) {
         setActiveAlert("Proactive Alert detected. Click to view in chat.");
      }
    };

    return () => eventSource.close();
  }, []);

  // --- NEW: Switch tabs when banner is clicked ---
  const goToChat = () => {
    setActiveAlert(null); 
    setActiveTab('ai-copilot'); // Switches the tab in App.tsx!
  };

  const fetchMetrics = async () => {
    setIsRefreshing(true);
    try {
      const transport = new SSEClientTransport(new URL("http://127.0.0.1:8000/sse"));
      const client = new Client({ name: "Dashboard", version: "1.0.0" }, { capabilities: {} });
      await client.connect(transport);

      const resultOrders = await client.callTool({ name: "get_table_history", arguments: { table_name: "orders" } });
      const historyOrders: HistoryData[] = JSON.parse((resultOrders.content as any)[0].text);
      setOrdersData(historyOrders);
      if (historyOrders.length > 0) {
        setOrdersAudit({ before: historyOrders.reduce((prev, curr) => (prev.files > curr.files) ? prev : curr), after: historyOrders[historyOrders.length - 1] });
      }

      const resultItems = await client.callTool({ name: "get_table_history", arguments: { table_name: "order_items" } });
      const historyItems: HistoryData[] = JSON.parse((resultItems.content as any)[0].text);
      setItemsData(historyItems);
      if (historyItems.length > 0) {
        setItemsAudit({ before: historyItems.reduce((prev, curr) => (prev.files > curr.files) ? prev : curr), after: historyItems[historyItems.length - 1] });
      }
    } catch (e) { 
      console.error(e); 
    } finally { 
      setIsRefreshing(false); 
    }
  };

  useEffect(() => { fetchMetrics(); }, []);

  const handleSimulation = async () => {
    setIsSimulating(true);
    setSimLogs(prev => [...prev, "> Initiating 50-batch incremental load...", "> Warning: Fragmenting tables..."]);
    try {
      const transport = new SSEClientTransport(new URL("http://127.0.0.1:8000/sse"));
      const simClient = new Client({ name: "Dashboard-Sim", version: "1.0.0" }, { capabilities: {} });
      await simClient.connect(transport);
      const result = await simClient.callTool({ name: "run_incremental_load", arguments: { batches: 50 } });
      setSimLogs(prev => [...prev, `> Server: ${(result.content as any)[0].text}`]);
    } catch (e) { 
      setSimLogs(prev => [...prev, `> Error: ${e}`]); 
    } finally { 
      setIsSimulating(false); 
    }
  };

  const combinedChartData = ordersData.map((od, i) => ({
    batch: od.batch,
    orders_files: od.files,
    orders_snapshots: od.snapshots,
    items_files: itemsData[i] ? itemsData[i].files : 0,
    items_snapshots: itemsData[i] ? itemsData[i].snapshots : 0
  }));

  const renderTableMetrics = (title: string, audit: typeof defaultAudit) => {
    const delFiles = audit.after.delete_files || 0;
    const delAvgSize = audit.after.delete_file_avg_kb || 0;
    const dataStorageMB = (audit.after.files * audit.after.avg_file_size_kb) / 1024;
    const deleteStorageMB = (delFiles * delAvgSize) / 1024;
    const totalStorageMB = (dataStorageMB + deleteStorageMB).toFixed(2);
    const bloatRatio = parseFloat(totalStorageMB) > 0 ? ((deleteStorageMB / parseFloat(totalStorageMB)) * 100).toFixed(0) : 0;
    const totalParquet = audit.after.files + delFiles;
    const healthScore = audit.after.health_score || 0;
    const healthColor = healthScore > 65 ? 'text-emerald-500' : healthScore > 40 ? 'text-amber-500' : 'text-red-500';
    
    return (
      <div className="bg-white p-6 rounded-2xl border shadow-sm flex flex-col gap-4">
        <div className="flex justify-between items-center border-b border-slate-100 pb-3">
          <h3 className="text-sm font-bold text-slate-800 uppercase tracking-wider">{title}</h3>
          <div className={`text-xl font-black ${healthColor}`}>{healthScore}% Health</div>
        </div>
        
        <div className="grid grid-cols-2 gap-4 mb-2">
          <div className="bg-slate-50 p-3 rounded-lg border border-slate-100">
            <h4 className="text-xs font-semibold text-slate-500 mb-1">True Disk Footprint</h4>
            <p className="text-3xl font-bold">{totalStorageMB} <span className="text-sm text-slate-400">MB</span></p>
            {delFiles > 0 && <p className="text-xs text-red-500 mt-1 font-medium">{bloatRatio}% Delete Bloat</p>}
          </div>
          <div className="bg-slate-50 p-3 rounded-lg border border-slate-100">
            <h4 className="text-xs font-semibold text-slate-500 mb-1">Total Parquet Files</h4>
            <p className="text-3xl font-bold">{totalParquet}</p>
            {audit.after.files > 5 ? <p className="text-xs text-red-500 mt-1 font-medium">Needs Compaction</p> : <p className="text-xs text-emerald-500 mt-1 font-medium">Optimized</p>}
          </div>
        </div>

        <div className="grid grid-cols-4 gap-4 mt-2">
          <div><h4 className="text-xs font-semibold text-slate-500 mb-1">Data Files</h4><p className="text-xl font-bold text-slate-700">{audit.after.files}</p><p className="text-[10px] text-slate-400 mt-0.5">{dataStorageMB.toFixed(2)} MB</p></div>
          <div><h4 className="text-xs font-semibold text-slate-500 mb-1">Delete Files</h4><p className="text-xl font-bold text-slate-700">{delFiles}</p><p className="text-[10px] text-slate-400 mt-0.5">{deleteStorageMB.toFixed(2)} MB</p></div>
          <div><h4 className="text-xs font-semibold text-slate-500 mb-1">Snapshots</h4><p className="text-xl font-bold text-slate-700">{audit.after.snapshots}</p></div>
          <div><h4 className="text-xs font-semibold text-slate-500 mb-1">Manifests</h4><p className="text-xl font-bold text-slate-700">{audit.after.manifests}</p></div>
        </div>
      </div>
    );
  };

  return (
    <div className="animate-in fade-in duration-500 max-w-7xl mx-auto">
      
      <header className="mb-8 flex justify-between items-end">
        <div>
          <h2 className="text-2xl font-bold text-slate-900">System Dashboard</h2>
          <p className="text-slate-500 text-sm mt-1">Monitoring real-time Iceberg telemetry for multiple tables.</p>
        </div>
        <button onClick={fetchMetrics} disabled={isRefreshing} className="px-4 py-2 bg-white border border-slate-200 rounded-lg text-sm font-medium hover:bg-slate-50 transition-colors">
          {isRefreshing ? 'Syncing...' : 'Refresh Metrics'}
        </button>
      </header>

      {/* --- NEW: THE CLICKABLE ALERT BANNER --- */}
      {activeAlert && (
        <div 
          onClick={goToChat}
          className="mb-6 w-full bg-gradient-to-r from-rose-900 to-rose-700 border border-rose-500 rounded-lg p-4 cursor-pointer hover:shadow-lg hover:shadow-rose-500/20 transition-all flex items-center justify-between group"
        >
          <div className="flex items-center gap-3">
            <span className="text-2xl animate-bounce">🚨</span>
            <p className="text-white font-medium text-sm">{activeAlert}</p>
          </div>
          <span className="text-rose-200 text-xs font-bold uppercase tracking-wider group-hover:text-white transition-colors">
            Open Chat &rarr;
          </span>
        </div>
      )}

      <div className="grid grid-cols-1 xl:grid-cols-2 gap-6 mb-6">
        {renderTableMetrics("Orders Table", ordersAudit)}
        {renderTableMetrics("Order Items Table", itemsAudit)}
      </div>

      <div className="grid grid-cols-12 gap-6 mb-6">
        <div className="col-span-12 lg:col-span-4 bg-white p-6 rounded-2xl border shadow-sm flex flex-col justify-between">
          <div>
             <h3 className="font-bold text-slate-800 mb-2">Pipeline Simulation</h3>
             <p className="text-sm text-slate-500 leading-relaxed">Trigger a PySpark streaming job to ingest 50 micro-batches into both fact tables concurrently.</p>
          </div>
          <button onClick={handleSimulation} disabled={isSimulating} className="w-full py-3 mt-6 bg-blue-600 text-white rounded-lg font-bold hover:bg-blue-700 transition-colors">
            {isSimulating ? 'Deploying...' : 'Trigger 50-Batch Load'}
          </button>
        </div>
        
        <div className="col-span-12 lg:col-span-8 bg-[#0F172A] p-4 rounded-2xl border border-slate-700 shadow-inner flex flex-col h-48">
          <div className="flex items-center gap-2 mb-3 border-b border-slate-700/50 pb-3">
             <div className="w-2.5 h-2.5 rounded-full bg-red-500"></div>
             <div className="w-2.5 h-2.5 rounded-full bg-amber-500"></div>
             <div className="w-2.5 h-2.5 rounded-full bg-emerald-500"></div>
             <span className="text-slate-500 ml-2 text-xs font-mono">pyspark_streaming.log</span>
          </div>
          <div className="flex-1 overflow-y-auto text-emerald-400/90 space-y-1.5 text-xs font-mono">
            {simLogs.map((log, i) => <div key={i}>{log}</div>)}
            {isSimulating && <div className="animate-pulse text-slate-500 mt-2">_ writing batches...</div>}
          </div>
        </div>
      </div>

      <div className="grid grid-cols-12 gap-6">
        <div className="col-span-12 lg:col-span-8 bg-white p-6 rounded-2xl border shadow-sm">
          <h3 className="text-sm font-semibold text-slate-800 mb-6">Fragmentation Rates (File Counts)</h3>
          <ResponsiveContainer width="100%" height={250}>
            <LineChart data={combinedChartData} margin={{ top: 10, right: 30, left: 10, bottom: 20 }}>
              <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#E2E8F0" />
              <XAxis dataKey="batch" axisLine={false} tickLine={false} tick={{fill: '#94A3B8', fontSize: 12}} dy={10} label={{ value: "Ingestion Micro-Batches", position: "insideBottom", offset: -15, fill: "#64748b", fontSize: 12, fontWeight: 500 }} />
              <YAxis axisLine={false} tickLine={false} tick={{fill: '#94A3B8', fontSize: 12}} dx={-10} label={{ value: "Count", angle: -90, position: "insideLeft", offset: -5, fill: "#64748b", fontSize: 12, fontWeight: 500 }} />
              <Tooltip contentStyle={{ borderRadius: '8px', border: '1px solid #E2E8F0', boxShadow: '0 4px 6px -1px rgb(0 0 0 / 0.05)' }} />
              <Legend verticalAlign="top" height={36}/>
              <ReferenceLine x={50} stroke="#EF4444" strokeDasharray="3 3" />
              <Line type="monotone" name="Orders (Files)" dataKey="orders_files" stroke="#0F766E" strokeWidth={6} dot={false} activeDot={{r: 6}} />
              <Line type="monotone" name="Orders (Snapshots)" dataKey="orders_snapshots" stroke="#0F766E" strokeWidth={4} strokeDasharray="4 4" opacity={0.5} dot={false} />
              <Line type="monotone" name="Order Items (Files)" dataKey="items_files" stroke="#EA580C" strokeWidth={2} dot={false} activeDot={{r: 6}} />
              <Line type="monotone" name="Order Items (Snapshots)" dataKey="items_snapshots" stroke="#EA580C" strokeWidth={2} strokeDasharray="4 4" dot={false} />
            </LineChart>
          </ResponsiveContainer>
        </div>

        <div className="col-span-12 lg:col-span-4 bg-white p-6 rounded-2xl border shadow-sm flex flex-col">
           <h3 className="text-sm font-semibold text-slate-800 mb-4 border-b border-slate-100 pb-3">Optimization Audit</h3>
           <div className="flex flex-col flex-1 justify-around">
             <div>
               <p className="text-xs font-bold text-blue-600 uppercase tracking-wider mb-2">Orders Table</p>
               <div className="flex justify-between text-sm mb-1.5"><span className="text-slate-500">Peak Bloat (Files):</span> <span className="font-mono text-red-500 font-medium">{ordersAudit.before.files + (ordersAudit.before.delete_files || 0)}</span></div>
               <div className="flex justify-between text-sm"><span className="text-slate-500">Current State:</span> <span className="font-mono text-emerald-500 font-medium">{ordersAudit.after.files + (ordersAudit.after.delete_files || 0)}</span></div>
             </div>
             <div className="w-full h-px bg-slate-100 my-2"></div>
             <div>
               <p className="text-xs font-bold text-purple-600 uppercase tracking-wider mb-2">Order Items Table</p>
               <div className="flex justify-between text-sm mb-1.5"><span className="text-slate-500">Peak Bloat (Files):</span> <span className="font-mono text-red-500 font-medium">{itemsAudit.before.files + (itemsAudit.before.delete_files || 0)}</span></div>
               <div className="flex justify-between text-sm"><span className="text-slate-500">Current State:</span> <span className="font-mono text-emerald-500 font-medium">{itemsAudit.after.files + (itemsAudit.after.delete_files || 0)}</span></div>
             </div>
           </div>
        </div>
      </div>
    </div>
  );
}