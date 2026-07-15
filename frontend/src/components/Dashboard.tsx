import { useEffect, useState, useRef } from 'react';
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { SSEClientTransport } from "@modelcontextprotocol/sdk/client/sse.js";
import { AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend } from 'recharts';

interface HistoryData {
  batch: number;
  files: number;
  data_files: number | undefined;
  snapshots: number;
  avg_file_size_kb: number;
  manifests: number;
  delete_files: number | undefined;
  delete_file_avg_kb: number | undefined;
  health_score: number | undefined;
  timestamp?: number;
  event?: string;
}

interface AuditShape {
  files: number;
  data_files: number | undefined;
  snapshots: number;
  avg_file_size_kb: number;
  manifests: number;
  delete_files: number | undefined;
  delete_file_avg_kb: number | undefined;
  health_score: number | undefined;
}

const defaultAudit: { before: AuditShape; after: AuditShape } = {
  before: { files: 0, data_files: 0, snapshots: 0, avg_file_size_kb: 0, manifests: 0, delete_files: 0, delete_file_avg_kb: 0, health_score: 100 },
  after: { files: 0, data_files: 0, snapshots: 0, avg_file_size_kb: 0, manifests: 0, delete_files: 0, delete_file_avg_kb: 0, health_score: 100 }
};

export default function Dashboard({ setActiveTab }: { setActiveTab: (tab: string) => void }) {
  const [ordersData, setOrdersData] = useState<HistoryData[]>([]);
  const [ordersAudit, setOrdersAudit] = useState(defaultAudit);

  const [itemsData, setItemsData] = useState<HistoryData[]>([]);
  const [itemsAudit, setItemsAudit] = useState(defaultAudit);

  const [isSimulating, setIsSimulating] = useState(false);
  const [isRefreshing, setIsRefreshing] = useState(false);
  const [activeAlert, setActiveAlert] = useState<string | null>(null);

  // --- NEW: Table Toggle State ---
  const [selectedTable, setSelectedTable] = useState<'orders' | 'order_items'>('orders');

  const pollIntervalRef = useRef<number | null>(null);
  const mcpClientRef = useRef<Client | null>(null);   // <-- ADD THIS LINE

  const getMcpClient = async () => {                  // <-- ADD THIS WHOLE FUNCTION
    if (mcpClientRef.current) return mcpClientRef.current;
    const transport = new SSEClientTransport(new URL("http://127.0.0.1:8000/sse"));
    const client = new Client({ name: "Dashboard", version: "1.0.0" }, { capabilities: {} });
    await client.connect(transport);
    mcpClientRef.current = client;
    return client;
  };

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

  const goToChat = () => {
    setActiveAlert(null);
    setActiveTab('ai-copilot');
  };

  const fetchMetrics = async () => {
    setIsRefreshing(true);
    try {
      const client = await getMcpClient();

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

  useEffect(() => {
    return () => {
      if (pollIntervalRef.current) clearInterval(pollIntervalRef.current);
    };
  }, []);

  const checkPipelineStatus = async () => {
    try {
      const client = await getMcpClient();
      const result = await client.callTool({ name: "get_pipeline_status", arguments: {} });
      const status = JSON.parse((result.content as any)[0].text);

      if (!status.running) {
        setIsSimulating(false);
        if (pollIntervalRef.current) {
          clearInterval(pollIntervalRef.current);
          pollIntervalRef.current = null;
        }
        fetchMetrics();
      }
    } catch (e) {
      console.error(e);
    }
  };

  const handleSimulation = async () => {
    if (isSimulating) return;
    setIsSimulating(true);
    try {
      const client = await getMcpClient();
      await client.callTool({ name: "run_incremental_load", arguments: { batches: 50 } });

      pollIntervalRef.current = window.setInterval(checkPipelineStatus, 4000);
    } catch (e) {
      console.error(e);
      setIsSimulating(false);
    }
  };

  // --- NEW: Dynamic Graph Data Processing (Detects Compaction Events) ---
  const activeDataset = selectedTable === 'orders' ? ordersData : itemsData;
  const chartData = activeDataset.map((dataPoint) => {
    const timeLabel = dataPoint.timestamp
      ? new Date(dataPoint.timestamp * 1000).toLocaleTimeString([], {
        hour: '2-digit', minute: '2-digit', second: '2-digit'
      })
      : '';

    return {
      eventName: timeLabel || 'N/A',
      files: dataPoint.files,
      snapshots: dataPoint.snapshots
    };
  });

  const renderTableMetrics = (title: string, audit: typeof defaultAudit) => {
    const delFiles = audit.after.delete_files || 0;
    const delAvgSize = audit.after.delete_file_avg_kb || 0;
    const dataFilesCount = audit.after.data_files ?? 0;   // pure data-file count, not combined
    const dataStorageMB = (dataFilesCount * audit.after.avg_file_size_kb) / 1024;
    const deleteStorageMB = (delFiles * delAvgSize) / 1024;
    const totalStorageMB = (dataStorageMB + deleteStorageMB).toFixed(2);
    const bloatRatio = parseFloat(totalStorageMB) > 0 ? ((deleteStorageMB / parseFloat(totalStorageMB)) * 100).toFixed(0) : 0;
    const totalParquet = audit.after.files;   // audit.after.files is ALREADY the combined total from history_logger.py
    // const healthScore = audit.after.health_score || 0;
    // const healthColor = healthScore > 65 ? 'text-emerald-500' : healthScore > 40 ? 'text-amber-500' : 'text-red-500';

    return (
      // TIGHTENED UI: p-6, rounded-xl, smaller text sizing
      <div className="bg-white p-6 rounded-xl border shadow-sm flex flex-col gap-5 w-full">
        <div className="flex justify-between items-center border-b border-slate-100 pb-3">
          <h3 className="text-sm font-bold text-slate-800 uppercase tracking-wider">{title}</h3>
          {/* <div className={`text-xl font-black ${healthColor}`}>{healthScore}% Health</div> */}
        </div>

        <div className="grid grid-cols-2 gap-4 mb-2">
          <div className="bg-slate-50 p-4 rounded-lg border border-slate-100">
            <h4 className="text-xs font-semibold text-slate-500 mb-1">True Disk Footprint</h4>
            <p className="text-3xl font-bold text-slate-800">{totalStorageMB} <span className="text-sm text-slate-400">MB</span></p>
            {delFiles > 0 && <p className="text-xs text-red-500 mt-1 font-medium">{bloatRatio}% Delete Bloat</p>}
          </div>
          <div className="bg-slate-50 p-4 rounded-lg border border-slate-100">
            <h4 className="text-xs font-semibold text-slate-500 mb-1">Total Parquet Files</h4>
            <p className="text-3xl font-bold text-slate-800">{totalParquet}</p>
            {dataFilesCount > 5 ? <p className="text-xs text-red-500 mt-1 font-medium">Needs Compaction</p> : <p className="text-xs text-emerald-500 mt-1 font-medium">Optimized</p>}
          </div>
        </div>

        <div className="grid grid-cols-4 gap-4 mt-1">
          <div><h4 className="text-xs font-semibold text-slate-500 mb-1">Data Files</h4><p className="text-xl font-bold text-slate-700">{dataFilesCount}</p><p className="text-[10px] text-slate-400 mt-0.5">{dataStorageMB.toFixed(2)} MB</p></div>
          <div><h4 className="text-xs font-semibold text-slate-500 mb-1">Delete Files</h4><p className="text-xl font-bold text-slate-700">{delFiles}</p><p className="text-[10px] text-slate-400 mt-0.5">{deleteStorageMB.toFixed(2)} MB</p></div>
          <div><h4 className="text-xs font-semibold text-slate-500 mb-1">Snapshots</h4><p className="text-xl font-bold text-slate-700">{audit.after.snapshots}</p></div>
          <div><h4 className="text-xs font-semibold text-slate-500 mb-1">Manifests</h4><p className="text-xl font-bold text-slate-700">{audit.after.manifests}</p></div>
        </div>
      </div>
    );
  };

  return (
    <div className="animate-in fade-in duration-500 max-w-7xl mx-auto">

      <header className="mb-6 flex justify-between items-end">
        <div>
          <h2 className="text-2xl font-bold text-slate-900">System Dashboard</h2>
          <p className="text-slate-500 text-sm mt-1">Monitoring real-time Iceberg telemetry.</p>
        </div>
        <button onClick={fetchMetrics} disabled={isRefreshing} className="px-4 py-2 bg-white border border-slate-200 rounded-lg text-sm font-medium hover:bg-slate-50 transition-colors">
          {isRefreshing ? 'Syncing...' : 'Refresh Metrics'}
        </button>
      </header>

      {/* Alert Banner */}
      {activeAlert && (
        <div onClick={goToChat} className="mb-6 w-full bg-gradient-to-r from-rose-900 to-rose-700 border border-rose-500 rounded-lg p-4 cursor-pointer hover:shadow-lg hover:shadow-rose-500/20 transition-all flex items-center justify-between group">
          <div className="flex items-center gap-3">
            <span className="text-2xl animate-bounce">🚨</span>
            <p className="text-white font-medium text-sm">{activeAlert}</p>
          </div>
          <span className="text-rose-200 text-xs font-bold uppercase tracking-wider group-hover:text-white transition-colors">
            Open Chat &rarr;
          </span>
        </div>
      )}

      {/* --- COMMAND BAR --- */}
      <div className="flex flex-wrap items-center justify-between gap-4 mb-6 w-full">
        <div className="flex bg-slate-200 p-1 rounded-lg w-fit">
          <button
            onClick={() => setSelectedTable('orders')}
            className={`px-4 py-2 rounded-md text-sm font-bold transition-all ${selectedTable === 'orders' ? 'bg-white shadow text-blue-600' : 'text-slate-500 hover:text-slate-700'
              }`}
          >
            Orders Table
          </button>
          <button
            onClick={() => setSelectedTable('order_items')}
            className={`px-4 py-2 rounded-md text-sm font-bold transition-all ${selectedTable === 'order_items' ? 'bg-white shadow text-purple-600' : 'text-slate-500 hover:text-slate-700'
              }`}
          >
            Order Items Table
          </button>
        </div>

        {/* Cleaned up Trigger Button with Disabled Styles */}
        <button
          onClick={handleSimulation}
          disabled={isSimulating}
          className={`px-6 py-2.5 rounded-lg font-bold text-sm transition-all flex items-center gap-2 ${isSimulating
            ? 'bg-slate-300 text-slate-500 cursor-not-allowed'
            : 'bg-blue-600 text-white hover:bg-blue-700 shadow-md hover:shadow-blue-500/20'
            }`}
        >
          {isSimulating ? (
            <>
              <div className="w-4 h-4 border-2 border-slate-500 border-t-transparent rounded-full animate-spin"></div>
              Ingesting Data...
            </>
          ) : 'Trigger 50-Batch Load'}
        </button>
      </div>

      {/* Full-Width Metrics Card */}
      {/* Top Row: Metrics Card + Optimization Audit side-by-side */}
      <div className="grid grid-cols-12 gap-6 mb-6">
        <div className="col-span-12 lg:col-span-8">
          {selectedTable === 'orders'
            ? renderTableMetrics("Orders Table", ordersAudit)
            : renderTableMetrics("Order Items Table", itemsAudit)}
        </div>

        {/* Dynamic Optimization Audit — now top-right */}
        <div className="col-span-12 lg:col-span-4 bg-white p-6 rounded-xl border shadow-sm flex flex-col">
          <h3 className="text-sm font-semibold text-slate-800 mb-4 border-b border-slate-100 pb-3">Optimization Audit</h3>
          <div className="flex flex-col flex-1 justify-center">

            {selectedTable === 'orders' ? (
              <div>
                <p className="text-xs font-bold text-blue-600 uppercase tracking-wider mb-4">Orders Table</p>
                <div className="flex justify-between text-sm mb-3"><span className="text-slate-500">Peak Bloat (Files):</span> <span className="font-mono text-red-500 font-medium text-lg">{ordersAudit.before.files}</span></div>
                <div className="flex justify-between text-sm"><span className="text-slate-500">Current State:</span> <span className="font-mono text-emerald-500 font-medium text-lg">{ordersAudit.after.files}</span></div>
              </div>
            ) : (
              <div>
                <p className="text-xs font-bold text-purple-600 uppercase tracking-wider mb-4">Order Items Table</p>
                <div className="flex justify-between text-sm mb-3"><span className="text-slate-500">Peak Bloat (Files):</span> <span className="font-mono text-red-500 font-medium text-lg">{itemsAudit.before.files + (itemsAudit.before.delete_files || 0)}</span></div>
                <div className="flex justify-between text-sm"><span className="text-slate-500">Current State:</span> <span className="font-mono text-emerald-500 font-medium text-lg">{itemsAudit.after.files + (itemsAudit.after.delete_files || 0)}</span></div>
              </div>
            )}

          </div>
        </div>
      </div>

      {/* Full-Width Graph, stretched below */}
      <div className="w-full bg-white p-6 rounded-xl border shadow-sm">
        <h3 className="text-sm font-semibold text-slate-800 mb-6">Fragmentation Rates (File Counts)</h3>
        <ResponsiveContainer width="100%" height={280}>
          <AreaChart data={chartData} margin={{ top: 20, right: 20, left: 0, bottom: 10 }}>
            <defs>
              <linearGradient id="filesGrad" x1="0" y1="0" x2="0" y2="1">
                <stop offset="0%" stopColor={selectedTable === 'orders' ? '#0F766E' : '#EA580C'} stopOpacity={0.35} />
                <stop offset="100%" stopColor={selectedTable === 'orders' ? '#0F766E' : '#EA580C'} stopOpacity={0} />
              </linearGradient>
              <linearGradient id="snapGrad" x1="0" y1="0" x2="0" y2="1">
                <stop offset="0%" stopColor="#94A3B8" stopOpacity={0.25} />
                <stop offset="100%" stopColor="#94A3B8" stopOpacity={0} />
              </linearGradient>
            </defs>

            <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#F1F5F9" />

            <XAxis
              dataKey="eventName"
              axisLine={false}
              tickLine={false}
              tick={{ fill: '#94A3B8', fontSize: 10 }}
              interval="preserveStartEnd"
              minTickGap={40}
              dy={10}
            />
            <YAxis
              axisLine={false}
              tickLine={false}
              tick={{ fill: '#94A3B8', fontSize: 11 }}
              width={40}
            />

            <Tooltip
              contentStyle={{
                borderRadius: '10px',
                border: 'none',
                boxShadow: '0 8px 24px rgba(0,0,0,0.08)',
                fontSize: '12px',
              }}
              labelStyle={{ fontWeight: 600, marginBottom: 4 }}
            />
            <Legend
              verticalAlign="top"
              height={36}
              iconType="circle"
              wrapperStyle={{ fontSize: '12px', fontWeight: 500 }}
            />

            <Area
              type="monotone"
              name="Total Files"
              dataKey="files"
              stroke={selectedTable === 'orders' ? '#0F766E' : '#EA580C'}
              strokeWidth={2.5}
              fill="url(#filesGrad)"
              dot={false}
              activeDot={{ r: 5, strokeWidth: 0 }}
            />
            <Area
              type="monotone"
              name="Snapshots"
              dataKey="snapshots"
              stroke="#94A3B8"
              strokeWidth={2}
              fill="url(#snapGrad)"
              dot={false}
              activeDot={{ r: 4, strokeWidth: 0 }}
            />
          </AreaChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}