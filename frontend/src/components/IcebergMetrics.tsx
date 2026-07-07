import { useEffect, useState } from 'react';
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { SSEClientTransport } from "@modelcontextprotocol/sdk/client/sse.js";

interface TelemetryData {
  snapshots: any[];
  manifests: any[];
  files: any[];
  error?: string;
}

export default function IcebergMetrics() {
  const [activeTable, setActiveTable] = useState<'orders' | 'order_items'>('orders');
  const [data, setData] = useState<TelemetryData>({ snapshots: [], manifests: [], files: [] });
  const [isLoading, setIsLoading] = useState(false);

  const fetchDeepTelemetry = async (tableName: string) => {
    setIsLoading(true);
    try {
      const transport = new SSEClientTransport(new URL("http://127.0.0.1:8000/sse"));
      const client = new Client({ name: "Telemetry-Tab", version: "1.0.0" }, { capabilities: {} });
      await client.connect(transport);
      
      const result = await client.callTool({ 
        name: "get_deep_telemetry", 
        arguments: { table_name: tableName } 
      });
      
      const parsedData = JSON.parse((result.content as any)[0].text);
      setData(parsedData);
    } catch (e) {
      console.error("Failed to fetch deep telemetry:", e);
    } finally {
      setIsLoading(false);
    }
  };

  useEffect(() => {
    fetchDeepTelemetry(activeTable);
  }, [activeTable]);

  return (
    <div className="animate-in fade-in duration-500 max-w-7xl mx-auto">
      <header className="mb-8 flex justify-between items-end">
        <div>
          <h2 className="text-2xl font-bold text-slate-900">Iceberg Deep Telemetry</h2>
          <p className="text-slate-500 text-sm mt-1">Inspecting raw metadata layers and physical files.</p>
        </div>
        
        {/* Table Toggle */}
        <div className="flex bg-slate-100 p-1 rounded-lg border border-slate-200">
          <button 
            onClick={() => setActiveTable('orders')}
            className={`px-4 py-1.5 text-sm font-medium rounded-md transition-all ${activeTable === 'orders' ? 'bg-white shadow-sm text-blue-600' : 'text-slate-500 hover:text-slate-700'}`}
          >
            orders
          </button>
          <button 
            onClick={() => setActiveTable('order_items')}
            className={`px-4 py-1.5 text-sm font-medium rounded-md transition-all ${activeTable === 'order_items' ? 'bg-white shadow-sm text-blue-600' : 'text-slate-500 hover:text-slate-700'}`}
          >
            order_items
          </button>
        </div>
      </header>

      {isLoading ? (
        <div className="flex justify-center items-center h-64 text-slate-400 animate-pulse">Syncing catalog metadata...</div>
      ) : data.error ? (
        <div className="bg-red-50 text-red-600 p-4 rounded-lg border border-red-100">Error: {data.error}. Make sure the table exists.</div>
      ) : (
        <div className="space-y-8">
          
          {/* Snapshots Table */}
          <div className="bg-white rounded-2xl border border-slate-200 shadow-sm overflow-hidden">
            <div className="bg-slate-50 px-6 py-4 border-b border-slate-200">
              <h3 className="font-semibold text-slate-800">Active Snapshots (Top 10)</h3>
            </div>
            <div className="overflow-x-auto">
              <table className="w-full text-sm text-left text-slate-600">
                <thead className="text-xs text-slate-400 uppercase bg-white border-b border-slate-100">
                  <tr><th className="px-6 py-3">Timestamp</th><th className="px-6 py-3">Snapshot ID</th><th className="px-6 py-3">Operation</th></tr>
                </thead>
                <tbody>
                  {data.snapshots.map((s, i) => (
                    <tr key={i} className="border-b border-slate-50 hover:bg-slate-50 font-mono text-xs">
                      <td className="px-6 py-3">{s.committed_at}</td><td className="px-6 py-3 text-blue-600">{s.snapshot_id}</td><td className="px-6 py-3">{s.operation}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>

          {/* Grid for Manifests and Files */}
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            
            {/* Manifests Table */}
            <div className="bg-white rounded-2xl border border-slate-200 shadow-sm overflow-hidden flex flex-col">
              <div className="bg-slate-50 px-6 py-4 border-b border-slate-200">
                <h3 className="font-semibold text-slate-800">Manifest Files (Metadata)</h3>
              </div>
              <div className="overflow-y-auto max-h-80">
                <table className="w-full text-sm text-left text-slate-600">
                  <thead className="text-xs text-slate-400 uppercase bg-white border-b border-slate-100 sticky top-0">
                    <tr><th className="px-6 py-3">File Name</th><th className="px-6 py-3">Size (Bytes)</th></tr>
                  </thead>
                  <tbody>
                    {data.manifests.map((m, i) => (
                      <tr key={i} className="border-b border-slate-50 hover:bg-slate-50 font-mono text-xs">
                        <td className="px-6 py-3 truncate max-w-[200px]" title={m.file_name}>{m.file_name}</td><td className="px-6 py-3">{m.length}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>

            {/* Data Files Table */}
            <div className="bg-white rounded-2xl border border-slate-200 shadow-sm overflow-hidden flex flex-col">
              <div className="bg-slate-50 px-6 py-4 border-b border-slate-200">
                <h3 className="font-semibold text-slate-800">Physical Data Files (Parquet)</h3>
              </div>
              <div className="overflow-y-auto max-h-80">
                <table className="w-full text-sm text-left text-slate-600">
                  <thead className="text-xs text-slate-400 uppercase bg-white border-b border-slate-100 sticky top-0">
                    <tr><th className="px-6 py-3">File Name</th><th className="px-6 py-3">Records</th><th className="px-6 py-3">Size</th></tr>
                  </thead>
                  <tbody>
                    {data.files.map((f, i) => (
                      <tr key={i} className="border-b border-slate-50 hover:bg-slate-50 font-mono text-xs">
                        <td className="px-6 py-3 truncate max-w-[150px]" title={f.file_name}>{f.file_name}</td><td className="px-6 py-3">{f.record_count}</td><td className="px-6 py-3">{f.file_size_in_bytes}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>

          </div>
        </div>
      )}
    </div>
  );
}