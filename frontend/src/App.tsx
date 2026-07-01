import { useEffect, useState } from 'react';
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { SSEClientTransport } from "@modelcontextprotocol/sdk/client/sse.js";
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, ReferenceLine } from 'recharts';

interface HistoryData {
  batch: number;
  files: number;
  snapshots: number;
  avg_file_size_kb: number;
}

function App() {
  const [data, setData] = useState<HistoryData[]>([]);
  const [currentMetrics, setCurrentMetrics] = useState<HistoryData>({ batch: 0, files: 0, snapshots: 0, avg_file_size_kb: 0 });

  useEffect(() => {
    async function init() {
      try {
        const transport = new SSEClientTransport(new URL("http://127.0.0.1:8000/sse"));
        const client = new Client({ name: "Dashboard", version: "1.0.0" }, { capabilities: {} });
        await client.connect(transport);
        
        // Ensure result exists before accessing
        const result = await client.callTool({ 
            name: "get_table_history", 
            arguments: { table_name: "orders" } 
        });
        
        // --- START REPLACING HERE ---
        // Explicitly cast result.content as an array of objects with a 'text' property
        const contentArray = result.content as { text: string }[];
        
        if (contentArray && contentArray.length > 0) {
            const parsedData: HistoryData[] = JSON.parse(contentArray[0].text);
            
            if (parsedData.length > 0) {
                setData(parsedData);
                // Sync the top cards to the latest batch data
                setCurrentMetrics(parsedData[parsedData.length - 1]);
            }
        }
        // --- END REPLACING HERE ---
      } catch (e) {
        console.error("Failed to connect or fetch data:", e);
      }
    }
    init();
  }, []); // Keeps the empty dependency array to run once on load

  return (
    <div className="p-10 bg-gray-50 min-h-screen font-sans">
      <h1 className="text-2xl font-bold text-gray-900 mb-2">Iceberg Maintenance Pipeline</h1>
      <p className="text-gray-600 mb-8">Monitoring: 50+ Batch Incremental Load & Compaction</p>

      {/* Grid: Your actual project metrics */}
      <div className="grid grid-cols-4 gap-4 mb-8">
        {[
          { label: "Total Batches", val: currentMetrics.batch },
          { label: "Data Files", val: currentMetrics.files },
          { label: "Snapshots", val: currentMetrics.snapshots },
          { label: "Avg File Size (KB)", val: currentMetrics.avg_file_size_kb }
        ].map((m, i) => (
          <div key={i} className="bg-white p-6 border rounded-lg shadow-sm">
            <p className="text-xs font-semibold text-gray-500 uppercase">{m.label}</p>
            <p className="text-3xl font-extrabold text-gray-900 mt-2">{m.val}</p>
          </div>
        ))}
      </div>

      {/* Visualization of the small-file spike and compaction cliff */}
      <div className="bg-white p-6 border rounded-lg shadow-sm">
        <h2 className="text-lg font-bold mb-6">File Count Trend (Compaction Analysis)</h2>
        <div className="h-80">
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={data}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="batch" label={{ value: 'Batch Number', position: 'insideBottom', offset: -5 }} />
              <YAxis label={{ value: 'File Count', angle: -90, position: 'insideLeft' }} />
              <Tooltip />
              <ReferenceLine x={50} stroke="red" label="Maintenance Applied" />
              <Line type="monotone" dataKey="files" stroke="#4f46e5" strokeWidth={2} />
            </LineChart>
          </ResponsiveContainer>
        </div>
      </div>
    </div>
  );
}

export default App;