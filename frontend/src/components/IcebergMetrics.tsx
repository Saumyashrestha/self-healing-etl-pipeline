export default function IcebergMetrics() {
  return (
    <div className="animate-in fade-in duration-500">
      <header className="mb-8">
        <h2 className="text-2xl font-bold">Iceberg Deep Telemetry</h2>
        <p className="text-slate-500 text-sm">Detailed view of manifest lists, orphaned files, and schema.</p>
      </header>
      <div className="bg-white p-6 rounded-2xl border border-slate-200 shadow-sm h-96 flex items-center justify-center">
         <p className="text-slate-400">Deep telemetry tables will go here.</p>
      </div>
    </div>
  );
}