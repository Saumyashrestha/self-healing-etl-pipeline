import React, { useState, useEffect, useRef } from 'react';

const OCCVisualizer: React.FC = () => {
  const [terminalLogs, setTerminalLogs] = useState<string[]>([]);
  const [v1Table, setV1Table] = useState<string[]>([]);
  const [v2Table, setV2Table] = useState<string[]>([]);
  const [isRunning, setIsRunning] = useState<boolean>(false);

  const terminalEndRef = useRef<HTMLDivElement | null>(null);

  // We use a ref to track where the incoming stream should be routed
  const targetRef = useRef<"terminal" | "v1" | "v2">("terminal");

  useEffect(() => {
    terminalEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [terminalLogs]);

  const triggerSimulation = () => {
    setIsRunning(true);
    setTerminalLogs(["Initializing OCC Concurrency Simulation..."]);
    setV1Table([]);
    setV2Table([]);
    targetRef.current = "terminal";

    const eventSource = new EventSource('http://localhost:8001/api/simulate-occ');

    eventSource.onmessage = (event: MessageEvent) => {
      const rawLine: string = event.data;

      if (rawLine === "[SIMULATION_COMPLETE]") {
        eventSource.close();
        setIsRunning(false);
        return;
      }

      // 1. Handle Data Tables (Right Pane)
      if (rawLine.startsWith("[DATA]")) {
        const cleanLine = rawLine.replace("[DATA] ", "");
        
        // Switch targets based on the phase
        if (cleanLine.includes("SNAPSHOT V1")) targetRef.current = "v1";
        else if (cleanLine.includes("SNAPSHOT V2")) targetRef.current = "v2";
        
        if (targetRef.current === "v1") {
          setV1Table(prev => [...prev, cleanLine]);
        } else if (targetRef.current === "v2") {
          setV2Table(prev => [...prev, cleanLine]);
        }
      } 
      // 2. Handle Terminal Logs (Left Pane)
      else if (rawLine.startsWith("[LOG]")) {
        const cleanLine = rawLine.replace("[LOG] ", "");
        setTerminalLogs(prev => [...prev, cleanLine]);
      }
    };

    eventSource.onerror = () => {
      // ONLY log the error if the simulation was actually expected to be running
      if (isRunning) {
        setTerminalLogs(prev => [...prev, "Connection closed unexpectedly!!!"]);
      }
      eventSource.close();
      setIsRunning(false);
    };
  };

  const getLogStyle = (text: string): React.CSSProperties => {
    if (text.includes("CRASHED!")) return { color: '#ff4d4d', fontWeight: 'bold' };
    if (text.includes("COMMIT SUCCESS")) return { color: '#00cc66', fontWeight: 'bold' };
    if (text.includes("[Worker A]")) return { color: '#3399ff' };
    if (text.includes("[Worker B]")) return { color: '#ff9933' };
    return { color: '#e6e6e6' };
  };

  return (
    <div className="flex flex-col w-full max-w-6xl bg-gray-900 rounded-xl shadow-2xl overflow-hidden border border-gray-700 font-mono">
      {/* Header */}
      <div className="flex items-center justify-between px-6 py-4 bg-gray-800 border-b border-gray-700">
        <div>
          <h3 className="text-white font-bold text-lg">Apache Iceberg: OCC Collision Engine</h3>
          <p className="text-gray-400 text-xs mt-1">Live parallel partition update simulation</p>
        </div>
        <button
          onClick={triggerSimulation}
          disabled={isRunning}
          className={`px-6 py-2.5 rounded-lg font-bold text-sm transition-colors shadow-lg ${isRunning
              ? 'bg-gray-700 text-gray-500 cursor-not-allowed'
              : 'bg-emerald-600 text-white hover:bg-emerald-500 hover:shadow-emerald-900/50'
            }`}
        >
          {isRunning ? 'Executing Simulation...' : 'Trigger Data Collision'}
        </button>
      </div>

      {/* Split Pane Layout */}
      <div className="grid grid-cols-1 lg:grid-cols-2 h-[500px] bg-black">

        {/* LEFT PANE: Action Terminal */}
        <div className="p-5 overflow-y-auto border-r border-gray-800 flex flex-col relative">
          <h4 className="text-gray-500 text-xs font-bold uppercase tracking-widest mb-4 sticky top-0 bg-black/90 pb-2">Execution Logs</h4>
          {terminalLogs.length === 0 ? (
            <p className="text-gray-700 italic mt-4 text-center">Awaiting execution trigger...</p>
          ) : (
            terminalLogs.map((log, index) => (
              <div key={index} style={getLogStyle(log)} className="mb-1.5 text-sm tracking-wide leading-relaxed">
                {log}
              </div>
            ))
          )}
          <div ref={terminalEndRef} />
        </div>

        {/* RIGHT PANE: Database State Verification */}
        <div className="flex flex-col bg-gray-900/50 min-h-0">

          {/* Top Half: Snapshot V1 */}
          <div className="flex flex-col bg-gray-900/50 min-h-0 overflow-y-auto">

            {/* Snapshot V1 */}
            <div className="p-4 border-b border-gray-800">
              <h4 className="text-blue-400 text-xs font-bold uppercase mb-3">Baseline (Snapshot V1)</h4>
              <table className="w-full text-[10px] text-gray-400 border-collapse">
                <thead><tr className="border-b border-gray-700 text-left"><th>ID</th><th>CAT</th><th>STATUS</th></tr></thead>
                <tbody>
                  {v1Table.filter(r => r.startsWith('|')).slice(1).map((row, i) => (
                    <tr key={i} className="border-b border-gray-800">
                      {row.split('|').filter(c => c.trim()).map((cell, j) => <td key={j} className="py-1">{cell.trim()}</td>)}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>

            {/* Snapshot V2 */}
            <div className="p-4">
              <h4 className="text-emerald-400 text-xs font-bold uppercase mb-3">Aftermath (Snapshot V2)</h4>
              <table className="w-full text-[10px] text-gray-400 border-collapse">
                <thead><tr className="border-b border-gray-700 text-left"><th>ID</th><th>CAT</th><th>STATUS</th></tr></thead>
                <tbody>
                  {v2Table.filter(r => r.startsWith('|')).slice(1).map((row, i) => {
                    const cells = row.split('|').filter(c => c.trim());
                    const isWinner = cells.includes('DONE_BY_B');
                    return (
                      <tr key={i} className={`border-b border-gray-800 ${isWinner ? 'text-emerald-400 font-bold bg-emerald-900/20' : ''}`}>
                        {cells.map((cell, j) => <td key={j} className="py-1">{cell.trim()}</td>)}
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </div>

        </div>
      </div>
    </div>
  );
};

export default OCCVisualizer;