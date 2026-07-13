import { useState } from 'react';

interface OCCDiagramProps {
  baselineTime: string;
  commitTime: string;
  crashTime: string;
}

function parseTime(t: string): number {
  const [h, m, rest] = t.split(':');
  const [s, ms] = rest.split('.');
  return (+h * 3600 + +m * 60 + +s) * 1000 + (+ms || 0);
}

function OCCTimelineSVG({ baselineTime, commitTime, crashTime }: OCCDiagramProps) {
  const baseMs = parseTime(baselineTime);
  const commitMs = parseTime(commitTime);
  const crashMs = parseTime(crashTime);
  const baselineToCrash = ((crashMs - baseMs) / 1000).toFixed(2);
  const commitToCrash = ((crashMs - commitMs) / 1000).toFixed(2);

  return (
    <svg width="100%" viewBox="0 0 680 470">
      <defs>
        <marker id="occ-arrow" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse">
          <path d="M2 1L8 5L2 9" fill="none" stroke="#94A3B8" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
        </marker>
      </defs>

      {/* Legend */}
      <circle cx="45" cy="26" r="5" fill="#3B82F6" />
      <text x="58" y="30" fill="#CBD5E1" fontSize="12">Read / baseline</text>
      <circle cx="205" cy="26" r="5" fill="#14B8A6" />
      <text x="218" y="30" fill="#CBD5E1" fontSize="12">Committed successfully</text>
      <circle cx="410" cy="26" r="5" fill="#EF4444" />
      <text x="423" y="30" fill="#CBD5E1" fontSize="12">Commit rejected</text>

      {/* Time markers */}
      <text x="10" y="132" fill="#94A3B8" fontSize="12">{baselineTime}</text>
      <text x="10" y="232" fill="#94A3B8" fontSize="12">{commitTime}</text>
      <text x="10" y="372" fill="#94A3B8" fontSize="12">{crashTime}</text>

      {/* Connectors */}
      <line x1="220" y1="156" x2="220" y2="338" stroke="#64748B" strokeWidth="1.5" markerEnd="url(#occ-arrow)" />
      <text x="232" y="250" fill="#94A3B8" fontSize="12">{baselineToCrash}s later</text>

      <path d="M400,256 L400,300 L280,300 L280,338" fill="none" stroke="#64748B" strokeWidth="1.5" markerEnd="url(#occ-arrow)" />
      <text x="290" y="292" fill="#94A3B8" fontSize="12">Table snapshot advances</text>

      {/* Worker A: baseline read */}
      <g>
        <rect x="100" y="100" width="240" height="56" rx="8" fill="#1E3A5F" stroke="#3B82F6" strokeWidth="0.5" />
        <text x="220" y="118" textAnchor="middle" fill="#93C5FD" fontSize="14" fontWeight="500">Read baseline</text>
        <text x="220" y="136" textAnchor="middle" fill="#93C5FD" fontSize="12">Worker A — snapshot v1</text>
      </g>

      {/* Worker B: successful commit */}
      <g>
        <rect x="400" y="200" width="240" height="56" rx="8" fill="#134E4A" stroke="#14B8A6" strokeWidth="0.5" />
        <text x="520" y="218" textAnchor="middle" fill="#5EEAD4" fontSize="14" fontWeight="500">Commit succeeds</text>
        <text x="520" y="236" textAnchor="middle" fill="#5EEAD4" fontSize="12">Worker B — advances to v2</text>
      </g>

      {/* Worker A: rejected commit */}
      <g>
        <rect x="100" y="340" width="240" height="56" rx="8" fill="#450A0A" stroke="#EF4444" strokeWidth="0.5" />
        <text x="220" y="358" textAnchor="middle" fill="#FCA5A5" fontSize="14" fontWeight="500">Commit rejected</text>
        <text x="220" y="376" textAnchor="middle" fill="#FCA5A5" fontSize="12">Worker A — baseline v1 is stale</text>
      </g>

      {/* Summary caption */}
      <text x="340" y="425" textAnchor="middle" fill="#CBD5E1" fontSize="12">
        <tspan x="340" dy="0">Worker A's commit arrived {commitToCrash}s after Worker B's, but Iceberg</tspan>
        <tspan x="340" dy="18">detected the snapshot mismatch and rejected it to protect data integrity.</tspan>
      </text>
    </svg>
  );
}

export default function OCCDiagram({ baselineTime, commitTime, crashTime }: OCCDiagramProps) {
  const [isExpanded, setIsExpanded] = useState(false);

  return (
    <>
      <div
        onClick={() => setIsExpanded(true)}
        className="mt-3 w-full max-w-[600px] ml-2 p-4 bg-slate-800 border border-slate-700 rounded-xl cursor-zoom-in hover:border-slate-500 transition-colors"
      >
        <OCCTimelineSVG baselineTime={baselineTime} commitTime={commitTime} crashTime={crashTime} />
      </div>

      {isExpanded && (
        <div
          className="fixed inset-0 bg-black/70 z-50 flex items-center justify-center p-8"
          onClick={() => setIsExpanded(false)}
        >
          <div
            className="bg-slate-800 border border-slate-700 rounded-xl p-8 max-w-3xl w-full"
            onClick={(e) => e.stopPropagation()}
          >
            <OCCTimelineSVG baselineTime={baselineTime} commitTime={commitTime} crashTime={crashTime} />
            <button
              onClick={() => setIsExpanded(false)}
              className="mt-4 px-4 py-2 bg-slate-700 text-slate-300 rounded-lg text-sm hover:bg-slate-600 transition-colors"
            >
              Close
            </button>
          </div>
        </div>
      )}
    </>
  );
}