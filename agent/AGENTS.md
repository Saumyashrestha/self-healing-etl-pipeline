# AGENTS.md

Scope: this file documents the codebase as it actually exists (inspected directly), not the aspirational design in the project brief. The brief was used only to understand intent/grading scope, never as a source for architecture or convention claims below. Anything asserted here traces to a specific file.

---

## 1. Project Overview

- Self-healing ETL pipeline into Apache Iceberg, paired with an AI copilot that observes table health and performs maintenance under explicit human confirmation.
- Two independently-running backend processes + one frontend. Not a monolith.
- Core loop: ingestion job appends small batches → Iceberg fragments (small files, delete-file bloat under merge-on-read) → agent detects degradation via real computed metrics → agent proposes maintenance → human confirms → agent executes compaction/expiry → metrics re-measured and logged.
- Stretch feature: isolated Optimistic Concurrency Control (OCC) conflict simulation, fully separate from the main pipeline/tables.

---

## 2. Tech Stack

- **Table format:** Apache Iceberg, format-version 2, Hadoop catalog, local `warehouse/` dir. `write.merge.mode` / `write.update.mode` = `merge-on-read` on both fact tables.
- **Compute:** PySpark, via `iceberg-spark-runtime-3.5_2.12:1.5.0` + `org.postgresql:postgresql:42.7.3` (jars pulled via `spark.jars.packages`).
- **OLTP source:** PostgreSQL, accessed two ways — `psycopg2` (direct mutation) and Spark JDBC (bulk/incremental reads).
- **Agent tool server:** Python `FastMCP` (`mcp.server.fastmcp`), SSE transport, port 8000.
- **Agent chat backend:** FastAPI, port 8001. LLM provider: Groq, model `llama-3.3-70b-versatile` (via `groq` Python SDK). Not the Claude Agent SDK named in the original brief — deviation is real, not a doc error.
- **Scheduling:** APScheduler (`AsyncIOScheduler`), 1-minute interval health audit job.
- **Frontend:** React + TypeScript, Tailwind CSS, `react-markdown`, `recharts` for the trend chart.
- **MCP client, Python side:** `mcp.client.sse` + `mcp.client.session` (`mcp_client.py`, used by the FastAPI backend to reach the tool server).
- **MCP client, JS side:** `@modelcontextprotocol/sdk` (`Client` + `SSEClientTransport`), used directly by `Dashboard.tsx` — a **separate** connection from the FastAPI backend's.

---

## 3. Run / Build / Test Commands

Order matters — later steps depend on earlier ones being complete.

```bash
# 1. Python deps (from project root, ideally in a venv)
pip install -r requirements.txt

# 2. Frontend deps
cd frontend && npm install && cd ..

# 3. Ensure Postgres reachable via DATABASE_URL in .env

# 4. Seed baseline OLTP data (drops/recreates orders, order_items)
python data_generate.py seed

# 5. One-time schema setup (adds updated_at/created_at, creates pipeline_watermark)
python run_ddl_setup.py

# 6. Start MCP tool server (port 8000)
python -m src.app.main
# (this calls mcp.sse_app(), registers tools from src/app/tools.py)

# 7. Start FastAPI chat backend (port 8001) — requires GROQ_API_KEY in .env
python main.py

# 8. Start frontend
cd frontend && npm run dev
```

**Full state reset** (required whenever generation logic in `catalog.py`/`data_generate.py`/`pipeline.py` changes, or state becomes inconsistent):
```bash
rm -rf warehouse/            # Windows: Remove-Item -Recurse -Force warehouse
python data_generate.py seed
python run_ddl_setup.py
python scripts/reset_watermark.py   # if present — truncates pipeline_watermark
```

**No automated test suite observed.** `tests/test_iceberg_concurrency.py` is the OCC simulation script itself (invoked as a subprocess by the FastAPI backend, not a pytest test). `tests/verify_data.py` is a manual standalone Spark query script, run directly (`python tests/verify_data.py`), not wired into any test runner.

---

## 4. Folder Structure

```
project2-self-healing-etl/
├── data_generate.py            # Postgres seeding + incremental OLTP simulation
├── main.py                     # FastAPI chat backend (port 8001)
├── mcp_client.py                # Python MCP client bridge (FastAPI -> MCP tool server)
├── run_ddl_setup.py             # One-time Postgres schema setup
├── requirements.txt
├── readme.md
├── frontend/
│   └── src/
│       ├── App.tsx
│       ├── App.css
│       └── components/
│           ├── AICopilot.tsx    # Chat UI, SSE notification listener
│           ├── Dashboard.tsx    # Health cards, trend chart, direct MCP JS client
│           ├── OCCDiagram.tsx   # SVG timeline for OCC conflict explanation
│           └── OCCVisualizer.tsx # Live OCC simulation stream UI
├── logs/
│   ├── occ_error.log            # OCC crash timestamp + raw Java exception
│   └── simulation_history.log   # OCC event timeline (all workers)
├── src/
│   ├── app/
│   │   ├── main.py              # MCP tool server entrypoint (port 8000)
│   │   └── tools.py             # MCP tool definitions
│   ├── ingestion/
│   │   └── pipeline.py          # Watermark-based CDC load + 50-batch degrade simulation
│   ├── maintenance/
│   │   └── maintenance.py       # Iceberg compaction/expiry procedures
│   ├── monitoring/
│   │   ├── metrics.py           # Live on-demand health scoring (get_table_metrics)
│   │   ├── history_logger.py    # Persistent JSONL trend history per table
│   │   └── spark_session.py     # Shared SparkSession singleton
│   └── utils/
│       └── catalog.py           # Fake-data realism: pricing, popularity tiers, status logic
├── tests/
│   ├── test_iceberg_concurrency.py  # OCC simulation script (not pytest)
│   └── verify_data.py               # Manual ad-hoc query script
```

---

## 5. Coding Conventions (observed directly)

- **Absolute imports rooted at `src`**, e.g. `from src.monitoring.history_logger import log_snapshot_with_session`. Scripts assume execution from project root.
- **Shared Spark session is a module-level singleton** (`src/monitoring/spark_session.py`, `get_shared_spark()`) — created once, reused everywhere (`pipeline.py`, `metrics.py`, `maintenance.py`, `tools.py`). No component creates its own competing `SparkSession`.
- **`get_spark_session()` in `metrics.py`** wraps `get_shared_spark()` rather than duplicating session logic — confirms single-session convention is intentional, not accidental.
- **Health scoring has one canonical implementation**: `calculate_health_score()` in `src/monitoring/metrics.py`. `history_logger.py` imports and calls this same function rather than reimplementing the formula — do not reintroduce a second scoring formula.
- **Watermark pattern**: every incremental read is followed by advancing `pipeline_watermark` only *after* a successful `MERGE INTO` — never advance the watermark before the merge commits (`update_watermark()` calls in `pipeline.py` all sit after the `spark.sql("MERGE INTO ...")` call in the same branch).
- **Dual-mode logging via `emit_log` closure** in `pipeline.py`: writes to a `Queue` if one is passed (for streaming), otherwise falls back to `print()`. Used for any code path that may run either interactively or as a background/streamed task.
- **MCP tool naming convention implies HITL staging**: `propose_*` tools only announce intent (no side effects); `execute_confirmed_*` tools perform the actual destructive action. This naming pattern is the load-bearing signal for which tools are "safe to call automatically" vs. "must be gated" — an agent should treat any `execute_confirmed_*`-named tool as requiring the confirmation flow in Section 6, and any `propose_*`-named tool as side-effect-free.
- **MCP tools return plain strings**, not structured JSON, except where a tool explicitly needs structured data for the frontend (`get_pipeline_status`, `get_deep_telemetry`, `get_table_history` return `json.dumps(...)` strings). Follow this split when adding new tools.
- **Business-logic modules are never imported directly by the FastAPI backend.** `main.py` (root) does not import `metrics.py`, `maintenance.py`, or `pipeline.py` — all access goes through `mcp_client.py`'s `call_mcp_tool()`. See Section 7 for why this matters.
- **In-progress state uses plain module-level dicts as flags**, not a proper state store: `_pipeline_status` (`tools.py`), `maintenance_in_progress` (root `main.py`). These are process-local — see Section 9 for scaling implications.

---

## 6. Destructive Operation Confirmation Rule (behavioral)

- Any operation that mutates or removes Iceberg table state (compaction, position-delete rewrite, manifest rewrite, snapshot expiry) is **never invoked as a direct response to a health check or a user's first request.**
- The flow is strictly two-phase:
  1. **Propose** — the system states what it intends to do and on which table(s). No Iceberg mutation occurs at this stage. The response to the user includes an explicit flag indicating a decision is pending, plus which table(s) are targeted.
  2. **Confirm and execute** — the destructive action is only invoked in direct response to an unambiguous, explicit affirmative signal tied to that specific proposal. Declining, ignoring, or sending an unrelated message must not result in execution.
- This applies identically whether the proposal originated from a direct user request ("clean up the orders table") or from an unprompted/proactive system-initiated alert — both must pass through the same confirm step before anything destructive runs.
- A pipeline load in progress must block destructive execution — the two must not run concurrently against the same table.
- This rule is UI-enforced (explicit confirm/dismiss action) and backend-enforced (a distinct code path only reachable after a recognized confirmation signal) — neither layer alone is sufficient; both must independently uphold it.

---

## 7. Architecture & Modularity

### Component map (as implemented, not as diagrammed in the brief)
```
Frontend (React)
 ├─ AICopilot.tsx  ──HTTP POST /chat──────────────▶ FastAPI backend (root main.py, :8001)
 │                 ◀──SSE /api/agent-notifications──
 │
 └─ Dashboard.tsx  ──MCP SSE (direct)─────────────▶ MCP tool server (src/app/main.py, :8000)
                    (bypasses FastAPI backend entirely for tool calls)

FastAPI backend (:8001)
 ├─ /chat            → mcp_client.call_mcp_tool() → MCP tool server (:8000)
 ├─ /api/agent-notifications  (SSE, per-client queue, fed by scheduler)
 ├─ /api/simulate-occ (SSE)  → subprocess: tests/test_iceberg_concurrency.py
 └─ APScheduler job (1 min)  → run_health_audit() → mcp_client.call_mcp_tool()

MCP tool server (:8000)
 ├─ tools.py registers all tools on a FastMCP instance
 ├─ tools directly import and call: metrics.py, maintenance.py, pipeline.py, history_logger.py, spark_session.py
 └─ single shared SparkSession backs every tool call
```

### Why this separation exists (stated/evident) vs. not stated

- **Two backend processes instead of one** — evident reason: the MCP tool server is a standard MCP-protocol boundary (SSE transport, tool-call semantics), decoupled from the chat/LLM orchestration layer. This is a conventional MCP client/server split, not unique to this repo.
- **FastAPI backend never imports business logic directly, always via `call_mcp_tool()`** — this is a real, consistently-followed layering rule: *the MCP tool server is the only layer allowed to touch Spark/Iceberg/Postgres business logic directly; the chat backend is a pure orchestration/LLM layer.* No violation of this rule was found in `main.py` (root).
- **Dashboard.tsx connects to the MCP server directly, bypassing the FastAPI backend** — **needs owner input.** No comment or code explains why the dashboard's tool calls (`get_table_history`, `get_pipeline_status`, `run_incremental_load`) skip the FastAPI layer while chat's tool calls go through it. This produces two independent MCP client sessions to the same server with no evident coordination, and means any cross-cutting concern added at the FastAPI layer (auth, logging, rate limiting) would silently not apply to the dashboard's calls.
- **OCC simulation is a subprocess launched from the FastAPI backend, not an MCP tool** — **needs owner input.** No comment explains why OCC uses a different invocation mechanism (raw `subprocess.Popen` + stdout streaming) than every other pipeline/maintenance action (which go through MCP). Given the project's own chat-routing logic elsewhere explicitly favors deterministic, non-LLM paths for OCC explanation, this may be intentional isolation — but that reasoning isn't written down anywhere near the actual endpoint code.
- **OCC uses a fully separate Iceberg table (`db.occ_test`) rebuilt from scratch each run** — stated implicitly by the code's own design (isolated namespace, drop+recreate on every setup call) — this one **is** evident from the code itself, not flagged.

### Layering rules inferred
1. Frontend → FastAPI backend (`/chat`, SSE notifications, OCC subprocess stream) — allowed.
2. Frontend → MCP tool server directly (Dashboard.tsx only) — currently exists, rationale not documented (see above).
3. FastAPI backend → MCP tool server, via `mcp_client.py` only — never via direct Python import of business-logic modules. Consistently followed.
4. MCP tool server → Spark/Iceberg/Postgres business logic — direct Python imports, allowed and expected; this is the one layer meant to touch them.
5. No layer other than the MCP tool server's registered tools should call `execute_table_maintenance()` or `run_pipeline()` directly — both are only invoked from within `tools.py`.

---

## 8. Known Dead / Unreachable Code Paths

- **`/api/simulate-load` endpoint** (`src/app/main.py`) — defined, mounted, but not called by any current frontend component. `Dashboard.tsx` uses the MCP `run_incremental_load` tool instead. Confirm before removing — it may be a manual/debug entrypoint.
- **`analyze_occ_crash_log` MCP-style tool definition and its associated system-prompt formatting instructions** (root `main.py`) — unreachable in normal use, because the `/chat` endpoint's regex interceptor (matching on `"occ"`, `"concurrency"`, `"conflict"`) answers OCC questions deterministically before the LLM tool-calling loop ever runs. Kept intentionally per project owner decision (lower priority than core acceptance criteria) — not a bug, but flagged so a future reader doesn't assume it's the live path.

---

## 9. Needs Owner Input (undocumented architectural decisions)

- Dual MCP client paths into the same tool server (chat via FastAPI bridge, dashboard via direct JS SDK) — no stated reason for the split; no stated coordination/consistency guarantee between them.
- OCC simulation's subprocess-based invocation instead of an MCP tool — no stated reason.
- `_pipeline_status` and `maintenance_in_progress` are process-local in-memory dicts, not persisted or shared across processes — acceptable for a single-instance demo deployment, but no comment states this is a known/accepted limitation versus an oversight.
- Fixed health-score threshold (`HEALTH_THRESHOLD = 70` in `run_health_audit()`) — no stated rationale for this specific number versus any other; not derived from any documented SLA or requirement.
- Two frontend log files (`occ_error.log`, `simulation_history.log`) are plain append/overwrite text files with no rotation or size bound — fine for a demo, unaddressed for anything longer-running.