# Self-Healing ETL Pipeline with a Maintenance Copilot

An incremental-load pipeline into Apache Iceberg that deliberately accumulates the "small-file problem," paired with an AI copilot that can observe table health and perform controlled, confirmed maintenance.

---

## 1. Project Purpose

An ingestion job appends small batches into a lakehouse table over time without maintenance ever being run. File counts explode, delete-file bloat accumulates, and query performance degrades. This project:

1. Builds a genuine incremental (merge/upsert) load pipeline from a Postgres OLTP source into Iceberg fact tables.
2. Deliberately reproduces the small-file/fragmentation problem through repeated small batches.
3. Quantifies table health with real, measured metrics — before and after maintenance.
4. Gives an AI agent the ability to observe that state and take a destructive maintenance action only after explicit user confirmation.

---

## 2. Tech Stack

| Layer | Technology |
|---|---|
| Table format | Apache Iceberg (Hadoop catalog, local warehouse) |
| Compute engine | Apache Spark (PySpark) |
| OLTP source | PostgreSQL |
| Backend (agent/chat) | FastAPI + Groq (Llama 3.3 70B) |
| Backend (tools) | FastMCP (Model Context Protocol server) |
| Scheduling | APScheduler (proactive health checks) |
| Postgres driver (mutation) | psycopg2 |
| Postgres driver (Spark reads) | JDBC (`org.postgresql:postgresql`) |
| Frontend | React + TypeScript, Tailwind CSS |
| Charting | Recharts |
| MCP client | `@modelcontextprotocol/sdk` (SSE transport) |
| Markdown rendering | react-markdown |

---

## 3. Architecture Overview

There are **two independent backend processes** and **one frontend**:

```
┌─────────────────────────────┐        ┌──────────────────────────────┐
│   FastAPI Agent Backend     │        │      FastMCP Tool Server     │
│   (main.py — port 8001)     │        │   (src/app/main.py+tools.py  │
│                              │        │        — port 8000)          │
│  - /chat (LLM + agent loop) │        │  - get_table_health          │
│  - /api/agent-notifications │        │  - propose_maintenance       │
│    (SSE proactive alerts)   │        │  - execute_confirmed_        │
│  - /api/simulate-occ (SSE)  │        │    maintenance               │
│  - Groq (Llama 3.3 70B)     │        │  - run_incremental_load      │
│  - APScheduler (1-min audit)│        │  - get_pipeline_status       │
└──────────────┬───────────────┘        │  - get_table_history         │
               │                        │  - get_deep_telemetry        │
               │                        └──────────────┬───────────────┘
               │                                        │
               └───────────────┬────────────────────────┘
                                │
                    ┌───────────▼────────────┐
                    │   React Dashboard        │
                    │  (Dashboard.tsx,          │
                    │   AICopilot.tsx)          │
                    └───────────────────────────┘

               ┌────────────────────────────────┐
               │   Shared Spark Session          │
               │  (src/monitoring/spark_session) │
               │  — one JVM, reused across all    │
               │    pipeline/maintenance/metrics  │
               │    calls, never stopped mid-life │
               └───────────────┬────────────────┘
                                │
                    ┌───────────▼────────────┐
                    │   Apache Iceberg Tables  │
                    │  local.db.orders          │
                    │  local.db.order_items     │
                    └───────────┬────────────┘
                                │
                    ┌───────────▼────────────┐
                    │   PostgreSQL (OLTP)      │
                    │  orders, order_items,    │
                    │  pipeline_watermark      │
                    └────────────────────────┘
```

---

## 4. Data Engineering Layer

### 4.1 Incremental Load Pattern (`src/ingestion/pipeline.py`)

The pipeline follows a genuine **watermark-based CDC (change data capture)** pattern, not a full reload:

1. **First run only**: performs a full initial load of `orders` and `order_items` from Postgres into Iceberg (`createOrReplace`), guarded by `spark.catalog.tableExists(...)` so subsequent runs skip this step entirely.
2. **Watermark initialization**: on first run, a `pipeline_watermark` control table in Postgres is seeded with the current server time for both `orders` and `order_items`.
3. **Each of 50 simulated batches**:
   - Randomly decides a batch composition (`insert_only`, `update_only`, or `both`) with a randomized row count (1–8 inserts, 1–6 updates) — this varies batch size instead of using a fixed shape.
   - **Mutates real rows in Postgres** (`mutate_postgres()`, via `psycopg2`): advances existing `Pending` orders to `Shipped`, and inserts brand-new orders + their line items.
   - **Reads only what changed since the watermark** from Postgres via Spark JDBC (`WHERE updated_at > watermark` / `WHERE created_at > watermark`).
   - **`MERGE INTO`** the changed rows into the corresponding Iceberg table.
   - **Advances the watermark** to the max timestamp actually processed, only after a successful merge.
   - Logs a real health snapshot to the history log after every batch.

This means the "new" data in each batch genuinely originates from the OLTP source system, matching real-world CDC pipelines — not a self-referential trick reading Iceberg's own prior state.

### 4.2 Small-File Problem Simulation

Both tables are configured with:
```
write.merge.mode = merge-on-read
write.update.mode = merge-on-read
```
Under merge-on-read, every `MERGE` containing an update writes a small new **data file** for changed rows *and* a small **delete file** marking the old version — without rewriting the whole table. Repeating this 50 times with small, varied batches produces genuine fragmentation: dozens of tiny data files and delete files, and one new snapshot per commit.

### 4.3 Health Metrics (`src/monitoring/metrics.py`, `src/monitoring/history_logger.py`)

Two related but distinct components:

- **`metrics.py` — `get_table_metrics()`**: an on-demand, live query against Iceberg's `.files`, `.snapshots`, `.manifests` metadata tables. Computes a weighted health score (70% fragmentation / 30% delete-bloat). Used by the chat agent and the MCP `get_table_health` tool for point-in-time answers.
- **`history_logger.py` — `log_snapshot_with_session()`**: appends **one permanent row** to a per-table JSONL log (`warehouse/history/{table}_history.jsonl`) every time it's called — during each pipeline batch and again after maintenance. This is what powers the dashboard's trend chart, giving it a real, continuous timeline rather than a single before/after comparison.

**Total Files metric.** The `files` value tracked here is deliberately computed as **data files + delete files combined**, not data files alone. This matters because both tables use merge-on-read: every `MERGE` containing an update writes a small new data file for the changed rows *and* a small delete file marking the old version as superseded, without rewriting the whole table. Since Iceberg always creates exactly one new snapshot per commit — regardless of how much data changed — but a single commit can contribute more than one new physical file whenever it includes updates, the "Total Files" line naturally climbs faster than the "Snapshots" line over time on the dashboard chart. This divergence is a real, textbook signature of merge-on-read delete-file bloat, and is exactly what the trend chart is designed to surface.

### 4.4 Maintenance (`src/maintenance/maintenance.py`)

`execute_table_maintenance(table_name)` runs, in order:
1. `CALL rewrite_data_files(...)` — compacts small data files into larger ones
2. `CALL rewrite_position_delete_files(...)` — resolves merge-on-read delete-file bloat
3. `CALL rewrite_manifests(...)` — reorganizes metadata pointers
4. `CALL expire_snapshots(..., retain_last => 1)` — removes old snapshot history and orphaned metadata

A final health snapshot is logged immediately afterward, appended to the same history log (never overwriting prior "unhealthy" points), so the dashboard chart shows the full climb-then-drop in one continuous line.

### 4.5 Shared Spark Session (`src/monitoring/spark_session.py`)

All components (pipeline, maintenance, metrics, deep telemetry) share **one long-lived SparkSession** via `get_shared_spark()`, created once and reused for the life of the server process. This allows the 50-batch load, health checks, and maintenance to be triggered repeatedly and in any order, without needing to restart the backend between runs.

---

## 5. AI Agent & Application Layer

### 5.1 MCP Tools (`src/app/tools.py`, port 8000)

| Tool | Purpose |
|---|---|
| `get_table_health` | Live health metrics for one or more tables |
| `propose_maintenance` | Step 1 of the human-in-the-loop flow — announces intent only |
| `execute_confirmed_maintenance` | Step 2 — runs the actual destructive maintenance, only reachable after confirmation |
| `run_incremental_load` | Triggers the 50-batch pipeline as a background task; rejects overlapping runs via a shared `_pipeline_status` flag |
| `get_pipeline_status` | Polled by the dashboard every 4 seconds to detect real completion (not just task launch) |
| `get_table_history` | Returns the full persisted JSONL history for the trend chart |
| `get_deep_telemetry` | Raw snapshot/manifest/file listings for a detailed inspection view |

### 5.2 API Endpoints (`main.py`, port 8001)

| Endpoint | Method | Purpose |
|---|---|---|
| `/chat` | POST | Main copilot endpoint (see 5.3 for internal logic) |
| `/api/agent-notifications` | GET (SSE) | Streams proactive health alerts to all connected clients |
| `/api/simulate-occ` | GET (SSE) | Runs and live-streams the OCC concurrency simulation subprocess |

### 5.3 `/chat` Endpoint Logic

The endpoint layers three mechanisms, evaluated in order:

1. **Deterministic interceptors** — bypass the LLM entirely for known, structured inputs:
   - Confirmation/cancellation replies from the maintenance confirmation buttons (regex-matched exact strings sent by the frontend)
   - OCC-conflict questions (keyword-matched: "occ", "concurrency", "conflict") — builds a fully data-driven explanation directly from log files, with no LLM involvement
2. **`needs_tools` keyword gate** — only offers the LLM tool access (`tool_choice="auto"` vs `"none"`) if the message plausibly relates to tables/health/maintenance, preventing casual messages like "hello" from triggering unnecessary or incorrect tool calls
3. **Bounded tool-calling loop** (max 5 iterations) — for genuinely open-ended questions, lets the model call tools, see results, and decide whether to call again or produce a final answer

### 5.4 Human-in-the-Loop Maintenance Flow

1. User asks to "clean up" a table (or a proactive alert fires) → agent calls `propose_maintenance` → frontend renders an explicit confirmation card with **Execute Compaction** / **Dismiss** buttons
2. Only clicking **Execute Compaction** sends the exact confirmation string the backend's deterministic interceptor matches
3. That interceptor — not the LLM — directly calls `execute_table_maintenance()` and then fetches real post-maintenance metrics, guaranteeing the maintenance action never runs without an explicit, unambiguous confirmation step, and is never fabricated or hallucinated

### 5.5 Proactive Monitoring

`APScheduler` runs `run_health_audit()` every minute, computing a real numeric health score from `get_table_metrics()` and comparing it against a fixed threshold — not an LLM's subjective judgment. If either table falls below the threshold, an alert is pushed over SSE to `/api/agent-notifications` and rendered in chat with the same confirmation-card flow. The audit is skipped entirely while `maintenance_in_progress["active"]` is `True`, preventing duplicate alerts from appearing mid-execution.

### 5.6 OCC Conflict Simulation (`tests/test_iceberg_concurrency.py`)

Two concurrent Spark processes (`Worker A`, `Worker B`) both read the same Iceberg partition, then attempt to commit conflicting updates. Worker B commits first; Worker A's later commit is rejected by Iceberg's optimistic concurrency control, since its baseline snapshot no longer matches the table's current state. Real timestamps for the baseline read, the winning commit, and the crash are logged to `logs/simulation_history.log` and `logs/occ_error.log`, and used to build a fully accurate (non-fabricated) explanation in chat.

---

## 6. Frontend

| Component | Responsibility |
|---|---|
| `Dashboard.tsx` | Table health cards, fragmentation trend chart (Recharts `AreaChart`), 50-batch load trigger with real completion polling |
| `AICopilot.tsx` | Full chat interface — Markdown rendering, proactive alert display, confirmation card locking |
| `IcebergMetrics.tsx` | Deep telemetry view (raw snapshots/manifests/files) |
| `OCCVisualizer.tsx` | Streams and displays the live OCC simulation |

The dashboard connects to the MCP server via a single **persistent SSE client connection**, reused across all tool calls — including the 4-second status polling loop used to detect when a triggered pipeline run has actually finished.

---

## 7. Running the Project

1. Ensure Postgres is running and reachable via `DATABASE_URL` in `.env`.
2. Run the one-time schema setup (`scripts/one_time_schema_setup.py`) to add `updated_at`/`created_at` columns and create the `pipeline_watermark` table, if not already applied.
3. Start the FastMCP tool server (port 8000).
4. Start the FastAPI agent backend (`main.py`, port 8001) — requires `GROQ_API_KEY` in `.env`.
5. Start the frontend (`npm run dev` in `frontend/`).
6. From the dashboard: click **Trigger 50-Batch Load** to degrade a table, then use the chat panel to ask "is the orders table healthy?" or say "clean it up" to confirm and run maintenance.