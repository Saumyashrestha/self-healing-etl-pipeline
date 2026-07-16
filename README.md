# Self-Healing ETL Pipeline with a Maintenance Copilot

An incremental-load pipeline into Apache Iceberg that deliberately accumulates the "small-file problem," paired with an AI copilot that can observe table health and perform controlled, human-confirmed maintenance.

---


## Overview

Real-world lakehouses degrade quietly: an ingestion job appends small batches for months, nobody runs maintenance, file counts explode, and queries slow down. This project reproduces that failure mode deliberately, then builds the tooling to diagnose and fix it:

1. A genuine incremental (merge/upsert) load pipeline from a Postgres OLTP source into two Iceberg fact tables.
2. Fifty-plus small append/update batches that intentionally fragment the tables under merge-on-read.
3. Real, measured health metrics — before and after maintenance — computed from Iceberg's own metadata tables, never invented.
4. An AI agent that can observe that state and take a destructive maintenance action **only** after explicit human confirmation.

---

## Features

- **Watermark-based CDC pipeline** — reads only what changed in Postgres since the last checkpoint, not a full reload every run.
- **Realistic OLTP data** — customer and product popularity follow capped, tiered skew (not uniform randomness); catalog prices are fixed per product with discrete sale tiers; order statuses include cancellations, returns, payment failures, and stuck orders.
- **Live health scoring** — a single, shared formula (fragmentation + delete-bloat weighted) used identically by the chat agent and the dashboard trend chart.
- **Human-in-the-loop maintenance** — the agent can only *propose* maintenance; execution requires an explicit confirmation step, enforced both in the UI and the backend.
- **Proactive monitoring** — a scheduled job checks table health every minute and proactively alerts in chat if a table crosses a fixed threshold, without waiting to be asked.
- **OCC conflict simulation (stretch)** — two concurrent Spark writers touch the same partition; the resulting Iceberg commit rejection is logged with real timestamps and explained by the agent in plain language on request.

---

## Architecture

Two independent backend processes, one frontend:

```
┌──────────────────────────────┐        ┌──────────────────────────────┐
│   FastAPI Chat Backend        │        │      FastMCP Tool Server     │
│   (main.py — port 8001)       │◀──────▶│   (src/app/main.py           │
│                                │  MCP   │    + src/app/tools.py        │
│  - /chat (LLM + tool loop)    │  (SSE) │        — port 8000)          │
│  - /api/agent-notifications   │        │                              │
│    (SSE proactive alerts)     │        │  - get_table_health          │
│  - /api/simulate-occ (SSE)    │        │  - propose_maintenance       │
│  - APScheduler (1-min audit)  │        │  - execute_confirmed_        │
└──────────────┬─────────────────┘        │    maintenance               │
               │                          │  - run_incremental_load      │
               │                          │  - get_pipeline_status       │
               │                          │  - get_table_history         │
               │                          │  - get_deep_telemetry        │
               │                          └──────────────┬───────────────┘
               │                                         │
               │            ┌────────────────────────────┘
               │            │
    ┌──────────▼────────────▼──────────┐
    │        React Dashboard            │
    │  Dashboard.tsx  (direct MCP link) │
    │  AICopilot.tsx  (via FastAPI)     │
    └────────────────────────────────────┘

               ┌────────────────────────────────┐
               │   Shared Spark Session          │
               │  one JVM, reused across all      │
               │  pipeline/maintenance/metrics    │
               │  calls, never stopped mid-life   │
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

**Note:** the dashboard connects to the MCP tool server directly (via `@modelcontextprotocol/sdk`'s JS client), separately from the chat panel's path through the FastAPI backend. Both reach the same tool server.

---

## Tech Stack

| Layer | Technology |
|---|---|
| Table format | Apache Iceberg (Hadoop catalog, local warehouse), format-version 2, merge-on-read |
| Compute engine | Apache Spark (PySpark) |
| OLTP source | PostgreSQL |
| Backend (chat/agent) | FastAPI + Groq (Llama 3.3 70B) |
| Backend (tools) | FastMCP (Model Context Protocol server) |
| Scheduling | APScheduler (proactive health checks) |
| Postgres driver (mutation) | psycopg2 |
| Postgres driver (Spark reads) | JDBC (`org.postgresql:postgresql:42.7.3`) |
| Frontend | React + TypeScript, Tailwind CSS |
| Charting | Recharts |
| MCP client (Python) | `mcp.client.sse` / `mcp.client.session` |
| MCP client (JS) | `@modelcontextprotocol/sdk` (SSE transport) |
| Markdown rendering | react-markdown |

---

## Prerequisites

- Python 3.10+
- Node.js 18+ and npm
- A running PostgreSQL instance
- A Groq API key ([console.groq.com](https://console.groq.com))
- Java 8/11/17 (required by PySpark)

---

## Installation

```bash
# 1. Clone and enter the project
git clone <your-repo-url>
cd project2-self-healing-etl

# 2. Python dependencies (ideally inside a virtual environment)
pip install -r requirements.txt

# 3. Frontend dependencies
cd frontend
npm install
cd ..
```

---

## Configuration

Create a `.env` file in the project root:

```env
DATABASE_URL=postgresql://<user>:<password>@<host>:<port>/<dbname>
GROQ_API_KEY=<your-groq-api-key>
```

---

## Running the Project

Run these in order, each in its own terminal:

```bash
# 1. Seed the OLTP source (drops/recreates orders, order_items; loads 10,000 baseline orders)
python data_generate.py seed

# 2. One-time schema setup (adds updated_at/created_at columns, creates pipeline_watermark)
python run_ddl_setup.py

# 3. Clear any stale watermark rows
python reset_watermark.py    

# 4. Start the FastMCP tool server (port 8000)
python -m src.app.main

# 5. Start the FastAPI chat backend (port 8001)
python main.py

# 6. Start the frontend
cd frontend
npm run dev
```

Open the frontend URL printed by Vite/npm (typically `http://localhost:5173`).

---

## Usage

1. From the **Dashboard** tab, click **Trigger 50-Batch Load** to run the incremental pipeline and intentionally degrade a table.
2. Watch the trend chart — file count should climb faster than snapshot count as merge-on-read accumulates small data and delete files.
3. Switch to the **AI Copilot** tab and ask:
   - *"Is the orders table healthy?"* — the agent reports real, current metrics.
   - *"Clean it up."* — the agent proposes maintenance; a confirmation card appears with **Execute Compaction** / **Dismiss**.
4. Click **Execute Compaction** — this is the only path that actually runs the destructive maintenance procedure. Post-maintenance health is reported using real numbers, not a generated summary.
5. (Stretch) Go to the **OCC Simulation** tab, click **Trigger Data Collision**, then ask the copilot *"What is the OCC conflict that occurred?"* for a plain-language, log-grounded explanation.

---

## Project Structure

```
project2-self-healing-etl/
├── data_generate.py            # Postgres seeding + incremental OLTP simulation
├── main.py                     # FastAPI chat backend (port 8001)
├── mcp_client.py                # Python MCP client bridge (FastAPI -> MCP tool server)
├── run_ddl_setup.py             # One-time Postgres schema setup
├── reset_watermark.py           # Truncates pipeline_watermark for a clean reset
├── requirements.txt
├── frontend/
│   └── src/
│       ├── App.tsx
│       └── components/
│           ├── AICopilot.tsx
│           ├── Dashboard.tsx
│           ├── OCCDiagram.tsx
│           └── OCCVisualizer.tsx
├── logs/
│   ├── occ_error.log
│   └── simulation_history.log
├── src/
│   ├── app/
│   │   ├── main.py              # MCP tool server entrypoint (port 8000)
│   │   └── tools.py              # MCP tool definitions
│   ├── ingestion/
│   │   └── pipeline.py           # Watermark-based CDC load + degrade simulation
│   ├── maintenance/
│   │   └── maintenance.py        # Iceberg compaction/expiry procedures
│   ├── monitoring/
│   │   ├── metrics.py            # Live health scoring
│   │   ├── history_logger.py     # Persistent JSONL trend history
│   │   └── spark_session.py      # Shared SparkSession singleton
│   └── utils/
│       └── catalog.py            # Realistic fake-data generation logic
├── tests/
│   ├── test_iceberg_concurrency.py  # OCC simulation script
│   └── verify_data.py               # Manual ad-hoc query script
└── warehouse/                    # Iceberg data + JSONL health history (git-ignored)
```

---

## MCP Tools Reference

| Tool | Purpose |
|---|---|
| `get_table_health` | Live health metrics for one or more tables |
| `propose_maintenance` | Step 1 of the human-in-the-loop flow — announces intent only, no side effects |
| `execute_confirmed_maintenance` | Step 2 — runs the actual destructive maintenance, only reachable after confirmation |
| `run_incremental_load` | Triggers the 50-batch pipeline as a background task; rejects overlapping runs |
| `get_pipeline_status` | Polled by the dashboard to detect real completion of a triggered load |
| `get_table_history` | Returns the full persisted JSONL history for the trend chart |
| `get_deep_telemetry` | Raw snapshot/manifest/file listings for detailed inspection |

---

## API Endpoints

| Endpoint | Method | Server | Purpose |
|---|---|---|---|
| `/chat` | POST | FastAPI (:8001) | Main copilot endpoint |
| `/api/agent-notifications` | GET (SSE) | FastAPI (:8001) | Streams proactive health alerts |
| `/api/simulate-occ` | GET (SSE) | FastAPI (:8001) | Runs and streams the OCC concurrency simulation |
| `/sse` | GET (SSE) | MCP server (:8000) | MCP tool-call transport |

---

## Resetting Project State

Run this whenever the underlying data-generation logic changes, or state becomes inconsistent:

```bash
# Windows PowerShell
Remove-Item -Recurse -Force warehouse
python data_generate.py seed
python run_ddl_setup.py
python reset_watermark.py

# macOS/Linux
rm -rf warehouse/
python data_generate.py seed
python run_ddl_setup.py
python reset_watermark.py
```

This clears the Iceberg warehouse (tables, snapshots, health history logs), re-seeds Postgres with fresh baseline data, restores the CDC-required columns and watermark table, and clears any stale watermark rows.

---

## Known Limitations

- `_pipeline_status` and `maintenance_in_progress` are in-memory flags local to the FastAPI process — not persisted, not shared across multiple backend instances.
- The dashboard connects to the MCP tool server directly, bypassing the FastAPI backend, while the chat panel goes through FastAPI — two independent client paths to the same server.
- The health-alert threshold (70%) is a fixed constant, not derived from a formal SLA.
- No automated test suite; `tests/` contains manual verification scripts, not pytest tests.

---