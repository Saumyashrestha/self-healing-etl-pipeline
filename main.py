import os
import json
import re
import asyncio
import subprocess
import sys
from datetime import datetime
from fastapi.responses import StreamingResponse
from fastapi import FastAPI, Request
from mcp_client import call_mcp_tool

from pathlib import Path
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from dotenv import load_dotenv
from fastapi.concurrency import run_in_threadpool

from groq import Groq
from contextlib import asynccontextmanager
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# --- DIRECTORY AND ENV SETUP ---
BASE_DIR = Path(__file__).resolve().parent
env_path = BASE_DIR / ".env"
load_dotenv(dotenv_path=env_path)

api_key = os.getenv("GROQ_API_KEY") 
if not api_key:
    raise ValueError("CRITICAL: GROQ_API_KEY not found in .env file.")

client = Groq(api_key=api_key)

# --- 1. DEFINE TOOLS AS JSON SCHEMAS (UPDATED TO PLURAL 'table_names') ---
agent_tools = [
    {
        "type": "function",
        "function": {
            "name": "get_table_health",
            "description": "Fetches health metrics for the requested Iceberg table(s).",
            "parameters": {
                "type": "object",
                "properties": {
                    "table_names": {"type": "string", "description": "Comma-separated table names (e.g., 'orders,order_items')"}
                },
                "required": ["table_names"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "propose_maintenance",
            "description": "Proposes maintenance. Must be called FIRST when user asks to clean or optimize.",
            "parameters": {
                "type": "object",
                "properties": {
                    "table_names": {"type": "string", "description": "Comma-separated table names to compact"}
                },
                "required": ["table_names"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "execute_confirmed_maintenance",
            "description": "Executes compaction. ONLY call this if user explicitly confirmed.",
            "parameters": {
                "type": "object",
                "properties": {
                    "table_names": {"type": "string", "description": "Comma-separated table names"}
                },
                "required": ["table_names"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "analyze_occ_crash_log",
            "description": "Reads the OCC error log to explain why a concurrency conflict occurred.",
            "parameters": {
                "type": "object",
                "properties": {},
                "required": []
            }
        }
    }
]

async def is_pipeline_running() -> bool:
    """Asks Server A (the real toolbox) whether the 50-batch load is currently running."""
    try:
        status_str = await call_mcp_tool("get_pipeline_status", {})
        status = json.loads(status_str)
        return status.get("running", False)
    except Exception as e:
        print(f"[Pipeline Check] Error checking pipeline status: {e}")
        return False

# --- PROACTIVE AGENT STATE ---
agent_message_queue = asyncio.Queue()

agent_message_clients = []
maintenance_in_progress = {"active": False}

async def run_health_audit():
    if maintenance_in_progress["active"]:
        print("[Agent Monitor] Skipping health audit — maintenance currently in progress.")
        return

    if await is_pipeline_running():
        print("[Agent Monitor] Skipping health audit — pipeline load currently in progress.")
        return
    try:
        combined_metrics = await call_mcp_tool("get_table_health", {"table_names": "orders,order_items"})
        
        messages = [
            {
                "role": "system",
                "content": (
                    "You are a proactive monitoring agent for an Apache Iceberg database. "
                    "Review the provided metrics for BOTH the 'orders' and 'order_items' tables. "
                    "If EITHER table is highly fragmented (e.g., too many small data files, large delete bloat), "
                    "output a SHORT, urgent 1-2 sentence warning suggesting compaction. "
                    "If BOTH tables are reasonably healthy, output EXACTLY the word: HEALTHY."
                )
            },
            {
                "role": "user", 
                "content": f"Combined Table Metrics: {combined_metrics}"
            }
        ]
        
        response = await run_in_threadpool(
            client.chat.completions.create,
            model="llama-3.3-70b-versatile",
            messages=messages
        )
        
        evaluation = response.choices[0].message.content.strip()
        print(f"[Agent Monitor] LLM thought: {evaluation}")
        
        if "HEALTHY" not in evaluation.upper():
            # 3. Target BOTH tables in the UI button payload
            alert_payload = json.dumps({
                "content": evaluation,
                "requires_confirmation": True,
                "target_table": "orders, order_items"  # <-- Fix is here
            })
            
            for client_queue in agent_message_clients:
                await client_queue.put(alert_payload)
                
    except Exception as e:
        print(f"[Agent Monitor] Error: {e}")

# --- APP LIFECYCLE ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    scheduler = AsyncIOScheduler()
    scheduler.add_job(run_health_audit, 'interval', minutes=1) 
    scheduler.start()
    print("[Agent Monitor] Proactive background scheduler started (2-min interval).")
    yield
    scheduler.shutdown()

# --- 2. FASTAPI SETUP ---
app = FastAPI(title="Iceberg Agentic Copilot Backend", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class ChatRequest(BaseModel):
    message: str

@app.get("/api/simulate-occ")
def stream_occ_simulation():
    def event_generator():
        script_path = BASE_DIR / "tests" / "test_iceberg_concurrency.py"
        
        # We use a synchronous Popen with bufsize=1 (line buffered)
        process = subprocess.Popen(
            [sys.executable, "-u", str(script_path)],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1
        )

        try:
            for line in iter(process.stdout.readline, ''):
                stripped = line.strip()
                if not stripped:
                    continue
                
                # Logic: Only stream things marked [LOG] or [DATA]
                if stripped.startswith("[LOG]") or stripped.startswith("[DATA]"):
                    yield f"data: {stripped}\n\n"
            
            process.stdout.close()
            process.wait()
            yield "data: [SIMULATION_COMPLETE]\n\n"
            
        except Exception as e:
            yield f"data: [LOG] CRITICAL ERROR: {str(e)}\n\n"
            yield "data: [SIMULATION_COMPLETE]\n\n"

    return StreamingResponse(event_generator(), media_type="text/event-stream")

@app.get("/api/agent-notifications")
async def stream_agent_notifications(request: Request):
    """Holds an open connection to stream background warnings to all connected clients."""
    # 1. Create a personal queue just for this specific UI component
    client_queue = asyncio.Queue()
    agent_message_clients.append(client_queue)
    
    async def event_generator():
        try:
            while True:
                # Wait until a warning drops into this specific queue
                message = await client_queue.get()
                yield f"data: {message}\n\n"
        except asyncio.CancelledError:
            # This cleanly handles when the user closes the browser tab
            pass
        finally:
            # Cleanup: remove the queue when the component disconnects
            if client_queue in agent_message_clients:
                agent_message_clients.remove(client_queue)
                
    return StreamingResponse(event_generator(), media_type="text/event-stream")

@app.post("/chat")
async def chat_endpoint(request: ChatRequest):
    try:
        # --- DETERMINISTIC CONFIRMATION INTERCEPTOR ---
        confirm_match = re.match(r"Yes, please proceed with execute_confirmed_maintenance on the (.+) table\.", request.message.strip())
        cancel_match = re.match(r"No, cancel the maintenance operation on (.+)\.", request.message.strip())

        if confirm_match:
            raw_names = confirm_match.group(1)

            if await is_pipeline_running():
                return {"reply": "⚠️ A pipeline load is currently in progress. Please wait for it to finish before running maintenance — running both at once can cause write conflicts on the table."}

            maintenance_in_progress["active"] = True
            try:
                maintenance_result = await call_mcp_tool("execute_confirmed_maintenance", {"table_names": raw_names})
            finally:
                maintenance_in_progress["active"] = False

            health_report = await call_mcp_tool("get_table_health", {"table_names": raw_names})

            reply = (
                "✅ **Maintenance complete.**\n\n"
                + maintenance_result
                + "\n\n**Post-Maintenance Health:**\n"
                + health_report
            )
            return {"reply": reply}

        if cancel_match:
            return {"reply": f"Understood — maintenance on {cancel_match.group(1)} has been cancelled. No changes were made."}

        # --- DETERMINISTIC OCC INTERCEPTOR ---
        occ_keywords = ["occ", "concurrency", "conflict"]
        if any(kw in request.message.lower() for kw in occ_keywords):
            history_path = BASE_DIR / "logs" / "simulation_history.log"
            error_path = BASE_DIR / "logs" / "occ_error.log"

            baseline_time = None
            commit_time = None

            if history_path.exists():
                with open(history_path, "r") as f:
                    for line in f:
                        line = line.strip()
                        if not line or " " not in line:
                            continue
                        ts_str, msg = line.split(" ", 1)
                        if "Reading partition to establish snapshot baseline" in msg and baseline_time is None:
                            baseline_time = ts_str
                        if "COMMIT SUCCESS" in msg and commit_time is None:
                            commit_time = ts_str

            crash_time = None
            if error_path.exists():
                with open(error_path, "r") as f:
                    for line in f:
                        if "CRASH TIMESTAMP:" in line:
                            crash_time = line.split("CRASH TIMESTAMP:")[1].strip()
                            break

            if baseline_time and commit_time and crash_time:
                fmt = "%H:%M:%S.%f"
                t_base = datetime.strptime(baseline_time, fmt)
                t_commit = datetime.strptime(commit_time, fmt)
                t_crash = datetime.strptime(crash_time, fmt)

                baseline_to_crash = (t_crash - t_base).total_seconds()
                commit_to_crash = (t_crash - t_commit).total_seconds()

                reply = (
                    "**1. TIMELINE:**\n"
                    f"- `{baseline_time}` — Worker A read the ELECTRONICS partition, establishing its snapshot baseline.\n"
                    f"- `{commit_time}` — Worker B committed its update to the same partition first, advancing the table's snapshot.\n"
                    f"- `{crash_time}` — Worker A attempted to commit its own update, {baseline_to_crash:.2f}s after its baseline read "
                    f"and {commit_to_crash:.2f}s after Worker B's commit. The commit was rejected.\n\n"
                    "**2. ROOT CAUSE:**\n"
                    "Worker A was still operating on the table state it read at its baseline timestamp. By the time it tried to "
                    "commit, Worker B had already advanced the table to a new snapshot. Iceberg's Optimistic Concurrency Control "
                    "detected that Worker A's base snapshot no longer matched the table's current snapshot and rejected the write "
                    "to prevent silently overwriting Worker B's change.\n\n"
                    "**3. SYSTEM IMPACT:**\n"
                    "- Data Integrity: Preserved — no corrupted data or lost updates\n"
                    "- Worker A Status: Failed — commit aborted\n"
                    "- Worker B Status: Succeeded — its update is the one reflected in the table\n"
                    "- Storage State: Worker A's partial data files are now orphaned and will be cleaned up by the next maintenance/compaction run"
                )

                return {
                    "reply": reply,
                    "show_occ_diagram": True,
                    "occ_timeline": {
                        "baseline_time": baseline_time,
                        "commit_time": commit_time,
                        "crash_time": crash_time
                    }
                }
            else:
                reply = "I couldn't find a complete OCC conflict record. Please run the OCC simulation first, then ask again."
                return {"reply": reply}
        
        messages = [
            {
                "role": "system", 
                "content": (
                    "You are a Data Engineering Copilot managing two specific Apache Iceberg tables: 'orders' and 'order_items'. "
                    "When asked about table health, ALWAYS call the get_table_health tool. "
                    "If a user asks to clean, compact, or maintain a table, you MUST call 'propose_maintenance' first. "
                    "CRITICAL: When asked about an OCC conflict, you MUST call 'analyze_occ_crash_log'. "
                    "CRITICAL: When responding with data from 'analyze_occ_crash_log', you MUST format your response EXACTLY like this template:\n\n"
                    "**1. TIMELINE:**\n"
                    "- Worker A Baseline: T-Minus 8s\n"
                    "- Worker B Commit: T-Minus 2s\n"
                    "- Worker A Crash: [Insert Crash Timestamp from tool data]\n\n"
                    "**2. ROOT CAUSE:**\n[Insert Cause from tool data]\n\n"
                    "**3. SYSTEM IMPACT:**\n[Insert Impact from tool data]\n\n"
                    "DO NOT write introductory or concluding paragraphs. DO NOT deviate from this layout."
                )
            },
            {"role": "user", "content": request.message}
        ]

        maintenance_keywords = ["health", "clean", "maintain", "maintenance", "compact", "optimize", "fragmented", "orders", "order_items", "table"]
        needs_tools = any(kw in request.message.lower() for kw in maintenance_keywords)

        max_iterations = 5
        for iteration in range(max_iterations):
            response = await run_in_threadpool(
                client.chat.completions.create,
                model="llama-3.3-70b-versatile",
                messages=messages,
                tools=agent_tools,
                tool_choice="auto" if (needs_tools and iteration == 0) else "none"
            )

            response_message = response.choices[0].message
            content_text = response_message.content or ""

            has_tool_call = False
            function_name = None
            function_args = {}
            tool_call_id = "call_fallback"

            if response_message.tool_calls:
                has_tool_call = True
                tool_call = response_message.tool_calls[0]
                function_name = tool_call.function.name
                function_args = json.loads(tool_call.function.arguments)
                tool_call_id = tool_call.id
                messages.append(response_message.model_dump(exclude_none=True))   # <-- was: messages.append(response_message)

            elif "<function=" in content_text:
                match = re.search(r'<function=(\w+)(.*?)(?:</function>|>)?', content_text)
                if match:
                    has_tool_call = True
                    function_name = match.group(1)
                    try:
                        function_args = json.loads(match.group(2).strip())
                    except:
                        function_args = {}
                    messages.append({"role": "assistant", "content": content_text})

            print(f"[Chat Loop] tool_call={has_tool_call}, function={function_name}, args={function_args}")

            if not has_tool_call:
                # Model gave a plain final answer -- we're done
                return {"reply": content_text if content_text else "How can I assist you?"}

            # --- INTERCEPTOR: The HITL Guard ---
            if function_name == "propose_maintenance":
                target = function_args.get("table_names", function_args.get("table_name", "all tables"))
                return {
                    "reply": f"I am ready to run a full compaction and vacuum cycle on: **{target}**. This is a destructive storage operation.",
                    "requires_confirmation": True,
                    "target_table": target
                }

            tool_result = ""
            if function_name == "get_table_health":
                try:
                    raw_names = function_args.get("table_names", function_args.get("table_name", ""))
                    tool_result = await call_mcp_tool("get_table_health", {"table_names": raw_names})
                except Exception as e:
                    tool_result = f"Error: {str(e)}"
                return {"reply": tool_result}

            elif function_name == "execute_confirmed_maintenance":
                raw_names = function_args.get("table_names", function_args.get("table_name", ""))

                if await is_pipeline_running():
                    tool_result = "Cannot run maintenance right now — a pipeline load is currently in progress."
                else:
                    maintenance_in_progress["active"] = True
                    try:
                        tool_result = await call_mcp_tool("execute_confirmed_maintenance", {"table_names": raw_names})
                    finally:
                        maintenance_in_progress["active"] = False

            elif function_name == "analyze_occ_crash_log":
                log_path = BASE_DIR / "logs" / "occ_error.log"
                crash_time = "[Timestamp Not Found]"
                error_content = "Log file not found."

                if log_path.exists():
                    with open(log_path, "r") as f:
                        error_content = f.read()
                    for line in error_content.split('\n'):
                        if "CRASH TIMESTAMP:" in line:
                            crash_time = line.split("CRASH TIMESTAMP:")[1].strip()
                            break

                tool_result = (
                    "SYSTEM INSTRUCTION: You MUST output EXACTLY this text. Use DOUBLE NEWLINES (\\n\\n) between every line so the frontend renders it correctly.\n\n"
                    "1. CHRONOLOGICAL TIMELINE:\n\n"
                    "- T-Minus 8s: Worker A established its baseline snapshot.\n\n"
                    "- T-Minus 2s: Worker B finished its write and committed, advancing the table version.\n\n"
                    f"- {crash_time}: Worker A attempted to commit. Validation failed because its baseline was stale.\n\n"
                    "2. THE CONFLICT DYNAMICS:\n\n"
                    "- Worker A was operating on a 'stale' view of the table.\n\n"
                    "- Iceberg enforces atomic updates. Worker A's snapshot didn't match the current table state, so it was rejected.\n\n"
                    "3. SYSTEM IMPACT:\n\n"
                    "- Data Integrity: Preserved (No corrupted data or race-condition overwrites).\n\n"
                    "- Worker A Status: Failed (Commit aborted).\n\n"
                    "- Storage State: Worker A's partial data files are now 'orphaned' and require cleanup."
                )

            messages.append({
                "tool_call_id": tool_call_id,
                "role": "tool",
                "name": function_name,
                "content": tool_result,
            })
            # loop continues -- model gets to see the tool result and decide what to do next

        return {"reply": "I wasn't able to complete this request within the allowed number of steps."}

    except Exception as e:
        return {"reply": f"Agent error: {str(e)}"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="127.0.0.1", port=8001, reload=True)