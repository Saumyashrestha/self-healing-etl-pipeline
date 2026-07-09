import os
import json
import re
import asyncio
import subprocess
import sys
from fastapi.responses import StreamingResponse
from fastapi import FastAPI, Request

from pathlib import Path
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from dotenv import load_dotenv
from fastapi.concurrency import run_in_threadpool

from groq import Groq
from src.monitoring.metrics import get_table_metrics
from src.maintenance.maintenance import execute_table_maintenance
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

# --- PROACTIVE AGENT STATE ---
agent_message_queue = asyncio.Queue()

agent_message_clients = []

async def run_health_audit():
    """Silently fetches metrics and asks the LLM if a warning is needed."""
    try:
        orders_metrics = await run_in_threadpool(get_table_metrics, "orders")
        items_metrics = await run_in_threadpool(get_table_metrics, "order_items")
        
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
                "content": f"Orders Table Metrics: {orders_metrics}\nOrder Items Table Metrics: {items_metrics}"
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

        response = await run_in_threadpool(
            client.chat.completions.create,
            model="llama-3.3-70b-versatile",
            messages=messages,
            tools=agent_tools,
            tool_choice="auto"
        )

        response_message = response.choices[0].message
        content_text = response_message.content or ""
        
        has_tool_call = False
        function_name = None
        function_args = {}
        tool_call_id = "call_fallback"

        # 1. Standard API Tool Call
        if response_message.tool_calls:
            has_tool_call = True
            tool_call = response_message.tool_calls[0]
            function_name = tool_call.function.name
            function_args = json.loads(tool_call.function.arguments)
            tool_call_id = tool_call.id
            messages.append(response_message)
            
        # 2. BULLETPROOF XML PARSER (Catches Llama-3's leaked tags)
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

        # --- EXECUTE THE TOOL IF DETECTED ---
        if has_tool_call and function_name:
            
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
                    # Fallback check just in case the LLM passes the old singular key
                    raw_names = function_args.get("table_names", function_args.get("table_name", ""))
                    tables = [t.strip() for t in raw_names.split(',')]
                    reports = []
                    for t in tables:
                        metrics = await run_in_threadpool(get_table_metrics, t)
                        reports.append(f"[{t.upper()}]: {metrics}")
                    tool_result = " \n ".join(reports)
                except Exception as e:
                    tool_result = f"Error: {str(e)}"
                    
            elif function_name == "execute_confirmed_maintenance":
                raw_names = function_args.get("table_names", function_args.get("table_name", ""))
                tables = [t.strip() for t in raw_names.split(',')]
                results = []
                for t in tables:
                    res = await run_in_threadpool(execute_table_maintenance, t)
                    results.append(res)
                tool_result = " \n ".join(results)

            elif function_name == "analyze_occ_crash_log":
                log_path = BASE_DIR / "logs" / "occ_error.log"
                crash_time = "[Timestamp Not Found]"
                error_content = "Log file not found."  # <-- FIX: Defined here
                
                if log_path.exists():
                    with open(log_path, "r") as f:
                        error_content = f.read()  # <-- FIX: Read the file
                        
                    # Extract the timestamp from the content we just read
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
            
            final_response = await run_in_threadpool(
                client.chat.completions.create,
                model="llama-3.3-70b-versatile",
                messages=messages
            )
            return {"reply": final_response.choices[0].message.content}
            
        else:
            return {"reply": content_text if content_text else "How can I assist you?"}

    except Exception as e:
        return {"reply": f"Agent error: {str(e)}"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="127.0.0.1", port=8001, reload=True)