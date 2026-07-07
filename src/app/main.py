# src/app/main.py
import asyncio
from queue import Queue
from threading import Thread
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from mcp.server.fastmcp import FastMCP

from src.app.tools import register_tools
from src.ingestion.pipeline import run_pipeline 

mcp = FastMCP("LakehouseMaintenance")
register_tools(mcp)

# mcp.sse_app() creates a Starlette app instance
app = mcp.sse_app()

# --- THE REAL-TIME STREAMING ENDPOINT ---
# Define the function as a standard async handler accepting the Starlette request
async def simulate_load_endpoint(request):
    log_queue = Queue()

    # 1. Spin up PySpark in a background thread
    thread = Thread(target=run_pipeline, args=(log_queue,))
    thread.start()

    # 2. Generator that safely passes text chunks to React
    async def log_generator():
        while True:
            while not log_queue.empty():
                item = log_queue.get()
                if item is None: 
                    return
                yield f"data: {item}\n\n"
                
            await asyncio.sleep(0.1)

    return StreamingResponse(log_generator(), media_type="text/event-stream")

# FIX: Explicitly mount the route to the Starlette application instance
app.add_route("/api/simulate-load", simulate_load_endpoint, methods=["GET"])
# ----------------------------------------

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"] 
)