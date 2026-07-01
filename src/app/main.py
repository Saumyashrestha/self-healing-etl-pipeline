# src/app/main.py
from fastapi.middleware.cors import CORSMiddleware
from mcp.server.fastmcp import FastMCP
from src.app.tools import register_tools

mcp = FastMCP("LakehouseMaintenance")
register_tools(mcp)

# THE FIX: Use sse_app() so Python actually speaks the SSE protocol
app = mcp.sse_app()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"] # Crucial for the browser to read the SSE session
)