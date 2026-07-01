# src/app/tools.py
from mcp.server.fastmcp import FastMCP
from src.monitoring.metrics import get_table_metrics 
from src.maintenance.maintenance import execute_table_maintenance

def register_tools(mcp: FastMCP):
    @mcp.tool()
    async def get_table_health(table_name: str) -> str:
        """Fetches health metrics for the requested Iceberg table."""
        metrics = get_table_metrics(table_name)
        return f"Health Report for {table_name}: {metrics}"

    @mcp.tool()
    async def run_maintenance(table_name: str, user_confirmed: bool) -> str:
        """
        Executes compaction and snapshot expiration. 
        The agent MUST NOT set user_confirmed to True unless the user has explicitly typed a confirmation (e.g., 'Yes', 'Proceed', 'Clean it up').
        """
        if not user_confirmed:
            return "Execution rejected. You must ask the user for explicit confirmation before running maintenance."
        
        # Call the PySpark logic
        result = execute_table_maintenance(table_name)
        return result
    
    @mcp.tool()
    async def get_table_history(table_name: str) -> str:
        """
        Returns a list of snapshot/file counts for the last 50 batches.
        Useful for populating the trend chart.
        """
        # Query your Iceberg history or the log you kept from your manual simulation
        # For now, return a list format the LLM/React can easily parse
        return f"""
        [
            {{"batch": 1, "files": 10, "snapshots": 2}},
            {{"batch": 25, "files": 150, "snapshots": 26}},
            {{"batch": 50, "files": 300, "snapshots": 51}},
            {{"batch": 51, "files": 5, "snapshots": 1}}
        ]
        """