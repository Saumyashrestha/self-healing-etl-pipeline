# src/app/tools.py
from mcp.server.fastmcp import FastMCP
from src.monitoring.metrics import get_table_metrics 
from src.maintenance.maintenance import execute_table_maintenance
import asyncio
import json

_background_tasks = set()
_pipeline_status = {"running": False, "message": ""}

def register_tools(mcp: FastMCP):
    @mcp.tool()
    async def get_table_health(table_names: str) -> str:
        tables = [t.strip() for t in table_names.split(',')]
        reports = []
        for table in tables:
            metrics = get_table_metrics(table)
            reports.append(f"[{table.upper()}]: {metrics}")
        return " \n ".join(reports)

    @mcp.tool()
    async def propose_maintenance(table_names: str) -> str:
        return f"I am ready to run a full compaction and vacuum cycle on: **{table_names}**. This is a destructive storage operation."

    @mcp.tool()
    async def execute_confirmed_maintenance(table_names: str) -> str:
        from src.maintenance.maintenance import execute_table_maintenance
        tables = [t.strip() for t in table_names.split(',')]
        results = []
        for t in tables:
            res = execute_table_maintenance(t)
            results.append(res)
        return " \n ".join(results)
    
    @mcp.tool()
    async def run_incremental_load(batches: int = 50) -> str:
        """Triggers the PySpark incremental load simulation."""
        if _pipeline_status["running"]:
            return "A pipeline run is already in progress. Please wait for it to finish before triggering another."

        from src.ingestion.pipeline import run_pipeline

        async def _run_and_track():
            _pipeline_status["running"] = True
            _pipeline_status["message"] = "Running..."
            try:
                await asyncio.to_thread(run_pipeline)
                _pipeline_status["message"] = "Completed successfully."
            except Exception as e:
                _pipeline_status["message"] = f"Failed: {e}"
            finally:
                _pipeline_status["running"] = False

        task = asyncio.create_task(_run_and_track())
        _background_tasks.add(task)
        task.add_done_callback(_background_tasks.discard)

        return "Simulation started successfully! PySpark is running in the background."

    @mcp.tool()
    async def get_pipeline_status() -> str:
        """Returns whether a pipeline run is currently in progress."""
        return json.dumps(_pipeline_status)

    @mcp.tool()
    async def get_deep_telemetry(table_name: str) -> str:
        """Fetches raw Iceberg metadata (snapshots, manifests, files) for the deep telemetry tab."""
        from src.monitoring.spark_session import get_shared_spark
        try:
            spark = get_shared_spark()

            snaps_df = spark.sql(f"SELECT cast(committed_at as string) as committed_at, cast(snapshot_id as string) as snapshot_id, operation FROM local.db.{table_name}.snapshots ORDER BY committed_at DESC LIMIT 10")
            snaps = [row.asDict() for row in snaps_df.collect()]

            manifests_df = spark.sql(f"SELECT split(path, '/')[size(split(path, '/'))-1] as file_name, length, cast(added_snapshot_id as string) as snapshot_id FROM local.db.{table_name}.manifests LIMIT 10")
            manifests = [row.asDict() for row in manifests_df.collect()]

            files_df = spark.sql(f"SELECT split(file_path, '/')[size(split(file_path, '/'))-1] as file_name, file_format, record_count, file_size_in_bytes FROM local.db.{table_name}.files WHERE content = 0 LIMIT 10")
            files = [row.asDict() for row in files_df.collect()]

            return json.dumps({"snapshots": snaps, "manifests": manifests, "files": files})
        except Exception as e:
            return json.dumps({"error": str(e)})
        
    @mcp.tool()
    async def get_table_history(table_name: str) -> str:
        """Returns the real, persisted metrics history for a table (all batches + maintenance events)."""
        from src.monitoring.history_logger import read_history
        history = read_history(table_name)
        return json.dumps(history)
    
    @mcp.tool()
    async def generate_incident_report(table_name: str) -> str:
        from src.monitoring.incident_report import generate_incident_report as gen_report
        return gen_report(table_name)