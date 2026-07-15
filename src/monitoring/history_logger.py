# src/monitoring/history_logger.py
import os
import json
import time
from src.monitoring.metrics import calculate_health_score

def _history_path(table_name):
    root_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    hist_dir = os.path.join(root_dir, "warehouse", "history")
    os.makedirs(hist_dir, exist_ok=True)
    return os.path.join(hist_dir, f"{table_name}_history.jsonl")

def _next_step(table_name):
    path = _history_path(table_name)
    if not os.path.exists(path):
        return 0
    with open(path, "r") as f:
        return sum(1 for line in f if line.strip())

def compute_metrics_from_spark(spark, table_name):
    data_files = spark.sql(f"SELECT count(*) as c FROM local.db.{table_name}.files WHERE content = 0").collect()[0]['c']
    delete_files = spark.sql(f"SELECT count(*) as c FROM local.db.{table_name}.files WHERE content != 0").collect()[0]['c']  # changed from content = 1 to != 0, catches both delete types
    manifests = spark.sql(f"SELECT count(*) as c FROM local.db.{table_name}.manifests").collect()[0]['c']
    snapshots = spark.sql(f"SELECT count(*) as c FROM local.db.{table_name}.snapshots").collect()[0]['c']

    avg_row = spark.sql(f"SELECT round(avg(file_size_in_bytes)/1024,2) as avg_kb FROM local.db.{table_name}.files WHERE content = 0").collect()[0]
    avg_kb = avg_row['avg_kb'] if avg_row['avg_kb'] else 0.0

    del_avg_row = spark.sql(f"SELECT round(avg(file_size_in_bytes)/1024,2) as avg_kb FROM local.db.{table_name}.files WHERE content != 0").collect()[0]
    del_avg_kb = del_avg_row['avg_kb'] if del_avg_row['avg_kb'] else 0.0

    return {
        "files": data_files + delete_files,
        "data_files": data_files,
        "snapshots": snapshots,
        "manifests": manifests,
        "delete_files": delete_files,
        "avg_file_size_kb": avg_kb,
        "delete_file_avg_kb": del_avg_kb,
        "health_score": calculate_health_score(data_files, avg_kb, delete_files, del_avg_kb),
    }

def log_snapshot_with_session(spark, table_name, event_label):
    """Append ONE real, permanent row to this table's history log. Never overwrites."""
    metrics = compute_metrics_from_spark(spark, table_name)
    row = {"batch": _next_step(table_name), "event": event_label, "timestamp": time.time(), **metrics}
    with open(_history_path(table_name), "a") as f:
        f.write(json.dumps(row) + "\n")
    return row

def read_history(table_name):
    path = _history_path(table_name)
    if not os.path.exists(path):
        return []
    with open(path, "r") as f:
        return [json.loads(line) for line in f if line.strip()]