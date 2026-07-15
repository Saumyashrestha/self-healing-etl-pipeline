import os
import sys
from pyspark.sql import SparkSession
from src.monitoring.spark_session import get_shared_spark

def get_spark_session():
    """Helper function to initialize Spark consistently."""
    current_script_dir = os.path.dirname(os.path.abspath(__file__))
    root_dir = os.path.dirname(os.path.dirname(current_script_dir))
    warehouse_path = os.path.join(root_dir, "warehouse")
    
    spark = get_shared_spark()
    
    spark.sparkContext.setLogLevel("ERROR")
    return spark

def calculate_health_score(data_files: int, data_file_avg_kb: float, delete_files: int, delete_file_avg_kb: float) -> int:
    """Single source of truth for health scoring. 70% fragmentation weight,
    30% delete-bloat weight. Used by both the live chat tool and the
    dashboard trend-history logger, so the two never disagree."""
    fragmentation_score = 100 if data_files <= 5 else max(0, 100 - (data_files * 2))

    total_size = (data_files * data_file_avg_kb) + (delete_files * delete_file_avg_kb)
    bloat_ratio = (delete_files * delete_file_avg_kb) / total_size if total_size > 0 else 0
    bloat_score = 100 - (bloat_ratio * 100)

    return int((fragmentation_score * 0.70) + (bloat_score * 0.30))

def get_table_metrics(table_name: str) -> str:
    """
    Called by the AI Agent to check the health of a specific table.
    Returns a formatted string with the exact health score and file counts.
    """
    spark = get_spark_session()
    
    try:
        # Fetch Snapshots
        snapshot_df = spark.sql(f"SELECT count(*) as total_snapshots FROM local.db.{table_name}.snapshots")
        total_snapshots = snapshot_df.collect()[0]['total_snapshots']

        # Fetch Manifests
        manifest_df = spark.sql(f"SELECT count(*) as total_manifests FROM local.db.{table_name}.manifests")
        total_manifests = manifest_df.collect()[0]['total_manifests']

        # Fetch File Counts and Sizes
        files_df = spark.sql(f"""
            SELECT 
                content,
                count(*) as file_count,
                round(avg(file_size_in_bytes) / 1024, 2) as avg_size_kb 
            FROM local.db.{table_name}.files 
            GROUP BY content
        """).collect()

        data_files = 0
        data_file_avg_kb = 0.0
        delete_files = 0
        delete_file_avg_kb = 0.0

        for row in files_df:
            if row['content'] == 0:  # Data Files
                data_files = row['file_count']
                data_file_avg_kb = row['avg_size_kb']
            elif row['content'] == 1:  # Position Delete Files
                delete_files = row['file_count']
                delete_file_avg_kb = row['avg_size_kb']

        # Health score computed via the single shared function (also used by history_logger.py)
        health_score = calculate_health_score(data_files, data_file_avg_kb, delete_files, delete_file_avg_kb)

        return (
            f"Health Score: {health_score}%. "
            f"Active Data Files: {data_files}. "
            f"Delete Bloat Files: {delete_files}. "
            f"Snapshots: {total_snapshots}. "
            f"Manifests: {total_manifests}."
        )
        
    except Exception as e:
        return f"Error retrieving metrics for {table_name}. Ensure the table exists. Error: {str(e)}"
    
def capture_metrics(phase_label):
    """
    Legacy CLI logger for writing metrics to a text file.
    """
    print(f"Initializing Spark to capture [{phase_label}] metrics...")
    spark = get_spark_session()
    
    current_script_dir = os.path.dirname(os.path.abspath(__file__))
    root_dir = os.path.dirname(os.path.dirname(current_script_dir))
    report_path = os.path.join(root_dir, "warehouse_health_report.txt")

    tables_to_track = ["orders", "order_items"]
    
    for table_name in tables_to_track:
        try:
            metrics_string = get_table_metrics(table_name)
            report_content = f"\n============================================================\n" \
                             f"PHASE: {phase_label.upper()} | TABLE: {table_name.upper()}\n" \
                             f"============================================================\n" \
                             f"{metrics_string}\n"
                             
            print(report_content)
            with open(report_path, "a") as f:
                f.write(report_content)
                
        except Exception as e:
            print(f"Failed to capture {table_name}: {e}")

if __name__ == "__main__":
    phase = sys.argv[1] if len(sys.argv) > 1 else "current_state"
    capture_metrics(phase)