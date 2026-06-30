import os
import sys
from pyspark.sql import SparkSession

def capture_metrics(phase_label):
    """
    Connects to Iceberg metadata tables, extracts health metrics,
    and appends them to a central health report file.
    """
    print(f"Initializing Spark to capture [{phase_label}] metrics...")
    
    current_script_dir = os.path.dirname(os.path.abspath(__file__))
    root_dir = os.path.dirname(os.path.dirname(current_script_dir))
    warehouse_path = os.path.join(root_dir, "warehouse")
    report_path = os.path.join(root_dir, "warehouse_health_report.txt")

    spark = SparkSession.builder \
        .appName("Iceberg-Metrics-Tracker") \
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type", "hadoop") \
        .config("spark.sql.catalog.local.warehouse", warehouse_path) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # 1. Fetch Snapshot Count
    snapshot_df = spark.sql("SELECT count(*) as total_snapshots FROM local.db.orders.snapshots")
    total_snapshots = snapshot_df.collect()[0]['total_snapshots']

    # 2. Fetch File Counts and Sizes
    files_df = spark.sql("""
        SELECT 
            content,
            count(*) as file_count,
            round(avg(file_size_in_bytes) / 1024, 2) as avg_size_kb 
        FROM local.db.orders.files 
        GROUP BY content
    """).collect()

    # Parse the file metrics safely
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

    # 3. Format the report block
    report_content = f"""
============================================================
PHASE: {phase_label.upper()}
============================================================
1. Table Snapshots:          {total_snapshots}
2. Active Data Files:         {data_files} (Avg Size: {data_file_avg_kb} KB)
3. Active Position Deletes:   {delete_files} (Avg Size: {delete_file_avg_kb} KB)
"""

    # Print to console for immediate visibility
    print(report_content)

    # Append to the permanent text report file
    with open(report_path, "a") as f:
        f.write(report_content)
        
    print(f"Successfully appended metrics to: {report_path}\n")

# Verification: Check Snapshot Count
    print("\n--- AUDIT: VERIFYING SNAPSHOT EXPIRATION ---")
    
    tables = ["db.orders", "db.order_items"]
    for table in tables:
        count = spark.sql(f"SELECT count(*) as total FROM local.{table}.snapshots").collect()[0]['total']
        print(f"Table: {table} | Remaining Snapshots: {count}")
        if count == 1:
            print(f"  Success: Snapshots for {table} expired correctly.")
        else:
            print(f"  Warning: Expected 1, found {count}.")
    
    print("\n--- AUDIT: IDENTIFYING ORPHANED FILES ---")
    spark.sql("CALL local.system.remove_orphan_files(table => 'db.orders', dry_run => true)").show(vertical=True)

    spark.stop()

if __name__ == "__main__":
    # Allow passing the phase as an argument (e.g., python metrics.py before)
    # Default to "current_state" if no argument is passed
    phase = sys.argv[1] if len(sys.argv) > 1 else "current_state"
    capture_metrics(phase)