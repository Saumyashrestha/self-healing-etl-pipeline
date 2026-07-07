import os
import sys
from pyspark.sql import SparkSession

def capture_metrics(phase_label):
    """
    Connects to Iceberg metadata tables, extracts health metrics for ALL tables,
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

    # --- FIX: Loop through both tables to capture history for both ---
    tables_to_track = ["orders", "order_items"]
    
    for table_name in tables_to_track:
        # 1. Fetch Snapshot Count
        snapshot_df = spark.sql(f"SELECT count(*) as total_snapshots FROM local.db.{table_name}.snapshots")
        total_snapshots = snapshot_df.collect()[0]['total_snapshots']

        # Fetch Manifest Count
        manifest_df = spark.sql(f"SELECT count(*) as total_manifests FROM local.db.{table_name}.manifests")
        total_manifests = manifest_df.collect()[0]['total_manifests']

        # 2. Fetch File Counts and Sizes
        files_df = spark.sql(f"""
            SELECT 
                content,
                count(*) as file_count,
                round(avg(file_size_in_bytes) / 1024, 2) as avg_size_kb 
            FROM local.db.{table_name}.files 
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

        # 3. Format the report block dynamically for each table
        report_content = f"""
============================================================
PHASE: {phase_label.upper()} | TABLE: {table_name.upper()}
============================================================
1. Table Snapshots:          {total_snapshots}
2. Manifest Files:           {total_manifests}  
3. Active Data Files:        {data_files} (Avg Size: {data_file_avg_kb} KB)
4. Active Position Deletes:  {delete_files} (Avg Size: {delete_file_avg_kb} KB)
"""

        # Print to console for immediate visibility
        print(report_content)

        # Append to the permanent text report file
        with open(report_path, "a") as f:
            f.write(report_content)
            
        print(f"Successfully appended {table_name} metrics to: {report_path}\n")

    # Verification: Check Snapshot Count
    print("\n--- AUDIT: VERIFYING SNAPSHOT EXPIRATION ---")
    
    for table in tables_to_track:
        count = spark.sql(f"SELECT count(*) as total FROM local.db.{table}.snapshots").collect()[0]['total']
        print(f"Table: db.{table} | Remaining Snapshots: {count}")
        if count == 1:
            print(f"  Success: Snapshots for db.{table} expired correctly.")
        else:
            print(f"  Warning: Expected 1, found {count}.")
    
    print("\n--- AUDIT: IDENTIFYING ORPHANED FILES ---")
    # We will just check orders here for a quick sanity check to save execution time
    spark.sql("CALL local.system.remove_orphan_files(table => 'db.orders', dry_run => true)").show(vertical=True)

    spark.stop()

    # Get or create a Spark session 
    # (We don't stop it at the end so the API can reuse it on multiple calls)
    spark = SparkSession.builder \
        .appName("Iceberg-Agent-Tool") \
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type", "hadoop") \
        .config("spark.sql.catalog.local.warehouse", warehouse_path) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    try:
        # Fetch Snapshot Count for the specific table
        snapshot_df = spark.sql(f"SELECT count(*) as total_snapshots FROM local.db.{table_name}.snapshots")
        total_snapshots = snapshot_df.collect()[0]['total_snapshots']

        # Fetch File Counts and Sizes for the specific table
        files_df = spark.sql(f"""
            SELECT 
                content,
                count(*) as file_count,
                round(avg(file_size_in_bytes) / 1024, 2) as avg_size_kb 
            FROM local.db.{table_name}.files 
            GROUP BY content
        """).collect()

        # 4. Parse the file metrics
        data_files = 0
        data_file_avg_kb = 0.0

        for row in files_df:
            if row['content'] == 0:  # Data Files
                data_files = row['file_count']
                data_file_avg_kb = row['avg_size_kb']

        return f"Metrics for {table_name} - Snapshots: {total_snapshots} | Active Data Files: {data_files} (Avg Size: {data_file_avg_kb} KB)"
        
    except Exception as e:
        return f"Error retrieving metrics for {table_name}. Ensure the table exists. Error: {str(e)}"
    
if __name__ == "__main__":
    # Allow passing the phase as an argument (e.g., python metrics.py before)
    # Default to "current_state" if no argument is passed
    phase = sys.argv[1] if len(sys.argv) > 1 else "current_state"
    capture_metrics(phase)