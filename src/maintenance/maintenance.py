import os
from src.monitoring.history_logger import log_snapshot_with_session
from src.monitoring.spark_session import get_shared_spark

def run_maintenance():
    print("Initializing Spark for Lakehouse Maintenance...")
    
    root_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    warehouse_path = os.path.join(root_dir, "warehouse")

    spark = get_shared_spark()

    spark.sparkContext.setLogLevel("ERROR")

    print("\n--- INITIATING LAKEHOUSE MAINTENANCE ---")

    # 1. Compaction (Rewrite Data & Delete Files)
    print("\n1. Compacting 'orders' table (Crushing small files)...")
    spark.sql("CALL local.system.rewrite_data_files(table => 'local.db.orders')").show(vertical=True)
    spark.sql("CALL local.system.rewrite_position_delete_files(table => 'local.db.orders')").show(vertical=True)

    print("2. Compacting 'order_items' table...")
    spark.sql("CALL local.system.rewrite_data_files(table => 'local.db.order_items')").show(vertical=True)
    spark.sql("CALL local.system.rewrite_position_delete_files(table => 'local.db.order_items')").show(vertical=True)

    # 2. Cleanup (Expire old snapshots and delete physical garbage files)
    print("\n3. Expiring old snapshots for 'orders' (Retaining only the latest 1)...")
    spark.sql("""
        CALL local.system.expire_snapshots(
            table => 'local.db.orders', 
            older_than => TIMESTAMP '2030-01-01 00:00:00', 
            retain_last => 1
        )
    """).show(vertical=True)

    print("4. Expiring old snapshots for 'order_items' (Retaining only the latest 1)...")
    spark.sql("""
        CALL local.system.expire_snapshots(
            table => 'local.db.order_items', 
            older_than => TIMESTAMP '2030-01-01 00:00:00', 
            retain_last => 1
        )
    """).show(vertical=True)

    print("\nMaintenance procedures complete! The tables are healed.")

def execute_table_maintenance(table_name: str) -> str:
    """
    Agent-facing function to execute compaction and expiration on a specific table.
    Returns a success or error message directly to the caller.
    """
    root_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    warehouse_path = os.path.join(root_dir, "warehouse")

    spark = get_shared_spark()

    spark.sparkContext.setLogLevel("ERROR")

    try:
        # Force single-partition output so tiny compacted data lands in exactly 1 file
        spark.conf.set("spark.sql.shuffle.partitions", "1")
        
        # 1. Compaction (Data Files)
        spark.sql(f"CALL local.system.rewrite_data_files(table => 'local.db.{table_name}')")
        
        # 2. Resolving MOR Bloat (Delete Files) -> CRITICAL ADDITION
        spark.sql(f"CALL local.system.rewrite_position_delete_files(table => 'local.db.{table_name}')")
        
        # 3. Reorganize Metadata (Manifest Files)
        spark.sql(f"CALL local.system.rewrite_manifests(table => 'local.db.{table_name}')")
        
        # 4. Cleanup (Expire old snapshots)
        spark.sql(f"""
            CALL local.system.expire_snapshots(
                table => 'local.db.{table_name}', 
                older_than => TIMESTAMP '2030-01-01 00:00:00', 
                retain_last => 1
            )
        """)

        log_snapshot_with_session(spark, table_name, "post_maintenance")
        
        return f"SUCCESS: Maintenance executed for {table_name}. Small files crushed, position deletes resolved, manifests rewritten, and old snapshots expired."
        
    except Exception as e:
        return f"FAILED: Could not run maintenance on {table_name}. Error: {str(e)}"

if __name__ == "__main__":
    run_maintenance()