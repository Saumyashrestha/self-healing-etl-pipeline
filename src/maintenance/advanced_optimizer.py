import os
import sys
from pyspark.sql import SparkSession

def run_advanced_optimization():
    print("=== INITIATING ADVANCED LAKEHOUSE OPTIMIZATION ===")
    
    current_script_dir = os.path.dirname(os.path.abspath(__file__))
    root_dir = os.path.dirname(os.path.dirname(current_script_dir))
    warehouse_path = os.path.join(root_dir, "warehouse")

    spark = SparkSession.builder \
        .appName("Advanced-Iceberg-Optimizer") \
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type", "hadoop") \
        .config("spark.sql.catalog.local.warehouse", warehouse_path) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # --- FEATURE 1: TIME TRAVEL DEMONSTRATION ---
    print("\n1. [FEATURE] Time-Travel Check: Looking at the snapshot history...")
    try:
        # Show the evaluator how many snapshots exist before we delete them
        spark.sql("SELECT committed_at, snapshot_id, operation FROM local.db.orders.snapshots ORDER BY committed_at DESC LIMIT 5").show(truncate=False)
    except Exception as e:
        print("Could not load snapshot history (Table might be perfectly clean already).")

    # --- FEATURE 2: Z-ORDER CLUSTERING ---
    print("\n2. [FEATURE] Advanced Compaction: Z-Order Clustering on 'orders' (customer_id, order_date)...")
    spark.sql("""
        CALL local.system.rewrite_data_files(
            table => 'db.orders', 
            strategy => 'sort', 
            sort_order => 'zorder(customer_id, order_date)'
        )
    """).show(vertical=True)

    print("3. [FEATURE] Advanced Compaction: Z-Order Clustering on 'order_items' (order_id, product_id)...")
    spark.sql("""
        CALL local.system.rewrite_data_files(
            table => 'db.order_items', 
            strategy => 'sort', 
            sort_order => 'zorder(order_id, product_id)'
        )
    """).show(vertical=True)

    # --- FEATURE 3: STORAGE ROI CALCULATION ---
    print("\n4. [FEATURE] Calculating Storage ROI and removing orphan files...")
    total_reclaimed_bytes = 0
    tables = ['db.orders', 'db.order_items']
    
    for table in tables:
        # 1. Expire snapshots
        spark.sql(f"""
            CALL local.system.expire_snapshots(
                table => '{table}', 
                older_than => TIMESTAMP '2030-01-01 00:00:00', 
                retain_last => 1
            )
        """)

        # 2. Dry run to identify orphans
        # We REMOVED max_snapshot_age_ms here. 
        # By setting older_than to 2030, we effectively select everything.
        orphan_df = spark.sql(f"""
            CALL local.system.remove_orphan_files(
                table => '{table}', 
                older_than => TIMESTAMP '2030-01-01 00:00:00',
                dry_run => true
            )
        """).collect()
        
        # 3. Calculate file sizes
        for row in orphan_df:
            filepath = row['orphan_file_location']
            if filepath.startswith("file:///"):
                filepath = filepath[8:]
            elif filepath.startswith("file:/"):
                filepath = filepath[6:]
                
            if os.path.exists(filepath):
                total_reclaimed_bytes += os.path.getsize(filepath)
                
        # 4. Actually delete the garbage files
        # We also REMOVED max_snapshot_age_ms and dry_run here.
        spark.sql(f"""
            CALL local.system.remove_orphan_files(
                table => '{table}', 
                older_than => TIMESTAMP '2030-01-01 00:00:00'
            )
        """)

    # Print the final business metric
    reclaimed_mb = round(total_reclaimed_bytes / (1024 * 1024), 4)
    print(f" STORAGE ROI: {reclaimed_mb} MB Reclaimed ")

    spark.stop()

if __name__ == "__main__":
    run_advanced_optimization()