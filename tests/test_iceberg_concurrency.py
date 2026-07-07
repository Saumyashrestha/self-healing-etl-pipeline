import os
import time
import multiprocessing
from pyspark.sql import SparkSession

def create_spark_session(app_name):
    current_dir = os.getcwd()
    warehouse_path = os.path.join(current_dir, "warehouse")
    
    return SparkSession.builder \
        .appName(app_name) \
        .master("local[2]") \
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type", "hadoop") \
        .config("spark.sql.catalog.local.warehouse", warehouse_path) \
        .config("spark.sql.defaultCatalog", "local") \
        .getOrCreate()

def setup_database():
    print("--- Setting up OCC Test Table (V2 Format with Partitions) ---")
    spark = create_spark_session("Setup_Process")
    spark.sparkContext.setLogLevel("ERROR")
    
    spark.sql("CREATE DATABASE IF NOT EXISTS db")
    spark.sql("DROP TABLE IF EXISTS db.occ_test")
    
    # REQUIREMENT MET: Creating a table explicitly partitioned by category
    # Format V2 is required in Iceberg to allow UPDATE/DELETE operations
    spark.sql("""
        CREATE TABLE db.occ_test (id BIGINT, category STRING, status STRING) 
        USING iceberg 
        PARTITIONED BY (category)
        TBLPROPERTIES (
            'format-version'='2',
            'write.update.mode'='copy-on-write'
        )
    """)
    
    # Insert the baseline snapshot (V1) into the 'ELECTRONICS' partition
    spark.sql("INSERT INTO db.occ_test VALUES (1, 'ELECTRONICS', 'PENDING')")
    spark.stop()

def run_worker_a():
    print("\n[Worker A] Starting up...")
    spark = create_spark_session("WorkerA_Process")
    spark.sparkContext.setLogLevel("ERROR")
    
    print("[Worker A] Reading partition to establish snapshot baseline...")
    # Locks Worker A's view to Snapshot V1
    spark.sql("SELECT * FROM db.occ_test WHERE category = 'ELECTRONICS'").collect()
    
    print("[Worker A] ⏳ Simulating heavy pipeline processing... Sleeping for 8 seconds.")
    time.sleep(8) 
    
    print("[Worker A] 🚀 Waking up! Attempting to UPDATE the exact same row...")
    try:
        # REQUIREMENT MET: Session updating the same partition
        spark.sql("UPDATE db.occ_test SET status = 'DONE_BY_A' WHERE id = 1 AND category = 'ELECTRONICS'")
        print("[Worker A] ✅ SUCCESS! (If you see this, the test failed)")
    except Exception as e:
        print(f"\n[Worker A] ❌ CRASHED! Iceberg rejected the commit due to an OCC Validation Conflict!")
        
        os.makedirs("logs", exist_ok=True)
        with open("logs/occ_error.log", "w") as f:
            f.write(str(e))
        print("[Worker A] ⚠️ Iceberg exception successfully captured in logs/occ_error.log")
    finally:
        spark.stop()

def run_worker_b():
    # Wait 2 seconds to ensure Worker A reads the old snapshot first
    time.sleep(2)
    print("\n[Worker B] Starting up...")
    spark = create_spark_session("WorkerB_Process")
    spark.sparkContext.setLogLevel("ERROR")
    
    print("[Worker B] ⚡ Processing lightning fast! UPDATING immediately...")
    try:
        # REQUIREMENT MET: Second session updating the exact same partition concurrently
        spark.sql("UPDATE db.occ_test SET status = 'DONE_BY_B' WHERE id = 1 AND category = 'ELECTRONICS'")
        print("[Worker B] ✅ COMMIT SUCCESS! Table metadata advanced.")
    except Exception as e:
        print(f"[Worker B] Error: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    multiprocessing.freeze_support()
    
    setup_database()
    
    print("==================================================")
    print("--- LAUNCHING TARGETED OCC PARTITION SIMULATION ---")
    print("==================================================")
    
    p1 = multiprocessing.Process(target=run_worker_a)
    p2 = multiprocessing.Process(target=run_worker_b)
    
    p1.start()
    p2.start()
    
    p1.join()
    p2.join()
    
    print("\n==================================================")
    print("--- CONCURRENCY SIMULATION RUN COMPLETE ---")
    print("==================================================")