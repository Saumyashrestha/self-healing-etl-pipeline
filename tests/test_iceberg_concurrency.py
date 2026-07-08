import os
import time
import multiprocessing
from datetime import datetime
from pyspark.sql import SparkSession

# --- ADD TIMESTAMP TO PROVE REAL-TIME EXECUTION ---
def log(msg):
    timestamp = datetime.now().strftime('%H:%M:%S.%f')[:-3]
    print(f"[LOG] {timestamp} {msg}")

def create_spark_session(app_name):
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type", "hadoop") \
        .config("spark.sql.catalog.local.warehouse", "warehouse") \
        .config("spark.sql.defaultCatalog", "local") \
        .getOrCreate()

def setup_database():
    spark = create_spark_session("Setup_Process")
    spark.sparkContext.setLogLevel("ERROR")
    
    log("--- Setting up OCC Test Table (V2 Format with Partitions) ---")
    spark.sql("CREATE DATABASE IF NOT EXISTS db")
    spark.sql("DROP TABLE IF EXISTS db.occ_test")
    
    spark.sql("""
        CREATE TABLE db.occ_test (
            id INT,
            category STRING,
            status STRING
        )
        USING iceberg
        PARTITIONED BY (category)
    """)
    
    log("--- Generating 10 baseline rows (Mixed Categories) ---")
    # Mixing categories to show table diversity
    spark.sql("""
        INSERT INTO db.occ_test VALUES 
        (1, 'ELECTRONICS', 'PENDING'),
        (2, 'ELECTRONICS', 'PENDING'),
        (3, 'FURNITURE', 'PENDING'),
        (4, 'FURNITURE', 'PENDING'),
        (5, 'ELECTRONICS', 'PENDING')
    """)
    spark.stop()

def show_table_state(phase_name):
    spark = create_spark_session(f"Verify_{phase_name}")
    spark.sparkContext.setLogLevel("ERROR")
    
    # Add the [DATA] prefix to everything that belongs in the right pane
    print(f"[DATA] === {phase_name} ===")
    
    table_data = spark.sql("SELECT * FROM db.occ_test ORDER BY id LIMIT 5").collect()
    print("[DATA] | ID | CAT | STATUS |") 
    print("[DATA] |---|---|---|")
    for row in table_data:
        print(f"[DATA] | {row.id} | {row.category} | {row.status} |")
    
    spark.stop()

def run_worker_a():
    log("\n[Worker A] Starting up...")
    spark = create_spark_session("WorkerA_Process")
    spark.sparkContext.setLogLevel("ERROR")
    
    log("[Worker A] Reading partition to establish snapshot baseline...")
    spark.sql("SELECT * FROM db.occ_test WHERE category = 'ELECTRONICS'").collect()
    
    log("[Worker A] Simulating heavy pipeline processing... Sleeping for 8 seconds.")
    time.sleep(8) 
    
    log("[Worker A] Waking up! Attempting to UPDATE the exact same partition...")
    try:
        spark.sql("UPDATE db.occ_test SET status = 'DONE_BY_A' WHERE id = 1 AND category = 'ELECTRONICS'")
        log("[Worker A] SUCCESS! (If you see this, the test failed)")
    except Exception as e:
        log(f"\n[Worker A] CRASHED! Iceberg rejected the commit due to an OCC Validation Conflict!")
        os.makedirs("logs", exist_ok=True)
        with open("logs/occ_error.log", "w") as f:
            f.write(str(e))
    finally:
        spark.stop()

def run_worker_b():
    time.sleep(2)
    log("\n[Worker B] Starting up...")
    spark = create_spark_session("WorkerB_Process")
    spark.sparkContext.setLogLevel("ERROR")
    
    log("[Worker B] Processing lightning fast! UPDATING immediately...")
    try:
        spark.sql("UPDATE db.occ_test SET status = 'DONE_BY_B' WHERE id = 1 AND category = 'ELECTRONICS'")
        log("[Worker B] COMMIT SUCCESS! Table metadata advanced.")
    except Exception as e:
        log(f"[Worker B] Error: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    multiprocessing.freeze_support()
    
    setup_database()
    show_table_state("SNAPSHOT V1 (BASELINE)")
    
    log("==================================================")
    log("--- LAUNCHING TARGETED OCC PARTITION SIMULATION ---")
    log("==================================================")
    
    p1 = multiprocessing.Process(target=run_worker_a)
    p2 = multiprocessing.Process(target=run_worker_b)
    p1.start()
    p2.start()
    p1.join()
    p2.join()
    
    log("==================================================")
    log("--- CONCURRENCY SIMULATION RUN COMPLETE ---")
    log("==================================================")
    
    show_table_state("SNAPSHOT V2 (AFTERMATH)")