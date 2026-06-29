import os
from pyspark.sql import SparkSession

def check_metrics():
    print("Initializing Spark to read Iceberg metadata...")
    spark = SparkSession.builder \
        .appName("Iceberg-Metrics") \
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type", "hadoop") \
        .config("spark.sql.catalog.local.warehouse", f"{os.getcwd()}/warehouse") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    print("\n--- CAPSTONE RUBRIC METRICS: BEFORE OPTIMIZATION ---")
    
    # 1. Snapshot Count
    print("\n1. Snapshot Count (Orders Table):")
    spark.sql("SELECT count(*) as total_snapshots FROM local.db.orders.snapshots").show()

    # 2. Data & Delete File Count
    print("2. File Count (Orders Table):")
    spark.sql("""
        SELECT 
            CASE content 
                WHEN 0 THEN 'Data File' 
                WHEN 1 THEN 'Position Delete' 
            END as file_type, 
            count(*) as count 
        FROM local.db.orders.files 
        GROUP BY content
    """).show()

    # 3. Average File Size
    print("3. Average File Size (Orders Table):")
    spark.sql("""
        SELECT 
            CASE content 
                WHEN 0 THEN 'Data File' 
                WHEN 1 THEN 'Position Delete' 
            END as file_type,
            round(avg(file_size_in_bytes) / 1024, 2) as avg_size_kb 
        FROM local.db.orders.files 
        GROUP BY content
    """).show()

    spark.stop()

if __name__ == "__main__":
    check_metrics()