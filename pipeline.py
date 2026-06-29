import os
from urllib.parse import urlparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
from dotenv import load_dotenv

# Load credentials
load_dotenv()
db_url = os.getenv("DATABASE_URL")
parsed_url = urlparse(db_url)
jdbc_url = f"jdbc:postgresql://{parsed_url.hostname}:{parsed_url.port}{parsed_url.path}"
db_user = parsed_url.username
db_password = parsed_url.password

def run_pipeline():
    print("Initializing Spark Session...")
    
    spark = SparkSession.builder \
        .appName("Iceberg-ETL-Pipeline") \
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.postgresql:postgresql:42.7.3") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type", "hadoop") \
        .config("spark.sql.catalog.local.warehouse", f"{os.getcwd()}/warehouse") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db")

    print("\n1. Extracting and loading 'orders' (Format V2 + Merge-On-Read)...")
    df_orders = spark.read.format("jdbc").option("url", jdbc_url).option("dbtable", "orders") \
        .option("user", db_user).option("password", db_password).option("driver", "org.postgresql.Driver").load()
        
    df_orders = df_orders.withColumn("ingestion_timestamp", current_timestamp())

    df_orders.writeTo("local.db.orders") \
        .tableProperty("format-version", "2") \
        .tableProperty("write.merge.mode", "merge-on-read") \
        .tableProperty("write.update.mode", "merge-on-read") \
        .createOrReplace()

    print("2. Extracting and loading 'order_items' (Format V2 + Merge-On-Read)...")
    df_items = spark.read.format("jdbc").option("url", jdbc_url).option("dbtable", "order_items") \
        .option("user", db_user).option("password", db_password).option("driver", "org.postgresql.Driver").load()
        
    df_items = df_items.withColumn("ingestion_timestamp", current_timestamp())
    
    df_items.writeTo("local.db.order_items") \
        .tableProperty("format-version", "2") \
        .tableProperty("write.merge.mode", "merge-on-read") \
        .tableProperty("write.update.mode", "merge-on-read") \
        .createOrReplace()

    print("\n3. Simulating the Upsert disaster (50 MERGE INTO operations)...")
    df_orders.limit(5).createOrReplaceTempView("batch_orders")
    df_items.limit(10).createOrReplaceTempView("batch_items")

    for i in range(50):
        if i % 10 == 0:
            print(f"  Running MERGE batch {i}/50...")
            
        spark.sql("""
            MERGE INTO local.db.orders t
            USING batch_orders s
            ON t.order_id = s.order_id
            WHEN MATCHED THEN UPDATE SET t.status = 'Processed'
            WHEN NOT MATCHED THEN INSERT *
        """)
        
        spark.sql("""
            MERGE INTO local.db.order_items t
            USING batch_items s
            ON t.item_id = s.item_id
            WHEN MATCHED THEN UPDATE SET t.price = s.price + 0.01
            WHEN NOT MATCHED THEN INSERT *
        """)

    print("\n--- TABLE HEALTH METRICS (THE MESS) ---")
    print("Content Legend: 0 = Data Files, 1 = Position Deletes, 2 = Equality Deletes")
    
    print("\nOrders Table Files:")
    spark.sql("""
        SELECT content, count(*) as file_count 
        FROM local.db.orders.files 
        GROUP BY content
    """).show()
    
    print("Order Items Table Files:")
    spark.sql("""
        SELECT content, count(*) as file_count 
        FROM local.db.order_items.files 
        GROUP BY content
    """).show()

    print("Pipeline complete. The tables are successfully loaded and degraded.")
    spark.stop()

if __name__ == "__main__":
    run_pipeline()