import os
import random
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

def run_pipeline(log_queue=None):
    # --- Helper function to route logs to the web stream OR the terminal ---
    def emit_log(msg):
        if log_queue:
            log_queue.put(msg + "\n")
        else:
            print(msg)

    try:
        emit_log("Initializing Spark Session...")
        
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

        emit_log("1. Extracting and loading 'orders' (Format V2 + Merge-On-Read)...")
        df_orders = spark.read.format("jdbc").option("url", jdbc_url).option("dbtable", "orders") \
            .option("user", db_user).option("password", db_password).option("driver", "org.postgresql.Driver").load()
            
        df_orders = df_orders.withColumn("ingestion_timestamp", current_timestamp())

        df_orders.writeTo("local.db.orders") \
            .tableProperty("format-version", "2") \
            .tableProperty("write.merge.mode", "merge-on-read") \
            .tableProperty("write.update.mode", "merge-on-read") \
            .createOrReplace()

        emit_log("2. Extracting and loading 'order_items'...")
        df_items = spark.read.format("jdbc").option("url", jdbc_url).option("dbtable", "order_items") \
            .option("user", db_user).option("password", db_password).option("driver", "org.postgresql.Driver").load()
            
        df_items = df_items.withColumn("ingestion_timestamp", current_timestamp())
        
        df_items.writeTo("local.db.order_items") \
            .tableProperty("format-version", "2") \
            .tableProperty("write.merge.mode", "merge-on-read") \
            .tableProperty("write.update.mode", "merge-on-read") \
            .createOrReplace()

        emit_log("3. Simulating a Realistic UPSERT Workload (50 Micro-batches)...")

        for i in range(50):
            emit_log(f"   > Running MERGE batch {i + 1}/50...")
            
            # Generate a massive random offset so our "new" insert IDs never collide
            offset = random.randint(100000, 900000) + (i * 1000)

            # --- DYNAMIC BATCH: ORDERS ---
            spark.sql(f"""
                CREATE OR REPLACE TEMP VIEW batch_orders AS
                
                -- THE UPDATES (2 Rows): Keep original order_date, only change status and ingestion time
                SELECT 
                    order_id, 
                    customer_id, 
                    order_date,       -- Kept original
                    amount, 
                    'Shipped' as status, 
                    current_timestamp() as ingestion_timestamp 
                FROM (
                    SELECT * FROM local.db.orders WHERE status = 'Pending' ORDER BY rand() LIMIT 2
                )
                
                UNION ALL
                
                -- THE INSERTS (3 Rows): Offset ID, brand new order_date, Pending status
                SELECT 
                    order_id + {offset} as order_id, 
                    customer_id, 
                    current_date() as order_date, -- BRAND NEW DATE
                    amount, 
                    'Pending' as status, 
                    current_timestamp() as ingestion_timestamp 
                FROM (
                    SELECT * FROM local.db.orders LIMIT 3
                )
            """)
            
            # --- DYNAMIC BATCH: ORDER ITEMS ---
            spark.sql(f"""
                CREATE OR REPLACE TEMP VIEW batch_items AS
                
                -- THE UPDATES (2 Rows): Only updating the ingestion metadata
                SELECT 
                    item_id, 
                    order_id, 
                    product_id, 
                    price, 
                    current_timestamp() as ingestion_timestamp 
                FROM (
                    SELECT * FROM local.db.order_items ORDER BY rand() LIMIT 2
                )
                
                UNION ALL
                
                -- THE INSERTS (3 Rows): Offset both IDs to link to the new parent orders
                SELECT 
                    item_id + {offset} as item_id, 
                    order_id + {offset} as order_id, 
                    product_id, 
                    price, 
                    current_timestamp() as ingestion_timestamp 
                FROM (
                    SELECT * FROM local.db.order_items LIMIT 3
                )
            """)

            orders_inserts = spark.sql("SELECT count(*) as c FROM batch_orders WHERE status = 'Pending'").collect()[0]['c']
            orders_updates = spark.sql("SELECT count(*) as c FROM batch_orders WHERE status = 'Shipped'").collect()[0]['c']
            items_inserts = spark.sql(f"SELECT count(*) as c FROM batch_items WHERE item_id >= {offset}").collect()[0]['c']
            items_updates = spark.sql(f"SELECT count(*) as c FROM batch_items WHERE item_id < {offset}").collect()[0]['c']

            emit_log(f"      [Audit] Orders -> Inserts: {orders_inserts} | Updates: {orders_updates}")
            emit_log(f"      [Audit] Items  -> Inserts: {items_inserts} | Updates: {items_updates}")

            # Execute MERGE for Orders
            spark.sql("""
                MERGE INTO local.db.orders t
                USING batch_orders s
                ON t.order_id = s.order_id
                WHEN MATCHED THEN UPDATE SET t.status = s.status, t.ingestion_timestamp = s.ingestion_timestamp
                WHEN NOT MATCHED THEN INSERT *
            """)
            
            # Execute MERGE for Order Items
            spark.sql("""
                MERGE INTO local.db.order_items t
                USING batch_items s
                ON t.item_id = s.item_id
                WHEN MATCHED THEN UPDATE SET t.ingestion_timestamp = s.ingestion_timestamp
                WHEN NOT MATCHED THEN INSERT *
            """)

        emit_log("Pipeline complete. The tables are successfully loaded and degraded.")
        
    except Exception as e:
        emit_log(f"CRITICAL ERROR: {str(e)}")
    finally:
        if 'spark' in locals():
            spark.stop()
        if log_queue:
            log_queue.put(None)

if __name__ == "__main__":
    run_pipeline()