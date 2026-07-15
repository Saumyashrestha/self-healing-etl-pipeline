import os
import random
from urllib.parse import urlparse
import psycopg2
from pyspark.sql import SparkSession
from dotenv import load_dotenv
from src.monitoring.history_logger import log_snapshot_with_session
from src.monitoring.spark_session import get_shared_spark
from src.utils.catalog import pick_product_id, get_product_price

# Load credentials
load_dotenv()
db_url = os.getenv("DATABASE_URL")
parsed_url = urlparse(db_url)
jdbc_url = f"jdbc:postgresql://{parsed_url.hostname}:{parsed_url.port}{parsed_url.path}"
db_user = parsed_url.username
db_password = parsed_url.password


def get_pg_connection():
    return psycopg2.connect(
        host=parsed_url.hostname,
        port=parsed_url.port,
        dbname=parsed_url.path.lstrip("/"),
        user=parsed_url.username,
        password=parsed_url.password
    )


def get_watermark(pg_conn, source_name):
    cur = pg_conn.cursor()
    cur.execute("SELECT last_loaded_at FROM pipeline_watermark WHERE source_name = %s", (source_name,))
    row = cur.fetchone()
    cur.close()
    return row[0] if row else None


def initialize_watermark(pg_conn, source_name):
    """Sets the watermark to the current Postgres server time. Used once, on first run."""
    cur = pg_conn.cursor()
    cur.execute("SELECT NOW();")
    now = cur.fetchone()[0]
    cur.execute("""
        INSERT INTO pipeline_watermark (source_name, last_loaded_at, updated_at)
        VALUES (%s, %s, NOW())
        ON CONFLICT (source_name) DO UPDATE SET last_loaded_at = EXCLUDED.last_loaded_at, updated_at = NOW()
    """, (source_name, now))
    pg_conn.commit()
    cur.close()
    return now


def update_watermark(pg_conn, source_name, new_timestamp):
    cur = pg_conn.cursor()
    cur.execute("""
        INSERT INTO pipeline_watermark (source_name, last_loaded_at, updated_at)
        VALUES (%s, %s, NOW())
        ON CONFLICT (source_name) DO UPDATE SET last_loaded_at = EXCLUDED.last_loaded_at, updated_at = NOW()
    """, (source_name, new_timestamp))
    pg_conn.commit()
    cur.close()


def mutate_postgres(pg_conn, insert_count, update_count, customer_ids, product_ids, id_counters):
    """Directly mutates the Postgres OLTP source: advances some existing orders,
    inserts brand-new orders + their order_items. This is the real 'upstream activity'
    that the incremental load will later pick up via the watermark."""
    cur = pg_conn.cursor()

    # --- Updates: advance some existing Pending orders to Shipped ---
    if update_count > 0:
        cur.execute("""
    SELECT order_id FROM orders 
    WHERE status = 'Pending' AND order_date < NOW() - INTERVAL '1 day'
    ORDER BY random() LIMIT %s
""", (update_count,))
        ids_to_update = [r[0] for r in cur.fetchall()]
        for oid in ids_to_update:
            cur.execute("""
                UPDATE orders SET status = 'Shipped', updated_at = NOW() WHERE order_id = %s
            """, (oid,))

    # --- Inserts: brand-new orders, each with 1-3 new order_items ---
    for _ in range(insert_count):
        id_counters["order_id"] += 1
        new_order_id = id_counters["order_id"]
        customer_id = random.choice(customer_ids)   # unchanged - see note below
        # amount is now the SUM of real item prices, computed after the items loop
        order_items_for_this_order = []

        num_items = random.randint(1, 3)
        for _ in range(num_items):
            id_counters["item_id"] += 1
            new_item_id = id_counters["item_id"]
            product_id = pick_product_id()              # CHANGED: was random.choice(product_ids)
            price = get_product_price(product_id)        # CHANGED: was random.uniform(5.0, 200.0)
            order_items_for_this_order.append((new_item_id, product_id, price))

        amount = round(sum(p for _, _, p in order_items_for_this_order), 2)  # CHANGED

        cur.execute("""
            INSERT INTO orders (order_id, customer_id, order_date, amount, status, updated_at)
            VALUES (%s, %s, CURRENT_DATE, %s, 'Pending', NOW())
        """, (new_order_id, customer_id, amount))

        for new_item_id, product_id, price in order_items_for_this_order:
            cur.execute("""
                INSERT INTO order_items (item_id, order_id, product_id, price, created_at)
                VALUES (%s, %s, %s, %s, NOW())
            """, (new_item_id, new_order_id, product_id, price))

    pg_conn.commit()
    cur.close()


def run_pipeline(log_queue=None):
    def emit_log(msg):
        if log_queue:
            log_queue.put(msg + "\n")
        else:
            print(msg)

    pg_conn = None
    try:
        emit_log("Initializing Spark Session...")

        spark = get_shared_spark()

        spark.sparkContext.setLogLevel("ERROR")
        spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db")

        pg_conn = get_pg_connection()

        # --- GUARD: only do the full initial load if tables don't exist yet ---
        orders_exists = spark.catalog.tableExists("local.db.orders")
        items_exists = spark.catalog.tableExists("local.db.order_items")

        if not orders_exists:
            emit_log("1. Extracting and loading 'orders' (Format V2 + Merge-On-Read) — first run, full load...")
            df_orders = spark.read.format("jdbc").option("url", jdbc_url).option("dbtable", "orders") \
                .option("user", db_user).option("password", db_password).option("driver", "org.postgresql.Driver").load()

            df_orders.writeTo("local.db.orders") \
                .tableProperty("format-version", "2") \
                .tableProperty("write.merge.mode", "merge-on-read") \
                .tableProperty("write.update.mode", "merge-on-read") \
                .createOrReplace()
        else:
            emit_log("1. 'orders' table already exists — skipping full reload, will merge incrementally.")

        if not items_exists:
            emit_log("2. Extracting and loading 'order_items' — first run, full load...")
            df_items = spark.read.format("jdbc").option("url", jdbc_url).option("dbtable", "order_items") \
                .option("user", db_user).option("password", db_password).option("driver", "org.postgresql.Driver").load()

            df_items.writeTo("local.db.order_items") \
                .tableProperty("format-version", "2") \
                .tableProperty("write.merge.mode", "merge-on-read") \
                .tableProperty("write.update.mode", "merge-on-read") \
                .createOrReplace()
        else:
            emit_log("2. 'order_items' table already exists — skipping full reload, will merge incrementally.")

        log_snapshot_with_session(spark, "orders", "baseline" if not orders_exists else "run_start")
        log_snapshot_with_session(spark, "order_items", "baseline" if not items_exists else "run_start")

        # --- Initialize watermarks if this is the first run ---
        wm_orders = get_watermark(pg_conn, "orders")
        if wm_orders is None:
            wm_orders = initialize_watermark(pg_conn, "orders")
            emit_log(f"Initialized 'orders' watermark to {wm_orders}")

        wm_items = get_watermark(pg_conn, "order_items")
        if wm_items is None:
            wm_items = initialize_watermark(pg_conn, "order_items")
            emit_log(f"Initialized 'order_items' watermark to {wm_items}")

        # --- Pull reference data needed to generate realistic new rows ---
        cur = pg_conn.cursor()
        cur.execute("SELECT DISTINCT customer_id FROM orders")
        customer_ids = [r[0] for r in cur.fetchall()]
        cur.execute("SELECT DISTINCT product_id FROM order_items")
        product_ids = [r[0] for r in cur.fetchall()]
        cur.execute("SELECT COALESCE(MAX(order_id), 0) FROM orders")
        max_order_id = cur.fetchone()[0]
        cur.execute("SELECT COALESCE(MAX(item_id), 0) FROM order_items")
        max_item_id = cur.fetchone()[0]
        cur.close()

        id_counters = {"order_id": max_order_id, "item_id": max_item_id}

        emit_log("3. Simulating a Realistic UPSERT Workload from Postgres (50 Micro-batches)...")

        for i in range(50):
            batch_type = random.choice(["insert_only", "update_only", "both", "both", "both"])
            insert_count = random.randint(1, 8) if batch_type in ("insert_only", "both") else 0
            update_count = random.randint(1, 6) if batch_type in ("update_only", "both") else 0

            emit_log(f"   > Running batch {i + 1}/50... [{batch_type}] inserts={insert_count} updates={update_count}")

            # --- STEP A: Mutate the real Postgres OLTP source ---
            mutate_postgres(pg_conn, insert_count, update_count, customer_ids, product_ids, id_counters)

            # --- STEP B: Pull only what changed since the last watermark ---
            orders_query = f"(SELECT * FROM orders WHERE updated_at > '{wm_orders}') AS inc_orders"
            items_query = f"(SELECT * FROM order_items WHERE created_at > '{wm_items}') AS inc_items"

            df_orders_inc = spark.read.format("jdbc").option("url", jdbc_url).option("dbtable", orders_query) \
                .option("user", db_user).option("password", db_password).option("driver", "org.postgresql.Driver").load()
            df_items_inc = spark.read.format("jdbc").option("url", jdbc_url).option("dbtable", items_query) \
                .option("user", db_user).option("password", db_password).option("driver", "org.postgresql.Driver").load()

            orders_count = df_orders_inc.count()
            items_count = df_items_inc.count()
            emit_log(f"      [Audit] Orders changed: {orders_count} | Items changed: {items_count}")

            # --- STEP C: Merge into Iceberg ---
            if orders_count > 0:
                df_orders_inc.createOrReplaceTempView("batch_orders")
                spark.sql("""
                    MERGE INTO local.db.orders t
                    USING batch_orders s
                    ON t.order_id = s.order_id
                    WHEN MATCHED THEN UPDATE SET t.status = s.status, t.updated_at = s.updated_at
                    WHEN NOT MATCHED THEN INSERT *
                """)
                max_updated = df_orders_inc.agg({"updated_at": "max"}).collect()[0][0]
                wm_orders = max_updated
                update_watermark(pg_conn, "orders", wm_orders)

            if items_count > 0:
                df_items_inc.createOrReplaceTempView("batch_items")
                spark.sql("""
                    MERGE INTO local.db.order_items t
                    USING batch_items s
                    ON t.item_id = s.item_id
                    WHEN NOT MATCHED THEN INSERT *
                """)
                max_created = df_items_inc.agg({"created_at": "max"}).collect()[0][0]
                wm_items = max_created
                update_watermark(pg_conn, "order_items", wm_items)

            log_snapshot_with_session(spark, "orders", f"batch_{i+1}")
            log_snapshot_with_session(spark, "order_items", f"batch_{i+1}")

        emit_log("Pipeline complete. The tables are successfully loaded and degraded.")

    # except Exception as e:
    #     emit_log(f"CRITICAL ERROR: {str(e)}")

    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        emit_log(f"CRITICAL ERROR: {type(e).__name__}")
        print(tb[-3000:])  # print last few thousand chars of the traceback to terminal
    
    finally:
        if pg_conn:
            pg_conn.close()
        if log_queue:
            log_queue.put(None)

if __name__ == "__main__":
    run_pipeline()