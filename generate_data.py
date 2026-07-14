import os
import sys
import random
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import execute_values
from faker import Faker
from dotenv import load_dotenv
from src.utils.catalog import pick_customer_id, pick_product_id, get_product_price, generate_status

# Load the database credentials
load_dotenv()
db_url = os.getenv("DATABASE_URL")
fake = Faker()

def generate_order_logic(order_id, is_recent=False):
    """
    Core business logic for generating a realistic order.
    If is_recent is True, it forces the order to be within the last 3 days.
    """
    customer_id = pick_customer_id()          # CHANGED: was random.randint(100, 999)

    # 1. Date Generation (unchanged)
    if is_recent:
        days_ago = random.randint(0, 3)
    else:
        days_ago = random.randint(0, 365)

    order_date = datetime.now() - timedelta(days=days_ago, hours=random.randint(0, 23))

    # 2. Status Generation
    status = generate_status(days_ago)          

    # 3. Item Generation
    num_items = random.randint(1, 5)
    order_total = 0
    items = []

    for _ in range(num_items):
        product_id = pick_product_id()           
        price = get_product_price(product_id)     
        order_total += price
        items.append((order_id, product_id, price))

    order = (order_id, customer_id, order_date, round(order_total, 2), status)
    return order, items

def seed_database(num_orders=10000):
    """Wipes the database and loads a massive historical baseline."""
    print(f"Connecting to Postgres to SEED {num_orders} rows...")
    conn = psycopg2.connect(db_url)
    cur = conn.cursor()

    print("Dropping and recreating tables...")
    cur.execute("""
        DROP TABLE IF EXISTS order_items CASCADE;
        DROP TABLE IF EXISTS orders CASCADE;

        CREATE TABLE orders (
            order_id INT PRIMARY KEY,
            customer_id INT,
            order_date TIMESTAMP,
            amount DECIMAL(10, 2),
            status VARCHAR(20)
        );

        CREATE TABLE order_items (
            item_id SERIAL PRIMARY KEY,
            order_id INT REFERENCES orders(order_id),
            product_id INT,
            price DECIMAL(10, 2)
        );
    """)

    orders_data, order_items_data = [], []
    for order_id in range(1, num_orders + 1):
        order, items = generate_order_logic(order_id, is_recent=False)
        orders_data.append(order)
        order_items_data.extend(items)

    print("Inserting data...")
    execute_values(cur, "INSERT INTO orders (order_id, customer_id, order_date, amount, status) VALUES %s", orders_data)
    execute_values(cur, "INSERT INTO order_items (order_id, product_id, price) VALUES %s", order_items_data)

    conn.commit()
    cur.close()
    conn.close()
    print("Seed Complete! Tables populated.")


def incremental_load(num_new_orders=500):
    """Appends new orders and updates existing ones to simulate daily activity."""
    print(f"Connecting to Postgres for INCREMENTAL LOAD of {num_new_orders} rows...")
    conn = psycopg2.connect(db_url)
    cur = conn.cursor()

    # Get the highest existing order_id to know where to start inserting
    cur.execute("SELECT COALESCE(MAX(order_id), 0) FROM orders;")
    max_id = cur.fetchone()[0]

    orders_data, order_items_data = [], []
    for order_id in range(max_id + 1, max_id + num_new_orders + 1):
        order, items = generate_order_logic(order_id, is_recent=True)
        orders_data.append(order)
        order_items_data.extend(items)

    print("Inserting new incremental data...")
    execute_values(cur, "INSERT INTO orders (order_id, customer_id, order_date, amount, status) VALUES %s", orders_data)
    execute_values(cur, "INSERT INTO order_items (order_id, product_id, price) VALUES %s", order_items_data)

    print("Updating old 'Pending' orders to 'Shipped' to simulate business progress...")
    cur.execute("""
        UPDATE orders 
        SET status = 'Shipped' 
        WHERE status = 'Pending' AND order_date < NOW() - INTERVAL '1 day';
    """)

    conn.commit()
    cur.close()
    conn.close()
    print(f"Incremental Load Complete! Added {num_new_orders} new orders and updated older statuses.")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python data_generate.py [seed|incremental]")
        sys.exit(1)

    mode = sys.argv[1].lower()
    
    if mode == "seed":
        # Change this number to scale your initial data
        seed_database(num_orders=10000) 
    elif mode == "incremental":
        # Change this number to control the size of your daily batches
        incremental_load(num_new_orders=500) 
    else:
        print("Invalid argument. Use 'seed' or 'incremental'.")








# import os
# import random
# from datetime import datetime, timedelta
# import psycopg2
# from psycopg2.extras import execute_values
# from faker import Faker
# from dotenv import load_dotenv

# # Load the database credentials
# load_dotenv()
# db_url = os.getenv("DATABASE_URL")

# fake = Faker()

# def generate_orders_and_items():
#     print("Connecting to Postgres...")
#     conn = psycopg2.connect(db_url)
#     cur = conn.cursor()

#     print("Creating the orders and order_items tables...")
#     # CASCADE ensures that if the orders table is dropped, the linked items are too
#     cur.execute("""
#         DROP TABLE IF EXISTS order_items CASCADE;
#         DROP TABLE IF EXISTS orders CASCADE;

#         CREATE TABLE orders (
#             order_id INT PRIMARY KEY,
#             customer_id INT,
#             order_date TIMESTAMP,
#             amount DECIMAL(10, 2),
#             status VARCHAR(20)
#         );

#         CREATE TABLE order_items (
#             item_id SERIAL PRIMARY KEY,
#             order_id INT REFERENCES orders(order_id),
#             product_id INT,
#             price DECIMAL(10, 2)
#         );
#     """)

#     print("Generating fake data...")
#     orders_data = []
#     order_items_data = []

#     # Generate exactly 1000 orders
#     for order_id in range(1, 1001): 
#         customer_id = random.randint(100, 999)
#         order_date = datetime.now() - timedelta(days=random.randint(0, 365), hours=random.randint(0,24))
#         status = random.choice(['Completed', 'Pending', 'Shipped'])
        
#         # Generate between 1 and 5 individual items for each order
#         num_items = random.randint(1, 5)
#         order_total = 0
        
#         for _ in range(num_items):
#             product_id = random.randint(1, 50)
#             price = round(random.uniform(10.0, 150.0), 2)
#             order_total += price
            
#             # Append the item data, linking it back to the current order_id
#             order_items_data.append((order_id, product_id, price))
            
#         # Append the parent order data, using the sum of the items as the total amount
#         orders_data.append((order_id, customer_id, order_date, round(order_total, 2), status))

#     print(f"Inserting {len(orders_data)} rows into 'orders'...")
#     execute_values(cur, 
#         "INSERT INTO orders (order_id, customer_id, order_date, amount, status) VALUES %s", 
#         orders_data
#     )

#     print(f"Inserting {len(order_items_data)} rows into 'order_items'...")
#     execute_values(cur, 
#         "INSERT INTO order_items (order_id, product_id, price) VALUES %s", 
#         order_items_data
#     )

#     # Commit the transaction and close the connection
#     conn.commit()
#     cur.close()
#     conn.close()
#     print("Success! Both tables are populated and relationally linked.")

# if __name__ == "__main__":
#     generate_orders_and_items()