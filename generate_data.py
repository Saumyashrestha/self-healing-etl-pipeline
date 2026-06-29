import os
import random
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import execute_values
from faker import Faker
from dotenv import load_dotenv

# Load the database credentials
load_dotenv()
db_url = os.getenv("DATABASE_URL")

fake = Faker()

def generate_orders_and_items():
    print("Connecting to Postgres...")
    conn = psycopg2.connect(db_url)
    cur = conn.cursor()

    print("Creating the orders and order_items tables...")
    # CASCADE ensures that if the orders table is dropped, the linked items are too
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

    print("Generating fake data...")
    orders_data = []
    order_items_data = []

    # Generate exactly 1000 orders
    for order_id in range(1, 1001): 
        customer_id = random.randint(100, 999)
        order_date = datetime.now() - timedelta(days=random.randint(0, 365), hours=random.randint(0,24))
        status = random.choice(['Completed', 'Pending', 'Shipped'])
        
        # Generate between 1 and 5 individual items for each order
        num_items = random.randint(1, 5)
        order_total = 0
        
        for _ in range(num_items):
            product_id = random.randint(1, 50)
            price = round(random.uniform(10.0, 150.0), 2)
            order_total += price
            
            # Append the item data, linking it back to the current order_id
            order_items_data.append((order_id, product_id, price))
            
        # Append the parent order data, using the sum of the items as the total amount
        orders_data.append((order_id, customer_id, order_date, round(order_total, 2), status))

    print(f"Inserting {len(orders_data)} rows into 'orders'...")
    execute_values(cur, 
        "INSERT INTO orders (order_id, customer_id, order_date, amount, status) VALUES %s", 
        orders_data
    )

    print(f"Inserting {len(order_items_data)} rows into 'order_items'...")
    execute_values(cur, 
        "INSERT INTO order_items (order_id, product_id, price) VALUES %s", 
        order_items_data
    )

    # Commit the transaction and close the connection
    conn.commit()
    cur.close()
    conn.close()
    print("Success! Both tables are populated and relationally linked.")

if __name__ == "__main__":
    generate_orders_and_items()