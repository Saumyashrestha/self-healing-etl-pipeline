import os
from urllib.parse import urlparse
import psycopg2
from dotenv import load_dotenv

load_dotenv()
db_url = os.getenv("DATABASE_URL")
parsed = urlparse(db_url)

conn = psycopg2.connect(
    host=parsed.hostname,
    port=parsed.port,
    dbname=parsed.path.lstrip("/"),
    user=parsed.username,
    password=parsed.password
)
cur = conn.cursor()

cur.execute("ALTER TABLE orders ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT NOW();")
cur.execute("ALTER TABLE order_items ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT NOW();")
cur.execute("""
    CREATE TABLE IF NOT EXISTS pipeline_watermark (
        source_name TEXT PRIMARY KEY,
        last_loaded_at TIMESTAMP NOT NULL,
        updated_at TIMESTAMP DEFAULT NOW()
    );
""")

conn.commit()
cur.close()
conn.close()
print("Schema setup complete.")