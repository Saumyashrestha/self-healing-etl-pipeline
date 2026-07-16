"""
Truncates the pipeline_watermark control table.

Run this as part of a full project reset (after re-seeding Postgres and
deleting the Iceberg warehouse), so the next pipeline run doesn't inherit
a stale watermark from a previous run. Without this step, get_watermark()
in src/ingestion/pipeline.py may find an old row and skip re-initializing
the watermark, causing the first incremental batch to pull the entire
freshly-seeded baseline instead of a small incremental slice.
"""

import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()
db_url = os.getenv("DATABASE_URL")

conn = psycopg2.connect(db_url)
cur = conn.cursor()
cur.execute("TRUNCATE TABLE pipeline_watermark;")
conn.commit()
cur.close()
conn.close()

print("Pipeline_watermark table truncated.")