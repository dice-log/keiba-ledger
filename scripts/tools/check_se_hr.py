"""Check SE/HR raw_text format"""
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

import psycopg2

conn = psycopg2.connect(
    host=os.getenv("LOCAL_DB_HOST", "localhost"),
    port=int(os.getenv("LOCAL_DB_PORT", "5432")),
    dbname=os.getenv("LOCAL_DB_NAME", "keiba"),
    user=os.getenv("LOCAL_DB_USER", "postgres"),
    password=os.getenv("LOCAL_DB_PASSWORD", ""),
)
cur = conn.cursor()
for rt in ["SE", "HR"]:
    cur.execute(
        "SELECT record_type, LENGTH(raw_text), raw_text FROM raw.jvdata WHERE record_type = %s LIMIT 1",
        (rt,),
    )
    row = cur.fetchone()
    if row:
        print(f"{rt}: len={row[1]}")
        print(f"  first 100 chars: {repr(row[2][:100])}")
cur.close()
conn.close()
