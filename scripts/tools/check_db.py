"""DB状態確認"""
import os
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
try:
    from dotenv import load_dotenv
    p = ROOT / ".env"
    if p.exists():
        try: load_dotenv(p, encoding="utf-8")
        except UnicodeDecodeError: load_dotenv(p, encoding="cp932")
except: pass
os.environ.setdefault("PGCLIENTENCODING", "UTF8")
import psycopg2
cfg = {
    "host": os.getenv("LOCAL_DB_HOST", "localhost"),
    "port": int(os.getenv("LOCAL_DB_PORT", "5432")),
    "dbname": os.getenv("LOCAL_DB_NAME", "keiba"),
    "user": os.getenv("LOCAL_DB_USER", "postgres"),
    "password": os.getenv("LOCAL_DB_PASSWORD", ""),
    "options": "-c client_encoding=UTF8",
}
conn = psycopg2.connect(**cfg)
cur = conn.cursor()
cur.execute("SELECT record_type, processed, COUNT(*) FROM raw.jvdata GROUP BY 1,2 ORDER BY 1,2")
for r in cur.fetchall():
    print(r)
cur.execute("SELECT COUNT(*) FROM analytics.races")
print("analytics.races:", cur.fetchone()[0])
cur.execute("SELECT id, record_type, LENGTH(raw_text) FROM raw.jvdata WHERE record_type='RA' AND processed=FALSE LIMIT 3")
for r in cur.fetchall():
    print("RA sample:", r)
conn.close()
