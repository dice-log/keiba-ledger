"""O2 normalize 進捗確認"""
import os
from pathlib import Path
ROOT = Path(__file__).resolve().parents[2]
try:
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env", encoding="utf-8")
except Exception:
    pass
import psycopg2

cfg = {
    "host": os.getenv("LOCAL_DB_HOST", "localhost"),
    "port": int(os.getenv("LOCAL_DB_PORT", "5432")),
    "dbname": os.getenv("LOCAL_DB_NAME", "keiba"),
    "user": os.getenv("LOCAL_DB_USER", "postgres"),
    "password": os.getenv("LOCAL_DB_PASSWORD", ""),
}
conn = psycopg2.connect(**cfg)
cur = conn.cursor()
cur.execute("SELECT bet_type, COUNT(*) FROM analytics.odds_final GROUP BY bet_type ORDER BY bet_type")
print("odds_final:")
for r in cur.fetchall():
    print(f"  {r[0]}: {r[1]:,}")
for rt in ("O2", "O3", "O4", "O5", "O6"):
    cur.execute(f"SELECT COUNT(*) FROM raw.jvdata WHERE record_type=%s AND processed=FALSE", (rt,))
    print(f"{rt} unprocessed:", cur.fetchone()[0])
    cur.execute(f"SELECT COUNT(*) FROM raw.jvdata WHERE record_type=%s AND processed=TRUE", (rt,))
    print(f"{rt} processed:", cur.fetchone()[0])
conn.close()
