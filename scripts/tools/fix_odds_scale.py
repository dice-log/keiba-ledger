"""odds_final の既存オッズを 1/10 に補正（JV-Data 仕様に合わせる）"""
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

cur.execute("SELECT COUNT(*) FROM analytics.odds_final")
before_count = cur.fetchone()[0]

cur.execute(
    "UPDATE analytics.odds_final SET odds = ROUND(odds::numeric / 10, 1)"
)
updated = cur.rowcount
conn.commit()

cur.execute("SELECT bet_type, MIN(odds), MAX(odds) FROM analytics.odds_final GROUP BY bet_type ORDER BY bet_type")
print("補正後 各bet_typeのオッズ範囲:")
for r in cur.fetchall():
    print(f"  {r[0]}: {r[1]} ～ {r[2]}")
conn.close()

print(f"\n[OK] {updated:,} 件を 1/10 に補正しました")
