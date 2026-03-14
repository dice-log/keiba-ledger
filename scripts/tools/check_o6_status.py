#!/usr/bin/env python3
"""O6正規化状況の確認"""
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
try:
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
except ImportError:
    pass

import psycopg2

def main():
    conn = psycopg2.connect(
        host=os.getenv("LOCAL_DB_HOST", "localhost"),
        port=int(os.getenv("LOCAL_DB_PORT", "5432")),
        dbname=os.getenv("LOCAL_DB_NAME", "keiba"),
        user=os.getenv("LOCAL_DB_USER", "postgres"),
        password=os.getenv("LOCAL_DB_PASSWORD", ""),
    )
    cur = conn.cursor()

    # O6の未処理・処理済み件数
    cur.execute("""
        SELECT processed, COUNT(*) FROM raw.jvdata WHERE record_type = 'O6'
        GROUP BY processed
    """)
    by_processed = dict(cur.fetchall())
    print("O6 raw.jvdata (processed/未処理):", by_processed)

    # O6由来のodds_final (trifecta) 件数
    cur.execute("SELECT COUNT(*) FROM analytics.odds_final WHERE bet_type = 'trifecta'")
    trifecta_count = cur.fetchone()[0]
    print("trifecta in odds_final:", trifecta_count)

    # 全O6レコード数
    cur.execute("SELECT COUNT(*) FROM raw.jvdata WHERE record_type = 'O6'")
    o6_total = cur.fetchone()[0]
    print("O6 total:", o6_total)

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
