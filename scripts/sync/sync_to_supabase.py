"""
ローカルPostgreSQL → Supabase 同期
設計書 Phase 1 Step 8

同期対象: races, race_entries（直近2年）, horses, jockeys（全件）
"""

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

try:
    import psycopg2
    from supabase import create_client, Client
except ImportError as e:
    print(f"❌ 依存関係不足: {e}")
    sys.exit(1)

from loguru import logger


def get_local_db():
    return psycopg2.connect(
        host=os.getenv("LOCAL_DB_HOST", "localhost"),
        port=int(os.getenv("LOCAL_DB_PORT", "5432")),
        dbname=os.getenv("LOCAL_DB_NAME", "keiba"),
        user=os.getenv("LOCAL_DB_USER", "postgres"),
        password=os.getenv("LOCAL_DB_PASSWORD", ""),
    )


def main():
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        logger.info("SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY 未設定のためスキップ")
        return

    (ROOT / "logs").mkdir(exist_ok=True)
    logger.add(ROOT / "logs" / "sync.log", rotation="1 MB", retention="7 days")

    conn = get_local_db()
    cur = conn.cursor()
    supabase: Client = create_client(url, key)

    # analytics.races: 直近2年
    cur.execute(
        """SELECT race_id, race_date, venue_code, venue_name, race_number, race_name,
                  grade, surface, distance, weather, track_condition, field_count
           FROM analytics.races
           WHERE race_date >= CURRENT_DATE - INTERVAL '2 years'
           ORDER BY race_date DESC"""
    )
    for row in cur.fetchall():
        try:
            supabase.table("races").upsert({
                "race_id": row[0],
                "race_date": str(row[1]),
                "venue_code": row[2],
                "venue_name": row[3],
                "race_number": row[4],
                "race_name": row[5],
                "grade": row[6],
                "surface": row[7],
                "distance": row[8],
                "weather": row[9],
                "track_condition": row[10],
                "field_count": row[11],
            }, on_conflict="race_id").execute()
        except Exception as e:
            logger.warning(f"races upsert {row[0]}: {e}")

    # analytics.race_entries: 直近2年
    cur.execute(
        """SELECT e.race_id, e.horse_id, e.horse_name, e.horse_number, e.frame_number,
                  e.jockey_name, e.win_odds, e.finish_pos
           FROM analytics.race_entries e
           JOIN analytics.races r ON e.race_id = r.race_id
           WHERE r.race_date >= CURRENT_DATE - INTERVAL '2 years'"""
    )
    entries = []
    for row in cur.fetchall():
        entries.append({
            "race_id": row[0],
            "horse_number": row[3],
            "horse_name": row[2],
            "jockey_name": row[5],
            "win_odds": float(row[6]) if row[6] else None,
            "finish_pos": row[7],
        })
    if entries:
        try:
            # Supabase race_entries に UNIQUE(race_id, horse_number) が必要
            supabase.table("race_entries").upsert(entries, on_conflict="race_id,horse_number").execute()
        except Exception as e:
            logger.warning(f"race_entries upsert: {e}")

    cur.close()
    conn.close()

    logger.info("sync_to_supabase done")


if __name__ == "__main__":
    main()
