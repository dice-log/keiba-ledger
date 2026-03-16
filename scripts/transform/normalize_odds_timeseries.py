"""
raw.odds_timeseries_raw → analytics.odds_timeseries 正規化

時系列オッズ（0B41/O1, 0B42/O2）の生データをパースして analytics.odds_timeseries に挿入する。
"""

import os
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

try:
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
except ImportError:
    pass

import psycopg2
from loguru import logger

sys.path.insert(0, str(Path(__file__).parent))
from parse_o1 import parse_o1_timeseries
from parse_o2 import parse_o2_timeseries


def get_db_config():
    return {
        "host": os.getenv("LOCAL_DB_HOST", "localhost"),
        "port": int(os.getenv("LOCAL_DB_PORT", "5432")),
        "dbname": os.getenv("LOCAL_DB_NAME", "keiba"),
        "user": os.getenv("LOCAL_DB_USER", "postgres"),
        "password": os.getenv("LOCAL_DB_PASSWORD", ""),
    }


def build_recorded_at(race_id: str, announce_mmddhhmm: str):
    """race_id の年 + 発表月日時分(MMDDhhmm) から recorded_at を組み立てる。"""
    if len(race_id) < 8 or not announce_mmddhhmm or len(announce_mmddhhmm) != 8:
        return None
    yyyy = race_id[:4]
    mmdd = announce_mmddhhmm[:4]
    hhmm = announce_mmddhhmm[4:]
    try:
        dt = datetime.strptime(yyyy + mmdd + hhmm, "%Y%m%d%H%M")
        return dt.replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def main():
    import argparse

    parser = argparse.ArgumentParser(description="時系列オッズ raw → analytics.odds_timeseries")
    parser.add_argument("--dry-run", action="store_true", help="DBに書き込まずテスト")
    parser.add_argument("--limit", type=int, default=0, help="処理件数制限")
    args = parser.parse_args()

    (ROOT / "logs").mkdir(exist_ok=True)
    logger.add(ROOT / "logs" / "normalize.log", rotation="1 MB", retention="7 days")
    logger.info("normalize_odds_timeseries start")

    conn = psycopg2.connect(**get_db_config())
    cur = conn.cursor()

    cur.execute(
        """SELECT id, dataspec, record_type, raw_text
           FROM raw.odds_timeseries_raw
           ORDER BY id"""
    )
    rows = cur.fetchall()
    if args.limit:
        rows = rows[: args.limit]

    known_race_ids = set()
    cur.execute("SELECT race_id FROM analytics.races")
    known_race_ids = {r[0] for r in cur.fetchall()}

    stats = {"O1": 0, "O2": 0, "skip": 0, "err": 0}
    inserted = 0

    for row_id, dataspec, record_type, raw_text in rows:
        raw_text = raw_text or ""
        try:
            if record_type == "O1":
                parsed = parse_o1_timeseries(raw_text)
            elif record_type == "O2":
                parsed = parse_o2_timeseries(raw_text)
            else:
                stats["skip"] += 1
                continue

            if not parsed:
                stats["skip"] += 1
                continue

            race_id, announce, items = parsed
            if race_id not in known_race_ids:
                stats["skip"] += 1
                continue

            recorded_at = build_recorded_at(race_id, announce)
            if not recorded_at:
                stats["skip"] += 1
                continue

            if record_type == "O1":
                stats["O1"] += 1
            else:
                stats["O2"] += 1

            if not args.dry_run:
                for item in items:
                    cur.execute(
                        """
                        INSERT INTO analytics.odds_timeseries
                            (race_id, bet_type, combination, odds, recorded_at, minutes_to_start)
                        VALUES (%s, %s, %s, %s, %s, NULL)
                        """,
                        (
                            race_id,
                            item["bet_type"],
                            item["combination"],
                            item["odds"],
                            recorded_at,
                        ),
                    )
                inserted += len(items)
                conn.commit()

        except Exception as e:
            stats["err"] += 1
            logger.warning(f"id={row_id} type={record_type} err={e}")
            conn.rollback()

    cur.close()
    conn.close()

    logger.info(f"normalize_odds_timeseries done: {stats} inserted={inserted}")
    print("[OK] 時系列オッズ正規化完了:", stats, "inserted=", inserted)


if __name__ == "__main__":
    main()
