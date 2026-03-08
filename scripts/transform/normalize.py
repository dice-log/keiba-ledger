"""
raw.jvdata → analytics 正規化
設計書 Phase 1 Step 5

processed=FALSE のレコードをパースして analytics テーブルに挿入し、processed=TRUE に更新。

処理対象: RA→races, SE→race_entries, HR→payouts
"""

import argparse
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
from loguru import logger

sys.path.insert(0, str(Path(__file__).parent))
from parse_ra import parse_ra
from parse_se import parse_se
from parse_hr import parse_hr


def get_db_config():
    return {
        "host": os.getenv("LOCAL_DB_HOST", "localhost"),
        "port": int(os.getenv("LOCAL_DB_PORT", "5432")),
        "dbname": os.getenv("LOCAL_DB_NAME", "keiba"),
        "user": os.getenv("LOCAL_DB_USER", "postgres"),
        "password": os.getenv("LOCAL_DB_PASSWORD", ""),
    }


def main():
    parser = argparse.ArgumentParser(description="raw.jvdata を analytics に正規化")
    parser.add_argument("--dry-run", action="store_true", help="DBに書き込まずテスト")
    parser.add_argument("--limit", type=int, default=0, help="処理件数制限")
    args = parser.parse_args()

    (ROOT / "logs").mkdir(exist_ok=True)
    logger.add(ROOT / "logs" / "normalize.log", rotation="1 MB", retention="7 days")
    logger.info("normalize start")

    conn = psycopg2.connect(**get_db_config())
    cur = conn.cursor()

    cur.execute(
        """SELECT id, record_type, raw_text FROM raw.jvdata WHERE processed = FALSE
           ORDER BY CASE record_type WHEN 'RA' THEN 1 WHEN 'SE' THEN 2 WHEN 'HR' THEN 3 ELSE 4 END, id"""
    )
    rows = cur.fetchall()
    if args.limit:
        rows = rows[: args.limit]

    stats = {"RA": 0, "SE": 0, "HR": 0, "skip": 0, "err": 0}
    processed_ids = []

    for row_id, record_type, raw_text in rows:
        raw_text = raw_text or ""
        try:
            if record_type == "RA":
                d = parse_ra(raw_text)
                if d and d.get("race_id"):
                    if not args.dry_run:
                        cur.execute(
                            """
                            INSERT INTO analytics.races (
                                race_id, race_date, venue_code, venue_name, race_number,
                                race_name, grade, surface, distance, weather, track_condition, field_count
                            ) VALUES (
                                %(race_id)s, %(race_date)s, %(venue_code)s, %(venue_name)s, %(race_number)s,
                                %(race_name)s, %(grade)s, %(surface)s, %(distance)s, %(weather)s,
                                %(track_condition)s, %(field_count)s
                            )
                            ON CONFLICT (race_id) DO UPDATE SET
                                race_date = EXCLUDED.race_date,
                                venue_name = EXCLUDED.venue_name,
                                race_name = EXCLUDED.race_name,
                                grade = EXCLUDED.grade,
                                surface = EXCLUDED.surface,
                                distance = EXCLUDED.distance,
                                weather = EXCLUDED.weather,
                                track_condition = EXCLUDED.track_condition,
                                field_count = EXCLUDED.field_count
                            """,
                            d,
                        )
                    stats["RA"] += 1
                    processed_ids.append(row_id)
                else:
                    stats["skip"] += 1

            elif record_type == "SE":
                d = parse_se(raw_text)
                if d and d.get("race_id") and d.get("horse_id"):
                    if not args.dry_run:
                        cur.execute(
                            """
                            INSERT INTO analytics.race_entries (
                                race_id, horse_id, horse_name, horse_number, frame_number,
                                jockey_id, jockey_name, trainer_id, trainer_name,
                                weight_carried, horse_weight, weight_diff,
                                finish_pos, finish_time, last_3f, win_odds, popularity,
                                running_style, corner_pos
                            ) VALUES (
                                %(race_id)s, %(horse_id)s, %(horse_name)s, %(horse_number)s, %(frame_number)s,
                                %(jockey_id)s, %(jockey_name)s, %(trainer_id)s, %(trainer_name)s,
                                %(weight_carried)s, %(horse_weight)s, %(weight_diff)s,
                                %(finish_pos)s, %(finish_time)s, %(last_3f)s, %(win_odds)s, %(popularity)s,
                                %(running_style)s, %(corner_pos)s
                            )
                            """,
                            d,
                        )
                    stats["SE"] += 1
                    processed_ids.append(row_id)
                else:
                    stats["skip"] += 1

            elif record_type == "HR":
                payouts = parse_hr(raw_text)
                if payouts:
                    if not args.dry_run:
                        for p in payouts:
                            cur.execute(
                                """
                                INSERT INTO analytics.payouts (race_id, bet_type, combination, payout, popularity)
                                VALUES (%(race_id)s, %(bet_type)s, %(combination)s, %(payout)s, %(popularity)s)
                                """,
                                p,
                            )
                    stats["HR"] += 1
                    processed_ids.append(row_id)
                else:
                    stats["skip"] += 1

            else:
                stats["skip"] += 1

        except Exception as e:
            stats["err"] += 1
            logger.warning(f"id={row_id} type={record_type} err={e}")

    if processed_ids and not args.dry_run:
        cur.execute(
            "UPDATE raw.jvdata SET processed = TRUE WHERE id = ANY(%s)",
            (processed_ids,),
        )

    conn.commit()
    cur.close()
    conn.close()

    logger.info(f"normalize done: {stats}")
    print("✅ 正規化完了:", stats)


if __name__ == "__main__":
    main()
