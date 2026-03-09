"""
raw.jvdata → analytics 正規化
設計書 Phase 1 Step 5

processed=FALSE のレコードをパースして analytics テーブルに挿入し、processed=TRUE に更新。

処理対象: RA→races, SE→race_entries, HR→payouts, UM→horses, KS→jockeys
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
from parse_um import parse_um
from parse_ks import parse_ks


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
    parser.add_argument("--um-ks-only", action="store_true", help="UM/KS のみ正規化")
    args = parser.parse_args()

    (ROOT / "logs").mkdir(exist_ok=True)
    logger.add(ROOT / "logs" / "normalize.log", rotation="1 MB", retention="7 days")
    logger.info("normalize start")

    conn = psycopg2.connect(**get_db_config())
    cur = conn.cursor()

    cur.execute(
        """SELECT id, record_type, raw_text FROM raw.jvdata WHERE processed = FALSE
           ORDER BY
             CASE record_type WHEN 'RA' THEN 1 WHEN 'SE' THEN 2 WHEN 'HR' THEN 3 WHEN 'UM' THEN 4 WHEN 'KS' THEN 5 ELSE 6 END,
             CASE WHEN record_type = 'RA' AND SUBSTRING(COALESCE(raw_text,'') FROM 3 FOR 1) IN ('5','6','7') THEN 1 ELSE 0 END,
             CASE WHEN record_type = 'SE' AND SUBSTRING(COALESCE(raw_text,'') FROM 3 FOR 1) IN ('5','6','7') THEN 1 ELSE 0 END,
             id"""
    )
    rows = cur.fetchall()
    if args.um_ks_only:
        rows = [r for r in rows if r[1] in ("UM", "KS")]
    if args.limit:
        rows = rows[: args.limit]

    cur.execute("SELECT race_id FROM analytics.races")
    known_race_ids = {r[0] for r in cur.fetchall()}

    stats = {"RA": 0, "SE": 0, "HR": 0, "UM": 0, "KS": 0, "skip": 0, "err": 0}
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
                                race_name, grade, surface, distance, direction,
                                weather, track_condition, field_count
                            ) VALUES (
                                %(race_id)s, %(race_date)s, %(venue_code)s, %(venue_name)s, %(race_number)s,
                                %(race_name)s, %(grade)s, %(surface)s, %(distance)s, %(direction)s,
                                %(weather)s, %(track_condition)s, %(field_count)s
                            )
                            ON CONFLICT (race_id) DO UPDATE SET
                                race_date = EXCLUDED.race_date,
                                venue_name = EXCLUDED.venue_name,
                                race_name = EXCLUDED.race_name,
                                grade = EXCLUDED.grade,
                                surface = EXCLUDED.surface,
                                distance = EXCLUDED.distance,
                                direction = EXCLUDED.direction,
                                weather = EXCLUDED.weather,
                                track_condition = EXCLUDED.track_condition,
                                field_count = EXCLUDED.field_count
                            """,
                            d,
                        )
                    stats["RA"] += 1
                    processed_ids.append(row_id)
                    known_race_ids.add(d["race_id"])
                else:
                    stats["skip"] += 1

            elif record_type == "SE":
                d = parse_se(raw_text)
                if d and d.get("race_id") and d.get("horse_id") and d["race_id"] in known_race_ids:
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
                            ON CONFLICT (race_id, horse_id) DO UPDATE SET
                                horse_name = EXCLUDED.horse_name,
                                horse_number = EXCLUDED.horse_number,
                                frame_number = EXCLUDED.frame_number,
                                jockey_id = EXCLUDED.jockey_id,
                                jockey_name = EXCLUDED.jockey_name,
                                trainer_id = EXCLUDED.trainer_id,
                                trainer_name = EXCLUDED.trainer_name,
                                weight_carried = EXCLUDED.weight_carried,
                                horse_weight = EXCLUDED.horse_weight,
                                weight_diff = EXCLUDED.weight_diff,
                                finish_pos = EXCLUDED.finish_pos,
                                finish_time = EXCLUDED.finish_time,
                                last_3f = EXCLUDED.last_3f,
                                win_odds = EXCLUDED.win_odds,
                                popularity = EXCLUDED.popularity,
                                running_style = EXCLUDED.running_style,
                                corner_pos = EXCLUDED.corner_pos
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
                    payouts = [p for p in payouts if p["race_id"] in known_race_ids]
                if payouts:
                    if not args.dry_run:
                        for p in payouts:
                            cur.execute(
                                """
                                INSERT INTO analytics.payouts (race_id, bet_type, combination, payout, popularity)
                                VALUES (%(race_id)s, %(bet_type)s, %(combination)s, %(payout)s, %(popularity)s)
                                ON CONFLICT (race_id, bet_type, combination) DO UPDATE SET
                                    payout = EXCLUDED.payout, popularity = EXCLUDED.popularity
                                """,
                                p,
                            )
                    stats["HR"] += 1
                    processed_ids.append(row_id)
                else:
                    stats["skip"] += 1

            elif record_type == "UM":
                d = parse_um(raw_text)
                if d and d.get("horse_id"):
                    if not args.dry_run:
                        cur.execute(
                            """
                            INSERT INTO analytics.horses (
                                horse_id, name, name_kana, birth_date, sex, coat_color,
                                sire_id, sire_name, dam_id, dam_name, broodmare_sire,
                                trainer_id, owner_name, breeder_name
                            ) VALUES (
                                %(horse_id)s, %(name)s, %(name_kana)s, %(birth_date)s, %(sex)s, %(coat_color)s,
                                %(sire_id)s, %(sire_name)s, %(dam_id)s, %(dam_name)s, %(broodmare_sire)s,
                                %(trainer_id)s, %(owner_name)s, %(breeder_name)s
                            )
                            ON CONFLICT (horse_id) DO UPDATE SET
                                name = EXCLUDED.name,
                                name_kana = EXCLUDED.name_kana,
                                birth_date = EXCLUDED.birth_date,
                                sex = EXCLUDED.sex,
                                coat_color = EXCLUDED.coat_color,
                                sire_id = EXCLUDED.sire_id,
                                sire_name = EXCLUDED.sire_name,
                                dam_id = EXCLUDED.dam_id,
                                dam_name = EXCLUDED.dam_name,
                                broodmare_sire = EXCLUDED.broodmare_sire,
                                trainer_id = EXCLUDED.trainer_id,
                                owner_name = EXCLUDED.owner_name,
                                breeder_name = EXCLUDED.breeder_name,
                                updated_at = NOW()
                            """,
                            d,
                        )
                    stats["UM"] += 1
                    processed_ids.append(row_id)
                else:
                    stats["skip"] += 1

            elif record_type == "KS":
                d = parse_ks(raw_text)
                if d and d.get("jockey_id"):
                    if not args.dry_run:
                        cur.execute(
                            """
                            INSERT INTO analytics.jockeys (jockey_id, name, name_kana, belong_to, birth_date)
                            VALUES (%(jockey_id)s, %(name)s, %(name_kana)s, %(belong_to)s, %(birth_date)s)
                            ON CONFLICT (jockey_id) DO UPDATE SET
                                name = EXCLUDED.name,
                                name_kana = EXCLUDED.name_kana,
                                belong_to = EXCLUDED.belong_to,
                                birth_date = EXCLUDED.birth_date
                            """,
                            d,
                        )
                    stats["KS"] += 1
                    processed_ids.append(row_id)
                else:
                    stats["skip"] += 1

            else:
                stats["skip"] += 1

        except Exception as e:
            stats["err"] += 1
            logger.warning(f"id={row_id} type={record_type} err={e}")
            conn.rollback()

    if processed_ids and not args.dry_run:
        cur.execute(
            "UPDATE raw.jvdata SET processed = TRUE WHERE id = ANY(%s)",
            (processed_ids,),
        )

    conn.commit()
    cur.close()
    conn.close()

    logger.info(f"normalize done: {stats}")
    print("[OK] 正規化完了:", stats)


if __name__ == "__main__":
    main()
