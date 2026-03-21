"""
DB からレース・出走馬データを取得

analytics.races と analytics.race_entries を JOIN し、
単勝オッズ・着順がある出走馬のみを返す。
"""

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

try:
    from dotenv import load_dotenv
    p = ROOT / ".env"
    if p.exists():
        try:
            load_dotenv(p, encoding="utf-8")
        except UnicodeDecodeError:
            load_dotenv(p, encoding="cp932")
except Exception:
    pass

import pandas as pd
import psycopg2


def get_db_config():
    return {
        "host": os.getenv("LOCAL_DB_HOST", "localhost"),
        "port": int(os.getenv("LOCAL_DB_PORT", "5432")),
        "dbname": os.getenv("LOCAL_DB_NAME", "keiba"),
        "user": os.getenv("LOCAL_DB_USER", "postgres"),
        "password": os.getenv("LOCAL_DB_PASSWORD", ""),
    }


def get_race_entries(from_date: str, to_date: str) -> pd.DataFrame:
    """
    指定期間のレース・出走馬データを取得。

    条件:
    - win_odds IS NOT NULL AND win_odds > 0
    - finish_pos IS NOT NULL（除外・取消を除く）

    Returns:
        DataFrame: race_id, race_date, horse_number, win_odds, finish_pos,
                   jockey_id, frame_number, weight_carried, surface, track_condition 等
    """
    conn = psycopg2.connect(**get_db_config())
    query = """
        SELECT
            r.race_id,
            r.race_date,
            r.venue_code,
            r.venue_name,
            r.race_number,
            r.surface,
            r.track_condition,
            r.weather,
            e.horse_id,
            e.horse_number,
            e.frame_number,
            e.win_odds,
            e.finish_pos,
            e.jockey_id,
            e.trainer_id,
            e.weight_carried,
            e.popularity
        FROM analytics.races r
        JOIN analytics.race_entries e ON r.race_id = e.race_id
        WHERE r.race_date BETWEEN %s AND %s
          AND e.win_odds IS NOT NULL
          AND e.win_odds > 0
          AND e.finish_pos IS NOT NULL
        ORDER BY r.race_date, r.race_id, e.horse_number
    """
    df = pd.read_sql(query, conn, params=(from_date, to_date))
    conn.close()
    return df
