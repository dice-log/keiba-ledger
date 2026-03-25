"""
ML用 データローダー

analytics.races と analytics.race_entries を JOIN し、
特徴量構築に必要な列を含めて返す。
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

import numpy as np
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


def get_win_odds_timeseries_summary(from_date: str, to_date: str) -> pd.DataFrame | None:
    """
    時系列オッズから単勝の「最初」「最後」オッズを集約。

    analytics.odds_timeseries にデータがある場合のみ返す。
    返り値: race_id, horse_number, odds_ts_first, odds_ts_last, odds_ts_change_rate
    """
    conn = psycopg2.connect(**get_db_config())
    query = """
        WITH first_odds AS (
            SELECT DISTINCT ON (t.race_id, t.combination)
                t.race_id, t.combination::int AS horse_number, t.odds AS odds_ts_first
            FROM analytics.odds_timeseries t
            JOIN analytics.races r ON r.race_id = t.race_id
            WHERE t.bet_type = 'win' AND r.race_date BETWEEN %s AND %s
              AND TRIM(t.combination) ~ '^[0-9]+$'
            ORDER BY t.race_id, t.combination, t.recorded_at ASC
        ),
        last_odds AS (
            SELECT DISTINCT ON (t.race_id, t.combination)
                t.race_id, t.combination::int AS horse_number, t.odds AS odds_ts_last
            FROM analytics.odds_timeseries t
            JOIN analytics.races r ON r.race_id = t.race_id
            WHERE t.bet_type = 'win' AND r.race_date BETWEEN %s AND %s
              AND TRIM(t.combination) ~ '^[0-9]+$'
            ORDER BY t.race_id, t.combination, t.recorded_at DESC
        )
        SELECT f.race_id, f.horse_number, f.odds_ts_first, l.odds_ts_last
        FROM first_odds f
        JOIN last_odds l ON f.race_id = l.race_id AND f.horse_number = l.horse_number
    """
    try:
        df = pd.read_sql(
            query, conn, params=(from_date, to_date, from_date, to_date)
        )
    except Exception:
        df = pd.DataFrame()
    conn.close()
    if df.empty:
        return None
    df["odds_ts_change_rate"] = np.where(
        df["odds_ts_first"] > 0,
        (df["odds_ts_last"] - df["odds_ts_first"]) / df["odds_ts_first"],
        np.nan,
    )
    return df


def get_race_entries_ml(
    from_date: str, to_date: str, use_timeseries: bool = False
) -> pd.DataFrame:
    """
    ML用：指定期間のレース・出走馬データを取得。

    Returns:
        DataFrame: race_id, race_date, horse_id, win_odds, finish_pos,
                   jockey_id, trainer_id, frame_number, weight_carried,
                   horse_weight, weight_diff, popularity, surface, track_condition,
                   distance, field_count, venue_code, grade 等
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
            r.distance,
            r.field_count,
            r.grade,
            e.horse_id,
            e.horse_number,
            e.frame_number,
            e.win_odds,
            e.finish_pos,
            e.jockey_id,
            e.trainer_id,
            e.weight_carried,
            e.horse_weight,
            e.weight_diff,
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

    if use_timeseries:
        ts = get_win_odds_timeseries_summary(from_date, to_date)
        if ts is not None and not ts.empty:
            df = df.merge(
                ts,
                on=["race_id", "horse_number"],
                how="left",
                suffixes=("", "_ts"),
            )

    return df
