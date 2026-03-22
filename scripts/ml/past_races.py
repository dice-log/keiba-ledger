"""
馬の過去レース集計ロジック（共通）

target_df の各行について、その時点より前の馬の出走履歴を集計し、
career_win_rate, days_since_last_race, surface_win_rate 等の特徴量を付与する。
"""

import pandas as pd
import numpy as np


def load_horse_history(from_date: str, to_date: str, db_config: dict | None = None) -> pd.DataFrame:
    """
    馬の出走履歴を取得。
    race_date, horse_id, finish_pos, surface, last_3f を含む。
    """
    if db_config is None:
        from ml.load_data import get_db_config
        db_config = get_db_config()

    import psycopg2
    conn = psycopg2.connect(**db_config)
    query = """
        SELECT
            r.race_date,
            r.race_id,
            r.surface,
            e.horse_id,
            e.finish_pos,
            e.last_3f
        FROM analytics.races r
        JOIN analytics.race_entries e ON r.race_id = e.race_id
        WHERE r.race_date BETWEEN %s AND %s
          AND e.finish_pos IS NOT NULL
        ORDER BY r.race_date
    """
    df = pd.read_sql(query, conn, params=(from_date, to_date))
    conn.close()
    return df


def compute_horse_past_stats(
    history_df: pd.DataFrame,
    target_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    target_df の各行に、その時点より前の馬の過去成績を付与。

    history_df: race_date, horse_id, finish_pos, surface, last_3f
    target_df: race_id, race_date, horse_id を含むこと

    Returns:
        target_df に以下の列を追加した DataFrame:
        - career_starts: 過去出走数
        - career_wins: 過去1着数
        - career_win_rate: 通算勝率
        - days_since_last_race: 前走からの日数
        - surface_win_rate_芝: 芝の勝率
        - surface_win_rate_ダート: ダートの勝率
        - avg_last_3f_3: 直近3走の上がり3F平均
        - last_3_avg_pos: 直近3走の平均着順
    """
    if history_df.empty:
        return _add_null_past_stats(target_df)

    hist = history_df.copy()
    hist["race_date"] = pd.to_datetime(hist["race_date"])
    hist["surface"] = hist["surface"].fillna("").astype(str)
    hist["won"] = (hist["finish_pos"] == 1).astype(int)
    hist = hist.rename(columns={"race_date": "hist_date", "race_id": "hist_race_id"})
    hist = hist[["hist_date", "horse_id", "finish_pos", "surface", "last_3f", "won"]]

    tgt = target_df[["race_id", "race_date", "horse_id"]].copy()
    tgt["race_date"] = pd.to_datetime(tgt["race_date"])

    merged = tgt.merge(hist, on="horse_id", how="left")
    past = merged[merged["hist_date"] < merged["race_date"]].copy()
    if past.empty:
        return _add_null_past_stats(target_df)

    def safe_rate(w: pd.Series, s: pd.Series) -> pd.Series:
        return np.where(s > 0, w / s, np.nan)

    career = past.groupby(["race_id", "horse_id"]).agg(
        career_starts=("finish_pos", "count"),
        career_wins=("won", "sum"),
    ).reset_index()
    career["career_win_rate"] = safe_rate(career["career_wins"], career["career_starts"])

    last_race = past.groupby(["race_id", "horse_id"])["hist_date"].max().reset_index()
    last_race.columns = ["race_id", "horse_id", "last_race_date"]
    career = career.merge(last_race, on=["race_id", "horse_id"], how="left")
    tgt_date = tgt[["race_id", "horse_id", "race_date"]].drop_duplicates()
    career = career.merge(tgt_date, on=["race_id", "horse_id"], how="left")
    career["days_since_last_race"] = (career["race_date"] - career["last_race_date"]).dt.days
    career = career.drop(columns=["last_race_date", "race_date"], errors="ignore")

    past_芝 = past[past["surface"].isin(["芝"])]
    past_ダ = past[past["surface"].isin(["ダ", "ダート"])]

    sur_芝 = past_芝.groupby(["race_id", "horse_id"]).agg(
        s_芝_starts=("finish_pos", "count"),
        s_芝_wins=("won", "sum"),
    ).reset_index()
    sur_芝["surface_win_rate_芝"] = safe_rate(sur_芝["s_芝_wins"], sur_芝["s_芝_starts"])
    sur_芝 = sur_芝[["race_id", "horse_id", "surface_win_rate_芝"]]

    sur_ダ = past_ダ.groupby(["race_id", "horse_id"]).agg(
        s_ダ_starts=("finish_pos", "count"),
        s_ダ_wins=("won", "sum"),
    ).reset_index()
    sur_ダ["surface_win_rate_ダート"] = safe_rate(sur_ダ["s_ダ_wins"], sur_ダ["s_ダ_starts"])
    sur_ダ = sur_ダ[["race_id", "horse_id", "surface_win_rate_ダート"]]

    past_sorted = past.sort_values(["race_id", "horse_id", "hist_date"], ascending=[True, True, False])
    last3 = past_sorted.groupby(["race_id", "horse_id"]).head(3)
    avg3 = last3.groupby(["race_id", "horse_id"]).agg(
        avg_last_3f_3=("last_3f", "mean"),
        last_3_avg_pos=("finish_pos", "mean"),
    ).reset_index()

    out = target_df.copy()
    out = out.merge(career[["race_id", "horse_id", "career_starts", "career_wins", "career_win_rate", "days_since_last_race"]], on=["race_id", "horse_id"], how="left")
    out = out.merge(sur_芝, on=["race_id", "horse_id"], how="left")
    out = out.merge(sur_ダ, on=["race_id", "horse_id"], how="left")
    out = out.merge(avg3, on=["race_id", "horse_id"], how="left")

    return _fill_missing_past(out)


def load_jockey_trainer_history(from_date: str, to_date: str, db_config: dict | None = None) -> pd.DataFrame:
    """
    騎手・調教師の出走履歴を取得（直近90日勝率用）。
    race_date, jockey_id, trainer_id, finish_pos を含む。
    """
    if db_config is None:
        from ml.load_data import get_db_config
        db_config = get_db_config()
    import psycopg2
    conn = psycopg2.connect(**db_config)
    query = """
        SELECT r.race_date, e.jockey_id, e.trainer_id, e.finish_pos
        FROM analytics.races r
        JOIN analytics.race_entries e ON r.race_id = e.race_id
        WHERE r.race_date BETWEEN %s AND %s AND e.finish_pos IS NOT NULL
    """
    df = pd.read_sql(query, conn, params=(from_date, to_date))
    conn.close()
    return df


def add_jockey_trainer_recent_stats(
    target_df: pd.DataFrame,
    history_from: str,
    history_to: str,
    days: int = 90,
    db_config: dict | None = None,
) -> pd.DataFrame:
    """
    騎手・調教師の直近 N 日勝率を付与。
    jockey_win_rate_90d, trainer_win_rate_90d を追加。
    """
    hist = load_jockey_trainer_history(history_from, history_to, db_config)
    if hist.empty:
        out = target_df.copy()
        out["jockey_win_rate_90d"] = 0.0
        out["trainer_win_rate_90d"] = 0.0
        return out
    hist["race_date"] = pd.to_datetime(hist["race_date"])
    hist["jockey_id"] = hist["jockey_id"].fillna("").astype(str)
    hist["trainer_id"] = hist["trainer_id"].fillna("").astype(str)
    hist["won"] = (hist["finish_pos"] == 1).astype(int)

    tgt = target_df[["race_id", "race_date", "jockey_id", "trainer_id"]].copy()
    tgt["race_date"] = pd.to_datetime(tgt["race_date"])
    tgt["jockey_id"] = tgt["jockey_id"].fillna("").astype(str)
    tgt["trainer_id"] = tgt["trainer_id"].fillna("").astype(str)
    tgt["race_date_min"] = tgt["race_date"] - pd.Timedelta(days=days)

    def compute_90d(id_col: str, prefix: str) -> pd.DataFrame:
        m = tgt[[id_col, "race_id", "race_date", "race_date_min"]].merge(
            hist[[id_col, "race_date", "won"]].rename(columns={"race_date": "h_date"}),
            on=id_col,
            how="left",
        )
        m = m[(m["h_date"] >= m["race_date_min"]) & (m["h_date"] < m["race_date"])]
        agg = m.groupby(["race_id", id_col]).agg(
            _starts=("won", "count"),
            _wins=("won", "sum"),
        ).reset_index()
        agg[prefix] = np.where(agg["_starts"] > 0, agg["_wins"] / agg["_starts"], 0.0)
        return agg[["race_id", id_col, prefix]]

    jk = compute_90d("jockey_id", "jockey_win_rate_90d")
    tr = compute_90d("trainer_id", "trainer_win_rate_90d")

    out = target_df.copy()
    out = out.merge(jk, on=["race_id", "jockey_id"], how="left")
    out = out.merge(tr, on=["race_id", "trainer_id"], how="left")
    out["jockey_win_rate_90d"] = out["jockey_win_rate_90d"].fillna(0.0)
    out["trainer_win_rate_90d"] = out["trainer_win_rate_90d"].fillna(0.0)
    return out


def add_horse_past_stats(
    target_df: pd.DataFrame,
    history_from: str,
    history_to: str,
    db_config: dict | None = None,
) -> pd.DataFrame:
    """
    便利関数: 履歴をロードして target_df に過去成績を付与。
    """
    history = load_horse_history(history_from, history_to, db_config)
    return compute_horse_past_stats(history, target_df)


def _add_null_past_stats(target_df: pd.DataFrame) -> pd.DataFrame:
    out = target_df.copy()
    for col in ["career_starts", "career_wins", "career_win_rate", "days_since_last_race",
                "surface_win_rate_芝", "surface_win_rate_ダート", "avg_last_3f_3", "last_3_avg_pos"]:
        out[col] = np.nan
    return _fill_missing_past(out)


def _fill_missing_past(df: pd.DataFrame) -> pd.DataFrame:
    for col in ["career_starts", "career_wins"]:
        if col in df.columns:
            df[col] = df[col].fillna(0).astype(int)
    for col in ["career_win_rate", "surface_win_rate_芝", "surface_win_rate_ダート"]:
        if col in df.columns:
            df[col] = df[col].fillna(0.0)
    if "days_since_last_race" in df.columns:
        df["days_since_last_race"] = df["days_since_last_race"].fillna(365).astype(int)
    if "avg_last_3f_3" in df.columns:
        df["avg_last_3f_3"] = df["avg_last_3f_3"].fillna(37.0)
    if "last_3_avg_pos" in df.columns:
        df["last_3_avg_pos"] = df["last_3_avg_pos"].fillna(8.0)
    return df
