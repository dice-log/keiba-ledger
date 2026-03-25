"""
単勝予測用 特徴量構築

レース・出走馬のその場の情報に加え、過去レース集計（past_races）を利用。
df に add_horse_past_stats / add_jockey_trainer_recent_stats で付与済みの列が必要。
"""

import pandas as pd
import numpy as np


def _fit_encode(series: pd.Series) -> dict:
    """カテゴリ → 整数のマッピングをfit"""
    uniq = series.fillna("").astype(str).unique()
    return {v: i for i, v in enumerate(sorted(u for u in uniq if u))}


def _transform_encode(series: pd.Series, mapping: dict, unseen: int = -1) -> pd.Series:
    """マッピングで変換、未見は unseen"""
    return series.fillna("").astype(str).map(lambda x: mapping.get(x, unseen))


# オッズ系（2.3 オッズなしで除外）
ODDS_COLS = ["win_odds", "implied_prob", "popularity"]

# 追加特徴量（3.2）
EXTRA_FEATURE_COLS = [
    "grade_enc",
    "odds_popularity_gap",
    "distance_短",
    "distance_マイル",
    "distance_中",
    "distance_長",
]
EXTRA_FEATURE_COLS_NO_ODDS = [c for c in EXTRA_FEATURE_COLS if c != "odds_popularity_gap"]

# 基本特徴量（過去レースなし）
FEATURE_COLS_BASE = ODDS_COLS + [
    "frame_number",
    "horse_number",
    "weight_carried",
    "horse_weight",
    "weight_diff",
    "field_count",
    "distance",
    "surface_芝",
    "surface_ダート",
    "track_良",
    "track_稍重",
    "track_重",
    "track_不良",
    "track_不明",
    "venue_enc",
    "jockey_enc",
    "trainer_enc",
]

# 過去レース系（追加分）
PAST_FEATURE_COLS = [
    "jockey_win_rate_90d",
    "trainer_win_rate_90d",
    "career_win_rate",
    "days_since_last_race",
    "surface_win_rate_芝",
    "surface_win_rate_ダート",
    "avg_last_3f_3",
    "last_3_avg_pos",
]

FEATURE_COLS = FEATURE_COLS_BASE + PAST_FEATURE_COLS
FEATURE_COLS_EXTRA = FEATURE_COLS_BASE + EXTRA_FEATURE_COLS + PAST_FEATURE_COLS

# オッズなし（2.3）
FEATURE_COLS_BASE_NO_ODDS = [c for c in FEATURE_COLS_BASE if c not in ODDS_COLS]
FEATURE_COLS_NO_ODDS = FEATURE_COLS_BASE_NO_ODDS + PAST_FEATURE_COLS

# 時系列オッズ特徴量（analytics.odds_timeseries がある場合）
# load_data.get_race_entries_ml(use_timeseries=True) で JOIN された列を使用
TIMESERIES_FEATURE_COLS = [
    "odds_ts_change_rate",  # (直前-公開)/公開。オッズ急騰・急落
]


def _ensure_timeseries_cols(out: pd.DataFrame) -> None:
    """時系列オッズ系の列が無い場合にダミー（NaN→0）で埋める"""
    for col in TIMESERIES_FEATURE_COLS:
        if col not in out.columns:
            out[col] = 0.0
        else:
            out[col] = out[col].fillna(0.0)


def _ensure_past_cols(out: pd.DataFrame) -> None:
    """過去レース系の列が無い場合にダミーを追加"""
    past_cols = {
        "jockey_win_rate_90d": 0.0,
        "trainer_win_rate_90d": 0.0,
        "career_win_rate": 0.0,
        "days_since_last_race": 365,
        "surface_win_rate_芝": 0.0,
        "surface_win_rate_ダート": 0.0,
        "avg_last_3f_3": 37.0,
        "last_3_avg_pos": 8.0,
    }
    for col, default in past_cols.items():
        if col not in out.columns:
            out[col] = default
        else:
            out[col] = out[col].fillna(default)


def build_features(
    df: pd.DataFrame,
    encoders: dict | None = None,
    use_past: bool = True,
    use_timeseries: bool = False,
) -> tuple[pd.DataFrame, dict]:
    """
    出走馬 DataFrame から特徴量を構築。
    use_past=False なら過去レース系は使わない（ベースライン用）。
    use_timeseries=True なら時系列オッズ特徴量を含む（データがある場合）。
    """
    out = df.copy()
    if use_past:
        _ensure_past_cols(out)
    if use_timeseries:
        _ensure_timeseries_cols(out)

    out["implied_prob"] = 1.0 / out["win_odds"]
    out["popularity"] = out["popularity"].fillna(out["popularity"].max() + 1).astype(int)
    out["frame_number"] = out["frame_number"].fillna(0).astype(int)
    out["horse_number"] = out["horse_number"].fillna(0).astype(int)
    out["weight_carried"] = out["weight_carried"].fillna(56).astype(float)
    out["horse_weight"] = out["horse_weight"].fillna(450).astype(float)
    out["weight_diff"] = out["weight_diff"].fillna(0).astype(float)
    out["field_count"] = out["field_count"].fillna(16).astype(int)
    out["distance"] = out["distance"].fillna(1600).astype(int)

    out["surface_芝"] = (out["surface"].fillna("") == "芝").astype(int)
    out["surface_ダート"] = out["surface"].fillna("").isin(["ダ", "ダート"]).astype(int)

    # 3.2 追加特徴量（implied_prob は上で定義済み）
    grade_map = {"G1": 4, "G2": 3, "G3": 2, "L": 1}
    out["grade_enc"] = out.get("grade", pd.Series([""] * len(out))).fillna("").astype(str).map(lambda x: grade_map.get(x, 0))
    out["odds_rank"] = out.groupby("race_id")["implied_prob"].rank(ascending=False, method="average")
    out["odds_popularity_gap"] = out["odds_rank"] - out["popularity"]
    dist = out["distance"].fillna(1600).astype(int)
    out["distance_短"] = (dist < 1600).astype(int)
    out["distance_マイル"] = ((dist >= 1600) & (dist <= 1800)).astype(int)
    out["distance_中"] = ((dist > 1800) & (dist <= 2400)).astype(int)
    out["distance_長"] = (dist > 2400).astype(int)

    track_map = {"良": "track_良", "稍重": "track_稍重", "重": "track_重", "不良": "track_不良"}
    for k, col in track_map.items():
        out[col] = (out["track_condition"].fillna("") == k).astype(int)
    out["track_不明"] = (out["track_condition"].fillna("").isin(["", "不明"])).astype(int)

    if encoders is None:
        encoders = {}
        encoders["venue"] = _fit_encode(out["venue_code"])
        encoders["jockey"] = _fit_encode(out["jockey_id"])
        encoders["trainer"] = _fit_encode(out["trainer_id"])
    unseen = max(len(encoders["venue"]), len(encoders["jockey"]), len(encoders["trainer"]), 1) * 2
    out["venue_enc"] = _transform_encode(out["venue_code"], encoders["venue"], unseen).astype(int)
    out["jockey_enc"] = _transform_encode(out["jockey_id"], encoders["jockey"], unseen).astype(int)
    out["trainer_enc"] = _transform_encode(out["trainer_id"], encoders["trainer"], unseen).astype(int)

    return out, encoders


def get_feature_matrix(
    df: pd.DataFrame,
    encoders: dict | None = None,
    use_past: bool = True,
    use_odds: bool = True,
    use_extra: bool = False,
    use_timeseries: bool = False,
) -> tuple[pd.DataFrame, dict]:
    """特徴量行列とencodersを返す。use_extraで3.2追加、use_timeseriesで時系列オッズ特徴量を含む。"""
    built, enc = build_features(
        df, encoders, use_past=use_past, use_timeseries=use_timeseries
    )
    if use_odds:
        cols = (
            FEATURE_COLS_EXTRA if use_extra else FEATURE_COLS
        ) if use_past else (
            FEATURE_COLS_BASE + EXTRA_FEATURE_COLS if use_extra else FEATURE_COLS_BASE
        )
    else:
        base = FEATURE_COLS_BASE_NO_ODDS + (
            EXTRA_FEATURE_COLS_NO_ODDS if use_extra else []
        )
        cols = base + (PAST_FEATURE_COLS if use_past else [])
    if use_timeseries:
        cols = list(cols) + [c for c in TIMESERIES_FEATURE_COLS if c in built.columns]
    return built[cols].astype(np.float32), enc
