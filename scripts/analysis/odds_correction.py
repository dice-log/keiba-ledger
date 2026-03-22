"""
オッズ補正モデル（2.2）

favorite-longshot bias に基づき、人気帯ごとに補正係数を学習。
market_prob * factor[band] で補正し、レース内で正規化。
"""

import pandas as pd


BAND_ORDER = ["1-2番人気", "3-4番人気", "5-6番人気", "7-10番人気", "11番人気〜"]


def _popularity_band(p) -> str:
    p = int(p) if pd.notna(p) else 99
    if p <= 2:
        return "1-2番人気"
    elif p <= 4:
        return "3-4番人気"
    elif p <= 6:
        return "5-6番人気"
    elif p <= 10:
        return "7-10番人気"
    else:
        return "11番人気〜"


def compute_band_factors(train_df: pd.DataFrame, market_col: str = "market_prob") -> dict[str, float]:
    """
    学習期間のデータから人気帯ごとの補正係数を算出。

    補正係数 = 実際の勝率 / 市場確率平均
    （割安帯は > 1、割高帯は < 1）
    """
    df = train_df.copy()
    df["band"] = df["popularity"].fillna(99).astype(int).apply(_popularity_band)
    df["won"] = (df["finish_pos"] == 1).astype(int)

    grp = df.groupby("band").agg(wins=("won", "sum"), n=("won", "count"), market_avg=(market_col, "mean"))
    grp["actual_rate"] = grp["wins"] / grp["n"]
    grp["factor"] = grp["actual_rate"] / grp["market_avg"].clip(lower=1e-6)

    factors = {b: grp.loc[b, "factor"] for b in BAND_ORDER if b in grp.index}
    for b in BAND_ORDER:
        if b not in factors:
            factors[b] = 1.0
    return factors


def apply_odds_correction(
    df: pd.DataFrame,
    factors: dict[str, float],
    market_col: str = "market_prob",
    out_col: str = "corrected_prob",
) -> pd.DataFrame:
    """
    市場確率に補正係数を掛け、レース内で正規化する。
    """
    out = df.copy()
    out["_band"] = out["popularity"].fillna(99).astype(int).apply(_popularity_band)
    out["_factor"] = out["_band"].map(lambda b: factors.get(b, 1.0))
    out[out_col] = out[market_col] * out["_factor"]
    out[out_col] = out.groupby("race_id")[out_col].transform(lambda x: x / x.sum())
    out = out.drop(columns=["_band", "_factor"])
    return out
