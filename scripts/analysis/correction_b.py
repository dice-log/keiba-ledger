"""
B: 騎手 / 調教師 / 騎手×馬場 補正

学習期間で騎手・調教師（または騎手×馬場）ごとの勝率を集計し、補正係数を算出。
市場確率に補正係数を掛けてから正規化する。
"""

import pandas as pd


def compute_trainer_factors(
    train_df: pd.DataFrame,
    min_starts: int = 30,
) -> dict:
    """
    学習期間のデータから調教師のみの補正係数を算出。

    補正係数 = (調教師の勝率) / (全体平均勝率)
    サンプル数が min_starts 未満の調教師は 1.0（補正なし）。

    Returns:
        trainer_id -> factor の辞書
    """
    if train_df.empty:
        return {}

    train_df = train_df.copy()
    train_df["trainer_id"] = train_df["trainer_id"].fillna("").astype(str)

    total_starts = len(train_df)
    total_wins = (train_df["finish_pos"] == 1).sum()
    overall_rate = total_wins / total_starts if total_starts > 0 else 0

    if overall_rate <= 0:
        return {}

    grp = train_df.groupby("trainer_id").agg(
        starts=("finish_pos", "count"),
        wins=("finish_pos", lambda x: (x == 1).sum()),
    )
    grp["rate"] = grp["wins"] / grp["starts"]
    grp["factor"] = grp["rate"] / overall_rate
    grp.loc[grp["starts"] < min_starts, "factor"] = 1.0

    return grp["factor"].to_dict()


def compute_jockey_factors(
    train_df: pd.DataFrame,
    min_starts: int = 30,
) -> dict:
    """
    学習期間のデータから騎手のみの補正係数を算出。

    補正係数 = (騎手の勝率) / (全体平均勝率)
    サンプル数が min_starts 未満の騎手は 1.0（補正なし）。

    Returns:
        jockey_id -> factor の辞書
    """
    if train_df.empty:
        return {}

    train_df = train_df.copy()
    train_df["jockey_id"] = train_df["jockey_id"].fillna("").astype(str)

    total_starts = len(train_df)
    total_wins = (train_df["finish_pos"] == 1).sum()
    overall_rate = total_wins / total_starts if total_starts > 0 else 0

    if overall_rate <= 0:
        return {}

    grp = train_df.groupby("jockey_id").agg(
        starts=("finish_pos", "count"),
        wins=("finish_pos", lambda x: (x == 1).sum()),
    )
    grp["rate"] = grp["wins"] / grp["starts"]
    grp["factor"] = grp["rate"] / overall_rate
    grp.loc[grp["starts"] < min_starts, "factor"] = 1.0

    return grp["factor"].to_dict()


def compute_jockey_track_factors(
    train_df: pd.DataFrame,
    min_starts: int = 30,
) -> dict:
    """
    学習期間のデータから騎手×馬場の補正係数を算出。

    補正係数 = (騎手×馬場の勝率) / (全体平均勝率)
    サンプル数が min_starts 未満の組み合わせは 1.0（補正なし）。

    Returns:
        (jockey_id, track_condition) -> factor の辞書
    """
    if train_df.empty:
        return {}

    train_df = train_df.copy()
    train_df["track_condition"] = train_df["track_condition"].fillna("不明").astype(str)
    train_df["jockey_id"] = train_df["jockey_id"].fillna("").astype(str)

    total_starts = len(train_df)
    total_wins = (train_df["finish_pos"] == 1).sum()
    overall_rate = total_wins / total_starts if total_starts > 0 else 0

    if overall_rate <= 0:
        return {}

    grp = train_df.groupby(["jockey_id", "track_condition"]).agg(
        starts=("finish_pos", "count"),
        wins=("finish_pos", lambda x: (x == 1).sum()),
    )
    grp["rate"] = grp["wins"] / grp["starts"]
    grp["factor"] = grp["rate"] / overall_rate
    grp.loc[grp["starts"] < min_starts, "factor"] = 1.0

    return grp["factor"].to_dict()


def apply_jockey_correction(
    df: pd.DataFrame,
    factors: dict,
    market_prob_col: str = "market_prob",
    output_col: str = "corrected_prob",
) -> pd.DataFrame:
    """
    市場確率に騎手の補正係数を掛け、レース内で正規化する。
    """
    out = df.copy()
    out["jockey_id"] = out["jockey_id"].fillna("").astype(str)

    out["_factor"] = out["jockey_id"].map(lambda x: factors.get(str(x), 1.0))
    out[output_col] = out[market_prob_col] * out["_factor"]
    out.drop(columns=["_factor"], inplace=True)

    def _renorm(s):
        total = s.sum()
        if total <= 0:
            return s
        return s / total

    out[output_col] = out.groupby("race_id")[output_col].transform(_renorm)
    return out


def apply_trainer_correction(
    df: pd.DataFrame,
    factors: dict,
    market_prob_col: str = "market_prob",
    output_col: str = "corrected_prob",
) -> pd.DataFrame:
    """
    市場確率に調教師の補正係数を掛け、レース内で正規化する。
    """
    out = df.copy()
    out["trainer_id"] = out["trainer_id"].fillna("").astype(str)

    out["_factor"] = out["trainer_id"].map(lambda x: factors.get(str(x), 1.0))
    out[output_col] = out[market_prob_col] * out["_factor"]
    out.drop(columns=["_factor"], inplace=True)

    def _renorm(s):
        total = s.sum()
        if total <= 0:
            return s
        return s / total

    out[output_col] = out.groupby("race_id")[output_col].transform(_renorm)
    return out


def apply_jockey_track_correction(
    df: pd.DataFrame,
    factors: dict[tuple[str, str], float],
    market_prob_col: str = "market_prob",
    output_col: str = "corrected_prob",
) -> pd.DataFrame:
    """
    市場確率に騎手×馬場の補正係数を掛け、レース内で正規化する。
    """
    out = df.copy()
    out["track_condition"] = out["track_condition"].fillna("不明").astype(str)
    out["jockey_id"] = out["jockey_id"].fillna("").astype(str)

    def _get_factor(row):
        key = (str(row["jockey_id"]), str(row["track_condition"]))
        return factors.get(key, 1.0)

    out["_factor"] = out.apply(_get_factor, axis=1)
    out[output_col] = out[market_prob_col] * out["_factor"]
    out.drop(columns=["_factor"], inplace=True)

    def _renorm(s):
        total = s.sum()
        if total <= 0:
            return s
        return s / total

    out[output_col] = out.groupby("race_id")[output_col].transform(_renorm)
    return out
