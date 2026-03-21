"""
ロジスコア計算

勝った馬に付けた確率 p に対して -log(p) を返す。
"""

import math

import pandas as pd


def log_score(probabilities: pd.Series, winner_index: int) -> float:
    """
    1レースのロジスコアを計算。

    Args:
        probabilities: 各馬の確率 Series（インデックスは馬の位置）
        winner_index: 勝ち馬のインデックス（0始まり）

    Returns:
        -log(p_winner)。p=0 の場合は inf を避けるため大きな値（例: 50）を返す。
    """
    if winner_index < 0 or winner_index >= len(probabilities):
        return float("inf")
    p = probabilities.iloc[winner_index]
    if p <= 0:
        return 50.0  # 実質的な上限
    return -math.log(p)


def mean_log_score(races_df: pd.DataFrame, prob_col: str = "market_prob") -> float:
    """
    複数レースの平均ロジスコアを計算。

    Args:
        races_df: レースごとにグループされた DataFrame。
                  race_id でグループ可能であること。
                  prob_col: 確率の列名。
                  各レースに finish_pos=1 の馬が1頭あること。

    Returns:
        平均ロジスコア
    """
    scores = []
    for race_id, grp in races_df.groupby("race_id"):
        grp = grp.sort_values("horse_number").reset_index(drop=True)
        winners = grp[grp["finish_pos"] == 1]
        if len(winners) == 0:
            continue
        winner_idx = winners.index[0]
        probs = grp[prob_col]
        if prob_col not in grp.columns:
            continue
        ls = log_score(probs, winner_idx)
        scores.append(ls)
    if not scores:
        return float("nan")
    return sum(scores) / len(scores)
