"""
オッズ → 確率変換（控除補正）

同一レース内で 1/odds を合計し、その合計で割って正規化（合計=1）。
"""

import pandas as pd


def odds_to_market_probability(odds_series: pd.Series) -> pd.Series:
    """
    オッズ Series を控除補正した市場確率に変換。

    Args:
        odds_series: 1レース分の単勝オッズ（正の値）

    Returns:
        対応する確率（合計=1）
    """
    inv = 1.0 / odds_series
    total = inv.sum()
    if total <= 0:
        return pd.Series([0.0] * len(odds_series), index=odds_series.index)
    return inv / total
