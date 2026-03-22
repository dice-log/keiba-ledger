"""
favorite-longshot bias 確認（2.1）

人気帯（またはオッズ帯）ごとに、市場の implied prob と実際の勝率を比較。
- 本命側で 実際 >  implied → 市場が過小評価（割安）
- 穴馬側で 実際 < implied → 市場が過大評価（割高）
"""

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = ROOT / "scripts"
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(SCRIPTS))

import pandas as pd

from analysis.load_races import get_race_entries
from analysis.eval_baseline import add_market_probability


def main():
    parser = argparse.ArgumentParser(description="favorite-longshot bias 確認")
    parser.add_argument("--val-from", default="2022-01-01")
    parser.add_argument("--val-to", default="2024-12-31")
    args = parser.parse_args()

    df = get_race_entries(args.val_from, args.val_to)
    df = add_market_probability(df)
    df["won"] = (df["finish_pos"] == 1).astype(int)

    print(f"対象期間: {args.val_from} ～ {args.val_to}")
    print(f"出走数: {len(df):,}")
    print()
    print("人気帯ごとの 実際の勝率 vs 市場確率（控除補正済み）")
    print("(実際 > 市場 → 割安, 実際 < 市場 → 割高)")
    print()

    def popularity_band(p):
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

    df["band"] = df["popularity"].fillna(99).astype(int).apply(popularity_band)

    grp = df.groupby("band").agg(
        n=("won", "count"),
        wins=("won", "sum"),
        market_avg=("market_prob", "mean"),
        avg_odds=("win_odds", "mean"),
    )
    grp["actual_rate"] = grp["wins"] / grp["n"]
    grp["diff"] = grp["actual_rate"] - grp["market_avg"]
    grp["roi_if_bet"] = (grp["actual_rate"] * grp["avg_odds"] - 1) * 100

    order = ["1-2番人気", "3-4番人気", "5-6番人気", "7-10番人気", "11番人気〜"]
    grp = grp.reindex([b for b in order if b in grp.index])

    for band in grp.index:
        r = grp.loc[band]
        label = "割安" if r["diff"] > 0 else "割高"
        print(f"  {band}:")
        print(f"    出走: {int(r['n']):,}  1着: {int(r['wins']):,}")
        print(f"    実際の勝率: {r['actual_rate']:.2%}  市場確率平均: {r['market_avg']:.2%}  → {label} (差: {r['diff']:+.2%})  ROI: {r['roi_if_bet']:+.1f}%")
        print()

    print("オッズ帯ごと（参考）:")
    df["odds_band"] = pd.cut(
        df["win_odds"],
        bins=[0, 3, 5, 10, 20, 1000],
        labels=["~3倍", "3-5倍", "5-10倍", "10-20倍", "20倍~"],
    )
    grp2 = df.groupby("odds_band", observed=True).agg(
        n=("won", "count"),
        wins=("won", "sum"),
        market_avg=("market_prob", "mean"),
    )
    grp2["actual_rate"] = grp2["wins"] / grp2["n"]
    grp2["diff"] = grp2["actual_rate"] - grp2["market_avg"]
    for band in grp2.index:
        r = grp2.loc[band]
        label = "割安" if r["diff"] > 0 else "割高"
        print(f"  {band}: 実際{r['actual_rate']:.2%} vs 市場{r['market_avg']:.2%} → {label}")


if __name__ == "__main__":
    main()
