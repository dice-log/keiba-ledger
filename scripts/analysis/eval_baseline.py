"""
単勝予測 ベースライン評価（A / B / Bj / Bt）

A: オッズを控除補正して市場確率として評価
B: 騎手×馬場で補正した確率で評価
Bj: 騎手のみで補正した確率で評価
Bt: 調教師のみで補正した確率で評価
"""

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = ROOT / "scripts"
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(SCRIPTS))

from analysis.load_races import get_race_entries
from analysis.probability import odds_to_market_probability
from analysis.log_score import mean_log_score
from analysis.correction_b import (
    compute_jockey_factors,
    apply_jockey_correction,
    compute_trainer_factors,
    apply_trainer_correction,
    compute_jockey_track_factors,
    apply_jockey_track_correction,
)

import pandas as pd


def add_market_probability(df: pd.DataFrame) -> pd.DataFrame:
    """
    DataFrame に market_prob 列を追加。
    レースごとに odds_to_market_probability を適用。
    """
    out = df.copy()

    def _norm(odds):
        inv = 1.0 / odds
        return inv / inv.sum()

    out["market_prob"] = out.groupby("race_id")["win_odds"].transform(_norm)
    return out


def main():
    parser = argparse.ArgumentParser(description="単勝ベースライン評価（ロジスコア）")
    parser.add_argument("--mode", choices=["a", "b", "bj", "bt"], default="a", help="A=市場 / B=騎手×馬場 / Bj=騎手のみ / Bt=調教師のみ")
    parser.add_argument("--train-from", default="2019-01-01", help="学習開始日 YYYY-MM-DD")
    parser.add_argument("--train-to", default="2022-12-31", help="学習終了日 YYYY-MM-DD")
    parser.add_argument("--val-from", default="2023-01-01", help="検証開始日 YYYY-MM-DD")
    parser.add_argument("--val-to", default="2023-12-31", help="検証終了日 YYYY-MM-DD")
    parser.add_argument("--min-starts", type=int, default=30, help="B: 補正に使う最小出走数")
    args = parser.parse_args()

    mode_labels = {"a": "A（市場）", "b": "B（騎手×馬場）", "bj": "Bj（騎手のみ）", "bt": "Bt（調教師のみ）"}
    print("検証期間:", args.val_from, "～", args.val_to)
    print("モード:", mode_labels[args.mode])

    val_df = get_race_entries(args.val_from, args.val_to)
    if val_df.empty:
        print("[NG] 検証期間にデータがありません")
        sys.exit(1)

    val_df = add_market_probability(val_df)

    if args.mode == "a":
        prob_col = "market_prob"
    else:
        train_df = get_race_entries(args.train_from, args.train_to)
        if train_df.empty:
            print("[NG] 学習期間にデータがありません")
            sys.exit(1)
        if args.mode == "bj":
            factors = compute_jockey_factors(train_df, min_starts=args.min_starts)
            val_df = apply_jockey_correction(val_df, factors)
        elif args.mode == "bt":
            factors = compute_trainer_factors(train_df, min_starts=args.min_starts)
            val_df = apply_trainer_correction(val_df, factors)
        else:
            factors = compute_jockey_track_factors(train_df, min_starts=args.min_starts)
            val_df = apply_jockey_track_correction(val_df, factors)
        prob_col = "corrected_prob"

    ls = mean_log_score(val_df, prob_col=prob_col)
    n_races = val_df["race_id"].nunique()
    n_entries = len(val_df)

    print(f"レース数: {n_races:,}")
    print(f"出走頭数: {n_entries:,}")
    label = mode_labels[args.mode].replace("（", "=").replace("）", "")
    print(f"平均ロジスコア（{label}）: {ls:.4f}")


if __name__ == "__main__":
    main()
