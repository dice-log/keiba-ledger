"""
ROI シミュレーション（1.2 / 1.3）

1.2: 各レースで「最も確率が高い」1頭に100円賭ける
1.3: 買い目を絞る - モデル確率 > 市場確率のときのみ賭ける
"""

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = ROOT / "scripts"
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(SCRIPTS))

import numpy as np
import pandas as pd

from analysis.load_races import get_race_entries
from analysis.eval_baseline import add_market_probability
from analysis.odds_correction import compute_band_factors, apply_odds_correction
from ml.load_data import get_race_entries_ml
from ml.features import get_feature_matrix
from ml.past_races import add_horse_past_stats, add_jockey_trainer_recent_stats

try:
    import lightgbm as lgb
except ImportError:
    lgb = None


def _shift_date(date_str: str, years: int) -> str:
    from datetime import datetime, timedelta
    d = datetime.strptime(date_str, "%Y-%m-%d")
    try:
        d = d.replace(year=d.year + years)
    except ValueError:
        d = d + timedelta(days=365 * years)
    return d.strftime("%Y-%m-%d")


def _race_softmax(scores, race_ids):
    probs = np.zeros_like(scores, dtype=float)
    for rid in np.unique(race_ids):
        mask = race_ids == rid
        s = scores[mask]
        e = np.exp(s - s.max())
        probs[mask] = e / e.sum()
    return probs


def add_market_prob(df: pd.DataFrame) -> pd.DataFrame:
    return add_market_probability(df)


def compute_roi(df: pd.DataFrame, prob_col: str, stake: int = 100, top_n: int = 1) -> dict:
    """
    各レースで prob_col が上位 top_n 頭の馬に stake 円ずつ賭けた場合の ROI。
    top_n=1: 本命のみ（従来通り）、top_n=2: 本命・対抗のみ（4.2）
    """
    if top_n == 1:
        idx = df.groupby("race_id")[prob_col].idxmax()
        picks = df.loc[idx]
    else:
        idx_topn = df.groupby("race_id")[prob_col].nlargest(top_n).index.get_level_values(1)
        picks = df.loc[idx_topn]
    total_stake = len(picks) * stake
    wins = picks["finish_pos"] == 1
    total_return = (picks.loc[wins, "win_odds"] * stake).sum()
    roi_pct = (total_return - total_stake) / total_stake * 100
    n_races = picks["race_id"].nunique()
    return {
        "n_races": n_races,
        "n_bets": len(picks),
        "n_wins": wins.sum(),
        "total_stake": total_stake,
        "total_return": total_return,
        "roi_pct": roi_pct,
    }


def compute_roi_filtered(
    df: pd.DataFrame,
    prob_col: str,
    market_col: str,
    stake: int = 100,
    filter_mode: str = "disagree",
    prob_threshold: float = 0.15,
) -> dict:
    """
    買い目を絞る。
    filter_mode:
      - "disagree": モデルと市場で1番人気が異なるときのみ賭ける
      - "model_confident": モデル確率が閾値以上のときのみ（4.3）
    """
    idx_model = df.groupby("race_id")[prob_col].idxmax()
    idx_market = df.groupby("race_id")[market_col].idxmax()
    picks_model = df.loc[idx_model].copy()
    picks_model["market_top_horse"] = picks_model["race_id"].apply(
        lambda r: df.loc[idx_market[r], "horse_id"] if r in idx_market.index else ""
    )
    picks_model["is_disagree"] = picks_model["horse_id"] != picks_model["market_top_horse"]
    picks_model["is_confident"] = picks_model[prob_col] >= prob_threshold

    if filter_mode == "disagree":
        picks = picks_model[picks_model["is_disagree"]]
    elif filter_mode == "model_confident":
        picks = picks_model[picks_model["is_confident"]]
    else:
        picks = picks_model

    if len(picks) == 0:
        return {"n_races": 0, "n_bets": 0, "n_wins": 0, "total_stake": 0, "total_return": 0, "roi_pct": 0.0}
    total_stake = len(picks) * stake
    wins = picks["finish_pos"] == 1
    total_return = (picks.loc[wins, "win_odds"] * stake).sum()
    roi_pct = (total_return - total_stake) / total_stake * 100
    return {
        "n_races": picks["race_id"].nunique(),
        "n_bets": len(picks),
        "n_wins": wins.sum(),
        "total_stake": total_stake,
        "total_return": total_return,
        "roi_pct": roi_pct,
    }


def main():
    parser = argparse.ArgumentParser(description="ROI シミュレーション")
    parser.add_argument("--train-from", default="2019-01-01")
    parser.add_argument("--train-to", default="2022-12-31")
    parser.add_argument("--val-from", default="2023-01-01")
    parser.add_argument("--val-to", default="2023-12-31")
    parser.add_argument("--stake", type=int, default=100)
    parser.add_argument("--no-past", action="store_true")
    parser.add_argument("--filter", action="store_true", help="買い目を絞る")
    parser.add_argument("--filter-mode", default="disagree", choices=["disagree", "model_confident"],
                        help="disagree=モデルと市場で1番が異なるとき, model_confident=モデル確率閾値以上（4.3）")
    parser.add_argument("--prob-threshold", type=float, default=0.15, help="model_confident 時の閾値")
    parser.add_argument("--top2", action="store_true", help="本命・対抗のみに賭ける（4.2）")
    args = parser.parse_args()

    top_n = 2 if args.top2 else 1
    print("検証期間:", args.val_from, "～", args.val_to)
    print("戦略: 各レースで最高確率の", top_n, "頭に", args.stake, "円ずつ", end="")
    if args.filter:
        print(f"（絞り: {args.filter_mode}" + (f", 閾値={args.prob_threshold}" if args.filter_mode == "model_confident" else "") + "）")
    else:
        print()
    print()

    # A: 市場確率（load_races は race_entries の形式）
    val_a = get_race_entries(args.val_from, args.val_to)
    val_a = add_market_prob(val_a)
    roi_a = compute_roi(val_a, "market_prob", args.stake, top_n=top_n)
    label_a = "A（市場・本命対抗）" if top_n == 2 else "A（市場確率）"
    print(f"{label_a}:")
    print(f"  賭け数: {roi_a['n_bets']}  的中: {roi_a['n_wins']}  回収率: {roi_a['roi_pct']:.2f}%")

    # A2: オッズ補正
    train_a2 = get_race_entries(args.train_from, args.train_to)
    train_a2 = add_market_probability(train_a2)
    factors = compute_band_factors(train_a2)
    val_a2 = apply_odds_correction(val_a.copy(), factors)
    roi_a2 = compute_roi(val_a2, "corrected_prob", args.stake, top_n=top_n)
    label_a2 = "A2（オッズ補正・本命対抗）" if top_n == 2 else "A2（オッズ補正）"
    print(f"{label_a2}:")
    print(f"  賭け数: {roi_a2['n_bets']}  的中: {roi_a2['n_wins']}  回収率: {roi_a2['roi_pct']:.2f}%")

    # C: モデル確率
    train_df = get_race_entries_ml(args.train_from, args.train_to)
    val_df = get_race_entries_ml(args.val_from, args.val_to)
    use_past = not args.no_past
    if use_past:
        hist_from = _shift_date(args.train_from, years=-3)
        train_df = add_horse_past_stats(train_df, hist_from, args.val_to)
        train_df = add_jockey_trainer_recent_stats(train_df, hist_from, args.val_to, 90)
        val_df = add_horse_past_stats(val_df, hist_from, args.val_to)
        val_df = add_jockey_trainer_recent_stats(val_df, hist_from, args.val_to, 90)

    X_train, enc = get_feature_matrix(train_df, use_past=use_past)
    y_train = (train_df["finish_pos"] == 1).astype(int).values
    X_val, _ = get_feature_matrix(val_df, encoders=enc, use_past=use_past)
    race_val = val_df["race_id"].values

    model = lgb.LGBMClassifier(
        n_estimators=500, max_depth=4, learning_rate=0.03, num_leaves=15,
        min_child_samples=100, reg_alpha=0.1, reg_lambda=0.1,
        random_state=42, verbosity=-1,
    )
    model.fit(X_train, y_train)
    raw = model.predict_proba(X_val)[:, 1]
    val_df = val_df.copy()
    val_df["ml_prob"] = _race_softmax(raw, race_val)

    # C 用に市場確率をマージ（val_df は race_id, horse_id で一意）
    val_ml = val_df.merge(
        val_a[["race_id", "horse_id", "market_prob"]],
        on=["race_id", "horse_id"],
        how="left",
    )
    val_ml["market_prob"] = val_ml["market_prob"].fillna(0)

    roi_c = compute_roi(val_ml, "ml_prob", args.stake, top_n=top_n)
    label_c = "C（LightGBM・本命対抗）" if top_n == 2 else "C（LightGBM）"
    print(f"{label_c}:")
    print(f"  賭け数: {roi_c['n_bets']}  的中: {roi_c['n_wins']}  回収率: {roi_c['roi_pct']:.2f}%")

    if args.filter:
        roi_filtered = compute_roi_filtered(
            val_ml, "ml_prob", "market_prob", args.stake, args.filter_mode, args.prob_threshold
        )
        print()
        filter_label = f"{args.filter_mode}" + (f"(閾値{args.prob_threshold})" if args.filter_mode == "model_confident" else "")
        print(f"C（買い目を絞る: {filter_label}）:")
        print(f"  賭け数: {roi_filtered['n_bets']}  的中: {roi_filtered['n_wins']}  回収率: {roi_filtered['roi_pct']:.2f}%")


if __name__ == "__main__":
    if lgb is None:
        print("[NG] lightgbm がインストールされていません")
        sys.exit(1)
    main()
