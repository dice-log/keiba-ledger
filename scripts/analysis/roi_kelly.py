"""
ROI シミュレーション - Kelly ベット（4.1）

各レースで最高確率の1頭に Kelly 基準で賭ける。
f* = (p * odds - 1) / (odds - 1)、負の場合は賭けない。
fractional Kelly（例: 0.5）で実行可能。
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


def compute_roi_kelly(
    df: pd.DataFrame,
    prob_col: str,
    bankroll: float = 100_000,
    frac: float = 0.5,
    top_only: bool = True,
) -> dict:
    """
    Kelly 基準で賭けた場合の ROI。
    f* = (p * odds - 1) / (odds - 1)、負なら賭けない。
    top_only=True: 各レースで最高確率の1頭のみ
    top_only=False: 期待値プラスの全馬に賭ける
    """
    if top_only:
        idx = df.groupby("race_id")[prob_col].idxmax()
        picks = df.loc[idx].copy()
    else:
        picks = df.copy()
    if "race_date" in picks.columns:
        picks = picks.sort_values(["race_date", "race_id"])
    else:
        picks = picks.sort_values("race_id")
    picks["p"] = picks[prob_col].values
    picks["b"] = picks["win_odds"].values - 1  # net odds

    picks["f_star"] = np.where(
        picks["b"] > 1e-6,
        (picks["p"] * picks["win_odds"] - 1) / picks["b"],
        0.0,
    )
    picks["f_star"] = np.clip(picks["f_star"], 0, 1) * frac
    bets = picks[picks["f_star"] > 0].copy()
    if len(bets) == 0:
        return {"n_races": len(picks), "n_bets": 0, "total_stake": 0, "total_return": 0, "roi_pct": 0.0, "final_bankroll": bankroll}

    B = bankroll
    total_stake = 0
    total_return = 0
    if "race_date" in bets.columns:
        race_order = bets.groupby("race_id")["race_date"].first().sort_values().index.tolist()
    else:
        race_order = bets["race_id"].unique().tolist()

    for race_id in race_order:
        grp = bets[bets["race_id"] == race_id]
        stakes = (grp["f_star"].values * B).round().astype(int).clip(min=0)
        race_stake = int(min(stakes.sum(), B))
        if race_stake <= 0:
            continue
        scale = race_stake / stakes.sum() if stakes.sum() > 0 else 1
        stakes = (stakes * scale).round().astype(int)
        total_stake += stakes.sum()
        B -= stakes.sum()
        race_return = 0
        for (_, row), stake in zip(grp.iterrows(), stakes):
            if stake > 0 and row["finish_pos"] == 1:
                race_return += row["win_odds"] * stake
        total_return += race_return
        B += race_return
    roi_pct = (total_return - total_stake) / total_stake * 100 if total_stake > 0 else 0
    return {
        "n_races": len(picks),
        "n_bets": len(bets),
        "total_stake": total_stake,
        "total_return": total_return,
        "roi_pct": roi_pct,
        "final_bankroll": B,
    }


def main():
    parser = argparse.ArgumentParser(description="Kelly ベット ROI シミュレーション")
    parser.add_argument("--train-from", default="2019-01-01")
    parser.add_argument("--train-to", default="2022-12-31")
    parser.add_argument("--val-from", default="2023-01-01")
    parser.add_argument("--val-to", default="2023-12-31")
    parser.add_argument("--bankroll", type=float, default=100_000)
    parser.add_argument("--frac", type=float, default=0.5, help="fractional Kelly (0.5 = half Kelly)")
    parser.add_argument("--all-positive", action="store_true", help="期待値プラスの全馬に賭ける（通常は1頭/レースのみ）")
    parser.add_argument("--no-past", action="store_true")
    args = parser.parse_args()

    print("検証期間:", args.val_from, "～", args.val_to)
    print("戦略: Kelly ベット (frac={}, bankroll={:,.0f})".format(args.frac, args.bankroll))
    print()

    val_a = get_race_entries(args.val_from, args.val_to)
    val_a = add_market_probability(val_a)

    top_only = not args.all_positive
    # A
    r_a = compute_roi_kelly(val_a, "market_prob", args.bankroll, args.frac, top_only=top_only)
    print("A（市場確率）:")
    print(f"  賭け数: {r_a['n_bets']}/{r_a['n_races']}  賭け金合計: {r_a['total_stake']:,.0f}")
    print(f"  回収: {r_a['total_return']:,.0f}  ROI: {r_a['roi_pct']:.2f}%  残資金: {r_a['final_bankroll']:,.0f}")

    # C
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

    r_c = compute_roi_kelly(val_df, "ml_prob", args.bankroll, args.frac, top_only=top_only)
    print("C（LightGBM）:")
    print(f"  賭け数: {r_c['n_bets']}/{r_c['n_races']}  賭け金合計: {r_c['total_stake']:,.0f}")
    print(f"  回収: {r_c['total_return']:,.0f}  ROI: {r_c['roi_pct']:.2f}%  残資金: {r_c['final_bankroll']:,.0f}")


if __name__ == "__main__":
    if lgb is None:
        print("[NG] lightgbm がインストールされていません")
        sys.exit(1)
    main()
