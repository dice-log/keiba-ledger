"""
過去レース特徴量のチェック

1. 特徴量の統計（min/max/mean/std）
2. feature importance
3. オッズなしで過去レースありのロジスコア
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = ROOT / "scripts"
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(SCRIPTS))

from ml.load_data import get_race_entries_ml
from ml.features import (
    get_feature_matrix,
    FEATURE_COLS_BASE,
    PAST_FEATURE_COLS,
    build_features,
)
from ml.past_races import add_horse_past_stats, add_jockey_trainer_recent_stats
from analysis.log_score import mean_log_score
import lightgbm as lgb
import numpy as np


def _shift_date(date_str: str, years: int) -> str:
    from datetime import datetime, timedelta
    d = datetime.strptime(date_str, "%Y-%m-%d")
    try:
        d = d.replace(year=d.year + years)
    except ValueError:
        d = d + timedelta(days=365 * years)
    return d.strftime("%Y-%m-%d")


def race_softmax(scores, race_ids):
    probs = np.zeros_like(scores, dtype=np.float64)
    for rid in np.unique(race_ids):
        mask = race_ids == rid
        s = scores[mask]
        e = np.exp(s - s.max())
        probs[mask] = e / e.sum()
    return probs


def main():
    print("=" * 60)
    print("チェック1: 過去レース特徴量の統計")
    print("=" * 60)

    val_df = get_race_entries_ml("2023-01-01", "2023-12-31")
    hist_from = _shift_date("2019-01-01", years=-3)
    val_df = add_horse_past_stats(val_df, hist_from, "2023-12-31")
    val_df = add_jockey_trainer_recent_stats(val_df, hist_from, "2023-12-31", days=90)

    for col in PAST_FEATURE_COLS:
        s = val_df[col]
        print(f"  {col}:")
        print(f"    min={s.min():.4f}, max={s.max():.4f}, mean={s.mean():.4f}, std={s.std():.4f}")
        print(f"    zero_pct={(s == 0).mean()*100:.1f}%, unique={s.nunique()}")

    print()
    print("=" * 60)
    print("チェック2: feature importance")
    print("=" * 60)

    train_df = get_race_entries_ml("2019-01-01", "2022-12-31")
    train_df = add_horse_past_stats(train_df, hist_from, "2023-12-31")
    train_df = add_jockey_trainer_recent_stats(train_df, hist_from, "2023-12-31", days=90)
    val_df = get_race_entries_ml("2023-01-01", "2023-12-31")
    val_df = add_horse_past_stats(val_df, hist_from, "2023-12-31")
    val_df = add_jockey_trainer_recent_stats(val_df, hist_from, "2023-12-31", days=90)

    X_train, enc = get_feature_matrix(train_df, use_past=True)
    y_train = (train_df["finish_pos"] == 1).astype(int).values
    X_val, _ = get_feature_matrix(val_df, encoders=enc, use_past=True)
    y_val = (val_df["finish_pos"] == 1).astype(int).values
    race_val = val_df["race_id"].values

    model = lgb.LGBMClassifier(
        n_estimators=500, max_depth=4, learning_rate=0.03, num_leaves=15,
        min_child_samples=100, reg_alpha=0.1, reg_lambda=0.1,
        random_state=42, verbosity=-1,
    )
    model.fit(X_train, y_train, eval_set=[(X_val, y_val)], callbacks=[lgb.early_stopping(50, verbose=False)])

    cols = list(X_train.columns)
    imp = model.feature_importances_
    for name, val in sorted(zip(cols, imp), key=lambda x: -x[1]):
        past = " [過去]" if name in PAST_FEATURE_COLS else ""
        print(f"  {name}: {val}{past}")

    print()
    print("=" * 60)
    print("チェック3: オッズなしで過去レースあり")
    print("=" * 60)

    cols_no_odds = [c for c in cols if c not in ("win_odds", "implied_prob")]
    X_train_no = X_train[cols_no_odds]
    X_val_no = X_val[cols_no_odds]

    model2 = lgb.LGBMClassifier(
        n_estimators=500, max_depth=4, learning_rate=0.03, num_leaves=15,
        min_child_samples=100, reg_alpha=0.1, reg_lambda=0.1,
        random_state=42, verbosity=-1,
    )
    model2.fit(X_train_no, y_train, eval_set=[(X_val_no, y_val)], callbacks=[lgb.early_stopping(50, verbose=False)])

    probs = race_softmax(model2.predict_proba(X_val_no)[:, 1], race_val)
    val_df = val_df.copy()
    val_df["ml_prob"] = probs
    ls = mean_log_score(val_df, prob_col="ml_prob")
    print(f"  ロジスコア（オッズなし・過去レースあり）: {ls:.4f}")
    print(f"  参考: A（市場）: 1.9274")


if __name__ == "__main__":
    main()
