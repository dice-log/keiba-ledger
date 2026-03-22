"""
C: LightGBM 単勝予測

学習 → 検証期間で予測 → ロジスコア評価。
A ベースラインとの比較用。
"""

import argparse
import pickle
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = ROOT / "scripts"
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(SCRIPTS))

import numpy as np
import pandas as pd

from ml.load_data import get_race_entries_ml
from ml.features import get_feature_matrix
from ml.past_races import add_horse_past_stats, add_jockey_trainer_recent_stats
from analysis.log_score import mean_log_score

try:
    import lightgbm as lgb
except ImportError:
    print("[NG] lightgbm がインストールされていません: pip install lightgbm")
    sys.exit(1)


def _shift_date(date_str: str, years: int) -> str:
    """YYYY-MM-DD に years を加算"""
    from datetime import datetime, timedelta
    d = datetime.strptime(date_str, "%Y-%m-%d")
    try:
        d = d.replace(year=d.year + years)
    except ValueError:
        d = d + timedelta(days=365 * years)
    return d.strftime("%Y-%m-%d")


def race_softmax(scores: np.ndarray, race_ids: np.ndarray) -> np.ndarray:
    """レースごとに softmax して確率に正規化"""
    probs = np.zeros_like(scores, dtype=np.float64)
    for rid in np.unique(race_ids):
        mask = race_ids == rid
        s = scores[mask]
        e = np.exp(s - s.max())
        probs[mask] = e / e.sum()
    return probs


def main():
    parser = argparse.ArgumentParser(description="C: LightGBM 単勝予測")
    parser.add_argument("--train-from", default="2019-01-01", help="学習開始日")
    parser.add_argument("--train-to", default="2022-12-31", help="学習終了日")
    parser.add_argument("--val-from", default="2023-01-01", help="検証開始日")
    parser.add_argument("--val-to", default="2023-12-31", help="検証終了日")
    parser.add_argument("--model-out", default=None, help="モデル保存パス")
    parser.add_argument("--save-encoders", action="store_true", help="encoders も保存")
    parser.add_argument("--no-past", action="store_true", help="過去レース特徴量を使わない（ベースライン確認用）")
    parser.add_argument("--no-odds", action="store_true", help="オッズ系特徴量を使わない（2.3）")
    parser.add_argument("--extra-features", action="store_true", help="3.2 追加特徴量（grade, odds_popularity_gap, distance_cat）")
    args = parser.parse_args()

    use_past = not args.no_past
    use_odds = not args.no_odds
    use_extra = args.extra_features
    print("学習期間:", args.train_from, "～", args.train_to)
    print("検証期間:", args.val_from, "～", args.val_to)
    print("過去レース:", "あり" if use_past else "なし（ベースライン）")
    print("オッズ系:", "あり" if use_odds else "なし（2.3）")
    print("追加特徴量:", "あり（3.2）" if use_extra else "なし")

    train_df = get_race_entries_ml(args.train_from, args.train_to)
    val_df = get_race_entries_ml(args.val_from, args.val_to)
    if train_df.empty or val_df.empty:
        print("[NG] データが不足しています")
        sys.exit(1)

    if use_past:
        hist_from = _shift_date(args.train_from, years=-3)
        hist_to = args.val_to
        print("過去レース付与中...")
        train_df = add_horse_past_stats(train_df, hist_from, hist_to)
        train_df = add_jockey_trainer_recent_stats(train_df, hist_from, hist_to, days=90)
        val_df = add_horse_past_stats(val_df, hist_from, hist_to)
        val_df = add_jockey_trainer_recent_stats(val_df, hist_from, hist_to, days=90)

    X_train, encoders = get_feature_matrix(train_df, encoders=None, use_past=use_past, use_odds=use_odds, use_extra=use_extra)
    y_train = (train_df["finish_pos"] == 1).astype(int).values

    X_val, _ = get_feature_matrix(val_df, encoders=encoders, use_past=use_past, use_odds=use_odds, use_extra=use_extra)
    y_val = (val_df["finish_pos"] == 1).astype(int).values
    race_val = val_df["race_id"].values

    model = lgb.LGBMClassifier(
        n_estimators=500,
        max_depth=4,
        learning_rate=0.03,
        num_leaves=15,
        min_child_samples=100,
        subsample=0.8,
        colsample_bytree=0.8,
        reg_alpha=0.1,
        reg_lambda=0.1,
        random_state=42,
        verbosity=-1,
    )
    model.fit(
        X_train,
        y_train,
        eval_set=[(X_val, y_val)],
        callbacks=[lgb.early_stopping(50, verbose=False)],
    )

    raw_scores = model.predict_proba(X_val)[:, 1]
    probs = race_softmax(raw_scores, race_val)

    val_df = val_df.copy()
    val_df["ml_prob"] = probs
    ls = mean_log_score(val_df, prob_col="ml_prob")
    n_races = val_df["race_id"].nunique()
    n_entries = len(val_df)

    print(f"レース数: {n_races:,}")
    print(f"出走頭数: {n_entries:,}")
    suffix = "（過去レースなし）" if not use_past else ""
    suffix += "（オッズなし）" if not use_odds else ""
    suffix += "（特徴量追加）" if use_extra else ""
    print(f"平均ロジスコア（C=LightGBM{suffix}）: {ls:.4f}")

    if args.model_out:
        out_path = Path(args.model_out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        model.booster_.save_model(str(out_path))
        if args.save_encoders:
            enc_path = out_path.with_suffix(".encoders.pkl")
            with open(enc_path, "wb") as f:
                pickle.dump(encoders, f)
        print(f"モデル保存: {out_path}")


if __name__ == "__main__":
    main()
