"""
時系列オッズ（0B41/O1, 0B42/O2）過去分一括取得

JV-Data 0B41（単複枠）, 0B42（馬連）は提供期間1年のため、年度ごとに実行する。
取得 → JSONL 出力 → load_timeseries_to_db.py で raw 投入 → normalize_odds_timeseries.py で analytics 投入。

実行例:
  py -3.11-32 scripts/fetch/fetch_timeseries_all.py --from 2023-01-01
  py -3.11-32 scripts/fetch/fetch_timeseries_all.py --from 2023-01-01 --to 2024-12-31
  py -3.11-32 scripts/fetch/fetch_timeseries_all.py --only-o1 --from 2024-01-01
"""

import argparse
import subprocess
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
FETCH_DIR = Path(__file__).resolve().parent


def parse_date(s: str) -> datetime:
    return datetime.strptime(s.replace("-", "").strip()[:8], "%Y%m%d")


def main():
    parser = argparse.ArgumentParser(
        description="時系列オッズ（O1/O2）過去分を年度ごとに取得",
    )
    parser.add_argument(
        "--from",
        dest="from_date",
        required=True,
        help="開始日 YYYY-MM-DD",
    )
    parser.add_argument(
        "--to",
        dest="to_date",
        default="",
        help="終了日 YYYY-MM-DD（未指定時は from から1年分のみ）",
    )
    parser.add_argument(
        "--only-o1",
        action="store_true",
        help="O1（単複枠・0B41）のみ取得",
    )
    parser.add_argument(
        "--only-o2",
        action="store_true",
        help="O2（馬連・0B42）のみ取得",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="取得件数制限（0=無制限・テスト用）",
    )
    parser.add_argument(
        "--output-dir",
        default="",
        help="出力ディレクトリ（未指定時は data/）",
    )
    args = parser.parse_args()

    if args.only_o1 and args.only_o2:
        print("[NG] --only-o1 と --only-o2 は同時指定できません", file=sys.stderr)
        sys.exit(1)

    out_dir = Path(args.output_dir) if args.output_dir else ROOT / "data"
    out_dir.mkdir(parents=True, exist_ok=True)

    start = parse_date(args.from_date)
    if args.to_date:
        end = parse_date(args.to_date)
        if end <= start:
            print("[NG] --to は --from より後の日付を指定してください", file=sys.stderr)
            sys.exit(1)
    else:
        # 1年分のみ（JV-Data 提供期間1年）
        from datetime import timedelta

        end = start + timedelta(days=365)

    extra = []
    if args.only_o1:
        extra.append("--only-o1")
    if args.only_o2:
        extra.append("--only-o2")
    if args.limit:
        extra.extend(["--limit", str(args.limit)])

    # 年度をまたぐ場合は年度ごとに実行（JV-Data は from から約1年分を返す）
    current = start
    total_files = 0
    while current < end:
        from_str = current.strftime("%Y-%m-%d")
        out_name = f"timeseries_{current.strftime('%Y%m%d')}.jsonl"
        out_path = out_dir / out_name

        cmd = [
            "py",
            "-3.11-32",
            str(FETCH_DIR / "fetch_timeseries_o1_o2.py"),
            "--from",
            from_str,
            "--output",
            str(out_path),
        ] + extra

        print(f"\n[実行] {from_str} から取得 -> {out_path}")
        try:
            subprocess.run(cmd, check=True, cwd=str(ROOT))
        except subprocess.CalledProcessError as e:
            print(f"[NG] 取得失敗: {e}", file=sys.stderr)
            sys.exit(1)
        except FileNotFoundError:
            print(
                "[NG] py -3.11-32 が見つかりません。JV-Link は 32bit Python が必要です。",
                file=sys.stderr,
            )
            sys.exit(1)

        total_files += 1
        # 次の年度の1月1日へ（JV-Data 提供期間は約1年）
        next_year = datetime(current.year + 1, 1, 1)
        if next_year >= end:
            break
        current = next_year

    print(f"\n[OK] {total_files} ファイル作成完了")
    print("次のステップ:")
    print("  1. python scripts/fetch/load_timeseries_to_db.py [作成したJSONL]")
    print("  2. python scripts/transform/normalize_odds_timeseries.py")


if __name__ == "__main__":
    main()
