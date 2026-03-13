"""
初回全データ取得 — 設計書 Phase 1 Step 4

32bit/64bit 分離構成:
  1. py -3.11-32 fetch_to_file.py  … JV-Link取得 → ファイル保存（psycopg2不要）
  2. python load_to_db.py          … ファイル → DB投入（64bit・psycopg2必要）

実行方法:
  python scripts/fetch/initial_fetch.py
  python scripts/fetch/initial_fetch.py --ra-se-hr-only  # races/entries/payouts を全範囲取得
  python scripts/fetch/initial_fetch.py --from 2015-01-01
  python scripts/fetch/initial_fetch.py --from 2023-01-01 --no-odds
  python scripts/fetch/initial_fetch.py --diff-only   # UM/KS/CH マスタのみ取得
"""

import argparse
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
FETCH_DIR = Path(__file__).resolve().parent


def main():
    parser = argparse.ArgumentParser(description="Keiba Ledger 初回データ取得")
    parser.add_argument("--from", dest="from_date", default=None, help="開始日 YYYY-MM-DD（--ra-se-hr-only時は未指定で1986-01-01）")
    parser.add_argument("--ra-se-hr-only", action="store_true", help="RA/SE/HR のみ全範囲取得（races/entries/payouts用・取得可能な最早日から）")
    parser.add_argument("--no-odds", action="store_true", help="オッズレコードを除外")
    parser.add_argument("--limit", type=int, default=0, help="取得件数制限（0=無制限）")
    parser.add_argument("--skip-load", action="store_true", help="取得のみ実施、DB投入はスキップ")
    parser.add_argument("--diff-only", action="store_true", help="DIFF（UM/KS/CH マスタ）のみ取得")
    parser.add_argument("--um-ks-only", action="store_true", help="UM/KS のみ取得")
    parser.add_argument("--setup", action="store_true", help="DIFN をセットアップ（option=3）で取得")
    args = parser.parse_args()

    from_date = args.from_date
    if from_date is None:
        from_date = "1986-01-01" if args.ra_se_hr_only else "2010-01-01"

    (ROOT / "data").mkdir(exist_ok=True)
    out_file = ROOT / "data" / "fetch_latest.jsonl"

    # Step 1: 32bit で JV-Link 取得 → ファイル
    cmd_fetch = [
        "py", "-3.11-32",
        str(FETCH_DIR / "fetch_to_file.py"),
        "--from", from_date,
        "--output", str(out_file),
    ]
    if args.ra_se_hr_only:
        cmd_fetch.append("--ra-se-hr-only")
    elif args.no_odds:
        cmd_fetch.append("--no-odds")
    if args.limit:
        cmd_fetch.extend(["--limit", str(args.limit)])
    if args.diff_only:
        cmd_fetch.append("--diff-only")
    if args.um_ks_only:
        cmd_fetch.append("--um-ks-only")
    if args.setup:
        cmd_fetch.append("--setup")

    if args.ra_se_hr_only:
        print(f"[1/2] JV-Link 取得 (32bit) - RA/SE/HR 全範囲 [{from_date}〜]...", flush=True)
    else:
        print("[1/2] JV-Link 取得 (32bit)...", flush=True)
    r1 = subprocess.run(cmd_fetch, cwd=str(ROOT))
    if r1.returncode != 0:
        print("[NG] fetch_to_file 失敗")
        sys.exit(1)

    if args.skip_load:
        print(f"[OK] 取得完了。DB投入はスキップしました: {out_file}")
        return

    # Step 2: 64bit でファイル → DB
    print("[2/2] DB 投入...", flush=True)
    r2 = subprocess.run(
        [sys.executable, str(FETCH_DIR / "load_to_db.py"), str(out_file)],
        cwd=str(ROOT),
    )
    if r2.returncode != 0:
        print("[NG] load_to_db 失敗")
        sys.exit(1)

    print("[OK] 初回取得完了")


if __name__ == "__main__":
    main()
