"""
時系列オッズ（O1: 単複枠, O2: 馬連）取得 → JSONL 出力

JV-Data dataspec 0B41（単複枠）, 0B42（馬連）。提供期間1年。

例:
  py -3.11-32 scripts/fetch/fetch_timeseries_o1_o2.py --from 2024-03-01 --only-o1
  py -3.11-32 scripts/fetch/fetch_timeseries_o1_o2.py --from 2024-03-01 --only-o2
  py -3.11-32 scripts/fetch/fetch_timeseries_o1_o2.py --from 2024-03-01 --limit 200
"""

import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

try:
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env", encoding="utf-8")
except Exception:
    pass

sys.path.insert(0, str(Path(__file__).resolve().parent))
from jvlink_client import fetch_stored_records

RECORD_TYPES_O1 = frozenset(["O1"])
RECORD_TYPES_O2 = frozenset(["O2"])


def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--from", dest="from_date", required=True, help="開始日 YYYY-MM-DD or YYYYMMDD")
    parser.add_argument("--only-o1", action="store_true", help="O1（単複枠）のみ")
    parser.add_argument("--only-o2", action="store_true", help="O2（馬連）のみ")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--output", type=str, default="")
    args = parser.parse_args()

    if args.only_o1 and args.only_o2:
        print("[NG] --only-o1 と --only-o2 は同時指定できません", file=sys.stderr)
        sys.exit(1)

    from_date = args.from_date.replace("-", "")
    if len(from_date) not in (8, 14):
        print("[NG] --from は YYYY-MM-DD または YYYYMMDD[hhmmss] 形式で指定してください", file=sys.stderr)
        sys.exit(1)

    (ROOT / "data").mkdir(exist_ok=True)
    if args.output:
        out_path = Path(args.output)
        if not out_path.is_absolute():
            out_path = ROOT / out_path
    else:
        from datetime import datetime
        out_path = ROOT / "data" / f"timeseries_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jsonl"

    total = 0
    t0 = time.time()
    PROGRESS_INTERVAL = 1000

    with out_path.open("w", encoding="utf-8") as f:
        if not args.only_o2:
            print("[O1 / 0B41] 時系列オッズ取得開始...", flush=True)
            for rt, raw in fetch_stored_records(
                from_date=from_date,
                dataspec="0B41",
                target_types=RECORD_TYPES_O1,
            ):
                row = {"dataspec": "0B41", "record_type": rt, "raw_text": raw}
                f.write(json.dumps(row, ensure_ascii=False) + "\n")
                total += 1
                if total % PROGRESS_INTERVAL == 0:
                    print(f"  O1: {total:,} 件 [経過 {time.time() - t0:.0f}秒]", flush=True)
                if args.limit and total >= args.limit:
                    break

        if (not args.limit or total < args.limit) and not args.only_o1:
            print("[O2 / 0B42] 時系列オッズ取得開始...", flush=True)
            for rt, raw in fetch_stored_records(
                from_date=from_date,
                dataspec="0B42",
                target_types=RECORD_TYPES_O2,
            ):
                row = {"dataspec": "0B42", "record_type": rt, "raw_text": raw}
                f.write(json.dumps(row, ensure_ascii=False) + "\n")
                total += 1
                if total % PROGRESS_INTERVAL == 0:
                    print(f"  O2: {total:,} 件 [経過 {time.time() - t0:.0f}秒]", flush=True)
                if args.limit and total >= args.limit:
                    break

    elapsed = time.time() - t0
    print(f"[OK] {total:,} records -> {out_path}  [経過 {elapsed:.0f}秒]")


if __name__ == "__main__":
    main()
