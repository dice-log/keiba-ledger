"""
JV-Link 取得 → ファイル出力（32bit Python 専用・psycopg2 不要）

実行: py -3.11-32 scripts/fetch/fetch_to_file.py --from 2024-01-01 --no-odds --limit 100
"""

import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

try:
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env", encoding="utf-8")
except Exception:
    pass

sys.path.insert(0, str(Path(__file__).resolve().parent))
from jvlink_client import JVLinkClient, TARGET_RECORD_TYPES

RECORD_TYPES_NO_ODDS = frozenset(["RA", "SE", "HR", "UM", "KS", "CH", "WH", "WE", "JG"])
# DIFF（蓄積情報）に含まれるマスタ。RACEには含まれない。
RECORD_TYPES_DIFF = frozenset(["UM", "KS", "CH"])
RECORD_TYPES_UM_KS = frozenset(["UM", "KS"])


def extract_source_date(record_type: str, raw_text: str) -> str | None:
    if not raw_text or len(raw_text) < 12:
        return None
    try:
        def _valid(y: int, m: int, d: int) -> bool:
            return 1990 <= y <= 2030 and 1 <= m <= 12 and 1 <= d <= 31

        if record_type in ("RA", "SE") and len(raw_text) >= 12:
            s = raw_text[4:12]
            if s.isdigit():
                y, m, d = int(s[:4]), int(s[4:6]), int(s[6:8])
                if _valid(y, m, d):
                    return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
        if record_type in ("UM", "KS", "CH") and len(raw_text) >= 12:
            s = raw_text[3:11]  # データ作成年月日 位置4-11
            if s.isdigit():
                y, m, d = int(s[:4]), int(s[4:6]), int(s[6:8])
                if _valid(y, m, d):
                    return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
        for i in range(min(20, len(raw_text) - 8)):
            s = raw_text[i : i + 8]
            if s.isdigit():
                y, m, d = int(s[:4]), int(s[4:6]), int(s[6:8])
                if _valid(y, m, d):
                    return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
    except Exception:
        pass
    return None


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--from", dest="from_date", default="2010-01-01")
    parser.add_argument("--no-odds", action="store_true")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--output", type=str, default="", help="Output file path")
    parser.add_argument("--diff-only", action="store_true", help="DIFF（UM/KS/CH）のみ取得")
    parser.add_argument("--um-ks-only", action="store_true", help="UM/KS のみ取得（DIFF の UM,KS のみ）")
    parser.add_argument("--setup", action="store_true", help="DIFN を option=3（セットアップ）で取得")
    args = parser.parse_args()
    if args.um_ks_only:
        args.diff_only = True

    from_date = args.from_date
    from_time = from_date.replace("-", "") + "000000"
    target_types = RECORD_TYPES_NO_ODDS if args.no_odds else TARGET_RECORD_TYPES

    (ROOT / "data").mkdir(exist_ok=True)
    out_path = args.output
    if not out_path:
        from datetime import datetime
        out_path = ROOT / "data" / f"fetch_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jsonl"

    client = JVLinkClient()
    if not client.init():
        print("[NG] JVInit failed")
        sys.exit(1)

    total = 0
    write_mode = "w"

    # DIFF のみの場合は RACE をスキップ（空ファイルを事前作成）
    if args.diff_only:
        with open(out_path, "w", encoding="utf-8") as _:
            pass
    if not args.diff_only:
        rc, dl_count, _ = client.open("RACE", from_time, option=1)
        if rc < 0:
            print(f"[NG] JVOpen(RACE) failed: rc={rc}")
            sys.exit(1)

        if dl_count > 0:
            print(f"[RACE] 取得開始 (予定: {dl_count:,} 件)...")
        else:
            print("[RACE] 取得開始...")

        PROGRESS_INTERVAL = 5000
        type_counts: dict[str, int] = {}
        with open(out_path, write_mode, encoding="utf-8") as f:
            for record_type, raw_text in client.read(target_types=target_types):
                source_date = extract_source_date(record_type, raw_text)
                row = {
                    "record_type": record_type,
                    "source_date": source_date,
                    "raw_text": raw_text,
                }
                f.write(json.dumps(row, ensure_ascii=False) + "\n")
                total += 1
                type_counts[record_type] = type_counts.get(record_type, 0) + 1
                if total % PROGRESS_INTERVAL == 0:
                    breakdown = " ".join(f"{t}:{c:,}" for t, c in sorted(type_counts.items()))
                    print(f"  RACE: {total:,} 件 ({breakdown})", flush=True)
                if args.limit and total >= args.limit:
                    break

        print(f"  RACE 完了: {total:,} 件", flush=True)
        client.close()
        write_mode = "a"  # DIFF は追記

    # DIFF（蓄積情報）で UM/KS/CH を取得（--no-odds または --diff-only 時）
    if args.no_odds or args.diff_only:
        if not args.diff_only:
            # RACE 後に close 済みのため再 init
            if not client.init():
                print("[NG] JVInit failed (DIFN)")
                sys.exit(1)
        # DIFN は 1986年から（RACE と同範囲）。仕様変更で DIFF→DIFN に移行済み。
        diff_from = "19860101000000" if from_time < "19860101000000" else from_time
        diff_option = 3 if args.setup else 1  # 3=セットアップ（全件）, 1=通常（差分）
        rc, dl_count, _ = client.open("DIFN", diff_from, option=diff_option)
        if rc >= 0:
            diff_count = 0
            diff_types = RECORD_TYPES_UM_KS if args.um_ks_only else RECORD_TYPES_DIFF
            label = "UM/KS" if args.um_ks_only else "UM/KS/CH"
            if dl_count > 0:
                print(f"[DIFN] 取得開始 (予定: {dl_count:,} 件, {label})...")
            else:
                print(f"[DIFN] 取得開始 ({label})...")
            PROGRESS_INTERVAL = 20000
            with open(out_path, write_mode, encoding="utf-8") as f:
                for record_type, raw_text in client.read(target_types=diff_types):
                    source_date = extract_source_date(record_type, raw_text)
                    row = {
                        "record_type": record_type,
                        "source_date": source_date,
                        "raw_text": raw_text,
                    }
                    f.write(json.dumps(row, ensure_ascii=False) + "\n")
                    diff_count += 1
                    if diff_count % PROGRESS_INTERVAL == 0:
                        print(f"  DIFN: {diff_count:,} 件", flush=True)
            total += diff_count
            print(f"  DIFN 完了: {diff_count:,} 件 ({label})", flush=True)
        else:
            print(f"[WARN] JVOpen(DIFN) failed: rc={rc} (UM/KS/CH は取得できません)")
        client.close()

    print(str(out_path))
    print(f"[OK] {total} records -> {out_path}")


if __name__ == "__main__":
    main()
