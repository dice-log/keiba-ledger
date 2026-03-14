"""
JV-Link 取得 → ファイル出力（32bit Python 専用・psycopg2 不要）

実行: py -3.11-32 scripts/fetch/fetch_to_file.py --from 2024-01-01 --no-odds --limit 100
      py -3.11-32 scripts/fetch/fetch_to_file.py --ra-se-hr-only  # races/entries/payouts のみ全件
"""

import json
import os
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
from jvlink_client import JVLinkClient, TARGET_RECORD_TYPES

RECORD_TYPES_NO_ODDS = frozenset(["RA", "SE", "HR", "UM", "KS", "CH", "WH", "WE", "JG"])
RECORD_TYPES_O1 = frozenset(["O1"])
RECORD_TYPES_O2 = frozenset(["O2"])
RECORD_TYPES_O3 = frozenset(["O3"])
RECORD_TYPES_O4 = frozenset(["O4"])
RECORD_TYPES_O5 = frozenset(["O5"])
RECORD_TYPES_O6 = frozenset(["O6"])
# races / race_entries / payouts のみ（DIFN はスキップ）
RECORD_TYPES_RA_SE_HR = frozenset(["RA", "SE", "HR"])
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
        if record_type in ("UM", "KS", "CH", "JG", "O1", "O2", "O3", "O4", "O5", "O6") and len(raw_text) >= 12:
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


def _progress_str(count: int, total: int | None, elapsed: float, type_counts: dict) -> str:
    """進捗表示用の文字列を生成
    ※total(dl_count)はファイル数でありレコード数と一致しないため、%は表示しない
    """
    parts = [f"{count:,} 件"]
    if elapsed >= 1:
        parts.append(f"[経過 {elapsed:.0f}秒]")
    breakdown = " ".join(f"{t}:{c:,}" for t, c in sorted(type_counts.items()))
    if breakdown:
        parts.append(f" ({breakdown})")
    return " ".join(parts)


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--from", dest="from_date", default="2010-01-01")
    parser.add_argument("--no-odds", action="store_true")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--output", type=str, default="", help="Output file path")
    parser.add_argument("--ra-se-hr-only", action="store_true", help="RA/SE/HR のみ取得（races/entries/payouts用・DIFNスキップ）")
    parser.add_argument("--diff-only", action="store_true", help="DIFF（UM/KS/CH）のみ取得")
    parser.add_argument("--um-ks-only", action="store_true", help="UM/KS のみ取得（DIFF の UM,KS のみ）")
    parser.add_argument("--setup", action="store_true", help="option=3（セットアップ）で全件取得。DIFN および --odds-o1-only 時の RACE に適用")
    parser.add_argument("--odds-o1-only", action="store_true", help="O1（単複枠オッズ）のみ取得")
    parser.add_argument("--odds-o2-only", action="store_true", help="O2（馬連オッズ）のみ取得")
    parser.add_argument("--odds-o3-only", action="store_true", help="O3（ワイドオッズ）のみ取得")
    parser.add_argument("--odds-o4-only", action="store_true", help="O4（馬単オッズ）のみ取得")
    parser.add_argument("--odds-o5-only", action="store_true", help="O5（3連複オッズ）のみ取得")
    parser.add_argument("--odds-o6-only", action="store_true", help="O6（3連単オッズ）のみ取得")
    args = parser.parse_args()
    if args.um_ks_only:
        args.diff_only = True

    from_date = args.from_date
    from_time = from_date.replace("-", "") + "000000"
    if args.ra_se_hr_only:
        target_types = RECORD_TYPES_RA_SE_HR
    elif args.odds_o1_only:
        target_types = RECORD_TYPES_O1
    elif args.odds_o2_only:
        target_types = RECORD_TYPES_O2
    elif args.odds_o3_only:
        target_types = RECORD_TYPES_O3
    elif args.odds_o4_only:
        target_types = RECORD_TYPES_O4
    elif args.odds_o5_only:
        target_types = RECORD_TYPES_O5
    elif args.odds_o6_only:
        target_types = RECORD_TYPES_O6
    else:
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

    # DIFF のみ or O1 のみの場合は事前準備
    if args.diff_only:
        with open(out_path, "w", encoding="utf-8") as _:
            pass
    if not args.diff_only:
        # 3=セットアップ（全件）, 1=通常。O1/O2 全件取得は --setup 必須
        race_option = 3 if (args.ra_se_hr_only or ((args.odds_o1_only or args.odds_o2_only or args.odds_o3_only or args.odds_o4_only or args.odds_o5_only or args.odds_o6_only) and args.setup)) else 1
        rc, dl_count, _ = client.open("RACE", from_time, option=race_option)
        if rc < 0:
            print(f"[NG] JVOpen(RACE) failed: rc={rc}")
            sys.exit(1)

        label = "RA/SE/HR" if args.ra_se_hr_only else ("O1" if args.odds_o1_only else ("O2" if args.odds_o2_only else ("O3" if args.odds_o3_only else ("O4" if args.odds_o4_only else ("O5" if args.odds_o5_only else ("O6" if args.odds_o6_only else "RACE"))))))
        if dl_count > 0:
            print(f"[{label}] 取得開始 (予定: {dl_count:,} 件)...", flush=True)
        else:
            print(f"[{label}] 取得開始...", flush=True)

        PROGRESS_INTERVAL = 1000
        type_counts: dict[str, int] = {}
        t0 = time.time()
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
                    elapsed = time.time() - t0
                    msg = _progress_str(total, dl_count if dl_count > 0 else None, elapsed, type_counts)
                    print(f"  {label}: {msg}", flush=True)
                if args.limit and total >= args.limit:
                    break

        elapsed = time.time() - t0
        print(f"  {label} 完了: {total:,} 件 [経過 {elapsed:.0f}秒]", flush=True)
        client.close()
        write_mode = "a"  # DIFF は追記

    # DIFF（蓄積情報）で UM/KS/CH を取得（--no-odds または --diff-only 時）。--ra-se-hr-only の場合はスキップ
    if (args.no_odds or args.diff_only) and not args.ra_se_hr_only:
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
            label = f"DIFN({('UM/KS' if args.um_ks_only else 'UM/KS/CH')})"
            if dl_count > 0:
                print(f"[{label}] 取得開始 (予定: {dl_count:,} 件)...", flush=True)
            else:
                print(f"[{label}] 取得開始...", flush=True)
            PROGRESS_INTERVAL = 10000
            type_counts: dict[str, int] = {}
            t0 = time.time()
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
                    type_counts[record_type] = type_counts.get(record_type, 0) + 1
                    if diff_count % PROGRESS_INTERVAL == 0:
                        elapsed = time.time() - t0
                        msg = _progress_str(diff_count, dl_count if dl_count > 0 else None, elapsed, type_counts)
                        print(f"  {label}: {msg}", flush=True)
            total += diff_count
            elapsed = time.time() - t0
            print(f"  {label} 完了: {diff_count:,} 件 [経過 {elapsed:.0f}秒]", flush=True)
        else:
            print(f"[WARN] JVOpen(DIFN) failed: rc={rc} (UM/KS/CH は取得できません)")
        client.close()

    print(str(out_path))
    print(f"[OK] {total} records -> {out_path}")


if __name__ == "__main__":
    main()
