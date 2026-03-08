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


def extract_source_date(record_type: str, raw_text: str) -> str | None:
    if not raw_text or len(raw_text) < 12:
        return None
    try:
        if record_type in ("RA", "SE") and len(raw_text) >= 12:
            s = raw_text[4:12]
            if s.isdigit():
                return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
        for i in range(min(20, len(raw_text) - 8)):
            s = raw_text[i : i + 8]
            if s.isdigit() and 1990 <= int(s[:4]) <= 2030:
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
    args = parser.parse_args()

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

    rc, dl_count, _ = client.open("RACE", from_time, option=1)
    if rc < 0:
        print(f"[NG] JVOpen failed: rc={rc}")
        sys.exit(1)

    total = 0
    with open(out_path, "w", encoding="utf-8") as f:
        for record_type, raw_text in client.read(target_types=target_types):
            source_date = extract_source_date(record_type, raw_text)
            row = {
                "record_type": record_type,
                "source_date": source_date,
                "raw_text": raw_text,
            }
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
            total += 1
            if args.limit and total >= args.limit:
                break

    client.close()
    print(str(out_path))
    print(f"[OK] {total} records -> {out_path}")


if __name__ == "__main__":
    main()
