"""
時系列オッズ JSONL → raw.odds_timeseries_raw 投入

fetch_timeseries_o1_o2.py で出力した JSONL を raw.odds_timeseries_raw に挿入する。

実行: python scripts/fetch/load_timeseries_to_db.py [timeseries_YYYYMMDD_HHMMSS.jsonl]
      未指定時は data/ 内の最新 timeseries_*.jsonl を使用
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

try:
    import psycopg2
    from psycopg2.extras import execute_values
except ImportError:
    print("[NG] pip install psycopg2-binary")
    sys.exit(1)


def get_db_config():
    return {
        "host": os.getenv("LOCAL_DB_HOST", "localhost"),
        "port": int(os.getenv("LOCAL_DB_PORT", "5432")),
        "dbname": os.getenv("LOCAL_DB_NAME", "keiba"),
        "user": os.getenv("LOCAL_DB_USER", "postgres"),
        "password": os.getenv("LOCAL_DB_PASSWORD", ""),
    }


def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("file", nargs="?", default="", help="時系列オッズ JSONL ファイル")
    args = parser.parse_args()

    data_dir = ROOT / "data"
    if args.file:
        path = Path(args.file)
        if not path.is_absolute():
            path = ROOT / path
    else:
        if not data_dir.exists():
            print("[NG] data/ がありません。fetch_timeseries_o1_o2.py を先に実行してください。")
            sys.exit(1)
        files = sorted(data_dir.glob("timeseries_*.jsonl"), key=lambda p: p.stat().st_mtime, reverse=True)
        if not files:
            print("[NG] data/ に timeseries_*.jsonl がありません。")
            sys.exit(1)
        path = files[0]

    if not path.exists():
        print(f"[NG] File not found: {path}")
        sys.exit(1)

    conn = psycopg2.connect(**get_db_config())
    cur = conn.cursor()

    batch = []
    BATCH_SIZE = 5000
    total = 0

    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            dataspec = obj.get("dataspec", "")
            record_type = obj.get("record_type", "")
            raw_text = obj.get("raw_text", "")
            if not dataspec or not record_type:
                continue
            batch.append((dataspec, record_type, raw_text))
            if len(batch) >= BATCH_SIZE:
                execute_values(
                    cur,
                    """
                    INSERT INTO raw.odds_timeseries_raw (dataspec, record_type, raw_text)
                    VALUES %s
                    """,
                    batch,
                )
                conn.commit()
                total += len(batch)
                print(f"  {total:,} 件投入...", flush=True)
                batch = []

    if batch:
        execute_values(
            cur,
            """
            INSERT INTO raw.odds_timeseries_raw (dataspec, record_type, raw_text)
            VALUES %s
            """,
            batch,
        )
        conn.commit()
        total += len(batch)

    cur.close()
    conn.close()
    print(f"[OK] {total:,} 件 -> raw.odds_timeseries_raw")


if __name__ == "__main__":
    main()
