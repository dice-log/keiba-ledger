"""
ファイル → raw.jvdata 投入（64bit Python・psycopg2 必要）

実行: python scripts/fetch/load_to_db.py [fetch_YYYYMMDD_HHMMSS.jsonl]
      未指定時は data/ 内の最新ファイルを使用
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
    parser.add_argument("file", nargs="?", default="", help="JSONL file path")
    args = parser.parse_args()

    data_dir = ROOT / "data"
    if args.file:
        path = Path(args.file)
        if not path.is_absolute():
            path = ROOT / path
    else:
        if not data_dir.exists():
            print("[NG] data/ folder empty. Run fetch_to_file first.")
            sys.exit(1)
        files = sorted(data_dir.glob("fetch_*.jsonl"), key=lambda p: p.stat().st_mtime, reverse=True)
        if not files:
            print("[NG] No fetch_*.jsonl in data/")
            sys.exit(1)
        path = files[0]

    if not path.exists():
        print(f"[NG] File not found: {path}")
        sys.exit(1)

    conn = psycopg2.connect(**get_db_config())
    cur = conn.cursor()

    cur.execute(
        """INSERT INTO raw.fetch_log (fetch_type, record_types, status)
           VALUES ('initial', ARRAY['RA','SE','HR'], 'running')
           RETURNING id"""
    )
    log_id = cur.fetchone()[0]
    conn.commit()

    batch = []
    total = 0
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                row = json.loads(line)
                rt = row.get("record_type", "")
                sd = row.get("source_date")
                raw = row.get("raw_text", "")
                payload = json.dumps({"record_type": rt, "raw_length": len(raw)})
                batch.append((rt, None, sd, payload, raw))
                if len(batch) >= 500:
                    execute_values(
                        cur,
                        """INSERT INTO raw.jvdata (record_type, record_spec, source_date, payload, raw_text, processed)
                           VALUES %s""",
                        [(r[0], r[1], r[2], r[3], r[4], False) for r in batch],
                    )
                    conn.commit()
                    total += len(batch)
                    batch = []

        if batch:
            execute_values(
                cur,
                """INSERT INTO raw.jvdata (record_type, record_spec, source_date, payload, raw_text, processed)
                   VALUES %s""",
                [(r[0], r[1], r[2], r[3], r[4], False) for r in batch],
            )
            conn.commit()
            total += len(batch)

        cur.execute(
            "UPDATE raw.fetch_log SET records_fetched = %s, status = 'success', finished_at = NOW() WHERE id = %s",
            (total, log_id),
        )
        conn.commit()
        print(f"[OK] {total} records loaded to DB")
    except Exception as e:
        cur.execute(
            "UPDATE raw.fetch_log SET status = 'error', error_message = %s, finished_at = NOW() WHERE id = %s",
            (str(e)[:500], log_id),
        )
        conn.commit()
        print(f"[NG] {e}")
        sys.exit(1)
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
