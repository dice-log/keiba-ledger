"""
ファイル → raw.jvdata 投入（64bit Python・psycopg2 必要）

実行: python scripts/fetch/load_to_db.py [fetch_YYYYMMDD_HHMMSS.jsonl]
      未指定時は data/ 内の最新ファイルを使用
"""

import hashlib
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
        if "*" in str(path):
            matches = sorted(path.parent.glob(path.name), key=lambda p: p.stat().st_mtime, reverse=True)
            if not matches:
                print(f"[NG] No match: {path}")
                sys.exit(1)
            path = matches[0]
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

    def _content_hash(raw: str) -> str:
        return hashlib.md5((raw or "").encode("utf-8")).hexdigest()

    def _valid_date(sd: str | None) -> str | None:
        if not sd or not isinstance(sd, str) or len(sd) != 10:
            return None
        try:
            y, m, d = int(sd[:4]), int(sd[5:7]), int(sd[8:10])
            if not (1990 <= y <= 2030 and 1 <= m <= 12 and 1 <= d <= 31):
                return None
            from datetime import datetime
            datetime(y, m, d)
            return sd
        except (ValueError, TypeError):
            return None

    cur.execute(
        "SELECT record_type || ':' || md5(COALESCE(raw_text, '')) FROM raw.jvdata"
    )
    existing_keys = {r[0] for r in cur.fetchall()}

    batch = []
    total = 0
    read_count = 0
    PROGRESS_INTERVAL = 10000
    try:
        print(f"[load] {path.name} を読込中...", flush=True)
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                row = json.loads(line)
                rt = row.get("record_type", "")
                sd = _valid_date(row.get("source_date"))
                raw = row.get("raw_text", "")
                payload = json.dumps({"record_type": rt, "raw_length": len(raw)})
                batch.append((rt, None, sd, payload, raw))
                read_count += 1
                if len(batch) >= 500:
                    new_batch = [
                        r for r in batch
                        if f"{r[0]}:{_content_hash(r[4])}" not in existing_keys
                    ]
                    if new_batch:
                        execute_values(
                            cur,
                            """INSERT INTO raw.jvdata (record_type, record_spec, source_date, payload, raw_text, processed)
                               VALUES %s""",
                            [(r[0], r[1], r[2], r[3], r[4], False) for r in new_batch],
                        )
                        conn.commit()
                        total += len(new_batch)
                        for r in new_batch:
                            existing_keys.add(f"{r[0]}:{_content_hash(r[4])}")
                    batch = []
                    if read_count % PROGRESS_INTERVAL == 0:
                        print(f"  load: 読込 {read_count:,} 件, 投入 {total:,} 件 (新規)", flush=True)

        if batch:
            new_batch = [
                r for r in batch
                if f"{r[0]}:{_content_hash(r[4])}" not in existing_keys
            ]
            if new_batch:
                execute_values(
                    cur,
                    """INSERT INTO raw.jvdata (record_type, record_spec, source_date, payload, raw_text, processed)
                       VALUES %s""",
                    [(r[0], r[1], r[2], r[3], r[4], False) for r in new_batch],
                )
                conn.commit()
                total += len(new_batch)

        cur.execute(
            "UPDATE raw.fetch_log SET records_fetched = %s, status = 'success', finished_at = NOW() WHERE id = %s",
            (total, log_id),
        )
        conn.commit()
        if read_count != total:
            print(f"[OK] 読込 {read_count:,} 件 → 投入 {total:,} 件 (重複 {read_count - total:,} 件スキップ)")
        else:
            print(f"[OK] {total:,} 件 loaded to DB")
    except Exception as e:
        conn.rollback()
        try:
            cur.execute(
                "UPDATE raw.fetch_log SET status = 'error', error_message = %s, finished_at = NOW() WHERE id = %s",
                (str(e)[:500], log_id),
            )
            conn.commit()
        except Exception:
            conn.rollback()
        print(f"[NG] {e}")
        sys.exit(1)
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
