"""
差分データ取得 — 設計書 Phase 1 Step 6

毎週月曜朝6時にタスクスケジューラから実行。
前回成功時刻以降のデータを取得し、normalize → sync_to_supabase を実行。
"""

import json
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

try:
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
except ImportError:
    pass

import psycopg2
from psycopg2.extras import execute_values
from loguru import logger

sys.path.insert(0, str(Path(__file__).parent))
from jvlink_client import JVLinkClient, TARGET_RECORD_TYPES


def get_db_config():
    return {
        "host": os.getenv("LOCAL_DB_HOST", "localhost"),
        "port": int(os.getenv("LOCAL_DB_PORT", "5432")),
        "dbname": os.getenv("LOCAL_DB_NAME", "keiba"),
        "user": os.getenv("LOCAL_DB_USER", "postgres"),
        "password": os.getenv("LOCAL_DB_PASSWORD", ""),
    }


def get_last_success_timestamp(cur) -> str:
    """raw.fetch_log から前回成功時の last_file_timestamp を取得"""
    try:
        cur.execute(
            """SELECT last_file_timestamp FROM raw.fetch_log
               WHERE fetch_type = 'incremental' AND status = 'success'
               AND last_file_timestamp IS NOT NULL AND last_file_timestamp != ''
               ORDER BY finished_at DESC LIMIT 1"""
        )
        row = cur.fetchone()
        return (row[0] or "").strip() if row else ""
    except psycopg2.ProgrammingError:
        return ""  # カラム未追加時


def main():
    (ROOT / "logs").mkdir(exist_ok=True)
    logger.add(ROOT / "logs" / "fetch.log", rotation="1 MB", retention="7 days")
    logger.info("incremental_fetch start")

    conn = psycopg2.connect(**get_db_config())
    cur = conn.cursor()

    cur.execute(
        """INSERT INTO raw.fetch_log (fetch_type, record_types, status)
           VALUES ('incremental', ARRAY['RA','SE','HR'], 'running')
           RETURNING id"""
    )
    fetch_log_id = cur.fetchone()[0]
    conn.commit()

    try:
        from datetime import datetime, timedelta
        last_ts = get_last_success_timestamp(cur)
        if last_ts:
            from_time = last_ts
        else:
            from_time = (datetime.now() - timedelta(days=7)).strftime("%Y%m%d") + "000000"

        client = JVLinkClient()
        if not client.init():
            raise RuntimeError("JVInit 失敗")

        rc, dl_count, new_last_ts = client.open("RACE", from_time, option=1)
        if rc < 0:
            raise RuntimeError(f"JVOpen 失敗: rc={rc}")

        batch = []
        total = 0
        for record_type, raw_text in client.read(target_types=TARGET_RECORD_TYPES):
            source_date = None
            if len(raw_text) >= 12 and raw_text[4:12].isdigit():
                s = raw_text[4:12]
                source_date = f"{s[:4]}-{s[4:6]}-{s[6:8]}"
            payload = json.dumps({"record_type": record_type, "raw_length": len(raw_text)})
            batch.append((record_type, None, source_date, payload, raw_text))

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

        client.close()

        subprocess.run(
            [sys.executable, str(ROOT / "scripts" / "transform" / "normalize.py")],
            cwd=str(ROOT),
            check=True,
        )

        if os.getenv("SUPABASE_URL"):
            subprocess.run(
                [sys.executable, str(ROOT / "scripts" / "sync" / "sync_to_supabase.py")],
                cwd=str(ROOT),
                check=False,
            )

        cur.execute(
            """UPDATE raw.fetch_log SET records_fetched = %s, status = 'success',
               last_file_timestamp = %s, finished_at = NOW() WHERE id = %s""",
            (total, new_last_ts, fetch_log_id),
        )
        conn.commit()

        logger.info(f"incremental_fetch done: {total} records")
        print(f"✅ 差分取得完了: {total} 件")

    except Exception as e:
        logger.exception(str(e))
        cur.execute(
            """UPDATE raw.fetch_log SET status = 'error', error_message = %s, finished_at = NOW()
               WHERE id = %s""",
            (str(e)[:1000], fetch_log_id),
        )
        conn.commit()
        print(f"❌ エラー: {e}")
        sys.exit(1)
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
