"""RA/SE/UM/KSリセット + 正規化実行

--ra-only : RAのみリセット
--se-only : SEのみリセット（race_entries 再投入用）
--um-only : UMのみリセット（horses 再投入用）
--ks-only : KSのみリセット（jockeys 再投入用）
指定なし: RA と SE 両方リセット
"""
import argparse
import os
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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ra-only", action="store_true", help="RAのみリセット")
    parser.add_argument("--se-only", action="store_true", help="SEのみリセット（race_entries再投入）")
    parser.add_argument("--um-only", action="store_true", help="UMのみリセット（horses再投入）")
    parser.add_argument("--ks-only", action="store_true", help="KSのみリセット（jockeys再投入）")
    args = parser.parse_args()

    cfg = {
        "host": os.getenv("LOCAL_DB_HOST", "localhost"),
        "port": int(os.getenv("LOCAL_DB_PORT", "5432")),
        "dbname": os.getenv("LOCAL_DB_NAME", "keiba"),
        "user": os.getenv("LOCAL_DB_USER", "postgres"),
        "password": os.getenv("LOCAL_DB_PASSWORD", ""),
    }
    conn = psycopg2.connect(**cfg)
    cur = conn.cursor()

    only_flags = [args.ra_only, args.se_only, args.um_only, args.ks_only]
    if any(only_flags):
        reset_ra = args.ra_only
        reset_se = args.se_only
        reset_um = args.um_only
        reset_ks = args.ks_only
    else:
        reset_ra = True
        reset_se = True
        reset_um = False
        reset_ks = False

    total = 0
    if reset_ra:
        cur.execute("UPDATE raw.jvdata SET processed = FALSE WHERE record_type = 'RA'")
        n = cur.rowcount
        total += n
        print(f"[1/2] Reset: {n} RA records -> processed=FALSE")
    if reset_se:
        cur.execute("UPDATE raw.jvdata SET processed = FALSE WHERE record_type = 'SE'")
        n = cur.rowcount
        total += n
        print(f"[1/2] Reset: {n} SE records -> processed=FALSE")
    if reset_um:
        cur.execute("UPDATE raw.jvdata SET processed = FALSE WHERE record_type = 'UM'")
        n = cur.rowcount
        total += n
        print(f"[1/2] Reset: {n} UM records -> processed=FALSE")
    if reset_ks:
        cur.execute("UPDATE raw.jvdata SET processed = FALSE WHERE record_type = 'KS'")
        n = cur.rowcount
        total += n
        print(f"[1/2] Reset: {n} KS records -> processed=FALSE")

    if total == 0 and (reset_ra or reset_se or reset_um or reset_ks):
        print("[1/2] No matching records to reset")
    conn.commit()
    conn.close()

    # 正規化実行（normalize に --se-only 等が渡らないよう argv をクリア）
    _saved_argv = sys.argv
    sys.argv = [sys.argv[0]]
    try:
        sys.path.insert(0, str(ROOT / "scripts" / "transform"))
        import normalize
        normalize.main()
    finally:
        sys.argv = _saved_argv


if __name__ == "__main__":
    main()
