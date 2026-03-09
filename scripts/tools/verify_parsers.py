"""
RA/SE/HR/UM/KS パーサー検証
仕様書の桁位置と実データを比較
"""
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

import psycopg2

sys.path.insert(0, str(ROOT / "scripts" / "transform"))
from parse_ra import parse_ra, _sub as ra_sub
from parse_se import parse_se, _sub as se_sub
from parse_hr import parse_hr, _sub as hr_sub
from parse_um import parse_um
from parse_ks import parse_ks


def _sub(s: str, start: int, length: int) -> str:
    if not s or start < 1:
        return ""
    return s[start - 1 : start - 1 + length]


def main():
    conn = psycopg2.connect(
        host=os.getenv("LOCAL_DB_HOST", "localhost"),
        port=int(os.getenv("LOCAL_DB_PORT", "5432")),
        dbname=os.getenv("LOCAL_DB_NAME", "keiba"),
        user=os.getenv("LOCAL_DB_USER", "postgres"),
        password=os.getenv("LOCAL_DB_PASSWORD", ""),
    )
    cur = conn.cursor()

    print("=" * 60)
    print("RA (race detail) verification")
    print("=" * 60)
    cur.execute(
        "SELECT id, raw_text FROM raw.jvdata WHERE record_type = 'RA' LIMIT 1"
    )
    row = cur.fetchone()
    if row:
        raw = row[1] or ""
        print(f"raw length: {len(raw)}")
        print(f"pos 1-2 (ID): {repr(_sub(raw, 1, 2))}")
        print(f"pos 4-11 (data_created): {repr(_sub(raw, 4, 8))}")
        print(f"pos 12-15 (year): {repr(_sub(raw, 12, 4))}")
        print(f"pos 16-19 (mmdd): {repr(_sub(raw, 16, 4))}")
        print(f"pos 20-21 (venue): {repr(_sub(raw, 20, 2))}")
        print(f"pos 33-92 (race_name): {repr(_sub(raw, 33, 60))}")
        d = parse_ra(raw)
        if d:
            print(f"parsed race_id: {d.get('race_id')}")
            print(f"parsed race_date: {d.get('race_date')}")
            print(f"parsed venue: {d.get('venue_name')}")
            rn = d.get('race_name') or ''
            print(f"parsed race_name: {rn[:40] if rn else '(empty)'}...")
        else:
            print("parse_ra returned None")

    print()
    print("=" * 60)
    print("SE (horse entry) verification")
    print("=" * 60)
    cur.execute(
        "SELECT id, raw_text FROM raw.jvdata WHERE record_type = 'SE' LIMIT 1"
    )
    row = cur.fetchone()
    if row:
        raw = row[1] or ""
        print(f"raw length: {len(raw)} (spec: 555)")
        print(f"pos 1-2 (ID): {repr(_sub(raw, 1, 2))}")
        print(f"pos 12-15 (year): {repr(_sub(raw, 12, 4))}")
        print(f"pos 16-19 (mmdd): {repr(_sub(raw, 16, 4))}")
        print(f"pos 20-21 (venue): {repr(_sub(raw, 20, 2))}")
        print(f"pos 28 (frame): {repr(_sub(raw, 28, 1))}")
        print(f"pos 29-30 (horse_no): {repr(_sub(raw, 29, 2))}")
        print(f"pos 31-40 (horse_id): {repr(_sub(raw, 31, 10))}")
        print(f"pos 41-76 (horse_name): {repr(_sub(raw, 41, 36))}")
        print(f"pos 333-334 (finish_pos/inline): {repr(_sub(raw, 333, 2))}")
        print(f"pos 335-336 (finish_pos): {repr(_sub(raw, 335, 2))}")
        print(f"pos 339-342 (time): {repr(_sub(raw, 339, 4))}")
        d = parse_se(raw)
        if d:
            print(f"parsed race_id: {d.get('race_id')}")
            print(f"parsed horse_name: {d.get('horse_name')}")
            print(f"parsed finish_pos: {d.get('finish_pos')}")
            print(f"parsed horse_number: {d.get('horse_number')}")
        else:
            print("parse_se returned None")

    print()
    print("=" * 60)
    print("HR (payout) verification")
    print("=" * 60)
    cur.execute(
        "SELECT id, raw_text FROM raw.jvdata WHERE record_type = 'HR' LIMIT 1"
    )
    row = cur.fetchone()
    if row:
        raw = row[1] or ""
        print(f"raw length: {len(raw)} (spec: 719)")
        print(f"pos 12-15 (year): {repr(_sub(raw, 12, 4))}")
        print(f"pos 16-19 (mmdd): {repr(_sub(raw, 16, 4))}")
        print(f"pos 20-21 (venue): {repr(_sub(raw, 20, 2))}")
        print(f"pos 103-114 (win1): uma={repr(_sub(raw, 104, 2))} payout={repr(_sub(raw, 106, 9))}")
        payouts = parse_hr(raw)
        print(f"parsed payouts count: {len(payouts)}")
        if payouts:
            for p in payouts[:3]:
                print(f"  {p}")

    print()
    print("=" * 60)
    print("UM (horse master) verification")
    print("=" * 60)
    cur.execute(
        "SELECT id, raw_text FROM raw.jvdata WHERE record_type = 'UM' LIMIT 1"
    )
    row = cur.fetchone()
    if row:
        raw = row[1] or ""
        print(f"raw length: {len(raw)} (spec: 1609)")
        d = parse_um(raw)
        print(f"parsed: {d}")
    else:
        print("No UM records in DB")

    print()
    print("=" * 60)
    print("KS (jockey master) verification")
    print("=" * 60)
    cur.execute(
        "SELECT id, raw_text FROM raw.jvdata WHERE record_type = 'KS' LIMIT 1"
    )
    row = cur.fetchone()
    if row:
        raw = row[1] or ""
        print(f"raw length: {len(raw)} (spec: 4173)")
        d = parse_ks(raw)
        print(f"parsed: {d}")
    else:
        print("No KS records in DB")

    cur.close()
    conn.close()
    print()
    print("Done")


if __name__ == "__main__":
    main()
