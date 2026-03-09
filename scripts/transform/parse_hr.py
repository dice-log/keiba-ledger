"""
HR（払戻）レコードのパーサー
JV-Data仕様書 ４．払戻 に基づく（位置は1始まりのバイト位置、719 byte）

※JV-Dataはバイト位置指定のため、cp932でバイト列に変換してからパースする。
"""


def _to_bytes(raw: str) -> bytes:
    """Unicode文字列をcp932バイト列に変換（JV-Data仕様）"""
    return raw.encode("cp932", errors="replace")


def _sub_bytes(b: bytes, start: int, length: int) -> str:
    """1始まりのバイト位置から length バイト切り出し、cp932でデコード"""
    if not b or start < 1:
        return ""
    chunk = b[start - 1 : start - 1 + length]
    return chunk.decode("cp932", errors="replace").strip()


def _int_or_none(s: str) -> int | None:
    if not s or not s.strip():
        return None
    try:
        return int(s)
    except ValueError:
        return None


# 馬券種別と bet_type の対応
BET_TYPES = [
    ("win", 103, 3, 13, 2, 9, 2),       # 単勝: 馬番(2) 払戻(9) 人気(2)
    ("place", 142, 5, 13, 2, 9, 2),     # 複勝
    ("bracket", 207, 3, 13, 2, 9, 2),   # 枠連: 組番(2)
    ("quinella", 246, 3, 16, 4, 9, 3),  # 馬連: 組番(4) ex "0307"
    ("wide", 294, 7, 16, 4, 9, 3),      # ワイド
    ("exacta", 454, 6, 16, 4, 9, 3),    # 馬単
    ("trio", 550, 3, 18, 6, 9, 3),      # 3連複: 組番(6)
    ("trifecta", 604, 6, 19, 6, 9, 4),  # 3連単: 組番(6) 人気(4)
]


def _fmt_combo(bet_type: str, combo_raw: str) -> str:
    """組番を analytics.payouts 用の combination 形式に変換"""
    c = (combo_raw or "").strip()
    if not c or c in ("00", "0000", "000000"):
        return ""
    # 枠連: "35" -> "3-5"
    if bet_type == "bracket" and len(c) == 2:
        return f"{c[0]}-{c[1]}" if c[0] != "0" else c
    # 馬連・ワイド: "0307" -> "3-7"
    if bet_type in ("quinella", "wide") and len(c) == 4:
        a, b = int(c[:2]), int(c[2:])
        return f"{min(a,b)}-{max(a,b)}" if a and b else c
    # 馬単: "0703" -> "7-3" (着順のまま)
    if bet_type == "exacta" and len(c) == 4:
        a, b = int(c[:2]), int(c[2:])
        return f"{a}-{b}" if a and b else c
    # 3連複: "030712" -> "3-7-12" (昇順)
    if bet_type == "trio" and len(c) == 6:
        a, b, d = int(c[:2]), int(c[2:4]), int(c[4:6])
        if a and b and d:
            return "-".join(str(x) for x in sorted([a, b, d]))
        return c
    # 3連単: "070312" -> "7-3-12" (着順のまま)
    if bet_type == "trifecta" and len(c) == 6:
        a, b, d = int(c[:2]), int(c[2:4]), int(c[4:6])
        return f"{a}-{b}-{d}" if a and b and d else c
    # 単勝・複勝: "07" -> "7"
    if bet_type in ("win", "place") and len(c) == 2:
        return str(int(c)) if c != "00" else ""
    return c


def parse_hr(raw: str) -> list[dict] | None:
    """
    HRレコードをパースして analytics.payouts 用の辞書リストを返す。
    1レコードで複数馬券種の払戻があるため、リストで返す。
    """
    try:
        b = _to_bytes(raw)
    except Exception:
        return None
    if len(b) < 604:
        return None

    year = _sub_bytes(b, 12, 4)
    mmdd = _sub_bytes(b, 16, 4)
    venue_code = _sub_bytes(b, 20, 2)
    kai = _sub_bytes(b, 22, 2)
    nichi = _sub_bytes(b, 24, 2)
    race_no = _sub_bytes(b, 26, 2)
    if not (year and mmdd and venue_code and race_no):
        return None

    race_id = f"{year}{mmdd}{venue_code}{kai}{nichi}{race_no}"
    result = []

    for bet_type, start, repeat, rec_len, combo_len, payout_len, pop_len in BET_TYPES:
        combo_offs = combo_len
        payout_offs = combo_len + payout_len
        pop_offs = combo_len + payout_len + pop_len

        for i in range(repeat):
            pos = start + i * rec_len
            combo_raw = _sub_bytes(b, pos, combo_len)
            payout_str = _sub_bytes(b, pos + combo_offs, payout_len)
            pop_str = _sub_bytes(b, pos + payout_offs, pop_len)

            payout = _int_or_none(payout_str)
            if payout is None or payout <= 0:
                continue
            combination = _fmt_combo(bet_type, combo_raw)
            if not combination:
                continue

            result.append({
                "race_id": race_id,
                "bet_type": bet_type,
                "combination": combination,
                "payout": payout,
                "popularity": _int_or_none(pop_str),
            })

    return result if result else None
