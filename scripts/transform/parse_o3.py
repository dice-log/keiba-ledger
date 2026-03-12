"""
O3（ワイドオッズ）レコードのパーサー
JV-Data仕様書 ９．オッズ3（ワイド）に基づく（2654バイト）

ワイドの確定オッズを analytics.odds_final 用に返す。
データ区分 3=最終, 4=確定, 5=確定(月曜) のみ処理。
最低・最高オッズの平均を odds として使用。
"""


def _to_bytes(raw: str) -> bytes:
    return raw.encode("cp932", errors="replace")


def _sub_bytes(b: bytes, start: int, length: int) -> str:
    if not b or start < 1:
        return ""
    chunk = b[start - 1 : start - 1 + length]
    return chunk.decode("cp932", errors="replace").strip()


def _num_or_none(s: str):
    if not s or not s.strip() or s.strip() in ("-----", "*****", "      ", "000000", "99999"):
        return None
    try:
        return float(s.replace(" ", ""))
    except ValueError:
        return None


def _int_or_none(s: str):
    if not s or not s.strip() or s.strip() in ("---", "***", " "):
        return None
    try:
        return int(s)
    except ValueError:
        return None


def parse_o3(raw: str) -> list[dict] | None:
    """
    O3レコードをパースして analytics.odds_final 用の辞書リストを返す。
    確定オッズ（データ区分 3,4,5）のみ。ワイド。
    """
    try:
        b = _to_bytes(raw)
    except Exception:
        return None
    if len(b) < 2640:
        return None

    data_kbn = _sub_bytes(b, 3, 1)
    if data_kbn not in ("3", "4", "5"):
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

    # ワイドオッズ: pos 41, 153回×17B (組番4, 最低5, 最高5, 人気3) 組番昇順 01-02～17-18
    for i in range(153):
        base = 41 + i * 17
        combo_raw = _sub_bytes(b, base, 4)
        odds_lo_s = _sub_bytes(b, base + 4, 5)
        odds_hi_s = _sub_bytes(b, base + 9, 5)
        pop_s = _sub_bytes(b, base + 14, 3)
        if not combo_raw or len(combo_raw) < 4:
            continue
        try:
            a, b_val = int(combo_raw[:2]), int(combo_raw[2:4])
            if a < 1 or b_val < 1 or a == b_val:
                continue
            combination = f"{min(a, b_val)}-{max(a, b_val)}"
        except (ValueError, TypeError):
            continue
        lo = _num_or_none(odds_lo_s)
        hi = _num_or_none(odds_hi_s)
        if lo is not None and hi is not None and lo > 0 and hi > 0:
            odds = (lo + hi) / 2
        elif lo is not None and lo > 0:
            odds = lo
        elif hi is not None and hi > 0:
            odds = hi
        else:
            continue
        result.append({
            "race_id": race_id,
            "bet_type": "wide",
            "combination": combination,
            "odds": round(odds, 1),
            "popularity": _int_or_none(pop_s),
        })

    return result if result else None
