"""
O2（馬連オッズ）レコードのパーサー
JV-Data仕様書 ８．オッズ2（馬連）に基づく（2042バイト）

馬連の確定オッズを analytics.odds_final 用に返す。
データ区分 3=最終, 4=確定, 5=確定(月曜) のみ処理。
"""


def _to_bytes(raw: str) -> bytes:
    return raw.encode("cp932", errors="replace")


def _sub_bytes(b: bytes, start: int, length: int) -> str:
    if not b or start < 1:
        return ""
    chunk = b[start - 1 : start - 1 + length]
    return chunk.decode("cp932", errors="replace").strip()


def _num_or_none(s: str):
    if not s or not s.strip() or s.strip() in ("-----", "*****", "      ", "000000"):
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


def parse_o2(raw: str) -> list[dict] | None:
    """
    O2レコードをパースして analytics.odds_final 用の辞書リストを返す。
    確定オッズ（データ区分 3,4,5）のみ。馬連。
    """
    try:
        b = _to_bytes(raw)
    except Exception:
        return None
    if len(b) < 2030:
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

    # 馬連オッズ: pos 41, 153回×13B (組番4, オッズ6, 人気3) 組番昇順 01-02～17-18
    for i in range(153):
        base = 41 + i * 13
        combo_raw = _sub_bytes(b, base, 4)  # "0102" etc
        odds_s = _sub_bytes(b, base + 4, 6)
        pop_s = _sub_bytes(b, base + 10, 3)
        if not combo_raw or len(combo_raw) < 4:
            continue
        try:
            a, b_val = int(combo_raw[:2]), int(combo_raw[2:4])
            if a < 1 or b_val < 1 or a == b_val:
                continue
            combination = f"{min(a, b_val)}-{max(a, b_val)}"
        except (ValueError, TypeError):
            continue
        odds = _num_or_none(odds_s)
        if odds is None or odds <= 0:
            continue
        result.append({
            "race_id": race_id,
            "bet_type": "quinella",
            "combination": combination,
            "odds": odds,
            "popularity": _int_or_none(pop_s),
        })

    return result if result else None
