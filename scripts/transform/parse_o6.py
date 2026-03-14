"""
O6（3連単オッズ）レコードのパーサー
JV-Data仕様書 １２．オッズ6（3連単）に基づく（83285バイト）

3連単の確定オッズを analytics.odds_final 用に返す。
データ区分 3=最終, 4=確定, 5=確定(月曜) のみ処理。
3連単は1着-2着-3着の順番（combination: "7-3-12" = 1着7番・2着3番・3着12番）
データ開始: 2004年8月
"""


def _to_bytes(raw: str) -> bytes:
    return raw.encode("cp932", errors="replace")


def _sub_bytes(b: bytes, start: int, length: int) -> str:
    if not b or start < 1:
        return ""
    chunk = b[start - 1 : start - 1 + length]
    return chunk.decode("cp932", errors="replace").strip()


def _num_or_none(s: str):
    if not s or not s.strip() or s.strip() in ("-------", "*******", "       ", "0000000"):
        return None
    try:
        return float(s.replace(" ", ""))
    except ValueError:
        return None


def _int_or_none(s: str):
    if not s or not s.strip() or s.strip() in ("----", "****", " "):
        return None
    try:
        return int(s)
    except ValueError:
        return None


def parse_o6(raw: str) -> list[dict] | None:
    """
    O6レコードをパースして analytics.odds_final 用の辞書リストを返す。
    確定オッズ（データ区分 3,4,5）のみ。3連単。
    """
    try:
        b = _to_bytes(raw)
    except Exception:
        return None
    if len(b) < 83280:
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

    # 3連単オッズ: pos 41, 4896回×17B (組番6, オッズ7, 人気4) 組番昇順 01-02-03～18-17-16
    for i in range(4896):
        base = 41 + i * 17
        combo_raw = _sub_bytes(b, base, 6)
        odds_s = _sub_bytes(b, base + 6, 7)
        pop_s = _sub_bytes(b, base + 13, 4)
        if not combo_raw or len(combo_raw) < 6:
            continue
        try:
            a = int(combo_raw[:2])
            b_val = int(combo_raw[2:4])
            c = int(combo_raw[4:6])
            if a < 1 or b_val < 1 or c < 1 or a == b_val or b_val == c or a == c:
                continue
            combination = f"{a}-{b_val}-{c}"
        except (ValueError, TypeError):
            continue
        odds = _num_or_none(odds_s)
        if odds is None or odds <= 0:
            continue
        odds = round(odds / 10, 1)  # JV-Data: 小数点1桁付き（7桁）
        result.append({
            "race_id": race_id,
            "bet_type": "trifecta",
            "combination": combination,
            "odds": odds,
            "popularity": _int_or_none(pop_s),
        })

    return result if result else None
