"""
O1（単複枠オッズ）レコードのパーサー
JV-Data仕様書 ７．オッズ1（単複枠）に基づく（962バイト）

単勝・複勝・枠連の確定オッズを analytics.odds_final 用に返す。
データ区分 3=最終, 4=確定, 5=確定(月曜) のみ処理。中間・前日はスキップ。
"""


def _to_bytes(raw: str) -> bytes:
    return raw.encode("cp932", errors="replace")


def _sub_bytes(b: bytes, start: int, length: int) -> str:
    if not b or start < 1:
        return ""
    chunk = b[start - 1 : start - 1 + length]
    return chunk.decode("cp932", errors="replace").strip()


def _num_or_none(s: str):
    if not s or not s.strip() or s.strip() in ("----", "****", "    ", "0000", "00000"):
        return None
    try:
        return float(s.replace(" ", ""))
    except ValueError:
        return None


def _int_or_none(s: str):
    if not s or not s.strip() or s.strip() in ("--", "**", " "):
        return None
    try:
        return int(s)
    except ValueError:
        return None


def parse_o1(raw: str) -> list[dict] | None:
    """
    O1レコードをパースして analytics.odds_final 用の辞書リストを返す。
    確定オッズ（データ区分 3,4,5）のみ。単勝・複勝・枠連。
    """
    try:
        b = _to_bytes(raw)
    except Exception:
        return None
    if len(b) < 610:
        return None

    data_kbn = _sub_bytes(b, 3, 1)
    if data_kbn not in ("3", "4", "5"):
        return None  # 中間1, 前日2, 中止9, 削除0 はスキップ

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

    # 単勝オッズ: pos 44, 28回×8B (馬番2, オッズ4, 人気2)
    for i in range(28):
        base = 44 + i * 8
        uma = _sub_bytes(b, base, 2)
        odds_s = _sub_bytes(b, base + 2, 4)
        pop_s = _sub_bytes(b, base + 6, 2)
        if not uma or uma == "  ":
            continue
        uma_int = _int_or_none(uma)
        if uma_int is None or uma_int <= 0:
            continue
        odds = _num_or_none(odds_s)
        if odds is None or odds <= 0:
            continue
        result.append({
            "race_id": race_id,
            "bet_type": "win",
            "combination": str(uma_int),
            "odds": odds,
            "popularity": _int_or_none(pop_s),
        })

    # 複勝オッズ: pos 268, 28回×12B (馬番2, 最低4, 最高4, 人気2)
    for i in range(28):
        base = 268 + i * 12
        uma = _sub_bytes(b, base, 2)
        low_s = _sub_bytes(b, base + 2, 4)
        high_s = _sub_bytes(b, base + 6, 4)
        pop_s = _sub_bytes(b, base + 10, 2)
        if not uma or uma == "  ":
            continue
        uma_int = _int_or_none(uma)
        if uma_int is None or uma_int <= 0:
            continue
        low = _num_or_none(low_s)
        high = _num_or_none(high_s)
        odds = (low + high) / 2 if (low and high) else (low or high)
        if odds is None or odds <= 0:
            continue
        result.append({
            "race_id": race_id,
            "bet_type": "place",
            "combination": str(uma_int),
            "odds": odds,
            "popularity": _int_or_none(pop_s),
        })

    # 枠連オッズ: pos 604, 36回×9B (組番2 e.g. "12"=1-2, オッズ5, 人気2)
    for i in range(36):
        base = 604 + i * 9
        combo = _sub_bytes(b, base, 2)
        odds_s = _sub_bytes(b, base + 2, 5)
        pop_s = _sub_bytes(b, base + 7, 2)
        if not combo or len(combo) < 2:
            continue
        try:
            w1, w2 = int(combo[0]), int(combo[1])
            if w1 < 1 or w1 > 8 or w2 < 1 or w2 > 8:
                continue
            combination = f"{min(w1,w2)}-{max(w1,w2)}"
        except (ValueError, IndexError):
            continue
        odds = _num_or_none(odds_s)
        if odds is None or odds <= 0:
            continue
        result.append({
            "race_id": race_id,
            "bet_type": "bracket",
            "combination": combination,
            "odds": odds,
            "popularity": _int_or_none(pop_s),
        })

    return result if result else None
