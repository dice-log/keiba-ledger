"""
HR（払戻）レコードのパーサー
JV-Data仕様書 ４．払戻 に基づく
"""


def _sub(s: str, start: int, length: int) -> str:
    return s[start - 1 : start - 1 + length].strip()


def _int_or_none(s: str):
    if not s or not s.strip() or s.strip() in ("sp",):
        return None
    try:
        return int(s)
    except ValueError:
        return None


def parse_hr(raw: str) -> list[dict]:
    """
    HRレコードをパースして analytics.payouts 用の辞書のリストを返す。
    単勝・複勝・枠連・馬連・ワイド・馬単・3連複・3連単
    """
    if len(raw) < 720:
        return []
    result = []
    try:
        year = _sub(raw, 12, 4)
        mmdd = _sub(raw, 16, 4)
        venue_code = _sub(raw, 20, 2)
        kai = _sub(raw, 22, 2)
        nichi = _sub(raw, 24, 2)
        race_no = _sub(raw, 26, 2)
        race_id = f"{year}{mmdd}{venue_code}{kai}{nichi}{race_no}"
        # 単勝払戻 103, 3×13
        for i in range(3):
            base = 103 + i * 13
            uma = _sub(raw, base + 1, 2)
            payout = _int_or_none(_sub(raw, base + 3, 9))
            pop = _int_or_none(_sub(raw, base + 12, 2))
            if uma and uma != "00" and payout is not None:
                result.append({"race_id": race_id, "bet_type": "WIN", "combination": uma, "payout": payout, "popularity": pop})
        # 複勝 142, 5×13
        for i in range(5):
            base = 142 + i * 13
            uma = _sub(raw, base + 1, 2)
            payout = _int_or_none(_sub(raw, base + 3, 9))
            pop = _int_or_none(_sub(raw, base + 12, 2))
            if uma and uma != "00" and payout is not None:
                result.append({"race_id": race_id, "bet_type": "PLACE", "combination": uma, "payout": payout, "popularity": pop})
        # 枠連 207, 3×13
        for i in range(3):
            base = 207 + i * 13
            kumi = _sub(raw, base + 1, 2)
            payout = _int_or_none(_sub(raw, base + 3, 9))
            pop = _int_or_none(_sub(raw, base + 12, 2))
            if kumi and kumi != "00" and payout is not None:
                result.append({"race_id": race_id, "bet_type": "FRAME_QUINELLA", "combination": kumi, "payout": payout, "popularity": pop})
        # 馬連 246, 3×16
        for i in range(3):
            base = 246 + i * 16
            kumi = _sub(raw, base + 1, 4)
            payout = _int_or_none(_sub(raw, base + 5, 9))
            pop = _int_or_none(_sub(raw, base + 14, 3))
            if kumi and kumi != "0000" and payout is not None:
                a, b = kumi[:2], kumi[2:]
                result.append({"race_id": race_id, "bet_type": "QUINELLA", "combination": f"{a}-{b}" if a != b else a, "payout": payout, "popularity": pop})
        # 馬単 454, 6×16
        for i in range(6):
            base = 454 + i * 16
            kumi = _sub(raw, base + 1, 4)
            payout = _int_or_none(_sub(raw, base + 5, 9))
            pop = _int_or_none(_sub(raw, base + 14, 3))
            if kumi and kumi != "0000" and payout is not None:
                result.append({"race_id": race_id, "bet_type": "EXACTA", "combination": f"{kumi[:2]}-{kumi[2:]}", "payout": payout, "popularity": pop})
        # 3連複 550, 3×18
        for i in range(3):
            base = 550 + i * 18
            kumi = _sub(raw, base + 1, 6)
            payout = _int_or_none(_sub(raw, base + 7, 9))
            pop = _int_or_none(_sub(raw, base + 16, 3))
            if kumi and kumi != "000000" and payout is not None:
                nums = [kumi[j:j+2] for j in range(0, 6, 2)]
                result.append({"race_id": race_id, "bet_type": "TRIO", "combination": "-".join(sorted(nums)), "payout": payout, "popularity": pop})
        # 3連単 604, 6×19
        for i in range(6):
            base = 604 + i * 19
            kumi = _sub(raw, base + 1, 6)
            payout = _int_or_none(_sub(raw, base + 7, 9))
            pop = _int_or_none(_sub(raw, base + 16, 4))
            if kumi and kumi != "000000" and payout is not None:
                result.append({"race_id": race_id, "bet_type": "TRIFECTA", "combination": f"{kumi[:2]}-{kumi[2:4]}-{kumi[4:6]}", "payout": payout, "popularity": pop})
    except Exception:
        pass
    return result
