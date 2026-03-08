"""
SE（馬毎レース情報）レコードのパーサー
JV-Data仕様書 ３．馬毎レース情報 に基づく
"""

RUNNING_STYLE = {"1": "逃げ", "2": "先行", "3": "差し", "4": "追込"}


def _sub(s: str, start: int, length: int) -> str:
    return s[start - 1 : start - 1 + length].strip()


def _int_or_none(s: str):
    if not s or not s.strip() or s.strip() in ("sp", "999", "000"):
        return None
    try:
        return int(s)
    except ValueError:
        return None


def _num_or_none(s: str):
    if not s or not s.strip() or s.strip() in ("sp", "999", "000"):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _weight_diff(sign: str, diff: str) -> int | None:
    """増減符号+増減差から馬体重増減を算出"""
    d = _int_or_none(diff)
    if d is None:
        return None
    if sign == "-":
        return -d
    return d


def parse_se(raw: str) -> dict | None:
    """
    SEレコードをパースして analytics.race_entries 用の辞書を返す。
    """
    if len(raw) < 395:
        return None
    try:
        year = _sub(raw, 12, 4)
        mmdd = _sub(raw, 16, 4)
        venue_code = _sub(raw, 20, 2)
        kai = _sub(raw, 22, 2)
        nichi = _sub(raw, 24, 2)
        race_no = _sub(raw, 26, 2)
        race_id = f"{year}{mmdd}{venue_code}{kai}{nichi}{race_no}"
        frame_number = _int_or_none(_sub(raw, 28, 1))
        horse_number = _int_or_none(_sub(raw, 29, 2))
        horse_id = _sub(raw, 31, 10)
        horse_name = _sub(raw, 41, 36)
        trainer_id = _sub(raw, 86, 5) or None
        trainer_name = _sub(raw, 91, 8) or None
        weight_carried = _num_or_none(_sub(raw, 289, 3))
        if weight_carried is not None:
            weight_carried = round(weight_carried / 10, 1)
        jockey_id = _sub(raw, 297, 5) or None
        jockey_name = _sub(raw, 307, 8) or None
        horse_weight = _int_or_none(_sub(raw, 325, 3))
        weight_diff_val = _weight_diff(_sub(raw, 328, 1), _sub(raw, 329, 3))
        finish_pos = _int_or_none(_sub(raw, 335, 2)) or _int_or_none(_sub(raw, 333, 2))
        finish_time_raw = _sub(raw, 339, 4)
        finish_time = None
        if finish_time_raw and finish_time_raw not in ("0000", "9999", "    "):
            try:
                m, s1, s2 = int(finish_time_raw[0]), int(finish_time_raw[1:3]), int(finish_time_raw[3])
                finish_time = round(m * 60 + s1 + s2 / 10, 1)
            except (ValueError, IndexError):
                pass
        last_3f = _num_or_none(_sub(raw, 391, 3))
        if last_3f is not None and last_3f != 99.9:
            last_3f = round(last_3f, 1)
        else:
            last_3f = None
        win_odds = _num_or_none(_sub(raw, 360, 4))
        popularity = _int_or_none(_sub(raw, 364, 2))
        running_style = RUNNING_STYLE.get(_sub(raw, 553, 1))
        c1 = _sub(raw, 352, 2)
        c2 = _sub(raw, 354, 2)
        c3 = _sub(raw, 356, 2)
        c4 = _sub(raw, 358, 2)
        corner_pos = "-".join(x for x in [c1, c2, c3, c4] if x) or None
        return {
            "race_id": race_id,
            "horse_id": horse_id,
            "horse_name": horse_name or "不明",
            "horse_number": horse_number,
            "frame_number": frame_number,
            "jockey_id": jockey_id,
            "jockey_name": jockey_name,
            "trainer_id": trainer_id,
            "trainer_name": trainer_name,
            "weight_carried": weight_carried,
            "horse_weight": horse_weight,
            "weight_diff": weight_diff_val,
            "finish_pos": finish_pos,
            "finish_time": finish_time,
            "last_3f": last_3f,
            "win_odds": win_odds,
            "popularity": popularity,
            "running_style": running_style,
            "corner_pos": corner_pos,
        }
    except Exception:
        return None
