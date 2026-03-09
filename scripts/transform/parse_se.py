"""
SE（馬毎レース情報）レコードのパーサー
JV-Data仕様書 ３．馬毎レース情報 に基づく（位置は1始まりのバイト位置）

※JV-Dataはバイト位置指定のため、cp932でバイト列に変換してからパースする。
  文字列のままだと全角文字(2バイト=1文字)で桁ずれが発生する。
"""

RUNNING_STYLE = {"1": "逃げ", "2": "先行", "3": "差し", "4": "追込"}


def _to_bytes(raw: str) -> bytes:
    """Unicode文字列をcp932バイト列に変換（JV-Data仕様）"""
    return raw.encode("cp932", errors="replace")


def _sub_bytes(b: bytes, start: int, length: int) -> str:
    """1始まりのバイト位置から length バイト切り出し、cp932でデコード"""
    if not b or start < 1:
        return ""
    chunk = b[start - 1 : start - 1 + length]
    return chunk.decode("cp932", errors="replace").strip()


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
    """増減符号+増減差から馬体重増減を算出
    JV-Data: 000=前差なし(0), スペース=初出走・出走取消(None), 001-998=有効値
    """
    s = (diff or "").strip()
    if not s or s == "sp" or s == "999":
        return None  # 初出走・出走取消・計量不能
    if s == "000":
        return 0  # 前差なし＝増減なし
    try:
        d = int(s)
        return -d if sign == "-" else d
    except ValueError:
        return None


def parse_se(raw: str) -> dict | None:
    """
    SEレコードをパースして analytics.race_entries 用の辞書を返す。
    """
    b = _to_bytes(raw)
    if len(b) < 98:
        return None
    try:
        year = _sub_bytes(b, 12, 4)
        mmdd = _sub_bytes(b, 16, 4)
        venue_code = _sub_bytes(b, 20, 2)
        kai = _sub_bytes(b, 22, 2)
        nichi = _sub_bytes(b, 24, 2)
        race_no = _sub_bytes(b, 26, 2)
        race_id = f"{year}{mmdd}{venue_code}{kai}{nichi}{race_no}"
        frame_number = _int_or_none(_sub_bytes(b, 28, 1))
        horse_number = _int_or_none(_sub_bytes(b, 29, 2))
        horse_id = _sub_bytes(b, 31, 10)
        horse_name = _sub_bytes(b, 41, 36)
        trainer_id = _sub_bytes(b, 86, 5) or None
        trainer_name = _sub_bytes(b, 91, 8) or None
        weight_carried = _num_or_none(_sub_bytes(b, 289, 3))
        if weight_carried is not None:
            weight_carried = round(weight_carried / 10, 1)
        jockey_id = _sub_bytes(b, 297, 5) or None
        jockey_name = _sub_bytes(b, 307, 8) or None
        horse_weight = _int_or_none(_sub_bytes(b, 325, 3))
        weight_diff_val = _weight_diff(_sub_bytes(b, 328, 1), _sub_bytes(b, 329, 3))
        finish_pos = _int_or_none(_sub_bytes(b, 335, 2)) or _int_or_none(_sub_bytes(b, 333, 2))
        finish_time_raw = _sub_bytes(b, 339, 4)
        finish_time = None
        if finish_time_raw and finish_time_raw not in ("0000", "9999", "    "):
            try:
                m, s1, s2 = int(finish_time_raw[0]), int(finish_time_raw[1:3]), int(finish_time_raw[3])
                finish_time = round(m * 60 + s1 + s2 / 10, 1)
            except (ValueError, IndexError):
                pass
        # 後3ハロンタイム: JV-Data仕様「単位:99.9秒」＝整数で小数第1位まで格納（385→38.5秒）
        _l3f = _num_or_none(_sub_bytes(b, 391, 3))
        if _l3f is not None and _l3f not in (999, 99.9):
            last_3f = round(_l3f / 10, 1)
        else:
            last_3f = None
        # 単勝オッズ: JV-Data仕様「999.9倍で設定」＝整数で小数第1位まで格納（1856→185.6倍）
        _win = _num_or_none(_sub_bytes(b, 360, 4))
        win_odds = round(_win / 10, 1) if _win is not None and _win > 0 else None
        popularity = _int_or_none(_sub_bytes(b, 364, 2))
        running_style = RUNNING_STYLE.get(_sub_bytes(b, 553, 1)) if len(b) >= 553 else None
        c1 = _sub_bytes(b, 352, 2) if len(b) >= 353 else ""
        c2 = _sub_bytes(b, 354, 2) if len(b) >= 355 else ""
        c3 = _sub_bytes(b, 356, 2) if len(b) >= 357 else ""
        c4 = _sub_bytes(b, 358, 2) if len(b) >= 359 else ""
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
