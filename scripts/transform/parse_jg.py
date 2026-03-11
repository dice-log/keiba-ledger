"""
JG（競走馬除外情報）レコードのパーサー
JV-Data仕様書 ３１．競走馬除外情報 に基づく（位置は1始まりのバイト位置、レコード長80バイト）

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


# 出走区分: 1=投票馬 2=締切での除外馬 4=再投票馬 5=再投票除外馬 6=馬番を付さない出走取消馬 9=取消馬
EXCLUSION_TYPE = {
    "1": "投票馬",
    "2": "締切除外",
    "4": "再投票馬",
    "5": "再投票除外",
    "6": "馬番なし取消",
    "9": "取消馬",
}
# 除外状態区分: 1=非抽選馬 2=非当選馬
LOTTERY_STATUS = {"1": "非抽選馬", "2": "非当選馬"}


def parse_jg(raw: str) -> dict | None:
    """
    JGレコードをパースして analytics.horse_exclusions 用の辞書を返す。
    データ区分=0（削除）の場合は None を返す。
    race_id は RA/SE/HR と同一形式（yyyy + mmdd + venue + kai + nichi + race_no）。
    """
    b = _to_bytes(raw)
    if len(b) < 78:
        return None
    try:
        data_kbn = _sub_bytes(b, 3, 1)
        if data_kbn == "0":
            return None  # 該当レコード削除

        year = _sub_bytes(b, 12, 4)
        mmdd = _sub_bytes(b, 16, 4)
        venue_code = _sub_bytes(b, 20, 2)
        kai = _sub_bytes(b, 22, 2)
        nichi = _sub_bytes(b, 24, 2)
        race_no = _sub_bytes(b, 26, 2)
        if not (year and mmdd and venue_code and kai and nichi and race_no):
            return None

        race_id = f"{year}{mmdd}{venue_code}{kai}{nichi}{race_no}"
        horse_id = _sub_bytes(b, 28, 10)
        if not horse_id or horse_id == "0000000000":
            return None

        horse_name = _sub_bytes(b, 38, 36) or None
        exclusion_code = _sub_bytes(b, 77, 1)
        exclusion_type = EXCLUSION_TYPE.get(exclusion_code, exclusion_code) if exclusion_code else None
        lottery_code = _sub_bytes(b, 78, 1)
        lottery_status = LOTTERY_STATUS.get(lottery_code, lottery_code) if lottery_code else None

        return {
            "race_id": race_id,
            "horse_id": horse_id,
            "horse_name": horse_name,
            "exclusion_type": exclusion_type,
            "lottery_status": lottery_status,
        }
    except Exception:
        return None
