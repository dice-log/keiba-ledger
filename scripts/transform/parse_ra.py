"""
RA（レース詳細）レコードのパーサー
JV-Data仕様書 ２．レース詳細 に基づく（位置は1始まり）
"""

# 競馬場コード→場名（JRA中央競馬のみ簡易対応）
VENUE_CODE = {
    "01": "札幌", "02": "函館", "03": "福島", "04": "新潟", "05": "東京",
    "06": "中山", "07": "中京", "08": "京都", "09": "阪神", "10": "小倉",
}
# トラックコード→芝/ダート
TRACK_CODE = {"11": "芝", "12": "芝", "13": "芝", "14": "芝", "15": "芝", "16": "芝",
              "21": "ダート", "22": "ダート", "23": "ダート", "24": "ダート",
              "31": "障害", "32": "障害", "33": "障害", "34": "障害", "35": "障害", "36": "障害"}
# 馬場状態
BAAB_CODE = {"1": "良", "2": "稍重", "3": "重", "4": "不良"}
# 天候
TENKO_CODE = {"1": "晴", "2": "曇", "3": "雨", "4": "小雨", "5": "雪"}


def _sub(s: str, start: int, length: int) -> str:
    """1始まりで start から length 文字"""
    return s[start - 1 : start - 1 + length].strip()


def _int_or_none(s: str):
    if not s or not s.strip():
        return None
    try:
        return int(s)
    except ValueError:
        return None


def _num_or_none(s: str):
    if not s or not s.strip():
        return None
    try:
        return float(s)
    except ValueError:
        return None


def parse_ra(raw: str) -> dict | None:
    """
    RAレコードをパースして analytics.races 用の辞書を返す。
    """
    if len(raw) < 890:
        return None
    try:
        year = _sub(raw, 12, 4)
        mmdd = _sub(raw, 16, 4)
        venue_code = _sub(raw, 20, 2)
        kai = _sub(raw, 22, 2)
        nichi = _sub(raw, 24, 2)
        race_no = _sub(raw, 26, 2)
        race_id = f"{year}{mmdd}{venue_code}{kai}{nichi}{race_no}"
        race_date = f"{year}-{mmdd[:2]}-{mmdd[2:]}" if year and mmdd else None
        venue_name = VENUE_CODE.get(venue_code, venue_code or "不明")
        race_name = _sub(raw, 33, 60)
        grade = _sub(raw, 615, 1) or None
        track_code = _sub(raw, 706, 2)
        surface = TRACK_CODE.get(track_code, "芝" if track_code in ("11","12","13","14","15","16") else "ダート" if track_code in ("21","22","23","24") else None)
        distance = _int_or_none(_sub(raw, 698, 4))
        field_count = _int_or_none(_sub(raw, 884, 2)) or _int_or_none(_sub(raw, 882, 2))
        weather = TENKO_CODE.get(_sub(raw, 888, 1), _sub(raw, 888, 1) or None)
        baab_s = _sub(raw, 889, 1)
        baab_d = _sub(raw, 890, 1)
        track_condition = BAAB_CODE.get(baab_s or baab_d, baab_s or baab_d or None)
        return {
            "race_id": race_id,
            "race_date": race_date,
            "venue_code": venue_code,
            "venue_name": venue_name,
            "race_number": _int_or_none(race_no),
            "race_name": race_name or None,
            "grade": grade,
            "surface": surface,
            "distance": distance,
            "weather": weather,
            "track_condition": track_condition,
            "field_count": field_count,
        }
    except Exception:
        return None
