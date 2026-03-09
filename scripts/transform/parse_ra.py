"""
RA（レース詳細）レコードのパーサー
JV-Data仕様書 ２．レース詳細 に基づく（位置は1始まりのバイト位置）

※JV-Dataはバイト位置指定のため、cp932でバイト列に変換してからパースする。
  文字列のままだと全角文字(2バイト=1文字)で桁ずれが発生する。
"""

# 競馬場コード→場名（JRA中央競馬のみ簡易対応）
VENUE_CODE = {
    "01": "札幌", "02": "函館", "03": "福島", "04": "新潟", "05": "東京",
    "06": "中山", "07": "中京", "08": "京都", "09": "阪神", "10": "小倉",
}
# トラックコード→芝/ダート（コード表2009: 10-22芝, 23-26,29ダート, 27-28サンド, 51-59障害芝）
TRACK_SURFACE = {
    "10": "芝", "11": "芝", "12": "芝", "13": "芝", "14": "芝", "15": "芝", "16": "芝",
    "17": "芝", "18": "芝", "19": "芝", "20": "芝", "21": "芝", "22": "芝",
    "23": "ダート", "24": "ダート", "25": "ダート", "26": "ダート", "29": "ダート",
    "27": "ダート", "28": "ダート",  # サンド→ダートとして扱う
    "51": "芝", "52": "芝", "53": "芝", "54": "芝", "55": "芝", "56": "芝", "57": "芝", "58": "芝", "59": "芝",
}
# トラックコード→方向（左/右/直）
TRACK_DIRECTION = {
    "10": "直", "29": "直",
    "11": "左", "12": "左", "13": "左", "14": "左", "15": "左", "16": "左",
    "23": "左", "25": "左", "27": "左",
    "17": "右", "18": "右", "19": "右", "20": "右", "21": "右", "22": "右",
    "24": "右", "26": "右", "28": "右",
    "51": "左", "52": "左", "53": "左",
    "54": "右",  # 障害・芝（方向未指定）→ 阪神など障害主要コースは右回りでフォールバック
    "55": "右", "56": "右", "57": "右", "58": "左", "59": "右",
}
# 馬場状態（コード表2010: 1=良 2=稍重 3=重 4=不良、0=未設定）
BAAB_CODE = {"1": "良", "2": "稍重", "3": "重", "4": "不良"}
# 天候（コード表2011: 1=晴 2=曇 3=雨 4=小雨 5=雪 6=小雪、0=未設定）
TENKO_CODE = {"1": "晴", "2": "曇", "3": "雨", "4": "小雨", "5": "雪", "6": "小雪"}


def _to_bytes(raw: str) -> bytes:
    """Unicode文字列をcp932バイト列に変換（JV-Data仕様）"""
    return raw.encode("cp932", errors="replace")


def _sub_bytes(b: bytes, start: int, length: int) -> str:
    """1始まりのバイト位置から length バイト切り出し、cp932でデコード"""
    chunk = b[start - 1 : start - 1 + length]
    return chunk.decode("cp932", errors="replace").strip()


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


def _valid_field_count(v: int | None) -> int | None:
    """出走頭数: 1〜24のみ有効"""
    if v is None:
        return None
    if 1 <= v <= 24:
        return v
    return None


def _valid_weather(s: str | None) -> str | None:
    """天候: コード表2011の1-6のみ有効、未設定・不正値はNone"""
    if not s or not s.strip():
        return None
    code = s.strip()
    if code in TENKO_CODE:
        return TENKO_CODE[code]
    # 未設定(0)や不正値(",", "(", ")" 等)はNone
    return None


def _valid_track_condition(baab_s: str, baab_d: str, surface: str | None) -> str | None:
    """馬場状態: 芝なら889(芝馬場)、ダートなら890(ダート馬場)から取得。コード1-4のみ有効"""
    code = ""
    if surface == "芝":
        code = (baab_s or "").strip()
    elif surface in ("ダート",):
        code = (baab_d or "").strip()
    else:
        code = (baab_s or baab_d or "").strip()
    if not code or code == "0":
        return None
    if code in BAAB_CODE:
        return BAAB_CODE[code]
    return None


def parse_ra(raw: str) -> dict | None:
    """
    RAレコードをパースして analytics.races 用の辞書を返す。
    JV-Data仕様はバイト位置のため、cp932バイト列としてパースする。
    """
    try:
        b = _to_bytes(raw)
    except Exception:
        return None
    if len(b) < 890:
        return None
    try:
        year = _sub_bytes(b, 12, 4)
        mmdd = _sub_bytes(b, 16, 4)
        venue_code = _sub_bytes(b, 20, 2)
        kai = _sub_bytes(b, 22, 2)
        nichi = _sub_bytes(b, 24, 2)
        race_no = _sub_bytes(b, 26, 2)
        race_id = f"{year}{mmdd}{venue_code}{kai}{nichi}{race_no}"
        race_date = f"{year}-{mmdd[:2]}-{mmdd[2:]}" if year and mmdd else None
        venue_name = VENUE_CODE.get(venue_code, venue_code or "不明")
        race_name = _sub_bytes(b, 33, 60)
        grade = _sub_bytes(b, 615, 1) or None
        track_code_raw = _sub_bytes(b, 706, 2)
        track_code = track_code_raw.zfill(2) if track_code_raw else None  # "1"→"01"
        surface = TRACK_SURFACE.get(track_code, None) if track_code else None
        direction = TRACK_DIRECTION.get(track_code, None) if track_code else None
        distance = _int_or_none(_sub_bytes(b, 698, 4))
        # 出走頭数(884)を優先、未設定時は登録頭数(882)をフォールバック
        fc1 = _int_or_none(_sub_bytes(b, 884, 2))
        fc2 = _int_or_none(_sub_bytes(b, 882, 2))
        field_count = _valid_field_count(fc1) or _valid_field_count(fc2)
        weather_raw = _sub_bytes(b, 888, 1)
        weather = _valid_weather(weather_raw)
        baab_s = _sub_bytes(b, 889, 1)
        baab_d = _sub_bytes(b, 890, 1)
        track_condition = _valid_track_condition(baab_s, baab_d, surface)
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
            "direction": direction,
            "weather": weather,
            "track_condition": track_condition,
            "field_count": field_count,
        }
    except Exception:
        return None
