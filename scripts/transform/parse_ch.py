"""
CH（調教師マスタ）レコードのパーサー
JV-Data仕様書 １５．調教師マスタ に基づく（位置は1始まりのバイト位置）

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


def _date_or_none(s: str):
    """yyyymmdd を DATE 用文字列に。無効なら None"""
    if not s or len(s) != 8:
        return None
    try:
        y, m, d = int(s[:4]), int(s[4:6]), int(s[6:8])
        if 1 <= m <= 12 and 1 <= d <= 31:
            return f"{y:04d}-{m:02d}-{d:02d}"
    except (ValueError, TypeError):
        pass
    return None


# 東西所属コード 2301: 1=美浦 2=栗東 3=地方招待 4=外国招待
BELONG_CODE = {"1": "美浦", "2": "栗東", "3": "地方招待", "4": "外国招待"}


def parse_ch(raw: str) -> dict | None:
    """
    CHレコードをパースして analytics.trainers 用の辞書を返す。
    データ区分=0（削除）の場合は None を返す。
    """
    b = _to_bytes(raw)
    if len(b) < 200:
        return None
    try:
        data_kbn = _sub_bytes(b, 3, 1)
        if data_kbn == "0":
            return None  # 該当レコード削除

        trainer_id = _sub_bytes(b, 12, 5)
        if not trainer_id or trainer_id == "00000":
            return None

        name = _sub_bytes(b, 42, 34)
        if not name:
            return None

        name_kana = _sub_bytes(b, 76, 30) or None
        name_abbr = _sub_bytes(b, 106, 8) or None
        birth_date = _date_or_none(_sub_bytes(b, 34, 8))
        retired_code = _sub_bytes(b, 17, 1)
        retired = retired_code == "1" if retired_code else False
        belong_code = _sub_bytes(b, 195, 1)
        belong_to = BELONG_CODE.get(belong_code) if belong_code else None

        return {
            "trainer_id": trainer_id,
            "name": name,
            "name_kana": name_kana,
            "name_abbr": name_abbr,
            "belong_to": belong_to,
            "birth_date": birth_date,
            "retired": retired,
        }
    except Exception:
        return None
