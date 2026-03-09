"""
UM（競走馬マスタ）レコードのパーサー
JV-Data仕様書 １３．競走馬マスタ に基づく（位置は1始まりのバイト位置）

※JV-Dataはバイト位置指定のため、cp932でバイト列に変換してからパースする。

Ver.4.9.0 (2023年8月) で競走馬マスタのレイアウト変更あり:
- 3代血統: 繁殖登録番号 8→10 byte (ブロック 44→46 byte)
- 生産者: コード 6→8 byte, 名称 70→72 byte
旧データはレコード長で判定し、旧オフセットでパースする。
"""


# 旧フォーマット判定: 新仕様レコード長 1609 + CRLF2 = 1611。旧は約 1579。
_UM_OLD_RECORD_LEN_THRESHOLD = 1590


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


# 性別コード 2202: 1=牡 2=牝 3=セ
SEX_CODE = {"1": "牡", "2": "牝", "3": "セ"}

# 毛色コード 2203 主要: 01=鹿毛 02=黒鹿毛 03=青鹿毛 ...
COAT_CODE = {
    "01": "鹿毛", "02": "黒鹿毛", "03": "青鹿毛", "04": "青毛", "05": "芦毛",
    "06": "栗毛", "07": "栃栗毛", "08": "白毛", "09": "黒鹿毛", "10": "青鹿毛",
}


def parse_um(raw: str) -> dict | None:
    """
    UMレコードをパースして analytics.horses 用の辞書を返す。
    データ区分=0（削除）の場合は None を返す。
    """
    b = _to_bytes(raw)
    if len(b) < 260:
        return None
    try:
        data_kbn = _sub_bytes(b, 3, 1)
        if data_kbn == "0":
            return None  # 該当レコード削除

        horse_id = _sub_bytes(b, 12, 10)
        if not horse_id or horse_id == "0000000000":
            return None

        name = _sub_bytes(b, 47, 36)
        if not name:
            return None

        name_kana = _sub_bytes(b, 83, 36) or None
        birth_date = _date_or_none(_sub_bytes(b, 39, 8))
        sex_code = _sub_bytes(b, 201, 1)
        sex = SEX_CODE.get(sex_code) if sex_code else None
        coat_code = _sub_bytes(b, 203, 2)
        coat_color = COAT_CODE.get(coat_code, coat_code) if coat_code else None

        # Ver.4.9.0 前後でレイアウトが異なる。レコード長で旧/新を判定
        n = len(b)
        is_old = n < _UM_OLD_RECORD_LEN_THRESHOLD

        if is_old:
            # 旧: 繁殖登録番号8B, 生産者6+70B。3代血統ブロック44B（id 8B + 馬名 36B）
            sire_id = _sub_bytes(b, 205, 8) or None
            if sire_id == "00000000":
                sire_id = None
            sire_name = _sub_bytes(b, 213, 36) or None  # 205+8 直後
            dam_id = _sub_bytes(b, 249, 8) or None     # 205+44
            if dam_id == "00000000":
                dam_id = None
            dam_name = _sub_bytes(b, 257, 36) or None  # 249+8 直後
            broodmare_sire = _sub_bytes(b, 389, 36) or None  # 381+8 直後（5ブロック目）
            trainer_id_raw = _sub_bytes(b, 822, 5)    # 821+1
            owner_name = _sub_bytes(b, 957, 64) or None
            breeder_name = _sub_bytes(b, 861, 70) or None
        else:
            # 新: 繁殖登録番号10B, 生産者8+72B。3代血統ブロック46B（id 10B + 馬名 36B）
            sire_id = _sub_bytes(b, 205, 10) or None
            if sire_id == "0000000000":
                sire_id = None
            sire_name = _sub_bytes(b, 215, 36) or None  # 205+10 直後
            dam_id = _sub_bytes(b, 251, 10) or None
            if dam_id == "0000000000":
                dam_id = None
            dam_name = _sub_bytes(b, 261, 36) or None   # 251+10 直後
            broodmare_sire = _sub_bytes(b, 399, 36) or None  # 389+10 直後（5ブロック目）
            trainer_id_raw = _sub_bytes(b, 850, 5)
            owner_name = _sub_bytes(b, 989, 64) or None
            breeder_name = _sub_bytes(b, 891, 72) or None

        trainer_id = trainer_id_raw or None
        if trainer_id == "00000":
            trainer_id = None

        return {
            "horse_id": horse_id,
            "name": name,
            "name_kana": name_kana,
            "birth_date": birth_date,
            "sex": sex,
            "coat_color": coat_color,
            "sire_id": sire_id,
            "sire_name": sire_name,
            "dam_id": dam_id,
            "dam_name": dam_name,
            "broodmare_sire": broodmare_sire,
            "trainer_id": trainer_id,
            "owner_name": owner_name,
            "breeder_name": breeder_name,
        }
    except Exception:
        return None
