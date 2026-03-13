"""
JV-Data レコード（RA/SE/HR）を1フィールドずつ表示するツール

使用例:
  python scripts/tools/show_record_fields.py data/fetch_20260310_094540.jsonl 0
  python scripts/tools/show_record_fields.py data/fetch_20260310_094540.jsonl --limit 3
  type data\fetch_20260310_094540.jsonl | python scripts/tools/show_record_fields.py - 0
"""

import json
import sys
from pathlib import Path

# JV-Data は cp932 バイト位置指定
def _to_bytes(raw: str) -> bytes:
    return raw.encode("cp932", errors="replace")


def _sub_bytes(b: bytes, start: int, length: int) -> str:
    """1始まりのバイト位置から length バイト切り出し"""
    if not b or start < 1:
        return ""
    chunk = b[start - 1 : start - 1 + length]
    return chunk.decode("cp932", errors="replace").rstrip()


# RA: レース詳細 (1272 byte) - JV-Data仕様書 ２．レース詳細
RA_FIELDS = [
    (1, 2, "レコード種別ID"),
    (3, 1, "データ区分"),
    (4, 8, "データ作成年月日"),
    (12, 4, "開催年"),
    (16, 4, "開催月日"),
    (20, 2, "競馬場コード"),
    (22, 2, "開催回"),
    (24, 2, "開催日目"),
    (26, 2, "レース番号"),
    (28, 1, "曜日コード"),
    (29, 4, "特別競走番号"),
    (33, 60, "競走名本題"),
    (93, 60, "競走名副題"),
    (153, 60, "競走名カッコ内"),
    (213, 120, "競走名本題欧字"),
    (333, 120, "競走名副題欧字"),
    (453, 120, "競走名カッコ内欧字"),
    (573, 20, "競走名略称10文字"),
    (593, 12, "競走名略称6文字"),
    (605, 6, "競走名略称3文字"),
    (611, 1, "競走名区分"),
    (612, 3, "重賞回次"),
    (615, 1, "グレードコード"),
    (616, 1, "変更前グレードコード"),
    (617, 2, "競走種別コード"),
    (619, 3, "競走記号コード"),
    (622, 1, "重量種別コード"),
    (623, 3, "競走条件コード2歳"),
    (626, 3, "競走条件コード3歳"),
    (629, 3, "競走条件コード4歳"),
    (632, 3, "競走条件コード5歳以上"),
    (635, 3, "競走条件コード最若年"),
    (638, 60, "競走条件名称"),
    (698, 4, "距離"),
    (702, 4, "変更前距離"),
    (706, 2, "トラックコード"),
    (708, 2, "変更前トラックコード"),
    (710, 2, "コース区分"),
    (714, 56, "本賞金(7×8)"),
    (770, 40, "変更前本賞金"),
    (810, 40, "付加賞金"),
    (850, 24, "変更前付加賞金"),
    (874, 4, "発走時刻"),
    (878, 4, "変更前発走時刻"),
    (882, 2, "登録頭数"),
    (884, 2, "出走頭数"),
    (886, 2, "入線頭数"),
    (888, 1, "天候コード"),
    (889, 1, "芝馬場状態コード"),
    (890, 1, "ダート馬場状態コード"),
]

# SE: 馬毎レース情報 (555 byte)
SE_FIELDS = [
    (1, 2, "レコード種別ID"),
    (3, 1, "データ区分"),
    (4, 8, "データ作成年月日"),
    (12, 4, "開催年"),
    (16, 4, "開催月日"),
    (20, 2, "競馬場コード"),
    (22, 2, "開催回"),
    (24, 2, "開催日目"),
    (26, 2, "レース番号"),
    (28, 1, "枠番"),
    (29, 2, "馬番"),
    (31, 10, "血統登録番号"),
    (41, 36, "馬名"),
    (77, 2, "馬記号コード"),
    (79, 1, "性別コード"),
    (80, 1, "品種コード"),
    (81, 2, "毛色コード"),
    (83, 2, "馬齢"),
    (85, 1, "東西所属コード"),
    (86, 5, "調教師コード"),
    (91, 8, "調教師名略称"),
    (99, 6, "馬主コード"),
    (105, 64, "馬主名"),
    (169, 60, "服色標示"),
    (289, 3, "負担重量"),
    (292, 3, "変更前負担重量"),
    (295, 1, "ブリンカー使用区分"),
    (297, 5, "騎手コード"),
    (302, 5, "変更前騎手コード"),
    (307, 8, "騎手名略称"),
    (315, 8, "変更前騎手名略称"),
    (323, 1, "騎手見習コード"),
    (325, 3, "馬体重"),
    (328, 1, "増減符号"),
    (329, 3, "増減差"),
    (332, 1, "異常区分コード"),
    (333, 2, "入線順位"),
    (335, 2, "確定着順"),
    (337, 1, "同着区分"),
    (338, 1, "同着頭数"),
    (339, 4, "走破タイム"),
    (343, 3, "着差コード"),
    (352, 2, "1コーナー順位"),
    (354, 2, "2コーナー順位"),
    (356, 2, "3コーナー順位"),
    (358, 2, "4コーナー順位"),
    (360, 4, "単勝オッズ"),
    (364, 2, "単勝人気順"),
]

# HR: 払戻 (719 byte)
HR_FIELDS = [
    (1, 2, "レコード種別ID"),
    (3, 1, "データ区分"),
    (4, 8, "データ作成年月日"),
    (12, 4, "開催年"),
    (16, 4, "開催月日"),
    (20, 2, "競馬場コード"),
    (22, 2, "開催回"),
    (24, 2, "開催日目"),
    (26, 2, "レース番号"),
    (28, 2, "登録頭数"),
    (30, 2, "出走頭数"),
    (32, 1, "不成立フラグ_単勝"),
    (33, 1, "不成立フラグ_複勝"),
    (34, 1, "不成立フラグ_枠連"),
    (35, 1, "不成立フラグ_馬連"),
    (36, 1, "不成立フラグ_ワイド"),
    (38, 1, "不成立フラグ_馬単"),
    (39, 1, "不成立フラグ_3連複"),
    (40, 1, "不成立フラグ_3連単"),
    (41, 1, "特払フラグ_単勝"),
    (42, 1, "特払フラグ_複勝"),
    (43, 1, "特払フラグ_枠連"),
    (44, 1, "特払フラグ_馬連"),
    (45, 1, "特払フラグ_ワイド"),
    (47, 1, "特払フラグ_馬単"),
    (48, 1, "特払フラグ_3連複"),
    (49, 1, "特払フラグ_3連単"),
    (103, 3, "単勝_馬番1"),
    (112, 9, "単勝_払戻1"),
    (121, 2, "単勝_人気1"),
]

FIELDS_MAP = {"RA": RA_FIELDS, "SE": SE_FIELDS, "HR": HR_FIELDS}


def show_record(record_type: str, raw_text: str, index: int = 0) -> None:
    """1レコードを1フィールドずつ表示"""
    fields = FIELDS_MAP.get(record_type)
    if not fields:
        print(f"[?] 未対応のレコード種別: {record_type}")
        return
    try:
        b = _to_bytes(raw_text)
    except Exception as e:
        print(f"[NG] エンコードエラー: {e}")
        return
    print(f"--- {record_type} レコード #{index} (全{len(b)}バイト) ---")
    for start, length, name in fields:
        if start - 1 + length > len(b):
            continue
        val = _sub_bytes(b, start, length)
        display = repr(val) if "\n" in val or "\t" in val else val
        if not display:
            display = "(空)"
        print(f"  {name:24} [{start:4}-{start+length-1:4}] = {display}")


def main():
    import argparse
    parser = argparse.ArgumentParser(description="JV-Data レコードを1フィールドずつ表示")
    parser.add_argument("input", nargs="?", default="-", help="JSONL ファイルパス（- で標準入力）")
    parser.add_argument("index", nargs="?", type=int, default=0, help="表示するレコード番号（0始まり）")
    parser.add_argument("--limit", type=int, default=0, help="先頭N件まで表示（0=indexの1件のみ）")
    parser.add_argument("--type", dest="record_type", choices=["RA", "SE", "HR"], default=None, help="対象レコード種別（指定時は最初に該当したもの）")
    args = parser.parse_args()

    if args.input == "-":
        lines = sys.stdin
    else:
        path = Path(args.input)
        if not path.exists():
            print(f"[NG] ファイルが存在しません: {path}")
            sys.exit(1)
        lines = path.open(encoding="utf-8")

    records = []
    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
            rt = obj.get("record_type", "")
            raw = obj.get("raw_text", "")
            if args.record_type and rt != args.record_type:
                continue
            if rt in FIELDS_MAP and raw:
                records.append((rt, raw))
        except json.JSONDecodeError:
            continue

    if not records:
        print("[?] 該当レコードがありません")
        return

    if args.limit > 0:
        to_show = records[: args.limit]
    else:
        idx = args.index
        if idx >= len(records):
            print(f"[?] インデックス {idx} は範囲外（0〜{len(records)-1}）")
            return
        to_show = [records[idx]]

    for i, (rt, raw) in enumerate(to_show):
        show_record(rt, raw, i)
        if i < len(to_show) - 1:
            print()


if __name__ == "__main__":
    main()
