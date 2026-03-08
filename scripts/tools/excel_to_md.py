# -*- coding: utf-8 -*-
"""JV-Data4901.xlsx を Markdown に変換（見やすく整理）"""
import pandas as pd
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
IN_PATH = Path(r"C:\Users\Dice\keiba_project\docs\JV-Data4901.xlsx")
OUT_PATH = ROOT / "docs" / "JV-Data4901_spec.md"

# セル表示の最大文字数
CELL_MAX = 80


def is_empty_row(row) -> bool:
    """行が実質空か"""
    return all(
        str(v).strip() == "" or str(v).strip() == "nan"
        for v in row
    )


def trim_table(df: pd.DataFrame) -> pd.DataFrame:
    """空行を削除（列は維持）"""
    mask = ~df.apply(is_empty_row, axis=1)
    return df[mask].reset_index(drop=True)


def cell_str(v, max_len: int = CELL_MAX) -> str:
    s = str(v).strip()
    if s == "nan" or s == "":
        return ""
    s = s.replace("\n", " ").replace("|", "｜")
    return s[:max_len] + ("…" if len(s) > max_len else "")


def sheet_to_md(sheet_name: str, df: pd.DataFrame, anchor: str = "") -> list:
    """シートをMarkdownに変換"""
    lines = []
    if anchor:
        lines.append(f'<a id="{anchor}"></a>')
    lines.append(f"## {sheet_name}")
    lines.append("")
    df = trim_table(df)
    if len(df) == 0:
        return lines

    # ヘッダー行
    header = "| " + " | ".join(cell_str(c) for c in df.iloc[0]) + " |"
    ncols = len(df.columns)
    sep = "|" + "|".join(["---"] * ncols) + "|"
    lines.append(header)
    lines.append(sep)

    for _, row in df.iloc[1:].iterrows():
        cells = [cell_str(v) for v in row]
        # 列数が合わない場合パディング
        while len(cells) < ncols:
            cells.append("")
        lines.append("| " + " | ".join(cells[:ncols]) + " |")

    lines.append("")
    return lines


def main():
    if not IN_PATH.exists():
        print(f"Not found: {IN_PATH}", file=sys.stderr)
        sys.exit(1)

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    xl = pd.ExcelFile(IN_PATH)

    # 目次
    toc = ["# JV-Data 仕様書 4.9.01", ""]
    toc.append("> JV-Data4901.xlsx を Markdown に変換しました。")
    toc.append("> 元ファイル: `keiba_project/docs/JV-Data4901.xlsx`")
    toc.append("")
    toc.append("## 目次")
    toc.append("")
    for i, name in enumerate(xl.sheet_names, 1):
        toc.append(f"{i}. [{name}](#sheet-{i})")
    toc.append("")
    toc.append("---")
    toc.append("")

    # Keiba Ledger 用クイックリファレンス
    toc.append("## Keiba Ledger で使用する主要フォーマット")
    toc.append("")
    toc.append("| レコード | 用途 | レコード長 |")
    toc.append("|----------|------|------------|")
    toc.append("| RA | レース詳細 | 1272 byte |")
    toc.append("| SE | 馬毎レース情報（着順・タイム等） | 555 byte |")
    toc.append("| HR | 払戻 | 719 byte |")
    toc.append("| O1 | オッズ（単複枠） | - |")
    toc.append("| O3 | オッズ（馬連） | - |")
    toc.append("| UM | 競走馬マスタ | - |")
    toc.append("| KS | 騎手マスタ | - |")
    toc.append("")
    toc.append("詳細は下記の各シートを参照。")
    toc.append("")
    toc.append("---")
    toc.append("")

    all_lines = toc

    for i, sheet_name in enumerate(xl.sheet_names, 1):
        df = pd.read_excel(IN_PATH, sheet_name=sheet_name, header=None)
        df = df.fillna("")
        lines = sheet_to_md(sheet_name, df, anchor=f"sheet-{i}")
        all_lines.extend(lines)

    OUT_PATH.write_text("\n".join(all_lines), encoding="utf-8")
    print(f"Saved: {OUT_PATH}")


if __name__ == "__main__":
    main()
