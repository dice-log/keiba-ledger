"""
JV-Linkラッパー — win32com経由でJRAvanデータ取得
設計書 2-5 / Phase 1 Step 3

JV-LinkはWindows専用COMオブジェクト。32bit Python必須。
win32com経由では戻り値がtuple/list形式になる。

主要メソッド:
- open(dataspec, from_date, option) — 蓄積データ取得開始
- read() — レコードをイテレート（record_type, raw_text）
- close() — 終了
"""

import os
import sys
import time
from pathlib import Path
from typing import Generator, Optional, Tuple

# プロジェクトルート
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

try:
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
except ImportError:
    pass

try:
    import win32com.client
except ImportError:
    raise ImportError("pywin32 が必要です: pip install pywin32 (32bit Python)")


# 設計書で取得対象とするレコード種別
TARGET_RECORD_TYPES = frozenset(["RA", "SE", "HR", "O1", "O2", "O3", "O4", "O5", "O6", "WF", "UM", "KS", "CH", "WH", "WE", "JG"])

# JVRead バッファサイズ（バイト）
READ_BUFFER_SIZE = 102890


class JVLinkClient:
    """JV-Link COM ラッパー。蓄積データ取得用。"""

    def __init__(self, auth_id: Optional[str] = None):
        """
        Args:
            auth_id: JVInit用。Noneまたは'UNKNOWN'で認証ダイアログ表示。
                     .envのJVLINK_SOFTWARE_ID等を指定可能。
        """
        self._auth_id = auth_id or os.getenv("JVLINK_SOFTWARE_ID") or "UNKNOWN"
        self._jv = None
        self._download_count = 0
        self._last_file_timestamp = ""

    def _ensure_jv(self):
        if self._jv is None:
            self._jv = win32com.client.Dispatch("JVDTLab.JVLink")

    def init(self) -> bool:
        """JVInit 実行。True=成功"""
        self._ensure_jv()
        rc = self._jv.JVInit(self._auth_id)
        return rc == 0

    def open(self, dataspec: str = "RACE", from_time: str = "", option: int = 1) -> Tuple[int, int, str]:
        """
        蓄積データ取得を開始。

        Args:
            dataspec: 'RACE' 等
            from_time: YYYYMMDDhhmmss。空なら最初から。
            option: 1=通常, 2=今週のみ, 3=セットアップ（ダイアログあり）

        Returns:
            (ret_code, download_count, last_file_timestamp)
            ret_code >= 0 なら成功
        """
        self._ensure_jv()
        ret = self._jv.JVOpen(dataspec, from_time or "00000001000000", option, 0, 0, "")

        if isinstance(ret, (tuple, list)):
            ret_code = int(ret[0]) if ret[0] != "" else -1
            self._download_count = int(ret[1]) if len(ret) > 1 and ret[1] != "" else 0
            self._last_file_timestamp = str(ret[3]) if len(ret) > 3 else ""
        else:
            ret_code = int(ret)
            self._download_count = 0
            self._last_file_timestamp = ""

        return ret_code, self._download_count, self._last_file_timestamp

    def read(
        self,
        target_types: Optional[frozenset] = None,
        skip_wait_sec: int = 2,
        max_wait_sec: int = 120,
    ) -> Generator[Tuple[str, str], None, None]:
        """
        レコードを1件ずつ yield。JVRead のラッパー。

        Args:
            target_types: 取得するレコード種別。None なら全種別。
            skip_wait_sec: rc=-3（ダウンロード中）時の待機秒数
            max_wait_sec: ダウンロード待ちの最大秒数

        Yields:
            (record_type, raw_text)
        """
        target_types = target_types or TARGET_RECORD_TYPES
        waited = 0

        while True:
            try:
                ret = self._jv.JVRead("", READ_BUFFER_SIZE, "")
            except Exception as e:
                raise RuntimeError(f"JVRead 例外: {e}") from e

            if isinstance(ret, (tuple, list)):
                rc = ret[0]
                buff = ret[1] if len(ret) > 1 else ""
                # size = ret[2] if len(ret) > 2 else 0
                fname = ret[3] if len(ret) > 3 else ""
            else:
                rc = ret
                buff = ""
                fname = ""

            # -3: ダウンロード中・準備中
            if rc == -3:
                if waited >= max_wait_sec:
                    raise TimeoutError(f"ダウンロード待ちタイムアウト ({max_wait_sec}秒)")
                time.sleep(skip_wait_sec)
                waited += skip_wait_sec
                continue

            waited = 0  # リセット

            # 0: 全件読み取り完了
            if rc == 0:
                return

            # -1: ファイル切り替え
            if rc == -1:
                continue

            # エラー
            if rc < 0:
                raise RuntimeError(f"JVRead エラー: rc={rc}")

            # 正常読み取り
            record_type = (buff[:2] if buff else "??").strip()
            raw_text = buff if isinstance(buff, str) else (buff.decode("cp932", errors="replace") if isinstance(buff, bytes) else "")

            if record_type in target_types:
                yield record_type, raw_text
            else:
                try:
                    self._jv.JVSkip()
                except Exception:
                    pass

    def close(self):
        """JVClose 実行"""
        if self._jv:
            try:
                self._jv.JVClose()
            except Exception:
                pass
            self._jv = None

    @property
    def last_file_timestamp(self) -> str:
        """前回取得の続き用タイムスタンプ"""
        return self._last_file_timestamp

    @property
    def download_count(self) -> int:
        """ダウンロード予定ファイル数"""
        return self._download_count

    def __enter__(self):
        if not self.init():
            raise RuntimeError("JVInit 失敗")
        return self

    def __exit__(self, *args):
        self.close()


def fetch_stored_records(
    from_date: str,
    to_date: Optional[str] = None,
    dataspec: str = "RACE",
    target_types: Optional[frozenset] = None,
) -> Generator[Tuple[str, str], None, None]:
    """
    蓄積データを取得する簡易API。

    Args:
        from_date: 開始日 YYYYMMDD または YYYYMMDDhhmmss
        to_date: 未使用（JV-Linkは from 以降を取得）
        dataspec: データ種別
        target_types: 取得するレコード種別

    Yields:
        (record_type, raw_text)
    """
    if len(from_date) == 8:
        from_time = from_date + "000000"
    else:
        from_time = from_date

    with JVLinkClient() as client:
        rc, dl_count, last_ts = client.open(dataspec, from_time, option=1)
        if rc < 0:
            raise RuntimeError(f"JVOpen 失敗: rc={rc}")

        yield from client.read(target_types=target_types)
