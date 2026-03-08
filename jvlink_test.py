"""
JV-Link 接続テストスクリプト v5
32bit Python専用版
実行方法: py -3.11-32 jvlink_test.py
"""

import sys
import struct
import time
from datetime import datetime, timedelta
import win32com.client

print("=" * 50)
print("Step 1-3: 接続・認証")
print("=" * 50)
jv = win32com.client.Dispatch("JVDTLab.JVLink")
rc = jv.JVInit("UNKNOWN")
if rc != 0:
    print(f"❌ JVInit 失敗: {rc}")
    sys.exit(1)
print("✅ 接続・認証 成功")

# -----------------------------------------------
# JVOpen
# -----------------------------------------------
print()
print("=" * 50)
print("Step 4: JVOpen")
print("=" * 50)

from_date = (datetime.now() - timedelta(days=30)).strftime("%Y%m%d000000")
print(f"   取得期間: {from_date} 〜 現在")

try:
    rc = jv.JVOpen("RACE", from_date, 1, 0, 0, "")
    if isinstance(rc, tuple):
        ret_code = rc[0]
        dl_count = rc[1] if len(rc) > 1 else 0
        last_ts  = rc[3] if len(rc) > 3 else ""
    else:
        ret_code = rc
        dl_count = 0
        last_ts  = ""

    print(f"   ret_code : {ret_code}")
    print(f"   dl_count : {dl_count}")

    if ret_code < 0:
        print(f"❌ JVOpen 失敗: {ret_code}")
        sys.exit(1)
    print("✅ JVOpen 成功")

except Exception as e:
    print(f"❌ JVOpen 例外: {e}")
    sys.exit(1)

# -----------------------------------------------
# JVRead（-3の場合はダウンロード完了待ち）
# -----------------------------------------------
print()
print("=" * 50)
print("Step 5: JVRead データ読み取り")
print("=" * 50)
print("   ダウンロード完了を待ちます...")

read_count  = 0
retry_count = 0
MAX_RETRY   = 30  # 最大30回×2秒 = 60秒待機

try:
    while read_count < 3:
        try:
            ret = jv.JVRead("", 40000, "")
        except Exception as e:
            print(f"❌ JVRead 例外: {e}")
            break

        if isinstance(ret, tuple):
            rc_read = ret[0]
            buff    = ret[1] if len(ret) > 1 else ""
            size    = ret[2] if len(ret) > 2 else 0
            fname   = ret[3] if len(ret) > 3 else ""
        else:
            rc_read = ret
            buff    = ""
            size    = 0
            fname   = ""

        # -3: ダウンロード中・準備中
        if rc_read == -3:
            retry_count += 1
            if retry_count > MAX_RETRY:
                print("❌ タイムアウト（60秒待っても準備できない）")
                break
            print(f"   ダウンロード中... ({retry_count}/{MAX_RETRY}) 待機中", end="\r")
            time.sleep(2)
            continue

        # 0: 全件読み取り完了
        if rc_read == 0:
            print()
            print("   → 全データ読み取り完了")
            break

        # -1: ファイル切り替え
        elif rc_read == -1:
            print(f"   → ファイル切り替え: {fname}")
            continue

        # その他エラー
        elif rc_read < 0:
            print(f"\n❌ JVRead エラー: {rc_read}")
            break

        # 正常読み取り
        read_count += 1
        print()
        record_type = buff[:2] if buff else "??"
        preview     = buff[:100] if buff else "(空)"
        print(f"✅ レコード {read_count} [種別:{record_type}] size={size}bytes")
        print(f"   {preview}...")

except KeyboardInterrupt:
    print("\n⚠️  中断されました")

finally:
    try:
        jv.JVClose()
        print()
        print("✅ JVClose 完了")
    except Exception as e:
        print(f"⚠️  JVClose: {e}")

# -----------------------------------------------
# 結果サマリー
# -----------------------------------------------
print()
print("=" * 50)
print("テスト結果サマリー")
print("=" * 50)
if read_count > 0:
    print("🎉 完全成功！JV-Linkからデータ取得できました！")
    print(f"   {read_count}件のレコードを確認")
    print()
    print("次のステップ:")
    print("  → PostgreSQL セットアップ")
    print("  → initial_fetch.py 本実装へ")
elif retry_count > 0:
    print("⚠️  ダウンロード待ちタイムアウト")
    print("   ネット接続を確認してから再実行してください")
    print("   または --wait オプションで待機時間を延ばしてください")
else:
    print("⚠️  JVOpen成功・JVRead要調整")
    print(f"   ダウンロード件数: {dl_count}")
