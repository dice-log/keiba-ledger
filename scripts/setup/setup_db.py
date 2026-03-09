"""
Keiba Ledger — DB初期化スクリプト
設計書 Phase 1 Step 1

使い方:
  python scripts/setup/setup_db.py           # create_tables.sql を実行
  python scripts/setup/setup_db.py --test    # 接続テストのみ
"""

import argparse
import os
import sys
from pathlib import Path

# プロジェクトルートをパスに追加
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

try:
    import psycopg2
    from psycopg2 import sql
except ImportError:
    print("[NG] psycopg2: pip install psycopg2-binary")
    sys.exit(1)

try:
    from dotenv import load_dotenv
    env_path = ROOT / ".env"
    if env_path.exists():
        try:
            load_dotenv(env_path, encoding="utf-8")
        except UnicodeDecodeError:
            load_dotenv(env_path, encoding="cp932")  # 日本語Windows
except ImportError:
    pass
except Exception:
    pass

os.environ.setdefault("PGCLIENTENCODING", "UTF8")  # 日本語Windows: サーバー通信をUTF-8に


def get_db_config():
    """環境変数からDB接続情報を取得"""
    return {
        "host": os.getenv("LOCAL_DB_HOST", "localhost"),
        "port": int(os.getenv("LOCAL_DB_PORT", "5432")),
        "dbname": os.getenv("LOCAL_DB_NAME", "keiba"),
        "user": os.getenv("LOCAL_DB_USER", "postgres"),
        "password": os.getenv("LOCAL_DB_PASSWORD", ""),
    }


def test_connection():
    """接続テスト"""
    config = get_db_config()
    print("接続先:", config["host"], config["port"], config["dbname"])
    try:
        conn = psycopg2.connect(**config)
        cur = conn.cursor()
        cur.execute("SELECT version();")
        version = cur.fetchone()[0]
        print("[OK] DB connection success")
        print("   ", version[:60] + "...")
        cur.close()
        conn.close()
        return True
    except Exception as e:
        print("[NG] DB connection failed:", e)
        return False


def create_schema(conn):
    """create_tables.sql を実行"""
    sql_path = Path(__file__).parent / "create_tables.sql"
    if not sql_path.exists():
        raise FileNotFoundError(f"create_tables.sql が見つかりません: {sql_path}")

    with open(sql_path, encoding="utf-8") as f:
        sql_content = f.read()

    cur = conn.cursor()
    cur.execute(sql_content)
    conn.commit()
    cur.close()


def main():
    parser = argparse.ArgumentParser(description="Keiba Ledger DB初期化")
    parser.add_argument("--test", action="store_true", help="接続テストのみ")
    args = parser.parse_args()

    if args.test:
        test_connection()
        return

    print("=" * 50)
    print("Keiba Ledger - DB初期化")
    print("=" * 50)

    config = get_db_config()
    if not config["password"]:
        print("[!] LOCAL_DB_PASSWORD not set in .env")

    try:
        conn = psycopg2.connect(**config)
        print("[OK] DB connected")
        create_schema(conn)
        print("[OK] Schema created (raw, analytics)")
        conn.close()
        print()
        print("次のステップ:")
        print("  python scripts/fetch/initial_fetch.py --from 2023-01-01 --no-odds")
    except Exception as e:
        print("[NG] Error:", e)
        if "does not exist" in str(e):
            print()
            print("ヒント: 先にDBを作成してください:")
            print("  psql -U postgres -c \"CREATE DATABASE keiba;\"")
        sys.exit(1)


if __name__ == "__main__":
    main()
