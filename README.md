# Keiba Ledger

競馬収支管理PWA + JRAvanデータを使ったオッズ歪み検出・回収率最大化ML予測システム

## Phase 1 — データ取得・正規化

### 前提

- Windows 11
- Python 3.11 32bit（JV-Link用）
- PostgreSQL 16
- JRAvan 会員 + JV-Link SDK インストール済み

### セットアップ

```powershell
# 1. PostgreSQL に DB 作成
psql -U postgres -c "CREATE DATABASE keiba;"

# 2. スキーマ作成
psql -U postgres -d keiba -f scripts/setup/create_tables.sql

# 3. Python 依存関係
pip install -r requirements.txt

# 4. 環境変数
copy .env.example .env
# .env を編集（LOCAL_DB_PASSWORD 等）
```

### 実行手順

```powershell
# 接続テスト
python scripts/setup/setup_db.py --test

# 初回データ取得（まず小規模で）
py -3.11-32 scripts/fetch/initial_fetch.py --from 2024-01-01 --no-odds --limit 100

# 正規化
python scripts/transform/normalize.py

# 差分取得（定期実行用）
py -3.11-32 scripts/fetch/incremental_fetch.py
```

### タスクスケジューラ（毎週月曜6:00）

```powershell
# 管理者権限で実行
.\scripts\scheduler\task_setup.ps1
```

### ディレクトリ構成

```
keiba-ledger/
├── scripts/
│   ├── setup/         # DB初期化
│   ├── fetch/         # JV-Link取得
│   ├── transform/     # 正規化（RA, SE, HR パース）
│   ├── sync/          # Supabase同期
│   ├── scheduler/     # タスクスケジューラ
│   └── tools/         # ユーティリティ
├── docs/              # 仕様書
├── logs/
└── 設計書.md
```

## 次のフェーズ

- **Phase 2**: Next.js PWA（収支管理・馬券スキャン）
- **Phase 3**: LightGBM 予測モデル
