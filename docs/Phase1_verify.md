# Phase 1 動作確認手順

## 重要：.env の編集

**PostgreSQL の postgres ユーザーのパスワード**を `.env` に設定してください。

1. `.env` を開く
2. `LOCAL_DB_PASSWORD=your_password` の部分を、インストール時に設定した PostgreSQL のパスワードに書き換える

```
LOCAL_DB_PASSWORD=実際のパスワード
```

パスワードを間違えると、`UnicodeDecodeError` や `FATAL` などのエラーが出ます。

### 2. PostgreSQL のパスを通す（必要な場合）

psql が見つからない場合、PowerShell で一時的に追加：

```powershell
$env:PATH = "C:\Program Files\PostgreSQL\18\bin;$env:PATH"
```

---

## 確認手順

### Step 1: DB・スキーマ作成

```powershell
cd C:\Users\Dice\keiba-ledger

# keiba DB 作成（パスワードを聞かれたら入力）
& "C:\Program Files\PostgreSQL\18\bin\psql.exe" -U postgres -c "CREATE DATABASE keiba"

# スキーマ作成
& "C:\Program Files\PostgreSQL\18\bin\psql.exe" -U postgres -d keiba -f scripts\setup\create_tables.sql
```

### Step 2: Python DB接続テスト

```powershell
python scripts/setup/setup_db.py --test
```

`[OK] DB connection success` と出ればOK。

### Step 3: 初回データ取得（小規模テスト）

```powershell
py -3.11-32 scripts/fetch/initial_fetch.py --from 2024-01-01 --no-odds --limit 50
```

JV-Link 認証ダイアログが出たら実行。50件取得されればOK。

### Step 4: 正規化

```powershell
python scripts/transform/normalize.py
```

`RA`, `SE`, `HR` の件数が表示されればOK。

### Step 5: データ確認

```powershell
& "C:\Program Files\PostgreSQL\18\bin\psql.exe" -U postgres -d keiba -c "SELECT COUNT(*) FROM analytics.races"
& "C:\Program Files\PostgreSQL\18\bin\psql.exe" -U postgres -d keiba -c "SELECT COUNT(*) FROM analytics.race_entries"
```

---

## トラブルシューティング

| 現象 | 対応 |
|------|------|
| psql が見つからない | PATH に PostgreSQL\18\bin を追加 |
| 接続失敗 | .env の LOCAL_DB_PASSWORD を確認 |
| JVInit 失敗 | JV-Link SDK 起動、32bit Python 確認 |
| パーサーエラー | raw.jvdata にデータが入っているか確認 |
