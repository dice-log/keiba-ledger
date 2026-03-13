# Keiba Ledger データベーススキーマ一覧

**最終更新**: create_tables.sql + migrate_dedup.sql + migrate_trainers_exclusions.sql に基づく

---

## raw スキーマ（生データ）

### raw.jvdata
JRAvan/JV-Link から取得した生データを保存するテーブル

| カラム | 型 | 説明 |
|--------|------|------|
| id | BIGSERIAL PRIMARY KEY | 自動採番ID |
| record_type | TEXT NOT NULL | レコード種別（'RA','SE','HR','O1','UM','KS','CH','JG'等） |
| record_spec | TEXT | レコード仕様バージョン |
| source_date | DATE | データの基準日 |
| payload | JSONB NOT NULL | 生データをJSONで保存 |
| raw_text | TEXT | 元の固定長テキスト（JV-Data形式） |
| fetched_at | TIMESTAMPTZ | 取得日時 |
| processed | BOOLEAN | 正規化済みフラグ |

**インデックス**: record_type, source_date, processed

---

### raw.fetch_log
取得処理の進捗・ログ管理

| カラム | 型 | 説明 |
|--------|------|------|
| id | SERIAL PRIMARY KEY | 自動採番ID |
| fetch_type | TEXT | 取得種別（'initial', 'incremental'） |
| record_types | TEXT[] | 取得したレコード種別の配列 |
| from_date | DATE | 取得開始日 |
| to_date | DATE | 取得終了日 |
| records_fetched | INT | 取得件数 |
| status | TEXT | ステータス（'success','error','running'） |
| error_message | TEXT | エラーメッセージ |
| last_file_timestamp | TEXT | JV-Link前回取得タイムスタンプ |
| started_at | TIMESTAMPTZ | 開始日時 |
| finished_at | TIMESTAMPTZ | 終了日時 |

---

## analytics スキーマ（正規化済み・分析用）

### analytics.races
レース基本情報

| カラム | 型 | 説明 |
|--------|------|------|
| race_id | TEXT PRIMARY KEY | JRAレースID（例: 2024010108010101） |
| race_date | DATE NOT NULL | レース開催日 |
| venue_code | TEXT NOT NULL | 開催場コード（'01'〜'10'） |
| venue_name | TEXT NOT NULL | 競馬場名（'東京','阪神'等） |
| race_number | INT | レース番号 |
| race_name | TEXT | レース名 |
| grade | TEXT | グレード（'G1','G2','G3','OP'等） |
| surface | TEXT | 馬場（'芝','ダート'） |
| distance | INT | 距離（メートル） |
| direction | TEXT | 回り（'右','左','直'） |
| weather | TEXT | 天候（'晴','曇','雨','小雨','雪'） |
| track_condition | TEXT | 馬場状態（'良','稍重','重','不良'） |
| field_count | INT | 出走頭数 |
| created_at | TIMESTAMPTZ | 作成日時 |

**インデックス**: race_date DESC, grade, venue_name

---

### analytics.race_entries
出走馬・着順情報（1レースあたり複数行）

| カラム | 型 | 説明 |
|--------|------|------|
| id | BIGSERIAL PRIMARY KEY | 自動採番ID |
| race_id | TEXT | レースID（→ analytics.races） |
| horse_id | TEXT NOT NULL | 馬ID |
| horse_name | TEXT NOT NULL | 馬名 |
| horse_number | INT | 馬番 |
| frame_number | INT | 枠番 |
| jockey_id | TEXT | 騎手ID |
| jockey_name | TEXT | 騎手名 |
| trainer_id | TEXT | 調教師ID |
| trainer_name | TEXT | 調教師名 |
| weight_carried | NUMERIC(4,1) | 斤量（kg） |
| horse_weight | INT | 馬体重（kg） |
| weight_diff | INT | 馬体重増減（±kg） |
| finish_pos | INT | 着順（NULL=除外・取消） |
| finish_time | NUMERIC(6,1) | 決勝タイム（秒） |
| last_3f | NUMERIC(4,1) | 上がり3F（秒） |
| win_odds | NUMERIC(7,1) | 単勝オッズ |
| place_odds_min | NUMERIC(6,1) | 複勝オッズ下限 |
| place_odds_max | NUMERIC(6,1) | 複勝オッズ上限 |
| popularity | INT | 人気順 |
| running_style | TEXT | 脚質（'逃げ','先行','差し','追込'） |
| corner_pos | TEXT | コーナー通過順位（例: '3-3-4-5'） |
| created_at | TIMESTAMPTZ | 作成日時 |

**インデックス**: race_id, horse_id, jockey_id  
**ユニーク制約**: (race_id, horse_id) ※migrate_dedup.sql

---

### analytics.horses
競走馬マスタ

| カラム | 型 | 説明 |
|--------|------|------|
| horse_id | TEXT PRIMARY KEY | 馬ID |
| name | TEXT NOT NULL | 馬名 |
| name_kana | TEXT | 馬名カナ |
| birth_date | DATE | 生年月日 |
| sex | TEXT | 性別（'牡','牝','セ'） |
| coat_color | TEXT | 毛色（'鹿毛','黒鹿毛'等） |
| sire_id | TEXT | 父馬ID |
| sire_name | TEXT | 父馬名 |
| dam_id | TEXT | 母馬ID |
| dam_name | TEXT | 母馬名 |
| broodmare_sire | TEXT | 母父名 |
| trainer_id | TEXT | 調教師ID |
| owner_name | TEXT | 馬主名 |
| breeder_name | TEXT | 生産者名 |
| created_at | TIMESTAMPTZ | 作成日時 |
| updated_at | TIMESTAMPTZ | 更新日時 |

---

### analytics.jockeys
騎手マスタ

| カラム | 型 | 説明 |
|--------|------|------|
| jockey_id | TEXT PRIMARY KEY | 騎手ID |
| name | TEXT NOT NULL | 騎手名 |
| name_kana | TEXT | 騎手名カナ |
| belong_to | TEXT | 所属（'美浦','栗東'等） |
| birth_date | DATE | 生年月日 |
| created_at | TIMESTAMPTZ | 作成日時 |

---

### analytics.trainers
調教師マスタ（CH レコード由来）

| カラム | 型 | 説明 |
|--------|------|------|
| trainer_id | TEXT PRIMARY KEY | 調教師ID |
| name | TEXT NOT NULL | 調教師名 |
| name_kana | TEXT | 調教師名カナ |
| name_abbr | TEXT | 調教師名略称 |
| belong_to | TEXT | 所属（'美浦','栗東'等） |
| birth_date | DATE | 生年月日 |
| retired | BOOLEAN | 抹消フラグ |
| created_at | TIMESTAMPTZ | 作成日時 |
| updated_at | TIMESTAMPTZ | 更新日時 |

---

### analytics.horse_exclusions
競走馬除外情報（JG レコード由来・出走取消・競走除外等）

| カラム | 型 | 説明 |
|--------|------|------|
| id | BIGSERIAL PRIMARY KEY | 自動採番ID |
| race_id | TEXT NOT NULL | レースID |
| horse_id | TEXT NOT NULL | 馬ID |
| horse_name | TEXT | 馬名 |
| exclusion_type | TEXT | 出走区分（'投票馬','締切除外','取消馬'等） |
| lottery_status | TEXT | 除外状態（'非抽選馬','非当選馬'等） |
| created_at | TIMESTAMPTZ | 作成日時 |

**インデックス**: race_id, horse_id  
**ユニーク制約**: (race_id, horse_id)

---

### analytics.odds_final
オッズ（最終オッズ）

| カラム | 型 | 説明 |
|--------|------|------|
| id | BIGSERIAL PRIMARY KEY | 自動採番ID |
| race_id | TEXT | レースID（→ analytics.races） |
| bet_type | TEXT NOT NULL | 馬券種別（'win','place','quinella'等） |
| combination | TEXT NOT NULL | 組合せ（'7','3-7','7-3-12'等） |
| odds | NUMERIC(8,1) | オッズ |
| popularity | INT | 人気順 |
| created_at | TIMESTAMPTZ | 作成日時 |

**インデックス**: race_id

---

### analytics.odds_timeseries
オッズ時系列（発走前の推移）

| カラム | 型 | 説明 |
|--------|------|------|
| id | BIGSERIAL PRIMARY KEY | 自動採番ID |
| race_id | TEXT | レースID（→ analytics.races） |
| bet_type | TEXT NOT NULL | 馬券種別 |
| combination | TEXT NOT NULL | 組合せ |
| odds | NUMERIC(8,1) | オッズ |
| recorded_at | TIMESTAMPTZ NOT NULL | 記録時刻 |
| minutes_to_start | INT | 発走まで何分前 |

**インデックス**: race_id, recorded_at

---

### analytics.payouts
払戻金情報

| カラム | 型 | 説明 |
|--------|------|------|
| id | BIGSERIAL PRIMARY KEY | 自動採番ID |
| race_id | TEXT | レースID（→ analytics.races） |
| bet_type | TEXT NOT NULL | 馬券種別（'win','place','quinella','exacta'等） |
| combination | TEXT NOT NULL | 組合せ（'7','3-7','7-3-12'等） |
| payout | INT NOT NULL | 払戻金額（100円あたり） |
| popularity | INT | 人気順 |
| created_at | TIMESTAMPTZ | 作成日時 |

**インデックス**: race_id  
**ユニーク制約**: (race_id, bet_type, combination) ※migrate_dedup.sql

---

## 馬券種別（bet_type）の例

| 値 | 日本語 |
|----|--------|
| win | 単勝 |
| place | 複勝 |
| bracket | 枠連 |
| quinella | 馬連 |
| wide | ワイド |
| exacta | 馬単 |
| trio | 3連複 |
| trifecta | 3連単 |

---

## テーブル関連図

```
raw.jvdata (RA→races, SE→race_entries, HR→payouts, UM→horses, KS→jockeys, CH→trainers, JG→horse_exclusions)
     │
     ▼
analytics.races ◄─── analytics.race_entries (horse_id→horses, jockey_id→jockeys, trainer_id→trainers)
     │              analytics.payouts
     │              analytics.horse_exclusions（出走取消・競走除外馬）
     │              analytics.odds_final
     └────────────── analytics.odds_timeseries
```
