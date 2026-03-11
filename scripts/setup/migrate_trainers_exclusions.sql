-- CH/JG 用テーブル追加
-- Run: psql -U postgres -d keiba -f scripts/setup/migrate_trainers_exclusions.sql

CREATE TABLE IF NOT EXISTS analytics.trainers (
    trainer_id       TEXT PRIMARY KEY,
    name             TEXT NOT NULL,
    name_kana        TEXT,
    name_abbr        TEXT,
    belong_to        TEXT,
    birth_date       DATE,
    retired          BOOLEAN DEFAULT FALSE,
    created_at       TIMESTAMPTZ DEFAULT NOW(),
    updated_at       TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS analytics.horse_exclusions (
    id               BIGSERIAL PRIMARY KEY,
    race_id          TEXT NOT NULL,
    horse_id         TEXT NOT NULL,
    horse_name       TEXT,
    exclusion_type   TEXT,
    lottery_status   TEXT,
    created_at       TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(race_id, horse_id)
);
CREATE INDEX IF NOT EXISTS idx_horse_exclusions_race ON analytics.horse_exclusions(race_id);
CREATE INDEX IF NOT EXISTS idx_horse_exclusions_horse ON analytics.horse_exclusions(horse_id);
