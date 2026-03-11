-- Keiba Ledger Phase 1 - PostgreSQL Schema
-- Run: psql -U postgres -d keiba -f create_tables.sql

CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE raw.jvdata (
    id          BIGSERIAL PRIMARY KEY,
    record_type TEXT NOT NULL,
    record_spec TEXT,
    source_date DATE,
    payload     JSONB NOT NULL,
    raw_text    TEXT,
    fetched_at  TIMESTAMPTZ DEFAULT NOW(),
    processed   BOOLEAN DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS idx_jvdata_type ON raw.jvdata(record_type);
CREATE INDEX IF NOT EXISTS idx_jvdata_date ON raw.jvdata(source_date);
CREATE INDEX IF NOT EXISTS idx_jvdata_processed ON raw.jvdata(processed);

CREATE TABLE raw.fetch_log (
    id                  SERIAL PRIMARY KEY,
    fetch_type          TEXT,
    record_types        TEXT[],
    from_date           DATE,
    to_date             DATE,
    records_fetched     INT,
    status              TEXT,
    error_message       TEXT,
    last_file_timestamp TEXT,
    started_at          TIMESTAMPTZ DEFAULT NOW(),
    finished_at         TIMESTAMPTZ
);

CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE analytics.races (
    race_id         TEXT PRIMARY KEY,
    race_date       DATE NOT NULL,
    venue_code      TEXT NOT NULL,
    venue_name      TEXT NOT NULL,
    race_number     INT,
    race_name       TEXT,
    grade           TEXT,
    surface         TEXT,
    distance        INT,
    direction       TEXT,
    weather         TEXT,
    track_condition TEXT,
    field_count     INT,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_races_date ON analytics.races(race_date DESC);
CREATE INDEX IF NOT EXISTS idx_races_grade ON analytics.races(grade);
CREATE INDEX IF NOT EXISTS idx_races_venue ON analytics.races(venue_name);

CREATE TABLE analytics.race_entries (
    id              BIGSERIAL PRIMARY KEY,
    race_id         TEXT REFERENCES analytics.races(race_id),
    horse_id        TEXT NOT NULL,
    horse_name      TEXT NOT NULL,
    horse_number    INT,
    frame_number    INT,
    jockey_id       TEXT,
    jockey_name     TEXT,
    trainer_id      TEXT,
    trainer_name    TEXT,
    weight_carried  NUMERIC(4,1),
    horse_weight    INT,
    weight_diff     INT,
    finish_pos      INT,
    finish_time     NUMERIC(6,1),
    last_3f         NUMERIC(4,1),
    win_odds        NUMERIC(7,1),
    place_odds_min  NUMERIC(6,1),
    place_odds_max  NUMERIC(6,1),
    popularity      INT,
    running_style   TEXT,
    corner_pos      TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_entries_race ON analytics.race_entries(race_id);
CREATE INDEX IF NOT EXISTS idx_entries_horse ON analytics.race_entries(horse_id);
CREATE INDEX IF NOT EXISTS idx_entries_jockey ON analytics.race_entries(jockey_id);

CREATE TABLE analytics.horses (
    horse_id        TEXT PRIMARY KEY,
    name            TEXT NOT NULL,
    name_kana       TEXT,
    birth_date      DATE,
    sex             TEXT,
    coat_color      TEXT,
    sire_id         TEXT,
    sire_name       TEXT,
    dam_id          TEXT,
    dam_name        TEXT,
    broodmare_sire  TEXT,
    trainer_id      TEXT,
    owner_name      TEXT,
    breeder_name    TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE analytics.jockeys (
    jockey_id       TEXT PRIMARY KEY,
    name            TEXT NOT NULL,
    name_kana       TEXT,
    belong_to       TEXT,
    birth_date      DATE,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE analytics.odds_final (
    id              BIGSERIAL PRIMARY KEY,
    race_id         TEXT REFERENCES analytics.races(race_id),
    bet_type        TEXT NOT NULL,
    combination     TEXT NOT NULL,
    odds            NUMERIC(8,1),
    popularity      INT,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_odds_race ON analytics.odds_final(race_id);

CREATE TABLE analytics.odds_timeseries (
    id              BIGSERIAL PRIMARY KEY,
    race_id         TEXT REFERENCES analytics.races(race_id),
    bet_type        TEXT NOT NULL,
    combination     TEXT NOT NULL,
    odds            NUMERIC(8,1),
    recorded_at     TIMESTAMPTZ NOT NULL,
    minutes_to_start INT
);

CREATE INDEX IF NOT EXISTS idx_odds_ts_race ON analytics.odds_timeseries(race_id);
CREATE INDEX IF NOT EXISTS idx_odds_ts_time ON analytics.odds_timeseries(recorded_at);

CREATE TABLE analytics.payouts (
    id              BIGSERIAL PRIMARY KEY,
    race_id         TEXT REFERENCES analytics.races(race_id),
    bet_type        TEXT NOT NULL,
    combination     TEXT NOT NULL,
    payout          INT NOT NULL,
    popularity      INT,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_payouts_race ON analytics.payouts(race_id);

CREATE TABLE analytics.trainers (
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

CREATE TABLE analytics.horse_exclusions (
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
