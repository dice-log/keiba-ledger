-- dedup schema migration
-- Run: psql -U postgres -d keiba -f scripts/setup/migrate_dedup.sql
-- raw.jvdata: load_to_db.py でアプリ側重複除外

-- analytics.race_entries: (race_id, horse_id) で一意
CREATE UNIQUE INDEX IF NOT EXISTS idx_race_entries_race_horse
  ON analytics.race_entries(race_id, horse_id);

-- analytics.payouts: (race_id, bet_type, combination) で一意
CREATE UNIQUE INDEX IF NOT EXISTS idx_payouts_race_bet_combo
  ON analytics.payouts(race_id, bet_type, combination);
