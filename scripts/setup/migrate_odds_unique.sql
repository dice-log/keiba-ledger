-- odds_final ユニーク制約（O1→analytics.odds_final 用）
-- Run: psql -U postgres -d keiba -f scripts/setup/migrate_odds_unique.sql

CREATE UNIQUE INDEX IF NOT EXISTS idx_odds_final_race_bet_combo
  ON analytics.odds_final(race_id, bet_type, combination);
