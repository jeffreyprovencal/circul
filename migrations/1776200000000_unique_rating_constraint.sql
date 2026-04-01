-- Prevent duplicate ratings: one rating per (transaction_id, rater_type, rater_id) triple
-- The partial index excludes NULL transaction_ids (non-transaction ratings are allowed)
DROP INDEX IF EXISTS idx_unique_transaction_rater;
CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_transaction_rater
  ON ratings (transaction_id, rater_type, rater_id)
  WHERE transaction_id IS NOT NULL;
