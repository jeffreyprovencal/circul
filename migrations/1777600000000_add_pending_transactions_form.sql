ALTER TABLE pending_transactions
  ADD COLUMN IF NOT EXISTS form VARCHAR(10)
  CHECK (form IS NULL OR form IN ('loose','baled'));
