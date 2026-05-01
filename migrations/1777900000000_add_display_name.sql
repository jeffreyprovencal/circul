-- Add display_name to aggregators + processors so variable-length buyer names
-- can't blow the USSD line budget at runtime. Bounded at 24 chars (column type)
-- and backfilled from name with a hard truncate-+-ellipsis at 23 chars.
ALTER TABLE aggregators
  ADD COLUMN IF NOT EXISTS display_name VARCHAR(24);

ALTER TABLE processors
  ADD COLUMN IF NOT EXISTS display_name VARCHAR(24);

UPDATE aggregators
SET display_name = CASE
  WHEN LENGTH(name) <= 24 THEN name
  ELSE LEFT(name, 23) || '…'
END
WHERE display_name IS NULL;

UPDATE processors
SET display_name = CASE
  WHEN LENGTH(name) <= 24 THEN name
  ELSE LEFT(name, 23) || '…'
END
WHERE display_name IS NULL;
