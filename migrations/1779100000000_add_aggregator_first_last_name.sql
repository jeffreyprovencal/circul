-- Bring aggregators in line with collectors/agents/drivers by splitting `name`
-- into first_name + last_name. Existing rows backfilled via naive first-space
-- split; bad splits get manually corrected post-deploy. New INSERT sites
-- populate all three (name, first_name, last_name) for backward compat with
-- read paths that still use `name`.

ALTER TABLE aggregators ADD COLUMN IF NOT EXISTS first_name VARCHAR(100);
ALTER TABLE aggregators ADD COLUMN IF NOT EXISTS last_name VARCHAR(100);

-- Backfill: split existing `name` by first space. Rows with no space leave
-- last_name NULL (NULLIF guards against duplicating the whole string when
-- POSITION returns 0 for single-word names).
UPDATE aggregators
SET first_name = SPLIT_PART(name, ' ', 1),
    last_name  = NULLIF(SUBSTRING(name FROM POSITION(' ' IN name) + 1), name)
WHERE first_name IS NULL
  AND name IS NOT NULL
  AND name <> '';
