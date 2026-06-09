-- Repair migration — 2026-06-09 USSD prod incident.
--
-- Prod symptom: POST /api/ussd returns "END System error" for aggregator,
-- agent, driver, and unregistered-number (welcome) paths; collector works.
-- The USSD aggregators lookup (server.js, commit a2b8e70) selects
-- first_name/last_name; web login selects neither and works. Hypothesis:
-- 1779100000000_add_aggregator_first_last_name never executed on prod
-- (or executed partially), despite the columns existing in every clean
-- bootstrap.
--
-- This re-runs the same idempotent SQL under a new migration name so the
-- standard deploy path (npm run build → migrate.js) applies it regardless
-- of the _migrations ledger state for the original. If the columns already
-- exist and are backfilled, every statement is a no-op.

ALTER TABLE aggregators ADD COLUMN IF NOT EXISTS first_name VARCHAR(100);
ALTER TABLE aggregators ADD COLUMN IF NOT EXISTS last_name VARCHAR(100);

UPDATE aggregators
SET first_name = SPLIT_PART(name, ' ', 1),
    last_name  = NULLIF(SUBSTRING(name FROM POSITION(' ' IN name) + 1), name)
WHERE first_name IS NULL
  AND name IS NOT NULL
  AND name <> '';
