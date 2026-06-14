-- Migration: Hash existing plaintext PINs
-- NOTE: This migration marks all existing collectors for PIN reset.
-- The server now stores hashed PINs, so plaintext PINs will no longer work.
-- Collectors with plaintext PINs will need to have their PIN reset to a hashed value.
-- Run the companion Node script (scripts/hash-existing-pins.js) after this migration.

-- Bootstrap guard (added 2026-06-09): no earlier migration creates
-- collectors.must_change_pin — historically it was added to prod out-of-band
-- during the PIN-hashing rollout, so fresh-DB bootstraps failed here. This
-- makes the column's creation explicit and idempotent. On databases where the
-- migration already ran (ledger-skipped) or the column exists, it is a no-op.
ALTER TABLE collectors ADD COLUMN IF NOT EXISTS must_change_pin BOOLEAN DEFAULT false;

-- Flag all collectors with plaintext PINs (not containing ':' separator) for reset
UPDATE collectors
SET must_change_pin = true
WHERE pin NOT LIKE '%:%';
