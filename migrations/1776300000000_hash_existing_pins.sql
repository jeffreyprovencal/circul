-- Migration: Hash existing plaintext PINs
-- NOTE: This migration marks all existing collectors for PIN reset.
-- The server now stores hashed PINs, so plaintext PINs will no longer work.
-- Collectors with plaintext PINs will need to have their PIN reset to a hashed value.
-- Run the companion Node script (scripts/hash-existing-pins.js) after this migration.

-- Flag all collectors with plaintext PINs (not containing ':' separator) for reset
UPDATE collectors
SET must_change_pin = true
WHERE pin NOT LIKE '%:%';
