// shared/text.js
// USSD-safe text helpers.

// deriveDisplayName: bound a name to fit the USSD display column.
// Defaults to 24 chars (matches the display_name column on aggregators/processors).
// Returns null for null/empty input so callers can rely on COALESCE upstream.
function deriveDisplayName(name, max = 24) {
  if (!name) return null;
  if (name.length <= max) return name;
  return name.slice(0, max - 1) + '…';
}

// Naive first-space split. Mirrors the backfill in
// migrations/1779100000000_add_aggregator_first_last_name.sql so write-time
// INSERTs stay consistent with what the backfill produced for pre-migration
// rows. Single-word names get a null last_name.
function splitFirstName(fullName) {
  if (!fullName) return null;
  return fullName.trim().split(/\s+/)[0] || null;
}

function splitLastName(fullName) {
  if (!fullName) return null;
  const parts = fullName.trim().split(/\s+/);
  if (parts.length <= 1) return null;
  return parts.slice(1).join(' ');
}

module.exports = { deriveDisplayName, splitFirstName, splitLastName };
