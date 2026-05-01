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

module.exports = { deriveDisplayName };
