// shared/format.js — canonical web formatters
(function (root, factory) {
  if (typeof module !== 'undefined' && module.exports) {
    module.exports = factory();
  } else {
    root.CirculFormat = factory();
  }
})(typeof self !== 'undefined' ? self : this, function () {
  // Format a number as Ghana Cedi for web display.
  // Glyph: ₵ (U+20B5 GHANA CEDI SIGN) — NOT ¢ (U+00A2 generic cent).
  // Sign-preserving: negative values render as "-GH₵ 50.00".
  // null/undefined/NaN → "GH₵ 0.00".
  function fmtGHS(n) {
    var v = parseFloat(n);
    if (!isFinite(v)) return 'GH₵ 0.00';
    var abs = Math.abs(v).toLocaleString('en-GH', {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2
    });
    return (v < 0 ? '-' : '') + 'GH₵ ' + abs;
  }

  // ── Date formatters (added in PR-D Commit 3) ──
  // Three canonical contexts:
  //   fmtDateTime — full timestamp ("13/05/2026, 16:42") for transaction rows
  //   fmtDateLong — date-only ("13 May 2026") for activity feeds and lists
  //   fmtDateISO  — sortable ISO ("2026-05-13") for form inputs

  // Falsy / unparseable input → "—" (em-dash) so empty cells stay tidy.
  function fmtDateTime(iso) {
    if (!iso) return '—';
    var d = new Date(iso);
    if (isNaN(d.getTime())) return '—';
    return d.toLocaleString('en-GB', { dateStyle: 'short', timeStyle: 'short' });
  }

  // day: '2-digit' matches the dominant existing convention pre-PR (6 of 9
  // dashboards) and gives zero-padding so column alignment holds in tables.
  function fmtDateLong(iso) {
    if (!iso) return '—';
    var d = new Date(iso);
    if (isNaN(d.getTime())) return '—';
    return d.toLocaleDateString('en-GB', { day: '2-digit', month: 'short', year: 'numeric' });
  }

  function fmtDateISO(date) {
    return (date || new Date()).toISOString().slice(0, 10);
  }

  return {
    fmtGHS: fmtGHS,
    fmtDateTime: fmtDateTime,
    fmtDateLong: fmtDateLong,
    fmtDateISO: fmtDateISO
  };
});
