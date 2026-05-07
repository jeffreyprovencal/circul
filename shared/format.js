// shared/format.js — canonical web currency formatter
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
  return { fmtGHS: fmtGHS };
});
