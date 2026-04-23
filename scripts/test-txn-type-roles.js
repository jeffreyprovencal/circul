#!/usr/bin/env node
// scripts/test-txn-type-roles.js
//
// Unit tests for shared/transaction-parties.js#txnTypeForRoles. Pure function,
// no DB required. Pre-PR6 the equivalent helper in server.js silently fell
// back to 'aggregator_sale' for any non-collector/non-aggregator seller — that
// mis-labeled processor→converter and recycler→converter discovery offer-
// accepts. PR6 fixes the helper to be seller-driven and throw on unsupported
// roles instead of guessing.
//
// Invoke: `node scripts/test-txn-type-roles.js`

'use strict';

const { txnTypeForRoles } = require('../shared/transaction-parties');

let passed = 0, failed = 0;

function runTest(name, fn) {
  try { fn(); console.log('PASS  ' + name); passed++; }
  catch (e) { console.log('FAIL  ' + name); console.log('       ' + (e.stack || e.message)); failed++; }
}

function assertEq(actual, expected, msg) {
  if (actual !== expected) throw new Error((msg || 'assertEq') + ' — expected ' + JSON.stringify(expected) + ', got ' + JSON.stringify(actual));
}
function assertThrows(fn, matchSubstring, msg) {
  let thrown = null;
  try { fn(); } catch (e) { thrown = e; }
  if (!thrown) throw new Error((msg || 'assertThrows') + ' — expected throw, got none');
  if (matchSubstring && (thrown.message || '').indexOf(matchSubstring) === -1) {
    throw new Error((msg || 'assertThrows') + ' — error message did not contain "' + matchSubstring + '" (got: "' + thrown.message + '")');
  }
}

// Capture console.warn so we can assert the coercion path emits a warning.
function withCapturedWarn(fn) {
  const original = console.warn;
  const warnings = [];
  console.warn = function () {
    warnings.push(Array.from(arguments).join(' '));
  };
  try { return { result: fn(), warnings: warnings }; }
  finally { console.warn = original; }
}

// ── Case 1: collector → aggregator (the canonical happy path)
runTest('case 1: collector → aggregator → collector_sale', () => {
  assertEq(txnTypeForRoles('collector', 'aggregator'), 'collector_sale');
});

// ── Case 2: collector → processor (anomaly path: warn-and-coerce)
runTest('case 2: collector → processor → coerced to collector_sale + warn emitted', () => {
  const captured = withCapturedWarn(() => txnTypeForRoles('collector', 'processor'));
  assertEq(captured.result, 'collector_sale', 'coerced result');
  if (captured.warnings.length === 0) throw new Error('expected console.warn, got none');
  if (captured.warnings[0].indexOf('unexpected collector→processor') === -1) {
    throw new Error('warn did not name the bad pair: ' + captured.warnings[0]);
  }
});

// ── Case 3: aggregator → processor (PR3-aligned downstream path)
runTest('case 3: aggregator → processor → aggregator_sale', () => {
  assertEq(txnTypeForRoles('aggregator', 'processor'), 'aggregator_sale');
});

// ── Case 4: processor → converter (the bug PR6 closes)
//   Pre-PR6 this returned 'aggregator_sale' (silently wrong).
runTest('case 4: processor → converter → processor_sale (was the broken path pre-PR6)', () => {
  assertEq(txnTypeForRoles('processor', 'converter'), 'processor_sale');
});

// ── Case 5: recycler → converter (other half of the bug)
runTest('case 5: recycler → converter → recycler_sale (was the broken path pre-PR6)', () => {
  assertEq(txnTypeForRoles('recycler', 'converter'), 'recycler_sale');
});

// ── Case 6: invalid seller → throw (loud failure on schema drift)
runTest('case 6: unsupported seller (converter) → throws', () => {
  assertThrows(() => txnTypeForRoles('converter', 'someone'), 'unsupported sellerRole: converter');
});

// Bonus: aggregator → recycler (also handled by the seller-driven mapping)
runTest('case 7: aggregator → recycler → aggregator_sale', () => {
  assertEq(txnTypeForRoles('aggregator', 'recycler'), 'aggregator_sale');
});

// Bonus: aggregator → converter (Miniplast multi-tier — sells direct to converter)
runTest('case 8: aggregator → converter → aggregator_sale', () => {
  assertEq(txnTypeForRoles('aggregator', 'converter'), 'aggregator_sale');
});

console.log('\n' + passed + ' passed, ' + failed + ' failed');
process.exit(failed > 0 ? 1 : 0);
