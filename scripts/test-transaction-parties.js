#!/usr/bin/env node
// scripts/test-transaction-parties.js
// Manual-run unit tests for shared/transaction-parties.js. No framework —
// plain node + assert, same pattern as scripts/test-chain-of-custody.js.
// Invoke via `node scripts/test-transaction-parties.js`. Exits non-zero on
// any failure.

'use strict';

const assert = require('assert');
const {
  PARTY_MAP,
  resolveSeller,
  resolveBuyer
} = require('../shared/transaction-parties');

// ── Harness ────────────────────────────────────────────────────────────────
let passed = 0;
let failed = 0;

function test(name, fn) {
  try {
    fn();
    console.log('PASS  ' + name);
    passed++;
  } catch (e) {
    console.log('FAIL  ' + name);
    console.log('       ' + (e.stack || e.message));
    failed++;
  }
}

// ── Tests ──────────────────────────────────────────────────────────────────

test('1. aggregator_sale → processor (processor_id set, others null)', () => {
  const row = { id: 1, transaction_type: 'aggregator_sale',
                aggregator_id: 10, processor_id: 20, recycler_id: null, converter_id: null };
  assert.deepStrictEqual(resolveBuyer(row), { kind: 'processor', id: 20 });
  assert.deepStrictEqual(resolveSeller(row), { kind: 'aggregator', id: 10 });
});

test('2. aggregator_sale → converter (converter_id set, others null)', () => {
  const row = { id: 2, transaction_type: 'aggregator_sale',
                aggregator_id: 10, processor_id: null, recycler_id: null, converter_id: 30 };
  assert.deepStrictEqual(resolveBuyer(row), { kind: 'converter', id: 30 });
  assert.deepStrictEqual(resolveSeller(row), { kind: 'aggregator', id: 10 });
});

test('3. Miniplast-style aggregator → converter direct sale', () => {
  // Canonical real-world case: a multi-tier entity (process+recycle+convert)
  // classified by its deepest tier (converter). Aggregator sells direct to
  // the converter; chain-of-custody tracking ends at that boundary.
  // Regression guard: before this fix, resolveParties returned buyer=null
  // and payment-auth routes 403'd for this row shape.
  const row = {
    id: 100,
    transaction_type: 'aggregator_sale',
    aggregator_id: 9,          // aggregator from demo seed
    processor_id: null,
    recycler_id: null,
    converter_id: 42,          // Miniplast-style converter
    material_type: 'PET',
    gross_weight_kg: 1200
  };
  const buyer = resolveBuyer(row);
  assert.strictEqual(buyer.kind, 'converter');
  assert.strictEqual(buyer.id, 42);
  const seller = resolveSeller(row);
  assert.strictEqual(seller.kind, 'aggregator');
  assert.strictEqual(seller.id, 9);
});

test('4. processor_sale → recycler (recycler_id set, converter_id null)', () => {
  const row = { id: 4, transaction_type: 'processor_sale',
                processor_id: 20, recycler_id: 30, converter_id: null };
  assert.deepStrictEqual(resolveBuyer(row), { kind: 'recycler', id: 30 });
  assert.deepStrictEqual(resolveSeller(row), { kind: 'processor', id: 20 });
});

test('5. processor_sale → converter (converter_id set, recycler_id null)', () => {
  const row = { id: 5, transaction_type: 'processor_sale',
                processor_id: 20, recycler_id: null, converter_id: 40 };
  assert.deepStrictEqual(resolveBuyer(row), { kind: 'converter', id: 40 });
  assert.deepStrictEqual(resolveSeller(row), { kind: 'processor', id: 20 });
});

test('6. collector_sale → aggregator', () => {
  const row = { id: 6, transaction_type: 'collector_sale',
                collector_id: 1, aggregator_id: 10 };
  assert.deepStrictEqual(resolveBuyer(row), { kind: 'aggregator', id: 10 });
  assert.deepStrictEqual(resolveSeller(row), { kind: 'collector', id: 1 });
});

test('7. aggregator_purchase → seller is collector (direction reversed)', () => {
  // The transaction_type is named from the aggregator's perspective ("I
  // purchased") but the SELLER is the collector. resolveSeller must reflect
  // that, regardless of the name.
  const row = { id: 7, transaction_type: 'aggregator_purchase',
                collector_id: 1, aggregator_id: 10 };
  assert.deepStrictEqual(resolveSeller(row), { kind: 'collector', id: 1 });
  assert.deepStrictEqual(resolveBuyer(row), { kind: 'aggregator', id: 10 });
});

test('8. recycler_sale → converter', () => {
  const row = { id: 8, transaction_type: 'recycler_sale',
                recycler_id: 30, converter_id: 40 };
  assert.deepStrictEqual(resolveBuyer(row), { kind: 'converter', id: 40 });
  assert.deepStrictEqual(resolveSeller(row), { kind: 'recycler', id: 30 });
});

test('9. Invalid row — no buyer FK set', () => {
  const row = { id: 9, transaction_type: 'aggregator_sale',
                aggregator_id: 10, processor_id: null, recycler_id: null, converter_id: null };
  assert.throws(() => resolveBuyer(row), /no buyer FK set/);
  // Seller is still resolvable — that FK is present.
  assert.deepStrictEqual(resolveSeller(row), { kind: 'aggregator', id: 10 });
});

test('9b. Invalid row — unknown transaction_type', () => {
  const row = { id: 99, transaction_type: 'made_up_type', aggregator_id: 10 };
  assert.throws(() => resolveBuyer(row), /unknown transaction_type/);
  assert.throws(() => resolveSeller(row), /unknown transaction_type/);
});

test('9c. Invalid row — missing seller FK', () => {
  const row = { id: 98, transaction_type: 'collector_sale',
                collector_id: null, aggregator_id: 10 };
  assert.throws(() => resolveSeller(row), /no seller FK set/);
});

test('10. Ambiguous row — aggregator_sale with BOTH processor_id and converter_id', () => {
  // Policy: throw. The schema allows this state (server doesn't enforce
  // at-most-one-buyer-FK on write yet) but silent tie-breaking would hide
  // data-integrity bugs.
  const row = { id: 10, transaction_type: 'aggregator_sale',
                aggregator_id: 10, processor_id: 20, recycler_id: null, converter_id: 30 };
  assert.throws(() => resolveBuyer(row), /ambiguous buyer/);
  // Error message should name the conflicting FKs.
  try { resolveBuyer(row); }
  catch (e) {
    assert.ok(/processor_id=20/.test(e.message), 'error names processor_id=20');
    assert.ok(/converter_id=30/.test(e.message), 'error names converter_id=30');
  }
});

test('11. aggregator_sale → recycler (rare Ghana flow per amended PARTY_MAP)', () => {
  // PARTY_MAP amendment: recycler is now a valid buyer for aggregator_sale.
  // Tests that the resolver picks it up correctly.
  const row = { id: 11, transaction_type: 'aggregator_sale',
                aggregator_id: 10, processor_id: null, recycler_id: 35, converter_id: null };
  assert.deepStrictEqual(resolveBuyer(row), { kind: 'recycler', id: 35 });
});

test('12. PARTY_MAP shape — every entry has buyerKinds as an array', () => {
  // Guard against regression to the pre-fix scalar buyerKind shape.
  Object.keys(PARTY_MAP).forEach((t) => {
    const cfg = PARTY_MAP[t];
    assert.ok(Array.isArray(cfg.buyerKinds), t + '.buyerKinds is an array');
    assert.ok(cfg.buyerKinds.length >= 1, t + '.buyerKinds non-empty');
    assert.strictEqual(typeof cfg.sellerKind, 'string', t + '.sellerKind is a string');
  });
});

// ── Summary ────────────────────────────────────────────────────────────────
console.log('');
console.log(passed + ' passed, ' + failed + ' failed');
process.exit(failed > 0 ? 1 : 0);
