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
  resolveBuyer,
  resolveParties,
  validateBuyerFks
} = require('../shared/transaction-parties');

const transactionParties = require('../shared/transaction-parties');

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

// ── validateBuyerFks (write-boundary helper) ───────────────────────────────

test('15. aggregator_sale + processor_id only → ok (dominant flow regression)', () => {
  const r = validateBuyerFks('aggregator_sale', { processor_id: 20 });
  assert.deepStrictEqual(r, { ok: true, kind: 'processor', id: 20 });
});

test('16. aggregator_sale + converter_id only (Miniplast-style) → ok', () => {
  const r = validateBuyerFks('aggregator_sale', { converter_id: 42 });
  assert.deepStrictEqual(r, { ok: true, kind: 'converter', id: 42 });
});

test('17. aggregator_sale + recycler_id only → ok (new rare Ghana flow)', () => {
  const r = validateBuyerFks('aggregator_sale', { recycler_id: 35 });
  assert.deepStrictEqual(r, { ok: true, kind: 'recycler', id: 35 });
});

test('18. aggregator_sale + processor_id + converter_id → reject', () => {
  const r = validateBuyerFks('aggregator_sale', { processor_id: 1, converter_id: 2 });
  assert.strictEqual(r.ok, false);
  assert.ok(/only one buyer FK may be set for aggregator_sale/.test(r.message));
  assert.ok(/processor_id/.test(r.message));
  assert.ok(/converter_id/.test(r.message));
});

test('19. aggregator_sale + processor_id + recycler_id → reject', () => {
  const r = validateBuyerFks('aggregator_sale', { processor_id: 1, recycler_id: 3 });
  assert.strictEqual(r.ok, false);
  assert.ok(/only one buyer FK may be set for aggregator_sale/.test(r.message));
  assert.ok(/processor_id/.test(r.message));
  assert.ok(/recycler_id/.test(r.message));
});

test('20. aggregator_sale + converter_id + recycler_id → reject', () => {
  const r = validateBuyerFks('aggregator_sale', { converter_id: 2, recycler_id: 3 });
  assert.strictEqual(r.ok, false);
  assert.ok(/only one buyer FK may be set for aggregator_sale/.test(r.message));
  assert.ok(/converter_id/.test(r.message));
  assert.ok(/recycler_id/.test(r.message));
});

test('21. aggregator_sale + all three → reject, message names all three', () => {
  const r = validateBuyerFks('aggregator_sale', { processor_id: 1, converter_id: 2, recycler_id: 3 });
  assert.strictEqual(r.ok, false);
  assert.ok(/processor_id/.test(r.message));
  assert.ok(/converter_id/.test(r.message));
  assert.ok(/recycler_id/.test(r.message));
});

test('22. aggregator_sale + none → reject', () => {
  const r = validateBuyerFks('aggregator_sale', {});
  assert.strictEqual(r.ok, false);
  assert.ok(/is required for aggregator_sale/.test(r.message));
  // Message enumerates all three valid kinds.
  assert.ok(/processor_id/.test(r.message));
  assert.ok(/recycler_id/.test(r.message));
  assert.ok(/converter_id/.test(r.message));
});

test('23. processor_sale + recycler_id only → ok', () => {
  const r = validateBuyerFks('processor_sale', { recycler_id: 30 });
  assert.deepStrictEqual(r, { ok: true, kind: 'recycler', id: 30 });
});

test('24. processor_sale + converter_id only → ok', () => {
  const r = validateBuyerFks('processor_sale', { converter_id: 40 });
  assert.deepStrictEqual(r, { ok: true, kind: 'converter', id: 40 });
});

test('25. processor_sale + both → reject', () => {
  const r = validateBuyerFks('processor_sale', { converter_id: 40, recycler_id: 30 });
  assert.strictEqual(r.ok, false);
  assert.ok(/only one buyer FK may be set for processor_sale/.test(r.message));
  assert.ok(/converter_id/.test(r.message));
  assert.ok(/recycler_id/.test(r.message));
});

test('26. validateBuyerFks + unknown transaction_type → reject gracefully', () => {
  const r = validateBuyerFks('made_up_type', { processor_id: 1 });
  assert.strictEqual(r.ok, false);
  assert.ok(/unknown transaction_type/.test(r.message));
  // Must not throw (distinct from resolveBuyer's strict behavior).
});

test('27. validateBuyerFks ignores irrelevant FKs for the transaction_type', () => {
  // collector_id isn't a valid BUYER for aggregator_sale (it's the seller side
  // on collector_sale / aggregator_purchase). Per the helper's docstring,
  // irrelevant FKs are IGNORED — seller-FK validation belongs on the caller.
  const r = validateBuyerFks('aggregator_sale', { collector_id: 99, processor_id: 20 });
  assert.deepStrictEqual(r, { ok: true, kind: 'processor', id: 20 });

  // And an aggregator_sale with ONLY an irrelevant FK should be treated as
  // missing a buyer (collector_id doesn't count toward buyer polymorphism).
  const r2 = validateBuyerFks('aggregator_sale', { collector_id: 99 });
  assert.strictEqual(r2.ok, false);
  assert.ok(/is required for aggregator_sale/.test(r2.message));
});

test('28. Exported-symbols guard (validateBuyerFks joins the public API)', () => {
  assert.strictEqual(typeof transactionParties.PARTY_MAP, 'object');
  assert.strictEqual(typeof transactionParties.KIND_TO_TABLE, 'object');
  assert.strictEqual(typeof transactionParties.resolveSeller, 'function');
  assert.strictEqual(typeof transactionParties.resolveBuyer, 'function');
  assert.strictEqual(typeof transactionParties.validateBuyerFks, 'function');
  assert.strictEqual(typeof transactionParties.resolveParties, 'function');
  assert.strictEqual(typeof transactionParties.userOwnsParty, 'function');
});

// ── resolveParties: transactions-table shape inference ────────────────────
// Regression guard for the 12-day latent payment-auth 403 bug. The transactions
// table has no transaction_type column (see 1774500000000_restructure_tiers.js);
// without inference, resolveParties would short-circuit and return {buyerKind:
// null}, which breaks the ownership gate at server.js:4884 / 4918.

// resolveParties is async but the test harness is sync — wrap the assertion
// in an IIFE + asyncTest helper that completes before moving on.
function asyncTest(name, fn) {
  // Defer synchronous result collection — plain node await at module scope
  // isn't available pre-ESM, so we chain a sentinel. We call fn(), which
  // returns a promise; inside, it throws on failure. We synchronously push a
  // pending record and then resolve it via the chain.
  return fn()
    .then(() => { console.log('PASS  ' + name); passed++; })
    .catch((e) => { console.log('FAIL  ' + name); console.log('       ' + (e.stack || e.message)); failed++; });
}

(async () => {
  await asyncTest('29. resolveParties: infers collector_sale from transactions-table row shape', async () => {
    // transactions row — no transaction_type column
    const row = {
      id: 1,
      collector_id: 5,
      aggregator_id: 10,
      material_type: 'PET',
      gross_weight_kg: '23.00',
      total_price: '53.13',
      created_at: '2026-04-19T10:00:00Z'
      // NOTE: no transaction_type field — this is the transactions-table shape
    };
    const mockPool = {
      query: async (sql, _params) => {
        if (sql.includes('FROM collectors')) return { rows: [{ id: 5, phone: '0241000001', name: 'Ama Mensah' }] };
        if (sql.includes('FROM aggregators')) return { rows: [{ id: 10, phone: '0300000002', name: 'Kwesi Amankwah' }] };
        return { rows: [] };
      }
    };
    const result = await resolveParties(mockPool, row);
    assert.strictEqual(result.sellerKind, 'collector');
    assert.strictEqual(result.buyerKind, 'aggregator');
    assert.strictEqual(result.seller.id, 5);
    assert.strictEqual(result.buyer.id, 10);
    // Caller's row must not have been mutated.
    assert.strictEqual(row.transaction_type, undefined, 'caller row.transaction_type must remain undefined');
  });

  await asyncTest('30. resolveParties: inference does not override explicit transaction_type', async () => {
    // pending_transactions row with explicit transaction_type — inference must NOT fire.
    const row = {
      id: 1,
      transaction_type: 'aggregator_purchase',
      collector_id: 5,
      aggregator_id: 10,
      material_type: 'PET'
    };
    const mockPool = { query: async () => ({ rows: [{ id: 5, phone: '', name: 'X' }] }) };
    const result = await resolveParties(mockPool, row);
    // aggregator_purchase has the same party shape as collector_sale, so the
    // kinds are identical — the key assertion is that the input row's
    // transaction_type was NOT silently rewritten to 'collector_sale'.
    assert.strictEqual(result.sellerKind, 'collector');
    assert.strictEqual(result.buyerKind, 'aggregator');
    assert.strictEqual(row.transaction_type, 'aggregator_purchase', 'caller row.transaction_type must not be mutated');
  });

  // ── Summary ──────────────────────────────────────────────────────────────
  console.log('');
  console.log(passed + ' passed, ' + failed + ' failed');
  process.exit(failed > 0 ? 1 : 0);
})();
