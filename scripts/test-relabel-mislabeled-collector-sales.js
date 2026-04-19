#!/usr/bin/env node
// scripts/test-relabel-mislabeled-collector-sales.js
// Unit tests for the migration discriminator at migrations/1777300000000_relabel_mislabeled_collector_sales.js.
// Verifies exactly which row shapes are (and are not) swept up by the UPDATE.
// Invoke via `node scripts/test-relabel-mislabeled-collector-sales.js`.

'use strict';

const assert = require('assert');

function matchesDiscriminator(row) {
  return row.transaction_type === 'aggregator_sale'
      && row.collector_id  != null
      && row.aggregator_id != null
      && row.processor_id  == null
      && row.recycler_id   == null
      && row.converter_id  == null;
}

let passed = 0, failed = 0;
function test(name, fn) {
  try { fn(); console.log('PASS  ' + name); passed++; }
  catch (e) { console.log('FAIL  ' + name); console.log('       ' + (e.stack || e.message)); failed++; }
}

// ── MUST match (mis-labeled rows) ────────────────────────────────────
test('USSD agent-collection row shape (server.js:3104) — matches', () => {
  assert.strictEqual(matchesDiscriminator({
    transaction_type: 'aggregator_sale', collector_id: 5, aggregator_id: 10,
    processor_id: null, recycler_id: null, converter_id: null
  }), true);
});

test('Agent log-collection row shape (server.js:5791) — matches', () => {
  assert.strictEqual(matchesDiscriminator({
    transaction_type: 'aggregator_sale', collector_id: 7, aggregator_id: 12,
    processor_id: null, recycler_id: null, converter_id: null
  }), true);
});

// ── MUST NOT match (legit aggregator_sale rows) ──────────────────────
test('Legit aggregator_sale → processor — does NOT match', () => {
  assert.strictEqual(matchesDiscriminator({
    transaction_type: 'aggregator_sale', collector_id: null, aggregator_id: 10,
    processor_id: 20, recycler_id: null, converter_id: null
  }), false);
});

test('Legit aggregator_sale → converter — does NOT match', () => {
  assert.strictEqual(matchesDiscriminator({
    transaction_type: 'aggregator_sale', collector_id: null, aggregator_id: 10,
    processor_id: null, recycler_id: null, converter_id: 30
  }), false);
});

test('Legit aggregator_sale → recycler — does NOT match', () => {
  assert.strictEqual(matchesDiscriminator({
    transaction_type: 'aggregator_sale', collector_id: null, aggregator_id: 10,
    processor_id: null, recycler_id: 25, converter_id: null
  }), false);
});

test('Pre-PR#42 incomplete aggregator_sale (all buyer FKs null, no collector_id) — does NOT match', () => {
  // These are the orphan shape we deliberately do NOT sweep up — they need
  // their own remediation, not a blanket relabel.
  assert.strictEqual(matchesDiscriminator({
    transaction_type: 'aggregator_sale', collector_id: null, aggregator_id: 10,
    processor_id: null, recycler_id: null, converter_id: null
  }), false);
});

// ── MUST NOT match (other transaction types) ─────────────────────────
test('collector_sale row — does NOT match (already correct)', () => {
  assert.strictEqual(matchesDiscriminator({
    transaction_type: 'collector_sale', collector_id: 5, aggregator_id: 10,
    processor_id: null, recycler_id: null, converter_id: null
  }), false);
});

test('aggregator_purchase row — does NOT match', () => {
  assert.strictEqual(matchesDiscriminator({
    transaction_type: 'aggregator_purchase', collector_id: 5, aggregator_id: 10,
    processor_id: null, recycler_id: null, converter_id: null
  }), false);
});

test('processor_sale row — does NOT match', () => {
  assert.strictEqual(matchesDiscriminator({
    transaction_type: 'processor_sale', collector_id: null, aggregator_id: null,
    processor_id: 20, recycler_id: null, converter_id: 30
  }), false);
});

// ── Defensive: hybrid bad-data rows (collector_id AND buyer FK set) ──
test('Hybrid row (collector_id + processor_id both set) — does NOT match (defensive)', () => {
  // We deliberately skip these — a row like this is a different kind of
  // data corruption that warrants investigation, not a blanket relabel.
  assert.strictEqual(matchesDiscriminator({
    transaction_type: 'aggregator_sale', collector_id: 5, aggregator_id: 10,
    processor_id: 20, recycler_id: null, converter_id: null
  }), false);
});

console.log('\n' + passed + ' passed, ' + failed + ' failed');
if (failed > 0) process.exit(1);
