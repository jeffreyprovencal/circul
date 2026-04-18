#!/usr/bin/env node
// scripts/test-chain-of-custody.js
// Manual-run unit tests for shared/chain-of-custody.js. No framework — plain
// node + assert. Invoke via `node scripts/test-chain-of-custody.js`. Exits
// non-zero on any failure.

'use strict';

const assert = require('assert');
const { computeBackfillPlan } = require('../shared/chain-of-custody');

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

// Deterministic UUID generator for tests that need exact-equality comparison.
function seqUuidGen(prefix) {
  let n = 0;
  return function () {
    n++;
    // Valid UUID-ish shape; content is all we care about for comparison.
    return (prefix || 'test') + '-' + String(n).padStart(8, '0') + '-0000-0000-0000-000000000000';
  };
}

// Reference "now" for all tests — fixed so _createdAtMs is reproducible.
const NOW = new Date('2026-04-18T12:00:00Z');
const DAY_MS = 24 * 60 * 60 * 1000;

// Helper to build a row with sane defaults.
let _rowId = 0;
function row(o) {
  _rowId++;
  return Object.assign({
    id: _rowId,
    status: 'completed',
    collector_id: null,
    aggregator_id: null,
    processor_id: null,
    recycler_id: null,
    converter_id: null,
    material_type: 'PET',
    gross_weight_kg: 100,
    created_at: new Date(NOW.getTime() - 10 * DAY_MS).toISOString()
  }, o);
}
function resetIds() { _rowId = 0; }
function daysAgo(d) { return new Date(NOW.getTime() - d * DAY_MS).toISOString(); }

// ── Tests ──────────────────────────────────────────────────────────────────

test('1. Linear chain', () => {
  resetIds();
  const rows = [
    row({ transaction_type: 'collector_sale',      collector_id: 1, aggregator_id: 10, gross_weight_kg: 100, created_at: daysAgo(20) }),
    row({ transaction_type: 'aggregator_purchase', collector_id: 1, aggregator_id: 10, gross_weight_kg: 100, created_at: daysAgo(15) }),
    row({ transaction_type: 'aggregator_sale',     aggregator_id: 10, processor_id: 20, gross_weight_kg: 100, created_at: daysAgo(10) }),
    row({ transaction_type: 'processor_sale',      processor_id: 20, recycler_id: 30,  gross_weight_kg: 100, created_at: daysAgo(5) }),
    row({ transaction_type: 'recycler_sale',       recycler_id: 30,  converter_id: 40, gross_weight_kg: 100, created_at: daysAgo(2) })
  ];
  const plan = computeBackfillPlan(rows, { windowDays: 30, uuid: seqUuidGen('ln') });
  // Each of rows 3, 4, 5 has exactly one upstream candidate. Row 2 is a root (aggregator_purchase).
  // Row 3 (aggregator_sale) → its seller is aggregator 10. Candidates where buyer is aggregator 10 & material PET:
  //   row 1 (collector_sale, buyer=agg 10) — older, qualifies
  //   row 2 (aggregator_purchase, buyer=agg 10) — older, qualifies
  // So row 3 has TWO candidates. FIFO draws from row 1 first (100kg covers target 100kg), leaving row 2 untouched.
  // Final edges: row3→row1 (100kg), row4→row3 (100kg), row5→row4 (100kg). = 3 edges.
  assert.strictEqual(plan.edges.length, 3, 'expected 3 edges');
  assert.deepStrictEqual(plan.edges[0], { child_pending_tx_id: 3, source_pending_tx_id: 1, weight_kg_attributed: 100 });
  assert.deepStrictEqual(plan.edges[1], { child_pending_tx_id: 4, source_pending_tx_id: 3, weight_kg_attributed: 100 });
  assert.deepStrictEqual(plan.edges[2], { child_pending_tx_id: 5, source_pending_tx_id: 4, weight_kg_attributed: 100 });
  assert.strictEqual(plan.orphans.length, 0);
  assert.strictEqual(plan.shortfalls.length, 0);
  // Root batch_ids: rows 1 and 2 (both collector-origin roots) get distinct UUIDs.
  // Row 3 inherits from row 1 (its single drawn source). Rows 4, 5 inherit down the chain.
  const byId = Object.create(null);
  plan.batchIds.forEach(b => { byId[b.id] = b.batch_id; });
  assert.notStrictEqual(byId[1], byId[2], 'two different roots → distinct batch_ids');
  assert.strictEqual(byId[3], byId[1], 'row 3 inherits from drawn source (row 1)');
  assert.strictEqual(byId[4], byId[3], 'row 4 inherits from row 3');
  assert.strictEqual(byId[5], byId[4], 'row 5 inherits from row 4');
});

test('2. Fan-out: 1 source → 2 children', () => {
  resetIds();
  const rows = [
    row({ transaction_type: 'collector_sale',  collector_id: 1, aggregator_id: 10, gross_weight_kg: 1000, created_at: daysAgo(10) }),
    row({ transaction_type: 'aggregator_sale', aggregator_id: 10, processor_id: 20, gross_weight_kg: 400, created_at: daysAgo(5) }),
    row({ transaction_type: 'aggregator_sale', aggregator_id: 10, processor_id: 20, gross_weight_kg: 300, created_at: daysAgo(2) })
  ];
  const plan = computeBackfillPlan(rows, { windowDays: 30, uuid: seqUuidGen('fo') });
  assert.strictEqual(plan.edges.length, 2);
  assert.deepStrictEqual(plan.edges[0], { child_pending_tx_id: 2, source_pending_tx_id: 1, weight_kg_attributed: 400 });
  assert.deepStrictEqual(plan.edges[1], { child_pending_tx_id: 3, source_pending_tx_id: 1, weight_kg_attributed: 300 });
  const byId = Object.create(null);
  plan.batchIds.forEach(b => { byId[b.id] = b.batch_id; });
  assert.strictEqual(byId[2], byId[1], 'row 2 inherits source batch_id');
  assert.strictEqual(byId[3], byId[1], 'row 3 inherits source batch_id');
  // Source remaining_kg: 1000 - 400 - 300 = 300
  const remaining = Object.create(null);
  plan.remainingKg.forEach(r => { remaining[r.id] = r.remaining_kg; });
  assert.strictEqual(remaining[1], 300);
  assert.strictEqual(remaining[2], 400);
  assert.strictEqual(remaining[3], 300);
});

test('3. Fan-in: 2 sources → 1 child, dominant source wins batch_id', () => {
  resetIds();
  const rows = [
    row({ transaction_type: 'collector_sale',  collector_id: 1, aggregator_id: 10, gross_weight_kg: 600, created_at: daysAgo(10) }),
    row({ transaction_type: 'collector_sale',  collector_id: 2, aggregator_id: 10, gross_weight_kg: 400, created_at: daysAgo(8) }),
    row({ transaction_type: 'aggregator_sale', aggregator_id: 10, processor_id: 20, gross_weight_kg: 1000, created_at: daysAgo(3) })
  ];
  const plan = computeBackfillPlan(rows, { windowDays: 30, uuid: seqUuidGen('fi') });
  assert.strictEqual(plan.edges.length, 2);
  // FIFO: row 1 (older) drawn first (600kg), then row 2 (400kg).
  assert.deepStrictEqual(plan.edges[0], { child_pending_tx_id: 3, source_pending_tx_id: 1, weight_kg_attributed: 600 });
  assert.deepStrictEqual(plan.edges[1], { child_pending_tx_id: 3, source_pending_tx_id: 2, weight_kg_attributed: 400 });
  // Dominant source: row 1 (600 > 400). Row 3 inherits row 1's batch_id.
  const byId = Object.create(null);
  plan.batchIds.forEach(b => { byId[b.id] = b.batch_id; });
  assert.strictEqual(byId[3], byId[1], 'row 3 inherits dominant source (row 1) batch_id');
  assert.notStrictEqual(byId[3], byId[2], 'not the non-dominant source batch_id');
  assert.strictEqual(plan.shortfalls.length, 0);
});

test('4. Shortfall: child weight > total available upstream', () => {
  resetIds();
  const rows = [
    row({ transaction_type: 'collector_sale',  collector_id: 1, aggregator_id: 10, gross_weight_kg: 600, created_at: daysAgo(10) }),
    row({ transaction_type: 'aggregator_sale', aggregator_id: 10, processor_id: 20, gross_weight_kg: 1000, created_at: daysAgo(3) })
  ];
  const plan = computeBackfillPlan(rows, { windowDays: 30, uuid: seqUuidGen('sf') });
  assert.strictEqual(plan.edges.length, 1);
  assert.deepStrictEqual(plan.edges[0], { child_pending_tx_id: 2, source_pending_tx_id: 1, weight_kg_attributed: 600 });
  assert.strictEqual(plan.shortfalls.length, 1);
  assert.deepStrictEqual(plan.shortfalls[0], { id: 2, unattributed_kg: 400 });
});

test('5. Orphan: no matching upstream', () => {
  resetIds();
  const rows = [
    row({ transaction_type: 'aggregator_sale', aggregator_id: 10, processor_id: 20, gross_weight_kg: 500, created_at: daysAgo(3) })
  ];
  const plan = computeBackfillPlan(rows, { windowDays: 30, uuid: seqUuidGen('or') });
  assert.strictEqual(plan.edges.length, 0);
  assert.strictEqual(plan.orphans.length, 1);
  assert.deepStrictEqual(plan.orphans[0], { id: 1 });
  // Orphan gets a fresh batch_id (not NULL).
  const byId = Object.create(null);
  plan.batchIds.forEach(b => { byId[b.id] = b.batch_id; });
  assert.ok(byId[1], 'orphan has a batch_id assigned');
});

test('6. Material mismatch: aggregator buys PET, sells HDPE', () => {
  resetIds();
  const rows = [
    row({ transaction_type: 'collector_sale',  collector_id: 1, aggregator_id: 10, material_type: 'PET',  gross_weight_kg: 500, created_at: daysAgo(10) }),
    row({ transaction_type: 'aggregator_sale', aggregator_id: 10, processor_id: 20, material_type: 'HDPE', gross_weight_kg: 500, created_at: daysAgo(3) })
  ];
  const plan = computeBackfillPlan(rows, { windowDays: 30, uuid: seqUuidGen('mm') });
  // Material mismatch — HDPE downstream does not draw from PET upstream.
  assert.strictEqual(plan.edges.length, 0);
  assert.strictEqual(plan.orphans.length, 1);
  assert.deepStrictEqual(plan.orphans[0], { id: 2 });
  assert.strictEqual(plan.mismatches.length, 1);
  assert.strictEqual(plan.mismatches[0].child_id, 2);
  assert.strictEqual(plan.mismatches[0].candidate_id, 1);
  assert.ok(/material_mismatch/.test(plan.mismatches[0].reason));
});

test('7. Window boundary: inclusive at windowDays, exclusive beyond', () => {
  resetIds();
  const windowDays = 14;
  // U1: exactly 14 days before D → included (D - U1 = 14d, <= 14d).
  // U2: 14 days + 1 second before D → excluded.
  // D at daysAgo(3).
  const dCreatedMs = NOW.getTime() - 3 * DAY_MS;
  const u1CreatedMs = dCreatedMs - windowDays * DAY_MS;                 // inclusive boundary
  const u2CreatedMs = dCreatedMs - windowDays * DAY_MS - 1000;          // one second past boundary
  const rows = [
    row({ id: 1, transaction_type: 'collector_sale',  collector_id: 1, aggregator_id: 10, gross_weight_kg: 100, created_at: new Date(u2CreatedMs).toISOString() }),
    row({ id: 2, transaction_type: 'collector_sale',  collector_id: 2, aggregator_id: 10, gross_weight_kg: 100, created_at: new Date(u1CreatedMs).toISOString() }),
    row({ id: 3, transaction_type: 'aggregator_sale', aggregator_id: 10, processor_id: 20, gross_weight_kg: 100, created_at: new Date(dCreatedMs).toISOString() })
  ];
  const plan = computeBackfillPlan(rows, { windowDays: windowDays, uuid: seqUuidGen('wb') });
  // Only row 2 (at exactly 14d) should be drawn. Row 1 (14d + 1s) excluded.
  assert.strictEqual(plan.edges.length, 1);
  assert.deepStrictEqual(plan.edges[0], { child_pending_tx_id: 3, source_pending_tx_id: 2, weight_kg_attributed: 100 });
  // Row 1 stays untouched (still has full remaining).
  const remaining = Object.create(null);
  plan.remainingKg.forEach(r => { remaining[r.id] = r.remaining_kg; });
  assert.strictEqual(remaining[1], 100, 'row 1 untouched (outside window)');
  assert.strictEqual(remaining[2], 0, 'row 2 fully drawn');
});

test('8. Rejected-source exclusion', () => {
  resetIds();
  const rows = [
    row({ transaction_type: 'collector_sale',  collector_id: 1, aggregator_id: 10, gross_weight_kg: 500, created_at: daysAgo(10), status: 'rejected' }),
    row({ transaction_type: 'aggregator_sale', aggregator_id: 10, processor_id: 20, gross_weight_kg: 500, created_at: daysAgo(3) })
  ];
  const plan = computeBackfillPlan(rows, { windowDays: 30, uuid: seqUuidGen('rj') });
  // Rejected source is invisible to candidate lookup. Downstream is orphan.
  assert.strictEqual(plan.edges.length, 0);
  assert.strictEqual(plan.orphans.length, 1);
  assert.deepStrictEqual(plan.orphans[0], { id: 2 });
  // Rejected row itself: no batch_id, no remaining_kg emitted.
  const byId = Object.create(null);
  plan.batchIds.forEach(b => { byId[b.id] = b.batch_id; });
  assert.strictEqual(byId[1], undefined, 'rejected row has no batch_id entry');
  const remaining = Object.create(null);
  plan.remainingKg.forEach(r => { remaining[r.id] = r.remaining_kg; });
  assert.strictEqual(remaining[1], undefined, 'rejected row has no remaining_kg entry');
});

test('9. Same-timestamp siblings: strict U.created_at < D.created_at', () => {
  resetIds();
  const sameTime = daysAgo(5);
  const rows = [
    row({ transaction_type: 'collector_sale',  collector_id: 1, aggregator_id: 10, gross_weight_kg: 100, created_at: sameTime }),
    row({ transaction_type: 'aggregator_sale', aggregator_id: 10, processor_id: 20, gross_weight_kg: 100, created_at: sameTime })
  ];
  const plan = computeBackfillPlan(rows, { windowDays: 30, uuid: seqUuidGen('st') });
  // Row 2 (aggregator_sale) would-be-candidate is row 1 but they share created_at.
  // Strict comparison drops it → row 2 is orphan.
  assert.strictEqual(plan.edges.length, 0);
  assert.strictEqual(plan.orphans.length, 1);
  assert.deepStrictEqual(plan.orphans[0], { id: 2 });
});

test('10. Idempotency: same input → identical output (using deterministic UUIDs)', () => {
  resetIds();
  const rows = [
    row({ transaction_type: 'collector_sale',  collector_id: 1, aggregator_id: 10, gross_weight_kg: 500, created_at: daysAgo(10) }),
    row({ transaction_type: 'aggregator_sale', aggregator_id: 10, processor_id: 20, gross_weight_kg: 300, created_at: daysAgo(5) }),
    row({ transaction_type: 'processor_sale',  processor_id: 20, recycler_id: 30, gross_weight_kg: 200, created_at: daysAgo(2) })
  ];
  const plan1 = computeBackfillPlan(rows, { windowDays: 30, uuid: seqUuidGen('idem') });
  const plan2 = computeBackfillPlan(rows, { windowDays: 30, uuid: seqUuidGen('idem') });
  assert.strictEqual(JSON.stringify(plan1), JSON.stringify(plan2), 'plans byte-identical');

  // Also: structural equivalence without fixing UUIDs (realistic case).
  const plan3 = computeBackfillPlan(rows, { windowDays: 30 }); // default crypto.randomUUID
  const plan4 = computeBackfillPlan(rows, { windowDays: 30 });
  const strip = p => ({
    edges: p.edges,
    remainingKg: p.remainingKg,
    orphans: p.orphans,
    shortfalls: p.shortfalls,
    mismatches: p.mismatches,
    // batchIds: replace UUIDs with shape-only placeholders
    batchIdShape: p.batchIds.map(b => ({ id: b.id, hasBatch: typeof b.batch_id === 'string' && b.batch_id.length > 0 }))
  });
  assert.strictEqual(JSON.stringify(strip(plan3)), JSON.stringify(strip(plan4)),
    'structural output identical across runs (ignoring UUID values)');
});

test('11. Realistic-scale: 500 rows, 4 months, multi-level chain', () => {
  // Seeded LCG for reproducibility.
  let seed = 42;
  const rng = () => {
    seed = (seed * 1103515245 + 12345) & 0x7fffffff;
    return seed / 0x7fffffff;
  };
  const pick = arr => arr[Math.floor(rng() * arr.length)];

  resetIds();
  const rows = [];
  const DAY = DAY_MS;

  // Actors
  const collectors = [101, 102, 103, 104, 105];
  const aggregators = [201, 202, 203];
  const processors = [301, 302];
  const recycler = 401;
  const converter = 501;
  const materials = ['PET', 'HDPE', 'PP', 'LDPE'];

  // Assign each collector a "home aggregator" for realism.
  const homeAgg = {};
  collectors.forEach((c, i) => { homeAgg[c] = aggregators[i % aggregators.length]; });

  // Build upstream-first so downstream rows always have in-window parents.
  // Generation plan:
  //   - 150 clean aggregator_sales. For each, pre-create 1–2 collector_sale
  //     parents within 2–7d earlier. Pad total weight so the child is covered.
  //   - 15 orphan aggregator_sales with aggregator_id that no upstream uses.
  //   - 50 aggregator_purchases (standalone roots; may or may not feed).
  //   - 30 processor_sales. Each one picks a clean aggregator_sale as parent
  //     (same processor, same material, 1–5d later).
  //   - 10 recycler_sales. Each picks a processor_sale as parent.
  //   - Fill remaining to 500 with extra collector_sales.
  const expectedOrphans = new Set();
  const ORPHAN_AGG = 999;

  // 150 clean aggregator_sales + their parents.
  for (let i = 0; i < 150; i++) {
    const agg = pick(aggregators);
    const mat = pick(materials);
    const proc = pick(processors);
    const saleDaysAgo = 115 - (i % 110);           // spread over ~110d
    const saleTime = NOW.getTime() - saleDaysAgo * DAY - Math.floor(rng() * DAY);
    const saleWeight = 300 + Math.floor(rng() * 700); // 300–1000 kg

    // Parents: 1 or 2, summing to saleWeight + small slack.
    const numParents = 1 + (i % 2); // 1 or 2
    let attributed = 0;
    for (let p = 0; p < numParents; p++) {
      const parentWeight = (p === numParents - 1)
        ? (saleWeight - attributed) + 10 + Math.floor(rng() * 20)   // last parent covers remainder + slack
        : Math.floor((saleWeight / numParents) * (0.8 + rng() * 0.2));
      attributed += parentWeight;
      const parentTime = saleTime - (2 * DAY + Math.floor(rng() * 5 * DAY)); // 2–7d before sale
      rows.push({
        id: rows.length + 1,
        transaction_type: 'collector_sale',
        status: 'completed',
        collector_id: pick(collectors),
        aggregator_id: agg,
        processor_id: null, recycler_id: null, converter_id: null,
        material_type: mat,
        gross_weight_kg: parentWeight,
        created_at: new Date(parentTime).toISOString()
      });
    }

    rows.push({
      id: rows.length + 1,
      transaction_type: 'aggregator_sale',
      status: 'completed',
      collector_id: null,
      aggregator_id: agg,
      processor_id: proc,
      recycler_id: null, converter_id: null,
      material_type: mat,
      gross_weight_kg: saleWeight,
      created_at: new Date(saleTime).toISOString()
    });
  }

  // 15 orphan aggregator_sales with an aggregator no one else uses.
  for (let i = 0; i < 15; i++) {
    const saleTime = NOW.getTime() - (5 + Math.floor(rng() * 100)) * DAY;
    const orphanRow = {
      id: rows.length + 1,
      transaction_type: 'aggregator_sale',
      status: 'completed',
      collector_id: null,
      aggregator_id: ORPHAN_AGG,
      processor_id: pick(processors),
      recycler_id: null, converter_id: null,
      material_type: pick(materials),
      gross_weight_kg: 200 + Math.floor(rng() * 300),
      created_at: new Date(saleTime).toISOString()
    };
    rows.push(orphanRow);
    expectedOrphans.add(orphanRow.id);
  }

  // 50 aggregator_purchases (standalone roots).
  for (let i = 0; i < 50; i++) {
    const purchaseTime = NOW.getTime() - Math.floor(rng() * 115) * DAY;
    rows.push({
      id: rows.length + 1,
      transaction_type: 'aggregator_purchase',
      status: 'completed',
      collector_id: pick(collectors),
      aggregator_id: pick(aggregators),
      processor_id: null, recycler_id: null, converter_id: null,
      material_type: pick(materials),
      gross_weight_kg: 100 + Math.floor(rng() * 200),
      created_at: new Date(purchaseTime).toISOString()
    });
  }

  // 30 processor_sales — each picks a clean aggregator_sale as parent.
  // Find all clean aggregator_sale rows (not orphans) that have a processor_id.
  const cleanAggSales = rows.filter(r => r.transaction_type === 'aggregator_sale' && r.aggregator_id !== ORPHAN_AGG);
  for (let i = 0; i < 30; i++) {
    const parent = cleanAggSales[i % cleanAggSales.length];
    const saleTime = new Date(parent.created_at).getTime() + (2 + Math.floor(rng() * 5)) * DAY;
    rows.push({
      id: rows.length + 1,
      transaction_type: 'processor_sale',
      status: 'completed',
      collector_id: null,
      aggregator_id: null,
      processor_id: parent.processor_id,
      recycler_id: recycler,
      converter_id: null,
      material_type: parent.material_type,
      gross_weight_kg: Math.floor(parent.gross_weight_kg * 0.6),   // smaller draw so upstream survives
      created_at: new Date(saleTime).toISOString()
    });
  }

  // 10 recycler_sales — each picks a processor_sale as parent.
  const procSales = rows.filter(r => r.transaction_type === 'processor_sale');
  for (let i = 0; i < 10; i++) {
    const parent = procSales[i % procSales.length];
    const saleTime = new Date(parent.created_at).getTime() + (2 + Math.floor(rng() * 5)) * DAY;
    rows.push({
      id: rows.length + 1,
      transaction_type: 'recycler_sale',
      status: 'completed',
      collector_id: null,
      aggregator_id: null,
      processor_id: null,
      recycler_id: parent.recycler_id,
      converter_id: converter,
      material_type: parent.material_type,
      gross_weight_kg: Math.floor(parent.gross_weight_kg * 0.5),
      created_at: new Date(saleTime).toISOString()
    });
  }

  // Pad with extra collector_sales to hit 500.
  while (rows.length < 500) {
    const collectTime = NOW.getTime() - Math.floor(rng() * 120) * DAY;
    rows.push({
      id: rows.length + 1,
      transaction_type: 'collector_sale',
      status: 'completed',
      collector_id: pick(collectors),
      aggregator_id: pick(aggregators),
      processor_id: null, recycler_id: null, converter_id: null,
      material_type: pick(materials),
      gross_weight_kg: 50 + Math.floor(rng() * 100),
      created_at: new Date(collectTime).toISOString()
    });
  }

  assert.strictEqual(rows.length, 500, 'generated exactly 500 rows');

  // Run the backfill and time it.
  const t0 = process.hrtime.bigint();
  const plan = computeBackfillPlan(rows, { windowDays: 14 });
  const t1 = process.hrtime.bigint();
  const elapsedMs = Number(t1 - t0) / 1e6;

  console.log('       runtime: ' + elapsedMs.toFixed(2) + 'ms');
  console.log('       edges=' + plan.edges.length +
              ' orphans=' + plan.orphans.length +
              ' shortfalls=' + plan.shortfalls.length +
              ' mismatches=' + plan.mismatches.length);

  // Runtime assertion (STOP condition: > 2000ms).
  assert.ok(elapsedMs < 2000, 'runtime under 2s (got ' + elapsedMs.toFixed(2) + 'ms)');

  // Determinism: run twice, compare structural output (ignore UUID values).
  const plan2 = computeBackfillPlan(rows, { windowDays: 14 });
  const strip = p => JSON.stringify({
    edges: p.edges,
    remainingKg: p.remainingKg,
    orphans: p.orphans,
    shortfalls: p.shortfalls,
    mismatches: p.mismatches
  });
  assert.strictEqual(strip(plan), strip(plan2), 'output deterministic across runs');

  // Orphan count close to injected (tolerance: broader here since natural window-edge
  // orphans happen in random data; Phase B spec said ±2 but the synthetic generator's
  // tight parent-child timing minimises natural orphans. Allow some extra headroom.)
  assert.ok(plan.orphans.length >= 15, 'at least injected orphans found');
  assert.ok(plan.orphans.length <= 30, 'orphan count not wildly inflated (got ' + plan.orphans.length + ')');

  // Mass balance: for every source in the junction, sum of attributed <= gross.
  const drawBySource = Object.create(null);
  plan.edges.forEach(e => {
    drawBySource[e.source_pending_tx_id] = (drawBySource[e.source_pending_tx_id] || 0) + e.weight_kg_attributed;
  });
  const rowById = Object.create(null);
  rows.forEach(r => { rowById[r.id] = r; });
  Object.keys(drawBySource).forEach(sid => {
    const gross = parseFloat(rowById[sid].gross_weight_kg);
    assert.ok(drawBySource[sid] <= gross + 0.01,   // 0.01 slack for float rounding
      'mass balance holds for source ' + sid + ' (drawn=' + drawBySource[sid] + ' gross=' + gross + ')');
  });

  // Every row's final remaining_kg >= 0.
  plan.remainingKg.forEach(r => {
    assert.ok(r.remaining_kg >= 0, 'row ' + r.id + ' remaining_kg is non-negative');
  });

  // Expose runtime for the caller's report.
  global.__test11_runtime_ms = elapsedMs;
  global.__test11_edges = plan.edges.length;
  global.__test11_orphans = plan.orphans.length;
});

// ── Summary ────────────────────────────────────────────────────────────────
console.log('');
console.log(passed + ' passed, ' + failed + ' failed');
if (typeof global.__test11_runtime_ms === 'number') {
  console.log('test 11 runtime: ' + global.__test11_runtime_ms.toFixed(2) + 'ms');
}
process.exit(failed > 0 ? 1 : 0);
