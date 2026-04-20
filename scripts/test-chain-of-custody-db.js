#!/usr/bin/env node
// scripts/test-chain-of-custody-db.js
// DB-integration tests for shared/chain-of-custody-db.js. Requires
// DATABASE_URL pointing at a local pg (e.g. circul_local). Each test opens
// its own transaction, seeds rows, runs assertions, then ROLLBACKS — no state
// leaks into the DB, script is safe to run repeatedly.
//
// Invoke: `set -a; source .env; set +a; node scripts/test-chain-of-custody-db.js`

'use strict';

const { Pool } = require('pg');
const {
  attributeAndInsert,
  insertRootTransaction,
  InsufficientSourceError
} = require('../shared/chain-of-custody-db');

const pool = new Pool({ connectionString: process.env.DATABASE_URL });

let passed = 0, failed = 0;

async function runTest(name, fn) {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await fn(client);
    console.log('PASS  ' + name);
    passed++;
  } catch (e) {
    console.log('FAIL  ' + name);
    console.log('       ' + (e.stack || e.message));
    failed++;
  } finally {
    // Always rollback — tests never commit.
    try { await client.query('ROLLBACK'); } catch (_) {}
    client.release();
  }
}

// Seeded demo ids (see migrations/1774500000000_restructure_tiers.js
// + 1776800000000_seed_demo_agent.js). All tests use these.
const AGG_ID = 9;         // Kwesi Amankwah's aggregator
const PROC_ID = 1;        // Jeffrey / rePATRN processor

// Clear all pending_transactions within the current txn so FIFO sees a
// clean slate. Junction rows cascade-delete via FK (ON DELETE CASCADE).
// All writes roll back at end of test — zero state leak. Required because
// circul_local accumulates smoke-test rows from prior merges that would
// otherwise pollute FIFO ordering and break order-sensitive assertions.
async function isolateScope(client) {
  await client.query('DELETE FROM pending_transactions');
}

async function main() {
  // ── Case 1: happy path — root + downstream, attribution works end-to-end.
  await runTest('db: happy path — attributeAndInsert writes row + edge + decrements source', async (client) => {
    await isolateScope(client);
    // Seed a root (collector_sale) with 100kg.
    const { row: rootRow } = await insertRootTransaction(client, {
      transaction_type: 'collector_sale',
      collector_id: 1,
      aggregator_id: AGG_ID,
      material_type: 'PET',
      gross_weight_kg: 100,
      price_per_kg: 2,
      total_price: 200
    });
    assertEq(rootRow.transaction_type, 'collector_sale', 'root type');
    assertTruthy(rootRow.batch_id, 'root has batch_id');
    assertEq(Number(rootRow.remaining_kg), 100, 'root remaining_kg = gross');

    // Run attribution: aggregator sells 60kg to processor.
    const { row: childRow, sources } = await attributeAndInsert(client, {
      transaction_type: 'aggregator_sale',
      aggregator_id: AGG_ID,
      processor_id: PROC_ID,
      material_type: 'PET',
      gross_weight_kg: 60,
      price_per_kg: 3,
      total_price: 180
    });

    assertEq(childRow.transaction_type, 'aggregator_sale', 'child type');
    assertEq(Number(childRow.remaining_kg), 60, 'child remaining_kg = its gross');
    assertEq(childRow.batch_id, rootRow.batch_id, 'child inherits root batch_id');
    assertEq(sources.length, 1, 'one source edge');
    assertEq(sources[0].id, rootRow.id, 'edge points at the root row');
    assertEq(Number(sources[0].weight_kg_attributed), 60, 'attributed 60kg');

    // Verify junction row exists.
    const junction = await client.query(
      'SELECT * FROM pending_transaction_sources WHERE child_pending_tx_id = $1',
      [childRow.id]
    );
    assertEq(junction.rows.length, 1, 'exactly one junction row');
    assertEq(Number(junction.rows[0].source_pending_tx_id), Number(rootRow.id), 'junction.source = root');
    assertEq(Number(junction.rows[0].weight_kg_attributed), 60, 'junction weight = 60');

    // Verify root's remaining_kg decremented.
    const rootAfter = await client.query('SELECT remaining_kg FROM pending_transactions WHERE id = $1', [rootRow.id]);
    assertEq(Number(rootAfter.rows[0].remaining_kg), 40, 'root remaining_kg decremented to 40');
  });

  // ── Case 2: shortfall rollback atomicity
  await runTest('db: shortfall throws InsufficientSourceError AND leaves zero partial writes', async (client) => {
    await isolateScope(client);
    // Seed a root with 50kg.
    const { row: rootRow } = await insertRootTransaction(client, {
      transaction_type: 'collector_sale',
      collector_id: 1,
      aggregator_id: AGG_ID,
      material_type: 'PET',
      gross_weight_kg: 50,
      price_per_kg: 2,
      total_price: 100
    });
    const rootIdBefore = rootRow.id;
    const remainingBefore = Number(rootRow.remaining_kg);
    assertEq(remainingBefore, 50, 'root starts at 50');

    // Count junction rows and pending_transactions referencing this root.
    const junctionCountBefore = (await client.query(
      'SELECT COUNT(*)::int AS c FROM pending_transaction_sources WHERE source_pending_tx_id = $1',
      [rootIdBefore]
    )).rows[0].c;
    const ptCountBefore = (await client.query(
      'SELECT COUNT(*)::int AS c FROM pending_transactions WHERE transaction_type = $1 AND aggregator_id = $2',
      ['aggregator_sale', AGG_ID]
    )).rows[0].c;

    // Attempt to draw 100kg from a 50kg root — must throw.
    let thrown = null;
    try {
      await attributeAndInsert(client, {
        transaction_type: 'aggregator_sale',
        aggregator_id: AGG_ID,
        processor_id: PROC_ID,
        material_type: 'PET',
        gross_weight_kg: 100,
        price_per_kg: 3,
        total_price: 300
      });
    } catch (e) {
      thrown = e;
    }
    assertTruthy(thrown, 'threw');
    assertEq(thrown.name, 'InsufficientSourceError', 'correct error type');
    assertTruthy(thrown instanceof InsufficientSourceError, 'instanceof InsufficientSourceError');
    assertEq(Number(thrown.shortfall_kg), 50, 'shortfall_kg = 50');

    // The critical assertion: source remaining_kg UNCHANGED.
    // (attributeAndInsert throws BEFORE executing any UPDATE/INSERT, so
    // there's nothing to roll back in this path. But if the throw ever
    // moved after the SQL writes, the outer ROLLBACK would still cover it.)
    const rootAfter = await client.query('SELECT remaining_kg FROM pending_transactions WHERE id = $1', [rootIdBefore]);
    assertEq(Number(rootAfter.rows[0].remaining_kg), remainingBefore, 'root remaining_kg unchanged');

    // No new junction rows referencing this source.
    const junctionCountAfter = (await client.query(
      'SELECT COUNT(*)::int AS c FROM pending_transaction_sources WHERE source_pending_tx_id = $1',
      [rootIdBefore]
    )).rows[0].c;
    assertEq(junctionCountAfter, junctionCountBefore, 'no orphan junction rows');

    // No new aggregator_sale rows.
    const ptCountAfter = (await client.query(
      'SELECT COUNT(*)::int AS c FROM pending_transactions WHERE transaction_type = $1 AND aggregator_id = $2',
      ['aggregator_sale', AGG_ID]
    )).rows[0].c;
    assertEq(ptCountAfter, ptCountBefore, 'no half-inserted aggregator_sale row');
  });

  // ── PR4-A: manual source-hint path ──────────────────────────────────────

  // Helper: seed two root rows for the AGG_ID aggregator with the given
  // (material, gross). Returns [rootId1, rootId2].
  async function seedTwoRoots(client, material, kg1, kg2) {
    const { row: r1 } = await insertRootTransaction(client, {
      transaction_type: 'collector_sale',
      collector_id: 1, aggregator_id: AGG_ID,
      material_type: material, gross_weight_kg: kg1,
      price_per_kg: 2, total_price: kg1 * 2
    });
    const { row: r2 } = await insertRootTransaction(client, {
      transaction_type: 'collector_sale',
      collector_id: 1, aggregator_id: AGG_ID,
      material_type: material, gross_weight_kg: kg2,
      price_per_kg: 2, total_price: kg2 * 2
    });
    return [r1.id, r2.id];
  }

  await runTest('db: PR4-A manual pick happy path — two-row hint, sum=target, all valid', async (client) => {
    await isolateScope(client);
    const [a, b] = await seedTwoRoots(client, 'PET', 60, 40);
    const { row, sources } = await attributeAndInsert(client, {
      transaction_type: 'aggregator_sale',
      aggregator_id: AGG_ID, processor_id: PROC_ID,
      material_type: 'PET', gross_weight_kg: 100,
      price_per_kg: 3, total_price: 300,
      sources: [{ source_id: a, kg: 60 }, { source_id: b, kg: 40 }]
    });
    assertEq(Number(row.gross_weight_kg), 100);
    assertEq(sources.length, 2);
    assertEq(Number(sources[0].id), Number(a));
    assertEq(Number(sources[0].weight_kg_attributed), 60);
    assertEq(Number(sources[1].id), Number(b));
    assertEq(Number(sources[1].weight_kg_attributed), 40);

    // Source remaining_kg decremented per hint.
    const after = await client.query(
      'SELECT id, remaining_kg FROM pending_transactions WHERE id = ANY($1::int[]) ORDER BY id',
      [[a, b]]
    );
    const byId = Object.create(null);
    after.rows.forEach(r => { byId[r.id] = Number(r.remaining_kg); });
    assertEq(byId[a], 0);
    assertEq(byId[b], 0);

    // Junction edges exist for both.
    const edges = await client.query(
      'SELECT source_pending_tx_id, weight_kg_attributed FROM pending_transaction_sources WHERE child_pending_tx_id = $1 ORDER BY source_pending_tx_id',
      [row.id]
    );
    assertEq(edges.rows.length, 2);
  });

  await runTest('db: PR4-A invalid source_id (not in caller scope) → invalid_manual_sources + invalid_source_ids', async (client) => {
    await isolateScope(client);
    const [a] = await seedTwoRoots(client, 'PET', 100, 1);
    let thrown = null;
    try {
      await attributeAndInsert(client, {
        transaction_type: 'aggregator_sale',
        aggregator_id: AGG_ID, processor_id: PROC_ID,
        material_type: 'PET', gross_weight_kg: 100,
        price_per_kg: 3, total_price: 300,
        sources: [{ source_id: 999999, kg: 100 }]   // doesn't exist at all
      });
    } catch (e) { thrown = e; }
    assertTruthy(thrown instanceof InsufficientSourceError, 'threw InsufficientSourceError');
    assertEq(thrown.reason, 'invalid_manual_sources');
    assertTruthy(Array.isArray(thrown.invalid_source_ids) && thrown.invalid_source_ids.indexOf(999999) !== -1,
      'invalid_source_ids includes 999999');
  });

  await runTest('db: PR4-A source belongs to different seller → invalid_source_ids', async (client) => {
    await isolateScope(client);
    // Seed a collector_sale under a DIFFERENT aggregator (id=12, seeded by restructure_tiers).
    // Then try to draw it as AGG_ID=9.
    const { row: foreignRoot } = await insertRootTransaction(client, {
      transaction_type: 'collector_sale',
      collector_id: 1, aggregator_id: 12,     // NOT AGG_ID
      material_type: 'PET', gross_weight_kg: 100,
      price_per_kg: 2, total_price: 200
    });
    let thrown = null;
    try {
      await attributeAndInsert(client, {
        transaction_type: 'aggregator_sale',
        aggregator_id: AGG_ID, processor_id: PROC_ID,
        material_type: 'PET', gross_weight_kg: 100,
        price_per_kg: 3, total_price: 300,
        sources: [{ source_id: foreignRoot.id, kg: 100 }]
      });
    } catch (e) { thrown = e; }
    assertTruthy(thrown instanceof InsufficientSourceError, 'threw');
    assertEq(thrown.reason, 'invalid_manual_sources');
    assertTruthy(thrown.invalid_source_ids.indexOf(Number(foreignRoot.id)) !== -1,
      'foreign source_id flagged as invalid');
  });

  await runTest('db: PR4-A source material mismatch → invalid_source_ids', async (client) => {
    await isolateScope(client);
    // Seed an HDPE root under AGG_ID, then try to draw it as PET target.
    const { row: hdpeRoot } = await insertRootTransaction(client, {
      transaction_type: 'collector_sale',
      collector_id: 1, aggregator_id: AGG_ID,
      material_type: 'HDPE', gross_weight_kg: 100,
      price_per_kg: 2, total_price: 200
    });
    let thrown = null;
    try {
      await attributeAndInsert(client, {
        transaction_type: 'aggregator_sale',
        aggregator_id: AGG_ID, processor_id: PROC_ID,
        material_type: 'PET', gross_weight_kg: 100,
        price_per_kg: 3, total_price: 300,
        sources: [{ source_id: hdpeRoot.id, kg: 100 }]
      });
    } catch (e) { thrown = e; }
    assertTruthy(thrown instanceof InsufficientSourceError, 'threw');
    assertEq(thrown.reason, 'invalid_manual_sources');
    assertTruthy(thrown.invalid_source_ids.indexOf(Number(hdpeRoot.id)) !== -1,
      'mismatched-material source flagged as invalid');
  });

  await runTest('db: PR4-A source outside 14-day window → invalid_source_ids', async (client) => {
    await isolateScope(client);
    // Seed a root with created_at 30 days ago (outside 14d window). Use a
    // raw INSERT since insertRootTransaction uses NOW().
    const batch_id = '00000000-0000-0000-0000-000000000042';
    const oldResult = await client.query(
      `INSERT INTO pending_transactions
        (transaction_type, status, collector_id, aggregator_id, material_type,
         gross_weight_kg, net_weight_kg, price_per_kg, total_price,
         batch_id, remaining_kg, created_at)
       VALUES ('collector_sale','pending',1,$1,'PET',100,100,2,200,$2::uuid,100,
               NOW() - INTERVAL '30 days')
       RETURNING id`,
      [AGG_ID, batch_id]
    );
    const oldId = oldResult.rows[0].id;
    let thrown = null;
    try {
      await attributeAndInsert(client, {
        transaction_type: 'aggregator_sale',
        aggregator_id: AGG_ID, processor_id: PROC_ID,
        material_type: 'PET', gross_weight_kg: 100,
        price_per_kg: 3, total_price: 300,
        sources: [{ source_id: oldId, kg: 100 }]
      });
    } catch (e) { thrown = e; }
    assertTruthy(thrown instanceof InsufficientSourceError, 'threw');
    assertEq(thrown.reason, 'invalid_manual_sources');
    assertTruthy(thrown.invalid_source_ids.indexOf(Number(oldId)) !== -1,
      'out-of-window source flagged as invalid');
  });

  await runTest('db: PR4-A hint kg > source remaining_kg → insufficient_remaining', async (client) => {
    await isolateScope(client);
    const [a] = await seedTwoRoots(client, 'PET', 30, 1);
    let thrown = null;
    try {
      await attributeAndInsert(client, {
        transaction_type: 'aggregator_sale',
        aggregator_id: AGG_ID, processor_id: PROC_ID,
        material_type: 'PET', gross_weight_kg: 100,
        price_per_kg: 3, total_price: 300,
        sources: [{ source_id: a, kg: 100 }]   // asks for 100, only 30 available
      });
    } catch (e) { thrown = e; }
    assertTruthy(thrown instanceof InsufficientSourceError, 'threw');
    assertEq(thrown.reason, 'invalid_manual_sources');
    assertTruthy(Array.isArray(thrown.insufficient_remaining) && thrown.insufficient_remaining.length === 1,
      'insufficient_remaining populated');
    assertEq(Number(thrown.insufficient_remaining[0].id), Number(a));
    assertEq(Number(thrown.insufficient_remaining[0].remaining_kg), 30);
    assertEq(Number(thrown.insufficient_remaining[0].requested_kg), 100);
  });

  await runTest('db: PR4-A sum(hint.kg) !== target.gross_weight_kg → sum_mismatch_kg', async (client) => {
    await isolateScope(client);
    const [a, b] = await seedTwoRoots(client, 'PET', 60, 40);
    let thrown = null;
    try {
      await attributeAndInsert(client, {
        transaction_type: 'aggregator_sale',
        aggregator_id: AGG_ID, processor_id: PROC_ID,
        material_type: 'PET', gross_weight_kg: 100,
        price_per_kg: 3, total_price: 300,
        sources: [{ source_id: a, kg: 60 }, { source_id: b, kg: 30 }]  // sum=90, target=100
      });
    } catch (e) { thrown = e; }
    assertTruthy(thrown instanceof InsufficientSourceError, 'threw');
    assertEq(thrown.reason, 'invalid_manual_sources');
    assertEq(Number(thrown.sum_mismatch_kg), -10, 'delta = hint 90 - target 100 = -10');
    assertEq(Number(thrown.hint_total_kg), 90);
    assertEq(Number(thrown.target_kg), 100);
  });

  await runTest('db: PR4-A empty sources array falls through to FIFO', async (client) => {
    await isolateScope(client);
    const [a, b] = await seedTwoRoots(client, 'PET', 60, 40);
    const { row, sources } = await attributeAndInsert(client, {
      transaction_type: 'aggregator_sale',
      aggregator_id: AGG_ID, processor_id: PROC_ID,
      material_type: 'PET', gross_weight_kg: 100,
      price_per_kg: 3, total_price: 300,
      sources: []   // empty → FIFO fall-through
    });
    assertEq(Number(row.gross_weight_kg), 100);
    assertEq(sources.length, 2);
    // FIFO order: oldest created_at first. Both seeded at NOW() (effectively
    // same timestamp) so id ASC tie-break — a (smaller id) drawn first.
    assertEq(Number(sources[0].id), Number(a));
    assertEq(Number(sources[1].id), Number(b));
    assertEq(Number(sources[0].weight_kg_attributed), 60);
    assertEq(Number(sources[1].weight_kg_attributed), 40);
  });

  console.log('\n' + passed + ' passed, ' + failed + ' failed');
  await pool.end();
  process.exit(failed > 0 ? 1 : 0);
}

function assertEq(actual, expected, msg) {
  if (actual !== expected) {
    throw new Error((msg || 'assertEq') + ' — expected ' + JSON.stringify(expected) + ', got ' + JSON.stringify(actual));
  }
}
function assertTruthy(val, msg) {
  if (!val) throw new Error((msg || 'assertTruthy') + ' — got ' + JSON.stringify(val));
}

main().catch((e) => {
  console.error('Fatal:', e);
  process.exit(2);
});
