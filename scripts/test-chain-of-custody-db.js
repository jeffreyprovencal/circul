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

async function main() {
  // ── Case 1: happy path — root + downstream, attribution works end-to-end.
  await runTest('db: happy path — attributeAndInsert writes row + edge + decrements source', async (client) => {
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
