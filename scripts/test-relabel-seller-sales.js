#!/usr/bin/env node
// scripts/test-relabel-seller-sales.js
//
// DB-integration tests for migrations/1777400000000_relabel_mislabeled_seller_sales.js.
// Each test opens a transaction, seeds rows matching specific FK patterns,
// invokes the migration's up(client) directly against the same client (so its
// queries run inside the seeded transaction), asserts counts, then ROLLBACKs.
// No state leakage; safe to re-run.
//
// Companion (pure-function) discriminator coverage lives in
// scripts/test-relabel-mislabeled-collector-sales.js; this file exercises the
// migration end-to-end against a live pg connection so we catch SQL-shape
// regressions (column name typos, NULL handling drift) that a unit test can't.
//
// Invoke: `set -a; source .env; set +a; node scripts/test-relabel-seller-sales.js`

'use strict';

const { Pool } = require('pg');
const migration = require('../migrations/1777400000000_relabel_mislabeled_seller_sales');

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
    try { await client.query('ROLLBACK'); } catch (_) {}
    client.release();
  }
}

function assertEq(actual, expected, msg) {
  if (actual !== expected) throw new Error((msg || 'assertEq') + ' — expected ' + JSON.stringify(expected) + ', got ' + JSON.stringify(actual));
}

// Seeded demo ids (see migrations/1774500000000_restructure_tiers.js).
const AGG_ID  = 9;
const PROC_ID = 1;

// Recyclers aren't seeded by default; insert a transient one inside the txn
// so FK constraints pass. Rolled back with the rest of the test.
async function ensureRecycler(client) {
  // Try existing first to keep the test idempotent across local seed variations.
  const existing = await client.query(`SELECT id FROM recyclers ORDER BY id LIMIT 1`);
  if (existing.rows.length) return existing.rows[0].id;
  const r = await client.query(
    `INSERT INTO recyclers (name, company, email, city, region, country, is_active)
     VALUES ('PR6 Test Recycler', 'PR6 Test Recycler Co', 'pr6-test@circul.demo', 'Tema', 'Greater Accra', 'Ghana', true)
     RETURNING id`
  );
  return r.rows[0].id;
}

// Insert a pending_transactions row matching a specific FK pattern. Bypasses
// chain-of-custody-db helpers — we want raw control over which FK columns are
// set so we can mimic the buggy historical writes the migration cleans up.
async function seedRow(client, opts) {
  const r = await client.query(
    `INSERT INTO pending_transactions
       (transaction_type, status, material_type, gross_weight_kg, net_weight_kg,
        price_per_kg, total_price,
        collector_id, aggregator_id, processor_id, recycler_id, converter_id,
        source)
     VALUES ($1, 'pending', $2, $3, $3, 1, $3, $4, $5, $6, $7, $8, 'discovery')
     RETURNING id, transaction_type`,
    [
      opts.transaction_type,
      opts.material_type || 'PET',
      opts.gross_weight_kg || 50,
      opts.collector_id  || null,
      opts.aggregator_id || null,
      opts.processor_id  || null,
      opts.recycler_id   || null,
      opts.converter_id  || null
    ]
  );
  return r.rows[0];
}

async function getType(client, id) {
  const r = await client.query(`SELECT transaction_type FROM pending_transactions WHERE id = $1`, [id]);
  return r.rows[0] ? r.rows[0].transaction_type : null;
}

async function main() {

  // ── Case 1: 3 rows matching the processor pattern → all relabel to processor_sale.
  await runTest('case 1: processor pattern → relabels 3/3 to processor_sale', async (client) => {
    await client.query('DELETE FROM pending_transactions');  // clean slate inside txn
    const r1 = await seedRow(client, { transaction_type: 'aggregator_sale', processor_id: PROC_ID });
    const r2 = await seedRow(client, { transaction_type: 'aggregator_sale', processor_id: PROC_ID, material_type: 'HDPE' });
    const r3 = await seedRow(client, { transaction_type: 'aggregator_sale', processor_id: PROC_ID, material_type: 'PP', gross_weight_kg: 75 });

    await migration.up(client);

    assertEq(await getType(client, r1.id), 'processor_sale', 'r1 relabeled');
    assertEq(await getType(client, r2.id), 'processor_sale', 'r2 relabeled');
    assertEq(await getType(client, r3.id), 'processor_sale', 'r3 relabeled');
  });

  // ── Case 2: 3 rows matching the recycler pattern → all relabel to recycler_sale.
  await runTest('case 2: recycler pattern → relabels 3/3 to recycler_sale', async (client) => {
    await client.query('DELETE FROM pending_transactions');
    const REC_ID = await ensureRecycler(client);
    const r1 = await seedRow(client, { transaction_type: 'aggregator_sale', recycler_id: REC_ID });
    const r2 = await seedRow(client, { transaction_type: 'aggregator_sale', recycler_id: REC_ID, material_type: 'HDPE' });
    const r3 = await seedRow(client, { transaction_type: 'aggregator_sale', recycler_id: REC_ID, material_type: 'LDPE' });

    await migration.up(client);

    assertEq(await getType(client, r1.id), 'recycler_sale', 'r1 relabeled');
    assertEq(await getType(client, r2.id), 'recycler_sale', 'r2 relabeled');
    assertEq(await getType(client, r3.id), 'recycler_sale', 'r3 relabeled');
  });

  // ── Case 3: legit aggregator_sale rows (aggregator_id set) are NOT touched.
  await runTest('case 3: legit aggregator_sale (aggregator_id+processor_id both set) → NOT touched', async (client) => {
    await client.query('DELETE FROM pending_transactions');
    // Legit aggregator→processor sale has BOTH aggregator_id (seller) and
    // processor_id (buyer) set. Must not be swept up.
    const legit = await seedRow(client, {
      transaction_type: 'aggregator_sale',
      aggregator_id: AGG_ID,
      processor_id: PROC_ID
    });
    await migration.up(client);
    assertEq(await getType(client, legit.id), 'aggregator_sale', 'legit row preserved');
  });

  // ── Case 4: collector_sale, aggregator_purchase, processor_sale rows untouched.
  await runTest('case 4: non-aggregator_sale rows untouched (collector_sale, aggregator_purchase, processor_sale)', async (client) => {
    await client.query('DELETE FROM pending_transactions');
    const collSale = await seedRow(client, { transaction_type: 'collector_sale', collector_id: 1, aggregator_id: AGG_ID });
    const aggPurch = await seedRow(client, { transaction_type: 'aggregator_purchase', collector_id: 1, aggregator_id: AGG_ID });
    const procSale = await seedRow(client, { transaction_type: 'processor_sale', processor_id: PROC_ID, converter_id: 1 });

    await migration.up(client);

    assertEq(await getType(client, collSale.id), 'collector_sale', 'collector_sale untouched');
    assertEq(await getType(client, aggPurch.id), 'aggregator_purchase', 'aggregator_purchase untouched');
    assertEq(await getType(client, procSale.id), 'processor_sale', 'processor_sale untouched');
  });

  // ── Case 5: mixed seed — only the matching shapes change; counts add up.
  await runTest('case 5: mixed seed → exactly the matching rows relabel; legit rows preserved', async (client) => {
    await client.query('DELETE FROM pending_transactions');
    const REC_ID = await ensureRecycler(client);

    const procRow  = await seedRow(client, { transaction_type: 'aggregator_sale', processor_id: PROC_ID });
    const recRow   = await seedRow(client, { transaction_type: 'aggregator_sale', recycler_id: REC_ID });
    const legit    = await seedRow(client, { transaction_type: 'aggregator_sale', aggregator_id: AGG_ID, processor_id: PROC_ID });
    const collSale = await seedRow(client, { transaction_type: 'collector_sale', collector_id: 1, aggregator_id: AGG_ID });

    await migration.up(client);

    assertEq(await getType(client, procRow.id),  'processor_sale',  'proc row relabeled');
    assertEq(await getType(client, recRow.id),   'recycler_sale',   'rec row relabeled');
    assertEq(await getType(client, legit.id),    'aggregator_sale', 'legit row preserved');
    assertEq(await getType(client, collSale.id), 'collector_sale',  'collector_sale untouched');

    // Final shape: by transaction_type
    const counts = (await client.query(
      `SELECT transaction_type, COUNT(*)::int AS n FROM pending_transactions GROUP BY transaction_type ORDER BY transaction_type`
    )).rows;
    const byType = {};
    for (const r of counts) byType[r.transaction_type] = r.n;
    assertEq(byType['processor_sale'], 1, 'one processor_sale after relabel');
    assertEq(byType['recycler_sale'],  1, 'one recycler_sale after relabel');
    assertEq(byType['aggregator_sale'], 1, 'one legit aggregator_sale preserved');
    assertEq(byType['collector_sale'], 1, 'one collector_sale unchanged');
  });

  // ── Case 6: ambiguous (seller+buyer both set, processor_id non-null) is NOT
  //   swept by the processor pattern because aggregator_id is also set. The
  //   migration's WHERE has `aggregator_id IS NULL`. Confirm.
  await runTest('case 6: ambiguous (processor_id+aggregator_id both set) → NOT touched', async (client) => {
    await client.query('DELETE FROM pending_transactions');
    const ambig = await seedRow(client, {
      transaction_type: 'aggregator_sale',
      aggregator_id: AGG_ID,
      processor_id: PROC_ID
    });
    await migration.up(client);
    assertEq(await getType(client, ambig.id), 'aggregator_sale', 'ambiguous row preserved (out of PR6 scope)');
  });

  // ── Case 7: empty input → no-op, no errors.
  await runTest('case 7: empty pending_transactions → no-op', async (client) => {
    await client.query('DELETE FROM pending_transactions');
    await migration.up(client);
    const c = (await client.query(`SELECT COUNT(*)::int AS n FROM pending_transactions`)).rows[0].n;
    assertEq(c, 0, 'still empty');
  });

  console.log('\n' + passed + ' passed, ' + failed + ' failed');
  await pool.end();
  process.exit(failed > 0 ? 1 : 0);
}

main().catch((e) => { console.error('Fatal:', e); process.exit(2); });
