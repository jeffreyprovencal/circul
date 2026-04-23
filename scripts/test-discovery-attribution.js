#!/usr/bin/env node
// scripts/test-discovery-attribution.js
//
// DB-integration tests for PR6-c — the wiring of insertRootTransaction +
// attributeAndInsert into the 3 discovery offer-accept sites at
// server.js:1494, server.js:3756, server.js:4206. Each test opens a
// transaction, seeds listings + offers, replicates the handler's accept
// logic (target construction + ROOT_TYPES branch + helper call), asserts
// the resulting pending_transactions / pending_transaction_sources state,
// then ROLLBACKs.
//
// Why replicate vs HTTP-mount: the discovery accept handlers grab
// pool.connect() inside the route, so even with an in-process express app
// the seed transaction wouldn't be visible to the helper queries (separate
// connections). Replicating the target shape + branching is the same logic
// the handler runs and asserts the helper integration directly.
//
// Invoke: `set -a; source .env; set +a; node scripts/test-discovery-attribution.js`

'use strict';

const { Pool } = require('pg');
const { txnTypeForRoles } = require('../shared/transaction-parties');
const {
  attributeAndInsert,
  insertRootTransaction,
  InsufficientSourceError
} = require('../shared/chain-of-custody-db');
const { ROOT_TYPES } = require('../shared/chain-of-custody');

const pool = new Pool({ connectionString: process.env.DATABASE_URL });

let passed = 0, failed = 0;

async function runTest(name, fn) {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await client.query('DELETE FROM pending_transactions');
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
function assertTruthy(val, msg) { if (!val) throw new Error((msg || 'assertTruthy') + ' — got ' + JSON.stringify(val)); }

// Mirror of ptColForRole in server.js. If that helper ever moves, mirror here.
function ptColForRole(role) {
  switch (role) {
    case 'collector':  return 'collector_id';
    case 'aggregator': return 'aggregator_id';
    case 'processor':  return 'processor_id';
    case 'recycler':   return 'recycler_id';
    case 'converter':  return 'converter_id';
    default: return null;
  }
}

// Replica of the handler's accept-write block (the only thing PR6-c changes).
// Throws InsufficientSourceError on shortfall, just like the handler.
async function discoveryAcceptWrite(client, listing, offer) {
  const txnType = txnTypeForRoles(listing.seller_role, offer.buyer_role);
  const offerQty = parseFloat(offer.quantity_kg);
  const totalPrice = parseFloat((offerQty * parseFloat(offer.price_per_kg)).toFixed(2));
  const sellerCol = ptColForRole(listing.seller_role);
  const buyerCol  = ptColForRole(offer.buyer_role);
  const target = {
    transaction_type: txnType,
    status: 'pending',
    material_type: listing.material_type,
    gross_weight_kg: offerQty,
    price_per_kg: parseFloat(offer.price_per_kg),
    total_price: totalPrice,
    source: 'discovery'
  };
  if (sellerCol) target[sellerCol] = listing.seller_id;
  if (buyerCol && buyerCol !== sellerCol) target[buyerCol] = offer.buyer_id;

  if (ROOT_TYPES[txnType]) {
    return (await insertRootTransaction(client, target)).row;
  } else {
    return (await attributeAndInsert(client, target)).row;
  }
}

// Seeded ids from migrations/1774500000000_restructure_tiers.js.
const COLLECTOR_ID = 1;
const AGG_ID  = 9;
const PROC_ID = 1;

async function main() {

  // ── Case (a): WEB offer-accept — aggregator sells to processor.
  //   listing.seller_role = 'aggregator', offer.buyer_role = 'processor'
  //   → txnType = 'aggregator_sale' (downstream) → attributeAndInsert.
  //   Aggregator needs available source rows (collector drops). Verify the
  //   resulting row inherits batch_id from the dominant source and writes a
  //   junction edge.
  await runTest('case (a): web aggregator→processor accept → aggregator_sale, attributed, junction edge written', async (client) => {
    // Seed a 60kg collector drop into AGG_ID (the aggregator's source row).
    const { row: rootRow } = await insertRootTransaction(client, {
      transaction_type: 'collector_sale',
      collector_id: COLLECTOR_ID, aggregator_id: AGG_ID,
      material_type: 'PET', gross_weight_kg: 60, price_per_kg: 2, total_price: 120
    });
    assertTruthy(rootRow.batch_id, 'root has batch_id');

    // Simulate listing + offer shapes the handler hands to discoveryAcceptWrite.
    const listing = { seller_role: 'aggregator', seller_id: AGG_ID, material_type: 'PET' };
    const offer   = { buyer_role: 'processor', buyer_id: PROC_ID, quantity_kg: 60, price_per_kg: 3 };

    const writtenRow = await discoveryAcceptWrite(client, listing, offer);

    assertEq(writtenRow.transaction_type, 'aggregator_sale', 'txn type is aggregator_sale');
    assertEq(writtenRow.source, 'discovery', 'source = discovery');
    assertEq(Number(writtenRow.aggregator_id), AGG_ID, 'aggregator_id = seller');
    assertEq(Number(writtenRow.processor_id), PROC_ID, 'processor_id = buyer');
    assertEq(writtenRow.batch_id, rootRow.batch_id, 'inherits batch_id from dominant source');
    assertEq(Number(writtenRow.gross_weight_kg), 60, 'weight matches offer qty');

    // Junction row exists and decremented the source.
    const edge = await client.query(
      `SELECT source_pending_tx_id, weight_kg_attributed FROM pending_transaction_sources WHERE child_pending_tx_id = $1`,
      [writtenRow.id]
    );
    assertEq(edge.rows.length, 1, 'one junction edge');
    assertEq(Number(edge.rows[0].source_pending_tx_id), Number(rootRow.id), 'edge points at root');
    assertEq(Number(edge.rows[0].weight_kg_attributed), 60, 'edge weight = 60');

    const rootAfter = await client.query(`SELECT remaining_kg FROM pending_transactions WHERE id = $1`, [rootRow.id]);
    assertEq(Number(rootAfter.rows[0].remaining_kg), 0, 'root drained to 0');
  });

  // ── Case (b): shortfall — seller has zero available sources → 400 path.
  //   The handler maps InsufficientSourceError to a 400 via handleInsufficientSource.
  //   Here we assert the helper itself throws the right error type so the
  //   handler has something to catch.
  await runTest('case (b): shortfall — aggregator has no sources → InsufficientSourceError thrown', async (client) => {
    // No collector_sale seeded — aggregator has zero available material.
    const listing = { seller_role: 'aggregator', seller_id: AGG_ID, material_type: 'PET' };
    const offer   = { buyer_role: 'processor', buyer_id: PROC_ID, quantity_kg: 100, price_per_kg: 3 };

    let thrown = null;
    try {
      await discoveryAcceptWrite(client, listing, offer);
    } catch (e) {
      thrown = e;
    }
    assertTruthy(thrown, 'expected throw');
    assertTruthy(thrown instanceof InsufficientSourceError, 'is InsufficientSourceError');
    assertEq(thrown.reason || 'shortfall', 'shortfall', 'reason is shortfall (default)');
    assertEq(Number(thrown.shortfall_kg), 100, 'shortfall = 100kg requested');
  });

  // ── Case (c): ROOT — collector posts listing, aggregator offers, collector accepts.
  //   listing.seller_role = 'collector', offer.buyer_role = 'aggregator'
  //   → txnType = 'collector_sale' (root) → insertRootTransaction.
  //   No junction edge; fresh batch_id; remaining_kg = gross_weight_kg.
  await runTest('case (c): collector→aggregator accept (root) → collector_sale, fresh batch_id, no junction edge', async (client) => {
    const listing = { seller_role: 'collector', seller_id: COLLECTOR_ID, material_type: 'PET' };
    const offer   = { buyer_role: 'aggregator', buyer_id: AGG_ID, quantity_kg: 25, price_per_kg: 3 };

    const writtenRow = await discoveryAcceptWrite(client, listing, offer);

    assertEq(writtenRow.transaction_type, 'collector_sale', 'txn type is collector_sale');
    assertEq(writtenRow.source, 'discovery', 'source = discovery');
    assertTruthy(writtenRow.batch_id, 'fresh batch_id assigned');
    assertEq(Number(writtenRow.collector_id), COLLECTOR_ID, 'seller is the collector');
    assertEq(Number(writtenRow.aggregator_id), AGG_ID, 'buyer is the aggregator');
    assertEq(Number(writtenRow.remaining_kg), 25, 'remaining_kg = gross_weight_kg');

    const edges = await client.query(
      `SELECT * FROM pending_transaction_sources WHERE child_pending_tx_id = $1`,
      [writtenRow.id]
    );
    assertEq(edges.rows.length, 0, 'roots have no junction edges');
  });

  // ── Bonus: processor→converter accept (the broken path PR6 closes end-to-end)
  //   Pre-PR6 this would have been mis-labeled aggregator_sale by the buggy
  //   helper. Post-PR6 it's correctly processor_sale, attributed against the
  //   processor's available source.
  await runTest('case (d) bonus: processor→converter accept → processor_sale (was the broken-helper path pre-PR6)', async (client) => {
    // Seed a chain: collector → aggregator (root) → aggregator_sale to processor.
    // The aggregator_sale row becomes the processor's available source.
    await insertRootTransaction(client, {
      transaction_type: 'collector_sale',
      collector_id: COLLECTOR_ID, aggregator_id: AGG_ID,
      material_type: 'PET', gross_weight_kg: 50, price_per_kg: 2, total_price: 100
    });
    await attributeAndInsert(client, {
      transaction_type: 'aggregator_sale',
      aggregator_id: AGG_ID, processor_id: PROC_ID,
      material_type: 'PET', gross_weight_kg: 50, price_per_kg: 3, total_price: 150
    });

    // Insert a converter row to satisfy FK on the processor_sale.
    const cvId = (await client.query(
      `INSERT INTO converters (name, company, email, city, region, country, is_active)
       VALUES ('PR6 Test Converter', 'PR6 Test Converter Co', 'pr6-conv@circul.demo', 'Tema', 'Greater Accra', 'Ghana', true)
       RETURNING id`
    )).rows[0].id;

    const listing = { seller_role: 'processor', seller_id: PROC_ID, material_type: 'PET' };
    const offer   = { buyer_role: 'converter', buyer_id: cvId, quantity_kg: 50, price_per_kg: 4 };

    const writtenRow = await discoveryAcceptWrite(client, listing, offer);

    assertEq(writtenRow.transaction_type, 'processor_sale', 'PR6-a fix: correctly labeled processor_sale (not aggregator_sale)');
    assertEq(Number(writtenRow.processor_id), PROC_ID, 'seller is processor');
    assertEq(Number(writtenRow.converter_id), cvId, 'buyer is converter');
    assertTruthy(writtenRow.batch_id, 'inherits batch_id from upstream chain');

    const edge = await client.query(
      `SELECT source_pending_tx_id, weight_kg_attributed FROM pending_transaction_sources WHERE child_pending_tx_id = $1`,
      [writtenRow.id]
    );
    assertEq(edge.rows.length, 1, 'one junction edge to the aggregator_sale');
    assertEq(Number(edge.rows[0].weight_kg_attributed), 50, 'edge weight = 50');
  });

  console.log('\n' + passed + ' passed, ' + failed + ' failed');
  await pool.end();
  process.exit(failed > 0 ? 1 : 0);
}

main().catch((e) => { console.error('Fatal:', e); process.exit(2); });
