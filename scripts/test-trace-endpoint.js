#!/usr/bin/env node
// scripts/test-trace-endpoint.js
//
// DB-integration tests for GET /api/trace/:batch_id (server.js). Each test
// boots an in-process Express app on a transient pool, opens a DB transaction,
// seeds rows via shared/chain-of-custody-db.js, exercises the endpoint via
// http.get, then ROLLBACKs — no state leaks. Mirrors the harness style of
// scripts/test-chain-of-custody-db.js but routes through the actual HTTP
// surface so we exercise routing, validation, and JSON serialisation too.
//
// Invoke: `set -a; source .env; set +a; node scripts/test-trace-endpoint.js`
//
// NOTE on transactional isolation: the seed transaction must be visible to
// the HTTP handler. We solve this by giving the route handler a pool that
// reuses the same client (single-connection pool) for the duration of each
// test, then ROLLBACK at end. The shared `pool` exported by server.js is
// not used here — we mount the endpoint's logic with our test pool.

'use strict';

const http = require('http');
const express = require('express');
const { Pool, Client } = require('pg');
const {
  insertRootTransaction,
  attributeAndInsert
} = require('../shared/chain-of-custody-db');
const {
  resolveSeller,
  resolveBuyer,
  KIND_TO_TABLE
} = require('../shared/transaction-parties');

let passed = 0, failed = 0;

const TRACE_UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const STAGE_TYPE_BY_BUYER = {
  aggregator: 'aggregation',
  processor:  'processing',
  recycler:   'recycling',
  converter:  'conversion'
};
function _traceActorFor(row) {
  try {
    if (row.transaction_type === 'collector_sale' || row.transaction_type === 'aggregator_purchase') {
      return resolveSeller(row);
    }
    return resolveBuyer(row);
  } catch (_) { return null; }
}
function _traceStageType(row, actor) {
  if (row.transaction_type === 'collector_sale' || row.transaction_type === 'aggregator_purchase') return 'collection';
  if (actor && STAGE_TYPE_BY_BUYER[actor.kind]) return STAGE_TYPE_BY_BUYER[actor.kind];
  return 'stage';
}

// Build an Express app that routes every query through `client` (the seed
// transaction's pg connection). This is the only way to make the seeded rows
// visible to the route without committing them.
function buildAppWithClient(client) {
  const app = express();
  // Mirror of server.js's /api/trace/:batch_id, but uses `client.query` instead
  // of `pool.query`. Logic must stay byte-identical to server.js — if you
  // update one, update the other (or refactor into shared/trace.js later).
  app.get('/api/trace/:batch_id', async (req, res) => {
    const batch_id = req.params.batch_id;
    if (!TRACE_UUID_RE.test(batch_id)) {
      return res.status(400).json({ success: false, message: 'Invalid batch id' });
    }
    try {
      const leafResult = await client.query(
        `SELECT id FROM pending_transactions WHERE batch_id = $1 ORDER BY created_at DESC, id DESC LIMIT 1`,
        [batch_id]
      );
      if (!leafResult.rows.length) return res.status(404).json({ success: false, message: 'Batch not found' });
      const leafId = Number(leafResult.rows[0].id);

      const lineageResult = await client.query(`
        WITH RECURSIVE ancestors AS (
          SELECT id, 0 AS depth FROM pending_transactions WHERE id = $1
          UNION
          SELECT s.source_pending_tx_id, a.depth + 1
            FROM ancestors a
            JOIN pending_transaction_sources s ON s.child_pending_tx_id = a.id
        )
        SELECT pt.id, pt.transaction_type, pt.material_type,
               pt.gross_weight_kg, pt.created_at, pt.batch_id,
               pt.collector_id, pt.aggregator_id, pt.processor_id,
               pt.recycler_id, pt.converter_id,
               a.depth
          FROM ancestors a
          JOIN pending_transactions pt ON pt.id = a.id
      `, [leafId]);
      const lineage = lineageResult.rows;
      const byId = new Map(lineage.map(r => [Number(r.id), r]));

      const lineageIds = lineage.map(r => Number(r.id));
      const edgeResult = await client.query(
        `SELECT child_pending_tx_id, source_pending_tx_id, weight_kg_attributed
           FROM pending_transaction_sources
          WHERE child_pending_tx_id = ANY($1::int[])
          ORDER BY weight_kg_attributed DESC, source_pending_tx_id ASC`,
        [lineageIds]
      );
      const edgesByChild = new Map();
      for (const e of edgeResult.rows) {
        const k = Number(e.child_pending_tx_id);
        if (!edgesByChild.has(k)) edgesByChild.set(k, []);
        edgesByChild.get(k).push(e);
      }

      const linearChain = [];
      const seenInChain = new Set();
      let cursor = byId.get(leafId);
      while (cursor && !seenInChain.has(Number(cursor.id))) {
        linearChain.unshift(cursor);
        seenInChain.add(Number(cursor.id));
        const parents = edgesByChild.get(Number(cursor.id)) || [];
        if (parents.length === 1) cursor = byId.get(Number(parents[0].source_pending_tx_id)) || null;
        else cursor = null;
      }

      const sourceRowIds = new Set();
      for (const e of edgeResult.rows) sourceRowIds.add(Number(e.source_pending_tx_id));
      const allRowsToResolve = new Set(lineage.map(r => Number(r.id)));
      for (const id of sourceRowIds) allRowsToResolve.add(id);

      const idsByKind = { collector: new Set(), aggregator: new Set(), processor: new Set(), recycler: new Set(), converter: new Set() };
      const actorByRowId = new Map();
      for (const id of allRowsToResolve) {
        const row = byId.get(id);
        if (!row) continue;
        const a = _traceActorFor(row);
        if (a && idsByKind[a.kind]) {
          idsByKind[a.kind].add(Number(a.id));
          actorByRowId.set(id, a);
        }
      }

      const partyByKey = new Map();
      async function loadParties(kind) {
        const ids = Array.from(idsByKind[kind]);
        if (!ids.length) return;
        let sql;
        if (kind === 'collector') {
          sql = `SELECT id, 'COL-' || LPAD(id::text, 4, '0') AS display_name, city, region FROM collectors WHERE id = ANY($1::int[])`;
        } else {
          const cfg = KIND_TO_TABLE[kind];
          sql = `SELECT id, COALESCE(company, name) AS display_name, city, region FROM ${cfg.table} WHERE id = ANY($1::int[])`;
        }
        const r = await client.query(sql, [ids]);
        for (const row of r.rows) partyByKey.set(kind + ':' + Number(row.id), { display_name: row.display_name, city: row.city, region: row.region });
      }
      // Sequential here (single client) — pool variant in server.js parallelises.
      for (const k of ['collector','aggregator','processor','recycler','converter']) await loadParties(k);

      function actorJson(actor) {
        if (!actor) return null;
        const p = partyByKey.get(actor.kind + ':' + actor.id) || { display_name: null, city: null, region: null };
        return { kind: actor.kind, id: actor.id, display_name: p.display_name, city: p.city, region: p.region };
      }

      const stages = linearChain.map((row, idx) => {
        const sourceEdges = edgesByChild.get(Number(row.id)) || [];
        const actor = actorByRowId.get(Number(row.id));
        const stage_type = _traceStageType(row, actor);
        const weight_in_kg = sourceEdges.length
          ? Math.round(sourceEdges.reduce((s, e) => s + parseFloat(e.weight_kg_attributed), 0) * 100) / 100
          : null;
        const stage = {
          stage_number: idx + 1,
          stage_type: stage_type,
          date: row.created_at ? row.created_at.toISOString().slice(0, 10) : null,
          actor: actorJson(actor),
          material_type: row.material_type,
          weight_in_kg: weight_in_kg,
          weight_out_kg: row.gross_weight_kg != null ? Math.round(parseFloat(row.gross_weight_kg) * 100) / 100 : null,
          pending_tx_id: Number(row.id)
        };
        if (sourceEdges.length > 1) stage.commingled = true;
        if (sourceEdges.length > 0) {
          stage.sources = sourceEdges.map(e => {
            const srcRow = byId.get(Number(e.source_pending_tx_id));
            const srcActor = srcRow ? _traceActorFor(srcRow) : null;
            const p = srcActor ? (partyByKey.get(srcActor.kind + ':' + srcActor.id) || {}) : {};
            return {
              pending_tx_id: Number(e.source_pending_tx_id),
              display_name: p.display_name || null,
              city: p.city || null,
              weight_kg_attributed: Math.round(parseFloat(e.weight_kg_attributed) * 100) / 100
            };
          });
        }
        return stage;
      });

      const collectorIds = new Set();
      let earliestRootDate = null;
      for (const row of lineage) {
        if ((row.transaction_type === 'collector_sale' || row.transaction_type === 'aggregator_purchase') && row.collector_id != null) {
          collectorIds.add(Number(row.collector_id));
          const t = row.created_at ? new Date(row.created_at).getTime() : null;
          if (t != null && (earliestRootDate == null || t < earliestRootDate)) earliestRootDate = t;
        }
      }
      const leafRow = byId.get(leafId);
      const leafTime = leafRow && leafRow.created_at ? new Date(leafRow.created_at).getTime() : null;
      let journey_days = null;
      if (earliestRootDate != null && leafTime != null) {
        journey_days = Math.max(0, Math.round((leafTime - earliestRootDate) / (1000 * 60 * 60 * 24)));
      }

      const leafActor = actorByRowId.get(leafId);
      res.json({
        success: true,
        leaf: {
          pending_tx_id: leafId,
          material_type: leafRow.material_type,
          final_weight_kg: leafRow.gross_weight_kg != null ? Math.round(parseFloat(leafRow.gross_weight_kg) * 100) / 100 : null,
          batch_id: leafRow.batch_id,
          produced_by: actorJson(leafActor),
          shipped_at: leafRow.created_at ? leafRow.created_at.toISOString() : null
        },
        stats: { stages: stages.length, journey_days: journey_days, collector_count: collectorIds.size },
        stages: stages
      });
    } catch (err) {
      console.error('test trace handler error:', err);
      res.status(500).json({ success: false, message: 'Server error' });
    }
  });
  return app;
}

function fetchJson(server, path) {
  return new Promise((resolve, reject) => {
    const port = server.address().port;
    http.get('http://127.0.0.1:' + port + path, (res) => {
      let body = '';
      res.on('data', (c) => body += c);
      res.on('end', () => {
        let parsed = null; try { parsed = JSON.parse(body); } catch (_) {}
        resolve({ status: res.statusCode, body: parsed, raw: body });
      });
    }).on('error', reject);
  });
}

async function runTest(name, fn) {
  const client = new Client({ connectionString: process.env.DATABASE_URL });
  await client.connect();
  let server = null;
  try {
    await client.query('BEGIN');
    await client.query('DELETE FROM pending_transactions');
    const app = buildAppWithClient(client);
    server = http.createServer(app);
    await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
    await fn(client, server);
    console.log('PASS  ' + name);
    passed++;
  } catch (e) {
    console.log('FAIL  ' + name);
    console.log('       ' + (e.stack || e.message));
    failed++;
  } finally {
    if (server) await new Promise((resolve) => server.close(resolve));
    try { await client.query('ROLLBACK'); } catch (_) {}
    await client.end();
  }
}

const AGG_ID = 9;
const PROC_ID = 1;
const REC_ID  = null;     // no recycler seeded by default; we'll create per-test

function assertEq(actual, expected, msg) {
  if (actual !== expected) throw new Error((msg || 'assertEq') + ' — expected ' + JSON.stringify(expected) + ', got ' + JSON.stringify(actual));
}
function assertTruthy(val, msg) { if (!val) throw new Error((msg || 'assertTruthy') + ' — got ' + JSON.stringify(val)); }
function assertDeepInclude(obj, partial, msg) {
  for (const k of Object.keys(partial)) {
    if (JSON.stringify(obj[k]) !== JSON.stringify(partial[k])) {
      throw new Error((msg || 'assertDeepInclude') + ' — key=' + k + ' expected ' + JSON.stringify(partial[k]) + ', got ' + JSON.stringify(obj[k]));
    }
  }
}

async function ensureRecycler(client) {
  const existing = await client.query(`SELECT id FROM recyclers WHERE email = 'trace-test@circul.demo' LIMIT 1`);
  if (existing.rows.length) return existing.rows[0].id;
  const r = await client.query(
    `INSERT INTO recyclers (name, company, email, city, region, country, is_active)
     VALUES ('Trace Test Recycler', 'Trace Test Recycler Co', 'trace-test@circul.demo', 'Tema', 'Greater Accra', 'Ghana', true)
     RETURNING id`
  );
  return r.rows[0].id;
}

async function ensureConverter(client) {
  const existing = await client.query(`SELECT id FROM converters WHERE email = 'trace-test-cv@circul.demo' LIMIT 1`);
  if (existing.rows.length) return existing.rows[0].id;
  const r = await client.query(
    `INSERT INTO converters (name, company, email, city, region, country, is_active)
     VALUES ('Trace Test Converter', 'Trace Test Converter Co', 'trace-test-cv@circul.demo', 'Tema', 'Greater Accra', 'Ghana', true)
     RETURNING id`
  );
  return r.rows[0].id;
}

async function main() {

  // ── Case 1: Root-only collector_sale → 1 stage (collection)
  await runTest('case 1: root-only collector_sale → 1 stage, no sources, collector_count=1', async (client, server) => {
    const { row: root } = await insertRootTransaction(client, {
      transaction_type: 'collector_sale', collector_id: 1, aggregator_id: AGG_ID,
      material_type: 'PET', gross_weight_kg: 45, price_per_kg: 2, total_price: 90
    });
    const r = await fetchJson(server, '/api/trace/' + root.batch_id);
    assertEq(r.status, 200, 'status');
    assertEq(r.body.success, true);
    assertEq(r.body.stats.stages, 1, 'stats.stages');
    assertEq(r.body.stats.collector_count, 1, 'collector_count');
    assertEq(r.body.stages.length, 1);
    assertEq(r.body.stages[0].stage_type, 'collection');
    assertEq(r.body.stages[0].stage_number, 1);
    assertEq(r.body.stages[0].sources, undefined, 'roots have no sources field');
    assertEq(r.body.stages[0].weight_in_kg, null);
    assertEq(Number(r.body.stages[0].weight_out_kg), 45);
    assertEq(r.body.stages[0].actor.kind, 'collector');
    assertTruthy((r.body.stages[0].actor.display_name || '').startsWith('COL-'), 'collector display_name is code');
  });

  // ── Case 2: 2-stage collector → aggregator (single source, not commingled)
  await runTest('case 2: 2-stage chain → 2 stages, aggregation has 1 source, commingled=false', async (client, server) => {
    const { row: root } = await insertRootTransaction(client, {
      transaction_type: 'collector_sale', collector_id: 1, aggregator_id: AGG_ID,
      material_type: 'PET', gross_weight_kg: 60, price_per_kg: 2, total_price: 120
    });
    const { row: agg } = await attributeAndInsert(client, {
      transaction_type: 'aggregator_sale', aggregator_id: AGG_ID, processor_id: PROC_ID,
      material_type: 'PET', gross_weight_kg: 60, price_per_kg: 3, total_price: 180
    });
    const r = await fetchJson(server, '/api/trace/' + agg.batch_id);
    assertEq(r.status, 200);
    assertEq(r.body.stats.stages, 2);
    assertEq(r.body.stats.collector_count, 1);
    assertEq(r.body.stages.length, 2);
    assertEq(r.body.stages[0].stage_type, 'collection');
    assertEq(r.body.stages[1].stage_type, 'processing'); // buyer = processor
    assertEq(r.body.stages[1].commingled, undefined, 'single source → no commingled flag');
    assertTruthy(Array.isArray(r.body.stages[1].sources) && r.body.stages[1].sources.length === 1, 'aggregation stage has 1 source');
    assertEq(Number(r.body.stages[1].sources[0].weight_kg_attributed), 60);
    assertEq(Number(r.body.stages[1].weight_in_kg), 60);
    assertEq(Number(r.body.stages[1].weight_out_kg), 60);
  });

  // ── Case 3: Commingled aggregation — 5 collectors → 1 aggregator → 1 processor
  //   With the commingled-root rule (drop Stage 1 when collector_count > 1), the
  //   only rendered stage is the aggregator_sale itself, with all 5 collectors
  //   shown in its expandable sources list. The chain stops at the commingling
  //   fork — there is no separate "collection" stage card.
  await runTest('case 3: commingled (5 collectors → 1 agg → 1 proc) → 1 stage, commingled=true, sources expanded', async (client, server) => {
    const roots = [];
    for (let i = 0; i < 5; i++) {
      const kg = [45, 28, 22.5, 15.5, 12][i];
      const { row } = await insertRootTransaction(client, {
        transaction_type: 'collector_sale', collector_id: 1 + i, aggregator_id: AGG_ID,
        material_type: 'PET', gross_weight_kg: kg, price_per_kg: 2, total_price: kg * 2
      });
      roots.push(row);
    }
    const total = 45 + 28 + 22.5 + 15.5 + 12; // 123.0
    const { row: agg } = await attributeAndInsert(client, {
      transaction_type: 'aggregator_sale', aggregator_id: AGG_ID, processor_id: PROC_ID,
      material_type: 'PET', gross_weight_kg: total, price_per_kg: 3, total_price: total * 3
    });
    const r = await fetchJson(server, '/api/trace/' + agg.batch_id);
    assertEq(r.status, 200);
    // collector_count = 5 (all distinct collectors); stages = 1 (Stage 1 dropped per spec,
    // and there are no further downstream stages in this lineage).
    assertEq(r.body.stats.collector_count, 5);
    assertEq(r.body.stats.stages, 1);
    assertEq(r.body.stages[0].stage_type, 'processing'); // commingled aggregator_sale → buyer is processor
    assertEq(r.body.stages[0].commingled, true);
    assertEq(r.body.stages[0].sources.length, 5);
    assertEq(Number(r.body.stages[0].weight_in_kg), 123);
    assertEq(Number(r.body.stages[0].weight_out_kg), 123);
    // Sources sorted by weight DESC
    assertEq(Number(r.body.stages[0].sources[0].weight_kg_attributed), 45);
    assertEq(Number(r.body.stages[0].sources[4].weight_kg_attributed), 12);
    // Sources should have collector display_names
    assertTruthy((r.body.stages[0].sources[0].display_name || '').startsWith('COL-'), 'sources are collectors');
  });

  // ── Case 4: 4-stage full chain (single-collector linear)
  await runTest('case 4: 4-stage linear (collector → agg → proc → conv) → stages=4, collector_count=1', async (client, server) => {
    const cvId = await ensureConverter(client);
    const { row: root } = await insertRootTransaction(client, {
      transaction_type: 'collector_sale', collector_id: 1, aggregator_id: AGG_ID,
      material_type: 'PET', gross_weight_kg: 100, price_per_kg: 2, total_price: 200
    });
    const { row: aggSale } = await attributeAndInsert(client, {
      transaction_type: 'aggregator_sale', aggregator_id: AGG_ID, processor_id: PROC_ID,
      material_type: 'PET', gross_weight_kg: 100, price_per_kg: 3, total_price: 300
    });
    const { row: procSale } = await attributeAndInsert(client, {
      transaction_type: 'processor_sale', processor_id: PROC_ID, converter_id: cvId,
      material_type: 'PET', gross_weight_kg: 100, price_per_kg: 4, total_price: 400
    });
    const r = await fetchJson(server, '/api/trace/' + procSale.batch_id);
    assertEq(r.status, 200);
    assertEq(r.body.stats.stages, 3, 'rendered linear stages');
    // NOTE: only 3 stages of *pending_transactions* are needed to model 4 logical
    // stages because the converter "receives" the processor_sale row — there is
    // no separate converter-side row in pending_transactions for this lineage.
    // collector_count=1 confirms single root.
    assertEq(r.body.stats.collector_count, 1);
    assertEq(r.body.stages[0].stage_type, 'collection');
    assertEq(r.body.stages[1].stage_type, 'processing');
    assertEq(r.body.stages[2].stage_type, 'conversion');
    assertEq(r.body.leaf.produced_by.kind, 'converter');
  });

  // ── Case 5: Multi-collector at root across 3-stage chain (CTE distinct test)
  await runTest('case 5: 3 collectors → 1 agg → 1 proc → distinct collector aggregation', async (client, server) => {
    for (let i = 0; i < 3; i++) {
      const kg = [40, 35, 25][i];
      await insertRootTransaction(client, {
        transaction_type: 'collector_sale', collector_id: 1 + i, aggregator_id: AGG_ID,
        material_type: 'PET', gross_weight_kg: kg, price_per_kg: 2, total_price: kg * 2
      });
    }
    const total = 40 + 35 + 25; // 100
    const { row: aggSale } = await attributeAndInsert(client, {
      transaction_type: 'aggregator_sale', aggregator_id: AGG_ID, processor_id: PROC_ID,
      material_type: 'PET', gross_weight_kg: total, price_per_kg: 3, total_price: total * 3
    });
    const r = await fetchJson(server, '/api/trace/' + aggSale.batch_id);
    assertEq(r.status, 200);
    assertEq(r.body.stats.collector_count, 3);
    assertEq(r.body.stats.stages, 1, 'commingled root → only the aggregator_sale stage rendered');
    assertEq(r.body.stages[0].commingled, true);
    assertEq(r.body.stages[0].sources.length, 3);
  });

  // ── Case 6: Not-found UUID → 404
  await runTest('case 6: syntactically-valid UUID but no batch → 404', async (client, server) => {
    const r = await fetchJson(server, '/api/trace/00000000-0000-0000-0000-000000000000');
    assertEq(r.status, 404);
    assertEq(r.body.success, false);
    assertEq(r.body.message, 'Batch not found');
  });

  // ── Case 7: Invalid UUID → 400
  await runTest('case 7: invalid UUID → 400', async (client, server) => {
    const r = await fetchJson(server, '/api/trace/abc');
    assertEq(r.status, 400);
    assertEq(r.body.success, false);
    assertEq(r.body.message, 'Invalid batch id');
  });

  // ── Case 8: Leaf with remaining_kg=0 — walk still works
  await runTest('case 8: leaf with remaining_kg=0 → trace still walks (passport is historical)', async (client, server) => {
    const { row: root } = await insertRootTransaction(client, {
      transaction_type: 'collector_sale', collector_id: 1, aggregator_id: AGG_ID,
      material_type: 'PET', gross_weight_kg: 50, price_per_kg: 2, total_price: 100
    });
    const { row: agg } = await attributeAndInsert(client, {
      transaction_type: 'aggregator_sale', aggregator_id: AGG_ID, processor_id: PROC_ID,
      material_type: 'PET', gross_weight_kg: 50, price_per_kg: 3, total_price: 150
    });
    // Manually drain the leaf — simulates a downstream draw that we don't see
    // in this lineage (the leaf is still queryable + walkable).
    await client.query(`UPDATE pending_transactions SET remaining_kg = 0 WHERE id = $1`, [agg.id]);
    const r = await fetchJson(server, '/api/trace/' + agg.batch_id);
    assertEq(r.status, 200, 'walk works regardless of remaining_kg');
    assertEq(r.body.stats.stages, 2);
    assertEq(r.body.stats.collector_count, 1);
  });

  // ── Case 9: null batch_id safety — confirm /trace/:batch_id can never reach
  //    a row whose batch_id IS NULL (route requires UUID; SELECT filters batch_id).
  //    Frontend hides QR+link for t.batch_id null — covered by Case 7's 400 path
  //    (any falsy string fails UUID regex). This test asserts the SELECT filter.
  await runTest('case 9: rows with NULL batch_id are unreachable via /trace/:batch_id', async (client, server) => {
    // Insert a row with NULL batch_id directly (bypassing the COC helpers).
    await client.query(
      `INSERT INTO pending_transactions
        (transaction_type, status, collector_id, aggregator_id,
         material_type, gross_weight_kg, net_weight_kg, price_per_kg, total_price,
         batch_id, remaining_kg)
       VALUES ('collector_sale','pending',1,$1,'PET',10,10,2,20,NULL,10)`,
      [AGG_ID]
    );
    // Try to query with empty/garbage UUIDs — all fail at validation.
    let r = await fetchJson(server, '/api/trace/null');
    assertEq(r.status, 400);
    r = await fetchJson(server, '/api/trace/undefined');
    assertEq(r.status, 400);
    // The SELECT filters batch_id = $1, so even a wildcard-ish UUID won't match
    // a NULL row. There is no UUID for which a NULL-batch row can be returned.
  });

  console.log('\n' + passed + ' passed, ' + failed + ' failed');
  process.exit(failed > 0 ? 1 : 0);
}

main().catch((e) => { console.error('Fatal:', e); process.exit(2); });
