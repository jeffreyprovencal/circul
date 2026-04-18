/**
 * Migration: backfill_chain_of_custody
 *
 * Populates batch_id + remaining_kg + pending_transaction_sources for every
 * existing pending_transactions row. PR2 of the chain-of-custody series.
 *
 * Algorithm (pure function): see shared/chain-of-custody.js.
 *   - FIFO attribution by source.created_at ASC, id ASC — mirrors real-world
 *     bale dispatch and ensures deterministic re-runs.
 *   - batch_id is the DOMINANT-source lineage pointer: on commingled rows, it
 *     resolves to the source with the largest weight_kg_attributed (ties
 *     broken by source created_at ASC, id ASC). The pending_transaction_sources
 *     junction table is authoritative for full provenance / mass balance.
 *     PR5's product-journey report MUST walk the junction, not batch_id.
 *   - Window default 14 days. Override via BACKFILL_WINDOW_DAYS env var for
 *     per-environment tuning (recommended for the first prod dry-run).
 *
 * Idempotency: re-runs are safe even if the migration tracking somehow drops.
 *   - Outer guard: SELECT only rows WHERE batch_id IS NULL.
 *   - Junction insert: ON CONFLICT (child_pending_tx_id, source_pending_tx_id)
 *     DO NOTHING — pts_unique_edge prevents dupes.
 *   - remaining_kg + batch_id updates only touch rows selected at the top.
 *
 * Audit trail: writes /tmp/backfill-report-<timestamp>.json with the full plan
 * (edges, orphans, shortfalls, mismatches) for post-deploy inspection, and
 * writes a one-line summary to error_log so it's queryable from the app.
 */
'use strict';

const fs = require('fs');
const path = require('path');
const { computeBackfillPlan } = require('../shared/chain-of-custody');

const UPDATE_BATCH_SIZE = 500;
const INSERT_BATCH_SIZE = 500;

module.exports = {
  name: 'backfill_chain_of_custody',
  up: async (client) => {
    const windowDays = parseInt(process.env.BACKFILL_WINDOW_DAYS, 10) || 14;

    // 1. Load all rows that still need a batch_id.
    const rowsResult = await client.query(`
      SELECT id, transaction_type, status,
             collector_id, aggregator_id, processor_id, recycler_id, converter_id,
             material_type, gross_weight_kg, created_at
      FROM pending_transactions
      WHERE batch_id IS NULL
    `);
    const rows = rowsResult.rows;

    if (rows.length === 0) {
      console.log('  ⏭  No pending_transactions rows need backfill — skipping');
      return;
    }

    // 2. Compute the plan (pure function, no DB access).
    const plan = computeBackfillPlan(rows, { windowDays: windowDays, now: new Date() });

    // 3. Sanity check: for every source, sum(attributed) <= gross_weight_kg.
    //    Should be impossible given the FIFO algorithm's min() guard, but
    //    belt-and-braces to catch any logic regressions.
    const drawBySource = Object.create(null);
    plan.edges.forEach((e) => {
      drawBySource[e.source_pending_tx_id] = (drawBySource[e.source_pending_tx_id] || 0) + e.weight_kg_attributed;
    });
    const grossById = Object.create(null);
    rows.forEach((r) => { grossById[r.id] = parseFloat(r.gross_weight_kg); });
    for (const sid in drawBySource) {
      const gross = grossById[sid];
      if (drawBySource[sid] > gross + 0.01) {     // 0.01 slack for float rounding
        throw new Error(
          'backfill_chain_of_custody sanity check failed: source ' + sid +
          ' would be over-drawn (attributed=' + drawBySource[sid] + ' gross=' + gross + ')'
        );
      }
    }

    // 4. Write the JSON audit report BEFORE executing writes, so we have a
    //    forensic trail even if the writes fail/rollback.
    const ts = new Date().toISOString().replace(/[:.]/g, '-');
    const reportPath = path.join('/tmp', 'backfill-report-' + ts + '.json');
    const shortfallsTotalKg = plan.shortfalls.reduce((a, s) => a + s.unattributed_kg, 0);
    const report = {
      generated_at: new Date().toISOString(),
      window_days: windowDays,
      total_rows_analysed: rows.length,
      edges_count: plan.edges.length,
      orphans_count: plan.orphans.length,
      shortfalls_count: plan.shortfalls.length,
      shortfalls_total_kg: Math.round(shortfallsTotalKg * 100) / 100,
      mismatches_count: plan.mismatches.length,
      edges: plan.edges,
      batch_ids: plan.batchIds,
      remaining_kg: plan.remainingKg,
      orphans: plan.orphans,
      shortfalls: plan.shortfalls,
      mismatches: plan.mismatches
    };
    try {
      fs.writeFileSync(reportPath, JSON.stringify(report, null, 2));
      console.log('  📋 Audit report: ' + reportPath);
    } catch (e) {
      // Non-fatal — on Render the writable-tmp assumption should hold, but if
      // we hit a read-only FS the migration should still complete.
      console.warn('  ⚠️  Failed to write audit report (' + e.message + '); continuing.');
    }

    // 5. Execute writes. All within the runner's BEGIN/COMMIT.

    // 5a. Junction edges — chunked INSERT with ON CONFLICT DO NOTHING.
    if (plan.edges.length > 0) {
      for (let i = 0; i < plan.edges.length; i += INSERT_BATCH_SIZE) {
        const chunk = plan.edges.slice(i, i + INSERT_BATCH_SIZE);
        const placeholders = [];
        const values = [];
        chunk.forEach((e, idx) => {
          const off = idx * 3;
          placeholders.push('($' + (off + 1) + '::int, $' + (off + 2) + '::int, $' + (off + 3) + '::numeric)');
          values.push(e.child_pending_tx_id, e.source_pending_tx_id, e.weight_kg_attributed);
        });
        await client.query(
          'INSERT INTO pending_transaction_sources (child_pending_tx_id, source_pending_tx_id, weight_kg_attributed) ' +
          'VALUES ' + placeholders.join(', ') + ' ' +
          'ON CONFLICT (child_pending_tx_id, source_pending_tx_id) DO NOTHING',
          values
        );
      }
    }

    // 5b. batch_id bulk UPDATE.
    if (plan.batchIds.length > 0) {
      for (let i = 0; i < plan.batchIds.length; i += UPDATE_BATCH_SIZE) {
        const chunk = plan.batchIds.slice(i, i + UPDATE_BATCH_SIZE);
        const placeholders = [];
        const values = [];
        chunk.forEach((b, idx) => {
          const off = idx * 2;
          placeholders.push('($' + (off + 1) + '::int, $' + (off + 2) + '::uuid)');
          values.push(b.id, b.batch_id);
        });
        await client.query(
          'UPDATE pending_transactions SET batch_id = v.batch_id ' +
          'FROM (VALUES ' + placeholders.join(', ') + ') AS v(id, batch_id) ' +
          'WHERE pending_transactions.id = v.id AND pending_transactions.batch_id IS NULL',
          values
        );
      }
    }

    // 5c. remaining_kg bulk UPDATE.
    if (plan.remainingKg.length > 0) {
      for (let i = 0; i < plan.remainingKg.length; i += UPDATE_BATCH_SIZE) {
        const chunk = plan.remainingKg.slice(i, i + UPDATE_BATCH_SIZE);
        const placeholders = [];
        const values = [];
        chunk.forEach((r, idx) => {
          const off = idx * 2;
          placeholders.push('($' + (off + 1) + '::int, $' + (off + 2) + '::numeric)');
          values.push(r.id, r.remaining_kg);
        });
        await client.query(
          'UPDATE pending_transactions SET remaining_kg = v.remaining_kg ' +
          'FROM (VALUES ' + placeholders.join(', ') + ') AS v(id, remaining_kg) ' +
          'WHERE pending_transactions.id = v.id AND pending_transactions.remaining_kg IS NULL',
          values
        );
      }
    }

    // 6. Summary to error_log so post-deploy queries can see what happened.
    const summary =
      'Backfill complete: ' + rows.length + ' rows analysed, ' +
      plan.edges.length + ' edges, ' +
      plan.orphans.length + ' orphans, ' +
      plan.shortfalls.length + ' shortfalls (' +
      (Math.round(shortfallsTotalKg * 100) / 100) + 'kg unattributed), ' +
      plan.mismatches.length + ' material mismatches';

    await client.query(
      `INSERT INTO error_log (source, dashboard, error_message)
       VALUES ('migration', 'chain_of_custody_backfill', $1)`,
      [summary]
    );

    console.log('  ✅ ' + summary);
  }
};
