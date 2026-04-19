#!/usr/bin/env node
// scripts/chain-of-custody-invariant.js
// Runnable mass-balance auditor for pending_transactions + pending_transaction_sources.
// Reads DATABASE_URL, runs 4 checks, prints a human-readable report.
//
// Exit codes:
//   0 — checks 1–3 clean (regardless of check 4 count)
//   1 — any of checks 1–3 shows a violation
//
// Usage: set -a; source .env; set +a; node scripts/chain-of-custody-invariant.js
//
// Check summary:
//   1. Downstream row's total_attributed matches its gross_weight_kg (±0.01).
//   2. Source row's remaining_kg matches gross_weight_kg − SUM(attributed) (±0.01).
//   3. No row has remaining_kg < 0 (DB CHECK should prevent; verified anyway).
//   4. Unattributed rows (batch_id IS NULL) by transaction_type — INFORMATIONAL.
//      Expected to be > 0 post-PR3 until the 3 discovery insert sites at
//      server.js:1469 / 3701 / 4131 are wired (see PR6 follow-up).

'use strict';

const { Pool } = require('pg');

async function main() {
  const pool = new Pool({ connectionString: process.env.DATABASE_URL });
  let exitCode = 0;

  try {
    console.log('=== Chain-of-custody invariant audit ===');
    console.log('DB: ' + (process.env.DATABASE_URL || '(unset)').replace(/:[^:@]*@/, ':***@'));
    console.log('');

    // ── Check 1: downstream row's total_attributed == gross_weight_kg ──────
    //
    // Every non-root, non-excluded-status row with AT LEAST ONE junction
    // edge must have SUM of weight_kg_attributed equal to gross_weight_kg.
    // Rows with ZERO edges are PR2-backfill orphans (backfill could not
    // find upstream candidates) — they are surfaced as informational in
    // Check 4b, not flagged here. Only partial attribution (some edges but
    // delta > 0.01) is a real mass-balance violation.
    console.log('=== Check 1: Downstream rows with edges must have total_attributed = gross_weight_kg ===');
    const check1 = await pool.query(`
      SELECT pt.id,
             pt.transaction_type,
             pt.material_type,
             pt.gross_weight_kg,
             SUM(pts.weight_kg_attributed) AS total_attributed,
             (pt.gross_weight_kg - SUM(pts.weight_kg_attributed)) AS delta_kg
        FROM pending_transactions pt
        JOIN pending_transaction_sources pts ON pts.child_pending_tx_id = pt.id
       WHERE pt.transaction_type NOT IN ('collector_sale', 'aggregator_purchase')
         AND pt.status NOT IN ('rejected','dispatch_rejected','grade_c_flagged')
         AND pt.batch_id IS NOT NULL
       GROUP BY pt.id
      HAVING ABS(pt.gross_weight_kg - SUM(pts.weight_kg_attributed)) > 0.01
       ORDER BY pt.id
    `);
    if (check1.rows.length === 0) {
      console.log('  ✅ CLEAN (0 violations)');
    } else {
      exitCode = 1;
      console.log('  ❌ ' + check1.rows.length + ' violation(s):');
      check1.rows.forEach(function (r) {
        console.log('    pending_tx=' + r.id + ' type=' + r.transaction_type +
                    ' material=' + r.material_type +
                    ' gross=' + r.gross_weight_kg +
                    ' attributed=' + r.total_attributed +
                    ' delta=' + r.delta_kg + 'kg');
      });
    }
    console.log('');

    // ── Check 2: source's remaining_kg == gross_weight_kg − SUM(attributed) ──
    //
    // Every row that could be a source (any included status with batch_id set)
    // must have its remaining_kg match the algebraic identity. Skip rows with
    // no outgoing edges — their identity is remaining_kg == gross, handled by
    // the query's COALESCE(…, 0).
    console.log('=== Check 2: Source rows must satisfy remaining_kg = gross_weight_kg − SUM(attributed) ===');
    const check2 = await pool.query(`
      SELECT pt.id,
             pt.transaction_type,
             pt.material_type,
             pt.gross_weight_kg,
             pt.remaining_kg,
             COALESCE(SUM(pts.weight_kg_attributed), 0) AS total_drawn,
             (pt.gross_weight_kg - COALESCE(SUM(pts.weight_kg_attributed), 0)) AS expected_remaining,
             (pt.remaining_kg - (pt.gross_weight_kg - COALESCE(SUM(pts.weight_kg_attributed), 0))) AS delta_kg
        FROM pending_transactions pt
        LEFT JOIN pending_transaction_sources pts ON pts.source_pending_tx_id = pt.id
       WHERE pt.status NOT IN ('rejected','dispatch_rejected','grade_c_flagged')
         AND pt.batch_id IS NOT NULL
         AND pt.remaining_kg IS NOT NULL
       GROUP BY pt.id
      HAVING ABS(pt.remaining_kg - (pt.gross_weight_kg - COALESCE(SUM(pts.weight_kg_attributed), 0))) > 0.01
       ORDER BY pt.id
    `);
    if (check2.rows.length === 0) {
      console.log('  ✅ CLEAN (0 violations)');
    } else {
      exitCode = 1;
      console.log('  ❌ ' + check2.rows.length + ' violation(s):');
      check2.rows.forEach(function (r) {
        console.log('    pending_tx=' + r.id + ' type=' + r.transaction_type +
                    ' gross=' + r.gross_weight_kg +
                    ' drawn=' + r.total_drawn +
                    ' remaining=' + r.remaining_kg +
                    ' expected=' + r.expected_remaining +
                    ' delta=' + r.delta_kg + 'kg');
      });
    }
    console.log('');

    // ── Check 3: no remaining_kg < 0 ──────────────────────────────────────
    //
    // Defensive — the pending_tx_remaining_kg_nonneg CHECK (from PR1) should
    // prevent this. Verify anyway in case constraint was ever dropped/disabled.
    console.log('=== Check 3: No row may have remaining_kg < 0 (DB CHECK guards this) ===');
    const check3 = await pool.query(`
      SELECT id, transaction_type, material_type, gross_weight_kg, remaining_kg
        FROM pending_transactions
       WHERE remaining_kg < 0
       ORDER BY id
    `);
    if (check3.rows.length === 0) {
      console.log('  ✅ CLEAN (0 violations)');
    } else {
      exitCode = 1;
      console.log('  ❌ ' + check3.rows.length + ' violation(s):');
      check3.rows.forEach(function (r) {
        console.log('    pending_tx=' + r.id + ' type=' + r.transaction_type +
                    ' remaining=' + r.remaining_kg + ' (NEGATIVE)');
      });
    }
    console.log('');

    // ── Check 4a: Unattributed rows (INFORMATIONAL, not a violation until PR6) ──
    //
    // Rows with batch_id IS NULL either come from pre-PR2-backfill state
    // (should be zero post-backfill) or from post-PR3 discovery-flow writes
    // at server.js:1469 / 3701 / 4131 that are deferred to PR6. PR6 will
    // tighten this check to exit non-zero on positive counts.
    console.log('=== Check 4a: Unattributed rows, batch_id IS NULL (INFORMATIONAL, tightens in PR6) ===');
    const check4a = await pool.query(`
      SELECT transaction_type, COUNT(*) AS count
        FROM pending_transactions
       WHERE batch_id IS NULL
         AND status NOT IN ('rejected', 'dispatch_rejected', 'grade_c_flagged')
       GROUP BY transaction_type
       ORDER BY transaction_type
    `);
    if (check4a.rows.length === 0) {
      console.log('  (zero unattributed rows across all transaction types)');
    } else {
      console.log('  transaction_type         count');
      console.log('  -----------------------  -----');
      check4a.rows.forEach(function (r) {
        console.log('  ' + String(r.transaction_type).padEnd(23) + '  ' + r.count);
      });
    }
    console.log('');

    // ── Check 4b: PR2 legacy orphans (INFORMATIONAL) ──────────────────────
    //
    // Non-root rows that have batch_id set (so not caught by 4a) but zero
    // junction edges. These are PR2-backfill orphans — the backfill could
    // not find upstream candidates within its 14-day window, so it assigned
    // a fresh batch_id and left attribution blank. Expected to be > 0 on
    // any DB whose backfill reported orphans (see PR #40 deploy audit).
    console.log('=== Check 4b: PR2 legacy orphans, non-root with batch_id but no edges (INFORMATIONAL) ===');
    const check4b = await pool.query(`
      SELECT pt.transaction_type, COUNT(*) AS count
        FROM pending_transactions pt
        LEFT JOIN pending_transaction_sources pts ON pts.child_pending_tx_id = pt.id
       WHERE pt.transaction_type NOT IN ('collector_sale', 'aggregator_purchase')
         AND pt.status NOT IN ('rejected', 'dispatch_rejected', 'grade_c_flagged')
         AND pt.batch_id IS NOT NULL
         AND pts.id IS NULL
       GROUP BY pt.transaction_type
       ORDER BY pt.transaction_type
    `);
    if (check4b.rows.length === 0) {
      console.log('  (zero legacy orphans)');
    } else {
      console.log('  transaction_type         count');
      console.log('  -----------------------  -----');
      check4b.rows.forEach(function (r) {
        console.log('  ' + String(r.transaction_type).padEnd(23) + '  ' + r.count);
      });
    }
    console.log('');

    // ── Summary ───────────────────────────────────────────────────────────
    if (exitCode === 0) {
      console.log('=== RESULT: checks 1–3 CLEAN ===');
    } else {
      console.log('=== RESULT: checks 1–3 VIOLATIONS FOUND (exit 1) ===');
    }
  } finally {
    await pool.end();
  }

  process.exit(exitCode);
}

main().catch(function (e) {
  console.error('Fatal:', e);
  process.exit(2);
});
