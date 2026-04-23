/**
 * Migration: relabel_mislabeled_seller_sales
 *
 * Companion to 1777300000000_relabel_mislabeled_collector_sales.js (PR43).
 * That migration handled the collector_id IS NOT NULL discriminator. This
 * one closes the second class of mis-labels caused by the buggy
 * txnTypeForRoles fallback (server.js:1346 pre-PR6, fixed in PR6-a):
 * processor and recycler offer-accepts in the discovery flow that wrote
 * transaction_type='aggregator_sale' even though neither party was an
 * aggregator.
 *
 * Scope discriminators — precise by FK pattern. The polymorphic write
 * model populates exactly one seller-side FK per row, so the seller's tier
 * is recoverable from the column shape:
 *
 *   processor_sale (mis-labeled aggregator_sale):
 *     transaction_type = 'aggregator_sale'
 *     processor_id  IS NOT NULL
 *     aggregator_id IS NULL                ← key: legit aggregator_sale rows
 *                                            always have aggregator_id set
 *
 *   recycler_sale (mis-labeled aggregator_sale):
 *     transaction_type = 'aggregator_sale'
 *     recycler_id   IS NOT NULL
 *     aggregator_id IS NULL
 *     processor_id  IS NULL                ← exclude rows that match the
 *                                            processor branch above
 *
 * Anomaly we deliberately do NOT touch:
 *     transaction_type = 'aggregator_sale'
 *     processor_id IS NOT NULL AND aggregator_id IS NOT NULL
 *   That's a different (worse) data shape — both seller and buyer FKs set
 *   on the same row. Out of PR6 scope; logged + counted but skipped.
 *
 * Behavior on prod: PR43 (collector-sale variant) found 0 mis-labeled rows
 * on prod — the bug is code-path only and only local-dev traffic accumulates
 * mis-labels. We expect this migration to find 0 too. Run regardless so the
 * audit trail records the dry-run counts.
 *
 * Wraps in migrate.js's BEGIN/COMMIT envelope so partial failure rolls
 * back atomically. _migrations row written by the runner on success.
 */
'use strict';

module.exports = {
  name: 'relabel_mislabeled_seller_sales',
  up: async (client) => {
    // ── 1. Dry-run detection (counts only, before any UPDATE).
    const procPattern = await client.query(`
      SELECT COUNT(*)::int AS c
        FROM pending_transactions
       WHERE transaction_type = 'aggregator_sale'
         AND processor_id  IS NOT NULL
         AND aggregator_id IS NULL
    `);
    const recPattern = await client.query(`
      SELECT COUNT(*)::int AS c
        FROM pending_transactions
       WHERE transaction_type = 'aggregator_sale'
         AND recycler_id   IS NOT NULL
         AND aggregator_id IS NULL
         AND processor_id  IS NULL
    `);
    // Anomaly counter — both seller AND buyer FKs set. Logged; not touched.
    const ambiguous = await client.query(`
      SELECT COUNT(*)::int AS c
        FROM pending_transactions
       WHERE transaction_type = 'aggregator_sale'
         AND processor_id  IS NOT NULL
         AND aggregator_id IS NOT NULL
    `);

    console.log(
      '[migration] relabel_mislabeled_seller_sales: dry-run — ' +
      'processor pattern=' + procPattern.rows[0].c + ', ' +
      'recycler pattern='  + recPattern.rows[0].c  + ', ' +
      'ambiguous (seller+buyer both set, NOT touched)=' + ambiguous.rows[0].c
    );

    // Per-material breakdown for audit trail (skipped if both patterns are 0).
    if ((procPattern.rows[0].c + recPattern.rows[0].c) > 0) {
      const breakdown = await client.query(`
        SELECT
          CASE
            WHEN processor_id IS NOT NULL AND aggregator_id IS NULL THEN 'processor_sale'
            WHEN recycler_id  IS NOT NULL AND aggregator_id IS NULL
                                          AND processor_id  IS NULL THEN 'recycler_sale'
          END AS new_type,
          material_type,
          COUNT(*)::int AS n
          FROM pending_transactions
         WHERE transaction_type = 'aggregator_sale'
           AND ((processor_id IS NOT NULL AND aggregator_id IS NULL)
             OR (recycler_id  IS NOT NULL AND aggregator_id IS NULL AND processor_id IS NULL))
         GROUP BY new_type, material_type
         ORDER BY new_type, material_type
      `);
      for (const r of breakdown.rows) {
        console.log(
          '[migration] relabel_mislabeled_seller_sales:   ' +
          r.new_type + ' / ' + r.material_type + ' = ' + r.n
        );
      }
    }

    // ── 2. UPDATE: processor pattern → 'processor_sale'.
    const procResult = await client.query(`
      UPDATE pending_transactions
         SET transaction_type = 'processor_sale',
             updated_at = NOW()
       WHERE transaction_type = 'aggregator_sale'
         AND processor_id  IS NOT NULL
         AND aggregator_id IS NULL
    `);
    console.log(
      '[migration] relabel_mislabeled_seller_sales: ' +
      procResult.rowCount + ' row(s) relabeled aggregator_sale → processor_sale'
    );

    // ── 3. UPDATE: recycler pattern → 'recycler_sale'.
    const recResult = await client.query(`
      UPDATE pending_transactions
         SET transaction_type = 'recycler_sale',
             updated_at = NOW()
       WHERE transaction_type = 'aggregator_sale'
         AND recycler_id   IS NOT NULL
         AND aggregator_id IS NULL
         AND processor_id  IS NULL
    `);
    console.log(
      '[migration] relabel_mislabeled_seller_sales: ' +
      recResult.rowCount + ' row(s) relabeled aggregator_sale → recycler_sale'
    );
  }
};
