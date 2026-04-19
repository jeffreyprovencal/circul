/**
 * Migration: relabel_mislabeled_collector_sales
 *
 * Retroactively relabels pending_transactions rows that were incorrectly
 * inserted with transaction_type='aggregator_sale' by two server.js sites
 * (USSD agent-collection at server.js:3104 and POST /api/agent/log-collection
 * at server.js:5791). Those flows are semantically collector→aggregator
 * drop-offs, and the correct PARTY_MAP type is 'collector_sale'
 * (sellerKind=collector, buyerKinds=['aggregator']).
 *
 * Scope discriminator — precise by construction:
 *   - transaction_type = 'aggregator_sale'
 *   - collector_id IS NOT NULL   (only mis-labeled rows populate this)
 *   - aggregator_id IS NOT NULL
 *   - processor_id IS NULL
 *   - recycler_id IS NULL
 *   - converter_id IS NULL
 *
 * Any legit aggregator_sale has seller=aggregator and no collector_id set,
 * so the `collector_id IS NOT NULL` clause alone is sufficient to isolate
 * the mis-labeled rows. The NULL-buyer-FK clauses are defensive: if any
 * row somehow has both a collector_id AND a buyer FK populated, that is a
 * different kind of bad data that should be investigated separately, not
 * swept up by this migration.
 *
 * No downstream harm: existing rows are all status='completed', which is
 * upstream of every 'pending'-filtered read path. Aggregator dashboards
 * that previously inflated total_sold with these rows will now correctly
 * exclude them; collector and aggregator purchase-side queries (which
 * filter IN ('collector_sale','aggregator_purchase')) will correctly
 * include them. These are corrections, not regressions.
 *
 * Wraps in migrate.js's BEGIN/COMMIT so partial failure rolls back atomically.
 */
'use strict';

module.exports = {
  name: 'relabel_mislabeled_collector_sales',
  up: async (client) => {
    const result = await client.query(`
      UPDATE pending_transactions
      SET transaction_type = 'collector_sale',
          updated_at = NOW()
      WHERE transaction_type = 'aggregator_sale'
        AND collector_id  IS NOT NULL
        AND aggregator_id IS NOT NULL
        AND processor_id  IS NULL
        AND recycler_id   IS NULL
        AND converter_id  IS NULL
    `);
    console.log(
      '[migration] relabel_mislabeled_collector_sales: ' +
      result.rowCount + ' row(s) relabeled aggregator_sale → collector_sale'
    );
  }
};
