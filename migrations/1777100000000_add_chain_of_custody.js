/**
 * Migration: add_chain_of_custody
 *
 * Schema foundation for end-to-end chain of custody on pending_transactions.
 *
 * IMPORTANT: PURE ADDITIVE. No existing read or write path uses these columns
 * or table. PR2 backfills historical rows; PR3 updates the aggregator-sale /
 * processor-sale / recycler-sale write endpoints to populate them and enforce
 * mass balance on insert.
 *
 * Adds:
 *   1. pending_transactions.batch_id UUID (nullable)
 *      Unique identifier for a physical lot. Generated at the Stage 1
 *      collector drop-off row (or the Stage 2 aggregator_purchase row, if no
 *      collector_sale preceded it) and propagated forward onto every
 *      downstream row that draws from the same material. Nullable during the
 *      PR2 backfill window; a later PR can tighten to NOT NULL.
 *
 *   2. pending_transactions.remaining_kg NUMERIC(10,2) (nullable)
 *      Balance of unattributed material on this row. Decremented at insert
 *      time on each downstream sale that draws from this row via the junction.
 *      Denormalised so mass-balance checks are O(1) rather than a GROUP BY
 *      across pending_transaction_sources.
 *
 *   3. pending_transaction_sources (new junction table)
 *      Many-to-many weight attribution. One downstream row may have N sources
 *      (commingled bale); one source row may feed N downstream rows (partial
 *      draws). Each edge carries weight_kg_attributed so a traceability DAG
 *      walk can compute mass balance at any depth.
 *
 * All ALTER / CREATE statements use IF NOT EXISTS for idempotency. The
 * migrate.js runner wraps every migration in BEGIN/COMMIT, so partial
 * failure rolls back atomically.
 */
'use strict';

module.exports = {
  name: 'add_chain_of_custody',
  up: async (client) => {

    // ── 1. Add batch_id to pending_transactions ────────────────────────────
    await client.query(`
      ALTER TABLE pending_transactions
        ADD COLUMN IF NOT EXISTS batch_id UUID
    `);

    // Partial index: we only query rows that HAVE a batch_id. Keeps the
    // index small during the backfill transition when most rows are NULL.
    await client.query(`
      CREATE INDEX IF NOT EXISTS idx_pending_tx_batch_id
        ON pending_transactions(batch_id)
        WHERE batch_id IS NOT NULL
    `);

    // ── 2. Add remaining_kg to pending_transactions ────────────────────────
    await client.query(`
      ALTER TABLE pending_transactions
        ADD COLUMN IF NOT EXISTS remaining_kg NUMERIC(10,2)
    `);

    // CHECK constraint: remaining_kg is either NULL (backfill window) or >= 0.
    // PR2 populates historical rows; PR3 can tighten to NOT NULL once backfill
    // completes. The IS NULL branch is intentional — it lets every pre-existing
    // row satisfy the constraint without touching data.
    //
    // Idempotency: matches the repo house style (DROP IF EXISTS + ADD). The
    // drop-and-add is transactional inside migrate.js's BEGIN/COMMIT, so there
    // is no window where the table is unconstrained. Safe to re-run because
    // NULL values trivially satisfy the re-added CHECK.
    await client.query(`
      ALTER TABLE pending_transactions
        DROP CONSTRAINT IF EXISTS pending_tx_remaining_kg_nonneg
    `);
    await client.query(`
      ALTER TABLE pending_transactions
        ADD CONSTRAINT pending_tx_remaining_kg_nonneg
        CHECK (remaining_kg IS NULL OR remaining_kg >= 0)
    `);

    // ── 3. Create pending_transaction_sources junction table ───────────────
    await client.query(`
      CREATE TABLE IF NOT EXISTS pending_transaction_sources (
        id                      SERIAL PRIMARY KEY,
        child_pending_tx_id     INTEGER NOT NULL
                                REFERENCES pending_transactions(id) ON DELETE CASCADE,
        source_pending_tx_id    INTEGER NOT NULL
                                REFERENCES pending_transactions(id) ON DELETE CASCADE,
        weight_kg_attributed    NUMERIC(10,2) NOT NULL
                                CHECK (weight_kg_attributed > 0),
        created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT pts_no_self_reference
          CHECK (child_pending_tx_id <> source_pending_tx_id),
        CONSTRAINT pts_unique_edge
          UNIQUE (child_pending_tx_id, source_pending_tx_id)
      )
    `);

    // DAG walk upstream: given a downstream row, find all its sources.
    await client.query(`
      CREATE INDEX IF NOT EXISTS idx_pts_child
        ON pending_transaction_sources(child_pending_tx_id)
    `);

    // DAG walk downstream: given an upstream row, find all draws from it.
    await client.query(`
      CREATE INDEX IF NOT EXISTS idx_pts_source
        ON pending_transaction_sources(source_pending_tx_id)
    `);

  }
};
