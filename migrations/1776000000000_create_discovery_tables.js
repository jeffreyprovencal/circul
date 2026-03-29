/**
 * Migration: create_discovery_tables
 *
 * Creates listings and offers tables for the Discovery feature.
 * Adds source column to pending_transactions to distinguish
 * discovery-originated transactions from direct ones.
 *
 * Idempotent: uses IF NOT EXISTS.
 */
module.exports = {
  name: 'create_discovery_tables',
  up: async (client) => {
    await client.query(`
      CREATE TABLE IF NOT EXISTS listings (
        id              SERIAL PRIMARY KEY,
        seller_id       INTEGER NOT NULL,
        seller_role     VARCHAR(20) NOT NULL,
        material_type   VARCHAR(10) NOT NULL,
        quantity_kg     NUMERIC(10,2) NOT NULL,
        original_qty_kg NUMERIC(10,2) NOT NULL,
        price_per_kg    NUMERIC(10,2),
        location        VARCHAR(255),
        photo_url       TEXT,
        status          VARCHAR(20) DEFAULT 'active' CHECK (status IN ('active', 'expired', 'closed')),
        expires_at      TIMESTAMPTZ NOT NULL,
        renewal_count   INTEGER DEFAULT 0,
        created_at      TIMESTAMPTZ DEFAULT NOW(),
        updated_at      TIMESTAMPTZ DEFAULT NOW()
      )
    `);

    await client.query(`
      CREATE TABLE IF NOT EXISTS offers (
        id              SERIAL PRIMARY KEY,
        listing_id      INTEGER NOT NULL REFERENCES listings(id),
        thread_id       UUID NOT NULL DEFAULT gen_random_uuid(),
        buyer_id        INTEGER NOT NULL,
        buyer_role      VARCHAR(20) NOT NULL,
        price_per_kg    NUMERIC(10,2) NOT NULL,
        quantity_kg     NUMERIC(10,2) NOT NULL,
        round           INTEGER NOT NULL DEFAULT 1,
        is_final        BOOLEAN DEFAULT FALSE,
        offered_by      VARCHAR(20) NOT NULL,
        status          VARCHAR(20) DEFAULT 'pending' CHECK (status IN ('pending', 'accepted', 'rejected', 'countered', 'expired')),
        parent_offer_id INTEGER REFERENCES offers(id),
        created_at      TIMESTAMPTZ DEFAULT NOW(),
        responded_at    TIMESTAMPTZ
      )
    `);

    // Indexes for listings
    await client.query(`CREATE INDEX IF NOT EXISTS idx_listings_active ON listings(status, seller_role) WHERE status = 'active'`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_listings_expiry ON listings(expires_at) WHERE status = 'active'`);

    // Indexes for offers
    await client.query(`CREATE INDEX IF NOT EXISTS idx_offers_listing ON offers(listing_id, status)`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_offers_thread ON offers(thread_id)`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_offers_buyer ON offers(buyer_id, buyer_role, status)`);

    // Add source column to pending_transactions
    await client.query(`ALTER TABLE pending_transactions ADD COLUMN IF NOT EXISTS source VARCHAR(20) DEFAULT 'direct'`);
  },
  down: async (client) => {
    await client.query(`DROP TABLE IF EXISTS offers`);
    await client.query(`DROP TABLE IF EXISTS listings`);
    await client.query(`ALTER TABLE pending_transactions DROP COLUMN IF EXISTS source`);
  }
};
