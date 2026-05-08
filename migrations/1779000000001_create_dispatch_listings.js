module.exports = {
  name: 'create_dispatch_listings',
  up: async (client) => {
    // Aggregator posts a dispatch need. Marketplace listings are region-scoped.
    // Direct invitations reuse this table with region='_DIRECT_' sentinel +
    // awarded_to_driver_id pre-set at creation (per spec rationale: avoids a
    // separate table while keeping driver "Available work" view unified).
    await client.query(`
      CREATE TABLE IF NOT EXISTS dispatch_listings (
        id                       SERIAL PRIMARY KEY,
        aggregator_id            INTEGER NOT NULL REFERENCES aggregators(id),
        pickup_location          TEXT NOT NULL,
        destination_buyer_kind   TEXT,        -- 'processor' | 'recycler' | 'converter'
        destination_buyer_id     INTEGER,
        material_type            TEXT NOT NULL,
        gross_weight_kg          NUMERIC(10,2) NOT NULL,
        proposed_fee_ghs         NUMERIC(10,2) NOT NULL,
        region                   TEXT NOT NULL,
          -- region scoping for driver visibility (no GPS — region match)
          -- '_DIRECT_' sentinel = direct invite to a specific driver
        rejected_disposition     TEXT NOT NULL DEFAULT 'leave_at_buyer',
          -- 'leave_at_buyer' (default) | 'bring_back' | 'sell_as_scrap'
        status                   TEXT NOT NULL DEFAULT 'open',
          -- 'open' | 'awarded' | 'expired' | 'cancelled'
        awarded_to_driver_id     INTEGER REFERENCES drivers(id),
        expires_at               TIMESTAMPTZ NOT NULL,
          -- 24h from creation for marketplace, 4h for direct invites
        pending_transaction_id   INTEGER REFERENCES pending_transactions(id),
          -- linked once accepted; transactions get created from listings
        created_at               TIMESTAMPTZ DEFAULT NOW()
      )
    `);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_listings_region_open ON dispatch_listings(region) WHERE status = 'open'`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_listings_aggregator ON dispatch_listings(aggregator_id)`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_listings_expires ON dispatch_listings(expires_at) WHERE status = 'open'`);

    // Driver offer on a listing — accept-as-is OR counter-offer
    await client.query(`
      CREATE TABLE IF NOT EXISTS driver_offers (
        id                   SERIAL PRIMARY KEY,
        listing_id           INTEGER NOT NULL REFERENCES dispatch_listings(id) ON DELETE CASCADE,
        driver_id            INTEGER NOT NULL REFERENCES drivers(id),
        offer_type           TEXT NOT NULL,
          -- 'accept_proposed' = driver accepts the listing's proposed_fee_ghs
          -- 'counter'         = driver offers a different fee
        counter_fee_ghs      NUMERIC(10,2),
          -- null when offer_type = 'accept_proposed'
        status               TEXT NOT NULL DEFAULT 'pending',
          -- 'pending' | 'accepted' | 'rejected' | 'withdrawn'
        created_at           TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE (listing_id, driver_id)
      )
    `);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_offers_listing ON driver_offers(listing_id)`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_offers_driver_pending ON driver_offers(driver_id) WHERE status = 'pending'`);
  },
  down: async (client) => {
    await client.query(`DROP TABLE IF EXISTS driver_offers CASCADE`);
    await client.query(`DROP TABLE IF EXISTS dispatch_listings CASCADE`);
  }
};
