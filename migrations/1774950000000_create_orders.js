/**
 * Migration: create_orders
 *
 * Creates the orders table for purchase orders placed by converters and recyclers.
 * Columns match the frontend submitOrder() payloads in converter-dashboard.html
 * and recycler-dashboard.html, plus the existing POST /api/orders route in server.js.
 *
 * Idempotent: uses IF NOT EXISTS.
 */
module.exports = {
  name: 'create_orders',
  up: async (client) => {
    await client.query(`
      CREATE TABLE IF NOT EXISTS orders (
        id                      SERIAL PRIMARY KEY,
        buyer_id                INTEGER NOT NULL,
        buyer_role              VARCHAR(20) NOT NULL,
        material_type           VARCHAR(20) NOT NULL,
        target_quantity_kg      NUMERIC(10,2) NOT NULL,
        price_per_kg            NUMERIC(10,2) NOT NULL,
        accepted_colours        TEXT,
        excluded_contaminants   TEXT,
        max_contamination_pct   NUMERIC(5,2),
        supplier_tier           VARCHAR(20),
        supplier_id             INTEGER,
        notes                   TEXT,
        status                  VARCHAR(30) NOT NULL DEFAULT 'open',
        fulfilled_kg            NUMERIC(10,2) NOT NULL DEFAULT 0,
        created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT orders_status_check CHECK (status IN ('open', 'accepted', 'partially_fulfilled', 'fulfilled', 'cancelled'))
      )
    `);

    // Index for buyer lookups (GET /api/orders/my)
    await client.query(`
      CREATE INDEX IF NOT EXISTS idx_orders_buyer ON orders (buyer_id, buyer_role)
    `);

    // Index for status filtering (open orders count)
    await client.query(`
      CREATE INDEX IF NOT EXISTS idx_orders_status ON orders (status) WHERE status = 'open'
    `);
  }
};
