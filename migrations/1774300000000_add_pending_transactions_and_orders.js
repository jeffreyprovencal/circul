'use strict';

module.exports = {
  name: 'add_pending_transactions_and_orders',
  up: async (client) => {

    // ── TABLE 1: pending_transactions ──────────────────────────────────────────
    await client.query(`
      CREATE TABLE IF NOT EXISTS pending_transactions (
        id SERIAL PRIMARY KEY,
        transaction_type VARCHAR(50) NOT NULL,
        -- transaction_type values:
        -- 'collector_sale'        — collector logged a sale, waiting for aggregator to accept
        -- 'aggregator_purchase'   — aggregator initiated a purchase, waiting for collector to confirm
        -- 'aggregator_sale'       — aggregator logged a sale to processor, waiting for processor to accept
        -- 'processor_purchase'    — processor initiated a purchase, waiting for aggregator to confirm
        -- 'processor_sale'        — processor logged a sale to converter, waiting for converter to accept
        status VARCHAR(20) NOT NULL DEFAULT 'pending',
        -- status values: 'pending', 'confirmed', 'rejected', 'cancelled'

        -- Parties involved
        collector_id              INTEGER REFERENCES collectors(id),
        aggregator_operator_id    INTEGER REFERENCES operators(id),
        processor_buyer_id        INTEGER REFERENCES buyers(id),
        converter_buyer_id        INTEGER REFERENCES buyers(id),

        -- Material details
        material_type             VARCHAR(10) NOT NULL,
        gross_weight_kg           NUMERIC(10,2) NOT NULL,
        price_per_kg              NUMERIC(10,2),
        total_price               NUMERIC(10,2),

        -- Grading (assigned by receiving party at intake)
        grade                     VARCHAR(1),
        -- 'A' = spec-compliant, <5% contamination, published price + 10%
        -- 'B' = minor issues, 5-15% contamination, published price
        -- 'C' = >15% contamination or off-spec, rejected or discounted
        grade_notes               TEXT,

        -- Photo dispatch review (for aggregator→processor batches over 500kg)
        photos_required           BOOLEAN DEFAULT FALSE,
        photos_submitted          BOOLEAN DEFAULT FALSE,
        dispatch_approved         BOOLEAN,
        dispatch_approved_at      TIMESTAMPTZ,
        dispatch_approved_by_buyer_id INTEGER REFERENCES buyers(id),

        -- Photo URLs (stored as JSON array of strings — Cloudinary URLs post-demo)
        photo_urls                JSONB DEFAULT '[]',

        -- Rejection
        rejection_reason          TEXT,
        rejected_at               TIMESTAMPTZ,

        -- Linked finalised transaction (set when confirmed and transaction created)
        transaction_id            INTEGER REFERENCES transactions(id),

        -- Notes
        notes                     TEXT,
        created_at                TIMESTAMPTZ DEFAULT NOW(),
        updated_at                TIMESTAMPTZ DEFAULT NOW()
      )
    `);

    await client.query(`CREATE INDEX IF NOT EXISTS idx_pending_transactions_collector  ON pending_transactions(collector_id)`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_pending_transactions_aggregator ON pending_transactions(aggregator_operator_id)`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_pending_transactions_processor  ON pending_transactions(processor_buyer_id)`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_pending_transactions_status     ON pending_transactions(status)`);

    // ── TABLE 2: orders ────────────────────────────────────────────────────────
    await client.query(`
      CREATE TABLE IF NOT EXISTS orders (
        id SERIAL PRIMARY KEY,
        order_number VARCHAR(20) UNIQUE,
        -- format: ORD-XXXXX, generated on insert
        status VARCHAR(20) NOT NULL DEFAULT 'pending',
        -- status values: 'pending', 'accepted', 'in_progress', 'fulfilled', 'cancelled'

        -- Converter places the order
        converter_buyer_id        INTEGER REFERENCES buyers(id) NOT NULL,
        -- Processor fulfils the order
        processor_buyer_id        INTEGER REFERENCES buyers(id),

        -- Material specification
        material_type             VARCHAR(10) NOT NULL,
        target_quantity_kg        NUMERIC(10,2) NOT NULL,
        fulfilled_quantity_kg     NUMERIC(10,2) DEFAULT 0,
        price_per_kg_offered      NUMERIC(10,2),

        -- Material specifications (what the converter requires)
        spec_accepted_colours     TEXT[],
        -- e.g. ARRAY['clear', 'light blue']
        spec_excluded_contaminants TEXT[],
        -- e.g. ARRAY['glass', 'sand', 'aluminium', 'residual liquid']
        spec_max_contamination_percent NUMERIC(5,2),
        spec_notes                TEXT,

        -- Photos mandatory on every processor→converter delivery for this order
        photos_required           BOOLEAN DEFAULT TRUE,

        -- Timestamps
        placed_at                 TIMESTAMPTZ DEFAULT NOW(),
        accepted_at               TIMESTAMPTZ,
        fulfilled_at              TIMESTAMPTZ,
        cancelled_at              TIMESTAMPTZ,
        created_at                TIMESTAMPTZ DEFAULT NOW(),
        updated_at                TIMESTAMPTZ DEFAULT NOW()
      )
    `);

    await client.query(`CREATE INDEX IF NOT EXISTS idx_orders_converter  ON orders(converter_buyer_id)`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_orders_processor  ON orders(processor_buyer_id)`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_orders_status     ON orders(status)`);

    // Auto-generate order_number on insert
    await client.query(`
      CREATE OR REPLACE FUNCTION generate_order_number()
      RETURNS TRIGGER AS $$
      BEGIN
        NEW.order_number := 'ORD-' || LPAD(NEW.id::TEXT, 5, '0');
        RETURN NEW;
      END;
      $$ LANGUAGE plpgsql
    `);

    await client.query(`DROP TRIGGER IF EXISTS set_order_number ON orders`);

    await client.query(`
      CREATE TRIGGER set_order_number
        BEFORE INSERT ON orders
        FOR EACH ROW
        WHEN (NEW.order_number IS NULL)
        EXECUTE FUNCTION generate_order_number()
    `);

  }
};
