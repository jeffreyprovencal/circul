/**
 * Expand schema for multi-role platform:
 * - Expand operators.role to include collector, aggregator, processor, converter
 * - Add location fields to operators and collectors
 * - Create posted_prices table (self-posted by aggregators/processors)
 * - Rebuild ratings table to support all role pairs
 * - Create collector_passports table
 * - Seed demo operators for all roles
 * - Seed demo collectors
 */

module.exports = {
  name: 'expand_schema',
  up: async (client) => {

    // 1. Expand operators.role constraint
    await client.query(`ALTER TABLE operators DROP CONSTRAINT IF EXISTS operators_role_check`);
    await client.query(`ALTER TABLE operators ADD CONSTRAINT operators_role_check CHECK (role IN ('operator','admin','collector','aggregator','processor','converter'))`);

    // 2. Add location and flag fields to operators
    await client.query(`ALTER TABLE operators ADD COLUMN IF NOT EXISTS city VARCHAR(100)`);
    await client.query(`ALTER TABLE operators ADD COLUMN IF NOT EXISTS region VARCHAR(100)`);
    await client.query(`ALTER TABLE operators ADD COLUMN IF NOT EXISTS country VARCHAR(100) DEFAULT 'Ghana'`);
    await client.query(`ALTER TABLE operators ADD COLUMN IF NOT EXISTS is_flagged BOOLEAN DEFAULT false`);

    // 3. Add location fields to collectors
    await client.query(`ALTER TABLE collectors ADD COLUMN IF NOT EXISTS city VARCHAR(100)`);

    // 4. Create posted_prices table
    await client.query(`
      CREATE TABLE IF NOT EXISTS posted_prices (
        id SERIAL PRIMARY KEY,
        operator_id INTEGER NOT NULL REFERENCES operators(id) ON DELETE CASCADE,
        material_type VARCHAR(10) NOT NULL CHECK (material_type IN ('PET','HDPE','LDPE','PP')),
        price_per_kg_usd NUMERIC(10,4) NOT NULL CHECK (price_per_kg_usd >= 0),
        price_per_kg_ghs NUMERIC(10,2),
        usd_to_ghs_rate NUMERIC(10,4),
        city VARCHAR(100),
        region VARCHAR(100),
        country VARCHAR(100) DEFAULT 'Ghana',
        expires_at TIMESTAMPTZ NOT NULL,
        is_active BOOLEAN DEFAULT true,
        posted_at TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE(operator_id, material_type, expires_at)
      )
    `);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_posted_prices_material ON posted_prices(material_type)`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_posted_prices_city ON posted_prices(city)`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_posted_prices_expires ON posted_prices(expires_at)`);

    // 5. Rebuild ratings table to support all role pairs
    await client.query(`DROP TABLE IF EXISTS ratings CASCADE`);
    await client.query(`
      CREATE TABLE IF NOT EXISTS ratings (
        id SERIAL PRIMARY KEY,
        transaction_id INTEGER REFERENCES transactions(id) ON DELETE CASCADE,
        rater_operator_id INTEGER REFERENCES operators(id) ON DELETE SET NULL,
        rated_operator_id INTEGER REFERENCES operators(id) ON DELETE CASCADE,
        rater_collector_id INTEGER REFERENCES collectors(id) ON DELETE SET NULL,
        rated_collector_id INTEGER REFERENCES collectors(id) ON DELETE CASCADE,
        rating INTEGER NOT NULL CHECK (rating >= 1 AND rating <= 5),
        tags JSONB DEFAULT '[]'::jsonb,
        notes TEXT,
        rating_direction VARCHAR(50),
        created_at TIMESTAMPTZ DEFAULT NOW(),
        window_expires_at TIMESTAMPTZ,
        CONSTRAINT one_rating_per_transaction_direction UNIQUE (transaction_id, rater_operator_id, rated_operator_id)
      )
    `);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_ratings_rated_operator ON ratings(rated_operator_id)`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_ratings_rater_operator ON ratings(rater_operator_id)`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_ratings_transaction ON ratings(transaction_id)`);

    // 6. Create collector_passports table
    await client.query(`
      CREATE TABLE IF NOT EXISTS collector_passports (
        collector_id INTEGER PRIMARY KEY REFERENCES collectors(id) ON DELETE CASCADE,
        total_kg_lifetime NUMERIC(12,2) DEFAULT 0,
        total_kg_last_12m NUMERIC(12,2) DEFAULT 0,
        total_earned_ghs NUMERIC(12,2) DEFAULT 0,
        total_earned_usd NUMERIC(12,2) DEFAULT 0,
        transaction_count INTEGER DEFAULT 0,
        active_since TIMESTAMPTZ,
        material_breakdown JSONB DEFAULT '{}'::jsonb,
        unique_aggregators INTEGER DEFAULT 0,
        avg_rating_from_aggregators NUMERIC(3,2),
        payment_reliability_score NUMERIC(5,2),
        last_updated TIMESTAMPTZ DEFAULT NOW()
      )
    `);

    // 7. Seed demo operators for all roles
    // Demo Collector (phone: 0300000001, PIN: 1111)
    await client.query(`
      INSERT INTO operators (name, company, phone, pin, role, city, region, country)
      VALUES ('Ama Mensah', 'Independent', '0300000001', '1111', 'collector', 'Accra', 'Greater Accra', 'Ghana')
      ON CONFLICT (phone) DO UPDATE SET role='collector', pin='1111', city='Accra', region='Greater Accra'
    `);

    // Demo Aggregator (phone: 0300000002, PIN: 2222)
    await client.query(`
      INSERT INTO operators (name, company, phone, pin, role, city, region, country)
      VALUES ('Green Collect GH', 'Green Collect Ghana Ltd', '0300000002', '2222', 'aggregator', 'Accra', 'Greater Accra', 'Ghana')
      ON CONFLICT (phone) DO UPDATE SET role='aggregator', pin='2222', city='Accra', region='Greater Accra'
    `);

    // Demo Processor = rePATRN (phone: 0300000003, PIN: 3333)
    await client.query(`
      INSERT INTO operators (name, company, phone, pin, role, city, region, country)
      VALUES ('rePATRN', 'rePATRN Ghana', '0300000003', '3333', 'processor', 'Tema', 'Greater Accra', 'Ghana')
      ON CONFLICT (phone) DO UPDATE SET role='processor', pin='3333', city='Tema', region='Greater Accra'
    `);

    // Demo Converter (phone: 0300000004, PIN: 4444)
    await client.query(`
      INSERT INTO operators (name, company, phone, pin, role, city, region, country)
      VALUES ('EcoPlast GH', 'EcoPlast Ghana Ltd', '0300000004', '4444', 'converter', 'Tema', 'Greater Accra', 'Ghana')
      ON CONFLICT (phone) DO UPDATE SET role='converter', pin='4444', city='Tema', region='Greater Accra'
    `);

    // 8. Seed demo collectors
    await client.query(`
      INSERT INTO collectors (first_name, last_name, phone, pin, region, city)
      VALUES
        ('Kwame', 'Asante', '0241000001', '0000', 'Greater Accra', 'Accra'),
        ('Abena', 'Boateng', '0241000002', '0000', 'Greater Accra', 'Accra'),
        ('Kofi', 'Darko', '0241000003', '0000', 'Greater Accra', 'Accra'),
        ('Akosua', 'Essien', '0241000004', '0000', 'Greater Accra', 'Accra'),
        ('Yaw', 'Frimpong', '0241000005', '0000', 'Greater Accra', 'Accra')
      ON CONFLICT (phone) DO NOTHING
    `);

    // 9. posted_prices are seeded via the app API after operator creation
  }
};
