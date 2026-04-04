module.exports = {
  name: 'batch8b_and_batch9',
  up: async (client) => {

    // ── BATCH 8b: Ghana Card (collectors, aggregators, agents) ──

    await client.query(`ALTER TABLE collectors ADD COLUMN IF NOT EXISTS ghana_card TEXT`);
    await client.query(`ALTER TABLE collectors ADD COLUMN IF NOT EXISTS ghana_card_photo TEXT`);
    await client.query(`ALTER TABLE aggregators ADD COLUMN IF NOT EXISTS ghana_card TEXT`);
    await client.query(`ALTER TABLE aggregators ADD COLUMN IF NOT EXISTS ghana_card_photo TEXT`);

    // ── BATCH 8b: Agents table ──

    await client.query(`
      CREATE TABLE IF NOT EXISTS agents (
        id SERIAL PRIMARY KEY,
        aggregator_id INTEGER NOT NULL REFERENCES aggregators(id),
        first_name TEXT NOT NULL,
        last_name TEXT NOT NULL,
        phone TEXT UNIQUE NOT NULL,
        pin TEXT NOT NULL,
        ghana_card TEXT,
        ghana_card_photo TEXT,
        city TEXT,
        region TEXT,
        is_active BOOLEAN DEFAULT true,
        must_change_pin BOOLEAN DEFAULT false,
        created_at TIMESTAMPTZ DEFAULT NOW()
      )
    `);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_agents_aggregator ON agents(aggregator_id)`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_agents_phone ON agents(phone)`);

    // ── BATCH 8b: Agent activity log ──

    await client.query(`
      CREATE TABLE IF NOT EXISTS agent_activity (
        id SERIAL PRIMARY KEY,
        agent_id INTEGER NOT NULL REFERENCES agents(id),
        aggregator_id INTEGER NOT NULL REFERENCES aggregators(id),
        action_type TEXT NOT NULL,
        description TEXT,
        related_id INTEGER,
        related_type TEXT,
        created_at TIMESTAMPTZ DEFAULT NOW()
      )
    `);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_agent_activity_agent ON agent_activity(agent_id)`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_agent_activity_aggregator ON agent_activity(aggregator_id)`);

    // ── BATCH 9: Processor supply requirements ──

    await client.query(`
      CREATE TABLE IF NOT EXISTS supply_requirements (
        id SERIAL PRIMARY KEY,
        processor_id INTEGER NOT NULL REFERENCES processors(id),
        material_type TEXT NOT NULL,
        accepted_forms TEXT[] NOT NULL,
        accepted_colours TEXT[],
        max_contamination_pct NUMERIC(5,2),
        max_moisture_pct NUMERIC(5,2),
        min_quantity_kg NUMERIC(10,2),
        price_premium_pct NUMERIC(5,2),
        client_reference TEXT,
        sorting_notes TEXT,
        is_active BOOLEAN DEFAULT true,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        updated_at TIMESTAMPTZ DEFAULT NOW()
      )
    `);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_supply_req_processor ON supply_requirements(processor_id)`);

    // ── BATCH 9: Spec-linked transactions ──

    await client.query(`ALTER TABLE pending_transactions ADD COLUMN IF NOT EXISTS requirement_id INTEGER REFERENCES supply_requirements(id)`);
    await client.query(`ALTER TABLE pending_transactions ADD COLUMN IF NOT EXISTS spec_compliance TEXT`);

    // ── Seed data for demo ──

    // Demo supply requirements for processor 1 (rePATRN Ghana)
    await client.query(`
      INSERT INTO supply_requirements (processor_id, material_type, accepted_forms, accepted_colours, max_contamination_pct, max_moisture_pct, min_quantity_kg, price_premium_pct, client_reference, sorting_notes)
      VALUES
        (1, 'PET', ARRAY['Loose','Bales'], ARRAY['Clear','Blue'], 3.0, 5.0, 200, 5.0, 'Poly Recycling — PO #PR-2026-041', 'Sort clear and blue PET separately — avoid mixed colours. Rinse if possible.'),
        (1, 'HDPE', ARRAY['Loose','Bales','Flakes'], ARRAY['Natural','White'], 4.0, 6.0, 150, 3.0, 'Miniplast Ghana — PO #MG-2026-017', 'Jerrycans and containers — empty and separate lids.')
      ON CONFLICT DO NOTHING
    `);
  }
};
