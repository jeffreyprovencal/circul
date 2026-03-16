module.exports = {
  name: 'create_pickers_and_collections',
  up: async (client) => {
    // Pickers table - waste collectors who log materials
    await client.query(`
      CREATE TABLE IF NOT EXISTS pickers (
        id SERIAL PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        phone VARCHAR(50),
        pin VARCHAR(6) NOT NULL,
        region VARCHAR(255),
        is_active BOOLEAN DEFAULT true,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        updated_at TIMESTAMPTZ DEFAULT NOW()
      )
    `);

    await client.query(`
      CREATE UNIQUE INDEX IF NOT EXISTS pickers_phone_unique_idx ON pickers (phone) WHERE phone IS NOT NULL
    `);

    // Collections table - each logged collection event
    await client.query(`
      CREATE TABLE IF NOT EXISTS collections (
        id SERIAL PRIMARY KEY,
        picker_id INTEGER NOT NULL REFERENCES pickers(id) ON DELETE CASCADE,
        material_type VARCHAR(50) NOT NULL,
        weight_kg DECIMAL(10,2) NOT NULL CHECK (weight_kg > 0),
        lat DECIMAL(10,7),
        lng DECIMAL(10,7),
        notes TEXT,
        created_at TIMESTAMPTZ DEFAULT NOW()
      )
    `);

    // Indexes for common queries
    await client.query(`CREATE INDEX IF NOT EXISTS idx_collections_picker_id ON collections (picker_id)`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_collections_material_type ON collections (material_type)`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_collections_created_at ON collections (created_at DESC)`);
  }
};
