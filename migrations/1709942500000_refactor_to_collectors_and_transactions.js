module.exports = {
  name: 'refactor_to_collectors_and_transactions',
  up: async (client) => {
    // ==================================================
    // PHASE 1: Rename pickers → collectors
    // ==================================================

    // Rename table
    await client.query(`ALTER TABLE pickers RENAME TO collectors`);

    // Split name into first_name and last_name
    await client.query(`
      ALTER TABLE collectors
      ADD COLUMN first_name VARCHAR(255),
      ADD COLUMN last_name VARCHAR(255)
    `);

    // Migrate existing name data (split by first space)
    await client.query(`
      UPDATE collectors
      SET
        first_name = SPLIT_PART(name, ' ', 1),
        last_name = CASE
          WHEN POSITION(' ' IN name) > 0 THEN SUBSTRING(name FROM POSITION(' ' IN name) + 1)
          ELSE ''
        END
      WHERE name IS NOT NULL
    `);

    // Make first_name required after migration
    await client.query(`ALTER TABLE collectors ALTER COLUMN first_name SET NOT NULL`);

    // Drop old name column
    await client.query(`ALTER TABLE collectors DROP COLUMN name`);

    // Add average_rating column for denormalized rating
    await client.query(`
      ALTER TABLE collectors
      ADD COLUMN average_rating DECIMAL(3,2) DEFAULT NULL
    `);

    // Rename foreign key in collections table
    await client.query(`ALTER TABLE collections RENAME COLUMN picker_id TO collector_id`);

    // Recreate the foreign key constraint with new name
    await client.query(`
      ALTER TABLE collections
      DROP CONSTRAINT IF EXISTS collections_picker_id_fkey
    `);
    await client.query(`
      ALTER TABLE collections
      ADD CONSTRAINT collections_collector_id_fkey
      FOREIGN KEY (collector_id) REFERENCES collectors(id) ON DELETE CASCADE
    `);

    // Rename index
    await client.query(`
      DROP INDEX IF EXISTS idx_collections_picker_id
    `);
    await client.query(`
      CREATE INDEX idx_collections_collector_id ON collections (collector_id)
    `);

    // ==================================================
    // PHASE 2: Create transactions table
    // ==================================================

    await client.query(`
      CREATE TABLE IF NOT EXISTS transactions (
        id SERIAL PRIMARY KEY,
        collector_id INTEGER NOT NULL REFERENCES collectors(id) ON DELETE CASCADE,
        buyer_id INTEGER REFERENCES collectors(id) ON DELETE SET NULL,
        material_type VARCHAR(50) NOT NULL,
        gross_weight_kg DECIMAL(10,2) NOT NULL CHECK (gross_weight_kg > 0),
        net_weight_kg DECIMAL(10,2) NOT NULL CHECK (net_weight_kg > 0),
        contamination_deduction_percent DECIMAL(5,2) DEFAULT 0 CHECK (contamination_deduction_percent >= 0 AND contamination_deduction_percent <= 100),
        contamination_types JSONB DEFAULT '[]'::jsonb,
        quality_notes TEXT,
        price_per_kg DECIMAL(10,2),
        total_price DECIMAL(10,2),
        lat DECIMAL(10,7),
        lng DECIMAL(10,7),
        notes TEXT,
        transaction_date TIMESTAMPTZ DEFAULT NOW(),
        created_at TIMESTAMPTZ DEFAULT NOW(),
        updated_at TIMESTAMPTZ DEFAULT NOW()
      )
    `);

    // Create indexes for transactions
    await client.query(`CREATE INDEX idx_transactions_collector_id ON transactions (collector_id)`);
    await client.query(`CREATE INDEX idx_transactions_buyer_id ON transactions (buyer_id)`);
    await client.query(`CREATE INDEX idx_transactions_material_type ON transactions (material_type)`);
    await client.query(`CREATE INDEX idx_transactions_transaction_date ON transactions (transaction_date DESC)`);
    await client.query(`CREATE INDEX idx_transactions_created_at ON transactions (created_at DESC)`);

    // ==================================================
    // PHASE 3: Create ratings table
    // ==================================================

    await client.query(`
      CREATE TABLE IF NOT EXISTS ratings (
        id SERIAL PRIMARY KEY,
        transaction_id INTEGER REFERENCES transactions(id) ON DELETE CASCADE,
        collector_id INTEGER NOT NULL REFERENCES collectors(id) ON DELETE CASCADE,
        buyer_id INTEGER REFERENCES collectors(id) ON DELETE SET NULL,
        rating INTEGER NOT NULL CHECK (rating >= 1 AND rating <= 5),
        notes TEXT,
        created_at TIMESTAMPTZ DEFAULT NOW()
      )
    `);

    // Create indexes for ratings
    await client.query(`CREATE INDEX idx_ratings_collector_id ON ratings (collector_id)`);
    await client.query(`CREATE INDEX idx_ratings_transaction_id ON ratings (transaction_id)`);
    await client.query(`CREATE INDEX idx_ratings_created_at ON ratings (created_at DESC)`);

    // Create unique constraint: one rating per transaction
    await client.query(`
      CREATE UNIQUE INDEX idx_ratings_transaction_unique ON ratings (transaction_id)
      WHERE transaction_id IS NOT NULL
    `);

    // ==================================================
    // PHASE 4: Create trigger to update average_rating
    // ==================================================

    await client.query(`
      CREATE OR REPLACE FUNCTION update_collector_average_rating()
      RETURNS TRIGGER AS $$
      BEGIN
        UPDATE collectors
        SET average_rating = (
          SELECT AVG(rating)::DECIMAL(3,2)
          FROM ratings
          WHERE collector_id = COALESCE(NEW.collector_id, OLD.collector_id)
        )
        WHERE id = COALESCE(NEW.collector_id, OLD.collector_id);
        RETURN NEW;
      END;
      $$ LANGUAGE plpgsql;
    `);

    await client.query(`
      CREATE TRIGGER trigger_update_collector_rating
      AFTER INSERT OR UPDATE OR DELETE ON ratings
      FOR EACH ROW
      EXECUTE FUNCTION update_collector_average_rating();
    `);

    // ==================================================
    // PHASE 5: Migrate existing collections to transactions
    // ==================================================

    // Copy all existing collections to transactions table
    // (Keep collections table for backward compatibility if needed)
    await client.query(`
      INSERT INTO transactions (
        collector_id,
        material_type,
        gross_weight_kg,
        net_weight_kg,
        lat,
        lng,
        notes,
        transaction_date,
        created_at
      )
      SELECT
        collector_id,
        material_type,
        weight_kg as gross_weight_kg,
        weight_kg as net_weight_kg,
        lat,
        lng,
        notes,
        created_at as transaction_date,
        created_at
      FROM collections
    `);
  },

  down: async (client) => {
    // Drop triggers and functions
    await client.query(`DROP TRIGGER IF EXISTS trigger_update_collector_rating ON ratings`);
    await client.query(`DROP FUNCTION IF EXISTS update_collector_average_rating()`);

    // Drop new tables
    await client.query(`DROP TABLE IF EXISTS ratings CASCADE`);
    await client.query(`DROP TABLE IF EXISTS transactions CASCADE`);

    // Restore foreign key name in collections
    await client.query(`ALTER TABLE collections RENAME COLUMN collector_id TO picker_id`);

    // Restore original name column
    await client.query(`ALTER TABLE collectors ADD COLUMN name VARCHAR(255)`);
    await client.query(`
      UPDATE collectors
      SET name = CONCAT(first_name, ' ', COALESCE(last_name, ''))
      WHERE first_name IS NOT NULL
    `);
    await client.query(`ALTER TABLE collectors ALTER COLUMN name SET NOT NULL`);

    // Drop new columns
    await client.query(`ALTER TABLE collectors DROP COLUMN IF EXISTS first_name`);
    await client.query(`ALTER TABLE collectors DROP COLUMN IF EXISTS last_name`);
    await client.query(`ALTER TABLE collectors DROP COLUMN IF EXISTS average_rating`);

    // Rename table back
    await client.query(`ALTER TABLE collectors RENAME TO pickers`);

    // Restore indexes
    await client.query(`DROP INDEX IF EXISTS idx_collections_collector_id`);
    await client.query(`CREATE INDEX idx_collections_picker_id ON collections (picker_id)`);
  }
};
