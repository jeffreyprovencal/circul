/**
 * Migration: create_expense_tables
 *
 * Creates expense_categories (with status workflow for suggest/approve/reject)
 * and expense_entries (aggregator expense tracking for P&L).
 * Seeds 7 default categories.
 *
 * Idempotent: uses IF NOT EXISTS.
 */
module.exports = {
  name: 'create_expense_tables',
  up: async (client) => {
    await client.query(`
      CREATE TABLE IF NOT EXISTS expense_categories (
        id               SERIAL PRIMARY KEY,
        name             TEXT NOT NULL,
        status           TEXT NOT NULL DEFAULT 'default',
        suggested_by     INTEGER,
        rejection_reason TEXT,
        reviewed_at      TIMESTAMPTZ,
        created_at       TIMESTAMPTZ DEFAULT NOW()
      )
    `);

    await client.query(`
      CREATE TABLE IF NOT EXISTS expense_entries (
        id               SERIAL PRIMARY KEY,
        aggregator_id    INTEGER NOT NULL REFERENCES aggregators(id),
        category_id      INTEGER NOT NULL REFERENCES expense_categories(id),
        amount           NUMERIC(12,2) NOT NULL,
        note             TEXT,
        receipt_url      TEXT,
        expense_date     DATE NOT NULL DEFAULT CURRENT_DATE,
        created_at       TIMESTAMPTZ DEFAULT NOW()
      )
    `);

    // Index for aggregator lookups
    await client.query(`
      CREATE INDEX IF NOT EXISTS idx_expense_entries_aggregator ON expense_entries (aggregator_id)
    `);

    // Seed default categories (idempotent — skip if any defaults already exist)
    const existing = await client.query(
      `SELECT COUNT(*) AS count FROM expense_categories WHERE status = 'default'`
    );
    if (parseInt(existing.rows[0].count) === 0) {
      await client.query(`
        INSERT INTO expense_categories (name, status) VALUES
          ('Transportation', 'default'),
          ('Fuel', 'default'),
          ('Storage', 'default'),
          ('Labour', 'default'),
          ('Equipment', 'default'),
          ('Maintenance', 'default'),
          ('Mobile money fees', 'default')
      `);
    }
  },
  down: async (client) => {
    await client.query(`DROP TABLE IF EXISTS expense_entries`);
    await client.query(`DROP TABLE IF EXISTS expense_categories`);
  }
};
