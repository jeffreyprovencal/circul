/**
 * Migration: add_recycler_tier
 *
 * Adds the Recycler tier between Processor and Converter.
 * - Creates `recyclers` table (mirrors processors structure)
 * - Adds `recycler_id` FK to `pending_transactions`
 * - Expands `poster_type` CHECK on `posted_prices` to include 'recycler'
 * - Seeds poly@circul.demo as a recycler
 * - Cleans up test rows (aggregator 16, processor 5)
 *
 * Idempotent: uses IF NOT EXISTS / IF EXISTS throughout.
 */
const crypto = require('crypto');
const util   = require('util');
const scrypt  = util.promisify(crypto.scrypt);

async function hashPwd(password) {
  const salt = crypto.randomBytes(16).toString('hex');
  const key  = await scrypt(password, salt, 64);
  return salt + ':' + key.toString('hex');
}

module.exports = {
  name: 'add_recycler_tier',
  up: async (client) => {
    const demoHash = await hashPwd('demo1234');

    // ── 1. Create recyclers table (mirrors processors) ─────────────────
    await client.query(`
      CREATE TABLE IF NOT EXISTS recyclers (
        id                  SERIAL PRIMARY KEY,
        name                VARCHAR(255) NOT NULL,
        company             VARCHAR(255) NOT NULL,
        email               VARCHAR(255) UNIQUE NOT NULL,
        password_hash       VARCHAR(255),
        phone               VARCHAR(50),
        city                VARCHAR(100),
        region              VARCHAR(100),
        country             VARCHAR(100) DEFAULT 'Ghana',
        is_active           BOOLEAN DEFAULT true,
        is_flagged          BOOLEAN DEFAULT false,
        created_at          TIMESTAMPTZ DEFAULT NOW()
      );
    `);

    // ── 2. Add recycler_id column to pending_transactions ──────────────
    const colCheck = await client.query(
      `SELECT column_name FROM information_schema.columns
       WHERE table_name='pending_transactions' AND column_name='recycler_id'`
    );
    if (!colCheck.rows.length) {
      await client.query(`
        ALTER TABLE pending_transactions
        ADD COLUMN recycler_id INTEGER REFERENCES recyclers(id) ON DELETE SET NULL
      `);
    }

    // ── 3. Expand poster_type CHECK to include 'recycler' ──────────────
    // Drop existing constraint and recreate with expanded values
    await client.query(`
      ALTER TABLE posted_prices
      DROP CONSTRAINT IF EXISTS posted_prices_poster_type_check
    `);
    await client.query(`
      ALTER TABLE posted_prices
      ADD CONSTRAINT posted_prices_poster_type_check
      CHECK (poster_type IN ('aggregator','processor','recycler','converter'))
    `);

    // ── 4. Seed poly@circul.demo as a recycler ─────────────────────────
    await client.query(
      `INSERT INTO recyclers (name, company, email, password_hash, city, region, country)
       VALUES ($1, $2, $3, $4, $5, $6, $7)
       ON CONFLICT (email) DO NOTHING`,
      ['Poly Recycling AG', 'Poly Recycling AG', 'poly@circul.demo', demoHash, 'Aarau', null, 'Switzerland']
    );

    // Remove poly from converters (it's a recycler, not a converter)
    await client.query(`DELETE FROM converters WHERE email = 'poly@circul.demo'`);

    // ── 5. DB cleanup: remove test rows ────────────────────────────────
    await client.query(`DELETE FROM posted_prices WHERE poster_type = 'aggregator' AND poster_id = 16`);
    await client.query(`DELETE FROM posted_prices WHERE poster_type = 'processor'  AND poster_id = 5`);
    await client.query(`DELETE FROM aggregators WHERE id = 16`);
    await client.query(`DELETE FROM processors WHERE id = 5`);

    // ── 6. Seed demo pending_transactions for recycler ─────────────────
    // Processor 1 (rePATRN) sells to Poly (recycler)
    const recyclerResult = await client.query(`SELECT id FROM recyclers WHERE email = 'poly@circul.demo'`);
    if (recyclerResult.rows.length) {
      const recycler_id = recyclerResult.rows[0].id;
      await client.query(`
        INSERT INTO pending_transactions
          (transaction_type, status, processor_id, recycler_id, material_type,
           gross_weight_kg, price_per_kg, total_price, grade,
           dispatch_approved, dispatch_approved_at, created_at, updated_at)
        VALUES
          ('processor_sale', 'completed', 1, $1, 'PET', 750.00, 5.00, 3750.00, 'A',
           true, NOW() - INTERVAL '5 days', NOW() - INTERVAL '7 days', NOW() - INTERVAL '5 days'),
          ('processor_sale', 'arrived', 1, $1, 'HDPE', 320.00, 4.50, 1440.00, 'A',
           true, NOW() - INTERVAL '1 day', NOW() - INTERVAL '2 days', NOW() - INTERVAL '1 day')
      `, [recycler_id]);
    }
  }
};
