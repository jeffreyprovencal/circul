/**
 * Add buyer accounts, buyer prices, and admin users.
 * - admin_users: email+password auth for /admin dashboard
 * - buyers: aggregators/processors created by admin, set prices
 * - buyer_prices: live prices per material per buyer
 * Also seeds demo accounts for WORK partner demo.
 */
const crypto = require('crypto');

function hashPassword(password) {
  return new Promise((resolve, reject) => {
    const salt = crypto.randomBytes(16).toString('hex');
    crypto.scrypt(password, salt, 64, (err, key) => {
      if (err) reject(err);
      else resolve(salt + ':' + key.toString('hex'));
    });
  });
}

module.exports = {
  name: 'add_buyers_and_admin',
  up: async (client) => {
    // Admin users (email + password, separate from operator phone+PIN)
    await client.query(`
      CREATE TABLE IF NOT EXISTS admin_users (
        id SERIAL PRIMARY KEY,
        email VARCHAR(255) UNIQUE NOT NULL,
        password_hash TEXT NOT NULL,
        name VARCHAR(255),
        is_active BOOLEAN DEFAULT true,
        created_at TIMESTAMPTZ DEFAULT NOW()
      )
    `);

    // Buyers: aggregators and processors (created by admin only)
    await client.query(`
      CREATE TABLE IF NOT EXISTS buyers (
        id SERIAL PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        company VARCHAR(255),
        email VARCHAR(255) UNIQUE NOT NULL,
        password_hash TEXT NOT NULL,
        role VARCHAR(20) NOT NULL DEFAULT 'aggregator' CHECK (role IN ('aggregator', 'processor')),
        is_active BOOLEAN DEFAULT true,
        created_at TIMESTAMPTZ DEFAULT NOW()
      )
    `);

    // Buyer prices: each buyer sets their price per material type
    await client.query(`
      CREATE TABLE IF NOT EXISTS buyer_prices (
        id SERIAL PRIMARY KEY,
        buyer_id INTEGER NOT NULL REFERENCES buyers(id) ON DELETE CASCADE,
        material_type VARCHAR(10) NOT NULL CHECK (material_type IN ('PET', 'HDPE', 'LDPE', 'PP')),
        price_per_kg NUMERIC(10,2) NOT NULL,
        updated_at TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE(buyer_id, material_type)
      )
    `);

    await client.query(`
      CREATE INDEX IF NOT EXISTS idx_buyer_prices_material ON buyer_prices(material_type)
    `);

    // Seed admin account: hello@repatrn.com / Circul2026!
    const adminHash = await hashPassword('Circul2026!');
    await client.query(`
      INSERT INTO admin_users (email, password_hash, name)
      VALUES ('hello@repatrn.com', $1, 'Circul Admin')
      ON CONFLICT (email) DO UPDATE SET password_hash = $1
    `, [adminHash]);

    // Seed demo operator: rePATRN, phone 0200000001, PIN 1234
    await client.query(`
      INSERT INTO operators (name, company, phone, pin, role)
      VALUES ('rePATRN', 'rePATRN', '0200000001', '1234', 'operator')
      ON CONFLICT (phone) DO UPDATE SET name = 'rePATRN', company = 'rePATRN', pin = '1234'
    `);
  },

  down: async (client) => {
    await client.query(`DROP TABLE IF EXISTS buyer_prices`);
    await client.query(`DROP TABLE IF EXISTS buyers`);
    await client.query(`DROP TABLE IF EXISTS admin_users`);
  }
};
