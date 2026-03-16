/**
 * Create operators table for role-based dashboard access.
 * Operators are buyers/processors who log in to see their transactions.
 * Admins see all transactions across all operators.
 */
module.exports = {
  name: 'create_operators',
  up: async (client) => {
    // Operators table
    await client.query(`
      CREATE TABLE IF NOT EXISTS operators (
        id SERIAL PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        company VARCHAR(255),
        phone VARCHAR(50) UNIQUE,
        pin VARCHAR(6) NOT NULL,
        role VARCHAR(20) DEFAULT 'operator' CHECK (role IN ('operator', 'admin')),
        is_active BOOLEAN DEFAULT true,
        created_at TIMESTAMPTZ DEFAULT NOW()
      )
    `);

    // Link transactions to operators (who recorded/received the transaction)
    await client.query(`
      ALTER TABLE transactions ADD COLUMN IF NOT EXISTS operator_id INTEGER REFERENCES operators(id)
    `);

    // Index for fast operator-scoped queries
    await client.query(`
      CREATE INDEX IF NOT EXISTS idx_transactions_operator_id ON transactions(operator_id)
    `);

    // Seed default admin account (PIN: 1234)
    await client.query(`
      INSERT INTO operators (name, company, phone, pin, role)
      VALUES ('Admin', 'Circul', '0000000000', '1234', 'admin')
      ON CONFLICT (phone) DO NOTHING
    `);

    // Seed Miniplast demo operator (PIN: 2024)
    await client.query(`
      INSERT INTO operators (name, company, phone, pin, role)
      VALUES ('Miniplast', 'Miniplast Ghana', '0200000000', '2024', 'operator')
      ON CONFLICT (phone) DO NOTHING
    `);
  }
};
