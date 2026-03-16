/**
 * Add payment tracking fields to transactions.
 * Supports Mobile Money payments (MTN MoMo, Vodafone Cash).
 */
module.exports = {
  name: 'add_payment_tracking',
  up: async (client) => {
    // Payment status on transactions
    await client.query(`
      ALTER TABLE transactions
      ADD COLUMN IF NOT EXISTS payment_status VARCHAR(20) DEFAULT 'unpaid'
        CHECK (payment_status IN ('unpaid', 'pending', 'paid', 'failed')),
      ADD COLUMN IF NOT EXISTS payment_method VARCHAR(30),
      ADD COLUMN IF NOT EXISTS payment_phone VARCHAR(50),
      ADD COLUMN IF NOT EXISTS payment_reference VARCHAR(255),
      ADD COLUMN IF NOT EXISTS payment_initiated_at TIMESTAMPTZ,
      ADD COLUMN IF NOT EXISTS payment_completed_at TIMESTAMPTZ
    `);

    // Payment history table for audit trail
    await client.query(`
      CREATE TABLE IF NOT EXISTS payments (
        id SERIAL PRIMARY KEY,
        transaction_id INTEGER REFERENCES transactions(id) NOT NULL,
        collector_id INTEGER REFERENCES collectors(id) NOT NULL,
        operator_id INTEGER REFERENCES operators(id),
        amount DECIMAL(10,2) NOT NULL,
        currency VARCHAR(3) DEFAULT 'GHS',
        phone VARCHAR(50) NOT NULL,
        provider VARCHAR(30) NOT NULL,
        status VARCHAR(20) DEFAULT 'pending' CHECK (status IN ('pending', 'processing', 'success', 'failed')),
        reference VARCHAR(255),
        provider_reference VARCHAR(255),
        error_message TEXT,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        updated_at TIMESTAMPTZ DEFAULT NOW()
      )
    `);

    await client.query(`
      CREATE INDEX IF NOT EXISTS idx_payments_transaction_id ON payments(transaction_id)
    `);
    await client.query(`
      CREATE INDEX IF NOT EXISTS idx_payments_collector_id ON payments(collector_id)
    `);
  }
};
