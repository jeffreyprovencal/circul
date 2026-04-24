module.exports = {
  name: 'create_pin_reset_codes',
  up: async (client) => {
    await client.query(`
      CREATE TABLE IF NOT EXISTS pin_reset_codes (
        id SERIAL PRIMARY KEY,
        phone VARCHAR(50) NOT NULL,
        user_type VARCHAR(20) NOT NULL CHECK (user_type IN ('collector','aggregator','agent')),
        user_id INTEGER NOT NULL,
        code_hash VARCHAR(64) NOT NULL,
        expires_at TIMESTAMPTZ NOT NULL,
        used_at TIMESTAMPTZ,
        attempts_remaining SMALLINT NOT NULL DEFAULT 3,
        created_at TIMESTAMPTZ DEFAULT NOW()
      )
    `);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_pin_reset_codes_phone ON pin_reset_codes (phone)`);
    // Partial unique index: one active (unused) reset per phone at a time.
    // Callers must check expires_at > NOW() themselves — NOW() is not immutable.
    await client.query(`
      CREATE UNIQUE INDEX IF NOT EXISTS idx_pin_reset_codes_active
      ON pin_reset_codes (phone)
      WHERE used_at IS NULL
    `);
  },
  down: async (client) => {
    await client.query(`DROP TABLE IF EXISTS pin_reset_codes CASCADE`);
  }
};
