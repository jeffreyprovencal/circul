module.exports = {
  name: 'create_phone_change_codes',
  up: async (client) => {
    await client.query(`
      CREATE TABLE IF NOT EXISTS phone_change_codes (
        id SERIAL PRIMARY KEY,
        user_type VARCHAR(20) NOT NULL CHECK (user_type IN ('collector','aggregator','agent')),
        user_id INTEGER NOT NULL,
        old_phone VARCHAR(50) NOT NULL,
        new_phone VARCHAR(50) NOT NULL,
        code_hash VARCHAR(64) NOT NULL,
        expires_at TIMESTAMPTZ NOT NULL,
        used_at TIMESTAMPTZ,
        attempts_remaining SMALLINT NOT NULL DEFAULT 3,
        initiated_by_admin_email VARCHAR(255) NOT NULL,
        created_at TIMESTAMPTZ DEFAULT NOW()
      )
    `);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_phone_change_codes_user ON phone_change_codes (user_type, user_id)`);
    await client.query(`
      CREATE UNIQUE INDEX IF NOT EXISTS idx_phone_change_codes_active
      ON phone_change_codes (user_type, user_id)
      WHERE used_at IS NULL
    `);
  },
  down: async (client) => {
    await client.query(`DROP TABLE IF EXISTS phone_change_codes CASCADE`);
  }
};
