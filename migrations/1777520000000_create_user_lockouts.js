module.exports = {
  name: 'create_user_lockouts',
  up: async (client) => {
    await client.query(`
      CREATE TABLE IF NOT EXISTS user_lockouts (
        id SERIAL PRIMARY KEY,
        user_type VARCHAR(20) NOT NULL CHECK (user_type IN ('collector','aggregator','agent')),
        user_id INTEGER NOT NULL,
        phone VARCHAR(50) NOT NULL,
        locked_until TIMESTAMPTZ NOT NULL,
        reason VARCHAR(50) NOT NULL CHECK (reason IN ('wrong_pin_x3','wrong_otp_x3')),
        created_at TIMESTAMPTZ DEFAULT NOW()
      )
    `);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_user_lockouts_phone_until ON user_lockouts (phone, locked_until)`);
  },
  down: async (client) => {
    await client.query(`DROP TABLE IF EXISTS user_lockouts CASCADE`);
  }
};
