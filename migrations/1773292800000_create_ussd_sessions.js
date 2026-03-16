module.exports = {
  name: 'create_ussd_sessions',
  up: async (client) => {
    // USSD session log table for analytics and debugging
    await client.query(`
      CREATE TABLE IF NOT EXISTS ussd_sessions (
        id SERIAL PRIMARY KEY,
        session_id VARCHAR(255) NOT NULL,
        phone VARCHAR(50) NOT NULL,
        service_code VARCHAR(50),
        collector_id INTEGER REFERENCES collectors(id) ON DELETE SET NULL,
        action VARCHAR(50),
        text_input TEXT DEFAULT '',
        response TEXT,
        created_at TIMESTAMPTZ DEFAULT NOW()
      )
    `);

    await client.query(`CREATE INDEX IF NOT EXISTS idx_ussd_sessions_phone ON ussd_sessions (phone)`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_ussd_sessions_session_id ON ussd_sessions (session_id)`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_ussd_sessions_created_at ON ussd_sessions (created_at DESC)`);
  }
};
