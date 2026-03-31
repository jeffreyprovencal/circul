exports.up = async (pool) => {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS error_log (
      id SERIAL PRIMARY KEY,
      source VARCHAR(20) NOT NULL DEFAULT 'server',
      dashboard VARCHAR(30),
      error_message TEXT NOT NULL,
      error_stack TEXT,
      url VARCHAR(500),
      user_id INTEGER,
      user_role VARCHAR(20),
      created_at TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE INDEX IF NOT EXISTS idx_error_log_created ON error_log (created_at DESC);
    CREATE INDEX IF NOT EXISTS idx_error_log_source ON error_log (source, created_at DESC);
  `);
};

exports.down = async (pool) => {
  await pool.query(`DROP TABLE IF EXISTS error_log;`);
};
