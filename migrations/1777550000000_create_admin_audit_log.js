module.exports = {
  name: 'create_admin_audit_log',
  up: async (client) => {
    await client.query(`
      CREATE TABLE IF NOT EXISTS admin_audit_log (
        id SERIAL PRIMARY KEY,
        actor_type VARCHAR(20) NOT NULL,
        actor_id INTEGER,
        actor_email VARCHAR(255),
        action VARCHAR(50) NOT NULL,
        target_type VARCHAR(20) NOT NULL,
        target_id INTEGER NOT NULL,
        details JSONB DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ DEFAULT NOW()
      )
    `);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_admin_audit_actor ON admin_audit_log (actor_id, created_at DESC)`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_admin_audit_target ON admin_audit_log (target_type, target_id, created_at DESC)`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_admin_audit_created ON admin_audit_log (created_at DESC)`);
  },
  down: async (client) => {
    await client.query(`DROP TABLE IF EXISTS admin_audit_log CASCADE`);
  }
};
