module.exports = {
  name: 'create_aggregator_registration_requests',
  up: async (client) => {
    await client.query(`
      CREATE TABLE IF NOT EXISTS aggregator_registration_requests (
        id                       SERIAL PRIMARY KEY,
        phone                    VARCHAR(50) NOT NULL,
        name                     VARCHAR(255) NOT NULL,
        company                  VARCHAR(255),
        city                     VARCHAR(100) NOT NULL,
        region                   VARCHAR(100),
        country                  VARCHAR(100) DEFAULT 'Ghana',
        status                   VARCHAR(20) NOT NULL DEFAULT 'pending'
                                 CHECK (status IN ('pending','code_issued','completed','rejected','expired','code_failed')),
        code_hash                VARCHAR(64),
        code_expires_at          TIMESTAMPTZ,
        code_attempts_remaining  SMALLINT DEFAULT 3,
        approved_by_admin_email  VARCHAR(255),
        approved_at              TIMESTAMPTZ,
        rejected_by_admin_email  VARCHAR(255),
        rejected_at              TIMESTAMPTZ,
        rejection_reason         TEXT,
        aggregator_id            INTEGER REFERENCES aggregators(id) ON DELETE SET NULL,
        source                   VARCHAR(20) DEFAULT 'ussd',
        created_at               TIMESTAMPTZ DEFAULT NOW(),
        updated_at               TIMESTAMPTZ DEFAULT NOW()
      )
    `);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_agg_reg_phone ON aggregator_registration_requests (phone)`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_agg_reg_status ON aggregator_registration_requests (status)`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_agg_reg_created ON aggregator_registration_requests (created_at DESC)`);
    // Partial unique: at most one active request (pending or code_issued) per phone
    await client.query(`
      CREATE UNIQUE INDEX IF NOT EXISTS idx_agg_reg_active
      ON aggregator_registration_requests (phone)
      WHERE status IN ('pending', 'code_issued')
    `);
  },
  down: async (client) => {
    await client.query(`DROP TABLE IF EXISTS aggregator_registration_requests CASCADE`);
  }
};
