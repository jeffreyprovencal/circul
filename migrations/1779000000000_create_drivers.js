module.exports = {
  name: 'create_drivers',
  up: async (client) => {
    // Drivers — standalone platform actors, multi-aggregator via join table.
    // Phone+PIN auth (mirrors collector pattern, NOT sub-account FK to aggregator).
    await client.query(`
      CREATE TABLE IF NOT EXISTS drivers (
        id                SERIAL PRIMARY KEY,
        first_name        TEXT NOT NULL,
        last_name         TEXT NOT NULL,
        phone             TEXT UNIQUE NOT NULL,
        pin               TEXT NOT NULL,
        city              TEXT,
        region            TEXT,
        is_active         BOOLEAN DEFAULT true,
        must_change_pin   BOOLEAN DEFAULT false,
        created_at        TIMESTAMPTZ DEFAULT NOW()
      )
    `);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_drivers_phone ON drivers(phone)`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_drivers_city ON drivers(city) WHERE is_active = true`);

    // Many-to-many: driver works with N aggregators
    await client.query(`
      CREATE TABLE IF NOT EXISTS driver_aggregator_relationships (
        id                    SERIAL PRIMARY KEY,
        driver_id             INTEGER NOT NULL REFERENCES drivers(id) ON DELETE CASCADE,
        aggregator_id         INTEGER NOT NULL REFERENCES aggregators(id) ON DELETE CASCADE,
        status                TEXT NOT NULL DEFAULT 'active',
          -- 'invite_pending' = aggregator invited, driver hasn't claimed
          -- 'active'         = both sides accepted, working together
          -- 'paused'         = relationship temporarily inactive
          -- 'ended'          = relationship terminated
        invite_initiated_by   TEXT NOT NULL DEFAULT 'aggregator',
          -- 'aggregator' = aggregator invited driver
          -- 'driver'     = driver self-joined via marketplace acceptance
        invite_expires_at     TIMESTAMPTZ,
        claimed_at            TIMESTAMPTZ,
        ended_at              TIMESTAMPTZ,
        created_at            TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE (driver_id, aggregator_id)
      )
    `);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_dar_driver ON driver_aggregator_relationships(driver_id) WHERE status IN ('active', 'invite_pending')`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_dar_aggregator ON driver_aggregator_relationships(aggregator_id) WHERE status IN ('active', 'invite_pending')`);

    // Driver activity log (mirrors agent_activity)
    await client.query(`
      CREATE TABLE IF NOT EXISTS driver_activity (
        id              SERIAL PRIMARY KEY,
        driver_id       INTEGER NOT NULL REFERENCES drivers(id),
        aggregator_id   INTEGER REFERENCES aggregators(id),
          -- nullable: self-register doesn't have an aggregator yet
        action_type     TEXT NOT NULL,
        description     TEXT,
        related_id      INTEGER,
        related_type    TEXT,
        created_at      TIMESTAMPTZ DEFAULT NOW()
      )
    `);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_driver_activity_driver ON driver_activity(driver_id)`);
  },
  down: async (client) => {
    await client.query(`DROP TABLE IF EXISTS driver_activity CASCADE`);
    await client.query(`DROP TABLE IF EXISTS driver_aggregator_relationships CASCADE`);
    await client.query(`DROP TABLE IF EXISTS drivers CASCADE`);
  }
};
