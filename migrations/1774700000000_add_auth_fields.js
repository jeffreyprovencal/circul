module.exports = {
  name: 'add_auth_fields',
  up: async (client) => {
    const collOrg = await client.query(
      `SELECT column_name FROM information_schema.columns
       WHERE table_name='collectors' AND column_name='organisation'`
    );
    if (!collOrg.rows.length) {
      await client.query(`ALTER TABLE collectors ADD COLUMN organisation VARCHAR(255)`);
    }

    const aggOrg = await client.query(
      `SELECT column_name FROM information_schema.columns
       WHERE table_name='aggregators' AND column_name='organisation'`
    );
    if (!aggOrg.rows.length) {
      await client.query(`ALTER TABLE aggregators ADD COLUMN organisation VARCHAR(255)`);
    }
  },
  down: async (client) => {
    await client.query(`ALTER TABLE collectors DROP COLUMN IF EXISTS organisation`);
    await client.query(`ALTER TABLE aggregators DROP COLUMN IF EXISTS organisation`);
  }
};
