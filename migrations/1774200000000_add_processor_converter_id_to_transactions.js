'use strict';

module.exports = {
  name: 'add_processor_converter_id_to_transactions',
  up: async (client) => {
    await client.query(`
      ALTER TABLE transactions
        ADD COLUMN IF NOT EXISTS processor_id INTEGER,
        ADD COLUMN IF NOT EXISTS converter_id INTEGER
    `);
  }
};
