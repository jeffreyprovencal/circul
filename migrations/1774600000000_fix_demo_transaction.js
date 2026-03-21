/**
 * Migration: fix_demo_transaction
 *
 * Assigns transaction #1 (Ama's test transaction) to Kwesi (aggregator id=9)
 * if it is currently orphaned (aggregator_id IS NULL).
 * Idempotent: WHERE clause ensures it only runs when needed.
 */
module.exports = {
  name: 'fix_demo_transaction',
  up: async (client) => {
    await client.query(`
      UPDATE transactions
      SET aggregator_id = 9
      WHERE id = 1 AND aggregator_id IS NULL
    `);
  },
  down: async (client) => {}
};
