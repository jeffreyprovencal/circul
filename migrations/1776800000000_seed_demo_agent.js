/**
 * Seed a demo agent account for Kwesi Amankwah's aggregator operation.
 * Agent: Kofi Mensah, phone 0300000003, PIN 3333 (hashed with scrypt).
 */
const crypto = require('crypto');

function hashPassword(password) {
  return new Promise((resolve, reject) => {
    const salt = crypto.randomBytes(16).toString('hex');
    crypto.scrypt(password, salt, 64, (err, key) => {
      if (err) reject(err);
      else resolve(salt + ':' + key.toString('hex'));
    });
  });
}

module.exports = {
  up: async (client) => {
    const hashedPin = await hashPassword('3333');
    await client.query(
      `INSERT INTO agents (aggregator_id, first_name, last_name, phone, pin, city, region, is_active)
       VALUES ($1, $2, $3, $4, $5, $6, $7, true)
       ON CONFLICT (phone) DO NOTHING`,
      [9, 'Kofi', 'Mensah', '0300000003', hashedPin, 'Accra', 'Greater Accra']
    );
  },
  down: async (client) => {
    await client.query(`DELETE FROM agents WHERE phone = '0300000003'`);
  }
};
