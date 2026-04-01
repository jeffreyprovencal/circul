#!/usr/bin/env node
// Hash existing plaintext collector PINs in-place.
// Usage: DATABASE_URL=... node scripts/hash-existing-pins.js

const crypto = require('crypto');
const { Pool } = require('pg');

const pool = new Pool({ connectionString: process.env.DATABASE_URL });

function hashPassword(password) {
  return new Promise((resolve, reject) => {
    const salt = crypto.randomBytes(16).toString('hex');
    crypto.scrypt(password, salt, 64, (err, key) => {
      if (err) reject(err);
      else resolve(salt + ':' + key.toString('hex'));
    });
  });
}

async function main() {
  const { rows } = await pool.query(
    `SELECT id, pin FROM collectors WHERE pin NOT LIKE '%:%'`
  );
  console.log(`Found ${rows.length} collectors with plaintext PINs`);
  for (const row of rows) {
    const hashed = await hashPassword(row.pin);
    await pool.query(`UPDATE collectors SET pin=$1 WHERE id=$2`, [hashed, row.id]);
    console.log(`  Hashed PIN for collector #${row.id}`);
  }
  console.log('Done.');
  await pool.end();
}

main().catch(err => { console.error(err); process.exit(1); });
