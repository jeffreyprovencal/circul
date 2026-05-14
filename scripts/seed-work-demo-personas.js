// scripts/seed-work-demo-personas.js
//
// Seeds the 7 WORK-demo personas locked in WORK-DEMO-LOGINS.md (Naa Adjeley
// Lamptey / Kwesi Quansah / Yaa Boateng / Selorm Agbeko / Sankofa Plastics /
// Veolia / Alpla) plus a coherent transaction chain from Naa to Quansah, plus
// tags Naa to Vivien Luk's Impact Partner network so her dashboard shows real
// kg attribution for the demo.
//
// Idempotent: safe to re-run. ON CONFLICT DO UPDATE on phone/email unique keys.
// Transactions use a count-of-existing guard since they have no natural unique.
//
// Run:
//   node scripts/seed-work-demo-personas.js
//
// Connects via $DATABASE_URL from env. Works both locally (Jojo's Neon) and
// on Render service shell (Polsia's prod Neon).

const crypto = require('crypto');
const util = require('util');
const scrypt = util.promisify(crypto.scrypt);
const { Pool } = require('pg');

const pool = new Pool({ connectionString: process.env.DATABASE_URL });

async function hashSecret(plain) {
  const salt = crypto.randomBytes(16).toString('hex');
  const key  = await scrypt(plain, salt, 64);
  return salt + ':' + key.toString('hex');
}

async function main() {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    // ── 1. Personas ────────────────────────────────────────────────────────

    // Naa Adjeley Lamptey — collector (PIN 4321)
    const naaPin = await hashSecret('4321');
    const naa = await client.query(`
      INSERT INTO collectors (first_name, last_name, phone, pin, region, city, country, is_active, must_change_pin)
      VALUES ('Naa Adjeley', 'Lamptey', '0241555001', $1, 'Greater Accra', 'Accra', 'Ghana', true, false)
      ON CONFLICT (phone) DO UPDATE SET
        first_name = EXCLUDED.first_name,
        last_name = EXCLUDED.last_name,
        pin = EXCLUDED.pin,
        region = EXCLUDED.region,
        city = EXCLUDED.city,
        country = EXCLUDED.country,
        is_active = true,
        must_change_pin = false
      RETURNING id
    `, [naaPin]);
    const naaId = naa.rows[0].id;

    // Quansah Recovery (Kwesi Quansah) — aggregator (PIN 5342)
    const quansahPin = await hashSecret('5342');
    const quansah = await client.query(`
      INSERT INTO aggregators (name, company, first_name, last_name, phone, pin, region, city, country, is_active, must_change_pin)
      VALUES ('Quansah Recovery', 'Quansah Recovery Ltd', 'Kwesi', 'Quansah', '0241555002', $1, 'Greater Accra', 'Accra', 'Ghana', true, false)
      ON CONFLICT (phone) DO UPDATE SET
        name = EXCLUDED.name,
        company = EXCLUDED.company,
        first_name = EXCLUDED.first_name,
        last_name = EXCLUDED.last_name,
        pin = EXCLUDED.pin,
        region = EXCLUDED.region,
        city = EXCLUDED.city,
        country = EXCLUDED.country,
        is_active = true,
        must_change_pin = false
      RETURNING id
    `, [quansahPin]);
    const quansahId = quansah.rows[0].id;

    // Yaa Boateng — agent (PIN 6453) under Quansah
    const yaaPin = await hashSecret('6453');
    const yaa = await client.query(`
      INSERT INTO agents (aggregator_id, first_name, last_name, phone, pin, region, city, is_active, must_change_pin)
      VALUES ($1, 'Yaa', 'Boateng', '0241555003', $2, 'Greater Accra', 'Accra', true, false)
      ON CONFLICT (phone) DO UPDATE SET
        aggregator_id = EXCLUDED.aggregator_id,
        first_name = EXCLUDED.first_name,
        last_name = EXCLUDED.last_name,
        pin = EXCLUDED.pin,
        region = EXCLUDED.region,
        city = EXCLUDED.city,
        is_active = true,
        must_change_pin = false
      RETURNING id
    `, [quansahId, yaaPin]);
    const yaaId = yaa.rows[0].id;

    // Selorm Agbeko — driver (PIN 7546)
    const selormPin = await hashSecret('7546');
    const selorm = await client.query(`
      INSERT INTO drivers (first_name, last_name, phone, pin, region, city, is_active, must_change_pin)
      VALUES ('Selorm', 'Agbeko', '0241555004', $1, 'Greater Accra', 'Accra', true, false)
      ON CONFLICT (phone) DO UPDATE SET
        first_name = EXCLUDED.first_name,
        last_name = EXCLUDED.last_name,
        pin = EXCLUDED.pin,
        region = EXCLUDED.region,
        city = EXCLUDED.city,
        is_active = true,
        must_change_pin = false
      RETURNING id
    `, [selormPin]);
    const selormId = selorm.rows[0].id;

    // Buyer-tier password (Sankofa, Veolia, Alpla all share)
    const buyerPwd = await hashSecret('WorkDemo2026!');

    // Sankofa Plastics — processor
    const sankofa = await client.query(`
      INSERT INTO processors (name, company, email, password_hash, city, region, country, is_active)
      VALUES ('Sankofa Plastics', 'Sankofa Plastics Ltd', 'sankofa@circul.demo', $1, 'Accra', 'Greater Accra', 'Ghana', true)
      ON CONFLICT (email) DO UPDATE SET
        name = EXCLUDED.name,
        company = EXCLUDED.company,
        password_hash = EXCLUDED.password_hash,
        is_active = true
      RETURNING id
    `, [buyerPwd]);
    const sankofaId = sankofa.rows[0].id;

    // Veolia — recycler
    const veolia = await client.query(`
      INSERT INTO recyclers (name, company, email, password_hash, city, region, country, is_active)
      VALUES ('Veolia Ghana', 'Veolia Environmental Services', 'veolia@circul.demo', $1, 'Tema', 'Greater Accra', 'Ghana', true)
      ON CONFLICT (email) DO UPDATE SET
        name = EXCLUDED.name,
        company = EXCLUDED.company,
        password_hash = EXCLUDED.password_hash,
        is_active = true
      RETURNING id
    `, [buyerPwd]);
    const veoliaId = veolia.rows[0].id;

    // Alpla Group — converter
    const alpla = await client.query(`
      INSERT INTO converters (name, company, email, password_hash, city, region, country, is_active)
      VALUES ('Alpla Group', 'Alpla Werke Alwin Lehner', 'alpla@circul.demo', $1, 'Tema', 'Greater Accra', 'Ghana', true)
      ON CONFLICT (email) DO UPDATE SET
        name = EXCLUDED.name,
        company = EXCLUDED.company,
        password_hash = EXCLUDED.password_hash,
        is_active = true
      RETURNING id
    `, [buyerPwd]);
    const alplaId = alpla.rows[0].id;

    // ── 2. Driver-Aggregator relationship ──────────────────────────────────
    // Selorm is one of Quansah's drivers (active relationship)
    await client.query(`
      INSERT INTO driver_aggregator_relationships
        (driver_id, aggregator_id, status, invite_initiated_by, claimed_at)
      VALUES ($1, $2, 'active', 'aggregator', NOW() - INTERVAL '30 days')
      ON CONFLICT (driver_id, aggregator_id) DO NOTHING
    `, [selormId, quansahId]);

    // ── 3. Naa → Quansah transactions ──────────────────────────────────────
    // Three completed transactions spanning the last 3 weeks. ~35 kg total.
    // These are what populates Vivien's IP report (Naa is tagged below).
    const existingTxn = await client.query(
      'SELECT COUNT(*) AS n FROM transactions WHERE collector_id = $1 AND aggregator_id = $2',
      [naaId, quansahId]
    );
    if (parseInt(existingTxn.rows[0].n, 10) === 0) {
      await client.query(`
        INSERT INTO transactions
          (collector_id, aggregator_id, material_type,
           gross_weight_kg, net_weight_kg, price_per_kg, total_price,
           payment_status, transaction_date)
        VALUES
          ($1, $2, 'PET',  12.5, 12.5, 2.5000,  31.25, 'paid', NOW() - INTERVAL '21 days'),
          ($1, $2, 'PET',  18.0, 18.0, 2.5000,  45.00, 'paid', NOW() - INTERVAL '14 days'),
          ($1, $2, 'HDPE',  5.0,  5.0, 3.0000,  15.00, 'paid', NOW() -  INTERVAL '7 days')
      `, [naaId, quansahId]);
    }

    // ── 4. Tag Naa Adjeley to Vivien's Impact Partner network ──────────────
    const vivien = await client.query(
      "SELECT id FROM impact_partners WHERE email = 'vivien@work.global'"
    );
    if (vivien.rows.length === 0) {
      throw new Error("Vivien impact_partner record missing — seed her first via the earlier hotfix");
    }
    const vivienId = vivien.rows[0].id;
    await client.query(`
      INSERT INTO impact_partner_actor_tags
        (impact_partner_id, actor_type, actor_id, active_since)
      VALUES ($1, 'collector', $2, NOW() - INTERVAL '21 days')
      ON CONFLICT (impact_partner_id, actor_type, actor_id) DO NOTHING
    `, [vivienId, naaId]);

    await client.query('COMMIT');

    // ── 5. Verification report ─────────────────────────────────────────────
    const txnSummary = await client.query(`
      SELECT COUNT(*)::int AS n,
             COALESCE(SUM(gross_weight_kg), 0)::numeric AS kg
      FROM transactions WHERE collector_id = $1 AND aggregator_id = $2
    `, [naaId, quansahId]);

    const tagCount = await client.query(`
      SELECT COUNT(*)::int AS n
      FROM impact_partner_actor_tags
      WHERE impact_partner_id = $1 AND deactivated_at IS NULL
    `, [vivienId]);

    const driverLink = await client.query(`
      SELECT status FROM driver_aggregator_relationships
      WHERE driver_id = $1 AND aggregator_id = $2
    `, [selormId, quansahId]);

    console.log('=== SEED COMPLETE ===');
    console.log('PERSONAS (id):');
    console.log('  collector  Naa Adjeley Lamptey       =', naaId);
    console.log('  aggregator Quansah Recovery (Kwesi)  =', quansahId);
    console.log('  agent      Yaa Boateng               =', yaaId);
    console.log('  driver     Selorm Agbeko             =', selormId);
    console.log('  processor  Sankofa Plastics          =', sankofaId);
    console.log('  recycler   Veolia Ghana              =', veoliaId);
    console.log('  converter  Alpla Group               =', alplaId);
    console.log('CHAIN:');
    console.log('  Naa → Quansah transactions:', txnSummary.rows[0].n, '(' + txnSummary.rows[0].kg + ' kg)');
    console.log('  Selorm ↔ Quansah link:', driverLink.rows[0] ? driverLink.rows[0].status : 'MISSING');
    console.log('IP NETWORK:');
    console.log('  Vivien tagged actors:', tagCount.rows[0].n, '(expect 1+)');

  } catch (err) {
    await client.query('ROLLBACK');
    console.error('SEED FAILED:', err.message);
    console.error(err.stack);
    process.exit(1);
  } finally {
    client.release();
    await pool.end();
  }
}

main();
