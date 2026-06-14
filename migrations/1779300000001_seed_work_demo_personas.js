// migrations/1779300000001_seed_work_demo_personas.js
//
// RENAMED from 1779000000004_… (2026-06-09): the original timestamp ordered
// this seed BEFORE its two schema dependencies — aggregators.first_name/
// last_name (1779100000000) and the impact_partners tables (1779200000000) —
// so fresh-DB bootstraps failed here. Prod was unaffected only because the
// dependencies happened to deploy first chronologically. The ledger identity
// is the exported `name` ('seed_work_demo_personas'), not the filename, so
// already-migrated databases skip this file exactly as before; only the
// fresh-bootstrap execution order changes. Note the "soft" Vivien guard below
// handles a missing impact_partners ROW, not a missing TABLE — the relation
// itself must exist, hence the ordering requirement.
//
// Seeds the 7 WORK-demo personas locked in WORK-DEMO-LOGINS.md (Naa Adjeley
// Lamptey / Kwesi Quansah / Yaa Boateng / Selorm Agbeko / Sankofa Plastics /
// Veolia / Alpla) plus a full cross-tier transaction chain (collector →
// aggregator → processor → recycler → converter) and tags Naa to Vivien Luk's
// Impact Partner network so the IP dashboard shows real kg attribution.
//
// Runs once per database (tracked in _migrations). The migration runner wraps
// the body in BEGIN/COMMIT with ROLLBACK on error.
//
// Idempotent inside its body — ON CONFLICT DO UPDATE on persona unique keys,
// count-of-existing guards on chain rows — so the same migration body is safe
// to extract and re-run manually via psql if ever needed.
//
// Vivien IP tag is SOFT (logs a warning if her impact_partners row is missing
// rather than throwing). On prod she exists. In fresh dev DBs she won't, and
// blocking every dev bootstrap on a missing IP record would be wrong.

const crypto = require('crypto');
const util   = require('util');
const scrypt = util.promisify(crypto.scrypt);

async function hashSecret(plain) {
  const salt = crypto.randomBytes(16).toString('hex');
  const key  = await scrypt(plain, salt, 64);
  return salt + ':' + key.toString('hex');
}

module.exports = {
  name: 'seed_work_demo_personas',
  up: async (client) => {

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

    // ── 3b. Quansah → Sankofa dispatches (aggregator_sale, driver=Selorm) ──
    // 2 dispatches via `pending_transactions` with transaction_type='aggregator_sale'
    // and status='completed'. Selorm is recorded as the driver who moved them.
    // Populates: Sankofa's processor inbox + Selorm's driver dashboard + Quansah's
    // outbound dispatch view.
    const existingQS = await client.query(
      `SELECT COUNT(*) AS n FROM pending_transactions
       WHERE transaction_type='aggregator_sale'
         AND aggregator_id=$1 AND processor_id=$2`,
      [quansahId, sankofaId]
    );
    if (parseInt(existingQS.rows[0].n, 10) === 0) {
      await client.query(`
        INSERT INTO pending_transactions
          (transaction_type, status,
           aggregator_id, processor_id, driver_id,
           material_type, gross_weight_kg, net_weight_kg, accepted_weight_kg,
           price_per_kg, total_price,
           grade, dispatch_approved, dispatch_approved_at,
           driver_confirmed_at, driver_fee_ghs, driver_fee_paid_at, driver_fee_paid_method,
           payment_status, created_at, updated_at)
        VALUES
          ('aggregator_sale','completed',
           $1, $2, $3,
           'PET', 300.00, 300.00, 300.00,
           3.5000, 1050.00,
           'A', true, NOW() - INTERVAL '14 days',
           NOW() - INTERVAL '14 days', 50.00, NOW() - INTERVAL '14 days', 'cash',
           'paid', NOW() - INTERVAL '14 days', NOW() - INTERVAL '14 days'),
          ('aggregator_sale','completed',
           $1, $2, $3,
           'HDPE', 150.00, 150.00, 150.00,
           4.0000, 600.00,
           'A', true, NOW() - INTERVAL '5 days',
           NOW() - INTERVAL '5 days', 50.00, NOW() - INTERVAL '5 days', 'cash',
           'paid', NOW() - INTERVAL '5 days', NOW() - INTERVAL '5 days')
      `, [quansahId, sankofaId, selormId]);
    }

    // ── 3c. Sankofa → Veolia dispatches (processor_sale) ────────────────────
    // Processor sells washed flake to recycler. 2 rows. Populates Veolia's
    // recycler inbox + Sankofa's outbound sales view.
    const existingSV = await client.query(
      `SELECT COUNT(*) AS n FROM pending_transactions
       WHERE transaction_type='processor_sale'
         AND processor_id=$1 AND recycler_id=$2`,
      [sankofaId, veoliaId]
    );
    if (parseInt(existingSV.rows[0].n, 10) === 0) {
      await client.query(`
        INSERT INTO pending_transactions
          (transaction_type, status,
           processor_id, recycler_id,
           material_type, gross_weight_kg, net_weight_kg, accepted_weight_kg,
           price_per_kg, total_price,
           grade, dispatch_approved, dispatch_approved_at,
           payment_status, created_at, updated_at)
        VALUES
          ('processor_sale','completed',
           $1, $2,
           'PET', 250.00, 250.00, 250.00,
           5.0000, 1250.00,
           'A', true, NOW() - INTERVAL '12 days',
           'paid', NOW() - INTERVAL '12 days', NOW() - INTERVAL '12 days'),
          ('processor_sale','completed',
           $1, $2,
           'HDPE', 120.00, 120.00, 120.00,
           5.5000, 660.00,
           'A', true, NOW() - INTERVAL '3 days',
           'paid', NOW() - INTERVAL '3 days', NOW() - INTERVAL '3 days')
      `, [sankofaId, veoliaId]);
    }

    // ── 3d. Veolia → Alpla dispatch (recycler_sale) ─────────────────────────
    // Recycler sells regranulate pellets to converter. 1 row. Populates Alpla's
    // converter inbox + Veolia's outbound sales view. End of chain.
    const existingVA = await client.query(
      `SELECT COUNT(*) AS n FROM pending_transactions
       WHERE transaction_type='recycler_sale'
         AND recycler_id=$1 AND converter_id=$2`,
      [veoliaId, alplaId]
    );
    if (parseInt(existingVA.rows[0].n, 10) === 0) {
      await client.query(`
        INSERT INTO pending_transactions
          (transaction_type, status,
           recycler_id, converter_id,
           material_type, gross_weight_kg, net_weight_kg, accepted_weight_kg,
           price_per_kg, total_price,
           grade, dispatch_approved, dispatch_approved_at,
           payment_status, created_at, updated_at)
        VALUES
          ('recycler_sale','completed',
           $1, $2,
           'PET', 200.00, 200.00, 200.00,
           7.0000, 1400.00,
           'A', true, NOW() - INTERVAL '1 day',
           'paid', NOW() - INTERVAL '1 day', NOW() - INTERVAL '1 day')
      `, [veoliaId, alplaId]);
    }

    // ── 4. Tag Naa Adjeley to Vivien's Impact Partner network ──────────────
    // SOFT: log a warning rather than throwing if Vivien doesn't exist. On prod
    // she's at id=1; in fresh dev DBs she may not be seeded yet, and we don't
    // want every dev bootstrap to block on this.
    const vivien = await client.query(
      "SELECT id FROM impact_partners WHERE email = 'vivien@work.global'"
    );
    let vivienTagged = false;
    let vivienTagCount = 0;
    if (vivien.rows.length === 0) {
      console.log('  [seed_work_demo_personas] WARNING: Vivien impact_partners record not found — skipping IP tag step. (Expected on fresh dev DBs.)');
    } else {
      const vivienId = vivien.rows[0].id;
      await client.query(`
        INSERT INTO impact_partner_actor_tags
          (impact_partner_id, actor_type, actor_id, active_since)
        VALUES ($1, 'collector', $2, NOW() - INTERVAL '21 days')
        ON CONFLICT (impact_partner_id, actor_type, actor_id) DO NOTHING
      `, [vivienId, naaId]);
      vivienTagged = true;
      const tagCount = await client.query(`
        SELECT COUNT(*)::int AS n
        FROM impact_partner_actor_tags
        WHERE impact_partner_id = $1 AND deactivated_at IS NULL
      `, [vivienId]);
      vivienTagCount = tagCount.rows[0].n;
    }

    // ── 5. Verification report ─────────────────────────────────────────────
    const txnSummary = await client.query(`
      SELECT COUNT(*)::int AS n,
             COALESCE(SUM(gross_weight_kg), 0)::numeric AS kg
      FROM transactions WHERE collector_id = $1 AND aggregator_id = $2
    `, [naaId, quansahId]);

    const driverLink = await client.query(`
      SELECT status FROM driver_aggregator_relationships
      WHERE driver_id = $1 AND aggregator_id = $2
    `, [selormId, quansahId]);

    const qsSummary = await client.query(`
      SELECT COUNT(*)::int AS n,
             COALESCE(SUM(gross_weight_kg), 0)::numeric AS kg
      FROM pending_transactions
      WHERE transaction_type='aggregator_sale' AND aggregator_id=$1 AND processor_id=$2
    `, [quansahId, sankofaId]);

    const svSummary = await client.query(`
      SELECT COUNT(*)::int AS n,
             COALESCE(SUM(gross_weight_kg), 0)::numeric AS kg
      FROM pending_transactions
      WHERE transaction_type='processor_sale' AND processor_id=$1 AND recycler_id=$2
    `, [sankofaId, veoliaId]);

    const vaSummary = await client.query(`
      SELECT COUNT(*)::int AS n,
             COALESCE(SUM(gross_weight_kg), 0)::numeric AS kg
      FROM pending_transactions
      WHERE transaction_type='recycler_sale' AND recycler_id=$1 AND converter_id=$2
    `, [veoliaId, alplaId]);

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
    console.log('  Naa     → Quansah transactions :', txnSummary.rows[0].n, '(' + txnSummary.rows[0].kg + ' kg)');
    console.log('  Quansah → Sankofa dispatches   :', qsSummary.rows[0].n,  '(' + qsSummary.rows[0].kg  + ' kg, driver=Selorm)');
    console.log('  Sankofa → Veolia  sales        :', svSummary.rows[0].n,  '(' + svSummary.rows[0].kg  + ' kg)');
    console.log('  Veolia  → Alpla   sales        :', vaSummary.rows[0].n,  '(' + vaSummary.rows[0].kg  + ' kg)');
    console.log('  Selorm  ↔ Quansah driver-link  :', driverLink.rows[0] ? driverLink.rows[0].status : 'MISSING');
    console.log('IP NETWORK:');
    if (vivienTagged) {
      console.log('  Vivien tagged actors:', vivienTagCount, '(expect 1+)');
    } else {
      console.log('  Vivien tagged actors: SKIPPED (impact_partners record missing)');
    }
  }
};
