/**
 * Migration: restructure_tiers
 *
 * Drops legacy operators + buyers tables, replaces with four clean
 * role-specific tables: collectors (extended), aggregators, processors, converters.
 * Rebuilds transactions, pending_transactions, posted_prices with new FK columns.
 * Re-seeds all demo accounts.
 *
 * Idempotent: uses IF NOT EXISTS / IF EXISTS throughout.
 * Runs inside a single transaction (managed by migrate.js runner).
 */
const crypto = require('crypto');
const util   = require('util');
const scrypt  = util.promisify(crypto.scrypt);

async function hashPwd(password) {
  const salt = crypto.randomBytes(16).toString('hex');
  const key  = await scrypt(password, salt, 64);
  return salt + ':' + key.toString('hex');
}

module.exports = {
  name: 'restructure_tiers',
  up: async (client) => {
    // Pre-hash demo password once
    const demoHash = await hashPwd('demo1234');

    // ── 1a. Drop legacy tables (CASCADE removes dependent FKs) ──────────────
    await client.query(`
      DROP TABLE IF EXISTS buyer_prices       CASCADE;
      DROP TABLE IF EXISTS orders             CASCADE;
      DROP TABLE IF EXISTS collector_passports CASCADE;
      DROP TABLE IF EXISTS ratings            CASCADE;
      DROP TABLE IF EXISTS ussd_sessions      CASCADE;
      DROP TABLE IF EXISTS posted_prices      CASCADE;
      DROP TABLE IF EXISTS pending_transactions CASCADE;
      DROP TABLE IF EXISTS transactions       CASCADE;
      DROP TABLE IF EXISTS buyers             CASCADE;
      DROP TABLE IF EXISTS operators          CASCADE;
    `);

    // ── 1b. Extend existing collectors table ────────────────────────────────
    await client.query(`
      ALTER TABLE collectors ADD COLUMN IF NOT EXISTS pin                VARCHAR(10);
      ALTER TABLE collectors ADD COLUMN IF NOT EXISTS date_of_birth      DATE;
      ALTER TABLE collectors ADD COLUMN IF NOT EXISTS gender             VARCHAR(20);
      ALTER TABLE collectors ADD COLUMN IF NOT EXISTS id_verified        BOOLEAN DEFAULT false;
      ALTER TABLE collectors ADD COLUMN IF NOT EXISTS id_verified_at     TIMESTAMPTZ;
      ALTER TABLE collectors ADD COLUMN IF NOT EXISTS id_verified_by     VARCHAR(255);
      ALTER TABLE collectors ADD COLUMN IF NOT EXISTS id_document_type   VARCHAR(50);
      ALTER TABLE collectors ADD COLUMN IF NOT EXISTS country            VARCHAR(100) DEFAULT 'Ghana';
      ALTER TABLE collectors ADD COLUMN IF NOT EXISTS is_flagged         BOOLEAN DEFAULT false;
      ALTER TABLE collectors ADD COLUMN IF NOT EXISTS email              VARCHAR(255);
      ALTER TABLE collectors ADD COLUMN IF NOT EXISTS password_hash      VARCHAR(255);
    `);

    // Set Ama Mensah's PIN (demo login collector)
    await client.query(`
      UPDATE collectors SET pin = '1111' WHERE phone = '0300000001';
    `);

    // ── 1c. Create aggregators table ────────────────────────────────────────
    await client.query(`
      CREATE TABLE IF NOT EXISTS aggregators (
        id                  SERIAL PRIMARY KEY,
        name                VARCHAR(255) NOT NULL,
        company             VARCHAR(255),
        phone               VARCHAR(50)  UNIQUE,
        pin                 VARCHAR(10),
        email               VARCHAR(255) UNIQUE,
        password_hash       VARCHAR(255),
        date_of_birth       DATE,
        gender              VARCHAR(20),
        city                VARCHAR(100),
        region              VARCHAR(100),
        country             VARCHAR(100) DEFAULT 'Ghana',
        is_active           BOOLEAN DEFAULT true,
        is_flagged          BOOLEAN DEFAULT false,
        id_verified         BOOLEAN DEFAULT false,
        id_verified_at      TIMESTAMPTZ,
        id_verified_by      VARCHAR(255),
        id_document_type    VARCHAR(50),
        created_at          TIMESTAMPTZ DEFAULT NOW()
      );
    `);

    // ── 1d. Create processors table ─────────────────────────────────────────
    await client.query(`
      CREATE TABLE IF NOT EXISTS processors (
        id                  SERIAL PRIMARY KEY,
        name                VARCHAR(255) NOT NULL,
        company             VARCHAR(255) NOT NULL,
        email               VARCHAR(255) UNIQUE NOT NULL,
        password_hash       VARCHAR(255),
        phone               VARCHAR(50),
        city                VARCHAR(100),
        region              VARCHAR(100),
        country             VARCHAR(100) DEFAULT 'Ghana',
        is_active           BOOLEAN DEFAULT true,
        is_flagged          BOOLEAN DEFAULT false,
        created_at          TIMESTAMPTZ DEFAULT NOW()
      );
    `);

    // ── 1e. Create converters table ─────────────────────────────────────────
    await client.query(`
      CREATE TABLE IF NOT EXISTS converters (
        id                  SERIAL PRIMARY KEY,
        name                VARCHAR(255) NOT NULL,
        company             VARCHAR(255) NOT NULL,
        email               VARCHAR(255) UNIQUE,
        password_hash       VARCHAR(255),
        phone               VARCHAR(50),
        city                VARCHAR(100),
        region              VARCHAR(100),
        country             VARCHAR(100),
        is_active           BOOLEAN DEFAULT true,
        is_flagged          BOOLEAN DEFAULT false,
        created_at          TIMESTAMPTZ DEFAULT NOW()
      );
    `);

    // ── 1f. Create transactions table (collector → aggregator sales) ─────────
    await client.query(`
      CREATE TABLE IF NOT EXISTS transactions (
        id                              SERIAL PRIMARY KEY,
        collector_id                    INTEGER NOT NULL REFERENCES collectors(id) ON DELETE CASCADE,
        aggregator_id                   INTEGER REFERENCES aggregators(id) ON DELETE SET NULL,
        material_type                   VARCHAR(50) NOT NULL,
        gross_weight_kg                 DECIMAL(10,2) NOT NULL CHECK (gross_weight_kg > 0),
        net_weight_kg                   DECIMAL(10,2),
        contamination_deduction_percent DECIMAL(5,2) DEFAULT 0,
        contamination_types             TEXT[] DEFAULT '{}',
        quality_notes                   TEXT,
        price_per_kg                    DECIMAL(10,4) NOT NULL DEFAULT 0,
        total_price                     DECIMAL(12,2) NOT NULL DEFAULT 0,
        lat                             DECIMAL(10,7),
        lng                             DECIMAL(10,7),
        notes                           TEXT,
        transaction_date                TIMESTAMPTZ DEFAULT NOW(),
        payment_status                  VARCHAR(20) DEFAULT 'unpaid' CHECK (payment_status IN ('unpaid','payment_sent','paid')),
        payment_method                  VARCHAR(20) CHECK (payment_method IN ('cash','mobile_money')),
        payment_reference               VARCHAR(100),
        payment_initiated_at            TIMESTAMPTZ,
        payment_completed_at            TIMESTAMPTZ,
        created_at                      TIMESTAMPTZ DEFAULT NOW(),
        updated_at                      TIMESTAMPTZ DEFAULT NOW()
      );
    `);

    // ── 1g. Create pending_transactions table ────────────────────────────────
    await client.query(`
      CREATE TABLE IF NOT EXISTS pending_transactions (
        id                          SERIAL PRIMARY KEY,
        transaction_type            VARCHAR(50) NOT NULL DEFAULT 'aggregator_sale',
        status                      VARCHAR(50) NOT NULL DEFAULT 'pending'
                                    CHECK (status IN ('pending','dispatched','arrived','completed','rejected',
                                                      'confirmed','dispatch_approved','dispatch_rejected','grade_c_flagged')),
        collector_id                INTEGER REFERENCES collectors(id)  ON DELETE SET NULL,
        aggregator_id               INTEGER REFERENCES aggregators(id) ON DELETE SET NULL,
        processor_id                INTEGER REFERENCES processors(id)  ON DELETE SET NULL,
        converter_id                INTEGER REFERENCES converters(id)  ON DELETE SET NULL,
        material_type               VARCHAR(50) NOT NULL,
        gross_weight_kg             DECIMAL(10,2) NOT NULL CHECK (gross_weight_kg > 0),
        net_weight_kg               DECIMAL(10,2),
        price_per_kg                DECIMAL(10,4) NOT NULL DEFAULT 0,
        total_price                 DECIMAL(12,2) NOT NULL DEFAULT 0,
        grade                       VARCHAR(10),
        grade_notes                 TEXT,
        photos_required             BOOLEAN DEFAULT false,
        photos_submitted            BOOLEAN DEFAULT false,
        dispatch_approved           BOOLEAN DEFAULT false,
        dispatch_approved_at        TIMESTAMPTZ,
        dispatch_approved_by_id     INTEGER,
        dispatch_approved_by_type   VARCHAR(20),
        photo_urls                  TEXT[] DEFAULT '{}',
        rejection_reason            TEXT,
        rejected_at                 TIMESTAMPTZ,
        transaction_id              INTEGER,
        notes                       TEXT,
        payment_status              VARCHAR(20) DEFAULT 'unpaid' CHECK (payment_status IN ('unpaid','payment_sent','paid')),
        payment_method              VARCHAR(20) CHECK (payment_method IN ('cash','mobile_money')),
        payment_reference           VARCHAR(100),
        payment_initiated_at        TIMESTAMPTZ,
        payment_completed_at        TIMESTAMPTZ,
        created_at                  TIMESTAMPTZ DEFAULT NOW(),
        updated_at                  TIMESTAMPTZ DEFAULT NOW()
      );
    `);

    // ── 1h. Create posted_prices table ──────────────────────────────────────
    await client.query(`
      CREATE TABLE IF NOT EXISTS posted_prices (
        id                SERIAL PRIMARY KEY,
        poster_type       VARCHAR(20) NOT NULL CHECK (poster_type IN ('aggregator','processor','converter')),
        poster_id         INTEGER NOT NULL,
        material_type     VARCHAR(50) NOT NULL,
        price_per_kg_usd  DECIMAL(10,4),
        price_per_kg_ghs  DECIMAL(10,4),
        usd_to_ghs_rate   DECIMAL(10,4),
        city              VARCHAR(100),
        region            VARCHAR(100),
        country           VARCHAR(100) DEFAULT 'Ghana',
        expires_at        TIMESTAMPTZ,
        is_active         BOOLEAN DEFAULT true,
        posted_at         TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE(poster_type, poster_id, material_type)
      );
    `);

    // ── 1i. Re-create ratings, collector_passports, ussd_sessions ────────────
    await client.query(`
      CREATE TABLE IF NOT EXISTS ratings (
        id                  SERIAL PRIMARY KEY,
        transaction_id      INTEGER REFERENCES transactions(id) ON DELETE SET NULL,
        rater_type          VARCHAR(20) NOT NULL,
        rater_id            INTEGER NOT NULL,
        rated_type          VARCHAR(20) NOT NULL,
        rated_id            INTEGER NOT NULL,
        rating              SMALLINT NOT NULL CHECK (rating BETWEEN 1 AND 5),
        tags                TEXT[] DEFAULT '{}',
        notes               TEXT,
        rating_direction    VARCHAR(30),
        created_at          TIMESTAMPTZ DEFAULT NOW(),
        window_expires_at   TIMESTAMPTZ
      );

      CREATE TABLE IF NOT EXISTS collector_passports (
        collector_id                    INTEGER PRIMARY KEY REFERENCES collectors(id) ON DELETE CASCADE,
        total_kg_lifetime               DECIMAL(12,2) DEFAULT 0,
        total_kg_last_12m               DECIMAL(12,2) DEFAULT 0,
        total_earned_ghs                DECIMAL(12,2) DEFAULT 0,
        total_earned_usd                DECIMAL(12,2) DEFAULT 0,
        transaction_count               INTEGER DEFAULT 0,
        active_since                    TIMESTAMPTZ,
        material_breakdown              JSONB DEFAULT '{}',
        unique_aggregators              INTEGER DEFAULT 0,
        avg_rating_from_aggregators     DECIMAL(3,2),
        payment_reliability_score       DECIMAL(3,2),
        last_updated                    TIMESTAMPTZ DEFAULT NOW()
      );

      CREATE TABLE IF NOT EXISTS ussd_sessions (
        id            SERIAL PRIMARY KEY,
        session_id    VARCHAR(255) UNIQUE NOT NULL,
        phone         VARCHAR(50) NOT NULL,
        service_code  VARCHAR(20),
        collector_id  INTEGER REFERENCES collectors(id) ON DELETE SET NULL,
        action        VARCHAR(100),
        text_input    TEXT,
        response      TEXT,
        created_at    TIMESTAMPTZ DEFAULT NOW()
      );
    `);

    // ── 1j. Seed demo data ───────────────────────────────────────────────────

    // Aggregators — preserve IDs using OVERRIDING SYSTEM VALUE
    await client.query(`
      INSERT INTO aggregators (id, name, phone, pin, city, region, country) OVERRIDING SYSTEM VALUE VALUES
        (9,  'Kwesi Amankwah', '0300000002', '2222', 'Accra', 'Greater Accra', 'Ghana'),
        (12, 'Abena Osei',     '0244100001', NULL,   'Accra', 'Greater Accra', 'Ghana'),
        (13, 'Kofi Nyarko',    '0244100002', NULL,   'Accra', 'Greater Accra', 'Ghana'),
        (14, 'Efua Asante',    '0244100003', NULL,   'Accra', 'Greater Accra', 'Ghana'),
        (15, 'Yaw Darko',      '0244100004', NULL,   'Accra', 'Greater Accra', 'Ghana')
      ON CONFLICT DO NOTHING;
    `);

    await client.query(`SELECT setval('aggregators_id_seq', (SELECT MAX(id) FROM aggregators));`);

    // Processors
    await client.query(
      `INSERT INTO processors (name, company, email, password_hash, phone, city, region, country) VALUES
        ($1, $2, $3, $4, $5, $6, $7, $8),
        ($1, $2, $9, $4, NULL, $6, $7, $8),
        ($1, $2, $10, $4, NULL, $6, $7, $8),
        ($1, $2, $11, $4, NULL, $6, $7, $8)
       ON CONFLICT (email) DO NOTHING`,
      [
        'Jeffrey Provencal', 'rePATRN Ghana', 'jeffrey@circul.demo', demoHash,
        '0300000003', 'Tema', 'Greater Accra', 'Ghana',
        'miniplast@circul.demo',
        'greenloop@circul.demo',
        'recycleforce@circul.demo'
      ]
    );
    // Fix names/companies for the other processors
    await client.query(`UPDATE processors SET name='Miniplast Ghana',  company='Miniplast Ghana'   WHERE email='miniplast@circul.demo'`);
    await client.query(`UPDATE processors SET name='GreenLoop Accra',  company='GreenLoop Accra'   WHERE email='greenloop@circul.demo'`);
    await client.query(`UPDATE processors SET name='RecycleForce GH',  company='RecycleForce Ghana' WHERE email='recycleforce@circul.demo'`);
    await client.query(`UPDATE processors SET city='Accra' WHERE email IN ('miniplast@circul.demo','greenloop@circul.demo','recycleforce@circul.demo')`);

    // Converters — note Miniplast shares same email+hash as in processors
    await client.query(
      `INSERT INTO converters (name, company, email, password_hash, city, region, country) VALUES
        ('Miniplast Ghana',   'Miniplast Ghana',   'miniplast@circul.demo', $1, 'Accra', 'Greater Accra', 'Ghana'),
        ('Poly Recycling AG', 'Poly Recycling AG', 'poly@circul.demo',      $1, 'Aarau', NULL,            'Switzerland'),
        ('Iterum',            'Iterum',            'iterum@circul.demo',    $1, NULL,    NULL,             NULL)
       ON CONFLICT (email) DO NOTHING`,
      [demoHash]
    );
    // converters email column may not have unique constraint — add it defensively
    // (no-op if already unique or no duplicates)

    // Posted prices — aggregator buy prices for collectors.
    // Uses CROSS JOIN VALUES instead of unnest-in-CASE (which Postgres rejects:
    // "set-returning functions are not allowed in CASE"). Semantically identical
    // to the original — four material rows per aggregator with the same prices.
    await client.query(`
      INSERT INTO posted_prices (poster_type, poster_id, material_type, price_per_kg_ghs, city, region, country, is_active)
      SELECT 'aggregator', a.id, m.material_type, m.price, a.city, a.region, a.country, true
      FROM aggregators a
      CROSS JOIN (VALUES
        ('PET',  3.20),
        ('HDPE', 2.80),
        ('PP',   2.50),
        ('LDPE', 1.90)
      ) AS m(material_type, price)
      ON CONFLICT DO NOTHING;
    `);

    // Demo pending_transactions: Kwesi (id=9) → Miniplast processor
    await client.query(`
      INSERT INTO pending_transactions
        (transaction_type, status, aggregator_id, processor_id, material_type, gross_weight_kg, price_per_kg, total_price, grade, dispatch_approved, dispatch_approved_at)
      SELECT 'aggregator_sale', 'arrived', 9, p.id, 'PET', 2100.00, 2.00, 4200.00, 'A', true, NOW()
      FROM processors p WHERE p.company = 'Miniplast Ghana'
      ON CONFLICT DO NOTHING;

      INSERT INTO pending_transactions
        (transaction_type, status, aggregator_id, processor_id, material_type, gross_weight_kg, price_per_kg, total_price, grade, dispatch_approved, dispatch_approved_at)
      SELECT 'aggregator_sale', 'arrived', 9, p.id, 'PET', 430.00, 2.00, 860.00, 'A', true, NOW()
      FROM processors p WHERE p.company = 'Miniplast Ghana'
      ON CONFLICT DO NOTHING;
    `);
  }
};
