'use strict';
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
  name: 'seed_full_supply_chain',
  up: async (client) => {

    // ── Schema: make collector_id nullable so agg→processor and processor→converter
    //   rows can exist without a collector seller
    await client.query(`ALTER TABLE transactions ALTER COLUMN collector_id DROP NOT NULL`);

    // ── Schema: add operator_id to transactions to identify the selling aggregator
    await client.query(`
      ALTER TABLE transactions
      ADD COLUMN IF NOT EXISTS operator_id INTEGER REFERENCES operators(id) ON DELETE SET NULL
    `);

    // ── Schema: expand buyers.role to include 'converter'
    await client.query(`ALTER TABLE buyers DROP CONSTRAINT IF EXISTS buyers_role_check`);
    await client.query(`
      ALTER TABLE buyers ADD CONSTRAINT buyers_role_check
        CHECK (role IN ('aggregator', 'processor', 'converter'))
    `);

    // ── 1. Rename existing demo aggregator
    await client.query(
      `UPDATE operators SET name = 'Kwesi Amankwah', company = NULL WHERE phone = '0300000002'`
    );

    // ── 2. Insert 4 additional aggregators (skip if phone exists)
    const newAggregators = [
      ['Abena Osei',  '0244100001', '3333'],
      ['Kofi Nyarko', '0244100002', '4444'],
      ['Efua Asante', '0244100003', '5555'],
      ['Yaw Darko',   '0244100004', '6666'],
    ];
    for (const [name, phone, pin] of newAggregators) {
      await client.query(`
        INSERT INTO operators (name, company, phone, pin, role, city, region, country)
        VALUES ($1, NULL, $2, $3, 'aggregator', 'Accra', 'Greater Accra', 'Ghana')
        ON CONFLICT (phone) DO NOTHING
      `, [name, phone, pin]);
    }

    // ── 3. Hash passwords for buyers (processors + converters)
    const pwDemo    = await hashPassword('demo1234');
    const pwRf      = await hashPassword('rf2026');
    const pwGl      = await hashPassword('gl2026');
    const pwVeolia  = await hashPassword('demo1234');
    const pwIterum  = await hashPassword('it2026');

    // ── 4. Insert processors (skip if email exists)
    await client.query(`
      INSERT INTO buyers (name, company, email, password_hash, role)
      VALUES ('Abena Owusu', 'Miniplast Ghana', 'abena@circul.demo', $1, 'processor')
      ON CONFLICT (email) DO NOTHING
    `, [pwDemo]);
    await client.query(`
      INSERT INTO buyers (name, company, email, password_hash, role)
      VALUES ('RecycleForce GH', 'RecycleForce Ghana', 'info@recycleforce.demo', $1, 'processor')
      ON CONFLICT (email) DO NOTHING
    `, [pwRf]);
    await client.query(`
      INSERT INTO buyers (name, company, email, password_hash, role)
      VALUES ('GreenLoop Accra', 'GreenLoop Accra', 'info@greenloop.demo', $1, 'processor')
      ON CONFLICT (email) DO NOTHING
    `, [pwGl]);

    // ── 5. Insert converters (skip if email exists)
    await client.query(`
      INSERT INTO buyers (name, company, email, password_hash, role)
      VALUES ('Veolia West Africa', 'Veolia West Africa', 'kweku@circul.demo', $1, 'converter')
      ON CONFLICT (email) DO NOTHING
    `, [pwVeolia]);
    await client.query(`
      INSERT INTO buyers (name, company, email, password_hash, role)
      VALUES ('Iterum', 'Iterum', 'info@iterum.demo', $1, 'converter')
      ON CONFLICT (email) DO NOTHING
    `, [pwIterum]);

    // ── 6. Insert 100 collectors (skip if phone exists)
    const collectorNames = [
      // 1-20
      ['Kwame','Adjei'],   ['Kofi','Boateng'],      ['Ama','Tetteh'],        ['Abena','Asare'],
      ['Yaw','Ofori'],     ['Akosua','Mensah'],      ['Kweku','Ansah'],       ['Efua','Owusu'],
      ['Kojo','Amoah'],    ['Adwoa','Quaye'],        ['Kwabena','Acheampong'],['Esi','Bonsu'],
      ['Nana','Frimpong'], ['Akua','Gyasi'],         ['Kwasi','Opoku'],       ['Abiba','Agyei'],
      ['Fiifi','Antwi'],   ['Maame','Appiah'],       ['Kweku','Baffoe'],      ['Esi','Baah'],
      // 21-40
      ['Kofi','Wiredu'],   ['Ama','Asante'],         ['Yaw','Obeng'],         ['Akosua','Danso'],
      ['Kwame','Kusi'],    ['Abena','Sarpong'],      ['Kojo','Ampofo'],       ['Efua','Ntim'],
      ['Kweku','Aidoo'],   ['Adwoa','Boakye'],       ['Nana','Adusei'],       ['Akua','Fofie'],
      ['Kwasi','Nimako'],  ['Abiba','Peprah'],       ['Fiifi','Mensah'],      ['Maame','Darko'],
      ['Kwabena','Asamoah'],['Esi','Annan'],         ['Yaw','Sarkodie'],      ['Ama','Dankwa'],
      // 41-60
      ['Kofi','Tawiah'],   ['Akosua','Bekoe'],       ['Kwame','Ocran'],       ['Abena','Laryea'],
      ['Kojo','Arhin'],    ['Efua','Ofei'],          ['Kweku','Nkrumah'],     ['Adwoa','Asiedu'],
      ['Nana','Amoako'],   ['Akua','Quaicoe'],       ['Kwasi','Osei'],        ['Abiba','Forson'],
      ['Fiifi','Kyei'],    ['Maame','Tutu'],         ['Kwabena','Larbi'],     ['Esi','Nsiah'],
      ['Yaw','Attah'],     ['Ama','Atta'],           ['Kofi','Yeboah'],       ['Akosua','Anim'],
      // 61-80
      ['Kwame','Donkor'],  ['Abena','Quansah'],      ['Kojo','Acheampong'],   ['Efua','Badu'],
      ['Kweku','Asante'],  ['Adwoa','Boateng'],      ['Nana','Nyarko'],       ['Akua','Ofori'],
      ['Kwasi','Mensah'],  ['Abiba','Owusu'],        ['Fiifi','Adjei'],       ['Maame','Ansah'],
      ['Kwabena','Tetteh'],['Esi','Asare'],          ['Yaw','Amoah'],         ['Ama','Quaye'],
      ['Kofi','Bonsu'],    ['Akosua','Frimpong'],    ['Kwame','Gyasi'],       ['Abena','Opoku'],
      // 81-100
      ['Kojo','Agyei'],    ['Efua','Antwi'],         ['Kweku','Appiah'],      ['Adwoa','Baffoe'],
      ['Nana','Baah'],     ['Akua','Wiredu'],        ['Kwasi','Obeng'],       ['Abiba','Danso'],
      ['Fiifi','Kusi'],    ['Maame','Sarpong'],      ['Kwabena','Ampofo'],    ['Esi','Ntim'],
      ['Yaw','Aidoo'],     ['Ama','Boakye'],         ['Kofi','Adusei'],       ['Akosua','Fofie'],
      ['Kwame','Nimako'],  ['Abena','Peprah'],       ['Kojo','Asante'],       ['Efua','Osei'],
    ];
    const regions = ['Greater Accra', 'Ashanti', 'Western', 'Central', 'Eastern'];
    for (let i = 0; i < 100; i++) {
      const phone = '0244200' + String(i + 1).padStart(3, '0');
      const [firstName, lastName] = collectorNames[i];
      const region = regions[i % 5];
      await client.query(`
        INSERT INTO collectors (first_name, last_name, phone, pin, region, city)
        VALUES ($1, $2, $3, '0000', $4, 'Accra')
        ON CONFLICT (phone) DO NOTHING
      `, [firstName, lastName, phone, region]);
    }

    // ── 7. Resolve operator IDs for all 5 aggregators (Kwesi first, then the 4 new ones)
    const aggPhones = ['0300000002', '0244100001', '0244100002', '0244100003', '0244100004'];
    const aggOperatorIds = [];
    for (const phone of aggPhones) {
      const r = await client.query(`SELECT id FROM operators WHERE phone = $1`, [phone]);
      aggOperatorIds.push(r.rows[0].id);
    }

    // ── 8. Resolve collectors.id for the 100 new collectors
    const collectorDbIds = [];
    for (let i = 0; i < 100; i++) {
      const phone = '0244200' + String(i + 1).padStart(3, '0');
      const r = await client.query(`SELECT id FROM collectors WHERE phone = $1`, [phone]);
      collectorDbIds.push(r.rows[0].id);
    }

    // ── 9. Collector → Aggregator transactions (8 per collector, skip collectors.id = 9 = Ama)
    //   Idempotent: skip a collector if they already have any transactions.
    const materials       = ['PET', 'HDPE', 'PP', 'LDPE'];
    const collectorPrices = { PET: 2.00, HDPE: 3.00, PP: 2.50, LDPE: 1.00 };
    const nowMs = Date.now();
    const dayMs = 86400000;

    for (let i = 0; i < 100; i++) {
      const collectorId = collectorDbIds[i];
      if (collectorId === 9) continue; // Ama — already has 12 transactions

      // Idempotency: skip if this collector already has seeded transactions
      const existCheck = await client.query(
        `SELECT COUNT(*) FROM transactions WHERE collector_id = $1`, [collectorId]
      );
      if (parseInt(existCheck.rows[0].count) > 0) continue;

      // Collectors 1-20 (i 0-19) → aggOperatorIds[0] (Kwesi), 21-40 → [1], etc.
      const aggOpId = aggOperatorIds[Math.floor(i / 20)];

      for (let t = 0; t < 8; t++) {
        const material    = materials[t % 4];
        const grossKg     = 5 + ((i * 8 + t) * 9) % 76;          // 5–80 kg
        const contamPct   = (i + t * 3) % 16;                     // 0–15 %
        const netKg       = parseFloat((grossKg * (1 - contamPct / 100)).toFixed(2));
        const pricePerKg  = collectorPrices[material];
        const totalPrice  = parseFloat((netKg * pricePerKg).toFixed(2));
        const daysAgo     = Math.round(90 * (1 - t / 7));         // spread over 90 days
        const txDate      = new Date(nowMs - daysAgo * dayMs).toISOString();

        await client.query(`
          INSERT INTO transactions
            (collector_id, operator_id, material_type, gross_weight_kg, net_weight_kg,
             contamination_deduction_percent, price_per_kg, total_price, payment_status, transaction_date)
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'paid', $9)
        `, [collectorId, aggOpId, material, grossKg, netKg, contamPct, pricePerKg, totalPrice, txDate]);
      }
    }

    // ── 10. Resolve processor buyer IDs
    const processorEmails = ['abena@circul.demo', 'info@recycleforce.demo', 'info@greenloop.demo'];
    const processorBuyerIds = [];
    for (const email of processorEmails) {
      const r = await client.query(`SELECT id FROM buyers WHERE email = $1`, [email]);
      processorBuyerIds.push(r.rows[0].id);
    }

    // ── 11. Aggregator → Processor transactions (6 per aggregator)
    //   agg 0,1 → processor 0 (Miniplast); agg 2,3 → processor 1 (RecycleForce); agg 4 → processor 2 (GreenLoop)
    //   Idempotent: skip if any agg→proc rows already exist for this operator_id.
    const aggToProcPrices = { PET: 3.50, HDPE: 4.50, PP: 3.00 };
    const aggToProcMap    = [0, 0, 1, 1, 2]; // processor index per aggregator

    for (let a = 0; a < 5; a++) {
      const aggOpId = aggOperatorIds[a];

      const existAggProc = await client.query(
        `SELECT COUNT(*) FROM transactions WHERE operator_id = $1 AND notes LIKE 'agg_to_proc%'`,
        [aggOpId]
      );
      if (parseInt(existAggProc.rows[0].count) > 0) continue;

      for (let t = 0; t < 6; t++) {
        const material   = materials[t % 3];   // PET, HDPE, PP only
        const grossKg    = 200 + ((a * 6 + t) % 7) * 100;   // 200–800 kg
        const netKg      = parseFloat((grossKg * 0.95).toFixed(2));
        const pricePerKg = aggToProcPrices[material];
        const totalPrice = parseFloat((netKg * pricePerKg).toFixed(2));
        const daysAgo    = Math.round(60 * (1 - t / 5));     // spread over 60 days
        const txDate     = new Date(nowMs - daysAgo * dayMs).toISOString();
        const procId     = processorBuyerIds[aggToProcMap[a]];

        await client.query(`
          INSERT INTO transactions
            (collector_id, operator_id, material_type, gross_weight_kg, net_weight_kg,
             contamination_deduction_percent, price_per_kg, total_price, payment_status,
             notes, transaction_date)
          VALUES (NULL, $1, $2, $3, $4, 5, $5, $6, 'paid', $7, $8)
        `, [aggOpId, material, grossKg, netKg, pricePerKg, totalPrice,
            `agg_to_proc:processor_id=${procId}`, txDate]);
      }
    }

    // ── 12. Resolve converter buyer IDs
    const converterEmails = ['kweku@circul.demo', 'info@iterum.demo'];
    const converterBuyerIds = [];
    for (const email of converterEmails) {
      const r = await client.query(`SELECT id FROM buyers WHERE email = $1`, [email]);
      converterBuyerIds.push(r.rows[0].id);
    }

    // ── 13. Processor → Converter transactions (4 per processor)
    //   proc 0 (Miniplast) → Veolia (all 4)
    //   proc 1 (RecycleForce) → Iterum (all 4)
    //   proc 2 (GreenLoop) → Veolia t=0,1 / Iterum t=2,3
    //   Idempotent: skip if any proc→conv rows exist for this processor_id in notes.
    const procToConvPrices = { PET: 5.00, HDPE: 6.00, PP: 4.50 };
    const procToConvMap = [
      [0, 0, 0, 0], // Miniplast → Veolia
      [1, 1, 1, 1], // RecycleForce → Iterum
      [0, 0, 1, 1], // GreenLoop → Veolia, Veolia, Iterum, Iterum
    ];

    for (let p = 0; p < 3; p++) {
      const procId = processorBuyerIds[p];

      const existProcConv = await client.query(
        `SELECT COUNT(*) FROM transactions WHERE notes LIKE $1`,
        [`proc_to_conv:processor_id=${procId}%`]
      );
      if (parseInt(existProcConv.rows[0].count) > 0) continue;

      for (let t = 0; t < 4; t++) {
        const material   = materials[t % 3];   // PET, HDPE, PP
        const grossKg    = 500 + (p * 4 + t) * 83;   // 500–1413 kg
        const netKg      = parseFloat((grossKg * 0.95).toFixed(2));
        const pricePerKg = procToConvPrices[material];
        const totalPrice = parseFloat((netKg * pricePerKg).toFixed(2));
        const daysAgo    = Math.round(30 * (1 - t / 3));     // spread over 30 days
        const txDate     = new Date(nowMs - daysAgo * dayMs).toISOString();
        const convId     = converterBuyerIds[procToConvMap[p][t]];

        await client.query(`
          INSERT INTO transactions
            (collector_id, operator_id, material_type, gross_weight_kg, net_weight_kg,
             contamination_deduction_percent, price_per_kg, total_price, payment_status,
             notes, transaction_date)
          VALUES (NULL, NULL, $1, $2, $3, 5, $4, $5, 'paid', $6, $7)
        `, [material, grossKg, netKg, pricePerKg, totalPrice,
            `proc_to_conv:processor_id=${procId},converter_id=${convId}`, txDate]);
      }
    }

    // ── 14. Posted prices for all 5 aggregators (PET, HDPE, PP, LDPE)
    //   Uses ON CONFLICT DO UPDATE for idempotency.
    const ppPrices   = { PET: 2.00, HDPE: 3.00, PP: 2.50, LDPE: 1.00 };
    const ppMaterials = ['PET', 'HDPE', 'PP', 'LDPE'];
    const expiresAt  = '2026-12-31T23:59:59Z';

    for (const opId of aggOperatorIds) {
      for (const mat of ppMaterials) {
        await client.query(`
          INSERT INTO posted_prices
            (operator_id, material_type, price_per_kg_usd, price_per_kg_ghs,
             city, region, country, expires_at, is_active)
          VALUES ($1, $2, 0, $3, 'Accra', 'Greater Accra', 'Ghana', $4, true)
          ON CONFLICT (operator_id, material_type, expires_at)
            DO UPDATE SET price_per_kg_ghs = EXCLUDED.price_per_kg_ghs, is_active = true
        `, [opId, mat, ppPrices[mat], expiresAt]);
      }
    }

  }
};
