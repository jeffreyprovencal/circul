/**
 * Seeds posted buy-prices for the demo recycler (Poly Recycling, poly@circul.demo).
 * Prices are in USD with GHS equivalent at 14.50 rate.
 *
 * Exports the standard { name, up } migration interface expected by migrate.js.
 * (Previously exported a bare async function, which the runner couldn't invoke —
 * migration.up was undefined. This migration never actually ran in production.)
 */
module.exports = {
  name: 'seed_recycler_prices',
  up: async (client) => {
    const recycler = await client.query(
      `SELECT id, city, region, country FROM recyclers WHERE email='poly@circul.demo' LIMIT 1`
    );
    if (!recycler.rows.length) {
      console.log('  ⏭  Recycler poly@circul.demo not found — skipping price seed');
      return;
    }

    const r = recycler.rows[0];
    const rate = 14.50;
    const prices = [
      { material: 'PET',  usd: 5.00 },
      { material: 'HDPE', usd: 4.50 },
      { material: 'PP',   usd: 4.00 },
      { material: 'LDPE', usd: 3.50 }
    ];

    const expiresAt = new Date();
    expiresAt.setMonth(expiresAt.getMonth() + 1);
    expiresAt.setDate(0);
    expiresAt.setHours(23, 59, 59);

    for (const p of prices) {
      await client.query(
        `INSERT INTO posted_prices
           (poster_type, poster_id, material_type, price_per_kg_usd, price_per_kg_ghs, usd_to_ghs_rate, city, region, country, expires_at)
         VALUES ('recycler', $1, $2, $3, $4, $5, $6, $7, $8, $9)
         ON CONFLICT (poster_type, poster_id, material_type) DO UPDATE
           SET price_per_kg_usd=$3, price_per_kg_ghs=$4, usd_to_ghs_rate=$5, posted_at=NOW(), is_active=true, expires_at=$9`,
        [r.id, p.material, p.usd, parseFloat((p.usd * rate).toFixed(2)), rate, r.city, r.region, r.country, expiresAt.toISOString()]
      );
    }

    console.log('  ✅ Seeded 4 recycler buy-prices for Poly Recycling');
  }
};
