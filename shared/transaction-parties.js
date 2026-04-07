// shared/transaction-parties.js
// Single source of truth for resolving seller/buyer party info from a
// transactions or pending_transactions row, for SMS notifications.
//
// Mapping: which side of a transaction is the SELLER (gets paid) vs BUYER.
//   collector_sale       → seller=collector,  buyer=aggregator
//   aggregator_purchase  → seller=collector,  buyer=aggregator
//   aggregator_sale      → seller=aggregator, buyer=processor
//   processor_sale       → seller=processor,  buyer=recycler OR converter
//   recycler_sale        → seller=recycler,   buyer=converter

const PARTY_MAP = {
  collector_sale:      { sellerKind: 'collector',  buyerKind: 'aggregator' },
  aggregator_purchase: { sellerKind: 'collector',  buyerKind: 'aggregator' },
  aggregator_sale:     { sellerKind: 'aggregator', buyerKind: 'processor'  },
  processor_sale:      { sellerKind: 'processor',  buyerKind: 'recycler_or_converter' },
  recycler_sale:       { sellerKind: 'recycler',   buyerKind: 'converter'  }
};

const KIND_TO_TABLE = {
  collector:  { table: 'collectors',  nameSql: "first_name || ' ' || last_name" },
  aggregator: { table: 'aggregators', nameSql: 'name' },
  processor:  { table: 'processors',  nameSql: 'name' },
  recycler:   { table: 'recyclers',   nameSql: 'name' },
  converter:  { table: 'converters',  nameSql: 'name' }
};

async function _lookupParty(pool, kind, id) {
  if (!id) return null;
  const cfg = KIND_TO_TABLE[kind];
  if (!cfg) return null;
  const row = (await pool.query(
    `SELECT id, phone, ${cfg.nameSql} AS name FROM ${cfg.table} WHERE id = $1`,
    [id]
  )).rows[0];
  return row || null;
}

async function resolveParties(pool, row) {
  if (!row || !row.transaction_type) {
    return { seller: null, buyer: null, material: null, qty: null, amount: null, ref: null };
  }
  const cfg = PARTY_MAP[row.transaction_type];
  if (!cfg) {
    return { seller: null, buyer: null, material: null, qty: null, amount: null, ref: null };
  }

  let buyerKind = cfg.buyerKind;
  let buyerId;
  if (cfg.buyerKind === 'recycler_or_converter') {
    if (row.recycler_id) { buyerKind = 'recycler'; buyerId = row.recycler_id; }
    else if (row.converter_id) { buyerKind = 'converter'; buyerId = row.converter_id; }
  } else {
    buyerId = row[buyerKind + '_id'];
  }
  const sellerKind = cfg.sellerKind;
  const sellerId = row[sellerKind + '_id'];

  const [seller, buyer] = await Promise.all([
    _lookupParty(pool, sellerKind, sellerId),
    _lookupParty(pool, buyerKind, buyerId)
  ]);

  const ref = 'TXN-' +
    (row.created_at ? new Date(row.created_at).toISOString().slice(0, 10).replace(/-/g, '') : 'XXXXXXXX') +
    '-' + String(row.id).padStart(4, '0');

  return {
    seller,
    buyer,
    material: row.material_type,
    qty: row.gross_weight_kg != null ? parseFloat(row.gross_weight_kg).toFixed(0) : null,
    amount: row.total_price != null ? parseFloat(row.total_price).toFixed(2) : null,
    ref
  };
}

module.exports = { resolveParties, PARTY_MAP, KIND_TO_TABLE };
