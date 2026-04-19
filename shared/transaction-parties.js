// shared/transaction-parties.js
// Single source of truth for resolving the seller and buyer of a
// pending_transactions (or transactions) row.
//
// Why this exists: sellers are single-kind per transaction_type, but buyers
// are polymorphic for two of the five transaction types. We express the
// polymorphism explicitly via PARTY_MAP's buyerKinds array (priority order)
// and two strict resolvers (resolveBuyer / resolveSeller) that throw on
// invalid row states.
//
// Buyer polymorphism matrix (confirmed from server.js insert sites):
//   collector_sale       seller=collector    buyer=aggregator
//   aggregator_purchase  seller=collector    buyer=aggregator
//   aggregator_sale      seller=aggregator   buyer=processor | recycler | converter
//   processor_sale       seller=processor    buyer=recycler  | converter
//   recycler_sale        seller=recycler     buyer=converter
//
// Miniplast-style entities (multi-tier companies classified by their DEEPEST
// tier — e.g. a process+recycle+convert shop is recorded as a converter) are
// the canonical reason aggregator_sale must allow a converter buyer: the
// aggregator sells direct to the converter, chain-of-custody tracking stops
// at that boundary. See project_circul_miniplast_multi_tier.md for full
// rationale.
//
// Ambiguous-row policy: if a row has more than one buyer FK populated (e.g.
// an aggregator_sale with BOTH processor_id and converter_id set), the
// resolver THROWS. Silent tie-breaking would hide data-integrity bugs. The
// server insert paths are expected to enforce exactly-one-buyer-FK on write
// (follow-up hardening tracked separately).

const PARTY_MAP = {
  collector_sale:      { sellerKind: 'collector',  buyerKinds: ['aggregator'] },
  aggregator_purchase: { sellerKind: 'collector',  buyerKinds: ['aggregator'] },
  aggregator_sale:     { sellerKind: 'aggregator', buyerKinds: ['processor', 'recycler', 'converter'] },
  processor_sale:      { sellerKind: 'processor',  buyerKinds: ['recycler', 'converter'] },
  recycler_sale:       { sellerKind: 'recycler',   buyerKinds: ['converter'] }
};

const KIND_TO_TABLE = {
  collector:  { table: 'collectors',  nameSql: "first_name || ' ' || last_name" },
  aggregator: { table: 'aggregators', nameSql: 'name' },
  processor:  { table: 'processors',  nameSql: 'name' },
  recycler:   { table: 'recyclers',   nameSql: 'name' },
  converter:  { table: 'converters',  nameSql: 'name' }
};

// ── Strict resolvers ──────────────────────────────────────────────────────
//
// Throw with a descriptive message on any invalid row state. Use these when
// you want error propagation (e.g. PR3's insert-path enforcement, or any
// code that wants loud failures on malformed rows). For lenient resolution
// that returns null-filled defaults instead, use resolveParties below.

/**
 * Resolve the seller of a row. Throws if the row's transaction_type is
 * unknown, or if the expected seller FK column is null.
 *
 * @param {object} row  A pending_transactions or transactions row.
 * @returns {{kind: string, id: number}}
 * @throws {Error} on unknown transaction_type or missing seller FK.
 */
function resolveSeller(row) {
  if (!row || !row.transaction_type) {
    throw new Error('resolveSeller: row missing or has no transaction_type');
  }
  const cfg = PARTY_MAP[row.transaction_type];
  if (!cfg) {
    throw new Error('resolveSeller: unknown transaction_type: ' + row.transaction_type);
  }
  const id = row[cfg.sellerKind + '_id'];
  if (id == null) {
    throw new Error(
      'resolveSeller: no seller FK set for ' + row.transaction_type +
      ' row id=' + row.id + '; expected ' + cfg.sellerKind + '_id'
    );
  }
  return { kind: cfg.sellerKind, id: Number(id) };
}

/**
 * Resolve the buyer of a row. Iterates PARTY_MAP[type].buyerKinds in priority
 * order and returns the single populated FK. Throws on:
 *   - unknown transaction_type
 *   - zero populated buyer FKs (invalid row)
 *   - more than one populated buyer FK (ambiguous; schema should prevent this)
 *
 * @param {object} row
 * @returns {{kind: string, id: number}}
 * @throws {Error} on invalid/ambiguous state.
 */
function resolveBuyer(row) {
  if (!row || !row.transaction_type) {
    throw new Error('resolveBuyer: row missing or has no transaction_type');
  }
  const cfg = PARTY_MAP[row.transaction_type];
  if (!cfg) {
    throw new Error('resolveBuyer: unknown transaction_type: ' + row.transaction_type);
  }

  const matches = [];
  for (const kind of cfg.buyerKinds) {
    const id = row[kind + '_id'];
    if (id != null) matches.push({ kind: kind, id: Number(id) });
  }

  if (matches.length === 0) {
    throw new Error(
      'resolveBuyer: no buyer FK set for ' + row.transaction_type +
      ' row id=' + row.id + '; expected one of: ' +
      cfg.buyerKinds.map(function (k) { return k + '_id'; }).join(', ')
    );
  }
  if (matches.length > 1) {
    const set = matches.map(function (m) { return m.kind + '_id=' + m.id; }).join(', ');
    throw new Error(
      'resolveBuyer: ambiguous buyer for ' + row.transaction_type +
      ' row id=' + row.id + '; multiple buyer FKs set: ' + set +
      '. Exactly one must be set.'
    );
  }
  return matches[0];
}

/**
 * Write-boundary buyer-FK validator. Same polymorphism rules as resolveBuyer
 * but returns a result object suitable for 400-response generation instead
 * of throwing. Callers: generic POST /api/pending-transactions and the
 * dedicated /api/pending-transactions/aggregator-sale endpoint.
 *
 * Only the FKs listed in PARTY_MAP[transaction_type].buyerKinds are
 * considered. Irrelevant FKs (e.g. collector_id passed on an aggregator_sale
 * body) are IGNORED — this helper's job is polymorphic-buyer resolution,
 * not full input-shape validation. Seller-FK checks belong on the caller.
 *
 * @param {string} transaction_type
 * @param {object} ids  Object whose keys may include aggregator_id,
 *                      processor_id, converter_id, recycler_id. Keys
 *                      outside PARTY_MAP[type].buyerKinds are ignored.
 * @returns {{ok: true, kind: string, id: number}}
 *        | {ok: false, message: string}
 */
function validateBuyerFks(transaction_type, ids) {
  const cfg = PARTY_MAP[transaction_type];
  if (!cfg) {
    return { ok: false, message: 'unknown transaction_type: ' + transaction_type };
  }
  ids = ids || {};
  const kinds = cfg.buyerKinds;

  const populated = [];
  for (const kind of kinds) {
    const id = ids[kind + '_id'];
    if (id != null && id !== '') populated.push({ kind: kind, id: Number(id) });
  }

  if (populated.length === 0) {
    const expected = kinds.map(function (k) { return k + '_id'; });
    const needs = expected.length === 1
      ? expected[0] + ' is required for ' + transaction_type
      : 'one of ' + expected.join(', ') + ' is required for ' + transaction_type;
    return { ok: false, message: needs };
  }

  if (populated.length > 1) {
    const got = populated.map(function (p) { return p.kind + '_id'; }).join(', ');
    return {
      ok: false,
      message: 'only one buyer FK may be set for ' + transaction_type + ' (got: ' + got + ')'
    };
  }

  return { ok: true, kind: populated[0].kind, id: populated[0].id };
}

// ── Lenient facade ────────────────────────────────────────────────────────

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

/**
 * Lenient resolver for SMS / payment-auth callers that want null-filled
 * defaults rather than exceptions on malformed rows. Internally wraps the
 * strict resolvers in try/catch. Returns `{seller, buyer, sellerKind,
 * buyerKind, material, qty, amount, ref}` where any field can be null if
 * the corresponding resolution failed.
 *
 * Backward compatible with pre-amendment callers: return shape unchanged,
 * and buyerKind now correctly resolves to 'converter'/'recycler' for
 * aggregator_sale rows that route direct to those tiers (previously
 * returned null buyer because the hard-coded 'processor' branch misrouted).
 */
async function resolveParties(pool, row) {
  // The `transactions` table has no transaction_type column — see
  // migrations/1774500000000_restructure_tiers.js:125-148, which created it
  // as collector→aggregator-only with the type implicit in schema shape
  // (collector_id NOT NULL, aggregator_id REFERENCES aggregators). Any row
  // read via SELECT * FROM transactions falls through the guard below and
  // returns {buyerKind: null}, which breaks the payment-auth routes at
  // server.js:4884 / 4918 (both call userOwnsParty with a null kind → 403).
  //
  // Infer transaction_type='collector_sale' when the row has both FKs set
  // but no transaction_type. Shallow-copy so we never mutate the caller's
  // row. pending_transactions rows always carry transaction_type (NOT NULL
  // default 'aggregator_sale'), so this guard is a no-op for them.
  if (row && !row.transaction_type && row.collector_id != null && row.aggregator_id != null) {
    row = Object.assign({}, row, { transaction_type: 'collector_sale' });
  }

  if (!row || !row.transaction_type || !PARTY_MAP[row.transaction_type]) {
    return { seller: null, buyer: null, sellerKind: null, buyerKind: null,
             material: null, qty: null, amount: null, ref: null };
  }

  let sellerInfo = null;
  let buyerInfo = null;
  try { sellerInfo = resolveSeller(row); } catch (_e) { /* leniently ignore */ }
  try { buyerInfo  = resolveBuyer(row);  } catch (_e) { /* leniently ignore */ }

  const [seller, buyer] = await Promise.all([
    sellerInfo ? _lookupParty(pool, sellerInfo.kind, sellerInfo.id) : null,
    buyerInfo  ? _lookupParty(pool, buyerInfo.kind,  buyerInfo.id)  : null
  ]);

  const ref = 'TXN-' +
    (row.created_at ? new Date(row.created_at).toISOString().slice(0, 10).replace(/-/g, '') : 'XXXXXXXX') +
    '-' + String(row.id).padStart(4, '0');

  return {
    seller: seller,
    buyer: buyer,
    sellerKind: sellerInfo ? sellerInfo.kind : null,
    buyerKind:  buyerInfo  ? buyerInfo.kind  : null,
    material: row.material_type,
    qty: row.gross_weight_kg != null ? parseFloat(row.gross_weight_kg).toFixed(0) : null,
    amount: row.total_price != null ? parseFloat(row.total_price).toFixed(2) : null,
    ref: ref
  };
}

function userOwnsParty(user, kind, partyId) {
  if (!user || partyId == null) return false;
  const roles = Array.isArray(user.roles) ? user.roles : (user.role ? [user.role] : []);
  if (!roles.includes(kind)) return false;
  if (kind === 'converter' && user.converter_id != null) {
    return Number(user.converter_id) === Number(partyId);
  }
  return Number(user.id) === Number(partyId);
}

module.exports = {
  PARTY_MAP: PARTY_MAP,
  KIND_TO_TABLE: KIND_TO_TABLE,
  resolveSeller: resolveSeller,
  resolveBuyer: resolveBuyer,
  validateBuyerFks: validateBuyerFks,
  resolveParties: resolveParties,
  userOwnsParty: userOwnsParty
};
