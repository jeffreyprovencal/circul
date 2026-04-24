// shared/chain-of-custody-db.js
//
// DB-bound orchestrators for chain-of-custody writes. Composes the pure
// pure-function algorithm in shared/chain-of-custody.js with the PARTY_MAP
// resolvers in shared/transaction-parties.js.
//
// Two entry points:
//   - attributeAndInsert(client, target)  — for downstream sales
//                                           (aggregator_sale, processor_sale,
//                                            recycler_sale). Locks candidate
//                                           sources via SELECT ... FOR UPDATE,
//                                           runs FIFO attribution, rejects
//                                           on shortfall, inserts junction
//                                           edges + decrements source
//                                           remaining_kg + inserts the new
//                                           row with inherited batch_id.
//   - insertRootTransaction(client, target)— for root types (collector_sale,
//                                           aggregator_purchase). Generates a
//                                           fresh batch_id and sets
//                                           remaining_kg = gross_weight_kg.
//                                           No junction writes.
//
// Transaction handling: both helpers expect the CALLER to own BEGIN/COMMIT.
// They take a pg `client` (from pool.connect()) and run raw queries against
// it. This keeps transaction boundaries visible at the endpoint and matches
// the existing pattern for payment routes (server.js:4663+).
//
// Postgres isolation: default READ COMMITTED. SELECT ... FOR UPDATE on the
// candidate set blocks concurrent writers against the same source rows
// until the holding transaction commits.
//
// Error path: shortfall → throw InsufficientSourceError. Endpoint maps to
// 400 with a copy-pasteable diagnostic message. Any other error (pg
// failure, malformed target) propagates and rolls back the caller's txn.

'use strict';

const crypto = require('crypto');
const { resolveSeller } = require('./transaction-parties');
const {
  computeWriteAttribution,
  EXCLUDED_STATUSES,
  ROOT_TYPES
} = require('./chain-of-custody');

const WINDOW_DAYS = 30;

// ── InsufficientSourceError ────────────────────────────────────────────────
//
// Two shapes:
//   reason='shortfall' (default, existing)   — FIFO path couldn't cover target.
//   reason='invalid_manual_sources' (PR4-A) — caller-supplied target.sources
//                                             hint failed validation.
//
// Diagnostic fields populated depend on reason:
//   shortfall:                shortfall_kg, candidates_considered, candidates_total_remaining_kg
//   invalid_manual_sources:   any of invalid_source_ids, insufficient_remaining,
//                             sum_mismatch_kg / hint_total_kg / target_kg,
//                             invalid_shape_entries
//
// Frontend uses `reason` to switch the UX (e.g. highlight specific rows vs
// show "pick more inventory" toast).
class InsufficientSourceError extends Error {
  constructor(params) {
    const { target, seller, reason = 'shortfall' } = params;
    const targetName = seller ? (seller.kind + ' ' + seller.id) : 'seller';

    let message;
    if (reason === 'invalid_manual_sources') {
      const parts = [];
      if (params.invalid_shape_entries && params.invalid_shape_entries.length) {
        parts.push('invalid hint entries: ' + params.invalid_shape_entries.length);
      }
      if (params.invalid_source_ids && params.invalid_source_ids.length) {
        parts.push('invalid source_ids: [' + params.invalid_source_ids.join(', ') + ']');
      }
      if (params.insufficient_remaining && params.insufficient_remaining.length) {
        parts.push('insufficient remaining: ' + params.insufficient_remaining.map(function (x) {
          return '#' + x.id + ' (' + x.remaining_kg + 'kg < requested ' + x.requested_kg + 'kg)';
        }).join(', '));
      }
      if (params.sum_mismatch_kg != null) {
        parts.push('sum mismatch: hint ' + params.hint_total_kg + 'kg ≠ target ' +
                   params.target_kg + 'kg (delta ' + params.sum_mismatch_kg + 'kg)');
      }
      message = 'Invalid manual source hint for ' + target.transaction_type +
                ' (' + targetName + ', target ' + target.gross_weight_kg + 'kg ' +
                target.material_type + '): ' + (parts.length ? parts.join('; ') : 'no diagnostic detail');
    } else {
      // shortfall
      const availableKg = params.candidates_total_remaining_kg != null ? params.candidates_total_remaining_kg : 0;
      message = 'Insufficient source material: tried to attribute ' +
        target.gross_weight_kg + 'kg ' + target.material_type +
        ' from ' + targetName +
        ', but only ' + availableKg + 'kg is available within the ' +
        WINDOW_DAYS + '-day window (shortfall: ' + params.shortfall_kg + 'kg).';
    }

    super(message);
    this.name = 'InsufficientSourceError';
    this.reason = reason;
    this.target = target;
    this.seller = seller;

    if (reason === 'shortfall') {
      this.shortfall_kg = params.shortfall_kg;
      this.candidates_considered = params.candidates_considered;
      this.candidates_total_remaining_kg = params.candidates_total_remaining_kg != null ? params.candidates_total_remaining_kg : 0;
    } else {
      // invalid_manual_sources
      if (params.invalid_shape_entries != null) this.invalid_shape_entries = params.invalid_shape_entries;
      if (params.invalid_source_ids != null)    this.invalid_source_ids = params.invalid_source_ids;
      if (params.insufficient_remaining != null) this.insufficient_remaining = params.insufficient_remaining;
      if (params.sum_mismatch_kg != null)       this.sum_mismatch_kg = params.sum_mismatch_kg;
      if (params.hint_total_kg != null)         this.hint_total_kg = params.hint_total_kg;
      if (params.target_kg != null)             this.target_kg = params.target_kg;
    }
  }
}

// ── Candidate WHERE-clause synthesis ───────────────────────────────────────
//
// Given a seller (kind, id) and material, build the SQL clause that selects
// rows whose buyer matches that seller. Derived from PARTY_MAP rather than a
// hardcoded switch: for each downstream transaction_type, there's exactly
// one column+transaction_type combination that identifies "rows where buyer
// is this seller".
//
// aggregator (sells to processor/recycler/converter) draws from:
//   rows where aggregator_id = seller.id
//       AND transaction_type IN ('collector_sale','aggregator_purchase')
// processor draws from:
//   rows where processor_id = seller.id
//       AND transaction_type = 'aggregator_sale'
// recycler draws from:
//   rows where recycler_id = seller.id
//       AND transaction_type IN ('aggregator_sale','processor_sale')
function candidateFilterForSeller(seller) {
  if (seller.kind === 'aggregator') {
    return {
      fkColumn: 'aggregator_id',
      parentTypes: ['collector_sale', 'aggregator_purchase']
    };
  }
  if (seller.kind === 'processor') {
    return {
      fkColumn: 'processor_id',
      parentTypes: ['aggregator_sale']
    };
  }
  if (seller.kind === 'recycler') {
    return {
      fkColumn: 'recycler_id',
      parentTypes: ['aggregator_sale', 'processor_sale']
    };
  }
  throw new Error(
    'candidateFilterForSeller: no candidate lookup defined for seller.kind=' +
    seller.kind + ' (downstream write paths should only originate from aggregator/processor/recycler)'
  );
}

// ── attributeAndInsert ─────────────────────────────────────────────────────
/**
 * Attribute FIFO sources, insert the new downstream row, insert junction
 * edges, decrement source remaining_kg. Caller owns BEGIN/COMMIT.
 *
 * @param {pg.Client} client — already connected, inside BEGIN
 * @param {object} target — downstream row spec; see README in file header
 * @returns {{ row: object, sources: Array<{id, weight_kg_attributed}> }}
 * @throws {InsufficientSourceError} on shortfall (caller maps to 400)
 * @throws {Error} on any other failure (caller rolls back)
 */
async function attributeAndInsert(client, target) {
  if (!target || !target.transaction_type) {
    throw new Error('attributeAndInsert: target.transaction_type required');
  }
  if (ROOT_TYPES[target.transaction_type]) {
    throw new Error(
      'attributeAndInsert: root type ' + target.transaction_type +
      ' must use insertRootTransaction'
    );
  }
  if (!target.material_type) {
    throw new Error('attributeAndInsert: target.material_type required');
  }
  if (target.gross_weight_kg == null) {
    throw new Error('attributeAndInsert: target.gross_weight_kg required');
  }

  const seller = resolveSeller(target);  // throws on missing/unknown — caller bug
  const filter = candidateFilterForSeller(seller);

  // Detect manual-source-hint mode. An empty array falls through to FIFO
  // (same as "no hint supplied").
  const hasHint = Array.isArray(target.sources) && target.sources.length > 0;

  // Diagnostic target snapshot used by InsufficientSourceError in both paths.
  const errTarget = {
    transaction_type: target.transaction_type,
    material_type: target.material_type,
    gross_weight_kg: parseFloat(target.gross_weight_kg)
  };

  let plan;

  if (hasHint) {
    // ── Manual source-hint path ──────────────────────────────────────────
    //
    // Load ONLY the hinted rows, applying the same filters FIFO would have
    // applied (seller FK, material, remaining_kg > 0, window, status, parent
    // types). Any hint row that doesn't come back from that query is invalid
    // — out-of-scope, wrong material, outside window, rejected status, etc.
    // — caller's frontend should surface the row as unusable.
    //
    // No ORDER BY: hint order is authoritative for dominant-source selection.

    // Validate hint shape first (fail fast before round-tripping to DB).
    const invalidShape = [];
    for (let i = 0; i < target.sources.length; i++) {
      const s = target.sources[i];
      if (!s || typeof s !== 'object' || s.source_id == null ||
          typeof s.kg !== 'number' || !isFinite(s.kg) || s.kg <= 0) {
        invalidShape.push({ index: i, entry: s });
      }
    }
    if (invalidShape.length > 0) {
      throw new InsufficientSourceError({
        target: errTarget,
        seller: seller,
        reason: 'invalid_manual_sources',
        invalid_shape_entries: invalidShape
      });
    }

    const hintIds = target.sources.map(function (s) { return Number(s.source_id); });

    const excludedListH = EXCLUDED_STATUSES.map(function (_, i) { return '$' + (i + 5); }).join(', ');
    const parentTypesListH = filter.parentTypes.map(function (_, i) {
      return '$' + (i + 5 + EXCLUDED_STATUSES.length);
    }).join(', ');

    const sqlHint =
      'SELECT id, gross_weight_kg, remaining_kg, batch_id, created_at ' +
      '  FROM pending_transactions ' +
      ' WHERE id = ANY($1::int[]) ' +
      '   AND ' + filter.fkColumn + ' = $2 ' +
      '   AND material_type = $3 ' +
      '   AND remaining_kg > 0 ' +
      '   AND created_at >= NOW() - ($4 || \' days\')::INTERVAL ' +
      '   AND status NOT IN (' + excludedListH + ') ' +
      '   AND transaction_type IN (' + parentTypesListH + ') ' +
      ' FOR UPDATE';

    const paramsHint = [hintIds, seller.id, target.material_type, String(WINDOW_DAYS)]
      .concat(EXCLUDED_STATUSES)
      .concat(filter.parentTypes);

    const hintResult = await client.query(sqlHint, paramsHint);
    const hintRows = hintResult.rows;
    const hintRowById = Object.create(null);
    hintRows.forEach(function (r) { hintRowById[Number(r.id)] = r; });

    // (a) Every hint source_id must be present in the filtered query result.
    const invalidIds = hintIds.filter(function (id) { return !(id in hintRowById); });
    if (invalidIds.length > 0) {
      throw new InsufficientSourceError({
        target: errTarget,
        seller: seller,
        reason: 'invalid_manual_sources',
        invalid_source_ids: invalidIds
      });
    }

    // (b) Each hint's kg must fit the row's remaining_kg (epsilon for float).
    const insufficientRemaining = [];
    for (let i = 0; i < target.sources.length; i++) {
      const s = target.sources[i];
      const row = hintRowById[Number(s.source_id)];
      const rem = parseFloat(row.remaining_kg);
      if (rem + 0.0001 < s.kg) {
        insufficientRemaining.push({
          id: Number(s.source_id),
          remaining_kg: Math.round(rem * 100) / 100,
          requested_kg: Math.round(s.kg * 100) / 100
        });
      }
    }
    if (insufficientRemaining.length > 0) {
      throw new InsufficientSourceError({
        target: errTarget,
        seller: seller,
        reason: 'invalid_manual_sources',
        insufficient_remaining: insufficientRemaining
      });
    }

    // (c) Sum of hint.kg must equal target.gross_weight_kg (the attribution
    //     invariant — Check 1 in scripts/chain-of-custody-invariant.js).
    const hintTotal = target.sources.reduce(function (a, s) { return a + s.kg; }, 0);
    const targetKg = parseFloat(target.gross_weight_kg);
    const delta = Math.round((hintTotal - targetKg) * 100) / 100;
    if (Math.abs(delta) > 0.01) {
      throw new InsufficientSourceError({
        target: errTarget,
        seller: seller,
        reason: 'invalid_manual_sources',
        sum_mismatch_kg: delta,
        hint_total_kg: Math.round(hintTotal * 100) / 100,
        target_kg: Math.round(targetKg * 100) / 100
      });
    }

    // Build plan from the hint directly. Dominant-source batch_id inheritance:
    // largest kg wins; ties broken by source created_at ASC, then source id ASC
    // (matches the FIFO rule enforced by computeWriteAttribution).
    const ranked = target.sources.slice().sort(function (a, b) {
      if (b.kg !== a.kg) return b.kg - a.kg;
      const ra = hintRowById[Number(a.source_id)];
      const rb = hintRowById[Number(b.source_id)];
      const ta = new Date(ra.created_at).getTime();
      const tb = new Date(rb.created_at).getTime();
      if (ta !== tb) return ta - tb;
      return Number(a.source_id) - Number(b.source_id);
    });
    const dominantRow = hintRowById[Number(ranked[0].source_id)];

    plan = {
      edges: target.sources.map(function (s) {
        return {
          source_id: Number(s.source_id),
          weight_kg_attributed: Math.round(s.kg * 100) / 100
        };
      }),
      sourceRemainingAfter: target.sources.map(function (s) {
        const row = hintRowById[Number(s.source_id)];
        return {
          id: Number(s.source_id),
          remaining_kg: Math.round((parseFloat(row.remaining_kg) - s.kg) * 100) / 100
        };
      }),
      batch_id: dominantRow ? dominantRow.batch_id : null,
      shortfall_kg: 0
    };

  } else {
    // ── FIFO auto-attribution path (default, existing behavior) ──────────
    const excludedList = EXCLUDED_STATUSES.map(function (_, i) { return '$' + (i + 4); }).join(', ');
    const parentTypesList = filter.parentTypes.map(function (_, i) {
      return '$' + (i + 4 + EXCLUDED_STATUSES.length);
    }).join(', ');

    const sql =
      'SELECT id, gross_weight_kg, remaining_kg, batch_id, created_at ' +
      '  FROM pending_transactions ' +
      ' WHERE ' + filter.fkColumn + ' = $1 ' +
      '   AND material_type = $2 ' +
      '   AND remaining_kg > 0 ' +
      '   AND created_at >= NOW() - ($3 || \' days\')::INTERVAL ' +
      '   AND status NOT IN (' + excludedList + ') ' +
      '   AND transaction_type IN (' + parentTypesList + ') ' +
      ' ORDER BY created_at ASC, id ASC ' +
      ' FOR UPDATE';

    const params = [seller.id, target.material_type, String(WINDOW_DAYS)]
      .concat(EXCLUDED_STATUSES)
      .concat(filter.parentTypes);

    const candResult = await client.query(sql, params);
    const candidates = candResult.rows;

    plan = computeWriteAttribution(target, candidates);

    if (plan.shortfall_kg > 0) {
      const totalRemaining = candidates.reduce(function (acc, c) {
        return acc + parseFloat(c.remaining_kg);
      }, 0);
      throw new InsufficientSourceError({
        target: errTarget,
        shortfall_kg: plan.shortfall_kg,
        candidates_considered: candidates.length,
        candidates_total_remaining_kg: Math.round(totalRemaining * 100) / 100,
        seller: seller
      });
    }
  }

  // 3. Decrement source remaining_kg in one batched UPDATE.
  if (plan.sourceRemainingAfter.length > 0) {
    const placeholders = [];
    const values = [];
    plan.sourceRemainingAfter.forEach(function (s, idx) {
      const off = idx * 2;
      placeholders.push('($' + (off + 1) + '::int, $' + (off + 2) + '::numeric)');
      values.push(s.id, s.remaining_kg);
    });
    await client.query(
      'UPDATE pending_transactions AS pt ' +
      '   SET remaining_kg = v.remaining_kg ' +
      '  FROM (VALUES ' + placeholders.join(', ') + ') AS v(id, remaining_kg) ' +
      ' WHERE pt.id = v.id',
      values
    );
  }

  // 4. Insert the new downstream row. batch_id inherited from dominant source.
  //    remaining_kg = gross_weight_kg (nothing has drawn from it yet).
  const insertResult = await client.query(
    'INSERT INTO pending_transactions (' +
    '  transaction_type, status, ' +
    '  aggregator_id, processor_id, converter_id, recycler_id, collector_id, ' +
    '  material_type, gross_weight_kg, net_weight_kg, price_per_kg, total_price, ' +
    '  batch_id, remaining_kg, ' +
    '  source, notes, ' +
    '  photos_required, photos_submitted, photo_urls, dispatch_approved' +
    ') VALUES (' +
    '  $1, COALESCE($2, \'pending\'), ' +
    '  $3, $4, $5, $6, $7, ' +
    '  $8, $9::numeric, $10::numeric, $11, $12, ' +
    '  $13::uuid, $9::numeric, ' +
    '  COALESCE($14, \'direct\'), $15, ' +
    '  COALESCE($16, false), COALESCE($17, false), COALESCE($18::text[], \'{}\'::text[]), COALESCE($19, false)' +
    ') RETURNING *',
    [
      target.transaction_type,
      target.status || null,
      target.aggregator_id || null,
      target.processor_id || null,
      target.converter_id || null,
      target.recycler_id || null,
      target.collector_id || null,
      target.material_type,
      target.gross_weight_kg,
      target.net_weight_kg != null ? target.net_weight_kg : target.gross_weight_kg,
      target.price_per_kg != null ? target.price_per_kg : 0,
      target.total_price != null ? target.total_price : 0,
      plan.batch_id,
      target.source || null,
      target.notes != null ? target.notes : null,
      target.photos_required != null ? target.photos_required : null,
      target.photos_submitted != null ? target.photos_submitted : null,
      target.photo_urls || null,
      target.dispatch_approved != null ? target.dispatch_approved : null
    ]
  );

  const insertedRow = insertResult.rows[0];
  const childId = insertedRow.id;

  // 5. Insert junction edges.
  if (plan.edges.length > 0) {
    const placeholders = [];
    const values = [];
    plan.edges.forEach(function (e, idx) {
      const off = idx * 3;
      placeholders.push('($' + (off + 1) + '::int, $' + (off + 2) + '::int, $' + (off + 3) + '::numeric)');
      values.push(childId, e.source_id, e.weight_kg_attributed);
    });
    await client.query(
      'INSERT INTO pending_transaction_sources ' +
      '  (child_pending_tx_id, source_pending_tx_id, weight_kg_attributed) ' +
      'VALUES ' + placeholders.join(', '),
      values
    );
  }

  return {
    row: insertedRow,
    sources: plan.edges.map(function (e) {
      return { id: e.source_id, weight_kg_attributed: e.weight_kg_attributed };
    })
  };
}

// ── insertRootTransaction ──────────────────────────────────────────────────
/**
 * Insert a root pending_transactions row (collector_sale / aggregator_purchase).
 * Generates a fresh batch_id and sets remaining_kg = gross_weight_kg. No
 * junction writes. Caller owns BEGIN/COMMIT.
 *
 * @param {pg.Client} client
 * @param {object} target
 * @returns {{ row: object }} — intentionally no `sources` field (roots have none)
 */
async function insertRootTransaction(client, target) {
  if (!target || !target.transaction_type) {
    throw new Error('insertRootTransaction: target.transaction_type required');
  }
  if (!ROOT_TYPES[target.transaction_type]) {
    throw new Error(
      'insertRootTransaction: non-root type ' + target.transaction_type +
      ' must use attributeAndInsert'
    );
  }
  if (!target.material_type) {
    throw new Error('insertRootTransaction: target.material_type required');
  }
  if (target.gross_weight_kg == null) {
    throw new Error('insertRootTransaction: target.gross_weight_kg required');
  }

  const batch_id = crypto.randomUUID();

  const insertResult = await client.query(
    'INSERT INTO pending_transactions (' +
    '  transaction_type, status, ' +
    '  aggregator_id, processor_id, converter_id, recycler_id, collector_id, ' +
    '  material_type, gross_weight_kg, net_weight_kg, price_per_kg, total_price, ' +
    '  batch_id, remaining_kg, ' +
    '  source, notes' +
    ') VALUES (' +
    '  $1, COALESCE($2, \'pending\'), ' +
    '  $3, $4, $5, $6, $7, ' +
    '  $8, $9::numeric, $10::numeric, $11, $12, ' +
    '  $13::uuid, $9::numeric, ' +
    '  COALESCE($14, \'direct\'), $15' +
    ') RETURNING *',
    [
      target.transaction_type,
      target.status || null,
      target.aggregator_id || null,
      target.processor_id || null,
      target.converter_id || null,
      target.recycler_id || null,
      target.collector_id || null,
      target.material_type,
      target.gross_weight_kg,
      target.net_weight_kg != null ? target.net_weight_kg : target.gross_weight_kg,
      target.price_per_kg != null ? target.price_per_kg : 0,
      target.total_price != null ? target.total_price : 0,
      batch_id,
      target.source || null,
      target.notes != null ? target.notes : null
    ]
  );

  return { row: insertResult.rows[0] };
}

module.exports = {
  attributeAndInsert: attributeAndInsert,
  insertRootTransaction: insertRootTransaction,
  InsufficientSourceError: InsufficientSourceError,
  WINDOW_DAYS: WINDOW_DAYS,
  EXCLUDED_STATUSES: EXCLUDED_STATUSES,
  // Exported for tests and /api/sources.
  candidateFilterForSeller: candidateFilterForSeller
};
