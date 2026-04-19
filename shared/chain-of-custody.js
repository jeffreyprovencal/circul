// shared/chain-of-custody.js
// Pure algorithm for reconstructing pending_transactions lineage.
//
// Used by migrations/1777200000000_backfill_chain_of_custody.js (historical
// backfill) and later by PR3's insert-path mass-balance enforcement. No DB
// access, no I/O. Given a list of pending_transactions rows, returns the
// complete attribution plan (junction edges, batch_ids, remaining_kg values)
// without touching the database.
//
// Purity contract:
//   - No Math.random(), no Date.now() except via the passed-in `now`.
//   - Input rows sorted deterministically at the top (created_at ASC, id ASC).
//   - Output is byte-identical on repeated calls with the same input — EXCEPT
//     for the UUID values themselves, which are non-deterministic by default
//     (crypto.randomUUID). Callers that need deterministic UUIDs (tests) can
//     pass `{ uuid: () => '...' }` as an option to override. Structural
//     equivalence (edge list, orphan list, shortfalls, mismatches) is
//     deterministic regardless of UUID source.
//
// Algorithm (approved in Phase A design doc):
//   - Source identification: U is a candidate source of D iff
//       buyerOf(U) === sellerOf(D)
//       ∧ U.material_type == D.material_type
//       ∧ U.created_at < D.created_at (strict)
//       ∧ D.created_at - U.created_at <= windowDays
//       ∧ U.status NOT IN (rejected, dispatch_rejected, grade_c_flagged)
//       ∧ U.remaining_kg > 0
//   - FIFO attribution: draw from candidates in created_at ASC / id ASC order.
//   - Window default: 14 days.
//   - batch_id semantic: root rows (collector_sale / aggregator_purchase) get
//     a fresh UUID. Single-source downstream rows inherit. Multi-source rows
//     get the batch_id of the edge with the LARGEST weight_kg_attributed; ties
//     broken by source created_at ASC, then source id ASC.
//   - remaining_kg = gross_weight_kg - SUM(attributed weight from this source).
//   - Orphans (no candidate source): get a fresh batch_id; logged.
//   - Shortfalls (candidates exhausted before D's weight fully attributed):
//     accepted; logged with unattributed_kg.
//   - Material mismatches (buyer-of-U = seller-of-D but materials differ):
//     excluded from candidates; logged.
//
// See Phase A design doc in PR #37 review thread for full rationale.

(function (root, factory) {
  if (typeof module !== 'undefined' && module.exports) {
    module.exports = factory(require('./transaction-parties'));
  } else {
    root.CirculChainOfCustody = factory(root.CirculTransactionParties);
  }
})(typeof self !== 'undefined' ? self : this, function (transactionParties) {

  // resolveBuyer/resolveSeller are strict: they throw on ambiguous, missing,
  // or unknown rows. The backfill treats those cases as "orphan" rather than
  // fatal, so we wrap them in lenient try/catch adapters below. This keeps
  // the single source of truth in shared/transaction-parties.js and removes
  // the local polymorphism workaround that was here pre-PR2.
  var resolveSeller = transactionParties.resolveSeller;
  var resolveBuyer  = transactionParties.resolveBuyer;

  var EXCLUDED_STATUSES = ['rejected', 'dispatch_rejected', 'grade_c_flagged'];
  var DAY_MS = 24 * 60 * 60 * 1000;

  // Root transaction types have seller=collector; nothing upstream of them.
  // (Collectors don't have predecessor pending_transactions rows in the model.)
  var ROOT_TYPES = { collector_sale: true, aggregator_purchase: true };

  // Returns { kind, id } for the seller (gives material) of a row, or null
  // if the row is malformed. Lenient wrapper over resolveSeller.
  function sellerOf(row) {
    try { return resolveSeller(row); } catch (_e) { return null; }
  }

  // Returns { kind, id } for the buyer (receives material) of a row, or null
  // if the row is malformed / ambiguous. Lenient wrapper over resolveBuyer.
  function buyerOf(row) {
    try { return resolveBuyer(row); } catch (_e) { return null; }
  }

  // Round to 2 decimal places (pending_transactions.gross_weight_kg is NUMERIC(10,2)).
  function round2(n) {
    return Math.round(n * 100) / 100;
  }

  function defaultUuid() {
    // crypto.randomUUID() is available on Node 14.17+ and the package.json
    // engines.node is >=18.0.0, so this is safe.
    var crypto = require('crypto');
    return crypto.randomUUID();
  }

  function computeBackfillPlan(rowsIn, opts) {
    opts = opts || {};
    var windowDays = opts.windowDays != null ? opts.windowDays : 14;
    var uuid = opts.uuid || defaultUuid;
    var windowMs = windowDays * DAY_MS;

    // Work on a shallow copy with a numeric created_at for fast comparisons.
    // Sort deterministically: created_at ASC, then id ASC.
    var rows = rowsIn.map(function (r) {
      return {
        id: Number(r.id),
        transaction_type: r.transaction_type,
        status: r.status,
        collector_id: r.collector_id,
        aggregator_id: r.aggregator_id,
        processor_id: r.processor_id,
        recycler_id: r.recycler_id,
        converter_id: r.converter_id,
        material_type: r.material_type,
        gross_weight_kg: parseFloat(r.gross_weight_kg),
        created_at: r.created_at,
        _createdAtMs: new Date(r.created_at).getTime()
      };
    });
    rows.sort(function (a, b) {
      return (a._createdAtMs - b._createdAtMs) || (a.id - b.id);
    });

    // Per-row mutable state.
    var stateById = Object.create(null);
    rows.forEach(function (r) {
      stateById[r.id] = {
        included: EXCLUDED_STATUSES.indexOf(r.status) === -1,
        remaining_kg: r.gross_weight_kg,
        batch_id: null
      };
    });

    // Index 1: buyer-actor + material → rows (main candidate lookup).
    // Key: `${buyer_kind}:${buyer_id}:${material_type}`
    var buyerMatIndex = Object.create(null);
    // Index 2: buyer-actor only → rows (for mismatch detection).
    // Key: `${buyer_kind}:${buyer_id}`
    var buyerActorIndex = Object.create(null);

    rows.forEach(function (r) {
      if (!stateById[r.id].included) return;
      var b = buyerOf(r);
      if (!b) return;
      var actorKey = b.kind + ':' + b.id;
      var matKey = actorKey + ':' + r.material_type;
      (buyerMatIndex[matKey] = buyerMatIndex[matKey] || []).push(r);
      (buyerActorIndex[actorKey] = buyerActorIndex[actorKey] || []).push(r);
    });
    // Both indexes preserve rows' global sort order (created_at ASC, id ASC).

    var edges = [];
    var batchIdsOut = [];
    var remainingKgOut = [];
    var orphans = [];
    var shortfalls = [];
    var mismatches = [];

    // Process rows in temporal order.
    rows.forEach(function (D) {
      var stD = stateById[D.id];
      if (!stD.included) return; // rejected/flagged rows: no batch_id, no remaining_kg

      // Root: collector-origin rows have no upstream.
      if (ROOT_TYPES[D.transaction_type]) {
        stD.batch_id = uuid();
        return;
      }

      var sD = sellerOf(D);
      if (!sD) {
        // Unknown transaction_type or missing seller — treat as orphan.
        stD.batch_id = uuid();
        orphans.push({ id: D.id });
        return;
      }

      var actorKey = sD.kind + ':' + sD.id;
      var matKey = actorKey + ':' + D.material_type;
      var candidates = buyerMatIndex[matKey] || [];

      // Filter to valid candidates at this moment.
      var valid = [];
      for (var i = 0; i < candidates.length; i++) {
        var U = candidates[i];
        if (U.id === D.id) continue;
        if (U._createdAtMs >= D._createdAtMs) continue;
        if (D._createdAtMs - U._createdAtMs > windowMs) continue;
        if (stateById[U.id].remaining_kg <= 0) continue;
        valid.push(U);
      }

      // Log material mismatches: same buyer-actor, different material, in-window.
      var actorBucket = buyerActorIndex[actorKey] || [];
      for (var m = 0; m < actorBucket.length; m++) {
        var UM = actorBucket[m];
        if (UM.material_type === D.material_type) continue; // not a mismatch
        if (UM.id === D.id) continue;
        if (UM._createdAtMs >= D._createdAtMs) continue;
        if (D._createdAtMs - UM._createdAtMs > windowMs) continue;
        mismatches.push({
          child_id: D.id,
          candidate_id: UM.id,
          reason: 'material_mismatch: child=' + D.material_type + ' candidate=' + UM.material_type
        });
      }

      if (valid.length === 0) {
        stD.batch_id = uuid();
        orphans.push({ id: D.id });
        return;
      }

      // FIFO attribution.
      var target = D.gross_weight_kg;
      var attributed = 0;
      var edgesForD = [];
      for (var j = 0; j < valid.length && attributed < target; j++) {
        var U2 = valid[j];
        var stU = stateById[U2.id];
        var remainingNeed = round2(target - attributed);
        var draw = Math.min(remainingNeed, stU.remaining_kg);
        draw = round2(draw);
        if (draw <= 0) continue;
        stU.remaining_kg = round2(stU.remaining_kg - draw);
        attributed = round2(attributed + draw);
        edgesForD.push({
          child_pending_tx_id: D.id,
          source_pending_tx_id: U2.id,
          weight_kg_attributed: draw
        });
      }

      edges.push.apply(edges, edgesForD);

      if (attributed < target) {
        var unattributed = round2(target - attributed);
        if (unattributed > 0) {
          shortfalls.push({ id: D.id, unattributed_kg: unattributed });
        }
      }

      // Dominant-source batch_id: largest weight_kg_attributed wins.
      // Tie-breaking: iterate edgesForD (already in candidates order, which is
      // created_at ASC / id ASC). First-seen wins on ties, so earliest source wins.
      if (edgesForD.length === 0) {
        // Defensive — shouldn't happen if valid.length > 0 and target > 0.
        stD.batch_id = uuid();
        orphans.push({ id: D.id });
      } else {
        var dominant = edgesForD[0];
        for (var k = 1; k < edgesForD.length; k++) {
          if (edgesForD[k].weight_kg_attributed > dominant.weight_kg_attributed) {
            dominant = edgesForD[k];
          }
        }
        stD.batch_id = stateById[dominant.source_pending_tx_id].batch_id;
      }
    });

    // Emit outputs for all included rows, in the sorted row order.
    rows.forEach(function (r) {
      var st = stateById[r.id];
      if (!st.included) return;
      batchIdsOut.push({ id: r.id, batch_id: st.batch_id });
      remainingKgOut.push({ id: r.id, remaining_kg: round2(st.remaining_kg) });
    });

    return {
      edges: edges,
      batchIds: batchIdsOut,
      remainingKg: remainingKgOut,
      orphans: orphans,
      shortfalls: shortfalls,
      mismatches: mismatches
    };
  }

  // ── computeWriteAttribution ───────────────────────────────────────────────
  // Write-time variant of computeBackfillPlan. Single target, pre-filtered
  // candidates. Used by shared/chain-of-custody-db.js attributeAndInsert()
  // after it does SELECT … FOR UPDATE on the lock-scoped candidate set.
  //
  // Differences vs. computeBackfillPlan (PR2):
  //   - Scope:       single target (not a full row graph)
  //   - Filtering:   caller pre-filters via SQL (buyer-of-U = seller-of-D,
  //                  material match, status not excluded, remaining_kg > 0,
  //                  within window, sorted created_at ASC / id ASC). Pure fn
  //                  trusts its input.
  //   - Roots:       throws — root types must use insertRootTransaction.
  //   - Orphan:      no orphan category at write time. Zero candidates + non-
  //                  zero target = shortfall.
  //   - Shortfall:   returned as shortfall_kg > 0. Caller throws 400.
  //   - batch_id:    inherited from the dominant source's existing batch_id
  //                  (not generated here). Same tie-breaking as backfill:
  //                  largest weight_kg_attributed; first-seen on ties (which
  //                  equals oldest created_at / smallest id given pre-sort).
  //
  // Input shape:
  //   target     = { transaction_type, material_type, gross_weight_kg }
  //   candidates = [{ id, gross_weight_kg, remaining_kg, batch_id, created_at }]
  //                (already filtered + sorted + locked by caller)
  //   opts       = { round2? } — round2 override for tests only
  //
  // Output shape:
  //   {
  //     edges: [{ source_id, weight_kg_attributed }],
  //     sourceRemainingAfter: [{ id, remaining_kg }],   // only for sources drawn from
  //     batch_id: <uuid from dominant source, or null on shortfall>,
  //     shortfall_kg: <0 if covered, positive if under>
  //   }
  function computeWriteAttribution(target, candidates, opts) {
    opts = opts || {};
    var _round2 = opts.round2 || round2;

    if (!target || !target.transaction_type) {
      throw new Error('computeWriteAttribution: target missing or has no transaction_type');
    }
    if (ROOT_TYPES[target.transaction_type]) {
      throw new Error(
        'computeWriteAttribution: root types have no upstream; caller must use insertRootTransaction. Got ' +
        target.transaction_type
      );
    }
    if (target.gross_weight_kg == null) {
      throw new Error('computeWriteAttribution: target.gross_weight_kg required');
    }

    var target_kg = _round2(parseFloat(target.gross_weight_kg));
    var attributed = 0;
    var edges = [];
    var sourceRemainingAfter = [];

    if (candidates && candidates.length > 0) {
      for (var i = 0; i < candidates.length; i++) {
        var c = candidates[i];
        if (attributed >= target_kg) break;
        var candRemaining = parseFloat(c.remaining_kg);
        if (candRemaining <= 0) continue;
        var need = _round2(target_kg - attributed);
        var draw = _round2(Math.min(need, candRemaining));
        if (draw <= 0) continue;
        edges.push({
          source_id: Number(c.id),
          weight_kg_attributed: draw
        });
        sourceRemainingAfter.push({
          id: Number(c.id),
          remaining_kg: _round2(candRemaining - draw)
        });
        attributed = _round2(attributed + draw);
      }
    }

    var shortfall = _round2(target_kg - attributed);
    if (shortfall > 0) {
      // Caller throws. Return partial diagnostic info regardless so logs can
      // show what was attempted.
      return {
        edges: edges,
        sourceRemainingAfter: sourceRemainingAfter,
        batch_id: null,
        shortfall_kg: shortfall
      };
    }

    // Dominant-source batch_id — largest weight_kg_attributed wins.
    // Ties broken by first-seen (which equals created_at ASC / id ASC given
    // the caller's required sort). Same rule as computeBackfillPlan.
    var dominant = edges[0];
    for (var k = 1; k < edges.length; k++) {
      if (edges[k].weight_kg_attributed > dominant.weight_kg_attributed) {
        dominant = edges[k];
      }
    }
    var dominantCandidate = null;
    for (var j = 0; j < candidates.length; j++) {
      if (Number(candidates[j].id) === dominant.source_id) {
        dominantCandidate = candidates[j];
        break;
      }
    }
    var batch_id = dominantCandidate ? dominantCandidate.batch_id : null;

    return {
      edges: edges,
      sourceRemainingAfter: sourceRemainingAfter,
      batch_id: batch_id,
      shortfall_kg: 0
    };
  }

  return {
    computeBackfillPlan: computeBackfillPlan,
    computeWriteAttribution: computeWriteAttribution,
    // Exported for tests that want to inspect the helpers.
    sellerOf: sellerOf,
    buyerOf: buyerOf,
    round2: round2,
    EXCLUDED_STATUSES: EXCLUDED_STATUSES,
    ROOT_TYPES: ROOT_TYPES
  };
});
