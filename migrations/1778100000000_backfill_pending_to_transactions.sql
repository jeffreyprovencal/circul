-- Backfill: every confirmed pending_transactions row of type
-- aggregator_purchase or collector_sale that has no transaction_id gets a
-- corresponding transactions row created, with the pending row's
-- transaction_id set as a back-reference.
--
-- Aggressive scope per CTO call (2026-05-04):
--   * aggregator_purchase rows promote REGARDLESS of current status. The
--     new auto-confirm semantic in the runtime PR (#78) treats the in-flow
--     CON Confirm screen as the aggregator's attestation; no second-party
--     check needed for routine pilot use. This backfill brings historical
--     data into alignment with that semantic.
--   * collector_sale rows promote only if status indicates aggregator/admin
--     already accepted them: 'accepted', 'confirmed', or 'completed'.
--     Rejected and still-pending collector_sale rows are NOT touched.
--
-- The pending row stays in place after promotion — chain-of-custody
-- (batch_id, remaining_kg, source, junction-table entries) remains intact.
-- Only status (for aggregator_purchase 'pending' rows) and transaction_id
-- change.
--
-- Idempotent: WHERE transaction_id IS NULL ensures we never duplicate.
-- Safe to re-run if the DO block aborts mid-loop (next run picks up
-- from the unfinished rows).
--
-- Pattern mirrors server.js line 6131 (processor-review of aggregator
-- sale, the only existing site that correctly populates transactions
-- on pending-confirmation).

DO $$
DECLARE
  pt_row RECORD;
  new_txn_id INTEGER;
  promoted_count INTEGER := 0;
BEGIN
  FOR pt_row IN
    SELECT *
    FROM pending_transactions
    WHERE transaction_type IN ('aggregator_purchase', 'collector_sale')
      AND transaction_id IS NULL
      AND collector_id IS NOT NULL
      AND gross_weight_kg > 0
      AND (
        transaction_type = 'aggregator_purchase'
        OR (transaction_type = 'collector_sale' AND status IN ('accepted', 'confirmed', 'completed'))
      )
  LOOP
    INSERT INTO transactions (
      collector_id,
      aggregator_id,
      material_type,
      gross_weight_kg,
      net_weight_kg,
      contamination_deduction_percent,
      price_per_kg,
      total_price,
      notes,
      transaction_date,
      payment_status,
      payment_method,
      payment_reference,
      payment_initiated_at,
      payment_completed_at,
      created_at
    ) VALUES (
      pt_row.collector_id,
      pt_row.aggregator_id,
      pt_row.material_type,
      pt_row.gross_weight_kg,
      COALESCE(pt_row.net_weight_kg, pt_row.gross_weight_kg),
      0,
      pt_row.price_per_kg,
      pt_row.total_price,
      pt_row.notes,
      pt_row.created_at,
      COALESCE(pt_row.payment_status, 'unpaid'),
      pt_row.payment_method,
      pt_row.payment_reference,
      pt_row.payment_initiated_at,
      pt_row.payment_completed_at,
      pt_row.created_at
    ) RETURNING id INTO new_txn_id;

    UPDATE pending_transactions
    SET transaction_id = new_txn_id,
        status = CASE
          WHEN status = 'pending' AND transaction_type = 'aggregator_purchase' THEN 'confirmed'
          ELSE status
        END,
        updated_at = NOW()
    WHERE id = pt_row.id;

    promoted_count := promoted_count + 1;
  END LOOP;

  RAISE NOTICE 'Backfill complete: % rows promoted to transactions', promoted_count;
END $$;
