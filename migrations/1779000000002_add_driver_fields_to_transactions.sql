-- Driver assignment + commercial fields on pending_transactions and transactions.
-- Mirrors the path: pending_transactions accumulates state during dispatch, then
-- promotes to transactions on completion. Both tables get the same shape.

-- pending_transactions ──────────────────────────────────────────────────────
ALTER TABLE pending_transactions ADD COLUMN IF NOT EXISTS driver_id              INTEGER REFERENCES drivers(id);
ALTER TABLE pending_transactions ADD COLUMN IF NOT EXISTS driver_confirmed_at    TIMESTAMPTZ;
ALTER TABLE pending_transactions ADD COLUMN IF NOT EXISTS driver_fee_ghs         NUMERIC(10,2);
ALTER TABLE pending_transactions ADD COLUMN IF NOT EXISTS driver_fee_paid_at     TIMESTAMPTZ;
ALTER TABLE pending_transactions ADD COLUMN IF NOT EXISTS driver_fee_paid_method TEXT;
  -- 'cash' | 'momo' | 'bank' | NULL
ALTER TABLE pending_transactions ADD COLUMN IF NOT EXISTS accepted_weight_kg     NUMERIC(10,2);
  -- What buyer paid for at delivery. Captured by driver via USSD. Optional (NULL until delivered).
ALTER TABLE pending_transactions ADD COLUMN IF NOT EXISTS rejected_disposition   TEXT DEFAULT 'leave_at_buyer';
  -- 'leave_at_buyer' (default) | 'bring_back' | 'sell_as_scrap'

CREATE INDEX IF NOT EXISTS idx_pending_driver         ON pending_transactions(driver_id) WHERE driver_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_pending_driver_unpaid  ON pending_transactions(driver_id, driver_fee_paid_at) WHERE driver_id IS NOT NULL AND driver_fee_paid_at IS NULL;

-- transactions ──────────────────────────────────────────────────────────────
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS driver_id              INTEGER REFERENCES drivers(id);
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS driver_confirmed_at    TIMESTAMPTZ;
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS driver_fee_ghs         NUMERIC(10,2);
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS driver_fee_paid_at     TIMESTAMPTZ;
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS driver_fee_paid_method TEXT;
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS accepted_weight_kg     NUMERIC(10,2);
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS rejected_disposition   TEXT;

CREATE INDEX IF NOT EXISTS idx_transactions_driver ON transactions(driver_id) WHERE driver_id IS NOT NULL;
