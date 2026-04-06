-- Fix ratings.transaction_id FK: should reference pending_transactions, not transactions
-- Root cause: /api/ratings/pending returns pending_transactions.id as txn_id,
-- but the FK constraint pointed at transactions(id) — a different table with different IDs.

ALTER TABLE ratings DROP CONSTRAINT IF EXISTS ratings_transaction_id_fkey;
ALTER TABLE ratings
  ADD CONSTRAINT ratings_transaction_id_fkey
  FOREIGN KEY (transaction_id) REFERENCES pending_transactions(id) ON DELETE SET NULL;
