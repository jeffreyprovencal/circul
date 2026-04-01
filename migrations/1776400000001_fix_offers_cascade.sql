-- Drop the existing FK and recreate with CASCADE
-- Deleting a listing should cascade-delete its offers
ALTER TABLE offers DROP CONSTRAINT IF EXISTS offers_listing_id_fkey;
ALTER TABLE offers ADD CONSTRAINT offers_listing_id_fkey
  FOREIGN KEY (listing_id) REFERENCES listings(id) ON DELETE CASCADE;
