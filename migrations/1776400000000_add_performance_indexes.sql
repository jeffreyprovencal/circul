-- Performance indexes for frequently queried columns

-- pending_transactions performance indexes
CREATE INDEX IF NOT EXISTS idx_pt_material ON pending_transactions(material_type);
CREATE INDEX IF NOT EXISTS idx_pt_status ON pending_transactions(status);
CREATE INDEX IF NOT EXISTS idx_pt_payment ON pending_transactions(payment_status);
CREATE INDEX IF NOT EXISTS idx_pt_created ON pending_transactions(created_at);

-- transactions performance indexes
CREATE INDEX IF NOT EXISTS idx_tx_material ON transactions(material_type);
CREATE INDEX IF NOT EXISTS idx_tx_status ON transactions(status);
CREATE INDEX IF NOT EXISTS idx_tx_payment ON transactions(payment_status);
CREATE INDEX IF NOT EXISTS idx_tx_created ON transactions(created_at);

-- listings performance indexes
CREATE INDEX IF NOT EXISTS idx_listings_seller ON listings(seller_id, seller_role);
CREATE INDEX IF NOT EXISTS idx_listings_status ON listings(status);
CREATE INDEX IF NOT EXISTS idx_listings_material ON listings(material_type);

-- offers performance index
CREATE INDEX IF NOT EXISTS idx_offers_listing ON offers(listing_id);
