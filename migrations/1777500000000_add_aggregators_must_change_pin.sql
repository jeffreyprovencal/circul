ALTER TABLE aggregators ADD COLUMN IF NOT EXISTS must_change_pin BOOLEAN DEFAULT false;
