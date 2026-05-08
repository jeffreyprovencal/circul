-- Track driver-side USSD sessions alongside collector_id / aggregator_id / agent_id.
-- Phase 4 dispatch cascade extends to drivers; sessions need to persist that mapping.
ALTER TABLE ussd_sessions ADD COLUMN IF NOT EXISTS driver_id INTEGER REFERENCES drivers(id);
CREATE INDEX IF NOT EXISTS idx_ussd_sessions_driver ON ussd_sessions(driver_id) WHERE driver_id IS NOT NULL;
