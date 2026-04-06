-- Add aggregator and agent tracking to USSD session logs
ALTER TABLE ussd_sessions ADD COLUMN IF NOT EXISTS aggregator_id INTEGER REFERENCES aggregators(id) ON DELETE SET NULL;
ALTER TABLE ussd_sessions ADD COLUMN IF NOT EXISTS agent_id INTEGER REFERENCES agents(id) ON DELETE SET NULL;
