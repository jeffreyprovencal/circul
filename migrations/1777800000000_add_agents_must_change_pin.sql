-- Add must_change_pin column to agents table.
-- Default false so existing rows are untouched.
-- New rows from web POST /api/agents and USSD register-agent flow set this to true explicitly.
ALTER TABLE agents
  ADD COLUMN IF NOT EXISTS must_change_pin BOOLEAN DEFAULT false;
