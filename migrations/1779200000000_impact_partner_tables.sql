-- Phase 5: Impact Partner tables
--
-- Introduces the WORK-tier "Impact Partner" role (apex above tier 1 buyer
-- roles): brands/orgs that tag actors they support and pull branded impact
-- reports for their tagged network. Three tables:
--
--   1. impact_partners                       — the partner account (email+pw login)
--   2. impact_partner_actor_tag_requests     — WORK submits these; Circul reviews
--   3. impact_partner_actor_tags             — approved tags (soft-deletable)
--
-- Schema spec: project_circul_phase5_impact_partner.md / mockup
-- mockups/mockup-impact-partner-feature-v1.html. UNIQUE constraint on
-- (partner, actor_type, actor_id) prevents duplicate tags. Soft-delete via
-- deactivated_at preserves audit history.

CREATE TABLE IF NOT EXISTS impact_partners (
  id               SERIAL PRIMARY KEY,
  name             VARCHAR(255) NOT NULL,         -- "WORK"
  company          VARCHAR(255),                  -- legal name if different
  email            VARCHAR(255) UNIQUE NOT NULL,
  password_hash    VARCHAR(255),
  contact_name     VARCHAR(255),                  -- primary contact, e.g. "Vivien Luk"
  country          VARCHAR(100) DEFAULT 'Ghana',
  is_active        BOOLEAN DEFAULT true,
  created_at       TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS impact_partner_actor_tag_requests (
  id                          SERIAL PRIMARY KEY,
  impact_partner_id           INTEGER NOT NULL REFERENCES impact_partners(id),
  mode                        VARCHAR(20) NOT NULL CHECK (mode IN ('existing','new')),
  actor_phone                 VARCHAR(50) NOT NULL,
  actor_role                  VARCHAR(20) NOT NULL CHECK (actor_role IN ('collector','aggregator','agent','driver')),
  actor_id_existing           INTEGER,            -- set if mode='existing'
  actor_table_existing        VARCHAR(50),        -- 'collectors' | 'aggregators' | 'agents' | 'drivers'
  actor_data_new_pending      JSONB,              -- if mode='new' — DEFERRED to v0.5
  proof_text                  TEXT NOT NULL,
  proof_photo_url             TEXT,
  status                      VARCHAR(20) NOT NULL DEFAULT 'pending_review'
                                CHECK (status IN ('pending_review','approved','rejected')),
  requested_at                TIMESTAMPTZ DEFAULT NOW(),
  requested_by_user_id        INTEGER,            -- denormalized; impact_partner.id for now
  reviewed_at                 TIMESTAMPTZ,
  reviewed_by_admin_id        INTEGER,            -- nullable FK; no admin_users yet
  decision_category           VARCHAR(60),        -- insufficient_proof_detail | insufficient_proof_document | not_verifiable | duplicate | other
  decision_note               TEXT,
  created_actor_id            INTEGER             -- populated on Mode 2 approval; v0.5 only
);

CREATE INDEX IF NOT EXISTS idx_impact_partner_tag_requests_status
  ON impact_partner_actor_tag_requests(status, requested_at DESC);
CREATE INDEX IF NOT EXISTS idx_impact_partner_tag_requests_partner
  ON impact_partner_actor_tag_requests(impact_partner_id);

CREATE TABLE IF NOT EXISTS impact_partner_actor_tags (
  id                  SERIAL PRIMARY KEY,
  impact_partner_id   INTEGER NOT NULL REFERENCES impact_partners(id),
  actor_type          VARCHAR(20) NOT NULL CHECK (actor_type IN ('collector','aggregator','agent','driver')),
  actor_id            INTEGER NOT NULL,
  tag_request_id      INTEGER REFERENCES impact_partner_actor_tag_requests(id),
  active_since        TIMESTAMPTZ DEFAULT NOW(),
  deactivated_at      TIMESTAMPTZ,
  deactivation_reason TEXT,
  UNIQUE (impact_partner_id, actor_type, actor_id)
);

CREATE INDEX IF NOT EXISTS idx_impact_partner_tags_active
  ON impact_partner_actor_tags(impact_partner_id, actor_type, actor_id)
  WHERE deactivated_at IS NULL;
