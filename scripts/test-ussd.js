// scripts/test-ussd.js — USSD smoke harness
//
// Programmatic smoke for the /api/ussd endpoint. Mirrors Africa's Talking
// stateless callback model: each step appends to the accumulated `text`
// history and POSTs the full string. Assertions are regex-based against
// the response body — intentionally fuzzy so cosmetic copy changes
// don't break the harness.
//
// Usage:
//   node scripts/test-ussd.js                         # run all tests
//   node scripts/test-ussd.js --filter=login          # run tests whose name contains 'login'
//   node scripts/test-ussd.js --base=http://host:port # run against a different server
//
// Exits 0 on all-pass, 1 on any fail, 2 on harness crash. Suitable for CI.
//
// Test data isolation: dedicated phone prefixes (+233900000/1/2) and
// session_id prefix (test-ussd-) so cleanup never touches real data.

const http = require('http');
const https = require('https');
const crypto = require('crypto');
const { URL } = require('url');
const { Pool } = require('pg');

const BASE = arg('--base') || 'http://localhost:3000';
const FILTER = arg('--filter');

const pool = new Pool({ connectionString: process.env.DATABASE_URL });

// Test data prefixes — never collide with real or seeded data.
const TEST_COLLECTOR_PREFIX  = '+233900000';
const TEST_AGGREGATOR_PREFIX = '+233900001';
const TEST_AGENT_PREFIX      = '+233900002';
// Phase 12: driver test prefix. Cleanup, seed + 10 new driver cases scoped here.
const TEST_DRIVER_PREFIX     = '+233900003';
const TEST_SESSION_PREFIX    = 'test-ussd-';

// Pre-seeded test accounts (phones used by the test cases below).
const TEST_COLLECTOR_PHONE   = '+233900000001'; // dialed as 0900000001
const TEST_COLLECTOR_PIN     = '0000';
const TEST_AGGREGATOR_PHONE  = '+233900001001'; // dialed as 0900001001
const TEST_AGGREGATOR_PIN    = '2222';
const TEST_AGENT_PHONE       = '+233900002001'; // dialed as 0900002001
const TEST_AGENT_PIN         = '3333';
// Phase 12: pre-seeded test driver. Cases 3-10 rely on this fixture existing
// (independent of Cases 1-2 which self-register at *003011 / *003012).
const TEST_DRIVER_PHONE      = '+233900003001'; // dialed as 0900003001
const TEST_DRIVER_PIN        = '5555';
const TEST_GATE_PHONE        = '+233900000098'; // dialed as 0900000098, must_change_pin=true
const TEST_AGG_GATE_PHONE    = '+233900001098'; // dialed as 0900001098, must_change_pin=true
const TEST_AGENT_GATE_PHONE  = '+233900002098'; // dialed as 0900002098, must_change_pin=true
const TEST_UNREGISTERED      = '0900099999';    // not in any table

// Marketplace fixtures for display_name regression coverage (PR-feat/ussd-critical-cons).
// TEST_BUY_AGG owns an open buy request on PET in Accra so the collector
// "browse buyers → match interest" flow has a deterministic target row.
// TEST_OFFER_PROC sends an incoming offer to TestAgg Probe so the aggregator
// "my offers → review" flow has a deterministic target row.
const TEST_BUY_AGG_PHONE     = '+233900001050';
const TEST_OFFER_PROC_EMAIL  = 'test-offer@circul-test.local';

// Phase 5: Impact Partner test fixture. Email+password account exercised by
// the impact-partner-* test cases. Admin fixture handles the approve/reject
// endpoint coverage in impact-partner-admin-approve.
const TEST_IP_EMAIL          = 'test-ip@circul-test.local';
const TEST_IP_PASSWORD       = 'demo1234';
const TEST_IP_NAME           = 'TestPartner';
const TEST_ADMIN_EMAIL       = 'test-admin@circul-test.local';
const TEST_ADMIN_PASSWORD    = 'demo1234';

// ── helpers ───────────────────────────────────────────────────────────────────

function arg(prefix) {
  const a = process.argv.find(x => x.startsWith(prefix + '='));
  return a ? a.slice(prefix.length + 1) : null;
}

function hashPin(pin) {
  return new Promise((resolve, reject) => {
    const salt = crypto.randomBytes(16).toString('hex');
    crypto.scrypt(pin, salt, 64, (err, key) => {
      if (err) reject(err);
      else resolve(salt + ':' + key.toString('hex'));
    });
  });
}

async function assertPromoted({ aggregator_phone, collector_phone, material, qty, label, expectedStatus }) {
  // Verify the most recent pending row matching the test flow has a non-null
  // transaction_id, the matching transactions row exists, and pending status
  // matches the expected promotion status.
  const ptRes = await pool.query(
    `SELECT pt.id, pt.transaction_id, pt.status
       FROM pending_transactions pt
       JOIN aggregators a ON a.id = pt.aggregator_id
       JOIN collectors c ON c.id = pt.collector_id
      WHERE a.phone = $1 AND c.phone = $2
        AND pt.material_type = $3 AND pt.gross_weight_kg = $4
      ORDER BY pt.created_at DESC LIMIT 1`,
    [aggregator_phone, collector_phone, material, qty]
  );
  if (!ptRes.rows.length) throw new Error(`[${label}] no pending_transactions row found`);
  const pt = ptRes.rows[0];
  if (!pt.transaction_id) throw new Error(`[${label}] pending row ${pt.id} has NULL transaction_id (status=${pt.status}); expected promoted`);
  if (pt.status !== expectedStatus) throw new Error(`[${label}] expected status='${expectedStatus}', got '${pt.status}'`);
  const txnRes = await pool.query(`SELECT id FROM transactions WHERE id = $1`, [pt.transaction_id]);
  if (!txnRes.rows.length) throw new Error(`[${label}] pending.transaction_id=${pt.transaction_id} but no transactions row exists`);
  return { pendingId: pt.id, txnId: pt.transaction_id, status: pt.status };
}

// Phase 5: generic HTTP helper for Impact Partner test cases that exercise
// JSON endpoints rather than the USSD endpoint. Returns { status, body } where
// body is the parsed JSON (or raw string if parse fails).
function httpJson(method, urlPath, payload, token) {
  const url = new URL(BASE + urlPath);
  const lib = url.protocol === 'https:' ? https : http;
  const body = payload == null ? null : JSON.stringify(payload);
  const headers = { 'Accept': 'application/json' };
  if (body != null) {
    headers['Content-Type'] = 'application/json';
    headers['Content-Length'] = Buffer.byteLength(body);
  }
  if (token) headers['Authorization'] = 'Bearer ' + token;
  return new Promise((resolve, reject) => {
    const req = lib.request({
      hostname: url.hostname,
      port: url.port || (url.protocol === 'https:' ? 443 : 80),
      path: url.pathname + (url.search || ''),
      method: method,
      headers: headers
    }, res => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        let parsed;
        try { parsed = JSON.parse(data); } catch (_) { parsed = data; }
        resolve({ status: res.statusCode, body: parsed });
      });
    });
    req.on('error', reject);
    if (body != null) req.write(body);
    req.end();
  });
}

// Phase 5b: HTTP helper that returns the raw response body as a Buffer + the
// response headers, used for binary downloads (PDF, XLSX) where JSON-parse
// would corrupt bytes. Mirrors httpJson's signature.
function httpRaw(method, urlPath, payload, token) {
  const url = new URL(BASE + urlPath);
  const lib = url.protocol === 'https:' ? https : http;
  const body = payload == null ? null : JSON.stringify(payload);
  const headers = {};
  if (body != null) {
    headers['Content-Type'] = 'application/json';
    headers['Content-Length'] = Buffer.byteLength(body);
  }
  if (token) headers['Authorization'] = 'Bearer ' + token;
  return new Promise((resolve, reject) => {
    const req = lib.request({
      hostname: url.hostname,
      port: url.port || (url.protocol === 'https:' ? 443 : 80),
      path: url.pathname + (url.search || ''),
      method: method,
      headers: headers
    }, res => {
      const chunks = [];
      res.on('data', chunk => chunks.push(chunk));
      res.on('end', () => {
        resolve({ status: res.statusCode, headers: res.headers, body: Buffer.concat(chunks) });
      });
    });
    req.on('error', reject);
    if (body != null) req.write(body);
    req.end();
  });
}

function postUssd({ sessionId, phoneNumber, serviceCode = '*920*54#', text }) {
  const url = new URL(BASE + '/api/ussd');
  const lib = url.protocol === 'https:' ? https : http;
  const body = new URLSearchParams({
    sessionId,
    phoneNumber,
    serviceCode,
    text: text || ''
  }).toString();

  return new Promise((resolve, reject) => {
    const req = lib.request({
      hostname: url.hostname,
      port: url.port || (url.protocol === 'https:' ? 443 : 80),
      path: url.pathname,
      method: 'POST',
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Content-Length': Buffer.byteLength(body),
      }
    }, res => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => resolve({ status: res.statusCode, body: data }));
    });
    req.on('error', reject);
    req.write(body);
    req.end();
  });
}

async function runStep(step, history, sessionId, phoneNumber) {
  history.push(step.input);
  const text = history.filter(s => s !== '').join('*');
  const res = await postUssd({ sessionId, phoneNumber, text });
  if (res.status !== 200) {
    return { ok: false, reason: `HTTP ${res.status}: ${res.body.substring(0, 200)}` };
  }
  if (step.match && !step.match.test(res.body)) {
    return { ok: false, reason: `did not match ${step.match}\n--- got ---\n${res.body.substring(0, 400)}` };
  }
  if (step.notMatch && step.notMatch.test(res.body)) {
    return { ok: false, reason: `unexpectedly matched ${step.notMatch}\n--- got ---\n${res.body.substring(0, 400)}` };
  }
  return { ok: true, response: res.body };
}

async function runTest(t) {
  // Phase 12: beforeHook lets driver test cases seed minimal fixture state
  // (e.g., open dispatch_listings, pending_transactions awaiting confirmation)
  // without chaining off prior test side-effects.
  if (typeof t.beforeHook === 'function') {
    try {
      await t.beforeHook();
    } catch (e) {
      return { name: t.name, ok: false, reason: 'before-hook failed: ' + e.message };
    }
  }
  const sessionId = TEST_SESSION_PREFIX + Math.random().toString(36).slice(2, 10);
  const history = [];
  const recordedResponses = [];
  for (let i = 0; i < t.steps.length; i++) {
    const r = await runStep(t.steps[i], history, sessionId, t.phoneNumber);
    if (!r.ok) return { name: t.name, ok: false, stepIndex: i, reason: r.reason };
    recordedResponses.push(r.response);
  }
  // Both `after` (legacy name) and `afterHook` (Phase 12 naming) supported.
  const after = t.afterHook || t.after;
  if (typeof after === 'function') {
    try {
      await after();
    } catch (e) {
      return { name: t.name, ok: false, reason: 'after-hook failed: ' + e.message };
    }
  }
  // Phase 12: customAssertions runs over the full set of response bodies.
  // Used by the driver-rates-aggregator test to assert no UCS-2 star glyph
  // appeared in any rating screen (Phase 9 ASCII-rating discipline).
  if (typeof t.customAssertions === 'function') {
    try {
      await t.customAssertions(recordedResponses);
    } catch (e) {
      return { name: t.name, ok: false, reason: 'custom-assertion failed: ' + e.message };
    }
  }
  return { name: t.name, ok: true };
}

// Phones may be stored either E.164 (`+233900...`) or local-format (`0900...`)
// depending on which path inserted the row, so cleanup matches both. Ghana
// mobile prefixes are 02X/03X/05X — there is no real `09X` block — so the
// `0900` prefix is reserved for tests with no risk of clobbering real data.
const COLLECTOR_LIKES   = ['+233900000%', '0900000%'];
const AGGREGATOR_LIKES  = ['+233900001%', '0900001%'];
const AGENT_LIKES       = ['+233900002%', '0900002%'];
// Phase 12: driver test phone band, e.g. +233900003001..099 / 0900003001..099.
const DRIVER_LIKES      = [TEST_DRIVER_PREFIX + '%', '0900003%'];
// Some inline-register flows use a 0900099xxx phone for "unknown collector"
// scenarios; sweep those too. ANY_TEST_LIKES already matches the driver band
// via the +233900% / 0900% wildcards — Phase 12 appends the explicit driver
// prefix as well for grep-discoverability and to satisfy the STOP gate.
const ANY_TEST_LIKES    = ['+233900%', '0900%', TEST_DRIVER_PREFIX + '%'];

async function safeDelete(label, fn) {
  try { await fn(); }
  catch (err) { console.warn('[cleanup] ' + label + ': ' + err.message); }
}

async function cleanupTestData() {
  // Order matters: FK dependencies dictate sequence. Marketplace fixtures
  // (offers → listings → orders) must clear before the parties they reference
  // (aggregators, processors). agent_activity FKs to agents + aggregators, so
  // it goes next, then agents, then aggregators, then collectors. Each step
  // is best-effort and isolated so a failure in one doesn't skip the rest.
  await safeDelete('sessions', () =>
    pool.query(`DELETE FROM ussd_sessions WHERE session_id LIKE $1`, [TEST_SESSION_PREFIX + '%'])
  );
  await safeDelete('offers', () =>
    pool.query(
      `DELETE FROM offers
       WHERE buyer_id IN (SELECT id FROM processors WHERE email LIKE 'test-%@circul-test.local')
          OR buyer_id IN (SELECT id FROM aggregators WHERE phone LIKE ANY($1))
          OR listing_id IN (SELECT id FROM listings WHERE seller_id IN (SELECT id FROM aggregators WHERE phone LIKE ANY($1)))`,
      [ANY_TEST_LIKES]
    )
  );
  await safeDelete('listings', () =>
    pool.query(
      `DELETE FROM listings
       WHERE (seller_role = 'aggregator' AND seller_id IN (SELECT id FROM aggregators WHERE phone LIKE ANY($1)))
          OR (seller_role = 'collector'  AND seller_id IN (SELECT id FROM collectors  WHERE phone LIKE ANY($1)))`,
      [ANY_TEST_LIKES]
    )
  );
  await safeDelete('orders', () =>
    pool.query(
      `DELETE FROM orders
       WHERE (buyer_role = 'aggregator' AND buyer_id IN (SELECT id FROM aggregators WHERE phone LIKE ANY($1)))
          OR (buyer_role = 'processor'  AND buyer_id IN (SELECT id FROM processors  WHERE email LIKE 'test-%@circul-test.local'))`,
      [ANY_TEST_LIKES]
    )
  );
  await safeDelete('agent_activity', () =>
    pool.query(
      `DELETE FROM agent_activity
       WHERE agent_id IN (SELECT id FROM agents WHERE phone LIKE ANY($1))
          OR aggregator_id IN (SELECT id FROM aggregators WHERE phone LIKE ANY($1))`,
      [ANY_TEST_LIKES]
    )
  );
  // ── Phase 12: driver-feature cleanup (children first per FK constraints) ──
  // driver_offers → dispatch_listings → driver_activity → driver_aggregator_relationships → drivers.
  // pending_transactions cleanup not needed here: existing aggregators/collectors
  // deletes cascade via the aggregator_id / collector_id FKs (ON DELETE SET NULL
  // for some, no FK for others — driver-linked rows are scoped by aggregator_id
  // in the same test phone band, so they get swept transitively).
  await safeDelete('driver_offers', () =>
    pool.query(
      `DELETE FROM driver_offers
       WHERE driver_id IN (SELECT id FROM drivers WHERE phone LIKE ANY($1))
          OR listing_id IN (SELECT id FROM dispatch_listings
                            WHERE aggregator_id IN (SELECT id FROM aggregators WHERE phone LIKE ANY($1)))`,
      [ANY_TEST_LIKES]
    )
  );
  await safeDelete('dispatch_listings', () =>
    pool.query(
      `DELETE FROM dispatch_listings
       WHERE aggregator_id IN (SELECT id FROM aggregators WHERE phone LIKE ANY($1))
          OR awarded_to_driver_id IN (SELECT id FROM drivers WHERE phone LIKE ANY($1))`,
      [ANY_TEST_LIKES]
    )
  );
  await safeDelete('driver_activity', () =>
    pool.query(
      `DELETE FROM driver_activity
       WHERE driver_id IN (SELECT id FROM drivers WHERE phone LIKE ANY($1))
          OR aggregator_id IN (SELECT id FROM aggregators WHERE phone LIKE ANY($1))`,
      [ANY_TEST_LIKES]
    )
  );
  await safeDelete('driver_aggregator_relationships', () =>
    pool.query(
      `DELETE FROM driver_aggregator_relationships
       WHERE driver_id IN (SELECT id FROM drivers WHERE phone LIKE ANY($1))
          OR aggregator_id IN (SELECT id FROM aggregators WHERE phone LIKE ANY($1))`,
      [ANY_TEST_LIKES]
    )
  );
  // Phase 5: Impact Partner cleanup (child tables first per FK).
  // tags → tag_requests → impact_partners. Sweep by test fixture email AND by
  // any tags referencing test-phone-band actors (so admin-approve / untag
  // tests can run independently without per-case afterHooks).
  await safeDelete('impact_partner_actor_tags', () =>
    pool.query(
      `DELETE FROM impact_partner_actor_tags
       WHERE impact_partner_id IN (SELECT id FROM impact_partners WHERE email LIKE 'test-%@circul-test.local')
          OR (actor_type = 'collector'  AND actor_id IN (SELECT id FROM collectors  WHERE phone LIKE ANY($1)))
          OR (actor_type = 'aggregator' AND actor_id IN (SELECT id FROM aggregators WHERE phone LIKE ANY($1)))
          OR (actor_type = 'driver'     AND actor_id IN (SELECT id FROM drivers     WHERE phone LIKE ANY($1)))
          OR (actor_type = 'agent'      AND actor_id IN (SELECT id FROM agents      WHERE phone LIKE ANY($1)))`,
      [ANY_TEST_LIKES]
    )
  );
  await safeDelete('impact_partner_actor_tag_requests', () =>
    pool.query(
      `DELETE FROM impact_partner_actor_tag_requests
       WHERE impact_partner_id IN (SELECT id FROM impact_partners WHERE email LIKE 'test-%@circul-test.local')`,
      []
    )
  );
  await safeDelete('impact_partners', () =>
    pool.query(`DELETE FROM impact_partners WHERE email LIKE 'test-%@circul-test.local'`)
  );
  // Test admin user (Phase 5)
  await safeDelete('admin_users', () =>
    pool.query(`DELETE FROM admin_users WHERE email LIKE 'test-%@circul-test.local'`)
  );
  // Also sweep pending_transactions linked to test drivers (driver_id FK).
  // pending_transactions doesn't ON DELETE SET NULL the driver_id, so without
  // this the next run would silently retain stale ratings/cap counts.
  await safeDelete('pending_transactions_driver', () =>
    pool.query(
      `DELETE FROM pending_transactions
       WHERE driver_id IN (SELECT id FROM drivers WHERE phone LIKE ANY($1))`,
      [ANY_TEST_LIKES]
    )
  );
  // transactions.driver_id FK has no ON DELETE clause (defaults to NO ACTION)
  // — nullify before deleting drivers to avoid FK violations.
  await safeDelete('transactions_driver_nullify', () =>
    pool.query(
      `UPDATE transactions SET driver_id = NULL
       WHERE driver_id IN (SELECT id FROM drivers WHERE phone LIKE ANY($1))`,
      [ANY_TEST_LIKES]
    )
  );
  await safeDelete('drivers', () =>
    pool.query(`DELETE FROM drivers WHERE phone LIKE ANY($1)`, [ANY_TEST_LIKES])
  );
  await safeDelete('agents', () =>
    pool.query(`DELETE FROM agents WHERE phone LIKE ANY($1)`, [ANY_TEST_LIKES])
  );
  await safeDelete('aggregators', () =>
    pool.query(`DELETE FROM aggregators WHERE phone LIKE ANY($1)`, [ANY_TEST_LIKES])
  );
  await safeDelete('processors', () =>
    pool.query(`DELETE FROM processors WHERE email LIKE 'test-%@circul-test.local'`)
  );
  await safeDelete('collectors', () =>
    pool.query(`DELETE FROM collectors WHERE phone LIKE ANY($1)`, [ANY_TEST_LIKES])
  );
  // Phase 12: ratings rows from driver/rate-aggregator coverage. Done LAST so
  // safeDelete on parties doesn't FK-block; ratings has no incoming FKs.
  await safeDelete('ratings', () =>
    pool.query(
      `DELETE FROM ratings
       WHERE (rater_type='driver' AND rater_id IN (SELECT id FROM drivers WHERE phone LIKE ANY($1)))
          OR (rated_type='driver' AND rated_id IN (SELECT id FROM drivers WHERE phone LIKE ANY($1)))`,
      [ANY_TEST_LIKES]
    )
  );
}

async function seedTestAccounts() {
  // Seed dedicated test users at the test phone prefixes. Idempotent via
  // ON CONFLICT so re-running the harness is safe.
  const collPin = await hashPin(TEST_COLLECTOR_PIN);
  const aggPin  = await hashPin(TEST_AGGREGATOR_PIN);
  const agtPin  = await hashPin(TEST_AGENT_PIN);
  const gatePin = await hashPin(TEST_COLLECTOR_PIN); // gate user starts with same default

  await pool.query(
    `INSERT INTO collectors (first_name, last_name, phone, pin, city, region, is_active, must_change_pin)
     VALUES ('TestColl', 'Probe', $1, $2, 'Accra', 'Greater Accra', true, false)
     ON CONFLICT (phone) DO UPDATE SET pin=EXCLUDED.pin, is_active=true, must_change_pin=false`,
    [TEST_COLLECTOR_PHONE, collPin]
  );

  await pool.query(
    `INSERT INTO aggregators (name, first_name, last_name, phone, pin, city, region, is_active, must_change_pin)
     VALUES ('TestAgg Probe', 'TestAgg', 'Probe', $1, $2, 'Accra', 'Greater Accra', true, false)
     ON CONFLICT (phone) DO UPDATE SET pin=EXCLUDED.pin, is_active=true, must_change_pin=false`,
    [TEST_AGGREGATOR_PHONE, aggPin]
  );

  await pool.query(
    `INSERT INTO agents (aggregator_id, first_name, last_name, phone, pin, city, region, is_active, must_change_pin)
     SELECT id, 'TestAgent', 'Probe', $1, $2, 'Accra', 'Greater Accra', true, false
     FROM aggregators WHERE phone = $3
     ON CONFLICT (phone) DO UPDATE SET pin=EXCLUDED.pin, is_active=true, must_change_pin=false`,
    [TEST_AGENT_PHONE, agtPin, TEST_AGGREGATOR_PHONE]
  );

  await pool.query(
    `INSERT INTO collectors (first_name, last_name, phone, pin, city, region, is_active, must_change_pin)
     VALUES ('GateTest', 'Probe', $1, $2, 'Accra', 'Greater Accra', true, true)
     ON CONFLICT (phone) DO UPDATE SET pin=EXCLUDED.pin, is_active=true, must_change_pin=true`,
    [TEST_GATE_PHONE, gatePin]
  );

  await pool.query(
    `INSERT INTO aggregators (name, first_name, last_name, phone, pin, city, region, is_active, must_change_pin)
     VALUES ('AggGateTest Probe', 'AggGateTest', 'Probe', $1, $2, 'Accra', 'Greater Accra', true, true)
     ON CONFLICT (phone) DO UPDATE SET pin=EXCLUDED.pin, is_active=true, must_change_pin=true`,
    [TEST_AGG_GATE_PHONE, gatePin]
  );

  await pool.query(
    `INSERT INTO agents (aggregator_id, first_name, last_name, phone, pin, city, region, is_active, must_change_pin)
     SELECT id, 'AgentGateTest', 'Probe', $1, $2, 'Accra', 'Greater Accra', true, true
     FROM aggregators WHERE phone = $3
     ON CONFLICT (phone) DO UPDATE SET pin=EXCLUDED.pin, is_active=true, must_change_pin=true`,
    [TEST_AGENT_GATE_PHONE, gatePin, TEST_AGGREGATOR_PHONE]
  );

  // ── Phase 12: pre-seeded test driver ─────────────────────────────────────────
  // Cases 3-10 need a stable driver at TEST_DRIVER_PHONE (PIN 5555, Accra/
  // Greater Accra). Cases 1-2 self-register at *003011 / *003012 to avoid
  // colliding with this seed.
  const drvPin = await hashPin(TEST_DRIVER_PIN);
  await pool.query(
    `INSERT INTO drivers (first_name, last_name, phone, pin, city, region, is_active, must_change_pin)
     VALUES ('TestDriver', 'Probe', $1, $2, 'Accra', 'Greater Accra', true, false)
     ON CONFLICT (phone) DO UPDATE SET pin=EXCLUDED.pin, is_active=true, must_change_pin=false`,
    [TEST_DRIVER_PHONE, drvPin]
  );

  // ── Marketplace fixtures (display_name regression coverage) ─────────────────
  // Buy-request aggregator with a known display_name. Drives the collector
  // "match interest" screen so we can assert the bounded buyer_name renders.
  const buyAggIns = await pool.query(
    `INSERT INTO aggregators (name, first_name, last_name, display_name, phone, pin, city, region, is_active, must_change_pin)
     VALUES ('Test Buy Co Aggregator', 'Test', 'Buy Co Aggregator', 'Test Buy Co', $1, $2, 'Accra', 'Greater Accra', true, false)
     ON CONFLICT (phone) DO UPDATE SET display_name='Test Buy Co', is_active=true, must_change_pin=false
     RETURNING id`,
    [TEST_BUY_AGG_PHONE, aggPin]
  );
  const buyAggId = buyAggIns.rows[0].id;
  await pool.query(
    `INSERT INTO orders (buyer_id, buyer_role, material_type, target_quantity_kg, price_per_kg, status)
     VALUES ($1, 'aggregator', 'PET', 100, 2.50, 'open')`,
    [buyAggId]
  );

  // Offer-sender processor with a known display_name. Combined with a TestAgg
  // listing + pending offer, drives the aggregator "my offers → review" screen.
  const procPwd = await hashPin('demo1234');
  const procIns = await pool.query(
    `INSERT INTO processors (name, display_name, company, email, password_hash, city, region, is_active)
     VALUES ('Test Offer Co Processor', 'Test Offer Co', 'Test Offer Co', $1, $2, 'Accra', 'Greater Accra', true)
     ON CONFLICT (email) DO UPDATE SET display_name='Test Offer Co', is_active=true
     RETURNING id`,
    [TEST_OFFER_PROC_EMAIL, procPwd]
  );
  const procId = procIns.rows[0].id;
  const testAgg = await pool.query(`SELECT id FROM aggregators WHERE phone = $1`, [TEST_AGGREGATOR_PHONE]);
  const testAggId = testAgg.rows[0].id;
  const listingIns = await pool.query(
    `INSERT INTO listings (seller_id, seller_role, material_type, quantity_kg, original_qty_kg, price_per_kg, location, expires_at, status)
     VALUES ($1, 'aggregator', 'PET', 500, 500, 2.00, 'Accra', NOW() + INTERVAL '7 days', 'active')
     RETURNING id`,
    [testAggId]
  );
  await pool.query(
    `INSERT INTO offers (listing_id, buyer_id, buyer_role, price_per_kg, quantity_kg, offered_by, status)
     VALUES ($1, $2, 'processor', 2.20, 500, 'processor', 'pending')`,
    [listingIns.rows[0].id, procId]
  );

  // ── Phase 5: Impact Partner test fixture + test admin ─────────────────────
  const ipPwd = await hashPin(TEST_IP_PASSWORD);
  await pool.query(
    `INSERT INTO impact_partners (name, company, email, password_hash, contact_name, country, is_active)
     VALUES ($1, $1, $2, $3, 'Test Contact', 'Ghana', true)
     ON CONFLICT (email) DO UPDATE SET password_hash=EXCLUDED.password_hash, is_active=true`,
    [TEST_IP_NAME, TEST_IP_EMAIL, ipPwd]
  );
  const adminPwd = await hashPin(TEST_ADMIN_PASSWORD);
  await pool.query(
    `INSERT INTO admin_users (email, password_hash, name, is_active)
     VALUES ($1, $2, 'Test Admin', true)
     ON CONFLICT (email) DO UPDATE SET password_hash=EXCLUDED.password_hash, is_active=true`,
    [TEST_ADMIN_EMAIL, adminPwd]
  );
}

// ── test cases ────────────────────────────────────────────────────────────────

const TESTS = [
  // ─── login flows ────────────────────────────────────────────────────────────
  {
    name: 'collector-login-happy-path',
    phoneNumber: '0900000001',
    steps: [
      { input: '',     match: /CON Circul Collector/ },
      { input: '0000', match: /CON 1\. Log Drop-off/ },
    ],
  },
  {
    name: 'collector-login-wrong-pin',
    phoneNumber: '0900000001',
    steps: [
      { input: '',     match: /CON Circul Collector/ },
      { input: '9999', match: /CON Wrong PIN\..*attempts left/ },
    ],
  },
  {
    name: 'aggregator-login-happy-path',
    phoneNumber: '0900001001',
    steps: [
      { input: '',     match: /CON Circul Aggregator\nWelcome back, TestAgg!/ },
      { input: '2222', match: /CON 1\. Register\n2\. Log Transaction/ },
    ],
  },
  {
    name: 'agent-login-happy-path',
    phoneNumber: '0900002001',
    steps: [
      { input: '',     match: /CON Circul Agent/ },
      { input: '3333', match: /CON Working for: TestAgg Probe\n1\. Log Collection/ },
    ],
  },

  // ─── unregistered welcome ───────────────────────────────────────────────────
  {
    name: 'unregistered-welcome-screen',
    phoneNumber: TEST_UNREGISTERED,
    steps: [
      { input: '', match: /CON Welcome to Circul/ },
    ],
  },

  // ─── aggregator main menu navigation (post-PR-#67) ──────────────────────────
  {
    name: 'aggregator-menu-register-submenu',
    phoneNumber: '0900001001',
    steps: [
      { input: '',     match: /CON Circul Aggregator/ },
      { input: '2222', match: /CON 1\. Register/ },
      // Driver MVP v0 (PR feat/drivers-mvp-v0): submenu now includes 3. Driver
      { input: '1',    match: /CON Register:\n1\. Collector\n2\. Agent\n3\. Driver\n0\. Back/ },
    ],
  },
  {
    name: 'aggregator-menu-more-submenu',
    phoneNumber: '0900001001',
    steps: [
      { input: '',     match: /CON Circul Aggregator/ },
      { input: '2222', match: /CON 1\. Register/ },
      { input: '4',    match: /CON More options/ },
    ],
  },
  {
    name: 'aggregator-cancel-from-register-submenu',
    phoneNumber: '0900001001',
    steps: [
      { input: '',     match: /CON Circul Aggregator/ },
      { input: '2222', match: /CON 1\. Register/ },
      { input: '1',    match: /CON Register:/ },
      { input: '0',    match: /CON 1\. Register\n2\. Log Transaction/ }, // back to main menu
    ],
  },

  // ─── register collector (aggregator path) ───────────────────────────────────
  {
    name: 'aggregator-register-collector-happy-path',
    phoneNumber: '0900001001',
    steps: [
      { input: '',           match: /CON Circul Aggregator/ },
      { input: '2222',       match: /CON 1\. Register/ },
      { input: '1',          match: /CON Register:/ },
      { input: '1',          match: /CON Enter collector's\nfirst name/ },
      { input: 'TestFirst',  match: /CON Enter collector's\nlast name/ },
      { input: 'TestLast',   match: /CON Enter collector's\nphone number/ },
      { input: '0900000050', match: /CON Select city/ },
      { input: '1',          match: /CON Register collector:/ },
      { input: '1',          match: /END.*registered!\nPhone: 0900000050\nPIN: 0000/ },
    ],
  },
  {
    name: 'aggregator-register-collector-cancel',
    phoneNumber: '0900001001',
    steps: [
      { input: '',           match: /CON Circul Aggregator/ },
      { input: '2222',       match: /CON 1\. Register/ },
      { input: '1',          match: /CON Register:/ },
      { input: '1',          match: /CON Enter collector's\nfirst name/ },
      { input: 'CancelMe',   match: /CON Enter collector's\nlast name/ },
      { input: 'TestLast',   match: /CON Enter collector's\nphone number/ },
      { input: '0900000051', match: /CON Select city/ },
      { input: '1',          match: /CON Register collector:/ },
      { input: '0',          match: /END Cancelled\./ },
    ],
  },

  // ─── register agent (aggregator path, PR #67) ───────────────────────────────
  {
    name: 'aggregator-register-agent-happy-path',
    phoneNumber: '0900001001',
    steps: [
      { input: '',           match: /CON Circul Aggregator/ },
      { input: '2222',       match: /CON 1\. Register/ },
      { input: '1',          match: /CON Register:/ },
      { input: '2',          match: /CON Enter agent's\nfirst name/ },
      { input: 'AgentNew',   match: /CON Enter agent's\nlast name/ },
      { input: 'TestLast',   match: /CON Enter agent's\nphone number/ },
      { input: '0900002050', match: /CON Select city/ },
      { input: '1',          match: /CON Register agent:/ },
      { input: '1',          match: /END.*registered!\nPhone: 0900002050\nPIN: 0000/ },
    ],
  },

  // ─── city-picker pagination coverage (Sister PR B / v3 audit Finding #9) ────
  // 16 regional capitals across 6 pages (3-3-3-3-3-1). Each page is 4+0
  // compliant per renderCityPickerScreen / parsePaginatedSelection. These
  // tests drive the aggregator-register-collector flow through pagination
  // to confirm reachability of capitals beyond the original pilot 8.

  // Wa (Upper West, index 8 = page 2 entry 3): advance ×2, pick '3'.
  {
    name: 'register-collector-pick-wa-upper-west',
    phoneNumber: '0900001001',
    steps: [
      { input: '',           match: /CON Circul Aggregator/ },
      { input: '2222',       match: /CON 1\. Register/ },
      { input: '1',          match: /CON Register:/ },
      { input: '1',          match: /CON Enter collector's\nfirst name/ },
      { input: 'WaPick',     match: /CON Enter collector's\nlast name/ },
      { input: 'Probe',      match: /CON Enter collector's\nphone number/ },
      { input: '0900000070', match: /CON Select city/ },             // page 0
      { input: '4',          match: /CON Select city/ },             // page 1
      { input: '4',          match: /1\. Tamale[\s\S]*2\. Bolgatanga[\s\S]*3\. Wa/ }, // page 2 — verify Wa visible
      { input: '3',          match: /CON Register collector:[\s\S]*Wa\n/ }, // confirm screen names Wa
      { input: '1',          match: /END.*registered!\nPhone: 0900000070/ },
    ],
  },

  // Goaso (Ahafo, index 11 = page 3 entry 3): advance ×3, pick '3'.
  {
    name: 'register-collector-pick-goaso-ahafo',
    phoneNumber: '0900001001',
    steps: [
      { input: '',           match: /CON Circul Aggregator/ },
      { input: '2222',       match: /CON 1\. Register/ },
      { input: '1',          match: /CON Register:/ },
      { input: '1',          match: /CON Enter collector's\nfirst name/ },
      { input: 'GoasoPick',  match: /CON Enter collector's\nlast name/ },
      { input: 'Probe',      match: /CON Enter collector's\nphone number/ },
      { input: '0900000071', match: /CON Select city/ },
      { input: '4',          match: /CON Select city/ },
      { input: '4',          match: /CON Select city/ },
      { input: '4',          match: /1\. Sunyani[\s\S]*2\. Techiman[\s\S]*3\. Goaso/ }, // page 3
      { input: '3',          match: /CON Register collector:[\s\S]*Goaso\n/ },
      { input: '1',          match: /END.*registered!\nPhone: 0900000071/ },
    ],
  },

  // Sefwi Wiawso (Western North, index 15 = page 5 entry 1): advance ×5, pick '1'.
  // Last page has 1 entry, no "More →" — verifies trailing-page rendering.
  {
    name: 'register-collector-pick-sefwi-wiawso-western-north',
    phoneNumber: '0900001001',
    steps: [
      { input: '',           match: /CON Circul Aggregator/ },
      { input: '2222',       match: /CON 1\. Register/ },
      { input: '1',          match: /CON Register:/ },
      { input: '1',          match: /CON Enter collector's\nfirst name/ },
      { input: 'SefwiPick',  match: /CON Enter collector's\nlast name/ },
      { input: 'Probe',      match: /CON Enter collector's\nphone number/ },
      { input: '0900000072', match: /CON Select city/ },
      { input: '4',          match: /CON Select city/ },             // → page 1
      { input: '4',          match: /CON Select city/ },             // → page 2
      { input: '4',          match: /CON Select city/ },             // → page 3
      { input: '4',          match: /CON Select city/ },             // → page 4
      { input: '4',          match: /1\. Sefwi Wiawso[\s\S]*0\. Cancel/ }, // page 5: only entry, no "More →"
      { input: '1',          match: /CON Register collector:[\s\S]*Sefwi Wiawso\n/ },
      { input: '1',          match: /END.*registered!\nPhone: 0900000072/ },
    ],
  },

  // ─── force-change-pin gate (PR #67, refactored to UPDATE+END) ───────────────
  // After both PINs match, gate UPDATEs immediately and returns END forcing
  // redial. This eliminates the same-session slot-replay bug where stale gate
  // digits would mis-route the next keystroke to "Invalid option".
  {
    name: 'force-change-pin-gate-collector',
    phoneNumber: '0900000098',
    steps: [
      { input: '',     match: /CON Circul Collector/ },
      { input: '0000', match: /CON You must set a new PIN/ },
      { input: '5678', match: /CON Confirm new PIN/ },
      { input: '5678', match: /END PIN saved\.\nDial \*920\*54# again\nto continue\./ },
    ],
  },

  // ─── post-PIN-change redial: gate is past, main menu navigates cleanly ─────
  // Regression coverage for the slot-replay bug fixed alongside the gate
  // refactor. Same test phone, fresh session — must_change_pin is now false
  // (set by the previous test's UPDATE), so depth=0 yields the main menu and
  // depth=1 ('2') routes into Sell My Material. Pre-fix this would have hit
  // "END Invalid option" because m_raw still carried stale gate digits.
  {
    name: 'force-change-pin-gate-collector-post-redial',
    phoneNumber: '0900000098',
    steps: [
      { input: '',     match: /CON Circul Collector/ },
      { input: '5678', match: /CON 1\. Log Drop-off/ },
      { input: '2',    match: /CON Sell My Material:/ },
    ],
  },

  // ─── force-change-pin gate (aggregator coverage, mirrors collector) ────────
  // The same gateForceChangePin function is called by handleAggregatorUssd
  // (server.js:3720). Same UPDATE+END behavior expected. Same post-redial
  // slot-mismatch concern resolved.
  {
    name: 'force-change-pin-gate-aggregator',
    phoneNumber: '0900001098',
    steps: [
      { input: '',     match: /CON Circul Aggregator/ },
      { input: '0000', match: /CON You must set a new PIN/ },
      { input: '5678', match: /CON Confirm new PIN/ },
      { input: '5678', match: /END PIN saved\.\nDial \*920\*54# again\nto continue\./ },
    ],
  },
  {
    name: 'force-change-pin-gate-aggregator-post-redial',
    phoneNumber: '0900001098',
    steps: [
      { input: '',     match: /CON Circul Aggregator/ },
      { input: '5678', match: /CON 1\. Register/ },
      { input: '2',    match: /CON Log Transaction/ },
    ],
  },

  // ─── force-change-pin gate (agent coverage, mirrors collector) ─────────────
  // Same gateForceChangePin function is called by handleAgentUssd
  // (server.js:4653). Note the agent main menu's first item is "Log Collection",
  // not "Log Drop-off" or "Register".
  {
    name: 'force-change-pin-gate-agent',
    phoneNumber: '0900002098',
    steps: [
      { input: '',     match: /CON Circul Agent/ },
      { input: '0000', match: /CON You must set a new PIN/ },
      { input: '5678', match: /CON Confirm new PIN/ },
      { input: '5678', match: /END PIN saved\.\nDial \*920\*54# again\nto continue\./ },
    ],
  },
  {
    name: 'force-change-pin-gate-agent-post-redial',
    phoneNumber: '0900002098',
    steps: [
      { input: '',     match: /CON Circul Agent/ },
      { input: '5678', match: /CON Working for:/ },
      { input: '2',    match: /No unpaid collections|Unpaid collections/ },
    ],
  },

  // ─── path A: aggregator USSD log-purchase auto-confirms + dual-row promotes ───
  // Verifies the dual-row pattern: pending_transactions row has status='confirmed'
  // and transaction_id set, transactions row exists.
  {
    name: 'path-A-aggregator-log-purchase-auto-promotes',
    phoneNumber: TEST_AGGREGATOR_PHONE.replace('+233', '0'),
    steps: [
      { input: '',                                        match: /CON Circul Aggregator/ },
      { input: TEST_AGGREGATOR_PIN,                       match: /CON 1\. Register/ },
      { input: '2',                                       match: /CON Log Transaction/ },
      { input: '1',                                       match: /Enter collector phone|Select collector/ },
      { input: TEST_COLLECTOR_PHONE.replace('+233', '0'), match: /CON Collector found:/ },
      { input: '1',                                       match: /CON Select material/ },
      { input: '1',                                       match: /CON Enter weight in kg/ },
      { input: '7',                                       match: /CON Enter price per kg/ },
      { input: '3.5',                                     match: /CON Confirm purchase:/ },
      { input: '1',                                       match: /END PURCHASE LOGGED/ },
    ],
    after: async () => {
      await assertPromoted({
        aggregator_phone: TEST_AGGREGATOR_PHONE,
        collector_phone: TEST_COLLECTOR_PHONE,
        material: 'PET',
        qty: 7,
        label: 'path-A',
        expectedStatus: 'confirmed'
      });
    },
  },

  // ─── path B: aggregator confirms collector drop-off → dual-row promotes ───
  // Setup: collector logs drop-off (creates pending status='pending').
  // Then aggregator goes to Pending Drop-offs and confirms.
  {
    name: 'path-B-collector-drop-off',
    phoneNumber: TEST_COLLECTOR_PHONE.replace('+233', '0'),
    steps: [
      { input: '',                 match: /CON Circul Collector/ },
      { input: TEST_COLLECTOR_PIN, match: /CON 1\. Log Drop-off/ },
      { input: '1',                match: /CON Select aggregator|CON Pick aggregator/ },
      { input: '1',                match: /CON Select material/ },
      { input: '1',                match: /CON Enter weight in kg/ },
      { input: '11',               match: /CON Confirm drop-off:/ },
      { input: '1',                match: /END DROP-OFF LOGGED|END Drop-off recorded/ },
    ],
  },
  {
    name: 'path-B-aggregator-confirm-promotes',
    phoneNumber: TEST_AGGREGATOR_PHONE.replace('+233', '0'),
    steps: [
      { input: '',                  match: /CON Circul Aggregator/ },
      { input: TEST_AGGREGATOR_PIN, match: /CON 1\. Register/ },
      { input: '3',                 match: /CON Pending drop-offs:/ },
      { input: '1',                 match: /from\n.*\(COL-/ },
      { input: '1',                 match: /END DROP-OFF CONFIRMED/ },
    ],
    after: async () => {
      await assertPromoted({
        aggregator_phone: TEST_AGGREGATOR_PHONE,
        collector_phone: TEST_COLLECTOR_PHONE,
        material: 'PET',
        qty: 11,
        label: 'path-B',
        expectedStatus: 'confirmed'
      });
    },
  },

  // ─── H7: Aggregator USSD Record Payment + H8 dual-table sync ─────────────
  // Setup: log a fresh purchase as aggregator (auto-confirms, dual-row
  // promotes via PR #79 path A). Then dial back in and use the new
  // Record Payment menu item (option 4 post-PR-#80) to mark it paid.
  // Assert payment_status='paid' on BOTH pending_transactions AND transactions
  // — verifies H8 dual-table sync.
  {
    name: 'pr80-H7-aggregator-ussd-record-payment',
    phoneNumber: TEST_AGGREGATOR_PHONE.replace('+233', '0'),
    steps: [
      { input: '',                                        match: /CON Circul Aggregator/ },
      { input: TEST_AGGREGATOR_PIN,                       match: /CON 1\. Register/ },
      // Step 1: log fresh purchase (auto-confirms via PR #79)
      { input: '2',                                       match: /CON Log Transaction/ },
      { input: '1',                                       match: /Enter collector phone|Select collector/ },
      { input: TEST_COLLECTOR_PHONE.replace('+233', '0'), match: /CON Collector found:/ },
      { input: '1',                                       match: /CON Select material/ },
      { input: '1',                                       match: /CON Enter weight in kg/ },
      { input: '13',                                      match: /CON Enter price per kg/ },
      { input: '4',                                       match: /CON Confirm purchase:/ },
      { input: '1',                                       match: /END PURCHASE LOGGED/ },
    ],
  },
  {
    name: 'pr80-H7-record-payment-flow',
    phoneNumber: TEST_AGGREGATOR_PHONE.replace('+233', '0'),
    steps: [
      { input: '',                                        match: /CON Circul Aggregator/ },
      { input: TEST_AGGREGATOR_PIN,                       match: /CON 1\. Register[\s\S]*4\. More[\s\S]*0\. Exit/ },
      { input: '4',                                       match: /CON More options[\s\S]*3\. Record Payment[\s\S]*0\. Back/ },
      { input: '3',                                       match: /CON Unpaid drop-offs:/ },
      { input: '1',                                       match: /CON Pay .* GHS/ },
      { input: '1',                                       match: /END Payment recorded!/ },
    ],
    after: async () => {
      // Verify H8: BOTH tables updated to payment_status='paid'.
      const ptRes = await pool.query(
        `SELECT pt.payment_status AS pt_pay, t.payment_status AS t_pay, pt.transaction_id
           FROM pending_transactions pt
           LEFT JOIN transactions t ON t.id = pt.transaction_id
           JOIN aggregators a ON a.id = pt.aggregator_id
           JOIN collectors c ON c.id = pt.collector_id
          WHERE a.phone = $1 AND c.phone = $2 AND pt.material_type = 'PET' AND pt.gross_weight_kg = 13
          ORDER BY pt.created_at DESC LIMIT 1`,
        [TEST_AGGREGATOR_PHONE, TEST_COLLECTOR_PHONE]
      );
      if (!ptRes.rows.length) throw new Error('[H7/H8] no test row found');
      const r = ptRes.rows[0];
      if (r.pt_pay !== 'paid') throw new Error(`[H7/H8] pending_transactions.payment_status expected 'paid', got '${r.pt_pay}'`);
      if (r.transaction_id && r.t_pay !== 'paid') throw new Error(`[H7/H8] transactions.payment_status expected 'paid', got '${r.t_pay}'`);
    },
  },

  // ─── confirm-action screens (post-#69 compressed) ───────────────────────────
  {
    name: 'aggregator-confirm-purchase-screen-renders',
    phoneNumber: '0900001001',
    steps: [
      { input: '',           match: /CON Circul Aggregator/ },
      { input: '2222',       match: /CON 1\. Register/ },
      { input: '2',          match: /CON Log Transaction/ },
      { input: '1',          match: /Enter collector phone\nnumber|Select collector/ },
      { input: '0900000001', match: /CON Collector found:\nTestColl Probe/ },
      { input: '1',          match: /CON Select material/ },
      { input: '1',          match: /CON Enter weight in kg/ },
      { input: '5',          match: /CON Enter price per kg/ },
      { input: '4.5',        match: /CON Confirm purchase:\n5kg PET\nfrom TestColl Probe\nGHS 22\.50\n1\. Confirm\n0\. Cancel/ },
      { input: '0',          match: /END Cancelled\./ },
    ],
  },

  // ─── inline register at purchase (PR #65) ───────────────────────────────────
  {
    name: 'inline-register-at-purchase-bridge',
    phoneNumber: '0900001001',
    steps: [
      { input: '',           match: /CON Circul Aggregator/ },
      { input: '2222',       match: /CON 1\. Register/ },
      { input: '2',          match: /CON Log Transaction/ },
      { input: '1',          match: /Enter collector phone/ },
      { input: '0900099887', match: /CON 0900099887 is not\nregistered on Circul/ },
      { input: '1',          match: /CON Enter collector's\nfirst name/ },
      { input: 'Inline',     match: /CON Enter collector's\nlast name/ },
      { input: 'Test',       match: /CON Select city/ },
      { input: '1',          match: /CON Register collector:/ },
      { input: '1',          match: /CON .*registered!\nPIN: 0000\n\n1\. Continue purchase\n0\. Done/ },
      { input: '0',          match: /END/ },
    ],
  },

  // ─── agent register collector (PR #65) ──────────────────────────────────────
  {
    name: 'agent-register-collector-happy-path',
    phoneNumber: '0900002001',
    steps: [
      { input: '',           match: /CON Circul Agent/ },
      { input: '3333',       match: /CON Working for:.*\n1\. Log Collection/ },
      { input: '3',          match: /CON Enter collector's\nfirst name/ },
      { input: 'AgentColl',  match: /CON Enter collector's\nlast name/ },
      { input: 'TestLast',   match: /CON Enter collector's\nphone number/ },
      { input: '0900000060', match: /CON Select city/ },
      { input: '1',          match: /CON Register collector:/ },
      { input: '1',          match: /END.*registered!\nPhone: 0900000060\nPIN: 0000.*\nFor: TestAgg Probe/s },
    ],
  },

  // ─── display_name plumbing: collector match-interest (#7) ───────────────────
  // Asserts the bounded buyer_name (via COALESCE(display_name, LEFT(name,24)))
  // renders on the post-compression match-interest screen.
  {
    name: 'collector-match-interest-uses-display-name',
    phoneNumber: '0900000001',
    steps: [
      { input: '',     match: /CON Circul Collector/ },
      { input: '0000', match: /CON 1\. Log Drop-off/ },
      { input: '3',    match: /CON Discovery:/ },
      { input: '1',    match: /CON Browse buyers for:/ },
      { input: '1',    match: /CON PET buyers/ },
      { input: '1',    match: /CON Test Buy Co\nWants 100kg PET\n.*\n1\. Match \(share phone\)\n2\. Not interested\n0\. Back/ },
    ],
  },

  // ─── display_name plumbing: aggregator offer-review (#12) ───────────────────
  // Asserts the bounded other_name (via COALESCE(display_name, LEFT(name,24)))
  // renders on the post-compression offer-review screen.
  {
    name: 'aggregator-offer-review-uses-display-name',
    phoneNumber: '0900001001',
    steps: [
      { input: '',     match: /CON Circul Aggregator/ },
      { input: '2222', match: /CON 1\. Register/ },
      { input: '4',    match: /CON More options/ },
      { input: '1',    match: /CON Marketplace:/ },
      { input: '4',    match: /CON My offers:/ },
      { input: '1',    match: /CON Offer from Test Offer Co:\n500kg PET\nGHS 2\.20\/kg = GHS 1100\.00\n1\. Accept\n2\. Decline\n0\. Back/ },
    ],
  },

  // ════════════════════════════════════════════════════════════════════════════
  // Phase 12 — driver actor MVP v0 (10 new cases)
  //
  // Cases 1-2 self-register at unique phones (*003011 / *003012) to avoid
  // colliding with the seedTestAccounts TEST_DRIVER_PHONE (*003001) that
  // Cases 3-10 rely on.
  //
  // Case #11 "Aggregator rates driver" is INTENTIONALLY OMITTED from v0 —
  // Phase 9 deferred aggregator-rates-driver to v1 (no USSD menu surface;
  // ratedKind UNION complexity in getPendingRatings).
  // ════════════════════════════════════════════════════════════════════════════

  // ─── Case 1: driver self-register (Accra path, page 0 entry 1) ─────────────
  {
    name: 'driver-self-register-accra',
    phoneNumber: '0900003011',
    beforeHook: async () => {
      // Defensive: in case of partial prior run, drop any driver at this phone.
      await pool.query(`DELETE FROM drivers WHERE phone = $1 OR phone = $2`,
        ['+233900003011', '0900003011']);
    },
    steps: [
      { input: '',          match: /What's your role\?[\s\S]*3\. Driver/ },
      { input: '3',          match: /Enter your first name/ },
      { input: 'Kojo',       match: /Enter your last name/ },
      { input: 'Asante',     match: /Select city|1\. Accra/ },
      { input: '1',          match: /Set 4-digit PIN/ },
      { input: '5555',       match: /Confirm 4-digit PIN/ },
      { input: '5555',       match: /CON Register driver:[\s\S]*Kojo Asante[\s\S]*Accra[\s\S]*1\. Confirm/ },
      { input: '1',          match: /END Welcome, Kojo!\nCode: DRV-/ },
    ],
    afterHook: async () => {
      const r = await pool.query(
        `SELECT first_name, city, region FROM drivers WHERE phone IN ($1, $2) ORDER BY id DESC LIMIT 1`,
        ['+233900003011', '0900003011']
      );
      if (!r.rows.length) throw new Error('driver row not inserted at 0900003011');
      if (r.rows[0].city !== 'Accra') throw new Error('city != Accra, got ' + r.rows[0].city);
      if (r.rows[0].region !== 'Greater Accra') throw new Error('region != Greater Accra, got ' + r.rows[0].region);
    },
  },

  // ─── Case 2: driver self-register (Sefwi Wiawso, paginated picker) ─────────
  // Sefwi Wiawso is the 16th capital, position 1 on page 5 (3-per-page layout).
  // Path: 5 page-advances ('4' each) then '1' on the last page. Same logic as
  // the existing register-collector-pick-sefwi-wiawso-western-north test —
  // confirms the picker works for self-register too.
  {
    name: 'driver-self-register-sefwi-wiawso',
    phoneNumber: '0900003012',
    beforeHook: async () => {
      await pool.query(`DELETE FROM drivers WHERE phone = $1 OR phone = $2`,
        ['+233900003012', '0900003012']);
    },
    steps: [
      { input: '',          match: /What's your role/ },
      { input: '3',          match: /Enter your first name/ },
      { input: 'Yaw',        match: /Enter your last name/ },
      { input: 'Mensah',     match: /Select city/ },              // page 0
      { input: '4',          match: /Select city/ },               // page 1
      { input: '4',          match: /Select city/ },               // page 2
      { input: '4',          match: /Select city/ },               // page 3
      { input: '4',          match: /Select city/ },               // page 4
      { input: '4',          match: /1\. Sefwi Wiawso/ },           // page 5 (only entry)
      { input: '1',          match: /Set 4-digit PIN/ },
      { input: '5555',       match: /Confirm 4-digit PIN/ },
      { input: '5555',       match: /CON Register driver:[\s\S]*Sefwi Wiawso/ },
      { input: '1',          match: /END Welcome, Yaw!\nCode: DRV-/ },
    ],
    afterHook: async () => {
      const r = await pool.query(
        `SELECT city, region FROM drivers WHERE phone IN ($1, $2) ORDER BY id DESC LIMIT 1`,
        ['+233900003012', '0900003012']
      );
      if (!r.rows.length) throw new Error('driver row not inserted at 0900003012');
      if (r.rows[0].city !== 'Sefwi Wiawso') {
        throw new Error('expected Sefwi Wiawso, got ' + r.rows[0].city);
      }
      if (r.rows[0].region !== 'Western North') {
        throw new Error('expected Western North, got ' + r.rows[0].region);
      }
    },
  },

  // ─── Case 3: aggregator invites unregistered driver → register-prompt path ──
  // Aggregator path: 2222 → 1 (Register submenu) → 3 (Driver) → enter phone of
  // an unknown driver. End screen tells aggregator "No registered driver" and
  // server fires DRIVER_REGISTER_PROMPT SMS to the unknown number.
  {
    name: 'aggregator-invites-driver-unregistered',
    phoneNumber: '0900001001',
    steps: [
      { input: '',           match: /CON Circul Aggregator/ },
      { input: '2222',       match: /CON 1\. Register/ },
      { input: '1',          match: /CON Register:\n1\. Collector\n2\. Agent\n3\. Driver/ },
      { input: '3',          match: /Enter driver's\nphone number/ },
      { input: '0900003999', match: /END No registered driver/ },
    ],
  },

  // ─── Case 4: aggregator invites registered driver → invite_pending row ─────
  // Driver TEST_DRIVER_PHONE exists (from seedTestAccounts). After this case,
  // a driver_aggregator_relationships row exists with status='invite_pending'.
  // The beforeHook scrubs any prior invite/active row so the test is
  // independently runnable (insert path, not upsert-back-to-pending path).
  {
    name: 'aggregator-invites-registered-driver-creates-invite-pending',
    phoneNumber: '0900001001',
    beforeHook: async () => {
      await pool.query(
        `DELETE FROM driver_aggregator_relationships
         WHERE driver_id IN (SELECT id FROM drivers WHERE phone = $1)
           AND aggregator_id IN (SELECT id FROM aggregators WHERE phone = $2)`,
        [TEST_DRIVER_PHONE, TEST_AGGREGATOR_PHONE]
      );
    },
    steps: [
      { input: '',           match: /CON Circul Aggregator/ },
      { input: '2222',       match: /CON 1\. Register/ },
      { input: '1',          match: /3\. Driver/ },
      { input: '3',          match: /Enter driver's\nphone number/ },
      { input: '0900003001', match: /END Invitation sent/ },
    ],
    afterHook: async () => {
      const r = await pool.query(
        `SELECT status FROM driver_aggregator_relationships
         WHERE driver_id IN (SELECT id FROM drivers WHERE phone = $1)
           AND aggregator_id IN (SELECT id FROM aggregators WHERE phone = $2)`,
        [TEST_DRIVER_PHONE, TEST_AGGREGATOR_PHONE]
      );
      if (!r.rows.length || r.rows[0].status !== 'invite_pending') {
        throw new Error('expected invite_pending, got ' + (r.rows.length ? r.rows[0].status : 'no row'));
      }
    },
  },

  // ─── Case 5: driver available work — empty state ───────────────────────────
  // Driver dials in. beforeHook upserts an active relationship (so no
  // invite-intercept fires) and clears dispatch_listings + pending_transactions
  // so the main menu shows "Available work (0)".
  {
    name: 'driver-available-work-empty',
    phoneNumber: '0900003001',
    beforeHook: async () => {
      await pool.query(
        `INSERT INTO driver_aggregator_relationships (driver_id, aggregator_id, status)
         SELECT d.id, a.id, 'active'
         FROM drivers d, aggregators a
         WHERE d.phone = $1 AND a.phone = $2
         ON CONFLICT (driver_id, aggregator_id)
         DO UPDATE SET status='active', claimed_at=NOW()`,
        [TEST_DRIVER_PHONE, TEST_AGGREGATOR_PHONE]
      );
      // No listings, no pending_transactions for this driver.
      await pool.query(
        `DELETE FROM dispatch_listings
         WHERE aggregator_id IN (SELECT id FROM aggregators WHERE phone = $1)`,
        [TEST_AGGREGATOR_PHONE]
      );
      await pool.query(
        `DELETE FROM pending_transactions
         WHERE driver_id IN (SELECT id FROM drivers WHERE phone = $1)`,
        [TEST_DRIVER_PHONE]
      );
    },
    steps: [
      { input: '',     match: /CON Circul Driver/ },
      { input: '5555', match: /Available work \(0\)/ },
    ],
  },

  // ─── Case 6: driver accepts marketplace listing (accept-as-is path) ────────
  // beforeHook creates an open marketplace listing in driver's region
  // (Greater Accra). Driver dials in, picks "Available work", picks the
  // listing, accepts at the proposed fee. Driver-offers row inserted with
  // offer_type='accept_proposed'.
  {
    name: 'driver-accepts-marketplace-listing',
    phoneNumber: '0900003001',
    beforeHook: async () => {
      // Active relationship + zero existing pending so cap is well under 2.
      await pool.query(
        `INSERT INTO driver_aggregator_relationships (driver_id, aggregator_id, status)
         SELECT d.id, a.id, 'active'
         FROM drivers d, aggregators a
         WHERE d.phone = $1 AND a.phone = $2
         ON CONFLICT (driver_id, aggregator_id) DO UPDATE SET status='active', claimed_at=NOW()`,
        [TEST_DRIVER_PHONE, TEST_AGGREGATOR_PHONE]
      );
      await pool.query(
        `DELETE FROM pending_transactions
         WHERE driver_id IN (SELECT id FROM drivers WHERE phone = $1)`,
        [TEST_DRIVER_PHONE]
      );
      await pool.query(
        `DELETE FROM dispatch_listings
         WHERE aggregator_id IN (SELECT id FROM aggregators WHERE phone = $1)`,
        [TEST_AGGREGATOR_PHONE]
      );
      // Open Greater Accra listing, 200kg PET, GHS 80 fee, 24h TTL.
      await pool.query(
        `INSERT INTO dispatch_listings
           (aggregator_id, pickup_location, material_type, gross_weight_kg,
            proposed_fee_ghs, region, status, expires_at)
         SELECT a.id, 'Madina', 'PET', 200, 80, 'Greater Accra', 'open',
                NOW() + INTERVAL '24 hours'
         FROM aggregators a WHERE a.phone = $1`,
        [TEST_AGGREGATOR_PHONE]
      );
    },
    steps: [
      { input: '',     match: /CON Circul Driver/ },
      { input: '5555', match: /Available work \(1\)/ },
      { input: '1',    match: /CON Available:[\s\S]*200kg PET GHS 80/ },
      { input: '1',    match: /CON 200kg PET[\s\S]*1\. Accept GHS 80/ },
      { input: '1',    match: /END/ },
    ],
    afterHook: async () => {
      const r = await pool.query(
        `SELECT do_.offer_type, do_.status
         FROM driver_offers do_
         JOIN drivers d ON d.id = do_.driver_id
         WHERE d.phone = $1
         ORDER BY do_.created_at DESC LIMIT 1`,
        [TEST_DRIVER_PHONE]
      );
      if (!r.rows.length) throw new Error('no driver_offers row after accept');
      if (r.rows[0].offer_type !== 'accept_proposed') {
        throw new Error('expected accept_proposed, got ' + r.rows[0].offer_type);
      }
    },
  },

  // ─── Case 7: driver counter-offers marketplace listing ─────────────────────
  // beforeHook creates the same fixture as Case 6 (fresh listing). Driver
  // picks "Counter", enters GHS 120, sends. Inserts a driver_offers row with
  // offer_type='counter' and counter_fee_ghs=120.
  {
    name: 'driver-counter-offers-marketplace-listing',
    phoneNumber: '0900003001',
    beforeHook: async () => {
      await pool.query(
        `INSERT INTO driver_aggregator_relationships (driver_id, aggregator_id, status)
         SELECT d.id, a.id, 'active'
         FROM drivers d, aggregators a
         WHERE d.phone = $1 AND a.phone = $2
         ON CONFLICT (driver_id, aggregator_id) DO UPDATE SET status='active', claimed_at=NOW()`,
        [TEST_DRIVER_PHONE, TEST_AGGREGATOR_PHONE]
      );
      // Order matters: dispatch_listings.pending_transaction_id FK points at
      // pending_transactions.id with NO ON DELETE action, so listings must die
      // first. driver_offers.listing_id has ON DELETE CASCADE, so dispatch_listings
      // DELETE sweeps any old offers automatically.
      await pool.query(
        `DELETE FROM dispatch_listings
         WHERE aggregator_id IN (SELECT id FROM aggregators WHERE phone = $1)`,
        [TEST_AGGREGATOR_PHONE]
      );
      await pool.query(
        `DELETE FROM pending_transactions
         WHERE driver_id IN (SELECT id FROM drivers WHERE phone = $1)`,
        [TEST_DRIVER_PHONE]
      );
      await pool.query(
        `DELETE FROM driver_offers
         WHERE driver_id IN (SELECT id FROM drivers WHERE phone = $1)`,
        [TEST_DRIVER_PHONE]
      );
      await pool.query(
        `INSERT INTO dispatch_listings
           (aggregator_id, pickup_location, material_type, gross_weight_kg,
            proposed_fee_ghs, region, status, expires_at)
         SELECT a.id, 'Madina', 'PET', 200, 80, 'Greater Accra', 'open',
                NOW() + INTERVAL '24 hours'
         FROM aggregators a WHERE a.phone = $1`,
        [TEST_AGGREGATOR_PHONE]
      );
    },
    steps: [
      { input: '',     match: /CON Circul Driver/ },
      { input: '5555', match: /Available work \(1\)/ },
      { input: '1',    match: /CON Available:[\s\S]*200kg PET GHS 80/ },
      { input: '1',    match: /2\. Counter/ },
      { input: '2',    match: /CON Your counter \(GHS\):/ },
      { input: '120',  match: /CON Counter GHS 120\n[\s\S]*1\. Send/ },
      { input: '1',    match: /END Counter sent/ },
    ],
    afterHook: async () => {
      const r = await pool.query(
        `SELECT do_.offer_type, do_.counter_fee_ghs
         FROM driver_offers do_
         JOIN drivers d ON d.id = do_.driver_id
         WHERE d.phone = $1
         ORDER BY do_.created_at DESC LIMIT 1`,
        [TEST_DRIVER_PHONE]
      );
      if (!r.rows.length) throw new Error('no driver_offers row after counter');
      if (r.rows[0].offer_type !== 'counter') throw new Error('expected counter, got ' + r.rows[0].offer_type);
      if (parseFloat(r.rows[0].counter_fee_ghs) !== 120) {
        throw new Error('expected counter_fee_ghs=120, got ' + r.rows[0].counter_fee_ghs);
      }
    },
  },

  // ─── Case 8: driver hits hard cap (3 active jobs) ──────────────────────────
  // beforeHook seeds 3 active pending_transactions for the driver (status
  // 'confirmed', driver_confirmed_at NULL) AND an open marketplace listing.
  // Driver tries to accept → server returns "Maximum 3 active jobs reached".
  {
    name: 'driver-hits-hard-cap',
    phoneNumber: '0900003001',
    beforeHook: async () => {
      await pool.query(
        `INSERT INTO driver_aggregator_relationships (driver_id, aggregator_id, status)
         SELECT d.id, a.id, 'active'
         FROM drivers d, aggregators a
         WHERE d.phone = $1 AND a.phone = $2
         ON CONFLICT (driver_id, aggregator_id) DO UPDATE SET status='active', claimed_at=NOW()`,
        [TEST_DRIVER_PHONE, TEST_AGGREGATOR_PHONE]
      );
      // Clear prior state — dispatch_listings BEFORE pending_transactions per FK.
      await pool.query(
        `DELETE FROM dispatch_listings
         WHERE aggregator_id IN (SELECT id FROM aggregators WHERE phone = $1)`,
        [TEST_AGGREGATOR_PHONE]
      );
      await pool.query(
        `DELETE FROM pending_transactions
         WHERE driver_id IN (SELECT id FROM drivers WHERE phone = $1)`,
        [TEST_DRIVER_PHONE]
      );
      // 3 active pending_transactions
      for (let i = 0; i < 3; i++) {
        await pool.query(
          `INSERT INTO pending_transactions
             (transaction_type, status, aggregator_id, driver_id, material_type,
              gross_weight_kg, price_per_kg, total_price, driver_fee_ghs)
           SELECT 'aggregator_sale', 'confirmed', a.id, d.id, 'PET', 100, 0, 0, 50
           FROM aggregators a, drivers d
           WHERE a.phone = $1 AND d.phone = $2`,
          [TEST_AGGREGATOR_PHONE, TEST_DRIVER_PHONE]
        );
      }
      // 4th — the listing we'll try to accept
      await pool.query(
        `INSERT INTO dispatch_listings
           (aggregator_id, pickup_location, material_type, gross_weight_kg,
            proposed_fee_ghs, region, status, expires_at)
         SELECT a.id, 'Madina', 'PET', 150, 60, 'Greater Accra', 'open',
                NOW() + INTERVAL '24 hours'
         FROM aggregators a WHERE a.phone = $1`,
        [TEST_AGGREGATOR_PHONE]
      );
    },
    steps: [
      { input: '',     match: /CON Circul Driver/ },
      { input: '5555', match: /Available work \(1\)/ },
      { input: '1',    match: /CON Available:[\s\S]*150kg PET GHS 60/ },
      { input: '1',    match: /1\. Accept GHS 60/ },
      { input: '1',    match: /END Maximum 3 active jobs reached/ },
    ],
  },

  // ─── Case 9: driver confirms delivery (single accepted weight) ─────────────
  // beforeHook creates 1 pending_transaction with driver_id + gross 100kg +
  // status='confirmed' + driver_confirmed_at NULL. Driver picks "Pending
  // deliveries", picks the row, enters accepted=95kg (within anomaly band),
  // confirms. driver_confirmed_at and accepted_weight_kg are written.
  {
    name: 'driver-confirms-delivery-single-weight',
    phoneNumber: '0900003001',
    beforeHook: async () => {
      await pool.query(
        `INSERT INTO driver_aggregator_relationships (driver_id, aggregator_id, status)
         SELECT d.id, a.id, 'active'
         FROM drivers d, aggregators a
         WHERE d.phone = $1 AND a.phone = $2
         ON CONFLICT (driver_id, aggregator_id) DO UPDATE SET status='active', claimed_at=NOW()`,
        [TEST_DRIVER_PHONE, TEST_AGGREGATOR_PHONE]
      );
      // dispatch_listings BEFORE pending_transactions per FK
      // (dispatch_listings.pending_transaction_id has no ON DELETE action).
      await pool.query(
        `DELETE FROM dispatch_listings
         WHERE aggregator_id IN (SELECT id FROM aggregators WHERE phone = $1)`,
        [TEST_AGGREGATOR_PHONE]
      );
      await pool.query(
        `DELETE FROM pending_transactions
         WHERE driver_id IN (SELECT id FROM drivers WHERE phone = $1)`,
        [TEST_DRIVER_PHONE]
      );
      // Clean ratings so rating-intercept doesn't fire on the post-confirm dial.
      await pool.query(
        `DELETE FROM ratings WHERE rater_type='driver'
         AND rater_id IN (SELECT id FROM drivers WHERE phone = $1)`,
        [TEST_DRIVER_PHONE]
      );
      await pool.query(
        `INSERT INTO pending_transactions
           (transaction_type, status, aggregator_id, driver_id, material_type,
            gross_weight_kg, price_per_kg, total_price, driver_fee_ghs)
         SELECT 'aggregator_sale', 'confirmed', a.id, d.id, 'PET', 100, 0, 0, 50
         FROM aggregators a, drivers d
         WHERE a.phone = $1 AND d.phone = $2`,
        [TEST_AGGREGATOR_PHONE, TEST_DRIVER_PHONE]
      );
    },
    steps: [
      { input: '',     match: /CON Circul Driver/ },
      { input: '5555', match: /Pending deliveries \(1\)/ },
      { input: '2',    match: /CON Confirm delivery:[\s\S]*1\. 100kg PET/ },
      { input: '1',    match: /Accepted weight/ },
      { input: '95',   match: /CON Pickup: 100kg\nAccepted: 95kg\nRejected: 5kg\n1\. Confirm/ },
      { input: '1',    match: /END/ },
    ],
    afterHook: async () => {
      const r = await pool.query(
        `SELECT accepted_weight_kg, driver_confirmed_at
         FROM pending_transactions
         WHERE driver_id IN (SELECT id FROM drivers WHERE phone = $1)
         ORDER BY created_at DESC LIMIT 1`,
        [TEST_DRIVER_PHONE]
      );
      if (!r.rows.length) throw new Error('no pending row found');
      if (!r.rows[0].driver_confirmed_at) throw new Error('driver_confirmed_at not set');
      if (parseFloat(r.rows[0].accepted_weight_kg) !== 95) {
        throw new Error('accepted_weight_kg expected 95, got ' + r.rows[0].accepted_weight_kg);
      }
    },
  },

  // ─── Case 10: driver rates aggregator (ASCII rating UI, no UCS-2 star) ─────
  // beforeHook creates a confirmed delivery (driver_confirmed_at recent) so
  // the pending-rating soft prompt fires. Driver dials in, picks "2. Rate now"
  // from the soft prompt, picks the transaction, picks "4. 5 stars". Server
  // returns ASCII text ("Your 5 star rating has been recorded").
  // customAssertions asserts NO UCS-2 star glyph appeared in any response
  // (Phase 9 ASCII discipline).
  {
    name: 'driver-rates-aggregator-ascii-no-star',
    phoneNumber: '0900003001',
    beforeHook: async () => {
      await pool.query(
        `INSERT INTO driver_aggregator_relationships (driver_id, aggregator_id, status)
         SELECT d.id, a.id, 'active'
         FROM drivers d, aggregators a
         WHERE d.phone = $1 AND a.phone = $2
         ON CONFLICT (driver_id, aggregator_id) DO UPDATE SET status='active', claimed_at=NOW()`,
        [TEST_DRIVER_PHONE, TEST_AGGREGATOR_PHONE]
      );
      // Clear prior pending + ratings to leave exactly one rateable row.
      // dispatch_listings BEFORE pending_transactions per FK.
      await pool.query(
        `DELETE FROM ratings WHERE rater_type='driver'
         AND rater_id IN (SELECT id FROM drivers WHERE phone = $1)`,
        [TEST_DRIVER_PHONE]
      );
      await pool.query(
        `DELETE FROM dispatch_listings
         WHERE aggregator_id IN (SELECT id FROM aggregators WHERE phone = $1)`,
        [TEST_AGGREGATOR_PHONE]
      );
      await pool.query(
        `DELETE FROM pending_transactions
         WHERE driver_id IN (SELECT id FROM drivers WHERE phone = $1)`,
        [TEST_DRIVER_PHONE]
      );
      // Confirmed delivery, recent driver_confirmed_at — eligible for rating.
      await pool.query(
        `INSERT INTO pending_transactions
           (transaction_type, status, aggregator_id, driver_id, material_type,
            gross_weight_kg, accepted_weight_kg, price_per_kg, total_price,
            driver_fee_ghs, driver_confirmed_at)
         SELECT 'aggregator_sale', 'confirmed', a.id, d.id, 'PET', 100, 95, 0, 0, 50, NOW()
         FROM aggregators a, drivers d
         WHERE a.phone = $1 AND d.phone = $2`,
        [TEST_AGGREGATOR_PHONE, TEST_DRIVER_PHONE]
      );
    },
    steps: [
      { input: '',     match: /Enter 4-digit PIN/ },
      { input: '5555', match: /CON 1 pending rating\n1\. Main menu\n2\. Rate now\n0\. Exit/ },
      { input: '2',    match: /CON Rate a transaction:[\s\S]*1\. 100kg PET/ },
      { input: '1',    match: /CON Rate [\s\S]*1\. 2 stars\n2\. 3 stars\n3\. 4 stars\n4\. 5 stars/ },
      { input: '4',    match: /END Thank you!\nYour 5 star rating/ },
    ],
    // Explicit negative assertion: no UCS-2 star glyph appeared in any response.
    // Per Phase 9, the platform-wide rating UI is ASCII-only — the star glyph
    // U+2605 forces UCS-2 encoding and truncates Yam-phone screens.
    customAssertions: (recordedResponses) => {
      const star = '★';
      const joined = recordedResponses.join('');
      if (joined.indexOf(star) !== -1) {
        throw new Error('UCS-2 star glyph found in rating screen - Phase 9 regression');
      }
    },
    afterHook: async () => {
      const r = await pool.query(
        `SELECT rating, rated_type FROM ratings
         WHERE rater_type = 'driver'
           AND rater_id IN (SELECT id FROM drivers WHERE phone = $1)
         ORDER BY id DESC LIMIT 1`,
        [TEST_DRIVER_PHONE]
      );
      if (!r.rows.length) throw new Error('rating row not inserted');
      if (r.rows[0].rating !== 5) throw new Error('expected 5 stars, got ' + r.rows[0].rating);
      if (r.rows[0].rated_type !== 'aggregator') {
        throw new Error('expected rated_type=aggregator, got ' + r.rows[0].rated_type);
      }
    },
  },

  // ─── Case 11: driver soft-prompt → "Main menu" path (no rating inserted) ───
  // Same fixture as case 10 (one rateable pending_transaction), but the driver
  // picks "1. Main menu" from the soft prompt instead of "2. Rate now".
  // Asserts: (a) the soft prompt appears post-PIN, (b) picking '1' falls
  // through to the regular main menu, (c) no rating row is inserted. Guards
  // against regression of the depth-math refactor in handleDriverUssd that
  // introduces `menuParts` to consume the soft-prompt slot.
  {
    name: 'driver-pending-rating-then-main-menu',
    phoneNumber: '0900003001',
    beforeHook: async () => {
      await pool.query(
        `INSERT INTO driver_aggregator_relationships (driver_id, aggregator_id, status)
         SELECT d.id, a.id, 'active'
         FROM drivers d, aggregators a
         WHERE d.phone = $1 AND a.phone = $2
         ON CONFLICT (driver_id, aggregator_id) DO UPDATE SET status='active', claimed_at=NOW()`,
        [TEST_DRIVER_PHONE, TEST_AGGREGATOR_PHONE]
      );
      await pool.query(
        `DELETE FROM ratings WHERE rater_type='driver'
         AND rater_id IN (SELECT id FROM drivers WHERE phone = $1)`,
        [TEST_DRIVER_PHONE]
      );
      await pool.query(
        `DELETE FROM dispatch_listings
         WHERE aggregator_id IN (SELECT id FROM aggregators WHERE phone = $1)`,
        [TEST_AGGREGATOR_PHONE]
      );
      await pool.query(
        `DELETE FROM pending_transactions
         WHERE driver_id IN (SELECT id FROM drivers WHERE phone = $1)`,
        [TEST_DRIVER_PHONE]
      );
      await pool.query(
        `INSERT INTO pending_transactions
           (transaction_type, status, aggregator_id, driver_id, material_type,
            gross_weight_kg, accepted_weight_kg, price_per_kg, total_price,
            driver_fee_ghs, driver_confirmed_at)
         SELECT 'aggregator_sale', 'confirmed', a.id, d.id, 'PET', 100, 95, 0, 0, 50, NOW()
         FROM aggregators a, drivers d
         WHERE a.phone = $1 AND d.phone = $2`,
        [TEST_AGGREGATOR_PHONE, TEST_DRIVER_PHONE]
      );
    },
    steps: [
      { input: '',     match: /Enter 4-digit PIN/ },
      { input: '5555', match: /CON 1 pending rating\n1\. Main menu\n2\. Rate now\n0\. Exit/ },
      { input: '1',    match: /CON Hi TestDriver!/ },
    ],
    afterHook: async () => {
      const r = await pool.query(
        `SELECT COUNT(*) AS n FROM ratings
         WHERE rater_type = 'driver'
           AND rater_id IN (SELECT id FROM drivers WHERE phone = $1)`,
        [TEST_DRIVER_PHONE]
      );
      if (parseInt(r.rows[0].n, 10) !== 0) {
        throw new Error('Expected 0 ratings after Main menu path, got ' + r.rows[0].n);
      }
    },
  },

  // ═══════════════════════════════════════════════════════════════════════════
  // PHASE 5: Impact Partner — 8 new test cases
  // ═══════════════════════════════════════════════════════════════════════════
  //
  // 6 HTTP-API tests + 2 USSD tests. The HTTP-API tests use steps=[] so the
  // runner's USSD loop is a no-op; all assertions live in beforeHook (which
  // throws on failure). Each test is self-contained — it logs in, performs
  // its endpoint calls, and asserts side-effects.

  // ─── HTTP: login happy path ─────────────────────────────────────────────
  {
    name: 'impact-partner-login-happy-path',
    phoneNumber: '0900099991',  // unused; required by harness shape
    steps: [],
    beforeHook: async () => {
      const r = await httpJson('POST', '/api/auth/login', {
        type: 'email', email: TEST_IP_EMAIL, password: TEST_IP_PASSWORD
      });
      if (r.status !== 200) throw new Error('login HTTP ' + r.status + ': ' + JSON.stringify(r.body));
      if (!r.body.success) throw new Error('login not success: ' + JSON.stringify(r.body));
      if (r.body.role !== 'impact_partner') throw new Error('expected role=impact_partner, got ' + r.body.role);
      if (!r.body.token) throw new Error('no token in response');
    },
  },

  // ─── HTTP: /network with no tags returns empty list ─────────────────────
  {
    name: 'impact-partner-network-empty',
    phoneNumber: '0900099991',
    steps: [],
    beforeHook: async () => {
      const login = await httpJson('POST', '/api/auth/login', {
        type: 'email', email: TEST_IP_EMAIL, password: TEST_IP_PASSWORD
      });
      const token = login.body.token;
      const r = await httpJson('GET', '/api/impact-partner/network', null, token);
      if (r.status !== 200) throw new Error('network HTTP ' + r.status);
      if (!r.body.success) throw new Error('network not success');
      if (!Array.isArray(r.body.network) || r.body.network.length !== 0) {
        throw new Error('expected empty network array, got ' + JSON.stringify(r.body.network));
      }
    },
  },

  // ─── HTTP: submit tag request for existing collector ────────────────────
  {
    name: 'impact-partner-tag-request-existing',
    phoneNumber: '0900099991',
    steps: [],
    beforeHook: async () => {
      const login = await httpJson('POST', '/api/auth/login', {
        type: 'email', email: TEST_IP_EMAIL, password: TEST_IP_PASSWORD
      });
      const token = login.body.token;
      const r = await httpJson('POST', '/api/impact-partner/tag-requests', {
        phone: TEST_COLLECTOR_PHONE.replace('+233', '0'),
        role: 'collector',
        proof_text: 'Test fixture — collector enrolled in our program 2026-04-01.'
      }, token);
      if (r.status !== 200) throw new Error('tag-request HTTP ' + r.status + ': ' + JSON.stringify(r.body));
      if (!r.body.success) throw new Error('tag-request not success: ' + JSON.stringify(r.body));
      if (r.body.tag_request.status !== 'pending_review') {
        throw new Error('expected pending_review, got ' + r.body.tag_request.status);
      }
      // Verify DB row
      const row = await pool.query(
        `SELECT status, mode FROM impact_partner_actor_tag_requests
         WHERE impact_partner_id = (SELECT id FROM impact_partners WHERE email = $1)
         ORDER BY id DESC LIMIT 1`,
        [TEST_IP_EMAIL]
      );
      if (!row.rows.length) throw new Error('no tag-request row in DB');
      if (row.rows[0].status !== 'pending_review') throw new Error('DB status not pending_review');
      if (row.rows[0].mode !== 'existing') throw new Error('DB mode not existing');
    },
  },

  // ─── HTTP: admin approve → impact_partner_actor_tags row inserted ───────
  {
    name: 'impact-partner-admin-approve',
    phoneNumber: '0900099991',
    steps: [],
    beforeHook: async () => {
      // Partner: submit a fresh tag request
      const login = await httpJson('POST', '/api/auth/login', {
        type: 'email', email: TEST_IP_EMAIL, password: TEST_IP_PASSWORD
      });
      const submit = await httpJson('POST', '/api/impact-partner/tag-requests', {
        phone: TEST_AGGREGATOR_PHONE.replace('+233', '0'),
        role: 'aggregator',
        proof_text: 'Test fixture aggregator partnership.'
      }, login.body.token);
      if (!submit.body.success) throw new Error('partner submit failed: ' + JSON.stringify(submit.body));
      const reqId = submit.body.tag_request.id;
      // Admin: login + approve
      const adminLogin = await httpJson('POST', '/api/admin/login', {
        email: TEST_ADMIN_EMAIL, password: TEST_ADMIN_PASSWORD
      });
      if (!adminLogin.body.success) throw new Error('admin login failed: ' + JSON.stringify(adminLogin.body));
      const approve = await httpJson(
        'POST', '/api/admin/impact-partner/tag-requests/' + reqId + '/approve',
        null, adminLogin.body.token
      );
      if (approve.status !== 200 || !approve.body.success) {
        throw new Error('approve failed: ' + JSON.stringify(approve.body));
      }
      // Verify tag row exists
      const tagRow = await pool.query(
        `SELECT id, deactivated_at FROM impact_partner_actor_tags
         WHERE tag_request_id = $1`,
        [reqId]
      );
      if (!tagRow.rows.length) throw new Error('no tag row inserted after approval');
      if (tagRow.rows[0].deactivated_at !== null) throw new Error('tag was inserted but deactivated');
    },
  },

  // ─── HTTP: untag sets deactivated_at ────────────────────────────────────
  {
    name: 'impact-partner-untag',
    phoneNumber: '0900099991',
    steps: [],
    beforeHook: async () => {
      // Seed: directly INSERT a tag for the test partner against the test collector
      const ip = await pool.query(`SELECT id FROM impact_partners WHERE email=$1`, [TEST_IP_EMAIL]);
      const col = await pool.query(`SELECT id FROM collectors WHERE phone=$1`, [TEST_COLLECTOR_PHONE]);
      if (!ip.rows.length || !col.rows.length) throw new Error('fixture missing (IP or collector)');
      const tag = await pool.query(
        `INSERT INTO impact_partner_actor_tags (impact_partner_id, actor_type, actor_id)
         VALUES ($1, 'collector', $2)
         ON CONFLICT (impact_partner_id, actor_type, actor_id) DO UPDATE
           SET deactivated_at = NULL, deactivation_reason = NULL, active_since = NOW()
         RETURNING id`,
        [ip.rows[0].id, col.rows[0].id]
      );
      const tagId = tag.rows[0].id;
      // Partner: login + untag
      const login = await httpJson('POST', '/api/auth/login', {
        type: 'email', email: TEST_IP_EMAIL, password: TEST_IP_PASSWORD
      });
      const del = await httpJson(
        'DELETE', '/api/impact-partner/tags/' + tagId, { reason: 'Test untag' }, login.body.token
      );
      if (del.status !== 200 || !del.body.success) {
        throw new Error('untag failed: ' + JSON.stringify(del.body));
      }
      const check = await pool.query(
        `SELECT deactivated_at, deactivation_reason FROM impact_partner_actor_tags WHERE id=$1`,
        [tagId]
      );
      if (!check.rows[0].deactivated_at) throw new Error('deactivated_at not set after untag');
      if (check.rows[0].deactivation_reason !== 'Test untag') {
        throw new Error('deactivation_reason mismatch: ' + check.rows[0].deactivation_reason);
      }
    },
  },

  // ─── HTTP: report shows kg for tagged actor's transactions ──────────────
  //
  // Seeds an active tag on TEST_COLLECTOR_PHONE + a transaction this YTD,
  // then calls /api/impact-partner/report and verifies total_kg >= 50.
  {
    name: 'impact-partner-report-kg',
    phoneNumber: '0900099991',
    steps: [],
    beforeHook: async () => {
      // Seed tag
      const ip = await pool.query(`SELECT id FROM impact_partners WHERE email=$1`, [TEST_IP_EMAIL]);
      const col = await pool.query(`SELECT id FROM collectors  WHERE phone=$1`, [TEST_COLLECTOR_PHONE]);
      const agg = await pool.query(`SELECT id FROM aggregators WHERE phone=$1`, [TEST_AGGREGATOR_PHONE]);
      if (!ip.rows.length || !col.rows.length || !agg.rows.length) throw new Error('fixtures missing');
      await pool.query(
        `INSERT INTO impact_partner_actor_tags (impact_partner_id, actor_type, actor_id)
         VALUES ($1, 'collector', $2)
         ON CONFLICT (impact_partner_id, actor_type, actor_id) DO UPDATE
           SET deactivated_at = NULL, deactivation_reason = NULL, active_since = NOW()`,
        [ip.rows[0].id, col.rows[0].id]
      );
      // Seed transaction (50kg PET, dated now)
      await pool.query(
        `INSERT INTO transactions
           (collector_id, aggregator_id, material_type, gross_weight_kg, net_weight_kg,
            price_per_kg, total_price, transaction_date)
         VALUES ($1, $2, 'PET', 50, 50, 0, 0, NOW())`,
        [col.rows[0].id, agg.rows[0].id]
      );
      // Call report endpoint
      const login = await httpJson('POST', '/api/auth/login', {
        type: 'email', email: TEST_IP_EMAIL, password: TEST_IP_PASSWORD
      });
      const r = await httpJson('GET', '/api/impact-partner/report?period=ytd', null, login.body.token);
      if (r.status !== 200 || !r.body.success) throw new Error('report failed: ' + JSON.stringify(r.body));
      const totalKg = parseFloat(r.body.totals.total_kg);
      if (totalKg < 50) {
        throw new Error('expected total_kg >= 50, got ' + totalKg + ' · breakdown=' + JSON.stringify(r.body.by_material));
      }
      if (!r.body.by_material || !r.body.by_material.PET || parseFloat(r.body.by_material.PET) < 50) {
        throw new Error('expected by_material.PET >= 50');
      }
    },
    afterHook: async () => {
      // Sweep the seed transactions so subsequent runs start clean.
      await pool.query(
        `DELETE FROM transactions
         WHERE collector_id IN (SELECT id FROM collectors WHERE phone = $1)
           AND aggregator_id IN (SELECT id FROM aggregators WHERE phone = $2)
           AND price_per_kg = 0`,
        [TEST_COLLECTOR_PHONE, TEST_AGGREGATOR_PHONE]
      );
    },
  },

  // ─── USSD: driver Programs view (More → 4. Programs) ────────────────────
  //
  // Driver dials, hits main menu (no pending rating since cleanup sweeps
  // pending_transactions), picks 4 (More), then 4 (Programs). Asserts the
  // END "Your programs:" screen renders the seeded TestPartner tag.
  {
    name: 'driver-programs-view',
    phoneNumber: '0900003001',
    beforeHook: async () => {
      const ip = await pool.query(`SELECT id FROM impact_partners WHERE email=$1`, [TEST_IP_EMAIL]);
      const drv = await pool.query(`SELECT id FROM drivers WHERE phone=$1`, [TEST_DRIVER_PHONE]);
      if (!ip.rows.length || !drv.rows.length) throw new Error('fixtures missing');
      // Ensure no pending rating intercept fires
      await pool.query(
        `DELETE FROM pending_transactions WHERE driver_id = $1`,
        [drv.rows[0].id]
      );
      await pool.query(
        `INSERT INTO impact_partner_actor_tags (impact_partner_id, actor_type, actor_id)
         VALUES ($1, 'driver', $2)
         ON CONFLICT (impact_partner_id, actor_type, actor_id) DO UPDATE
           SET deactivated_at = NULL, deactivation_reason = NULL, active_since = NOW()`,
        [ip.rows[0].id, drv.rows[0].id]
      );
    },
    steps: [
      { input: '',     match: /Enter 4-digit PIN/ },
      { input: '5555', match: /CON Hi TestDriver/ },
      { input: '4',    match: /CON More options[\s\S]*4\. Programs/ },
      { input: '4',    match: /END Your programs:[\s\S]*TestPartner/ },
    ],
  },

  // ─── USSD: collector Programs view (main menu → 5) ──────────────────────
  {
    name: 'collector-programs-view',
    phoneNumber: '0900000001',
    beforeHook: async () => {
      // Seed an active tag for the test collector.
      const ip = await pool.query(`SELECT id FROM impact_partners WHERE email=$1`, [TEST_IP_EMAIL]);
      const col = await pool.query(`SELECT id FROM collectors WHERE phone=$1`, [TEST_COLLECTOR_PHONE]);
      if (!ip.rows.length || !col.rows.length) throw new Error('fixtures missing');
      await pool.query(
        `INSERT INTO impact_partner_actor_tags (impact_partner_id, actor_type, actor_id)
         VALUES ($1, 'collector', $2)
         ON CONFLICT (impact_partner_id, actor_type, actor_id) DO UPDATE
           SET deactivated_at = NULL, deactivation_reason = NULL, active_since = NOW()`,
        [ip.rows[0].id, col.rows[0].id]
      );
    },
    steps: [
      { input: '',     match: /CON Circul Collector/ },
      { input: '0000', match: /CON 1\. Log Drop-off[\s\S]*5\. Programs/ },
      { input: '5',    match: /END Your programs:[\s\S]*TestPartner/ },
    ],
  },

  // ═══════════════════════════════════════════════════════════════════════════
  // PHASE 5b: Brand-aligned PDF + Excel reports — 5 new test cases
  // ═══════════════════════════════════════════════════════════════════════════
  //
  // Each test is order-independent: beforeHook scrubs tags + transactions
  // for the test partner / collector before setting up the precise state it
  // needs. Binary response bodies inspected via httpRaw (Buffer-safe).

  // ─── HTTP: PDF empty state ──────────────────────────────────────────────
  // Partner has zero active tags → PDF should still render (~empty-state
  // block visible) and content-type + size sanity-check.
  {
    name: 'impact-partner-report-pdf-empty',
    phoneNumber: '0900099991',
    steps: [],
    beforeHook: async () => {
      // Scrub all tags for test partner so the window is provably empty
      await pool.query(
        `DELETE FROM impact_partner_actor_tags
         WHERE impact_partner_id IN (SELECT id FROM impact_partners WHERE email=$1)`,
        [TEST_IP_EMAIL]
      );
      const login = await httpJson('POST', '/api/auth/login', {
        type: 'email', email: TEST_IP_EMAIL, password: TEST_IP_PASSWORD
      });
      const r = await httpRaw('GET', '/api/impact-partner/report.pdf?period=ytd', null, login.body.token);
      if (r.status !== 200) throw new Error('PDF empty HTTP ' + r.status + ' body=' + r.body.toString('utf8').slice(0, 200));
      const ct = r.headers['content-type'] || '';
      if (!ct.startsWith('application/pdf')) throw new Error('expected pdf content-type, got ' + ct);
      if (r.body.length < 1000) throw new Error('PDF empty body too small: ' + r.body.length + ' bytes');
      // PDF magic number sanity
      if (r.body.slice(0, 4).toString() !== '%PDF') throw new Error('body does not start with %PDF magic');
    },
  },

  // ─── HTTP: PDF populated ────────────────────────────────────────────────
  // Seeded tag + transaction → PDF size > 5000 bytes (header band + hero +
  // table + methodology + footer all rendering means the byte budget grows).
  {
    name: 'impact-partner-report-pdf-populated',
    phoneNumber: '0900099991',
    steps: [],
    beforeHook: async () => {
      const ip = await pool.query(`SELECT id FROM impact_partners WHERE email=$1`, [TEST_IP_EMAIL]);
      const col = await pool.query(`SELECT id FROM collectors  WHERE phone=$1`, [TEST_COLLECTOR_PHONE]);
      const agg = await pool.query(`SELECT id FROM aggregators WHERE phone=$1`, [TEST_AGGREGATOR_PHONE]);
      if (!ip.rows.length || !col.rows.length || !agg.rows.length) throw new Error('fixtures missing');
      // Clean slate then re-seed deterministic state
      await pool.query(
        `DELETE FROM impact_partner_actor_tags
         WHERE impact_partner_id = $1`, [ip.rows[0].id]
      );
      await pool.query(
        `DELETE FROM transactions WHERE collector_id = $1 AND aggregator_id = $2 AND price_per_kg = 0`,
        [col.rows[0].id, agg.rows[0].id]
      );
      await pool.query(
        `INSERT INTO impact_partner_actor_tags (impact_partner_id, actor_type, actor_id)
         VALUES ($1, 'collector', $2)
         ON CONFLICT (impact_partner_id, actor_type, actor_id) DO UPDATE
           SET deactivated_at = NULL, deactivation_reason = NULL, active_since = NOW()`,
        [ip.rows[0].id, col.rows[0].id]
      );
      await pool.query(
        `INSERT INTO transactions
           (collector_id, aggregator_id, material_type, gross_weight_kg, net_weight_kg,
            price_per_kg, total_price, transaction_date)
         VALUES ($1, $2, 'PET', 55, 55, 0, 0, NOW())`,
        [col.rows[0].id, agg.rows[0].id]
      );
      const login = await httpJson('POST', '/api/auth/login', {
        type: 'email', email: TEST_IP_EMAIL, password: TEST_IP_PASSWORD
      });
      const r = await httpRaw('GET', '/api/impact-partner/report.pdf?period=ytd', null, login.body.token);
      if (r.status !== 200) throw new Error('PDF populated HTTP ' + r.status);
      const ct = r.headers['content-type'] || '';
      if (!ct.startsWith('application/pdf')) throw new Error('expected pdf content-type, got ' + ct);
      if (r.body.length < 5000) throw new Error('PDF populated body too small: ' + r.body.length + ' bytes');
      if (r.body.slice(0, 4).toString() !== '%PDF') throw new Error('body does not start with %PDF magic');
    },
    afterHook: async () => {
      // Sweep the seed transaction so the next run starts clean
      await pool.query(
        `DELETE FROM transactions
         WHERE collector_id IN (SELECT id FROM collectors WHERE phone = $1)
           AND aggregator_id IN (SELECT id FROM aggregators WHERE phone = $2)
           AND price_per_kg = 0`,
        [TEST_COLLECTOR_PHONE, TEST_AGGREGATOR_PHONE]
      );
    },
  },

  // ─── HTTP: Excel empty state ────────────────────────────────────────────
  {
    name: 'impact-partner-report-xlsx-empty',
    phoneNumber: '0900099991',
    steps: [],
    beforeHook: async () => {
      await pool.query(
        `DELETE FROM impact_partner_actor_tags
         WHERE impact_partner_id IN (SELECT id FROM impact_partners WHERE email=$1)`,
        [TEST_IP_EMAIL]
      );
      const login = await httpJson('POST', '/api/auth/login', {
        type: 'email', email: TEST_IP_EMAIL, password: TEST_IP_PASSWORD
      });
      const r = await httpRaw('GET', '/api/impact-partner/report.xlsx?period=ytd', null, login.body.token);
      if (r.status !== 200) throw new Error('XLSX empty HTTP ' + r.status);
      const ct = r.headers['content-type'] || '';
      if (!ct.includes('spreadsheetml')) throw new Error('expected xlsx content-type, got ' + ct);
      if (r.body.length < 1000) throw new Error('XLSX empty body too small: ' + r.body.length + ' bytes');
      // XLSX is a zip — first two bytes "PK"
      if (r.body[0] !== 0x50 || r.body[1] !== 0x4b) throw new Error('body does not start with PK zip magic');
    },
  },

  // ─── HTTP: Excel populated ──────────────────────────────────────────────
  {
    name: 'impact-partner-report-xlsx-populated',
    phoneNumber: '0900099991',
    steps: [],
    beforeHook: async () => {
      const ip = await pool.query(`SELECT id FROM impact_partners WHERE email=$1`, [TEST_IP_EMAIL]);
      const col = await pool.query(`SELECT id FROM collectors  WHERE phone=$1`, [TEST_COLLECTOR_PHONE]);
      const agg = await pool.query(`SELECT id FROM aggregators WHERE phone=$1`, [TEST_AGGREGATOR_PHONE]);
      await pool.query(
        `DELETE FROM impact_partner_actor_tags WHERE impact_partner_id = $1`, [ip.rows[0].id]
      );
      await pool.query(
        `DELETE FROM transactions WHERE collector_id = $1 AND aggregator_id = $2 AND price_per_kg = 0`,
        [col.rows[0].id, agg.rows[0].id]
      );
      await pool.query(
        `INSERT INTO impact_partner_actor_tags (impact_partner_id, actor_type, actor_id)
         VALUES ($1, 'collector', $2)
         ON CONFLICT (impact_partner_id, actor_type, actor_id) DO UPDATE
           SET deactivated_at = NULL, deactivation_reason = NULL, active_since = NOW()`,
        [ip.rows[0].id, col.rows[0].id]
      );
      await pool.query(
        `INSERT INTO transactions
           (collector_id, aggregator_id, material_type, gross_weight_kg, net_weight_kg,
            price_per_kg, total_price, transaction_date)
         VALUES ($1, $2, 'HDPE', 28, 28, 0, 0, NOW())`,
        [col.rows[0].id, agg.rows[0].id]
      );
      const login = await httpJson('POST', '/api/auth/login', {
        type: 'email', email: TEST_IP_EMAIL, password: TEST_IP_PASSWORD
      });
      const r = await httpRaw('GET', '/api/impact-partner/report.xlsx?period=ytd', null, login.body.token);
      if (r.status !== 200) throw new Error('XLSX populated HTTP ' + r.status);
      const ct = r.headers['content-type'] || '';
      if (!ct.includes('spreadsheetml')) throw new Error('expected xlsx content-type, got ' + ct);
      if (r.body.length < 5000) throw new Error('XLSX populated body too small: ' + r.body.length + ' bytes');
      if (r.body[0] !== 0x50 || r.body[1] !== 0x4b) throw new Error('body does not start with PK zip magic');
    },
    afterHook: async () => {
      await pool.query(
        `DELETE FROM transactions
         WHERE collector_id IN (SELECT id FROM collectors WHERE phone = $1)
           AND aggregator_id IN (SELECT id FROM aggregators WHERE phone = $2)
           AND price_per_kg = 0`,
        [TEST_COLLECTOR_PHONE, TEST_AGGREGATOR_PHONE]
      );
    },
  },

  // ─── HTTP: filename slug pattern verified on Content-Disposition ────────
  // Hits both .pdf and .xlsx endpoints; asserts the header matches
  // circul-impact-report-{slug}-{period}-{YYYY-MM-DD}.{ext}
  {
    name: 'impact-partner-report-filename',
    phoneNumber: '0900099991',
    steps: [],
    beforeHook: async () => {
      const login = await httpJson('POST', '/api/auth/login', {
        type: 'email', email: TEST_IP_EMAIL, password: TEST_IP_PASSWORD
      });
      // TEST_IP_NAME = 'TestPartner' → slug 'testpartner'
      const expectPdf  = /attachment; filename="circul-impact-report-testpartner-ytd-\d{4}-\d{2}-\d{2}\.pdf"/;
      const expectXlsx = /attachment; filename="circul-impact-report-testpartner-mtd-\d{4}-\d{2}-\d{2}\.xlsx"/;
      const pdf  = await httpRaw('GET', '/api/impact-partner/report.pdf?period=ytd',  null, login.body.token);
      const xlsx = await httpRaw('GET', '/api/impact-partner/report.xlsx?period=mtd', null, login.body.token);
      const cdPdf  = pdf.headers['content-disposition']  || '';
      const cdXlsx = xlsx.headers['content-disposition'] || '';
      if (!expectPdf.test(cdPdf))   throw new Error('PDF filename mismatch · got: ' + cdPdf);
      if (!expectXlsx.test(cdXlsx)) throw new Error('XLSX filename mismatch · got: ' + cdXlsx);
    },
  },
];

// ── runner ────────────────────────────────────────────────────────────────────

async function main() {
  console.log('[test-ussd] target: ' + BASE);
  const filtered = FILTER ? TESTS.filter(t => t.name.includes(FILTER)) : TESTS;
  console.log('[test-ussd] cases: ' + filtered.length + ' (of ' + TESTS.length + ')' + (FILTER ? ' [filter: ' + FILTER + ']' : ''));

  await cleanupTestData();
  await seedTestAccounts();
  console.log('[test-ussd] pre-run cleanup + seed done\n');

  const results = [];
  for (const t of filtered) {
    const r = await runTest(t);
    results.push(r);
    if (r.ok) {
      console.log('  PASS  ' + r.name);
    } else {
      console.log('  FAIL  ' + r.name + ' (step ' + r.stepIndex + ')');
      console.log('        ' + r.reason.replace(/\n/g, '\n        '));
    }
  }

  await cleanupTestData();
  console.log('\n[test-ussd] post-run cleanup done');

  const pass = results.filter(r => r.ok).length;
  const fail = results.length - pass;
  console.log('\n[test-ussd] summary: ' + pass + ' passed, ' + fail + ' failed (of ' + results.length + ')');

  await pool.end();
  process.exit(fail === 0 ? 0 : 1);
}

main().catch(err => {
  console.error('[test-ussd] runner crashed:', err);
  pool.end().catch(() => {});
  process.exit(2);
});
