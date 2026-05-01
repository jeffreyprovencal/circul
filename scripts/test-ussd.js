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
const TEST_SESSION_PREFIX    = 'test-ussd-';

// Pre-seeded test accounts (phones used by the test cases below).
const TEST_COLLECTOR_PHONE   = '+233900000001'; // dialed as 0900000001
const TEST_COLLECTOR_PIN     = '0000';
const TEST_AGGREGATOR_PHONE  = '+233900001001'; // dialed as 0900001001
const TEST_AGGREGATOR_PIN    = '2222';
const TEST_AGENT_PHONE       = '+233900002001'; // dialed as 0900002001
const TEST_AGENT_PIN         = '3333';
const TEST_GATE_PHONE        = '+233900000098'; // dialed as 0900000098, must_change_pin=true
const TEST_UNREGISTERED      = '0900099999';    // not in any table

// Marketplace fixtures for display_name regression coverage (PR-feat/ussd-critical-cons).
// TEST_BUY_AGG owns an open buy request on PET in Accra so the collector
// "browse buyers → match interest" flow has a deterministic target row.
// TEST_OFFER_PROC sends an incoming offer to TestAgg Probe so the aggregator
// "my offers → review" flow has a deterministic target row.
const TEST_BUY_AGG_PHONE     = '+233900001050';
const TEST_OFFER_PROC_EMAIL  = 'test-offer@circul-test.local';

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
  const sessionId = TEST_SESSION_PREFIX + Math.random().toString(36).slice(2, 10);
  const history = [];
  for (let i = 0; i < t.steps.length; i++) {
    const r = await runStep(t.steps[i], history, sessionId, t.phoneNumber);
    if (!r.ok) return { name: t.name, ok: false, stepIndex: i, reason: r.reason };
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
// Some inline-register flows use a 0900099xxx phone for "unknown collector"
// scenarios; sweep those too.
const ANY_TEST_LIKES    = ['+233900%', '0900%'];

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
    `INSERT INTO aggregators (name, phone, pin, city, region, is_active, must_change_pin)
     VALUES ('TestAgg Probe', $1, $2, 'Accra', 'Greater Accra', true, false)
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

  // ── Marketplace fixtures (display_name regression coverage) ─────────────────
  // Buy-request aggregator with a known display_name. Drives the collector
  // "match interest" screen so we can assert the bounded buyer_name renders.
  const buyAggIns = await pool.query(
    `INSERT INTO aggregators (name, display_name, phone, pin, city, region, is_active, must_change_pin)
     VALUES ('Test Buy Co Aggregator', 'Test Buy Co', $1, $2, 'Accra', 'Greater Accra', true, false)
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
      { input: '',     match: /CON Circul Aggregator/ },
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
      { input: '1',    match: /CON Register:\n1\. Collector\n2\. Agent\n0\. Back/ },
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

  // ─── force-change-pin gate (PR #67) ─────────────────────────────────────────
  {
    name: 'force-change-pin-gate-collector',
    phoneNumber: '0900000098',
    steps: [
      { input: '',     match: /CON Circul Collector/ },
      { input: '0000', match: /CON You must set a new PIN/ },
      { input: '5678', match: /CON Confirm new PIN/ },
      { input: '5678', match: /CON PIN saved!/ },
      { input: '1',    match: /CON 1\. Log Drop-off/ }, // bridges into main menu
    ],
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
      { input: '4.5',        match: /CON Confirm purchase:\n5kg PET\nfrom TestColl Probe\nGH₵22\.50\n1\. Confirm\n0\. Cancel/ },
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
      { input: '1',    match: /CON Offer from Test Offer Co:\n500kg PET\nGH₵ 2\.20\/kg = GH₵ 1100\.00\n1\. Accept\n2\. Decline\n0\. Back/ },
    ],
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
