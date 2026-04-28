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
  // Order matters: agent_activity FKs to agents + aggregators, so it must
  // be cleared first. Then agents (FK to aggregators), then aggregators,
  // then collectors. Each step is best-effort and isolated so a failure in
  // one doesn't skip the rest.
  await safeDelete('sessions', () =>
    pool.query(`DELETE FROM ussd_sessions WHERE session_id LIKE $1`, [TEST_SESSION_PREFIX + '%'])
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
      { input: '4.5',        match: /CON Confirm purchase:\n5kg PET from\nTestColl Probe.*\nGH₵22\.50 \(GH₵4\.50\/kg\)\n\n1\. Confirm\n0\. Cancel/ },
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
