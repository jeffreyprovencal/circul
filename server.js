const express = require('express');
const { Pool } = require('pg');
const path = require('path');
const fs = require('fs');
const crypto = require('crypto');
const multer = require('multer');
const CirculRoles = require('./shared/roles');
const { EVENTS, notify, notifyAdmin } = require('./shared/notifications');
const { normalizeGhanaPhone, getPhoneVariants } = require('./shared/phone');
const { getPendingRatings, createRating } = require('./shared/ratings');
const {
  resolveParties,
  userOwnsParty,
  validateBuyerFks,
  resolveSeller,
  resolveBuyer,
  txnTypeForRoles,
  PARTY_MAP,
  KIND_TO_TABLE
} = require('./shared/transaction-parties');
const {
  attributeAndInsert,
  insertRootTransaction,
  InsufficientSourceError
} = require('./shared/chain-of-custody-db');
const { ROOT_TYPES: COC_ROOT_TYPES } = require('./shared/chain-of-custody');

const {
  WINDOW_DAYS: COC_WINDOW_DAYS,
  EXCLUDED_STATUSES: COC_EXCLUDED_STATUSES,
  candidateFilterForSeller
} = require('./shared/chain-of-custody-db');

// Translate an InsufficientSourceError into a 400 response body.
// Returns true if the error was handled (response sent); false otherwise.
//
// err.reason distinguishes two shapes:
//   'shortfall'              — FIFO path couldn't cover target
//   'invalid_manual_sources' — caller's explicit target.sources hint was bad
// Frontend reads `reason` to switch UX (toast vs highlight-specific-rows).
function handleInsufficientSource(res, err) {
  if (!(err instanceof InsufficientSourceError)) return false;
  const reason = err.reason || 'shortfall';
  const details = {
    reason: reason,
    seller_kind: err.seller ? err.seller.kind : null,
    seller_id: err.seller ? err.seller.id : null,
    material_type: err.target.material_type,
    requested_kg: err.target.gross_weight_kg
  };
  if (reason === 'shortfall') {
    details.shortfall_kg = err.shortfall_kg;
    details.candidates_considered = err.candidates_considered;
    details.candidates_total_remaining_kg = err.candidates_total_remaining_kg;
  } else {
    // invalid_manual_sources — pass through whichever diagnostic fields fired.
    if (err.invalid_shape_entries != null)  details.invalid_shape_entries = err.invalid_shape_entries;
    if (err.invalid_source_ids != null)     details.invalid_source_ids = err.invalid_source_ids;
    if (err.insufficient_remaining != null) details.insufficient_remaining = err.insufficient_remaining;
    if (err.sum_mismatch_kg != null)        details.sum_mismatch_kg = err.sum_mismatch_kg;
    if (err.hint_total_kg != null)          details.hint_total_kg = err.hint_total_kg;
    if (err.target_kg != null)              details.target_kg = err.target_kg;
  }
  res.status(400).json({
    success: false,
    error: 'insufficient_source_material',
    message: err.message,
    details: details
  });
  return true;
}
const app = express();
const port = process.env.PORT || 3000;

// Expense receipt uploads → public/uploads/receipts/
const receiptDir = path.join(__dirname, 'public', 'uploads', 'receipts');
if (!fs.existsSync(receiptDir)) fs.mkdirSync(receiptDir, { recursive: true });

const receiptUpload = multer({
  storage: multer.diskStorage({
    destination: (req, file, cb) => cb(null, receiptDir),
    filename: (req, file, cb) => {
      const ext = path.extname(file.originalname) || '.jpg';
      cb(null, `receipt-${Date.now()}${ext}`);
    }
  }),
  limits: { fileSize: 5 * 1024 * 1024 }, // 5 MB
  fileFilter: (req, file, cb) => {
    const allowed = /jpeg|jpg|png|webp/;
    cb(null, allowed.test(file.mimetype));
  }
});

if (!process.env.DATABASE_URL) {
  console.error('ERROR: DATABASE_URL environment variable is required');
  process.exit(1);
}

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL?.includes('localhost') ? false : { rejectUnauthorized: false }
});

app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.set('trust proxy', 1);

app.get('/health', (req, res) => res.json({ status: 'healthy' }));
app.use('/shared', express.static(path.join(__dirname, 'shared')));

// Clean URLs — serve .html files without the extension
app.use((req, res, next) => {
    if (req.method !== 'GET' || req.path.includes('.') || req.path === '/') return next();
    const htmlPath = path.join(__dirname, 'public', req.path + '.html');
    fs.access(htmlPath, fs.constants.F_OK, (err) => {
          if (err) return next();
          res.sendFile(htmlPath);
    });
});
app.use(express.static(path.join(__dirname, 'public')));

// ============================================
// AUTH HELPERS
// ============================================

const ADMIN_SECRET = process.env.ADMIN_SECRET;
const AUTH_SECRET  = process.env.AUTH_SECRET || process.env.BUYER_SECRET;

if (!ADMIN_SECRET || !AUTH_SECRET) {
  console.error('FATAL: ADMIN_SECRET and AUTH_SECRET environment variables are required.');
  console.error('Set them in your Render environment or .env file.');
  process.exit(1);
}

function generateToken(payload, secret) {
  const withExp = { ...payload, iat: Math.floor(Date.now() / 1000), exp: Math.floor(Date.now() / 1000) + (24 * 60 * 60) };
  const data = JSON.stringify(withExp);
  const b64  = Buffer.from(data).toString('base64url');
  const sig  = crypto.createHmac('sha256', secret).update(b64).digest('base64url');
  return b64 + '.' + sig;
}

function verifyToken(token, secret) {
  try {
    const [b64, sig] = token.split('.');
    const expected = crypto.createHmac('sha256', secret).update(b64).digest('base64url');
    if (sig !== expected) return null;
    const decoded = JSON.parse(Buffer.from(b64, 'base64url').toString());
    if (decoded.exp && decoded.exp < Math.floor(Date.now() / 1000)) return null;
    return decoded;
  } catch { return null; }
}

async function verifyPassword(password, hash) {
  if (!hash) return false;
  // Legacy: if hash has no colon, it's plaintext from old registrations
  if (!hash.includes(':')) {
    const match = password === hash;
    if (match) {
      // Silently migrate legacy plaintext password to scrypt hash
      hashPassword(password).then(newHash => {
        // Fire-and-forget: update all possible tables
        const tables = ['collectors', 'aggregators', 'processors', 'converters', 'recyclers'];
        tables.forEach(t => {
          pool.query(`UPDATE ${t} SET pin = $1 WHERE pin = $2`, [newHash, hash]).catch(() => {});
        });
        // Also update password_hash for email-based logins
        ['processors', 'converters', 'recyclers'].forEach(t => {
          pool.query(`UPDATE ${t} SET password_hash = $1 WHERE password_hash = $2`, [newHash, hash]).catch(() => {});
        });
      }).catch(() => {});
    }
    return match;
  }
  return new Promise((resolve, reject) => {
    const [salt, stored] = hash.split(':');
    crypto.scrypt(password, salt, 64, (err, key) => {
      if (err) reject(err);
      else resolve(key.toString('hex') === stored);
    });
  });
}

async function hashPassword(password) {
  return new Promise((resolve, reject) => {
    const salt = crypto.randomBytes(16).toString('hex');
    crypto.scrypt(password, salt, 64, (err, key) => {
      if (err) reject(err);
      else resolve(salt + ':' + key.toString('hex'));
    });
  });
}

// ── Account-recovery helpers (shared by USSD reset flow + admin endpoints) ──

function generateOtp() {
  return String(crypto.randomInt(100000, 1000000));
}

function hashOtp(code) {
  return crypto.createHash('sha256').update(String(code)).digest('hex');
}

function verifyOtp(entered, hash) {
  const enteredHash = hashOtp(entered);
  if (enteredHash.length !== hash.length) return false;
  return crypto.timingSafeEqual(Buffer.from(enteredHash), Buffer.from(hash));
}

async function recordAdminAction(client, { actor_type, actor_id, actor_email, action, target_type, target_id, details }) {
  const runner = client || pool;
  await runner.query(
    `INSERT INTO admin_audit_log (actor_type, actor_id, actor_email, action, target_type, target_id, details)
     VALUES ($1,$2,$3,$4,$5,$6,$7)`,
    [actor_type, actor_id || null, actor_email || null, action, target_type, target_id, details || {}]
  );
}

function requireAdmin(req, res, next) {
  const auth  = req.headers.authorization || '';
  const token = auth.replace('Bearer ', '').trim() || req.query.token;
  if (!token) return res.status(401).json({ success: false, message: 'Admin auth required' });
  const payload = verifyToken(token, ADMIN_SECRET);
  if (!payload || payload.type !== 'admin') return res.status(401).json({ success: false, message: 'Invalid admin token' });
  req.admin = payload;
  next();
}

function requireAuth(req, res, next) {
  const auth  = req.headers.authorization || '';
  const token = auth.replace('Bearer ', '').trim() || req.query.token;
  if (!token) return res.status(401).json({ success: false, message: 'Auth required' });
  const payload = verifyToken(token, AUTH_SECRET);
  if (!payload) return res.status(401).json({ success: false, message: 'Invalid token' });
  req.user = payload;
  req.user.hasRole = (r) =>
    req.user.role === r || (Array.isArray(req.user.roles) && req.user.roles.includes(r));
  next();
}

// ── Login rate limiting ──
const loginAttempts = new Map(); // phone → { count, firstAttempt }
const MAX_LOGIN_ATTEMPTS = 5;
const LOCKOUT_DURATION_MS = 15 * 60 * 1000; // 15 minutes

function checkRateLimit(phone) {
  const entry = loginAttempts.get(phone);
  if (!entry) return { blocked: false };
  if (Date.now() - entry.firstAttempt > LOCKOUT_DURATION_MS) {
    loginAttempts.delete(phone);
    return { blocked: false };
  }
  if (entry.count >= MAX_LOGIN_ATTEMPTS) {
    const remainMs = LOCKOUT_DURATION_MS - (Date.now() - entry.firstAttempt);
    return { blocked: true, remainMin: Math.ceil(remainMs / 60000) };
  }
  return { blocked: false };
}

function recordFailedLogin(phone) {
  const entry = loginAttempts.get(phone);
  if (!entry) {
    loginAttempts.set(phone, { count: 1, firstAttempt: Date.now() });
  } else {
    entry.count++;
  }
}

function clearLoginAttempts(phone) {
  loginAttempts.delete(phone);
}

// Clean up stale rate-limit entries every hour
setInterval(() => {
  const now = Date.now();
  for (const [phone, entry] of loginAttempts) {
    if (now - entry.firstAttempt > LOCKOUT_DURATION_MS) loginAttempts.delete(phone);
  }
}, 60 * 60 * 1000);

// ── Name-privacy helper ──
// Adjacent tiers always see real names. Non-adjacent tiers see names
// only if they share a completed direct transaction.
async function canSeeName(viewerRole, viewerId, counterpartyRole, counterpartyId) {
  // Adjacent tiers → always visible
  if (CirculRoles.isAdjacentTier(viewerRole, counterpartyRole)) return true;
  // Same tier → visible
  if (viewerRole === counterpartyRole) return true;
  // Check for a direct completed transaction between these two users
  const roleCol = (r) => r + '_id';
  const vCol = roleCol(viewerRole);
  const cCol = roleCol(counterpartyRole);
  // Check transactions table (collector↔aggregator completions)
  const txCheck = await pool.query(
    `SELECT 1 FROM transactions WHERE ${vCol} = $1 AND ${cCol} = $2 LIMIT 1`,
    [viewerId, counterpartyId]
  ).catch(() => ({ rows: [] }));
  if (txCheck.rows.length > 0) return true;
  // Check pending_transactions table (all other tier completions)
  const ptCheck = await pool.query(
    `SELECT 1 FROM pending_transactions WHERE ${vCol} = $1 AND ${cCol} = $2 AND status = 'completed' LIMIT 1`,
    [viewerId, counterpartyId]
  ).catch(() => ({ rows: [] }));
  return ptCheck.rows.length > 0;
}

// ============================================
// COLLECTORS
// ============================================

app.post('/api/collectors', async (req, res) => {
  try {
    const { first_name, last_name, phone, pin, region } = req.body;
    if (!first_name || !pin) return res.status(400).json({ success: false, message: 'First name and PIN are required' });
    if (pin.length < 4 || pin.length > 6) return res.status(400).json({ success: false, message: 'PIN must be 4-6 digits' });
    const result = await pool.query(
      `INSERT INTO collectors (first_name, last_name, phone, pin, region)
       VALUES ($1,$2,$3,$4,$5)
       RETURNING id, first_name, last_name, phone, region, average_rating, created_at`,
      [first_name, last_name || '', phone || null, pin, region || null]
    );
    res.status(201).json({ success: true, collector: result.rows[0] });
  } catch (err) {
    if (err.code === '23505') return res.status(409).json({ success: false, message: 'Phone number already registered' });
    console.error('Error creating collector:', err);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

// Register a new collector from aggregator dashboard
app.post('/api/aggregators/:id/register-collector', async (req, res) => {
  try {
    const { name, phone, region } = req.body;
    if (!name || !name.trim()) return res.status(400).json({ success: false, message: 'Name is required' });
    if (!phone || !/^0\d{9}$/.test(phone.trim())) return res.status(400).json({ success: false, message: 'Phone must be 10 digits starting with 0' });
    if (!region || !CirculRoles.GHANA_REGIONS.includes(region)) return res.status(400).json({ success: false, message: 'Valid Ghana region is required' });
    const parts = name.trim().split(/\s+/);
    const first_name = parts[0];
    const last_name = parts.slice(1).join(' ') || '';
    const result = await pool.query(
      `INSERT INTO collectors (first_name, last_name, phone, pin, region, must_change_pin)
       VALUES ($1,$2,$3,$4,$5,true)
       RETURNING id, first_name, last_name, phone, region`,
      [first_name, last_name, phone.trim(), await hashPassword('0000'), region]
    );
    const c = result.rows[0];
    res.status(201).json({ success: true, id: c.id, name: ((c.first_name || '') + ' ' + (c.last_name || '')).trim(), phone: c.phone });
  } catch (err) {
    if (err.code === '23505') return res.status(409).json({ success: false, message: 'Phone number already registered' });
    console.error('POST /api/aggregators/:id/register-collector error:', err);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

// Change collector PIN (first-login flow)
app.patch('/api/collectors/:id/change-pin', requireAuth, async (req, res) => {
  try {
    if (!req.user.hasRole('collector')) return res.status(403).json({ success: false, message: 'Collector access only' });
    if (parseInt(req.params.id) !== req.user.id) return res.status(403).json({ success: false, message: 'Can only change your own PIN' });
    const { pin } = req.body;
    if (!pin || pin.length < 4 || pin.length > 6 || !/^\d+$/.test(pin)) return res.status(400).json({ success: false, message: 'PIN must be 4-6 digits' });
    const hashedPin = await hashPassword(pin);
    await pool.query(`UPDATE collectors SET pin=$1, must_change_pin=false, updated_at=NOW() WHERE id=$2`, [hashedPin, req.user.id]);
    res.json({ success: true, message: 'PIN changed successfully' });
  } catch (err) {
    console.error('PATCH /api/collectors/:id/change-pin error:', err);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

app.post('/api/collectors/login', async (req, res) => {
  try {
    const { phone, pin } = req.body;
    if (!phone || !pin) return res.status(400).json({ success: false, message: 'Phone and PIN required' });
    const rl = checkRateLimit(phone.trim());
    if (rl.blocked) return res.status(429).json({ success: false, message: 'Too many failed attempts. Try again in ' + rl.remainMin + ' minutes.' });
    const result = await pool.query(
      `SELECT id, first_name, last_name, phone, pin, region, average_rating, created_at
       FROM collectors WHERE phone=$1 AND is_active=true`,
      [phone]
    );
    if (!result.rows.length) {
      recordFailedLogin(phone.trim());
      return res.status(401).json({ success: false, message: 'Invalid phone or PIN' });
    }
    const pinValid = await verifyPassword(pin, result.rows[0].pin);
    if (!pinValid) {
      recordFailedLogin(phone.trim());
      return res.status(401).json({ success: false, message: 'Invalid phone or PIN' });
    }
    clearLoginAttempts(phone.trim());
    const collector = result.rows[0];
    delete collector.pin;
    res.json({ success: true, collector: { ...collector, role: 'collector' } });
  } catch (err) {
    console.error('Error logging in collector:', err);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

app.get('/api/collectors', async (req, res) => {
  try {
    const { phone } = req.query;
    const params = [];
    let whereExtra = '';
    if (phone) { params.push(phone.trim()); whereExtra = ` AND c.phone=$${params.length}`; }
    const result = await pool.query(
      `SELECT c.id, c.first_name, c.last_name, c.phone, c.city, c.region, c.average_rating,
              c.is_active, c.id_verified, c.created_at,
              'COL-' || LPAD(c.id::text, 4, '0') AS display_name,
              COALESCE(SUM(t.net_weight_kg),0) as total_weight_kg,
              COUNT(t.id) as transaction_count
       FROM collectors c
       LEFT JOIN transactions t ON t.collector_id=c.id
       WHERE c.is_active=true${whereExtra}
       GROUP BY c.id
       ORDER BY c.average_rating DESC NULLS LAST, c.created_at DESC`,
      params
    );
    res.json({ success: true, collectors: result.rows });
  } catch (err) {
    console.error('Error listing collectors:', err);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

app.get('/api/collectors/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const result = await pool.query(
      `SELECT c.id, c.first_name, c.last_name, c.phone, c.region, c.city, c.average_rating,
              c.is_active, c.id_verified, c.created_at,
              'COL-' || LPAD(c.id::text, 4, '0') AS display_name,
              COALESCE(SUM(t.net_weight_kg),0) as total_weight_kg,
              COUNT(t.id) as transaction_count
       FROM collectors c
       LEFT JOIN transactions t ON t.collector_id=c.id
       WHERE c.id=$1 GROUP BY c.id`,
      [parseInt(id)]
    );
    if (!result.rows.length) return res.status(404).json({ success: false, message: 'Collector not found' });
    const c = result.rows[0];
    c.name = ((c.first_name||'') + (c.last_name ? ' '+c.last_name : '')).trim();
    return res.json({ success: true, collector: c });
  } catch (err) {
    console.error('Error fetching collector:', err);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

app.get('/api/collectors/:id/stats', async (req, res) => {
  try {
    const { id } = req.params;
    const collector = await pool.query(
      `SELECT id, first_name, last_name, phone, region, city, average_rating, id_verified, created_at FROM collectors WHERE id=$1`,
      [id]
    );
    if (!collector.rows.length) return res.status(404).json({ success: false, message: 'Collector not found' });

    const [stats, total, recent, todayStats] = await Promise.all([
      pool.query(`SELECT material_type, SUM(net_weight_kg) as total_kg, SUM(total_price) as total_earned, AVG(contamination_deduction_percent) as avg_contamination, COUNT(*) as count FROM transactions WHERE collector_id=$1 GROUP BY material_type ORDER BY total_kg DESC`, [id]),
      pool.query(`SELECT COALESCE(SUM(net_weight_kg),0) as total_kg, COALESCE(SUM(total_price),0) as total_earned, COUNT(*) as total_transactions FROM transactions WHERE collector_id=$1`, [id]),
      pool.query(`SELECT * FROM transactions WHERE collector_id=$1 ORDER BY transaction_date DESC LIMIT 10`, [id]),
      pool.query(`SELECT COALESCE(SUM(net_weight_kg),0) as today_kg, COALESCE(SUM(total_price),0) as today_earned, COUNT(*) as today_count FROM transactions WHERE collector_id=$1 AND transaction_date>=CURRENT_DATE`, [id])
    ]);

    res.json({
      success: true,
      collector: collector.rows[0],
      stats: { ...total.rows[0], today: todayStats.rows[0], by_material: stats.rows, recent_transactions: recent.rows }
    });
  } catch (err) {
    console.error('Error fetching collector stats:', err);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

// ============================================
// COLLECTOR (authenticated, singular) ROUTES
// ============================================

app.get('/api/collector/me', requireAuth, async (req, res) => {
  try {
    if (!req.user.hasRole('collector')) return res.status(403).json({ success: false, message: 'Collector access only' });
    const id = req.user.id;
    // avg_rating / ratings_count come from a live scalar subquery against the
    // ratings table (same source of truth as /api/collector/stats line 481).
    // Do NOT read collectors.average_rating — that denormalized column drifts
    // (no backfill on rating insert), which caused the 2026-04-22 audit failure
    // where the hero said "No ratings yet" while the stat card showed ⭐ 4.7.
    const result = await pool.query(
      `SELECT c.id, c.first_name, c.last_name, c.phone, c.region, c.city,
              c.is_active, c.id_verified, c.created_at,
              'COL-' || LPAD(c.id::text, 4, '0') AS display_name,
              COALESCE(SUM(t.net_weight_kg),0) as total_weight_kg,
              COUNT(t.id) as transaction_count,
              (SELECT AVG(rating)::NUMERIC(3,2) FROM ratings
                 WHERE rated_type='collector' AND rated_id=c.id) AS avg_rating,
              (SELECT COUNT(*) FROM ratings
                 WHERE rated_type='collector' AND rated_id=c.id) AS ratings_count
       FROM collectors c
       LEFT JOIN transactions t ON t.collector_id=c.id
       WHERE c.id=$1 GROUP BY c.id`, [id]
    );
    if (!result.rows.length) return res.status(404).json({ success: false, message: 'Collector not found' });
    const c = result.rows[0];
    c.name = ((c.first_name||'') + (c.last_name ? ' '+c.last_name : '')).trim();
    c.collector_id = c.id;
    c.status = c.is_active ? 'Active' : 'Inactive';
    res.json(c);
  } catch (err) { console.error('GET /api/collector/me error:', err); res.status(500).json({ success: false, message: 'Server error' }); }
});

app.get('/api/collector/stats', requireAuth, async (req, res) => {
  try {
    if (!req.user.hasRole('collector')) return res.status(403).json({ success: false, message: 'Collector access only' });
    const id = req.user.id;
    const thisMonth = new Date(); thisMonth.setDate(1); thisMonth.setHours(0,0,0,0);
    const ytdStart = new Date(new Date().getFullYear(), 0, 1).toISOString();
    const [total, monthly, ytd, ratings] = await Promise.all([
      pool.query(`SELECT COALESCE(SUM(net_weight_kg),0) as total_kg, COALESCE(SUM(total_price),0) as total_earned, COUNT(*) as total_collections FROM transactions WHERE collector_id=$1`, [id]),
      pool.query(`SELECT COALESCE(SUM(net_weight_kg),0) as month_kg, COALESCE(SUM(total_price),0) as month_earned, COUNT(*) as month_collections FROM transactions WHERE collector_id=$1 AND transaction_date>=$2`, [id, thisMonth.toISOString()]),
      pool.query(`SELECT COALESCE(SUM(net_weight_kg),0) as ytd_kg FROM transactions WHERE collector_id=$1 AND transaction_date>=$2`, [id, ytdStart]),
      pool.query(`SELECT AVG(rating)::NUMERIC(3,2) as avg_rating, COUNT(*) as count FROM ratings WHERE rated_type='collector' AND rated_id=$1`, [id]).catch(() => ({ rows: [{ avg_rating: null, count: 0 }] }))
    ]);
    res.json({ ...total.rows[0], ...monthly.rows[0], ytd_kg: ytd.rows[0].ytd_kg, avg_rating: ratings.rows[0].avg_rating });
  } catch (err) { console.error('GET /api/collector/stats error:', err); res.status(500).json({ success: false, message: 'Server error' }); }
});

app.get('/api/collector/transactions', requireAuth, async (req, res) => {
  try {
    if (!req.user.hasRole('collector')) return res.status(403).json({ success: false, message: 'Collector access only' });
    const id = req.user.id;
    const result = await pool.query(
      `SELECT t.*, a.name as aggregator_name, a.company as aggregator_company,
              t.aggregator_id,
              EXISTS(SELECT 1 FROM ratings r WHERE r.transaction_id=t.id AND r.rater_type='collector' AND r.rater_id=$1) as rated
       FROM transactions t
       LEFT JOIN aggregators a ON a.id=t.aggregator_id
       WHERE t.collector_id=$1
       ORDER BY t.transaction_date DESC LIMIT 50`, [id]
    );
    result.rows.forEach(r => {
      r.aggregator_code = CirculRoles.circulCode('aggregator', r.aggregator_id);
      r.aggregator_name_visible = true; // collector↔aggregator is adjacent
    });
    res.json(result.rows);
  } catch (err) { console.error('GET /api/collector/transactions error:', err); res.status(500).json({ success: false, message: 'Server error' }); }
});

app.get('/api/collector/prices', requireAuth, async (req, res) => {
  try {
    if (!req.user.hasRole('collector')) return res.status(403).json({ success: false, message: 'Collector access only' });
    const result = await pool.query(
      `SELECT pp.poster_id as aggregator_id, a.name as aggregator_name, a.company,
              COALESCE((SELECT AVG(r.rating)::NUMERIC(3,2) FROM ratings r WHERE r.rated_type='aggregator' AND r.rated_id=a.id), 0) as avg_rating,
              (SELECT COUNT(*) FROM ratings WHERE rated_type='aggregator' AND rated_id=a.id) as rating_count,
              json_object_agg(pp.material_type, pp.price_per_kg_ghs) FILTER (WHERE pp.material_type IS NOT NULL) as prices
       FROM posted_prices pp
       JOIN aggregators a ON a.id=pp.poster_id AND a.is_active=true
       WHERE pp.poster_type='aggregator' AND pp.is_active=true AND pp.material_type IS NOT NULL
       GROUP BY pp.poster_id, a.id`
    );
    result.rows.forEach(r => {
      r.aggregator_code = CirculRoles.circulCode('aggregator', r.aggregator_id);
      r.name_visible = true; // collector↔aggregator adjacent
    });
    res.json(result.rows);
  } catch (err) { console.error('GET /api/collector/prices error:', err); res.status(500).json({ success: false, message: 'Server error' }); }
});

app.get('/api/collector/top-buyers', requireAuth, async (req, res) => {
  try {
    if (!req.user.hasRole('collector')) return res.status(403).json({ success: false, message: 'Collector access only' });
    const id = req.user.id;
    const { period } = req.query;
    const since = period === 'ytd'
      ? new Date(new Date().getFullYear(), 0, 1).toISOString()
      : (() => { const d = new Date(); d.setDate(1); d.setHours(0,0,0,0); return d.toISOString(); })();
    const result = await pool.query(
      `SELECT a.id as aggregator_id, COALESCE(a.company, a.name, 'Unknown') as aggregator_name,
              SUM(t.net_weight_kg) as ytd_kg,
              SUM(CASE WHEN t.transaction_date >= $2 THEN t.net_weight_kg ELSE 0 END) as month_kg,
              AVG(t.price_per_kg) as avg_price,
              COUNT(*) as transaction_count
       FROM transactions t
       JOIN aggregators a ON a.id=t.aggregator_id
       WHERE t.collector_id=$1
       GROUP BY a.id ORDER BY ytd_kg DESC LIMIT 5`,
      [id, since]
    );
    result.rows.forEach(r => {
      r.aggregator_code = CirculRoles.circulCode('aggregator', r.aggregator_id);
      r.name_visible = true;
    });
    res.json(result.rows);
  } catch (err) { console.error('GET /api/collector/top-buyers error:', err); res.status(500).json({ success: false, message: 'Server error' }); }
});

app.get('/api/collector/pl', requireAuth, async (req, res) => {
  try {
    if (!req.user.hasRole('collector')) return res.status(403).json({ success: false, message: 'Collector access only' });
    const id = req.user.id;
    const qMonth = parseInt(req.query.month);
    const qYear = parseInt(req.query.year);
    const now = new Date();
    const selYear = (qYear && qYear >= 2020 && qYear <= now.getFullYear()) ? qYear : now.getFullYear();
    const selMonth = (qMonth && qMonth >= 1 && qMonth <= 12) ? qMonth - 1 : now.getMonth();
    const monthStart = new Date(selYear, selMonth, 1);
    const monthEnd = new Date(selYear, selMonth + 1, 1);
    const ytdStart = new Date(selYear, 0, 1).toISOString();
    const ytdEnd = new Date(selYear + 1, 0, 1).toISOString();
    const [monthly, ytd] = await Promise.all([
      pool.query(`SELECT material_type, COALESCE(SUM(total_price),0) as earned FROM transactions WHERE collector_id=$1 AND transaction_date>=$2 AND transaction_date<$3 GROUP BY material_type`, [id, monthStart.toISOString(), monthEnd.toISOString()]),
      pool.query(`SELECT material_type, COALESCE(SUM(total_price),0) as earned FROM transactions WHERE collector_id=$1 AND transaction_date>=$2 AND transaction_date<$3 GROUP BY material_type`, [id, ytdStart, ytdEnd])
    ]);
    const monthMap = {}; monthly.rows.forEach(r => { monthMap[r.material_type] = parseFloat(r.earned); });
    const ytdMap = {}; ytd.rows.forEach(r => { ytdMap[r.material_type] = parseFloat(r.earned); });
    res.json({ month: monthMap, ytd: ytdMap });
  } catch (err) { console.error('GET /api/collector/pl error:', err); res.status(500).json({ success: false, message: 'Server error' }); }
});

app.get('/api/collector/pending-purchases', requireAuth, async (req, res) => {
  try {
    if (!req.user.hasRole('collector')) return res.status(403).json({ success: false, message: 'Collector access only' });
    const id = req.user.id;
    const result = await pool.query(
      `SELECT pt.*, a.name as aggregator_name, a.company as aggregator_company,
              'AGG-' || LPAD(a.id::text, 4, '0') AS aggregator_display
       FROM pending_transactions pt
       LEFT JOIN aggregators a ON a.id=pt.aggregator_id
       WHERE pt.collector_id=$1 AND pt.transaction_type='aggregator_purchase' AND pt.status='pending'
       ORDER BY pt.created_at DESC`, [id]
    );
    res.json(result.rows);
  } catch (err) { console.error('GET /api/collector/pending-purchases error:', err); res.status(500).json({ success: false, message: 'Server error' }); }
});

app.post('/api/collector/confirm-receipt', requireAuth, async (req, res) => {
  try {
    if (!req.user.hasRole('collector')) return res.status(403).json({ success: false, message: 'Collector access only' });
    const { transaction_id } = req.body;
    if (!transaction_id) return res.status(400).json({ success: false, message: 'transaction_id required' });
    const txn = await pool.query(`SELECT * FROM transactions WHERE id=$1 AND collector_id=$2`, [transaction_id, req.user.id]);
    if (!txn.rows.length) return res.status(404).json({ success: false, message: 'Transaction not found' });
    const result = await pool.query(
      `UPDATE transactions SET payment_status='paid', updated_at=NOW() WHERE id=$1 AND collector_id=$2 RETURNING *`,
      [transaction_id, req.user.id]
    );
    res.json({ success: true, transaction: result.rows[0] });
  } catch (err) { console.error('POST /api/collector/confirm-receipt error:', err); res.status(500).json({ success: false, message: 'Server error' }); }
});

app.post('/api/collector/transactions/:id/confirm', requireAuth, async (req, res) => {
  try {
    if (!req.user.hasRole('collector')) return res.status(403).json({ success: false, message: 'Collector access only' });
    const collectorId = req.user.id;
    const txnId = req.params.id;
    const txn = await pool.query(`SELECT * FROM transactions WHERE id=$1 AND collector_id=$2`, [txnId, collectorId]);
    if (!txn.rows.length) return res.status(404).json({ success: false, message: 'Transaction not found' });
    const result = await pool.query(
      `UPDATE transactions SET payment_status='paid', updated_at=NOW() WHERE id=$1 AND collector_id=$2 RETURNING *`,
      [txnId, collectorId]
    );
    res.json({ success: true, transaction: result.rows[0] });
  } catch (err) { console.error('POST /api/collector/transactions/:id/confirm error:', err); res.status(500).json({ success: false, message: 'Server error' }); }
});

app.post('/api/collector/pending-purchases/:id/accept', requireAuth, async (req, res) => {
  try {
    if (!req.user.hasRole('collector')) return res.status(403).json({ success: false, message: 'Collector access only' });
    const result = await pool.query(
      `UPDATE pending_transactions SET status='accepted', updated_at=NOW() WHERE id=$1 AND collector_id=$2 AND status='pending' RETURNING *`,
      [req.params.id, req.user.id]
    );
    if (!result.rows.length) {
      const check = await pool.query(`SELECT id, collector_id, status FROM pending_transactions WHERE id=$1`, [req.params.id]);
      if (!check.rows.length) return res.status(404).json({ success: false, message: 'Pending transaction not found' });
      if (check.rows[0].collector_id !== req.user.id) return res.status(403).json({ success: false, message: 'This transaction belongs to a different collector' });
      if (check.rows[0].status !== 'pending') return res.status(409).json({ success: false, message: 'Transaction already ' + check.rows[0].status });
      return res.status(404).json({ success: false, message: 'Could not accept transaction' });
    }
    res.json({ success: true, pending_transaction: result.rows[0] });
  } catch (err) { console.error('POST /api/collector/pending-purchases/:id/accept error:', err); res.status(500).json({ success: false, message: 'Server error' }); }
});

app.post('/api/collector/pending-purchases/:id/decline', requireAuth, async (req, res) => {
  try {
    if (!req.user.hasRole('collector')) return res.status(403).json({ success: false, message: 'Collector access only' });
    const result = await pool.query(
      `UPDATE pending_transactions SET status='declined', updated_at=NOW() WHERE id=$1 AND collector_id=$2 AND status='pending' RETURNING *`,
      [req.params.id, req.user.id]
    );
    if (!result.rows.length) {
      const check = await pool.query(`SELECT id, collector_id, status FROM pending_transactions WHERE id=$1`, [req.params.id]);
      if (!check.rows.length) return res.status(404).json({ success: false, message: 'Pending transaction not found' });
      if (check.rows[0].collector_id !== req.user.id) return res.status(403).json({ success: false, message: 'This transaction belongs to a different collector' });
      if (check.rows[0].status !== 'pending') return res.status(409).json({ success: false, message: 'Transaction already ' + check.rows[0].status });
      return res.status(404).json({ success: false, message: 'Could not decline transaction' });
    }
    res.json({ success: true, pending_transaction: result.rows[0] });
  } catch (err) { console.error('POST /api/collector/pending-purchases/:id/decline error:', err); res.status(500).json({ success: false, message: 'Server error' }); }
});

app.post('/api/collector/rate-aggregator', requireAuth, async (req, res) => {
  try {
    if (!req.user.hasRole('collector')) return res.status(403).json({ success: false, message: 'Collector access only' });
    const { transaction_id, aggregator_id, rating, tags, note } = req.body;
    if (!rating || rating < 1 || rating > 5) return res.status(400).json({ success: false, message: 'Rating must be 1-5' });
    // Prevent duplicate ratings
    const existing = await pool.query(
      `SELECT id FROM ratings WHERE transaction_id=$1 AND rater_type='collector' AND rater_id=$2`, [transaction_id, req.user.id]
    );
    if (existing.rows.length) return res.status(409).json({ success: false, message: 'You have already rated this transaction' });
    const pgTags = Array.isArray(tags) && tags.length ? '{' + tags.map(t => '"' + String(t).replace(/"/g, '\\"') + '"').join(',') + '}' : '{}';
    const result = await pool.query(
      `INSERT INTO ratings (transaction_id, rater_type, rater_id, rated_type, rated_id, rating, tags, notes, rating_direction)
       VALUES ($1, 'collector', $2, 'aggregator', $3, $4, $5::TEXT[], $6, 'upward') RETURNING *`,
      [transaction_id, req.user.id, aggregator_id, rating, pgTags, note || null]
    );
    res.json({ success: true, rating: result.rows[0] });
  } catch (err) {
    if (err.code === '23505') return res.status(409).json({ success: false, message: 'You have already rated this transaction' });
    console.error('POST /api/collector/rate-aggregator error:', err); res.status(500).json({ success: false, message: 'Server error' });
  }
});

// ============================================
// ROLE /me ENDPOINTS (authenticated, token-based)
// ============================================

app.get('/api/aggregator/me', requireAuth, async (req, res) => {
  try {
    if (!req.user.hasRole('aggregator')) return res.status(403).json({ success: false, message: 'Aggregator access only' });
    const id = req.user.id;
    const result = await pool.query(
      `SELECT a.id, a.name, a.company, a.phone, a.city, a.region, a.country,
              a.is_active, a.id_verified, a.created_at,
              COALESCE((SELECT AVG(r.rating)::NUMERIC(3,2) FROM ratings r WHERE r.rated_type='aggregator' AND r.rated_id=a.id), 0) AS avg_rating,
              'AGG-' || LPAD(a.id::text, 4, '0') AS display_name
       FROM aggregators a WHERE a.id=$1`, [id]
    );
    if (!result.rows.length) return res.status(404).json({ success: false, message: 'Aggregator not found' });
    const a = result.rows[0];
    a.status = a.is_active ? 'Active' : 'Inactive';
    res.json({ success: true, aggregator: a });
  } catch (err) { console.error('GET /api/aggregator/me error:', err); res.status(500).json({ success: false, message: 'Server error' }); }
});

app.get('/api/processor/me', requireAuth, async (req, res) => {
  try {
    if (!req.user.hasRole('processor')) return res.status(403).json({ success: false, message: 'Processor access only' });
    const id = req.user.id;
    const result = await pool.query(
      `SELECT id, name, company, email, city, region, country, is_active, created_at
       FROM processors WHERE id=$1`, [id]
    );
    if (!result.rows.length) return res.status(404).json({ success: false, message: 'Processor not found' });
    const p = result.rows[0];
    p.status = p.is_active ? 'Active' : 'Inactive';
    res.json({ success: true, processor: p });
  } catch (err) { console.error('GET /api/processor/me error:', err); res.status(500).json({ success: false, message: 'Server error' }); }
});

app.get('/api/converter/me', requireAuth, async (req, res) => {
  try {
    if (!req.user.hasRole('converter')) return res.status(403).json({ success: false, message: 'Converter access only' });
    const id = req.user.converter_id || req.user.id;
    const result = await pool.query(
      `SELECT id, name, company, email, city, region, country, is_active, created_at
       FROM converters WHERE id=$1`, [id]
    );
    if (!result.rows.length) return res.status(404).json({ success: false, message: 'Converter not found' });
    const c = result.rows[0];
    c.status = c.is_active ? 'Active' : 'Inactive';
    res.json({ success: true, converter: c });
  } catch (err) { console.error('GET /api/converter/me error:', err); res.status(500).json({ success: false, message: 'Server error' }); }
});

app.get('/api/recycler/me', requireAuth, async (req, res) => {
  try {
    if (!req.user.hasRole('recycler')) return res.status(403).json({ success: false, message: 'Recycler access only' });
    const id = req.user.id;
    const result = await pool.query(
      `SELECT id, name, company, email, city, region, country, is_active, created_at
       FROM recyclers WHERE id=$1`, [id]
    );
    if (!result.rows.length) return res.status(404).json({ success: false, message: 'Recycler not found' });
    const r = result.rows[0];
    r.status = r.is_active ? 'Active' : 'Inactive';
    res.json({ success: true, recycler: r });
  } catch (err) { console.error('GET /api/recycler/me error:', err); res.status(500).json({ success: false, message: 'Server error' }); }
});

// ============================================
// AGGREGATORS
// ============================================

app.get('/api/aggregators', async (req, res) => {
  try {
    const { phone } = req.query;
    const params = []; let where = 'WHERE is_active=true';
    if (phone) { params.push(phone.trim()); where += ` AND phone=$${params.length}`; }
    const result = await pool.query(
      `SELECT id, name, company, phone, city, region, country, is_active, id_verified, created_at, 'AGG-' || LPAD(id::text, 4, '0') AS display_name FROM aggregators ${where} ORDER BY name ASC`,
      params
    );
    res.json({ success: true, aggregators: result.rows });
  } catch (err) {
    console.error('Error listing aggregators:', err);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

app.get('/api/aggregators/:id', async (req, res) => {
  try {
    const result = await pool.query(
      `SELECT id, name, company, phone, city, region, country, is_active, id_verified, created_at, 'AGG-' || LPAD(id::text, 4, '0') AS display_name FROM aggregators WHERE id=$1`,
      [req.params.id]
    );
    if (!result.rows.length) return res.status(404).json({ success: false, message: 'Aggregator not found' });
    res.json({ success: true, aggregator: result.rows[0] });
  } catch (err) { res.status(500).json({ success: false, message: 'Server error' }); }
});

app.get('/api/aggregators/:id/stats', async (req, res) => {
  try {
    const { id } = req.params;
    const agg = await pool.query(`SELECT * FROM aggregators WHERE id=$1 AND is_active=true`, [id]);
    if (!agg.rows.length) return res.status(404).json({ success: false, message: 'Aggregator not found' });
    const aggregator = agg.rows[0];
    const thisMonth = new Date(); thisMonth.setDate(1); thisMonth.setHours(0,0,0,0);

    const [totals, monthlyTotals, pending, activeCollectors, byMaterial, topCollectors, postedPrices, ratings, sales] = await Promise.all([
      pool.query(`SELECT COALESCE(SUM(net_weight_kg),0) as total_kg, COALESCE(SUM(total_price),0) as total_value, COUNT(*) as total_txns FROM transactions WHERE aggregator_id=$1`, [id]),
      pool.query(`SELECT COALESCE(SUM(net_weight_kg),0) as month_kg, COALESCE(SUM(total_price),0) as month_value, COUNT(*) as month_txns FROM transactions WHERE aggregator_id=$1 AND transaction_date>=$2`, [id, thisMonth.toISOString()]),
      pool.query(`SELECT COUNT(*) as count, COALESCE(SUM(total_price),0) as value FROM transactions WHERE aggregator_id=$1 AND payment_status='unpaid' AND total_price>0`, [id]),
      pool.query(`SELECT COUNT(DISTINCT collector_id) as count FROM transactions WHERE aggregator_id=$1`, [id]),
      pool.query(`SELECT material_type, SUM(net_weight_kg) as kg, COUNT(*) as txns FROM transactions WHERE aggregator_id=$1 GROUP BY material_type ORDER BY kg DESC`, [id]),
      pool.query(`SELECT c.id, c.first_name, c.last_name, c.phone, c.average_rating, c.city, 'COL-' || LPAD(c.id::text, 4, '0') AS display_name, SUM(t.net_weight_kg) as total_kg, COUNT(t.id) as txns FROM collectors c JOIN transactions t ON t.collector_id=c.id WHERE t.aggregator_id=$1 GROUP BY c.id ORDER BY total_kg DESC LIMIT 20`, [id]),
      pool.query(`SELECT * FROM posted_prices WHERE poster_type='aggregator' AND poster_id=$1 AND is_active=true ORDER BY material_type`, [id]).catch(() => ({ rows: [] })),
      pool.query(`SELECT AVG(rating)::NUMERIC(3,2) as avg_rating, COUNT(*) as count FROM ratings WHERE rated_type='aggregator' AND rated_id=$1`, [id]).catch(() => ({ rows: [{ avg_rating: null, count: 0 }] })),
      pool.query(`SELECT COALESCE(SUM(total_price),0) as total_sold, COALESCE(SUM(CASE WHEN created_at >= $2 THEN total_price ELSE 0 END),0) as month_sold FROM pending_transactions WHERE aggregator_id=$1 AND transaction_type='aggregator_sale' AND status IN ('dispatch_approved','arrived','completed')`, [id, thisMonth.toISOString()]).catch(() => ({ rows: [{ total_sold: 0, month_sold: 0 }] }))
    ]);

    // ── P&L data (month-over-month) ──────────────────────────
    const now = new Date();
    const plThisStart = new Date(now.getFullYear(), now.getMonth(), 1).toISOString().slice(0, 10);
    const plLastStart = new Date(now.getFullYear(), now.getMonth() - 1, 1).toISOString().slice(0, 10);
    const plLastEnd   = new Date(now.getFullYear(), now.getMonth(), 0).toISOString().slice(0, 10);
    const plToday     = now.toISOString().slice(0, 10);

    const [revThis, revLast, cogsLast, opexThis, opexLast] = await Promise.all([
      // Revenue this month (aggregator sales to processors)
      pool.query(
        `SELECT COALESCE(SUM(total_price),0) AS total FROM pending_transactions
         WHERE aggregator_id=$1 AND transaction_type='aggregator_sale'
           AND status IN ('dispatch_approved','arrived','completed')
           AND created_at >= $2`,
        [id, plThisStart]
      ).catch(() => ({ rows: [{ total: 0 }] })),
      // Revenue last month
      pool.query(
        `SELECT COALESCE(SUM(total_price),0) AS total FROM pending_transactions
         WHERE aggregator_id=$1 AND transaction_type='aggregator_sale'
           AND status IN ('dispatch_approved','arrived','completed')
           AND created_at >= $2 AND created_at < $3`,
        [id, plLastStart, plThisStart]
      ).catch(() => ({ rows: [{ total: 0 }] })),
      // COGS last month (purchases from collectors)
      pool.query(
        `SELECT COALESCE(SUM(total_price),0) AS total FROM transactions
         WHERE aggregator_id=$1
           AND transaction_date >= $2 AND transaction_date < $3`,
        [id, plLastStart, plThisStart]
      ).catch(() => ({ rows: [{ total: 0 }] })),
      // OpEx this month
      pool.query(
        `SELECT COALESCE(SUM(amount),0) AS total FROM expense_entries
         WHERE aggregator_id=$1 AND expense_date >= $2 AND expense_date <= $3`,
        [id, plThisStart, plToday]
      ).catch(() => ({ rows: [{ total: 0 }] })),
      // OpEx last month
      pool.query(
        `SELECT COALESCE(SUM(amount),0) AS total FROM expense_entries
         WHERE aggregator_id=$1 AND expense_date >= $2 AND expense_date <= $3`,
        [id, plLastStart, plLastEnd]
      ).catch(() => ({ rows: [{ total: 0 }] }))
    ]);

    // Revenue & COGS this month already available from earlier queries
    const revenue     = parseFloat(revThis.rows[0].total);
    const revenuePrev = parseFloat(revLast.rows[0].total);
    const cogs        = parseFloat(monthlyTotals.rows[0].month_value);
    const cogsPrev    = parseFloat(cogsLast.rows[0].total);
    const opex        = parseFloat(opexThis.rows[0].total);
    const opexPrev    = parseFloat(opexLast.rows[0].total);

    const gross     = revenue - cogs;
    const grossPrev = revenuePrev - cogsPrev;
    const net       = gross - opex;
    const netPrev   = grossPrev - opexPrev;

    const pct = (part, whole) => whole === 0 ? 0 : Math.round((part / whole) * 1000) / 10;
    const mom = (curr, prev) => prev === 0 ? (curr > 0 ? 100 : 0) : Math.round(((curr - prev) / prev) * 1000) / 10;

    res.json({
      success: true,
      operator: { ...aggregator, role: 'aggregator' },
      stats: {
        totals: totals.rows[0], this_month: monthlyTotals.rows[0],
        pending_payments: pending.rows[0], active_collectors: activeCollectors.rows[0].count,
        by_material: byMaterial.rows, top_collectors: topCollectors.rows,
        posted_prices: postedPrices.rows, ratings: ratings.rows[0],
        sales: sales.rows[0],
        pl: {
          revenue: { amount: revenue, mom: mom(revenue, revenuePrev) },
          cogs:    { amount: cogs,    mom: mom(cogs, cogsPrev) },
          gross:   { amount: gross,   mom: mom(gross, grossPrev), pct: pct(gross, revenue) },
          opex:    { amount: opex,    mom: mom(opex, opexPrev) },
          net:     { amount: net,     mom: mom(net, netPrev),     pct: pct(net, revenue) },
          period:  { from: plThisStart, to: plToday }
        }
      }
    });
  } catch (err) {
    console.error('Aggregator stats error:', err);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

// GET /api/aggregators/:id/agent-ratings — ratings submitted by this aggregator's field agents
app.get('/api/aggregators/:id/agent-ratings', requireAuth, async (req, res) => {
  try {
    const { id } = req.params;
    const aggregatorId = Number(id);
    if (!Number.isInteger(aggregatorId) || aggregatorId <= 0) {
      return res.status(400).json({ success: false, message: 'Invalid aggregator id' });
    }
    if (!req.user.hasRole('aggregator') || Number(req.user.id) !== aggregatorId) {
      return res.status(403).json({ success: false, message: 'Not authorized' });
    }
    const limit = Math.min(parseInt(req.query.limit || '20', 10) || 20, 100);
    const result = await pool.query(
      `SELECT r.id, r.rating, r.tags, r.notes, r.rating_direction, r.created_at, r.transaction_id,
              ag.id AS agent_id,
              ag.first_name || ' ' || ag.last_name AS agent_name,
              c.id AS collector_id,
              c.first_name || ' ' || c.last_name AS collector_name,
              pt.material_type, pt.gross_weight_kg, pt.total_price
         FROM ratings r
         JOIN agents ag ON ag.id = r.rater_id AND r.rater_type = 'agent'
         JOIN collectors c ON c.id = r.rated_id AND r.rated_type = 'collector'
         LEFT JOIN pending_transactions pt ON pt.id = r.transaction_id
        WHERE ag.aggregator_id = $1
        ORDER BY r.created_at DESC
        LIMIT $2`,
      [aggregatorId, limit]
    );
    res.json({ success: true, ratings: result.rows });
  } catch (err) {
    console.error('GET /api/aggregators/:id/agent-ratings error:', err.message);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

// ============================================
// EXPENSE CATEGORIES
// ============================================

app.get('/api/expense-categories', async (req, res) => {
  try {
    const { rows } = await pool.query(
      `SELECT id, name, status FROM expense_categories WHERE status IN ('default','approved') ORDER BY name`
    );
    res.json(rows);
  } catch (err) {
    console.error('GET /api/expense-categories error:', err);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

app.post('/api/expense-categories/suggest', requireAuth, async (req, res) => {
  try {
    const { name, aggregator_id } = req.body;
    if (!name || !name.trim()) return res.status(400).json({ success: false, message: 'Category name is required' });
    if (!aggregator_id) return res.status(400).json({ success: false, message: 'aggregator_id is required' });
    const { rows } = await pool.query(
      `INSERT INTO expense_categories (name, status, suggested_by) VALUES ($1, 'pending', $2) RETURNING id, name, status`,
      [name.trim(), aggregator_id]
    );
    res.status(201).json(rows[0]);
  } catch (err) {
    console.error('POST /api/expense-categories/suggest error:', err);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

app.get('/api/expense-categories/pending', requireAdmin, async (req, res) => {
  try {
    const { rows } = await pool.query(
      `SELECT ec.id, ec.name, ec.status, ec.created_at, o.name AS suggested_by_name
       FROM expense_categories ec
       LEFT JOIN aggregators o ON o.id = ec.suggested_by
       WHERE ec.status = 'pending'
       ORDER BY ec.created_at DESC`
    );
    res.json(rows);
  } catch (err) {
    console.error('GET /api/expense-categories/pending error:', err);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

app.patch('/api/expense-categories/:id/approve', requireAdmin, async (req, res) => {
  try {
    const { id } = req.params;
    const newName = req.body.name;
    const { rows } = await pool.query(
      `UPDATE expense_categories SET status = 'approved', name = COALESCE($1, name), reviewed_at = NOW() WHERE id = $2 AND status = 'pending' RETURNING *`,
      [newName || null, id]
    );
    if (!rows.length) return res.status(404).json({ success: false, message: 'Category not found or already reviewed' });
    res.json(rows[0]);
  } catch (err) {
    console.error('PATCH /api/expense-categories/:id/approve error:', err);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

app.patch('/api/expense-categories/:id/reject', requireAdmin, async (req, res) => {
  try {
    const { id } = req.params;
    const { rejection_reason } = req.body;
    if (!rejection_reason || !rejection_reason.trim()) {
      return res.status(400).json({ success: false, message: 'Rejection reason is required' });
    }
    const { rows } = await pool.query(
      `UPDATE expense_categories SET status = 'rejected', rejection_reason = $1, reviewed_at = NOW() WHERE id = $2 AND status = 'pending' RETURNING *`,
      [rejection_reason.trim(), id]
    );
    if (!rows.length) return res.status(404).json({ success: false, message: 'Category not found or already reviewed' });
    res.json(rows[0]);
  } catch (err) {
    console.error('PATCH /api/expense-categories/:id/reject error:', err);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

// ============================================
// EXPENSE ENTRIES
// ============================================

app.post('/api/aggregators/:id/expenses', requireAuth, receiptUpload.single('receipt'), async (req, res) => {
  try {
    const aggregator_id = parseInt(req.params.id);
    if (req.user.id !== aggregator_id) return res.status(403).json({ success: false, message: 'Access denied' });
    const { category_id, amount, note, expense_date } = req.body;

    if (!category_id || !amount || isNaN(parseFloat(amount))) {
      return res.status(400).json({ success: false, message: 'category_id and amount are required' });
    }

    const receipt_url = req.file ? `/uploads/receipts/${req.file.filename}` : null;
    const date = expense_date || new Date().toISOString().slice(0, 10);

    let rows;
    try {
      const result = await pool.query(
        `INSERT INTO expense_entries (aggregator_id, category_id, amount, note, receipt_url, expense_date)
         VALUES ($1, $2, $3, $4, $5, $6)
         RETURNING *`,
        [aggregator_id, category_id, parseFloat(amount), note || null, receipt_url, date]
      );
      rows = result.rows;
    } catch (colErr) {
      if (colErr.message && colErr.message.includes('note')) {
        console.warn('expense_entries.note column missing on INSERT — retrying without it');
        const result = await pool.query(
          `INSERT INTO expense_entries (aggregator_id, category_id, amount, receipt_url, expense_date)
           VALUES ($1, $2, $3, $4, $5)
           RETURNING *`,
          [aggregator_id, category_id, parseFloat(amount), receipt_url, date]
        );
        rows = result.rows;
      } else {
        throw colErr;
      }
    }
    res.status(201).json(rows[0]);
  } catch (err) {
    console.error('POST /api/aggregators/:id/expenses error:', err);
    res.status(500).json({ success: false, message: 'Failed to log expense' });
  }
});

app.get('/api/aggregators/:id/expenses', requireAuth, async (req, res) => {
  try {
    const aggregator_id = parseInt(req.params.id);
    if (req.user.id !== aggregator_id) return res.status(403).json({ success: false, message: 'Access denied' });
    if (isNaN(aggregator_id)) return res.status(400).json({ success: false, message: 'Invalid aggregator ID' });
    const { from, to } = req.query;

    // Default: current month
    const startDate = from || new Date(new Date().getFullYear(), new Date().getMonth(), 1).toISOString().slice(0, 10);
    const endDate = to || new Date().toISOString().slice(0, 10);

    // Try with note column first; fall back without it if column missing on production
    let rows;
    try {
      const result = await pool.query(
        `SELECT ee.id, ee.amount, ee.note, ee.receipt_url, ee.expense_date, ee.created_at,
                ec.id AS category_id, ec.name AS category_name
         FROM expense_entries ee
         JOIN expense_categories ec ON ec.id = ee.category_id
         WHERE ee.aggregator_id = $1
           AND ee.expense_date BETWEEN $2 AND $3
         ORDER BY ec.name, ee.expense_date DESC`,
        [aggregator_id, startDate, endDate]
      );
      rows = result.rows;
    } catch (colErr) {
      if (colErr.message && colErr.message.includes('note')) {
        console.warn('expense_entries.note column missing — retrying without it');
        const result = await pool.query(
          `SELECT ee.id, ee.amount, ee.receipt_url, ee.expense_date, ee.created_at,
                  ec.id AS category_id, ec.name AS category_name
           FROM expense_entries ee
           JOIN expense_categories ec ON ec.id = ee.category_id
           WHERE ee.aggregator_id = $1
             AND ee.expense_date BETWEEN $2 AND $3
           ORDER BY ec.name, ee.expense_date DESC`,
          [aggregator_id, startDate, endDate]
        );
        rows = result.rows;
      } else {
        throw colErr;
      }
    }

    // Group by category
    const grouped = {};
    for (const row of rows) {
      if (!grouped[row.category_name]) {
        grouped[row.category_name] = { category_id: row.category_id, category_name: row.category_name, total: 0, entries: [] };
      }
      grouped[row.category_name].total += parseFloat(row.amount);
      grouped[row.category_name].entries.push({
        id: row.id, amount: parseFloat(row.amount), note: row.note || null,
        receipt_url: row.receipt_url, expense_date: row.expense_date, created_at: row.created_at
      });
    }

    res.json({
      from: startDate, to: endDate,
      total: rows.reduce((s, r) => s + parseFloat(r.amount), 0),
      entry_count: rows.length,
      categories: Object.values(grouped)
    });
  } catch (err) {
    console.error('GET /api/aggregators/:id/expenses error:', err.message);
    res.status(500).json({ success: false, message: 'Failed to fetch expenses' });
  }
});

app.delete('/api/aggregators/:id/expenses/:eid', requireAuth, async (req, res) => {
  try {
    const { id, eid } = req.params;
    if (req.user.id !== parseInt(id)) return res.status(403).json({ success: false, message: 'Access denied' });
    const { rows } = await pool.query(
      `DELETE FROM expense_entries WHERE id = $1 AND aggregator_id = $2 RETURNING *`,
      [eid, id]
    );
    if (!rows.length) return res.status(404).json({ success: false, message: 'Entry not found' });

    // Clean up receipt file if it exists
    if (rows[0].receipt_url) {
      const filePath = path.join(__dirname, 'public', rows[0].receipt_url);
      fs.unlink(filePath, () => {}); // best-effort delete
    }

    res.json({ deleted: rows[0] });
  } catch (err) {
    console.error('DELETE /api/aggregators/:id/expenses/:eid error:', err);
    res.status(500).json({ success: false, message: 'Failed to delete expense' });
  }
});

// ── Discovery: Listings ──────────────────────────────────────────────

const VALID_MATERIALS = ['PET', 'HDPE', 'LDPE', 'PP'];
const MIN_KG = { collector: 30, aggregator: 500 };

// Determine which seller_role a buyer sees based on tier
function sellerRoleForBuyer(buyerRole) {
  if (buyerRole === 'aggregator') return 'collector';
  if (['processor', 'recycler', 'converter'].includes(buyerRole)) return 'aggregator';
  return null;
}

// POST /api/listings — create a listing (collectors + aggregators only)
app.post('/api/listings', requireAuth, async (req, res) => {
  try {
    const role = req.user.role;
    if (role !== 'collector' && role !== 'aggregator') {
      if (!req.user.hasRole('collector') && !req.user.hasRole('aggregator'))
        return res.status(403).json({ success: false, message: 'Only collectors and aggregators can create listings' });
    }
    const sellerRole = req.user.hasRole('collector') ? 'collector' : 'aggregator';
    const { material_type, quantity_kg, price_per_kg, location, photo_url } = req.body;
    if (!material_type || !quantity_kg) return res.status(400).json({ success: false, message: 'material_type and quantity_kg required' });
    const mat = material_type.toUpperCase();
    if (!VALID_MATERIALS.includes(mat)) return res.status(400).json({ success: false, message: 'material_type must be one of: ' + VALID_MATERIALS.join(', ') });
    const kg = parseFloat(quantity_kg);
    if (isNaN(kg) || kg <= 0) return res.status(400).json({ success: false, message: 'quantity_kg must be a positive number' });
    const minKg = MIN_KG[sellerRole] || 30;
    if (kg < minKg) return res.status(400).json({ success: false, message: sellerRole + ' listings require at least ' + minKg + ' kg' });
    const price = price_per_kg != null && price_per_kg !== '' ? parseFloat(price_per_kg) : null;
    const result = await pool.query(
      `INSERT INTO listings (seller_id, seller_role, material_type, quantity_kg, original_qty_kg, price_per_kg, location, photo_url, expires_at)
       VALUES ($1, $2, $3, $4, $4, $5, $6, $7, NOW() + INTERVAL '7 days') RETURNING *`,
      [req.user.id, sellerRole, mat, kg, price, location || null, photo_url || null]
    );
    res.status(201).json({ success: true, listing: result.rows[0] });
  } catch (err) { console.error('POST /api/listings error:', err); res.status(500).json({ success: false, message: 'Server error' }); }
});

// GET /api/listings/locations — distinct active listing locations
app.get('/api/listings/locations', async (req, res) => {
  try {
    const result = await pool.query(`SELECT DISTINCT location FROM listings WHERE status = 'active' AND location IS NOT NULL AND location != '' ORDER BY location`);
    res.json({ locations: result.rows.map(r => r.location) });
  } catch (err) { console.error('GET /api/listings/locations error:', err); res.status(500).json({ locations: [] }); }
});

// GET /api/listings — browse active listings (buyer sees tier below)
app.get('/api/listings', requireAuth, async (req, res) => {
  try {
    const buyerRole = req.user.role || (Array.isArray(req.user.roles) ? req.user.roles[0] : null);
    const targetSellerRole = sellerRoleForBuyer(buyerRole);
    if (!targetSellerRole) return res.status(403).json({ success: false, message: 'Your role cannot browse listings' });
    const sellerTable = CirculRoles.TABLE_MAP[targetSellerRole];
    const nameCol = targetSellerRole === 'collector' ? "s.first_name || ' ' || s.last_name" : 's.name';
    const ratingCol = targetSellerRole === 'collector' ? 's.average_rating' : 'NULL';

    let where = `l.status = 'active' AND l.expires_at > NOW() AND l.seller_role = $1`;
    const params = [targetSellerRole];
    const { material, min_kg, max_kg, location } = req.query;
    if (material) { params.push(material.toUpperCase()); where += ` AND l.material_type = $${params.length}`; }
    if (min_kg) { params.push(parseFloat(min_kg)); where += ` AND l.quantity_kg >= $${params.length}`; }
    if (max_kg) { params.push(parseFloat(max_kg)); where += ` AND l.quantity_kg <= $${params.length}`; }
    if (location) { params.push('%' + location + '%'); where += ` AND l.location ILIKE $${params.length}`; }

    const result = await pool.query(
      `SELECT l.*, ${nameCol} AS seller_name, ${ratingCol} AS seller_rating
       FROM listings l
       LEFT JOIN ${sellerTable} s ON s.id = l.seller_id
       WHERE ${where}
       ORDER BY l.created_at DESC`,
      params
    );
    // Add seller codes and visibility flags
    const buyerId = req.user.id;
    for (const row of result.rows) {
      row.seller_code = CirculRoles.circulCode(row.seller_role, row.seller_id);
      row.seller_name_visible = await canSeeName(buyerRole, buyerId, row.seller_role, row.seller_id);
    }
    res.json({ success: true, listings: result.rows });
  } catch (err) { console.error('GET /api/listings error:', err); res.status(500).json({ success: false, message: 'Server error' }); }
});

// GET /api/listings/mine — seller's own listings with pending offer counts
app.get('/api/listings/mine', requireAuth, async (req, res) => {
  try {
    const result = await pool.query(
      `SELECT l.*, COALESCE(oc.pending_count, 0)::int AS pending_offers
       FROM listings l
       LEFT JOIN (SELECT listing_id, COUNT(*) AS pending_count FROM offers WHERE status = 'pending' GROUP BY listing_id) oc ON oc.listing_id = l.id
       WHERE l.seller_id = $1 AND l.seller_role = $2
       ORDER BY l.created_at DESC`,
      [req.user.id, req.user.role || (Array.isArray(req.user.roles) ? req.user.roles[0] : null)]
    );
    res.json({ success: true, listings: result.rows });
  } catch (err) { console.error('GET /api/listings/mine error:', err); res.status(500).json({ success: false, message: 'Server error' }); }
});

// GET /api/listings/:id — single listing with seller info
app.get('/api/listings/:id', requireAuth, async (req, res) => {
  try {
    const row = await pool.query(`SELECT * FROM listings WHERE id = $1`, [req.params.id]);
    if (!row.rows.length) return res.status(404).json({ success: false, message: 'Listing not found' });
    const listing = row.rows[0];
    const sellerTable = CirculRoles.TABLE_MAP[listing.seller_role];
    const nameCol = listing.seller_role === 'collector' ? "first_name || ' ' || last_name" : 'name';
    const ratingCol = listing.seller_role === 'collector' ? 'average_rating' : 'NULL';
    const seller = await pool.query(
      `SELECT id, ${nameCol} AS name, ${ratingCol} AS rating FROM ${sellerTable} WHERE id = $1`,
      [listing.seller_id]
    );
    listing.seller_name = seller.rows.length ? seller.rows[0].name : null;
    listing.seller_rating = seller.rows.length ? seller.rows[0].rating : null;
    res.json({ success: true, listing });
  } catch (err) { console.error('GET /api/listings/:id error:', err); res.status(500).json({ success: false, message: 'Server error' }); }
});

// PATCH /api/listings/:id/renew — extend expiry by 7 days
app.patch('/api/listings/:id/renew', requireAuth, async (req, res) => {
  try {
    const row = await pool.query(`SELECT * FROM listings WHERE id = $1`, [req.params.id]);
    if (!row.rows.length) return res.status(404).json({ success: false, message: 'Listing not found' });
    const listing = row.rows[0];
    const userRole = req.user.role || (Array.isArray(req.user.roles) ? req.user.roles[0] : null);
    if (listing.seller_id !== req.user.id || listing.seller_role !== userRole)
      return res.status(403).json({ success: false, message: 'Not your listing' });
    const result = await pool.query(
      `UPDATE listings SET expires_at = NOW() + INTERVAL '7 days', renewal_count = renewal_count + 1, status = 'active', updated_at = NOW() WHERE id = $1 RETURNING *`,
      [req.params.id]
    );
    res.json({ success: true, listing: result.rows[0] });
  } catch (err) { console.error('PATCH /api/listings/:id/renew error:', err); res.status(500).json({ success: false, message: 'Server error' }); }
});

// PATCH /api/listings/:id/close — close listing + reject pending offers
app.patch('/api/listings/:id/close', requireAuth, async (req, res) => {
  try {
    const row = await pool.query(`SELECT * FROM listings WHERE id = $1`, [req.params.id]);
    if (!row.rows.length) return res.status(404).json({ success: false, message: 'Listing not found' });
    const listing = row.rows[0];
    const userRole = req.user.role || (Array.isArray(req.user.roles) ? req.user.roles[0] : null);
    if (listing.seller_id !== req.user.id || listing.seller_role !== userRole)
      return res.status(403).json({ success: false, message: 'Not your listing' });
    await pool.query(`UPDATE offers SET status = 'rejected', responded_at = NOW() WHERE listing_id = $1 AND status = 'pending'`, [req.params.id]);
    const result = await pool.query(
      `UPDATE listings SET status = 'closed', updated_at = NOW() WHERE id = $1 RETURNING *`,
      [req.params.id]
    );
    res.json({ success: true, listing: result.rows[0] });
  } catch (err) { console.error('PATCH /api/listings/:id/close error:', err); res.status(500).json({ success: false, message: 'Server error' }); }
});

// DELETE /api/listings/:id — hard delete (only if no accepted offers)
app.delete('/api/listings/:id', requireAuth, async (req, res) => {
  try {
    const row = await pool.query(`SELECT * FROM listings WHERE id = $1`, [req.params.id]);
    if (!row.rows.length) return res.status(404).json({ success: false, message: 'Listing not found' });
    const listing = row.rows[0];
    const userRole = req.user.role || (Array.isArray(req.user.roles) ? req.user.roles[0] : null);
    if (listing.seller_id !== req.user.id || listing.seller_role !== userRole)
      return res.status(403).json({ success: false, message: 'Not your listing' });
    const accepted = await pool.query(`SELECT COUNT(*) FROM offers WHERE listing_id = $1 AND status = 'accepted'`, [req.params.id]);
    if (parseInt(accepted.rows[0].count) > 0)
      return res.status(400).json({ success: false, message: 'Cannot delete listing with accepted offers' });
    await pool.query(`DELETE FROM listings WHERE id = $1`, [req.params.id]);
    res.status(204).end();
  } catch (err) { console.error('DELETE /api/listings/:id error:', err); res.status(500).json({ success: false, message: 'Server error' }); }
});

// ── Discovery: Offers ────────────────────────────────────────────────

// Map role to the correct pending_transactions column for that role
function ptColForRole(role) {
  switch (role) {
    case 'collector':  return 'collector_id';
    case 'aggregator': return 'aggregator_id';
    case 'processor':  return 'processor_id';
    case 'recycler':   return 'recycler_id';
    case 'converter':  return 'converter_id';
    default: return null;
  }
}

// txnTypeForRoles is now imported from shared/transaction-parties.js. The
// pre-PR6 in-file definition silently fell back to 'aggregator_sale' for any
// non-collector/non-aggregator seller, mis-labeling processor and recycler
// discovery offer-accepts. PR6 hoists it to the shared module so callers from
// server.js + tests get one source of truth.

// Check if user is the receiver (not the sender) of an offer
function isReceiver(offer, listing, userId, userRole) {
  if (offer.offered_by === 'buyer') {
    return listing.seller_id === userId && listing.seller_role === userRole;
  }
  return offer.buyer_id === userId && offer.buyer_role === userRole;
}

// GET /api/listings/:id/offers — get all offers on a listing (seller only)
app.get('/api/listings/:id/offers', requireAuth, async (req, res) => {
  try {
    const listing = (await pool.query(`SELECT * FROM listings WHERE id = $1`, [req.params.id])).rows[0];
    if (!listing) return res.status(404).json({ success: false, message: 'Listing not found' });
    const userRole = req.user.role || (Array.isArray(req.user.roles) ? req.user.roles[0] : null);
    if (listing.seller_id !== req.user.id || listing.seller_role !== userRole) {
      return res.status(403).json({ success: false, message: 'Not your listing' });
    }
    const buyerRole = listing.seller_role === 'collector' ? 'aggregator' : 'processor';
    const buyerTable = CirculRoles.TABLE_MAP[buyerRole];
    if (!buyerTable) return res.status(400).json({ success: false, message: 'Invalid role: ' + buyerRole });
    const offers = (await pool.query(
      `SELECT o.*, b.name AS buyer_name FROM offers o
       LEFT JOIN ${buyerTable} b ON b.id = o.buyer_id
       WHERE o.listing_id = $1 ORDER BY o.created_at DESC`, [req.params.id]
    )).rows;
    for (const o of offers) {
      o.buyer_code = CirculRoles.circulCode(o.buyer_role || buyerRole, o.buyer_id);
      o.buyer_name_visible = await canSeeName(userRole, req.user.id, o.buyer_role || buyerRole, o.buyer_id);
    }
    res.json({ success: true, offers: offers });
  } catch (err) { console.error('GET /api/listings/:id/offers error:', err); res.status(500).json({ success: false, message: 'Server error' }); }
});

// POST /api/listings/:id/offers — buyer places an offer
app.post('/api/listings/:id/offers', requireAuth, async (req, res) => {
  try {
    const listing = (await pool.query(`SELECT * FROM listings WHERE id = $1`, [req.params.id])).rows[0];
    if (!listing) return res.status(404).json({ success: false, message: 'Listing not found' });
    if (listing.status !== 'active') return res.status(400).json({ success: false, message: 'Listing is not active' });
    const buyerRole = req.user.role || (Array.isArray(req.user.roles) ? req.user.roles[0] : null);
    const expectedSeller = sellerRoleForBuyer(buyerRole);
    if (listing.seller_role !== expectedSeller)
      return res.status(403).json({ success: false, message: 'You can only make offers on listings from the tier below you' });
    const { price_per_kg, quantity_kg } = req.body;
    if (!price_per_kg || !quantity_kg) return res.status(400).json({ success: false, message: 'price_per_kg and quantity_kg required' });
    const price = parseFloat(price_per_kg);
    const qty = parseFloat(quantity_kg);
    if (isNaN(price) || price <= 0) return res.status(400).json({ success: false, message: 'price_per_kg must be a positive number' });
    if (isNaN(qty) || qty <= 0) return res.status(400).json({ success: false, message: 'quantity_kg must be a positive number' });
    if (qty > parseFloat(listing.quantity_kg))
      return res.status(400).json({ success: false, message: 'quantity_kg exceeds available quantity (' + listing.quantity_kg + ' kg)' });
    const existing = await pool.query(
      `SELECT id FROM offers WHERE listing_id = $1 AND buyer_id = $2 AND buyer_role = $3 AND status = 'pending'`,
      [req.params.id, req.user.id, buyerRole]
    );
    if (existing.rows.length) return res.status(400).json({ success: false, message: 'You already have a pending offer on this listing' });
    const result = await pool.query(
      `INSERT INTO offers (listing_id, buyer_id, buyer_role, price_per_kg, quantity_kg, round, is_final, offered_by, status)
       VALUES ($1, $2, $3, $4, $5, 1, FALSE, 'buyer', 'pending') RETURNING *`,
      [req.params.id, req.user.id, buyerRole, price, qty]
    );
    // Notify listing seller about the new offer
    try {
      const sellerTable = CirculRoles.TABLE_MAP[listing.seller_role];
      if (!sellerTable) throw new Error('Unknown seller role: ' + listing.seller_role);
      const nameCol = listing.seller_role === 'collector' ? "first_name || ' ' || last_name" : 'name';
      const seller = (await pool.query(`SELECT phone, ${nameCol} AS name FROM ${sellerTable} WHERE id = $1`, [listing.seller_id])).rows[0];
      const buyerTable = CirculRoles.TABLE_MAP[buyerRole];
      if (!buyerTable) throw new Error('Unknown buyer role: ' + buyerRole);
      const buyerNameCol = buyerRole === 'collector' ? "first_name || ' ' || last_name" : 'name';
      const buyer = (await pool.query(`SELECT ${buyerNameCol} AS name FROM ${buyerTable} WHERE id = $1`, [req.user.id])).rows[0];
      if (seller && seller.phone) {
        notify(EVENTS.NEW_OFFER, seller.phone, { buyer_name: buyer ? buyer.name : 'A buyer', price: price, material: listing.material_type, qty: qty });
      }
    } catch (notifyErr) { console.warn('Notification error (new_offer):', notifyErr.message); }
    res.status(201).json({ success: true, offer: result.rows[0] });
  } catch (err) { console.error('POST /api/listings/:id/offers error:', err); res.status(500).json({ success: false, message: 'Server error' }); }
});

// GET /api/offers/mine — all offers involving the authenticated user
app.get('/api/offers/mine', requireAuth, async (req, res) => {
  try {
    const userRole = req.user.role || (Array.isArray(req.user.roles) ? req.user.roles[0] : null);
    const result = await pool.query(
      `SELECT o.*, l.material_type AS listing_material, l.quantity_kg AS listing_qty, l.seller_id, l.seller_role
       FROM offers o
       JOIN listings l ON l.id = o.listing_id
       WHERE (o.buyer_id = $1 AND o.buyer_role = $2)
          OR (l.seller_id = $1 AND l.seller_role = $2)
       ORDER BY o.created_at DESC`,
      [req.user.id, userRole]
    );
    res.json({ success: true, offers: result.rows });
  } catch (err) { console.error('GET /api/offers/mine error:', err); res.status(500).json({ success: false, message: 'Server error' }); }
});

// GET /api/offers/:id/thread — full negotiation thread
app.get('/api/offers/:id/thread', requireAuth, async (req, res) => {
  try {
    const offer = (await pool.query(`SELECT * FROM offers WHERE id = $1`, [req.params.id])).rows[0];
    if (!offer) return res.status(404).json({ success: false, message: 'Offer not found' });
    const listing = (await pool.query(`SELECT * FROM listings WHERE id = $1`, [offer.listing_id])).rows[0];
    if (!listing) return res.status(404).json({ success: false, message: 'Listing not found' });
    const userRole = req.user.role || (Array.isArray(req.user.roles) ? req.user.roles[0] : null);
    const isBuyer = offer.buyer_id === req.user.id && offer.buyer_role === userRole;
    const isSeller = listing.seller_id === req.user.id && listing.seller_role === userRole;
    if (!isBuyer && !isSeller) return res.status(403).json({ success: false, message: 'Not your negotiation' });
    const thread = await pool.query(
      `SELECT * FROM offers WHERE thread_id = $1 ORDER BY round ASC, created_at ASC`,
      [offer.thread_id]
    );
    res.json({ success: true, listing, offers: thread.rows });
  } catch (err) { console.error('GET /api/offers/:id/thread error:', err); res.status(500).json({ success: false, message: 'Server error' }); }
});

// POST /api/offers/:id/accept — accept an offer → create pending_transaction
app.post('/api/offers/:id/accept', requireAuth, async (req, res) => {
  try {
    const offer = (await pool.query(`SELECT * FROM offers WHERE id = $1`, [req.params.id])).rows[0];
    if (!offer) return res.status(404).json({ success: false, message: 'Offer not found' });
    if (offer.status !== 'pending') return res.status(400).json({ success: false, message: 'Offer is not pending' });
    const listing = (await pool.query(`SELECT * FROM listings WHERE id = $1`, [offer.listing_id])).rows[0];
    if (!listing) return res.status(404).json({ success: false, message: 'Listing not found' });
    const userRole = req.user.role || (Array.isArray(req.user.roles) ? req.user.roles[0] : null);
    if (!isReceiver(offer, listing, req.user.id, userRole))
      return res.status(403).json({ success: false, message: 'Only the receiving party can accept' });
    const offerQty = parseFloat(offer.quantity_kg);
    if (offerQty > parseFloat(listing.quantity_kg))
      return res.status(400).json({ success: false, message: 'Quantity no longer available — listing has been partially filled' });

    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      await client.query(`UPDATE offers SET status = 'accepted', responded_at = NOW() WHERE id = $1`, [offer.id]);
      const remainingQty = parseFloat(listing.quantity_kg) - offerQty;
      if (remainingQty <= 0) {
        await client.query(`UPDATE listings SET quantity_kg = 0, status = 'closed', updated_at = NOW() WHERE id = $1`, [listing.id]);
      } else {
        await client.query(`UPDATE listings SET quantity_kg = $1, updated_at = NOW() WHERE id = $2`, [remainingQty, listing.id]);
      }

      // PR6-c: route through chain-of-custody helpers so discovery accepts get
      // a fresh batch_id (root) or FIFO-attributed sources + junction edges
      // (downstream). Pre-PR6 this was a raw INSERT with no batch_id and no
      // mass-balance enforcement — discovery rows were invisible to the
      // chain-of-custody graph.
      const txnType = txnTypeForRoles(listing.seller_role, offer.buyer_role);
      const totalPrice = parseFloat((offerQty * parseFloat(offer.price_per_kg)).toFixed(2));
      const sellerCol = ptColForRole(listing.seller_role);
      const buyerCol  = ptColForRole(offer.buyer_role);
      const target = {
        transaction_type: txnType,
        status: 'pending',
        material_type: listing.material_type,
        gross_weight_kg: offerQty,
        price_per_kg: parseFloat(offer.price_per_kg),
        total_price: totalPrice,
        source: 'discovery'
      };
      // Map seller + buyer roles to their FK columns. For collector→aggregator
      // root flows the buyer FK and seller FK are different columns; for
      // downstream flows seller and buyer are always different columns. The
      // legacy code's same-column branch was a no-op (the only same-role pair
      // would be aggregator→aggregator which doesn't exist).
      if (sellerCol) target[sellerCol] = listing.seller_id;
      if (buyerCol && buyerCol !== sellerCol) target[buyerCol] = offer.buyer_id;

      let ptRow;
      if (COC_ROOT_TYPES[txnType]) {
        // Root: collector_sale (or theoretical aggregator_purchase via discovery).
        // insertRootTransaction generates a fresh batch_id, sets
        // remaining_kg = gross_weight_kg, no junction writes.
        const { row } = await insertRootTransaction(client, target);
        ptRow = row;
      } else {
        // Downstream: aggregator_sale / processor_sale / recycler_sale.
        // FIFO attribute against the seller's available source rows + write
        // junction edges. Throws InsufficientSourceError on shortfall.
        const { row } = await attributeAndInsert(client, target);
        ptRow = row;
      }
      await client.query('COMMIT');
      // Notify the buyer that their offer was accepted
      try {
        const buyerTable = CirculRoles.TABLE_MAP[offer.buyer_role];
        if (!buyerTable) throw new Error('Unknown buyer role: ' + offer.buyer_role);
        const buyerNameCol = offer.buyer_role === 'collector' ? "first_name || ' ' || last_name" : 'name';
        const buyerRow = (await pool.query(`SELECT phone, ${buyerNameCol} AS name FROM ${buyerTable} WHERE id = $1`, [offer.buyer_id])).rows[0];
        const sellerTable = CirculRoles.TABLE_MAP[listing.seller_role];
        if (!sellerTable) throw new Error('Unknown seller role: ' + listing.seller_role);
        const sellerNameCol = listing.seller_role === 'collector' ? "first_name || ' ' || last_name" : 'name';
        const sellerRow = (await pool.query(`SELECT ${sellerNameCol} AS name FROM ${sellerTable} WHERE id = $1`, [listing.seller_id])).rows[0];
        if (buyerRow && buyerRow.phone) {
          notify(EVENTS.OFFER_ACCEPTED, buyerRow.phone, { material: listing.material_type, qty: offerQty, seller_name: sellerRow ? sellerRow.name : 'the seller' });
        }
      } catch (notifyErr) { console.warn('Notification error (offer_accepted):', notifyErr.message); }
      res.json({ success: true, pending_transaction: ptRow, offer: { id: offer.id, status: 'accepted' } });
    } catch (txErr) {
      await client.query('ROLLBACK').catch(() => {});
      // PR6-c: surface InsufficientSourceError as a 400 (same shape as the
      // PR3 wire-ins at /api/pending-transactions/processor-sale etc.).
      if (handleInsufficientSource(res, txErr)) return;
      throw txErr;
    } finally {
      client.release();
    }
  } catch (err) { console.error('POST /api/offers/:id/accept error:', err); res.status(500).json({ success: false, message: 'Server error' }); }
});

// POST /api/offers/:id/reject — reject a pending offer
app.post('/api/offers/:id/reject', requireAuth, async (req, res) => {
  try {
    const offer = (await pool.query(`SELECT * FROM offers WHERE id = $1`, [req.params.id])).rows[0];
    if (!offer) return res.status(404).json({ success: false, message: 'Offer not found' });
    if (offer.status !== 'pending') return res.status(400).json({ success: false, message: 'Offer is not pending' });
    const listing = (await pool.query(`SELECT * FROM listings WHERE id = $1`, [offer.listing_id])).rows[0];
    if (!listing) return res.status(404).json({ success: false, message: 'Listing not found' });
    const userRole = req.user.role || (Array.isArray(req.user.roles) ? req.user.roles[0] : null);
    if (!isReceiver(offer, listing, req.user.id, userRole))
      return res.status(403).json({ success: false, message: 'Only the receiving party can reject' });
    await pool.query(`UPDATE offers SET status = 'rejected', responded_at = NOW() WHERE id = $1`, [offer.id]);
    // Notify the buyer that their offer was rejected
    try {
      const buyerTable = CirculRoles.TABLE_MAP[offer.buyer_role];
      if (!buyerTable) throw new Error('Unknown buyer role: ' + offer.buyer_role);
      const buyerNameCol = offer.buyer_role === 'collector' ? "first_name || ' ' || last_name" : 'name';
      const buyerRow = (await pool.query(`SELECT phone, ${buyerNameCol} AS name FROM ${buyerTable} WHERE id = $1`, [offer.buyer_id])).rows[0];
      const sellerTable = CirculRoles.TABLE_MAP[listing.seller_role];
      if (!sellerTable) throw new Error('Unknown seller role: ' + listing.seller_role);
      const sellerNameCol = listing.seller_role === 'collector' ? "first_name || ' ' || last_name" : 'name';
      const sellerRow = (await pool.query(`SELECT ${sellerNameCol} AS name FROM ${sellerTable} WHERE id = $1`, [listing.seller_id])).rows[0];
      if (buyerRow && buyerRow.phone) {
        notify(EVENTS.OFFER_REJECTED, buyerRow.phone, { material: listing.material_type, seller_name: sellerRow ? sellerRow.name : 'the seller' });
      }
    } catch (notifyErr) { console.warn('Notification error (offer_rejected):', notifyErr.message); }
    res.json({ success: true, offer: { id: offer.id, status: 'rejected' } });
  } catch (err) { console.error('POST /api/offers/:id/reject error:', err); res.status(500).json({ success: false, message: 'Server error' }); }
});

// POST /api/offers/:id/counter — counter an offer (max 2 rounds)
app.post('/api/offers/:id/counter', requireAuth, async (req, res) => {
  try {
    const offer = (await pool.query(`SELECT * FROM offers WHERE id = $1`, [req.params.id])).rows[0];
    if (!offer) return res.status(404).json({ success: false, message: 'Offer not found' });
    if (offer.status !== 'pending') return res.status(400).json({ success: false, message: 'Offer is not pending' });
    if (offer.round >= 2) return res.status(400).json({ success: false, message: 'Cannot counter a final offer' });
    const listing = (await pool.query(`SELECT * FROM listings WHERE id = $1`, [offer.listing_id])).rows[0];
    if (!listing) return res.status(404).json({ success: false, message: 'Listing not found' });
    const userRole = req.user.role || (Array.isArray(req.user.roles) ? req.user.roles[0] : null);
    if (!isReceiver(offer, listing, req.user.id, userRole))
      return res.status(403).json({ success: false, message: 'Only the receiving party can counter' });
    const { price_per_kg, quantity_kg } = req.body;
    if (!price_per_kg || !quantity_kg) return res.status(400).json({ success: false, message: 'price_per_kg and quantity_kg required' });
    const price = parseFloat(price_per_kg);
    const qty = parseFloat(quantity_kg);
    if (isNaN(price) || price <= 0) return res.status(400).json({ success: false, message: 'price_per_kg must be a positive number' });
    if (isNaN(qty) || qty <= 0) return res.status(400).json({ success: false, message: 'quantity_kg must be a positive number' });
    if (qty > parseFloat(listing.quantity_kg))
      return res.status(400).json({ success: false, message: 'quantity_kg exceeds available quantity' });
    // Determine who is countering
    const isSeller = listing.seller_id === req.user.id && listing.seller_role === userRole;
    const offeredBy = isSeller ? 'seller' : 'buyer';
    const newRound = offer.round + 1;

    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      await client.query(`UPDATE offers SET status = 'countered', responded_at = NOW() WHERE id = $1`, [offer.id]);
      const result = await client.query(
        `INSERT INTO offers (listing_id, thread_id, buyer_id, buyer_role, price_per_kg, quantity_kg, round, is_final, offered_by, status, parent_offer_id)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'pending', $10) RETURNING *`,
        [offer.listing_id, offer.thread_id, offer.buyer_id, offer.buyer_role, price, qty, newRound, newRound >= 2, offeredBy, offer.id]
      );
      await client.query('COMMIT');
      // Notify the other party about the counter-offer
      try {
        const recipientId = isSeller ? offer.buyer_id : listing.seller_id;
        const recipientRole = isSeller ? offer.buyer_role : listing.seller_role;
        const recipientTable = CirculRoles.TABLE_MAP[recipientRole];
        if (!recipientTable) throw new Error('Unknown recipient role: ' + recipientRole);
        const recipientNameCol = recipientRole === 'collector' ? "first_name || ' ' || last_name" : 'name';
        const recipientRow = (await pool.query(`SELECT phone, ${recipientNameCol} AS name FROM ${recipientTable} WHERE id = $1`, [recipientId])).rows[0];
        const counterpartyId = isSeller ? listing.seller_id : offer.buyer_id;
        const counterpartyRole = isSeller ? listing.seller_role : offer.buyer_role;
        const counterpartyTable = CirculRoles.TABLE_MAP[counterpartyRole];
        if (!counterpartyTable) throw new Error('Unknown counterparty role: ' + counterpartyRole);
        const counterpartyNameCol = counterpartyRole === 'collector' ? "first_name || ' ' || last_name" : 'name';
        const counterpartyRow = (await pool.query(`SELECT ${counterpartyNameCol} AS name FROM ${counterpartyTable} WHERE id = $1`, [counterpartyId])).rows[0];
        if (recipientRow && recipientRow.phone) {
          notify(EVENTS.COUNTER_OFFER, recipientRow.phone, { counterparty: counterpartyRow ? counterpartyRow.name : 'Your counterparty', price: price, qty: qty, material: listing.material_type });
        }
      } catch (notifyErr) { console.warn('Notification error (counter_offer):', notifyErr.message); }
      res.status(201).json({ success: true, offer: result.rows[0] });
    } catch (txErr) {
      await client.query('ROLLBACK');
      throw txErr;
    } finally {
      client.release();
    }
  } catch (err) { console.error('POST /api/offers/:id/counter error:', err); res.status(500).json({ success: false, message: 'Server error' }); }
});

// ── End Discovery: Offers ────────────────────────────────────────────

app.get('/api/aggregator/top-suppliers', requireAuth, async (req, res) => {
  try {
    if (!req.user.hasRole('aggregator')) return res.status(403).json({ success: false, message: 'Aggregator access only' });
    const aggId = req.user.id;
    const { period } = req.query;
    const since = period === 'month'
      ? (() => { const d = new Date(); d.setDate(1); d.setHours(0,0,0,0); return d.toISOString(); })()
      : new Date(new Date().getFullYear(), 0, 1).toISOString();
    const result = await pool.query(
      `SELECT c.id AS collector_id,
              COALESCE(c.first_name || ' ' || c.last_name, 'Unknown') AS collector_name,
              COALESCE(c.first_name || ' ' || c.last_name, 'Unknown') AS name,
              SUM(t.net_weight_kg) AS ytd_kg,
              SUM(CASE WHEN t.transaction_date >= $2 THEN t.net_weight_kg ELSE 0 END) AS month_kg,
              AVG(t.price_per_kg) AS avg_price,
              COUNT(*) AS transaction_count
       FROM transactions t
       LEFT JOIN collectors c ON c.id = t.collector_id
       WHERE t.aggregator_id = $1 AND t.transaction_date >= $3
       GROUP BY c.id ORDER BY ytd_kg DESC LIMIT 5`,
      [aggId, since, new Date(new Date().getFullYear(), 0, 1).toISOString()]
    );
    result.rows.forEach(r => {
      r.collector_code = CirculRoles.circulCode('collector', r.collector_id);
      r.name_visible = true; // aggregator↔collector adjacent
    });
    res.json(result.rows);
  } catch (err) {
    console.error('Aggregator top-suppliers error:', err);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

app.get('/api/aggregator/top-buyers', requireAuth, async (req, res) => {
  try {
    if (!req.user.hasRole('aggregator')) return res.status(403).json({ success: false, message: 'Aggregator access only' });
    const aggId = req.user.id;
    const { period } = req.query;
    const since = period === 'month'
      ? (() => { const d = new Date(); d.setDate(1); d.setHours(0,0,0,0); return d.toISOString(); })()
      : new Date(new Date().getFullYear(), 0, 1).toISOString();
    const result = await pool.query(
      `SELECT p.id AS processor_id,
              COALESCE(p.company, p.name, 'Unknown') AS processor_name,
              COALESCE(p.company, p.name, 'Unknown') AS name,
              SUM(pt.gross_weight_kg) AS ytd_kg,
              SUM(CASE WHEN pt.created_at >= $2 THEN pt.gross_weight_kg ELSE 0 END) AS month_kg,
              AVG(pt.price_per_kg) AS avg_price,
              COUNT(*) AS transaction_count
       FROM pending_transactions pt
       LEFT JOIN processors p ON p.id = pt.processor_id
       WHERE pt.aggregator_id = $1 AND pt.transaction_type = 'aggregator_sale' AND pt.created_at >= $3
       GROUP BY p.id ORDER BY ytd_kg DESC LIMIT 5`,
      [aggId, since, new Date(new Date().getFullYear(), 0, 1).toISOString()]
    );
    result.rows.forEach(r => {
      r.processor_code = CirculRoles.circulCode('processor', r.processor_id);
      r.name_visible = true; // aggregator↔processor adjacent
    });
    res.json(result.rows);
  } catch (err) {
    console.error('Aggregator top-buyers error:', err);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

// ============================================
// AGGREGATOR REPORTS — Sourcing + Sales
// ============================================
//
// Privacy contract:
//   Sourcing endpoint MUST NOT join the collectors table. Names cannot enter
//   the response. Sourcing rows are filtered to collector_id IS NOT NULL at
//   the SQL layer — declared / walk-in stock is excluded by construction.
//
//   The collectors-list lookup endpoint (UI-input only) DOES join collectors
//   to render `Ama Mensah (COL-0026)` in the dropdown. That data must not
//   round-trip into any export.

// Shared filter validators
function _reportsValidateCommon(req, res) {
  const { from, to, materials, collector_id, buyer_kind, buyer_id } = req.query;
  if (!from || !to) {
    res.status(400).json({ success: false, message: 'from and to (YYYY-MM-DD) required' });
    return null;
  }
  if (!/^\d{4}-\d{2}-\d{2}$/.test(from) || !/^\d{4}-\d{2}-\d{2}$/.test(to)) {
    res.status(400).json({ success: false, message: 'from and to must be YYYY-MM-DD' });
    return null;
  }
  const fromDate = new Date(from + 'T00:00:00Z');
  const toDate = new Date(to + 'T00:00:00Z');
  if (isNaN(fromDate.getTime()) || isNaN(toDate.getTime())) {
    res.status(400).json({ success: false, message: 'from or to is not a valid date' });
    return null;
  }
  if (fromDate > toDate) {
    res.status(400).json({ success: false, message: 'from must be <= to' });
    return null;
  }
  // SQL injection defence on materials — only A-Z and commas allowed.
  if (materials && !/^[A-Z]+(,[A-Z]+)*$/.test(materials)) {
    res.status(400).json({ success: false, message: 'materials must be CSV of uppercase letters' });
    return null;
  }
  if (collector_id !== undefined && collector_id !== '' && !/^\d+$/.test(collector_id)) {
    res.status(400).json({ success: false, message: 'collector_id must be a positive integer' });
    return null;
  }
  if (buyer_kind !== undefined && buyer_kind !== '' && !['processor','recycler','converter'].includes(buyer_kind)) {
    res.status(400).json({ success: false, message: 'buyer_kind must be processor, recycler, or converter' });
    return null;
  }
  if (buyer_id !== undefined && buyer_id !== '' && !/^\d+$/.test(buyer_id)) {
    res.status(400).json({ success: false, message: 'buyer_id must be a positive integer' });
    return null;
  }
  if ((buyer_id !== undefined && buyer_id !== '') && (!buyer_kind || buyer_kind === '')) {
    res.status(400).json({ success: false, message: 'buyer_id requires buyer_kind' });
    return null;
  }
  // Inclusive `to`: shift to the next day's 00:00 so created_at < that captures the whole day.
  const toDatePlusOne = new Date(toDate.getTime() + 24 * 60 * 60 * 1000);
  return {
    from: fromDate.toISOString(),
    toExclusive: toDatePlusOne.toISOString(),
    materials: materials || null,
    collector_id: collector_id ? parseInt(collector_id, 10) : null,
    buyer_kind: buyer_kind || null,
    buyer_id: buyer_id ? parseInt(buyer_id, 10) : null
  };
}

// Sourcing report — collectors → this aggregator. Registered-only by SQL filter.
// Privacy: NO JOIN to collectors. Only collector_code (COL-XXXX) returned.
app.get('/api/aggregator/reports/sourcing', requireAuth, async (req, res) => {
  try {
    if (!req.user.hasRole('aggregator')) return res.status(403).json({ success: false, message: 'Aggregator access only' });
    const filters = _reportsValidateCommon(req, res);
    if (!filters) return;
    const aggId = req.user.id;
    const result = await pool.query(
      `SELECT
         pt.id,
         pt.created_at AS date,
         'TXN-' || TO_CHAR(pt.created_at, 'YYYYMMDD') || '-' || LPAD(pt.id::text, 4, '0') AS ref,
         pt.collector_id,
         'COL-' || LPAD(pt.collector_id::text, 4, '0') AS collector_code,
         pt.material_type,
         pt.form,
         pt.gross_weight_kg,
         COALESCE(pt.net_weight_kg, pt.gross_weight_kg) AS net_weight_kg,
         pt.price_per_kg,
         pt.total_price,
         pt.payment_status,
         pt.payment_completed_at AS paid_at
       FROM pending_transactions pt
       WHERE pt.transaction_type IN ('collector_sale', 'aggregator_purchase')
         AND pt.aggregator_id = $1
         AND pt.collector_id IS NOT NULL
         AND pt.created_at >= $2 AND pt.created_at < $3
         AND ($4::text IS NULL OR pt.material_type = ANY(string_to_array($4, ',')))
         AND ($5::int IS NULL OR pt.collector_id = $5)
       ORDER BY pt.created_at DESC`,
      [aggId, filters.from, filters.toExclusive, filters.materials, filters.collector_id]
    );
    const rows = result.rows;
    // Coerce numerics to JS numbers for client side ergonomics
    rows.forEach(function (r) {
      r.gross_weight_kg = Number(r.gross_weight_kg || 0);
      r.net_weight_kg = Number(r.net_weight_kg || 0);
      r.price_per_kg = Number(r.price_per_kg || 0);
      r.total_price = Number(r.total_price || 0);
    });
    // Summary
    const totalSourcedKg = rows.reduce(function (s, r) { return s + r.net_weight_kg; }, 0);
    const totalPaidGhs = rows.reduce(function (s, r) { return s + r.total_price; }, 0);
    const byCollector = {};
    for (let i = 0; i < rows.length; i++) {
      const code = rows[i].collector_code;
      byCollector[code] = (byCollector[code] || 0) + rows[i].net_weight_kg;
    }
    let largestSupplierCode = null;
    let largestSupplierKg = 0;
    Object.keys(byCollector).forEach(function (code) {
      if (byCollector[code] > largestSupplierKg) {
        largestSupplierKg = byCollector[code];
        largestSupplierCode = code;
      }
    });
    const largestSupplierPct = totalSourcedKg > 0 ? Math.round((largestSupplierKg / totalSourcedKg) * 100) : 0;
    const materialsPresent = Array.from(new Set(rows.map(function (r) { return r.material_type; }))).sort();
    res.json({
      success: true,
      summary: {
        total_sourced_kg: Math.round(totalSourcedKg * 100) / 100,
        total_paid_out_ghs: Math.round(totalPaidGhs * 100) / 100,
        transaction_count: rows.length,
        collector_count: Object.keys(byCollector).length,
        largest_supplier_code: largestSupplierCode,
        largest_supplier_kg: Math.round(largestSupplierKg * 100) / 100,
        largest_supplier_pct: largestSupplierPct
      },
      rows: rows,
      materials_present: materialsPresent
    });
  } catch (err) {
    console.error('[aggregator/reports/sourcing]', err);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

// Sales report — this aggregator → processor / recycler / converter.
app.get('/api/aggregator/reports/sales', requireAuth, async (req, res) => {
  try {
    if (!req.user.hasRole('aggregator')) return res.status(403).json({ success: false, message: 'Aggregator access only' });
    const filters = _reportsValidateCommon(req, res);
    if (!filters) return;
    const aggId = req.user.id;
    const result = await pool.query(
      `WITH sales AS (
         SELECT
           pt.id,
           pt.created_at AS date,
           'TXN-' || TO_CHAR(pt.created_at, 'YYYYMMDD') || '-' || LPAD(pt.id::text, 4, '0') AS ref,
           CASE
             WHEN pt.processor_id IS NOT NULL THEN 'processor'
             WHEN pt.recycler_id  IS NOT NULL THEN 'recycler'
             WHEN pt.converter_id IS NOT NULL THEN 'converter'
           END AS buyer_kind,
           CASE
             WHEN pt.processor_id IS NOT NULL THEN 'PRO-' || LPAD(pt.processor_id::text, 4, '0')
             WHEN pt.recycler_id  IS NOT NULL THEN 'REC-' || LPAD(pt.recycler_id::text, 4, '0')
             WHEN pt.converter_id IS NOT NULL THEN 'CNV-' || LPAD(pt.converter_id::text, 4, '0')
           END AS buyer_code,
           COALESCE(p.company, p.name, r.company, r.name, c.company, c.name) AS buyer_name,
           pt.material_type,
           pt.form,
           pt.gross_weight_kg,
           COALESCE(pt.net_weight_kg, pt.gross_weight_kg) AS net_weight_kg,
           pt.price_per_kg,
           pt.total_price,
           pt.payment_status,
           pt.payment_completed_at AS paid_at
         FROM pending_transactions pt
         LEFT JOIN processors  p ON p.id = pt.processor_id
         LEFT JOIN recyclers   r ON r.id = pt.recycler_id
         LEFT JOIN converters  c ON c.id = pt.converter_id
         WHERE pt.transaction_type = 'aggregator_sale'
           AND pt.aggregator_id = $1
           AND pt.created_at >= $2 AND pt.created_at < $3
           AND ($4::text IS NULL OR pt.material_type = ANY(string_to_array($4, ',')))
           AND (
             $5::text IS NULL
             OR ($5 = 'processor' AND pt.processor_id IS NOT NULL AND ($6::int IS NULL OR pt.processor_id = $6))
             OR ($5 = 'recycler'  AND pt.recycler_id  IS NOT NULL AND ($6::int IS NULL OR pt.recycler_id  = $6))
             OR ($5 = 'converter' AND pt.converter_id IS NOT NULL AND ($6::int IS NULL OR pt.converter_id = $6))
           )
       )
       SELECT
         s.*,
         COALESCE((
           SELECT SUM(pts.weight_kg_attributed)
           FROM pending_transaction_sources pts
           JOIN pending_transactions src ON src.id = pts.source_pending_tx_id
           WHERE pts.child_pending_tx_id = s.id AND src.collector_id IS NOT NULL
         ), 0) AS traced_kg_in_row,
         COALESCE((
           SELECT SUM(pts.weight_kg_attributed)
           FROM pending_transaction_sources pts
           WHERE pts.child_pending_tx_id = s.id
         ), 0) AS attributed_kg_in_row,
         (
           SELECT ARRAY_AGG(DISTINCT 'COL-' || LPAD(src.collector_id::text, 4, '0') ORDER BY 'COL-' || LPAD(src.collector_id::text, 4, '0'))
           FROM pending_transaction_sources pts
           JOIN pending_transactions src ON src.id = pts.source_pending_tx_id
           WHERE pts.child_pending_tx_id = s.id AND src.collector_id IS NOT NULL
         ) AS collector_codes_array
       FROM sales s
       ORDER BY s.date DESC`,
      [aggId, filters.from, filters.toExclusive, filters.materials, filters.buyer_kind, filters.buyer_id]
    );
    const rows = result.rows.map(function (r) {
      const grossKg = Number(r.gross_weight_kg || 0);
      const netKg = Number(r.net_weight_kg || 0);
      const tracedInRow = Number(r.traced_kg_in_row || 0);
      const attributedInRow = Number(r.attributed_kg_in_row || 0);
      const tracedPct = attributedInRow > 0 ? Math.round((tracedInRow / attributedInRow) * 100) : 0;
      return {
        id: r.id,
        date: r.date,
        ref: r.ref,
        buyer_kind: r.buyer_kind,
        buyer_code: r.buyer_code,
        buyer_name: r.buyer_name,
        material_type: r.material_type,
        form: r.form,
        gross_weight_kg: grossKg,
        net_weight_kg: netKg,
        price_per_kg: Number(r.price_per_kg || 0),
        total_price: Number(r.total_price || 0),
        traced_pct: tracedPct,
        collector_codes: r.collector_codes_array || [],
        trace_kind: tracedPct >= 100 ? 'traced' : 'declared',
        payment_status: r.payment_status,
        paid_at: r.paid_at
      };
    });
    // Summary
    const totalVolumeKg = rows.reduce(function (s, r) { return s + r.net_weight_kg; }, 0);
    const totalValueGhs = rows.reduce(function (s, r) { return s + r.total_price; }, 0);
    let tracedKgSum = 0;
    let attributedKgSum = 0;
    for (let i = 0; i < result.rows.length; i++) {
      tracedKgSum += Number(result.rows[i].traced_kg_in_row || 0);
      attributedKgSum += Number(result.rows[i].attributed_kg_in_row || 0);
    }
    const tracedPctByKg = totalVolumeKg > 0 ? Math.round((tracedKgSum / totalVolumeKg) * 100) : 0;
    const declaredKg = Math.max(0, totalVolumeKg - tracedKgSum);
    const buyerCount = new Set(rows.map(function (r) { return r.buyer_code; })).size;
    const materialsPresent = Array.from(new Set(rows.map(function (r) { return r.material_type; }))).sort();
    res.json({
      success: true,
      summary: {
        total_volume_kg: Math.round(totalVolumeKg * 100) / 100,
        total_value_ghs: Math.round(totalValueGhs * 100) / 100,
        transaction_count: rows.length,
        buyer_count: buyerCount,
        traced_pct_by_kg: tracedPctByKg,
        traced_kg: Math.round(tracedKgSum * 100) / 100,
        declared_kg: Math.round(declaredKg * 100) / 100
      },
      rows: rows,
      materials_present: materialsPresent
    });
  } catch (err) {
    console.error('[aggregator/reports/sales]', err);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

// Lookup: distinct registered collectors this aggregator has sourced from.
// Joins collectors for name display in the FILTER UI only — never written to
// any export. Output here is consumed by the dropdown in the dashboard form.
app.get('/api/aggregator/reports/collectors-list', requireAuth, async (req, res) => {
  try {
    if (!req.user.hasRole('aggregator')) return res.status(403).json({ success: false, message: 'Aggregator access only' });
    const aggId = req.user.id;
    const result = await pool.query(
      `SELECT
         c.id AS collector_id,
         'COL-' || LPAD(c.id::text, 4, '0') AS collector_code,
         TRIM(c.first_name || ' ' || COALESCE(c.last_name, '')) AS name,
         COUNT(pt.id) AS sale_count,
         COALESCE(SUM(pt.net_weight_kg), 0) AS total_kg,
         COALESCE(SUM(pt.total_price), 0) AS total_ghs
       FROM pending_transactions pt
       JOIN collectors c ON c.id = pt.collector_id
       WHERE pt.aggregator_id = $1
         AND pt.transaction_type IN ('collector_sale', 'aggregator_purchase')
         AND pt.collector_id IS NOT NULL
       GROUP BY c.id, c.first_name, c.last_name
       ORDER BY total_kg DESC`,
      [aggId]
    );
    res.json({
      success: true,
      collectors: result.rows.map(function (r) {
        return {
          collector_id: r.collector_id,
          collector_code: r.collector_code,
          name: r.name,
          sale_count: parseInt(r.sale_count, 10),
          total_kg: Math.round(Number(r.total_kg) * 100) / 100,
          total_ghs: Math.round(Number(r.total_ghs) * 100) / 100
        };
      })
    });
  } catch (err) {
    console.error('[aggregator/reports/collectors-list]', err);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

// Lookup: distinct buyers this aggregator has sold to (across all 3 buyer kinds).
app.get('/api/aggregator/reports/buyers-list', requireAuth, async (req, res) => {
  try {
    if (!req.user.hasRole('aggregator')) return res.status(403).json({ success: false, message: 'Aggregator access only' });
    const aggId = req.user.id;
    const result = await pool.query(
      `SELECT
         CASE
           WHEN pt.processor_id IS NOT NULL THEN 'processor'
           WHEN pt.recycler_id  IS NOT NULL THEN 'recycler'
           WHEN pt.converter_id IS NOT NULL THEN 'converter'
         END AS buyer_kind,
         COALESCE(pt.processor_id, pt.recycler_id, pt.converter_id) AS buyer_id,
         CASE
           WHEN pt.processor_id IS NOT NULL THEN 'PRO-' || LPAD(pt.processor_id::text, 4, '0')
           WHEN pt.recycler_id  IS NOT NULL THEN 'REC-' || LPAD(pt.recycler_id::text, 4, '0')
           WHEN pt.converter_id IS NOT NULL THEN 'CNV-' || LPAD(pt.converter_id::text, 4, '0')
         END AS buyer_code,
         COALESCE(p.company, p.name, r.company, r.name, c.company, c.name) AS buyer_name,
         COUNT(pt.id) AS sale_count,
         COALESCE(SUM(pt.net_weight_kg), 0) AS total_kg,
         COALESCE(SUM(pt.total_price), 0) AS total_ghs
       FROM pending_transactions pt
       LEFT JOIN processors  p ON p.id = pt.processor_id
       LEFT JOIN recyclers   r ON r.id = pt.recycler_id
       LEFT JOIN converters  c ON c.id = pt.converter_id
       WHERE pt.aggregator_id = $1
         AND pt.transaction_type = 'aggregator_sale'
       GROUP BY buyer_kind, buyer_id, buyer_code, buyer_name
       ORDER BY total_kg DESC`,
      [aggId]
    );
    res.json({
      success: true,
      buyers: result.rows.map(function (r) {
        return {
          buyer_kind: r.buyer_kind,
          buyer_id: r.buyer_id,
          buyer_code: r.buyer_code,
          buyer_name: r.buyer_name,
          sale_count: parseInt(r.sale_count, 10),
          total_kg: Math.round(Number(r.total_kg) * 100) / 100,
          total_ghs: Math.round(Number(r.total_ghs) * 100) / 100
        };
      })
    });
  } catch (err) {
    console.error('[aggregator/reports/buyers-list]', err);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

// ============================================
// PROCESSORS
// ============================================

app.get('/api/processors', async (req, res) => {
  try {
    const result = await pool.query(
      `SELECT id, name, company, email, phone, city, region, country, is_active, created_at FROM processors WHERE is_active=true AND LOWER(COALESCE(company,name,'')) NOT LIKE '%miniplast%' ORDER BY company, name`
    );
    res.json({ success: true, processors: result.rows });
  } catch (err) {
    console.error('Error listing processors:', err);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

// GET /api/processors/:id — fetch single processor record
app.get('/api/processors/:id', requireAuth, async (req, res) => {
  try {
    const { id } = req.params;
    const result = await pool.query(
      `SELECT id, name, company, email, city, region, country
       FROM processors WHERE id = $1`,
      [id]
    );
    if (result.rows.length === 0) return res.status(404).json({ success: false, message: 'Processor not found' });
    res.json({ success: true, processor: result.rows[0] });
  } catch (err) {
    console.error('GET /api/processors/:id error:', err);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

app.get('/api/processors/:id/stats', async (req, res) => {
  try {
    const { id } = req.params;
    const pr = await pool.query(`SELECT id, name, company, email, city, region FROM processors WHERE id=$1 AND is_active=true`, [id]);
    if (!pr.rows.length) return res.status(404).json({ success: false, message: 'Processor not found' });
    const thisMonth = new Date(); thisMonth.setDate(1); thisMonth.setHours(0,0,0,0);

    const [totals, monthlyTotals, inboundPending, postedPrices] = await Promise.all([
      pool.query(`SELECT COALESCE(SUM(gross_weight_kg),0) as total_kg, COALESCE(SUM(total_price),0) as total_value, COUNT(*) as total_txns FROM pending_transactions WHERE processor_id=$1 AND transaction_type='aggregator_sale'`, [id]),
      pool.query(`SELECT COALESCE(SUM(gross_weight_kg),0) as month_kg, COALESCE(SUM(total_price),0) as month_value, COUNT(*) as month_txns FROM pending_transactions WHERE processor_id=$1 AND transaction_type='aggregator_sale' AND created_at>=$2`, [id, thisMonth.toISOString()]),
      pool.query(`SELECT COUNT(*) as count, COALESCE(SUM(total_price),0) as value FROM pending_transactions WHERE processor_id=$1 AND status IN ('pending','dispatch_approved') AND transaction_type='aggregator_sale'`, [id]),
      pool.query(`SELECT * FROM posted_prices WHERE poster_type='processor' AND poster_id=$1 AND is_active=true ORDER BY material_type`, [id]).catch(() => ({ rows: [] }))
    ]);

    res.json({
      success: true, buyer: pr.rows[0],
      stats: { totals: totals.rows[0], this_month: monthlyTotals.rows[0], pending_payments: inboundPending.rows[0], posted_prices: postedPrices.rows }
    });
  } catch (err) {
    console.error('Processor stats error:', err);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

// Processor buying prices — per-processor
app.post('/api/processors/:id/prices', async (req, res) => {
  try {
    const id = parseInt(req.params.id);
    const { material, price_per_kg, currency } = req.body;
    if (!material || !price_per_kg) return res.status(400).json({ success: false, message: 'material and price_per_kg required' });
    const validMaterials = ['PET','HDPE','LDPE','PP'];
    if (!validMaterials.includes(material.toUpperCase())) return res.status(400).json({ success: false, message: 'Invalid material type' });
    const ghs = parseFloat(price_per_kg);
    if (isNaN(ghs) || ghs <= 0) return res.status(400).json({ success: false, message: 'price_per_kg must be a positive number' });
    const now = new Date();
    const expiresAt = new Date(now.getFullYear(), now.getMonth()+1, 0, 23, 59, 59);
    let city = null, region = null, country = 'Ghana';
    const loc = await pool.query('SELECT city, region, country FROM processors WHERE id=$1', [id]);
    if (loc.rows.length) { city = loc.rows[0].city; region = loc.rows[0].region; country = loc.rows[0].country; }
    const result = await pool.query(
      `INSERT INTO posted_prices (poster_type, poster_id, material_type, price_per_kg_ghs, city, region, country, expires_at)
       VALUES ('processor',$1,$2,$3,$4,$5,$6,$7)
       ON CONFLICT (poster_type, poster_id, material_type)
       DO UPDATE SET price_per_kg_ghs=$3, posted_at=NOW(), is_active=true, expires_at=$7
       RETURNING *`,
      [id, material.toUpperCase(), ghs, city, region, country||'Ghana', expiresAt.toISOString()]
    );
    res.json({ success: true, price: result.rows[0] });
  } catch (err) { console.error('Processor post price error:', err); res.status(500).json({ success: false, message: 'Server error' }); }
});

app.get('/api/processors/:id/prices', async (req, res) => {
  try {
    const id = parseInt(req.params.id);
    const result = await pool.query('SELECT * FROM posted_prices WHERE poster_type=\'processor\' AND poster_id=$1 AND is_active=true ORDER BY material_type', [id]);
    res.json({ success: true, prices: result.rows });
  } catch (err) { console.error('Processor get prices error:', err); res.status(500).json({ success: false, message: 'Server error' }); }
});

// All processor buying prices — public endpoint for aggregator marketplace
app.get('/api/processor-prices', async (req, res) => {
  try {
    const result = await pool.query(
      `SELECT pp.material_type, pp.price_per_kg_ghs, pp.posted_at as updated_at, pp.poster_id as processor_id,
              (SELECT COALESCE(company, name) FROM processors WHERE id=pp.poster_id LIMIT 1) as processor_name
       FROM posted_prices pp
       WHERE pp.poster_type='processor' AND pp.is_active=true
       ORDER BY pp.material_type, pp.price_per_kg_ghs DESC`
    );
    result.rows.forEach(r => {
      r.processor_code = CirculRoles.circulCode('processor', r.processor_id);
      r.name_visible = true; // public price listings show processor name
    });
    res.json({ success: true, prices: result.rows });
  } catch (err) { console.error('Processor prices error:', err); res.status(500).json({ success: false, message: 'Server error' }); }
});

app.get('/api/processor/top-suppliers', requireAuth, async (req, res) => {
  try {
    const { period } = req.query;
    const since = period === 'ytd'
      ? new Date(new Date().getFullYear(), 0, 1).toISOString()
      : (() => { const d = new Date(); d.setDate(1); d.setHours(0,0,0,0); return d.toISOString(); })();
    const result = await pool.query(
      `SELECT a.id AS partner_id, COALESCE(a.company, a.name, 'Unknown') AS name, 'aggregator' AS tier,
              SUM(pt.gross_weight_kg) AS volume, AVG(pt.price_per_kg) AS avg_price_paid
       FROM pending_transactions pt
       LEFT JOIN aggregators a ON a.id = pt.aggregator_id
       WHERE pt.processor_id IS NOT NULL AND pt.transaction_type = 'aggregator_sale' AND pt.created_at >= $1
       GROUP BY a.id ORDER BY volume DESC LIMIT 5`,
      [since]
    );
    result.rows.forEach(r => {
      r.partner_code = CirculRoles.circulCode(r.tier, r.partner_id);
      r.name_visible = true; // processor↔aggregator adjacent
    });
    res.json(result.rows);
  } catch (err) {
    console.error('Processor top-suppliers error:', err);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

app.get('/api/processor/top-buyers', requireAuth, async (req, res) => {
  try {
    if (!req.user.hasRole('processor')) return res.status(403).json({ success: false, message: 'Processor access only' });
    const { period } = req.query;
    const since = period === 'ytd'
      ? new Date(new Date().getFullYear(), 0, 1).toISOString()
      : (() => { const d = new Date(); d.setDate(1); d.setHours(0,0,0,0); return d.toISOString(); })();
    const result = await pool.query(
      `SELECT COALESCE(c.id, r.id) AS partner_id,
              COALESCE(c.company, c.name, r.company, r.name, 'Unknown') AS name,
              CASE WHEN pt.recycler_id IS NOT NULL THEN 'recycler' ELSE 'converter' END AS tier,
              SUM(pt.gross_weight_kg) AS volume, AVG(pt.price_per_kg) AS avg_price_paid
       FROM pending_transactions pt
       LEFT JOIN converters c ON c.id = pt.converter_id
       LEFT JOIN recyclers r ON r.id = pt.recycler_id
       WHERE pt.processor_id = $2 AND pt.transaction_type = 'processor_sale' AND pt.created_at >= $1
       GROUP BY 1, 2, 3 ORDER BY volume DESC LIMIT 5`,
      [since, req.user.id]
    );
    result.rows.forEach(r => {
      r.partner_code = CirculRoles.circulCode(r.tier, r.partner_id);
      r.name_visible = true; // processor↔recycler/converter adjacent
    });
    res.json(result.rows);
  } catch (err) {
    console.error('Processor top-buyers error:', err);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

app.get('/api/processor/transactions', requireAuth, async (req, res) => {
  try {
    if (!req.user.hasRole('processor')) return res.status(403).json({ success: false, message: 'Processor access only' });
    const procId = req.user.id;
    const result = await pool.query(
      `SELECT pt.id, pt.material_type, pt.gross_weight_kg, pt.price_per_kg, pt.total_price,
              pt.status, pt.transaction_type, pt.created_at,
              pt.aggregator_id, pt.converter_id, pt.recycler_id,
              COALESCE(a.company, a.name, 'Unknown') AS aggregator_name,
              COALESCE(c.company, c.name, r.company, r.name, 'Unknown') AS buyer_name
       FROM pending_transactions pt
       LEFT JOIN aggregators a ON a.id = pt.aggregator_id
       LEFT JOIN converters c ON c.id = pt.converter_id
       LEFT JOIN recyclers r ON r.id = pt.recycler_id
       WHERE pt.processor_id = $1
       ORDER BY pt.created_at DESC LIMIT 30`,
      [procId]
    );
    result.rows.forEach(r => {
      if (r.aggregator_id) r.aggregator_code = CirculRoles.circulCode('aggregator', r.aggregator_id);
      if (r.converter_id) r.buyer_code = CirculRoles.circulCode('converter', r.converter_id);
      else if (r.recycler_id) r.buyer_code = CirculRoles.circulCode('recycler', r.recycler_id);
      r.aggregator_name_visible = true; // processor↔aggregator adjacent
      r.buyer_name_visible = true; // processor↔converter/recycler adjacent
    });
    res.json({ success: true, transactions: result.rows });
  } catch (err) { console.error('GET /api/processor/transactions error:', err); res.status(500).json({ success: false, message: 'Server error' }); }
});

app.get('/api/recycler/transactions', requireAuth, async (req, res) => {
  try {
    if (!req.user.hasRole('recycler')) return res.status(403).json({ success: false, message: 'Recycler access only' });
    const recId = req.user.id;
    const result = await pool.query(
      `SELECT pt.id, pt.material_type, pt.gross_weight_kg, pt.price_per_kg, pt.total_price,
              pt.status, pt.transaction_type, pt.created_at,
              pt.processor_id, pt.converter_id,
              COALESCE(p.company, p.name, 'Unknown') AS processor_name,
              COALESCE(c.company, c.name, 'Unknown') AS converter_name
       FROM pending_transactions pt
       LEFT JOIN processors p ON p.id = pt.processor_id
       LEFT JOIN converters c ON c.id = pt.converter_id
       WHERE pt.recycler_id = $1
       ORDER BY pt.created_at DESC LIMIT 30`,
      [recId]
    );
    result.rows.forEach(r => {
      if (r.processor_id) r.processor_code = CirculRoles.circulCode('processor', r.processor_id);
      if (r.converter_id) r.converter_code = CirculRoles.circulCode('converter', r.converter_id);
      r.processor_name_visible = true; // recycler↔processor adjacent
      r.converter_name_visible = true; // recycler↔converter adjacent
    });
    res.json({ success: true, transactions: result.rows });
  } catch (err) { console.error('GET /api/recycler/transactions error:', err); res.status(500).json({ success: false, message: 'Server error' }); }
});

// ============================================
// CONVERTERS
// ============================================

app.get('/api/converters', async (req, res) => {
  try {
    const { country } = req.query;
    const params = []; let where = `WHERE is_active=true`;
    if (country) { params.push(country); where += ` AND country=$${params.length}`; }
    const result = await pool.query(
      `SELECT id, name, company, email, phone, city, region, country, is_active, created_at FROM converters ${where} ORDER BY company, name`,
      params
    );
    res.json({ success: true, converters: result.rows });
  } catch (err) {
    console.error('Error listing converters:', err);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

// GET /api/converters/:id — fetch single converter record
app.get('/api/converters/:id', requireAuth, async (req, res) => {
  try {
    const { id } = req.params;
    const result = await pool.query(
      `SELECT id, name, company, email, city, region, country
       FROM converters WHERE id = $1`,
      [id]
    );
    if (result.rows.length === 0) return res.status(404).json({ success: false, message: 'Converter not found' });
    res.json({ success: true, converter: result.rows[0] });
  } catch (err) {
    console.error('GET /api/converters/:id error:', err);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

app.get('/api/converters/:id/stats', async (req, res) => {
  try {
    const { id } = req.params;
    const cv = await pool.query(`SELECT id, name, company, email, city, region, country FROM converters WHERE id=$1 AND is_active=true`, [id]);
    if (!cv.rows.length) return res.status(404).json({ success: false, message: 'Converter not found' });
    const thisMonth = new Date(); thisMonth.setDate(1); thisMonth.setHours(0,0,0,0);

    const [totals, monthlyTotals, ptTotals, ptMonthly, inboundPending, postedPrices, orderStats] = await Promise.all([
      // Query the transactions table (completed trades) — converter_id does not exist
      // on the transactions table (restructure migration only has collector_id + aggregator_id),
      // so we return zeros here; converter trade data lives in pending_transactions.
      Promise.resolve({ rows: [{ total_kg: 0, total_value: 0, total_txns: 0 }] }),
      Promise.resolve({ rows: [{ month_kg: 0, month_value: 0, month_txns: 0 }] }),
      // Query pending_transactions for converter trades
      pool.query(`SELECT COALESCE(SUM(gross_weight_kg),0) as total_kg, COALESCE(SUM(total_price),0) as total_value, COUNT(*) as total_txns FROM pending_transactions WHERE converter_id=$1 AND transaction_type IN ('processor_sale','recycler_sale')`, [id]).catch(() => ({ rows: [{ total_kg: 0, total_value: 0, total_txns: 0 }] })),
      pool.query(`SELECT COALESCE(SUM(gross_weight_kg),0) as month_kg, COALESCE(SUM(total_price),0) as month_value, COUNT(*) as month_txns FROM pending_transactions WHERE converter_id=$1 AND transaction_type IN ('processor_sale','recycler_sale') AND created_at>=$2`, [id, thisMonth.toISOString()]).catch(() => ({ rows: [{ month_kg: 0, month_value: 0, month_txns: 0 }] })),
      pool.query(`SELECT COUNT(*) as count, COALESCE(SUM(total_price),0) as value FROM pending_transactions WHERE converter_id=$1 AND status IN ('pending','dispatch_approved') AND transaction_type IN ('processor_sale','recycler_sale')`, [id]).catch(() => ({ rows: [{ count: 0, value: 0 }] })),
      pool.query(`SELECT * FROM posted_prices WHERE poster_type='converter' AND poster_id=$1 AND is_active=true ORDER BY material_type`, [id]).catch(() => ({ rows: [] })),
      pool.query(`SELECT COUNT(*) as total_orders, COUNT(*) FILTER (WHERE status='open') as open_orders, COALESCE(SUM(fulfilled_kg),0) as fulfilled_kg FROM orders WHERE buyer_id=$1`, [id]).catch(() => ({ rows: [{ total_orders: 0, open_orders: 0, fulfilled_kg: 0 }] }))
    ]);
    // Merge totals from both tables
    const mergedTotals = {
      total_kg: parseFloat(totals.rows[0].total_kg) + parseFloat(ptTotals.rows[0].total_kg),
      total_value: parseFloat(totals.rows[0].total_value) + parseFloat(ptTotals.rows[0].total_value),
      total_txns: parseInt(totals.rows[0].total_txns) + parseInt(ptTotals.rows[0].total_txns)
    };
    const mergedMonthly = {
      month_kg: parseFloat(monthlyTotals.rows[0].month_kg) + parseFloat(ptMonthly.rows[0].month_kg),
      month_value: parseFloat(monthlyTotals.rows[0].month_value) + parseFloat(ptMonthly.rows[0].month_value),
      month_txns: parseInt(monthlyTotals.rows[0].month_txns) + parseInt(ptMonthly.rows[0].month_txns)
    };

    res.json({
      success: true, buyer: cv.rows[0],
      stats: { totals: mergedTotals, this_month: mergedMonthly, pending_payments: inboundPending.rows[0], posted_prices: postedPrices.rows, orders: orderStats.rows[0] }
    });
  } catch (err) {
    console.error('Converter stats error:', err);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

app.get('/api/converter/top-suppliers', requireAuth, async (req, res) => {
  try {
    if (!req.user.hasRole('converter')) return res.status(403).json({ success: false, message: 'Converter access only' });
    const converterId = req.user.converter_id || req.user.id;
    const { period } = req.query;
    const since = period === 'ytd'
      ? new Date(new Date().getFullYear(), 0, 1).toISOString()
      : (() => { const d = new Date(); d.setDate(1); d.setHours(0,0,0,0); return d.toISOString(); })();
    const result = await pool.query(
      `SELECT
         COALESCE(p.id, r.id) AS partner_id,
         COALESCE(p.company, p.name, r.company, r.name, 'Unknown') AS name,
         CASE WHEN pt.recycler_id IS NOT NULL THEN 'recycler' ELSE 'processor' END AS tier,
         SUM(pt.gross_weight_kg) AS volume,
         AVG(pt.price_per_kg) AS avg_price_paid
       FROM pending_transactions pt
       LEFT JOIN processors p ON p.id = pt.processor_id
       LEFT JOIN recyclers r ON r.id = pt.recycler_id
       WHERE pt.converter_id = $2
         AND pt.transaction_type IN ('processor_sale','recycler_sale')
         AND pt.created_at >= $1
       GROUP BY 1, 2, 3
       ORDER BY volume DESC
       LIMIT 5`,
      [since, converterId]
    );
    result.rows.forEach(r => {
      r.partner_code = CirculRoles.circulCode(r.tier, r.partner_id);
      r.name_visible = true; // converter↔processor/recycler adjacent
    });
    res.json(result.rows);
  } catch (err) {
    console.error('Converter top-suppliers error:', err);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

// ============================================
// RECYCLERS
// ============================================

app.get('/api/recyclers', async (req, res) => {
  try {
    const result = await pool.query(
      `SELECT id, name, company, email, phone, city, region, country, is_active, created_at FROM recyclers WHERE is_active=true ORDER BY company, name`
    );
    res.json({ success: true, recyclers: result.rows });
  } catch (err) {
    console.error('Error listing recyclers:', err);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

app.get('/api/recyclers/:id', requireAuth, async (req, res) => {
  try {
    const { id } = req.params;
    const result = await pool.query(
      `SELECT id, name, company, email, city, region, country FROM recyclers WHERE id = $1`, [id]
    );
    if (result.rows.length === 0) return res.status(404).json({ success: false, message: 'Recycler not found' });
    res.json({ success: true, recycler: result.rows[0] });
  } catch (err) {
    console.error('GET /api/recyclers/:id error:', err);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

app.get('/api/recyclers/:id/stats', async (req, res) => {
  try {
    const { id } = req.params;
    const rc = await pool.query(`SELECT id, name, company, email, city, region, country FROM recyclers WHERE id=$1 AND is_active=true`, [id]);
    if (!rc.rows.length) return res.status(404).json({ success: false, message: 'Recycler not found' });
    const thisMonth = new Date(); thisMonth.setDate(1); thisMonth.setHours(0,0,0,0);

    const [totals, monthlyTotals, inboundPending, postedPrices] = await Promise.all([
      pool.query(`SELECT COALESCE(SUM(gross_weight_kg),0) as total_kg, COALESCE(SUM(total_price),0) as total_value, COUNT(*) as total_txns FROM pending_transactions WHERE recycler_id=$1 AND transaction_type='processor_sale'`, [id]),
      pool.query(`SELECT COALESCE(SUM(gross_weight_kg),0) as month_kg, COALESCE(SUM(total_price),0) as month_value, COUNT(*) as month_txns FROM pending_transactions WHERE recycler_id=$1 AND transaction_type='processor_sale' AND created_at>=$2`, [id, thisMonth.toISOString()]),
      pool.query(`SELECT COUNT(*) as count, COALESCE(SUM(total_price),0) as value FROM pending_transactions WHERE recycler_id=$1 AND status IN ('pending','dispatch_approved') AND transaction_type='processor_sale'`, [id]),
      pool.query(`SELECT * FROM posted_prices WHERE poster_type='recycler' AND poster_id=$1 AND is_active=true ORDER BY material_type`, [id]).catch(() => ({ rows: [] }))
    ]);

    res.json({
      success: true, buyer: rc.rows[0],
      stats: { totals: totals.rows[0], this_month: monthlyTotals.rows[0], pending_payments: inboundPending.rows[0], posted_prices: postedPrices.rows }
    });
  } catch (err) {
    console.error('Recycler stats error:', err);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

app.get('/api/recycler/top-suppliers', requireAuth, async (req, res) => {
  try {
    if (!req.user.hasRole('recycler')) return res.status(403).json({ success: false, message: 'Recycler access only' });
    const { period } = req.query;
    const since = period === 'ytd'
      ? new Date(new Date().getFullYear(), 0, 1).toISOString()
      : (() => { const d = new Date(); d.setDate(1); d.setHours(0,0,0,0); return d.toISOString(); })();
    const result = await pool.query(
      `SELECT p.id AS partner_id, COALESCE(p.company, p.name, 'Unknown') AS name, 'processor' AS tier,
              SUM(pt.gross_weight_kg) AS volume, AVG(pt.price_per_kg) AS avg_price_paid
       FROM pending_transactions pt
       LEFT JOIN processors p ON p.id = pt.processor_id
       WHERE pt.recycler_id = $2 AND pt.transaction_type = 'processor_sale' AND pt.created_at >= $1
       GROUP BY p.id ORDER BY volume DESC LIMIT 5`,
      [since, req.user.id]
    );
    result.rows.forEach(r => {
      r.partner_code = CirculRoles.circulCode('processor', r.partner_id);
      r.name_visible = true; // recycler↔processor adjacent
    });
    res.json(result.rows);
  } catch (err) {
    console.error('Recycler top-suppliers error:', err);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

app.get('/api/recycler/top-buyers', requireAuth, async (req, res) => {
  try {
    const { period } = req.query;
    const since = period === 'ytd'
      ? new Date(new Date().getFullYear(), 0, 1).toISOString()
      : (() => { const d = new Date(); d.setDate(1); d.setHours(0,0,0,0); return d.toISOString(); })();
    const result = await pool.query(
      `SELECT c.id AS partner_id, COALESCE(c.company, c.name, 'Unknown') AS name, 'converter' AS tier,
              SUM(pt.gross_weight_kg) AS volume, AVG(pt.price_per_kg) AS avg_price_paid
       FROM pending_transactions pt
       LEFT JOIN converters c ON c.id = pt.converter_id
       WHERE pt.recycler_id IS NOT NULL AND pt.transaction_type = 'recycler_sale' AND pt.created_at >= $1
       GROUP BY c.id ORDER BY volume DESC LIMIT 5`,
      [since]
    );
    result.rows.forEach(r => {
      r.partner_code = CirculRoles.circulCode('converter', r.partner_id);
      r.name_visible = true; // recycler↔converter adjacent
    });
    res.json(result.rows);
  } catch (err) {
    console.error('Recycler top-buyers error:', err);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

// ============================================
// TRANSACTIONS
// ============================================

app.post('/api/transactions', requireAuth, async (req, res) => {
  try {
    const { collector_id, aggregator_id, material_type, gross_weight_kg, contamination_deduction_percent = 0, contamination_types = [], quality_notes, price_per_kg, lat, lng, notes } = req.body;
    if (!collector_id || !material_type || !gross_weight_kg) return res.status(400).json({ success: false, message: 'collector_id, material_type, and gross_weight_kg are required' });
    // Verify the requester is the collector or aggregator
    const userId = req.user.id;
    if (userId !== parseInt(collector_id) && userId !== parseInt(aggregator_id)) return res.status(403).json({ success: false, message: 'Access denied' });
    const validMaterials = ['PET','HDPE','LDPE','PP'];
    if (!validMaterials.includes(material_type.toUpperCase())) return res.status(400).json({ success: false, message: `Invalid material type. Must be one of: ${validMaterials.join(', ')}` });
    const _wkg = parseFloat(gross_weight_kg);
    if (_wkg <= 0) return res.status(400).json({ success: false, message: 'Weight must be greater than 0' });
    if (_wkg > 500) return res.status(400).json({ success: false, message: 'gross_weight_kg must be > 0 and at most 500 kg' });
    const deduction = parseFloat(contamination_deduction_percent) || 0;
    const net_weight = parseFloat(gross_weight_kg) * (1 - deduction / 100);
    const pricePer = parseFloat(price_per_kg) || 0;
    const total_price = parseFloat((net_weight * pricePer).toFixed(2));
    const result = await pool.query(
      `INSERT INTO transactions (collector_id, aggregator_id, material_type, gross_weight_kg, net_weight_kg, contamination_deduction_percent, contamination_types, quality_notes, price_per_kg, total_price, lat, lng, notes)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13) RETURNING *`,
      [collector_id, aggregator_id||null, material_type.toUpperCase(), gross_weight_kg, net_weight, deduction, JSON.stringify(contamination_types), quality_notes||null, pricePer, total_price, lat||null, lng||null, notes||null]
    );
    res.status(201).json({ success: true, transaction: result.rows[0] });
  } catch (err) {
    if (err.code === '23503') return res.status(400).json({ success: false, message: 'Collector not found' });
    console.error('Error creating transaction:', err);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

app.get('/api/converter/transactions', requireAuth, async (req, res) => {
  try {
    if (!req.user.hasRole('converter')) return res.status(403).json({ success: false, message: 'Converter access only' });
    const converterId = req.user.converter_id || req.user.id;
    const { start_date, end_date, limit = 30, offset = 0 } = req.query;
    let query = `SELECT pt.id, pt.batch_id, pt.material_type, pt.gross_weight_kg AS net_weight_kg, pt.price_per_kg, pt.total_price,
                        pt.status AS payment_status, pt.transaction_type, pt.created_at AS transaction_date,
                        pt.processor_id, pt.recycler_id,
                        COALESCE(p.company, p.name) AS processor_name, p.company AS processor_company,
                        COALESCE(r.company, r.name) AS recycler_name, r.company AS recycler_company
                 FROM pending_transactions pt
                 LEFT JOIN processors p ON p.id = pt.processor_id
                 LEFT JOIN recyclers r ON r.id = pt.recycler_id
                 WHERE pt.converter_id = $1
                   AND pt.transaction_type IN ('processor_sale','recycler_sale')`;
    const params = [converterId];
    if (start_date) { params.push(start_date); query += ` AND pt.created_at >= $${params.length}::timestamptz`; }
    if (end_date) { params.push(end_date); query += ` AND pt.created_at <= $${params.length}::timestamptz`; }
    query += ` ORDER BY pt.created_at DESC`;
    params.push(parseInt(limit)); query += ` LIMIT $${params.length}`;
    const result = await pool.query(query, params);
    for (const row of result.rows) {
      if (row.processor_id) {
        row.processor_code = CirculRoles.circulCode('processor', row.processor_id);
        row.supplier_code = row.processor_code;
        row.supplier_name_visible = true;
      }
      if (row.recycler_id) {
        row.recycler_code = CirculRoles.circulCode('recycler', row.recycler_id);
        row.supplier_code = row.recycler_code;
        row.supplier_name_visible = true;
      }
    }
    res.json({ success: true, transactions: result.rows, total: result.rows.length });
  } catch (err) {
    console.error('GET /api/converter/transactions error:', err);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

app.get('/api/transactions', async (req, res) => {
  try {
    const { collector_id, aggregator_id, material_type, start_date, end_date, payment_status, limit = 100, offset = 0 } = req.query;
    let query = `SELECT t.*, c.first_name as collector_first_name, c.last_name as collector_last_name, c.phone as collector_phone, c.average_rating as collector_rating, 'COL-' || LPAD(c.id::text, 4, '0') AS collector_display_name, a.name as aggregator_name, 'AGG-' || LPAD(a.id::text, 4, '0') AS aggregator_code FROM transactions t LEFT JOIN collectors c ON c.id=t.collector_id LEFT JOIN aggregators a ON a.id=t.aggregator_id WHERE 1=1`;
    const params = [];
    if (collector_id) { params.push(collector_id); query += ` AND t.collector_id=$${params.length}`; }
    if (aggregator_id) { params.push(aggregator_id); query += ` AND t.aggregator_id=$${params.length}`; }
    if (material_type) { params.push(material_type.toUpperCase()); query += ` AND t.material_type=$${params.length}`; }
    if (start_date) { params.push(start_date); query += ` AND t.transaction_date>=$${params.length}::timestamptz`; }
    if (end_date) { params.push(end_date); query += ` AND t.transaction_date<=$${params.length}::timestamptz`; }
    if (payment_status) { const statuses = payment_status.split(',').map(s => s.trim()).filter(Boolean); if (statuses.length) { params.push(statuses); query += ` AND t.payment_status=ANY($${params.length})`; } }
    const countResult = await pool.query(query.replace(/SELECT t\.\*.*?FROM/s, 'SELECT COUNT(*) as total FROM'), params);
    params.push(parseInt(limit)); query += ` ORDER BY t.transaction_date DESC LIMIT $${params.length}`;
    params.push(parseInt(offset)); query += ` OFFSET $${params.length}`;
    const result = await pool.query(query, params);
    res.json({ success: true, transactions: result.rows, total: parseInt(countResult.rows?.[0]?.total||0), limit: parseInt(limit), offset: parseInt(offset) });
  } catch (err) {
    console.error('GET /api/transactions error:', err.message);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

app.get('/api/stats', async (req, res) => {
  try {
    const { start_date, end_date, aggregator_id } = req.query;
    const params = []; let dateFilter = '';
    if (start_date) { params.push(start_date); dateFilter += ` AND t.transaction_date>=$${params.length}::timestamptz`; }
    if (end_date) { params.push(end_date); dateFilter += ` AND t.transaction_date<=$${params.length}::timestamptz`; }
    if (aggregator_id) { params.push(aggregator_id); dateFilter += ` AND t.aggregator_id=$${params.length}`; }
    const [materialStats, totals, topCollectors] = await Promise.all([
      pool.query(`SELECT material_type, SUM(net_weight_kg) as total_kg, SUM(gross_weight_kg) as total_gross_kg, AVG(contamination_deduction_percent) as avg_contamination_percent, COUNT(*) as transaction_count FROM transactions t WHERE 1=1 ${dateFilter} GROUP BY material_type ORDER BY total_kg DESC`, params),
      pool.query(`SELECT COALESCE(SUM(net_weight_kg),0) as total_weight_kg, COALESCE(SUM(gross_weight_kg),0) as total_gross_weight_kg, COALESCE(SUM(total_price),0) as total_revenue, COUNT(*) as total_transactions, COUNT(DISTINCT collector_id) as active_collectors FROM transactions t WHERE 1=1 ${dateFilter}`, params),
      pool.query(`SELECT c.id, c.first_name, c.last_name, c.phone, c.average_rating, SUM(t.net_weight_kg) as total_kg, COUNT(t.id) as transactions FROM collectors c JOIN transactions t ON t.collector_id=c.id WHERE 1=1 ${dateFilter} GROUP BY c.id ORDER BY total_kg DESC LIMIT 10`, params)
    ]);
    let todayFilter = ''; const todayParams = [];
    if (aggregator_id) { todayParams.push(aggregator_id); todayFilter = ` AND aggregator_id=$${todayParams.length}`; }
    const today = await pool.query(`SELECT COALESCE(SUM(net_weight_kg),0) as today_weight_kg, COALESCE(SUM(total_price),0) as today_revenue, COUNT(*) as today_transactions FROM transactions WHERE transaction_date>=CURRENT_DATE${todayFilter}`, todayParams);
    res.json({ success: true, stats: { totals: totals.rows[0], today: today.rows[0], by_material: materialStats.rows, top_collectors: topCollectors.rows } });
  } catch (err) {
    console.error('Error fetching stats:', err);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

// ============================================
// RATINGS
// ============================================

async function handleCreateRating(req, res) {
  try {
    const { transaction_id, rater_type, rater_id, rated_type, rated_id, rating, tags, notes, rating_direction } = req.body;
    const finalRaterType = rater_type || req.user.role || (Array.isArray(req.user.roles) ? req.user.roles[0] : null);
    const finalRaterId   = rater_id   || req.user.id;
    const finalRatedType = rated_type;
    const finalRatedId   = rated_id;
    if (!finalRaterType || !finalRaterId) return res.status(400).json({ success: false, message: 'rater_type and rater_id are required' });
    if (!finalRatedType || !finalRatedId) return res.status(400).json({ success: false, message: 'rated_type and rated_id are required' });
    if (!rating || rating < 1 || rating > 5) return res.status(400).json({ success: false, message: 'rating must be 1-5' });
    if (transaction_id) {
      const dup = await pool.query(`SELECT id FROM ratings WHERE transaction_id=$1 AND rater_type=$2 AND rater_id=$3`, [transaction_id, finalRaterType, finalRaterId]);
      if (dup.rows.length) return res.status(409).json({ success: false, message: 'You have already rated this transaction' });
    }
    const ratingRow = await createRating(pool, {
      transaction_id, rater_type: finalRaterType, rater_id: finalRaterId,
      rated_type: finalRatedType, rated_id: finalRatedId,
      rating, tags, notes, rating_direction
    });
    // Notify the rated user
    try {
      const ratedTable = CirculRoles.TABLE_MAP[finalRatedType];
      const raterTable = CirculRoles.TABLE_MAP[finalRaterType];
      if (!ratedTable || !raterTable) {
        console.warn('[RATING] unknown role in notify lookup:', { finalRatedType, finalRaterType });
      } else {
        const ratedNameCol = (finalRatedType === 'collector' || finalRatedType === 'agent') ? "first_name || ' ' || last_name" : 'name';
        const ratedRow = (await pool.query(`SELECT phone, ${ratedNameCol} AS name FROM ${ratedTable} WHERE id = $1`, [finalRatedId])).rows[0];
        const raterNameCol = (finalRaterType === 'collector' || finalRaterType === 'agent') ? "first_name || ' ' || last_name" : 'name';
        const raterRow = (await pool.query(`SELECT ${raterNameCol} AS name FROM ${raterTable} WHERE id = $1`, [finalRaterId])).rows[0];
        if (ratedRow && ratedRow.phone) {
          notify(EVENTS.RATING_RECEIVED, ratedRow.phone, { rater_name: raterRow ? raterRow.name : 'Someone', stars: rating });
        }
      }
    } catch (notifyErr) { console.warn('Notification error (rating_received):', notifyErr.message); }
    res.status(201).json({ success: true, rating: ratingRow });
  } catch (err) {
    if (err.code === '23505') return res.status(409).json({ success: false, message: 'You have already rated this transaction' });
    console.error('Rating error:', err);
    res.status(500).json({ success: false, message: 'Server error' });
  }
}

async function handleGetRatingsByOperator(req, res) {
  try {
    const { id } = req.params;
    const role = req.query.role;
    const typeFilter = role ? [role] : ['aggregator','processor','recycler','converter'];
    const ratings = await pool.query(`SELECT r.* FROM ratings r WHERE r.rated_id=$1 AND r.rated_type = ANY($2) ORDER BY r.created_at DESC LIMIT 50`, [id, typeFilter]);
    const avg = await pool.query(`SELECT AVG(rating)::NUMERIC(3,2) as avg_rating, COUNT(*) as count FROM ratings WHERE rated_id=$1 AND rated_type = ANY($2)`, [id, typeFilter]);
    res.json({ success: true, ratings: ratings.rows, summary: avg.rows[0] });
  } catch (err) { res.status(500).json({ success: false, message: 'Server error' }); }
}

app.post('/api/ratings', requireAuth, handleCreateRating);
app.get('/api/ratings/:id(\\d+)', handleGetRatingsByOperator);

app.get('/api/collectors/:id/ratings', async (req, res) => {
  try {
    const { id } = req.params;
    const collector = await pool.query(`SELECT id, first_name, last_name, average_rating FROM collectors WHERE id=$1`, [id]);
    if (!collector.rows.length) return res.status(404).json({ success: false, message: 'Collector not found' });
    const ratings = await pool.query(
      `SELECT r.*, t.material_type, t.net_weight_kg, t.transaction_date FROM ratings r LEFT JOIN transactions t ON t.id=r.transaction_id WHERE r.rated_id=$1 AND r.rated_type='collector' ORDER BY r.created_at DESC LIMIT $2 OFFSET $3`,
      [id, parseInt(req.query.limit||50), parseInt(req.query.offset||0)]
    );
    res.json({ success: true, collector: collector.rows[0], ratings: ratings.rows });
  } catch (err) { res.status(500).json({ success: false, message: 'Server error' }); }
});

// GET /api/ratings/pending — unrated recent transactions for current user
app.get('/api/ratings/pending', requireAuth, async (req, res) => {
  try {
    const role = req.user.role || (req.user.roles && req.user.roles[0]);
    const userId = req.user.id;
    if (!role || !userId) return res.json({ success: true, pending: [] });
    const pending = await getPendingRatings(pool, role, userId, 5);
    res.json({ success: true, pending });
  } catch (err) {
    console.error('GET /api/ratings/pending error:', err.message);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

// ============================================
// USSD
// ============================================

function parsePaginatedSelection(menuParts) {
  let page = 0;
  let idx = 0;
  while (idx < menuParts.length && menuParts[idx] === '4') {
    page++;
    idx++;
  }
  return { page, offset: page * 3, remaining: menuParts.slice(idx) };
}

const USSD_MATERIALS = { '1': 'PET', '2': 'HDPE', '3': 'LDPE', '4': 'PP' };
// Pilot regional capitals — 8 cities across 3 paginated USSD pages (3 + 3 + 2 layout)
// matching the parsePaginatedSelection helper convention. Add post-pilot regional
// capitals (Wa / Bolgatanga / Techiman / Sekondi / Dambai / Nalerigu / Damongo / Goaso)
// when geographic footprint warrants it.
const USSD_CITIES_LIST = [
  { city: 'Accra',      region: 'Greater Accra' },
  { city: 'Kumasi',     region: 'Ashanti' },
  { city: 'Tamale',     region: 'Northern' },
  { city: 'Takoradi',   region: 'Western' },
  { city: 'Cape Coast', region: 'Central' },
  { city: 'Koforidua',  region: 'Eastern' },
  { city: 'Ho',         region: 'Volta' },
  { city: 'Sunyani',    region: 'Bono' }
];

// Backwards-compat object form keyed by 1-based string index for any code still
// indexing by parts[N] directly (legacy non-paginated callers).
const USSD_CITIES = USSD_CITIES_LIST.reduce(function (acc, c, i) {
  acc[String(i + 1)] = c;
  return acc;
}, {});

// Render the paginated city-picker screen for a given slice of input parts.
// Returns { screen, offset, more } where screen is the CON text, offset is the
// item-index start for this page, and more=true if further pages exist.
function renderCityPickerScreen(pickerParts) {
  const sel = parsePaginatedSelection(pickerParts || []);
  const offset = sel.offset;
  const page = USSD_CITIES_LIST.slice(offset, offset + 3);
  if (page.length === 0) {
    return { screen: 'END Invalid city.\nDial again to retry.', offset: -1, more: false };
  }
  const more = (offset + 3) < USSD_CITIES_LIST.length;
  let msg = 'CON Select your city:\n';
  for (let i = 0; i < page.length; i++) {
    msg += (i + 1) + '. ' + page[i].city + '\n';
  }
  if (more) msg += '4. More \u2192\n';
  msg += '0. Cancel';
  return { screen: msg, offset: offset, more: more };
}

// Resolve the city record the user picked from a paginated input sequence.
// pickerParts is shaped as zero or more '4' (advance-page) followed by a
// final 1/2/3 city selection. Returns null if the pick is invalid or absent.
function resolveCityFromPaginatedParts(pickerParts) {
  const sel = parsePaginatedSelection(pickerParts || []);
  if (sel.remaining.length === 0) return null;
  const pick = parseInt(sel.remaining[0], 10);
  if (isNaN(pick) || pick < 1 || pick > 3) return null;
  const idx = sel.offset + (pick - 1);
  if (idx >= USSD_CITIES_LIST.length) return null;
  return USSD_CITIES_LIST[idx];
}

// Aggregator registration request flow — candidate collects first name, last
// name, optional company, city (paginated), then confirms. Creates a pending
// request row, fires admin notification (ntfy + Ghana SMS fallback), ENDs the
// USSD session. Admin approves via dashboard → 6-digit code is SMSed →
// candidate re-dials and the dispatch routes to handleAggregatorRegistrationCode.
async function handleAggregatorRegistrationRequest(parts, phone) {
  // Caller has consumed parts[0]='2' (aggregator role). This function sees
  // everything after: firstName, lastName, company (or '0'), city-picker, confirm.
  const level = parts.length;

  if (level === 0) {
    const phoneVariants = getPhoneVariants(phone);

    // Collision: existing collector on this phone
    const existingCol = await pool.query(
      `SELECT 1 FROM collectors WHERE phone = ANY($1) AND is_active=true LIMIT 1`,
      [phoneVariants]
    );
    if (existingCol.rows.length) {
      return 'END A collector on Circul is\nusing this phone. To\nregister as an aggregator\ninstead, contact Circul\nsupport first.';
    }

    // Defensive: registered aggregator shouldn't land here (dispatch routes to
    // handleAggregatorUssd first), but guard anyway.
    const existingAgg = await pool.query(
      `SELECT 1 FROM aggregators WHERE phone = ANY($1) AND is_active=true LIMIT 1`,
      [phoneVariants]
    );
    if (existingAgg.rows.length) {
      return 'END This phone is already an\naggregator on Circul.\n\nDial *920*54# and enter\nyour PIN to log in.';
    }

    // Rate limit: 3 requests / phone / 24h
    const recentCount = await pool.query(
      `SELECT COUNT(*)::int AS n FROM aggregator_registration_requests
       WHERE phone = ANY($1) AND created_at > NOW() - INTERVAL '24 hours'`,
      [phoneVariants]
    );
    if (recentCount.rows[0].n >= 3) {
      return 'END Too many requests.\n\nYou\'ve submitted 3 requests\nin the last 24 hours. Wait\n24 hours before trying\nagain.\n\nCall 024 131 48 41\nfor urgent help.';
    }

    // An active pending/code_issued request blocks new submissions.
    const activeReq = await pool.query(
      `SELECT 1 FROM aggregator_registration_requests
       WHERE phone = ANY($1) AND status IN ('pending', 'code_issued') LIMIT 1`,
      [phoneVariants]
    );
    if (activeReq.rows.length) {
      return 'END Request already pending.\n\nYou have an active\naggregator registration\nrequest. Wait for Circul\nsupport to review it.\n\nCall 024 131 48 41 if\nit\'s been over 24 hours.';
    }

    return 'CON Aggregator registration\n\nEnter your first name:';
  }

  if (level === 1) return 'CON Enter your last name:';

  if (level === 2) {
    const lastName = parts[1].trim();
    if (!lastName) return 'END Last name required.\nDial again.';
    return 'CON Company name\n(Optional \u2014 dial 0 to skip):';
  }

  // parts.slice(3) is the city-picker input (variable length due to pagination)
  const pickerParts = parts.slice(3);
  if (pickerParts.length === 0) return renderCityPickerScreen([]).screen;
  if (pickerParts[0] === '0') return 'END Cancelled.';

  const sel = parsePaginatedSelection(pickerParts);
  if (sel.remaining.length === 0) {
    // Still paging — show next page
    return renderCityPickerScreen(pickerParts).screen;
  }

  const cityData = resolveCityFromPaginatedParts(pickerParts);
  if (!cityData) return 'END Invalid city.\nDial again to retry.';

  const cityPartsLen = sel.page + 1;
  const afterCity = level - 3 - cityPartsLen;  // 0 = show confirm, 1 = commit/cancel

  const firstName = parts[0].trim();
  const lastName = parts[1].trim();
  const company = parts[2] === '0' ? null : parts[2].trim();
  const fullName = firstName + ' ' + lastName;

  if (afterCity === 0) {
    return 'CON Confirm request:\n\n' + fullName + (company ? '\n' + company : '') + '\n' + cityData.city + '\n\n1. Submit\n0. Cancel';
  }

  if (afterCity === 1) {
    const choice = parts[level - 1];
    if (choice === '0') return 'END Cancelled.';
    if (choice !== '1') return 'END Invalid option.\nDial again to retry.';

    try {
      await pool.query(
        `INSERT INTO aggregator_registration_requests (phone, name, company, city, region, status, source)
         VALUES ($1, $2, $3, $4, $5, 'pending', 'ussd')`,
        [phone, fullName, company, cityData.city, cityData.region]
      );
    } catch (err) {
      console.error('[agg-reg-request] insert failed:', err);
      return 'END System error.\nTry again later.';
    }

    // Fire admin notification — non-blocking
    notifyAdmin(EVENTS.AGGREGATOR_REQUEST_RECEIVED, {
      name: fullName,
      company: company,
      city: cityData.city,
      phone: phone
    }).catch(function (e) { console.warn('[agg-reg-request] notify-admin failed:', e.message); });

    return 'END Request submitted, ' + firstName + '.\n\nCircul support will SMS\nyou in 24h.\n\nQuestions: 024 131 48 41';
  }

  return 'END Invalid option.\nDial again to retry.';
}

// Aggregator registration code-entry flow — dispatched by /api/ussd when an
// active aggregator_registration_requests row exists with status='code_issued'.
// Candidate enters code → sets PIN → confirms PIN → registration commits
// atomically (INSERT aggregator + mark request completed + write audit row).
async function handleAggregatorRegistrationCode(parts, requestRow) {
  const level = parts.length;

  if (level === 0) {
    const remainMs = new Date(requestRow.code_expires_at).getTime() - Date.now();
    const remainMin = Math.max(0, Math.floor(remainMs / 60000));
    const remainSec = Math.max(0, Math.floor((remainMs % 60000) / 1000));
    return 'CON Welcome back, ' + requestRow.name.split(' ')[0] + '!\n\nFinish your aggregator\nregistration.\n\nEnter the 6-digit code\nfrom your SMS:\n(Expires in ' + remainMin + ' min ' + remainSec + 's)';
  }

  const enteredCode = parts[0];

  if (!verifyOtp(enteredCode, requestRow.code_hash)) {
    const remaining = requestRow.code_attempts_remaining - 1;
    if (remaining <= 0) {
      await pool.query(
        `UPDATE aggregator_registration_requests SET status = 'code_failed', code_attempts_remaining = 0, updated_at = NOW() WHERE id = $1`,
        [requestRow.id]
      );
      return 'END Too many wrong codes.\n\nYour code is invalid.\nCall Circul support\nat 024 131 48 41\nfor a new code.';
    }
    await pool.query(
      `UPDATE aggregator_registration_requests SET code_attempts_remaining = $1, updated_at = NOW() WHERE id = $2`,
      [remaining, requestRow.id]
    );
    return 'CON Wrong code. ' + remaining + ' attempt' + (remaining > 1 ? 's' : '') + ' left.\n\nEnter the 6-digit code\nfrom your SMS:';
  }

  if (level === 1) {
    return "CON Create a 4-digit PIN:\n\n4\u20136 digits, numbers only.\nAvoid 0000, 1234, or your\nbirth year.";
  }

  const newPin = parts[1];
  if (newPin.length < 4 || newPin.length > 6 || !/^\d+$/.test(newPin)) {
    return 'END PIN must be 4-6 digits.\nDial again to retry.';
  }

  if (level === 2) return 'CON Confirm PIN:';

  const confirmPin = parts[2];
  if (confirmPin !== newPin) return 'END PINs did not match.\nDial again to retry.';

  // Atomic commit: insert aggregator + mark request completed + audit
  const hashedPin = await hashPassword(newPin);
  const firstName = requestRow.name.split(' ')[0];
  const aggName = requestRow.name;

  const client = await pool.connect();
  let aggregatorId;
  try {
    await client.query('BEGIN');
    const aggInsert = await client.query(
      `INSERT INTO aggregators (name, company, phone, pin, city, region, country, is_active)
       VALUES ($1, $2, $3, $4, $5, $6, $7, true)
       RETURNING id`,
      [aggName, requestRow.company, requestRow.phone, hashedPin, requestRow.city, requestRow.region, requestRow.country || 'Ghana']
    );
    aggregatorId = aggInsert.rows[0].id;
    await client.query(
      `UPDATE aggregator_registration_requests
       SET status = 'completed', aggregator_id = $1, updated_at = NOW()
       WHERE id = $2`,
      [aggregatorId, requestRow.id]
    );
    await recordAdminAction(client, {
      actor_type: 'system',
      actor_email: null,
      action: 'aggregator_registration_completed',
      target_type: 'aggregator',
      target_id: aggregatorId,
      details: { request_id: requestRow.id, name: aggName, company: requestRow.company, city: requestRow.city, source: 'ussd' }
    });
    await client.query('COMMIT');
  } catch (err) {
    await client.query('ROLLBACK').catch(function () {});
    if (err.code === '23505') return 'END Phone already registered.\nDial again to login.';
    console.error('[agg-reg-code] commit failed:', err);
    return 'END System error.\nTry again later.';
  } finally {
    client.release();
  }

  const aggCode = 'AGG-' + String(aggregatorId).padStart(4, '0');
  notify(EVENTS.AGGREGATOR_REGISTRATION_COMPLETED, requestRow.phone, {
    name: firstName,
    company: requestRow.company
  }).catch(function (e) { console.warn('[agg-reg-code] welcome SMS failed:', e.message); });

  notifyAdmin(EVENTS.AGGREGATOR_REGISTRATION_COMPLETED_ADMIN, {
    name: aggName,
    company: requestRow.company,
    city: requestRow.city,
    agg_code: aggCode
  }).catch(function (e) { console.warn('[agg-reg-code] admin notify failed:', e.message); });

  return 'END Welcome to Circul,\n' + firstName + '!\n\nYou\'re now an aggregator' + (requestRow.company ? '\nat ' + requestRow.company : '') + '.\n\nDial *920*54# to log\npurchases and sales.\n\nCall 024 131 48 41 for\nhelp.';
}

async function handleUnregisteredUssd(parts, phone) {
  const level = parts.length;

  // Welcome — role split (collector vs aggregator vs exit)
  if (level === 0) return 'CON Welcome to Circul\nThe operating system for\nGhana\'s waste workers.\n\nSell. Track. Get paid.\n\nRegister as:\n1. Collector\n2. Aggregator\n0. Exit';

  if (parts[0] === '0') return 'END Thank you for using Circul.';

  // Aggregator path — hand off to request handler (Phase 4)
  if (parts[0] === '2') {
    return await handleAggregatorRegistrationRequest(parts.slice(1), phone);
  }

  // Collector path
  if (parts[0] === '1') {
    if (level === 1) return 'CON Collector registration\n\nEnter your first name:';
    if (level === 2) return 'CON Enter your last name:';

    const firstName = parts[1].trim();
    const lastName = parts[2].trim();
    if (!lastName) return 'END Last name required.\nDial again.';

    // parts.slice(3) is the city-picker input — variable length due to pagination.
    const pickerParts = parts.slice(3);
    if (pickerParts.length === 0) return renderCityPickerScreen([]).screen;
    if (pickerParts[0] === '0') return 'END Cancelled.';

    const sel = parsePaginatedSelection(pickerParts);
    if (sel.remaining.length === 0) {
      // All parts so far are '4's — show the next page
      return renderCityPickerScreen(pickerParts).screen;
    }

    const cityData = resolveCityFromPaginatedParts(pickerParts);
    if (!cityData) return 'END Invalid city.\nDial again to retry.';

    // After city pick, collect PIN. City consumed (sel.page + 1) parts.
    const cityPartsLen = sel.page + 1;
    const pinDepth = level - 3 - cityPartsLen;

    if (pinDepth === 0) return 'CON Create a 4-digit PIN:';
    if (pinDepth === 1) {
      const pin = parts[level - 1].trim();
      if (pin.length < 4 || pin.length > 6 || !/^\d+$/.test(pin)) {
        return 'END PIN must be 4-6 digits.\nDial again to retry.';
      }
      try {
        const hashedPin = await hashPassword(pin);
        await pool.query(
          `INSERT INTO collectors (first_name, last_name, phone, pin, city, region) VALUES ($1,$2,$3,$4,$5,$6)`,
          [firstName, lastName, phone, hashedPin, cityData.city, cityData.region]
        );
        return `END Registered! Welcome ${firstName}.\nCity: ${cityData.city}\n\nYour phone = your Circul ID. Keep PIN secret. Lose phone? Call your aggregator.\n\nDial again.`;
      } catch (err) {
        if (err.code === '23505') return 'END Phone already registered.\nDial again to login.';
        throw err;
      }
    }
    return 'END Invalid option.\nDial again to retry.';
  }

  return 'END Invalid option.\nDial again to retry.';
}

// ── Forgot-PIN reset request (triggered by "0" at welcome) ──
// Shared by collector / aggregator / agent handlers. parts[0] ('0') is already
// consumed by the caller; this sees everything after.
async function requestPinReset(remainingParts, user) {
  const depth = remainingParts.length;

  if (depth === 0) {
    return `CON Reset PIN?\n\nWe'll SMS a 6-digit code to\n${user.phone}.\n\n1. Send code\n0. Cancel`;
  }

  if (remainingParts[0] === '0') return 'END Cancelled.';
  if (remainingParts[0] !== '1') return 'END Invalid option.\nDial again to retry.';

  // Rate limit: max 3 resets per phone per 24h
  const recentCount = await pool.query(
    `SELECT COUNT(*) AS n FROM pin_reset_codes
     WHERE phone=$1 AND created_at > NOW() - INTERVAL '24 hours'`,
    [user.phone]
  );
  if (parseInt(recentCount.rows[0].n, 10) >= 3) {
    return 'END Too many resets today.\n\nWait 24 hours before\nrequesting another\nreset code.\n\nContact your aggregator\nfor urgent help.';
  }

  // Invalidate any previous active reset for this phone
  await pool.query(
    `UPDATE pin_reset_codes SET used_at = NOW()
     WHERE phone = $1 AND used_at IS NULL`,
    [user.phone]
  );

  const code = generateOtp();
  const codeHash = hashOtp(code);
  await pool.query(
    `INSERT INTO pin_reset_codes (phone, user_type, user_id, code_hash, expires_at)
     VALUES ($1, $2, $3, $4, NOW() + INTERVAL '10 minutes')`,
    [user.phone, user.user_type, user.user_id, codeHash]
  );

  try {
    await notify(EVENTS.PIN_RESET_OTP, user.phone, { code: code, minutes: 10 });
  } catch (e) { console.warn('[RESET] OTP notify failed:', e.message); }

  return 'END Reset code sent.\n\nCheck your SMS, then\ndial *920*54# again\nand enter the code.\n\nCode expires in 10 min.';
}

// ── Forgot-PIN reset execution (dispatched when an active reset row exists for this phone) ──
async function handleForgotPinUssd(parts, resetRow) {
  const depth = parts.length;

  if (depth === 0) {
    return 'CON Reset PIN\nEnter the 6-digit code\nfrom your SMS:';
  }

  const entered = parts[0];

  if (!verifyOtp(entered, resetRow.code_hash)) {
    const remaining = resetRow.attempts_remaining - 1;
    if (remaining <= 0) {
      await pool.query(
        `UPDATE pin_reset_codes SET attempts_remaining = 0, used_at = NOW() WHERE id = $1`,
        [resetRow.id]
      );
      return 'END Too many wrong codes.\n\nYour reset code is now\ninvalid. Dial *920*54#\nand request a new code.';
    }
    await pool.query(
      `UPDATE pin_reset_codes SET attempts_remaining = $1 WHERE id = $2`,
      [remaining, resetRow.id]
    );
    return `CON Wrong code. ${remaining} attempt${remaining > 1 ? 's' : ''} left.\n\nEnter the 6-digit code\nfrom your SMS:`;
  }

  if (depth === 1) {
    return "CON Enter new 4-digit PIN:\n\n4\u20136 digits, numbers only.\nAvoid 0000, 1234, or your\nbirth year.";
  }

  const newPin = parts[1];
  if (newPin.length < 4 || newPin.length > 6 || !/^\d+$/.test(newPin)) {
    return 'END PIN must be 4-6 digits.\nDial again to retry.';
  }

  if (depth === 2) {
    return 'CON Confirm new PIN:';
  }

  const confirm = parts[2];
  if (confirm !== newPin) {
    return 'END PINs did not match.\nDial again to retry.';
  }

  const hashedPin = await hashPassword(newPin);
  const userTable = resetRow.user_type === 'collector' ? 'collectors'
    : resetRow.user_type === 'aggregator' ? 'aggregators'
    : 'agents';

  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await client.query(
      `UPDATE ${userTable} SET pin = $1, must_change_pin = false WHERE id = $2`,
      [hashedPin, resetRow.user_id]
    );
    await client.query(
      `UPDATE pin_reset_codes SET used_at = NOW() WHERE id = $1`,
      [resetRow.id]
    );
    await client.query('COMMIT');
  } catch (err) {
    await client.query('ROLLBACK').catch(() => {});
    throw err;
  } finally {
    client.release();
  }

  fireResetCompletedNotifications(resetRow.user_type, resetRow.user_id, resetRow.phone).catch(err => {
    console.error('[RESET] notification error:', err.message);
  });

  return 'END PIN reset successfully.\n\nDial *920*54# and log in\nwith your new PIN.';
}

// Force-change-PIN gate. Fires on any USSD login where user.must_change_pin = true,
// for all three roles. Universal — collectors, aggregators, agents share this code path.
//
// Caller passes:
//   m         — slice of dial parts AFTER the validated PIN (i.e. parts.slice(pinIndex + 1)).
//   user      — fetched row including must_change_pin.
//   userTable — 'collectors' | 'aggregators' | 'agents' (whitelisted).
//
// Returns { needsGate, response, menuParts }:
//   needsGate=true → caller returns `response` directly without entering main menu logic.
//   needsGate=false → caller treats `menuParts` as the input to its main-menu dispatch.
//
// Depth math under USSD's stateless replay:
//   m=[]                                  → G1 prompt new PIN
//   m=[pin]                               → G2 prompt confirm
//   m=[pin, confirm] (matches)            → G3 success bridge — UPDATE happens here
//   m=[pin, confirm, '1']                 → continue to main menu (menuParts = m.slice(3))
//   m=[pin, confirm, '0']                 → END
const ALLOWED_USER_TABLES_FOR_GATE = ['collectors', 'aggregators', 'agents'];
async function gateForceChangePin(m, user, userTable) {
  if (!ALLOWED_USER_TABLES_FOR_GATE.includes(userTable)) {
    throw new Error('gateForceChangePin: invalid userTable: ' + userTable);
  }
  if (!user || !user.must_change_pin) {
    return { needsGate: false, menuParts: m };
  }

  // G1: prompt new PIN
  if (m.length === 0) {
    return {
      needsGate: true,
      response: 'CON You must set a new PIN.\n\nEnter new 4-digit PIN:\n4\u20136 digits, numbers only.\nAvoid 0000, 1234, or your\nbirth year.'
    };
  }

  const newPin = m[0];
  if (!/^\d{4,6}$/.test(newPin)) {
    return {
      needsGate: true,
      response: 'END PIN must be 4-6 digits.\nDial *920*54# to retry.'
    };
  }

  // G2: prompt confirm
  if (m.length === 1) {
    return {
      needsGate: true,
      response: 'CON Confirm new PIN:'
    };
  }

  const confirm = m[1];
  if (confirm !== newPin) {
    return {
      needsGate: true,
      response: 'END PINs don\'t match.\nDial *920*54# to retry.'
    };
  }

  // G3: success bridge — DO NOT update yet. USSD replays the full text on the
  // next dial; if we UPDATE here, the next dial would fail PIN validation against
  // the old default and mis-route. Defer UPDATE to the bridge response so it
  // happens in the same dial as either Continue or Exit.
  if (m.length === 2) {
    return {
      needsGate: true,
      response: 'CON PIN saved!\n\nUse this PIN next time\nyou log in.\n\n1. Continue\n0. Exit'
    };
  }

  // Bridge response: UPDATE happens here so subsequent dials use the new PIN.
  if (m[2] === '0') {
    const hashed = await hashPassword(newPin);
    await pool.query(
      `UPDATE ${userTable} SET pin = $1, must_change_pin = false WHERE id = $2`,
      [hashed, user.id]
    );
    return {
      needsGate: true,
      response: 'END Done. Dial *920*54# again to use the platform.'
    };
  }
  if (m[2] === '1') {
    const hashed = await hashPassword(newPin);
    await pool.query(
      `UPDATE ${userTable} SET pin = $1, must_change_pin = false WHERE id = $2`,
      [hashed, user.id]
    );
    return { needsGate: false, menuParts: m.slice(3) };
  }

  return {
    needsGate: true,
    response: 'END Invalid option.\nDial *920*54# to retry.'
  };
}

async function fireResetCompletedNotifications(userType, userId, userPhone) {
  const time = new Date().toLocaleTimeString('en-GB', { hour: '2-digit', minute: '2-digit' });
  await notify(EVENTS.PIN_RESET_COMPLETED, userPhone, { time: time });

  if (userType === 'collector') {
    // collectors.aggregator_id does NOT exist — derive most-recent via transactions
    const r = await pool.query(
      `SELECT a.phone, c.first_name || ' ' || COALESCE(c.last_name, '') AS name,
              'COL-' || LPAD(c.id::text, 4, '0') AS code
       FROM transactions t
       JOIN aggregators a ON a.id = t.aggregator_id
       JOIN collectors c ON c.id = t.collector_id
       WHERE t.collector_id = $1
       ORDER BY t.transaction_date DESC LIMIT 1`,
      [userId]
    );
    if (r.rows.length && r.rows[0].phone) {
      await notify(EVENTS.PIN_RESET_UPSTREAM_COLLECTOR, r.rows[0].phone, {
        user_name: r.rows[0].name.trim(),
        user_code: r.rows[0].code,
        time: time
      });
    }
  } else if (userType === 'aggregator') {
    // Fan out to processors transacted with in last 90d
    const r = await pool.query(
      `SELECT DISTINCT p.phone, a.name, 'AGG-' || LPAD(a.id::text, 4, '0') AS code
       FROM pending_transactions pt
       JOIN processors p ON p.id = pt.processor_id
       JOIN aggregators a ON a.id = pt.aggregator_id
       WHERE pt.aggregator_id = $1 AND pt.created_at > NOW() - INTERVAL '90 days' AND p.phone IS NOT NULL`,
      [userId]
    );
    for (const row of r.rows) {
      await notify(EVENTS.PIN_RESET_UPSTREAM_AGGREGATOR, row.phone, {
        user_name: row.name,
        user_code: row.code,
        time: time
      });
    }
  } else if (userType === 'agent') {
    const r = await pool.query(
      `SELECT a.phone, ag.first_name || ' ' || COALESCE(ag.last_name, '') AS name,
              'AGT-' || LPAD(ag.id::text, 4, '0') AS code
       FROM agents ag
       JOIN aggregators a ON a.id = ag.aggregator_id
       WHERE ag.id = $1`,
      [userId]
    );
    if (r.rows.length && r.rows[0].phone) {
      await notify(EVENTS.PIN_RESET_UPSTREAM_AGENT, r.rows[0].phone, {
        user_name: r.rows[0].name.trim(),
        user_code: r.rows[0].code,
        time: time
      });
    }
  }
}

// Phase 5C: shared rating sub-flow used by collector, aggregator, AND agent USSD My Stats.
async function handleUssdRating(menuParts, role, userId) {
  const depth = menuParts.length;
  const ratedKind = (role === 'aggregator' || role === 'agent') ? 'collector' : 'aggregator';

  const pending = await getPendingRatings(pool, role, userId, 4);
  if (!pending.length) {
    return 'END No recent transactions\nto rate.\nDial again later.';
  }

  if (depth === 0) {
    let menu = 'CON Rate a transaction:\n';
    for (let i = 0; i < pending.length; i++) {
      const t = pending[i];
      const peer = (t.peer_name || 'Unknown').slice(0, 14);
      const kg = parseFloat(t.gross_weight_kg).toFixed(0);
      menu += (i + 1) + '. ' + kg + 'kg ' + t.material_type + ' / ' + peer + '\n';
    }
    menu += '0. Cancel';
    return menu;
  }

  if (depth === 1) {
    if (menuParts[0] === '0') return 'END Cancelled.';
    const idx = parseInt(menuParts[0]) - 1;
    if (isNaN(idx) || idx < 0 || idx >= pending.length) {
      return 'END Invalid choice.\nDial again to retry.';
    }
    const txn = pending[idx];
    const peer = (txn.peer_name || 'Unknown').slice(0, 20);
    return 'CON Rate ' + parseFloat(txn.gross_weight_kg).toFixed(0) + 'kg '
      + txn.material_type + '\nfrom ' + peer + ':\n'
      + '1. \u2605\n2. \u2605\u2605\n3. \u2605\u2605\u2605\n4. \u2605\u2605\u2605\u2605\n5. \u2605\u2605\u2605\u2605\u2605\n0. Cancel';
  }

  if (depth === 2) {
    if (menuParts[1] === '0') return 'END Cancelled.';
    const idx = parseInt(menuParts[0]) - 1;
    const stars = parseInt(menuParts[1]);
    if (isNaN(idx) || idx < 0 || idx >= pending.length) {
      return 'END Invalid choice.\nDial again to retry.';
    }
    if (isNaN(stars) || stars < 1 || stars > 5) {
      return 'END Invalid rating.\nDial again to retry.';
    }
    const txn = pending[idx];
    try {
      const dup = await pool.query(
        `SELECT id FROM ratings WHERE transaction_id=$1 AND rater_type=$2 AND rater_id=$3`,
        [txn.txn_id, role, userId]
      );
      if (dup.rows.length) return 'END Already rated.\nThank you!';

      await createRating(pool, {
        transaction_id: txn.txn_id,
        rater_type: role,
        rater_id: userId,
        rated_type: ratedKind,
        rated_id: txn.peer_id,
        rating: stars,
        tags: [],
        notes: null,
        rating_direction: role + '_to_' + ratedKind
      });
    } catch (e) {
      console.error('[USSD rating] save failed:', e.message);
      return 'END Could not save rating.\nPlease try again later.';
    }
    return 'END Thank you!\nYour ' + stars + '\u2605 rating\nhas been recorded.';
  }

  return 'END Invalid option.\nDial again to retry.';
}

async function handleRegisteredUssd(parts, collector) {
  if (parts.length === 0) return `CON Circul Collector\nWelcome back, ${collector.first_name}!\n\nEnter 4-digit PIN:\n0. Forgot PIN`;

  // ── Forgot PIN entry point ──
  if (parts[0] === '0') {
    return await requestPinReset(parts.slice(1), { user_type: 'collector', user_id: collector.id, phone: collector.phone, name: collector.first_name });
  }

  // ── PIN validation with retry (max 3 attempts) ──
  let pinIndex = -1;
  for (let i = 0; i < Math.min(parts.length, 3); i++) {
    if (await verifyPassword(parts[i], collector.pin)) {
      pinIndex = i;
      break;
    }
  }
  if (pinIndex === -1) {
    const attempts = parts.length;
    if (attempts >= 3) {
      await pool.query(
        `INSERT INTO user_lockouts (user_type, user_id, phone, locked_until, reason)
         VALUES ($1, $2, $3, NOW() + INTERVAL '30 minutes', 'wrong_pin_x3')`,
        ['collector', collector.id, collector.phone]
      );
      return 'END Too many wrong PINs.\n\nAccount locked for 30 min.\nAfter lockout, dial\n*920*54# and select\n"0. Forgot PIN" to reset.';
    }
    const remaining = 3 - attempts;
    return `CON Wrong PIN. ${remaining} attempt${remaining > 1 ? 's' : ''} left.\n\nEnter 4-digit PIN:\n0. Forgot PIN`;
  }

  // ── Menu navigation (parts after valid PIN) ──
  // PIN validated. Apply force-change-PIN gate before main menu.
  const m_raw = parts.slice(pinIndex + 1);
  const gate = await gateForceChangePin(m_raw, collector, 'collectors');
  if (gate.needsGate) return gate.response;
  const m = gate.menuParts;
  const depth = m.length;

  if (depth === 0) return 'CON 1. Log Drop-off\n2. Sell My Material\n3. Discovery\n4. My Stats\n0. Exit';

  // ── Exit ──
  if (m[0] === '0') return `END Thank you, ${collector.first_name}!`;

  // ── Sell My Material ──
  if (m[0] === '2') return await handleCollectorSell(m.slice(1), collector);

  // ── Discovery ──
  if (m[0] === '3') return await handleCollectorDiscovery(m.slice(1), collector);

  // ── My Stats (with rating sub-menu) ──
  if (m[0] === '4') {
    if (m.length === 1) {
      const now = new Date();
      const monthStart = new Date(now.getFullYear(), now.getMonth(), 1).toISOString().slice(0, 10);
      const yearStart = new Date(now.getFullYear(), 0, 1).toISOString().slice(0, 10);
      const [confirmed, pending, rating] = await Promise.all([
        pool.query(
          `SELECT COALESCE(SUM(CASE WHEN transaction_date >= $2 THEN net_weight_kg ELSE 0 END), 0) as month_kg,
                  COALESCE(SUM(net_weight_kg), 0) as ytd_kg,
                  COALESCE(SUM(total_price), 0) as total_earned,
                  CASE WHEN SUM(net_weight_kg) > 0 THEN (SUM(total_price) / SUM(net_weight_kg))::NUMERIC(10,2) ELSE 0 END as avg_price,
                  COUNT(*) as total_txns
           FROM transactions WHERE collector_id = $1 AND transaction_date >= $3`,
          [collector.id, monthStart, yearStart]
        ),
        pool.query(`SELECT COUNT(*) as count FROM pending_transactions WHERE collector_id = $1 AND status = 'pending'`, [collector.id]),
        pool.query(`SELECT COALESCE(AVG(rating)::NUMERIC(3,1), 0) as avg, COUNT(*) as count FROM ratings WHERE rated_type = 'collector' AND rated_id = $1`, [collector.id])
      ]);
      const c = confirmed.rows[0], p = pending.rows[0], r = rating.rows[0];
      return `CON My Stats\n${parseFloat(c.month_kg).toFixed(1)}kg this month\n${parseFloat(c.ytd_kg).toFixed(1)}kg YTD / GH\u20b5${parseFloat(c.total_earned).toFixed(0)}\nRating: ${parseFloat(r.avg) > 0 ? '\u2605' + parseFloat(r.avg).toFixed(1) + ' (' + r.count + ')' : 'none'}\n${c.total_txns} done, ${p.count} pending\n\n1. Rate a transaction\n0. Back`;
    }
    if (m[1] === '0') return `END Goodbye, ${collector.first_name}!`;
    if (m[1] === '1') return await handleUssdRating(m.slice(2), 'collector', collector.id);
    return 'END Invalid option.\nDial again to retry.';
  }

  // ── Log Drop-off ──
  if (m[0] === '1') {
    // depth 1: select material
    if (depth === 1) return 'CON Select material:\n1. PET\n2. HDPE\n3. LDPE\n4. PP';

    const material = USSD_MATERIALS[m[1]];
    if (!material) return 'END Invalid material.\nDial again to retry.';

    // depth 2: enter weight
    if (depth === 2) return 'CON Enter weight in kg:';

    const weight = parseFloat(m[2]);
    if (isNaN(weight) || weight <= 0 || weight > 9999) return 'END Invalid weight.\nDial again to retry.';

    // depth 3: select aggregator
    if (depth === 3) {
      const city = collector.city || 'Accra';
      const aggs = await pool.query(
        `SELECT a.id, a.name, a.city,
                COALESCE((SELECT AVG(r.rating)::NUMERIC(3,1) FROM ratings r WHERE r.rated_type='aggregator' AND r.rated_id=a.id), 0) as rating,
                pp.price_per_kg_ghs
         FROM aggregators a
         JOIN posted_prices pp ON pp.poster_type = 'aggregator' AND pp.poster_id = a.id
           AND pp.material_type = $1 AND pp.is_active = true
         WHERE a.is_active = true AND a.city = $2
         ORDER BY pp.price_per_kg_ghs DESC
         LIMIT 4`,
        [material, city]
      );
      if (!aggs.rows.length) return `END No aggregators buying ${material} near ${city}.\nDial again to try another material.`;
      let msg = 'CON Select aggregator:\n';
      aggs.rows.forEach(function(a, i) {
        var ratingStr = parseFloat(a.rating) > 0 ? ' ★' + parseFloat(a.rating).toFixed(1) : '';
        msg += (i + 1) + '. ' + a.name + '\n   ' + a.city + ratingStr + ' GH₵' + parseFloat(a.price_per_kg_ghs).toFixed(2) + '/kg\n';
      });
      msg += '0. Cancel';
      return msg;
    }

    // depth 4: confirm — re-fetch aggregator data
    if (depth === 4) {
      if (m[3] === '0') return 'END Cancelled.';
      const aggChoice = parseInt(m[3]);
      if (isNaN(aggChoice) || aggChoice < 1) return 'END Invalid choice.\nDial again to retry.';
      const city = collector.city || 'Accra';
      const aggs = await pool.query(
        `SELECT a.id, a.name, a.phone, a.city,
                pp.price_per_kg_ghs
         FROM aggregators a
         JOIN posted_prices pp ON pp.poster_type = 'aggregator' AND pp.poster_id = a.id
           AND pp.material_type = $1 AND pp.is_active = true
         WHERE a.is_active = true AND a.city = $2
         ORDER BY pp.price_per_kg_ghs DESC
         LIMIT 4`,
        [material, city]
      );
      const agg = aggs.rows[aggChoice - 1];
      if (!agg) return 'END Invalid aggregator.\nDial again to retry.';
      const price = parseFloat(agg.price_per_kg_ghs);
      const total = (weight * price).toFixed(2);
      return `CON Confirm drop-off:\n${weight}kg ${material}\nTo: ${agg.name}\nPrice: GH₵${price.toFixed(2)}/kg\nTotal: GH₵${total}\n\n1. Confirm\n2. Cancel`;
    }

    // depth 5: execute
    if (depth === 5) {
      if (m[4] === '2') return 'END Cancelled.';
      if (m[4] === '1') {
        const aggChoice = parseInt(m[3]);
        const city = collector.city || 'Accra';
        const aggs = await pool.query(
          `SELECT a.id, a.name, a.phone, a.city,
                  pp.price_per_kg_ghs
           FROM aggregators a
           JOIN posted_prices pp ON pp.poster_type = 'aggregator' AND pp.poster_id = a.id
             AND pp.material_type = $1 AND pp.is_active = true
           WHERE a.is_active = true AND a.city = $2
           ORDER BY pp.price_per_kg_ghs DESC
           LIMIT 4`,
          [material, city]
        );
        const agg = aggs.rows[aggChoice - 1];
        if (!agg) return 'END Error: aggregator not found.\nDial again to retry.';
        const price = parseFloat(agg.price_per_kg_ghs);
        const total = weight * price;
        const ussdClient = await pool.connect();
        let rootRow;
        try {
          await ussdClient.query('BEGIN');
          const { row } = await insertRootTransaction(ussdClient, {
            transaction_type: 'collector_sale',
            status: 'pending',
            collector_id: collector.id,
            aggregator_id: agg.id,
            material_type: material,
            gross_weight_kg: weight,
            net_weight_kg: weight,
            price_per_kg: price,
            total_price: total,
            source: 'ussd'
          });
          await ussdClient.query('COMMIT');
          rootRow = row;
        } catch (e) {
          await ussdClient.query('ROLLBACK').catch(() => {});
          throw e;
        } finally {
          ussdClient.release();
        }
        const txnId = rootRow.id;
        const ref = 'TXN-' + new Date().toISOString().slice(0, 10).replace(/-/g, '') + '-' + String(txnId).padStart(4, '0');
        try {
          if (agg.phone) {
            await notify(EVENTS.DROPOFF_LOGGED, agg.phone, {
              collector_name: ((collector.first_name || '') + ' ' + (collector.last_name || '')).trim(),
              qty: weight,
              material: material,
              ref: ref
            });
          }
        } catch (e) { console.warn('[NOTIFY] dropoff_logged failed:', e.message); }
        return `END DROP-OFF LOGGED\nRef: ${ref}\n${weight}kg ${material} → ${agg.name}\nStatus: Pending confirmation\nContact: ${agg.phone}, ${agg.city}`;
      }
    }
  }

  return 'END Invalid option.\nDial again to retry.';
}

async function handleAggregatorUssd(parts, aggregator) {
  if (parts.length === 0) return `CON Circul Aggregator\nWelcome back, ${aggregator.name}!\n\nEnter 4-digit PIN:\n0. Forgot PIN`;

  // ── Forgot PIN entry point ──
  if (parts[0] === '0') {
    return await requestPinReset(parts.slice(1), { user_type: 'aggregator', user_id: aggregator.id, phone: aggregator.phone, name: aggregator.name });
  }

  // ── PIN validation with retry (max 3 attempts) ──
  let pinIndex = -1;
  for (let i = 0; i < Math.min(parts.length, 3); i++) {
    if (await verifyPassword(parts[i], aggregator.pin)) {
      pinIndex = i;
      break;
    }
  }
  if (pinIndex === -1) {
    const attempts = parts.length;
    if (attempts >= 3) {
      await pool.query(
        `INSERT INTO user_lockouts (user_type, user_id, phone, locked_until, reason)
         VALUES ($1, $2, $3, NOW() + INTERVAL '30 minutes', 'wrong_pin_x3')`,
        ['aggregator', aggregator.id, aggregator.phone]
      );
      return 'END Too many wrong PINs.\n\nAccount locked for 30 min.\nAfter lockout, dial\n*920*54# and select\n"0. Forgot PIN" to reset.';
    }
    const remaining = 3 - attempts;
    return `CON Wrong PIN. ${remaining} attempt${remaining > 1 ? 's' : ''} left.\n\nEnter 4-digit PIN:\n0. Forgot PIN`;
  }

  // ── Menu navigation (parts after valid PIN) ──
  // PIN validated. Apply force-change-PIN gate before main menu.
  const m_raw = parts.slice(pinIndex + 1);
  const gate = await gateForceChangePin(m_raw, aggregator, 'aggregators');
  if (gate.needsGate) return gate.response;
  const m = gate.menuParts;
  const depth = m.length;

  if (depth === 0) return 'CON 1. Register Collector\n2. Log Transaction\n3. Pending Drop-offs\n4. More\n0. Exit';

  // ── Exit ──
  if (m[0] === '0') return `END Thank you, ${aggregator.name}!`;

  // ── Register Collector (top-level path) ──
  if (m[0] === '1') {
    return await handleAggregatorRegister(m.slice(1), aggregator, null);
  }

  // ── Log Transaction (Purchase or Sale) ──
  if (m[0] === '2') {
    if (m.length === 1) {
      return 'CON Log Transaction\n\n1. Purchase (from collector)\n2. Sale (to processor)\n0. Back';
    }
    if (m[1] === '0') return 'END Cancelled.';
    if (m[1] === '1') return await handleAggregatorPurchase(m.slice(2), aggregator);
    if (m[1] === '2') return await handleAggregatorSale(m.slice(2), aggregator);
    return 'END Invalid option.\nDial again to retry.';
  }

  // ── Pending Drop-offs ──
  if (m[0] === '3') {
    return await handleAggregatorPending(m.slice(1), aggregator);
  }

  // ── More sub-menu (Marketplace + My Stats) ──
  if (m[0] === '4') {
    if (m.length === 1) return 'CON More options\n1. Marketplace\n2. My Stats\n0. Back';
    if (m[1] === '0') return 'CON 1. Register Collector\n2. Log Transaction\n3. Pending Drop-offs\n4. More\n0. Exit';

    // Marketplace
    if (m[1] === '1') return await handleAggregatorMarketplace(m.slice(2), aggregator);

    // My Stats (with rating sub-menu) — depth offset by 2 (m[0]='4', m[1]='2')
    if (m[1] === '2') {
      if (m.length === 2) {
        const now = new Date();
        const monthStart = new Date(now.getFullYear(), now.getMonth(), 1).toISOString().slice(0, 10);
        const yearStart = new Date(now.getFullYear(), 0, 1).toISOString().slice(0, 10);
        const [volume, unpaid, rating, collCount, pendingCount] = await Promise.all([
          pool.query(
            `SELECT COALESCE(SUM(CASE WHEN transaction_date >= $2 THEN net_weight_kg ELSE 0 END), 0) as month_kg,
                    COALESCE(SUM(net_weight_kg), 0) as ytd_kg,
                    COALESCE(SUM(total_price), 0) as revenue
             FROM transactions WHERE aggregator_id = $1 AND transaction_date >= $3`,
            [aggregator.id, monthStart, yearStart]
          ),
          pool.query(
            `SELECT COUNT(*) as count, COALESCE(SUM(total_price), 0) as value
             FROM transactions WHERE aggregator_id = $1 AND payment_status = 'unpaid' AND total_price > 0`,
            [aggregator.id]
          ),
          pool.query(
            `SELECT COALESCE(AVG(rating)::NUMERIC(3,1), 0) as avg, COUNT(*) as count
             FROM ratings WHERE rated_type = 'aggregator' AND rated_id = $1`,
            [aggregator.id]
          ),
          pool.query(
            `SELECT COUNT(DISTINCT collector_id) as count FROM transactions WHERE aggregator_id = $1`,
            [aggregator.id]
          ),
          pool.query(
            `SELECT COUNT(*) as count FROM pending_transactions
             WHERE aggregator_id = $1 AND status = 'pending'
               AND transaction_type IN ('collector_sale','aggregator_purchase')`,
            [aggregator.id]
          )
        ]);
        const v = volume.rows[0], u = unpaid.rows[0], r = rating.rows[0], cc = collCount.rows[0], pc = pendingCount.rows[0];
        return `CON My Stats\n${parseFloat(v.month_kg).toFixed(0)}kg mo / ${parseFloat(v.ytd_kg).toFixed(0)}kg YTD\nRev: GH\u20b5${parseFloat(v.revenue).toFixed(0)}\nUnpaid: GH\u20b5${parseFloat(u.value).toFixed(0)} (${u.count})\nRating: ${parseFloat(r.avg) > 0 ? '\u2605' + parseFloat(r.avg).toFixed(1) + ' (' + r.count + ')' : 'none'}\n${cc.count} collectors, ${pc.count} pending\n\n1. Rate a transaction\n0. Back`;
      }
      if (m[2] === '0') return `END Thank you, ${aggregator.name}!`;
      if (m[2] === '1') return await handleUssdRating(m.slice(3), 'aggregator', aggregator.id);
      return 'END Invalid option.\nDial again to retry.';
    }
    return 'END Invalid option.\nDial again to retry.';
  }

  return 'END Invalid option.\nDial again to retry.';
}

// ── Aggregator registers a collector ──
//
// Mirrors handleAgentRegister (line ~4622) field-for-field. Two differences:
//   1. Audit log target is admin_audit_log (aggregators have no agent_activity).
//   2. Inline-path success returns CON (bridge prompt) instead of END so the
//      caller can drop back into the purchase flow with the new collector_id.
//
// `prefilledPhone` non-null = inline-at-purchase path (skips phone entry,
// shorter parts list). null = top-level menu path (full flow).
async function handleAggregatorRegister(m, aggregator, prefilledPhone) {
  const depth = m.length;

  // depth 0: first name
  if (depth === 0) return 'CON Enter collector\'s\nfirst name:';
  const firstName = m[0];

  // depth 1: last name
  if (depth === 1) return 'CON Enter collector\'s\nlast name:';
  const lastName = m[1];

  if (prefilledPhone) {
    // Inline path — skip phone entry, go straight to city
    if (depth === 2) return 'CON Select city:\n1. Accra\n2. Kumasi\n3. Tamale\n4. Takoradi';
    const cityData = USSD_CITIES[m[2]];
    if (!cityData) return 'END Invalid city.\nDial again to retry.';

    if (depth === 3) {
      const phone = normalizeGhanaPhone(prefilledPhone);
      const displayPhone = phone && phone.startsWith('+233') ? '0' + phone.slice(4) : prefilledPhone;
      return `CON Register collector:\nName: ${firstName} ${lastName}\nPhone: ${displayPhone}\nCity: ${cityData.city}\n\n1. Confirm\n2. Cancel`;
    }

    if (depth === 4) {
      if (m[3] === '2') return 'END Cancelled.';
      if (m[3] === '1') {
        try {
          const hashedPin = await hashPassword('0000');
          const normalized = normalizeGhanaPhone(prefilledPhone);
          const phoneToStore = normalized && normalized.startsWith('+233') ? '0' + normalized.slice(4) : prefilledPhone;
          const result = await pool.query(
            `INSERT INTO collectors (first_name, last_name, phone, pin, city, region, must_change_pin)
             VALUES ($1, $2, $3, $4, $5, $6, true) RETURNING id`,
            [firstName.trim(), lastName.trim(), phoneToStore, hashedPin, cityData.city, cityData.region]
          );
          await pool.query(
            `INSERT INTO admin_audit_log (actor_type, actor_id, actor_email, action, target_type, target_id, details)
             VALUES ($1, $2, $3, $4, $5, $6, $7)`,
            [
              'aggregator',
              aggregator.id,
              null,
              'aggregator_registered_collector',
              'collector',
              result.rows[0].id,
              JSON.stringify({
                collector_phone: phoneToStore,
                collector_name: firstName.trim() + ' ' + lastName.trim(),
                city: cityData.city,
                via: 'inline_purchase',
                source: 'ussd'
              })
            ]
          );
          // Inline-path success: bridge CON back to caller (resolveCollectorForPurchase)
          return `CON ${firstName} registered!\nPIN: 0000 (tell them\nto change on first use)\n\nContinue purchase:\n1. Yes, log purchase\n0. Done for now`;
        } catch (err) {
          if (err.code === '23505') return 'END This phone number is\nalready registered.\n\nUse Log Transaction to\nrecord purchases from\nexisting collectors.';
          throw err;
        }
      }
    }
    return 'END Invalid option.\nDial again to retry.';
  }

  // Top-level path — collect phone in flow
  if (depth === 2) return 'CON Enter collector\'s\nphone number:';
  const phone = m[2];

  if (depth === 3) return 'CON Select city:\n1. Accra\n2. Kumasi\n3. Tamale\n4. Takoradi';
  const cityData = USSD_CITIES[m[3]];
  if (!cityData) return 'END Invalid city.\nDial again to retry.';

  if (depth === 4) {
    const normalized = normalizeGhanaPhone(phone);
    const displayPhone = normalized && normalized.startsWith('+233') ? '0' + normalized.slice(4) : phone;
    return `CON Register collector:\nName: ${firstName} ${lastName}\nPhone: ${displayPhone}\nCity: ${cityData.city}\n\n1. Confirm\n2. Cancel`;
  }

  if (depth === 5) {
    if (m[4] === '2') return 'END Cancelled.';
    if (m[4] === '1') {
      try {
        const hashedPin = await hashPassword('0000');
        const normalized = normalizeGhanaPhone(phone);
        const phoneToStore = normalized && normalized.startsWith('+233') ? '0' + normalized.slice(4) : phone;
        const result = await pool.query(
          `INSERT INTO collectors (first_name, last_name, phone, pin, city, region, must_change_pin)
           VALUES ($1, $2, $3, $4, $5, $6, true) RETURNING id`,
          [firstName.trim(), lastName.trim(), phoneToStore, hashedPin, cityData.city, cityData.region]
        );
        await pool.query(
          `INSERT INTO admin_audit_log (actor_type, actor_id, actor_email, action, target_type, target_id, details)
           VALUES ($1, $2, $3, $4, $5, $6, $7)`,
          [
            'aggregator',
            aggregator.id,
            null,
            'aggregator_registered_collector',
            'collector',
            result.rows[0].id,
            JSON.stringify({
              collector_phone: phoneToStore,
              collector_name: firstName.trim() + ' ' + lastName.trim(),
              city: cityData.city,
              via: 'top_level',
              source: 'ussd'
            })
          ]
        );
        return `END Collector registered!\n\n${firstName} ${lastName}\nPhone: ${phoneToStore}\nDefault PIN: 0000\n\nTell them to dial\n*920*54# and change\ntheir PIN on first use.`;
      } catch (err) {
        if (err.code === '23505') return 'END This phone number is\nalready registered.\n\nUse Log Transaction to\nrecord purchases from\nexisting collectors.';
        throw err;
      }
    }
  }

  return 'END Invalid option.\nDial again to retry.';
}

async function handleAggregatorPurchase(m, aggregator) {
  const depth = m.length;

  // depth 0: select collector (show list + phone lookup option)
  if (depth === 0) {
    const collectors = await pool.query(
      `SELECT c.id, c.first_name, c.last_name, c.phone,
              'COL-' || LPAD(c.id::text, 4, '0') AS display_name,
              COUNT(t.id) as txns
       FROM collectors c
       JOIN transactions t ON t.collector_id = c.id
       WHERE t.aggregator_id = $1
       GROUP BY c.id
       ORDER BY txns DESC
       LIMIT 3`,
      [aggregator.id]
    );
    if (!collectors.rows.length) {
      // Empty state: first-time aggregator, no transaction history
      return 'CON No previous collectors.\nEnter collector phone\nnumber:';
    }
    let msg = 'CON Select collector:\n';
    collectors.rows.forEach(function(c, i) {
      var name = ((c.first_name || '') + ' ' + (c.last_name || '')).trim() || c.display_name;
      msg += (i + 1) + '. ' + name + '\n   ' + c.display_name + ' · ' + c.txns + ' txn' + (c.txns > 1 ? 's' : '') + '\n';
    });
    msg += (collectors.rows.length + 1) + '. Enter phone number\n0. Cancel';
    return msg;
  }

  const resolved = await resolveCollectorForPurchase(m, aggregator);
  if (resolved.response) return resolved.response;
  if (!resolved.collector) return 'END Error resolving collector.\nDial again to retry.';

  const collector = resolved.collector;
  const mp = resolved.menuParts;
  const mpDepth = mp.length;

  // Select material
  if (mpDepth === 0) return 'CON Select material:\n1. PET\n2. HDPE\n3. LDPE\n4. PP';

  const material = USSD_MATERIALS[mp[0]];
  if (!material) return 'END Invalid material.\nDial again to retry.';

  // Enter weight
  if (mpDepth === 1) return 'CON Enter weight in kg:';

  const weight = parseFloat(mp[1]);
  if (isNaN(weight) || weight <= 0 || weight > 9999) return 'END Invalid weight.\nDial again to retry.';

  // Enter price per kg
  if (mpDepth === 2) return 'CON Enter price per kg\n(GH₵):';

  const price = parseFloat(mp[2]);
  if (isNaN(price) || price <= 0 || price > 999) return 'END Invalid price.\nDial again to retry.';

  // Confirm
  const total = (weight * price).toFixed(2);
  const collName = ((collector.first_name || '') + ' ' + (collector.last_name || '')).trim();
  const collCode = 'COL-' + String(collector.id).padStart(4, '0');

  if (mpDepth === 3) {
    return `CON Confirm purchase:\n${weight}kg ${material}\nFrom: ${collName} (${collCode})\nPrice: GH₵${price.toFixed(2)}/kg\nTotal: GH₵${total}\n\n1. Confirm\n2. Cancel`;
  }

  // Execute
  if (mpDepth === 4) {
    if (mp[3] === '2') return 'END Cancelled.';
    if (mp[3] === '1') {
      const ussdClient = await pool.connect();
      let rootRow;
      try {
        await ussdClient.query('BEGIN');
        const { row } = await insertRootTransaction(ussdClient, {
          transaction_type: 'aggregator_purchase',
          status: 'pending',
          collector_id: collector.id,
          aggregator_id: aggregator.id,
          material_type: material,
          gross_weight_kg: weight,
          net_weight_kg: weight,
          price_per_kg: price,
          total_price: parseFloat(total),
          source: 'ussd'
        });
        await ussdClient.query('COMMIT');
        rootRow = row;
      } catch (e) {
        await ussdClient.query('ROLLBACK').catch(() => {});
        throw e;
      } finally {
        ussdClient.release();
      }
      const txnId = rootRow.id;
      const ref = 'TXN-' + new Date().toISOString().slice(0, 10).replace(/-/g, '') + '-' + String(txnId).padStart(4, '0');
      try {
        if (collector.phone) {
          await notify(EVENTS.PURCHASE_LOGGED, collector.phone, {
            buyer_name: aggregator.name,
            qty: weight,
            material: material,
            amount: total,
            ref: ref
          });
        }
      } catch (e) { console.warn('[NOTIFY] purchase_logged failed:', e.message); }
      return `END PURCHASE LOGGED\nRef: ${ref}\n${weight}kg ${material} from ${collName}\nTotal: GH₵${total}\nStatus: Pending\nCollector: ${collector.phone}, ${collector.city || ''}`;
    }
  }

  return 'END Invalid option.\nDial again to retry.';
}

// ── Aggregator sell-upstream flow ──
// parts: what the aggregator typed after direction=2 was chosen.
//   depth 0: pick material (inventory shown inline)
//   depth 1: pick form (loose/baled)
//   depth 2: enter weight
//   depth 3: declare-shortfall prompt (only when weight > tracked)
//   depth 4: pick buyer
//   depth 5: confirm
async function handleAggregatorSale(m, aggregator) {
  const depth = m.length;

  // Tracked inventory per material — mirrors attributeAndInsert FIFO filter
  // exactly so the number we show == the number we can attribute.
  const inventoryRows = await pool.query(
    `SELECT material_type, COALESCE(SUM(remaining_kg), 0)::numeric AS available_kg
       FROM pending_transactions
      WHERE aggregator_id = $1
        AND transaction_type IN ('collector_sale', 'aggregator_purchase')
        AND remaining_kg > 0
        AND created_at >= NOW() - INTERVAL '30 days'
        AND status NOT IN ('rejected', 'dispatch_rejected', 'grade_c_flagged')
      GROUP BY material_type`,
    [aggregator.id]
  );
  const inventoryByMat = Object.create(null);
  for (const r of inventoryRows.rows) {
    inventoryByMat[r.material_type] = parseFloat(r.available_kg);
  }

  const MATERIALS = [
    { key: '1', name: 'PET' },
    { key: '2', name: 'HDPE' },
    { key: '3', name: 'LDPE' },
    { key: '4', name: 'PP' }
  ];

  // Screen S1: material + inventory
  if (depth === 0) {
    let msg = 'CON Sell which material?\n';
    for (const mat of MATERIALS) {
      const kg = inventoryByMat[mat.name] || 0;
      msg += `${mat.key}. ${mat.name} (${kg.toFixed(0)} kg)\n`;
    }
    msg += '0. Back';
    return msg;
  }

  if (m[0] === '0') return 'END Cancelled.';
  const material = (MATERIALS.find(x => x.key === m[0]) || {}).name;
  if (!material) return 'END Invalid material.\nDial again to retry.';

  const trackedKg = inventoryByMat[material] || 0;

  // Screen S2: form toggle
  if (depth === 1) {
    return `CON ${material} available: ${trackedKg.toFixed(0)} kg\n\nForm:\n1. Loose\n2. Baled\n0. Cancel`;
  }

  if (m[1] === '0') return 'END Cancelled.';
  const form = m[1] === '1' ? 'loose' : m[1] === '2' ? 'baled' : null;
  if (!form) return 'END Invalid form.\nDial again to retry.';

  // Screen S3: weight entry
  if (depth === 2) {
    return 'CON Enter weight in kg:\n(Max is whatever you hold — you can declare extra stock on the next screen.)';
  }

  const weight = parseFloat(m[2]);
  if (isNaN(weight) || weight <= 0 || weight > 99999) {
    return 'END Invalid weight.\nDial again to retry.';
  }

  const declared = Math.max(0, weight - trackedKg);
  const needsDeclare = declared > 0;

  // Screens S3.5a / S3.5b: declare-shortfall (conditional)
  if (needsDeclare && depth === 3) {
    if (trackedKg > 0) {
      // Variant a: partial shortfall
      return `CON You have ${trackedKg.toFixed(0)}kg ${material} tracked.\nYou entered ${weight.toFixed(0)}kg.\n\nDeclare ${declared.toFixed(0)}kg as existing stock?\n\n1. Yes, declare + sell ${weight.toFixed(0)}kg\n2. No, sell ${trackedKg.toFixed(0)}kg only\n0. Cancel`;
    }
    // Variant b: zero tracked
    return `CON You have 0kg ${material} tracked.\nYou entered ${weight.toFixed(0)}kg.\n\nDeclare ${weight.toFixed(0)}kg as existing stock and proceed?\n\n1. Yes\n0. Cancel`;
  }

  // Process declare choice
  let saleWeight = weight;
  let saleDeclared = declared;
  let declareDepthConsumed = 0;
  if (needsDeclare) {
    declareDepthConsumed = 1;
    const choice = m[3];
    if (choice === '0') return 'END Cancelled.';
    if (trackedKg > 0 && choice === '2') {
      saleWeight = trackedKg;
      saleDeclared = 0;
    } else if (choice === '1') {
      // declare+sell full weight — already the default
    } else {
      return 'END Invalid option.\nDial again to retry.';
    }
  }

  // Buyer discovery — region-preferred, country-wide fallback.
  async function loadBuyers() {
    const base =
      `SELECT pp.poster_type, pp.poster_id, pp.price_per_kg_ghs,
              CASE pp.poster_type
                WHEN 'processor' THEN (SELECT company FROM processors WHERE id = pp.poster_id)
                WHEN 'recycler'  THEN (SELECT company FROM recyclers  WHERE id = pp.poster_id)
                WHEN 'converter' THEN (SELECT company FROM converters WHERE id = pp.poster_id)
              END AS name,
              CASE pp.poster_type
                WHEN 'processor' THEN (SELECT phone FROM processors WHERE id = pp.poster_id)
                WHEN 'recycler'  THEN (SELECT phone FROM recyclers  WHERE id = pp.poster_id)
                WHEN 'converter' THEN (SELECT phone FROM converters WHERE id = pp.poster_id)
              END AS phone,
              CASE pp.poster_type
                WHEN 'processor' THEN (SELECT city FROM processors WHERE id = pp.poster_id)
                WHEN 'recycler'  THEN (SELECT city FROM recyclers  WHERE id = pp.poster_id)
                WHEN 'converter' THEN (SELECT city FROM converters WHERE id = pp.poster_id)
              END AS city
         FROM posted_prices pp
        WHERE pp.material_type = $1
          AND pp.is_active = true
          AND pp.poster_type IN ('processor','recycler','converter')`;
    let rows = (await pool.query(
      base + ' AND (pp.region = $2 OR pp.region IS NULL) ORDER BY pp.price_per_kg_ghs DESC LIMIT 10',
      [material, aggregator.region || null]
    )).rows;
    if (!rows.length) {
      rows = (await pool.query(
        base + ' ORDER BY pp.price_per_kg_ghs DESC LIMIT 10',
        [material]
      )).rows;
    }
    return rows;
  }

  // Screen S4: buyer list
  if (depth === 3 + declareDepthConsumed) {
    const buyers = await loadBuyers();
    if (!buyers.length) {
      return `END No buyers for ${material}\nright now.\n\nTry Marketplace (menu 3)\nto post a listing.`;
    }
    const BADGE = { processor: 'P', recycler: 'R', converter: 'C' };
    let msg = 'CON Sell to:\n';
    const limit = Math.min(4, buyers.length);
    for (let i = 0; i < limit; i++) {
      const b = buyers[i];
      const badge = BADGE[b.poster_type] || '?';
      msg += `${i + 1}. ${b.name} (${badge})\n   ${b.city || '—'} GH₵${parseFloat(b.price_per_kg_ghs).toFixed(2)}/kg\n`;
    }
    msg += '0. Cancel';
    return msg;
  }

  // Process buyer choice
  const buyerTok = m[3 + declareDepthConsumed];
  if (buyerTok === '0') return 'END Cancelled.';
  const buyerIdx = parseInt(buyerTok, 10);
  if (isNaN(buyerIdx) || buyerIdx < 1) return 'END Invalid choice.\nDial again to retry.';

  const buyers = await loadBuyers();
  const buyer = buyers[buyerIdx - 1];
  if (!buyer) return 'END Invalid buyer.\nDial again to retry.';
  const price = parseFloat(buyer.price_per_kg_ghs);
  const total = saleWeight * price;

  // Screen S5: confirm (with traced/declared split when declared > 0)
  if (depth === 4 + declareDepthConsumed) {
    let msg = `CON Confirm sale:\n${saleWeight.toFixed(0)}kg ${material} (${form})\nTo: ${buyer.name}\nPrice: GH₵${price.toFixed(2)}/kg\nTotal: GH₵${total.toFixed(2)}\n`;
    if (saleDeclared > 0) {
      const traced = saleWeight - saleDeclared;
      msg += `\nTraced: ${traced.toFixed(0)}kg\nDeclared: ${saleDeclared.toFixed(0)}kg\n`;
    }
    msg += '\n1. Confirm\n2. Cancel';
    return msg;
  }

  // Screen S6: commit
  const confirmTok = m[4 + declareDepthConsumed];
  if (confirmTok === '2') return 'END Cancelled.';
  if (confirmTok !== '1') return 'END Invalid option.\nDial again to retry.';

  const buyerFkCol = buyer.poster_type === 'processor' ? 'processor_id'
                   : buyer.poster_type === 'recycler'  ? 'recycler_id'
                   : 'converter_id';

  const client = await pool.connect();
  let txnId;
  try {
    await client.query('BEGIN');

    if (saleDeclared > 0) {
      await insertRootTransaction(client, {
        transaction_type: 'aggregator_purchase',
        status: 'confirmed',
        aggregator_id: aggregator.id,
        collector_id: null,
        material_type: material,
        gross_weight_kg: saleDeclared,
        net_weight_kg: saleDeclared,
        price_per_kg: 0,
        total_price: 0,
        source: 'ussd_declared',
        notes: 'Aggregator-declared existing stock (untraced origin)'
      });
    }

    const target = {
      transaction_type: 'aggregator_sale',
      status: 'pending',
      aggregator_id: aggregator.id,
      material_type: material,
      gross_weight_kg: saleWeight,
      net_weight_kg: saleWeight,
      price_per_kg: price,
      total_price: total,
      source: 'ussd',
      form: form
    };
    target[buyerFkCol] = buyer.poster_id;
    const { row } = await attributeAndInsert(client, target);
    txnId = row.id;

    await client.query('COMMIT');
  } catch (err) {
    await client.query('ROLLBACK').catch(() => {});
    if (err instanceof InsufficientSourceError) {
      return 'END Inventory changed since\nyou started. Dial again\nto retry with current\nnumbers.';
    }
    console.error('[aggregator-sale]', err);
    return 'END System error.\nTry again later.';
  } finally {
    client.release();
  }

  const ref = 'TXN-' + new Date().toISOString().slice(0, 10).replace(/-/g, '') + '-' + String(txnId).padStart(4, '0');

  try {
    if (buyer.phone) {
      await notify(EVENTS.DELIVERY_PENDING, buyer.phone, {
        sender_name: aggregator.name,
        qty: saleWeight,
        material: material
      });
    }
  } catch (e) { console.warn('[NOTIFY] delivery_pending failed:', e.message); }

  let successMsg = `END SALE LOGGED\nRef: ${ref}\n${saleWeight.toFixed(0)}kg ${material} → ${buyer.name}\n`;
  if (saleDeclared > 0) {
    const traced = saleWeight - saleDeclared;
    successMsg += `${traced.toFixed(0)}kg traced + ${saleDeclared.toFixed(0)}kg declared\n`;
  }
  successMsg += `Status: Pending arrival\n\nContact: ${buyer.phone || '—'}, ${buyer.city || '—'}`;
  return successMsg;
}

async function resolveCollectorForPurchase(m, aggregator) {
  // First, check if the aggregator has any previous collectors
  const collectors = await pool.query(
    `SELECT c.id, c.first_name, c.last_name, c.phone, c.city,
            COUNT(t.id) as txns
     FROM collectors c
     JOIN transactions t ON t.collector_id = c.id
     WHERE t.aggregator_id = $1
     GROUP BY c.id
     ORDER BY txns DESC
     LIMIT 3`,
    [aggregator.id]
  );

  const hasList = collectors.rows.length > 0;
  const phoneOptionIndex = hasList ? collectors.rows.length + 1 : null;

  // Detect mid-inline-register state from m's structure: a non-numeric input
  // at the register-firstName slot signals the user typed a name, so we're in
  // the inline register flow regardless of whether the lookup now finds the
  // freshly-inserted collector. (USSD is stateless — without this check, the
  // post-INSERT lookup would mis-route mid-flow inputs as found-confirm + material.)
  const looksLikeName = function (s) { return typeof s === 'string' && /[a-zA-Z]/.test(s); };

  // ── Empty state: m[0] is a phone number directly ──
  if (!hasList) {
    if (m.length === 0) return { response: null };

    // m[0] = phone number
    const phoneVariants = getPhoneVariants(normalizeGhanaPhone(m[0]));
    const found = await pool.query(
      `SELECT id, first_name, last_name, phone, city FROM collectors WHERE phone=ANY($1) AND is_active=true LIMIT 1`,
      [phoneVariants]
    );

    // Inline-register signature: m[1]='1' AND (lookup says not-found OR m[2] looks
    // like a name). Either path lands us in the inline register handling.
    const inlineSignature = m.length >= 2 && m[1] === '1' && (!found.rows.length || looksLikeName(m[2]));
    if (!found.rows.length || inlineSignature) {
      // m shape:
      //   m[0]=phone, m[1]='1' (yes register) or '0' (cancel)
      //   m[2..5] = firstName, lastName, city, confirm — passed to handleAggregatorRegister
      //   m[6] = bridge response ('1' continue purchase, '0' done)
      //   m[7..] = material/weight/price/confirm picked up by handleAggregatorPurchase
      if (m.length === 1) {
        return { response: 'CON ' + m[0] + ' is not\nregistered on Circul.\n\nRegister them now to\nlog this purchase?\n\n1. Yes, register\n0. Cancel' };
      }
      if (m[1] === '0') return { response: 'END Cancelled.' };
      if (m[1] !== '1') return { response: 'END Invalid option.\nDial again to retry.' };
      const regSlice = m.slice(2);
      if (regSlice.length <= 4) {
        return { response: await handleAggregatorRegister(regSlice, aggregator, m[0]) };
      }
      if (regSlice[4] === '0') return { response: 'END Done. Thanks for registering.' };
      if (regSlice[4] !== '1') return { response: 'END Invalid option.\nDial again to retry.' };
      const found2 = await pool.query(
        `SELECT id, first_name, last_name, phone, city FROM collectors WHERE phone=ANY($1) AND is_active=true LIMIT 1`,
        [phoneVariants]
      );
      if (!found2.rows.length) return { response: 'END Error: collector not found after registration.' };
      return { collector: found2.rows[0], menuParts: m.slice(7) };
    }
    const coll = found.rows[0];
    const collName = ((coll.first_name || '') + ' ' + (coll.last_name || '')).trim();
    const collCode = 'COL-' + String(coll.id).padStart(4, '0');
    if (m.length === 1) {
      return { response: `CON Collector found:\n${collName} (${collCode})\n${coll.city || ''}\n\n1. Confirm — proceed\n2. Try different number\n0. Cancel` };
    }
    if (m[1] === '0') return { response: 'END Cancelled.' };
    if (m[1] === '2') return { response: 'CON Enter collector phone\nnumber:' };
    return { collector: coll, menuParts: m.slice(2) };
  }

  // ── Has list: m[0] is either a collector index, the phone option, or cancel ──
  if (m[0] === '0') return { response: 'END Cancelled.' };

  const choice = parseInt(m[0]);

  // Phone lookup option (last numbered option in list)
  if (choice === phoneOptionIndex) {
    if (m.length === 1) return { response: 'CON Enter collector phone\nnumber:' };
    const phoneVariants = getPhoneVariants(normalizeGhanaPhone(m[1]));
    const found = await pool.query(
      `SELECT id, first_name, last_name, phone, city FROM collectors WHERE phone=ANY($1) AND is_active=true LIMIT 1`,
      [phoneVariants]
    );
    // Inline-register signature for has-list variant: m[2]='1' AND (lookup not-found OR m[3] looks like a name).
    const inlineSig2 = m.length >= 3 && m[2] === '1' && (!found.rows.length || looksLikeName(m[3]));
    if (!found.rows.length || inlineSig2) {
      // m shape: m[0]=phoneOptIdx, m[1]=phone, m[2]='1'/'0', m[3..6]=register, m[7]=bridge, m[8..]=purchase
      if (m.length === 2) {
        return { response: 'CON ' + m[1] + ' is not\nregistered on Circul.\n\nRegister them now to\nlog this purchase?\n\n1. Yes, register\n0. Cancel' };
      }
      if (m[2] === '0') return { response: 'END Cancelled.' };
      if (m[2] !== '1') return { response: 'END Invalid option.\nDial again to retry.' };
      const regSlice = m.slice(3);
      if (regSlice.length <= 4) {
        return { response: await handleAggregatorRegister(regSlice, aggregator, m[1]) };
      }
      if (regSlice[4] === '0') return { response: 'END Done. Thanks for registering.' };
      if (regSlice[4] !== '1') return { response: 'END Invalid option.\nDial again to retry.' };
      const found2 = await pool.query(
        `SELECT id, first_name, last_name, phone, city FROM collectors WHERE phone=ANY($1) AND is_active=true LIMIT 1`,
        [phoneVariants]
      );
      if (!found2.rows.length) return { response: 'END Error: collector not found after registration.' };
      return { collector: found2.rows[0], menuParts: m.slice(8) };
    }
    const coll = found.rows[0];
    const collName = ((coll.first_name || '') + ' ' + (coll.last_name || '')).trim();
    const collCode = 'COL-' + String(coll.id).padStart(4, '0');
    if (m.length === 2) {
      return { response: `CON Collector found:\n${collName} (${collCode})\n${coll.city || ''}\n\n1. Confirm — proceed\n2. Try different number\n0. Cancel` };
    }
    if (m[2] === '0') return { response: 'END Cancelled.' };
    if (m[2] === '2') return { response: 'CON Enter collector phone\nnumber:' };
    return { collector: coll, menuParts: m.slice(3) };
  }

  // List selection
  const selectedCollector = collectors.rows[choice - 1];
  if (!selectedCollector) return { response: 'END Invalid choice.\nDial again to retry.' };

  const fullColl = await pool.query(
    `SELECT id, first_name, last_name, phone, city FROM collectors WHERE id = $1`,
    [selectedCollector.id]
  );
  return { collector: fullColl.rows[0], menuParts: m.slice(1) };
}

async function handleAggregatorPending(m, aggregator) {
  const depth = m.length;

  const REJECTION_REASONS = {
    '1': 'Weight mismatch',
    '2': 'Wrong material',
    '3': 'Contaminated',
    '4': 'Not received'
  };

  // depth 0: list pending drop-offs
  if (depth === 0) {
    const pending = await pool.query(
      `SELECT pt.id, pt.material_type, pt.gross_weight_kg, pt.created_at,
              c.first_name AS collector_first_name, c.last_name AS collector_last_name,
              c.phone AS collector_phone, c.city AS collector_city,
              'COL-' || LPAD(c.id::text, 4, '0') AS collector_code
       FROM pending_transactions pt
       LEFT JOIN collectors c ON c.id = pt.collector_id
       WHERE pt.aggregator_id = $1 AND pt.status = 'pending'
         AND pt.transaction_type IN ('collector_sale', 'aggregator_purchase')
       ORDER BY pt.created_at DESC
       LIMIT 4`,
      [aggregator.id]
    );
    if (!pending.rows.length) return 'END No pending drop-offs.';
    let msg = 'CON Pending drop-offs:\n';
    pending.rows.forEach(function(p, i) {
      var name = ((p.collector_first_name || '') + ' ' + (p.collector_last_name || '')).trim() || p.collector_code;
      var date = new Date(p.created_at);
      var dateStr = date.toLocaleDateString('en-GB', { month: 'short', day: 'numeric' });
      msg += (i + 1) + '. ' + name + '\n   ' + parseFloat(p.gross_weight_kg).toFixed(0) + 'kg ' + p.material_type + ' · ' + dateStr + '\n';
    });
    msg += '0. Back';
    return msg;
  }

  // depth 1: show details of selected drop-off
  const choice = parseInt(m[0]);
  if (m[0] === '0') return 'CON 1. Log Purchase\n2. Pending Drop-offs\n3. My Stats\n0. Exit';

  // Re-fetch pending list to get the selected item
  const pending = await pool.query(
    `SELECT pt.id, pt.material_type, pt.gross_weight_kg, pt.created_at,
            c.first_name AS collector_first_name, c.last_name AS collector_last_name,
            c.phone AS collector_phone, c.city AS collector_city,
            'COL-' || LPAD(c.id::text, 4, '0') AS collector_code
     FROM pending_transactions pt
     LEFT JOIN collectors c ON c.id = pt.collector_id
     WHERE pt.aggregator_id = $1 AND pt.status = 'pending'
       AND pt.transaction_type IN ('collector_sale', 'aggregator_purchase')
     ORDER BY pt.created_at DESC
     LIMIT 4`,
    [aggregator.id]
  );
  const selected = pending.rows[choice - 1];
  if (!selected) return 'END Invalid choice.\nDial again to retry.';

  if (depth === 1) {
    var name = ((selected.collector_first_name || '') + ' ' + (selected.collector_last_name || '')).trim() || selected.collector_code;
    var date = new Date(selected.created_at);
    var dateStr = date.toLocaleDateString('en-GB', { year: 'numeric', month: 'short', day: 'numeric' });
    return `CON Drop-off details:\n${name} (${selected.collector_code})\n${parseFloat(selected.gross_weight_kg).toFixed(0)}kg ${selected.material_type}\nSubmitted: ${dateStr}\nPhone: ${selected.collector_phone || '—'}\n\n1. Confirm receipt\n2. Reject\n0. Back`;
  }

  // depth 2: confirm or reject
  if (depth === 2) {
    if (m[1] === '0') return 'CON 1. Log Purchase\n2. Pending Drop-offs\n3. My Stats\n0. Exit';

    if (m[1] === '1') {
      // Confirm
      await pool.query(
        `UPDATE pending_transactions SET status = 'confirmed', updated_at = NOW() WHERE id = $1`,
        [selected.id]
      );
      const remainCount = await pool.query(
        `SELECT COUNT(*) as count FROM pending_transactions
         WHERE aggregator_id = $1 AND status = 'pending'
           AND transaction_type IN ('collector_sale', 'aggregator_purchase')`,
        [aggregator.id]
      );
      var name = ((selected.collector_first_name || '') + ' ' + (selected.collector_last_name || '')).trim() || selected.collector_code;
      return `END DROP-OFF CONFIRMED\n${parseFloat(selected.gross_weight_kg).toFixed(0)}kg ${selected.material_type} from ${name}\nStatus: Confirmed\n\nRemaining pending: ${remainCount.rows[0].count}`;
    }

    if (m[1] === '2') {
      // Rejection reason selection
      return 'CON Reason for rejection:\n1. Weight mismatch\n2. Wrong material\n3. Contaminated\n4. Not received';
    }
  }

  // depth 3: execute rejection
  if (depth === 3 && m[1] === '2') {
    const reason = REJECTION_REASONS[m[2]];
    if (!reason) return 'END Invalid choice.\nDial again to retry.';
    await pool.query(
      `UPDATE pending_transactions SET status = 'rejected', rejection_reason = $1, updated_at = NOW() WHERE id = $2`,
      [reason, selected.id]
    );
    const remainCount = await pool.query(
      `SELECT COUNT(*) as count FROM pending_transactions
       WHERE aggregator_id = $1 AND status = 'pending'
         AND transaction_type IN ('collector_sale', 'aggregator_purchase')`,
      [aggregator.id]
    );
    var name = ((selected.collector_first_name || '') + ' ' + (selected.collector_last_name || '')).trim() || selected.collector_code;
    return `END DROP-OFF REJECTED\n${parseFloat(selected.gross_weight_kg).toFixed(0)}kg ${selected.material_type} from ${name}\nReason: ${reason}\n\nRemaining pending: ${remainCount.rows[0].count}`;
  }

  return 'END Invalid option.\nDial again to retry.';
}

async function handleAgentUssd(parts, agent) {
  if (parts.length === 0) return `CON Circul Agent\nWelcome back, ${agent.first_name}!\nWorking for: ${agent.aggregator_name}\n\nEnter 4-digit PIN:\n0. Forgot PIN`;

  // ── Forgot PIN entry point ──
  if (parts[0] === '0') {
    return await requestPinReset(parts.slice(1), { user_type: 'agent', user_id: agent.id, phone: agent.phone, name: agent.first_name });
  }

  // ── PIN validation with retry (max 3 attempts) ──
  let pinIndex = -1;
  for (let i = 0; i < Math.min(parts.length, 3); i++) {
    if (await verifyPassword(parts[i], agent.pin)) {
      pinIndex = i;
      break;
    }
  }
  if (pinIndex === -1) {
    const attempts = parts.length;
    if (attempts >= 3) {
      await pool.query(
        `INSERT INTO user_lockouts (user_type, user_id, phone, locked_until, reason)
         VALUES ($1, $2, $3, NOW() + INTERVAL '30 minutes', 'wrong_pin_x3')`,
        ['agent', agent.id, agent.phone]
      );
      return 'END Too many wrong PINs.\n\nAccount locked for 30 min.\nAfter lockout, dial\n*920*54# and select\n"0. Forgot PIN" to reset.';
    }
    const remaining = 3 - attempts;
    return `CON Wrong PIN. ${remaining} attempt${remaining > 1 ? 's' : ''} left.\n\nEnter 4-digit PIN:\n0. Forgot PIN`;
  }

  // PIN validated. Apply force-change-PIN gate before main menu.
  const m_raw = parts.slice(pinIndex + 1);
  const gate = await gateForceChangePin(m_raw, agent, 'agents');
  if (gate.needsGate) return gate.response;
  const m = gate.menuParts;
  const depth = m.length;

  // Main menu — 4 items (at the spec max)
  if (depth === 0) return `CON Working for: ${agent.aggregator_name}\n1. Log Collection\n2. Record Payment\n3. Register Collector\n4. My Stats\n0. Exit`;

  // ── Exit ──
  if (m[0] === '0') return `END Thank you, ${agent.first_name}!\nWorking for: ${agent.aggregator_name}\n\nQuestions? Call your\naggregator: ${agent.aggregator_phone}`;

  // ── Log Collection ──
  if (m[0] === '1') return await handleAgentCollection(m.slice(1), agent);

  // ── Record Payment ──
  if (m[0] === '2') return await handleAgentPayment(m.slice(1), agent);

  // ── Register Collector ──
  if (m[0] === '3') return await handleAgentRegister(m.slice(1), agent, null);

  // ── My Stats (with rating sub-menu) ──
  if (m[0] === '4') {
    if (m.length === 1) {
      const [todayStats, weekStats, regCount] = await Promise.all([
        pool.query(
          `SELECT COUNT(*) as count, COALESCE(SUM(pt.gross_weight_kg), 0) as kg
           FROM agent_activity aa
           JOIN pending_transactions pt ON aa.related_id = pt.id AND aa.related_type = 'transaction'
           WHERE aa.agent_id = $1 AND aa.action_type = 'collection'
           AND aa.created_at >= CURRENT_DATE`,
          [agent.id]
        ),
        pool.query(
          `SELECT COUNT(*) as count, COALESCE(SUM(pt.gross_weight_kg), 0) as kg
           FROM agent_activity aa
           JOIN pending_transactions pt ON aa.related_id = pt.id AND aa.related_type = 'transaction'
           WHERE aa.agent_id = $1 AND aa.action_type = 'collection'
           AND aa.created_at >= date_trunc('week', CURRENT_DATE)`,
          [agent.id]
        ),
        pool.query(
          `SELECT COUNT(*) as count FROM agent_activity WHERE agent_id = $1 AND action_type = 'registered_collector'`,
          [agent.id]
        )
      ]);
      const t = todayStats.rows[0], w = weekStats.rows[0], rc = regCount.rows[0];
      return `CON Stats, ${agent.first_name}:\nToday: ${t.count} coll / ${parseFloat(t.kg).toFixed(0)}kg\nWeek: ${w.count} coll / ${parseFloat(w.kg).toFixed(0)}kg\nRegistered: ${rc.count}\nFor: ${agent.aggregator_name}\n\n1. Rate a transaction\n0. Back`;
    }
    if (m[1] === '0') return `END Thank you, ${agent.first_name}!`;
    if (m[1] === '1') return await handleUssdRating(m.slice(2), 'agent', agent.id);
    return 'END Invalid option.\nDial again to retry.';
  }

  return 'END Invalid option.\nDial again to retry.';
}

async function handleAgentCollection(m, agent) {
  const depth = m.length;

  // depth 0: select collector (show list + phone lookup option)
  if (depth === 0) {
    const collectors = await pool.query(
      `SELECT c.id, c.first_name, c.last_name, c.phone,
              'COL-' || LPAD(c.id::text, 4, '0') AS display_code,
              COUNT(t.id) as txns
       FROM collectors c
       JOIN pending_transactions t ON t.collector_id = c.id
       WHERE t.aggregator_id = $1
       GROUP BY c.id
       ORDER BY MAX(t.created_at) DESC
       LIMIT 3`,
      [agent.aggregator_id]
    );
    if (!collectors.rows.length) {
      return 'CON No previous collectors.\nEnter collector phone\nnumber:';
    }
    let msg = 'CON Select collector:\n';
    collectors.rows.forEach(function(c, i) {
      var name = ((c.first_name || '') + ' ' + (c.last_name || '')).trim() || c.display_code;
      msg += (i + 1) + '. ' + name + '\n   ' + c.display_code + ' \u00b7 ' + c.txns + ' txn' + (parseInt(c.txns) > 1 ? 's' : '') + '\n';
    });
    msg += (collectors.rows.length + 1) + '. Enter phone number\n0. Cancel';
    return msg;
  }

  const resolved = await resolveCollectorForAgent(m, agent);
  if (resolved.response) return resolved.response;
  if (!resolved.collector) return 'END Error resolving collector.\nDial again to retry.';

  const collector = resolved.collector;
  const mp = resolved.menuParts;
  const mpDepth = mp.length;

  // Select material
  if (mpDepth === 0) return 'CON Select material:\n1. PET\n2. HDPE\n3. LDPE\n4. PP';

  const material = USSD_MATERIALS[mp[0]];
  if (!material) return 'END Invalid material.\nDial again to retry.';

  // Enter weight
  if (mpDepth === 1) return 'CON Enter weight in kg:';

  const weight = parseFloat(mp[1]);
  if (isNaN(weight) || weight <= 0 || weight > 9999) return 'END Invalid weight.\nDial again to retry.';

  // Enter price per kg
  if (mpDepth === 2) return 'CON Enter price per kg\n(GH\u20b5):';

  const price = parseFloat(mp[2]);
  if (isNaN(price) || price <= 0 || price > 999) return 'END Invalid price.\nDial again to retry.';

  // Confirm
  const total = (weight * price).toFixed(2);
  const collName = ((collector.first_name || '') + ' ' + (collector.last_name || '')).trim();
  const collCode = 'COL-' + String(collector.id).padStart(4, '0');

  if (mpDepth === 3) {
    return `CON Confirm collection:\n${weight}kg ${material}\nFrom: ${collName} (${collCode})\nPrice: GH\u20b5${price.toFixed(2)}/kg\nTotal: GH\u20b5${total}\n\n1. Confirm\n2. Cancel`;
  }

  // Execute
  if (mpDepth === 4) {
    if (mp[3] === '2') return 'END Cancelled.';
    if (mp[3] === '1') {
      const ussdClient = await pool.connect();
      let rootRow;
      try {
        await ussdClient.query('BEGIN');
        const { row } = await insertRootTransaction(ussdClient, {
          transaction_type: 'collector_sale',
          status: 'completed',
          collector_id: collector.id,
          aggregator_id: agent.aggregator_id,
          material_type: material,
          gross_weight_kg: weight,
          net_weight_kg: weight,
          price_per_kg: price,
          total_price: parseFloat(total),
          source: 'ussd'
        });
        await ussdClient.query('COMMIT');
        rootRow = row;
      } catch (e) {
        await ussdClient.query('ROLLBACK').catch(() => {});
        throw e;
      } finally {
        ussdClient.release();
      }
      const txnId = rootRow.id;
      const ref = 'TXN-' + new Date().toISOString().slice(0, 10).replace(/-/g, '') + '-' + String(txnId).padStart(4, '0');
      // Log to agent_activity
      await pool.query(
        `INSERT INTO agent_activity (agent_id, aggregator_id, action_type, description, related_id, related_type)
         VALUES ($1, $2, 'collection', $3, $4, 'transaction')`,
        [agent.id, agent.aggregator_id,
         `Logged ${weight} kg ${material} from collector ${collector.id} via USSD`,
         txnId]
      );
      try {
        if (collector.phone) {
          await notify(EVENTS.AGENT_COLLECTION, collector.phone, {
            qty: weight,
            material: material,
            aggregator_name: agent.aggregator_name,
            amount: total,
            ref: ref
          });
        }
      } catch (e) { console.warn('[NOTIFY] agent_collection failed:', e.message); }
      return `END COLLECTION LOGGED\nRef: ${ref}\n${weight}kg ${material}\nFrom: ${collName}\nPhone: ${collector.phone}\nCity: ${collector.city || ''}\nTotal: GH\u20b5${total}\n\nFor: ${agent.aggregator_name}`;
    }
  }

  return 'END Invalid option.\nDial again to retry.';
}

async function resolveCollectorForAgent(m, agent) {
  const collectors = await pool.query(
    `SELECT c.id, c.first_name, c.last_name, c.phone, c.city,
            COUNT(t.id) as txns
     FROM collectors c
     JOIN pending_transactions t ON t.collector_id = c.id
     WHERE t.aggregator_id = $1
     GROUP BY c.id
     ORDER BY MAX(t.created_at) DESC
     LIMIT 3`,
    [agent.aggregator_id]
  );

  const hasList = collectors.rows.length > 0;
  const phoneOptionIndex = hasList ? collectors.rows.length + 1 : null;

  // ── Empty state: m[0] is a phone number directly ──
  if (!hasList) {
    if (m.length === 0) return { response: null };

    const phoneVariants = getPhoneVariants(normalizeGhanaPhone(m[0]));
    const found = await pool.query(
      `SELECT id, first_name, last_name, phone, city FROM collectors WHERE phone=ANY($1) AND is_active=true LIMIT 1`,
      [phoneVariants]
    );
    if (!found.rows.length) {
      // Not registered — offer to register (difference from aggregator flow)
      if (m.length === 1) return { response: `CON No collector found\nwith that phone.\n\n1. Register them now\n0. Cancel` };
      if (m[1] === '0') return { response: 'END Cancelled.' };
      if (m[1] === '1') {
        // Redirect into register flow with phone pre-filled
        return { response: await handleAgentRegister(m.slice(2), agent, m[0]) };
      }
      return { response: 'END Invalid option.\nDial again to retry.' };
    }
    const coll = found.rows[0];
    const collName = ((coll.first_name || '') + ' ' + (coll.last_name || '')).trim();
    const collCode = 'COL-' + String(coll.id).padStart(4, '0');
    if (m.length === 1) {
      return { response: `CON Collector found:\n${collName} (${collCode})\n${coll.city || ''}\n\n1. Confirm \u2014 proceed\n2. Try different number\n0. Cancel` };
    }
    if (m[1] === '0') return { response: 'END Cancelled.' };
    if (m[1] === '2') return { response: 'CON Enter collector phone\nnumber:' };
    return { collector: coll, menuParts: m.slice(2) };
  }

  // ── Has list ──
  if (m[0] === '0') return { response: 'END Cancelled.' };

  const choice = parseInt(m[0]);

  // Phone lookup option
  if (choice === phoneOptionIndex) {
    if (m.length === 1) return { response: 'CON Enter collector phone\nnumber:' };
    const phoneVariants = getPhoneVariants(normalizeGhanaPhone(m[1]));
    const found = await pool.query(
      `SELECT id, first_name, last_name, phone, city FROM collectors WHERE phone=ANY($1) AND is_active=true LIMIT 1`,
      [phoneVariants]
    );
    if (!found.rows.length) {
      if (m.length === 2) return { response: `CON No collector found\nwith that phone.\n\n1. Register them now\n0. Cancel` };
      if (m[2] === '0') return { response: 'END Cancelled.' };
      if (m[2] === '1') {
        return { response: await handleAgentRegister(m.slice(3), agent, m[1]) };
      }
      return { response: 'END Invalid option.\nDial again to retry.' };
    }
    const coll = found.rows[0];
    const collName = ((coll.first_name || '') + ' ' + (coll.last_name || '')).trim();
    const collCode = 'COL-' + String(coll.id).padStart(4, '0');
    if (m.length === 2) {
      return { response: `CON Collector found:\n${collName} (${collCode})\n${coll.city || ''}\n\n1. Confirm \u2014 proceed\n2. Try different number\n0. Cancel` };
    }
    if (m[2] === '0') return { response: 'END Cancelled.' };
    if (m[2] === '2') return { response: 'CON Enter collector phone\nnumber:' };
    return { collector: coll, menuParts: m.slice(3) };
  }

  // List selection
  const selectedCollector = collectors.rows[choice - 1];
  if (!selectedCollector) return { response: 'END Invalid choice.\nDial again to retry.' };

  const fullColl = await pool.query(
    `SELECT id, first_name, last_name, phone, city FROM collectors WHERE id = $1`,
    [selectedCollector.id]
  );
  return { collector: fullColl.rows[0], menuParts: m.slice(1) };
}

async function handleAgentPayment(m, agent) {
  const depth = m.length;

  // depth 0: list unpaid collections (agent-specific via agent_activity)
  if (depth === 0) {
    const unpaid = await pool.query(
      `SELECT pt.id, pt.material_type, pt.gross_weight_kg, pt.total_price,
              c.first_name, c.last_name, c.phone,
              'COL-' || LPAD(c.id::text, 4, '0') AS collector_code
       FROM agent_activity aa
       JOIN pending_transactions pt ON aa.related_id = pt.id AND aa.related_type = 'transaction'
       JOIN collectors c ON pt.collector_id = c.id
       WHERE aa.agent_id = $1 AND aa.action_type = 'collection'
       AND pt.payment_status = 'unpaid'
       ORDER BY pt.created_at DESC
       LIMIT 3`,
      [agent.id]
    );
    if (!unpaid.rows.length) return 'END No unpaid collections.\n\nLog a collection first,\nthen come back to\nrecord the payment.';
    let msg = 'CON Unpaid collections:\n';
    unpaid.rows.forEach(function(u, i) {
      var name = ((u.first_name || '') + ' ' + (u.last_name || '')).trim();
      var shortName = name.length > 12 ? name.split(' ')[0] + ' ' + (name.split(' ')[1] || '').charAt(0) + '.' : name;
      msg += (i + 1) + '. ' + shortName + ' ' + parseFloat(u.gross_weight_kg).toFixed(0) + 'kg ' + u.material_type + '\n   GH\u20b5 ' + parseFloat(u.total_price).toFixed(2) + '\n';
    });
    msg += '0. Back';
    return msg;
  }

  if (m[0] === '0') return `CON Working for: ${agent.aggregator_name}\n1. Log Collection\n2. Record Payment\n3. Register Collector\n4. My Stats\n0. Exit`;

  // Re-fetch to get selected item
  const unpaid = await pool.query(
    `SELECT pt.id, pt.material_type, pt.gross_weight_kg, pt.total_price,
            c.id AS collector_id, c.first_name, c.last_name, c.phone, c.city,
            'COL-' || LPAD(c.id::text, 4, '0') AS collector_code
     FROM agent_activity aa
     JOIN pending_transactions pt ON aa.related_id = pt.id AND aa.related_type = 'transaction'
     JOIN collectors c ON pt.collector_id = c.id
     WHERE aa.agent_id = $1 AND aa.action_type = 'collection'
     AND pt.payment_status = 'unpaid'
     ORDER BY pt.created_at DESC
     LIMIT 3`,
    [agent.id]
  );

  const choiceIdx = parseInt(m[0]) - 1;
  const selected = unpaid.rows[choiceIdx];
  if (!selected) return 'END Invalid choice.\nDial again to retry.';

  const collName = ((selected.first_name || '') + ' ' + (selected.last_name || '')).trim();

  // depth 1: confirm payment
  if (depth === 1) {
    return `CON Pay collector:\n${collName} (${selected.collector_code})\n${parseFloat(selected.gross_weight_kg).toFixed(0)} kg ${selected.material_type}\nAmount: GH\u20b5 ${parseFloat(selected.total_price).toFixed(2)}\n\n1. Confirm payment\n2. Cancel`;
  }

  // depth 2: execute
  if (depth === 2) {
    if (m[1] === '2') return 'END Cancelled.';
    if (m[1] === '1') {
      // Phase 5B: agent cash payments are out-of-band (paid on the spot in cash).
      // No PAYMENT_SENT or PAYMENT_CONFIRMED SMS — collector already has the money,
      // and PAYMENT_CONFIRMED semantically targets a remote buyer, not the seller.
      await pool.query(
        `UPDATE pending_transactions
         SET payment_status = 'paid', payment_method = 'cash',
             payment_completed_at = NOW(), updated_at = NOW()
         WHERE id = $1 AND payment_status = 'unpaid'`,
        [selected.id]
      );
      await pool.query(
        `INSERT INTO agent_activity (agent_id, aggregator_id, action_type, description, related_id, related_type)
         VALUES ($1, $2, 'payment', $3, $4, 'transaction')`,
        [agent.id, agent.aggregator_id,
         `Paid GH\u20b5${parseFloat(selected.total_price).toFixed(2)} to ${collName} for ${parseFloat(selected.gross_weight_kg).toFixed(0)}kg ${selected.material_type}`,
         selected.id]
      );
      const ref = 'TXN-' + new Date().toISOString().slice(0, 10).replace(/-/g, '') + '-' + String(selected.id).padStart(4, '0');
      return `END Payment recorded!\nRef: ${ref}\n\nGH\u20b5 ${parseFloat(selected.total_price).toFixed(2)} paid to\n${collName}\nPhone: ${selected.phone}\n\nFor: ${agent.aggregator_name}`;
    }
  }

  return 'END Invalid option.\nDial again to retry.';
}

async function handleAgentRegister(m, agent, prefilledPhone) {
  const depth = m.length;

  // If phone is pre-filled (redirect from collection flow), skip phone entry
  // Flow: first_name > last_name > city > confirm > END
  // If no pre-fill: first_name > last_name > phone > city > confirm > END

  // depth 0: first name
  if (depth === 0) return 'CON Enter collector\'s\nfirst name:';

  const firstName = m[0];

  // depth 1: last name
  if (depth === 1) return 'CON Enter collector\'s\nlast name:';

  const lastName = m[1];

  if (prefilledPhone) {
    // Skip phone — go straight to city
    // depth 2: city
    if (depth === 2) return 'CON Select city:\n1. Accra\n2. Kumasi\n3. Tamale\n4. Takoradi';

    const cityData = USSD_CITIES[m[2]];
    if (!cityData) return 'END Invalid city.\nDial again to retry.';

    // depth 3: confirm
    if (depth === 3) {
      const phone = normalizeGhanaPhone(prefilledPhone);
      const displayPhone = phone && phone.startsWith('+233') ? '0' + phone.slice(4) : prefilledPhone;
      return `CON Register collector:\nName: ${firstName} ${lastName}\nPhone: ${displayPhone}\nCity: ${cityData.city}\n\n1. Confirm\n2. Cancel`;
    }

    // depth 4: execute
    if (depth === 4) {
      if (m[3] === '2') return 'END Cancelled.';
      if (m[3] === '1') {
        try {
          const hashedPin = await hashPassword('0000');
          const normalized = normalizeGhanaPhone(prefilledPhone);
          const phoneToStore = normalized && normalized.startsWith('+233') ? '0' + normalized.slice(4) : prefilledPhone;
          const result = await pool.query(
            `INSERT INTO collectors (first_name, last_name, phone, pin, city, region, must_change_pin)
             VALUES ($1, $2, $3, $4, $5, $6, true) RETURNING id`,
            [firstName.trim(), lastName.trim(), phoneToStore, hashedPin, cityData.city, cityData.region]
          );
          await pool.query(
            `INSERT INTO agent_activity (agent_id, aggregator_id, action_type, description, related_id, related_type)
             VALUES ($1, $2, 'registered_collector', $3, $4, 'collector')`,
            [agent.id, agent.aggregator_id,
             `Registered collector ${firstName} ${lastName} (${phoneToStore}) via USSD`,
             result.rows[0].id]
          );
          return `END Collector registered!\n\n${firstName} ${lastName}\nPhone: ${phoneToStore}\nDefault PIN: 0000\nAsk them to change\ntheir PIN on first use.\n\nFor: ${agent.aggregator_name}`;
        } catch (err) {
          if (err.code === '23505') return 'END This phone number is\nalready registered.\n\nUse Log Collection to\nrecord from existing\ncollectors.';
          throw err;
        }
      }
    }
    return 'END Invalid option.\nDial again to retry.';
  }

  // No pre-filled phone — full flow
  // depth 2: phone
  if (depth === 2) return 'CON Enter collector\'s\nphone number:';

  const phone = m[2];

  // depth 3: city
  if (depth === 3) return 'CON Select city:\n1. Accra\n2. Kumasi\n3. Tamale\n4. Takoradi';

  const cityData = USSD_CITIES[m[3]];
  if (!cityData) return 'END Invalid city.\nDial again to retry.';

  // depth 4: confirm
  if (depth === 4) {
    const normalized = normalizeGhanaPhone(phone);
    const displayPhone = normalized && normalized.startsWith('+233') ? '0' + normalized.slice(4) : phone;
    return `CON Register collector:\nName: ${firstName} ${lastName}\nPhone: ${displayPhone}\nCity: ${cityData.city}\n\n1. Confirm\n2. Cancel`;
  }

  // depth 5: execute
  if (depth === 5) {
    if (m[4] === '2') return 'END Cancelled.';
    if (m[4] === '1') {
      try {
        const hashedPin = await hashPassword('0000');
        const normalized = normalizeGhanaPhone(phone);
        const phoneToStore = normalized && normalized.startsWith('+233') ? '0' + normalized.slice(4) : phone;
        const result = await pool.query(
          `INSERT INTO collectors (first_name, last_name, phone, pin, city, region, must_change_pin)
           VALUES ($1, $2, $3, $4, $5, $6, true) RETURNING id`,
          [firstName.trim(), lastName.trim(), phoneToStore, hashedPin, cityData.city, cityData.region]
        );
        await pool.query(
          `INSERT INTO agent_activity (agent_id, aggregator_id, action_type, description, related_id, related_type)
           VALUES ($1, $2, 'registered_collector', $3, $4, 'collector')`,
          [agent.id, agent.aggregator_id,
           `Registered collector ${firstName} ${lastName} (${phoneToStore}) via USSD`,
           result.rows[0].id]
        );
        return `END Collector registered!\n\n${firstName} ${lastName}\nPhone: ${phoneToStore}\nDefault PIN: 0000\nAsk them to change\ntheir PIN on first use.\n\nFor: ${agent.aggregator_name}`;
      } catch (err) {
        if (err.code === '23505') return 'END This phone number is\nalready registered.\n\nUse Log Collection to\nrecord from existing\ncollectors.';
        throw err;
      }
    }
  }

  return 'END Invalid option.\nDial again to retry.';
}

async function handleCollectorSell(m, collector) {
  if (m.length === 0) return 'CON Sell My Material:\n1. Post New Listing\n2. My Listings\n3. My Offers\n0. Back';

  if (m[0] === '0') return 'CON 1. Log Drop-off\n2. Sell My Material\n3. Discovery\n4. My Stats\n0. Exit';
  if (m[0] === '1') return await handleCollectorPostListing(m.slice(1), collector);
  if (m[0] === '2') return await handleCollectorMyListings(m.slice(1), collector);
  if (m[0] === '3') return await handleCollectorMyOffers(m.slice(1), collector);

  return 'END Invalid option.\nDial again to retry.';
}

async function handleCollectorPostListing(m, collector) {
  const depth = m.length;

  if (depth === 0) return 'CON What material?\n1. PET\n2. HDPE\n3. LDPE\n4. PP';

  const material = USSD_MATERIALS[m[0]];
  if (!material) return 'END Invalid material.\nDial again to retry.';

  if (depth === 1) return `CON How many kg of ${material}\ndo you have to sell?\n\n(minimum 30 kg)\n\nEnter weight in kg:`;

  const qty = parseFloat(m[1]);
  if (isNaN(qty) || qty <= 0 || qty > 99999) return 'END Invalid quantity.\nDial again to retry.';
  if (qty < 30) return 'END Minimum listing is\n30 kg for collectors.\n\nCollect more material\nand try again.';

  if (depth === 2) return `CON Your asking price\nper kg? (GH\u20b5)\n\n(Enter 0 if open\nto offers)`;

  const priceInput = parseFloat(m[2]);
  if (isNaN(priceInput) || priceInput < 0 || priceInput > 999) return 'END Invalid price.\nDial again to retry.';
  const price = priceInput === 0 ? null : priceInput;
  const priceStr = price ? `GH\u20b5 ${price.toFixed(2)}/kg` : 'Open to offers';
  const location = collector.city || 'Ghana';

  if (depth === 3) {
    return `CON Confirm listing:\nMaterial: ${material}\nQuantity: ${qty} kg\nPrice: ${priceStr}\nLocation: ${location}\nExpires: 7 days\n\n1. Confirm\n2. Cancel`;
  }

  if (depth === 4) {
    if (m[3] === '2') return 'END Cancelled.';
    if (m[3] === '1') {
      await pool.query(
        `INSERT INTO listings (seller_id, seller_role, material_type, quantity_kg, original_qty_kg, price_per_kg, location, expires_at)
         VALUES ($1, 'collector', $2, $3, $3, $4, $5, NOW() + INTERVAL '7 days') RETURNING id`,
        [collector.id, material, qty, price, location]
      );
      return `END LISTING POSTED!\n\n${qty} kg ${material}${price ? ' at GH\u20b5 ' + price.toFixed(2) + '/kg' : ' (open to offers)'}\nLocation: ${location}\nExpires in 7 days.\n\nAggregators can now see\nyour listing. You'll be\nnotified when offers\ncome in.`;
    }
  }

  return 'END Invalid option.\nDial again to retry.';
}

async function handleCollectorMyListings(m, collector) {
  const { page, offset, remaining } = parsePaginatedSelection(m);

  const listings = await pool.query(
    `SELECT l.id, l.material_type, l.quantity_kg, l.price_per_kg, l.expires_at,
            (SELECT COUNT(*) FROM offers o WHERE o.listing_id = l.id AND o.status = 'pending') as pending_offers
     FROM listings l
     WHERE l.seller_id = $1 AND l.seller_role = 'collector' AND l.status = 'active'
     ORDER BY l.created_at DESC
     LIMIT 4 OFFSET $2`,
    [collector.id, offset]
  );

  if (!listings.rows.length && page === 0) return 'END No active listings.\n\nUse "Post New Listing"\nto advertise your\nmaterial to buyers.';
  if (!listings.rows.length) return 'END No more listings.';

  const hasMore = listings.rows.length > 3;
  const display = listings.rows.slice(0, 3);

  if (remaining.length === 0) {
    let msg = 'CON Your listings:\n';
    display.forEach(function(l, i) {
      const priceStr = l.price_per_kg ? 'GH\u20b5' + parseFloat(l.price_per_kg).toFixed(2) + '/kg' : '(open)';
      const daysLeft = Math.max(0, Math.ceil((new Date(l.expires_at) - new Date()) / 86400000));
      msg += (i + 1) + '. ' + parseFloat(l.quantity_kg).toFixed(0) + 'kg ' + l.material_type + ' ' + priceStr + '\n   Expires in ' + daysLeft + ' day' + (daysLeft !== 1 ? 's' : '') + '\n';
    });
    if (hasMore) msg += '4. More results \u2192\n';
    msg += '0. Back';
    return msg;
  }

  const sel = remaining[0];
  if (sel === '0') return 'CON Sell My Material:\n1. Post New Listing\n2. My Listings\n3. My Offers\n0. Back';

  const selIdx = parseInt(sel) - 1;
  const selected = display[selIdx];
  if (!selected) return 'END Invalid choice.\nDial again to retry.';

  const priceStr = selected.price_per_kg ? 'GH\u20b5' + parseFloat(selected.price_per_kg).toFixed(2) + '/kg' : '(open)';
  const daysLeft = Math.max(0, Math.ceil((new Date(selected.expires_at) - new Date()) / 86400000));

  if (remaining.length === 1) {
    return `CON ${parseFloat(selected.quantity_kg).toFixed(0)}kg ${selected.material_type} ${priceStr}\nLocation: ${collector.city || 'Ghana'}\nExpires in ${daysLeft} day${daysLeft !== 1 ? 's' : ''}\n${selected.pending_offers} pending offer${parseInt(selected.pending_offers) !== 1 ? 's' : ''}\n\n1. Renew (+7 days)\n2. Close listing\n0. Back`;
  }

  if (remaining.length === 2) {
    if (remaining[1] === '0') return 'CON Sell My Material:\n1. Post New Listing\n2. My Listings\n3. My Offers\n0. Back';
    if (remaining[1] === '1') {
      await pool.query(
        `UPDATE listings SET expires_at = expires_at + INTERVAL '7 days', renewal_count = renewal_count + 1, updated_at = NOW() WHERE id = $1`,
        [selected.id]
      );
      const newDays = daysLeft + 7;
      return `END Listing renewed!\n\n${parseFloat(selected.quantity_kg).toFixed(0)}kg ${selected.material_type} ${priceStr}\nNow expires in ${newDays} days.`;
    }
    if (remaining[1] === '2') {
      await pool.query(
        `UPDATE listings SET status = 'closed', updated_at = NOW() WHERE id = $1`,
        [selected.id]
      );
      await pool.query(
        `UPDATE offers SET status = 'rejected', responded_at = NOW() WHERE listing_id = $1 AND status = 'pending'`,
        [selected.id]
      );
      return `END Listing closed.\n\n${parseFloat(selected.quantity_kg).toFixed(0)}kg ${selected.material_type}\nPending offers declined.`;
    }
  }

  return 'END Invalid option.\nDial again to retry.';
}

async function handleCollectorMyOffers(m, collector) {
  const { page, offset, remaining } = parsePaginatedSelection(m);

  const offers = await pool.query(
    `SELECT o.id, o.price_per_kg, o.quantity_kg, o.status, o.buyer_id, o.buyer_role, o.listing_id,
            l.material_type,
            CASE WHEN o.buyer_role = 'aggregator' THEN (SELECT name FROM aggregators WHERE id = o.buyer_id) ELSE 'Buyer' END as buyer_name,
            'AGG-' || LPAD(o.buyer_id::text, 4, '0') as buyer_code
     FROM offers o
     JOIN listings l ON o.listing_id = l.id
     WHERE l.seller_id = $1 AND l.seller_role = 'collector' AND o.status = 'pending'
     ORDER BY o.created_at DESC
     LIMIT 4 OFFSET $2`,
    [collector.id, offset]
  );

  if (!offers.rows.length && page === 0) return 'END No pending offers.\n\nPost a listing to start\nreceiving offers from\naggregators.';
  if (!offers.rows.length) return 'END No more offers.';

  const hasMore = offers.rows.length > 3;
  const display = offers.rows.slice(0, 3);

  if (remaining.length === 0) {
    let msg = 'CON Offers received:\n';
    display.forEach(function(o, i) {
      const name = (o.buyer_name || o.buyer_code).length > 16 ? (o.buyer_name || o.buyer_code).substring(0, 15) + '.' : (o.buyer_name || o.buyer_code);
      msg += (i + 1) + '. ' + name + '\n   ' + parseFloat(o.quantity_kg).toFixed(0) + 'kg ' + o.material_type + ' @GH\u20b5' + parseFloat(o.price_per_kg).toFixed(2) + '/kg\n';
    });
    if (hasMore) msg += '4. More results \u2192\n';
    msg += '0. Back';
    return msg;
  }

  const sel = remaining[0];
  if (sel === '0') return 'CON Sell My Material:\n1. Post New Listing\n2. My Listings\n3. My Offers\n0. Back';

  const selIdx = parseInt(sel) - 1;
  const selected = display[selIdx];
  if (!selected) return 'END Invalid choice.\nDial again to retry.';

  const total = (parseFloat(selected.quantity_kg) * parseFloat(selected.price_per_kg)).toFixed(2);

  if (remaining.length === 1) {
    return `CON Offer from:\n${selected.buyer_name} (${selected.buyer_code})\nMaterial: ${selected.material_type}\nQty: ${parseFloat(selected.quantity_kg).toFixed(0)} kg\nOffer: GH\u20b5 ${parseFloat(selected.price_per_kg).toFixed(2)}/kg\nTotal: GH\u20b5 ${total}\n\n1. Accept offer\n2. Decline\n0. Back`;
  }

  if (remaining.length === 2) {
    if (remaining[1] === '0') return 'CON Sell My Material:\n1. Post New Listing\n2. My Listings\n3. My Offers\n0. Back';

    if (remaining[1] === '2') {
      await pool.query(`UPDATE offers SET status = 'rejected', responded_at = NOW() WHERE id = $1`, [selected.id]);
      return 'END Offer declined.\n\nYour listing remains\nactive for other buyers.';
    }

    if (remaining[1] === '1') {
      const listing = (await pool.query(`SELECT * FROM listings WHERE id = $1`, [selected.listing_id])).rows[0];
      if (!listing) return 'END Error: listing not found.\nDial again to retry.';

      const offerQty = parseFloat(selected.quantity_kg);
      if (offerQty > parseFloat(listing.quantity_kg)) return 'END Quantity no longer\navailable. Listing has\nbeen partially filled.';

      const client = await pool.connect();
      try {
        await client.query('BEGIN');
        await client.query(`UPDATE offers SET status = 'accepted', responded_at = NOW() WHERE id = $1`, [selected.id]);
        const remainingQty = parseFloat(listing.quantity_kg) - offerQty;
        if (remainingQty <= 0) {
          await client.query(`UPDATE listings SET quantity_kg = 0, status = 'closed', updated_at = NOW() WHERE id = $1`, [listing.id]);
        } else {
          await client.query(`UPDATE listings SET quantity_kg = $1, updated_at = NOW() WHERE id = $2`, [remainingQty, listing.id]);
        }

        // PR6-c: route through chain-of-custody helpers (matches the web
        // discovery accept at /api/offers/:id/accept). USSD-side: catch
        // InsufficientSourceError and surface as a USSD-friendly END message.
        const txnType = txnTypeForRoles(listing.seller_role, selected.buyer_role);
        const totalPrice = parseFloat((offerQty * parseFloat(selected.price_per_kg)).toFixed(2));
        const sellerCol = ptColForRole(listing.seller_role);
        const buyerCol  = ptColForRole(selected.buyer_role);
        const target = {
          transaction_type: txnType,
          status: 'pending',
          material_type: listing.material_type,
          gross_weight_kg: offerQty,
          price_per_kg: parseFloat(selected.price_per_kg),
          total_price: totalPrice,
          source: 'discovery'
        };
        if (sellerCol) target[sellerCol] = listing.seller_id;
        if (buyerCol && buyerCol !== sellerCol) target[buyerCol] = selected.buyer_id;

        if (COC_ROOT_TYPES[txnType]) {
          await insertRootTransaction(client, target);
        } else {
          await attributeAndInsert(client, target);
        }

        await client.query('COMMIT');
      } catch (txErr) {
        await client.query('ROLLBACK').catch(() => {});
        client.release();
        if (txErr instanceof InsufficientSourceError) {
          // USSD has no JSON envelope — return a short END message so the
          // dial-in user sees a clear failure instead of a generic 500.
          return 'END Insufficient inventory.\nNot enough source material\nfor this sale.\nDial again to retry.';
        }
        throw txErr;
      }
      client.release();

      let buyerPhone = '', buyerCity = '';
      if (selected.buyer_role === 'aggregator') {
        const agg = (await pool.query(`SELECT phone, city FROM aggregators WHERE id = $1`, [selected.buyer_id])).rows[0];
        if (agg) { buyerPhone = agg.phone || ''; buyerCity = agg.city || ''; }
      }

      return `END OFFER ACCEPTED!\n\n${parseFloat(selected.quantity_kg).toFixed(0)}kg ${selected.material_type} @ GH\u20b5 ${parseFloat(selected.price_per_kg).toFixed(2)}/kg\nTotal: GH\u20b5 ${total}\n\nContact buyer:\n${selected.buyer_name}\nPhone: ${buyerPhone}\nLocation: ${buyerCity}\n\nCall to arrange drop-off.\nTransaction logged.`;
    }
  }

  return 'END Invalid option.\nDial again to retry.';
}

async function handleCollectorDiscovery(m, collector) {
  if (m.length === 0) return 'CON Discovery:\n1. Browse Buyers\n2. Prices Near Me\n0. Back';

  if (m[0] === '0') return 'CON 1. Log Drop-off\n2. Sell My Material\n3. Discovery\n4. My Stats\n0. Exit';
  if (m[0] === '1') return await handleCollectorBrowseBuyers(m.slice(1), collector);

  if (m[0] === '2') {
    const city = collector.city || 'Accra';
    const prices = await pool.query(
      `SELECT DISTINCT ON (pp.material_type)
              pp.material_type, pp.price_per_kg_ghs, a.name
       FROM posted_prices pp
       JOIN aggregators a ON a.id = pp.poster_id
       WHERE pp.poster_type = 'aggregator' AND pp.is_active = true AND a.city = $1
       ORDER BY pp.material_type, pp.price_per_kg_ghs DESC`,
      [city]
    );
    if (!prices.rows.length) return `END No prices posted near ${city}.\nCheck back later.`;
    let msg = `END Prices Near Me (${city})\n\n`;
    prices.rows.forEach(function(p) {
      var name = (p.name || '').length > 10 ? p.name.substring(0, 9) + '.' : (p.name || '\u2014');
      msg += p.material_type.padEnd(6) + 'GH\u20b5' + parseFloat(p.price_per_kg_ghs).toFixed(2) + '/kg  ' + name + '\n';
    });
    msg += '\nDial again to log a drop-off.';
    return msg;
  }

  return 'END Invalid option.\nDial again to retry.';
}

async function handleCollectorBrowseBuyers(m, collector) {
  const depth = m.length;

  if (depth === 0) return 'CON Browse buyers for:\n1. PET\n2. HDPE\n3. LDPE\n4. PP';

  const material = USSD_MATERIALS[m[0]];
  if (!material) return 'END Invalid material.\nDial again to retry.';

  const city = collector.city || 'Accra';

  const subParts = m.slice(1);
  const { page, offset, remaining } = parsePaginatedSelection(subParts);

  const buyers = await pool.query(
    `SELECT o.id, o.target_quantity_kg, o.price_per_kg, o.buyer_id, o.buyer_role,
            a.name as buyer_name, a.phone as buyer_phone, a.city as buyer_city
     FROM orders o
     JOIN aggregators a ON o.buyer_id = a.id AND o.buyer_role = 'aggregator'
     WHERE o.material_type = $1 AND o.status = 'open' AND a.city = $2
     ORDER BY o.created_at DESC
     LIMIT 4 OFFSET $3`,
    [material, city, offset]
  );

  if (!buyers.rows.length && page === 0) return `END No ${material} buyers near\n${city} right now.\n\nTry another material or\ncheck back later.`;
  if (!buyers.rows.length) return 'END No more results.';

  const hasMore = buyers.rows.length > 3;
  const display = buyers.rows.slice(0, 3);

  if (remaining.length === 0) {
    let msg = `CON ${material} buyers (${city}):\n`;
    display.forEach(function(b, i) {
      const name = (b.buyer_name || '').length > 16 ? b.buyer_name.substring(0, 15) + '.' : (b.buyer_name || 'Buyer');
      const priceStr = b.price_per_kg && parseFloat(b.price_per_kg) > 0 ? '@GH\u20b5' + parseFloat(b.price_per_kg).toFixed(2) : '(open)';
      msg += (i + 1) + '. ' + name + '\n   Wants ' + parseFloat(b.target_quantity_kg).toFixed(0) + 'kg ' + priceStr + '\n';
    });
    if (hasMore) msg += '4. More results \u2192\n';
    msg += '0. Back';
    return msg;
  }

  const sel = remaining[0];
  if (sel === '0') return 'CON Browse buyers for:\n1. PET\n2. HDPE\n3. LDPE\n4. PP';

  const selIdx = parseInt(sel) - 1;
  const selected = display[selIdx];
  if (!selected) return 'END Invalid choice.\nDial again to retry.';

  const priceStr = selected.price_per_kg && parseFloat(selected.price_per_kg) > 0 ? 'GH\u20b5 ' + parseFloat(selected.price_per_kg).toFixed(2) + '/kg' : 'Open to negotiation';

  if (remaining.length === 1) {
    return `CON ${selected.buyer_name}\nWants: ${parseFloat(selected.target_quantity_kg).toFixed(0)}kg ${material}\nPrice: ${priceStr}\nLocation: ${selected.buyer_city || city}\n\n1. I have this material\n   (shares your phone)\n2. Not interested\n0. Back`;
  }

  if (remaining.length === 2) {
    if (remaining[1] === '0' || remaining[1] === '2') return `CON ${material} buyers (${city}):\n` + display.map(function(b, i) { return (i+1) + '. ' + (b.buyer_name || 'Buyer'); }).join('\n') + '\n0. Back';

    if (remaining[1] === '1') {
      return `END Interest sent!\n\nYour details shared with\n${selected.buyer_name}.\n\nBuyer contact:\nPhone: ${selected.buyer_phone || 'N/A'}\nLocation: ${selected.buyer_city || ''}\n\nThey may call you to\narrange a pickup.`;
    }
  }

  return 'END Invalid option.\nDial again to retry.';
}

async function handleAggregatorMarketplace(m, aggregator) {
  if (m.length === 0) return 'CON Marketplace:\n1. Browse Sellers\n2. Post Buy Request\n3. Sell to Processors\n4. My Offers\n0. Back';

  if (m[0] === '0') return 'CON 1. Log Purchase\n2. Pending Drop-offs\n3. Marketplace\n4. My Stats\n0. Exit';
  if (m[0] === '1') return await handleAggregatorBrowseSellers(m.slice(1), aggregator);
  if (m[0] === '2') return await handleAggregatorPostBuyRequest(m.slice(1), aggregator);
  if (m[0] === '3') return await handleAggregatorSellToProcessors(m.slice(1), aggregator);
  if (m[0] === '4') return await handleAggregatorMyOffers(m.slice(1), aggregator);

  return 'END Invalid option.\nDial again to retry.';
}

async function handleAggregatorBrowseSellers(m, aggregator) {
  const depth = m.length;

  if (depth === 0) return 'CON Browse sellers for:\n1. PET\n2. HDPE\n3. LDPE\n4. PP';

  const material = USSD_MATERIALS[m[0]];
  if (!material) return 'END Invalid material.\nDial again to retry.';

  const city = aggregator.city || 'Accra';
  const subParts = m.slice(1);
  const { page, offset, remaining } = parsePaginatedSelection(subParts);

  const listings = await pool.query(
    `SELECT l.id, l.quantity_kg, l.price_per_kg, l.expires_at, l.seller_id,
            'COL-' || LPAD(l.seller_id::text, 4, '0') as seller_code
     FROM listings l
     WHERE l.seller_role = 'collector' AND l.status = 'active' AND l.material_type = $1
     AND l.location = $2
     ORDER BY l.created_at DESC
     LIMIT 4 OFFSET $3`,
    [material, city, offset]
  );

  if (!listings.rows.length && page === 0) return `END No ${material} listings near\n${city} right now.\n\nTry another material or\ncheck back later.`;
  if (!listings.rows.length) return 'END No more results.';

  const hasMore = listings.rows.length > 3;
  const display = listings.rows.slice(0, 3);

  if (remaining.length === 0) {
    let msg = `CON ${material} sellers (${city}):\n`;
    display.forEach(function(l, i) {
      const priceStr = l.price_per_kg ? '@GH\u20b5' + parseFloat(l.price_per_kg).toFixed(2) + '/kg' : '(open price)';
      const daysLeft = Math.max(0, Math.ceil((new Date(l.expires_at) - new Date()) / 86400000));
      msg += (i + 1) + '. ' + l.seller_code + '\n   ' + parseFloat(l.quantity_kg).toFixed(0) + 'kg ' + priceStr + '\n   ' + daysLeft + ' day' + (daysLeft !== 1 ? 's' : '') + ' left\n';
    });
    if (hasMore) msg += '4. More results \u2192\n';
    msg += '0. Back';
    return msg;
  }

  const sel = remaining[0];
  if (sel === '0') return 'CON Marketplace:\n1. Browse Sellers\n2. Post Buy Request\n3. Sell to Processors\n4. My Offers\n0. Back';

  const selIdx = parseInt(sel) - 1;
  const selected = display[selIdx];
  if (!selected) return 'END Invalid choice.\nDial again to retry.';

  const listingPrice = selected.price_per_kg ? 'GH\u20b5 ' + parseFloat(selected.price_per_kg).toFixed(2) + '/kg' : 'open price';

  if (remaining.length === 1) {
    return `CON ${selected.seller_code} listing:\n${parseFloat(selected.quantity_kg).toFixed(0)}kg ${material} @ ${listingPrice}\nLocation: ${city}\n\nYour offer price/kg?\n(Enter 0 to match\ntheir asking price)\n\nGH\u20b5 per kg:`;
  }

  if (remaining.length === 2) {
    let offerPrice = parseFloat(remaining[1]);
    if (isNaN(offerPrice) || offerPrice < 0 || offerPrice > 999) return 'END Invalid price.\nDial again to retry.';
    if (offerPrice === 0) {
      if (selected.price_per_kg && parseFloat(selected.price_per_kg) > 0) {
        offerPrice = parseFloat(selected.price_per_kg);
      } else {
        return 'END Listing has no asking\nprice. Enter your offer\nprice.\n\nDial again to retry.';
      }
    }
    const qty = parseFloat(selected.quantity_kg);
    const total = (qty * offerPrice).toFixed(2);
    return `CON Confirm offer:\nTo: ${selected.seller_code}\nMaterial: ${material}, ${qty.toFixed(0)} kg\nYour offer: GH\u20b5 ${offerPrice.toFixed(2)}/kg\nTotal: GH\u20b5 ${total}\n\n1. Send offer\n2. Cancel`;
  }

  if (remaining.length === 3) {
    if (remaining[2] === '2') return 'END Cancelled.';
    if (remaining[2] === '1') {
      let offerPrice = parseFloat(remaining[1]);
      if (offerPrice === 0 && selected.price_per_kg) offerPrice = parseFloat(selected.price_per_kg);
      const qty = parseFloat(selected.quantity_kg);

      const existing = await pool.query(
        `SELECT id FROM offers WHERE listing_id = $1 AND buyer_id = $2 AND buyer_role = 'aggregator' AND status = 'pending'`,
        [selected.id, aggregator.id]
      );
      if (existing.rows.length) return 'END You already have a\npending offer on this\nlisting.\n\nCheck "My Offers" for\nstatus.';

      await pool.query(
        `INSERT INTO offers (listing_id, buyer_id, buyer_role, price_per_kg, quantity_kg, round, is_final, offered_by, status)
         VALUES ($1, $2, 'aggregator', $3, $4, 1, false, 'buyer', 'pending')`,
        [selected.id, aggregator.id, offerPrice, qty]
      );

      return `END OFFER SENT!\n\nGH\u20b5 ${offerPrice.toFixed(2)}/kg for ${qty.toFixed(0)}kg ${material}\nto ${selected.seller_code}.\n\nThe collector will be\nnotified. You'll receive\ntheir contact details\nwhen they accept.\n\nCheck "My Offers" for\nstatus updates.`;
    }
  }

  return 'END Invalid option.\nDial again to retry.';
}

async function handleAggregatorPostBuyRequest(m, aggregator) {
  const depth = m.length;

  if (depth === 0) return 'CON What material do\nyou need?\n1. PET\n2. HDPE\n3. LDPE\n4. PP';

  const material = USSD_MATERIALS[m[0]];
  if (!material) return 'END Invalid material.\nDial again to retry.';

  if (depth === 1) return `CON How many kg of ${material}\ndo you need?\n\nEnter quantity in kg:`;

  const qty = parseFloat(m[1]);
  if (isNaN(qty) || qty <= 0 || qty > 99999) return 'END Invalid quantity.\nDial again to retry.';

  if (depth === 2) return 'CON Your buying price\nper kg? (GH\u20b5)\n\n(Enter 0 if open\nto negotiation)';

  const priceInput = parseFloat(m[2]);
  if (isNaN(priceInput) || priceInput < 0 || priceInput > 999) return 'END Invalid price.\nDial again to retry.';
  const price = priceInput === 0 ? null : priceInput;
  const priceStr = price ? `GH\u20b5 ${price.toFixed(2)}/kg` : 'Open to negotiation';
  const location = aggregator.city || 'Ghana';

  if (depth === 3) {
    return `CON Confirm buy request:\nMaterial: ${material}\nQuantity: ${qty.toFixed(0)} kg\nPrice: ${priceStr}\nLocation: ${location}\n\n1. Confirm\n2. Cancel`;
  }

  if (depth === 4) {
    if (m[3] === '2') return 'END Cancelled.';
    if (m[3] === '1') {
      await createOrder({
        buyerId: aggregator.id,
        buyerRole: 'aggregator',
        material_type: material,
        target_quantity_kg: qty,
        price_per_kg: price || 0
      });
      return `END BUY REQUEST POSTED!\n\n${qty.toFixed(0)} kg ${material}${price ? ' at GH\u20b5 ' + price.toFixed(2) + '/kg' : ''}\nLocation: ${location}\n\nCollectors can now see\nyour request and respond.\nYou'll be notified when\nsomeone has material.`;
    }
  }

  return 'END Invalid option.\nDial again to retry.';
}

async function handleAggregatorSellToProcessors(m, aggregator) {
  const depth = m.length;

  if (depth === 0) return 'CON What material are\nyou selling?\n1. PET\n2. HDPE\n3. LDPE\n4. PP';

  const material = USSD_MATERIALS[m[0]];
  if (!material) return 'END Invalid material.\nDial again to retry.';

  if (depth === 1) return `CON How many kg of ${material}\ndo you have to sell?\n\n(minimum 500 kg)\n\nEnter weight in kg:`;

  const qty = parseFloat(m[1]);
  if (isNaN(qty) || qty <= 0 || qty > 99999) return 'END Invalid quantity.\nDial again to retry.';
  if (qty < 500) return 'END Minimum listing is\n500 kg for aggregators.\n\nConsolidate more material\nand try again.';

  if (depth === 2) return `CON Your asking price\nper kg? (GH\u20b5)\n\n(Enter 0 if open\nto offers)`;

  const priceInput = parseFloat(m[2]);
  if (isNaN(priceInput) || priceInput < 0 || priceInput > 999) return 'END Invalid price.\nDial again to retry.';
  const price = priceInput === 0 ? null : priceInput;
  const priceStr = price ? `GH\u20b5 ${price.toFixed(2)}/kg` : 'Open to offers';
  const location = aggregator.city || 'Ghana';

  if (depth === 3) {
    return `CON Confirm listing:\nMaterial: ${material}\nQuantity: ${qty.toFixed(0)} kg\nPrice: ${priceStr}\nLocation: ${location}\nExpires: 7 days\n\n1. Confirm\n2. Cancel`;
  }

  if (depth === 4) {
    if (m[3] === '2') return 'END Cancelled.';
    if (m[3] === '1') {
      await pool.query(
        `INSERT INTO listings (seller_id, seller_role, material_type, quantity_kg, original_qty_kg, price_per_kg, location, expires_at)
         VALUES ($1, 'aggregator', $2, $3, $3, $4, $5, NOW() + INTERVAL '7 days') RETURNING id`,
        [aggregator.id, material, qty, price, location]
      );
      return `END LISTING POSTED!\n\n${qty.toFixed(0)} kg ${material}${price ? ' at GH\u20b5 ' + price.toFixed(2) + '/kg' : ' (open to offers)'}\nLocation: ${location}\nExpires in 7 days.\n\nProcessors and recyclers\ncan now see your listing\nand make offers.`;
    }
  }

  return 'END Invalid option.\nDial again to retry.';
}

async function handleAggregatorMyOffers(m, aggregator) {
  const { page, offset, remaining } = parsePaginatedSelection(m);

  const sentOffers = await pool.query(
    `SELECT o.id, o.price_per_kg, o.quantity_kg, o.status, o.listing_id,
            l.material_type,
            'COL-' || LPAD(l.seller_id::text, 4, '0') as other_code,
            'sent' as direction
     FROM offers o
     JOIN listings l ON o.listing_id = l.id
     WHERE o.buyer_id = $1 AND o.buyer_role = 'aggregator'
     AND o.status IN ('pending', 'accepted')
     ORDER BY o.created_at DESC
     LIMIT 10`,
    [aggregator.id]
  );

  const recvOffers = await pool.query(
    `SELECT o.id, o.price_per_kg, o.quantity_kg, o.status, o.buyer_id, o.buyer_role, o.listing_id,
            l.material_type,
            CASE
              WHEN o.buyer_role = 'processor' THEN (SELECT name FROM processors WHERE id = o.buyer_id)
              WHEN o.buyer_role = 'recycler' THEN (SELECT name FROM recyclers WHERE id = o.buyer_id)
              WHEN o.buyer_role = 'converter' THEN (SELECT name FROM converters WHERE id = o.buyer_id)
              ELSE 'Buyer'
            END as other_name,
            'recv' as direction
     FROM offers o
     JOIN listings l ON o.listing_id = l.id
     WHERE l.seller_id = $1 AND l.seller_role = 'aggregator'
     AND o.status = 'pending'
     ORDER BY o.created_at DESC
     LIMIT 10`,
    [aggregator.id]
  );

  const allOffers = recvOffers.rows.concat(sentOffers.rows);
  const paginated = allOffers.slice(offset, offset + 4);

  if (!paginated.length && page === 0) return 'END No offers yet.\n\nBrowse seller listings\nto find material and\nmake offers.';
  if (!paginated.length) return 'END No more offers.';

  const hasMore = paginated.length > 3;
  const display = paginated.slice(0, 3);

  if (remaining.length === 0) {
    let msg = 'CON My offers:\n';
    display.forEach(function(o, i) {
      const dirLabel = o.direction === 'sent' ? 'SENT' : 'RECV';
      const code = o.other_code || (o.other_name || 'Buyer');
      const name = code.length > 12 ? code.substring(0, 11) + '.' : code;
      const statusStr = o.direction === 'recv' ? '\u2605 NEW' : o.status;
      msg += (i + 1) + '. ' + dirLabel + ' ' + name + '\n   ' + parseFloat(o.quantity_kg).toFixed(0) + 'kg ' + o.material_type + ' ' + statusStr + '\n';
    });
    if (hasMore) msg += '4. More results \u2192\n';
    msg += '0. Back';
    return msg;
  }

  const sel = remaining[0];
  if (sel === '0') return 'CON Marketplace:\n1. Browse Sellers\n2. Post Buy Request\n3. Sell to Processors\n4. My Offers\n0. Back';

  const selIdx = parseInt(sel) - 1;
  const selected = display[selIdx];
  if (!selected) return 'END Invalid choice.\nDial again to retry.';

  if (selected.direction === 'sent') {
    const statusMsg = selected.status === 'accepted' ? 'ACCEPTED!' : selected.status === 'pending' ? 'Pending' : selected.status;
    let msg = `END Your offer to ${selected.other_code}:\n${parseFloat(selected.quantity_kg).toFixed(0)}kg ${selected.material_type} @ GH\u20b5 ${parseFloat(selected.price_per_kg).toFixed(2)}/kg\nStatus: ${statusMsg}`;
    if (selected.status === 'accepted') {
      const sellerId = parseInt(selected.other_code.replace('COL-', ''));
      const coll = (await pool.query(`SELECT first_name, last_name, phone, city FROM collectors WHERE id = $1`, [sellerId])).rows[0];
      if (coll) {
        const collName = ((coll.first_name || '') + ' ' + (coll.last_name || '')).trim();
        msg += `\n\nContact seller:\n${collName}\nPhone: ${coll.phone}\nLocation: ${coll.city || ''}`;
      }
    } else {
      msg += '\n\nYou\'ll be notified when\nthe collector responds.';
    }
    return msg;
  }

  const total = (parseFloat(selected.quantity_kg) * parseFloat(selected.price_per_kg)).toFixed(2);

  if (remaining.length === 1) {
    return `CON Offer from:\n${selected.other_name || 'Buyer'}\nMaterial: ${selected.material_type}, ${parseFloat(selected.quantity_kg).toFixed(0)} kg\nOffer: GH\u20b5 ${parseFloat(selected.price_per_kg).toFixed(2)}/kg\nTotal: GH\u20b5 ${total}\n\n1. Accept offer\n2. Decline\n0. Back`;
  }

  if (remaining.length === 2) {
    if (remaining[1] === '0') return 'CON Marketplace:\n1. Browse Sellers\n2. Post Buy Request\n3. Sell to Processors\n4. My Offers\n0. Back';

    if (remaining[1] === '2') {
      await pool.query(`UPDATE offers SET status = 'rejected', responded_at = NOW() WHERE id = $1`, [selected.id]);
      return 'END Offer declined.\n\nYour listing remains\nactive for other buyers.';
    }

    if (remaining[1] === '1') {
      const listing = (await pool.query(`SELECT * FROM listings WHERE id = $1`, [selected.listing_id])).rows[0];
      if (!listing) return 'END Error: listing not found.\nDial again to retry.';
      const offerQty = parseFloat(selected.quantity_kg);
      if (offerQty > parseFloat(listing.quantity_kg)) return 'END Quantity no longer\navailable.';

      const client = await pool.connect();
      try {
        await client.query('BEGIN');
        await client.query(`UPDATE offers SET status = 'accepted', responded_at = NOW() WHERE id = $1`, [selected.id]);
        const remainingQty = parseFloat(listing.quantity_kg) - offerQty;
        if (remainingQty <= 0) {
          await client.query(`UPDATE listings SET quantity_kg = 0, status = 'closed', updated_at = NOW() WHERE id = $1`, [listing.id]);
        } else {
          await client.query(`UPDATE listings SET quantity_kg = $1, updated_at = NOW() WHERE id = $2`, [remainingQty, listing.id]);
        }
        // PR6-c: route through chain-of-custody helpers (matches the web
        // discovery accept). USSD-side: catch InsufficientSourceError and
        // surface as a USSD-friendly END message.
        const txnType = txnTypeForRoles(listing.seller_role, selected.buyer_role);
        const totalPrice = parseFloat((offerQty * parseFloat(selected.price_per_kg)).toFixed(2));
        const sellerCol = ptColForRole(listing.seller_role);
        const buyerCol  = ptColForRole(selected.buyer_role);
        const target = {
          transaction_type: txnType,
          status: 'pending',
          material_type: listing.material_type,
          gross_weight_kg: offerQty,
          price_per_kg: parseFloat(selected.price_per_kg),
          total_price: totalPrice,
          source: 'discovery'
        };
        if (sellerCol) target[sellerCol] = listing.seller_id;
        if (buyerCol && buyerCol !== sellerCol) target[buyerCol] = selected.buyer_id;

        if (COC_ROOT_TYPES[txnType]) {
          await insertRootTransaction(client, target);
        } else {
          await attributeAndInsert(client, target);
        }
        await client.query('COMMIT');
      } catch (txErr) {
        await client.query('ROLLBACK').catch(() => {});
        client.release();
        if (txErr instanceof InsufficientSourceError) {
          return 'END Insufficient inventory.\nNot enough source material\nfor this sale.\nDial again to retry.';
        }
        throw txErr;
      }
      client.release();

      let buyerPhone = '', buyerLocation = '';
      const buyerTable = selected.buyer_role === 'processor' ? 'processors' : selected.buyer_role === 'recycler' ? 'recyclers' : 'converters';
      try {
        const buyer = (await pool.query(`SELECT phone, city FROM ${buyerTable} WHERE id = $1`, [selected.buyer_id])).rows[0];
        if (buyer) { buyerPhone = buyer.phone || ''; buyerLocation = buyer.city || ''; }
      } catch (_) {}

      return `END OFFER ACCEPTED!\n\n${parseFloat(selected.quantity_kg).toFixed(0)}kg ${selected.material_type} @ GH\u20b5 ${parseFloat(selected.price_per_kg).toFixed(2)}/kg\nTotal: GH\u20b5 ${total}\n\nContact buyer:\n${selected.other_name || 'Buyer'}\nPhone: ${buyerPhone}\nLocation: ${buyerLocation}\n\nTransaction logged.`;
    }
  }

  return 'END Invalid option.\nDial again to retry.';
}

app.post('/api/ussd', async (req, res) => {
  const { sessionId, serviceCode, phoneNumber, text } = req.body;
  const phone = normalizeGhanaPhone(phoneNumber);
  const parts = text ? text.split('*') : [];
  let response = '', collectorId = null, aggregatorId = null, agentId = null;
  try {
    const phoneVariants = getPhoneVariants(phone);

    // ── Lockout check (wrong-PIN 30-min or wrong-OTP) ──
    const lockoutRow = await pool.query(
      `SELECT locked_until FROM user_lockouts
       WHERE phone=ANY($1) AND locked_until > NOW()
       ORDER BY locked_until DESC LIMIT 1`,
      [phoneVariants]
    );
    if (lockoutRow.rows.length) {
      const until = new Date(lockoutRow.rows[0].locked_until);
      const mins = Math.max(1, Math.ceil((until.getTime() - Date.now()) / 60000));
      response = 'END Account locked. Try again in ' + mins + ' min.';
    } else {
      // ── Active forgot-PIN check (takes precedence over normal login) ──
      const activeResetRow = await pool.query(
        `SELECT * FROM pin_reset_codes
         WHERE phone=ANY($1) AND used_at IS NULL AND expires_at > NOW()
         ORDER BY created_at DESC LIMIT 1`,
        [phoneVariants]
      );
      if (activeResetRow.rows.length) {
        const row = activeResetRow.rows[0];
        response = await handleForgotPinUssd(parts, row);
        if (row.user_type === 'collector') collectorId = row.user_id;
        else if (row.user_type === 'aggregator') aggregatorId = row.user_id;
        else if (row.user_type === 'agent') agentId = row.user_id;
      } else {
        // ── Active aggregator-registration code check (precedence over welcome) ──
        const activeAggReg = await pool.query(
          `SELECT * FROM aggregator_registration_requests
           WHERE phone=ANY($1) AND status = 'code_issued' AND code_expires_at > NOW()
           ORDER BY created_at DESC LIMIT 1`,
          [phoneVariants]
        );
        if (activeAggReg.rows.length) {
          response = await handleAggregatorRegistrationCode(parts, activeAggReg.rows[0]);
        } else {
        // 1. Check collectors first (largest USSD user group)
        const collResult = await pool.query(
          `SELECT id, first_name, last_name, phone, pin, city, must_change_pin FROM collectors WHERE phone=ANY($1) AND is_active=true LIMIT 1`,
          [phoneVariants]
        );
        if (collResult.rows.length) {
          collectorId = collResult.rows[0].id;
          response = await handleRegisteredUssd(parts, collResult.rows[0]);
        } else {
          // 2. Check aggregators
          const aggResult = await pool.query(
            `SELECT id, name, company, phone, pin, city, region, must_change_pin FROM aggregators WHERE phone=ANY($1) AND is_active=true LIMIT 1`,
            [phoneVariants]
          );
          if (aggResult.rows.length) {
            aggregatorId = aggResult.rows[0].id;
            response = await handleAggregatorUssd(parts, aggResult.rows[0]);
          } else {
            // 3. Check agents
            const agentResult = await pool.query(
              `SELECT a.id, a.aggregator_id, a.first_name, a.last_name, a.phone, a.pin, a.city, a.region, a.must_change_pin,
                      agg.name AS aggregator_name, agg.phone AS aggregator_phone
               FROM agents a
               JOIN aggregators agg ON agg.id = a.aggregator_id
               WHERE a.phone=ANY($1) AND a.is_active=true LIMIT 1`,
              [phoneVariants]
            );
            if (agentResult.rows.length) {
              agentId = agentResult.rows[0].id;
              response = await handleAgentUssd(parts, agentResult.rows[0]);
            } else {
              // 4. Unregistered — collector self-registration or aggregator request
              response = await handleUnregisteredUssd(parts, phone);
            }
          }
        }
        }  // close active-aggregator-registration else
      }
    }
  } catch (err) { console.error('[USSD] Error:', err); response = 'END System error. Try again later.'; }

  // Log session with role-specific ID
  try {
    await pool.query(
      `INSERT INTO ussd_sessions (session_id, phone, service_code, collector_id, aggregator_id, agent_id, text_input, response)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
      [sessionId, phone, serviceCode, collectorId, aggregatorId, agentId, text||'', response]
    );
  } catch (logErr) { console.error('[USSD] Log error:', logErr); }

  res.set('Content-Type', 'text/plain');
  res.send(response);
});

app.get('/api/ussd/stats', async (req, res) => {
  try {
    const stats = await pool.query(`SELECT COUNT(DISTINCT session_id) as total_sessions, COUNT(DISTINCT phone) as unique_phones, COUNT(*) FILTER (WHERE response LIKE 'END Logged!%') as successful_logs, COUNT(*) FILTER (WHERE response LIKE 'END Registered!%') as registrations, COUNT(*) FILTER (WHERE created_at>=CURRENT_DATE) as today_sessions FROM ussd_sessions`);
    const recentSessions = await pool.query(`SELECT session_id, phone, text_input, response, created_at FROM ussd_sessions ORDER BY created_at DESC LIMIT 20`);
    res.json({ success: true, stats: stats.rows[0], recent: recentSessions.rows });
  } catch (err) { res.status(500).json({ success: false, message: 'Server error' }); }
});

// ============================================
// PENDING TRANSACTIONS
// ============================================

app.post('/api/pending-transactions', async (req, res) => {
  try {
    const b = req.body;
    const transaction_type = b.transaction_type;
    if (!transaction_type) return res.status(400).json({ success: false, message: 'transaction_type is required' });

    // Map param names to correct pending_transactions column names
    const collectorId = b.collector_id || null;
    const aggId = b.aggregator_id || b.aggregator_id || null;
    const procId = b.processor_id || b.processor_id || null;
    const convId = b.converter_id || b.converter_id || null;
    const recyclerId = b.recycler_id || null;
    const material_type = b.material_type;
    const price_per_kg = b.price_per_kg;

    // Common validations
    if (!material_type) return res.status(400).json({ success: false, message: 'material_type is required' });
    const validMaterials = ['PET','HDPE','LDPE','PP'];
    if (!validMaterials.includes(material_type.toUpperCase())) return res.status(400).json({ success: false, message: 'material_type must be one of PET, HDPE, LDPE, PP' });
    if (!b.gross_weight_kg) return res.status(400).json({ success: false, message: 'gross_weight_kg is required' });
    const kg = parseFloat(b.gross_weight_kg);
    if (isNaN(kg) || kg <= 0) return res.status(400).json({ success: false, message: 'gross_weight_kg must be > 0' });

    // Per-type validation. Buyer-FK polymorphism is delegated to
    // validateBuyerFks (shared/transaction-parties.js) — single source of
    // truth with resolveBuyer. Seller-FK checks stay inline.
    if (transaction_type === 'collector_sale' || transaction_type === 'aggregator_purchase') {
      if (!collectorId) return res.status(400).json({ success: false, message: 'collector_id is required for ' + transaction_type });
      if (!aggId) return res.status(400).json({ success: false, message: 'aggregator_id is required for ' + transaction_type });
    } else if (transaction_type === 'aggregator_sale') {
      if (!aggId) return res.status(400).json({ success: false, message: 'aggregator_id is required for aggregator_sale' });
      const buyerCheck = validateBuyerFks(transaction_type, { processor_id: procId, converter_id: convId, recycler_id: recyclerId });
      if (!buyerCheck.ok) return res.status(400).json({ success: false, message: buyerCheck.message });
    } else if (transaction_type === 'processor_sale') {
      if (!procId) return res.status(400).json({ success: false, message: 'processor_id is required for processor_sale' });
      const buyerCheck = validateBuyerFks(transaction_type, { converter_id: convId, recycler_id: recyclerId });
      if (!buyerCheck.ok) return res.status(400).json({ success: false, message: buyerCheck.message });
    } else if (transaction_type === 'recycler_sale') {
      if (!recyclerId) return res.status(400).json({ success: false, message: 'recycler_id is required for recycler_sale' });
      if (!convId) return res.status(400).json({ success: false, message: 'converter_id is required for recycler_sale' });
    }

    // Resolve price
    let pricePer = null;
    if (price_per_kg !== undefined && price_per_kg !== null && price_per_kg !== '') {
      pricePer = parseFloat(price_per_kg);
    } else if (aggId) {
      const postedResult = await pool.query(`SELECT price_per_kg_ghs FROM posted_prices WHERE poster_type='aggregator' AND poster_id=$1 AND material_type=$2 AND is_active=true ORDER BY posted_at DESC LIMIT 1`, [aggId, material_type.toUpperCase()]);
      pricePer = postedResult.rows.length ? parseFloat(postedResult.rows[0].price_per_kg_ghs) : null;
    }
    const totalPrice = pricePer !== null ? parseFloat((kg * pricePer).toFixed(2)) : null;

    // Branch on transaction_type: root types (collector_sale / aggregator_purchase)
    // use insertRootTransaction; downstream types go through attributeAndInsert
    // (FIFO attribution + mass-balance enforcement).
    const isRoot = (transaction_type === 'collector_sale' || transaction_type === 'aggregator_purchase');
    const target = {
      transaction_type: transaction_type,
      collector_id: collectorId,
      aggregator_id: aggId,
      processor_id: procId,
      converter_id: convId,
      recycler_id: recyclerId,
      material_type: material_type.toUpperCase(),
      gross_weight_kg: kg,
      price_per_kg: pricePer != null ? pricePer : 0,
      total_price: totalPrice != null ? totalPrice : 0,
      notes: b.notes || null
    };
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      let responseBody;
      if (isRoot) {
        const { row } = await insertRootTransaction(client, target);
        responseBody = { success: true, pending_transaction: row };
      } else {
        const { row, sources } = await attributeAndInsert(client, target);
        responseBody = { success: true, pending_transaction: row, sources: sources };
      }
      await client.query('COMMIT');
      return res.status(201).json(responseBody);
    } catch (e) {
      await client.query('ROLLBACK').catch(() => {});
      if (handleInsufficientSource(res, e)) return;
      throw e;
    } finally {
      client.release();
    }
  } catch (err) { console.error('Create pending transaction error:', err); res.status(500).json({ success: false, message: 'Server error' }); }
});

app.get('/api/pending-transactions', async (req, res) => {
  try {
    var collector_id = req.query.collector_id;
    var aggregator_id = req.query.aggregator_id;
    var processor_id = req.query.processor_id || req.query.processor_id;
    var converter_id = req.query.converter_id || req.query.converter_id;
    var type = req.query.type;
    if (!collector_id && !aggregator_id && !processor_id && !converter_id) return res.status(400).json({ success: false, message: 'collector_id, aggregator_id, processor_id, or converter_id required' });
    let query, params;
    if (collector_id) {
      query = `SELECT pt.*, a.name AS aggregator_name, 'AGG-' || LPAD(a.id::text, 4, '0') AS aggregator_code FROM pending_transactions pt LEFT JOIN aggregators a ON a.id=pt.aggregator_id WHERE pt.collector_id=$1 AND pt.status='pending' ORDER BY pt.created_at DESC`;
      params = [collector_id];
    } else if (processor_id) {
      query = `SELECT pt.*, a.name AS aggregator_name, 'AGG-' || LPAD(a.id::text, 4, '0') AS aggregator_code FROM pending_transactions pt LEFT JOIN aggregators a ON a.id=pt.aggregator_id WHERE pt.processor_id=$1 AND pt.status='pending' AND pt.transaction_type='aggregator_sale' ORDER BY pt.created_at DESC`;
      params = [processor_id];
    } else if (type === 'aggregator_sale') {
      query = `SELECT pt.*, COALESCE(p.company, p.name) AS processor_name, p.company AS processor_company FROM pending_transactions pt LEFT JOIN processors p ON p.id=pt.processor_id WHERE pt.aggregator_id=$1 AND pt.status='pending' AND pt.transaction_type='aggregator_sale' ORDER BY pt.created_at DESC`;
      params = [aggregator_id];
    } else {
      query = `SELECT pt.*, c.first_name AS collector_first_name, c.last_name AS collector_last_name, 'COL-' || LPAD(c.id::text, 4, '0') AS collector_display_name FROM pending_transactions pt LEFT JOIN collectors c ON c.id=pt.collector_id WHERE pt.aggregator_id=$1 AND pt.status='pending' AND pt.transaction_type IN ('collector_sale','aggregator_purchase') ORDER BY pt.created_at DESC`;
      params = [aggregator_id];
    }
    const result = await pool.query(query, params);
    res.json({ success: true, pending_transactions: result.rows });
  } catch (err) { console.error('GET /api/pending-transactions error:', err.message); res.status(500).json({ success: false, message: 'Server error' }); }
});

app.get('/api/pending-transactions/collector-sales', requireAuth, async (req, res) => {
  try {
    const { collector_id } = req.query;
    if (req.user.id !== parseInt(collector_id)) return res.status(403).json({ success: false, message: 'Access denied' });
    if (!collector_id) return res.status(400).json({ success: false, message: 'collector_id required' });
    const result = await pool.query(`SELECT pt.*, a.name AS aggregator_name, a.company AS aggregator_company, t.price_per_kg AS final_price_per_kg, t.total_price AS final_total_price FROM pending_transactions pt LEFT JOIN aggregators a ON a.id=pt.aggregator_id LEFT JOIN transactions t ON t.id=pt.transaction_id WHERE pt.transaction_type='collector_sale' AND pt.collector_id=$1 ORDER BY pt.created_at DESC LIMIT 20`, [collector_id]);
    res.json({ success: true, pending_transactions: result.rows });
  } catch (err) { console.error('GET /api/pending-transactions/collector-sales error:', err.message); res.status(500).json({ success: false, message: 'Server error' }); }
});

app.get('/api/pending-transactions/aggregator-sales', requireAuth, async (req, res) => {
  try {
    var aggregator_id = req.query.aggregator_id;
    if (req.user.id !== parseInt(aggregator_id)) return res.status(403).json({ success: false, message: 'Access denied' });
    if (!aggregator_id) return res.status(400).json({ success: false, message: 'aggregator_id required' });
    const result = await pool.query(`SELECT pt.*, COALESCE(p.company, p.name) AS processor_company, COALESCE(p.company, p.name) AS processor_name, p.id AS processor_id, COALESCE(c.company, c.name) AS converter_company, c.name AS converter_name, c.id AS converter_id FROM pending_transactions pt LEFT JOIN processors p ON p.id=pt.processor_id LEFT JOIN converters c ON c.id=pt.converter_id WHERE pt.transaction_type='aggregator_sale' AND pt.aggregator_id=$1 ORDER BY pt.created_at DESC LIMIT 20`, [aggregator_id]);
    for (const row of result.rows) {
      if (row.processor_id) {
        var pCode = CirculRoles.circulCode('processor', row.processor_id);
        var pVis = await canSeeName('aggregator', parseInt(aggregator_id), 'processor', row.processor_id);
        row.processor_code = pCode; row.processor_name_visible = pVis;
      }
      if (row.converter_id) {
        var cCode = CirculRoles.circulCode('converter', row.converter_id);
        var cVis = await canSeeName('aggregator', parseInt(aggregator_id), 'converter', row.converter_id);
        row.converter_code = cCode; row.converter_name_visible = cVis;
      }
    }
    res.json({ success: true, pending_transactions: result.rows });
  } catch (err) { console.error('GET /api/pending-transactions/aggregator-sales error:', err.message); res.status(500).json({ success: false, message: 'Server error' }); }
});

app.get('/api/pending-transactions/aggregator-purchases', requireAuth, async (req, res) => {
  try {
    var aggregator_id = req.query.aggregator_id;
    if (req.user.id !== parseInt(aggregator_id)) return res.status(403).json({ success: false, message: 'Access denied' });
    if (!aggregator_id) return res.status(400).json({ success: false, message: 'aggregator_id required' });
    var result = await pool.query(`SELECT pt.*, c.first_name AS collector_first_name, c.last_name AS collector_last_name, 'COL-' || LPAD(c.id::text, 4, '0') AS collector_display_name FROM pending_transactions pt LEFT JOIN collectors c ON c.id=pt.collector_id WHERE pt.aggregator_id=$1 AND pt.collector_id IS NOT NULL AND pt.transaction_type IN ('collector_sale','aggregator_purchase') ORDER BY pt.created_at DESC LIMIT 20`, [aggregator_id]);
    res.json({ success: true, pending_transactions: result.rows });
  } catch (err) { console.error('GET /api/pending-transactions/aggregator-purchases error:', err.message); res.status(500).json({ success: false, message: 'Server error' }); }
});

app.patch('/api/pending-transactions/:id/review', requireAuth, async (req, res) => {
  try {
    const { id } = req.params;
    const { action, grade, grade_notes, rejection_reason, price_per_kg } = req.body;
    if (!action || !['accept','reject'].includes(action)) return res.status(400).json({ success: false, message: 'action must be "accept" or "reject"' });
    const ptResult = await pool.query(`SELECT * FROM pending_transactions WHERE id=$1`, [id]);
    if (!ptResult.rows.length) return res.status(404).json({ success: false, message: 'Pending transaction not found' });
    const pt = ptResult.rows[0];
    // Verify the reviewer is a party to this transaction
    const userId = req.user.id;
    const isParty = pt.collector_id === userId || pt.aggregator_id === userId || pt.processor_id === userId || pt.recycler_id === userId || pt.converter_id === userId;
    if (!isParty) return res.status(403).json({ success: false, message: 'Access denied' });
    if (pt.status !== 'pending') return res.status(409).json({ success: false, message: 'Transaction is no longer pending' });
    if (pt.transaction_type === 'aggregator_purchase') {
      if (action === 'reject') {
        if (!rejection_reason) return res.status(400).json({ success: false, message: 'rejection_reason is required' });
        const updated = await pool.query(`UPDATE pending_transactions SET status='rejected', rejection_reason=$1, updated_at=NOW() WHERE id=$2 RETURNING *`, [rejection_reason, id]);
        return res.json({ success: true, pending_transaction: updated.rows[0] });
      }
      const updated = await pool.query(`UPDATE pending_transactions SET status='confirmed', updated_at=NOW() WHERE id=$1 RETURNING *`, [id]);
      return res.json({ success: true, pending_transaction: updated.rows[0] });
    }
    if (pt.transaction_type !== 'collector_sale') return res.status(400).json({ success: false, message: 'Only collector_sale transactions can be reviewed this way' });
    if (action === 'reject') {
      if (!rejection_reason) return res.status(400).json({ success: false, message: 'rejection_reason is required' });
      const updated = await pool.query(`UPDATE pending_transactions SET status='rejected', rejected_at=NOW(), rejection_reason=$1, updated_at=NOW() WHERE id=$2 RETURNING *`, [rejection_reason, id]);
      return res.json({ success: true, pending_transaction: updated.rows[0] });
    }
    if (!grade || !['A','B','C'].includes(grade)) return res.status(400).json({ success: false, message: 'grade (A, B, or C) is required' });
    let basePricePerKg;
    if (price_per_kg !== undefined && price_per_kg !== null && !isNaN(parseFloat(price_per_kg))) {
      basePricePerKg = parseFloat(price_per_kg);
    } else {
      const postedResult = await pool.query(`SELECT price_per_kg_ghs FROM posted_prices WHERE poster_type='aggregator' AND poster_id=$1 AND material_type=$2 AND is_active=true ORDER BY posted_at DESC LIMIT 1`, [pt.aggregator_id, pt.material_type]);
      basePricePerKg = postedResult.rows.length ? parseFloat(postedResult.rows[0].price_per_kg_ghs) : parseFloat(pt.price_per_kg||0);
    }
    const multiplier = grade === 'A' ? 1.10 : grade === 'C' ? 0.75 : 1.0;
    const adjustedPrice = parseFloat((basePricePerKg * multiplier).toFixed(2));
    const totalPrice = parseFloat((adjustedPrice * parseFloat(pt.gross_weight_kg)).toFixed(2));
    const txnResult = await pool.query(`INSERT INTO transactions (collector_id, aggregator_id, material_type, gross_weight_kg, net_weight_kg, contamination_deduction_percent, price_per_kg, total_price, payment_status, notes) VALUES ($1,$2,$3,$4,$4,0,$5,$6,'unpaid',$7) RETURNING *`, [pt.collector_id, pt.aggregator_id, pt.material_type, pt.gross_weight_kg, adjustedPrice, totalPrice, 'grade:'+grade]);
    const updatedPt = await pool.query(`UPDATE pending_transactions SET status='confirmed', grade=$1, grade_notes=$2, transaction_id=$3, updated_at=NOW() WHERE id=$4 RETURNING *`, [grade, grade_notes||null, txnResult.rows[0].id, id]);
    return res.json({ success: true, pending_transaction: updatedPt.rows[0], transaction: txnResult.rows[0], final_price_per_kg: adjustedPrice });
  } catch (err) { console.error('Review pending transaction error:', err); res.status(500).json({ success: false, message: 'Server error' }); }
});

app.post('/api/pending-transactions/aggregator-purchase', requireAuth, async (req, res) => {
  try {
    const { aggregator_id, collector_id, material_type, gross_weight_kg, price_per_kg } = req.body;
    if (!aggregator_id || !collector_id || !material_type || !gross_weight_kg) return res.status(400).json({ success: false, message: 'aggregator_id, collector_id, material_type, and gross_weight_kg are required' });
    if (req.user.id !== parseInt(aggregator_id)) return res.status(403).json({ success: false, message: 'Access denied' });
    const kg = parseFloat(gross_weight_kg);
    if (isNaN(kg) || kg <= 0 || kg > 500) return res.status(400).json({ success: false, message: 'gross_weight_kg must be > 0 and at most 500 kg' });
    const aggCheck = await pool.query(`SELECT id FROM aggregators WHERE id=$1 AND is_active=true`, [aggregator_id]);
    if (!aggCheck.rows.length) return res.status(400).json({ success: false, message: 'Aggregator not found' });
    const collCheck = await pool.query(`SELECT id FROM collectors WHERE id=$1 AND is_active=true`, [collector_id]);
    if (!collCheck.rows.length) return res.status(400).json({ success: false, message: 'Collector not found' });
    const pricePer = price_per_kg ? parseFloat(price_per_kg) : 0;
    const totalPrice = parseFloat((kg * pricePer).toFixed(2));
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      const { row } = await insertRootTransaction(client, {
        transaction_type: 'aggregator_purchase',
        status: 'pending',
        collector_id: parseInt(collector_id),
        aggregator_id: parseInt(aggregator_id),
        material_type: material_type.toUpperCase(),
        gross_weight_kg: kg,
        price_per_kg: pricePer,
        total_price: totalPrice
      });
      await client.query('COMMIT');
      return res.status(201).json({ success: true, pending_transaction: row });
    } catch (e) {
      await client.query('ROLLBACK').catch(() => {});
      throw e;
    } finally {
      client.release();
    }
  } catch (err) { console.error('Aggregator purchase error:', err); res.status(500).json({ success: false, message: 'Server error' }); }
});

app.post('/api/pending-transactions/aggregator-sale', requireAuth, async (req, res) => {
  try {
    const { aggregator_id, processor_id, converter_id, recycler_id, material_type, gross_weight_kg, price_per_kg, notes, photo_urls, sources } = req.body;
    if (!aggregator_id) return res.status(400).json({ success: false, message: 'aggregator_id is required for aggregator_sale' });
    const buyerCheck = validateBuyerFks('aggregator_sale', { processor_id, converter_id, recycler_id });
    if (!buyerCheck.ok) return res.status(400).json({ success: false, message: buyerCheck.message });
    if (!material_type || !gross_weight_kg || !price_per_kg) return res.status(400).json({ success: false, message: 'material_type, gross_weight_kg, and price_per_kg are required' });
    if (req.user.id !== parseInt(aggregator_id)) return res.status(403).json({ success: false, message: 'Access denied' });
    const kg = parseFloat(gross_weight_kg);
    if (isNaN(kg) || kg <= 0 || kg > 4000) return res.status(400).json({ success: false, message: 'gross_weight_kg must be > 0 and at most 4000 kg' });
    const aggCheck = await pool.query(`SELECT id FROM aggregators WHERE id=$1 AND is_active=true`, [aggregator_id]);
    if (!aggCheck.rows.length) return res.status(400).json({ success: false, message: 'Aggregator not found' });
    let resolvedProcessorId = null, resolvedConverterId = null, resolvedRecyclerId = null;
    if (processor_id) { const pr = await pool.query(`SELECT id FROM processors WHERE id=$1 AND is_active=true`, [processor_id]); if (!pr.rows.length) return res.status(400).json({ success: false, message: 'Processor not found' }); resolvedProcessorId = parseInt(processor_id); }
    if (converter_id) { const cv = await pool.query(`SELECT id FROM converters WHERE id=$1 AND is_active=true`, [converter_id]); if (!cv.rows.length) return res.status(400).json({ success: false, message: 'Converter not found' }); resolvedConverterId = parseInt(converter_id); }
    if (recycler_id)  { const rc = await pool.query(`SELECT id FROM recyclers  WHERE id=$1 AND is_active=true`, [recycler_id]);  if (!rc.rows.length) return res.status(400).json({ success: false, message: 'Recycler not found' });  resolvedRecyclerId  = parseInt(recycler_id);  }
    const price = parseFloat(price_per_kg);
    const totalPrice = parseFloat((kg * price).toFixed(2));
    const photosRequired = kg > 500;
    const dispatchApproved = photosRequired ? false : true;
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      const { row, sources: attributedSources } = await attributeAndInsert(client, {
        transaction_type: 'aggregator_sale',
        aggregator_id: parseInt(aggregator_id),
        processor_id: resolvedProcessorId,
        converter_id: resolvedConverterId,
        recycler_id: resolvedRecyclerId,
        material_type: material_type.toUpperCase(),
        gross_weight_kg: kg,
        price_per_kg: price,
        total_price: totalPrice,
        photos_required: photosRequired,
        photos_submitted: false,
        photo_urls: photo_urls || [],
        dispatch_approved: dispatchApproved,
        notes: notes || null,
        sources: sources   // PR4-A: optional manual source-picker hint from req.body
      });
      await client.query('COMMIT');
      return res.status(201).json({ success: true, pending_transaction: row, sources: attributedSources, photos_required: photosRequired });
    } catch (e) {
      await client.query('ROLLBACK').catch(() => {});
      if (handleInsufficientSource(res, e)) return;
      throw e;
    } finally {
      client.release();
    }
  } catch (err) { console.error('Aggregator sale error:', err); res.status(500).json({ success: false, message: 'Server error' }); }
});

app.get('/api/pending-transactions/processor-queue', requireAuth, async (req, res) => {
  try {
    if (!req.user.hasRole('processor')) return res.status(403).json({ success: false, message: 'Processor access only' });
    const result = await pool.query(`SELECT pt.*, COALESCE(a.company, a.name) AS aggregator_name, a.company AS aggregator_company, 'AGG-' || LPAD(a.id::text, 4, '0') AS aggregator_code FROM pending_transactions pt LEFT JOIN aggregators a ON a.id=pt.aggregator_id WHERE pt.processor_id=$1 AND pt.transaction_type='aggregator_sale' ORDER BY pt.created_at DESC`, [req.user.id]);
    for (const row of result.rows) {
      if (row.aggregator_id) {
        row.aggregator_code = CirculRoles.circulCode('aggregator', row.aggregator_id);
        row.aggregator_name_visible = await canSeeName('processor', req.user.id, 'aggregator', row.aggregator_id);
      }
    }
    res.json({ success: true, pending_transactions: result.rows });
  } catch (err) { console.error('Processor queue error:', err); res.status(500).json({ success: false, message: 'Server error' }); }
});

app.post('/api/pending-transactions/:id/dispatch-decision', requireAuth, async (req, res) => {
  try {
    if (!req.user.hasRole('processor')) return res.status(403).json({ success: false, message: 'Processor access only' });
    const { id } = req.params;
    const { decision, rejection_reason, waive_photos } = req.body;
    if (!decision || !['approve','reject'].includes(decision)) return res.status(400).json({ success: false, message: 'decision must be "approve" or "reject"' });
    const ptResult = await pool.query(`SELECT * FROM pending_transactions WHERE id=$1`, [id]);
    if (!ptResult.rows.length) return res.status(404).json({ success: false, message: 'Pending transaction not found' });
    const pt = ptResult.rows[0];
    if (parseInt(pt.processor_id) !== parseInt(req.user.id)) return res.status(403).json({ success: false, message: 'Not authorised' });
    if (pt.status !== 'pending') return res.status(409).json({ success: false, message: 'Transaction is no longer pending' });
    if (decision === 'reject') {
      if (!rejection_reason?.trim()) return res.status(400).json({ success: false, message: 'Rejection reason is required' });
      const updated = await pool.query(`UPDATE pending_transactions SET status='dispatch_rejected', rejection_reason=$1, updated_at=NOW() WHERE id=$2 RETURNING *`, [rejection_reason.trim(), id]);
      return res.json({ success: true, pending_transaction: updated.rows[0] });
    }
    const photoUrls = pt.photo_urls || [];
    const noPhotos = !Array.isArray(photoUrls) || photoUrls.length === 0;
    if (parseFloat(pt.gross_weight_kg) > 500 && noPhotos && !waive_photos) return res.status(400).json({ success: false, message: 'Photos required for batches over 500 kg', error: 'photos_required' });
    const updated = await pool.query(`UPDATE pending_transactions SET status='dispatch_approved', dispatch_approved=true, dispatch_approved_at=NOW(), dispatch_approved_by_id=$1, dispatch_approved_by_type='processor', updated_at=NOW() WHERE id=$2 RETURNING *`, [req.user.id, id]);
    // Notify the sender (aggregator) that dispatch was approved
    try {
      const aggRow = (await pool.query(`SELECT phone, name FROM aggregators WHERE id = $1`, [pt.aggregator_id])).rows[0];
      const procRow = (await pool.query(`SELECT COALESCE(company, name) AS name FROM processors WHERE id = $1`, [req.user.id])).rows[0];
      if (aggRow && aggRow.phone) {
        notify(EVENTS.DELIVERY_APPROVED, aggRow.phone, { receiver_name: procRow ? procRow.name : 'the processor', qty: pt.gross_weight_kg, material: pt.material_type });
      }
    } catch (notifyErr) { console.warn('Notification error (delivery_approved):', notifyErr.message); }
    return res.json({ success: true, pending_transaction: updated.rows[0] });
  } catch (err) { console.error('Dispatch decision error:', err); res.status(500).json({ success: false, message: 'Server error' }); }
});

app.post('/api/pending-transactions/:id/arrival-confirmation', requireAuth, async (req, res) => {
  try {
    if (!req.user.hasRole('processor')) return res.status(403).json({ success: false, message: 'Processor access only' });
    const { id } = req.params;
    const { actual_weight_kg, grade, rejection_reason } = req.body;
    if (!actual_weight_kg || isNaN(parseFloat(actual_weight_kg)) || parseFloat(actual_weight_kg) <= 0) return res.status(400).json({ success: false, message: 'actual_weight_kg is required and must be positive' });
    if (!grade || !['A','B','C'].includes(grade)) return res.status(400).json({ success: false, message: 'grade must be A, B, or C' });
    const ptResult = await pool.query(`SELECT * FROM pending_transactions WHERE id=$1`, [id]);
    if (!ptResult.rows.length) return res.status(404).json({ success: false, message: 'Pending transaction not found' });
    const pt = ptResult.rows[0];
    if (parseInt(pt.processor_id) !== parseInt(req.user.id)) return res.status(403).json({ success: false, message: 'Not authorised' });
    if (pt.status !== 'dispatch_approved') return res.status(400).json({ success: false, message: 'Dispatch must be approved before logging arrival' });
    const kg = parseFloat(actual_weight_kg);
    const basePrice = parseFloat(pt.price_per_kg||0);
    const multiplier = grade === 'A' ? 1.10 : grade === 'C' ? 0.75 : 1.0;
    const finalPrice = parseFloat((basePrice * multiplier).toFixed(2));
    const totalPrice = parseFloat((finalPrice * kg).toFixed(2));
    const newStatus = grade === 'C' ? 'grade_c_flagged' : 'arrived';
    const updatedPt = await pool.query(`UPDATE pending_transactions SET status=$1, grade=$2, gross_weight_kg=$3, total_price=$4, rejection_reason=$5, updated_at=NOW() WHERE id=$6 RETURNING *`, [newStatus, grade, kg, totalPrice, rejection_reason||null, id]);
    res.json({ success: true, pending_transaction: updatedPt.rows[0] });
  } catch (err) { console.error('Arrival confirmation error:', err); res.status(500).json({ success: false, message: 'Server error' }); }
});

app.post('/api/pending-transactions/processor-sale', requireAuth, async (req, res) => {
  try {
    if (!req.user.hasRole('processor')) return res.status(403).json({ success: false, message: 'Processor access only' });
    const { converter_id, recycler_id, material_type, gross_weight_kg, price_per_kg, notes, sources } = req.body;
    if (!material_type || !gross_weight_kg || !price_per_kg) return res.status(400).json({ success: false, message: 'material_type, gross_weight_kg, price_per_kg required' });
    if (!converter_id && !recycler_id) return res.status(400).json({ success: false, message: 'Either converter_id or recycler_id is required' });
    if (converter_id && recycler_id) return res.status(400).json({ success: false, message: 'Provide converter_id or recycler_id, not both' });
    const kg = parseFloat(gross_weight_kg), price = parseFloat(price_per_kg);
    if (isNaN(kg) || kg <= 0) return res.status(400).json({ success: false, message: 'Invalid weight' });
    if (isNaN(price) || price <= 0) return res.status(400).json({ success: false, message: 'Invalid price' });
    if (recycler_id) {
      const recResult = await pool.query(`SELECT id FROM recyclers WHERE id=$1 AND is_active=true`, [recycler_id]);
      if (!recResult.rows.length) return res.status(400).json({ success: false, message: 'Recycler not found' });
    } else {
      const convResult = await pool.query(`SELECT id FROM converters WHERE id=$1 AND is_active=true`, [converter_id]);
      if (!convResult.rows.length) return res.status(400).json({ success: false, message: 'Converter not found' });
    }
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      const { row, sources: attributedSources } = await attributeAndInsert(client, {
        transaction_type: 'processor_sale',
        processor_id: parseInt(req.user.id),
        recycler_id: recycler_id ? parseInt(recycler_id) : null,
        converter_id: converter_id ? parseInt(converter_id) : null,
        material_type: material_type,
        gross_weight_kg: kg,
        price_per_kg: price,
        total_price: kg * price,
        photos_required: true,
        photos_submitted: false,
        photo_urls: [],
        notes: notes || null,
        sources: sources   // PR4-A: optional manual source-picker hint
      });
      await client.query('COMMIT');
      return res.status(201).json({ success: true, pending_transaction: row, sources: attributedSources });
    } catch (e) {
      await client.query('ROLLBACK').catch(() => {});
      if (handleInsufficientSource(res, e)) return;
      throw e;
    } finally {
      client.release();
    }
  } catch (err) { console.error('Processor sale error:', err); res.status(500).json({ success: false, message: 'Server error' }); }
});

app.get('/api/pending-transactions/processor-sales', requireAuth, async (req, res) => {
  try {
    if (!req.user.hasRole('processor')) return res.status(403).json({ success: false, message: 'Processor access only' });
    const result = await pool.query(`SELECT pt.*, c.name AS converter_name, c.company AS converter_company, c.id AS converter_id, r.name AS recycler_name, r.company AS recycler_company, r.id AS recycler_id FROM pending_transactions pt LEFT JOIN converters c ON c.id=pt.converter_id LEFT JOIN recyclers r ON r.id=pt.recycler_id WHERE pt.transaction_type='processor_sale' AND pt.processor_id=$1 ORDER BY pt.created_at DESC LIMIT 20`, [req.user.id]);
    for (const row of result.rows) {
      if (row.converter_id) {
        row.converter_code = CirculRoles.circulCode('converter', row.converter_id);
        row.converter_name_visible = await canSeeName('processor', req.user.id, 'converter', row.converter_id);
      }
      if (row.recycler_id) {
        row.recycler_code = CirculRoles.circulCode('recycler', row.recycler_id);
        row.recycler_name_visible = await canSeeName('processor', req.user.id, 'recycler', row.recycler_id);
      }
    }
    res.json({ success: true, pending_transactions: result.rows });
  } catch (err) { console.error('Get processor sales error:', err); res.status(500).json({ success: false, message: 'Server error' }); }
});

// ── Recycler inbound queue (processor_sale → recycler) ──

app.get('/api/pending-transactions/recycler-queue', requireAuth, async (req, res) => {
  try {
    if (!req.user.hasRole('recycler')) return res.status(403).json({ success: false, message: 'Recycler access only' });
    const result = await pool.query(`SELECT pt.*, COALESCE(p.company, p.name) AS processor_name, p.company AS processor_company FROM pending_transactions pt LEFT JOIN processors p ON p.id=pt.processor_id WHERE pt.transaction_type='processor_sale' AND pt.recycler_id=$1 ORDER BY pt.created_at DESC`, [req.user.id]);
    for (const row of result.rows) {
      if (row.processor_id) {
        row.processor_code = CirculRoles.circulCode('processor', row.processor_id);
        row.processor_name_visible = await canSeeName('recycler', req.user.id, 'processor', row.processor_id);
      }
    }
    res.json({ success: true, pending_transactions: result.rows });
  } catch (err) { console.error('Recycler queue error:', err); res.status(500).json({ success: false, message: 'Server error' }); }
});

app.post('/api/pending-transactions/:id/recycler-dispatch-decision', requireAuth, async (req, res) => {
  try {
    if (!req.user.hasRole('recycler')) return res.status(403).json({ success: false, message: 'Recycler access only' });
    const { id } = req.params;
    const { decision, rejection_reason } = req.body;
    if (!decision || !['approve','reject'].includes(decision)) return res.status(400).json({ success: false, message: 'decision must be approve or reject' });
    const ptResult = await pool.query(`SELECT * FROM pending_transactions WHERE id=$1`, [id]);
    if (!ptResult.rows.length) return res.status(404).json({ success: false, message: 'Not found' });
    const pt = ptResult.rows[0];
    if (parseInt(pt.recycler_id) !== parseInt(req.user.id)) return res.status(403).json({ success: false, message: 'Not your delivery' });
    if (pt.status !== 'pending') return res.status(400).json({ success: false, message: 'Delivery is not in pending status' });
    if (decision === 'reject') {
      if (!rejection_reason?.trim()) return res.status(400).json({ success: false, message: 'rejection_reason required' });
      const updated = await pool.query(`UPDATE pending_transactions SET status='dispatch_rejected', rejection_reason=$1, updated_at=NOW() WHERE id=$2 RETURNING *`, [rejection_reason.trim(), id]);
      return res.json({ success: true, pending_transaction: updated.rows[0] });
    }
    const updated = await pool.query(`UPDATE pending_transactions SET status='dispatch_approved', updated_at=NOW() WHERE id=$1 RETURNING *`, [id]);
    // Notify the sender (processor) that dispatch was approved
    try {
      const procRow = (await pool.query(`SELECT phone, COALESCE(company, name) AS name FROM processors WHERE id = $1`, [pt.processor_id])).rows[0];
      const recRow = (await pool.query(`SELECT name FROM recyclers WHERE id = $1`, [req.user.id])).rows[0];
      if (procRow && procRow.phone) {
        notify(EVENTS.DELIVERY_APPROVED, procRow.phone, { receiver_name: recRow ? recRow.name : 'the recycler', qty: pt.gross_weight_kg, material: pt.material_type });
      }
    } catch (notifyErr) { console.warn('Notification error (delivery_approved):', notifyErr.message); }
    res.json({ success: true, pending_transaction: updated.rows[0] });
  } catch (err) { console.error('Recycler dispatch decision error:', err); res.status(500).json({ success: false, message: 'Server error' }); }
});

app.post('/api/pending-transactions/:id/recycler-arrival', requireAuth, async (req, res) => {
  try {
    if (!req.user.hasRole('recycler')) return res.status(403).json({ success: false, message: 'Recycler access only' });
    const { id } = req.params;
    const { actual_weight_kg, grade, rejection_reason } = req.body;
    if (!actual_weight_kg || isNaN(parseFloat(actual_weight_kg)) || parseFloat(actual_weight_kg) <= 0) return res.status(400).json({ success: false, message: 'actual_weight_kg is required and must be positive' });
    if (!grade || !['A','B','C'].includes(grade)) return res.status(400).json({ success: false, message: 'grade must be A, B, or C' });
    const ptResult = await pool.query(`SELECT * FROM pending_transactions WHERE id=$1`, [id]);
    if (!ptResult.rows.length) return res.status(404).json({ success: false, message: 'Not found' });
    const pt = ptResult.rows[0];
    if (parseInt(pt.recycler_id) !== parseInt(req.user.id)) return res.status(403).json({ success: false, message: 'Not your delivery' });
    if (pt.status !== 'dispatch_approved') return res.status(400).json({ success: false, message: 'Dispatch must be approved before logging arrival' });
    const kg = parseFloat(actual_weight_kg);
    const basePrice = parseFloat(pt.price_per_kg||0);
    const multiplier = grade === 'A' ? 1.10 : grade === 'C' ? 0.75 : 1.0;
    const finalPrice = parseFloat((basePrice * multiplier).toFixed(2));
    const totalPrice = parseFloat((finalPrice * kg).toFixed(2));
    const newStatus = grade === 'C' ? 'grade_c_flagged' : 'arrived';
    const updatedPt = await pool.query(`UPDATE pending_transactions SET status=$1, grade=$2, gross_weight_kg=$3, total_price=$4, rejection_reason=$5, updated_at=NOW() WHERE id=$6 RETURNING *`, [newStatus, grade, kg, totalPrice, rejection_reason||null, id]);
    res.json({ success: true, pending_transaction: updatedPt.rows[0] });
  } catch (err) { console.error('Recycler arrival error:', err); res.status(500).json({ success: false, message: 'Server error' }); }
});

// ── Recycler outbound sales (recycler → converter) ──

app.post('/api/pending-transactions/recycler-sale', requireAuth, async (req, res) => {
  try {
    if (!req.user.hasRole('recycler')) return res.status(403).json({ success: false, message: 'Recycler access only' });
    const { converter_id, material_type, gross_weight_kg, price_per_kg, notes, sources } = req.body;
    if (!converter_id || !material_type || !gross_weight_kg || !price_per_kg) return res.status(400).json({ success: false, message: 'converter_id, material_type, gross_weight_kg, price_per_kg required' });
    const kg = parseFloat(gross_weight_kg), price = parseFloat(price_per_kg);
    if (isNaN(kg) || kg <= 0) return res.status(400).json({ success: false, message: 'Invalid weight' });
    if (isNaN(price) || price <= 0) return res.status(400).json({ success: false, message: 'Invalid price' });
    const convResult = await pool.query(`SELECT id FROM converters WHERE id=$1 AND is_active=true`, [converter_id]);
    if (!convResult.rows.length) return res.status(400).json({ success: false, message: 'Converter not found' });
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      const { row, sources: attributedSources } = await attributeAndInsert(client, {
        transaction_type: 'recycler_sale',
        recycler_id: parseInt(req.user.id),
        converter_id: parseInt(converter_id),
        material_type: material_type,
        gross_weight_kg: kg,
        price_per_kg: price,
        total_price: kg * price,
        photos_required: true,
        photos_submitted: false,
        photo_urls: [],
        notes: notes || null,
        sources: sources   // PR4-A: optional manual source-picker hint
      });
      await client.query('COMMIT');
      return res.status(201).json({ success: true, pending_transaction: row, sources: attributedSources });
    } catch (e) {
      await client.query('ROLLBACK').catch(() => {});
      if (handleInsufficientSource(res, e)) return;
      throw e;
    } finally {
      client.release();
    }
  } catch (err) { console.error('Recycler sale error:', err); res.status(500).json({ success: false, message: 'Server error' }); }
});

// ── GET /api/sources ──────────────────────────────────────────────────────
// Candidate source list for the web source-picker (PR4 Phase B).
// Returns the same set attributeAndInsert's FIFO path would consider, scoped
// to the requested seller tier: aggregator sees collector-tier inbound,
// processor sees aggregator-tier inbound, recycler sees aggregator+processor
// tier inbound. Ordered by created_at ASC / id ASC (FIFO-natural).
//
// Query params (both required):
//   material_type — e.g. 'PET'
//   seller_role   — 'aggregator' | 'processor' | 'recycler'. Caller MUST
//                   hold this role via req.user.hasRole(seller_role). Dual-
//                   role users (e.g. Miniplast-style processor+converter)
//                   pass the tier matching the dashboard they're on, so a
//                   recycler+processor user on the recycler dashboard gets
//                   recycler-tier sources (fixes the pre-PR4-B fall-through
//                   bug where hasRole precedence silently picked the wrong
//                   tier).
//
// Response: [{ source_id, transaction_type, material_type, remaining_kg,
//              created_at, batch_id, supplier_name, supplier_role }, ...]
const VALID_SELLER_ROLES = ['aggregator', 'processor', 'recycler'];
app.get('/api/sources', requireAuth, async (req, res) => {
  try {
    const material_type = req.query.material_type;
    const seller_role = req.query.seller_role;
    if (!material_type) return res.status(400).json({ success: false, message: 'material_type query param is required' });
    if (!seller_role || VALID_SELLER_ROLES.indexOf(seller_role) === -1) {
      return res.status(400).json({ success: false, message: 'seller_role query param is required and must be one of: ' + VALID_SELLER_ROLES.join(', ') });
    }
    if (!req.user.hasRole(seller_role)) {
      return res.status(403).json({ success: false, message: 'Authenticated caller does not hold role: ' + seller_role });
    }

    const sellerKind = seller_role;
    const filter = candidateFilterForSeller({ kind: sellerKind, id: req.user.id });

    const excludedList = COC_EXCLUDED_STATUSES.map(function (_, i) { return '$' + (i + 4); }).join(', ');
    const parentTypesList = filter.parentTypes.map(function (_, i) {
      return '$' + (i + 4 + COC_EXCLUDED_STATUSES.length);
    }).join(', ');

    const sql =
      "SELECT pt.id AS source_id, pt.transaction_type, pt.material_type, pt.remaining_kg, pt.created_at, pt.batch_id, " +
      "       COALESCE(NULLIF(TRIM(COALESCE(c.first_name, '') || ' ' || COALESCE(c.last_name, '')), ''), a.name, p.name) AS supplier_name, " +
      "       CASE " +
      "         WHEN pt.transaction_type IN ('collector_sale','aggregator_purchase') THEN 'collector' " +
      "         WHEN pt.transaction_type = 'aggregator_sale' THEN 'aggregator' " +
      "         WHEN pt.transaction_type = 'processor_sale' THEN 'processor' " +
      "       END AS supplier_role " +
      "  FROM pending_transactions pt " +
      "  LEFT JOIN collectors  c ON c.id = pt.collector_id " +
      "  LEFT JOIN aggregators a ON a.id = pt.aggregator_id " +
      "  LEFT JOIN processors  p ON p.id = pt.processor_id " +
      " WHERE pt." + filter.fkColumn + " = $1 " +
      "   AND pt.material_type = $2 " +
      "   AND pt.remaining_kg > 0 " +
      "   AND pt.created_at >= NOW() - ($3 || ' days')::INTERVAL " +
      "   AND pt.status NOT IN (" + excludedList + ") " +
      "   AND pt.transaction_type IN (" + parentTypesList + ") " +
      " ORDER BY pt.created_at ASC, pt.id ASC";

    const params = [req.user.id, material_type, String(COC_WINDOW_DAYS)]
      .concat(COC_EXCLUDED_STATUSES)
      .concat(filter.parentTypes);

    const result = await pool.query(sql, params);
    res.json(result.rows);
  } catch (err) {
    console.error('GET /api/sources error:', err);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

app.get('/api/pending-transactions/recycler-sales', requireAuth, async (req, res) => {
  try {
    if (!req.user.hasRole('recycler')) return res.status(403).json({ success: false, message: 'Recycler access only' });
    const result = await pool.query(`SELECT pt.*, c.name AS converter_name, c.company AS converter_company, c.id AS converter_id FROM pending_transactions pt LEFT JOIN converters c ON c.id=pt.converter_id WHERE pt.transaction_type='recycler_sale' AND pt.recycler_id=$1 ORDER BY pt.created_at DESC LIMIT 20`, [req.user.id]);
    for (const row of result.rows) {
      if (row.converter_id) {
        row.converter_code = CirculRoles.circulCode('converter', row.converter_id);
        row.converter_name_visible = await canSeeName('recycler', req.user.id, 'converter', row.converter_id);
      }
    }
    res.json({ success: true, pending_transactions: result.rows });
  } catch (err) { console.error('Get recycler sales error:', err); res.status(500).json({ success: false, message: 'Server error' }); }
});

// ── Converter inbound queue (processor_sale or recycler_sale → converter) ──

app.get('/api/pending-transactions/converter-queue', requireAuth, async (req, res) => {
  try {
    if (!req.user.hasRole('converter')) return res.status(403).json({ success: false, message: 'Converter access only' });
    const converterId = req.user.converter_id || req.user.id;
    const result = await pool.query(`SELECT pt.*, COALESCE(p.company, p.name) AS processor_name, p.company AS processor_company, p.id AS processor_id, r.name AS recycler_name, r.company AS recycler_company, r.id AS recycler_id FROM pending_transactions pt LEFT JOIN processors p ON p.id=pt.processor_id LEFT JOIN recyclers r ON r.id=pt.recycler_id WHERE pt.transaction_type IN ('processor_sale','recycler_sale') AND pt.converter_id=$1 ORDER BY pt.created_at DESC`, [converterId]);
    for (const row of result.rows) {
      if (row.processor_id) {
        row.processor_code = CirculRoles.circulCode('processor', row.processor_id);
        row.processor_name_visible = await canSeeName('converter', converterId, 'processor', row.processor_id);
      }
      if (row.recycler_id) {
        row.recycler_code = CirculRoles.circulCode('recycler', row.recycler_id);
        row.recycler_name_visible = await canSeeName('converter', converterId, 'recycler', row.recycler_id);
      }
    }
    res.json({ success: true, pending_transactions: result.rows });
  } catch (err) { console.error('Converter queue error:', err); res.status(500).json({ success: false, message: 'Server error' }); }
});

app.post('/api/pending-transactions/:id/converter-dispatch-decision', requireAuth, async (req, res) => {
  try {
    if (!req.user.hasRole('converter')) return res.status(403).json({ success: false, message: 'Converter access only' });
    const { id } = req.params;
    const { decision, rejection_reason } = req.body;
    if (!decision || !['approve','reject'].includes(decision)) return res.status(400).json({ success: false, message: 'decision must be approve or reject' });
    const ptResult = await pool.query(`SELECT * FROM pending_transactions WHERE id=$1`, [id]);
    if (!ptResult.rows.length) return res.status(404).json({ success: false, message: 'Not found' });
    const pt = ptResult.rows[0];
    const converterId = req.user.converter_id || req.user.id;
    if (parseInt(pt.converter_id) !== parseInt(converterId)) return res.status(403).json({ success: false, message: 'Not your delivery' });
    if (pt.status !== 'pending') return res.status(400).json({ success: false, message: 'Delivery is not in pending status' });
    if (decision === 'reject') {
      if (!rejection_reason?.trim()) return res.status(400).json({ success: false, message: 'rejection_reason required' });
      const updated = await pool.query(`UPDATE pending_transactions SET status='dispatch_rejected', rejection_reason=$1, updated_at=NOW() WHERE id=$2 RETURNING *`, [rejection_reason.trim(), id]);
      return res.json({ success: true, pending_transaction: updated.rows[0] });
    }
    const updated = await pool.query(`UPDATE pending_transactions SET status='dispatch_approved', updated_at=NOW() WHERE id=$1 RETURNING *`, [id]);
    // Notify the sender that dispatch was approved
    try {
      var senderTable = pt.recycler_id ? 'recyclers' : 'processors';
      var senderId = pt.recycler_id || pt.processor_id;
      var senderRow = (await pool.query(`SELECT phone, name FROM ${senderTable} WHERE id = $1`, [senderId])).rows[0];
      var convRow = (await pool.query(`SELECT name FROM converters WHERE id = $1`, [converterId])).rows[0];
      if (senderRow && senderRow.phone) {
        notify(EVENTS.DELIVERY_APPROVED, senderRow.phone, { receiver_name: convRow ? convRow.name : 'the converter', qty: pt.gross_weight_kg, material: pt.material_type });
      }
    } catch (notifyErr) { console.warn('Notification error (delivery_approved):', notifyErr.message); }
    res.json({ success: true, pending_transaction: updated.rows[0] });
  } catch (err) { console.error('Converter dispatch decision error:', err); res.status(500).json({ success: false, message: 'Server error' }); }
});

app.post('/api/pending-transactions/:id/converter-arrival', requireAuth, async (req, res) => {
  try {
    if (!req.user.hasRole('converter')) return res.status(403).json({ success: false, message: 'Converter access only' });
    const { id } = req.params;
    const { actual_weight_kg } = req.body;
    const kg = parseFloat(actual_weight_kg);
    if (!actual_weight_kg || isNaN(kg) || kg <= 0) return res.status(400).json({ success: false, message: 'Valid actual_weight_kg required' });
    const ptResult = await pool.query(`SELECT * FROM pending_transactions WHERE id=$1`, [id]);
    if (!ptResult.rows.length) return res.status(404).json({ success: false, message: 'Not found' });
    const pt = ptResult.rows[0];
    const converterId = req.user.converter_id || req.user.id;
    if (parseInt(pt.converter_id) !== parseInt(converterId)) return res.status(403).json({ success: false, message: 'Not your delivery' });
    if (pt.status !== 'dispatch_approved') return res.status(400).json({ success: false, message: 'Delivery must be in dispatch_approved status' });
    const totalPrice = kg * parseFloat(pt.price_per_kg);
    const updatedPt = await pool.query(`UPDATE pending_transactions SET status='arrived', gross_weight_kg=$1, total_price=$2, updated_at=NOW() WHERE id=$3 RETURNING *`, [kg, totalPrice, id]);
    res.json({ success: true, pending_transaction: updatedPt.rows[0] });
  } catch (err) { console.error('Converter arrival error:', err); res.status(500).json({ success: false, message: 'Server error' }); }
});

// ============================================
// PAYMENT FLOW
// ============================================

app.patch('/api/transactions/:id/payment-initiate', requireAuth, async (req, res) => {
  const client = await pool.connect();
  try {
    const { id } = req.params;
    const { payment_method, payment_reference } = req.body;
    if (!['cash', 'mobile_money'].includes(payment_method))
      return res.status(400).json({ success: false, message: 'payment_method must be cash or mobile_money' });
    let ref;
    if (payment_method === 'mobile_money') {
      if (!payment_reference || !payment_reference.trim())
        return res.status(400).json({ success: false, message: 'payment_reference required for mobile_money' });
      if (payment_reference.trim().length > 50)
        return res.status(400).json({ success: false, message: 'payment_reference max 50 characters' });
      ref = payment_reference.trim();
    } else {
      ref = 'CASH';
    }
    await client.query('BEGIN');
    const existing = await client.query('SELECT * FROM transactions WHERE id=$1 FOR UPDATE', [id]);
    if (!existing.rows.length) { await client.query('ROLLBACK'); return res.status(404).json({ success: false, message: 'Transaction not found' }); }
    const row = existing.rows[0];
    if (row.payment_status !== 'unpaid') { await client.query('ROLLBACK'); return res.status(400).json({ success: false, message: 'Payment already recorded' }); }
    const parties = await resolveParties(client, row);
    if (!parties.buyerKind || !parties.buyer || !userOwnsParty(req.user, parties.buyerKind, parties.buyer.id)) {
      await client.query('ROLLBACK');
      return res.status(403).json({ success: false, message: 'Not authorized for this transaction' });
    }
    const result = await client.query(
      `UPDATE transactions SET payment_status='payment_sent', payment_method=$1, payment_reference=$2, payment_initiated_at=NOW() WHERE id=$3 RETURNING *`,
      [payment_method, ref, id]
    );
    await client.query('COMMIT');
    try {
      if (parties.seller && parties.seller.phone) {
        await notify(EVENTS.PAYMENT_SENT, parties.seller.phone, {
          buyer_name: parties.buyer ? parties.buyer.name : 'Buyer',
          qty: parties.qty,
          material: parties.material,
          amount: parties.amount,
          ref: parties.ref
        });
      }
    } catch (e) { console.warn('[NOTIFY] payment_sent (txn) failed:', e.message); }
    res.json({ success: true, transaction: result.rows[0] });
  } catch (err) { await client.query('ROLLBACK').catch(() => {}); console.error('Payment initiate error:', err); res.status(500).json({ success: false, message: 'Server error' }); } finally { client.release(); }
});

app.patch('/api/transactions/:id/payment-confirm', requireAuth, async (req, res) => {
  const client = await pool.connect();
  try {
    const { id } = req.params;
    await client.query('BEGIN');
    const existing = await client.query('SELECT * FROM transactions WHERE id=$1 FOR UPDATE', [id]);
    if (!existing.rows.length) { await client.query('ROLLBACK'); return res.status(404).json({ success: false, message: 'Transaction not found' }); }
    const row = existing.rows[0];
    if (row.payment_status !== 'payment_sent') { await client.query('ROLLBACK'); return res.status(400).json({ success: false, message: 'No payment to confirm' }); }
    const parties = await resolveParties(client, row);
    if (!parties.sellerKind || !parties.seller || !userOwnsParty(req.user, parties.sellerKind, parties.seller.id)) {
      await client.query('ROLLBACK');
      return res.status(403).json({ success: false, message: 'Not authorized for this transaction' });
    }
    const result = await client.query(
      `UPDATE transactions SET payment_status='paid', payment_completed_at=NOW() WHERE id=$1 RETURNING *`,
      [id]
    );
    await client.query('COMMIT');
    try {
      if (parties.buyer && parties.buyer.phone) {
        await notify(EVENTS.PAYMENT_CONFIRMED, parties.buyer.phone, {
          seller_name: parties.seller ? parties.seller.name : 'Seller',
          qty: parties.qty,
          material: parties.material,
          amount: parties.amount,
          ref: parties.ref
        });
      }
    } catch (e) { console.warn('[NOTIFY] payment_confirmed (txn) failed:', e.message); }
    res.json({ success: true, transaction: result.rows[0] });
  } catch (err) { await client.query('ROLLBACK').catch(() => {}); console.error('Payment confirm error:', err); res.status(500).json({ success: false, message: 'Server error' }); } finally { client.release(); }
});

app.patch('/api/pending-transactions/:id/payment-initiate', requireAuth, async (req, res) => {
  const client = await pool.connect();
  try {
    const { id } = req.params;
    const { payment_method, payment_reference } = req.body;
    if (!['cash', 'mobile_money'].includes(payment_method))
      return res.status(400).json({ success: false, message: 'payment_method must be cash or mobile_money' });
    let ref;
    if (payment_method === 'mobile_money') {
      if (!payment_reference || !payment_reference.trim())
        return res.status(400).json({ success: false, message: 'payment_reference required for mobile_money' });
      if (payment_reference.trim().length > 50)
        return res.status(400).json({ success: false, message: 'payment_reference max 50 characters' });
      ref = payment_reference.trim();
    } else {
      ref = 'CASH';
    }
    await client.query('BEGIN');
    const existing = await client.query('SELECT * FROM pending_transactions WHERE id=$1 FOR UPDATE', [id]);
    if (!existing.rows.length) { await client.query('ROLLBACK'); return res.status(404).json({ success: false, message: 'Pending transaction not found' }); }
    const row = existing.rows[0];
    if (row.payment_status !== 'unpaid') { await client.query('ROLLBACK'); return res.status(400).json({ success: false, message: 'Payment already recorded' }); }
    const parties = await resolveParties(client, row);
    if (!parties.buyerKind || !parties.buyer || !userOwnsParty(req.user, parties.buyerKind, parties.buyer.id)) {
      await client.query('ROLLBACK');
      return res.status(403).json({ success: false, message: 'Not authorized for this transaction' });
    }
    const result = await client.query(
      `UPDATE pending_transactions SET payment_status='payment_sent', payment_method=$1, payment_reference=$2, payment_initiated_at=NOW() WHERE id=$3 RETURNING *`,
      [payment_method, ref, id]
    );
    await client.query('COMMIT');
    try {
      if (parties.seller && parties.seller.phone) {
        await notify(EVENTS.PAYMENT_SENT, parties.seller.phone, {
          buyer_name: parties.buyer ? parties.buyer.name : 'Buyer',
          qty: parties.qty,
          material: parties.material,
          amount: parties.amount,
          ref: parties.ref
        });
      }
    } catch (e) { console.warn('[NOTIFY] payment_sent (PT) failed:', e.message); }
    res.json({ success: true, pending_transaction: result.rows[0] });
  } catch (err) { await client.query('ROLLBACK').catch(() => {}); console.error('PT payment initiate error:', err); res.status(500).json({ success: false, message: 'Server error' }); } finally { client.release(); }
});

app.patch('/api/pending-transactions/:id/payment-confirm', requireAuth, async (req, res) => {
  const client = await pool.connect();
  try {
    const { id } = req.params;
    await client.query('BEGIN');
    const existing = await client.query('SELECT * FROM pending_transactions WHERE id=$1 FOR UPDATE', [id]);
    if (!existing.rows.length) { await client.query('ROLLBACK'); return res.status(404).json({ success: false, message: 'Pending transaction not found' }); }
    const row = existing.rows[0];
    if (row.payment_status !== 'payment_sent') { await client.query('ROLLBACK'); return res.status(400).json({ success: false, message: 'No payment to confirm' }); }
    const parties = await resolveParties(client, row);
    if (!parties.sellerKind || !parties.seller || !userOwnsParty(req.user, parties.sellerKind, parties.seller.id)) {
      await client.query('ROLLBACK');
      return res.status(403).json({ success: false, message: 'Not authorized for this transaction' });
    }
    const result = await client.query(
      `UPDATE pending_transactions SET payment_status='paid', payment_completed_at=NOW() WHERE id=$1 RETURNING *`,
      [id]
    );
    await client.query('COMMIT');
    try {
      if (parties.buyer && parties.buyer.phone) {
        await notify(EVENTS.PAYMENT_CONFIRMED, parties.buyer.phone, {
          seller_name: parties.seller ? parties.seller.name : 'Seller',
          qty: parties.qty,
          material: parties.material,
          amount: parties.amount,
          ref: parties.ref
        });
      }
    } catch (e) { console.warn('[NOTIFY] payment_confirmed (PT) failed:', e.message); }
    res.json({ success: true, pending_transaction: result.rows[0] });
  } catch (err) { await client.query('ROLLBACK').catch(() => {}); console.error('PT payment confirm error:', err); res.status(500).json({ success: false, message: 'Server error' }); } finally { client.release(); }
});

// ============================================
// ORDERS API
// ============================================

// ── Shared order creation (used by both POST /api/orders and USSD aggregator handler) ──
async function createOrder({ buyerId, buyerRole, material_type, target_quantity_kg, price_per_kg, notes, accepted_colours, excluded_contaminants, max_contamination_pct, supplier_tier, supplier_id }) {
  if (!buyerId) throw new Error('createOrder: buyerId required');
  if (!buyerRole) throw new Error('createOrder: buyerRole required');
  if (!material_type) throw new Error('createOrder: material_type required');
  const qty = parseFloat(target_quantity_kg);
  const price = parseFloat(price_per_kg);
  if (isNaN(qty) || qty <= 0) throw new Error('Invalid target_quantity_kg');
  if (isNaN(price) || price < 0) throw new Error('Invalid price_per_kg');

  const tableCheck = await pool.query(
    `SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'orders')`
  ).catch(() => ({ rows: [{ exists: false }] }));
  if (!tableCheck.rows[0].exists) throw new Error('Orders table not yet deployed');

  const result = await pool.query(
    `INSERT INTO orders (buyer_id, buyer_role, material_type, target_quantity_kg, price_per_kg,
                         accepted_colours, excluded_contaminants, max_contamination_pct,
                         supplier_tier, supplier_id, notes)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) RETURNING *`,
    [buyerId, buyerRole, material_type, qty, price,
     accepted_colours || null,
     excluded_contaminants || null,
     (max_contamination_pct != null && max_contamination_pct !== '') ? parseFloat(max_contamination_pct) : null,
     supplier_tier || null,
     supplier_id || null,
     notes || null]
  );
  return result.rows[0];
}

app.post('/api/orders', requireAuth, async (req, res) => {
  try {
    if (!req.user.hasRole('converter') && !req.user.hasRole('recycler') && !req.user.hasRole('aggregator'))
      return res.status(403).json({ success: false, message: 'Converter, recycler, or aggregator access only' });

    const { material_type, target_quantity_kg, price_per_kg, accepted_colours, excluded_contaminants, max_contamination_pct, notes, supplier_tier, supplier_id } = req.body;
    if (!material_type || !target_quantity_kg || !price_per_kg)
      return res.status(400).json({ success: false, message: 'material_type, target_quantity_kg, price_per_kg required' });

    let buyerRole, buyerId;
    if (req.user.hasRole('converter')) {
      buyerRole = 'converter';
      buyerId = req.user.converter_id || req.user.id;
    } else if (req.user.hasRole('recycler')) {
      buyerRole = 'recycler';
      buyerId = req.user.id;
    } else {
      buyerRole = 'aggregator';
      buyerId = req.user.id;
    }

    try {
      const order = await createOrder({
        buyerId, buyerRole, material_type, target_quantity_kg, price_per_kg,
        notes, accepted_colours, excluded_contaminants, max_contamination_pct, supplier_tier, supplier_id
      });
      res.status(201).json({ success: true, order });
    } catch (e) {
      const msg = String(e && e.message || e);
      if (msg.includes('not yet deployed')) return res.status(503).json({ success: false, message: 'Orders feature is being deployed. Please try again in a few minutes.' });
      if (msg.startsWith('Invalid') || msg.startsWith('createOrder:')) return res.status(400).json({ success: false, message: msg });
      throw e;
    }
  } catch (err) { console.error('Create order error:', err); res.status(500).json({ success: false, message: 'Server error' }); }
});

app.get('/api/orders/my', async (req, res) => {
  try {
    // Parse token (optional auth — no 401, just empty orders)
    let user = null;
    const auth = req.headers.authorization || '';
    const token = auth.replace('Bearer ', '').trim() || req.query.token;
    if (token) {
      try { user = verifyToken(token, AUTH_SECRET); } catch (_) { /* invalid token */ }
    }
    if (!user || typeof user !== 'object') return res.json({ success: true, orders: [] });

    // Determine buyer identity based on role
    const isConverter  = user.role === 'converter'  || (Array.isArray(user.roles) && user.roles.includes('converter'));
    const isRecycler   = user.role === 'recycler'   || (Array.isArray(user.roles) && user.roles.includes('recycler'));
    const isAggregator = user.role === 'aggregator' || (Array.isArray(user.roles) && user.roles.includes('aggregator'));
    if (!isConverter && !isRecycler && !isAggregator) return res.json({ success: true, orders: [] });
    let buyerId, buyerRole;
    if (isConverter)        { buyerRole = 'converter';  buyerId = user.converter_id || user.id; }
    else if (isRecycler)    { buyerRole = 'recycler';   buyerId = user.id; }
    else                    { buyerRole = 'aggregator'; buyerId = user.id; }
    if (!buyerId) return res.json({ success: true, orders: [] });

    // Check if orders table exists before querying
    const tableCheck = await pool.query(
      `SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'orders')`
    ).catch(() => ({ rows: [{ exists: false }] }));
    if (!tableCheck.rows[0].exists) return res.json({ success: true, orders: [] });

    const result = await pool.query(
      `SELECT * FROM orders WHERE buyer_id=$1 AND buyer_role=$2 ORDER BY created_at DESC LIMIT 50`, [buyerId, buyerRole]
    ).catch(() => ({ rows: [] }));
    res.json({ success: true, orders: result.rows });
  } catch (err) { console.error('GET /api/orders/my error:', err); res.json({ success: true, orders: [] }); }
});

// Cancel a buy request — owner-only.
app.post('/api/orders/:id/cancel', requireAuth, async (req, res) => {
  try {
    const orderId = parseInt(req.params.id, 10);
    if (!orderId) return res.status(400).json({ success: false, message: 'Invalid order id' });

    let buyerRole, buyerId;
    if (req.user.hasRole('converter'))       { buyerRole = 'converter';  buyerId = req.user.converter_id || req.user.id; }
    else if (req.user.hasRole('recycler'))   { buyerRole = 'recycler';   buyerId = req.user.id; }
    else if (req.user.hasRole('aggregator')) { buyerRole = 'aggregator'; buyerId = req.user.id; }
    else return res.status(403).json({ success: false, message: 'Not allowed' });

    const r = await pool.query(
      `UPDATE orders SET status='cancelled', updated_at=NOW()
         WHERE id=$1 AND buyer_id=$2 AND buyer_role=$3 AND status='open'
         RETURNING *`,
      [orderId, buyerId, buyerRole]
    );
    if (!r.rows.length) return res.status(404).json({ success: false, message: 'Order not found or not cancellable' });
    res.json({ success: true, order: r.rows[0] });
  } catch (err) {
    console.error('Cancel order error:', err);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

// ============================================
// UNIFIED AUTH LOGIN
// ============================================

app.post('/api/admin/login', async (req, res) => {
  try {
    const { email, password } = req.body;
    if (!email || !password) return res.status(400).json({ success: false, message: 'Email and password required' });
    const result = await pool.query(`SELECT id, email, name, password_hash FROM admin_users WHERE email=$1 AND is_active=true`, [email.toLowerCase().trim()]);
    if (!result.rows.length) return res.status(401).json({ success: false, message: 'Invalid credentials' });
    const admin = result.rows[0];
    const valid = await verifyPassword(password, admin.password_hash);
    if (!valid) return res.status(401).json({ success: false, message: 'Invalid credentials' });
    const token = generateToken({ type: 'admin', id: admin.id, email: admin.email }, ADMIN_SECRET);
    res.json({ success: true, token, admin: { id: admin.id, email: admin.email, name: admin.name } });
  } catch (err) { console.error('Admin login error:', err); res.status(500).json({ success: false, message: 'Server error' }); }
});

app.post('/api/auth/login', async (req, res) => {
  try {
    const { type, phone, pin, email, password } = req.body;

    if (email || type === 'email') {
      if (!email || !password) return res.status(400).json({ success: false, message: 'Email and password required' });
      const emailLower = email.toLowerCase().trim();

      // 1. Admin
      const adminResult = await pool.query(`SELECT id, email, name, password_hash FROM admin_users WHERE email=$1 AND is_active=true`, [emailLower]);
      if (adminResult.rows.length) {
        const admin = adminResult.rows[0];
        const valid = await verifyPassword(password, admin.password_hash);
        if (!valid) return res.status(401).json({ success: false, message: 'Invalid credentials' });
        const token = generateToken({ type: 'admin', id: admin.id, email: admin.email }, ADMIN_SECRET);
        return res.json({ success: true, role: 'admin', roles: null, token, user: { id: admin.id, email: admin.email, name: admin.name, role: 'admin' } });
      }

      // 2. Processor
      const procResult = await pool.query(`SELECT id, name, company, email, password_hash FROM processors WHERE email=$1 AND is_active=true`, [emailLower]);
      // 3. Recycler
      const recResult = await pool.query(`SELECT id, name, company, email, password_hash FROM recyclers WHERE email=$1 AND is_active=true`, [emailLower]);
      // 4. Converter
      const convResult = await pool.query(`SELECT id, name, company, email, password_hash FROM converters WHERE email=$1 AND is_active=true`, [emailLower]);

      if (!procResult.rows.length && !recResult.rows.length && !convResult.rows.length) return res.status(401).json({ success: false, message: 'Invalid credentials' });

      const checkRow = procResult.rows[0] || recResult.rows[0] || convResult.rows[0];
      const valid = await verifyPassword(password, checkRow.password_hash);
      if (!valid) return res.status(401).json({ success: false, message: 'Invalid credentials' });

      const isProcessor = procResult.rows.length > 0;
      const isRecycler  = recResult.rows.length > 0;
      const isConverter = convResult.rows.length > 0;

      // Multi-role combos
      if (isProcessor && isConverter) {
        const proc = procResult.rows[0], conv = convResult.rows[0];
        const token = generateToken({ type: 'buyer', id: proc.id, converter_id: conv.id, email: emailLower, roles: ['processor','converter'] }, AUTH_SECRET);
        return res.json({ success: true, role: 'processor', roles: ['processor','converter'], token, user: { id: proc.id, converter_id: conv.id, name: proc.name, company: proc.company, email: emailLower, role: 'processor' } });
      }

      if (isRecycler && isConverter) {
        const rec = recResult.rows[0], conv = convResult.rows[0];
        const token = generateToken({ type: 'buyer', id: rec.id, converter_id: conv.id, email: emailLower, roles: ['recycler','converter'] }, AUTH_SECRET);
        return res.json({ success: true, role: 'recycler', roles: ['recycler','converter'], token, user: { id: rec.id, converter_id: conv.id, name: rec.name, company: rec.company, email: emailLower, role: 'recycler' } });
      }

      if (isProcessor) {
        const proc = procResult.rows[0];
        const token = generateToken({ type: 'buyer', id: proc.id, email: emailLower, role: 'processor' }, AUTH_SECRET);
        return res.json({ success: true, role: 'processor', roles: null, token, user: { id: proc.id, name: proc.name, company: proc.company, email: emailLower, role: 'processor' } });
      }

      if (isRecycler) {
        const rec = recResult.rows[0];
        const token = generateToken({ type: 'buyer', id: rec.id, email: emailLower, role: 'recycler' }, AUTH_SECRET);
        return res.json({ success: true, role: 'recycler', roles: null, token, user: { id: rec.id, name: rec.name, company: rec.company, email: emailLower, role: 'recycler' } });
      }

      const conv = convResult.rows[0];
      const token = generateToken({ type: 'buyer', id: conv.id, email: emailLower, role: 'converter' }, AUTH_SECRET);
      return res.json({ success: true, role: 'converter', roles: null, token, user: { id: conv.id, name: conv.name, company: conv.company, email: emailLower, role: 'converter' } });

    } else {
      if (!phone || !pin) return res.status(400).json({ success: false, message: 'Phone and PIN required' });
      const rl = checkRateLimit(phone.trim());
      if (rl.blocked) return res.status(429).json({ success: false, message: 'Too many failed attempts. Try again in ' + rl.remainMin + ' minutes.' });

      // 1. Collectors
      const collResult = await pool.query(`SELECT id, first_name, last_name, phone, pin, must_change_pin FROM collectors WHERE phone=$1 AND is_active=true`, [phone.trim()]);
      if (collResult.rows.length && await verifyPassword(pin.trim(), collResult.rows[0].pin)) {
        clearLoginAttempts(phone.trim());
        const c = collResult.rows[0];
        const name = ((c.first_name||'') + (c.last_name ? ' '+c.last_name : '')).trim();
        const token = generateToken({ type: 'collector', id: c.id, phone: c.phone, role: 'collector' }, AUTH_SECRET);
        return res.json({ success: true, role: 'collector', roles: null, token, user: { id: c.id, name, phone: c.phone, role: 'collector', must_change_pin: !!c.must_change_pin } });
      }

      // 2. Aggregators
      const aggResult = await pool.query(
        `SELECT id, name, company, phone, pin, must_change_pin FROM aggregators WHERE phone=$1 AND is_active=true`,
        [phone.trim()]
      );
      if (aggResult.rows.length && await verifyPassword(pin.trim(), aggResult.rows[0].pin)) {
        clearLoginAttempts(phone.trim());
        const a = aggResult.rows[0];
        const token = generateToken({ type: 'aggregator', id: a.id, phone: a.phone, role: 'aggregator' }, AUTH_SECRET);
        return res.json({ success: true, role: 'aggregator', roles: null, token, user: { id: a.id, name: a.name, company: a.company||null, phone: a.phone, role: 'aggregator', must_change_pin: !!a.must_change_pin } });
      }

      // 3. Agents (sub-accounts under aggregators)
      const agentResult = await pool.query(
        `SELECT id, aggregator_id, first_name, last_name, phone, pin, city, region, must_change_pin
         FROM agents WHERE phone=$1 AND is_active=true`, [phone.trim()]
      );
      if (agentResult.rows.length) {
        const ag = agentResult.rows[0];
        if (await verifyPassword(pin.trim(), ag.pin)) {
          clearLoginAttempts(phone.trim());
          const token = generateToken({
            type: 'agent', id: ag.id, aggregator_id: ag.aggregator_id,
            phone: ag.phone, role: 'agent'
          }, AUTH_SECRET);
          return res.json({
            success: true, role: 'agent', roles: null, token,
            user: { id: ag.id, name: ag.first_name + ' ' + ag.last_name,
                    phone: ag.phone, role: 'agent', aggregator_id: ag.aggregator_id,
                    must_change_pin: ag.must_change_pin }
          });
        }
      }

      recordFailedLogin(phone.trim());
      return res.status(401).json({ success: false, message: 'Invalid phone number or PIN' });
    }
  } catch (err) { console.error('Unified auth login error:', err); res.status(500).json({ success: false, message: 'Server error' }); }
});

app.get('/api/me/prices', requireAuth, async (req, res) => {
  try {
    const role = req.user.role || (req.user.roles && req.user.roles[0]);
    if (!CirculRoles.PAID_ROLES.includes(role)) return res.status(403).json({ success: false, message: 'Access denied' });
    const result = await pool.query(`SELECT * FROM posted_prices WHERE poster_type=$1 AND poster_id=$2 AND is_active=true ORDER BY material_type`, [role, req.user.id]);
    res.json({ success: true, prices: result.rows });
  } catch (err) { res.status(500).json({ success: false, message: 'Server error' }); }
});

// ============================================
// ADMIN ENDPOINTS
// ============================================

app.get('/api/admin/stats', requireAdmin, async (req, res) => {
  try {
    const now = new Date();
    const startOfMonth = new Date(now.getFullYear(), now.getMonth(), 1).toISOString();
    const startOfYear = new Date(now.getFullYear(), 0, 1).toISOString();
    const startOfDay = new Date(now.getFullYear(), now.getMonth(), now.getDate()).toISOString();
    const sevenDaysAgo = new Date(now - 7 * 86400000).toISOString();

    const [collectors, aggregators, processors, recyclers, converters, pendingProc, pendingRec, pendingConv,
           txnTotal, txnToday, txnWeek, txnMonth,
           volTotal, volMonth, volYtd,
           revMonth, revYtd, recentTxns] = await Promise.all([
      pool.query(`SELECT COUNT(*) as count FROM collectors WHERE is_active=true`),
      pool.query(`SELECT COUNT(*) as count FROM aggregators WHERE is_active=true`),
      pool.query(`SELECT COUNT(*) as count FROM processors WHERE is_active=true`),
      pool.query(`SELECT COUNT(*) as count FROM recyclers WHERE is_active=true`),
      pool.query(`SELECT COUNT(*) as count FROM converters WHERE is_active=true`),
      pool.query(`SELECT COUNT(*) as count FROM processors WHERE is_active=false`),
      pool.query(`SELECT COUNT(*) as count FROM recyclers WHERE is_active=false`),
      pool.query(`SELECT COUNT(*) as count FROM converters WHERE is_active=false`),
      pool.query(`SELECT COUNT(*) as count FROM transactions`),
      pool.query(`SELECT COUNT(*) as count FROM transactions WHERE transaction_date >= $1`, [startOfDay]),
      pool.query(`SELECT COUNT(*) as count FROM transactions WHERE transaction_date >= $1`, [sevenDaysAgo]),
      pool.query(`SELECT COUNT(*) as count FROM transactions WHERE transaction_date >= $1`, [startOfMonth]),
      pool.query(`SELECT COALESCE(SUM(net_weight_kg),0) as kg FROM transactions`),
      pool.query(`SELECT COALESCE(SUM(net_weight_kg),0) as kg FROM transactions WHERE transaction_date >= $1`, [startOfMonth]),
      pool.query(`SELECT COALESCE(SUM(net_weight_kg),0) as kg FROM transactions WHERE transaction_date >= $1`, [startOfYear]),
      pool.query(`SELECT COALESCE(SUM(total_price),0) as amt FROM transactions WHERE transaction_date >= $1`, [startOfMonth]),
      pool.query(`SELECT COALESCE(SUM(total_price),0) as amt FROM transactions WHERE transaction_date >= $1`, [startOfYear]),
      pool.query(`SELECT t.id, t.transaction_date, t.material_type, t.net_weight_kg, t.total_price, t.payment_status, c.first_name || ' ' || c.last_name AS seller_name, a.name AS buyer_name FROM transactions t LEFT JOIN collectors c ON c.id=t.collector_id LEFT JOIN aggregators a ON a.id=t.aggregator_id ORDER BY t.transaction_date DESC LIMIT 10`)
    ]);

    // Discovery stats — handle missing tables gracefully
    let discovery = { active_listings: 0, total_offers: 0, accepted_offers: 0 };
    try {
      const [listings, offers, accepted] = await Promise.all([
        pool.query(`SELECT COUNT(*) as count FROM listings WHERE status='active'`),
        pool.query(`SELECT COUNT(*) as count FROM offers`),
        pool.query(`SELECT COUNT(*) as count FROM offers WHERE status='accepted'`)
      ]);
      discovery = { active_listings: parseInt(listings.rows[0].count), total_offers: parseInt(offers.rows[0].count), accepted_offers: parseInt(accepted.rows[0].count) };
    } catch (e) { /* tables may not exist */ }

    const pending = parseInt(pendingProc.rows[0].count) + parseInt(pendingRec.rows[0].count) + parseInt(pendingConv.rows[0].count);
    const coll = parseInt(collectors.rows[0].count), agg = parseInt(aggregators.rows[0].count), proc = parseInt(processors.rows[0].count), rec = parseInt(recyclers.rows[0].count), conv = parseInt(converters.rows[0].count);

    res.json({
      collectors: coll, aggregators: agg, processors: proc, recyclers: rec, converters: conv, pending: pending,
      users: { collectors: coll, aggregators: agg, processors: proc, recyclers: rec, converters: conv, pending: pending, total: coll + agg + proc + rec + conv },
      transactions: { today: parseInt(txnToday.rows[0].count), this_week: parseInt(txnWeek.rows[0].count), this_month: parseInt(txnMonth.rows[0].count), total: parseInt(txnTotal.rows[0].count) },
      volume: { this_month_kg: parseFloat(volMonth.rows[0].kg), ytd_kg: parseFloat(volYtd.rows[0].kg), total_kg: parseFloat(volTotal.rows[0].kg) },
      revenue: { this_month: parseFloat(revMonth.rows[0].amt), ytd: parseFloat(revYtd.rows[0].amt) },
      discovery: discovery,
      recent_activity: recentTxns.rows
    });
  } catch (err) { console.error('Admin stats error:', err); res.status(500).json({ success: false, message: 'Server error' }); }
});

app.get('/api/admin/transactions', requireAdmin, async (req, res) => {
  try {
    const { limit = 100, offset = 0, material_type } = req.query;
    let where = 'WHERE 1=1'; const params = [];
    if (material_type) { params.push(material_type.toUpperCase()); where += ` AND t.material_type=$${params.length}`; }
    params.push(parseInt(limit)); params.push(parseInt(offset));
    const result = await pool.query(`SELECT t.id, t.material_type, t.gross_weight_kg, t.net_weight_kg, t.contamination_deduction_percent, t.price_per_kg, t.total_price, t.payment_status, t.transaction_date, c.first_name||' '||c.last_name as collector_name, c.phone as collector_phone, a.name as aggregator_name FROM transactions t JOIN collectors c ON c.id=t.collector_id LEFT JOIN aggregators a ON a.id=t.aggregator_id ${where} ORDER BY t.transaction_date DESC LIMIT $${params.length-1} OFFSET $${params.length}`, params);
    res.json({ success: true, transactions: result.rows });
  } catch (err) { console.error('Admin transactions error:', err); res.status(500).json({ success: false, message: 'Server error' }); }
});

app.get('/api/admin/collectors', requireAdmin, async (req, res) => {
  try {
    const result = await pool.query(`SELECT c.id, c.first_name, c.last_name, c.phone, c.region, c.city, c.average_rating, c.id_verified, c.is_active, c.created_at, COALESCE(SUM(t.net_weight_kg),0) as total_weight_kg, COUNT(t.id) as transaction_count FROM collectors c LEFT JOIN transactions t ON t.collector_id=c.id WHERE c.is_active=true GROUP BY c.id ORDER BY c.created_at DESC`);
    res.json({ success: true, collectors: result.rows });
  } catch (err) { console.error('Admin collectors error:', err); res.status(500).json({ success: false, message: 'Server error' }); }
});

app.get('/api/admin/aggregators', requireAdmin, async (req, res) => {
  try {
    const result = await pool.query(`SELECT id, name, company, phone, city, region, is_active, id_verified, created_at FROM aggregators ORDER BY created_at DESC`);
    res.json({ success: true, aggregators: result.rows });
  } catch (err) { res.status(500).json({ success: false, message: 'Server error' }); }
});

app.put('/api/admin/aggregators/:id', requireAdmin, async (req, res) => {
  try {
    const { name, company, phone, pin, is_active, is_flagged, city, region, country } = req.body;
    if (pin !== undefined && (pin.length < 4 || pin.length > 6 || !/^\d+$/.test(pin))) {
      return res.status(400).json({ success: false, message: 'PIN must be 4-6 digits' });
    }
    let hashedPin;
    if (pin !== undefined) hashedPin = await hashPassword(pin);
    const fields = [], params = [];
    if (name !== undefined) { params.push(name); fields.push(`name=$${params.length}`); }
    if (company !== undefined) { params.push(company); fields.push(`company=$${params.length}`); }
    if (phone !== undefined) { params.push(phone); fields.push(`phone=$${params.length}`); }
    if (pin !== undefined) { params.push(hashedPin); fields.push(`pin=$${params.length}`); }
    if (is_active !== undefined) { params.push(is_active); fields.push(`is_active=$${params.length}`); }
    if (is_flagged !== undefined) { params.push(is_flagged); fields.push(`is_flagged=$${params.length}`); }
    if (city !== undefined) { params.push(city); fields.push(`city=$${params.length}`); }
    if (region !== undefined) { params.push(region); fields.push(`region=$${params.length}`); }
    if (country !== undefined) { params.push(country); fields.push(`country=$${params.length}`); }
    if (!fields.length) return res.status(400).json({ success: false, message: 'Nothing to update' });
    params.push(req.params.id);
    const result = await pool.query(`UPDATE aggregators SET ${fields.join(',')} WHERE id=$${params.length} RETURNING id, name, company, phone, is_active, is_flagged, city`, params);
    if (!result.rows.length) return res.status(404).json({ success: false, message: 'Not found' });
    await recordAdminAction(null, {
      actor_type: 'admin', actor_email: req.admin.email,
      action: 'aggregator_updated', target_type: 'aggregator',
      target_id: parseInt(req.params.id, 10),
      details: { updated_fields: fields.map(f => f.split('=')[0]).filter(f => f !== 'pin') }
    });
    if (pin !== undefined) {
      const time = new Date().toLocaleTimeString('en-GB', { hour: '2-digit', minute: '2-digit' });
      const targetPhone = result.rows[0].phone;
      if (targetPhone) {
        notify(EVENTS.ADMIN_PIN_CHANGED, targetPhone, { time: time }).catch(err => {
          console.warn('[admin-pin-changed] notify failed:', err.message);
        });
      }
      await recordAdminAction(null, {
        actor_type: 'admin', actor_email: req.admin.email,
        action: 'admin_pin_changed', target_type: 'aggregator',
        target_id: parseInt(req.params.id, 10), details: {}
      });
    }
    res.json({ success: true, aggregator: result.rows[0] });
  } catch (err) { console.error('[aggregators PUT]', err); res.status(500).json({ success: false, message: 'Server error' }); }
});

app.put('/api/admin/collectors/:id', requireAdmin, async (req, res) => {
  try {
    const { first_name, last_name, phone, pin, is_active, is_flagged, city, region } = req.body;
    if (pin !== undefined && (pin.length < 4 || pin.length > 6 || !/^\d+$/.test(pin))) {
      return res.status(400).json({ success: false, message: 'PIN must be 4-6 digits' });
    }
    let hashedPin;
    if (pin !== undefined) hashedPin = await hashPassword(pin);
    const fields = [], params = [];
    if (first_name  !== undefined) { params.push(first_name);  fields.push(`first_name=$${params.length}`); }
    if (last_name   !== undefined) { params.push(last_name);   fields.push(`last_name=$${params.length}`); }
    if (phone       !== undefined) { params.push(phone);       fields.push(`phone=$${params.length}`); }
    if (pin         !== undefined) { params.push(hashedPin);   fields.push(`pin=$${params.length}`); }
    if (is_active   !== undefined) { params.push(is_active);   fields.push(`is_active=$${params.length}`); }
    if (is_flagged  !== undefined) { params.push(is_flagged);  fields.push(`is_flagged=$${params.length}`); }
    if (city        !== undefined) { params.push(city);        fields.push(`city=$${params.length}`); }
    if (region      !== undefined) { params.push(region);      fields.push(`region=$${params.length}`); }
    if (!fields.length) return res.status(400).json({ success: false, message: 'Nothing to update' });
    params.push(req.params.id);
    const result = await pool.query(
      `UPDATE collectors SET ${fields.join(',')} WHERE id=$${params.length} RETURNING id, first_name, last_name, phone, is_active, city`,
      params
    );
    if (!result.rows.length) return res.status(404).json({ success: false, message: 'Not found' });
    await recordAdminAction(null, {
      actor_type: 'admin', actor_email: req.admin.email,
      action: 'collector_updated', target_type: 'collector',
      target_id: parseInt(req.params.id, 10),
      details: { updated_fields: fields.map(f => f.split('=')[0]).filter(f => f !== 'pin') }
    });
    if (pin !== undefined) {
      const time = new Date().toLocaleTimeString('en-GB', { hour: '2-digit', minute: '2-digit' });
      const targetPhone = result.rows[0].phone;
      if (targetPhone) {
        notify(EVENTS.ADMIN_PIN_CHANGED, targetPhone, { time: time }).catch(err => {
          console.warn('[admin-pin-changed] notify failed:', err.message);
        });
      }
      await recordAdminAction(null, {
        actor_type: 'admin', actor_email: req.admin.email,
        action: 'admin_pin_changed', target_type: 'collector',
        target_id: parseInt(req.params.id, 10), details: {}
      });
    }
    res.json({ success: true, collector: result.rows[0] });
  } catch (err) { console.error('[collectors PUT]', err); res.status(500).json({ success: false, message: 'Server error' }); }
});

app.put('/api/admin/collectors/:id/verify', requireAdmin, async (req, res) => {
  try {
    const result = await pool.query(`UPDATE collectors SET id_verified=true, id_verified_at=NOW(), id_verified_by=$1 WHERE id=$2 RETURNING id, first_name, last_name, id_verified, id_verified_at`, [req.admin.email, req.params.id]);
    if (!result.rows.length) return res.status(404).json({ success: false, message: 'Collector not found' });
    await recordAdminAction(null, {
      actor_type: 'admin', actor_email: req.admin.email,
      action: 'collector_verified', target_type: 'collector',
      target_id: parseInt(req.params.id, 10), details: {}
    });
    res.json({ success: true, collector: result.rows[0] });
  } catch (err) { console.error('[collectors/verify]', err); res.status(500).json({ success: false, message: 'Server error' }); }
});

app.put('/api/admin/aggregators/:id/verify', requireAdmin, async (req, res) => {
  try {
    const result = await pool.query(`UPDATE aggregators SET id_verified=true, id_verified_at=NOW(), id_verified_by=$1 WHERE id=$2 RETURNING id, name, id_verified, id_verified_at`, [req.admin.email, req.params.id]);
    if (!result.rows.length) return res.status(404).json({ success: false, message: 'Aggregator not found' });
    await recordAdminAction(null, {
      actor_type: 'admin', actor_email: req.admin.email,
      action: 'aggregator_verified', target_type: 'aggregator',
      target_id: parseInt(req.params.id, 10), details: {}
    });
    res.json({ success: true, aggregator: result.rows[0] });
  } catch (err) { console.error('[aggregators/verify]', err); res.status(500).json({ success: false, message: 'Server error' }); }
});

app.get('/api/admin/agents', requireAdmin, async (req, res) => {
  try {
    const result = await pool.query(
      `SELECT a.id, a.first_name, a.last_name, a.phone, a.city, a.region,
              a.is_active, a.must_change_pin, a.created_at,
              a.aggregator_id, agg.name AS aggregator_name
       FROM agents a
       LEFT JOIN aggregators agg ON agg.id = a.aggregator_id
       ORDER BY a.created_at DESC`
    );
    res.json({ success: true, agents: result.rows });
  } catch (err) { console.error('[admin/agents]', err); res.status(500).json({ success: false, message: 'Server error' }); }
});

// ── Account-recovery admin endpoints ──

// Phone-change: start — SMS an OTP to the prospective new number.
app.post('/api/admin/users/:type/:id/change-phone-start', requireAdmin, async (req, res) => {
  try {
    const { type, id } = req.params;
    const { new_phone } = req.body;
    if (!['collector','aggregator','agent'].includes(type)) {
      return res.status(400).json({ success: false, message: 'Invalid user type' });
    }
    if (!new_phone) return res.status(400).json({ success: false, message: 'new_phone required' });
    const normalizedNew = normalizeGhanaPhone(new_phone);
    if (!normalizedNew) return res.status(400).json({ success: false, message: 'Invalid phone format' });
    const userTable = type === 'collector' ? 'collectors' : type === 'aggregator' ? 'aggregators' : 'agents';
    const current = await pool.query(`SELECT id, phone FROM ${userTable} WHERE id = $1`, [id]);
    if (!current.rows.length) return res.status(404).json({ success: false, message: 'User not found' });
    const currentPhone = current.rows[0].phone;
    if (currentPhone === normalizedNew) return res.status(400).json({ success: false, message: 'New phone is same as current' });
    // Cross-table collision — the new phone must not belong to anyone else
    const variants = getPhoneVariants(normalizedNew);
    const collisions = await pool.query(
      `SELECT 'collector' AS t, id FROM collectors WHERE phone = ANY($1)
       UNION ALL SELECT 'aggregator', id FROM aggregators WHERE phone = ANY($1)
       UNION ALL SELECT 'agent', id FROM agents WHERE phone = ANY($1)`,
      [variants]
    );
    if (collisions.rows.length) {
      return res.status(409).json({
        success: false, message: 'Phone already in use',
        collision: collisions.rows[0]
      });
    }
    // Invalidate any prior active code for this user
    await pool.query(
      `UPDATE phone_change_codes SET used_at = NOW()
       WHERE user_type = $1 AND user_id = $2 AND used_at IS NULL`,
      [type, id]
    );
    const code = generateOtp();
    const codeHash = hashOtp(code);
    const inserted = await pool.query(
      `INSERT INTO phone_change_codes (user_type, user_id, old_phone, new_phone, code_hash, expires_at, initiated_by_admin_email)
       VALUES ($1,$2,$3,$4,$5, NOW() + INTERVAL '10 minutes', $6)
       RETURNING id`,
      [type, id, currentPhone, normalizedNew, codeHash, req.admin.email]
    );
    try {
      await notify(EVENTS.PHONE_CHANGE_OTP, normalizedNew, { code: code, minutes: 10 });
    } catch (e) { console.warn('[change-phone-start] notify failed:', e.message); }
    res.json({ success: true, code_id: inserted.rows[0].id });
  } catch (err) {
    console.error('[change-phone-start]', err);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

// Phone-change: confirm — admin submits the code the user read back to them.
app.post('/api/admin/users/:type/:id/change-phone-confirm', requireAdmin, async (req, res) => {
  const { type, id } = req.params;
  const { code_id, code } = req.body;
  if (!['collector','aggregator','agent'].includes(type)) {
    return res.status(400).json({ success: false, message: 'Invalid user type' });
  }
  if (!code_id || !code) return res.status(400).json({ success: false, message: 'code_id and code required' });

  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const codeRow = await client.query(
      `SELECT * FROM phone_change_codes
       WHERE id = $1 AND user_type = $2 AND user_id = $3
       FOR UPDATE`,
      [code_id, type, id]
    );
    if (!codeRow.rows.length) {
      await client.query('ROLLBACK');
      return res.status(404).json({ success: false, message: 'Code not found' });
    }
    const row = codeRow.rows[0];
    if (row.used_at) {
      await client.query('ROLLBACK');
      return res.status(400).json({ success: false, message: 'Code already used' });
    }
    if (new Date(row.expires_at) < new Date()) {
      await client.query('ROLLBACK');
      return res.status(400).json({ success: false, message: 'Code expired' });
    }
    if (!verifyOtp(code, row.code_hash)) {
      const remaining = row.attempts_remaining - 1;
      if (remaining <= 0) {
        await client.query(`UPDATE phone_change_codes SET attempts_remaining = 0, used_at = NOW() WHERE id = $1`, [code_id]);
        await client.query('COMMIT');
        return res.status(400).json({ success: false, message: 'Too many wrong attempts; code invalidated' });
      }
      await client.query(`UPDATE phone_change_codes SET attempts_remaining = $1 WHERE id = $2`, [remaining, code_id]);
      await client.query('COMMIT');
      return res.status(400).json({ success: false, message: 'Wrong code', attempts_remaining: remaining });
    }
    const userTable = type === 'collector' ? 'collectors' : type === 'aggregator' ? 'aggregators' : 'agents';
    await client.query(
      `UPDATE ${userTable} SET phone = $1 WHERE id = $2`,
      [row.new_phone, id]
    );
    await client.query(`UPDATE phone_change_codes SET used_at = NOW() WHERE id = $1`, [code_id]);
    await recordAdminAction(client, {
      actor_type: 'admin', actor_email: req.admin.email,
      action: 'phone_changed', target_type: type,
      target_id: parseInt(id, 10),
      details: { old_phone: row.old_phone, new_phone: row.new_phone, code_id: code_id }
    });
    await client.query('COMMIT');
    firePhoneChangeNotifications(type, parseInt(id, 10), row.old_phone, row.new_phone, req.admin.email).catch(err => {
      console.error('[phone-change] notification error:', err.message);
    });
    res.json({ success: true });
  } catch (err) {
    await client.query('ROLLBACK').catch(() => {});
    console.error('[change-phone-confirm]', err);
    res.status(500).json({ success: false, message: 'Server error' });
  } finally {
    client.release();
  }
});

// Admin force-set phone. Skips OTP — ops-only path (demo cleanup, migration).
// Distinct audit action (`admin_forced_phone_change`) so elevated-risk events
// are filterable separately from the normal OTP-verified phone-change flow.
app.post('/api/admin/users/:type/:id/force-set-phone', requireAdmin, async (req, res) => {
  try {
    const { type, id } = req.params;
    const { new_phone, confirm } = req.body;
    if (!['collector','aggregator','agent'].includes(type)) {
      return res.status(400).json({ success: false, message: 'Invalid user type' });
    }
    if (confirm !== true) {
      return res.status(400).json({ success: false, message: 'Confirmation required (pass { confirm: true })' });
    }
    let normalizedNew = null;
    if (new_phone !== null && new_phone !== undefined && new_phone !== '') {
      normalizedNew = normalizeGhanaPhone(new_phone);
      if (!normalizedNew) return res.status(400).json({ success: false, message: 'Invalid phone format' });
    }
    const userTable = type === 'collector' ? 'collectors' : type === 'aggregator' ? 'aggregators' : 'agents';
    const selfKey = id + ':' + type;
    if (normalizedNew) {
      const variants = getPhoneVariants(normalizedNew);
      const collisions = await pool.query(
        `SELECT 'collector' AS t, id FROM collectors WHERE phone = ANY($1) AND id::text || ':collector' != $2
         UNION ALL SELECT 'aggregator', id FROM aggregators WHERE phone = ANY($1) AND id::text || ':aggregator' != $2
         UNION ALL SELECT 'agent',      id FROM agents      WHERE phone = ANY($1) AND id::text || ':agent' != $2`,
        [variants, selfKey]
      );
      if (collisions.rows.length) {
        return res.status(409).json({ success: false, message: 'Phone already in use', collision: collisions.rows[0] });
      }
    }
    const before = await pool.query(`SELECT phone FROM ${userTable} WHERE id = $1`, [id]);
    if (!before.rows.length) return res.status(404).json({ success: false, message: 'User not found' });
    const oldPhone = before.rows[0].phone;
    await pool.query(`UPDATE ${userTable} SET phone = $1 WHERE id = $2`, [normalizedNew, id]);
    await recordAdminAction(null, {
      actor_type: 'admin',
      actor_email: req.admin.email,
      action: 'admin_forced_phone_change',
      target_type: type,
      target_id: parseInt(id, 10),
      details: { old_phone: oldPhone, new_phone: normalizedNew, skipped_otp: true }
    });
    res.json({ success: true, new_phone: normalizedNew, old_phone: oldPhone });
  } catch (err) {
    console.error('[force-set-phone]', err);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

async function firePhoneChangeNotifications(userType, userId, oldPhone, newPhone, adminEmail) {
  const time = new Date().toLocaleTimeString('en-GB', { hour: '2-digit', minute: '2-digit' });
  await notify(EVENTS.PHONE_CHANGED_NEW, newPhone, {});
  await notify(EVENTS.PHONE_CHANGED_OLD, oldPhone, { new_phone: newPhone, time: time, admin_email: adminEmail });

  if (userType === 'collector') {
    const r = await pool.query(
      `SELECT a.phone, c.first_name || ' ' || COALESCE(c.last_name, '') AS name,
              'COL-' || LPAD(c.id::text, 4, '0') AS code
       FROM transactions t JOIN aggregators a ON a.id = t.aggregator_id JOIN collectors c ON c.id = t.collector_id
       WHERE t.collector_id = $1 ORDER BY t.transaction_date DESC LIMIT 1`,
      [userId]
    );
    if (r.rows.length && r.rows[0].phone) {
      await notify(EVENTS.PHONE_CHANGED_UPSTREAM, r.rows[0].phone, {
        user_name: r.rows[0].name.trim(), user_code: r.rows[0].code,
        old_phone: oldPhone, new_phone: newPhone, time: time
      });
    }
  } else if (userType === 'aggregator') {
    const r = await pool.query(
      `SELECT DISTINCT p.phone, a.name, 'AGG-' || LPAD(a.id::text, 4, '0') AS code
       FROM pending_transactions pt JOIN processors p ON p.id = pt.processor_id JOIN aggregators a ON a.id = pt.aggregator_id
       WHERE pt.aggregator_id = $1 AND pt.created_at > NOW() - INTERVAL '90 days' AND p.phone IS NOT NULL`,
      [userId]
    );
    for (const row of r.rows) {
      await notify(EVENTS.PHONE_CHANGED_UPSTREAM, row.phone, {
        user_name: row.name, user_code: row.code,
        old_phone: oldPhone, new_phone: newPhone, time: time
      });
    }
  } else if (userType === 'agent') {
    const r = await pool.query(
      `SELECT a.phone, ag.first_name || ' ' || COALESCE(ag.last_name, '') AS name,
              'AGT-' || LPAD(ag.id::text, 4, '0') AS code
       FROM agents ag JOIN aggregators a ON a.id = ag.aggregator_id WHERE ag.id = $1`,
      [userId]
    );
    if (r.rows.length && r.rows[0].phone) {
      await notify(EVENTS.PHONE_CHANGED_UPSTREAM, r.rows[0].phone, {
        user_name: r.rows[0].name.trim(), user_code: r.rows[0].code,
        old_phone: oldPhone, new_phone: newPhone, time: time
      });
    }
  }
}

// Admin-initiated PIN reset. Flags must_change_pin and SMSes the user, who
// then self-serves via the USSD forgot-PIN flow. Admin never issues a PIN.
app.post('/api/admin/users/:type/:id/reset-pin', requireAdmin, async (req, res) => {
  try {
    const { type, id } = req.params;
    if (!['collector','aggregator','agent'].includes(type)) {
      return res.status(400).json({ success: false, message: 'Invalid user type' });
    }
    const userTable = type === 'collector' ? 'collectors' : type === 'aggregator' ? 'aggregators' : 'agents';
    const r = await pool.query(`SELECT phone FROM ${userTable} WHERE id = $1`, [id]);
    if (!r.rows.length) return res.status(404).json({ success: false, message: 'User not found' });
    const phone = r.rows[0].phone;
    await pool.query(`UPDATE ${userTable} SET must_change_pin = true WHERE id = $1`, [id]);
    await recordAdminAction(null, {
      actor_type: 'admin', actor_email: req.admin.email,
      action: 'pin_reset_triggered', target_type: type,
      target_id: parseInt(id, 10), details: {}
    });
    const time = new Date().toLocaleTimeString('en-GB', { hour: '2-digit', minute: '2-digit' });
    notify(EVENTS.ADMIN_PIN_RESET_TRIGGERED, phone, { time: time }).catch(err => {
      console.error('[admin-pin-reset] notification error:', err.message);
    });
    res.json({ success: true });
  } catch (err) {
    console.error('[reset-pin]', err);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

// Admin audit log viewer — filterable, paginated.
app.get('/api/admin/audit-log', requireAdmin, async (req, res) => {
  try {
    const limit = Math.min(parseInt(req.query.limit || '50', 10), 200);
    const offset = parseInt(req.query.offset || '0', 10);
    const filters = [];
    const params = [];
    if (req.query.actor_email) { params.push(req.query.actor_email); filters.push(`actor_email = $${params.length}`); }
    if (req.query.target_type)  { params.push(req.query.target_type);  filters.push(`target_type = $${params.length}`); }
    if (req.query.target_id)    { params.push(parseInt(req.query.target_id, 10)); filters.push(`target_id = $${params.length}`); }
    if (req.query.action)       { params.push(req.query.action);       filters.push(`action = $${params.length}`); }
    const where = filters.length ? 'WHERE ' + filters.join(' AND ') : '';
    params.push(limit); params.push(offset);
    const rows = await pool.query(
      `SELECT id, actor_type, actor_email, action, target_type, target_id, details, created_at
       FROM admin_audit_log ${where}
       ORDER BY created_at DESC
       LIMIT $${params.length - 1} OFFSET $${params.length}`,
      params
    );
    res.json({ success: true, entries: rows.rows, limit: limit, offset: offset });
  } catch (err) {
    console.error('[audit-log]', err);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

// List aggregator-registration requests, filtered by status (default 'pending').
app.get('/api/admin/aggregator-requests', requireAdmin, async (req, res) => {
  try {
    const status = req.query.status || 'pending';
    const limit = Math.min(parseInt(req.query.limit || '50', 10), 200);
    const validStatuses = ['pending', 'code_issued', 'completed', 'rejected', 'expired', 'code_failed', 'all'];
    if (!validStatuses.includes(status)) {
      return res.status(400).json({ success: false, message: 'Invalid status filter' });
    }
    const where = status === 'all' ? '' : `WHERE status = $1`;
    const params = status === 'all' ? [limit] : [status, limit];
    const sql =
      `SELECT id, phone, name, company, city, region, status, created_at, approved_at, rejected_at, rejection_reason, aggregator_id
       FROM aggregator_registration_requests
       ${where}
       ORDER BY created_at ASC
       LIMIT $${params.length}`;
    const rows = await pool.query(sql, params);
    res.json({ success: true, requests: rows.rows });
  } catch (err) {
    console.error('[admin/aggregator-requests]', err);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

// Approve a pending aggregator request: generate 6-digit code, SMS the candidate,
// transition status to code_issued. First-write-wins via WHERE status='pending'.
app.post('/api/admin/aggregator-requests/:id/approve', requireAdmin, async (req, res) => {
  try {
    const { id } = req.params;
    const code = generateOtp();
    const codeHash = hashOtp(code);
    const result = await pool.query(
      `UPDATE aggregator_registration_requests
       SET status = 'code_issued',
           code_hash = $1,
           code_expires_at = NOW() + INTERVAL '10 minutes',
           code_attempts_remaining = 3,
           approved_by_admin_email = $2,
           approved_at = NOW(),
           updated_at = NOW()
       WHERE id = $3 AND status = 'pending'
       RETURNING id, phone, name, company, city`,
      [codeHash, req.admin.email, id]
    );
    if (!result.rows.length) {
      return res.status(409).json({ success: false, message: 'Request not in pending status (already approved, rejected, or expired)' });
    }
    const row = result.rows[0];
    await recordAdminAction(null, {
      actor_type: 'admin',
      actor_email: req.admin.email,
      action: 'aggregator_request_approved',
      target_type: 'aggregator_request',
      target_id: parseInt(id, 10),
      details: { phone: row.phone, name: row.name }
    });
    notify(EVENTS.AGGREGATOR_CODE_ISSUED, row.phone, { code: code, minutes: 10 })
      .catch(function (e) { console.warn('[agg-req-approve] code SMS failed:', e.message); });
    res.json({ success: true, request: row });
  } catch (err) {
    console.error('[admin/aggregator-request approve]', err);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

// Reject a pending aggregator request with a reason; SMSes the candidate.
app.post('/api/admin/aggregator-requests/:id/reject', requireAdmin, async (req, res) => {
  try {
    const { id } = req.params;
    const { reason } = req.body;
    if (!reason || !reason.trim()) {
      return res.status(400).json({ success: false, message: 'Rejection reason required' });
    }
    const result = await pool.query(
      `UPDATE aggregator_registration_requests
       SET status = 'rejected',
           rejected_by_admin_email = $1,
           rejected_at = NOW(),
           rejection_reason = $2,
           updated_at = NOW()
       WHERE id = $3 AND status = 'pending'
       RETURNING id, phone, name`,
      [req.admin.email, reason.trim(), id]
    );
    if (!result.rows.length) {
      return res.status(409).json({ success: false, message: 'Request not in pending status' });
    }
    const row = result.rows[0];
    await recordAdminAction(null, {
      actor_type: 'admin',
      actor_email: req.admin.email,
      action: 'aggregator_request_rejected',
      target_type: 'aggregator_request',
      target_id: parseInt(id, 10),
      details: { phone: row.phone, name: row.name, reason: reason.trim() }
    });
    notify(EVENTS.AGGREGATOR_REQUEST_REJECTED, row.phone, { reason: reason.trim() })
      .catch(function (e) { console.warn('[agg-req-reject] notify failed:', e.message); });
    res.json({ success: true });
  } catch (err) {
    console.error('[admin/aggregator-request reject]', err);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

// List active user lockouts with resolved display names — for the admin UI.
app.get('/api/admin/lockouts', requireAdmin, async (req, res) => {
  try {
    const rows = await pool.query(
      `SELECT l.id, l.user_type, l.user_id, l.phone, l.reason, l.locked_until, l.created_at,
              CASE l.user_type
                WHEN 'collector'  THEN (SELECT first_name || ' ' || COALESCE(last_name, '') FROM collectors  WHERE id = l.user_id)
                WHEN 'aggregator' THEN (SELECT name FROM aggregators WHERE id = l.user_id)
                WHEN 'agent'      THEN (SELECT first_name || ' ' || COALESCE(last_name, '') FROM agents      WHERE id = l.user_id)
              END AS user_name
         FROM user_lockouts l
        WHERE l.locked_until > NOW()
        ORDER BY l.locked_until ASC`
    );
    res.json({ success: true, lockouts: rows.rows });
  } catch (err) {
    console.error('[admin/lockouts]', err);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

// Manually clear an active lockout + audit. No-op if the user isn't locked.
app.delete('/api/admin/users/:type/:id/lockout', requireAdmin, async (req, res) => {
  try {
    const { type, id } = req.params;
    if (!['collector','aggregator','agent'].includes(type)) {
      return res.status(400).json({ success: false, message: 'Invalid user type' });
    }
    const result = await pool.query(
      `DELETE FROM user_lockouts WHERE user_type = $1 AND user_id = $2 AND locked_until > NOW() RETURNING id, phone, reason`,
      [type, parseInt(id, 10)]
    );
    if (!result.rows.length) {
      return res.status(404).json({ success: false, message: 'No active lockout for this user' });
    }
    await recordAdminAction(null, {
      actor_type: 'admin',
      actor_email: req.admin.email,
      action: 'lockout_cleared',
      target_type: type,
      target_id: parseInt(id, 10),
      details: { lockout_id: result.rows[0].id, reason: result.rows[0].reason, phone: result.rows[0].phone }
    });
    res.json({ success: true, cleared: result.rows[0] });
  } catch (err) {
    console.error('[admin/lockout-clear]', err);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

app.get('/api/admin/processors', requireAdmin, async (req, res) => {
  try {
    const result = await pool.query(`SELECT id, name, company, email, phone, city, region, is_active, created_at FROM processors ORDER BY created_at DESC`);
    res.json({ success: true, processors: result.rows });
  } catch (err) { res.status(500).json({ success: false, message: 'Server error' }); }
});

app.put('/api/admin/processors/:id', requireAdmin, async (req, res) => {
  try {
    const { name, company, email, password, is_active, is_flagged } = req.body;
    const fields = [], params = [];
    if (name !== undefined) { params.push(name); fields.push(`name=$${params.length}`); }
    if (company !== undefined) { params.push(company); fields.push(`company=$${params.length}`); }
    if (email !== undefined) { params.push(email.toLowerCase()); fields.push(`email=$${params.length}`); }
    if (is_active !== undefined) { params.push(is_active); fields.push(`is_active=$${params.length}`); }
    if (is_flagged !== undefined) { params.push(is_flagged); fields.push(`is_flagged=$${params.length}`); }
    if (password) { const h = await hashPassword(password); params.push(h); fields.push(`password_hash=$${params.length}`); }
    if (!fields.length) return res.status(400).json({ success: false, message: 'Nothing to update' });
    params.push(req.params.id);
    const result = await pool.query(`UPDATE processors SET ${fields.join(',')} WHERE id=$${params.length} RETURNING id, name, company, email, is_active`, params);
    if (!result.rows.length) return res.status(404).json({ success: false, message: 'Not found' });
    await recordAdminAction(null, {
      actor_type: 'admin', actor_email: req.admin.email,
      action: 'processor_updated', target_type: 'processor',
      target_id: parseInt(req.params.id, 10),
      details: { updated_fields: fields.map(f => f.split('=')[0]).filter(f => f !== 'password' && f !== 'password_hash') }
    });
    res.json({ success: true, processor: result.rows[0] });
  } catch (err) { console.error('[processors PUT]', err); res.status(500).json({ success: false, message: 'Server error' }); }
});

app.get('/api/admin/converters', requireAdmin, async (req, res) => {
  try {
    const result = await pool.query(`SELECT id, name, company, email, phone, city, region, country, is_active, created_at FROM converters ORDER BY created_at DESC`);
    res.json({ success: true, converters: result.rows });
  } catch (err) { res.status(500).json({ success: false, message: 'Server error' }); }
});

app.put('/api/admin/converters/:id', requireAdmin, async (req, res) => {
  try {
    const { name, company, email, password, is_active, is_flagged } = req.body;
    const fields = [], params = [];
    if (name !== undefined) { params.push(name); fields.push(`name=$${params.length}`); }
    if (company !== undefined) { params.push(company); fields.push(`company=$${params.length}`); }
    if (email !== undefined) { params.push(email.toLowerCase()); fields.push(`email=$${params.length}`); }
    if (is_active !== undefined) { params.push(is_active); fields.push(`is_active=$${params.length}`); }
    if (is_flagged !== undefined) { params.push(is_flagged); fields.push(`is_flagged=$${params.length}`); }
    if (password) { const h = await hashPassword(password); params.push(h); fields.push(`password_hash=$${params.length}`); }
    if (!fields.length) return res.status(400).json({ success: false, message: 'Nothing to update' });
    params.push(req.params.id);
    const result = await pool.query(`UPDATE converters SET ${fields.join(',')} WHERE id=$${params.length} RETURNING id, name, company, email, is_active`, params);
    if (!result.rows.length) return res.status(404).json({ success: false, message: 'Not found' });
    res.json({ success: true, converter: result.rows[0] });
  } catch (err) { res.status(500).json({ success: false, message: 'Server error' }); }
});

app.get('/api/admin/recyclers', requireAdmin, async (req, res) => {
  try {
    const result = await pool.query(`SELECT id, name, company, email, phone, city, region, country, is_active, created_at FROM recyclers ORDER BY created_at DESC`);
    res.json({ success: true, recyclers: result.rows });
  } catch (err) { res.status(500).json({ success: false, message: 'Server error' }); }
});

app.put('/api/admin/recyclers/:id', requireAdmin, async (req, res) => {
  try {
    const { name, company, email, password, is_active, is_flagged } = req.body;
    const fields = [], params = [];
    if (name !== undefined) { params.push(name); fields.push(`name=$${params.length}`); }
    if (company !== undefined) { params.push(company); fields.push(`company=$${params.length}`); }
    if (email !== undefined) { params.push(email.toLowerCase()); fields.push(`email=$${params.length}`); }
    if (is_active !== undefined) { params.push(is_active); fields.push(`is_active=$${params.length}`); }
    if (is_flagged !== undefined) { params.push(is_flagged); fields.push(`is_flagged=$${params.length}`); }
    if (password) { const h = await hashPassword(password); params.push(h); fields.push(`password_hash=$${params.length}`); }
    if (!fields.length) return res.status(400).json({ success: false, message: 'Nothing to update' });
    params.push(req.params.id);
    const result = await pool.query(`UPDATE recyclers SET ${fields.join(',')} WHERE id=$${params.length} RETURNING id, name, company, email, is_active`, params);
    if (!result.rows.length) return res.status(404).json({ success: false, message: 'Not found' });
    res.json({ success: true, recycler: result.rows[0] });
  } catch (err) { res.status(500).json({ success: false, message: 'Server error' }); }
});

// ============================================
// POSTED PRICES
// ============================================

app.post('/api/prices', async (req, res) => {
  try {
    const { poster_type, poster_id, operator_id, material_type, price_per_kg_usd, price_per_kg_ghs, usd_to_ghs_rate } = req.body;
    const resolvedPosterType = poster_type || 'aggregator';
    const resolvedPosterId   = poster_id || operator_id;
    if (!resolvedPosterId || !material_type || (!price_per_kg_usd && !price_per_kg_ghs)) return res.status(400).json({ success: false, message: 'poster_id, material_type, and price required' });
    const validMaterials = ['PET','HDPE','LDPE','PP'];
    if (!validMaterials.includes(material_type.toUpperCase())) return res.status(400).json({ success: false, message: 'Invalid material type' });
    const now = new Date();
    const expiresAt = new Date(now.getFullYear(), now.getMonth()+1, 0, 23, 59, 59);
    let city = null, region = null, country = 'Ghana';
    const tbl = CirculRoles.TABLE_MAP[resolvedPosterType];
    if (tbl) {
      const row = await pool.query(`SELECT city, region, country FROM ${tbl} WHERE id=$1`, [resolvedPosterId]);
      if (row.rows.length) { city = row.rows[0].city; region = row.rows[0].region; country = row.rows[0].country; }
    }
    const result = await pool.query(`INSERT INTO posted_prices (poster_type, poster_id, material_type, price_per_kg_usd, price_per_kg_ghs, usd_to_ghs_rate, city, region, country, expires_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) ON CONFLICT (poster_type, poster_id, material_type) DO UPDATE SET price_per_kg_usd=$4, price_per_kg_ghs=$5, usd_to_ghs_rate=$6, posted_at=NOW(), is_active=true, expires_at=$10 RETURNING *`, [resolvedPosterType, resolvedPosterId, material_type.toUpperCase(), price_per_kg_usd ? parseFloat(price_per_kg_usd) : null, price_per_kg_ghs ? parseFloat(price_per_kg_ghs) : null, usd_to_ghs_rate ? parseFloat(usd_to_ghs_rate) : null, city, region, country||'Ghana', expiresAt.toISOString()]);
    res.json({ success: true, price: result.rows[0] });
  } catch (err) { console.error('Post price error:', err); res.status(500).json({ success: false, message: 'Server error' }); }
});

app.get('/api/prices', async (req, res) => {
  try {
    const { role, material, city } = req.query;
    let posterTypes = CirculRoles.getPosterTypes(role);
    const params = [posterTypes];
    let whereExtra = '';
    if (material) { params.push(material.toUpperCase()); whereExtra += ` AND pp.material_type=$${params.length}`; }
    let nearPrices = { rows: [] };
    if (city) {
      const nearParams = [...params, city];
      nearPrices = await pool.query(`SELECT pp.material_type, pp.price_per_kg_ghs, pp.posted_at as updated_at, pp.poster_type as operator_role, pp.city, pp.poster_id as aggregator_id, CASE pp.poster_type WHEN 'aggregator' THEN (SELECT name FROM aggregators WHERE id=pp.poster_id LIMIT 1) WHEN 'processor' THEN (SELECT COALESCE(company, name) FROM processors WHERE id=pp.poster_id LIMIT 1) WHEN 'recycler' THEN (SELECT name FROM recyclers WHERE id=pp.poster_id LIMIT 1) WHEN 'converter' THEN (SELECT name FROM converters WHERE id=pp.poster_id LIMIT 1) END as operator_name FROM posted_prices pp WHERE pp.poster_type=ANY($1) AND pp.is_active=true AND pp.city=$${nearParams.length}${whereExtra} ORDER BY pp.material_type, pp.price_per_kg_ghs DESC`, nearParams);
    }
    const allPrices = await pool.query(`SELECT pp.material_type, pp.price_per_kg_ghs, pp.posted_at as updated_at, pp.poster_type as operator_role, pp.city, pp.poster_id as aggregator_id, CASE pp.poster_type WHEN 'aggregator' THEN (SELECT name FROM aggregators WHERE id=pp.poster_id LIMIT 1) WHEN 'processor' THEN (SELECT COALESCE(company, name) FROM processors WHERE id=pp.poster_id LIMIT 1) WHEN 'recycler' THEN (SELECT name FROM recyclers WHERE id=pp.poster_id LIMIT 1) WHEN 'converter' THEN (SELECT name FROM converters WHERE id=pp.poster_id LIMIT 1) END as operator_name FROM posted_prices pp WHERE pp.poster_type=ANY($1) AND pp.is_active=true${whereExtra} ORDER BY pp.material_type, pp.price_per_kg_ghs DESC`, params);
    const nationalAvg = await pool.query(`SELECT material_type, AVG(price_per_kg_ghs) as avg_usd, COUNT(DISTINCT poster_id) as buyer_count FROM posted_prices WHERE poster_type=ANY($1) AND is_active=true${whereExtra.replace(/pp\./g,'')} GROUP BY material_type ORDER BY material_type`, params);
    let nearRows = nearPrices.rows;
    if (nearRows.length === 0 && allPrices.rows.length > 0) {
      nearRows = allPrices.rows.slice(0, 10);
    }
    res.json({ success: true, near_prices: nearRows, national_averages: nationalAvg.rows, all_prices: allPrices.rows });
  } catch (err) { console.error('Get prices error:', err); res.status(500).json({ success: false, message: 'Server error' }); }
});

app.get('/api/market-prices', async (req, res) => {
  try {
    const result = await pool.query(`SELECT pp.material_type, pp.price_per_kg_ghs, pp.posted_at as updated_at, (SELECT name FROM aggregators WHERE id=pp.poster_id LIMIT 1) as buyer_name FROM posted_prices pp WHERE pp.poster_type='aggregator' AND pp.is_active=true ORDER BY pp.material_type, pp.price_per_kg_ghs DESC`);
    const best = {};
    for (const row of result.rows) {
      if (!best[row.material_type] || parseFloat(row.price_per_kg_ghs) > parseFloat(best[row.material_type].price_per_kg_ghs)) best[row.material_type] = row;
    }
    res.json({ success: true, prices: Object.values(best), all_aggregator_prices: result.rows });
  } catch (err) { res.status(500).json({ success: false, message: 'Server error' }); }
});

// ============================================
// COLLECTOR PASSPORT
// ============================================

app.get('/api/collectors/:id/passport', async (req, res) => {
  try {
    const { id } = req.params;
    const collector = await pool.query(`SELECT * FROM collectors WHERE id=$1`, [id]);
    if (!collector.rows.length) return res.status(404).json({ success: false, message: 'Collector not found' });
    const c = collector.rows[0];
    const twelveMonthsAgo = new Date(); twelveMonthsAgo.setFullYear(twelveMonthsAgo.getFullYear()-1);
    const [totals, last12m, byMaterial, aggregators, recent, ratingsReceived] = await Promise.all([
      pool.query(`SELECT COALESCE(SUM(net_weight_kg),0) as total_kg, COALESCE(SUM(total_price),0) as total_earned_ghs, COUNT(*) as txn_count, MIN(transaction_date) as active_since FROM transactions WHERE collector_id=$1`, [id]),
      pool.query(`SELECT COALESCE(SUM(net_weight_kg),0) as kg_12m, COALESCE(SUM(total_price),0) as earned_12m, COUNT(*) as txns_12m FROM transactions WHERE collector_id=$1 AND transaction_date>=$2`, [id, twelveMonthsAgo.toISOString()]),
      pool.query(`SELECT material_type, SUM(net_weight_kg) as kg, SUM(total_price) as earned, COUNT(*) as txns FROM transactions WHERE collector_id=$1 GROUP BY material_type ORDER BY kg DESC`, [id]),
      pool.query(`SELECT DISTINCT a.id, a.name, a.company, a.city FROM aggregators a JOIN transactions t ON t.aggregator_id=a.id WHERE t.collector_id=$1`, [id]),
      pool.query(`SELECT t.*, a.name as aggregator_name FROM transactions t LEFT JOIN aggregators a ON a.id=t.aggregator_id WHERE t.collector_id=$1 ORDER BY t.transaction_date DESC LIMIT 20`, [id]),
      pool.query(`SELECT AVG(rating)::NUMERIC(3,2) as avg, COUNT(*) as count FROM ratings WHERE rated_id=$1 AND rated_type='collector'`, [id]).catch(() => ({ rows: [{ avg: null, count: 0 }] }))
    ]);
    res.json({ success: true, passport: { collector: c, total_kg_lifetime: parseFloat(totals.rows[0].total_kg), total_kg_last_12m: parseFloat(last12m.rows[0].kg_12m), total_earned_ghs: parseFloat(totals.rows[0].total_earned_ghs), transaction_count: parseInt(totals.rows[0].txn_count), active_since: totals.rows[0].active_since, material_breakdown: byMaterial.rows, aggregators_transacted_with: aggregators.rows, unique_aggregator_count: aggregators.rows.length, avg_rating_from_aggregators: ratingsReceived.rows[0].avg, ratings_count: parseInt(ratingsReceived.rows[0].count), recent_transactions: recent.rows } });
  } catch (err) { console.error('Passport error:', err); res.status(500).json({ success: false, message: 'Server error' }); }
});

// ============================================
// COMPLIANCE REPORTS
// ============================================

app.get('/api/reports/compliance/:aggregator_id', async (req, res) => {
  try {
    const { aggregator_id } = req.params;
    const { start_date, end_date, format = 'json' } = req.query;
    const agg = await pool.query(`SELECT * FROM aggregators WHERE id=$1`, [aggregator_id]);
    if (!agg.rows.length) return res.status(404).json({ success: false, message: 'Aggregator not found' });
    let dateFilter = ''; const params = [aggregator_id];
    if (start_date) { params.push(start_date); dateFilter += ` AND t.transaction_date>=$${params.length}::timestamptz`; }
    if (end_date) { params.push(end_date); dateFilter += ` AND t.transaction_date<=$${params.length}::timestamptz`; }
    const transactions = await pool.query(`SELECT t.id, t.transaction_date, t.material_type, t.gross_weight_kg, t.net_weight_kg, t.contamination_deduction_percent, t.price_per_kg, t.total_price, t.payment_status, t.lat, t.lng, 'COL-' || LPAD(c.id::text, 4, '0') AS collector_name, c.city as collector_city, c.region as collector_region FROM transactions t JOIN collectors c ON c.id=t.collector_id WHERE t.aggregator_id=$1 ${dateFilter} ORDER BY t.transaction_date ASC`, params);
    const summary = await pool.query(`SELECT material_type, COUNT(*) as transaction_count, SUM(net_weight_kg) as total_kg_net, SUM(gross_weight_kg) as total_kg_gross, SUM(total_price) as total_paid_ghs, COUNT(DISTINCT t.collector_id) as unique_collectors FROM transactions t WHERE t.aggregator_id=$1 ${dateFilter} GROUP BY material_type ORDER BY material_type`, params);
    const report = { report_type: 'EPR_CSRD_COMPLIANCE', generated_at: new Date().toISOString(), aggregator: agg.rows[0], period: { start: start_date||'all-time', end: end_date||new Date().toISOString() }, summary_by_material: summary.rows, total_transactions: transactions.rows.length, transactions: transactions.rows, '@context': 'https://schema.org', '@type': 'DigitalProductPassport' };
    if (format === 'json') res.setHeader('Content-Disposition', `attachment; filename="compliance-report-${aggregator_id}-${Date.now()}.json"`);
    res.json(report);
  } catch (err) { console.error('Compliance report error:', err); res.status(500).json({ success: false, message: 'Server error' }); }
});

// ============================================
// PUBLIC PRODUCT PASSPORT — /api/trace/:batch_id
// ============================================
//
// Walks the chain-of-custody junction (pending_transaction_sources) from a
// leaf pending_transaction back to its root collector drop-offs, returning a
// stage-shaped JSON for the public passport page at /trace/:batch_id.
//
// Read-side mirror of shared/chain-of-custody-db.js's write pattern. The
// junction is authoritative for full provenance (mass balance), batch_id is
// the dominant-source lineage pointer used to discover the leaf row.
//
// Public, no auth: every viewer is an anonymous consumer scanning a QR off a
// converter's pellet bag. URL is UUID-keyed (not enumerable), name privacy
// preserves collector identity (codes only) but exposes business names for
// upper tiers.
const TRACE_UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const STAGE_TYPE_BY_BUYER = {
  aggregator: 'aggregation',
  processor:  'processing',
  recycler:   'recycling',
  converter:  'conversion'
};

// Resolve { kind, id } for the actor of a pending_transactions row. For
// collector_sale and aggregator_purchase the actor is the seller (collector,
// the one who deposited the material). For all other types, the actor is the
// buyer (who took custody at this stage). Routes through resolveSeller /
// resolveBuyer so FK conventions stay in shared/transaction-parties.js.
// Returns null on any resolution failure (lenient — the passport must keep
// rendering even on slightly malformed historical rows).
function _traceActorFor(row) {
  try {
    if (row.transaction_type === 'collector_sale' || row.transaction_type === 'aggregator_purchase') {
      // Declared-stock root: aggregator_purchase with no collector_id. The
      // aggregator is the party that possesses the material at this stage.
      if (row.transaction_type === 'aggregator_purchase' && row.collector_id == null) {
        if (row.aggregator_id == null) return null;
        return { kind: 'aggregator', id: Number(row.aggregator_id) };
      }
      return resolveSeller(row);
    }
    return resolveBuyer(row);
  } catch (_) { return null; }
}

// Map transaction_type + actor.kind → stage_type label shown on the passport.
// collector_sale / aggregator_purchase render as 'collection' regardless of
// buyer kind — they represent the initial drop-off into the system.
function _traceStageType(row, actor) {
  if (row.transaction_type === 'collector_sale' || row.transaction_type === 'aggregator_purchase') {
    return 'collection';
  }
  if (actor && STAGE_TYPE_BY_BUYER[actor.kind]) return STAGE_TYPE_BY_BUYER[actor.kind];
  return 'stage';
}

app.get('/api/trace/:batch_id', async (req, res) => {
  const batch_id = req.params.batch_id;
  if (!TRACE_UUID_RE.test(batch_id)) {
    return res.status(400).json({ success: false, message: 'Invalid batch id' });
  }
  try {
    // 1. Find the leaf pending_transaction for this batch_id.
    //    "Leaf" = the most-recent row carrying that batch_id — typically the
    //    downstream-most stage. Tie-break by id DESC for determinism when two
    //    rows share a created_at timestamp (test seeds, replay windows).
    const leafResult = await pool.query(
      `SELECT id FROM pending_transactions
        WHERE batch_id = $1
        ORDER BY created_at DESC, id DESC
        LIMIT 1`,
      [batch_id]
    );
    if (!leafResult.rows.length) {
      return res.status(404).json({ success: false, message: 'Batch not found' });
    }
    const leafId = Number(leafResult.rows[0].id);

    // 2. Recursive CTE walks ancestors upward via pending_transaction_sources.
    //    UNION (not UNION ALL) dedupes nodes reachable through multiple paths
    //    (DAG, not strict tree — same source can feed two downstreams that both
    //    end up in this lineage). Returns full row + depth from leaf.
    const lineageResult = await pool.query(`
      WITH RECURSIVE ancestors AS (
        SELECT id, 0 AS depth FROM pending_transactions WHERE id = $1
        UNION
        SELECT s.source_pending_tx_id, a.depth + 1
          FROM ancestors a
          JOIN pending_transaction_sources s ON s.child_pending_tx_id = a.id
      )
      SELECT pt.id, pt.transaction_type, pt.material_type,
             pt.gross_weight_kg, pt.created_at, pt.batch_id,
             pt.collector_id, pt.aggregator_id, pt.processor_id,
             pt.recycler_id, pt.converter_id,
             a.depth
        FROM ancestors a
        JOIN pending_transactions pt ON pt.id = a.id
    `, [leafId]);
    const lineage = lineageResult.rows;
    const byId = new Map(lineage.map(r => [Number(r.id), r]));

    // 3. Junction edges within lineage. Used both for the linear-chain walk
    //    (back from leaf, stop at the first commingling fork) and for each
    //    stage's sources block.
    const lineageIds = lineage.map(r => Number(r.id));
    const edgeResult = await pool.query(
      `SELECT child_pending_tx_id, source_pending_tx_id, weight_kg_attributed
         FROM pending_transaction_sources
        WHERE child_pending_tx_id = ANY($1::int[])
        ORDER BY weight_kg_attributed DESC, source_pending_tx_id ASC`,
      [lineageIds]
    );
    const edgesByChild = new Map();
    for (const e of edgeResult.rows) {
      const k = Number(e.child_pending_tx_id);
      if (!edgesByChild.has(k)) edgesByChild.set(k, []);
      edgesByChild.get(k).push(e);
    }

    // 4. Build the linear chain from leaf upward.
    //    At each step, walk to the single source iff the current stage has
    //    exactly one source. Multiple sources = commingling fork; we stop and
    //    show those as the current stage's expandable sources rather than
    //    fanning out into N separate Stage-1 cards. Single-collector chains
    //    walk all the way to the collector_sale root and render Stage 1.
    const linearChain = [];
    const seenInChain = new Set();
    let cursor = byId.get(leafId);
    while (cursor && !seenInChain.has(Number(cursor.id))) {
      linearChain.unshift(cursor); // root-first order on output
      seenInChain.add(Number(cursor.id));
      const parents = edgesByChild.get(Number(cursor.id)) || [];
      if (parents.length === 1) {
        cursor = byId.get(Number(parents[0].source_pending_tx_id)) || null;
      } else {
        // 0 sources → root; >1 sources → commingling fork, stop walking
        cursor = null;
      }
    }

    // 5. Resolve actors in batch by kind (one query per role table).
    //    Includes actors for chain rows AND for sources (so the sources block
    //    can show display_name/city for each contributor without N+1).
    const sourceRowIds = new Set();
    for (const e of edgeResult.rows) sourceRowIds.add(Number(e.source_pending_tx_id));
    const allRowsToResolve = new Set(lineage.map(r => Number(r.id)));
    for (const id of sourceRowIds) allRowsToResolve.add(id);

    const idsByKind = { collector: new Set(), aggregator: new Set(), processor: new Set(), recycler: new Set(), converter: new Set() };
    const actorByRowId = new Map();
    for (const id of allRowsToResolve) {
      const row = byId.get(id);
      if (!row) continue;
      const a = _traceActorFor(row);
      if (a && idsByKind[a.kind]) {
        idsByKind[a.kind].add(Number(a.id));
        actorByRowId.set(id, a);
      }
    }

    const partyByKey = new Map(); // key: `${kind}:${id}` → { display_name, city, region }
    async function loadParties(kind) {
      const ids = Array.from(idsByKind[kind]);
      if (!ids.length) return;
      let sql;
      if (kind === 'collector') {
        // Code only — never first/last name on a public passport.
        sql = `SELECT id, 'COL-' || LPAD(id::text, 4, '0') AS display_name, city, region FROM collectors WHERE id = ANY($1::int[])`;
      } else {
        const cfg = KIND_TO_TABLE[kind];
        // Business names — company falls back to name.
        sql = `SELECT id, COALESCE(company, name) AS display_name, city, region FROM ${cfg.table} WHERE id = ANY($1::int[])`;
      }
      const r = await pool.query(sql, [ids]);
      for (const row of r.rows) partyByKey.set(kind + ':' + Number(row.id), { display_name: row.display_name, city: row.city, region: row.region });
    }
    await Promise.all(['collector','aggregator','processor','recycler','converter'].map(loadParties));

    function actorJson(actor) {
      if (!actor) return null;
      const p = partyByKey.get(actor.kind + ':' + actor.id) || { display_name: null, city: null, region: null };
      return { kind: actor.kind, id: actor.id, display_name: p.display_name, city: p.city, region: p.region };
    }

    // 6. Build stage objects in chain order (root→leaf).
    const stages = linearChain.map((row, idx) => {
      const sourceEdges = edgesByChild.get(Number(row.id)) || [];
      const actor = actorByRowId.get(Number(row.id));
      const stage_type = _traceStageType(row, actor);
      const weight_in_kg = sourceEdges.length
        ? Math.round(sourceEdges.reduce((s, e) => s + parseFloat(e.weight_kg_attributed), 0) * 100) / 100
        : null;
      const stage = {
        stage_number: idx + 1,
        stage_type: stage_type,
        date: row.created_at ? row.created_at.toISOString().slice(0, 10) : null,
        actor: actorJson(actor),
        material_type: row.material_type,
        weight_in_kg: weight_in_kg,
        weight_out_kg: row.gross_weight_kg != null ? Math.round(parseFloat(row.gross_weight_kg) * 100) / 100 : null,
        // An aggregator_purchase root row with no collector_id is aggregator-declared
        // existing stock — origin untraced. Surface to the public passport.
        is_declared: row.transaction_type === 'aggregator_purchase' && row.collector_id == null,
        pending_tx_id: Number(row.id)
      };
      if (sourceEdges.length > 1) stage.commingled = true;
      if (sourceEdges.length > 0) {
        stage.sources = sourceEdges.map(e => {
          const srcRow = byId.get(Number(e.source_pending_tx_id));
          const srcActor = srcRow ? _traceActorFor(srcRow) : null;
          const p = srcActor ? (partyByKey.get(srcActor.kind + ':' + srcActor.id) || {}) : {};
          return {
            pending_tx_id: Number(e.source_pending_tx_id),
            display_name: p.display_name || null,
            city: p.city || null,
            weight_kg_attributed: Math.round(parseFloat(e.weight_kg_attributed) * 100) / 100
          };
        });
      }
      return stage;
    });

    // 7. Stats. collector_count = distinct collector ids across all root-stage
    //    pending_transactions in the lineage (collector_sale / aggregator_purchase
    //    rows — those are the only types that carry collector_id as the seller).
    const collectorIds = new Set();
    let earliestRootDate = null;
    for (const row of lineage) {
      if ((row.transaction_type === 'collector_sale' || row.transaction_type === 'aggregator_purchase') && row.collector_id != null) {
        collectorIds.add(Number(row.collector_id));
        const t = row.created_at ? new Date(row.created_at).getTime() : null;
        if (t != null && (earliestRootDate == null || t < earliestRootDate)) earliestRootDate = t;
      }
    }
    const leafRow = byId.get(leafId);
    const leafTime = leafRow && leafRow.created_at ? new Date(leafRow.created_at).getTime() : null;
    let journey_days = null;
    if (earliestRootDate != null && leafTime != null) {
      journey_days = Math.max(0, Math.round((leafTime - earliestRootDate) / (1000 * 60 * 60 * 24)));
    }

    const leafActor = actorByRowId.get(leafId);
    const leafActorJson = actorJson(leafActor);

    res.json({
      success: true,
      leaf: {
        pending_tx_id: leafId,
        material_type: leafRow.material_type,
        final_weight_kg: leafRow.gross_weight_kg != null ? Math.round(parseFloat(leafRow.gross_weight_kg) * 100) / 100 : null,
        batch_id: leafRow.batch_id,
        produced_by: leafActorJson,
        shipped_at: leafRow.created_at ? leafRow.created_at.toISOString() : null
      },
      stats: {
        stages: stages.length,
        journey_days: journey_days,
        collector_count: collectorIds.size
      },
      stages: stages
    });
  } catch (err) {
    console.error('GET /api/trace/:batch_id error:', err);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

app.get('/api/reports/product-journey/:transaction_id', async (req, res) => {
  try {
    const { transaction_id } = req.params;
    const result = await pool.query(`SELECT t.*, 'COL-' || LPAD(c.id::text, 4, '0') AS collector_identifier, c.city as collector_city, c.region as collector_region, a.name as aggregator_name, a.company as aggregator_company, 'AGG-' || LPAD(a.id::text, 4, '0') AS aggregator_identifier, a.city as aggregator_city FROM transactions t JOIN collectors c ON c.id=t.collector_id LEFT JOIN aggregators a ON a.id=t.aggregator_id WHERE t.id=$1`, [transaction_id]);
    if (!result.rows.length) return res.status(404).json({ success: false, message: 'Transaction not found' });
    const t = result.rows[0];
    res.json({ success: true, journey: { collector: { id: t.collector_identifier, city: t.collector_city, region: t.collector_region }, material: t.material_type, weight_kg: t.net_weight_kg, collected_at: t.transaction_date, location: t.lat ? { lat: t.lat, lng: t.lng } : { city: t.collector_city }, aggregator: t.aggregator_name ? { id: t.aggregator_identifier, name: t.aggregator_name, company: t.aggregator_company, city: t.aggregator_city } : null, verified: t.payment_status === 'paid' } });
  } catch (err) { res.status(500).json({ success: false, message: 'Server error' }); }
});

// ============================================
// CODE EXPORT
// ============================================

app.get('/code-export.txt', (req, res) => {
  const exportFiles = ['server.js','migrate.js','package.json','render.yaml','.gitignore','.nvmrc','README.md','public/index.html','public/collect.html','public/login.html','public/admin.html','public/collector-dashboard.html','public/aggregator-dashboard.html','public/processor-dashboard.html','public/converter-dashboard.html','public/report.html'];
  let output = `CIRCUL CODEBASE EXPORT\nGenerated: ${new Date().toISOString()}\n\n`;
  for (const filePath of exportFiles) {
    const fullPath = path.join(__dirname, filePath);
    output += `\n===== FILE: ${filePath} =====\n\n`;
    if (fs.existsSync(fullPath)) { try { output += fs.readFileSync(fullPath, 'utf8'); } catch (err) { output += `[Error reading file: ${err.message}]\n`; } } else output += `[File not found]\n`;
    output += '\n';
  }
  res.setHeader('Content-Type', 'text/plain; charset=utf-8');
  res.setHeader('Content-Disposition', 'attachment; filename="circul-codebase-export.txt"');
  res.send(output);
});

// ============================================
// PAGE ROUTES
// ============================================

app.get('/', (req, res) => {
  const slug = process.env.POLSIA_ANALYTICS_SLUG || '';
  const htmlPath = path.join(__dirname, 'public', 'index.html');
  if (fs.existsSync(htmlPath)) { let html = fs.readFileSync(htmlPath, 'utf8'); html = html.replace('__POLSIA_SLUG__', slug); res.type('html').send(html); }
  else res.json({ message: 'Hello from Circul!' });
});

app.get('/collect',              (req, res) => res.sendFile(path.join(__dirname, 'public', 'collect.html')));
app.get('/dashboard',            (req, res) => res.redirect(301, '/aggregator-dashboard.html'));
app.get('/admin',                (req, res) => res.sendFile(path.join(__dirname, 'public', 'admin.html')));
app.get('/collector-dashboard',  (req, res) => res.sendFile(path.join(__dirname, 'public', 'collector-dashboard.html')));
app.get('/aggregator-dashboard', (req, res) => res.sendFile(path.join(__dirname, 'public', 'aggregator-dashboard.html')));
app.get('/processor-dashboard',  (req, res) => res.sendFile(path.join(__dirname, 'public', 'processor-dashboard.html')));
app.get('/converter-dashboard',  (req, res) => res.sendFile(path.join(__dirname, 'public', 'converter-dashboard.html')));
app.get('/recycler-dashboard',  (req, res) => res.sendFile(path.join(__dirname, 'public', 'recycler-dashboard.html')));
app.get('/report',               (req, res) => res.sendFile(path.join(__dirname, 'public', 'report.html')));
app.get('/passport',             (req, res) => res.sendFile(path.join(__dirname, 'public', 'report.html')));
app.get('/collector-passport/:id', (req, res) => res.sendFile(path.join(__dirname, 'public', 'collector-passport.html')));
app.get('/trace/:batch_id',      (req, res) => res.sendFile(path.join(__dirname, 'public', 'trace.html')));
app.get('/login',                (req, res) => res.sendFile(path.join(__dirname, 'public', 'login.html')));
app.get('/register',             (req, res) => res.sendFile(path.join(__dirname, 'public', 'register.html')));
app.get('/prices',               (req, res) => res.redirect('/'));

// ============================================
// AUTH — REGISTER (collectors + aggregators)
// ============================================

app.post('/api/auth/register', async (req, res) => {
  const { role, phone, pin } = req.body;
  try {
    const hashedPin = await hashPassword(pin);
    if (role === 'collector') {
      const { first_name, last_name } = req.body;
      await pool.query(
        `INSERT INTO collectors (first_name, last_name, phone, pin, is_active) VALUES ($1, $2, $3, $4, true)`,
        [first_name, last_name, phone, hashedPin]
      );
    } else if (role === 'aggregator') {
      const { name, company } = req.body;
      await pool.query(
        `INSERT INTO aggregators (name, company, phone, pin, is_active) VALUES ($1, $2, $3, $4, true)`,
        [name, company, phone, hashedPin]
      );
    } else {
      return res.status(400).json({ success: false, message: 'Invalid role for self-registration' });
    }
    res.json({ success: true });
  } catch (err) {
    console.error('Register error:', err);
    res.status(500).json({ success: false, message: 'Registration failed' });
  }
});

// ============================================
// AUTH — REQUEST ACCESS (processors + converters)
// ============================================

app.post('/api/auth/request-access', async (req, res) => {
  const { role, name, company, email, phone } = req.body;
  try {
    const table = CirculRoles.TABLE_MAP[role];
    if (!table || !CirculRoles.PAID_ROLES.includes(role)) {
      return res.status(400).json({ success: false, message: 'Invalid role for access request' });
    }
    await pool.query(
      `INSERT INTO ${table} (name, company, email, phone, password_hash, is_active) VALUES ($1, $2, $3, $4, '', false)`,
      [name, company, email, phone || null]
    );
    res.json({ success: true });
  } catch (err) {
    console.error('Request access error:', err);
    res.status(500).json({ success: false, message: 'Request failed' });
  }
});

// ============================================
// ADMIN — PENDING / APPROVE / REJECT
// ============================================

app.get('/api/admin/pending', requireAdmin, async (req, res) => {
  try {
    const queries = CirculRoles.PAID_ROLES.map(role => {
      const table = CirculRoles.TABLE_MAP[role];
      return pool.query(`SELECT id, name, company, email, phone, created_at FROM ${table} WHERE is_active=false ORDER BY created_at DESC`)
        .then(res => res.rows.map(r => ({ ...r, role })));
    });
    const results = await Promise.all(queries);
    const result = results.flat();
    res.json(result);
  } catch (err) {
    console.error('Pending error:', err);
    res.status(500).json({ success: false, message: 'Failed to fetch pending requests' });
  }
});

app.post('/api/admin/approve', requireAdmin, async (req, res) => {
  const { id, role } = req.body;
  try {
    const table = CirculRoles.TABLE_MAP[role];
    if (!table) return res.status(400).json({ success: false, message: 'Invalid role' });
    await pool.query(`UPDATE ${table} SET is_active=true WHERE id=$1`, [id]);
    await recordAdminAction(null, {
      actor_type: 'admin', actor_email: req.admin.email,
      action: 'role_access_approved', target_type: role || 'unknown',
      target_id: parseInt(id, 10), details: { role: role }
    });
    res.json({ success: true });
  } catch (err) {
    console.error('Approve error:', err);
    res.status(500).json({ success: false, message: 'Approval failed' });
  }
});

app.post('/api/admin/reject', requireAdmin, async (req, res) => {
  const { id, role } = req.body;
  try {
    const table = CirculRoles.TABLE_MAP[role];
    if (!table) return res.status(400).json({ success: false, message: 'Invalid role' });
    await pool.query(`DELETE FROM ${table} WHERE id=$1`, [id]);
    await recordAdminAction(null, {
      actor_type: 'admin', actor_email: req.admin.email,
      action: 'role_access_rejected', target_type: role || 'unknown',
      target_id: parseInt(id, 10), details: { role: role }
    });
    res.json({ success: true });
  } catch (err) {
    console.error('Reject error:', err);
    res.status(500).json({ success: false, message: 'Rejection failed' });
  }
});

// ── Discovery Crons (hourly) ──────────────────────────────────────
async function runDiscoveryCrons() {
  try {
    // 1. Expire stale offers (pending > 48h)
    const expiredOffers = await pool.query(
      `UPDATE offers SET status = 'expired', responded_at = NOW()
       WHERE status = 'pending' AND created_at < NOW() - INTERVAL '48 hours'`
    );
    console.log(`[cron] Expired ${expiredOffers.rowCount} stale offers`);

    // 2. Expire old listings (past expires_at, no pending offers)
    const expiredListings = await pool.query(
      `UPDATE listings SET status = 'expired', updated_at = NOW()
       WHERE status = 'active' AND expires_at < NOW()
       AND id NOT IN (SELECT DISTINCT listing_id FROM offers WHERE status = 'pending')`
    );
    console.log(`[cron] Expired ${expiredListings.rowCount} stale listings`);

    // 3. Renewal reminders (listings expiring within 24h)
    const expiringSoon = await pool.query(
      `SELECT id, seller_id, seller_role, material_type, quantity_kg
       FROM listings
       WHERE status = 'active'
       AND expires_at BETWEEN NOW() AND NOW() + INTERVAL '24 hours'
       AND id NOT IN (SELECT DISTINCT listing_id FROM offers WHERE status = 'pending')`
    );
    console.log(`[cron] ${expiringSoon.rowCount} listings expiring within 24h`);
  } catch (err) {
    console.error('[cron] Discovery cron error:', err.message);
  }
}

setInterval(runDiscoveryCrons, 60 * 60 * 1000);
// Run once on startup after a short delay
setTimeout(runDiscoveryCrons, 10000);

// ── Production Error Logging ──

app.post('/api/error-log', async (req, res) => {
  try {
    const { source, dashboard, error_message, error_stack, url } = req.body;
    if (!error_message) return res.status(400).json({ success: false, message: 'error_message required' });

    // Extract user info from token if present (but don't require it)
    let user_id = null, user_role = null;
    try {
      const token = req.headers.authorization?.replace('Bearer ', '');
      if (token) {
        const decoded = verifyToken(token, AUTH_SECRET);
        user_id = decoded.id;
        user_role = decoded.role;
      }
    } catch (e) { /* token invalid or missing — that's fine */ }

    await pool.query(
      `INSERT INTO error_log (source, dashboard, error_message, error_stack, url, user_id, user_role)
       VALUES ($1, $2, $3, $4, $5, $6, $7)`,
      [source || 'frontend', dashboard || null, (error_message || '').substring(0, 2000),
       (error_stack || '').substring(0, 5000), (url || '').substring(0, 500),
       user_id, user_role]
    );

    // Spike alert via ntfy.sh — check if errors are spiking (>5 distinct errors in last 10 min)
    if (!global._lastAlertAt || Date.now() - global._lastAlertAt > 10 * 60 * 1000) {
      try {
        const spike = await pool.query(
          `SELECT COUNT(DISTINCT error_message) as cnt
           FROM error_log WHERE created_at > NOW() - INTERVAL '10 minutes'`
        );
        if (parseInt(spike.rows[0].cnt) >= 5) {
          global._lastAlertAt = Date.now();
          fetch('https://ntfy.sh/' + (process.env.NTFY_TOPIC || 'circul-errors'), {
            method: 'POST',
            headers: { 'Title': 'Circul Error Spike', 'Priority': 'high', 'Tags': 'warning' },
            body: spike.rows[0].cnt + ' distinct errors in the last 10 min on '
                  + (dashboard || source || 'unknown') + '. Check /api/error-log?since=10m'
          }).catch(() => {});
        }
      } catch(e) { /* alert check failed — don't block the response */ }
    }

    res.json({ ok: true });
  } catch (e) {
    console.error('Error logging failed:', e.message);
    res.status(500).json({ success: false, message: 'logging failed' });
  }
});

app.get('/api/error-log', requireAuth, async (req, res) => {
  try {
    const since = req.query.since || '24h';
    const intervals = { '10m': '10 minutes', '1h': '1 hour', '6h': '6 hours', '24h': '24 hours', '48h': '48 hours', '7d': '7 days' };
    const interval = intervals[since] || '24 hours';
    const result = await pool.query(
      `SELECT source, dashboard, error_message, COUNT(*) as count,
              MAX(created_at) as last_seen, MIN(created_at) as first_seen
       FROM error_log
       WHERE created_at > NOW() - $1::interval
       GROUP BY source, dashboard, error_message
       ORDER BY count DESC
       LIMIT 100`,
      [interval]
    );
    res.json(result.rows);
  } catch (e) {
    res.status(500).json({ success: false, message: e.message });
  }
});

// ============================================
// BATCH 8b + 9: Ghana Card, Agents, Supply Requirements
// ============================================

// Serve agent dashboard
app.get('/agent-dashboard', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'agent-dashboard.html'));
});

// GET /api/profile/ghana-card — get current user's Ghana Card info
app.get('/api/profile/ghana-card', requireAuth, async (req, res) => {
  try {
    const role = req.user.role;
    const tableMap = { collector: 'collectors', aggregator: 'aggregators', agent: 'agents' };
    const table = tableMap[role];
    if (!table) return res.status(403).json({ success: false, message: 'Ghana Card not applicable for this role' });
    const result = await pool.query(
      `SELECT ghana_card, ghana_card_photo FROM ${table} WHERE id=$1`, [req.user.id]
    );
    if (!result.rows.length) return res.status(404).json({ success: false, message: 'User not found' });
    res.json({ success: true, ghana_card: result.rows[0].ghana_card, ghana_card_photo: result.rows[0].ghana_card_photo });
  } catch (err) { console.error(err); res.status(500).json({ success: false, message: 'Server error' }); }
});

// PATCH /api/profile/ghana-card — update Ghana Card number and/or photo
app.patch('/api/profile/ghana-card', requireAuth, async (req, res) => {
  try {
    const role = req.user.role;
    const tableMap = { collector: 'collectors', aggregator: 'aggregators', agent: 'agents' };
    const table = tableMap[role];
    if (!table) return res.status(403).json({ success: false, message: 'Ghana Card not applicable for this role' });
    const { ghana_card, ghana_card_photo } = req.body;
    if (ghana_card && !/^GHA-\d{9}-\d$/.test(ghana_card)) {
      return res.status(400).json({ success: false, message: 'Invalid Ghana Card format. Expected GHA-XXXXXXXXX-X' });
    }
    const sets = [];
    const vals = [];
    let idx = 1;
    if (ghana_card !== undefined) { sets.push(`ghana_card=$${idx++}`); vals.push(ghana_card); }
    if (ghana_card_photo !== undefined) { sets.push(`ghana_card_photo=$${idx++}`); vals.push(ghana_card_photo); }
    if (!sets.length) return res.status(400).json({ success: false, message: 'No fields to update' });
    vals.push(req.user.id);
    await pool.query(`UPDATE ${table} SET ${sets.join(', ')} WHERE id=$${idx}`, vals);
    res.json({ success: true });
  } catch (err) { console.error(err); res.status(500).json({ success: false, message: 'Server error' }); }
});

// GET /api/agents — aggregator gets their agents + per-agent 7-day kg for the field-agents table.
//
// The kg_7d subquery joins agent_activity -> transactions (related_type='transaction',
// action_type='collection') over the last 7 days and sums gross_weight_kg — gives
// aggregators a single-glance productivity read per agent. Default sort puts productive
// agents to the top; ties fall back to most-recently-registered.
app.get('/api/agents', requireAuth, async (req, res) => {
  try {
    if (req.user.role !== 'aggregator') return res.status(403).json({ success: false, message: 'Aggregators only' });
    const result = await pool.query(
      `SELECT a.id, a.first_name, a.last_name, a.phone, a.city, a.region, a.ghana_card, a.is_active, a.created_at,
              COALESCE((
                SELECT SUM(t.gross_weight_kg)
                FROM agent_activity aa
                JOIN transactions t ON aa.related_id = t.id AND aa.related_type = 'transaction'
                WHERE aa.agent_id = a.id
                  AND aa.action_type = 'collection'
                  AND aa.created_at >= NOW() - INTERVAL '7 days'
              ), 0)::float AS kg_7d
       FROM agents a
       WHERE a.aggregator_id = $1
       ORDER BY kg_7d DESC, a.created_at DESC`, [req.user.id]
    );
    res.json({ success: true, agents: result.rows });
  } catch (err) { console.error(err); res.status(500).json({ success: false, message: 'Server error' }); }
});

// POST /api/agents — aggregator registers a new agent
app.post('/api/agents', requireAuth, async (req, res) => {
  try {
    if (req.user.role !== 'aggregator') return res.status(403).json({ success: false, message: 'Aggregators only' });
    const { first_name, last_name, phone, pin, city, region, ghana_card } = req.body;
    if (!first_name || !last_name || !phone || !pin) {
      return res.status(400).json({ success: false, message: 'first_name, last_name, phone, pin required' });
    }
    if (pin.length < 4 || pin.length > 6 || !/^\d+$/.test(pin)) return res.status(400).json({ success: false, message: 'PIN must be 4-6 digits' });
    const hashedPin = await hashPassword(pin.trim());
    const result = await pool.query(
      `INSERT INTO agents (aggregator_id, first_name, last_name, phone, pin, city, region, ghana_card)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8) RETURNING id, first_name, last_name, phone, city`,
      [req.user.id, first_name.trim(), last_name.trim(), phone.trim(), hashedPin, city||null, region||null, ghana_card||null]
    );
    await pool.query(
      `INSERT INTO agent_activity (agent_id, aggregator_id, action_type, description)
       VALUES ($1,$2,'registered','Agent registered by aggregator')`,
      [result.rows[0].id, req.user.id]
    );
    res.status(201).json({ success: true, agent: result.rows[0] });
  } catch (err) {
    if (err.code === '23505') return res.status(409).json({ success: false, message: 'Phone number already registered' });
    console.error(err); res.status(500).json({ success: false, message: 'Server error' });
  }
});

// GET /api/agent/me — agent gets their own profile
app.get('/api/agent/me', requireAuth, async (req, res) => {
  try {
    if (req.user.role !== 'agent') return res.status(403).json({ success: false, message: 'Agents only' });
    const result = await pool.query(
      `SELECT a.id, a.first_name, a.last_name, a.phone, a.city, a.region, a.ghana_card, a.ghana_card_photo,
              a.aggregator_id, agg.name AS aggregator_name
       FROM agents a JOIN aggregators agg ON a.aggregator_id = agg.id
       WHERE a.id=$1`, [req.user.id]
    );
    if (!result.rows.length) return res.status(404).json({ success: false });
    res.json({ success: true, agent: result.rows[0] });
  } catch (err) { console.error(err); res.status(500).json({ success: false, message: 'Server error' }); }
});

// GET /api/agent/stats — hero stats for the agent dashboard.
//
// Replaces the placeholder fields (collections_today/month_kg/month_value) that
// /api/agent/me never actually populated — so the hero was always showing zeros.
// Addresses the "performance metrics" half of the 2026-04-22 post-deploy audit P1:
// adds kg_7d (pacing window between today and month) + collectors_reached_7d
// (relational dimension that kg volume alone doesn't capture) + collectors_new_7d
// (supporting subcount under the collectors tile). Single query, five scalar
// subqueries against agent_activity (+ transactions for kg/collector joins).
// No new table, no migration.
app.get('/api/agent/stats', requireAuth, async (req, res) => {
  try {
    if (req.user.role !== 'agent') return res.status(403).json({ success: false, message: 'Agents only' });
    const result = await pool.query(
      `SELECT
         COALESCE((
           SELECT COUNT(*) FROM agent_activity
           WHERE agent_id = $1 AND action_type = 'collection'
             AND created_at::date = CURRENT_DATE
         ), 0)::int AS collections_today,
         COALESCE((
           SELECT SUM(t.gross_weight_kg)
           FROM agent_activity aa
           JOIN transactions t ON aa.related_id = t.id AND aa.related_type = 'transaction'
           WHERE aa.agent_id = $1 AND aa.action_type = 'collection'
             AND aa.created_at >= NOW() - INTERVAL '7 days'
         ), 0)::float AS kg_7d,
         COALESCE((
           SELECT COUNT(DISTINCT t.collector_id)
           FROM agent_activity aa
           JOIN transactions t ON aa.related_id = t.id AND aa.related_type = 'transaction'
           WHERE aa.agent_id = $1 AND aa.action_type = 'collection'
             AND aa.created_at >= NOW() - INTERVAL '7 days'
         ), 0)::int AS collectors_reached_7d,
         COALESCE((
           SELECT COUNT(DISTINCT related_id) FROM agent_activity
           WHERE agent_id = $1
             AND action_type = 'registered_collector'
             AND related_type = 'collector'
             AND created_at >= NOW() - INTERVAL '7 days'
         ), 0)::int AS collectors_new_7d,
         COALESCE((
           SELECT SUM(t.gross_weight_kg)
           FROM agent_activity aa
           JOIN transactions t ON aa.related_id = t.id AND aa.related_type = 'transaction'
           WHERE aa.agent_id = $1 AND aa.action_type = 'collection'
             AND date_trunc('month', aa.created_at) = date_trunc('month', CURRENT_DATE)
         ), 0)::float AS month_kg`,
      [req.user.id]
    );
    res.json({ success: true, stats: result.rows[0] });
  } catch (err) {
    console.error('GET /api/agent/stats error:', err);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

// GET /api/agent/collectors — collectors known to the agent's parent aggregator.
//
// Why this endpoint exists: agent-dashboard previously populated its collector
// dropdown from /aggregators/:id/stats.top_collectors, which is an INNER JOIN
// on transactions and therefore excludes collectors who have been onboarded
// but have not yet completed a purchase. The 2026-04-22 audit caught this as
// "No collectors found" on agents whose parent aggregator had 2 active
// collectors. The union below surfaces collectors via EITHER a prior
// agent_activity touchpoint (registered-by-agent, logged-a-collection, etc.)
// OR a prior transaction against this aggregator — whichever exists first.
app.get('/api/agent/collectors', requireAuth, async (req, res) => {
  try {
    if (req.user.role !== 'agent') return res.status(403).json({ success: false, message: 'Agents only' });
    const aggId = req.user.aggregator_id;
    if (!aggId) return res.status(400).json({ success: false, message: 'Agent has no aggregator assignment' });
    const result = await pool.query(
      `SELECT DISTINCT c.id, c.first_name, c.last_name, c.phone, c.city,
              'COL-' || LPAD(c.id::text, 4, '0') AS display_name
       FROM collectors c
       WHERE c.is_active = true
         AND (
           c.id IN (SELECT related_id FROM agent_activity
                    WHERE aggregator_id = $1 AND related_type = 'collector' AND related_id IS NOT NULL)
           OR c.id IN (SELECT DISTINCT collector_id FROM transactions
                       WHERE aggregator_id = $1 AND collector_id IS NOT NULL)
         )
       ORDER BY c.first_name, c.last_name`,
      [aggId]
    );
    res.json({ success: true, collectors: result.rows });
  } catch (err) { console.error('GET /api/agent/collectors error:', err); res.status(500).json({ success: false, message: 'Server error' }); }
});

// POST /api/agent/log-collection — agent logs a collection
app.post('/api/agent/log-collection', requireAuth, async (req, res) => {
  try {
    if (req.user.role !== 'agent') return res.status(403).json({ success: false, message: 'Agents only' });
    const { collector_id, material_type, gross_weight_kg, price_per_kg } = req.body;
    if (!collector_id || !material_type || !gross_weight_kg) {
      return res.status(400).json({ success: false, message: 'collector_id, material_type, gross_weight_kg required' });
    }
    const total = parseFloat((gross_weight_kg * (price_per_kg || 0)).toFixed(2));
    const client = await pool.connect();
    let insertedId;
    try {
      await client.query('BEGIN');
      const { row } = await insertRootTransaction(client, {
        transaction_type: 'collector_sale',
        status: 'completed',
        collector_id: parseInt(collector_id),
        aggregator_id: req.user.aggregator_id,
        material_type: material_type,
        gross_weight_kg: parseFloat(gross_weight_kg),
        price_per_kg: parseFloat(price_per_kg || 0),
        total_price: total
      });
      insertedId = row.id;
      await client.query(
        `INSERT INTO agent_activity (agent_id, aggregator_id, action_type, description, related_id, related_type)
         VALUES ($1,$2,'collection',$3,$4,'transaction')`,
        [req.user.id, req.user.aggregator_id,
         `Logged ${gross_weight_kg} kg ${material_type} from collector ${collector_id}`,
         insertedId]
      );
      await client.query('COMMIT');
    } catch (e) {
      await client.query('ROLLBACK').catch(() => {});
      throw e;
    } finally {
      client.release();
    }
    res.status(201).json({ success: true, transaction_id: insertedId });
  } catch (err) { console.error(err); res.status(500).json({ success: false, message: 'Server error' }); }
});

// POST /api/agent/register-collector — agent registers a new collector
app.post('/api/agent/register-collector', requireAuth, async (req, res) => {
  try {
    if (req.user.role !== 'agent') return res.status(403).json({ success: false, message: 'Agents only' });
    const { first_name, last_name, phone, pin, city, region, ghana_card } = req.body;
    if (!first_name || !last_name || !phone || !pin) {
      return res.status(400).json({ success: false, message: 'first_name, last_name, phone, pin required' });
    }
    const hashedPin = await hashPassword(pin.trim());
    const result = await pool.query(
      `INSERT INTO collectors (first_name, last_name, phone, pin, city, ghana_card, region)
       VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING id, first_name, last_name, phone`,
      [first_name.trim(), last_name.trim(), phone.trim(), hashedPin, city||null, ghana_card||null, region||null]
    );
    await pool.query(
      `INSERT INTO agent_activity (agent_id, aggregator_id, action_type, description, related_id, related_type)
       VALUES ($1,$2,'registered_collector',$3,$4,'collector')`,
      [req.user.id, req.user.aggregator_id,
       `Registered collector ${first_name} ${last_name} (${phone})`,
       result.rows[0].id]
    );
    res.status(201).json({ success: true, collector: result.rows[0] });
  } catch (err) {
    if (err.code === '23505') return res.status(409).json({ success: false, message: 'Phone number already registered' });
    console.error(err); res.status(500).json({ success: false, message: 'Server error' });
  }
});

// GET /api/aggregator/agent-activity — aggregator sees agent activity feed
app.get('/api/aggregator/agent-activity', requireAuth, async (req, res) => {
  try {
    if (req.user.role !== 'aggregator') return res.status(403).json({ success: false, message: 'Aggregators only' });
    const result = await pool.query(
      `SELECT aa.*, a.first_name || ' ' || a.last_name AS agent_name
       FROM agent_activity aa JOIN agents a ON aa.agent_id = a.id
       WHERE aa.aggregator_id=$1 ORDER BY aa.created_at DESC LIMIT 50`, [req.user.id]
    );
    res.json({ success: true, activity: result.rows });
  } catch (err) { console.error(err); res.status(500).json({ success: false, message: 'Server error' }); }
});

// GET /api/supply-requirements — processor gets their own requirements; aggregator/collector/agent see downstream
app.get('/api/supply-requirements', requireAuth, async (req, res) => {
  try {
    if (!['processor','aggregator','collector','agent'].includes(req.user.role)) {
      return res.status(403).json({ success: false, message: 'Not applicable' });
    }
    let processorId;
    if (req.user.role === 'processor') {
      processorId = req.user.id;
    } else {
      processorId = req.query.processor_id;
      if (!processorId) {
        const result = await pool.query(
          `SELECT sr.id, sr.material_type, sr.accepted_forms, sr.accepted_colours,
                  sr.max_contamination_pct, sr.max_moisture_pct, sr.min_quantity_kg,
                  sr.price_premium_pct, sr.sorting_notes, sr.is_active,
                  p.company AS processor_name
           FROM supply_requirements sr JOIN processors p ON sr.processor_id = p.id
           WHERE sr.is_active = true ORDER BY sr.material_type`
        );
        return res.json({ success: true, requirements: result.rows });
      }
    }
    const includeClient = req.user.role === 'processor';
    const fields = `id, material_type, accepted_forms, accepted_colours, max_contamination_pct, max_moisture_pct, min_quantity_kg, price_premium_pct, sorting_notes, is_active${includeClient ? ', client_reference' : ''}`;
    const result = await pool.query(
      `SELECT ${fields} FROM supply_requirements WHERE processor_id=$1 AND is_active=true ORDER BY material_type`,
      [processorId]
    );
    res.json({ success: true, requirements: result.rows });
  } catch (err) { console.error(err); res.status(500).json({ success: false, message: 'Server error' }); }
});

// POST /api/supply-requirements — processor creates a requirement
app.post('/api/supply-requirements', requireAuth, async (req, res) => {
  try {
    if (req.user.role !== 'processor') return res.status(403).json({ success: false, message: 'Processors only' });
    const { material_type, accepted_forms, accepted_colours, max_contamination_pct, max_moisture_pct, min_quantity_kg, price_premium_pct, client_reference, sorting_notes } = req.body;
    if (!material_type || !accepted_forms || !accepted_forms.length) {
      return res.status(400).json({ success: false, message: 'material_type and accepted_forms required' });
    }
    const result = await pool.query(
      `INSERT INTO supply_requirements (processor_id, material_type, accepted_forms, accepted_colours, max_contamination_pct, max_moisture_pct, min_quantity_kg, price_premium_pct, client_reference, sorting_notes)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) RETURNING *`,
      [req.user.id, material_type, accepted_forms, accepted_colours||null, max_contamination_pct||null, max_moisture_pct||null, min_quantity_kg||null, price_premium_pct||null, client_reference||null, sorting_notes||null]
    );
    res.status(201).json({ success: true, requirement: result.rows[0] });
  } catch (err) { console.error(err); res.status(500).json({ success: false, message: 'Server error' }); }
});

// PATCH /api/supply-requirements/:id — processor updates a requirement
app.patch('/api/supply-requirements/:id', requireAuth, async (req, res) => {
  try {
    if (req.user.role !== 'processor') return res.status(403).json({ success: false, message: 'Processors only' });
    const fields = ['material_type','accepted_forms','accepted_colours','max_contamination_pct','max_moisture_pct','min_quantity_kg','price_premium_pct','client_reference','sorting_notes','is_active'];
    const sets = []; const vals = []; let idx = 1;
    for (const f of fields) {
      if (req.body[f] !== undefined) { sets.push(`${f}=$${idx++}`); vals.push(req.body[f]); }
    }
    if (!sets.length) return res.status(400).json({ success: false, message: 'No fields to update' });
    sets.push(`updated_at=NOW()`);
    vals.push(req.params.id, req.user.id);
    const result = await pool.query(
      `UPDATE supply_requirements SET ${sets.join(', ')} WHERE id=$${idx} AND processor_id=$${idx+1} RETURNING *`, vals
    );
    if (!result.rows.length) return res.status(404).json({ success: false, message: 'Requirement not found' });
    res.json({ success: true, requirement: result.rows[0] });
  } catch (err) { console.error(err); res.status(500).json({ success: false, message: 'Server error' }); }
});

// DELETE /api/supply-requirements/:id — processor deactivates a requirement
app.delete('/api/supply-requirements/:id', requireAuth, async (req, res) => {
  try {
    if (req.user.role !== 'processor') return res.status(403).json({ success: false, message: 'Processors only' });
    await pool.query(
      `UPDATE supply_requirements SET is_active=false, updated_at=NOW() WHERE id=$1 AND processor_id=$2`,
      [req.params.id, req.user.id]
    );
    res.json({ success: true });
  } catch (err) { console.error(err); res.status(500).json({ success: false, message: 'Server error' }); }
});

// PATCH /api/pending-transactions/:id/link-requirement — processor links a delivery to a requirement
app.patch('/api/pending-transactions/:id/link-requirement', requireAuth, async (req, res) => {
  try {
    if (req.user.role !== 'processor') return res.status(403).json({ success: false, message: 'Processors only' });
    const { requirement_id, spec_compliance } = req.body;
    if (!requirement_id || !spec_compliance) {
      return res.status(400).json({ success: false, message: 'requirement_id and spec_compliance required' });
    }
    if (!['meets','partial','below'].includes(spec_compliance)) {
      return res.status(400).json({ success: false, message: 'spec_compliance must be meets, partial, or below' });
    }
    const result = await pool.query(
      `UPDATE pending_transactions SET requirement_id=$1, spec_compliance=$2 WHERE id=$3 RETURNING id`,
      [requirement_id, spec_compliance, req.params.id]
    );
    if (!result.rows.length) return res.status(404).json({ success: false, message: 'Transaction not found' });
    res.json({ success: true });
  } catch (err) { console.error(err); res.status(500).json({ success: false, message: 'Server error' }); }
});

// Express error-handling middleware — log server errors to error_log table
app.use(async (err, req, res, next) => {
  console.error('Server error:', err.message);
  try {
    await pool.query(
      `INSERT INTO error_log (source, dashboard, error_message, error_stack, url)
       VALUES ('server', NULL, $1, $2, $3)`,
      [err.message?.substring(0, 2000), err.stack?.substring(0, 5000),
       req.originalUrl?.substring(0, 500)]
    );
  } catch (logErr) { console.error('Error log insert failed:', logErr.message); }
  res.status(500).json({ error: 'Internal server error' });
});

app.listen(port, () => console.log(`Circul server running on port ${port}`));
