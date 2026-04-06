const express = require('express');
const { Pool } = require('pg');
const path = require('path');
const fs = require('fs');
const crypto = require('crypto');
const multer = require('multer');
const CirculRoles = require('./shared/roles');
const { EVENTS, notify } = require('./shared/notifications');
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
  console.error('ERROR: DATABASE_URL environment variable is equired');
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
    if (!pin || !/^\d{4}$/.test(pin)) return res.status(400).json({ success: false, message: 'PIN must be exactly 4 digits' });
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
    const result = await pool.query(
      `SELECT c.id, c.first_name, c.last_name, c.phone, c.region, c.city, c.average_rating,
              c.is_active, c.id_verified, c.created_at,
              'COL-' || LPAD(c.id::text, 4, '0') AS display_name,
              COALESCE(SUM(t.net_weight_kg),0) as total_weight_kg,
              COUNT(t.id) as transaction_count
       FROM collectors c
       LEFT JOIN transactions t ON t.collector_id=c.id
       WHERE c.id=$1 GROUP BY c.id`, [id]
    );
    if (!result.rows.length) return res.status(404).json({ success: false, message: 'Collector not found' });
    const c = result.rows[0];
    c.name = ((c.first_name||'') + (c.last_name ? ' '+c.last_name : '')).trim();
    c.collector_id = c.id;
    c.avg_rating = c.average_rating;
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

app.post('/api/aggregator/suggest-cost-category', async (req, res) => {
  try {
    const { category_name, aggregator_id } = req.body;
    if (!category_name || !category_name.trim()) return res.status(400).json({ success: false, message: 'Category name required' });
    console.log('Cost category suggestion:', { category_name: category_name.trim(), aggregator_id, submitted_at: new Date().toISOString() });
    res.json({ success: true, message: 'Suggestion received' });
  } catch (err) { console.error('POST /api/aggregator/suggest-cost-category error:', err); res.status(500).json({ success: false, message: 'Server error' }); }
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

app.get('/api/aggregators/:id/expense-categories', async (req, res) => {
  try {
    const { rows } = await pool.query(
      `SELECT id, name, status FROM expense_categories WHERE status IN ('default','approved') ORDER BY name`
    );
    res.json(rows);
  } catch (err) {
    console.error('GET /api/aggregators/:id/expense-categories error:', err);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

app.post('/api/expense-categories/suggest', async (req, res) => {
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

app.get('/api/expense-categories/pending', async (req, res) => {
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

app.patch('/api/expense-categories/:id/approve', async (req, res) => {
  try {
    const { id } = req.params;
    const newName = req.body.name;
    const { rows } = await pool.query(
      `UPDATE expense_categories SET status = 'approved', name = COALESCE($1, name), reviewed_at = NOW() WHERE id = $2 AND status = 'pending' RETURNING *`,
      [newName || null, id]
    );
    if (!rows.length) return res.status(404).json({ error: 'Category not found or already reviewed' });
    res.json(rows[0]);
  } catch (err) {
    console.error('PATCH /api/expense-categories/:id/approve error:', err);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

app.patch('/api/expense-categories/:id/reject', async (req, res) => {
  try {
    const { id } = req.params;
    const { rejection_reason } = req.body;
    if (!rejection_reason || !rejection_reason.trim()) {
      return res.status(400).json({ error: 'Rejection reason is required' });
    }
    const { rows } = await pool.query(
      `UPDATE expense_categories SET status = 'rejected', rejection_reason = $1, reviewed_at = NOW() WHERE id = $2 AND status = 'pending' RETURNING *`,
      [rejection_reason.trim(), id]
    );
    if (!rows.length) return res.status(404).json({ error: 'Category not found or already reviewed' });
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
      return res.status(400).json({ error: 'category_id and amount are required' });
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
    res.status(500).json({ error: 'Failed to log expense' });
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
    if (!rows.length) return res.status(404).json({ error: 'Entry not found' });

    // Clean up receipt file if it exists
    if (rows[0].receipt_url) {
      const filePath = path.join(__dirname, 'public', rows[0].receipt_url);
      fs.unlink(filePath, () => {}); // best-effort delete
    }

    res.json({ deleted: rows[0] });
  } catch (err) {
    console.error('DELETE /api/aggregators/:id/expenses/:eid error:', err);
    res.status(500).json({ error: 'Failed to delete expense' });
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

// Determine transaction_type from seller→buyer roles
function txnTypeForRoles(sellerRole, buyerRole) {
  if (sellerRole === 'collector' && buyerRole === 'aggregator') return 'collector_sale';
  if (sellerRole === 'aggregator') return 'aggregator_sale';
  return 'aggregator_sale'; // fallback
}

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
    const buyerTable = CirculRoles.TABLE_MAP[buyerRole] || 'operators';
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
      const sellerTable = CirculRoles.TABLE_MAP[listing.seller_role] || 'collectors';
      const nameCol = listing.seller_role === 'collector' ? "first_name || ' ' || last_name" : 'name';
      const seller = (await pool.query(`SELECT phone, ${nameCol} AS name FROM ${sellerTable} WHERE id = $1`, [listing.seller_id])).rows[0];
      const buyerTable = CirculRoles.TABLE_MAP[buyerRole] || 'operators';
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

      // Build pending_transaction INSERT with correct column names
      const sellerCol = ptColForRole(listing.seller_role);
      const buyerCol = ptColForRole(offer.buyer_role);
      const txnType = txnTypeForRoles(listing.seller_role, offer.buyer_role);
      const totalPrice = parseFloat((offerQty * parseFloat(offer.price_per_kg)).toFixed(2));
      const cols = ['transaction_type', 'status', 'material_type', 'gross_weight_kg', 'price_per_kg', 'total_price', 'source'];
      const vals = [txnType, 'pending', listing.material_type, offerQty, parseFloat(offer.price_per_kg), totalPrice, 'discovery'];
      if (sellerCol) { cols.push(sellerCol); vals.push(listing.seller_id); }
      if (buyerCol && buyerCol !== sellerCol) { cols.push(buyerCol); vals.push(offer.buyer_id); }
      else if (buyerCol && buyerCol === sellerCol) {
        // Same column (e.g. aggregator buying from collector — aggregator_id is the buyer)
        // seller is collector_id, buyer is aggregator_id — no conflict
        cols.push(buyerCol); vals.push(offer.buyer_id);
      }
      const placeholders = vals.map((_, i) => '$' + (i + 1)).join(', ');
      const ptResult = await client.query(
        `INSERT INTO pending_transactions (${cols.join(', ')}) VALUES (${placeholders}) RETURNING *`,
        vals
      );
      await client.query('COMMIT');
      // Notify the buyer that their offer was accepted
      try {
        const buyerTable = CirculRoles.TABLE_MAP[offer.buyer_role] || 'operators';
        const buyerNameCol = offer.buyer_role === 'collector' ? "first_name || ' ' || last_name" : 'name';
        const buyerRow = (await pool.query(`SELECT phone, ${buyerNameCol} AS name FROM ${buyerTable} WHERE id = $1`, [offer.buyer_id])).rows[0];
        const sellerTable = CirculRoles.TABLE_MAP[listing.seller_role] || 'collectors';
        const sellerNameCol = listing.seller_role === 'collector' ? "first_name || ' ' || last_name" : 'name';
        const sellerRow = (await pool.query(`SELECT ${sellerNameCol} AS name FROM ${sellerTable} WHERE id = $1`, [listing.seller_id])).rows[0];
        if (buyerRow && buyerRow.phone) {
          notify(EVENTS.OFFER_ACCEPTED, buyerRow.phone, { material: listing.material_type, qty: offerQty, seller_name: sellerRow ? sellerRow.name : 'the seller' });
        }
      } catch (notifyErr) { console.warn('Notification error (offer_accepted):', notifyErr.message); }
      res.json({ success: true, pending_transaction: ptResult.rows[0], offer: { id: offer.id, status: 'accepted' } });
    } catch (txErr) {
      await client.query('ROLLBACK');
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
      const buyerTable = CirculRoles.TABLE_MAP[offer.buyer_role] || 'operators';
      const buyerNameCol = offer.buyer_role === 'collector' ? "first_name || ' ' || last_name" : 'name';
      const buyerRow = (await pool.query(`SELECT phone, ${buyerNameCol} AS name FROM ${buyerTable} WHERE id = $1`, [offer.buyer_id])).rows[0];
      const sellerTable = CirculRoles.TABLE_MAP[listing.seller_role] || 'collectors';
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
        const recipientTable = CirculRoles.TABLE_MAP[recipientRole] || 'operators';
        const recipientNameCol = recipientRole === 'collector' ? "first_name || ' ' || last_name" : 'name';
        const recipientRow = (await pool.query(`SELECT phone, ${recipientNameCol} AS name FROM ${recipientTable} WHERE id = $1`, [recipientId])).rows[0];
        const counterpartyId = isSeller ? listing.seller_id : offer.buyer_id;
        const counterpartyRole = isSeller ? listing.seller_role : offer.buyer_role;
        const counterpartyTable = CirculRoles.TABLE_MAP[counterpartyRole] || 'operators';
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
    let query = `SELECT pt.id, pt.material_type, pt.gross_weight_kg AS net_weight_kg, pt.price_per_kg, pt.total_price,
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

app.post('/api/ratings/operator', requireAuth, async (req, res) => {
  try {
    const { transaction_id, rater_type, rater_id, rated_type, rated_id, rater_operator_id, rated_operator_id, rater_collector_id, rated_collector_id, rating, tags, notes, rating_direction } = req.body;
    // Fall back to token-derived user when body fields are missing
    const finalRaterType = rater_type || req.user.role || (rater_operator_id ? 'aggregator' : 'collector');
    const finalRaterId   = rater_id   || req.user.id || rater_operator_id || rater_collector_id;
    const finalRatedType = rated_type || (rated_operator_id ? 'aggregator' : 'collector');
    const finalRatedId   = rated_id   || rated_operator_id || rated_collector_id;
    if (!finalRaterId) return res.status(400).json({ success: false, message: 'rater_id is required' });
    if (!finalRaterId || !finalRatedId || !rating) return res.status(400).json({ success: false, message: 'rater, rated, and rating are required' });
    if (rating < 1 || rating > 5) return res.status(400).json({ success: false, message: 'Rating must be 1-5' });
    // Prevent duplicate ratings
    if (transaction_id) {
      const dup = await pool.query(`SELECT id FROM ratings WHERE transaction_id=$1 AND rater_type=$2 AND rater_id=$3`, [transaction_id, finalRaterType, finalRaterId]);
      if (dup.rows.length) return res.status(409).json({ success: false, message: 'You have already rated this transaction' });
    }
    const windowExpires = new Date(); windowExpires.setDate(windowExpires.getDate() + 30);
    let result;
    try {
      result = await pool.query(
        `INSERT INTO ratings (transaction_id, rater_type, rater_id, rated_type, rated_id, rating, tags, notes, rating_direction, window_expires_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) RETURNING *`,
        [transaction_id||null, finalRaterType, finalRaterId, finalRatedType, finalRatedId, rating, tags||[], notes||null, rating_direction||null, windowExpires.toISOString()]
      );
    } catch (insertErr) {
      result = await pool.query(
        `INSERT INTO ratings (transaction_id, rater_type, rater_id, rated_type, rated_id, rating, tags, notes, rating_direction) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) RETURNING *`,
        [transaction_id||null, finalRaterType, finalRaterId, finalRatedType, finalRatedId, rating, tags||[], notes||null, rating_direction||null]
      );
    }
    // Notify the rated user
    try {
      const ratedTable = CirculRoles.TABLE_MAP[finalRatedType] || 'operators';
      const ratedNameCol = finalRatedType === 'collector' ? "first_name || ' ' || last_name" : 'name';
      const ratedRow = (await pool.query(`SELECT phone, ${ratedNameCol} AS name FROM ${ratedTable} WHERE id = $1`, [finalRatedId])).rows[0];
      const raterTable = CirculRoles.TABLE_MAP[finalRaterType] || 'operators';
      const raterNameCol = finalRaterType === 'collector' ? "first_name || ' ' || last_name" : 'name';
      const raterRow = (await pool.query(`SELECT ${raterNameCol} AS name FROM ${raterTable} WHERE id = $1`, [finalRaterId])).rows[0];
      if (ratedRow && ratedRow.phone) {
        notify(EVENTS.RATING_RECEIVED, ratedRow.phone, { rater_name: raterRow ? raterRow.name : 'Someone', stars: rating });
      }
    } catch (notifyErr) { console.warn('Notification error (rating_received):', notifyErr.message); }
    res.status(201).json({ success: true, rating: result.rows[0] });
  } catch (err) {
    if (err.code === '23505') return res.status(409).json({ success: false, message: 'You have already rated this transaction' });
    console.error('Rating error:', err);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

app.get('/api/ratings/operator/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const role = req.query.role;
    const typeFilter = role ? [role] : ['aggregator','processor','recycler','converter'];
    const ratings = await pool.query(`SELECT r.* FROM ratings r WHERE r.rated_id=$1 AND r.rated_type = ANY($2) ORDER BY r.created_at DESC LIMIT 50`, [id, typeFilter]);
    const avg = await pool.query(`SELECT AVG(rating)::NUMERIC(3,2) as avg_rating, COUNT(*) as count FROM ratings WHERE rated_id=$1 AND rated_type = ANY($2)`, [id, typeFilter]);
    res.json({ success: true, ratings: ratings.rows, summary: avg.rows[0] });
  } catch (err) { res.status(500).json({ success: false, message: 'Server error' }); }
});

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

    // Map role → which column in pending_transactions identifies this user,
    // and which column/table identifies the counterparty they should rate
    const ROLE_MAP = {
      collector:  { myCol: 'collector_id',  peerCol: 'aggregator_id', peerTable: 'aggregators', peerName: 'name' },
      aggregator: { myCol: 'aggregator_id', peerCol: 'collector_id',  peerTable: 'collectors',  peerName: "first_name || ' ' || last_name" },
      processor:  { myCol: 'processor_id',  peerCol: 'aggregator_id', peerTable: 'aggregators', peerName: 'name' },
      recycler:   { myCol: 'recycler_id',   peerCol: 'processor_id',  peerTable: 'processors',  peerName: 'name' },
      converter:  { myCol: 'converter_id',  peerCol: 'processor_id',  peerTable: 'processors',  peerName: 'name' },
    };
    const cfg = ROLE_MAP[role];
    if (!cfg) return res.json({ success: true, pending: [] });

    const rows = await pool.query(
      `SELECT pt.id AS txn_id, pt.material_type, pt.gross_weight_kg, pt.created_at,
              pt.${cfg.peerCol} AS peer_id,
              p.${cfg.peerName} AS peer_name
       FROM pending_transactions pt
       LEFT JOIN ${cfg.peerTable} p ON p.id = pt.${cfg.peerCol}
       WHERE pt.${cfg.myCol} = $1
         AND pt.status IN ('completed','confirmed')
         AND pt.created_at > NOW() - INTERVAL '30 days'
         AND NOT EXISTS (
           SELECT 1 FROM ratings r
           WHERE r.transaction_id = pt.id
             AND r.rater_type = $2
             AND r.rater_id = $1
         )
       ORDER BY pt.created_at DESC
       LIMIT 5`,
      [userId, role]
    );
    res.json({ success: true, pending: rows.rows });
  } catch (err) {
    console.error('GET /api/ratings/pending error:', err.message);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

// ============================================
// USSD
// ============================================

function normalizeGhanaPhone(phone) {
  if (!phone) return null;
  let cleaned = phone.replace(/[\s\-()]/g, '');
  if (cleaned.startsWith('+233')) return cleaned;
  if (cleaned.startsWith('233')) return '+' + cleaned;
  if (cleaned.startsWith('0')) return '+233' + cleaned.slice(1);
  return cleaned;
}

function getPhoneVariants(normalizedPhone) {
  if (!normalizedPhone) return [];
  const variants = [normalizedPhone];
  if (normalizedPhone.startsWith('+233')) {
    variants.push('0' + normalizedPhone.slice(4));
    variants.push(normalizedPhone.slice(1));
  }
  return variants;
}

const USSD_MATERIALS = { '1': 'PET', '2': 'HDPE', '3': 'LDPE', '4': 'PP' };

async function handleUnregisteredUssd(parts, phone) {
  const level = parts.length;
  if (level === 0) return 'CON Welcome to Circul\n1. Register\n2. Exit';
  if (parts[0] === '2') return 'END Thank you for using Circul.';
  if (parts[0] === '1') {
    if (level === 1) return 'CON Enter your first name:';
    if (level === 2) return 'CON Enter last name\n(0 to skip):';
    if (level === 3) return 'CON Create a 4-digit PIN:';
    if (level === 4) {
      const firstName = parts[1].trim(), lastName = parts[2] === '0' ? '' : parts[2].trim(), pin = parts[3].trim();
      if (pin.length < 4 || pin.length > 6 || !/^\d+$/.test(pin)) return 'END PIN must be 4-6 digits.\nDial again to retry.';
      try {
        await pool.query(`INSERT INTO collectors (first_name, last_name, phone, pin, region) VALUES ($1,$2,$3,$4,$5)`, [firstName, lastName, phone, pin, 'Ghana']);
        return `END Registered! Welcome ${firstName}.\nDial again to start.`;
      } catch (err) {
        if (err.code === '23505') return 'END Phone already registered.\nDial again to login.';
        throw err;
      }
    }
  }
  return 'END Invalid option.\nDial again to retry.';
}

async function handleRegisteredUssd(parts, collector) {
  const level = parts.length;
  if (level === 0) return `CON Welcome ${collector.first_name}!\nEnter your PIN:`;
  if (!(await verifyPassword(parts[0], collector.pin))) return 'END Invalid PIN.\nDial again to retry.';
  if (level === 1) return 'CON 1. Log Collection\n2. Check Balance\n3. Exit';
  const menu = parts[1];
  if (menu === '3') return `END Thank you, ${collector.first_name}!`;
  if (menu === '2') {
    const stats = await pool.query(`SELECT COALESCE(SUM(net_weight_kg),0) as total_kg, COALESCE(SUM(total_price),0) as earned, COUNT(*) as txns FROM transactions WHERE collector_id=$1`, [collector.id]);
    const s = stats.rows[0];
    return `END Balance:\nTotal: ${parseFloat(s.total_kg).toFixed(1)}kg\nEarned: GHS ${parseFloat(s.earned).toFixed(2)}\nTransactions: ${s.txns}`;
  }
  if (menu === '1') {
    if (level === 2) return 'CON Select material:\n1.PET 2.HDPE\n3.LDPE 4.PP';
    const material = USSD_MATERIALS[parts[2]];
    if (!material) return 'END Invalid material.\nDial again to retry.';
    if (level === 3) return 'CON Enter weight in kg:';
    const weight = parseFloat(parts[3]);
    if (isNaN(weight) || weight <= 0 || weight > 9999) return 'END Invalid weight.\nDial again to retry.';
    if (level === 4) return `CON Log ${weight}kg ${material}?\n1. Confirm\n2. Cancel`;
    if (level === 5) {
      if (parts[4] === '2') return 'END Cancelled.';
      if (parts[4] === '1') {
        await pool.query(`INSERT INTO transactions (collector_id, material_type, gross_weight_kg, net_weight_kg, price_per_kg, total_price) VALUES ($1,$2,$3,$4,0,0)`, [collector.id, material, weight, weight]);
        const today = await pool.query(`SELECT COALESCE(SUM(net_weight_kg),0) as today_kg FROM transactions WHERE collector_id=$1 AND transaction_date>=CURRENT_DATE`, [collector.id]);
        return `END Logged! ${weight}kg ${material}\nToday: ${parseFloat(today.rows[0].today_kg).toFixed(1)}kg total`;
      }
    }
  }
  return 'END Invalid option.\nDial again to retry.';
}

app.post('/api/ussd', async (req, res) => {
  const { sessionId, serviceCode, phoneNumber, text } = req.body;
  const phone = normalizeGhanaPhone(phoneNumber);
  const parts = text ? text.split('*') : [];
  let response = '', collectorId = null;
  try {
    const phoneVariants = getPhoneVariants(phone);
    const result = await pool.query(`SELECT id, first_name, last_name, phone, pin FROM collectors WHERE phone=ANY($1) AND is_active=true LIMIT 1`, [phoneVariants]);
    if (!result.rows.length) response = await handleUnregisteredUssd(parts, phone);
    else { collectorId = result.rows[0].id; response = await handleRegisteredUssd(parts, result.rows[0]); }
  } catch (err) { console.error('[USSD] Error:', err); response = 'END System error. Try again later.'; }
  try { await pool.query(`INSERT INTO ussd_sessions (session_id, phone, service_code, collector_id, text_input, response) VALUES ($1,$2,$3,$4,$5,$6)`, [sessionId, phone, serviceCode, collectorId, text||'', response]); } catch (logErr) { console.error('[USSD] Log error:', logErr); }
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

    // Per-type validation
    if (transaction_type === 'collector_sale' || transaction_type === 'aggregator_purchase') {
      if (!collectorId) return res.status(400).json({ success: false, message: 'collector_id is required for ' + transaction_type });
      if (!aggId) return res.status(400).json({ success: false, message: 'aggregator_id or aggregator_id is required for ' + transaction_type });
    } else if (transaction_type === 'aggregator_sale') {
      if (!aggId) return res.status(400).json({ success: false, message: 'aggregator_id or aggregator_id is required for aggregator_sale' });
      if (!procId && !convId) return res.status(400).json({ success: false, message: 'processor_id or converter_id is required for aggregator_sale' });
    } else if (transaction_type === 'processor_sale') {
      if (!procId) return res.status(400).json({ success: false, message: 'processor_id or processor_id is required for processor_sale' });
      if (!convId && !recyclerId) return res.status(400).json({ success: false, message: 'converter_id or recycler_id is required for processor_sale' });
    } else if (transaction_type === 'recycler_sale') {
      if (!recyclerId) return res.status(400).json({ success: false, message: 'recycler_id is required for recycler_sale' });
      if (!convId) return res.status(400).json({ success: false, message: 'converter_id or converter_id is required for recycler_sale' });
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

    const result = await pool.query(
      `INSERT INTO pending_transactions (transaction_type, collector_id, aggregator_id, processor_id, converter_id, recycler_id, material_type, gross_weight_kg, price_per_kg, total_price, status, notes)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,'pending',$11) RETURNING *`,
      [transaction_type, collectorId, aggId, procId, convId, recyclerId, material_type.toUpperCase(), kg, pricePer, totalPrice, b.notes || null]
    );
    res.status(201).json({ success: true, pending_transaction: result.rows[0] });
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
    const result = await pool.query(`INSERT INTO pending_transactions (transaction_type, collector_id, aggregator_id, material_type, gross_weight_kg, price_per_kg, total_price, status) VALUES ('aggregator_purchase',$1,$2,$3,$4,$5,$6,'pending') RETURNING *`, [collector_id, aggregator_id, material_type.toUpperCase(), kg, pricePer, totalPrice]);
    res.status(201).json({ success: true, pending_transaction: result.rows[0] });
  } catch (err) { console.error('Aggregator purchase error:', err); res.status(500).json({ success: false, message: 'Server error' }); }
});

app.post('/api/pending-transactions/aggregator-sale', requireAuth, async (req, res) => {
  try {
    const { aggregator_id, processor_id, converter_id, material_type, gross_weight_kg, price_per_kg, notes, photo_urls } = req.body;
    if (!aggregator_id || (!processor_id && !converter_id) || !material_type || !gross_weight_kg || !price_per_kg) return res.status(400).json({ success: false, message: 'aggregator_id, processor_id or converter_id, material_type, gross_weight_kg, and price_per_kg are required' });
    if (req.user.id !== parseInt(aggregator_id)) return res.status(403).json({ success: false, message: 'Access denied' });
    const kg = parseFloat(gross_weight_kg);
    if (isNaN(kg) || kg <= 0 || kg > 4000) return res.status(400).json({ success: false, message: 'gross_weight_kg must be > 0 and at most 4000 kg' });
    const aggCheck = await pool.query(`SELECT id FROM aggregators WHERE id=$1 AND is_active=true`, [aggregator_id]);
    if (!aggCheck.rows.length) return res.status(400).json({ success: false, message: 'Aggregator not found' });
    let resolvedProcessorId = null, resolvedConverterId = null;
    if (processor_id) { const pr = await pool.query(`SELECT id FROM processors WHERE id=$1 AND is_active=true`, [processor_id]); if (!pr.rows.length) return res.status(400).json({ success: false, message: 'Processor not found' }); resolvedProcessorId = parseInt(processor_id); }
    if (converter_id) { const cv = await pool.query(`SELECT id FROM converters WHERE id=$1 AND is_active=true`, [converter_id]); if (!cv.rows.length) return res.status(400).json({ success: false, message: 'Converter not found' }); resolvedConverterId = parseInt(converter_id); }
    const price = parseFloat(price_per_kg);
    const totalPrice = parseFloat((kg * price).toFixed(2));
    const photosRequired = kg > 500;
    const dispatchApproved = photosRequired ? false : true;
    const result = await pool.query(`INSERT INTO pending_transactions (transaction_type, aggregator_id, processor_id, converter_id, material_type, gross_weight_kg, price_per_kg, total_price, status, photos_required, photos_submitted, dispatch_approved, photo_urls, notes) VALUES ('aggregator_sale',$1,$2,$3,$4,$5,$6,$7,'pending',$8,false,$9,$10,$11) RETURNING *`, [aggregator_id, resolvedProcessorId, resolvedConverterId, material_type.toUpperCase(), kg, price, totalPrice, photosRequired, dispatchApproved, photo_urls||[], notes||null]);
    res.status(201).json({ success: true, pending_transaction: result.rows[0], photos_required: photosRequired });
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
    const { converter_id, recycler_id, material_type, gross_weight_kg, price_per_kg, notes } = req.body;
    if (!material_type || !gross_weight_kg || !price_per_kg) return res.status(400).json({ success: false, message: 'material_type, gross_weight_kg, price_per_kg required' });
    if (!converter_id && !recycler_id) return res.status(400).json({ success: false, message: 'Either converter_id or recycler_id is required' });
    if (converter_id && recycler_id) return res.status(400).json({ success: false, message: 'Provide converter_id or recycler_id, not both' });
    const kg = parseFloat(gross_weight_kg), price = parseFloat(price_per_kg);
    if (isNaN(kg) || kg <= 0) return res.status(400).json({ success: false, message: 'Invalid weight' });
    if (isNaN(price) || price <= 0) return res.status(400).json({ success: false, message: 'Invalid price' });
    if (recycler_id) {
      const recResult = await pool.query(`SELECT id FROM recyclers WHERE id=$1 AND is_active=true`, [recycler_id]);
      if (!recResult.rows.length) return res.status(400).json({ success: false, message: 'Recycler not found' });
      const result = await pool.query(`INSERT INTO pending_transactions (transaction_type, status, processor_id, recycler_id, material_type, gross_weight_kg, price_per_kg, total_price, photos_required, photos_submitted, photo_urls, notes) VALUES ('processor_sale','pending',$1,$2,$3,$4,$5,$6,true,false,'{}', $7) RETURNING *`, [req.user.id, recycler_id, material_type, kg, price, kg*price, notes||null]);
      return res.status(201).json({ success: true, pending_transaction: result.rows[0] });
    }
    const convResult = await pool.query(`SELECT id FROM converters WHERE id=$1 AND is_active=true`, [converter_id]);
    if (!convResult.rows.length) return res.status(400).json({ success: false, message: 'Converter not found' });
    const result = await pool.query(`INSERT INTO pending_transactions (transaction_type, status, processor_id, converter_id, material_type, gross_weight_kg, price_per_kg, total_price, photos_required, photos_submitted, photo_urls, notes) VALUES ('processor_sale','pending',$1,$2,$3,$4,$5,$6,true,false,'{}', $7) RETURNING *`, [req.user.id, converter_id, material_type, kg, price, kg*price, notes||null]);
    res.status(201).json({ success: true, pending_transaction: result.rows[0] });
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
    const { converter_id, material_type, gross_weight_kg, price_per_kg, notes } = req.body;
    if (!converter_id || !material_type || !gross_weight_kg || !price_per_kg) return res.status(400).json({ success: false, message: 'converter_id, material_type, gross_weight_kg, price_per_kg required' });
    const kg = parseFloat(gross_weight_kg), price = parseFloat(price_per_kg);
    if (isNaN(kg) || kg <= 0) return res.status(400).json({ success: false, message: 'Invalid weight' });
    if (isNaN(price) || price <= 0) return res.status(400).json({ success: false, message: 'Invalid price' });
    const convResult = await pool.query(`SELECT id FROM converters WHERE id=$1 AND is_active=true`, [converter_id]);
    if (!convResult.rows.length) return res.status(400).json({ success: false, message: 'Converter not found' });
    const result = await pool.query(`INSERT INTO pending_transactions (transaction_type, status, recycler_id, converter_id, material_type, gross_weight_kg, price_per_kg, total_price, photos_required, photos_submitted, photo_urls, notes) VALUES ('recycler_sale','pending',$1,$2,$3,$4,$5,$6,true,false,'{}', $7) RETURNING *`, [req.user.id, converter_id, material_type, kg, price, kg*price, notes||null]);
    res.status(201).json({ success: true, pending_transaction: result.rows[0] });
  } catch (err) { console.error('Recycler sale error:', err); res.status(500).json({ success: false, message: 'Server error' }); }
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

app.patch('/api/transactions/:id/payment-initiate', async (req, res) => {
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
    const existing = await client.query('SELECT payment_status FROM transactions WHERE id=$1 FOR UPDATE', [id]);
    if (!existing.rows.length) { await client.query('ROLLBACK'); return res.status(404).json({ success: false, message: 'Transaction not found' }); }
    if (existing.rows[0].payment_status !== 'unpaid') { await client.query('ROLLBACK'); return res.status(400).json({ success: false, message: 'Payment already recorded' }); }
    const result = await client.query(
      `UPDATE transactions SET payment_status='payment_sent', payment_method=$1, payment_reference=$2, payment_initiated_at=NOW() WHERE id=$3 RETURNING *`,
      [payment_method, ref, id]
    );
    await client.query('COMMIT');
    res.json({ success: true, transaction: result.rows[0] });
  } catch (err) { await client.query('ROLLBACK').catch(() => {}); console.error('Payment initiate error:', err); res.status(500).json({ success: false, message: 'Server error' }); } finally { client.release(); }
});

app.patch('/api/transactions/:id/payment-confirm', async (req, res) => {
  const client = await pool.connect();
  try {
    const { id } = req.params;
    await client.query('BEGIN');
    const existing = await client.query('SELECT payment_status FROM transactions WHERE id=$1 FOR UPDATE', [id]);
    if (!existing.rows.length) { await client.query('ROLLBACK'); return res.status(404).json({ success: false, message: 'Transaction not found' }); }
    if (existing.rows[0].payment_status !== 'payment_sent') { await client.query('ROLLBACK'); return res.status(400).json({ success: false, message: 'No payment to confirm' }); }
    const result = await client.query(
      `UPDATE transactions SET payment_status='paid', payment_completed_at=NOW() WHERE id=$1 RETURNING *`,
      [id]
    );
    await client.query('COMMIT');
    res.json({ success: true, transaction: result.rows[0] });
  } catch (err) { await client.query('ROLLBACK').catch(() => {}); console.error('Payment confirm error:', err); res.status(500).json({ success: false, message: 'Server error' }); } finally { client.release(); }
});

app.patch('/api/pending-transactions/:id/payment-initiate', async (req, res) => {
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
    const existing = await client.query('SELECT payment_status FROM pending_transactions WHERE id=$1 FOR UPDATE', [id]);
    if (!existing.rows.length) { await client.query('ROLLBACK'); return res.status(404).json({ success: false, message: 'Pending transaction not found' }); }
    if (existing.rows[0].payment_status !== 'unpaid') { await client.query('ROLLBACK'); return res.status(400).json({ success: false, message: 'Payment already recorded' }); }
    const result = await client.query(
      `UPDATE pending_transactions SET payment_status='payment_sent', payment_method=$1, payment_reference=$2, payment_initiated_at=NOW() WHERE id=$3 RETURNING *`,
      [payment_method, ref, id]
    );
    await client.query('COMMIT');
    res.json({ success: true, pending_transaction: result.rows[0] });
  } catch (err) { await client.query('ROLLBACK').catch(() => {}); console.error('PT payment initiate error:', err); res.status(500).json({ success: false, message: 'Server error' }); } finally { client.release(); }
});

app.patch('/api/pending-transactions/:id/payment-confirm', async (req, res) => {
  const client = await pool.connect();
  try {
    const { id } = req.params;
    await client.query('BEGIN');
    const existing = await client.query('SELECT payment_status FROM pending_transactions WHERE id=$1 FOR UPDATE', [id]);
    if (!existing.rows.length) { await client.query('ROLLBACK'); return res.status(404).json({ success: false, message: 'Pending transaction not found' }); }
    if (existing.rows[0].payment_status !== 'payment_sent') { await client.query('ROLLBACK'); return res.status(400).json({ success: false, message: 'No payment to confirm' }); }
    const result = await client.query(
      `UPDATE pending_transactions SET payment_status='paid', payment_completed_at=NOW() WHERE id=$1 RETURNING *`,
      [id]
    );
    await client.query('COMMIT');
    res.json({ success: true, pending_transaction: result.rows[0] });
  } catch (err) { await client.query('ROLLBACK').catch(() => {}); console.error('PT payment confirm error:', err); res.status(500).json({ success: false, message: 'Server error' }); } finally { client.release(); }
});

// ============================================
// ORDERS API
// ============================================

app.post('/api/orders', requireAuth, async (req, res) => {
  try {
    if (!req.user.hasRole('converter') && !req.user.hasRole('recycler')) return res.status(403).json({ success: false, message: 'Converter or recycler access only' });
    const { material_type, target_quantity_kg, price_per_kg, accepted_colours, excluded_contaminants, max_contamination_pct, notes, supplier_tier, supplier_id } = req.body;
    if (!material_type || !target_quantity_kg || !price_per_kg) return res.status(400).json({ success: false, message: 'material_type, target_quantity_kg, price_per_kg required' });
    const qty = parseFloat(target_quantity_kg), price = parseFloat(price_per_kg);
    if (isNaN(qty) || qty <= 0) return res.status(400).json({ success: false, message: 'Invalid target_quantity_kg' });
    if (isNaN(price) || price <= 0) return res.status(400).json({ success: false, message: 'Invalid price_per_kg' });
    // Check if orders table exists
    const tableCheck = await pool.query(`SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'orders')`).catch(() => ({ rows: [{ exists: false }] }));
    if (!tableCheck.rows[0].exists) return res.status(503).json({ success: false, message: 'Orders feature is being deployed. Please try again in a few minutes.' });
    // Determine buyer identity and role
    const buyerRole = req.user.hasRole('converter') ? 'converter' : 'recycler';
    const buyerId = buyerRole === 'converter' ? (req.user.converter_id || req.user.id) : req.user.id;
    const result = await pool.query(
      `INSERT INTO orders (buyer_id, buyer_role, material_type, target_quantity_kg, price_per_kg, accepted_colours, excluded_contaminants, max_contamination_pct, supplier_tier, supplier_id, notes)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) RETURNING *`,
      [buyerId, buyerRole, material_type, qty, price, accepted_colours||null, excluded_contaminants||null,
       max_contamination_pct != null && max_contamination_pct !== '' ? parseFloat(max_contamination_pct) : null,
       supplier_tier||null, supplier_id||null, notes||null]
    );
    res.status(201).json({ success: true, order: result.rows[0] });
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
    const isConverter = user.role === 'converter' || (Array.isArray(user.roles) && user.roles.includes('converter'));
    const isRecycler = user.role === 'recycler' || (Array.isArray(user.roles) && user.roles.includes('recycler'));
    const buyerId = isConverter ? (user.converter_id || user.id) : user.id;
    if (!buyerId) return res.json({ success: true, orders: [] });

    // Check if orders table exists before querying
    const tableCheck = await pool.query(
      `SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'orders')`
    ).catch(() => ({ rows: [{ exists: false }] }));
    if (!tableCheck.rows[0].exists) return res.json({ success: true, orders: [] });

    const result = await pool.query(
      `SELECT * FROM orders WHERE buyer_id=$1 ORDER BY created_at DESC LIMIT 50`, [buyerId]
    ).catch(() => ({ rows: [] }));
    res.json({ success: true, orders: result.rows });
  } catch (err) { console.error('GET /api/orders/my error:', err); res.json({ success: true, orders: [] }); }
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
        return res.json({ success: true, role: null, roles: ['processor','converter'], token, user: { id: proc.id, converter_id: conv.id, name: proc.name, company: proc.company, email: emailLower } });
      }

      if (isRecycler && isConverter) {
        const rec = recResult.rows[0], conv = convResult.rows[0];
        const token = generateToken({ type: 'buyer', id: rec.id, converter_id: conv.id, email: emailLower, roles: ['recycler','converter'] }, AUTH_SECRET);
        return res.json({ success: true, role: null, roles: ['recycler','converter'], token, user: { id: rec.id, converter_id: conv.id, name: rec.name, company: rec.company, email: emailLower } });
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
      const aggResult = await pool.query(`SELECT id, name, company, phone FROM aggregators WHERE phone=$1 AND pin=$2 AND is_active=true`, [phone.trim(), pin.trim()]);
      if (aggResult.rows.length) {
        clearLoginAttempts(phone.trim());
        const a = aggResult.rows[0];
        const token = generateToken({ type: 'aggregator', id: a.id, phone: a.phone, role: 'aggregator' }, AUTH_SECRET);
        return res.json({ success: true, role: 'aggregator', roles: null, token, user: { id: a.id, name: a.name, company: a.company||null, phone: a.phone, role: 'aggregator' } });
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
    const fields = [], params = [];
    if (name !== undefined) { params.push(name); fields.push(`name=$${params.length}`); }
    if (company !== undefined) { params.push(company); fields.push(`company=$${params.length}`); }
    if (phone !== undefined) { params.push(phone); fields.push(`phone=$${params.length}`); }
    if (pin !== undefined) { params.push(pin); fields.push(`pin=$${params.length}`); }
    if (is_active !== undefined) { params.push(is_active); fields.push(`is_active=$${params.length}`); }
    if (is_flagged !== undefined) { params.push(is_flagged); fields.push(`is_flagged=$${params.length}`); }
    if (city !== undefined) { params.push(city); fields.push(`city=$${params.length}`); }
    if (region !== undefined) { params.push(region); fields.push(`region=$${params.length}`); }
    if (country !== undefined) { params.push(country); fields.push(`country=$${params.length}`); }
    if (!fields.length) return res.status(400).json({ success: false, message: 'Nothing to update' });
    params.push(req.params.id);
    const result = await pool.query(`UPDATE aggregators SET ${fields.join(',')} WHERE id=$${params.length} RETURNING id, name, company, phone, is_active, is_flagged, city`, params);
    if (!result.rows.length) return res.status(404).json({ success: false, message: 'Not found' });
    res.json({ success: true, aggregator: result.rows[0] });
  } catch (err) { res.status(500).json({ success: false, message: 'Server error' }); }
});

app.put('/api/admin/collectors/:id', requireAdmin, async (req, res) => {
  try {
    const { first_name, last_name, phone, pin, is_active, is_flagged, city, region } = req.body;
    const fields = [], params = [];
    if (first_name  !== undefined) { params.push(first_name);  fields.push(`first_name=$${params.length}`); }
    if (last_name   !== undefined) { params.push(last_name);   fields.push(`last_name=$${params.length}`); }
    if (phone       !== undefined) { params.push(phone);       fields.push(`phone=$${params.length}`); }
    if (pin         !== undefined) { params.push(pin);         fields.push(`pin=$${params.length}`); }
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
    res.json({ success: true, collector: result.rows[0] });
  } catch (err) { res.status(500).json({ success: false, message: 'Server error' }); }
});

app.put('/api/admin/collectors/:id/verify', requireAdmin, async (req, res) => {
  try {
    const result = await pool.query(`UPDATE collectors SET id_verified=true, id_verified_at=NOW(), id_verified_by=$1 WHERE id=$2 RETURNING id, first_name, last_name, id_verified, id_verified_at`, [req.admin.email, req.params.id]);
    if (!result.rows.length) return res.status(404).json({ success: false, message: 'Collector not found' });
    res.json({ success: true, collector: result.rows[0] });
  } catch (err) { res.status(500).json({ success: false, message: 'Server error' }); }
});

app.put('/api/admin/aggregators/:id/verify', requireAdmin, async (req, res) => {
  try {
    const result = await pool.query(`UPDATE aggregators SET id_verified=true, id_verified_at=NOW(), id_verified_by=$1 WHERE id=$2 RETURNING id, name, id_verified, id_verified_at`, [req.admin.email, req.params.id]);
    if (!result.rows.length) return res.status(404).json({ success: false, message: 'Aggregator not found' });
    res.json({ success: true, aggregator: result.rows[0] });
  } catch (err) { res.status(500).json({ success: false, message: 'Server error' }); }
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
    res.json({ success: true, processor: result.rows[0] });
  } catch (err) { res.status(500).json({ success: false, message: 'Server error' }); }
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
  const exportFiles = ['server.js','migrate.js','package.json','render.yaml','.gitignore','.nvmrc','README.md','public/index.html','public/collect.html','public/login.html','public/dashboard.html','public/admin.html','public/collector-dashboard.html','public/aggregator-dashboard.html','public/processor-dashboard.html','public/converter-dashboard.html','public/report.html'];
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
app.get('/dashboard',            (req, res) => res.sendFile(path.join(__dirname, 'public', 'dashboard.html')));
app.get('/admin',                (req, res) => res.sendFile(path.join(__dirname, 'public', 'admin.html')));
app.get('/collector-dashboard',  (req, res) => res.sendFile(path.join(__dirname, 'public', 'collector-dashboard.html')));
app.get('/aggregator-dashboard', (req, res) => res.sendFile(path.join(__dirname, 'public', 'aggregator-dashboard.html')));
app.get('/processor-dashboard',  (req, res) => res.sendFile(path.join(__dirname, 'public', 'processor-dashboard.html')));
app.get('/converter-dashboard',  (req, res) => res.sendFile(path.join(__dirname, 'public', 'converter-dashboard.html')));
app.get('/recycler-dashboard',  (req, res) => res.sendFile(path.join(__dirname, 'public', 'recycler-dashboard.html')));
app.get('/report',               (req, res) => res.sendFile(path.join(__dirname, 'public', 'report.html')));
app.get('/passport',             (req, res) => res.sendFile(path.join(__dirname, 'public', 'report.html')));
app.get('/collector-passport/:id', (req, res) => res.sendFile(path.join(__dirname, 'public', 'collector-passport.html')));
app.get('/login',                (req, res) => res.sendFile(path.join(__dirname, 'public', 'login.html')));
app.get('/register',             (req, res) => res.sendFile(path.join(__dirname, 'public', 'register.html')));
app.get('/prices',               (req, res) => res.redirect('/'));

// ============================================
// AUTH — REGISTER (collectors + aggregators)
// ============================================

app.post('/api/auth/register', async (req, res) => {
  const { role, phone, pin } = req.body;
  try {
    if (role === 'collector') {
      const { first_name, last_name } = req.body;
      await pool.query(
        `INSERT INTO collectors (first_name, last_name, phone, pin, is_active) VALUES ($1, $2, $3, $4, true)`,
        [first_name, last_name, phone, pin]
      );
    } else if (role === 'aggregator') {
      const { name, company } = req.body;
      await pool.query(
        `INSERT INTO aggregators (name, company, phone, pin, is_active) VALUES ($1, $2, $3, $4, true)`,
        [name, company, phone, pin]
      );
    } else {
      return res.status(400).json({ error: 'Invalid role for self-registration' });
    }
    res.json({ success: true });
  } catch (err) {
    console.error('Register error:', err);
    res.status(500).json({ error: 'Registration failed' });
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
      return res.status(400).json({ error: 'Invalid role for access request' });
    }
    await pool.query(
      `INSERT INTO ${table} (name, company, email, phone, password_hash, is_active) VALUES ($1, $2, $3, $4, '', false)`,
      [name, company, email, phone || null]
    );
    res.json({ success: true });
  } catch (err) {
    console.error('Request access error:', err);
    res.status(500).json({ error: 'Request failed' });
  }
});

// ============================================
// ADMIN — PENDING / APPROVE / REJECT
// ============================================

app.get('/api/admin/pending', async (req, res) => {
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
    res.status(500).json({ error: 'Failed to fetch pending requests' });
  }
});

app.post('/api/admin/approve', async (req, res) => {
  const { id, role } = req.body;
  try {
    const table = CirculRoles.TABLE_MAP[role];
    if (!table) return res.status(400).json({ error: 'Invalid role' });
    await pool.query(`UPDATE ${table} SET is_active=true WHERE id=$1`, [id]);
    res.json({ success: true });
  } catch (err) {
    console.error('Approve error:', err);
    res.status(500).json({ error: 'Approval failed' });
  }
});

app.post('/api/admin/reject', async (req, res) => {
  const { id, role } = req.body;
  try {
    const table = CirculRoles.TABLE_MAP[role];
    if (!table) return res.status(400).json({ error: 'Invalid role' });
    await pool.query(`DELETE FROM ${table} WHERE id=$1`, [id]);
    res.json({ success: true });
  } catch (err) {
    console.error('Reject error:', err);
    res.status(500).json({ error: 'Rejection failed' });
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
    if (!error_message) return res.status(400).json({ error: 'error_message required' });

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
    res.status(500).json({ error: 'logging failed' });
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
    res.status(500).json({ error: e.message });
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

// GET /api/agents — aggregator gets their agents
app.get('/api/agents', requireAuth, async (req, res) => {
  try {
    if (req.user.role !== 'aggregator') return res.status(403).json({ success: false, message: 'Aggregators only' });
    const result = await pool.query(
      `SELECT id, first_name, last_name, phone, city, region, ghana_card, is_active, created_at
       FROM agents WHERE aggregator_id=$1 ORDER BY created_at DESC`, [req.user.id]
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
    if (pin.length < 4) return res.status(400).json({ success: false, message: 'PIN must be at least 4 digits' });
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

// POST /api/agent/log-collection — agent logs a collection
app.post('/api/agent/log-collection', requireAuth, async (req, res) => {
  try {
    if (req.user.role !== 'agent') return res.status(403).json({ success: false, message: 'Agents only' });
    const { collector_id, material_type, gross_weight_kg, price_per_kg } = req.body;
    if (!collector_id || !material_type || !gross_weight_kg) {
      return res.status(400).json({ success: false, message: 'collector_id, material_type, gross_weight_kg required' });
    }
    const total = (gross_weight_kg * (price_per_kg || 0)).toFixed(2);
    const result = await pool.query(
      `INSERT INTO pending_transactions (collector_id, aggregator_id, material_type, gross_weight_kg, price_per_kg, total_price, status, transaction_type, created_at)
       VALUES ($1, $2, $3, $4, $5, $6, 'completed', 'aggregator_sale', NOW()) RETURNING id`,
      [collector_id, req.user.aggregator_id, material_type, gross_weight_kg, price_per_kg || 0, total]
    );
    await pool.query(
      `INSERT INTO agent_activity (agent_id, aggregator_id, action_type, description, related_id, related_type)
       VALUES ($1,$2,'collection',$3,$4,'transaction')`,
      [req.user.id, req.user.aggregator_id,
       `Logged ${gross_weight_kg} kg ${material_type} from collector ${collector_id}`,
       result.rows[0].id]
    );
    res.status(201).json({ success: true, transaction_id: result.rows[0].id });
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
