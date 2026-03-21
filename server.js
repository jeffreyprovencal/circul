const express = require('express');
const { Pool } = require('pg');
const path = require('path');
const fs = require('fs');
const crypto = require('crypto');

const app = express();
const port = process.env.PORT || 3000;

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
app.use(express.static(path.join(__dirname, 'public')));

// ============================================
// AUTH HELPERS
// ============================================

const ADMIN_SECRET = process.env.ADMIN_SECRET || 'circul-admin-secret-2026';
const AUTH_SECRET  = process.env.AUTH_SECRET  || process.env.BUYER_SECRET || 'circul-buyer-secret-2026';

function generateToken(payload, secret) {
  const data = JSON.stringify(payload);
  const b64  = Buffer.from(data).toString('base64url');
  const sig  = crypto.createHmac('sha256', secret).update(b64).digest('base64url');
  return b64 + '.' + sig;
}

function verifyToken(token, secret) {
  try {
    const [b64, sig] = token.split('.');
    const expected = crypto.createHmac('sha256', secret).update(b64).digest('base64url');
    if (sig !== expected) return null;
    return JSON.parse(Buffer.from(b64, 'base64url').toString());
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

app.post('/api/collectors/login', async (req, res) => {
  try {
    const { phone, pin } = req.body;
    if (!phone || !pin) return res.status(400).json({ success: false, message: 'Phone and PIN required' });
    const result = await pool.query(
      `SELECT id, first_name, last_name, phone, region, average_rating, created_at
       FROM collectors WHERE phone=$1 AND pin=$2 AND is_active=true`,
      [phone, pin]
    );
    if (!result.rows.length) return res.status(401).json({ success: false, message: 'Invalid phone or PIN' });
    res.json({ success: true, collector: result.rows[0] });
  } catch (err) {
    console.error('Error logging in collector:', err);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

app.get('/api/collectors', async (req, res) => {
  try {
    const { phone, include_login } = req.query;
    const params = [];
    let whereExtra = '';
    if (phone) { params.push(phone.trim()); whereExtra = ` AND c.phone=$${params.length}`; }
    const pinField = include_login === 'true' ? ', c.pin' : '';
    const result = await pool.query(
      `SELECT c.id, c.first_name, c.last_name, c.phone, c.city, c.region, c.average_rating,
              c.is_active, c.id_verified, c.created_at${pinField},
              'CIR-' || LPAD(c.id::text, 5, '0') AS display_name,
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
              'CIR-' || LPAD(c.id::text, 5, '0') AS display_name,
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
// AGGREGATORS
// ============================================

app.get('/api/aggregators', async (req, res) => {
  try {
    const { phone } = req.query;
    const params = []; let where = 'WHERE is_active=true';
    if (phone) { params.push(phone.trim()); where += ` AND phone=$${params.length}`; }
    const result = await pool.query(
      `SELECT id, name, company, phone, city, region, country, is_active, id_verified, created_at, 'AGG-' || LPAD(id::text, 5, '0') AS display_name FROM aggregators ${where} ORDER BY name ASC`,
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
      `SELECT id, name, company, phone, city, region, country, is_active, id_verified, created_at, 'AGG-' || LPAD(id::text, 5, '0') AS display_name FROM aggregators WHERE id=$1`,
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

    const [totals, monthlyTotals, pending, activeCollectors, byMaterial, topCollectors, postedPrices, ratings] = await Promise.all([
      pool.query(`SELECT COALESCE(SUM(net_weight_kg),0) as total_kg, COALESCE(SUM(total_price),0) as total_value, COUNT(*) as total_txns FROM transactions WHERE aggregator_id=$1`, [id]),
      pool.query(`SELECT COALESCE(SUM(net_weight_kg),0) as month_kg, COALESCE(SUM(total_price),0) as month_value, COUNT(*) as month_txns FROM transactions WHERE aggregator_id=$1 AND transaction_date>=$2`, [id, thisMonth.toISOString()]),
      pool.query(`SELECT COUNT(*) as count, COALESCE(SUM(total_price),0) as value FROM transactions WHERE aggregator_id=$1 AND payment_status='unpaid' AND total_price>0`, [id]),
      pool.query(`SELECT COUNT(DISTINCT collector_id) as count FROM transactions WHERE aggregator_id=$1`, [id]),
      pool.query(`SELECT material_type, SUM(net_weight_kg) as kg, COUNT(*) as txns FROM transactions WHERE aggregator_id=$1 GROUP BY material_type ORDER BY kg DESC`, [id]),
      pool.query(`SELECT c.id, c.first_name, c.last_name, c.phone, c.average_rating, c.city, 'CIR-' || LPAD(c.id::text, 5, '0') AS display_name, SUM(t.net_weight_kg) as total_kg, COUNT(t.id) as txns FROM collectors c JOIN transactions t ON t.collector_id=c.id WHERE t.aggregator_id=$1 GROUP BY c.id ORDER BY total_kg DESC LIMIT 20`, [id]),
      pool.query(`SELECT * FROM posted_prices WHERE poster_type='aggregator' AND poster_id=$1 AND is_active=true ORDER BY material_type`, [id]).catch(() => ({ rows: [] })),
      pool.query(`SELECT AVG(rating)::NUMERIC(3,2) as avg_rating, COUNT(*) as count FROM ratings WHERE rated_type='aggregator' AND rated_id=$1`, [id]).catch(() => ({ rows: [{ avg_rating: null, count: 0 }] }))
    ]);

    res.json({
      success: true,
      operator: { ...aggregator, role: 'aggregator' },
      stats: {
        totals: totals.rows[0], this_month: monthlyTotals.rows[0],
        pending_payments: pending.rows[0], active_collectors: activeCollectors.rows[0].count,
        by_material: byMaterial.rows, top_collectors: topCollectors.rows,
        posted_prices: postedPrices.rows, ratings: ratings.rows[0]
      }
    });
  } catch (err) {
    console.error('Aggregator stats error:', err);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

// ============================================
// PROCESSORS
// ============================================

app.get('/api/processors', async (req, res) => {
  try {
    const result = await pool.query(
      `SELECT id, name, company, email, phone, city, region, country, is_active, created_at FROM processors WHERE is_active=true ORDER BY company, name`
    );
    res.json({ success: true, processors: result.rows });
  } catch (err) {
    console.error('Error listing processors:', err);
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

// ============================================
// CONVERTERS
// ============================================

app.get('/api/converters', async (req, res) => {
  try {
    const { country } = req.query;
    const params = []; let where = 'WHERE is_active=true';
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

app.get('/api/converters/:id/stats', async (req, res) => {
  try {
    const { id } = req.params;
    const cv = await pool.query(`SELECT id, name, company, email, city, region, country FROM converters WHERE id=$1 AND is_active=true`, [id]);
    if (!cv.rows.length) return res.status(404).json({ success: false, message: 'Converter not found' });
    const thisMonth = new Date(); thisMonth.setDate(1); thisMonth.setHours(0,0,0,0);

    const [totals, monthlyTotals, inboundPending, postedPrices] = await Promise.all([
      pool.query(`SELECT COALESCE(SUM(gross_weight_kg),0) as total_kg, COALESCE(SUM(total_price),0) as total_value, COUNT(*) as total_txns FROM pending_transactions WHERE converter_id=$1 AND transaction_type='processor_sale'`, [id]),
      pool.query(`SELECT COALESCE(SUM(gross_weight_kg),0) as month_kg, COALESCE(SUM(total_price),0) as month_value, COUNT(*) as month_txns FROM pending_transactions WHERE converter_id=$1 AND transaction_type='processor_sale' AND created_at>=$2`, [id, thisMonth.toISOString()]),
      pool.query(`SELECT COUNT(*) as count, COALESCE(SUM(total_price),0) as value FROM pending_transactions WHERE converter_id=$1 AND status IN ('pending','dispatch_approved') AND transaction_type='processor_sale'`, [id]),
      pool.query(`SELECT * FROM posted_prices WHERE poster_type='converter' AND poster_id=$1 AND is_active=true ORDER BY material_type`, [id]).catch(() => ({ rows: [] }))
    ]);

    res.json({
      success: true, buyer: cv.rows[0],
      stats: { totals: totals.rows[0], this_month: monthlyTotals.rows[0], pending_payments: inboundPending.rows[0], posted_prices: postedPrices.rows }
    });
  } catch (err) {
    console.error('Converter stats error:', err);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

// ============================================
// TRANSACTIONS
// ============================================

app.post('/api/transactions', async (req, res) => {
  try {
    const { collector_id, aggregator_id, material_type, gross_weight_kg, contamination_deduction_percent = 0, contamination_types = [], quality_notes, price_per_kg, lat, lng, notes } = req.body;
    if (!collector_id || !material_type || !gross_weight_kg) return res.status(400).json({ success: false, message: 'collector_id, material_type, and gross_weight_kg are required' });
    const validMaterials = ['PET','HDPE','LDPE','PP'];
    if (!validMaterials.includes(material_type.toUpperCase())) return res.status(400).json({ success: false, message: `Invalid material type. Must be one of: ${validMaterials.join(', ')}` });
    if (parseFloat(gross_weight_kg) <= 0) return res.status(400).json({ success: false, message: 'Weight must be greater than 0' });
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

app.get('/api/transactions', async (req, res) => {
  try {
    const { collector_id, aggregator_id, material_type, start_date, end_date, limit = 100, offset = 0 } = req.query;
    let query = `SELECT t.*, c.first_name as collector_first_name, c.last_name as collector_last_name, c.phone as collector_phone, c.average_rating as collector_rating, 'CIR-' || LPAD(c.id::text, 5, '0') AS collector_display_name, a.name as aggregator_name, 'AGG-' || LPAD(a.id::text, 5, '0') AS aggregator_display_name FROM transactions t JOIN collectors c ON c.id=t.collector_id LEFT JOIN aggregators a ON a.id=t.aggregator_id WHERE 1=1`;
    const params = [];
    if (collector_id) { params.push(collector_id); query += ` AND t.collector_id=$${params.length}`; }
    if (aggregator_id) { params.push(aggregator_id); query += ` AND t.aggregator_id=$${params.length}`; }
    if (material_type) { params.push(material_type.toUpperCase()); query += ` AND t.material_type=$${params.length}`; }
    if (start_date) { params.push(start_date); query += ` AND t.transaction_date>=$${params.length}::timestamptz`; }
    if (end_date) { params.push(end_date); query += ` AND t.transaction_date<=$${params.length}::timestamptz`; }
    const countResult = await pool.query(query.replace(/SELECT t\.\*.*?FROM/s, 'SELECT COUNT(*) as total FROM'), params);
    params.push(parseInt(limit)); query += ` ORDER BY t.transaction_date DESC LIMIT $${params.length}`;
    params.push(parseInt(offset)); query += ` OFFSET $${params.length}`;
    const result = await pool.query(query, params);
    res.json({ success: true, transactions: result.rows, total: parseInt(countResult.rows?.[0]?.total||0), limit: parseInt(limit), offset: parseInt(offset) });
  } catch (err) {
    console.error('Error listing transactions:', err);
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

app.post('/api/ratings/operator', async (req, res) => {
  try {
    const { transaction_id, rater_type, rater_id, rated_type, rated_id, rater_operator_id, rated_operator_id, rater_collector_id, rated_collector_id, rating, tags, notes, rating_direction } = req.body;
    const finalRaterType = rater_type || (rater_operator_id ? 'aggregator' : 'collector');
    const finalRaterId   = rater_id   || rater_operator_id || rater_collector_id;
    const finalRatedType = rated_type || (rated_operator_id ? 'aggregator' : 'collector');
    const finalRatedId   = rated_id   || rated_operator_id || rated_collector_id;
    if (!finalRaterId || !finalRatedId || !rating) return res.status(400).json({ success: false, message: 'rater, rated, and rating are required' });
    if (rating < 1 || rating > 5) return res.status(400).json({ success: false, message: 'Rating must be 1-5' });
    const windowExpires = new Date(); windowExpires.setDate(windowExpires.getDate() + 30);
    const result = await pool.query(
      `INSERT INTO ratings (transaction_id, rater_type, rater_id, rated_type, rated_id, rating, tags, notes, rating_direction, window_expires_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) RETURNING *`,
      [transaction_id||null, finalRaterType, finalRaterId, finalRatedType, finalRatedId, rating, JSON.stringify(tags||[]), notes||null, rating_direction||null, windowExpires.toISOString()]
    );
    res.status(201).json({ success: true, rating: result.rows[0] });
  } catch (err) {
    console.error('Rating error:', err);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

app.get('/api/ratings/operator/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const ratings = await pool.query(`SELECT r.* FROM ratings r WHERE r.rated_id=$1 AND r.rated_type IN ('aggregator','processor','converter') ORDER BY r.created_at DESC LIMIT 50`, [id]);
    const avg = await pool.query(`SELECT AVG(rating)::NUMERIC(3,2) as avg_rating, COUNT(*) as count FROM ratings WHERE rated_id=$1 AND rated_type IN ('aggregator','processor','converter')`, [id]);
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
  if (parts[0] !== collector.pin) return 'END Invalid PIN.\nDial again to retry.';
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
    const { transaction_type, collector_id, aggregator_id, material_type, gross_weight_kg, price_per_kg } = req.body;
    if (!transaction_type || !collector_id || !aggregator_id || !material_type || !gross_weight_kg) return res.status(400).json({ success: false, message: 'transaction_type, collector_id, aggregator_id, material_type, and gross_weight_kg are required' });
    const validMaterials = ['PET','HDPE','LDPE','PP'];
    if (!validMaterials.includes(material_type.toUpperCase())) return res.status(400).json({ success: false, message: 'material_type must be one of PET, HDPE, LDPE, PP' });
    const kg = parseFloat(gross_weight_kg);
    if (isNaN(kg) || kg <= 0 || kg > 500) return res.status(400).json({ success: false, message: 'gross_weight_kg must be > 0 and at most 500 kg' });
    const collectorCheck = await pool.query(`SELECT id FROM collectors WHERE id=$1 AND is_active=true`, [collector_id]);
    if (!collectorCheck.rows.length) return res.status(400).json({ success: false, message: 'Collector not found' });
    const aggCheck = await pool.query(`SELECT id FROM aggregators WHERE id=$1 AND is_active=true`, [aggregator_id]);
    if (!aggCheck.rows.length) return res.status(400).json({ success: false, message: 'Aggregator not found' });
    const pricePer = price_per_kg ? parseFloat(price_per_kg) : 0;
    const totalPrice = parseFloat((kg * pricePer).toFixed(2));
    const result = await pool.query(`INSERT INTO pending_transactions (transaction_type, collector_id, aggregator_id, material_type, gross_weight_kg, price_per_kg, total_price, status) VALUES ($1,$2,$3,$4,$5,$6,$7,'pending') RETURNING *`, [transaction_type, collector_id, aggregator_id, material_type.toUpperCase(), kg, pricePer, totalPrice]);
    res.status(201).json({ success: true, pending_transaction: result.rows[0] });
  } catch (err) { console.error('Create pending transaction error:', err); res.status(500).json({ success: false, message: 'Server error' }); }
});

app.get('/api/pending-transactions', async (req, res) => {
  try {
    const { collector_id, aggregator_id, processor_id, type } = req.query;
    if (!collector_id && !aggregator_id && !processor_id) return res.status(400).json({ success: false, message: 'collector_id, aggregator_id, or processor_id required' });
    let query, params;
    if (collector_id) {
      query = `SELECT pt.*, a.name AS aggregator_name, 'AGG-' || LPAD(a.id::text, 5, '0') AS aggregator_display_name FROM pending_transactions pt LEFT JOIN aggregators a ON a.id=pt.aggregator_id WHERE pt.collector_id=$1 AND pt.status='pending' ORDER BY pt.created_at DESC`;
      params = [collector_id];
    } else if (processor_id) {
      query = `SELECT pt.*, a.name AS aggregator_name, 'AGG-' || LPAD(a.id::text, 5, '0') AS aggregator_display_name FROM pending_transactions pt LEFT JOIN aggregators a ON a.id=pt.aggregator_id WHERE pt.processor_id=$1 AND pt.status='pending' AND pt.transaction_type='aggregator_sale' ORDER BY pt.created_at DESC`;
      params = [processor_id];
    } else if (type === 'aggregator_sale') {
      query = `SELECT pt.*, p.name AS processor_name, p.company AS processor_company FROM pending_transactions pt LEFT JOIN processors p ON p.id=pt.processor_id WHERE pt.aggregator_id=$1 AND pt.status='pending' AND pt.transaction_type='aggregator_sale' ORDER BY pt.created_at DESC`;
      params = [aggregator_id];
    } else {
      query = `SELECT pt.*, c.first_name AS collector_first_name, c.last_name AS collector_last_name, 'CIR-' || LPAD(c.id::text, 5, '0') AS collector_display_name FROM pending_transactions pt LEFT JOIN collectors c ON c.id=pt.collector_id WHERE pt.aggregator_id=$1 AND pt.status='pending' AND pt.transaction_type IN ('collector_sale','aggregator_purchase') ORDER BY pt.created_at DESC`;
      params = [aggregator_id];
    }
    const result = await pool.query(query, params);
    res.json({ success: true, pending_transactions: result.rows });
  } catch (err) { console.error('Get pending transactions error:', err); res.status(500).json({ success: false, message: 'Server error' }); }
});

app.get('/api/pending-transactions/collector-sales', async (req, res) => {
  try {
    const { collector_id } = req.query;
    if (!collector_id) return res.status(400).json({ success: false, message: 'collector_id required' });
    const result = await pool.query(`SELECT pt.*, a.name AS aggregator_name, a.company AS aggregator_company, t.price_per_kg AS final_price_per_kg, t.total_price AS final_total_price FROM pending_transactions pt LEFT JOIN aggregators a ON a.id=pt.aggregator_id LEFT JOIN transactions t ON t.id=pt.transaction_id WHERE pt.transaction_type='collector_sale' AND pt.collector_id=$1 ORDER BY pt.created_at DESC LIMIT 20`, [collector_id]);
    res.json({ success: true, pending_transactions: result.rows });
  } catch (err) { console.error('Collector sales error:', err); res.status(500).json({ success: false, message: 'Server error' }); }
});

app.get('/api/pending-transactions/aggregator-sales', async (req, res) => {
  try {
    const { aggregator_id } = req.query;
    if (!aggregator_id) return res.status(400).json({ success: false, message: 'aggregator_id required' });
    const result = await pool.query(`SELECT pt.*, COALESCE(p.company, p.name) AS processor_company, p.name AS processor_name, COALESCE(c.company, c.name) AS converter_company, c.name AS converter_name FROM pending_transactions pt LEFT JOIN processors p ON p.id=pt.processor_id LEFT JOIN converters c ON c.id=pt.converter_id WHERE pt.transaction_type='aggregator_sale' AND pt.aggregator_id=$1 ORDER BY pt.created_at DESC LIMIT 20`, [aggregator_id]);
    res.json({ success: true, pending_transactions: result.rows });
  } catch (err) { console.error('Aggregator sales error:', err); res.status(500).json({ success: false, message: 'Server error' }); }
});

app.patch('/api/pending-transactions/:id/review', async (req, res) => {
  try {
    const { id } = req.params;
    const { action, grade, grade_notes, rejection_reason, price_per_kg } = req.body;
    if (!action || !['accept','reject'].includes(action)) return res.status(400).json({ success: false, message: 'action must be "accept" or "reject"' });
    const ptResult = await pool.query(`SELECT * FROM pending_transactions WHERE id=$1`, [id]);
    if (!ptResult.rows.length) return res.status(404).json({ success: false, message: 'Pending transaction not found' });
    const pt = ptResult.rows[0];
    if (pt.status !== 'pending') return res.status(409).json({ success: false, message: 'Transaction is no longer pending' });
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

app.post('/api/pending-transactions/aggregator-purchase', async (req, res) => {
  try {
    const { aggregator_id, collector_id, material_type, gross_weight_kg, price_per_kg } = req.body;
    if (!aggregator_id || !collector_id || !material_type || !gross_weight_kg) return res.status(400).json({ success: false, message: 'aggregator_id, collector_id, material_type, and gross_weight_kg are required' });
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

app.post('/api/pending-transactions/aggregator-sale', async (req, res) => {
  try {
    const { aggregator_id, processor_id, converter_id, material_type, gross_weight_kg, price_per_kg, notes, photo_urls } = req.body;
    if (!aggregator_id || (!processor_id && !converter_id) || !material_type || !gross_weight_kg || !price_per_kg) return res.status(400).json({ success: false, message: 'aggregator_id, processor_id or converter_id, material_type, gross_weight_kg, and price_per_kg are required' });
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
    const dispatchApproved = photosRequired ? null : true;
    const result = await pool.query(`INSERT INTO pending_transactions (transaction_type, aggregator_id, processor_id, converter_id, material_type, gross_weight_kg, price_per_kg, total_price, status, photos_required, photos_submitted, dispatch_approved, photo_urls, notes) VALUES ('aggregator_sale',$1,$2,$3,$4,$5,$6,$7,'pending',$8,false,$9,$10,$11) RETURNING *`, [aggregator_id, resolvedProcessorId, resolvedConverterId, material_type.toUpperCase(), kg, price, totalPrice, photosRequired, dispatchApproved, JSON.stringify(photo_urls||[]), notes||null]);
    res.status(201).json({ success: true, pending_transaction: result.rows[0], photos_required: photosRequired });
  } catch (err) { console.error('Aggregator sale error:', err); res.status(500).json({ success: false, message: 'Server error' }); }
});

app.get('/api/pending-transactions/processor-queue', requireAuth, async (req, res) => {
  try {
    if (!req.user.hasRole('processor')) return res.status(403).json({ success: false, message: 'Processor access only' });
    const result = await pool.query(`SELECT pt.*, a.name AS aggregator_name, a.company AS aggregator_company, 'AGG-' || LPAD(a.id::text, 5, '0') AS aggregator_display_name FROM pending_transactions pt LEFT JOIN aggregators a ON a.id=pt.aggregator_id WHERE pt.processor_id=$1 AND pt.transaction_type='aggregator_sale' ORDER BY pt.created_at DESC`, [req.user.id]);
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
    const { converter_id, material_type, gross_weight_kg, price_per_kg, notes } = req.body;
    if (!converter_id || !material_type || !gross_weight_kg || !price_per_kg) return res.status(400).json({ success: false, message: 'converter_id, material_type, gross_weight_kg, price_per_kg required' });
    const kg = parseFloat(gross_weight_kg), price = parseFloat(price_per_kg);
    if (isNaN(kg) || kg <= 0) return res.status(400).json({ success: false, message: 'Invalid weight' });
    if (isNaN(price) || price <= 0) return res.status(400).json({ success: false, message: 'Invalid price' });
    const convResult = await pool.query(`SELECT id FROM converters WHERE id=$1 AND is_active=true`, [converter_id]);
    if (!convResult.rows.length) return res.status(400).json({ success: false, message: 'Converter not found' });
    const result = await pool.query(`INSERT INTO pending_transactions (transaction_type, status, processor_id, converter_id, material_type, gross_weight_kg, price_per_kg, total_price, photos_required, photos_submitted, photo_urls, notes) VALUES ('processor_sale','pending',$1,$2,$3,$4,$5,$6,true,false,'{}', $7) RETURNING *`, [req.user.id, converter_id, material_type, kg, price, kg*price, notes||null]);
    res.status(201).json({ success: true, pending_transaction: result.rows[0] });
  } catch (err) { console.error('Processor sale error:', err); res.status(500).json({ success: false, message: 'Server error' }); }
});

app.get('/api/pending-transactions/processor-sales', requireAuth, async (req, res) => {
  try {
    if (!req.user.hasRole('processor')) return res.status(403).json({ success: false, message: 'Processor access only' });
    const result = await pool.query(`SELECT pt.*, c.name AS converter_name, c.company AS converter_company FROM pending_transactions pt LEFT JOIN converters c ON c.id=pt.converter_id WHERE pt.transaction_type='processor_sale' AND pt.processor_id=$1 ORDER BY pt.created_at DESC LIMIT 20`, [req.user.id]);
    res.json({ success: true, pending_transactions: result.rows });
  } catch (err) { console.error('Get processor sales error:', err); res.status(500).json({ success: false, message: 'Server error' }); }
});

app.get('/api/pending-transactions/converter-queue', requireAuth, async (req, res) => {
  try {
    if (!req.user.hasRole('converter')) return res.status(403).json({ success: false, message: 'Converter access only' });
    const converterId = req.user.converter_id || req.user.id;
    const result = await pool.query(`SELECT pt.*, p.name AS processor_name, p.company AS processor_company FROM pending_transactions pt LEFT JOIN processors p ON p.id=pt.processor_id WHERE pt.transaction_type='processor_sale' AND pt.converter_id=$1 ORDER BY pt.created_at DESC`, [converterId]);
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
// ORDERS API
// ============================================

app.post('/api/orders', requireAuth, async (req, res) => {
  try {
    if (!req.user.hasRole('converter')) return res.status(403).json({ success: false, message: 'Converter access only' });
    const { material_type, target_quantity_kg, price_per_kg, accepted_colours, excluded_contaminants, max_contamination_pct, notes } = req.body;
    if (!material_type || !target_quantity_kg || !price_per_kg) return res.status(400).json({ success: false, message: 'material_type, target_quantity_kg, price_per_kg required' });
    const qty = parseFloat(target_quantity_kg), price = parseFloat(price_per_kg);
    if (isNaN(qty) || qty <= 0) return res.status(400).json({ success: false, message: 'Invalid target_quantity_kg' });
    if (isNaN(price) || price <= 0) return res.status(400).json({ success: false, message: 'Invalid price_per_kg' });
    const converterId = req.user.converter_id || req.user.id;
    const result = await pool.query(`INSERT INTO orders (converter_id, material_type, target_quantity_kg, price_per_kg, accepted_colours, excluded_contaminants, max_contamination_pct, status, fulfilled_kg, notes) VALUES ($1,$2,$3,$4,$5,$6,$7,'open',0,$8) RETURNING *`, [converterId, material_type, qty, price, accepted_colours||null, excluded_contaminants||null, max_contamination_pct != null && max_contamination_pct !== '' ? parseFloat(max_contamination_pct) : null, notes||null]);
    res.status(201).json({ success: true, order: result.rows[0] });
  } catch (err) { console.error('Create order error:', err); res.status(500).json({ success: false, message: 'Server error' }); }
});

app.get('/api/orders/my', requireAuth, async (req, res) => {
  try {
    if (!req.user.hasRole('converter')) return res.status(403).json({ success: false, message: 'Converter access only' });
    const converterId = req.user.converter_id || req.user.id;
    const result = await pool.query(`SELECT * FROM orders WHERE converter_id=$1 ORDER BY created_at DESC LIMIT 20`, [converterId]);
    res.json({ success: true, orders: result.rows });
  } catch (err) { console.error('Get my orders error:', err); res.status(500).json({ success: false, message: 'Server error' }); }
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
      // 3. Converter
      const convResult = await pool.query(`SELECT id, name, company, email, password_hash FROM converters WHERE email=$1 AND is_active=true`, [emailLower]);

      if (!procResult.rows.length && !convResult.rows.length) return res.status(401).json({ success: false, message: 'Invalid credentials' });

      const checkRow = procResult.rows[0] || convResult.rows[0];
      const valid = await verifyPassword(password, checkRow.password_hash);
      if (!valid) return res.status(401).json({ success: false, message: 'Invalid credentials' });

      const isProcessor = procResult.rows.length > 0;
      const isConverter = convResult.rows.length > 0;

      if (isProcessor && isConverter) {
        const proc = procResult.rows[0], conv = convResult.rows[0];
        const token = generateToken({ type: 'buyer', id: proc.id, converter_id: conv.id, email: emailLower, roles: ['processor','converter'] }, AUTH_SECRET);
        return res.json({ success: true, role: null, roles: ['processor','converter'], token, user: { id: proc.id, converter_id: conv.id, name: proc.name, company: proc.company, email: emailLower } });
      }

      if (isProcessor) {
        const proc = procResult.rows[0];
        const token = generateToken({ type: 'buyer', id: proc.id, email: emailLower, role: 'processor' }, AUTH_SECRET);
        return res.json({ success: true, role: 'processor', roles: null, token, user: { id: proc.id, name: proc.name, company: proc.company, email: emailLower, role: 'processor' } });
      }

      const conv = convResult.rows[0];
      const token = generateToken({ type: 'buyer', id: conv.id, email: emailLower, role: 'converter' }, AUTH_SECRET);
      return res.json({ success: true, role: 'converter', roles: null, token, user: { id: conv.id, name: conv.name, company: conv.company, email: emailLower, role: 'converter' } });

    } else {
      if (!phone || !pin) return res.status(400).json({ success: false, message: 'Phone and PIN required' });

      // 1. Collectors
      const collResult = await pool.query(`SELECT id, first_name, last_name, phone FROM collectors WHERE phone=$1 AND pin=$2 AND is_active=true`, [phone.trim(), pin.trim()]);
      if (collResult.rows.length) {
        const c = collResult.rows[0];
        const name = ((c.first_name||'') + (c.last_name ? ' '+c.last_name : '')).trim();
        const token = generateToken({ type: 'collector', id: c.id, phone: c.phone, role: 'collector' }, AUTH_SECRET);
        return res.json({ success: true, role: 'collector', roles: null, token, user: { id: c.id, name, phone: c.phone, role: 'collector' } });
      }

      // 2. Aggregators
      const aggResult = await pool.query(`SELECT id, name, company, phone FROM aggregators WHERE phone=$1 AND pin=$2 AND is_active=true`, [phone.trim(), pin.trim()]);
      if (aggResult.rows.length) {
        const a = aggResult.rows[0];
        const token = generateToken({ type: 'aggregator', id: a.id, phone: a.phone, role: 'aggregator' }, AUTH_SECRET);
        return res.json({ success: true, role: 'aggregator', roles: null, token, user: { id: a.id, name: a.name, company: a.company||null, phone: a.phone, role: 'aggregator' } });
      }

      return res.status(401).json({ success: false, message: 'Invalid phone number or PIN' });
    }
  } catch (err) { console.error('Unified auth login error:', err); res.status(500).json({ success: false, message: 'Server error' }); }
});

app.get('/api/me/prices', requireAuth, async (req, res) => {
  try {
    const role = req.user.role || (req.user.roles && req.user.roles[0]);
    if (!['processor','converter'].includes(role)) return res.status(403).json({ success: false, message: 'Access denied' });
    const result = await pool.query(`SELECT * FROM posted_prices WHERE poster_type=$1 AND poster_id=$2 AND is_active=true ORDER BY material_type`, [role, req.user.id]);
    res.json({ success: true, prices: result.rows });
  } catch (err) { res.status(500).json({ success: false, message: 'Server error' }); }
});

// ============================================
// ADMIN ENDPOINTS
// ============================================

app.get('/api/admin/stats', requireAdmin, async (req, res) => {
  try {
    const [collectors, aggregators, processors, converters, transactions, volume] = await Promise.all([
      pool.query(`SELECT COUNT(*) as count FROM collectors WHERE is_active=true`),
      pool.query(`SELECT COUNT(*) as count FROM aggregators WHERE is_active=true`),
      pool.query(`SELECT COUNT(*) as count FROM processors WHERE is_active=true`),
      pool.query(`SELECT COUNT(*) as count FROM converters WHERE is_active=true`),
      pool.query(`SELECT COUNT(*) as count FROM transactions`),
      pool.query(`SELECT material_type, COALESCE(SUM(net_weight_kg),0) as total_kg, COUNT(*) as count FROM transactions GROUP BY material_type ORDER BY total_kg DESC`)
    ]);
    const totalVol = await pool.query(`SELECT COALESCE(SUM(net_weight_kg),0) as total FROM transactions`);
    res.json({ success: true, stats: { collectors: parseInt(collectors.rows[0].count), aggregators: parseInt(aggregators.rows[0].count), processors: parseInt(processors.rows[0].count), converters: parseInt(converters.rows[0].count), transactions: parseInt(transactions.rows[0].count), total_volume_kg: parseFloat(totalVol.rows[0].total), by_material: volume.rows } });
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
    const tableMap = { aggregator: 'aggregators', processor: 'processors', converter: 'converters' };
    const tbl = tableMap[resolvedPosterType];
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
    let posterTypes = [];
    if (role === 'collector') posterTypes = ['aggregator'];
    else if (role === 'aggregator') posterTypes = ['processor','aggregator'];
    else if (role === 'processor') posterTypes = ['processor','converter'];
    else if (role === 'converter') posterTypes = ['converter'];
    else posterTypes = ['aggregator','processor','converter'];
    const params = [posterTypes];
    let whereExtra = '';
    if (material) { params.push(material.toUpperCase()); whereExtra += ` AND pp.material_type=$${params.length}`; }
    let nearPrices = { rows: [] };
    if (city) {
      const nearParams = [...params, city];
      nearPrices = await pool.query(`SELECT pp.material_type, pp.price_per_kg_ghs, pp.posted_at as updated_at, pp.poster_type as operator_role, pp.city, CASE pp.poster_type WHEN 'aggregator' THEN (SELECT name FROM aggregators WHERE id=pp.poster_id LIMIT 1) WHEN 'processor' THEN (SELECT name FROM processors WHERE id=pp.poster_id LIMIT 1) WHEN 'converter' THEN (SELECT name FROM converters WHERE id=pp.poster_id LIMIT 1) END as operator_name FROM posted_prices pp WHERE pp.poster_type=ANY($1) AND pp.is_active=true AND pp.city=$${nearParams.length}${whereExtra} ORDER BY pp.material_type, pp.price_per_kg_ghs DESC`, nearParams);
    }
    const allPrices = await pool.query(`SELECT pp.material_type, pp.price_per_kg_ghs, pp.posted_at as updated_at, pp.poster_type as operator_role, pp.city, CASE pp.poster_type WHEN 'aggregator' THEN (SELECT name FROM aggregators WHERE id=pp.poster_id LIMIT 1) WHEN 'processor' THEN (SELECT name FROM processors WHERE id=pp.poster_id LIMIT 1) WHEN 'converter' THEN (SELECT name FROM converters WHERE id=pp.poster_id LIMIT 1) END as operator_name FROM posted_prices pp WHERE pp.poster_type=ANY($1) AND pp.is_active=true${whereExtra} ORDER BY pp.material_type, pp.price_per_kg_ghs DESC`, params);
    const nationalAvg = await pool.query(`SELECT material_type, AVG(price_per_kg_ghs) as avg_usd, COUNT(DISTINCT poster_id) as buyer_count FROM posted_prices WHERE poster_type=ANY($1) AND is_active=true${whereExtra.replace(/pp\./g,'')} GROUP BY material_type ORDER BY material_type`, params);
    res.json({ success: true, near_prices: nearPrices.rows, national_averages: nationalAvg.rows, all_prices: allPrices.rows });
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
    const transactions = await pool.query(`SELECT t.id, t.transaction_date, t.material_type, t.gross_weight_kg, t.net_weight_kg, t.contamination_deduction_percent, t.price_per_kg, t.total_price, t.payment_status, t.lat, t.lng, c.first_name||' '||c.last_name as collector_name, c.phone as collector_phone, c.city as collector_city, c.region as collector_region FROM transactions t JOIN collectors c ON c.id=t.collector_id WHERE t.aggregator_id=$1 ${dateFilter} ORDER BY t.transaction_date ASC`, params);
    const summary = await pool.query(`SELECT material_type, COUNT(*) as transaction_count, SUM(net_weight_kg) as total_kg_net, SUM(gross_weight_kg) as total_kg_gross, SUM(total_price) as total_paid_ghs, COUNT(DISTINCT t.collector_id) as unique_collectors FROM transactions t WHERE t.aggregator_id=$1 ${dateFilter} GROUP BY material_type ORDER BY material_type`, params);
    const report = { report_type: 'EPR_CSRD_COMPLIANCE', generated_at: new Date().toISOString(), aggregator: agg.rows[0], period: { start: start_date||'all-time', end: end_date||new Date().toISOString() }, summary_by_material: summary.rows, total_transactions: transactions.rows.length, transactions: transactions.rows, '@context': 'https://schema.org', '@type': 'DigitalProductPassport' };
    if (format === 'json') res.setHeader('Content-Disposition', `attachment; filename="compliance-report-${aggregator_id}-${Date.now()}.json"`);
    res.json(report);
  } catch (err) { console.error('Compliance report error:', err); res.status(500).json({ success: false, message: 'Server error' }); }
});

app.get('/api/reports/product-journey/:transaction_id', async (req, res) => {
  try {
    const { transaction_id } = req.params;
    const result = await pool.query(`SELECT t.*, c.first_name, c.last_name, c.city as collector_city, c.region as collector_region, a.name as aggregator_name, a.company as aggregator_company, a.city as aggregator_city FROM transactions t JOIN collectors c ON c.id=t.collector_id LEFT JOIN aggregators a ON a.id=t.aggregator_id WHERE t.id=$1`, [transaction_id]);
    if (!result.rows.length) return res.status(404).json({ success: false, message: 'Transaction not found' });
    const t = result.rows[0];
    res.json({ success: true, journey: { collector: { name: t.first_name+' '+t.last_name, city: t.collector_city, region: t.collector_region }, material: t.material_type, weight_kg: t.net_weight_kg, collected_at: t.transaction_date, location: t.lat ? { lat: t.lat, lng: t.lng } : { city: t.collector_city }, aggregator: t.aggregator_name ? { name: t.aggregator_name, company: t.aggregator_company, city: t.aggregator_city } : null, verified: t.payment_status === 'paid' } });
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
app.get('/report',               (req, res) => res.sendFile(path.join(__dirname, 'public', 'report.html')));
app.get('/passport',             (req, res) => res.sendFile(path.join(__dirname, 'public', 'report.html')));
app.get('/login',                (req, res) => res.sendFile(path.join(__dirname, 'public', 'login.html')));
app.get('/prices',               (req, res) => res.redirect('/'));

app.listen(port, () => console.log(`Circul server running on port ${port}`));
