const express = require('express');
const { Pool } = require('pg');
const path = require('path');
const fs = require('fs');

const app = express();
const port = process.env.PORT || 3000;

// Fail fast if DATABASE_URL is missing
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

// Trust proxy for Render
app.set('trust proxy', 1);

// Health check endpoint (required for Render)
app.get('/health', (req, res) => {
  res.json({ status: 'healthy' });
});

// Serve static files from public folder
app.use(express.static(path.join(__dirname, 'public')));

// ============================================
// API ROUTES
// ============================================

// --- COLLECTORS ---

// Register a new collector
app.post('/api/collectors', async (req, res) => {
  try {
    const { first_name, last_name, phone, pin, region } = req.body;
    if (!first_name || !pin) {
      return res.status(400).json({ success: false, message: 'First name and PIN are required' });
    }
    if (pin.length < 4 || pin.length > 6) {
      return res.status(400).json({ success: false, message: 'PIN must be 4-6 digits' });
    }
    const result = await pool.query(
      `INSERT INTO collectors (first_name, last_name, phone, pin, region) VALUES ($1, $2, $3, $4, $5) RETURNING id, first_name, last_name, phone, region, average_rating, created_at`,
      [first_name, last_name || '', phone || null, pin, region || null]
    );
    res.status(201).json({ success: true, collector: result.rows[0] });
  } catch (err) {
    if (err.code === '23505') {
      return res.status(409).json({ success: false, message: 'Phone number already registered' });
    }
    console.error('Error creating collector:', err);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

// Collector login (simple PIN auth)
app.post('/api/collectors/login', async (req, res) => {
  try {
    const { phone, pin } = req.body;
    if (!phone || !pin) {
      return res.status(400).json({ success: false, message: 'Phone and PIN required' });
    }
    const result = await pool.query(
      `SELECT id, first_name, last_name, phone, region, average_rating, created_at FROM collectors WHERE phone = $1 AND pin = $2 AND is_active = true`,
      [phone, pin]
    );
    if (result.rows.length === 0) {
      return res.status(401).json({ success: false, message: 'Invalid phone or PIN' });
    }
    res.json({ success: true, collector: result.rows[0] });
  } catch (err) {
    console.error('Error logging in collector:', err);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

// List collectors (for operator)
app.get('/api/collectors', async (req, res) => {
  try {
    const { phone } = req.query;
    const params = [];
    let whereExtra = '';
    if (phone) {
      params.push(phone.trim());
      whereExtra = ' AND c.phone = $1';
    }
    const result = await pool.query(
      `SELECT c.id, c.first_name, c.last_name, c.phone, c.region, c.average_rating, c.created_at, c.is_active,
        COALESCE(SUM(t.net_weight_kg), 0) as total_weight_kg,
        COUNT(t.id) as transaction_count
       FROM collectors c
       LEFT JOIN transactions t ON t.collector_id = c.id
       WHERE c.is_active = true${whereExtra}
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

// GET single collector by collectors.id
app.get('/api/collectors/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const result = await pool.query(
      `SELECT c.id, c.first_name, c.last_name, c.phone, c.region, c.average_rating, c.created_at, c.is_active,
        COALESCE(SUM(t.net_weight_kg), 0) as total_weight_kg,
        COUNT(t.id) as transaction_count
       FROM collectors c
       LEFT JOIN transactions t ON t.collector_id = c.id
       WHERE c.id = $1
       GROUP BY c.id`,
      [parseInt(id)]
    );
    if (!result.rows.length) return res.status(404).json({ success: false, message: 'Collector not found' });
    const c = result.rows[0];
    c.name = ((c.first_name || '') + (c.last_name ? ' ' + c.last_name : '')).trim();
    return res.json({ success: true, collector: c });
  } catch (err) {
    console.error('Error fetching collector:', err);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

// --- TRANSACTIONS ---

// Create a new transaction (replaces collection logging)
app.post('/api/transactions', async (req, res) => {
  try {
    const {
      collector_id,
      buyer_id,
      material_type,
      gross_weight_kg,
      contamination_deduction_percent = 0,
      contamination_types = [],
      quality_notes,
      price_per_kg,
      lat,
      lng,
      notes
    } = req.body;

    if (!collector_id || !material_type || !gross_weight_kg) {
      return res.status(400).json({ success: false, message: 'collector_id, material_type, and gross_weight_kg are required' });
    }

    const validMaterials = ['PET', 'HDPE', 'LDPE', 'PP'];
    if (!validMaterials.includes(material_type.toUpperCase())) {
      return res.status(400).json({ success: false, message: `Invalid material type. Must be one of: ${validMaterials.join(', ')}` });
    }

    if (parseFloat(gross_weight_kg) <= 0) {
      return res.status(400).json({ success: false, message: 'Weight must be greater than 0' });
    }

    // Calculate net weight after contamination deduction
    const deduction = parseFloat(contamination_deduction_percent) || 0;
    const net_weight_kg = parseFloat(gross_weight_kg) * (1 - deduction / 100);

    // Calculate total price if price_per_kg provided
    const total_price = price_per_kg ? (net_weight_kg * parseFloat(price_per_kg)).toFixed(2) : null;

    const result = await pool.query(
      `INSERT INTO transactions (
        collector_id, buyer_id, material_type,
        gross_weight_kg, net_weight_kg, contamination_deduction_percent, contamination_types,
        quality_notes, price_per_kg, total_price, lat, lng, notes
      )
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
       RETURNING *`,
      [
        collector_id,
        buyer_id || null,
        material_type.toUpperCase(),
        gross_weight_kg,
        net_weight_kg,
        deduction,
        JSON.stringify(contamination_types),
        quality_notes || null,
        price_per_kg || null,
        total_price,
        lat || null,
        lng || null,
        notes || null
      ]
    );

    res.status(201).json({ success: true, transaction: result.rows[0] });
  } catch (err) {
    if (err.code === '23503') {
      return res.status(400).json({ success: false, message: 'Collector not found' });
    }
    console.error('Error creating transaction:', err);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

// Get transactions with filters
app.get('/api/transactions', async (req, res) => {
  try {
    const { collector_id, buyer_id, operator_id, material_type, start_date, end_date, limit = 100, offset = 0 } = req.query;

    let query = `
      SELECT t.*, t.payment_status, t.payment_method, t.payment_phone,
        c.first_name as collector_first_name,
        c.last_name as collector_last_name,
        c.phone as collector_phone,
        c.average_rating as collector_rating,
        b.first_name as buyer_first_name,
        b.last_name as buyer_last_name
      FROM transactions t
      JOIN collectors c ON c.id = t.collector_id
      LEFT JOIN collectors b ON b.id = t.buyer_id
      WHERE 1=1
    `;
    const params = [];

    if (collector_id) {
      params.push(collector_id);
      query += ` AND t.collector_id = $${params.length}`;
    }
    if (buyer_id) {
      params.push(buyer_id);
      query += ` AND t.buyer_id = $${params.length}`;
    }
    if (operator_id) {
      params.push(operator_id);
      query += ` AND t.operator_id = $${params.length}`;
    }
    if (material_type) {
      params.push(material_type.toUpperCase());
      query += ` AND t.material_type = $${params.length}`;
    }
    if (start_date) {
      params.push(start_date);
      query += ` AND t.transaction_date >= $${params.length}::timestamptz`;
    }
    if (end_date) {
      params.push(end_date);
      query += ` AND t.transaction_date <= $${params.length}::timestamptz`;
    }

    // Count total — use non-greedy dotAll regex so it matches across newlines in the multi-line SELECT clause
    const countResult = await pool.query(
      query.replace(/SELECT t\.\*.*?FROM/s, 'SELECT COUNT(*) as total FROM'),
      params
    );

    // Add pagination
    params.push(parseInt(limit));
    query += ` ORDER BY t.transaction_date DESC LIMIT $${params.length}`;
    params.push(parseInt(offset));
    query += ` OFFSET $${params.length}`;

    const result = await pool.query(query, params);

    res.json({
      success: true,
      transactions: result.rows,
      total: parseInt(countResult.rows?.[0]?.total || 0),
      limit: parseInt(limit),
      offset: parseInt(offset)
    });
  } catch (err) {
    console.error('Error listing transactions:', err);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

// Dashboard stats (supports operator_id filter for role-based views)
app.get('/api/stats', async (req, res) => {
  try {
    const { start_date, end_date, operator_id } = req.query;

    let dateFilter = '';
    const params = [];
    if (start_date) {
      params.push(start_date);
      dateFilter += ` AND t.transaction_date >= $${params.length}::timestamptz`;
    }
    if (end_date) {
      params.push(end_date);
      dateFilter += ` AND t.transaction_date <= $${params.length}::timestamptz`;
    }
    if (operator_id) {
      params.push(operator_id);
      dateFilter += ` AND t.operator_id = $${params.length}`;
    }

    // Total weight by material (using net weight after contamination deduction)
    const materialStats = await pool.query(
      `SELECT material_type,
              SUM(net_weight_kg) as total_kg,
              SUM(gross_weight_kg) as total_gross_kg,
              AVG(contamination_deduction_percent) as avg_contamination_percent,
              COUNT(*) as transaction_count
       FROM transactions t
       WHERE 1=1 ${dateFilter}
       GROUP BY material_type
       ORDER BY total_kg DESC`,
      params
    );

    // Overall totals
    const totals = await pool.query(
      `SELECT
        COALESCE(SUM(net_weight_kg), 0) as total_weight_kg,
        COALESCE(SUM(gross_weight_kg), 0) as total_gross_weight_kg,
        COALESCE(SUM(total_price), 0) as total_revenue,
        COUNT(*) as total_transactions,
        COUNT(DISTINCT collector_id) as active_collectors
       FROM transactions t
       WHERE 1=1 ${dateFilter}`,
      params
    );

    // Today's transactions
    const todayParams = [];
    let todayFilter = '';
    if (operator_id) {
      todayParams.push(operator_id);
      todayFilter = ` AND operator_id = $${todayParams.length}`;
    }
    const today = await pool.query(
      `SELECT
        COALESCE(SUM(net_weight_kg), 0) as today_weight_kg,
        COALESCE(SUM(total_price), 0) as today_revenue,
        COUNT(*) as today_transactions
       FROM transactions
       WHERE transaction_date >= CURRENT_DATE${todayFilter}`,
      todayParams
    );

    // Top collectors (by net weight and rating)
    const topCollectors = await pool.query(
      `SELECT c.id, c.first_name, c.last_name, c.phone, c.average_rating,
              SUM(t.net_weight_kg) as total_kg,
              AVG(t.contamination_deduction_percent) as avg_contamination,
              COUNT(t.id) as transactions
       FROM collectors c
       JOIN transactions t ON t.collector_id = c.id
       WHERE 1=1 ${dateFilter}
       GROUP BY c.id
       ORDER BY total_kg DESC
       LIMIT 10`,
      params
    );

    // Daily trend (last 7 days)
    const trendParams = [];
    let trendFilter = '';
    if (operator_id) {
      trendParams.push(operator_id);
      trendFilter = ` AND operator_id = $${trendParams.length}`;
    }
    const dailyTrend = await pool.query(
      `SELECT DATE(transaction_date) as date,
              SUM(net_weight_kg) as total_kg,
              SUM(total_price) as revenue,
              COUNT(*) as transactions
       FROM transactions
       WHERE transaction_date >= CURRENT_DATE - INTERVAL '7 days'${trendFilter}
       GROUP BY DATE(transaction_date)
       ORDER BY date ASC`,
      trendParams
    );

    // Quality stats
    const qualityParams = [];
    let qualityFilter = '';
    if (operator_id) {
      qualityParams.push(operator_id);
      qualityFilter = ` AND operator_id = $${qualityParams.length}`;
    }
    const qualityStats = await pool.query(
      `SELECT
        AVG(contamination_deduction_percent) as avg_contamination,
        COUNT(*) FILTER (WHERE contamination_deduction_percent = 0) as clean_count,
        COUNT(*) FILTER (WHERE contamination_deduction_percent > 0 AND contamination_deduction_percent <= 10) as low_contamination,
        COUNT(*) FILTER (WHERE contamination_deduction_percent > 10) as high_contamination
       FROM transactions
       WHERE transaction_date >= CURRENT_DATE - INTERVAL '30 days'${qualityFilter}`,
      qualityParams
    );

    res.json({
      success: true,
      stats: {
        totals: totals.rows[0],
        today: today.rows[0],
        by_material: materialStats.rows,
        top_collectors: topCollectors.rows,
        daily_trend: dailyTrend.rows,
        quality: qualityStats.rows[0]
      }
    });
  } catch (err) {
    console.error('Error fetching stats:', err);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

// Collector's own stats
app.get('/api/collectors/:id/stats', async (req, res) => {
  try {
    const { id } = req.params;

    const collector = await pool.query(
      `SELECT id, first_name, last_name, phone, region, average_rating, created_at FROM collectors WHERE id = $1`,
      [id]
    );
    if (collector.rows.length === 0) {
      return res.status(404).json({ success: false, message: 'Collector not found' });
    }

    const stats = await pool.query(
      `SELECT material_type,
              SUM(net_weight_kg) as total_kg,
              SUM(total_price) as total_earned,
              AVG(contamination_deduction_percent) as avg_contamination,
              COUNT(*) as count
       FROM transactions WHERE collector_id = $1
       GROUP BY material_type ORDER BY total_kg DESC`,
      [id]
    );

    const total = await pool.query(
      `SELECT
        COALESCE(SUM(net_weight_kg), 0) as total_kg,
        COALESCE(SUM(total_price), 0) as total_earned,
        COUNT(*) as total_transactions
       FROM transactions WHERE collector_id = $1`,
      [id]
    );

    const recent = await pool.query(
      `SELECT * FROM transactions WHERE collector_id = $1 ORDER BY transaction_date DESC LIMIT 10`,
      [id]
    );

    const todayStats = await pool.query(
      `SELECT
        COALESCE(SUM(net_weight_kg), 0) as today_kg,
        COALESCE(SUM(total_price), 0) as today_earned,
        COUNT(*) as today_count
       FROM transactions WHERE collector_id = $1 AND transaction_date >= CURRENT_DATE`,
      [id]
    );

    const ratings = await pool.query(
      `SELECT r.*, t.material_type, t.net_weight_kg
       FROM ratings r
       LEFT JOIN transactions t ON t.id = r.transaction_id
       WHERE r.collector_id = $1
       ORDER BY r.created_at DESC LIMIT 10`,
      [id]
    ).catch(() => ({ rows: [] }));

    res.json({
      success: true,
      collector: collector.rows[0],
      stats: {
        ...total.rows[0],
        today: todayStats.rows[0],
        by_material: stats.rows,
        recent_transactions: recent.rows,
        recent_ratings: ratings.rows
      }
    });
  } catch (err) {
    console.error('Error fetching collector stats:', err);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

// --- RATINGS ---

// Submit a rating for a collector
app.post('/api/ratings', async (req, res) => {
  try {
    const { transaction_id, collector_id, buyer_id, rating, notes } = req.body;

    if (!collector_id || !rating) {
      return res.status(400).json({ success: false, message: 'collector_id and rating are required' });
    }

    if (rating < 1 || rating > 5) {
      return res.status(400).json({ success: false, message: 'Rating must be between 1 and 5' });
    }

    // Check if transaction exists (if provided)
    if (transaction_id) {
      const txCheck = await pool.query(
        `SELECT id FROM transactions WHERE id = $1 AND collector_id = $2`,
        [transaction_id, collector_id]
      );
      if (txCheck.rows.length === 0) {
        return res.status(400).json({ success: false, message: 'Transaction not found or does not belong to this collector' });
      }

      // Check if rating already exists for this transaction
      const existingRating = await pool.query(
        `SELECT id FROM ratings WHERE transaction_id = $1`,
        [transaction_id]
      );
      if (existingRating.rows.length > 0) {
        return res.status(409).json({ success: false, message: 'Rating already exists for this transaction' });
      }
    }

    const result = await pool.query(
      `INSERT INTO ratings (transaction_id, collector_id, buyer_id, rating, notes)
       VALUES ($1, $2, $3, $4, $5)
       RETURNING *`,
      [transaction_id || null, collector_id, buyer_id || null, rating, notes || null]
    );

    res.status(201).json({ success: true, rating: result.rows[0] });
  } catch (err) {
    console.error('Error creating rating:', err);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

// Get all ratings for a collector
app.get('/api/collectors/:id/ratings', async (req, res) => {
  try {
    const { id } = req.params;
    const { limit = 50, offset = 0 } = req.query;

    // Check if collector exists
    const collector = await pool.query(
      `SELECT id, first_name, last_name, average_rating FROM collectors WHERE id = $1`,
      [id]
    );
    if (collector.rows.length === 0) {
      return res.status(404).json({ success: false, message: 'Collector not found' });
    }

    // Get ratings with transaction details
    const ratings = await pool.query(
      `SELECT r.*,
              t.material_type, t.net_weight_kg, t.transaction_date,
              b.first_name as buyer_first_name, b.last_name as buyer_last_name
       FROM ratings r
       LEFT JOIN transactions t ON t.id = r.transaction_id
       LEFT JOIN collectors b ON b.id = r.buyer_id
       WHERE r.collector_id = $1
       ORDER BY r.created_at DESC
       LIMIT $2 OFFSET $3`,
      [id, parseInt(limit), parseInt(offset)]
    );

    // Get rating distribution
    const distribution = await pool.query(
      `SELECT rating, COUNT(*) as count
       FROM ratings
       WHERE collector_id = $1
       GROUP BY rating
       ORDER BY rating DESC`,
      [id]
    );

    res.json({
      success: true,
      collector: collector.rows[0],
      ratings: ratings.rows,
      distribution: distribution.rows
    });
  } catch (err) {
    console.error('Error fetching ratings:', err);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

// --- MOBILE MONEY PAYMENTS ---

// Initiate payment to collector after transaction
app.post('/api/payments/initiate', async (req, res) => {
  try {
    const { transaction_id, operator_id, phone, provider } = req.body;

    if (!transaction_id || !phone || !provider) {
      return res.status(400).json({ success: false, message: 'transaction_id, phone, and provider are required' });
    }

    const validProviders = ['mtn_momo', 'vodafone_cash', 'airteltigo'];
    if (!validProviders.includes(provider)) {
      return res.status(400).json({ success: false, message: `Provider must be one of: ${validProviders.join(', ')}` });
    }

    // Get transaction details
    const txResult = await pool.query(
      `SELECT t.*, c.first_name, c.last_name, c.phone as collector_phone
       FROM transactions t
       JOIN collectors c ON c.id = t.collector_id
       WHERE t.id = $1`,
      [transaction_id]
    );

    if (txResult.rows.length === 0) {
      return res.status(404).json({ success: false, message: 'Transaction not found' });
    }

    const tx = txResult.rows[0];

    if (tx.payment_status === 'paid') {
      return res.status(409).json({ success: false, message: 'Transaction already paid' });
    }

    if (tx.payment_status === 'pending') {
      return res.status(409).json({ success: false, message: 'Payment already initiated and pending' });
    }

    const amount = parseFloat(tx.total_price) || 0;
    if (amount <= 0) {
      return res.status(400).json({ success: false, message: 'Transaction has no price set. Set price_per_kg first.' });
    }

    // Normalize phone for Ghana
    const paymentPhone = normalizeGhanaPhone(phone);
    const reference = 'CIR-' + Date.now() + '-' + transaction_id;

    // Create payment record
    const paymentResult = await pool.query(
      `INSERT INTO payments (transaction_id, collector_id, operator_id, amount, phone, provider, reference, status)
       VALUES ($1, $2, $3, $4, $5, $6, $7, 'pending')
       RETURNING *`,
      [transaction_id, tx.collector_id, operator_id || null, amount, paymentPhone, provider, reference]
    );

    // Update transaction payment status
    await pool.query(
      `UPDATE transactions SET payment_status = 'pending', payment_method = $1, payment_phone = $2, payment_reference = $3, payment_initiated_at = NOW()
       WHERE id = $4`,
      [provider, paymentPhone, reference, transaction_id]
    );

    // TODO: In production, call the actual Mobile Money API here:
    // - MTN MoMo: POST to https://sandbox.momodeveloper.mtn.com/disbursement/v1_0/transfer
    // - Vodafone Cash: Via Hubtel or direct API
    // - Paystack: POST to https://api.paystack.co/transfer
    //
    // For now, we simulate a successful payment after a short delay.
    // The webhook/callback would update the payment status.

    // Simulate: mark as processing (in production, the webhook handles this)
    await pool.query(
      `UPDATE payments SET status = 'processing' WHERE id = $1`,
      [paymentResult.rows[0].id]
    );

    res.status(201).json({
      success: true,
      payment: paymentResult.rows[0],
      message: `Payment of GHS ${amount.toFixed(2)} initiated to ${paymentPhone} via ${provider}`,
      note: 'Payment is being processed. Check status with GET /api/payments/:id'
    });
  } catch (err) {
    console.error('Error initiating payment:', err);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

// Confirm/complete a payment (webhook callback or manual confirmation)
app.post('/api/payments/:id/confirm', async (req, res) => {
  try {
    const { id } = req.params;
    const { provider_reference } = req.body;

    const payment = await pool.query(`SELECT * FROM payments WHERE id = $1`, [id]);
    if (payment.rows.length === 0) {
      return res.status(404).json({ success: false, message: 'Payment not found' });
    }

    const p = payment.rows[0];
    if (p.status === 'success') {
      return res.status(409).json({ success: false, message: 'Payment already confirmed' });
    }

    // Mark payment as successful
    await pool.query(
      `UPDATE payments SET status = 'success', provider_reference = $1, updated_at = NOW() WHERE id = $2`,
      [provider_reference || null, id]
    );

    // Update transaction
    await pool.query(
      `UPDATE transactions SET payment_status = 'paid', payment_completed_at = NOW() WHERE id = $1`,
      [p.transaction_id]
    );

    res.json({ success: true, message: 'Payment confirmed' });
  } catch (err) {
    console.error('Error confirming payment:', err);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

// Get payment status
app.get('/api/payments/:id', async (req, res) => {
  try {
    const result = await pool.query(
      `SELECT p.*, c.first_name, c.last_name, t.material_type, t.net_weight_kg
       FROM payments p
       JOIN collectors c ON c.id = p.collector_id
       JOIN transactions t ON t.id = p.transaction_id
       WHERE p.id = $1`,
      [req.params.id]
    );
    if (result.rows.length === 0) {
      return res.status(404).json({ success: false, message: 'Payment not found' });
    }
    res.json({ success: true, payment: result.rows[0] });
  } catch (err) {
    console.error('Error fetching payment:', err);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

// List payments for a transaction
app.get('/api/transactions/:id/payments', async (req, res) => {
  try {
    const result = await pool.query(
      `SELECT * FROM payments WHERE transaction_id = $1 ORDER BY created_at DESC`,
      [req.params.id]
    );
    res.json({ success: true, payments: result.rows });
  } catch (err) {
    console.error('Error fetching payments:', err);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

// ============================================
// USSD INTERFACE (Africa's Talking)
// ============================================

// Phone number normalization for Ghana (+233)
function normalizeGhanaPhone(phone) {
  if (!phone) return null;
  let cleaned = phone.replace(/[\s\-()]/g, '');
  if (cleaned.startsWith('+233')) return cleaned;
  if (cleaned.startsWith('233')) return '+' + cleaned;
  if (cleaned.startsWith('0')) return '+233' + cleaned.slice(1);
  return cleaned;
}

// Get all possible phone formats to match DB entries
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

// Handle unregistered user USSD flow
async function handleUnregisteredUssd(parts, phone) {
  const level = parts.length;

  if (level === 0) {
    return 'CON Welcome to Circul\n1. Register\n2. Exit';
  }

  if (parts[0] === '2') {
    return 'END Thank you for using Circul.';
  }

  if (parts[0] === '1') {
    if (level === 1) return 'CON Enter your first name:';
    if (level === 2) return 'CON Enter last name\n(0 to skip):';
    if (level === 3) return 'CON Create a 4-digit PIN:';
    if (level === 4) {
      const firstName = parts[1].trim();
      const lastName = parts[2] === '0' ? '' : parts[2].trim();
      const pin = parts[3].trim();

      if (pin.length < 4 || pin.length > 6 || !/^\d+$/.test(pin)) {
        return 'END PIN must be 4-6 digits.\nDial again to retry.';
      }

      try {
        await pool.query(
          `INSERT INTO collectors (first_name, last_name, phone, pin, region)
           VALUES ($1, $2, $3, $4, $5)`,
          [firstName, lastName, phone, pin, 'Ghana']
        );
        return `END Registered! Welcome ${firstName}.\nDial again to start.`;
      } catch (err) {
        if (err.code === '23505') {
          return 'END Phone already registered.\nDial again to login.';
        }
        throw err;
      }
    }
  }

  return 'END Invalid option.\nDial again to retry.';
}

// Handle registered user USSD flow
async function handleRegisteredUssd(parts, collector) {
  const level = parts.length;

  if (level === 0) {
    return `CON Welcome ${collector.first_name}!\nEnter your PIN:`;
  }

  // Verify PIN (first input is always PIN)
  if (parts[0] !== collector.pin) {
    return 'END Invalid PIN.\nDial again to retry.';
  }

  // PIN verified — show main menu
  if (level === 1) {
    return 'CON 1. Log Collection\n2. Check Balance\n3. Exit';
  }

  const menu = parts[1];

  // --- EXIT ---
  if (menu === '3') {
    return `END Thank you, ${collector.first_name}!`;
  }

  // --- CHECK BALANCE ---
  if (menu === '2') {
    const stats = await pool.query(
      `SELECT COALESCE(SUM(net_weight_kg),0) as total_kg,
              COALESCE(SUM(total_price),0) as earned,
              COUNT(*) as txns
       FROM transactions WHERE collector_id = $1`,
      [collector.id]
    );
    const s = stats.rows[0];
    return `END Balance:\nTotal: ${parseFloat(s.total_kg).toFixed(1)}kg\nEarned: GHS ${parseFloat(s.earned).toFixed(2)}\nTransactions: ${s.txns}`;
  }

  // --- LOG COLLECTION ---
  if (menu === '1') {
    if (level === 2) {
      return 'CON Select material:\n1.PET 2.HDPE\n3.LDPE 4.PP';
    }

    const material = USSD_MATERIALS[parts[2]];
    if (!material) {
      return 'END Invalid material.\nDial again to retry.';
    }

    if (level === 3) {
      return `CON Enter weight in kg:`;
    }

    const weight = parseFloat(parts[3]);
    if (isNaN(weight) || weight <= 0 || weight > 9999) {
      return 'END Invalid weight.\nDial again to retry.';
    }

    if (level === 4) {
      return `CON Log ${weight}kg ${material}?\n1. Confirm\n2. Cancel`;
    }

    if (level === 5) {
      if (parts[4] === '2') {
        return 'END Cancelled.';
      }
      if (parts[4] === '1') {
        await pool.query(
          `INSERT INTO transactions (collector_id, material_type, gross_weight_kg, net_weight_kg)
           VALUES ($1, $2, $3, $4)`,
          [collector.id, material, weight, weight]
        );

        const today = await pool.query(
          `SELECT COALESCE(SUM(net_weight_kg),0) as today_kg
           FROM transactions
           WHERE collector_id = $1 AND transaction_date >= CURRENT_DATE`,
          [collector.id]
        );

        return `END Logged! ${weight}kg ${material}\nToday: ${parseFloat(today.rows[0].today_kg).toFixed(1)}kg total`;
      }
    }
  }

  return 'END Invalid option.\nDial again to retry.';
}

// Africa's Talking USSD callback
// POST body (x-www-form-urlencoded): sessionId, serviceCode, phoneNumber, text
// Response: plain text prefixed with "CON " (continue) or "END " (terminate)
app.post('/api/ussd', async (req, res) => {
  const { sessionId, serviceCode, phoneNumber, text } = req.body;
  const phone = normalizeGhanaPhone(phoneNumber);
  const parts = text ? text.split('*') : [];
  let response = '';
  let collectorId = null;

  try {
    // Look up collector by phone (try all Ghana phone formats)
    const phoneVariants = getPhoneVariants(phone);
    const result = await pool.query(
      `SELECT id, first_name, last_name, phone, pin
       FROM collectors
       WHERE phone = ANY($1) AND is_active = true
       LIMIT 1`,
      [phoneVariants]
    );

    if (result.rows.length === 0) {
      response = await handleUnregisteredUssd(parts, phone);
    } else {
      collectorId = result.rows[0].id;
      response = await handleRegisteredUssd(parts, result.rows[0]);
    }
  } catch (err) {
    console.error('[USSD] Error:', err);
    response = 'END System error. Try again later.';
  }

  // Log session for analytics
  try {
    await pool.query(
      `INSERT INTO ussd_sessions (session_id, phone, service_code, collector_id, text_input, response)
       VALUES ($1, $2, $3, $4, $5, $6)`,
      [sessionId, phone, serviceCode, collectorId, text || '', response]
    );
  } catch (logErr) {
    console.error('[USSD] Log error:', logErr);
  }

  res.set('Content-Type', 'text/plain');
  res.send(response);
});

// USSD session analytics (for operator dashboard)
app.get('/api/ussd/stats', async (req, res) => {
  try {
    const stats = await pool.query(`
      SELECT
        COUNT(DISTINCT session_id) as total_sessions,
        COUNT(DISTINCT phone) as unique_phones,
        COUNT(*) FILTER (WHERE response LIKE 'END Logged!%') as successful_logs,
        COUNT(*) FILTER (WHERE response LIKE 'END Registered!%') as registrations,
        COUNT(*) FILTER (WHERE created_at >= CURRENT_DATE) as today_sessions
      FROM ussd_sessions
    `);

    const recentSessions = await pool.query(`
      SELECT session_id, phone, text_input, response, created_at
      FROM ussd_sessions
      ORDER BY created_at DESC
      LIMIT 20
    `);

    res.json({
      success: true,
      stats: stats.rows[0],
      recent: recentSessions.rows
    });
  } catch (err) {
    console.error('[USSD] Stats error:', err);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

// --- OPERATORS (Role-based auth for dashboard) ---

// Register operator
app.post('/api/operators', async (req, res) => {
  try {
    const { name, company, phone, pin, role } = req.body;
    if (!name || !phone || !pin) {
      return res.status(400).json({ success: false, message: 'Name, phone, and PIN are required' });
    }
    if (pin.length < 4 || pin.length > 6) {
      return res.status(400).json({ success: false, message: 'PIN must be 4-6 digits' });
    }
    const validRoles = ['operator', 'admin', 'collector', 'aggregator', 'processor', 'converter'];
    const operatorRole = validRoles.includes(role) ? role : 'operator';
    const result = await pool.query(
      `INSERT INTO operators (name, company, phone, pin, role)
       VALUES ($1, $2, $3, $4, $5)
       RETURNING id, name, company, phone, role, created_at`,
      [name, company || null, phone, pin, operatorRole]
    );
    res.status(201).json({ success: true, operator: result.rows[0] });
  } catch (err) {
    if (err.code === '23505') {
      return res.status(409).json({ success: false, message: 'Phone number already registered' });
    }
    console.error('Error creating operator:', err);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

// Operator login
app.post('/api/operators/login', async (req, res) => {
  try {
    const { phone, pin } = req.body;
    if (!phone || !pin) {
      return res.status(400).json({ success: false, message: 'Phone and PIN required' });
    }
    const result = await pool.query(
      `SELECT id, name, company, phone, role, created_at FROM operators WHERE phone = $1 AND pin = $2 AND is_active = true`,
      [phone, pin]
    );
    if (result.rows.length === 0) {
      return res.status(401).json({ success: false, message: 'Invalid phone or PIN' });
    }
    res.json({ success: true, operator: result.rows[0] });
  } catch (err) {
    console.error('Error logging in operator:', err);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

// List operators (admin only)
app.get('/api/operators', async (req, res) => {
  try {
    const { role, phone } = req.query;
    const params = [];
    let where = 'WHERE is_active = true';
    if (role)  { params.push(role);  where += ` AND role = $${params.length}`; }
    if (phone) { params.push(phone); where += ` AND phone = $${params.length}`; }
    const result = await pool.query(
      `SELECT id, name, company, phone, role, city, region, created_at FROM operators ${where} ORDER BY name ASC`,
      params
    );
    res.json({ success: true, operators: result.rows });
  } catch (err) {
    console.error('Error listing operators:', err);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

// ============================================
// PENDING TRANSACTIONS
// ============================================

// POST /api/pending-transactions — collector logs a sale, creates pending record
app.post('/api/pending-transactions', async (req, res) => {
  try {
    const { transaction_type, collector_id, aggregator_operator_id, material_type, gross_weight_kg, price_per_kg } = req.body;

    // Required field check
    if (!transaction_type || !collector_id || !aggregator_operator_id || !material_type || !gross_weight_kg) {
      return res.status(400).json({ success: false, message: 'transaction_type, collector_id, aggregator_operator_id, material_type, and gross_weight_kg are required' });
    }
    const validMaterials = ['PET', 'HDPE', 'LDPE', 'PP'];
    if (!validMaterials.includes(material_type.toUpperCase())) {
      return res.status(400).json({ success: false, message: 'material_type must be one of PET, HDPE, LDPE, PP' });
    }
    const kg = parseFloat(gross_weight_kg);
    if (isNaN(kg) || kg <= 0 || kg > 100) {
      return res.status(400).json({ success: false, message: 'gross_weight_kg must be greater than 0 and at most 100 kg' });
    }

    // Verify collector exists
    const collectorCheck = await pool.query(`SELECT id FROM collectors WHERE id = $1 AND is_active = true`, [collector_id]);
    if (!collectorCheck.rows.length) {
      return res.status(400).json({ success: false, message: 'Collector not found' });
    }

    // Verify aggregator exists with role = 'aggregator'
    const aggCheck = await pool.query(`SELECT id FROM operators WHERE id = $1 AND role = 'aggregator' AND is_active = true`, [aggregator_operator_id]);
    if (!aggCheck.rows.length) {
      return res.status(400).json({ success: false, message: 'Aggregator not found' });
    }

    const totalPrice = price_per_kg ? parseFloat((kg * parseFloat(price_per_kg)).toFixed(2)) : null;

    const result = await pool.query(`
      INSERT INTO pending_transactions
        (transaction_type, collector_id, aggregator_operator_id, material_type,
         gross_weight_kg, price_per_kg, total_price, status)
      VALUES ($1, $2, $3, $4, $5, $6, $7, 'pending')
      RETURNING *
    `, [
      transaction_type,
      collector_id,
      aggregator_operator_id,
      material_type.toUpperCase(),
      kg,
      price_per_kg ? parseFloat(price_per_kg) : null,
      totalPrice
    ]);

    res.status(201).json({ success: true, pending_transaction: result.rows[0] });
  } catch (err) {
    console.error('Create pending transaction error:', err);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

// GET /api/pending-transactions — list pending transactions for a collector or aggregator
app.get('/api/pending-transactions', async (req, res) => {
  try {
    const { collector_id, aggregator_operator_id } = req.query;

    if (!collector_id && !aggregator_operator_id) {
      return res.status(400).json({ success: false, message: 'collector_id or aggregator_operator_id query param required' });
    }

    let query, params;
    if (collector_id) {
      query = `
        SELECT pt.*, o.name AS aggregator_name
        FROM pending_transactions pt
        LEFT JOIN operators o ON o.id = pt.aggregator_operator_id
        WHERE pt.collector_id = $1 AND pt.status = 'pending'
        ORDER BY pt.created_at DESC
      `;
      params = [collector_id];
    } else {
      query = `
        SELECT pt.*,
               c.first_name AS collector_first_name,
               c.last_name  AS collector_last_name
        FROM pending_transactions pt
        LEFT JOIN collectors c ON c.id = pt.collector_id
        WHERE pt.aggregator_operator_id = $1 AND pt.status = 'pending'
        ORDER BY pt.created_at DESC
      `;
      params = [aggregator_operator_id];
    }

    const result = await pool.query(query, params);
    res.json({ success: true, pending_transactions: result.rows });
  } catch (err) {
    console.error('Get pending transactions error:', err);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

// PATCH /api/pending-transactions/:id/review — aggregator accepts or rejects a collector_sale
app.patch('/api/pending-transactions/:id/review', async (req, res) => {
  try {
    const { id } = req.params;
    const { action, grade, grade_notes, rejection_reason, price_per_kg } = req.body;

    if (!action || !['accept', 'reject'].includes(action)) {
      return res.status(400).json({ success: false, message: 'action must be "accept" or "reject"' });
    }

    // Fetch pending transaction
    const ptResult = await pool.query(`SELECT * FROM pending_transactions WHERE id = $1`, [id]);
    if (!ptResult.rows.length) return res.status(404).json({ success: false, message: 'Pending transaction not found' });
    const pt = ptResult.rows[0];
    if (pt.status !== 'pending') return res.status(409).json({ success: false, message: 'Transaction is no longer pending' });
    if (pt.transaction_type !== 'collector_sale') return res.status(400).json({ success: false, message: 'Only collector_sale transactions can be reviewed this way' });

    // ── REJECT ──────────────────────────────────────────────────────────────
    if (action === 'reject') {
      if (!rejection_reason) return res.status(400).json({ success: false, message: 'rejection_reason is required when rejecting' });
      const updated = await pool.query(
        `UPDATE pending_transactions
         SET status = 'rejected', rejected_at = NOW(), rejection_reason = $1, updated_at = NOW()
         WHERE id = $2 RETURNING *`,
        [rejection_reason, id]
      );
      return res.json({ success: true, pending_transaction: updated.rows[0] });
    }

    // ── ACCEPT ───────────────────────────────────────────────────────────────
    if (!grade || !['A', 'B', 'C'].includes(grade)) {
      return res.status(400).json({ success: false, message: 'grade (A, B, or C) is required when accepting' });
    }

    // Determine base price: body override → posted price → pending_transaction.price_per_kg
    let basePricePerKg;
    if (price_per_kg !== undefined && price_per_kg !== null && !isNaN(parseFloat(price_per_kg))) {
      basePricePerKg = parseFloat(price_per_kg);
    } else {
      const postedResult = await pool.query(
        `SELECT price_per_kg_ghs FROM posted_prices
         WHERE operator_id = $1 AND material_type = $2 AND is_active = true AND expires_at > NOW()
         ORDER BY posted_at DESC LIMIT 1`,
        [pt.aggregator_operator_id, pt.material_type]
      );
      basePricePerKg = postedResult.rows.length
        ? parseFloat(postedResult.rows[0].price_per_kg_ghs)
        : parseFloat(pt.price_per_kg || 0);
    }

    // Grade multiplier
    const multiplier = grade === 'A' ? 1.10 : grade === 'C' ? 0.75 : 1.0;
    const adjustedPrice = parseFloat((basePricePerKg * multiplier).toFixed(2));
    const totalPrice    = parseFloat((adjustedPrice * parseFloat(pt.gross_weight_kg)).toFixed(2));

    // Try to find aggregator's collectors.id by matching phones
    const aggRow = await pool.query(`SELECT phone FROM operators WHERE id = $1`, [pt.aggregator_operator_id]);
    let buyerId = null;
    if (aggRow.rows.length && aggRow.rows[0].phone) {
      const collRow = await pool.query(`SELECT id FROM collectors WHERE phone = $1`, [aggRow.rows[0].phone]);
      if (collRow.rows.length) buyerId = collRow.rows[0].id;
    }

    // Insert finalised transaction
    const txnResult = await pool.query(`
      INSERT INTO transactions
        (collector_id, operator_id, buyer_id, material_type,
         gross_weight_kg, net_weight_kg, contamination_deduction_percent,
         price_per_kg, total_price, payment_status, notes)
      VALUES ($1, $2, $3, $4, $5, $5, 0, $6, $7, 'unpaid', $8)
      RETURNING *
    `, [pt.collector_id, pt.aggregator_operator_id, buyerId,
        pt.material_type, pt.gross_weight_kg, adjustedPrice, totalPrice,
        'grade:' + grade]);
    const newTxn = txnResult.rows[0];

    // Update pending_transaction to confirmed
    const updatedPt = await pool.query(`
      UPDATE pending_transactions
      SET status = 'confirmed', grade = $1, grade_notes = $2, transaction_id = $3, updated_at = NOW()
      WHERE id = $4
      RETURNING *
    `, [grade, grade_notes || null, newTxn.id, id]);

    return res.json({
      success: true,
      pending_transaction: updatedPt.rows[0],
      transaction: newTxn,
      final_price_per_kg: adjustedPrice
    });
  } catch (err) {
    console.error('Review pending transaction error:', err);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

// POST /api/pending-transactions/aggregator-purchase — aggregator initiates a purchase from a collector
app.post('/api/pending-transactions/aggregator-purchase', async (req, res) => {
  try {
    const { aggregator_operator_id, collector_id, material_type, gross_weight_kg, price_per_kg } = req.body;

    if (!aggregator_operator_id || !collector_id || !material_type || !gross_weight_kg) {
      return res.status(400).json({ success: false, message: 'aggregator_operator_id, collector_id, material_type, and gross_weight_kg are required' });
    }
    const validMaterials = ['PET', 'HDPE', 'LDPE', 'PP'];
    if (!validMaterials.includes(material_type.toUpperCase())) {
      return res.status(400).json({ success: false, message: 'material_type must be one of PET, HDPE, LDPE, PP' });
    }
    const kg = parseFloat(gross_weight_kg);
    if (isNaN(kg) || kg <= 0 || kg > 100) {
      return res.status(400).json({ success: false, message: 'gross_weight_kg must be greater than 0 and at most 100 kg' });
    }

    const aggCheck = await pool.query(`SELECT id FROM operators WHERE id = $1 AND role = 'aggregator' AND is_active = true`, [aggregator_operator_id]);
    if (!aggCheck.rows.length) return res.status(400).json({ success: false, message: 'Aggregator not found' });

    const collCheck = await pool.query(`SELECT id FROM collectors WHERE id = $1 AND is_active = true`, [collector_id]);
    if (!collCheck.rows.length) return res.status(400).json({ success: false, message: 'Collector not found' });

    const totalPrice = price_per_kg ? parseFloat((kg * parseFloat(price_per_kg)).toFixed(2)) : null;

    const result = await pool.query(`
      INSERT INTO pending_transactions
        (transaction_type, collector_id, aggregator_operator_id, material_type,
         gross_weight_kg, price_per_kg, total_price, status)
      VALUES ('aggregator_purchase', $1, $2, $3, $4, $5, $6, 'pending')
      RETURNING *
    `, [collector_id, aggregator_operator_id, material_type.toUpperCase(), kg,
        price_per_kg ? parseFloat(price_per_kg) : null, totalPrice]);

    res.status(201).json({ success: true, pending_transaction: result.rows[0] });
  } catch (err) {
    console.error('Aggregator purchase error:', err);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

// ============================================
// ADMIN AUTH + BUYERS + PRICES
// ============================================

const crypto = require('crypto');
const ADMIN_SECRET = process.env.ADMIN_SECRET || 'circul-admin-secret-2026';
const BUYER_SECRET = process.env.BUYER_SECRET || 'circul-buyer-secret-2026';

function generateToken(payload, secret) {
  const data = JSON.stringify(payload);
  const b64 = Buffer.from(data).toString('base64url');
  const sig = crypto.createHmac('sha256', secret).update(b64).digest('base64url');
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

// Middleware: require admin token
function requireAdmin(req, res, next) {
  const auth = req.headers.authorization || '';
  const token = auth.replace('Bearer ', '').trim() || req.query.token;
  if (!token) return res.status(401).json({ success: false, message: 'Admin auth required' });
  const payload = verifyToken(token, ADMIN_SECRET);
  if (!payload || payload.type !== 'admin') return res.status(401).json({ success: false, message: 'Invalid admin token' });
  req.admin = payload;
  next();
}

// Middleware: require buyer token
function requireBuyer(req, res, next) {
  const auth = req.headers.authorization || '';
  const token = auth.replace('Bearer ', '').trim() || req.query.token;
  if (!token) return res.status(401).json({ success: false, message: 'Buyer auth required' });
  const payload = verifyToken(token, BUYER_SECRET);
  if (!payload || payload.type !== 'buyer') return res.status(401).json({ success: false, message: 'Invalid buyer token' });
  req.buyer = payload;
  next();
}

// --- ADMIN LOGIN ---
app.post('/api/admin/login', async (req, res) => {
  try {
    const { email, password } = req.body;
    if (!email || !password) return res.status(400).json({ success: false, message: 'Email and password required' });
    const result = await pool.query(
      `SELECT id, email, name, password_hash FROM admin_users WHERE email = $1 AND is_active = true`,
      [email.toLowerCase().trim()]
    );
    if (result.rows.length === 0) return res.status(401).json({ success: false, message: 'Invalid credentials' });
    const admin = result.rows[0];
    const valid = await verifyPassword(password, admin.password_hash);
    if (!valid) return res.status(401).json({ success: false, message: 'Invalid credentials' });
    const token = generateToken({ type: 'admin', id: admin.id, email: admin.email }, ADMIN_SECRET);
    res.json({ success: true, token, admin: { id: admin.id, email: admin.email, name: admin.name } });
  } catch (err) {
    console.error('Admin login error:', err);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

// --- UNIFIED AUTH LOGIN (used by login.html smart form) ---
// Phone + PIN  → checks operators table (all field roles)
// Email + pass → checks admin_users table
app.post('/api/auth/login', async (req, res) => {
  try {
    const { type, phone, pin, email, password } = req.body;

    if (email || type === 'email') {
      // Email path — admin_users first, then buyers (processor / converter)
      if (!email || !password) return res.status(400).json({ success: false, message: 'Email and password required' });

      // 1. Look up in admin_users
      let userResult = await pool.query(
        `SELECT id, email, name, NULL::text as company, password_hash, 'admin' as role FROM admin_users WHERE email = $1 AND is_active = true`,
        [email.toLowerCase().trim()]
      );

      // 2. Fall back to buyers table (processor / converter)
      if (userResult.rows.length === 0) {
        userResult = await pool.query(
          `SELECT id, email, name, company, password_hash, role FROM buyers WHERE email = $1 AND is_active = true`,
          [email.toLowerCase().trim()]
        );
      }

      if (userResult.rows.length === 0) return res.status(401).json({ success: false, message: 'Invalid credentials' });
      const user = userResult.rows[0];

      // Guard: only fires after DB lookup confirms role — collector/aggregator must use phone + PIN
      if (user.role === 'collector' || user.role === 'aggregator') {
        return res.status(403).json({
          error: 'collector_email_login',
          message: 'This account uses phone + PIN login. Please enter your phone number instead.'
        });
      }

      // Verify password — only reached for admin, processor, converter
      const valid = await verifyPassword(password, user.password_hash);
      if (!valid) return res.status(401).json({ success: false, message: 'Invalid credentials' });

      // Issue token scoped to role
      if (user.role === 'admin') {
        const token = generateToken({ type: 'admin', id: user.id, email: user.email }, ADMIN_SECRET);
        return res.json({ success: true, role: 'admin', token, operator: { id: user.id, email: user.email, name: user.name, role: 'admin' } });
      }
      // processor / converter from buyers table
      const token = generateToken({ type: 'buyer', id: user.id, email: user.email, role: user.role }, BUYER_SECRET);
      return res.json({ success: true, role: user.role, token, operator: { id: user.id, email: user.email, name: user.name, company: user.company || null, role: user.role } });
    } else {
      // Phone + PIN path — operators table covers all field roles
      if (!phone || !pin) return res.status(400).json({ success: false, message: 'Phone and PIN required' });
      const result = await pool.query(
        `SELECT id, name, company, phone, role FROM operators WHERE phone = $1 AND pin = $2 AND is_active = true`,
        [phone.trim(), pin.trim()]
      );
      if (result.rows.length === 0) return res.status(401).json({ success: false, message: 'Invalid phone number or PIN' });
      const op = result.rows[0];
      return res.json({ success: true, role: op.role, operator: op });
    }
  } catch (err) {
    console.error('Unified auth login error:', err);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

// --- ADMIN: PLATFORM STATS ---
app.get('/api/admin/stats', requireAdmin, async (req, res) => {
  try {
    const [collectors, operators, buyers, transactions, volume] = await Promise.all([
      pool.query(`SELECT COUNT(*) as count FROM collectors WHERE is_active = true`),
      pool.query(`SELECT COUNT(*) as count FROM operators WHERE is_active = true`),
      pool.query(`SELECT COUNT(*) as count FROM buyers WHERE is_active = true`),
      pool.query(`SELECT COUNT(*) as count FROM transactions`),
      pool.query(`
        SELECT material_type, COALESCE(SUM(net_weight_kg),0) as total_kg, COUNT(*) as count
        FROM transactions GROUP BY material_type ORDER BY total_kg DESC
      `)
    ]);
    const totalVol = await pool.query(`SELECT COALESCE(SUM(net_weight_kg),0) as total FROM transactions`);
    res.json({
      success: true,
      stats: {
        collectors: parseInt(collectors.rows[0].count),
        operators: parseInt(operators.rows[0].count),
        buyers: parseInt(buyers.rows[0].count),
        transactions: parseInt(transactions.rows[0].count),
        total_volume_kg: parseFloat(totalVol.rows[0].total),
        by_material: volume.rows
      }
    });
  } catch (err) {
    console.error('Admin stats error:', err);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

// --- ADMIN: ALL TRANSACTIONS ---
app.get('/api/admin/transactions', requireAdmin, async (req, res) => {
  try {
    const { limit = 100, offset = 0, material_type } = req.query;
    let where = 'WHERE 1=1';
    const params = [];
    if (material_type) { params.push(material_type.toUpperCase()); where += ` AND t.material_type = $${params.length}`; }
    params.push(parseInt(limit)); params.push(parseInt(offset));
    const result = await pool.query(`
      SELECT t.id, t.material_type, t.gross_weight_kg, t.net_weight_kg, t.contamination_deduction_percent,
             t.price_per_kg, t.total_price, t.payment_status, t.transaction_date,
             c.first_name || ' ' || c.last_name as collector_name, c.phone as collector_phone,
             o.name as operator_name
      FROM transactions t
      JOIN collectors c ON c.id = t.collector_id
      LEFT JOIN operators o ON o.id = t.operator_id
      ${where}
      ORDER BY t.transaction_date DESC
      LIMIT $${params.length - 1} OFFSET $${params.length}
    `, params);
    res.json({ success: true, transactions: result.rows });
  } catch (err) {
    console.error('Admin transactions error:', err);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

// --- ADMIN: ALL COLLECTORS ---
app.get('/api/admin/collectors', requireAdmin, async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT c.id, c.first_name, c.last_name, c.phone, c.region, c.average_rating, c.created_at,
             COALESCE(SUM(t.net_weight_kg), 0) as total_weight_kg,
             COUNT(t.id) as transaction_count
      FROM collectors c
      LEFT JOIN transactions t ON t.collector_id = c.id
      WHERE c.is_active = true
      GROUP BY c.id ORDER BY c.created_at DESC
    `);
    res.json({ success: true, collectors: result.rows });
  } catch (err) {
    console.error('Admin collectors error:', err);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

// --- ADMIN: MANAGE OPERATORS ---
app.get('/api/admin/operators', requireAdmin, async (req, res) => {
  try {
    const result = await pool.query(`SELECT id, name, company, phone, role, is_active, created_at FROM operators ORDER BY created_at DESC`);
    res.json({ success: true, operators: result.rows });
  } catch (err) {
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

app.post('/api/admin/operators', requireAdmin, async (req, res) => {
  try {
    const { name, company, phone, pin, role } = req.body;
    if (!name || !phone || !pin) return res.status(400).json({ success: false, message: 'name, phone, pin required' });
    const validRole = ['operator', 'admin', 'collector', 'aggregator', 'processor', 'converter'].includes(role) ? role : 'operator';
    const result = await pool.query(
      `INSERT INTO operators (name, company, phone, pin, role) VALUES ($1,$2,$3,$4,$5) RETURNING id, name, company, phone, role, created_at`,
      [name, company || null, phone, pin, validRole]
    );
    res.status(201).json({ success: true, operator: result.rows[0] });
  } catch (err) {
    if (err.code === '23505') return res.status(409).json({ success: false, message: 'Phone already registered' });
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

app.put('/api/admin/operators/:id', requireAdmin, async (req, res) => {
  try {
    const { name, company, phone, pin, role, is_active, is_flagged, city, region, country } = req.body;
    const fields = [], params = [];
    if (name !== undefined) { params.push(name); fields.push(`name=$${params.length}`); }
    if (company !== undefined) { params.push(company); fields.push(`company=$${params.length}`); }
    if (phone !== undefined) { params.push(phone); fields.push(`phone=$${params.length}`); }
    if (pin !== undefined) { params.push(pin); fields.push(`pin=$${params.length}`); }
    if (role !== undefined) { params.push(role); fields.push(`role=$${params.length}`); }
    if (is_active !== undefined) { params.push(is_active); fields.push(`is_active=$${params.length}`); }
    if (is_flagged !== undefined) { params.push(is_flagged); fields.push(`is_flagged=$${params.length}`); }
    if (city !== undefined) { params.push(city); fields.push(`city=$${params.length}`); }
    if (region !== undefined) { params.push(region); fields.push(`region=$${params.length}`); }
    if (country !== undefined) { params.push(country); fields.push(`country=$${params.length}`); }
    if (!fields.length) return res.status(400).json({ success: false, message: 'Nothing to update' });
    params.push(req.params.id);
    const result = await pool.query(`UPDATE operators SET ${fields.join(',')} WHERE id=$${params.length} RETURNING id, name, company, phone, role, is_active, is_flagged, city`, params);
    if (!result.rows.length) return res.status(404).json({ success: false, message: 'Not found' });
    res.json({ success: true, operator: result.rows[0] });
  } catch (err) {
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

// --- ADMIN: MANAGE BUYERS (aggregators + processors) ---
app.get('/api/admin/buyers', requireAdmin, async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT b.id, b.name, b.company, b.email, b.role, b.is_active, b.created_at,
             json_agg(json_build_object('material_type', bp.material_type, 'price_per_kg', bp.price_per_kg, 'updated_at', bp.updated_at) ORDER BY bp.material_type) FILTER (WHERE bp.id IS NOT NULL) as prices
      FROM buyers b
      LEFT JOIN buyer_prices bp ON bp.buyer_id = b.id
      GROUP BY b.id ORDER BY b.created_at DESC
    `);
    res.json({ success: true, buyers: result.rows });
  } catch (err) {
    console.error('Admin buyers error:', err);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

app.post('/api/admin/buyers', requireAdmin, async (req, res) => {
  try {
    const { name, company, email, password, role } = req.body;
    if (!name || !email || !password) return res.status(400).json({ success: false, message: 'name, email, password required' });
    const validRole = ['aggregator', 'processor'].includes(role) ? role : 'aggregator';
    const pwHash = await hashPassword(password);
    const result = await pool.query(
      `INSERT INTO buyers (name, company, email, password_hash, role) VALUES ($1,$2,$3,$4,$5) RETURNING id, name, company, email, role, created_at`,
      [name, company || null, email.toLowerCase().trim(), pwHash, validRole]
    );
    res.status(201).json({ success: true, buyer: result.rows[0] });
  } catch (err) {
    if (err.code === '23505') return res.status(409).json({ success: false, message: 'Email already registered' });
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

app.put('/api/admin/buyers/:id', requireAdmin, async (req, res) => {
  try {
    const { name, company, email, password, role, is_active } = req.body;
    const fields = [], params = [];
    if (name !== undefined) { params.push(name); fields.push(`name=$${params.length}`); }
    if (company !== undefined) { params.push(company); fields.push(`company=$${params.length}`); }
    if (email !== undefined) { params.push(email.toLowerCase()); fields.push(`email=$${params.length}`); }
    if (role !== undefined) { params.push(role); fields.push(`role=$${params.length}`); }
    if (is_active !== undefined) { params.push(is_active); fields.push(`is_active=$${params.length}`); }
    if (password) { const h = await hashPassword(password); params.push(h); fields.push(`password_hash=$${params.length}`); }
    if (!fields.length) return res.status(400).json({ success: false, message: 'Nothing to update' });
    params.push(req.params.id);
    const result = await pool.query(`UPDATE buyers SET ${fields.join(',')} WHERE id=$${params.length} RETURNING id, name, company, email, role, is_active`, params);
    if (!result.rows.length) return res.status(404).json({ success: false, message: 'Not found' });
    res.json({ success: true, buyer: result.rows[0] });
  } catch (err) {
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

// --- BUYER LOGIN ---
app.post('/api/buyers/login', async (req, res) => {
  try {
    const { email, password } = req.body;
    if (!email || !password) return res.status(400).json({ success: false, message: 'Email and password required' });
    const result = await pool.query(
      `SELECT id, name, company, email, role, password_hash FROM buyers WHERE email = $1 AND is_active = true`,
      [email.toLowerCase().trim()]
    );
    if (!result.rows.length) return res.status(401).json({ success: false, message: 'Invalid credentials' });
    const buyer = result.rows[0];
    const valid = await verifyPassword(password, buyer.password_hash);
    if (!valid) return res.status(401).json({ success: false, message: 'Invalid credentials' });
    const token = generateToken({ type: 'buyer', id: buyer.id, email: buyer.email, role: buyer.role }, BUYER_SECRET);
    res.json({ success: true, token, buyer: { id: buyer.id, name: buyer.name, company: buyer.company, email: buyer.email, role: buyer.role } });
  } catch (err) {
    console.error('Buyer login error:', err);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

// Buyer: get their own prices
app.get('/api/buyers/me/prices', requireBuyer, async (req, res) => {
  try {
    const result = await pool.query(
      `SELECT material_type, price_per_kg, updated_at FROM buyer_prices WHERE buyer_id = $1 ORDER BY material_type`,
      [req.buyer.id]
    );
    const buyerInfo = await pool.query(`SELECT id, name, company, email, role FROM buyers WHERE id = $1`, [req.buyer.id]);
    res.json({ success: true, buyer: buyerInfo.rows[0], prices: result.rows });
  } catch (err) {
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

// Buyer: set/update price for a material
app.put('/api/buyers/me/prices/:material_type', requireBuyer, async (req, res) => {
  try {
    const material = req.params.material_type.toUpperCase();
    const validMaterials = ['PET', 'HDPE', 'LDPE', 'PP'];
    if (!validMaterials.includes(material)) return res.status(400).json({ success: false, message: 'Invalid material type' });
    const { price_per_kg } = req.body;
    if (price_per_kg === undefined || isNaN(parseFloat(price_per_kg)) || parseFloat(price_per_kg) < 0) {
      return res.status(400).json({ success: false, message: 'Valid price_per_kg required' });
    }
    const result = await pool.query(`
      INSERT INTO buyer_prices (buyer_id, material_type, price_per_kg, updated_at)
      VALUES ($1, $2, $3, NOW())
      ON CONFLICT (buyer_id, material_type) DO UPDATE SET price_per_kg = $3, updated_at = NOW()
      RETURNING material_type, price_per_kg, updated_at
    `, [req.buyer.id, material, parseFloat(price_per_kg)]);
    res.json({ success: true, price: result.rows[0] });
  } catch (err) {
    console.error('Set price error:', err);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

// Public: get aggregator prices (for collector app — Phase 1 shows only aggregator prices)
app.get('/api/market-prices', async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT bp.material_type, bp.price_per_kg, bp.updated_at,
             b.name as buyer_name, b.company as buyer_company, b.role
      FROM buyer_prices bp
      JOIN buyers b ON b.id = bp.buyer_id
      WHERE b.is_active = true AND b.role = 'aggregator'
      ORDER BY bp.material_type, bp.price_per_kg DESC
    `);
    // Best (highest) price per material across all aggregators
    const best = {};
    for (const row of result.rows) {
      if (!best[row.material_type] || parseFloat(row.price_per_kg) > parseFloat(best[row.material_type].price_per_kg)) {
        best[row.material_type] = row;
      }
    }
    res.json({ success: true, prices: Object.values(best), all_aggregator_prices: result.rows });
  } catch (err) {
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

// ============================================
// ROLE-BASED DASHBOARD STATS
// ============================================

// GET /api/operators/:id/dashboard-stats — returns role-appropriate stats
app.get('/api/operators/:id/dashboard-stats', async (req, res) => {
  try {
    const { id } = req.params;
    const op = await pool.query(`SELECT * FROM operators WHERE id = $1 AND is_active = true`, [id]);
    if (!op.rows.length) return res.status(404).json({ success: false, message: 'Operator not found' });
    const operator = op.rows[0];

    const thisMonth = new Date(); thisMonth.setDate(1); thisMonth.setHours(0,0,0,0);

    const [totals, monthlyTotals, pending, paymentHistory, activeCollectors] = await Promise.all([
      pool.query(`SELECT COALESCE(SUM(net_weight_kg),0) as total_kg, COALESCE(SUM(total_price),0) as total_value, COUNT(*) as total_txns FROM transactions WHERE operator_id=$1`, [id]),
      pool.query(`SELECT COALESCE(SUM(net_weight_kg),0) as month_kg, COALESCE(SUM(total_price),0) as month_value, COUNT(*) as month_txns FROM transactions WHERE operator_id=$1 AND transaction_date >= $2`, [id, thisMonth.toISOString()]),
      pool.query(`SELECT COUNT(*) as count, COALESCE(SUM(total_price),0) as value FROM transactions WHERE operator_id=$1 AND payment_status='unpaid' AND total_price > 0`, [id]),
      pool.query(`SELECT p.*, c.first_name, c.last_name FROM payments p JOIN collectors c ON c.id=p.collector_id WHERE p.operator_id=$1 ORDER BY p.created_at DESC LIMIT 20`, [id]),
      pool.query(`SELECT COUNT(DISTINCT collector_id) as count FROM transactions WHERE operator_id=$1`, [id])
    ]);

    const byMaterial = await pool.query(`SELECT material_type, SUM(net_weight_kg) as kg, COUNT(*) as txns FROM transactions WHERE operator_id=$1 GROUP BY material_type ORDER BY kg DESC`, [id]);

    const topCollectors = await pool.query(`
      SELECT c.id, c.first_name, c.last_name, c.phone, c.average_rating, c.city,
             SUM(t.net_weight_kg) as total_kg, COUNT(t.id) as txns
      FROM collectors c JOIN transactions t ON t.collector_id=c.id
      WHERE t.operator_id=$1
      GROUP BY c.id ORDER BY total_kg DESC LIMIT 20`, [id]);

    const postedPrices = await pool.query(`SELECT * FROM posted_prices WHERE operator_id=$1 AND is_active=true AND expires_at > NOW() ORDER BY material_type`, [id]).catch(() => ({ rows: [] }));

    const ratings = await pool.query(`SELECT AVG(rating)::NUMERIC(3,2) as avg_rating, COUNT(*) as count FROM ratings WHERE rated_operator_id=$1`, [id]).catch(() => ({ rows: [{ avg_rating: null, count: 0 }] }));

    res.json({
      success: true,
      operator,
      stats: {
        totals: totals.rows[0],
        this_month: monthlyTotals.rows[0],
        pending_payments: pending.rows[0],
        payment_history: paymentHistory.rows,
        active_collectors: activeCollectors.rows[0].count,
        by_material: byMaterial.rows,
        top_collectors: topCollectors.rows,
        posted_prices: postedPrices.rows,
        ratings: ratings.rows[0]
      }
    });
  } catch (err) {
    console.error('Dashboard stats error:', err);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

// GET /api/buyers/:id/stats — returns dashboard stats for a buyer (processor or converter)
app.get('/api/buyers/:id/stats', async (req, res) => {
  try {
    const { id } = req.params;
    const buyerResult = await pool.query(
      `SELECT id, name, company, email, role FROM buyers WHERE id = $1 AND is_active = true`,
      [id]
    );
    if (!buyerResult.rows.length) return res.status(404).json({ success: false, message: 'Buyer not found' });
    const buyer = buyerResult.rows[0];

    // Processors appear in transactions via processor_id; converters via converter_id
    const idCol = buyer.role === 'converter' ? 'converter_id' : 'processor_id';

    const thisMonth = new Date(); thisMonth.setDate(1); thisMonth.setHours(0, 0, 0, 0);

    const [totals, monthlyTotals, byMaterial, recentTxns, buyerPrices] = await Promise.all([
      pool.query(
        `SELECT COALESCE(SUM(net_weight_kg),0) as total_kg, COALESCE(SUM(total_price),0) as total_value, COUNT(*) as total_txns FROM transactions WHERE ${idCol} = $1`,
        [id]
      ),
      pool.query(
        `SELECT COALESCE(SUM(net_weight_kg),0) as month_kg, COALESCE(SUM(total_price),0) as month_value, COUNT(*) as month_txns FROM transactions WHERE ${idCol} = $1 AND transaction_date >= $2`,
        [id, thisMonth.toISOString()]
      ),
      pool.query(
        `SELECT material_type, SUM(net_weight_kg) as kg, COUNT(*) as txns FROM transactions WHERE ${idCol} = $1 GROUP BY material_type ORDER BY kg DESC`,
        [id]
      ),
      pool.query(
        `SELECT * FROM transactions WHERE ${idCol} = $1 ORDER BY transaction_date DESC LIMIT 20`,
        [id]
      ),
      pool.query(
        `SELECT material_type, price_per_kg AS price_per_kg_ghs, updated_at FROM buyer_prices WHERE buyer_id = $1 ORDER BY material_type`,
        [id]
      )
    ]);

    res.json({
      success: true,
      buyer,
      stats: {
        totals: totals.rows[0],
        this_month: monthlyTotals.rows[0],
        pending_payments: { count: 0, value: 0 },
        by_material: byMaterial.rows,
        posted_prices: buyerPrices.rows,
        recent_transactions: recentTxns.rows
      }
    });
  } catch (err) {
    console.error('Buyer stats error:', err);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

// ============================================
// POSTED PRICES API
// ============================================

// POST /api/prices — operator posts their buy price
app.post('/api/prices', async (req, res) => {
  try {
    const { operator_id, material_type, price_per_kg_usd, price_per_kg_ghs, usd_to_ghs_rate } = req.body;
    if (!operator_id || !material_type || !price_per_kg_usd) {
      return res.status(400).json({ success: false, message: 'operator_id, material_type, price_per_kg_usd required' });
    }
    const validMaterials = ['PET','HDPE','LDPE','PP'];
    if (!validMaterials.includes(material_type.toUpperCase())) {
      return res.status(400).json({ success: false, message: 'Invalid material type' });
    }
    const now = new Date();
    const expiresAt = new Date(now.getFullYear(), now.getMonth() + 1, 0, 23, 59, 59);

    const op = await pool.query(`SELECT city, region, country FROM operators WHERE id=$1`, [operator_id]);
    if (!op.rows.length) return res.status(404).json({ success: false, message: 'Operator not found' });
    const { city, region, country } = op.rows[0];

    const result = await pool.query(`
      INSERT INTO posted_prices (operator_id, material_type, price_per_kg_usd, price_per_kg_ghs, usd_to_ghs_rate, city, region, country, expires_at)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
      ON CONFLICT (operator_id, material_type, expires_at)
      DO UPDATE SET price_per_kg_usd=$3, price_per_kg_ghs=$4, usd_to_ghs_rate=$5, posted_at=NOW(), is_active=true
      RETURNING *
    `, [operator_id, material_type.toUpperCase(), parseFloat(price_per_kg_usd), price_per_kg_ghs ? parseFloat(price_per_kg_ghs) : null, usd_to_ghs_rate ? parseFloat(usd_to_ghs_rate) : null, city, region, country || 'Ghana', expiresAt.toISOString()]);

    res.json({ success: true, price: result.rows[0] });
  } catch (err) {
    console.error('Post price error:', err);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

// GET /api/prices — get prices relevant to a role/city
app.get('/api/prices', async (req, res) => {
  try {
    const { role, material, city, operator_id } = req.query;

    let buyerRoles = [];
    if (role === 'collector') buyerRoles = ['aggregator'];
    else if (role === 'aggregator') buyerRoles = ['processor', 'aggregator'];
    else if (role === 'processor') buyerRoles = ['processor', 'converter'];
    else if (role === 'converter') buyerRoles = ['converter'];
    else buyerRoles = ['aggregator', 'processor', 'converter'];

    let params = [buyerRoles, new Date().toISOString()];
    let whereExtra = '';
    if (material) { params.push(material.toUpperCase()); whereExtra += ` AND pp.material_type = $${params.length}`; }
    // Mirror of whereExtra for the buyer_prices side of UNIONs (uses bp. prefix instead of pp.)
    const bpWhereExtra = whereExtra.replace('pp.material_type', 'bp.material_type');

    let nearPrices = { rows: [] };
    if (city) {
      const nearParams = [...params, city];
      nearPrices = await pool.query(`
        SELECT pp.material_type, pp.price_per_kg_ghs, pp.updated_at,
               o.name as operator_name, o.role as operator_role, pp.city
        FROM posted_prices pp JOIN operators o ON o.id=pp.operator_id
        WHERE o.role = ANY($1) AND pp.expires_at > $2 AND pp.is_active=true AND pp.city = $${nearParams.length}${whereExtra}
        UNION ALL
        SELECT bp.material_type, bp.price_per_kg as price_per_kg_ghs, bp.updated_at,
               b.name as operator_name, b.role as operator_role, NULL as city
        FROM buyer_prices bp JOIN buyers b ON b.id=bp.buyer_id
        WHERE b.role IN ('aggregator', 'processor', 'converter') AND b.is_active=true${bpWhereExtra}
        ORDER BY material_type, price_per_kg_ghs DESC
      `, nearParams);
    }

    const nationalAvg = await pool.query(`
      SELECT sub.material_type,
             AVG(sub.price_ghs) as avg_usd,
             COUNT(DISTINCT sub.buyer_id) as buyer_count
      FROM (
        SELECT pp.material_type, pp.price_per_kg_ghs as price_ghs, pp.operator_id as buyer_id
        FROM posted_prices pp JOIN operators o ON o.id=pp.operator_id
        WHERE o.role = ANY($1) AND pp.expires_at > $2 AND pp.is_active=true${material ? ` AND pp.material_type = $${params.length}` : ''}
        UNION ALL
        SELECT bp.material_type, bp.price_per_kg as price_ghs, b.id as buyer_id
        FROM buyer_prices bp JOIN buyers b ON b.id=bp.buyer_id
        WHERE b.role IN ('aggregator', 'processor', 'converter') AND b.is_active=true${material ? ` AND bp.material_type = $${params.length}` : ''}
      ) sub
      GROUP BY sub.material_type ORDER BY sub.material_type
    `, params);

    const allPrices = await pool.query(`
      SELECT pp.material_type, pp.price_per_kg_ghs, pp.updated_at,
             o.name as operator_name, o.role as operator_role, pp.city
      FROM posted_prices pp JOIN operators o ON o.id=pp.operator_id
      WHERE o.role = ANY($1) AND pp.expires_at > $2 AND pp.is_active=true${whereExtra}
      UNION ALL
      SELECT bp.material_type, bp.price_per_kg as price_per_kg_ghs, bp.updated_at,
             b.name as operator_name, b.role as operator_role, NULL as city
      FROM buyer_prices bp JOIN buyers b ON b.id=bp.buyer_id
      WHERE b.role IN ('aggregator', 'processor', 'converter') AND b.is_active=true${bpWhereExtra}
      ORDER BY material_type, price_per_kg_ghs DESC
    `, params);

    res.json({
      success: true,
      near_prices: nearPrices.rows,
      national_averages: nationalAvg.rows,
      all_prices: allPrices.rows
    });
  } catch (err) {
    console.error('Get prices error:', err);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

// ============================================
// RATINGS API
// ============================================

// POST /api/ratings/operator — rate another operator
app.post('/api/ratings/operator', async (req, res) => {
  try {
    const { transaction_id, rater_operator_id, rated_operator_id, rater_collector_id, rated_collector_id, rating, tags, notes, rating_direction } = req.body;
    if ((!rater_operator_id && !rater_collector_id) || (!rated_operator_id && !rated_collector_id) || !rating) {
      return res.status(400).json({ success: false, message: 'rater, rated, and rating are required' });
    }
    if (rating < 1 || rating > 5) return res.status(400).json({ success: false, message: 'Rating must be 1-5' });

    const windowExpires = new Date(); windowExpires.setDate(windowExpires.getDate() + 30);

    const result = await pool.query(`
      INSERT INTO ratings (transaction_id, rater_operator_id, rated_operator_id, rater_collector_id, rated_collector_id, rating, tags, notes, rating_direction, window_expires_at)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
      ON CONFLICT (transaction_id, rater_operator_id, rated_operator_id) DO UPDATE SET rating=$6, tags=$7, notes=$8
      RETURNING *
    `, [transaction_id || null, rater_operator_id || null, rated_operator_id || null, rater_collector_id || null, rated_collector_id || null, rating, JSON.stringify(tags || []), notes || null, rating_direction || null, windowExpires.toISOString()]);

    res.status(201).json({ success: true, rating: result.rows[0] });
  } catch (err) {
    console.error('Rating error:', err);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

// GET /api/ratings/operator/:id — get ratings for an operator
app.get('/api/ratings/operator/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const ratings = await pool.query(`
      SELECT r.*, o.name as rater_name, o.role as rater_role
      FROM ratings r
      LEFT JOIN operators o ON o.id = r.rater_operator_id
      WHERE r.rated_operator_id = $1
      ORDER BY r.created_at DESC LIMIT 50
    `, [id]);
    const avg = await pool.query(`SELECT AVG(rating)::NUMERIC(3,2) as avg_rating, COUNT(*) as count FROM ratings WHERE rated_operator_id=$1`, [id]);
    res.json({ success: true, ratings: ratings.rows, summary: avg.rows[0] });
  } catch (err) {
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

// ============================================
// COLLECTOR PASSPORT API
// ============================================

// GET /api/collectors/:id/passport — collector's full passport data
app.get('/api/collectors/:id/passport', async (req, res) => {
  try {
    const { id } = req.params;
    const collector = await pool.query(`SELECT * FROM collectors WHERE id=$1`, [id]);
    if (!collector.rows.length) return res.status(404).json({ success: false, message: 'Collector not found' });
    const c = collector.rows[0];

    const twelveMonthsAgo = new Date(); twelveMonthsAgo.setFullYear(twelveMonthsAgo.getFullYear() - 1);

    const [totals, last12m, byMaterial, aggregators, recent, ratingsReceived] = await Promise.all([
      pool.query(`SELECT COALESCE(SUM(net_weight_kg),0) as total_kg, COALESCE(SUM(total_price),0) as total_earned_ghs, COUNT(*) as txn_count, MIN(transaction_date) as active_since FROM transactions WHERE collector_id=$1`, [id]),
      pool.query(`SELECT COALESCE(SUM(net_weight_kg),0) as kg_12m, COALESCE(SUM(total_price),0) as earned_12m, COUNT(*) as txns_12m FROM transactions WHERE collector_id=$1 AND transaction_date >= $2`, [id, twelveMonthsAgo.toISOString()]),
      pool.query(`SELECT material_type, SUM(net_weight_kg) as kg, SUM(total_price) as earned, COUNT(*) as txns FROM transactions WHERE collector_id=$1 GROUP BY material_type ORDER BY kg DESC`, [id]),
      pool.query(`SELECT DISTINCT o.id, o.name, o.company, o.city FROM operators o JOIN transactions t ON t.operator_id=o.id WHERE t.collector_id=$1 AND o.role='aggregator'`, [id]),
      pool.query(`SELECT t.*, o.name as operator_name FROM transactions t LEFT JOIN operators o ON o.id=t.operator_id WHERE t.collector_id=$1 ORDER BY t.transaction_date DESC LIMIT 20`, [id]),
      pool.query(`SELECT AVG(rating)::NUMERIC(3,2) as avg, COUNT(*) as count, json_agg(json_build_object('rating',rating,'tags',tags,'direction',rating_direction,'created_at',created_at)) as ratings FROM ratings WHERE rated_collector_id=$1`, [id]).catch(() => ({ rows: [{ avg: null, count: 0, ratings: [] }] }))
    ]);

    const passport = {
      collector: c,
      total_kg_lifetime: parseFloat(totals.rows[0].total_kg),
      total_kg_last_12m: parseFloat(last12m.rows[0].kg_12m),
      total_earned_ghs: parseFloat(totals.rows[0].total_earned_ghs),
      transaction_count: parseInt(totals.rows[0].txn_count),
      active_since: totals.rows[0].active_since,
      material_breakdown: byMaterial.rows,
      aggregators_transacted_with: aggregators.rows,
      unique_aggregator_count: aggregators.rows.length,
      avg_rating_from_aggregators: ratingsReceived.rows[0].avg,
      ratings_count: parseInt(ratingsReceived.rows[0].count),
      recent_transactions: recent.rows,
    };

    res.json({ success: true, passport });
  } catch (err) {
    console.error('Passport error:', err);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

// ============================================
// COMPLIANCE / EPR REPORT API
// ============================================

// GET /api/reports/compliance/:operator_id — EPR/CSRD-ready report
app.get('/api/reports/compliance/:operator_id', async (req, res) => {
  try {
    const { operator_id } = req.params;
    const { start_date, end_date, format = 'json' } = req.query;

    const op = await pool.query(`SELECT * FROM operators WHERE id=$1`, [operator_id]);
    if (!op.rows.length) return res.status(404).json({ success: false, message: 'Operator not found' });

    let dateFilter = '';
    const params = [operator_id];
    if (start_date) { params.push(start_date); dateFilter += ` AND t.transaction_date >= $${params.length}::timestamptz`; }
    if (end_date) { params.push(end_date); dateFilter += ` AND t.transaction_date <= $${params.length}::timestamptz`; }

    const transactions = await pool.query(`
      SELECT t.id, t.transaction_date, t.material_type, t.gross_weight_kg, t.net_weight_kg,
             t.contamination_deduction_percent, t.price_per_kg, t.total_price, t.payment_status,
             t.lat, t.lng, t.payment_reference,
             c.first_name || ' ' || c.last_name as collector_name, c.phone as collector_phone, c.city as collector_city, c.region as collector_region
      FROM transactions t
      JOIN collectors c ON c.id=t.collector_id
      WHERE t.operator_id=$1 ${dateFilter}
      ORDER BY t.transaction_date ASC
    `, params);

    const summary = await pool.query(`
      SELECT material_type, COUNT(*) as transaction_count, SUM(net_weight_kg) as total_kg_net,
             SUM(gross_weight_kg) as total_kg_gross, SUM(total_price) as total_paid_ghs,
             COUNT(DISTINCT t.collector_id) as unique_collectors,
             COUNT(*) FILTER (WHERE t.lat IS NOT NULL) as gps_verified_count
      FROM transactions t WHERE t.operator_id=$1 ${dateFilter}
      GROUP BY material_type ORDER BY material_type
    `, params);

    const report = {
      report_type: 'EPR_CSRD_COMPLIANCE',
      generated_at: new Date().toISOString(),
      operator: op.rows[0],
      period: { start: start_date || 'all-time', end: end_date || new Date().toISOString() },
      summary_by_material: summary.rows,
      total_transactions: transactions.rows.length,
      transactions: transactions.rows,
      '@context': 'https://schema.org',
      '@type': 'DigitalProductPassport',
      chain_of_custody: transactions.rows.map(t => ({
        transaction_id: t.id,
        date: t.transaction_date,
        collector: { name: t.collector_name, phone: t.collector_phone, city: t.collector_city },
        material: t.material_type,
        weight_kg_net: t.net_weight_kg,
        gps: t.lat ? { lat: t.lat, lng: t.lng } : null,
        payment_verified: t.payment_status === 'paid'
      }))
    };

    if (format === 'json') {
      res.setHeader('Content-Disposition', `attachment; filename="compliance-report-${operator_id}-${Date.now()}.json"`);
      res.json(report);
    } else {
      res.json(report);
    }
  } catch (err) {
    console.error('Compliance report error:', err);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

// GET /api/reports/product-journey/:transaction_id — consumer-facing QR journey
app.get('/api/reports/product-journey/:transaction_id', async (req, res) => {
  try {
    const { transaction_id } = req.params;
    const result = await pool.query(`
      SELECT t.*, c.first_name, c.last_name, c.city as collector_city, c.region as collector_region,
             o.name as operator_name, o.company as operator_company, o.city as operator_city
      FROM transactions t
      JOIN collectors c ON c.id=t.collector_id
      LEFT JOIN operators o ON o.id=t.operator_id
      WHERE t.id=$1
    `, [transaction_id]);
    if (!result.rows.length) return res.status(404).json({ success: false, message: 'Transaction not found' });
    const t = result.rows[0];
    res.json({
      success: true,
      journey: {
        collector: { name: t.first_name + ' ' + t.last_name, city: t.collector_city, region: t.collector_region },
        material: t.material_type,
        weight_kg: t.net_weight_kg,
        collected_at: t.transaction_date,
        location: t.lat ? { lat: t.lat, lng: t.lng } : { city: t.collector_city },
        processor: t.operator_name ? { name: t.operator_name, company: t.operator_company, city: t.operator_city } : null,
        verified: t.payment_status === 'paid'
      }
    });
  } catch (err) {
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

// TODO: Plastic credits — activate when ready (F1)
// Steps when ready: select standard (recommend Verra), get data model audited,
// register as approved data provider, build credit issuance module,
// decide on marketplace vs broker model. The transaction + GPS + weight data
// already captured here is the right shape for credit issuance.

// ============================================
// CODE EXPORT
// ============================================

app.get('/code-export.txt', (req, res) => {
  const exportFiles = [
    'server.js',
    'migrate.js',
    'package.json',
    'render.yaml',
    '.gitignore',
    '.nvmrc',
    'README.md',
    'public/index.html',
    'public/collect.html',
    'public/dashboard.html',
    'public/admin.html',
    'public/prices.html',
    'migrations/1709942400000_create_pickers_and_collections.js',
    'migrations/1709942500000_refactor_to_collectors_and_transactions.js',
    'migrations/1741564800000_create_operators.js',
    'migrations/1741651200000_add_payment_tracking.js',
    'migrations/1773292800000_create_ussd_sessions.js',
    'migrations/1773379200000_add_buyers_and_admin.js',
    'migrations/1774000000000_expand_schema.js',
    'public/collector-dashboard.html',
    'public/aggregator-dashboard.html',
    'public/processor-dashboard.html',
    'public/converter-dashboard.html',
    'public/report.html',
  ];

  let output = `CIRCUL CODEBASE EXPORT\nGenerated: ${new Date().toISOString()}\n\n`;

  for (const filePath of exportFiles) {
    const fullPath = path.join(__dirname, filePath);
    output += `\n===== FILE: ${filePath} =====\n\n`;
    if (fs.existsSync(fullPath)) {
      try {
        output += fs.readFileSync(fullPath, 'utf8');
      } catch (err) {
        output += `[Error reading file: ${err.message}]\n`;
      }
    } else {
      output += `[File not found]\n`;
    }
    output += '\n';
  }

  res.setHeader('Content-Type', 'text/plain; charset=utf-8');
  res.setHeader('Content-Disposition', 'attachment; filename="circul-codebase-export.txt"');
  res.send(output);
});

// ============================================
// PAGE ROUTES
// ============================================

// Landing page with analytics beacon
app.get('/', (req, res) => {
  const slug = process.env.POLSIA_ANALYTICS_SLUG || '';
  const htmlPath = path.join(__dirname, 'public', 'index.html');

  if (fs.existsSync(htmlPath)) {
    let html = fs.readFileSync(htmlPath, 'utf8');
    html = html.replace('__POLSIA_SLUG__', slug);
    res.type('html').send(html);
  } else {
    res.json({ message: 'Hello from Circul!' });
  }
});

// Collector app
app.get('/collect', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'collect.html'));
});

// Operator dashboard
app.get('/dashboard', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'dashboard.html'));
});

// Admin dashboard
app.get('/admin', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'admin.html'));
});

// Role dashboards
app.get('/collector-dashboard', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'collector-dashboard.html'));
});
app.get('/aggregator-dashboard', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'aggregator-dashboard.html'));
});
app.get('/processor-dashboard', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'processor-dashboard.html'));
});
app.get('/converter-dashboard', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'converter-dashboard.html'));
});
// Collector report / passport
app.get('/report', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'report.html'));
});
app.get('/passport', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'report.html'));
});
// Prices redirect to homepage (no separate /prices page per global rules)
app.get('/prices', (req, res) => {
  res.redirect('/');
});

// Login page
app.get('/login', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'login.html'));
});

app.listen(port, () => {
  console.log(`Circul server running on port ${port}`);
});
