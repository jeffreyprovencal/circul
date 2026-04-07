// shared/ratings.js
// Shared rating helpers — pending lookups + creation with dispute window.

const RATING_WINDOW_DAYS = parseInt(process.env.RATING_WINDOW_DAYS || '7', 10);

// role → which column in pending_transactions identifies this user,
// and which column/table identifies the counterparty they should rate.
const ROLE_MAP = {
  collector:  { myCol: 'collector_id',  peerCol: 'aggregator_id', peerTable: 'aggregators', peerName: 'name' },
  aggregator: { myCol: 'aggregator_id', peerCol: 'collector_id',  peerTable: 'collectors',  peerName: "first_name || ' ' || last_name" },
  processor:  { myCol: 'processor_id',  peerCol: 'aggregator_id', peerTable: 'aggregators', peerName: 'name' },
  recycler:   { myCol: 'recycler_id',   peerCol: 'processor_id',  peerTable: 'processors',  peerName: 'name' },
  converter:  { myCol: 'converter_id',  peerCol: 'processor_id',  peerTable: 'processors',  peerName: 'name' },
};

async function getPendingRatings(pool, role, userId, limit = 5) {
  if (!userId) return [];

  // Agent branch: agents see only the transactions they personally collected,
  // joined via agent_activity. The peer is always a collector.
  if (role === 'agent') {
    const rows = await pool.query(
      `SELECT pt.id AS txn_id, pt.material_type, pt.gross_weight_kg, pt.created_at,
              pt.collector_id AS peer_id,
              (c.first_name || ' ' || c.last_name) AS peer_name
       FROM pending_transactions pt
       JOIN agent_activity aa ON aa.related_id = pt.id
                              AND aa.related_type = 'transaction'
                              AND aa.action_type = 'collection'
                              AND aa.agent_id = $1
       LEFT JOIN collectors c ON c.id = pt.collector_id
       WHERE pt.status IN ('completed','confirmed')
         AND pt.created_at > NOW() - ($2 || ' days')::INTERVAL
         AND NOT EXISTS (
           SELECT 1 FROM ratings r
           WHERE r.transaction_id = pt.id
             AND r.rater_type = 'agent'
             AND r.rater_id = $1
         )
       ORDER BY pt.created_at DESC
       LIMIT $3`,
      [userId, String(RATING_WINDOW_DAYS), limit]
    );
    return rows.rows;
  }

  const cfg = ROLE_MAP[role];
  if (!cfg) return [];
  const rows = await pool.query(
    `SELECT pt.id AS txn_id, pt.material_type, pt.gross_weight_kg, pt.created_at,
            pt.${cfg.peerCol} AS peer_id,
            p.${cfg.peerName} AS peer_name
     FROM pending_transactions pt
     LEFT JOIN ${cfg.peerTable} p ON p.id = pt.${cfg.peerCol}
     WHERE pt.${cfg.myCol} = $1
       AND pt.status IN ('completed','confirmed')
       AND pt.created_at > NOW() - ($3 || ' days')::INTERVAL
       AND NOT EXISTS (
         SELECT 1 FROM ratings r
         WHERE r.transaction_id = pt.id
           AND r.rater_type = $2
           AND r.rater_id = $1
       )
     ORDER BY pt.created_at DESC
     LIMIT $4`,
    [userId, role, String(RATING_WINDOW_DAYS), limit]
  );
  return rows.rows;
}

async function createRating(pool, params) {
  const {
    transaction_id, rater_type, rater_id, rated_type, rated_id,
    rating, tags, notes, rating_direction
  } = params;
  const windowExpires = new Date();
  windowExpires.setDate(windowExpires.getDate() + 30);
  try {
    const r = await pool.query(
      `INSERT INTO ratings (transaction_id, rater_type, rater_id, rated_type, rated_id, rating, tags, notes, rating_direction, window_expires_at)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) RETURNING *`,
      [transaction_id||null, rater_type, rater_id, rated_type, rated_id, rating, tags||[], notes||null, rating_direction||null, windowExpires.toISOString()]
    );
    return r.rows[0];
  } catch (e) {
    const r = await pool.query(
      `INSERT INTO ratings (transaction_id, rater_type, rater_id, rated_type, rated_id, rating, tags, notes, rating_direction)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) RETURNING *`,
      [transaction_id||null, rater_type, rater_id, rated_type, rated_id, rating, tags||[], notes||null, rating_direction||null]
    );
    return r.rows[0];
  }
}

module.exports = { RATING_WINDOW_DAYS, ROLE_MAP, getPendingRatings, createRating };
