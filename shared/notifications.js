// shared/notifications.js
// Notification hooks — sends to console in dev, ready for SMS/WhatsApp provider in prod

var phoneLib = require('./phone');
var SMS_DAILY_CAP = parseInt(process.env.SMS_DAILY_CAP || '20', 10);
var _smsCounts = {}; // { 'YYYY-MM-DD': { '+233...': count } }

function _todayKey() { return new Date().toISOString().slice(0, 10); }

async function _sendViaAfricasTalking(phone, message) {
  var apiKey = process.env.AT_API_KEY;
  var username = process.env.AT_USERNAME;
  if (!apiKey || !username) return { sent: false, reason: 'AT_API_KEY or AT_USERNAME missing' };
  var sender = process.env.AT_SENDER || 'Circul';
  var params = new URLSearchParams({ username: username, to: phone, message: message, from: sender });
  try {
    var res = await fetch('https://api.africastalking.com/version1/messaging', {
      method: 'POST',
      headers: { Accept: 'application/json', 'Content-Type': 'application/x-www-form-urlencoded', apiKey: apiKey },
      body: params.toString()
    });
    var body = await res.json().catch(function () { return {}; });
    var recipients = (body.SMSMessageData && body.SMSMessageData.Recipients) || [];
    var first = recipients[0] || {};
    var ok = res.ok && first.statusCode === 101;
    return { sent: ok, status: res.status, body: body, reason: ok ? undefined : first.status || 'africastalking error' };
  } catch (e) {
    return { sent: false, reason: e.message };
  }
}

var EVENTS = {
  NEW_OFFER:         'new_offer',
  OFFER_ACCEPTED:    'offer_accepted',
  OFFER_REJECTED:    'offer_rejected',
  COUNTER_OFFER:     'counter_offer',
  DELIVERY_PENDING:  'delivery_pending',
  DELIVERY_APPROVED: 'delivery_approved',
  PAYMENT_RECEIVED:  'payment_received',
  RATING_RECEIVED:   'rating_received',
  LISTING_EXPIRING:  'listing_expiring',
  // Phase 5B
  DROPOFF_LOGGED:    'dropoff_logged',
  PURCHASE_LOGGED:   'purchase_logged',
  AGENT_COLLECTION:  'agent_collection',
  PAYMENT_SENT:      'payment_sent',
  PAYMENT_CONFIRMED: 'payment_confirmed'
};

var TEMPLATES = {
  new_offer: function (data) {
    return 'Circul: ' + data.buyer_name + ' made a GH\u20b5' + data.price + '/kg offer on your ' + data.material + ' listing (' + data.qty + 'kg). Log in to respond.';
  },
  offer_accepted: function (data) {
    return 'Circul: Your offer on ' + data.material + ' (' + data.qty + 'kg) was accepted by ' + data.seller_name + '! A transaction has been created.';
  },
  offer_rejected: function (data) {
    return 'Circul: ' + data.seller_name + ' declined your offer on ' + data.material + '. Browse other listings on Circul.';
  },
  counter_offer: function (data) {
    return 'Circul: ' + data.counterparty + ' countered your offer \u2014 GH\u20b5' + data.price + '/kg for ' + data.qty + 'kg ' + data.material + '. Log in to respond.';
  },
  delivery_pending: function (data) {
    return 'Circul: ' + data.sender_name + ' dispatched ' + data.qty + 'kg ' + data.material + ' to you. Log in to approve delivery.';
  },
  delivery_approved: function (data) {
    return 'Circul: ' + data.receiver_name + ' approved your ' + data.qty + 'kg ' + data.material + ' delivery. Payment is being processed.';
  },
  payment_received: function (data) {
    return 'Circul: You received GH\u20b5' + data.amount + ' for ' + data.qty + 'kg ' + data.material + '. Check your dashboard for details.';
  },
  rating_received: function (data) {
    return 'Circul: ' + data.rater_name + ' rated you ' + data.stars + '\u2605. View your ratings on your dashboard.';
  },
  listing_expiring: function (data) {
    return 'Circul: Your ' + data.material + ' listing (' + data.qty + 'kg) expires tomorrow. Log in to renew it.';
  },
  // Phase 5B
  dropoff_logged: function (data) {
    return 'Circul: ' + data.collector_name + ' logged a ' + data.qty + 'kg ' + data.material + ' drop-off (Ref ' + data.ref + '). Log in to confirm.';
  },
  purchase_logged: function (data) {
    return 'Circul: ' + data.buyer_name + ' recorded a purchase of ' + data.qty + 'kg ' + data.material + ' from you for GH\u20b5' + data.amount + ' (Ref ' + data.ref + ').';
  },
  agent_collection: function (data) {
    return 'Circul: Agent collected ' + data.qty + 'kg ' + data.material + ' from you for ' + data.aggregator_name + '. Total GH\u20b5' + data.amount + ' (Ref ' + data.ref + ').';
  },
  payment_sent: function (data) {
    return 'Circul: ' + data.buyer_name + ' marked GH\u20b5' + data.amount + ' as sent for your ' + data.qty + 'kg ' + data.material + ' (Ref ' + data.ref + '). Confirm receipt on Circul.';
  },
  payment_confirmed: function (data) {
    return 'Circul: ' + data.seller_name + ' confirmed receipt of GH\u20b5' + data.amount + ' for ' + data.qty + 'kg ' + data.material + ' (Ref ' + data.ref + '). Transaction complete.';
  }
};

async function notify(event, recipientPhone, data) {
  var template = TEMPLATES[event];
  if (!template) { console.warn('Unknown notification event:', event); return; }

  var message = template(data);
  var phone = phoneLib.normalizeGhanaPhone(recipientPhone) || recipientPhone;
  var provider = process.env.NOTIFICATION_PROVIDER || 'console';

  if (provider === 'console') {
    console.log('[NOTIFY] ' + event + ' \u2192 ' + phone + ': ' + message);
    return { sent: false, reason: 'console-only mode' };
  }

  if (provider === 'africastalking') {
    // Pre-check cap (don't increment yet)
    var day = _todayKey();
    if (!_smsCounts[day]) _smsCounts = { [day]: {} };
    var bucket = _smsCounts[day];
    if ((bucket[phone] || 0) >= SMS_DAILY_CAP) {
      console.warn('[NOTIFY] daily cap reached for ' + phone + ' (' + event + ')');
      return { sent: false, reason: 'daily cap reached' };
    }
    try {
      var result = await _sendViaAfricasTalking(phone, message);
      if (result.sent) bucket[phone] = (bucket[phone] || 0) + 1;
      else console.warn('[NOTIFY] africastalking failed (' + event + '):', result.reason || result.status);
      return result;
    } catch (e) {
      console.warn('[NOTIFY] africastalking error:', e.message);
      return { sent: false, reason: e.message };
    }
  }

  console.warn('[NOTIFY] unknown provider:', provider);
  return { sent: false, reason: 'unknown provider' };
}

module.exports = { EVENTS: EVENTS, TEMPLATES: TEMPLATES, notify: notify };
