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
  PAYMENT_CONFIRMED: 'payment_confirmed',
  // Account recovery
  PIN_RESET_OTP:                 'pin_reset_otp',
  PIN_RESET_COMPLETED:           'pin_reset_completed',
  PIN_RESET_UPSTREAM_COLLECTOR:  'pin_reset_upstream_collector',
  PIN_RESET_UPSTREAM_AGGREGATOR: 'pin_reset_upstream_aggregator',
  PIN_RESET_UPSTREAM_AGENT:      'pin_reset_upstream_agent',
  PHONE_CHANGE_OTP:              'phone_change_otp',
  PHONE_CHANGED_NEW:             'phone_changed_new',
  PHONE_CHANGED_OLD:             'phone_changed_old',
  PHONE_CHANGED_UPSTREAM:        'phone_changed_upstream',
  ADMIN_PIN_RESET_TRIGGERED:     'admin_pin_reset_triggered'
};

// Security events bypass the daily SMS cap — an account-recovery alert that
// gets swallowed by a rate limit is worse than one extra SMS/day.
var SECURITY_EVENTS = new Set([
  EVENTS.PIN_RESET_OTP,
  EVENTS.PIN_RESET_COMPLETED,
  EVENTS.PIN_RESET_UPSTREAM_COLLECTOR,
  EVENTS.PIN_RESET_UPSTREAM_AGGREGATOR,
  EVENTS.PIN_RESET_UPSTREAM_AGENT,
  EVENTS.PHONE_CHANGE_OTP,
  EVENTS.PHONE_CHANGED_NEW,
  EVENTS.PHONE_CHANGED_OLD,
  EVENTS.PHONE_CHANGED_UPSTREAM,
  EVENTS.ADMIN_PIN_RESET_TRIGGERED
]);

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
  },
  // Account recovery — templates use { code, minutes, time, user_name, user_code, old_phone, new_phone, admin_email }
  pin_reset_otp: function (d) {
    return 'Your Circul reset code: ' + d.code + '\n\nEnter this on *920*54# to set a new PIN. Expires in ' + d.minutes + ' min. Never share this code.';
  },
  pin_reset_completed: function (d) {
    return 'Your Circul PIN was reset at ' + d.time + '.\n\nIf this wasn\'t you, call your aggregator immediately and request an account freeze.';
  },
  pin_reset_upstream_collector: function (d) {
    return 'Collector PIN reset: ' + d.user_name + ' (' + d.user_code + ') reset their PIN at ' + d.time + '.\n\nWatch for unusual activity; contact Circul support if suspicious.';
  },
  pin_reset_upstream_aggregator: function (d) {
    return 'Aggregator PIN reset: ' + d.user_name + ' (' + d.user_code + ') reset their PIN at ' + d.time + '.\n\nWatch for unusual dispatch approvals or drop-off confirmations. Contact Circul support if suspicious.';
  },
  pin_reset_upstream_agent: function (d) {
    return 'Agent PIN reset: ' + d.user_name + ' (' + d.user_code + ') reset their PIN at ' + d.time + '. Agent works under you.\n\nWatch for unusual activity.';
  },
  phone_change_otp: function (d) {
    return 'Your Circul verification code: ' + d.code + '\n\nGive this to Circul admin to confirm the phone change to this number. Expires in ' + d.minutes + ' min. Never share unless you requested a phone change.';
  },
  phone_changed_new: function (d) {
    return 'Your Circul phone was changed to this number. All your history is preserved.\n\nIf this wasn\'t you, call Circul support immediately.';
  },
  phone_changed_old: function (d) {
    return 'Your Circul phone number was changed to ' + d.new_phone + ' by Circul admin at ' + d.time + '.\n\nIf this wasn\'t you, call Circul support immediately.';
  },
  phone_changed_upstream: function (d) {
    return 'Phone change: ' + d.user_code + ' (' + d.user_name + ') \u2014 phone updated by Circul admin at ' + d.time + '. Was ' + d.old_phone + ', now ' + d.new_phone + '. Watch for unusual activity.';
  },
  admin_pin_reset_triggered: function (d) {
    return 'Circul admin triggered a PIN reset for your account at ' + d.time + '. Dial *920*54# and follow the prompts to set a new PIN.\n\nIf you didn\'t request this, call Circul support immediately.';
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
    var isSecurity = SECURITY_EVENTS.has(event);
    var day = _todayKey();
    if (!_smsCounts[day]) _smsCounts = { [day]: {} };
    var bucket = _smsCounts[day];
    if (!isSecurity && (bucket[phone] || 0) >= SMS_DAILY_CAP) {
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

module.exports = { EVENTS: EVENTS, TEMPLATES: TEMPLATES, notify: notify, SECURITY_EVENTS: SECURITY_EVENTS };
