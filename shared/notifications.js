// shared/notifications.js
// Notification hooks — sends to console in dev, ready for SMS/WhatsApp provider in prod

var EVENTS = {
  NEW_OFFER:         'new_offer',
  OFFER_ACCEPTED:    'offer_accepted',
  OFFER_REJECTED:    'offer_rejected',
  COUNTER_OFFER:     'counter_offer',
  DELIVERY_PENDING:  'delivery_pending',
  DELIVERY_APPROVED: 'delivery_approved',
  PAYMENT_RECEIVED:  'payment_received',
  RATING_RECEIVED:   'rating_received',
  LISTING_EXPIRING:  'listing_expiring'
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
  }
};

async function notify(event, recipientPhone, data) {
  var template = TEMPLATES[event];
  if (!template) { console.warn('Unknown notification event:', event); return; }

  var message = template(data);

  // --- PROVIDER HOOK ---
  // When ready, replace this block with actual SMS/WhatsApp API call
  // e.g., Twilio, Africa's Talking, or WhatsApp Business API
  if (process.env.NOTIFICATION_PROVIDER === 'console' || !process.env.NOTIFICATION_PROVIDER) {
    console.log('[NOTIFY] ' + event + ' \u2192 ' + recipientPhone + ': ' + message);
    return { sent: false, reason: 'console-only mode' };
  }

  // Future: switch on process.env.NOTIFICATION_PROVIDER
  // case 'twilio': return sendViaTwilio(recipientPhone, message);
  // case 'africastalking': return sendViaAT(recipientPhone, message);
}

module.exports = { EVENTS: EVENTS, TEMPLATES: TEMPLATES, notify: notify };
