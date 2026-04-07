// shared/phone.js
// Single source of truth for Ghana phone number normalization.

function normalizeGhanaPhone(phone) {
  if (!phone) return null;
  let cleaned = String(phone).replace(/[\s\-()]/g, '');
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

module.exports = { normalizeGhanaPhone, getPhoneVariants };
