// shared/roles.js — Single source of truth for all role definitions
(function (root, factory) {
  if (typeof module !== 'undefined' && module.exports) {
    module.exports = factory();
  } else {
    root.CirculRoles = factory();
  }
})(typeof self !== 'undefined' ? self : this, function () {

  // ── Tier hierarchy (highest → lowest) ──
  // This order controls: auto-routing for dual-role users, display priority
  var TIER_HIERARCHY = ['converter', 'recycler', 'processor', 'aggregator', 'collector'];

  // ── Role definitions ──
  // Every role in the system. To add a new tier, add one entry here.
  var ROLES = {
    collector: {
      label: 'Collector',
      tier: 5,
      dashboard: '/collector-dashboard',
      table: 'collectors',
      pillColor: '#00e676',
      pillBg: '#0a2e1a',
      authType: 'phone',
      isFree: true,
      posterTypes: ['aggregator'],
      canPostPrices: false,
      description: 'Street-level pickup'
    },
    aggregator: {
      label: 'Aggregator',
      tier: 4,
      dashboard: '/aggregator-dashboard',
      table: 'aggregators',
      pillColor: '#26c6da',
      pillBg: '#0a1e2e',
      authType: 'phone',
      isFree: true,
      posterTypes: ['processor', 'aggregator'],
      canPostPrices: true,
      description: 'Sort & bundle'
    },
    processor: {
      label: 'Processor',
      tier: 3,
      dashboard: '/processor-dashboard',
      table: 'processors',
      pillColor: '#42a5f5',
      pillBg: '#0a1a2e',
      authType: 'email',
      isFree: false,
      price: 49,
      posterTypes: ['processor', 'converter', 'recycler'],
      canPostPrices: true,
      description: 'Clean & process'
    },
    recycler: {
      label: 'Recycler',
      tier: 2,
      dashboard: '/recycler-dashboard',
      table: 'recyclers',
      pillColor: '#b388ff',
      pillBg: '#160d2e',
      authType: 'email',
      isFree: false,
      price: 149,
      posterTypes: ['processor'],
      canPostPrices: true,
      description: 'Flakes & pellets'
    },
    converter: {
      label: 'Converter',
      tier: 1,
      dashboard: '/converter-dashboard',
      table: 'converters',
      pillColor: '#f48fb1',
      pillBg: '#2a0a1a',
      authType: 'email',
      isFree: false,
      price: 299,
      posterTypes: ['converter', 'recycler'],
      canPostPrices: true,
      description: 'Manufacture & sell'
    },
    admin: {
      label: 'Admin',
      tier: 0,
      dashboard: '/admin',
      table: 'operators',
      pillColor: '#ffd740',
      pillBg: '#2e2a0a',
      authType: 'email',
      isFree: false,
      posterTypes: [],
      canPostPrices: false,
      description: 'Platform admin'
    },
    operator: {
      label: 'Operator',
      tier: 0,
      dashboard: '/admin',
      table: 'operators',
      pillColor: '#ffd740',
      pillBg: '#2e2a0a',
      authType: 'phone',
      isFree: false,
      posterTypes: [],
      canPostPrices: false,
      description: 'Field operator'
    }
  };

  // ── Derived helpers ──

  // Role → dashboard route map (replaces all ROLE_ROUTES objects)
  var ROLE_ROUTES = {};
  Object.keys(ROLES).forEach(function (r) { ROLE_ROUTES[r] = ROLES[r].dashboard; });

  // Role → DB table map (replaces hardcoded tableMap in admin endpoints)
  var TABLE_MAP = {};
  Object.keys(ROLES).forEach(function (r) { if (ROLES[r].table) TABLE_MAP[r] = ROLES[r].table; });

  // Paid roles that require email auth (for login cascade, request-access, admin approval)
  var PAID_ROLES = Object.keys(ROLES).filter(function (r) { return !ROLES[r].isFree && r !== 'admin' && r !== 'operator'; });

  // Free roles (phone auth)
  var FREE_ROLES = Object.keys(ROLES).filter(function (r) { return ROLES[r].isFree; });

  // Roles that can post prices
  var PRICE_POSTER_ROLES = Object.keys(ROLES).filter(function (r) { return ROLES[r].canPostPrices; });

  // For a given role, what poster_types are visible in price listings
  function getPosterTypes(role) {
    var r = ROLES[role];
    return r ? r.posterTypes : Object.keys(ROLES).filter(function (k) { return ROLES[k].canPostPrices; });
  }

  // Given an array of roles, return the highest-tier one
  function highestRole(roles) {
    for (var i = 0; i < TIER_HIERARCHY.length; i++) {
      if (roles.indexOf(TIER_HIERARCHY[i]) !== -1) return TIER_HIERARCHY[i];
    }
    return roles[0] || null;
  }

  // Get dashboard URL for a role
  function dashboardFor(role) {
    return ROLE_ROUTES[role] || '/';
  }

  // Get pill styling for a role
  function pillStyle(role) {
    var r = ROLES[role];
    return r ? { color: r.pillColor, bg: r.pillBg, label: r.label.toUpperCase() } : { color: '#ccc', bg: '#333', label: role.toUpperCase() };
  }

  return {
    ROLES: ROLES,
    TIER_HIERARCHY: TIER_HIERARCHY,
    ROLE_ROUTES: ROLE_ROUTES,
    TABLE_MAP: TABLE_MAP,
    PAID_ROLES: PAID_ROLES,
    FREE_ROLES: FREE_ROLES,
    PRICE_POSTER_ROLES: PRICE_POSTER_ROLES,
    getPosterTypes: getPosterTypes,
    highestRole: highestRole,
    dashboardFor: dashboardFor,
    pillStyle: pillStyle
  };
});
