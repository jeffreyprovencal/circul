# Circul — Information Architecture

Generated 2026-04-18. Factual snapshot of what exists in the code on `main` (HEAD = ad93bd1). Flags and gaps are called out inline.

---

## 1. File Tree

```
circul/
├── README.md
├── package.json                 # express 4, multer 2, pg 8; node >= 18
├── package-lock.json
├── migrate.js                   # custom migration runner (.js + .sql)
├── server.js                    # 5954 lines — single-file Express API
├── render.yaml                  # Render deploy config
├── .env.example
├── .gitignore
├── .nvmrc
│
├── migrations/                  # 29 files, timestamped
│   ├── 1709942400000_create_pickers_and_collections.js
│   ├── 1709942500000_refactor_to_collectors_and_transactions.js
│   ├── 1741564800000_create_operators.js
│   ├── 1741651200000_add_payment_tracking.js
│   ├── 1773292800000_create_ussd_sessions.js
│   ├── 1773379200000_add_buyers_and_admin.js
│   ├── 1774000000000_expand_schema.js
│   ├── 1774100000000_seed_full_supply_chain.js
│   ├── 1774200000000_add_processor_converter_id_to_transactions.js
│   ├── 1774300000000_add_pending_transactions_and_orders.js
│   ├── 1774500000000_restructure_tiers.js
│   ├── 1774600000000_fix_demo_transaction.js
│   ├── 1774700000000_add_auth_fields.js
│   ├── 1774800000000_add_recycler_tier.js
│   ├── 1774900000000_seed_recycler_prices.js
│   ├── 1774950000000_create_orders.js
│   ├── 1775000000000_create_expense_tables.js
│   ├── 1776000000000_create_discovery_tables.js
│   ├── 1776100000000_create_error_log.js
│   ├── 1776200000000_unique_rating_constraint.sql
│   ├── 1776300000000_add_must_change_pin.sql
│   ├── 1776300000000_hash_existing_pins.sql        ⚠ duplicate timestamp
│   ├── 1776400000000_add_performance_indexes.sql
│   ├── 1776400000001_fix_offers_cascade.sql
│   ├── 1776500000000_reset_expense_categories.js
│   ├── 1776600000000_batch8b_and_batch9.js
│   ├── 1776700000000_fix_ratings_fk.sql
│   ├── 1776800000000_seed_demo_agent.js
│   └── 1777000000000_ussd_sessions_multi_role.sql
│
├── public/                      # all user-facing HTML served by Express
│   ├── admin.html
│   ├── agent-dashboard.html
│   ├── aggregator-dashboard.html
│   ├── collect.html
│   ├── collector-dashboard.html
│   ├── collector-passport.html
│   ├── converter-dashboard.html
│   ├── demo-access.html
│   ├── index.html
│   ├── login.html
│   ├── mockup-rating-system.html ⚠ dead mockup checked into /public
│   ├── prices.html              ⚠ route /prices redirects to /
│   ├── processor-dashboard.html
│   ├── recycler-dashboard.html
│   ├── register.html
│   ├── report.html
│   ├── shared.css
│   └── uploads/                 # multer destination for expense receipts + photos
│
├── shared/                      # served at /shared and require()d by server
│   ├── roles.js                 # single source of truth for role config
│   ├── transaction-parties.js   # seller/buyer resolution by transaction_type
│   ├── ratings.js               # pending rating lookups + createRating
│   ├── notifications.js         # SMS via Africa's Talking
│   └── phone.js                 # Ghana phone normalization
│
├── scripts/
│   ├── hash-existing-pins.js    # companion for 1776300000000_hash_existing_pins
│   ├── seed-poly.sql
│   └── seed-repatrn.sql
│
├── mockups/                     # 24 design mockups (not served)
│   └── mockup-*.html
│
├── strategy/                    # planning docs (markdown, docx, xlsx)
│   ├── WORK-partnership.md
│   ├── brand-tier-recovery-credits.md
│   ├── domain-and-email-plan.md
│   ├── partner-pricing-strategy.md
│   ├── circul-build-roadmap.xlsx
│   ├── circul-market-expansion-strategy.docx
│   └── circul-strategy-meeting-april-15.docx
│
└── <audit / sync / prompt markdowns at repo root>   ⚠ 40+ loose .md files
```

**Root-level clutter flag:** 40+ audit, sync, and "claude-code-prompt-*" markdown files live at the repo root plus three large `circul-*chunk*.txt` exports. Consider an `archive/` folder.

---

## 2. Routes

All routes are defined in [server.js](server.js). Auth is a **custom HMAC-SHA256 bearer token** (not JWT lib). Middleware: `requireAuth` (user tokens, `AUTH_SECRET`), `requireAdmin` (admin tokens, `ADMIN_SECRET`). Role check: `req.user.hasRole(role)` over `req.user.role` plus `req.user.roles[]`. Token is read from `Authorization: Bearer …` or `?token=` query.

### 2.1 Public (no auth)

#### HTML & static
| Method | Path | Line | Description |
|---|---|---|---|
| GET | `/health` | 48 | Health check |
| GET | `/` | 5442 | index.html (marketing + price widget) |
| GET | `/collect` | 5449 | collect.html (collector drop-off app) |
| GET | `/dashboard` | 6990 | 301 redirect to /aggregator-dashboard.html |
| GET | `/admin` | 5451 | admin.html |
| GET | `/collector-dashboard` | 5452 | collector-dashboard.html |
| GET | `/aggregator-dashboard` | 5453 | aggregator-dashboard.html |
| GET | `/processor-dashboard` | 5454 | processor-dashboard.html |
| GET | `/converter-dashboard` | 5455 | converter-dashboard.html |
| GET | `/recycler-dashboard` | 5456 | recycler-dashboard.html |
| GET | `/report` | 5457 | report.html |
| GET | `/passport` | 5458 | collector-passport.html |
| GET | `/collector-passport/:id` | 5459 | collector-passport.html (public profile) |
| GET | `/login` | 5460 | login.html |
| GET | `/register` | 5461 | register.html |
| GET | `/prices` | 5462 | 302 → `/` |
| — | `/shared/*` | 49 | static |
| — | `/public/*` | 60 | static |
| GET | `/code-export.txt` | 5424 | raw source export (⚠ exposes server.js contents publicly) |

#### Auth / registration
| Method | Path | Line | Description |
|---|---|---|---|
| POST | `/api/auth/login` | 5958 | Unified login — phone+PIN or email+password across every role |
| POST | `/api/auth/register` | 5468 | Self-register collector or aggregator |
| POST | `/api/auth/request-access` | 5498 | Paid-role access request (processor / recycler / converter) |
| POST | `/api/admin/login` | 4944 | Admin email+password → admin token |
| POST | `/api/collectors/login` | 286 | Legacy collector phone+PIN login |
| POST | `/api/collectors` | 226 | Create a new collector |
| POST | `/api/aggregators/:id/register-collector` | 246 | Aggregator-side collector onboarding |

#### Public read endpoints
| Method | Path | Line | Description |
|---|---|---|---|
| GET | `/api/collectors` | 316 | List/search collectors (phone query) |
| GET | `/api/collectors/:id` | 342 | Collector profile |
| GET | `/api/collectors/:id/stats` | 366 | Collector stats |
| GET | `/api/collectors/:id/passport` | 5370 | Public passport payload |
| GET | `/api/aggregators` | 706 | List aggregators |
| GET | `/api/aggregators/:id` | 722 | Aggregator profile |
| GET | `/api/aggregators/:id/stats` | 733 | Aggregator stats |
| GET | `/api/processors` | 1636 | List processors |
| GET | `/api/processors/:id/stats` | 1665 | Processor stats |
| GET | `/api/processors/:id/prices` | 1716 | Prices posted by processor |
| POST | `/api/processors/:id/prices` | 1690 | ⚠ Public POST that writes prices — no auth check in handler |
| GET | `/api/processor-prices` | 1725 | Aggregate processor-price feed |
| GET | `/api/converters` | 1858 | List converters |
| GET | `/api/recyclers` | 1974 | List recyclers |
| GET | `/api/transactions` | 2150 | Paginated transactions feed |
| GET | `/api/stats` | 2172 | System-wide stats |
| GET | `/api/prices` | 5333 | Unified posted-prices feed |
| POST | `/api/prices` | 5312 | ⚠ Public POST — handler does not call `requireAuth` |
| GET | `/api/market-prices` | 5355 | Market price snapshot |
| GET | `/api/reports/compliance/:aggregator_id` | 5393 | Compliance/EPR report |
| GET | `/api/reports/product-journey/:transaction_id` | 5410 | "Product journey" for a `transactions.id` |
| GET | `/api/expense-categories` | 878 | Category list |
| POST | `/api/listings/locations` | 1136 | Distinct listing city list |
| POST | `/api/ussd` | 4080 | Africa's Talking USSD gateway handler |
| GET | `/api/ussd/stats` | 4139 | USSD session stats |
| POST | `/api/pending-transactions` | 4151 | Create pending tx (used by collect.html without login) |
| GET | `/api/pending-transactions` | 4208 | Paginated listing |
| POST | `/api/error-log` | 5600 | Client-side error beacon |

### 2.2 Authenticated — by role

> ⚠ Several endpoints are labelled "requires auth" in code review but inspection shows some handlers on `/api/pending-transactions/*` rely on body params (no `requireAuth` on the router); treat the role column below as **intended** access, not always enforced. A security pass is warranted.

#### Collector (`requireAuth` + `role='collector'`)
| Method | Path | Line | Description |
|---|---|---|---|
| PATCH | `/api/collectors/:id/change-pin` | 271 | Change own PIN (self-ID check) |
| GET | `/api/collector/me` | 397 | Authenticated profile |
| GET | `/api/collector/stats` | 421 | Own stats |
| GET | `/api/collector/transactions` | 437 | Own transactions |
| GET | `/api/collector/prices` | 458 | Prices I have posted |
| GET | `/api/collector/top-buyers` | 479 | Top aggregators I sell to |
| GET | `/api/collector/pl` | 507 | Own P&L |
| GET | `/api/collector/pending-purchases` | 530 | Pending offers / purchases |
| POST | `/api/collector/confirm-receipt` | 546 | Confirm item receipt |
| POST | `/api/collector/transactions/:id/confirm` | 561 | Confirm a transaction |
| POST | `/api/collector/pending-purchases/:id/accept` | 576 | Accept pending purchase |
| POST | `/api/collector/pending-purchases/:id/decline` | 594 | Decline pending purchase |
| POST | `/api/collector/rate-aggregator` | 612 | Rate an aggregator after a tx |

#### Aggregator
| Method | Path | Line | Description |
|---|---|---|---|
| GET | `/api/aggregator/me` | 639 | Profile |
| GET | `/api/aggregators/:id/agent-ratings` | 840 | Ratings of my agents |
| POST | `/api/aggregators/:id/expenses` | 961 | Upload expense receipt (multer) |
| GET | `/api/aggregators/:id/expenses` | 1004 | My expenses |
| DELETE | `/api/aggregators/:id/expenses/:eid` | 1073 | Delete expense |
| GET | `/api/aggregator/top-suppliers` | 1566 | Top collectors |
| GET | `/api/aggregator/top-buyers` | 1599 | Top processors |
| GET | `/api/aggregator/agent-activity` | 5826 | Agents' action log |
| GET | `/api/agents` | 5719 | Agents under me |
| POST | `/api/agents` | 5731 | Create an agent |

#### Processor
| Method | Path | Line | Description |
|---|---|---|---|
| GET | `/api/processor/me` | 657 | Profile |
| GET | `/api/processor/top-suppliers` | 1742 | Top aggregators |
| GET | `/api/processor/top-buyers` | 1768 | Top recyclers/converters |
| GET | `/api/processor/transactions` | 1798 | Processor txns |
| GET | `/api/pending-transactions/processor-queue` | 4361 | Inbound aggregator-sales awaiting dispatch/arrival |
| POST | `/api/pending-transactions/:id/dispatch-decision` | 4375 | Approve/reject dispatch |
| POST | `/api/pending-transactions/:id/arrival-confirmation` | 4407 | Confirm arrival + regrade |
| POST | `/api/pending-transactions/processor-sale` | 4430 | Create outbound sale to recycler/converter |
| GET | `/api/pending-transactions/processor-sales` | 4453 | My outbound sales |
| POST | `/api/supply-requirements` | 5872 | Post spec |
| PATCH | `/api/supply-requirements/:id` | 5889 | Update spec |
| DELETE | `/api/supply-requirements/:id` | 5909 | Delete spec |
| PATCH | `/api/pending-transactions/:id/link-requirement` | 5921 | Manually tag a tx with a spec |

#### Recycler
| Method | Path | Line | Description |
|---|---|---|---|
| GET | `/api/recycler/me` | 687 | Profile |
| GET | `/api/recycler/transactions` | 1827 | Recycler txns |
| GET | `/api/recycler/top-suppliers` | 2024 | Top processors |
| GET | `/api/recycler/top-buyers` | 2051 | Top converters |
| GET | `/api/pending-transactions/recycler-queue` | 4473 | Inbound processor-sales |
| POST | `/api/pending-transactions/:id/recycler-dispatch-decision` | 4487 | Dispatch approve/reject |
| POST | `/api/pending-transactions/:id/recycler-arrival` | 4516 | Arrival + regrade |
| POST | `/api/pending-transactions/recycler-sale` | 4541 | Create outbound sale to converter |
| GET | `/api/pending-transactions/recycler-sales` | 4556 | My outbound sales |

#### Converter
| Method | Path | Line | Description |
|---|---|---|---|
| GET | `/api/converter/me` | 672 | Profile |
| GET | `/api/converter/transactions` | 2110 | Converter txns |
| GET | `/api/converter/top-suppliers` | 1933 | Top processors/recyclers |
| GET | `/api/pending-transactions/converter-queue` | 4572 | Inbound processor- or recycler-sales |
| POST | `/api/pending-transactions/:id/converter-dispatch-decision` | 4591 | Dispatch approve/reject |
| POST | `/api/pending-transactions/:id/converter-arrival` | 4623 | Arrival |

#### Agent (aggregator field agent)
| Method | Path | Line | Description |
|---|---|---|---|
| GET | `/api/agent/me` | 5758 | Profile |
| POST | `/api/agent/log-collection` | 5773 | Log pickup on behalf of aggregator |
| POST | `/api/agent/register-collector` | 5798 | Onboard a collector in the field |

#### Any authenticated user (cross-role)
| Method | Path | Line | Description |
|---|---|---|---|
| POST | `/api/expense-categories/suggest` | 890 | Suggest new category (admin approves) |
| GET | `/api/listings` | 1144 | Browse Discovery listings |
| GET | `/api/listings/mine` | 1180 | My listings |
| GET | `/api/listings/:id` | 1195 | Listing detail |
| PATCH | `/api/listings/:id/renew` | 1214 | Renew |
| PATCH | `/api/listings/:id/close` | 1231 | Close |
| DELETE | `/api/listings/:id` | 1249 | Delete |
| GET | `/api/listings/:id/offers` | 1295 | Offers on a listing |
| POST | `/api/listings/:id/offers` | 1320 | Make an offer |
| GET | `/api/offers/mine` | 1366 | My offers |
| GET | `/api/offers/:id/thread` | 1383 | Negotiation thread |
| POST | `/api/offers/:id/accept` | 1402 | Accept — creates pending_transaction |
| POST | `/api/offers/:id/reject` | 1472 | Reject |
| POST | `/api/offers/:id/counter` | 1502 | Counter |
| POST | `/api/ratings` | 2252 | Rate counterparty on a tx |
| GET | `/api/ratings/:id` | 2253 | Ratings *of* an operator |
| GET | `/api/collectors/:id/ratings` | 2255 | Ratings of a collector |
| GET | `/api/ratings/pending` | 2269 | Pending ratings for the caller |
| POST | `/api/transactions` | 2081 | Legacy direct collector→aggregator transaction |
| GET | `/api/pending-transactions/collector-sales` | 4235 | Collector's sales |
| GET | `/api/pending-transactions/aggregator-sales` | 4245 | Aggregator's sales |
| GET | `/api/pending-transactions/aggregator-purchases` | 4267 | Aggregator's purchases |
| PATCH | `/api/pending-transactions/:id/review` | 4277 | Aggregator grades + confirms a collector drop-off |
| POST | `/api/pending-transactions/aggregator-purchase` | 4322 | Aggregator records a collector purchase |
| POST | `/api/pending-transactions/aggregator-sale` | 4340 | Aggregator dispatches to processor/converter |
| PATCH | `/api/transactions/:id/payment-initiate` | 4646 | Mark payment sent |
| PATCH | `/api/transactions/:id/payment-confirm` | 4693 | Mark payment received |
| PATCH | `/api/pending-transactions/:id/payment-initiate` | 4727 | Same for pending_transactions |
| PATCH | `/api/pending-transactions/:id/payment-confirm` | 4774 | |
| POST | `/api/orders` | 4843 | Create purchase order (converter/recycler/aggregator) |
| GET | `/api/orders/my` | 4879 | My orders |
| POST | `/api/orders/:id/cancel` | 4915 | Cancel |
| GET | `/api/me/prices` | 5077 | My posted prices (paid roles only) |
| GET | `/api/error-log` | 5650 | Aggregated client errors |
| GET | `/api/profile/ghana-card` | 5681 | Ghana card info |
| PATCH | `/api/profile/ghana-card` | 5696 | Update Ghana card info |
| GET | `/api/supply-requirements` | 5839 | Read specs (all downstream roles) |
| GET | `/api/processors/:id` | 1649 | Processor detail |
| GET | `/api/converters/:id` | 1875 | Converter detail |
| GET | `/api/recyclers/:id` | 1986 | Recycler detail |

#### Admin (`requireAdmin`)
| Method | Path | Line | Description |
|---|---|---|---|
| GET | `/api/admin/stats` | 5090 | Admin dashboard numbers |
| GET | `/api/admin/transactions` | 5148 | All txns |
| GET | `/api/admin/collectors` | 5159 | List |
| PUT | `/api/admin/collectors/:id` | 5194 | Edit |
| PUT | `/api/admin/collectors/:id/verify` | 5217 | Mark ID verified |
| GET | `/api/admin/aggregators` | 5166 | List |
| PUT | `/api/admin/aggregators/:id` | 5173 | Edit |
| PUT | `/api/admin/aggregators/:id/verify` | 5225 | Verify |
| GET | `/api/admin/processors` | 5233 | List |
| PUT | `/api/admin/processors/:id` | 5240 | Edit |
| GET | `/api/admin/converters` | 5258 | List |
| PUT | `/api/admin/converters/:id` | 5265 | Edit |
| GET | `/api/admin/recyclers` | 5283 | List |
| PUT | `/api/admin/recyclers/:id` | 5290 | Edit |
| GET | `/api/admin/pending` | 5520 | Pending access requests |
| POST | `/api/admin/approve` | 5536 | Approve request |
| POST | `/api/admin/reject` | 5549 | Reject request |

**Totals:** ~138 API routes — 51 public, 87 authenticated (17 admin-only).

---

## 3. Pages

Each page lives under [public/](public/) and is served by one of the static-page routes in §2.1.

### [admin.html](public/admin.html) — Admin / Operator
Admin console: approve paid-role registrations, review pending expense categories, view platform stats.
Calls: `GET /api/admin/stats`, `GET /api/admin/pending`, `POST /api/admin/approve`, `POST /api/admin/reject`, `GET /api/expense-categories/pending`, `PATCH /api/expense-categories/:id/approve|reject`.

### [agent-dashboard.html](public/agent-dashboard.html) — Agent
Field agent console: log a pickup, register a new collector, see recent activity.
Calls: `POST /api/auth/login`, `GET /api/agent/me`, `POST /api/agent/log-collection`, `POST /api/agent/register-collector`, `GET /api/aggregators/:id/stats`, `GET /api/transactions?aggregator_id=`, `PATCH /api/profile/ghana-card`.

### [aggregator-dashboard.html](public/aggregator-dashboard.html) — Aggregator
Biggest single page (3434 lines). Queue review, price posting, ratings, expense ledger, Discovery listings/offers, dispatch to processor.
Calls: `/api/auth/login`, `/api/aggregators/:id`, `/api/prices`, `/api/ratings`, `/api/pending-transactions/*` (review, aggregator-purchase, aggregator-sale), `/api/listings*`, `/api/offers*`, `/api/orders`.

### [collect.html](public/collect.html) — Collector (drop-off app)
Lightweight mobile-web flow for logging a sale without full login (uses phone lookup).
Calls: `GET /api/collectors?phone=`, `GET /api/aggregators`, `POST /api/pending-transactions`, `GET /api/pending-transactions/collector-sales`.

### [collector-dashboard.html](public/collector-dashboard.html) — Collector
Authenticated collector dashboard: earnings, rate aggregators, change PIN.
Calls: `POST /api/auth/login`, `GET /api/collectors/:id`, `PATCH /api/collectors/:id/change-pin` (and implicitly other `/api/collector/*` feeds).

### [collector-passport.html](public/collector-passport.html) — Public
Public-link collector "passport" (stats, material breakdown, verified badge).
Calls: `GET /api/collectors/:id/passport`.

### [converter-dashboard.html](public/converter-dashboard.html) — Converter
Source recycled material, place orders, track arrivals, Discovery, ratings.
Calls: `/api/auth/login`, `/api/converters/:id`, `/api/prices?role=converter`, `/api/orders*`, `/api/pending-transactions/converter-queue`, `/api/pending-transactions/:id/converter-dispatch-decision|converter-arrival`, `/api/ratings`, `/api/listings*`, `/api/offers*`.

### [demo-access.html](public/demo-access.html) — Public
Static tile page linking to pre-filled demo logins. No API calls.

### [index.html](public/index.html) — Public (marketing)
Landing page with market-price widget. Static, no fetch calls in source (price widget reads via inline script but no external API).

### [login.html](public/login.html) — Public
Unified login form — auto-detects phone vs email, routes to correct dashboard via `CirculRoles.ROLE_ROUTES`.
Calls: `POST /api/auth/login`.

### [mockup-rating-system.html](public/mockup-rating-system.html) — ⚠ Dead mockup in public/
Ratings UI mockup checked into the served folder. No fetch calls. Should be moved to `mockups/` or deleted.

### [prices.html](public/prices.html) — ⚠ Legacy buyer portal
Old buyer price portal. The server redirects `/prices → /`, but the file is still on disk and referenced via `/prices.html` direct URL.
Calls: `POST /api/buyers/login` (⚠ **endpoint does not exist** — `buyers` table was dropped in migration 1774500000000), `GET /api/buyers/me/prices` (same).

### [processor-dashboard.html](public/processor-dashboard.html) — Processor
rePATRN-branded processor console. Dispatch queue, supply requirements, outbound sales.
Calls: `/api/auth/login`, `/api/processors/:id`, `/api/transactions?processor_id=`, `/api/pending-transactions/processor-queue`, `/api/pending-transactions/processor-sale(s)`, `/api/converters`, `/api/listings*`, `/api/offers*`.

### [recycler-dashboard.html](public/recycler-dashboard.html) — Recycler
Flakes/pellets tier between processor and converter.
Calls: `/api/auth/login`, `/api/recyclers/:id`, `/api/orders*`, `/api/pending-transactions/recycler-queue`, `/api/pending-transactions/recycler-sale(s)`, `/api/prices?role=recycler`, `/api/converters`, `/api/ratings`, `/api/listings*`, `/api/offers*`.

### [register.html](public/register.html) — Public
Branching registration: collector/aggregator self-serve; paid roles submit access request.
Calls: `POST /api/auth/register`, `POST /api/auth/request-access`.

### [report.html](public/report.html) — Aggregator / analytics
Collector passport viewer + aggregator tx reports.
Calls: `GET /api/transactions?aggregator_id=`, `GET /api/collectors/:id`.

**Page-level flags**
- `mockup-rating-system.html` is dead. (`dashboard.html` was removed; `/dashboard` now 301-redirects to `/aggregator-dashboard.html`.)
- `prices.html` is half-disconnected (still in `public/`, calls a dropped auth table).
- `/code-export.txt` route exposes the raw source file publicly.

---

## 4. Database Schema

Final state after running all migrations in timestamp order.

### `collectors`
Waste collectors (bottom of chain). Phone+PIN auth.
- `id SERIAL PK`
- `first_name`, `last_name VARCHAR(255)`
- `phone VARCHAR(50) UNIQUE`, `pin VARCHAR(10)` (hashed — see scripts/hash-existing-pins.js)
- `region`, `city`, `country VARCHAR DEFAULT 'Ghana'`
- `email VARCHAR(255)`, `password_hash VARCHAR(255)`
- `date_of_birth DATE`, `gender VARCHAR(20)`, `organisation VARCHAR(255)`
- `ghana_card TEXT`, `ghana_card_photo TEXT`
- `is_active BOOLEAN DEFAULT true`, `is_flagged BOOLEAN DEFAULT false`
- `id_verified BOOLEAN`, `id_verified_at TIMESTAMPTZ`, `id_verified_by VARCHAR(255)`, `id_document_type VARCHAR(50)`
- `must_change_pin BOOLEAN DEFAULT false`
- `created_at`, `updated_at TIMESTAMPTZ`

### `aggregators`
Buy from collectors, sell to processors. Phone+PIN auth.
- `id SERIAL PK`
- `name VARCHAR(255)`, `company VARCHAR(255)`
- `phone VARCHAR(50) UNIQUE`, `pin VARCHAR(10)`
- `email VARCHAR(255) UNIQUE`, `password_hash VARCHAR(255)`
- location + id-verification + ghana-card columns (same shape as `collectors`)
- `created_at TIMESTAMPTZ`

### `processors`
Clean & sort. Email+password auth.
- `id SERIAL PK`
- `name`, `company VARCHAR(255) NOT NULL`
- `email VARCHAR(255) UNIQUE NOT NULL`, `password_hash VARCHAR(255)`
- `phone`, `city`, `region`, `country`, `is_active`, `is_flagged`
- `created_at`

### `recyclers`
Flakes & pellets tier — added in migration 1774800000000.
- Same shape as `processors`.

### `converters`
Manufacturers. Email+password auth. Same shape as `processors` (email NOT UNIQUE in migration source — ⚠ inconsistent with other role tables).

### `agents`
Field agents working **under** an aggregator.
- `id SERIAL PK`
- `aggregator_id INTEGER NOT NULL → aggregators(id)`
- `first_name`, `last_name TEXT`, `phone TEXT UNIQUE`, `pin TEXT`
- `ghana_card`, `ghana_card_photo`, `city`, `region`
- `is_active BOOLEAN`, `must_change_pin BOOLEAN`
- `created_at`

### `agent_activity`
Audit trail of agent actions.
- `id`, `agent_id → agents(id)`, `aggregator_id → aggregators(id)`
- `action_type TEXT`, `description TEXT`
- `related_id INTEGER`, `related_type TEXT` (⚠ untyped polymorphic pointer — used by ratings.js `JOIN aa.related_type = 'transaction'` onto `pending_transactions.id`)
- `created_at`

### `admin_users`
Admin email+password accounts (created in 1773379200000_add_buyers_and_admin.js).
- `id`, `email UNIQUE`, `password_hash`, `name`, `created_at`.

### `transactions` — legacy collector→aggregator table
Written by `POST /api/transactions` (legacy) and by `PATCH /api/pending-transactions/:id/review` (when an aggregator accepts a drop-off).
- `id SERIAL PK`
- `collector_id → collectors(id) ON DELETE CASCADE`
- `aggregator_id → aggregators(id) ON DELETE SET NULL`
- `material_type`, `gross_weight_kg`, `net_weight_kg`
- `contamination_deduction_percent DECIMAL(5,2)`, `contamination_types TEXT[]`, `quality_notes TEXT`
- `price_per_kg`, `total_price`
- `lat`, `lng DECIMAL(10,7)`, `notes TEXT`
- `payment_status CHECK IN ('unpaid','payment_sent','paid')`, `payment_method ('cash','mobile_money')`, `payment_reference`, `payment_initiated_at`, `payment_completed_at`
- `transaction_date`, `created_at`, `updated_at`

> **Correction (2026-04-18):** an earlier revision of this audit claimed `transactions.processor_id` / `converter_id` existed as FK-less dead columns added by `1774200000000_add_processor_converter_id_to_transactions`. That was wrong. Both columns are added by `1774100000000_seed_full_supply_chain` (and by the no-op `1774200000000` migration), but `1774500000000_restructure_tiers` then `DROP TABLE IF EXISTS transactions CASCADE` ([migrations/1774500000000_restructure_tiers.js:37](migrations/1774500000000_restructure_tiers.js:37)) and recreates the table with only `collector_id` + `aggregator_id`. The final schema has neither column on `transactions`. Tier-linked lineage lives on `pending_transactions`, which has `processor_id`, `converter_id`, and `recycler_id` as proper FKs from the restructure (`ON DELETE SET NULL`). In-code evidence: [server.js:1899-1901](server.js:1899) explicitly notes *"converter_id does not exist on the transactions table (restructure migration only has collector_id + aggregator_id)"*.

### `pending_transactions` — main supply-chain flow table
Holds **every** stage of the chain (collector_sale → aggregator_purchase → aggregator_sale → processor_sale → recycler_sale) as **independent rows**.
- `id SERIAL PK`
- `transaction_type VARCHAR(50)`: `collector_sale`, `aggregator_purchase`, `aggregator_sale`, `processor_sale`, `recycler_sale`
- `status VARCHAR(50)` CHECK IN (`pending`, `dispatched`, `arrived`, `completed`, `rejected`, `confirmed`, `dispatch_approved`, `dispatch_rejected`, `grade_c_flagged`)
- `collector_id → collectors(id) ON DELETE SET NULL`
- `aggregator_id → aggregators(id) ON DELETE SET NULL`
- `processor_id → processors(id) ON DELETE SET NULL`
- `recycler_id → recyclers(id) ON DELETE SET NULL`
- `converter_id → converters(id) ON DELETE SET NULL`
- `material_type`, `gross_weight_kg`, `net_weight_kg`, `price_per_kg`, `total_price`
- `grade VARCHAR(10)`, `grade_notes TEXT`
- `photos_required`, `photos_submitted BOOLEAN`, `photo_urls TEXT[]`
- `dispatch_approved BOOLEAN`, `dispatch_approved_at`, `dispatch_approved_by_id INTEGER` (⚠ no FK — polymorphic via `dispatch_approved_by_type`)
- `rejection_reason TEXT`, `rejected_at TIMESTAMPTZ`
- `transaction_id INTEGER` (⚠ **no explicit FK** — populated only by the `review` endpoint, points at `transactions.id`)
- `requirement_id → supply_requirements(id)` (manual link)
- `spec_compliance TEXT`
- `source VARCHAR(20) DEFAULT 'direct'` — observed values in code: `'direct'`, `'ussd'`
- `payment_status/method/reference/initiated_at/completed_at`
- `notes`, `created_at`, `updated_at`

### `posted_prices`
Live buy/sell prices by role.
- `id SERIAL PK`
- `poster_type CHECK IN ('aggregator','processor','recycler','converter')`
- `poster_id INTEGER` (⚠ polymorphic — no FK)
- `material_type`
- `price_per_kg_usd`, `price_per_kg_ghs`, `usd_to_ghs_rate DECIMAL(10,4)`
- `city`, `region`, `country DEFAULT 'Ghana'`
- `expires_at`, `is_active`, `posted_at`
- UNIQUE (`poster_type`, `poster_id`, `material_type`) — acts as upsert key.

### `ratings`
Peer ratings between parties, tied to a `pending_transactions` row.
- `id SERIAL PK`
- `transaction_id → pending_transactions(id) ON DELETE SET NULL` (⚠ FK was originally pointed at `transactions` — corrected by 1776700000000_fix_ratings_fk.sql)
- `rater_type`, `rater_id` / `rated_type`, `rated_id` (polymorphic)
- `rating SMALLINT CHECK BETWEEN 1 AND 5`
- `tags TEXT[]`, `notes TEXT`, `rating_direction VARCHAR(30)`
- `created_at`, `window_expires_at TIMESTAMPTZ` (default +30d, see [shared/ratings.js](shared/ratings.js))
- UNIQUE (`transaction_id`, `rater_type`, `rater_id`) WHERE `transaction_id IS NOT NULL`.

### `collector_passports`
Denormalized summary for each collector (refreshed application-side).
- `collector_id PK → collectors(id) ON DELETE CASCADE`
- `total_kg_lifetime`, `total_kg_last_12m`
- `total_earned_ghs`, `total_earned_usd`
- `transaction_count`, `active_since`
- `material_breakdown JSONB`
- `unique_aggregators INTEGER`
- `avg_rating_from_aggregators`, `payment_reliability_score DECIMAL(3,2)`
- `last_updated`

### `ussd_sessions`
- `id`, `session_id VARCHAR UNIQUE`, `phone`, `service_code`
- `collector_id`, `aggregator_id`, `agent_id` (all nullable FKs)
- `action`, `text_input`, `response TEXT`
- `created_at`

### `payments`
- `id`, `transaction_id → transactions(id)`, `collector_id → collectors(id)`
- `aggregator_id INTEGER` (⚠ **no FK**)
- `amount DECIMAL(10,2)`, `currency VARCHAR(3) DEFAULT 'GHS'`
- `phone`, `provider VARCHAR(30)`, `status CHECK IN ('pending','processing','success','failed')`
- `reference`, `provider_reference`, `error_message`
- Created in 1741651200000 but **no server.js writes observed** — appears to be dead after the restructure.

### `orders`
Purchase orders placed by converters / recyclers / aggregators.
- `id`, `buyer_id`, `buyer_role VARCHAR(20)` (polymorphic)
- `material_type`, `target_quantity_kg`, `fulfilled_kg`, `price_per_kg`
- `accepted_colours TEXT`, `excluded_contaminants TEXT`, `max_contamination_pct`
- `supplier_tier VARCHAR(20)`, `supplier_id INTEGER`
- `notes`, `status CHECK IN ('open','accepted','partially_fulfilled','fulfilled','cancelled')`
- `created_at`, `updated_at`

### `listings` (Discovery)
- `id`, `seller_id INTEGER`, `seller_role VARCHAR(20)` (polymorphic)
- `material_type`, `quantity_kg`, `original_qty_kg`, `price_per_kg`
- `location`, `photo_url`
- `status CHECK IN ('active','expired','closed')`
- `expires_at`, `renewal_count`, `created_at`, `updated_at`

### `offers`
- `id`, `listing_id → listings(id) ON DELETE CASCADE`
- `thread_id UUID DEFAULT gen_random_uuid()`
- `buyer_id`, `buyer_role`
- `price_per_kg`, `quantity_kg`, `round INTEGER`, `is_final BOOLEAN`
- `offered_by VARCHAR(20)` (buyer vs seller)
- `status CHECK IN ('pending','accepted','rejected','countered','expired')`
- `parent_offer_id → offers(id)`
- `created_at`, `responded_at`

### `expense_categories`
- `id`, `name TEXT`, `status ('default','pending','approved','rejected')`
- `suggested_by INTEGER`, `rejection_reason`, `reviewed_at`, `created_at`

### `expense_entries`
- `id`, `aggregator_id → aggregators(id)`, `category_id → expense_categories(id)`
- `amount NUMERIC(12,2)`, `note`, `receipt_url` (multer upload)
- `expense_date DATE`, `created_at`

### `supply_requirements`
Processor spec sheets (colours, contamination caps, min quantities).
- `id`, `processor_id → processors(id)`
- `material_type`, `accepted_forms TEXT[]`, `accepted_colours TEXT[]`
- `max_contamination_pct`, `max_moisture_pct`, `min_quantity_kg`
- `price_premium_pct`, `client_reference`, `sorting_notes`
- `is_active`, `created_at`, `updated_at`

### `error_log`
- `id`, `source ('server'|'client')`, `dashboard VARCHAR(30)`
- `error_message TEXT`, `error_stack TEXT`, `url`
- `user_id`, `user_role`, `created_at`

### `_migrations`
Internal runner table (tracks applied files).

### Dropped / deprecated tables (not in final schema)
- `collections` — created in migration 1, renamed to `collectors` in migration 2.
- `pickers` — same.
- `operators` — dropped in **1774500000000_restructure_tiers.js** (CASCADE). Replaced by four role tables plus `admin_users`. **⚠ The shared memory and some older audits still refer to `operators`; no such table exists today.**
- `buyers` — dropped in same migration. `prices.html` still POSTs to `/api/buyers/login`, which 404s.

### Inconsistencies / flags
1. **Duplicate timestamp** — `1776300000000_add_must_change_pin.sql` and `1776300000000_hash_existing_pins.sql` share a prefix. Ordering relies on filename sort.
2. **Polymorphic FKs without enforcement**: `posted_prices.poster_id`, `orders.buyer_id`/`supplier_id`, `listings.seller_id`, `offers.buyer_id`, `agent_activity.related_id`, `dispatch_approved_by_id` — none have referential integrity.
3. **`pending_transactions.transaction_id`** has no FK constraint despite pointing at `transactions(id)`.
4. **`transactions.processor_id`/`converter_id` do not exist on the final schema.** They are added by `1774100000000_seed_full_supply_chain` and by the no-op `1774200000000_add_processor_converter_id_to_transactions`, then dropped by `1774500000000_restructure_tiers` which `DROP TABLE IF EXISTS transactions CASCADE` ([migrations/1774500000000_restructure_tiers.js:37](migrations/1774500000000_restructure_tiers.js:37)) and recreates the table without them. `server.js` treats both columns as absent — confirmed by the comment at [server.js:1899-1901](server.js:1899). Tier lineage past collector→aggregator lives on `pending_transactions`, which has `processor_id`, `converter_id`, and `recycler_id` as proper `ON DELETE SET NULL` FKs.
5. **`converters.email`** is not UNIQUE — breaks the pattern in the other role tables.
6. **`payments` table** is orphaned — no writers observed after the tier restructure.
7. **Ratings FK was initially wrong** and corrected late (1776700000000); any `ratings` rows inserted before that migration may reference invalid IDs.
8. **Cross-migration seeds** (1774100000000 and the demo-seed migrations) assume a demo dataset; rerunning in a different order could fail.

---

## 5. Roles

Canonical definitions live in [shared/roles.js](shared/roles.js). Distinct role values:

| Role | Tier | Auth | Free? | Table | Dashboard | Posts prices | Notes |
|---|---|---|---|---|---|---|---|
| `admin` | 0 | email | — | `admin_users` | `/admin` | no | Platform admin; uses `ADMIN_SECRET` tokens via `requireAdmin`. |
| `operator` | 0 | phone | — | (none) | `/admin` | no | ⚠ Legacy role — operators table was dropped; still referenced in [shared/roles.js](shared/roles.js:97) but unreachable. |
| `collector` | 5 | phone+PIN | yes | `collectors` | `/collector-dashboard` | no | Street-level pickup. |
| `aggregator` | 4 | phone+PIN | yes | `aggregators` | `/aggregator-dashboard` | yes | Can also register collectors and agents. |
| `agent` | 4 | phone+PIN | yes | `agents` | `/agent-dashboard` | no | Works under one aggregator; logs pickups on their behalf. |
| `processor` | 3 | email+password | no ($49) | `processors` | `/processor-dashboard` | yes | rePATRN is the seed processor. |
| `recycler` | 2 | email+password | no ($149) | `recyclers` | `/recycler-dashboard` | yes | Flakes & pellets. |
| `converter` | 1 | email+password | no ($299) | `converters` | `/converter-dashboard` | yes | Final buyer / manufacturer. |

### What each role can access

| | Public pages | Own dashboard | Own `/api/<role>/*` feeds | Pending-tx queue/sale endpoints | Discovery (listings/offers) | Ratings | Orders | Admin APIs |
|---|---|---|---|---|---|---|---|---|
| collector | ✅ | `/collector-dashboard` | `/api/collector/*` | review/purchase *(as counterparty)*, `/collector-sales` | read-only | rate aggregator | — | — |
| aggregator | ✅ | `/aggregator-dashboard` | `/api/aggregator/*` | review, aggregator-purchase, aggregator-sale | ✅ | ✅ | create | — |
| agent | ✅ | `/agent-dashboard` | `/api/agent/*` | log-collection (via agent) | — | ✅ | — | — |
| processor | ✅ | `/processor-dashboard` | `/api/processor/*`, `/api/supply-requirements*` | processor-queue, dispatch-decision, arrival-confirmation, processor-sale(s) | ✅ | ✅ | — | — |
| recycler | ✅ | `/recycler-dashboard` | `/api/recycler/*` | recycler-queue, recycler-dispatch-decision, recycler-arrival, recycler-sale(s) | ✅ | ✅ | create | — |
| converter | ✅ | `/converter-dashboard` | `/api/converter/*` | converter-queue, converter-dispatch-decision, converter-arrival | ✅ | ✅ | create | — |
| admin | ✅ | `/admin` | — | can read all via `/api/admin/transactions` | — | — | — | all `/api/admin/*` |

**Role flags**
- **Multi-role support**: `req.user.roles[]` is read by `hasRole()`, so an operator with both `processor` and `converter` roles passes both middlewares. `highestRole()` (shared/roles.js:197) chooses the landing dashboard.
- **`operator` role is stale**: the `operators` table has been dropped, but the role definition and dashboard route `/admin` are still in code.
- **`agent` is tier 4** (same as aggregator) in [shared/roles.js:111](shared/roles.js:111); this may surprise price-feed logic that ranks tiers.

---

## 6. Traceability flow — one PET batch end-to-end

Goal: follow a single PET batch from collector pickup to converter receipt through the code.

### Stage-by-stage writes

#### Stage 1 — Collector logs a drop-off
Entry points:
- **Direct (web):** `POST /api/pending-transactions` ([server.js:4151](server.js:4151)) from `collect.html`.
- **Legacy direct:** `POST /api/transactions` ([server.js:2081](server.js:2081)) — writes directly to `transactions`, bypassing the queue.
- **USSD:** [server.js:2561](server.js:2561) writes `pending_transactions` with `source='ussd'`, `transaction_type='collector_sale'`, `status='pending'`.

Row written: **`pending_transactions`**
Fields set: `collector_id`, `aggregator_id`, `material_type`, `gross_weight_kg`, `price_per_kg`, `total_price`, `transaction_type='collector_sale'` (or `aggregator_purchase`), `status='pending'`, `source`.

#### Stage 2 — Aggregator reviews and grades
`PATCH /api/pending-transactions/:id/review` ([server.js:4277](server.js:4277)).

- **Writes `transactions`**: `INSERT (collector_id, aggregator_id, material_type, gross_weight_kg, net_weight_kg, contamination_deduction_percent, price_per_kg, total_price, payment_status='unpaid', notes='grade:A|B|C')`.
- **Updates `pending_transactions`**: `SET status='confirmed', grade, grade_notes, transaction_id=<new transactions.id>`. This is the only cross-stage FK in the system.

The aggregator may also run `POST /api/pending-transactions/aggregator-purchase` ([server.js:4322](server.js:4322)) to manually record a pickup they received — this writes a **fresh** `pending_transactions` row with no back-link to the collector row.

#### Stage 3 — Aggregator dispatches to processor
`POST /api/pending-transactions/aggregator-sale` ([server.js:4340](server.js:4340)).

Row written: **new `pending_transactions`**
Fields set: `transaction_type='aggregator_sale'`, `status='pending'`, `aggregator_id`, `processor_id` (or `converter_id`), `material_type`, `gross_weight_kg`, `price_per_kg`, `total_price`, `photos_required/submitted`, `photo_urls`.

**⚠ No back-pointer to Stage 1/2.** No `parent_transaction_id`, no `batch_id`, no `source_pending_transaction_id`. The only implicit link is `aggregator_id + material_type + time window`.

#### Stage 4 — Processor receives
- `POST /api/pending-transactions/:id/dispatch-decision` ([server.js:4375](server.js:4375)) — `UPDATE … SET status='dispatch_approved', dispatch_approved=true, dispatch_approved_by_id=<processor.id>, dispatch_approved_by_type='processor'`.
- `POST /api/pending-transactions/:id/arrival-confirmation` ([server.js:4407](server.js:4407)) — `UPDATE … SET status='arrived' OR 'grade_c_flagged', grade, gross_weight_kg, total_price, rejection_reason` (weight can be re-graded down here).

Same row is mutated in place. No new row.

#### Stage 5 — Processor sells onward
`POST /api/pending-transactions/processor-sale` ([server.js:4430](server.js:4430)).

Row written: **new `pending_transactions`**, `transaction_type='processor_sale'`, `processor_id` + (`recycler_id` XOR `converter_id`). Again, **no parent link**.

#### Stage 6 — Recycler receives & resells (optional tier)
- Arrival: `POST /api/pending-transactions/:id/recycler-dispatch-decision` ([server.js:4487](server.js:4487)) + `.../recycler-arrival` ([server.js:4516](server.js:4516)). Same-row UPDATE.
- Resell: `POST /api/pending-transactions/recycler-sale` ([server.js:4541](server.js:4541)) — new row, `transaction_type='recycler_sale'`, `recycler_id` + `converter_id`.

#### Stage 7 — Converter receives
- `POST /api/pending-transactions/:id/converter-dispatch-decision` ([server.js:4591](server.js:4591))
- `POST /api/pending-transactions/:id/converter-arrival` ([server.js:4623](server.js:4623))

Same-row UPDATE. No new row. Chain ends.

### Summary of chain-of-custody links

| From → To | Mechanism | Strength |
|---|---|---|
| collector drop-off (pending_transactions.collector_sale) → legacy transaction (transactions) | `pending_transactions.transaction_id` set by `review` | ✅ strong (but **no FK constraint**) |
| collector drop-off → aggregator_purchase | — | ❌ none; separate rows |
| aggregator_purchase → aggregator_sale | — | ⚠ implicit: same `aggregator_id` + `material_type` + time |
| aggregator_sale → processor_sale | — | ⚠ implicit: same `processor_id` + `material_type` |
| processor_sale → recycler_sale | — | ⚠ implicit: same `recycler_id` + `material_type` |
| recycler_sale → converter_arrival | — | same row (mutated) |

### Chain-of-custody assessment — **broken end-to-end**

A single PET batch is represented as **multiple disconnected rows** in `pending_transactions`. There is:
- **No `batch_id`, `parent_transaction_id`, or `chain_id` column** on `pending_transactions`.
- **Only one cross-row pointer**: `pending_transactions.transaction_id → transactions.id`, set only by the aggregator review endpoint. It does not survive past Stage 2.
- **`requirement_id`** points at a processor spec sheet, not at an upstream batch — useful for matching processor-side intent, not for provenance.
- **`source`** distinguishes USSD vs direct entry but says nothing about lineage.

The intended-traceability endpoint `GET /api/reports/product-journey/:transaction_id` ([server.js:5410](server.js:5410)) only joins `transactions` → `collectors` → `aggregators`; it **stops at the aggregator** and cannot follow the batch into processor/recycler/converter stages.

### What a real chain-of-custody would need
To make a PET batch followable from Stage 1 → Stage 7:
1. Add `batch_id UUID` to `pending_transactions`, set once at Stage 1 and propagated on each downstream insert.
2. Add `parent_pending_tx_id INTEGER → pending_transactions(id)` for explicit prior-step linkage.
3. Auto-link `requirement_id` on Stage 5 inserts (processor_sale ↔ processor's active spec for that material).
4. Extend `product-journey` to recurse through `pending_transactions` by `batch_id` or `parent_pending_tx_id` instead of reading only `transactions`.

None of these exist today. The backend can **log** every stage, but it cannot **prove** that a given converter shipment originated from a given collector's drop-off.

---

## Appendix — inconsistencies flagged during this audit

1. Two public POST endpoints write data without `requireAuth`: `POST /api/prices` ([server.js:5312](server.js:5312)) and `POST /api/processors/:id/prices` ([server.js:1690](server.js:1690)).
2. `GET /code-export.txt` serves the raw `server.js` text publicly.
3. `public/prices.html` calls `/api/buyers/login` and `/api/buyers/me/prices` — the `buyers` table was dropped and these endpoints do not exist.
4. `public/mockup-rating-system.html` is dead. (`public/dashboard.html` was removed in the dead-code cleanup; `/dashboard` now 301-redirects to `/aggregator-dashboard.html`.)
5. `transactions.processor_id` / `converter_id` are **not in the final schema** — added by the `1774100000000` and `1774200000000` migrations, then dropped (with the rest of the `transactions` table) by `1774500000000_restructure_tiers` CASCADE. Lineage past collector→aggregator lives on `pending_transactions`. Evidence: [server.js:1899-1901](server.js:1899).
6. `payments` table exists but is orphaned.
7. `operators` role still defined in [shared/roles.js:97](shared/roles.js:97) even though the `operators` table was dropped.
8. `converters.email` is not UNIQUE while every sibling role table is.
9. Duplicate migration timestamp `1776300000000` on two `.sql` files; ordering is filename-dependent.
10. Polymorphic `*_id` columns (see §4) have no FK enforcement.
11. `pending_transactions.transaction_id` has no FK to `transactions.id` despite being used that way.
12. `ratings.transaction_id` initially pointed at `transactions`; only fixed in 1776700000000. Pre-fix rows may be mislinked.
13. 40+ loose audit/prompt markdown files at repo root — recommend an `archive/` folder.
