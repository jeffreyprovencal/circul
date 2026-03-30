# Circul — Project Knowledge

## What Circul Is

Circul is a waste-to-resource operating system for Africa's plastic recycling supply chain. It connects 5 tiers — Collector → Aggregator → Processor → Recycler → Converter — in a digital marketplace where materials flow upstream and payments flow downstream.

**Tech stack:** Node.js (Express), PostgreSQL, vanilla HTML/CSS/JS. Deployed on Render via `render.yaml`.

---

## Deploy Pipeline

```
jeffreyprovencal/circul (Jeff's fork)
        ↓  (Polsia syncs from here)
Polsia-Inc/circul (private)
        ↓  (Render auto-deploy)
circul.polsia.app (live)
```

Polsia has a script `scripts/fetch-upstream-dashboards.js` that defines which files auto-sync. When adding new files, update this list in the Polsia repo before triggering deploy.

### Polsia Sync Prompt Template

When syncing to Polsia, use this format:

```
Please sync `jeffreyprovencal/circul` main to `Polsia-Inc/circul`.

Before triggering the Render deploy, add [NEW FILES] to the file list in `scripts/fetch-upstream-dashboards.js` so they auto-sync on future builds. The current list has N files — it should become N+X:

- [list all files including new ones, mark new with ← new]

Changes being synced (PR #N, merged to main):
1. [numbered list of changes with affected files]

After deploy, I'll verify at circul.polsia.app.
```

### CDN Cache Warning

Polsia's `fetch-upstream-dashboards.js` fetches files from `raw.githubusercontent.com`. GitHub's raw CDN caches aggressively — sometimes 5+ minutes after a merge. If a sync runs too soon after merging a PR, the CDN can serve stale pre-merge file contents.

**Mitigation:** The fetch URLs in `fetch-upstream-dashboards.js` should include a cache-busting query parameter: `?t=${Date.now()}`. This forces GitHub to serve the actual latest content. Polsia was asked to add this permanently (March 2026).

**After every sync:** Verify on circul.polsia.app that the deployed code reflects the expected changes. Do not assume a sync succeeded just because the file sizes changed.

---

## Architecture

### Role System — Single Source of Truth

`shared/roles.js` is a UMD module (works in Node.js via `require()` and browser via `<script src>`). It defines ALL role metadata:

| Role | Tier | Auth | Price | Dashboard |
|------|------|------|-------|-----------|
| Converter | 1 (highest) | email+password | $299/mo | /converter-dashboard |
| Recycler | 2 | email+password | $149/mo | /recycler-dashboard |
| Processor | 3 | email+password | $49/mo | /processor-dashboard |
| Aggregator | 4 | phone+PIN | Free | /aggregator-dashboard |
| Collector | 5 (lowest) | phone+PIN | Free | /collector-dashboard |

**Key exports:** `ROLES`, `TIER_HIERARCHY`, `ROLE_ROUTES`, `TABLE_MAP`, `PAID_ROLES`, `FREE_ROLES`, `getPosterTypes()`, `highestRole()`, `dashboardFor()`, `pillStyle()`

**When adding or modifying roles, update `shared/roles.js` FIRST**, then update server.js, migrations, and dashboards to match.

### Authentication

- **localStorage keys:** `circul_token`, `circul_user`
- **Token format:** `base64(JSON_payload).base64(HMAC-SHA256_signature)` — custom JWT-like
- **Two auth types:** phone+PIN (free tiers), email+password (paid tiers)
- **Dual-role users:** Login returns `roles: ["processor", "converter"]` — login.html auto-routes to highest tier using `TIER_HIERARCHY`

### Dashboard Login Overlay Pattern

Every paid dashboard has an inline login overlay (`<div class="login-overlay" id="loginOverlay">`). On page load, `checkAuth()` checks localStorage for `circul_user`. If found with correct role, it calls `showDashboard()` which adds class `hidden`. The CSS rule `.login-overlay.hidden { display: none }` is in `shared.css`.

### Shared CSS

`public/shared.css` contains: Google Fonts import (Plus Jakarta Sans), CSS custom properties (colors, backgrounds, borders, role pill colors, radii, shadows), reset/base rules, typography, and the login overlay `.hidden` rule. All 14 HTML files link to it. Page-specific styles remain in each file's `<style>` block. Recycler dashboard overrides `:root` with purple theme, converter with pink.

### Expense Tracking Architecture

- **DB tables**: `expense_categories` (with status workflow: default → approved, or pending → approved/rejected), `expense_entries` (with receipt_url, expense_date)
- **Receipt storage**: `public/uploads/receipts/` (gitignored), served as static files
- **Category approval flow**: aggregator suggests → admin sees in admin.html → approves (optionally renames) or rejects (with mandatory reason)
- **Offline queue**: localStorage key `circul_expense_queue`, syncs on `window.online` event
- **PDF export**: client-side via jsPDF CDN, generates branded A4 statement

---

## Demo Accounts

| Role | Credentials | Login Type |
|------|-------------|------------|
| Collector | 0241000001 / 0000 | Phone + PIN |
| Aggregator | 0300000002 / 2222 | Phone + PIN |
| Processor | jeffrey@circul.demo / demo1234 | Email + Password |
| Recycler | poly@circul.demo / demo1234 | Email + Password |
| Converter | miniplast@circul.demo / demo1234 | Email + Password |

miniplast@circul.demo is a dual-role account (processor + converter) — should auto-route to converter dashboard (highest tier).

---

## File Structure

```
server.js              — All API routes + auth (1789 lines)
shared/roles.js        — Role definitions (single source of truth)
public/shared.css      — Global design system
public/login.html      — Unified login (phone+PIN or email+password)
public/register.html   — Account creation
public/index.html      — Landing page
public/*-dashboard.html — 5 role-specific dashboards + dashboard.html (generic)
public/admin.html      — Admin panel
public/collect.html, prices.html, report.html, demo-access.html — Utility pages
migrations/            — Timestamped database migrations
scripts/               — SQL seed scripts
```

---

## Testing Rules

**CRITICAL — read this before every review or verification task:**

1. **NEVER bypass UI with JavaScript when testing.** Do not use `document.querySelector('.login-overlay').style.display = 'none'` or `fetch('/api/auth/login')` to skip login flows. Always test the real user experience — click through forms, submit buttons, follow redirects.

2. **Test the full flow, not just the endpoint.** A successful API response (200) does not mean the feature works. The login API can return 200 while the dashboard overlay stays broken. Always verify the VISUAL result.

3. **When reviewing code changes, check EXISTING code that assumes a fixed set of roles.** New roles break hardcoded role lists, route maps, and if/else chains. Search the entire codebase for hardcoded role references.

4. **After any CSS change, verify every dashboard visually on the live site.** CSS extractions and refactors can silently break layouts. Take screenshots of all 5 dashboards + landing page + login page.

5. **After merge, always verify on circul.polsia.app** — not just in the local repo. The deploy pipeline can introduce its own issues.

### Pre-Commit Self-Audit Checklist

**Run these checks before every commit that touches dashboard HTML files or server.js routes.**

#### DOM ID Consistency
For each dashboard HTML file changed:
- Extract every `document.getElementById('...')` and `document.querySelector('...')` call from the `<script>` block.
- Verify each referenced ID/selector exists as an actual element in the HTML portion of the same file.
- Every DOM write (`el.textContent = ...`, `el.innerHTML = ...`) must have a null guard or the element must be guaranteed to exist.
- List any mismatches. Fix before committing.

#### SQL Column Audit
For each SQL query added or changed in server.js:
- Verify every column name exists in the corresponding table. Cross-reference against `migrations/` files.
- Verify every GROUP BY is unambiguous — use positional (`GROUP BY 1, 2`) or fully qualified (`table.column`), never bare column aliases that could clash with table columns.
- Verify JOINs reference valid foreign keys.
- Verify WHERE clauses handle NULL values.

#### Route Completeness
For each dashboard HTML file changed:
- Verify every `fetch()` or `apiFetch()` URL in the `<script>` block has a corresponding route in server.js.
- If a frontend calls an endpoint that doesn't exist, either build the route or remove the frontend call.

#### UI Consistency
After any HTML/CSS changes to dashboard files:
- All 5 dashboard headers must match: linked Circul logo, UPPERCASE tier pill, user name, identifier code, "← Home" link, "Log out" button.
- No dashboard should contain the supply chain tier bar (Collector → Aggregator → ... → Converter).
- Login overlay style should be consistent across dashboards.

#### 503 Retry
Every dashboard's `apiFetch` function (or equivalent) must include retry logic for 503 responses: 1 retry after 1-second delay.

---

## Known Issues & Patterns

- **`/api/collector/prices` returns 500** — fix is in PR #19 (pending deploy). Uses a ratings subquery to replace non-existent `aggregators.average_rating` column.
- **Collector passport section shows `COL-XXXXXX` format but header shows `C-XXXX`.** Should be consistent — use `C-XXXX` everywhere.
- **Aggregator dashboard has a section labeled "Batch sales history"** — should be renamed to "Sales history".
- **Some aggregator API calls return 503 intermittently** — likely Render cold start or DB connection pool issue. All dashboards now have 503 retry logic.

---

## PR & Commit Conventions

- **Branch naming:** `feat/`, `fix/`, `refactor/`, `style/` prefixes
- **Safe refactoring:** Use two-commit approach — Commit 1 is purely additive (can't break), Commit 2 removes old code (independently revertable)
- **Merge command:** `gh pr merge N --merge`
- **After merge:** Always provide Polsia sync prompt using the template above

---

## Lessons Learned

### Cowork → Claude Code Prompt Pipeline (Batch 3, March 2026)

The most productive workflow for complex multi-file changes:

1. **Cowork writes surgical markdown prompts** — each prompt is a single commit's worth of work with explicit "read first" instructions, exact file paths, code snippets adapted to the project's patterns, and a pre-written commit message
2. **User pastes each prompt into Claude Code** — Code executes with full repo access, adapts the prompt to reality (different variable names, table structures, etc.)
3. **User reports back the commit hash + any adaptations** — Cowork adjusts subsequent prompts based on what actually happened
4. **Batch syncs to Polsia** — instead of syncing after every commit, batch all changes and sync once (saves Render deploy credits)

Key principles that made prompts reliable:
- **Always start with "Read first"** — list the exact files Claude Code must read before making changes. Never assume column names, variable names, or patterns.
- **Follow existing patterns** — prompts should say "follow the same pattern as X" rather than prescribing ES6 when the codebase uses ES5, or `apiFetch` when the file uses plain `fetch`.
- **One commit per prompt** — keeps changes reviewable and rollback-friendly.
- **Include verification steps** — `node -c server.js`, `grep` for orphaned references, etc.

### Table Naming Mismatch: transactions vs pending_transactions (March 2026)

The two transaction tables use **different column naming conventions**:

| Role       | `transactions` column  | `pending_transactions` column |
|------------|----------------------|-------------------------------|
| Collector  | `collector_id`       | `collector_id`                |
| Aggregator | `aggregator_id`      | `aggregator_operator_id`      |
| Processor  | `processor_id`       | `processor_buyer_id`          |
| Converter  | `converter_id`       | `converter_buyer_id`          |

This caused the "converter_id does not exist" bug. Any future query touching `pending_transactions` must use the `_operator_id` / `_buyer_id` suffixed names. **Do not assume column names are the same across tables.**

### FK References: aggregators vs operators (March 2026)

Aggregators live in the `aggregators` table, not `operators`. When creating foreign keys that reference an aggregator, use `REFERENCES aggregators(id)` — not `REFERENCES operators(id)`. The `operators` table is for processors/converters/recyclers.

### CSS Centralization Strategy (Phase A–C, March 2026)

Successful 3-phase approach to eliminating inline styles:
- **Phase A**: Extract component classes (rating modal, toast, nav pills, status pills, submit buttons)
- **Phase B**: Extract card/form/badge patterns (delivery cards, order cards, inline forms, tab panels)
- **Phase C**: Replace JS inline styles with class toggles (`.hidden` class, `.msg-error`/`.msg-success`, `.fade-out`)

Total impact: +136 shared CSS lines, -163 duplicate lines across dashboards, ~144 JS `el.style.*` calls replaced with `classList.toggle`.

The `.hidden` pattern (`el.classList.add('hidden')` / `el.classList.remove('hidden')`) is now the standard for show/hide across all files. Never use `el.style.display = 'none'` / `el.style.display = ''`.

---

## Backlog

### ~~CSS Design System Refactor~~ ✓ Complete (March 2026)

Completed in Phases A–C: shared.css now contains all component classes, card/form/badge patterns, and utility classes. JS inline styles replaced with class toggles across all 5 dashboards and 5 small files.

### Expense Tracking — Batch 3 (March 2026) ✓ Complete

Backend: `expense_categories` + `expense_entries` tables, 8 API endpoints (categories CRUD + suggest/approve/reject, entries CRUD with photo upload), P&L stats enhancement with MoM trends and margin %.

Frontend: 3-level collapsible P&L accordion, log expense form (date picker, photo receipt, category suggest), admin approval panel in admin.html, offline localStorage queue with auto-sync, PDF export via jsPDF.

Default expense categories: Transportation, Fuel, Storage, Labour, Equipment, Maintenance, Mobile money fees.

### Discovery Marketplace — Batch 3 (March 2026) ✓ Complete

Marketplace feature allowing collectors and aggregators to advertise available materials to tier-above buyers. Buyers browse listings filtered by material type, make offers, and negotiate price/quantity through a max 2-round thread. Accepted offers automatically create `pending_transactions` with `source = 'discovery'`.

#### Data Model

**`listings`** — Material advertisements posted by sellers:
- `id`, `seller_id`, `seller_role` (collector or aggregator), `material_type` (PET, HDPE, LDPE, PP, Other)
- `quantity_kg`, `price_per_kg` (GH₵), `description`
- `status` (active, closed, expired), `expires_at` (7 days from creation), `created_at`

**`offers`** — Buyer bids on listings:
- `id`, `listing_id` (FK → listings), `buyer_id`, `buyer_role`
- `price_per_kg`, `quantity_kg`, `round` (1 or 2), `is_counter` (boolean)
- `status` (pending, accepted, rejected, expired, countered), `parent_offer_id` (FK → offers, for counter chains)
- `created_at`

#### Business Rules

- **Tier-up visibility:** Collectors' listings visible to aggregators; aggregators' listings visible to processors, recyclers, and converters
- **Minimum thresholds:** 30 kg for collectors, 500 kg for aggregators
- **7-day expiry** with one-click renewal (resets `expires_at` to +7 days)
- **2-round negotiation:** Round 1 is the initial offer; round 2 is marked "Final Offer" — no further counters allowed
- **Partial fills:** Offer quantity can be less than listing quantity
- **Accepted offer → transaction:** Creates a `pending_transaction` row with `source = 'discovery'`, mapping buyer/seller IDs to the correct `pending_transactions` columns

#### API Endpoints (14 total)

**Listings (8):**

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/listings` | Create a new listing (seller only) |
| GET | `/api/listings` | Browse active listings (buyers see tier-appropriate listings) |
| GET | `/api/listings/mine` | Get current user's own listings |
| GET | `/api/listings/:id` | Get single listing details |
| PATCH | `/api/listings/:id/renew` | Renew listing for another 7 days |
| PATCH | `/api/listings/:id/close` | Close listing (mark as closed) |
| DELETE | `/api/listings/:id` | Delete own listing |
| GET | `/api/listings/:id/offers` | Get all offers on a listing (seller only, includes buyer name) |

**Offers (6):**

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/listings/:id/offers` | Submit an offer on a listing |
| GET | `/api/offers/mine` | Get current user's sent offers |
| GET | `/api/offers/:id/thread` | Get full offer thread (original + counters) |
| POST | `/api/offers/:id/accept` | Accept an offer (creates pending_transaction) |
| POST | `/api/offers/:id/reject` | Reject an offer |
| POST | `/api/offers/:id/counter` | Counter an offer (increments round, max 2) |

#### Dashboard Integration

- **Collectors:** "My Listings" section — create/renew/close listings, view incoming offers with accept/reject/counter
- **Aggregators:** "My Listings" section (same as collector) + "Discover Materials" section — browse collector listings, submit offers, track sent offers
- **Processors, Recyclers, Converters:** "Discover Materials" section — browse aggregator listings, submit offers, track sent offers

#### Cron Jobs (hourly via `runDiscoveryCrons()`)

1. **Offer expiry:** Expire pending offers older than 48 hours
2. **Listing expiry:** Expire active listings past `expires_at`, but protect listings that have pending offers (NOT IN subquery)
3. **Renewal reminders:** Log listings expiring within 24 hours (+ in-dashboard renewal banners)

#### Toast Notification System

Discovery actions use a dedicated `showDiscToast(type, title, message)` function with rich formatting (icon + title + body). Types: `success` (green), `warning` (orange), `info` (blue). CSS class prefix `disc-toast-*` avoids collision with existing dashboard toast systems.

#### Phase 2 Notes

- Waste source tagging (municipal, industrial, commercial)
- Location-based discovery with GPS coordinates and radius search
- Push notifications for new listings matching buyer preferences
- Discovery analytics dashboard (conversion rates, average negotiation rounds, fill rates)

### Revenue Breakdown in P&L (Future)

The P&L accordion's Revenue row currently shows a "coming soon" placeholder when expanded. Needs: sub-breakdown by buyer type (sales to processors vs sales to converters) with drill-down into individual sale transactions. Requires enhancing the stats API to return revenue grouped by buyer role.

### Offline Support Expansion (Future)

Currently only expense entries are queued offline. Consider extending to: purchase registration, batch submission, price posting. Would require a more general offline queue architecture and potentially a service worker.

### Batch 6 — Server Hotfixes + UX Polish (March 30, 2026)

Comprehensive live audit of circul.polsia.app found 20 bugs (8 P0, 5 P1, 7 P2) after Batch 5 deploy. Root cause of most P0 issues: SQL column name mismatches in pending_transactions queries — the table uses aggregator_operator_id, processor_buyer_id, converter_buyer_id but many queries reference the simpler names (aggregator_id, processor_id, converter_id).

**P0 Critical (blocks core flows):**
- BUG-01: POST /api/pending-transactions returns 500 for collections/purchases — INSERT query uses wrong column names
- BUG-02: POST /api/pending-transactions validation requires collector_id + aggregator_id for ALL transaction types including sales — needs per-type validation
- BUG-03: GET /api/pending-transactions returns 500 with aggregator_id param — WHERE clause column mismatch
- BUG-04: GET /api/pending-transactions/aggregator-sales returns 400 — expects aggregator_id but frontend sends aggregator_operator_id
- BUG-05: GET /api/pending-transactions/aggregator-purchase returns 404 — route missing or misnamed
- BUG-06: POST /api/aggregators/:id/expenses returns 500 — INSERT references nonexistent note column
- BUG-07: GET /api/collector/pending-purchases returns 500 — SQL column error
- BUG-08: GET /api/ratings/pending returns 500 — query still has a column error despite Prompt 2 fix

**P1 High (feature broken):**
- BUG-09: Rating pill click does nothing — recycler/converter have no ratingModal element or rating form; pill stays hidden on all dashboards because BUG-08 prevents count
- BUG-10: Aggregator P&L shows "offline" — stats endpoint works (200) but P&L rendering JS incorrectly shows offline message
- BUG-11: Processor volume stats show dashes — stat elements exist, JS writes to them, but values don't populate
- BUG-12: GET /api/ratings/operator returns 404 — route missing or uses different params
- BUG-13: GET /api/aggregators/:id/expense-categories returns 404 — frontend calls wrong route (working route is /api/expense-categories?aggregator_id=:id)

**P2 Medium (UX/feature requests from Miniplast demo):**
- BUG-14: Collector "My Earnings" needs month/year pill navigation
- BUG-15: Collector "Who's buying near you" should autopopulate price on aggregator+material selection
- BUG-16: Collector listing location should be a dropdown from /api/listings/locations, not free text
- BUG-17: Aggregator "Discover Materials" can't fetch specific collector listings
- BUG-18: Processor listing visibility — clarify that processors see aggregator listings only (tier-up rule)
- BUG-19: Collector passport view returns error — /api/collector/me returns 200 but empty data
- BUG-20: Aggregator "Register a collector" feature missing

**Fix Plan (5 prompts):**
- Prompt 1: Fix pending-transactions POST + GET (BUG-01 through 05) — correct all column names in INSERT/WHERE, add per-type validation, fix param names
- Prompt 2: Fix ratings, expenses, collector endpoints (BUG-06 through 08, 12, 13)
- Prompt 3: Fix frontend data binding (BUG-09 through 11) — rating pill targets, P&L offline, processor stats
- Prompt 4: Collector UX improvements (BUG-14 through 16, 19)
- Prompt 5: Aggregator/Processor UX (BUG-17, 18, 20)

**Endpoint Status (tested March 30, 2026):**
Working (200): /api/auth/login (all roles), /api/prices, /api/transactions, /api/listings, /api/listings/mine, /api/listings/locations, /api/offers/mine, /api/collectors/4/passport, /api/collector/me, /api/collector/stats, /api/collector/pl, /api/collector/top-buyers, /api/collector/transactions, /api/collector/prices, /api/aggregators/9/stats, /api/aggregators/9/expenses (GET), /api/expense-categories, /api/processors/1
Broken: POST /api/pending-transactions (500/400), GET /api/pending-transactions (500), /api/pending-transactions/aggregator-sales (400), /api/pending-transactions/aggregator-purchase (404), POST /api/aggregators/:id/expenses (500), /api/collector/pending-purchases (500), /api/ratings/pending (500), /api/ratings/operator (404), /api/aggregators/:id/expense-categories (404)
