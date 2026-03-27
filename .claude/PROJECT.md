# Circul — Project Knowledge

## What Circul Is

Circul is a waste-to-resource operating system for Africa's plastic recycling supply chain. It connects 5 tiers — Collector → Aggregator → Processor → Recycler → Converter — in a digital marketplace where materials flow upstream and payments flow downstream.

Tech stack: Node.js (Express), PostgreSQL, vanilla HTML/CSS/JS. Deployed on Render via `render.yaml`.

## Deploy Pipeline

```
jeffreyprovencal/circul (Jeff's fork)
  ↓ (Polsia syncs from here)
Polsia-Inc/circul (private)
  ↓ (Render auto-deploy)
circul.polsia.app (live)
```

Polsia has a script `scripts/fetch-upstream-dashboards.js` that defines **which files auto-sync**. When adding new files, update this list in the Polsia repo before triggering deploy.

**CRITICAL:** Any new file added to `jeffreyprovencal/circul` (including files outside `public/`, such as `shared/roles.js`) MUST be added to the Polsia sync script. Forgetting this step will cause silent 404s on deploy. See Lessons Learned: "shared/roles.js sync miss" below.

### Polsia Sync Prompt Template

When syncing to Polsia, use this format:

> Please sync `jeffreyprovencal/circul` main to `Polsia-Inc/circul`. Before triggering the Render deploy, add [NEW FILES] to the file list in `scripts/fetch-upstream-dashboards.js` so they auto-sync on future builds.
>
> The current list has N files — it should become N+X:
> - [list all files including new ones, mark new with ← new]
>
> Changes being synced (PR #N, merged to main):
> 1. [numbered list of changes with affected files]
>
> After deploy, I'll verify at circul.polsia.app.

### CDN Cache Warning

Polsia's `fetch-upstream-dashboards.js` fetches files from `raw.githubusercontent.com`. GitHub's raw CDN caches aggressively — sometimes 5+ minutes after a merge. If a sync runs too soon after merging a PR, the CDN can serve stale pre-merge file contents.

**Mitigation:** The fetch URLs in `fetch-upstream-dashboards.js` should include a cache-busting query parameter: `?t=${Date.now()}`. This forces GitHub to serve the actual latest content. Polsia was asked to add this permanently (March 2026).

**After every sync:** Verify on circul.polsia.app that the deployed code reflects the expected changes. Do not assume a sync succeeded just because the file sizes changed.

---

## Architecture

### Role System — Single Source of Truth

`shared/roles.js` is a UMD module (works in Node.js via `require()` and browser via `<script src>`). It defines ALL role metadata:

| Role       | Tier         | Auth            | Price    | Dashboard              |
|------------|-------------|-----------------|----------|------------------------|
| Converter  | 1 (highest) | email+password  | $299/mo  | /converter-dashboard   |
| Recycler   | 2           | email+password  | $149/mo  | /recycler-dashboard    |
| Processor  | 3           | email+password  | $49/mo   | /processor-dashboard   |
| Aggregator | 4           | phone+PIN       | Free     | /aggregator-dashboard  |
| Collector  | 5 (lowest)  | phone+PIN       | Free     | /collector-dashboard   |

Key exports: `ROLES`, `TIER_HIERARCHY`, `ROLE_ROUTES`, `TABLE_MAP`, `PAID_ROLES`, `FREE_ROLES`, `getPosterTypes()`, `highestRole()`, `dashboardFor()`, `pillStyle()`

**When adding or modifying roles, update `shared/roles.js` FIRST**, then update server.js, migrations, and dashboards to match.

### Authentication

localStorage keys: `circul_token`, `circul_user`
Token format: `base64(JSON_payload).base64(HMAC-SHA256_signature)` — custom JWT-like
Two auth types: phone+PIN (free tiers), email+password (paid tiers)
Dual-role users: Login returns `roles: ["processor", "converter"]` — login.html auto-routes to highest tier using `TIER_HIERARCHY`

### Dashboard Login Overlay Pattern

Every paid dashboard has an inline login overlay (`<div class="login-overlay" id="loginOverlay">`). On page load, `checkAuth()` checks localStorage for `circul_user`. If found with correct role, it calls `showDashboard()` which adds class `hidden`. The CSS rule `.login-overlay.hidden { display: none }` is in `shared.css`.

### Shared CSS — Design System v3

`public/shared.css` is the full design system, linked by all 14 HTML files. Page-specific styles remain in each file's `<style>` block. Recycler dashboard overrides `:root` with purple theme, converter with pink.

**CSS custom properties (`:root`):**

| Variable          | Value                          | Purpose                    |
|-------------------|--------------------------------|----------------------------|
| `--font`          | Plus Jakarta Sans + fallbacks  | Global font stack          |
| `--accent`        | `#00e676`                      | Primary green              |
| `--accent-hover`  | `#33eb8e`                      | Hover state                |
| `--accent-muted`  | `rgba(0,230,118,0.1)`         | Subtle accent backgrounds  |
| `--bg`            | `#0a1a0f`                      | Page background            |
| `--surface`       | `#0d2818`                      | Card/section background    |
| `--surface-alt`   | `#0c1e0e`                      | Alternate surface          |
| `--nav-bg`        | `rgba(10,26,15,0.95)`         | Sticky nav background      |
| `--text-1`        | `#e8f5e9`                      | Primary text               |
| `--text-2`        | `#9ab8a0`                      | Secondary text             |
| `--text-3`        | `#7a9a7a`                      | Muted text / placeholders  |
| `--border`        | `#1a3a1a`                      | Default border             |
| `--border-nav`    | `#1a2e1a`                      | Nav border                 |
| `--r-sm`          | `8px`                          | Small border radius        |
| `--r-md`          | `12px`                         | Medium border radius       |
| `--gap`           | `24px`                         | Default spacing            |

**Component classes in shared.css:** nav (sticky header with logo, pill, name, id, links, logout button), hero (page header with name + meta), stat cards (`.stats-3`, `.stats-4` grids), section cards, tabs, two-column layouts, tables (`.table-wrap` for mobile scroll), pills (material-type pills: `.pill-pet`, `.pill-hdpe`, `.pill-ldpe`, `.pill-pp`; status pills: `.pill-paid`, `.pill-sent`, `.pill-pending`), buttons (`.btn`, `.btn-green`, `.btn-primary`), price cards (with inline edit inputs), forms (`.form-grid`, `.form-input`, `.form-select`), earnings/P&L grids, empty states, passport section, rate cards, mockup banner.

**Responsive breakpoint:** `@media(max-width:768px)` — collapses grids to single column, hides nav pill/name/id, reduces padding.

**Critical rule:** `.login-overlay.hidden { display: none }` — at the bottom of shared.css. Without this, paid dashboard overlays won't hide after login.

**Note:** Google Fonts (Plus Jakarta Sans) is loaded via `<link>` tags in each HTML file, not via `@import` in shared.css.

### Demo Accounts

| Role       | Credentials                     | Login Type         |
|------------|--------------------------------|--------------------|
| Collector  | `0241000001` / `0000`          | Phone + PIN        |
| Aggregator | `0300000002` / `2222`          | Phone + PIN        |
| Processor  | `jeffrey@circul.demo` / `demo1234`  | Email + Password  |
| Recycler   | `poly@circul.demo` / `demo1234`     | Email + Password  |
| Converter  | `miniplast@circul.demo` / `demo1234`| Email + Password  |

`miniplast@circul.demo` is a dual-role account (processor + converter) — should auto-route to converter dashboard (highest tier).

---

## File Structure

```
server.js                          — All API routes + auth
shared/roles.js                    — Role definitions (single source of truth)
public/shared.css                  — Global design system v3
public/login.html                  — Unified login (phone+PIN or email+password)
public/register.html               — Account creation
public/index.html                  — Landing page
public/collector-dashboard.html    — Collector dashboard
public/aggregator-dashboard.html   — Aggregator dashboard
public/processor-dashboard.html    — Processor dashboard
public/recycler-dashboard.html     — Recycler dashboard
public/converter-dashboard.html    — Converter dashboard
public/dashboard.html              — Generic (redirects to aggregator-dashboard)
public/admin.html                  — Admin panel
public/collect.html                — Collection logging
public/prices.html                 — Price board
public/report.html                 — Report page
public/demo-access.html            — Demo access page
migrations/                        — Timestamped database migrations
scripts/                           — SQL seed scripts
.claude/PROJECT.md                 — This file
render.yaml                        — Render deployment config
```

---

## Testing Rules

**CRITICAL — read this before every review or verification task:**

NEVER bypass UI with JavaScript when testing. Do not use `document.querySelector('.login-overlay').style.display = 'none'` or `fetch('/api/auth/login')` to skip login flows. Always test the real user experience — click through forms, submit buttons, follow redirects.

Test the full flow, not just the endpoint. A successful API response (200) does not mean the feature works. The login API can return 200 while the dashboard overlay stays broken. Always verify the VISUAL result.

When reviewing code changes, check EXISTING code that assumes a fixed set of roles. New roles break hardcoded role lists, route maps, and if/else chains. Search the entire codebase for hardcoded role references.

After any CSS change, verify every dashboard visually on the live site. CSS extractions and refactors can silently break layouts. Take screenshots of all 5 dashboards + landing page + login page.

After merge, always verify on circul.polsia.app — not just in the local repo. The deploy pipeline can introduce its own issues.

### Pre-Commit Self-Audit Checklist

Run these checks before every commit that touches dashboard HTML files or server.js routes.

**DOM ID Consistency**
For each dashboard HTML file changed: Extract every `document.getElementById('...')` and `document.querySelector('...')` call from the `<script>` block. Verify each referenced ID/selector exists as an actual element in the HTML portion of the same file. Every DOM write (`el.textContent = ...`, `el.innerHTML = ...`) must have a null guard or the element must be guaranteed to exist. List any mismatches. Fix before committing.

**SQL Column Audit**
For each SQL query added or changed in server.js: Verify every column name exists in the corresponding table. Cross-reference against `migrations/` files. Verify every GROUP BY is unambiguous — use positional (`GROUP BY 1, 2`) or fully qualified (`table.column`), never bare column aliases that could clash with table columns. Verify JOINs reference valid foreign keys. Verify WHERE clauses handle NULL values.

**Route Completeness**
For each dashboard HTML file changed: Verify every `fetch()` or `apiFetch()` URL in the `<script>` block has a corresponding route in server.js. If a frontend calls an endpoint that doesn't exist, either build the route or remove the frontend call.

**UI Consistency**
After any HTML/CSS changes to dashboard files: All 5 dashboard headers must match: linked Circul logo, UPPERCASE tier pill, user name, identifier code, "← Home" link, "Log out" button. No dashboard should contain the supply chain tier bar (Collector → Aggregator → ... → Converter). Login overlay style should be consistent across dashboards.

**503 Retry**
Every dashboard's `apiFetch` function (or equivalent) must include retry logic for 503 responses: 1 retry after 1-second delay.

### PR Verification Checklist

Before marking any PR ready:
For every INSERT/UPDATE, verify each column's type from `migrations/*.js` — no assumptions.
For every string literal in a constrained column, verify it against the CHECK constraint.
Run a curl smoke test against each modified endpoint on the local server (or staging).
For seed SQL, validate column count and types match the table schema before committing.
Never call a fix "done" without a passing API call response in the terminal output.

---

## Known Issues & Patterns

- **shared/roles.js not deployed (URGENT):** `shared/roles.js` was never added to Polsia's `scripts/fetch-upstream-dashboards.js` after PR #14 created it. This causes login to fail with "Unable to connect" — the API call succeeds but `CirculRoles` is undefined so the success handler crashes. See Backlog for fix instructions.
- Collector passport section shows COL-XXXXXX format but header shows C-XXXX. Should be consistent — use C-XXXX everywhere.
- Some aggregator API calls return 503 intermittently — likely Render cold start or DB connection pool issue. All dashboards now have 503 retry logic.

---

## PR & Commit Conventions

Branch naming: `feat/`, `fix/`, `refactor/`, `style/` prefixes
Safe refactoring: Use two-commit approach — Commit 1 is purely additive (can't break), Commit 2 removes old code (independently revertable)
Merge command: `gh pr merge N --merge`
After merge: Always provide Polsia sync prompt using the template above

---

## Lessons Learned

### Sync Prompt Discipline (PR #25, March 2026)

Always verify filenames against the actual repo before writing sync prompts. Mockup filenames (`admin-dashboard.html`, `collector-passport.html`, `converter-output.html`) were used instead of checking `public/` on GitHub. This caused 3 unnecessary 404s on deploy.

Always run `ls public/` or check the repo file tree before listing files. Read PROJECT.md before writing any operational prompt. The sync template, cache buster requirement, and file structure are all documented here. Writing from memory missed the format and the `?t=${Date.now()}` pattern. Check the source of truth first.

The mockup-to-repo filename mapping must be explicit. Mockup files and repo files can have different names. Cross-reference the mapping (documented in the implementation prompt) when writing deploy or sync prompts.

### shared/roles.js Sync Miss (PR #14 → PR #25, March 2026)

PR #14 created `shared/roles.js` and added `app.use('/shared', express.static(...))` to server.js. The file worked when Express handled all requests. But `shared/roles.js` was never added to Polsia's `scripts/fetch-upstream-dashboards.js`. The file didn't exist on the deployed server, causing `/shared/roles.js` to 404. This went unnoticed until PR #25 triggered a redeploy. The symptom — "Unable to connect. Please check your internet connection." on login — was misleading because the login API worked fine; the crash happened in the success handler when `CirculRoles` was `undefined`.

**Rule:** Every new file outside `public/` that the browser or server needs MUST be added to the Polsia sync script at the same time it's created. Treat the sync script update as part of the PR, not a post-merge afterthought.

---

## Backlog

### CSS Design System Refactor

A contributed `style.css` was reviewed (March 26, 2026) with improvements worth adopting in a future sprint: Raw → semantic token split: abstract hex colors into named variables (`--green-500`) then reference them in semantic tokens (`--accent: var(--green-500)`). Light/dark mode: add `@media (prefers-color-scheme: light)` auto-detection and `[data-theme="light"]` manual override. Responsive typography: replace fixed font sizes with `clamp()` for fluid scaling. Reusable component classes: `.card`, `.button`, `.pill`, `.container`, `.grid`, `.flex`.

Do NOT drop-in replace shared.css. The contributed file is missing critical rules and variables the dashboards depend on (`.login-overlay.hidden`, role pill colors, `--nav-bg`, `--border-nav`, `--accent-muted`, `--text-3` for placeholders, `--r-sm`/`--r-md`, link reset, `min-height: 100vh`). Adopt the good ideas incrementally into the existing shared.css using the two-commit safe refactoring approach.

### Login Break — shared/roles.js not deployed (URGENT)

**Status:** Blocked — needs Polsia sync fix
**Root cause:** `shared/roles.js` was created in PR #14 but never added to `scripts/fetch-upstream-dashboards.js` in the Polsia repo. The file exists in jeffreyprovencal/circul but doesn't get synced to the Polsia deploy. Result: the browser loads login.html, the `<script src="/shared/roles.js">` 404s, `CirculRoles` is undefined, and any successful login crashes in the `.then()` handler with "Unable to connect."
**Immediate fix:** Add `shared/roles.js` to the file list in Polsia's `scripts/fetch-upstream-dashboards.js`. Use this sync prompt:

> Please sync `jeffreyprovencal/circul` main to `Polsia-Inc/circul`. Before triggering the Render deploy, add `shared/roles.js` to the file list in `scripts/fetch-upstream-dashboards.js` so it auto-syncs on future builds. The file path on the server must remain `shared/roles.js` (NOT inside `public/`), and the Express route `app.use('/shared', express.static(...))` in server.js already handles serving it.

**Longer-term fix:** The Phase 3 Vite migration below eliminates this class of bug entirely.

### Phase 2 — Clean URLs (remove .html from addresses)

**Status:** Ready to implement
**Effort:** Small — ~15 lines in server.js
**What:** Add Express middleware so `/login` serves `login.html`, `/register` serves `register.html`, etc. No `.html` in the browser address bar.
**Approach:** Add middleware before the static file handler that checks if a request path (without extension) maps to a `.html` file in `public/`, and serves it. Update all internal `<a href>` links across HTML files to drop the `.html` suffix.
**Why now:** Professional URLs, better for SEO if the site ever gets indexed, and it's a quick win before the bigger Vite migration.

### Phase 3 — Vite Migration

**Status:** Planning
**Effort:** ~1 day
**What:** Replace the current "HTML files with inline scripts served by Express" architecture with a Vite build pipeline. Shared modules like `roles.js` get properly bundled, cache-busted, and served from a `dist/` folder.
**Benefits:**
- Eliminates the shared/roles.js sync problem permanently (Vite resolves imports at build time)
- Automatic cache-busting (hashed filenames like `roles-3a8f2c.js`) — no more stale browser caches
- Hot reload during local development
- Foundation for future framework adoption (React/Vue/Svelte) if needed
- Clean URLs supported natively in dev mode

**Migration steps:**
1. `npm install vite --save-dev`
2. Create `vite.config.js` with multi-page app config pointing to all HTML files in `public/`
3. Extract inline `<script>` blocks from each HTML file into separate `.js` files
4. Convert `shared/roles.js` to ES module imports (`import { dashboardFor } from '/shared/roles.js'`)
5. Update `server.js` to serve from `dist/` instead of `public/`
6. Update `render.yaml` buildCommand: `npm install && npx vite build && npm run migrate`
7. Test all 5 dashboards + login + register + admin
8. Update Polsia sync prompt — `dist/` output replaces individual file syncing

**No new services required.** Vite and esbuild are free, open-source npm packages.
