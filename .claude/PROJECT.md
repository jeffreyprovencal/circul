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

- **`/api/collector/me` returns 404** — this route was never built in server.js. Collector dashboard renders but can't load personal stats.
- **`/api/orders/my` returns 500** — order endpoint has a bug (seen in network logs).
- **`/api/recycler/top-suppliers` and `/api/recycler/top-buyers` return 500** — these endpoints exist but error on certain queries.
- **`/api/converters/2/stats` returns 404** — converter stats route may not handle all converter IDs.
- **Some aggregator API calls return 503 intermittently** — likely Render cold start or DB connection pool issue.

---

## PR & Commit Conventions

- **Branch naming:** `feat/`, `fix/`, `refactor/`, `style/` prefixes
- **Safe refactoring:** Use two-commit approach — Commit 1 is purely additive (can't break), Commit 2 removes old code (independently revertable)
- **Merge command:** `gh pr merge N --merge`
- **After merge:** Always provide Polsia sync prompt using the template above
