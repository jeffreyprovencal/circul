# Circul post-deploy audit methodology

**Source of truth** for the post-deploy audit that runs against `circul.polsia.app` after each Polsia sync. The `circul-post-deploy-audit` scheduled task executes this methodology and writes a report to `circul-audit-YYYY-MM-DD.md` in the working folder.

Passing threshold: **95%**.

---

## Why this document exists

The audit has drifted in the past. This doc is its canonical version — update it here, then propagate changes to the scheduled task prompt so the two don't disagree.

Specific drift events this methodology now guards against:

- **Batch 7 (March 31, 2026)** — smoke test used `read_page` to confirm sections rendered but never clicked a button. Every bug found was an interaction bug.
- **Batch 8a (April 2, 2026)** — P&L "offline" bar survived three batches because every diagnosis focused on JS/API logic. Root cause was CSS specificity (`.pl-offline-bar{display:flex}` overriding `.hidden{display:none}`). A 10-second DevTools check would have found it.
- **Batch 10–11 (April 3–4, 2026)** — audit flagged endpoints it had guessed rather than endpoints that exist in the code (`/api/collector-passport/:id` vs actual `/api/collectors/:id/passport`). Three of five reported bugs were false positives.
- **2026-04-22** — audit flagged `pricing.html` and `mockup-ussd-v2.html` as 404 failures. Neither was ever scoped as a public route. Also flagged `assigned tasks` as a missing Agent section when it was an unsized backlog concept, not a shipped-and-reverted feature.

---

## Pre-audit setup

1. Open https://circul.polsia.app in Chrome.
2. Have Chrome DevTools console open for every dashboard.
3. Log in with demo credentials (see `reference_circul_demo_accounts.md` in auto-memory).
4. Clear localStorage before each dashboard so you test the fresh-login flow.

---

## Level 0 — CSS & computed-style check (do this first)

Before any interaction testing, on each dashboard:

1. Run `document.querySelectorAll('.hidden')` — for every element with class `hidden`, verify `getComputedStyle(el).display === 'none'`. Any `.hidden` element with `display: flex/grid/block` is a CSS specificity bug.
2. Check for elements that should be hidden but are visible (bars, overlays, modals, forms).
3. Check for elements that should be visible but are hidden (empty sections that should show content).
4. Check console for any CSS-related warnings.

Why this is Level 0: the P&L offline-bar bug survived three batches because nobody checked computed styles. This takes 30 seconds per dashboard and catches an entire class of bugs that code reading misses.

---

## Level 1 — UI/CSS integrity

For each of the six dashboards (Collector, Aggregator, Processor, Recycler, Converter, Agent):

- Design tokens applied: `--accent: #00e676`, `--bg: #0a1a0f`, `--surface: #0d2818`, Plus Jakarta Sans font.
- `.hidden` elements are not visible (Level 0 test).
- No broken images, missing icons, or layout shifts.
- Mobile responsive at 375px viewport.
- Dropdowns used instead of free-text inputs for cities, materials, and other closed-set fields.

---

## Level 2 — Per-section interaction test (mandatory)

For every section on every dashboard, complete an action table:

| Action | Input | Expected | Actual | Status |
|--------|-------|----------|--------|--------|
| what you click/submit | what data you enter | API call + UI change | what happened (screenshot/response) | PASS/FAIL/BLOCKED |

Rules:

- If the "Actual" column is empty, the section isn't tested. Move it to BLOCKED, not PASS.
- "Actual" must come from observing the live site, not from reading code.
- Check network tab for API responses, not just UI changes.
- After each action, check console for errors.
- For tabbed sections: click each tab and verify the content loads. Tab-lazy-loaded content starts as "Loading..." by design — this is NOT a bug unless clicking the tab fails to trigger the load.

### Per-dashboard section coverage

**Collector:** hero stats, Log a Collection form, Outbound table, Marketplace listings, Aggregator prices + "Sell to" button, Transactions table, Financials, Profile, Rating pill + modal.

**Aggregator:** hero stats, Register Purchase form, Incoming collections (accept/decline/grade), Batch Sales, Marketplace prices + listings, Processor prices, Financials P&L + Log Expense, Top Suppliers/Buyers, Rating pill + modal, Profile, My Field Agents table.

**Processor:** hero stats, Delivery Queue (pending/completed), Sell to Converters form, My Buying Prices (post price), Discover marketplace, Top Suppliers/Buyers, CSV export, Rating pill + modal, Compliance download.

**Recycler:** hero stats, Incoming deliveries, Outbound sales, Marketplace prices, Top Suppliers/Buyers, Rating section, Profile.

**Converter:** hero stats, Incoming deliveries, My Buying Prices, Orders, Available Material tab, Traceability tab, Ratings tab, Top Suppliers, Profile.

**Agent:** hero stats (performance band), Log a Collection, Register Collector, Recent Collections table, Profile. Do not check for "assigned tasks" — that's a parked roadmap concept, not a shipped feature (see `project_circul_agent_dispatch_parked.md`).

Upper-tier P&L: Processor, Recycler, Converter lack on-platform P&L/expense modules **by design** (they have their own accounting systems; Circul just exports data). Do not flag these as missing.

---

## Level 3 — End-to-end flow tests

1. **Collection flow:** Collector logs → Aggregator sees in incoming → accept with grade → payment → paid status reflects on both dashboards.
2. **Rating flow:** click rating pill → modal opens with correct transaction → submit → pill disappears → "Rated ★" replaces button → duplicate blocked (409).
3. **Discovery flow:** create listing → listing appears → make offer from another tier → offer appears → accept → pending transaction created.
4. **Sale flow:** log sale → receiver sees in queue → approve dispatch → mark arrived → grade → pay.
5. **Price flow:** post buying price on processor → aggregator sees it in Processor Prices section with correct company name.

---

## Level 4 — Negative / edge-case tests

- Submit every form with empty required fields — verify error messages (not silent failure).
- Click disabled buttons — verify feedback (toast, visual hint, or truly non-clickable).
- Rate same transaction twice — should be blocked with 409.
- Accept already-accepted pending transaction — should get clear error.
- Check zero-state displays on fresh/empty accounts.
- Check 500-error behavior (does data get lost? does UI recover?).
- Test offline behavior where applicable (expense queue).

---

## Level 5 — Cross-dashboard consistency

- Number formatting with commas everywhere.
- No login overlay flash on any dashboard.
- Same rating UX pattern (one-per-transaction, pill status after rating).
- Same payment flow pattern.
- Header consistency (logo, role pill, name, ID, Home, Log out).
- Section ordering follows shared skeleton.
- Name privacy: non-adjacent tiers see ID codes; adjacent see names.
- Company names render as `COALESCE(company, name)` — not person names.

---

## Progress tracking (report only, do not score)

The audit should report status on in-flight roadmap items but **must not count them against the 95% score**. This section is informational only.

- USSD build status (currently running via Africa's Talking, no dashboard route).
- Partner pricing page status (currently inline on `public/index.html`; no dedicated page scoped today).
- Demo agent account (Kofi Mensah, 0300000003 / PIN 3333) — login works, dashboard loads.
- Expense tracking coverage across dashboards.

---

## Post-audit review — before submitting the report

Run every flagged failure through these checks before including it in the report:

1. **Phantom-URL guard for 404 failures.** Before scoring a 404 as a failure, grep the repo (`server.js` + `public/`) for references to that path. **Zero references ≠ failure.** Report as "unscoped / roadmap gap," tracked in progress tracking, not scored. Historical precedent: `pricing.html` and `mockup-ussd-v2.html` were flagged as failures on 2026-04-22 despite zero in-code references. If the path becomes real work later, it graduates back into the audit.
2. **Regression vs gap distinction.** "Missing section X" is only a failure if X was a shipped feature that regressed, or a committed spec. If X is an unsized backlog concept, it's a gap — log in progress tracking, don't score. Historical precedent: Agent "assigned tasks" was scored as a failure on 2026-04-22 despite being a parked roadmap concept with no scoped spec.
3. **Endpoint cross-check.** For any API failure, verify the tested URL exists in `server.js`. Don't invent likely endpoint names. Historical precedent: audit tested `/api/collector-passport/:id` when actual code has `/api/collectors/:id/passport`.
4. **Tab-lazy-load check.** For any "Loading..." flagged as a bug, verify the section isn't inside an inactive tab panel. Tab-lazy-loaded content shows "Loading..." by design until the tab is clicked.
5. **Deployment-lag check.** For "API returns data but DOM not updated" bugs, confirm the fix has actually deployed. Sometimes the frontend JS in the test target is older than the backend.

---

## Reporting format

```
Level X — [PASS/FAIL] — X/Y checks passed (Z%)
```

List any failures with exact dashboard + section + description. Flag any new bugs not in the known-issues list. Mark progress-tracking items explicitly as "REPORT ONLY — not scored."

Overall score must meet the 95% threshold.

---

## Root causes the audit exists to catch

1. Fixes written against documentation, not the running app.
2. Verification steps that are syntactic (`node -c`), not functional (click, observe).
3. Each batch testing only what it changed, not the full flow.
4. Smoke tests substituting DOM reading for interaction.
5. Backend route existence being mistaken for frontend wiring.
6. CSS specificity blindness — `.hidden{display:none}` overridden by later `display:flex/grid/block` rules.
7. Large rewrites causing silent regressions (e.g., a later prompt reverting an earlier prompt's fix).
8. "Verified" being treated as equivalent to "tested" — reading code is not clicking buttons.
9. Diagnosis bias — fixating on one layer (JS/API) when the bug is in another layer (CSS). Always check the simplest layer first.
10. Auditor guessing endpoints or paths that don't exist in code and reporting their 404s as failures.
11. Scoring unscoped roadmap gaps as regressions — dragging the score for work that was never shipped.

---

## When this document changes

1. Update this file in a PR against `main`.
2. After merge, update the `circul-post-deploy-audit` scheduled task prompt so the two don't drift. The task prompt should include the level structure inline (so it can execute without fetching), with a footer pointing back at this doc as the canonical version.
3. Next audit run validates the changes against live prod.
