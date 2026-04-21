#!/usr/bin/env node
// scripts/test-source-picker.js
// Headless jsdom harness that drives shared/source-picker.js through all
// documented states + submit-gating paths.  Exists because the prompt's
// "Manual browser QA, one per dashboard" verification step can't run in
// a sandbox without a live browser+server — this gets us 80% coverage
// of the picker's state machine against mocked /api/sources responses.
//
// Exit 0 on success, 1 on any failing assertion. Follows the repo's
// existing test-*.js shell-friendly conventions.

'use strict';
const { JSDOM } = require('jsdom');
const fs = require('fs');
const path = require('path');

const PICKER_SRC = fs.readFileSync(
  path.join(__dirname, '..', 'shared', 'source-picker.js'),
  'utf8'
);

let pass = 0, fail = 0;
function ok(label, cond, detail) {
  if (cond) { pass++; console.log(`  ✓ ${label}`); }
  else { fail++; console.log(`  ✗ ${label}${detail ? ' — ' + detail : ''}`); }
}

// Build a fresh DOM with the picker script evaluated.
function makeEnv(fetchImpl) {
  const dom = new JSDOM(
    `<!doctype html><html><body><div id="mount"></div></body></html>`,
    { runScripts: 'outside-only', url: 'http://localhost/' }
  );
  dom.window.fetch = fetchImpl;
  dom.window.localStorage.setItem('circul_token', 'test-token');
  dom.window.eval(PICKER_SRC);
  return dom;
}

// Helper: wait for N microtasks (fetch promise resolution).
function flush(dom, n) {
  n = n || 5;
  return new Promise(resolve => {
    let i = 0;
    function tick() {
      if (i++ >= n) return resolve();
      dom.window.queueMicrotask(tick);
    }
    tick();
  });
}

function mkCandidates(material, lots) {
  // lots = [[kg, supplier_name, batch_id, created_at], ...]
  return lots.map((l, i) => ({
    source_id: 100 + i,
    transaction_type: 'aggregator_purchase',
    material_type: material,
    remaining_kg: l[0],
    created_at: l[3] || '2026-04-10T00:00:00Z',
    batch_id: l[2] || ('batch-' + i),
    supplier_name: l[1] || ('Supplier ' + i),
    supplier_role: 'collector'
  }));
}

// ── Scenario runner ─────────────────────────────────────────────────────
async function run() {
  console.log('source-picker.js — headless jsdom tests\n');

  // ─────────────────────────────────────────────────────────────────
  console.log('[1] mount() with no material/weight → idle state, no fetch');
  {
    let fetched = false;
    const dom = makeEnv(() => { fetched = true; return Promise.reject(new Error('no')); });
    const mount = dom.window.document.getElementById('mount');
    let last = null;
    const p = dom.window.SourcePicker.mount(mount, {
      sellerRole: 'aggregator',
      materialType: '',
      targetKg: 0,
      onChange: s => { last = s; }
    });
    await flush(dom);
    ok('no fetch when inputs are empty', !fetched);
    ok('emits canSubmit=false when idle', last && last.canSubmit === false);
    ok('idle status propagates', last && last.status === 'idle');
    ok('getSources returns []', p.getSources().length === 0);
  }

  // ─────────────────────────────────────────────────────────────────
  console.log('\n[2] FIFO prefill: 3 lots, target 500 kg → uses first lot (500kg)');
  {
    const rows = mkCandidates('PET', [[500, 'Alpha Co', 'b1', '2026-04-01T00:00:00Z'],
                                      [500, 'Beta Co',  'b2', '2026-04-05T00:00:00Z'],
                                      [300, 'Gamma Co', 'b3', '2026-04-10T00:00:00Z']]);
    const dom = makeEnv(() => Promise.resolve({ ok: true, json: () => Promise.resolve(rows) }));
    const mount = dom.window.document.getElementById('mount');
    let last = null;
    const p = dom.window.SourcePicker.mount(mount, {
      sellerRole: 'aggregator', materialType: 'PET', targetKg: 500,
      onChange: s => { last = s; }
    });
    await flush(dom, 20);
    const s = p.getSources();
    ok('exactly 1 source picked', s.length === 1);
    ok('first lot chosen (FIFO)', s[0] && s[0].source_id === 100);
    ok('allocation = 500kg', s[0] && s[0].kg === 500);
    ok('status = balanced', last && last.status === 'balanced');
    ok('canSubmit = true', last && last.canSubmit === true);
  }

  // ─────────────────────────────────────────────────────────────────
  console.log('\n[3] FIFO spans multiple lots: target 700 kg across 500/500/300');
  {
    const rows = mkCandidates('PET', [[500, 'A', 'b1'], [500, 'B', 'b2'], [300, 'C', 'b3']]);
    const dom = makeEnv(() => Promise.resolve({ ok: true, json: () => Promise.resolve(rows) }));
    const p = dom.window.SourcePicker.mount(dom.window.document.getElementById('mount'), {
      sellerRole: 'aggregator', materialType: 'PET', targetKg: 700,
      onChange: () => {}
    });
    await flush(dom, 20);
    const s = p.getSources();
    ok('2 lots picked', s.length === 2);
    ok('first lot fully drawn (500)', s[0] && s[0].kg === 500);
    ok('second lot partial (200)', s[1] && s[1].kg === 200);
  }

  // ─────────────────────────────────────────────────────────────────
  console.log('\n[4] Over-allocation via input edit → status=over, canSubmit=false');
  {
    const rows = mkCandidates('PET', [[500, 'A'], [500, 'B']]);
    const dom = makeEnv(() => Promise.resolve({ ok: true, json: () => Promise.resolve(rows) }));
    let last = null;
    const p = dom.window.SourcePicker.mount(dom.window.document.getElementById('mount'), {
      sellerRole: 'aggregator', materialType: 'PET', targetKg: 500,
      onChange: s => { last = s; }
    });
    await flush(dom, 20);
    // Simulate editing the first lot's input up to 540.
    const input = dom.window.document.querySelector('.sp-alloc-input');
    input.value = '540';
    input.dispatchEvent(new dom.window.Event('input'));
    ok('status = over when sum exceeds target', last && last.status === 'over');
    ok('canSubmit = false when over', last && last.canSubmit === false);
    ok('total = 540', last && last.total_kg === 540);
  }

  // ─────────────────────────────────────────────────────────────────
  console.log('\n[5] Under-allocation → status=under, canSubmit=false');
  {
    const rows = mkCandidates('PET', [[500, 'A'], [500, 'B']]);
    const dom = makeEnv(() => Promise.resolve({ ok: true, json: () => Promise.resolve(rows) }));
    let last = null;
    const p = dom.window.SourcePicker.mount(dom.window.document.getElementById('mount'), {
      sellerRole: 'aggregator', materialType: 'PET', targetKg: 500,
      onChange: s => { last = s; }
    });
    await flush(dom, 20);
    const input = dom.window.document.querySelector('.sp-alloc-input');
    input.value = '400';
    input.dispatchEvent(new dom.window.Event('input'));
    ok('status = under when sum < target', last && last.status === 'under');
    ok('canSubmit = false when under', last && last.canSubmit === false);
  }

  // ─────────────────────────────────────────────────────────────────
  console.log('\n[6] Reset-to-FIFO: over → reset → balanced');
  {
    const rows = mkCandidates('PET', [[500, 'A'], [500, 'B']]);
    const dom = makeEnv(() => Promise.resolve({ ok: true, json: () => Promise.resolve(rows) }));
    let last = null;
    const p = dom.window.SourcePicker.mount(dom.window.document.getElementById('mount'), {
      sellerRole: 'aggregator', materialType: 'PET', targetKg: 500,
      onChange: s => { last = s; }
    });
    await flush(dom, 20);
    const input = dom.window.document.querySelector('.sp-alloc-input');
    input.value = '600';
    input.dispatchEvent(new dom.window.Event('input'));
    ok('perturbed to over', last && last.status === 'over');
    // Find and click the Reset-to-FIFO link button (text match).
    const btns = dom.window.document.querySelectorAll('.sp-btn-link');
    let resetBtn = null;
    btns.forEach(b => { if (/Reset/i.test(b.textContent)) resetBtn = b; });
    ok('Reset-to-FIFO button present', !!resetBtn);
    resetBtn.click();
    ok('status = balanced after reset', last && last.status === 'balanced');
    ok('canSubmit = true after reset', last && last.canSubmit === true);
  }

  // ─────────────────────────────────────────────────────────────────
  console.log('\n[7] Empty state — aggregator tier: CTA present + event dispatch');
  {
    const dom = makeEnv(() => Promise.resolve({ ok: true, json: () => Promise.resolve([]) }));
    const mount = dom.window.document.getElementById('mount');
    let ctaFired = false;
    mount.addEventListener('source-picker:log-purchase', () => { ctaFired = true; });
    const p = dom.window.SourcePicker.mount(mount, {
      sellerRole: 'aggregator', materialType: 'PET', targetKg: 500,
      onChange: () => {}
    });
    await flush(dom, 20);
    const title = dom.window.document.querySelector('.sp-empty-title');
    const cta = dom.window.document.querySelector('.sp-btn-cta');
    ok('empty title rendered', title && /No PET inventory/.test(title.textContent));
    ok('CTA button rendered', cta && /Log a purchase/.test(cta.textContent));
    cta.click();
    ok('CTA click fires source-picker:log-purchase event', ctaFired);
  }

  // ─────────────────────────────────────────────────────────────────
  console.log('\n[8] Empty state — processor tier: no CTA, honest copy');
  {
    const dom = makeEnv(() => Promise.resolve({ ok: true, json: () => Promise.resolve([]) }));
    const p = dom.window.SourcePicker.mount(dom.window.document.getElementById('mount'), {
      sellerRole: 'processor', materialType: 'PET', targetKg: 500,
      onChange: () => {}
    });
    await flush(dom, 20);
    const title = dom.window.document.querySelector('.sp-empty-title');
    const cta = dom.window.document.querySelector('.sp-btn-cta');
    ok('processor empty title mentions aggregators', title && /aggregators/.test(title.textContent));
    ok('no CTA for processor', !cta);
  }

  // ─────────────────────────────────────────────────────────────────
  console.log('\n[9] Empty state — recycler tier: no CTA, mentions processors');
  {
    const dom = makeEnv(() => Promise.resolve({ ok: true, json: () => Promise.resolve([]) }));
    const p = dom.window.SourcePicker.mount(dom.window.document.getElementById('mount'), {
      sellerRole: 'recycler', materialType: 'HDPE', targetKg: 300,
      onChange: () => {}
    });
    await flush(dom, 20);
    const title = dom.window.document.querySelector('.sp-empty-title');
    const cta = dom.window.document.querySelector('.sp-btn-cta');
    ok('recycler empty title mentions processors', title && /processors/.test(title.textContent));
    ok('no CTA for recycler', !cta);
  }

  // ─────────────────────────────────────────────────────────────────
  console.log('\n[10] .update() with new material → refetches');
  {
    let url = null;
    const dom = makeEnv((u) => {
      url = u;
      return Promise.resolve({ ok: true, json: () => Promise.resolve(
        u.indexOf('PET') !== -1 ? mkCandidates('PET', [[500, 'A']]) : mkCandidates('HDPE', [[300, 'X']])
      ) });
    });
    const p = dom.window.SourcePicker.mount(dom.window.document.getElementById('mount'), {
      sellerRole: 'aggregator', materialType: 'PET', targetKg: 500,
      onChange: () => {}
    });
    await flush(dom, 20);
    ok('first fetch was for PET', url.indexOf('material_type=PET') !== -1);
    ok('first fetch carries seller_role', url.indexOf('seller_role=aggregator') !== -1);
    p.update({ materialType: 'HDPE', targetKg: 300 });
    await flush(dom, 20);
    ok('second fetch was for HDPE', url.indexOf('material_type=HDPE') !== -1);
    const s = p.getSources();
    ok('re-prefilled for HDPE', s.length === 1 && s[0].kg === 300);
  }

  // ─────────────────────────────────────────────────────────────────
  console.log('\n[11] .showError() — sum_mismatch_kg renders inline banner');
  {
    const rows = mkCandidates('PET', [[500, 'A']]);
    const dom = makeEnv(() => Promise.resolve({ ok: true, json: () => Promise.resolve(rows) }));
    const p = dom.window.SourcePicker.mount(dom.window.document.getElementById('mount'), {
      sellerRole: 'aggregator', materialType: 'PET', targetKg: 500,
      onChange: () => {}
    });
    await flush(dom, 20);
    p.showError({
      reason: 'invalid_manual_sources',
      sum_mismatch_kg: 40, hint_total_kg: 540, target_kg: 500
    });
    const inlineErr = dom.window.document.querySelector('.sp-inline-err');
    ok('inline sum-error banner rendered', !!inlineErr);
    ok('banner mentions 540 kg', inlineErr && /540/.test(inlineErr.textContent));
    ok('banner mentions 40 kg', inlineErr && /40/.test(inlineErr.textContent));
  }

  // ─────────────────────────────────────────────────────────────────
  console.log('\n[12] .showError() — insufficient_remaining locks row + Refresh button');
  {
    const rows = mkCandidates('PET', [[500, 'A'], [500, 'B']]);
    const dom = makeEnv(() => Promise.resolve({ ok: true, json: () => Promise.resolve(rows) }));
    const p = dom.window.SourcePicker.mount(dom.window.document.getElementById('mount'), {
      sellerRole: 'aggregator', materialType: 'PET', targetKg: 500,
      onChange: () => {}
    });
    await flush(dom, 20);
    p.showError({
      reason: 'invalid_manual_sources',
      insufficient_remaining: [{ id: 100, remaining_kg: 300, requested_kg: 500 }]
    });
    const errRow = dom.window.document.querySelector('.sp-lot-error');
    ok('errored lot gets sp-lot-error class', !!errRow);
    const cbs = dom.window.document.querySelectorAll('.sp-cb');
    ok('errored row checkbox is disabled', cbs[0] && cbs[0].disabled === true);
    // Footer should show "Refresh sources" (not "Reset to FIFO") when row errors present.
    const btns = dom.window.document.querySelectorAll('.sp-btn-link');
    let hasRefresh = false;
    btns.forEach(b => { if (/Refresh/.test(b.textContent)) hasRefresh = true; });
    ok('footer shows Refresh sources link', hasRefresh);
  }

  // ─────────────────────────────────────────────────────────────────
  console.log('\n[13] .showError(reason=shortfall) → falls through to empty state');
  {
    const rows = mkCandidates('PET', [[500, 'A']]);
    const dom = makeEnv(() => Promise.resolve({ ok: true, json: () => Promise.resolve(rows) }));
    let last = null;
    const p = dom.window.SourcePicker.mount(dom.window.document.getElementById('mount'), {
      sellerRole: 'aggregator', materialType: 'PET', targetKg: 500,
      onChange: s => { last = s; }
    });
    await flush(dom, 20);
    p.showError({ reason: 'shortfall' });
    ok('status = empty after shortfall', last && last.status === 'empty');
    const title = dom.window.document.querySelector('.sp-empty-title');
    ok('empty copy rendered', !!title);
  }

  // ─────────────────────────────────────────────────────────────────
  console.log('\n[14] .clearError() clears row + sum errors');
  {
    const rows = mkCandidates('PET', [[500, 'A']]);
    const dom = makeEnv(() => Promise.resolve({ ok: true, json: () => Promise.resolve(rows) }));
    const p = dom.window.SourcePicker.mount(dom.window.document.getElementById('mount'), {
      sellerRole: 'aggregator', materialType: 'PET', targetKg: 500,
      onChange: () => {}
    });
    await flush(dom, 20);
    p.showError({
      reason: 'invalid_manual_sources',
      sum_mismatch_kg: 40, hint_total_kg: 540, target_kg: 500
    });
    ok('sum error present', !!dom.window.document.querySelector('.sp-inline-err'));
    p.clearError();
    ok('sum error cleared', !dom.window.document.querySelector('.sp-inline-err'));
  }

  // ─────────────────────────────────────────────────────────────────
  console.log('\n[15] Network error → error state rendered with Retry');
  {
    const dom = makeEnv(() => Promise.resolve({ ok: false, status: 500, json: () => Promise.resolve({ message: 'boom' }) }));
    let last = null;
    const p = dom.window.SourcePicker.mount(dom.window.document.getElementById('mount'), {
      sellerRole: 'aggregator', materialType: 'PET', targetKg: 500,
      onChange: s => { last = s; }
    });
    await flush(dom, 20);
    ok('status = error', last && last.status === 'error');
    const body = dom.window.document.querySelector('.sp-empty-body');
    ok('error body mentions boom', body && /boom/.test(body.textContent));
  }

  // ─────────────────────────────────────────────────────────────────
  console.log('\n[16] seller_role passed to /api/sources (dual-role correctness)');
  {
    const urls = [];
    const dom = makeEnv((u) => {
      urls.push(u);
      return Promise.resolve({ ok: true, json: () => Promise.resolve(mkCandidates('PET', [[100, 'A']])) });
    });
    const p = dom.window.SourcePicker.mount(dom.window.document.getElementById('mount'), {
      sellerRole: 'recycler', materialType: 'PET', targetKg: 100,
      onChange: () => {}
    });
    await flush(dom, 20);
    ok('request URL includes seller_role=recycler', urls[0] && urls[0].indexOf('seller_role=recycler') !== -1);
    ok('request URL includes material_type=PET', urls[0] && urls[0].indexOf('material_type=PET') !== -1);
  }

  // ─────────────────────────────────────────────────────────────────
  console.log('\n[17] Race-guard: stale fetch response discarded');
  {
    const pending = [];
    const dom = makeEnv(() => new Promise(resolve => pending.push(resolve)));
    let last = null;
    const p = dom.window.SourcePicker.mount(dom.window.document.getElementById('mount'), {
      sellerRole: 'aggregator', materialType: 'PET', targetKg: 500,
      onChange: s => { last = s; }
    });
    await flush(dom);
    p.update({ materialType: 'HDPE' });
    await flush(dom);
    // Now resolve the FIRST fetch (PET) late — picker should ignore it.
    pending[0]({ ok: true, json: () => Promise.resolve(mkCandidates('PET', [[500, 'A']])) });
    pending[1]({ ok: true, json: () => Promise.resolve(mkCandidates('HDPE', [[250, 'X']])) });
    await flush(dom, 30);
    const s = p.getSources();
    ok('only the second (HDPE) fetch applied', s.length === 1 && s[0].source_id === 100);
  }

  // ─────────────────────────────────────────────────────────────────
  console.log('\n[18] Invalid sellerRole at mount() throws');
  {
    const dom = makeEnv(() => Promise.reject(new Error('nope')));
    let threw = false;
    try {
      dom.window.SourcePicker.mount(dom.window.document.getElementById('mount'), {
        sellerRole: 'converter', materialType: 'PET', targetKg: 500, onChange: () => {}
      });
    } catch (_e) { threw = true; }
    ok('throws on invalid sellerRole', threw);
  }

  // ─────────────────────────────────────────────────────────────────
  console.log('\n[19] Missing sellerRole at mount() throws');
  {
    const dom = makeEnv(() => Promise.reject(new Error('nope')));
    let threw = false;
    try {
      dom.window.SourcePicker.mount(dom.window.document.getElementById('mount'), {
        materialType: 'PET', targetKg: 500, onChange: () => {}
      });
    } catch (_e) { threw = true; }
    ok('throws on missing sellerRole', threw);
  }

  // ─────────────────────────────────────────────────────────────────
  console.log('\n[20] destroy() clears DOM and stops callbacks');
  {
    const rows = mkCandidates('PET', [[500, 'A']]);
    const dom = makeEnv(() => Promise.resolve({ ok: true, json: () => Promise.resolve(rows) }));
    const mount = dom.window.document.getElementById('mount');
    let fireCount = 0;
    const p = dom.window.SourcePicker.mount(mount, {
      sellerRole: 'aggregator', materialType: 'PET', targetKg: 500,
      onChange: () => { fireCount++; }
    });
    await flush(dom, 20);
    const before = fireCount;
    p.destroy();
    ok('mount cleared on destroy', mount.innerHTML === '');
    p.update({ materialType: 'HDPE' });  // should be no-op after destroy
    await flush(dom, 10);
    ok('no onChange after destroy', fireCount === before);
  }

  // ─────────────────────────────────────────────────────────────────
  console.log('\n[21] Mobile-viewport CSS class still applies (stacking hint)');
  {
    // The CSS-level responsive behavior is enforced by a @media rule in
    // public/shared.css; we can't exercise matchMedia inside jsdom, but
    // we can verify the root element carries the classes we key off of.
    const rows = mkCandidates('PET', [[500, 'A']]);
    const dom = makeEnv(() => Promise.resolve({ ok: true, json: () => Promise.resolve(rows) }));
    const p = dom.window.SourcePicker.mount(dom.window.document.getElementById('mount'), {
      sellerRole: 'aggregator', materialType: 'PET', targetKg: 500,
      onChange: () => {}
    });
    await flush(dom, 20);
    const lot = dom.window.document.querySelector('.sp-lot');
    const alloc = dom.window.document.querySelector('.sp-alloc');
    ok('.sp-lot element present (CSS media query key)', !!lot);
    ok('.sp-alloc element present (CSS media query key)', !!alloc);
  }

  // ─────────────────────────────────────────────────────────────────
  console.log('\n─────────────────────────────────────────────');
  console.log(`PASS: ${pass}`);
  console.log(`FAIL: ${fail}`);
  if (fail > 0) process.exit(1);
}

run().catch(e => { console.error('Runner crashed:', e); process.exit(2); });
