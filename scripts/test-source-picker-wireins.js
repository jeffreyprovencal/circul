#!/usr/bin/env node
// scripts/test-source-picker-wireins.js
// Smoke tests the three dashboard wire-ins: verify the picker script tag
// is loaded, the mount div is present, the submit handler has the gating
// + sources payload + showError hook, and the aggregator CTA handler
// points at IDs that actually exist on the same page.

'use strict';
const fs = require('fs');
const path = require('path');

let pass = 0, fail = 0;
function ok(label, cond, detail) {
  if (cond) { pass++; console.log(`  ✓ ${label}`); }
  else { fail++; console.log(`  ✗ ${label}${detail ? ' — ' + detail : ''}`); }
}

const ROOT = path.join(__dirname, '..', 'public');

function check(file, seller) {
  console.log(`\n[${seller}] ${file}`);
  const html = fs.readFileSync(path.join(ROOT, file), 'utf8');

  ok('loads /shared/source-picker.js', /src=["']\/shared\/source-picker\.js["']/.test(html));
  ok('has mount div (#source-picker-mount)', /id=["']source-picker-mount["']/.test(html));
  ok('submit gates on picker.canSubmit', /window\.__picker.*!window\.__pickerState\.canSubmit|!window\.__pickerState\.canSubmit/.test(html));
  ok('submit posts sources:', /sources:\s*pickerSources/.test(html));
  ok('submit calls showError(data.details)', /picker\.showError\(\s*data\.details/.test(html));
  ok('submit resets picker after success', /__picker\.update\(\s*\{\s*materialType:\s*['"]{2},\s*targetKg:\s*0/.test(html));
  ok('mounts with correct sellerRole', new RegExp(`sellerRole:\\s*['"]${seller}['"]`).test(html));
  ok('material-change handler wired', /on(Sell|Proc|Rec)MaterialChanged/.test(html));
  ok('weight-change handler wired', /on(Sell|Proc|Rec)WeightChanged/.test(html));
}

check('aggregator-dashboard.html', 'aggregator');
check('processor-dashboard.html',  'processor');
check('recycler-dashboard.html',   'recycler');

// ── Aggregator CTA handler: IDs it references must actually exist on
// the same page (the Phase-B-initial bug that shipped referenced
// buyCard/aggregatorPurchaseCard, neither of which exists).
console.log('\n[aggregator CTA handler integrity]');
{
  const html = fs.readFileSync(path.join(ROOT, 'aggregator-dashboard.html'), 'utf8');
  const ctaMatch = html.match(/source-picker:log-purchase[\s\S]*?\}\);/);
  ok('CTA handler present', !!ctaMatch);
  const handler = ctaMatch ? ctaMatch[0] : '';
  const ids = (handler.match(/getElementById\(['"]([^'"]+)['"]\)/g) || [])
    .map(s => s.match(/['"]([^'"]+)['"]/)[1]);
  ok('handler references at least one getElementById', ids.length > 0);
  ids.forEach(id => {
    const idExists = new RegExp(`id=["']${id}["']`).test(html);
    ok(`handler id "${id}" exists on page`, idExists);
  });
  ok('handler calls toggleRegisterForm',
    /toggleRegisterForm\s*\(\s*\)/.test(handler),
    'the register-purchase inline form is the CTA target — scrolling without opening it is half a fix');
}

// ── Processor picker lifecycle: initProcessorSourcePicker should fire
// inside the openDispatchForm wrapper, and the wrapper should reset
// picker state when the form transitions hidden → visible.
console.log('\n[processor picker lifecycle]');
{
  const html = fs.readFileSync(path.join(ROOT, 'processor-dashboard.html'), 'utf8');
  ok('openDispatchForm wrapped',
    /var origOpen\s*=\s*window\.openDispatchForm/.test(html));
  ok('picker reinit guarded by ptId-is-undefined (log-sale path only)',
    /ptId\s*===\s*undefined\s*\|\|\s*typeof\s*ptId\s*!==\s*['"]number['"]/.test(html));
  ok('hidden→visible transition pushes fresh inputs to picker',
    /wasHidden[\s\S]{1,400}window\.__picker\.update\(/.test(html));
}

console.log('\n─────────────────────────────────────────────');
console.log(`PASS: ${pass}`);
console.log(`FAIL: ${fail}`);
if (fail > 0) process.exit(1);
