// scripts/lint-ussd-screens.js — USSD line-budget lint (static analyzer)
//
// Walks server.js via the acorn AST, finds every CON/END response screen,
// and flags violations of the iOS visible-line budget. Catches the bug class
// from PR #66 (request-submitted truncation) and PR #69 (Confirm purchase /
// Collector found truncation) at parse time, before merge.
//
// Usage:
//   node scripts/lint-ussd-screens.js                   # lint, exit 1 on ERROR
//   node scripts/lint-ussd-screens.js --warn-fail       # exit 1 on WARN too
//   node scripts/lint-ussd-screens.js --json            # machine-readable
//   node scripts/lint-ussd-screens.js --file=path.js    # lint a different file
//   LINT_USSD_VERBOSE=1 node scripts/lint-ussd-screens.js   # show INFOs
//
// Exit 0 on clean, 1 on violations at fail-severity.
//
// Inline waiver: place `// ussd-lint-allow: <reason>` on a line within
// 2 lines above a flagged screen to downgrade all its violations to INFO.
//
// AST-based, not regex — handles nested template literals, `+` concatenation
// chains, and escaped quotes correctly.

const fs = require('fs');
const path = require('path');
const acorn = require('acorn');

// ─── config ─────────────────────────────────────────────────────────────────

function arg(prefix) {
  const a = process.argv.find(x => x.startsWith(prefix + '='));
  return a ? a.slice(prefix.length + 1) : null;
}

const TARGET_FILE = arg('--file') || path.join(__dirname, '..', 'server.js');
const FAIL_ON_WARN = process.argv.includes('--warn-fail');
const JSON_OUTPUT = process.argv.includes('--json');
const VERBOSE = !!process.env.LINT_USSD_VERBOSE;

// Visible-line budgets per screen type. Bands are inclusive lower bounds:
// lineCount >= warn → WARN, lineCount >= error → ERROR.
//
// Day-one calibration (2026-04-29):
//   - 7-line CONs (Confirm purchase et al, post-PR-#69 shape) sit at the cap
//     and pass clean. 8-9 line CONs warn (post-pilot sweep target). 10+ errs.
//   - 9-11 line ENDs warn (info loss but no dead-end). 12+ errs.
// Tune in one place if real-device testing surfaces a tighter iPhone budget.
const BUDGETS = {
  CON: { warn: 8, error: 10 },  // CON cap = 7 lines, 8-9 warn (sweep target), 10+ truncates badly
  END: { warn: 9, error: 12 },  // END cap = 8 lines per memory, 9-11 warn, 12+ definitely clipped
  perLineWarn: 33,              // > 32 chars likely wraps on iPhone (USSD dialog ≈ 32-36 wide)
  perLineInfo: 29,              // > 28 chars tight but usually OK — surfaced for awareness
};

// Conservative placeholder for `${expr}` slots — assume short strings (digits,
// ID codes). Actual runtime values may be longer; we flag the literal portion
// so the line *baseline* can be reasoned about. A 4-char placeholder mirrors
// values like "5kg" or "PET" without inflating empty lines.
const INTERP_PLACEHOLDER_LEN = 4;
const INTERP_PLACEHOLDER = 'X'.repeat(INTERP_PLACEHOLDER_LEN);

const WAIVER_RE = /\/\/\s*ussd-lint-allow\s*:\s*(.+)$/;

// ─── extractor ──────────────────────────────────────────────────────────────

// Walk an AST node and return its resolved literal-only text (with `${...}`
// interpolations replaced by INTERP_PLACEHOLDER), or null if the node is not a
// statically-resolvable string.
function resolveStringNode(node) {
  if (!node) return null;
  if (node.type === 'Literal' && typeof node.value === 'string') return node.value;
  if (node.type === 'TemplateLiteral') {
    let out = '';
    for (let i = 0; i < node.quasis.length; i++) {
      out += node.quasis[i].value.cooked;
      if (i < node.expressions.length) out += INTERP_PLACEHOLDER;
    }
    return out;
  }
  if (node.type === 'BinaryExpression' && node.operator === '+') {
    const left = resolveStringNode(node.left);
    const right = resolveStringNode(node.right);
    if (left == null || right == null) return null;
    return left + right;
  }
  return null;
}

// Recursively visit every node. We do NOT prune at the resolveStringNode boundary —
// we want to find inner string nodes too (e.g., a function call argument that
// contains a CON/END literal). The walker dedupes by start-position so each
// CON/END string is reported once even if seen via multiple ancestor paths.
function walk(node, visit) {
  if (!node || typeof node !== 'object') return;
  if (Array.isArray(node)) {
    for (const child of node) walk(child, visit);
    return;
  }
  if (typeof node.type === 'string') visit(node);
  for (const key of Object.keys(node)) {
    if (key === 'loc' || key === 'range' || key === 'start' || key === 'end') continue;
    const val = node[key];
    if (val && typeof val === 'object') walk(val, visit);
  }
}

function extractScreens(ast) {
  const seen = new Set();
  const out = [];
  walk(ast, node => {
    if (node.type !== 'Literal' && node.type !== 'TemplateLiteral' && node.type !== 'BinaryExpression') return;
    const text = resolveStringNode(node);
    if (text == null) return;
    if (!/^(CON|END) /.test(text)) return;
    const key = node.start + ':' + node.end;
    if (seen.has(key)) return;
    seen.add(key);
    // For BinaryExpression, the outer `+` chain may contain inner Literals
    // that also start with 'CON ' / 'END ' — but since we already captured
    // the outer concat, we want to skip those inner nodes. Mark every inner
    // string-resolvable node as seen.
    if (node.type === 'BinaryExpression') {
      walk(node, inner => {
        if (inner === node) return;
        if (inner.type === 'Literal' || inner.type === 'TemplateLiteral' || inner.type === 'BinaryExpression') {
          seen.add(inner.start + ':' + inner.end);
        }
      });
    }
    const startLine = node.loc.start.line;
    const endLine = node.loc.end.line;
    const type = text.startsWith('CON ') ? 'CON' : 'END';
    const hasInterpolation = node.type === 'TemplateLiteral'
      ? node.expressions.length > 0
      : node.type === 'BinaryExpression';
    out.push({ startLine, endLine, type, literalText: text, hasInterpolation });
  });
  return out;
}

// ─── rule engine ────────────────────────────────────────────────────────────

function checkScreen(screen) {
  const violations = [];
  const lines = screen.literalText.split('\n');
  const lineCount = lines.length;
  const budget = BUDGETS[screen.type];

  if (lineCount >= budget.error) {
    violations.push({
      severity: 'ERROR', rule: 'line-count',
      reason: `${screen.type} screen has ${lineCount} lines (max ${budget.error - 1})`,
    });
  } else if (lineCount >= budget.warn) {
    violations.push({
      severity: 'WARN', rule: 'line-count',
      reason: `${screen.type} screen has ${lineCount} lines (target ≤${budget.warn - 1})`,
    });
  }

  for (let i = 0; i < lines.length; i++) {
    // Strip the leading 'CON '/'END ' prefix on line 0 so we don't double-count it.
    const stripped = i === 0 ? lines[i].replace(/^(CON|END) /, '') : lines[i];
    const len = stripped.length;
    if (len > BUDGETS.perLineWarn) {
      violations.push({
        severity: 'WARN', rule: 'line-length',
        reason: `line ${i + 1} is ${len} literal chars (likely to wrap on iOS at ~32)`,
        snippet: stripped.length > 80 ? stripped.slice(0, 77) + '...' : stripped,
      });
    } else if (len > BUDGETS.perLineInfo) {
      violations.push({
        severity: 'INFO', rule: 'line-length',
        reason: `line ${i + 1} is ${len} literal chars (close to wrap threshold)`,
        snippet: stripped,
      });
    }
  }

  return violations;
}

// ─── waiver handler ─────────────────────────────────────────────────────────

function applyWaivers(violations, sourceLines, screen) {
  // Look at the 2 source lines immediately above the screen's start. The AST
  // line is 1-indexed; sourceLines is 0-indexed.
  const idx = screen.startLine - 1;
  for (let off = 1; off <= 2; off++) {
    const candidate = sourceLines[idx - off];
    if (!candidate) continue;
    const m = candidate.match(WAIVER_RE);
    if (m) {
      const reason = m[1].trim();
      return violations.map(v => ({ ...v, severity: 'INFO', waivedReason: reason }));
    }
  }
  return violations;
}

// ─── reporter ───────────────────────────────────────────────────────────────

function formatRecord(r, file) {
  const head = `${path.basename(file)}:${r.startLine}  [${r.rule}] ${r.type} — ${r.reason}`;
  const waiver = r.waivedReason ? `\n        waived: ${r.waivedReason}` : '';
  const snippet = r.snippet ? `\n        > ${r.snippet}` : '';
  return head + waiver + snippet;
}

function reportText(records, file, screensCount) {
  const errors = records.filter(r => r.severity === 'ERROR');
  const warns = records.filter(r => r.severity === 'WARN');
  const infos = records.filter(r => r.severity === 'INFO');
  const waived = infos.filter(r => r.waivedReason);

  console.log('[lint-ussd] target: ' + path.relative(process.cwd(), file));
  console.log('[lint-ussd] screens checked: ' + screensCount);

  if (errors.length) {
    console.log('\n=== ERRORS (' + errors.length + ') ===');
    for (const r of errors) console.log('  ' + formatRecord(r, file));
  }
  if (warns.length) {
    console.log('\n=== WARNINGS (' + warns.length + ') ===');
    for (const r of warns) console.log('  ' + formatRecord(r, file));
  }
  if (infos.length && VERBOSE) {
    console.log('\n=== INFO (' + infos.length + ') ===');
    for (const r of infos) console.log('  ' + formatRecord(r, file));
  }

  console.log(
    '\n[lint-ussd] summary: ' +
      errors.length + ' errors, ' +
      warns.length + ' warnings, ' +
      infos.length + ' info' +
      (waived.length ? ' (' + waived.length + ' waived)' : '') +
      (!VERBOSE && infos.length ? ' — set LINT_USSD_VERBOSE=1 to show INFO' : '')
  );
}

// ─── runner ─────────────────────────────────────────────────────────────────

function main() {
  const source = fs.readFileSync(TARGET_FILE, 'utf8');
  const sourceLines = source.split('\n');

  let ast;
  try {
    ast = acorn.parse(source, {
      ecmaVersion: 'latest',
      sourceType: 'script',
      locations: true,
    });
  } catch (err) {
    console.error('[lint-ussd] parse error: ' + err.message);
    process.exit(2);
  }

  const screens = extractScreens(ast);
  const records = [];
  for (const screen of screens) {
    let violations = checkScreen(screen);
    violations = applyWaivers(violations, sourceLines, screen);
    for (const v of violations) {
      records.push({
        ...v,
        file: TARGET_FILE,
        startLine: screen.startLine,
        type: screen.type,
        hasInterpolation: screen.hasInterpolation,
      });
    }
  }

  if (JSON_OUTPUT) {
    console.log(JSON.stringify(records, null, 2));
  } else {
    reportText(records, TARGET_FILE, screens.length);
  }

  const failCount = records.filter(r =>
    r.severity === 'ERROR' || (FAIL_ON_WARN && r.severity === 'WARN')
  ).length;
  process.exit(failCount === 0 ? 0 : 1);
}

main();
