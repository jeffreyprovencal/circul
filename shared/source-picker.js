// shared/source-picker.js
//
// Vanilla-JS source-selector component. Mounted into each paid-dashboard's
// Record Sale form; replaces the implicit FIFO write with an explicit,
// editable prefilled allocation UI. Matches the PR4-B visual spec in
// PR4-MOCKUP-V2.html (deleted on merge; git history preserves).
//
// Public API (exposed as window.SourcePicker):
//   mount(container, opts) → controller
//     opts = { sellerRole, materialType, targetKg, onChange }
//     sellerRole: 'aggregator' | 'processor' | 'recycler'   (required)
//     materialType: current material (string; may change via .update())
//     targetKg: current sale weight (number; may change via .update())
//     onChange({ sources, total_kg, status, canSubmit }) fires on every
//       state change so the host form can drive submit-button disabled.
//
//   controller exposes:
//     .update({ materialType?, targetKg? })   // refetch + re-prefill
//     .refresh()                               // re-fetch + re-prefill
//     .showError(details)                      // render backend 400 details
//     .clearError()                            // used by input edits
//     .getSources()                            // [{source_id, kg}]
//     .destroy()
//
// Design invariants:
//   - Picker always open (no toggle). FIFO prefill is the default interaction,
//     not a hidden advanced mode.
//   - Sum invariant uses gross_weight_kg (matches invariant Check 1 and
//     attributeAndInsert's FIFO path).
//   - seller_role is always passed explicitly to /api/sources — picker does
//     not fall back to "current caller's highest role" so dual-role users
//     always see the tier of the dashboard they're on.
//   - Empty states are tier-aware. Aggregator gets a CTA that dispatches a
//     'source-picker:log-purchase' custom event on the container — the host
//     dashboard owns navigation. Processor/recycler get the honest copy
//     about declared-external material being a scoped follow-up PR.

(function (root) {
  'use strict';

  // ── Utilities ────────────────────────────────────────────────────────────
  function el(tag, attrs, children) {
    var n = document.createElement(tag);
    if (attrs) {
      Object.keys(attrs).forEach(function (k) {
        if (k === 'class') n.className = attrs[k];
        else if (k === 'dataset') {
          Object.keys(attrs[k]).forEach(function (dk) { n.dataset[dk] = attrs[k][dk]; });
        }
        else if (k.indexOf('on') === 0) n.addEventListener(k.slice(2), attrs[k]);
        else if (k === 'html') n.innerHTML = attrs[k];
        else n.setAttribute(k, attrs[k]);
      });
    }
    (children || []).forEach(function (c) {
      if (c == null) return;
      if (typeof c === 'string') n.appendChild(document.createTextNode(c));
      else n.appendChild(c);
    });
    return n;
  }

  function round2(n) { return Math.round(n * 100) / 100; }

  function fmtKg(n) { return (Math.round(n * 10) / 10).toFixed(1); }

  function fmtDate(iso) {
    try {
      var d = new Date(iso);
      return d.toLocaleDateString('en-GB', { day: '2-digit', month: 'short', year: 'numeric' });
    } catch (_e) { return ''; }
  }

  // Batch id → short display string. Falls back to last 8 chars of UUID.
  function batchLabel(batch_id) {
    if (!batch_id) return '';
    var s = String(batch_id);
    return 'B-' + s.slice(-6).toUpperCase();
  }

  function authHeaders() {
    var token = '';
    try { token = localStorage.getItem('circul_token') || ''; } catch (_e) {}
    var h = { 'Content-Type': 'application/json' };
    if (token) h.Authorization = 'Bearer ' + token;
    return h;
  }

  // ── Empty-state copy ─────────────────────────────────────────────────────
  // Keep in sync with PR4-MOCKUP-V2.html. Processor/Recycler copy is
  // deliberately honest about the declared-external gap.
  function emptyCopy(sellerRole, materialType) {
    var mat = (materialType || 'this material').toUpperCase();
    if (sellerRole === 'aggregator') {
      return {
        title: 'No ' + mat + ' inventory tracked in Circul yet',
        body: 'This sale needs upstream inventory to draw from. Log an upstream purchase first — this sale will then pull from it automatically.',
        ctaLabel: 'Log a purchase →',
        ctaEvent: 'source-picker:log-purchase'
      };
    }
    var supplierTier = sellerRole === 'processor' ? 'aggregators' : 'processors';
    return {
      title: 'No eligible ' + mat + ' inventory from supplier ' + supplierTier,
      body: "If you have pre-Circul stock or material from non-Circul suppliers, it can't be attributed through the current flow. A dedicated path for declared-external material is scoped for a follow-up PR — contact admin in the meantime.",
      ctaLabel: null,
      ctaEvent: null
    };
  }

  // ── Picker class ─────────────────────────────────────────────────────────
  function SourcePicker(container, opts) {
    if (!container) throw new Error('SourcePicker: container element is required');
    if (!opts || !opts.sellerRole) throw new Error('SourcePicker: opts.sellerRole is required');
    if (['aggregator','processor','recycler'].indexOf(opts.sellerRole) === -1) {
      throw new Error('SourcePicker: opts.sellerRole must be aggregator|processor|recycler');
    }

    this.container    = container;
    this.sellerRole   = opts.sellerRole;
    this.materialType = opts.materialType || '';
    this.targetKg     = parseFloat(opts.targetKg) || 0;
    this.onChange     = typeof opts.onChange === 'function' ? opts.onChange : function () {};

    // State
    this.candidates   = [];           // array from /api/sources
    this.alloc        = {};           // source_id (Number) → kg (Number)
    this.rowErrors    = {};           // source_id → string
    this.sumError     = null;         // string | null — backend sum mismatch message
    this.state        = 'idle';       // 'idle' | 'loading' | 'ready' | 'empty' | 'error'
    this.fetchToken   = 0;            // race-guard for concurrent /api/sources calls
    this.destroyed    = false;

    this._buildSkeleton();
    if (this.materialType && this.targetKg > 0) {
      this._fetch();
    } else {
      this.state = 'idle';
      this._render();
    }
  }

  SourcePicker.prototype._buildSkeleton = function () {
    this.container.innerHTML = '';
    this.container.classList.add('sp-root');
    this.root = el('div', { class: 'sp-root-inner' });
    this.container.appendChild(this.root);
  };

  // ── .update({ materialType?, targetKg? }) ───────────────────────────────
  SourcePicker.prototype.update = function (partial) {
    if (this.destroyed) return;
    var changed = false;
    if (partial && partial.materialType != null && partial.materialType !== this.materialType) {
      this.materialType = partial.materialType;
      changed = true;
    }
    if (partial && partial.targetKg != null) {
      var tk = parseFloat(partial.targetKg) || 0;
      if (tk !== this.targetKg) { this.targetKg = tk; changed = true; }
    }
    if (changed) {
      this.rowErrors = {};
      this.sumError  = null;
      if (this.materialType && this.targetKg > 0) this._fetch();
      else {
        this.candidates = [];
        this.alloc = {};
        this.state = 'idle';
        this._render();
      }
    }
  };

  // ── .refresh() — re-fetch + re-prefill ──────────────────────────────────
  SourcePicker.prototype.refresh = function () {
    if (this.destroyed) return;
    this.rowErrors = {};
    this.sumError  = null;
    if (this.materialType && this.targetKg > 0) this._fetch();
    else this._render();
  };

  // ── .showError(details) — render backend 400 body ───────────────────────
  // details from InsufficientSourceError payload:
  //   { reason, insufficient_remaining?, invalid_source_ids?, sum_mismatch_kg?, hint_total_kg?, target_kg? }
  SourcePicker.prototype.showError = function (details) {
    if (this.destroyed || !details) return;
    this.rowErrors = {};
    this.sumError  = null;

    if (details.reason === 'invalid_manual_sources') {
      if (Array.isArray(details.insufficient_remaining)) {
        details.insufficient_remaining.forEach(function (e) {
          this.rowErrors[Number(e.id)] =
            'Only ' + fmtKg(Number(e.remaining_kg)) + ' kg remaining (you picked ' +
            fmtKg(Number(e.requested_kg)) + ' kg). Another sale drew from this lot — refresh sources.';
        }.bind(this));
      }
      if (Array.isArray(details.invalid_source_ids)) {
        details.invalid_source_ids.forEach(function (id) {
          if (!this.rowErrors[Number(id)]) {
            this.rowErrors[Number(id)] =
              'This lot is no longer eligible (drained, outside 14-day window, status changed, or wrong material). Refresh sources.';
          }
        }.bind(this));
      }
      if (details.sum_mismatch_kg != null) {
        var delta = Number(details.sum_mismatch_kg);
        var hint  = Number(details.hint_total_kg);
        var tgt   = Number(details.target_kg);
        this.sumError = delta > 0
          ? 'Your picked sources sum to ' + fmtKg(hint) + ' kg but the sale target is ' + fmtKg(tgt) + ' kg. Reduce one or more allocations by ' + fmtKg(delta) + ' kg so the totals match the target weight.'
          : 'Your picked sources sum to ' + fmtKg(hint) + ' kg but the sale target is ' + fmtKg(tgt) + ' kg. Increase one or more allocations by ' + fmtKg(Math.abs(delta)) + ' kg so the totals match the target weight.';
      }
    } else if (details.reason === 'shortfall') {
      // Backend FIFO found the seller has insufficient inventory.
      // Treat as empty-state fallback (same tier-aware copy).
      this.state = 'empty';
    }
    this._render();
  };

  // ── .clearError() ─────────────────────────────────────────────────────────
  SourcePicker.prototype.clearError = function () {
    if (this.destroyed) return;
    if (Object.keys(this.rowErrors).length || this.sumError) {
      this.rowErrors = {};
      this.sumError  = null;
      this._render();
    }
  };

  // ── .getSources() ────────────────────────────────────────────────────────
  SourcePicker.prototype.getSources = function () {
    var self = this;
    return Object.keys(this.alloc)
      .map(function (k) { return Number(k); })
      .filter(function (id) { return self.alloc[id] > 0; })
      .map(function (id) { return { source_id: id, kg: round2(self.alloc[id]) }; });
  };

  // ── .destroy() ───────────────────────────────────────────────────────────
  SourcePicker.prototype.destroy = function () {
    this.destroyed = true;
    if (this.container) {
      this.container.innerHTML = '';
      this.container.classList.remove('sp-root');
    }
  };

  // ── Internal: fetch /api/sources with race-guard ─────────────────────────
  SourcePicker.prototype._fetch = function () {
    var self = this;
    var token = ++this.fetchToken;
    this.state = 'loading';
    this._render();

    var url = '/api/sources?material_type=' + encodeURIComponent(this.materialType) +
              '&seller_role=' + encodeURIComponent(this.sellerRole);

    fetch(url, { headers: authHeaders() })
      .then(function (r) {
        if (!r.ok) return r.json().then(function (j) { throw new Error(j.message || ('HTTP ' + r.status)); }, function () { throw new Error('HTTP ' + r.status); });
        return r.json();
      })
      .then(function (rows) {
        if (self.destroyed || token !== self.fetchToken) return; // stale
        self.candidates = Array.isArray(rows) ? rows : [];
        self.state = self.candidates.length > 0 ? 'ready' : 'empty';
        self._applyFifoPrefill();
        self._render();
      })
      .catch(function (err) {
        if (self.destroyed || token !== self.fetchToken) return;
        self.candidates = [];
        self.alloc = {};
        self.state = 'error';
        self.sumError = 'Failed to load candidate sources: ' + (err.message || 'unknown error');
        self._render();
      });
  };

  // ── Internal: FIFO prefill from current candidates + targetKg ───────────
  SourcePicker.prototype._applyFifoPrefill = function () {
    this.alloc = {};
    if (this.targetKg <= 0) return;
    var remaining = this.targetKg;
    for (var i = 0; i < this.candidates.length && remaining > 0.001; i++) {
      var c = this.candidates[i];
      var avail = parseFloat(c.remaining_kg);
      if (!(avail > 0)) continue;
      var take = Math.min(avail, remaining);
      take = round2(take);
      if (take <= 0) continue;
      this.alloc[Number(c.source_id)] = take;
      remaining = round2(remaining - take);
    }
  };

  // ── Internal: compute allocation totals + submit-state ──────────────────
  SourcePicker.prototype._allocationSummary = function () {
    var self = this;
    var total = 0;
    Object.keys(this.alloc).forEach(function (k) {
      var v = parseFloat(self.alloc[k]);
      if (v > 0) total += v;
    });
    total = round2(total);
    var delta = round2(total - this.targetKg);
    var pickedCount = Object.keys(this.alloc).filter(function (k) { return self.alloc[k] > 0; }).length;
    var hasRowErr   = Object.keys(this.rowErrors).length > 0;
    var hasSumErr   = !!this.sumError;

    var status;
    if (this.state === 'loading')       status = 'loading';
    else if (this.state === 'empty')    status = 'empty';
    else if (this.state === 'error')    status = 'error';
    else if (this.state === 'idle')     status = 'idle';
    else if (hasSumErr)                 status = 'sum_error';
    else if (hasRowErr)                 status = 'row_error';
    else if (pickedCount === 0)         status = 'under';
    else if (Math.abs(delta) < 0.01)    status = 'balanced';
    else if (delta > 0)                 status = 'over';
    else                                status = 'under';

    var canSubmit = status === 'balanced';

    return {
      total_kg: total,
      delta_kg: delta,
      picked_count: pickedCount,
      status: status,
      canSubmit: canSubmit
    };
  };

  // ── Internal: emit onChange ─────────────────────────────────────────────
  SourcePicker.prototype._emit = function () {
    if (this.destroyed) return;
    var summary = this._allocationSummary();
    this.onChange({
      sources: this.getSources(),
      total_kg: summary.total_kg,
      status: summary.status,
      canSubmit: summary.canSubmit
    });
  };

  // ── Internal: render ────────────────────────────────────────────────────
  SourcePicker.prototype._render = function () {
    if (this.destroyed) return;
    this.root.innerHTML = '';

    var header = el('div', { class: 'sp-head' }, [
      el('div', { class: 'sp-head-title' }, ['Sources']),
      el('div', { class: 'sp-head-sub' }, [this._headerSub()])
    ]);
    this.root.appendChild(header);

    // Body by state
    if (this.state === 'idle')    this.root.appendChild(this._renderIdle());
    else if (this.state === 'loading') this.root.appendChild(this._renderLoading());
    else if (this.state === 'empty')   this.root.appendChild(this._renderEmpty());
    else if (this.state === 'error')   this.root.appendChild(this._renderError());
    else                               this.root.appendChild(this._renderLots());

    // Totals + footer — only when we have candidates (ready/error-with-rows)
    if (this.state === 'ready') {
      this.root.appendChild(this._renderTotals());
      if (this.sumError) this.root.appendChild(this._renderSumError());
      this.root.appendChild(this._renderFooter());
    }

    this._emit();
  };

  SourcePicker.prototype._headerSub = function () {
    if (this.state === 'loading') return 'Fetching eligible sources…';
    if (this.state === 'idle')    return 'Select material and target weight to see eligible sources.';
    if (this.state === 'empty' || this.state === 'error') return '14-day window, upstream tier only.';
    var s = this._allocationSummary();
    if (s.picked_count === 0) return 'No lots picked yet.';
    return 'FIFO-prefilled · ' + fmtKg(s.total_kg) + ' kg across ' + s.picked_count + ' lot' + (s.picked_count > 1 ? 's' : '') + '. Edit below to override.';
  };

  SourcePicker.prototype._renderIdle = function () {
    return el('div', { class: 'sp-empty' }, [
      el('div', { class: 'sp-empty-title' }, ['Pick a material and target weight']),
      el('div', { class: 'sp-empty-body' }, ['Sources will load automatically once material and weight are set.'])
    ]);
  };

  SourcePicker.prototype._renderLoading = function () {
    var wrap = el('div', { class: 'sp-lots' });
    for (var i = 0; i < 3; i++) {
      wrap.appendChild(el('div', { class: 'sp-skel' }, [
        el('div', { class: 'sp-skel-box sp-skel-cb' }),
        el('div', { class: 'sp-skel-col' }, [
          el('div', { class: 'sp-skel-box sp-skel-md' }),
          el('div', { class: 'sp-skel-box sp-skel-lg' })
        ]),
        el('div', { class: 'sp-skel-box sp-skel-alloc' })
      ]));
    }
    return wrap;
  };

  SourcePicker.prototype._renderEmpty = function () {
    var copy = emptyCopy(this.sellerRole, this.materialType);
    var children = [
      el('div', { class: 'sp-empty-title' }, [copy.title]),
      el('div', { class: 'sp-empty-body' }, [copy.body])
    ];
    if (copy.ctaLabel) {
      var self = this;
      children.push(el('div', { class: 'sp-empty-cta' }, [
        el('button', {
          type: 'button',
          class: 'sp-btn-cta',
          onclick: function () {
            self.container.dispatchEvent(new CustomEvent(copy.ctaEvent, { bubbles: true }));
          }
        }, [copy.ctaLabel])
      ]));
    }
    return el('div', { class: 'sp-empty' }, children);
  };

  SourcePicker.prototype._renderError = function () {
    return el('div', { class: 'sp-empty' }, [
      el('div', { class: 'sp-empty-title' }, ['Could not load sources']),
      el('div', { class: 'sp-empty-body' }, [this.sumError || 'Unknown error.']),
      el('div', { class: 'sp-empty-cta' }, [
        el('button', {
          type: 'button',
          class: 'sp-btn-cta',
          onclick: function () { this.refresh(); }.bind(this)
        }, ['Retry'])
      ])
    ]);
  };

  SourcePicker.prototype._renderLots = function () {
    var self = this;
    var wrap = el('div', { class: 'sp-lots' });
    this.candidates.forEach(function (c) {
      var id = Number(c.source_id);
      var picked = self.alloc[id] > 0;
      var errMsg = self.rowErrors[id];
      var pickedClass = picked ? ' sp-lot-picked' : '';
      var errClass    = errMsg ? ' sp-lot-error' : '';

      var cb = el('input', { type: 'checkbox', class: 'sp-cb' });
      if (picked)  cb.checked  = true;
      if (errMsg)  cb.disabled = true;
      cb.addEventListener('change', function () { self._togglePick(id); });

      var pillClass = 'sp-pill sp-pill-' + (c.material_type || '').toLowerCase();
      var meta = el('div', { class: 'sp-lot-meta' }, [
        el('div', { class: 'sp-lot-supplier' }, [c.supplier_name || '(unknown supplier)']),
        el('div', { class: 'sp-lot-line2' }, [
          el('span', { class: pillClass }, [c.material_type || '—']),
          el('span', { class: 'sp-tier-pill' }, [(c.supplier_role || '').toUpperCase() || '—']),
          el('span', null, [fmtKg(parseFloat(c.remaining_kg)) + ' kg remaining']),
          el('span', null, [fmtDate(c.created_at)]),
          el('span', { class: 'sp-mono' }, [batchLabel(c.batch_id)])
        ])
      ]);

      var allocInput = el('input', {
        type: 'number',
        class: 'sp-alloc-input',
        step: '0.1',
        min: '0',
        max: String(parseFloat(c.remaining_kg))
      });
      allocInput.value = picked ? fmtKg(parseFloat(self.alloc[id])) : '0.0';
      if (!picked || errMsg) allocInput.disabled = true;
      allocInput.addEventListener('input', function (ev) { self._updateAlloc(id, ev.target.value); });

      var allocWrap = el('div', { class: 'sp-alloc' }, [
        allocInput,
        el('span', { class: 'sp-alloc-unit' }, ['kg'])
      ]);

      var rowChildren = [cb, meta, allocWrap];
      if (errMsg) rowChildren.push(el('div', { class: 'sp-lot-err' }, [errMsg]));

      wrap.appendChild(el('div', { class: 'sp-lot' + pickedClass + errClass }, rowChildren));
    });
    return wrap;
  };

  SourcePicker.prototype._renderTotals = function () {
    var s = this._allocationSummary();
    var stateClass = ' sp-totals-' + s.status;
    var stateLabel;
    if (s.status === 'balanced')       stateLabel = 'Balanced';
    else if (s.status === 'over')      stateLabel = 'Over by ' + fmtKg(s.delta_kg) + ' kg';
    else if (s.status === 'under')     stateLabel = s.picked_count === 0 ? 'Nothing picked' : 'Under by ' + fmtKg(Math.abs(s.delta_kg)) + ' kg';
    else if (s.status === 'row_error') stateLabel = 'Row rejected';
    else if (s.status === 'sum_error') stateLabel = 'Sum rejected';
    else                               stateLabel = 'Allocation';

    return el('div', { class: 'sp-totals' + stateClass }, [
      el('span', { class: 'sp-totals-state' }, [stateLabel]),
      el('span', { class: 'sp-totals-num' }, [
        fmtKg(s.total_kg) + ' / ' + fmtKg(this.targetKg) + ' kg'
      ])
    ]);
  };

  SourcePicker.prototype._renderSumError = function () {
    return el('div', { class: 'sp-inline-err' }, [
      el('div', { class: 'sp-inline-err-title' }, ['Server: allocation sum does not match target']),
      el('div', { class: 'sp-inline-err-body' }, [this.sumError])
    ]);
  };

  SourcePicker.prototype._renderFooter = function () {
    var self = this;
    var leftBtns = el('div', { class: 'sp-footer-left' });

    var hasRowErr = Object.keys(this.rowErrors).length > 0;
    if (hasRowErr) {
      leftBtns.appendChild(el('button', {
        type: 'button',
        class: 'sp-btn-link',
        onclick: function () { self.refresh(); }
      }, ['Refresh sources']));
    } else {
      leftBtns.appendChild(el('button', {
        type: 'button',
        class: 'sp-btn-link',
        onclick: function () { self._resetFifo(); }
      }, ['Reset to FIFO']));
    }

    var legend = el('div', { class: 'sp-legend' }, [
      el('span', null, ['FIFO is the default']),
      el('span', null, ['Lots expire after 14 days'])
    ]);

    return el('div', { class: 'sp-footer' }, [leftBtns, legend]);
  };

  // ── Internal: user interactions ─────────────────────────────────────────
  SourcePicker.prototype._togglePick = function (id) {
    if (this.rowErrors[id]) return;
    id = Number(id);
    var cand = null;
    for (var i = 0; i < this.candidates.length; i++) {
      if (Number(this.candidates[i].source_id) === id) { cand = this.candidates[i]; break; }
    }
    if (!cand) return;
    this.clearError();
    if (this.alloc[id] > 0) delete this.alloc[id];
    else this.alloc[id] = parseFloat(cand.remaining_kg);
    this._render();
  };

  SourcePicker.prototype._updateAlloc = function (id, val) {
    id = Number(id);
    var n = parseFloat(val);
    if (!(n > 0)) delete this.alloc[id];
    else this.alloc[id] = round2(n);
    this.clearError();
    this._render();
  };

  SourcePicker.prototype._resetFifo = function () {
    this.rowErrors = {};
    this.sumError  = null;
    this._applyFifoPrefill();
    this._render();
  };

  // ── Public mount() wrapper ──────────────────────────────────────────────
  var SourcePickerAPI = {
    mount: function (container, opts) {
      return new SourcePicker(container, opts);
    }
  };

  if (typeof module !== 'undefined' && module.exports) {
    module.exports = SourcePickerAPI;
  } else {
    root.SourcePicker = SourcePickerAPI;
  }
})(typeof self !== 'undefined' ? self : this);
