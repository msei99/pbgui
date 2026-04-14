(function(global) {
  'use strict';

  function resolveElement(target) {
    if (!target) return null;
    if (typeof target === 'string') return document.getElementById(target);
    return target;
  }

  function captureTextareaAnchor(target) {
    var el = resolveElement(target);
    if (!el || document.activeElement !== el) return null;
    return {
      top: el.getBoundingClientRect().top,
      scrollTop: el.scrollTop,
      selectionStart: el.selectionStart,
      selectionEnd: el.selectionEnd,
    };
  }

  function restoreTextareaAnchor(target, anchor) {
    if (!anchor) return;
    var el = resolveElement(target);
    if (!el) return;
    var delta = el.getBoundingClientRect().top - anchor.top;
    if (delta) window.scrollBy(0, delta);
    el.scrollTop = anchor.scrollTop;
    try {
      el.setSelectionRange(anchor.selectionStart, anchor.selectionEnd);
    } catch (e) {}
  }

  function autoResizeTextarea(target) {
    var el = resolveElement(target);
    if (!el) return;
    var anchor = captureTextareaAnchor(el);
    el.style.height = 'auto';
    el.style.height = el.scrollHeight + 'px';
    restoreTextareaAnchor(el, anchor);
  }

  function openModal(target) {
    var el = resolveElement(target);
    if (el) el.classList.add('open');
    return el;
  }

  function closeModal(target) {
    var el = resolveElement(target);
    if (el) el.classList.remove('open');
    return el;
  }

  function createDebouncedRunner(fn, delay) {
    var timerId = null;
    function schedule() {
      var context = this;
      var args = arguments;
      clearTimeout(timerId);
      timerId = setTimeout(function() {
        timerId = null;
        fn.apply(context, args);
      }, delay);
    }
    schedule.cancel = function() {
      clearTimeout(timerId);
      timerId = null;
    };
    return schedule;
  }

  function escapeHtml(value) {
    if (value === null || value === undefined) return '';
    return String(value)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#39;');
  }

  function clearInlineStatus(target, baseClassName) {
    var el = resolveElement(target);
    if (!el) return null;
    el.className = baseClassName || 'field-status';
    el.innerHTML = '';
    return el;
  }

  function setInlineStatusError(target, opts) {
    var el = resolveElement(target);
    if (!el) return null;
    var baseClassName = opts && opts.baseClassName ? opts.baseClassName : 'field-status';
    var summary = opts && opts.summary ? opts.summary : 'Error';
    var message = opts && opts.message ? opts.message : '';
    var html = '<div class="field-status-main">' + escapeHtml(summary) + '</div>';
    if (message) {
      html += '<div class="field-status-meta">' + escapeHtml(message) + '</div>';
    }
    el.className = baseClassName + ' error';
    el.innerHTML = html;
    return el;
  }

  async function resolveJsonResult(resultOrPromise) {
    var result = await resultOrPromise;
    if (result && typeof result.json === 'function') {
      if (!result.ok) {
        var detail = 'HTTP ' + result.status;
        try {
          var err = await result.json();
          if (err && err.detail) detail = err.detail;
        } catch (e) {
          if (result.statusText) detail = result.statusText;
        }
        throw new Error(detail);
      }
      return result.json();
    }
    return result;
  }

  function normalizeEditorConfigPayload(data, fallbackConfig) {
    var fallback = fallbackConfig && typeof fallbackConfig === 'object' && !Array.isArray(fallbackConfig)
      ? fallbackConfig
      : null;
    if (!data || typeof data !== 'object' || Array.isArray(data)) {
      if (fallback) {
        return { name: '', config: fallback, param_status: {} };
      }
      throw new Error('Invalid editor config payload');
    }

    var hasWrappedConfig = data.config && typeof data.config === 'object' && !Array.isArray(data.config);
    var cfg = hasWrappedConfig ? data.config : data;
    var paramStatus = data.param_status && typeof data.param_status === 'object' && !Array.isArray(data.param_status)
      ? data.param_status
      : {};

    if ((!paramStatus || !Object.keys(paramStatus).length) && data._pbgui_param_status && typeof data._pbgui_param_status === 'object') {
      paramStatus = data._pbgui_param_status;
    }
    if ((!paramStatus || !Object.keys(paramStatus).length) && cfg && cfg._pbgui_param_status && typeof cfg._pbgui_param_status === 'object') {
      paramStatus = cfg._pbgui_param_status;
    }

    if (cfg && cfg._pbgui_param_status) {
      cfg = Object.assign({}, cfg);
      delete cfg._pbgui_param_status;
    }

    if (!cfg || typeof cfg !== 'object' || Array.isArray(cfg)) {
      if (fallback) cfg = fallback;
      else throw new Error('Prepared config missing');
    }

    return {
      name: typeof data.name === 'string' ? data.name : '',
      config: cfg,
      param_status: paramStatus || {},
    };
  }

  async function resolveEditorConfigPayload(resultOrPromise, fallbackConfig) {
    var data = await resolveJsonResult(resultOrPromise);
    return normalizeEditorConfigPayload(data, fallbackConfig);
  }

  function getBalanceCalcApiBase(apiBase) {
    var base = String(apiBase || '');
    if (!base) throw new Error('Missing API base');
    if (/\/api\/balance-calc$/.test(base)) return base;
    return base
      .replace(/\/api\/v7$/, '/api/balance-calc')
      .replace(/\/api\/backtest-v7$/, '/api/balance-calc')
      .replace(/\/v7$/, '/balance-calc')
      .replace(/\/backtest-v7$/, '/balance-calc');
  }

  async function createBalanceCalcDraft(opts) {
    if (!opts || !opts.config || typeof opts.config !== 'object' || Array.isArray(opts.config)) {
      throw new Error('Config must be a JSON object');
    }
    var apiBase = getBalanceCalcApiBase(opts.apiBase);
    var data = await resolveJsonResult(fetch(apiBase + '/draft', {
      method: 'POST',
      headers: {
        'Authorization': 'Bearer ' + (opts.token || ''),
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({ config: opts.config })
    }));
    if (!data || !data.draft_id) {
      throw new Error('Balance Calculator draft creation failed');
    }
    return data.draft_id;
  }

  async function openBalanceCalcPage(opts) {
    var exchange = String((opts && opts.exchange) || '').trim().toLowerCase();
    if (!exchange) throw new Error('Missing exchange');
    var apiBase = getBalanceCalcApiBase(opts && opts.apiBase);
    var draftId = await createBalanceCalcDraft(opts);
    var url = apiBase + '/main_page?token=' + encodeURIComponent((opts && opts.token) || '') +
      '&st_base=' + encodeURIComponent((opts && opts.stBase) || '') +
      '&draft_id=' + encodeURIComponent(draftId) +
      '&exchange=' + encodeURIComponent(exchange);
    if (!opts || opts.navigate !== false) {
      window.location.href = url;
    }
    return { draft_id: draftId, url: url };
  }

  async function requestBalanceCalculation(opts) {
    if (!opts || !opts.config || typeof opts.config !== 'object' || Array.isArray(opts.config)) {
      throw new Error('Config must be a JSON object');
    }
    var exchange = String((opts && opts.exchange) || '').trim().toLowerCase();
    if (!exchange) throw new Error('Missing exchange');
    var apiBase = getBalanceCalcApiBase(opts.apiBase);
    return resolveJsonResult(fetch(apiBase + '/calculate', {
      method: 'POST',
      headers: {
        'Authorization': 'Bearer ' + (opts.token || ''),
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({
        config: opts.config,
        exchange: exchange,
      })
    }));
  }

  function createMultiselectController(opts) {
    opts = opts || {};

    var stateMap = {};
    var counterpartMap = opts.counterpartMap || {};
    var coinIds = opts.coinIds || {};
    var controllerName = opts.controllerName || 'pb-ms';
    var isExclusiveValue = typeof opts.isExclusiveValue === 'function'
      ? opts.isExclusiveValue
      : function() { return false; };
    var onAfterRender = typeof opts.onAfterRender === 'function' ? opts.onAfterRender : null;
    var statusSummaryBuilder = typeof opts.statusSummaryBuilder === 'function' ? opts.statusSummaryBuilder : null;
    var formatTagLabel = typeof opts.formatTagLabel === 'function'
      ? opts.formatTagLabel
      : function(ctx) {
        if (ctx.meta && ctx.meta.status === 'invalid') return '? ' + (ctx.meta.raw || ctx.value);
        return (ctx.meta && ctx.meta.normalized) || ctx.value;
      };
    var formatTagClass = typeof opts.formatTagClass === 'function'
      ? opts.formatTagClass
      : function(ctx) {
        return 'ms-tag' + (ctx.meta && ctx.meta.status === 'invalid' ? ' ms-tag-invalid' : '');
      };
    var formatTagTitle = typeof opts.formatTagTitle === 'function'
      ? opts.formatTagTitle
      : function(ctx) {
        if (!ctx.meta) return '';
        if (ctx.meta.status === 'invalid') {
          return (ctx.meta.changed ? ('Normalized from ' + ctx.meta.raw + ' to ' + ctx.meta.normalized + '. ') : '') +
            'CoinData could not resolve this entry to an active coin.';
        }
        if (ctx.meta.changed) {
          return 'Normalized from ' + ctx.meta.raw + ' to ' + ctx.meta.normalized + '.';
        }
        return '';
      };
    var formatOptionLabel = typeof opts.formatOptionLabel === 'function'
      ? opts.formatOptionLabel
      : function(ctx) {
        return ctx.inOther ? (ctx.value + ' ⇄') : ctx.value;
      };
    var bindAttr = 'data-pb-ms-bound';
    var api = null;

    function getState(id) {
      return stateMap[id] || null;
    }

    function getCoinMeta(id, value) {
      var state = getState(id);
      if (!state || !state.coinMeta) return null;
      return state.coinMeta[value] || null;
    }

    function ensureStatusEl(id) {
      var wrap = resolveElement(id);
      if (!wrap) return null;
      var group = wrap.closest('.form-group');
      if (!group) return null;
      var el = group.querySelector('.ms-status-summary[data-for="' + id + '"]');
      if (!el) {
        el = document.createElement('div');
        el.className = 'ms-status-summary';
        el.setAttribute('data-for', id);
        group.appendChild(el);
      }
      return el;
    }

    function updateStatusSummary(id) {
      if (!coinIds[id]) return;
      var el = ensureStatusEl(id);
      if (!el) return;
      var state = getState(id);
      if (!state || !state.selected.length) {
        el.textContent = '';
        el.style.display = 'none';
        return;
      }

      var invalidCount = 0;
      state.selected.forEach(function(value) {
        var meta = getCoinMeta(id, value);
        if (meta && meta.status === 'invalid') invalidCount += 1;
      });

      if (!invalidCount) {
        el.textContent = '';
        el.style.display = 'none';
        return;
      }

      var summary = statusSummaryBuilder
        ? statusSummaryBuilder({ id: id, invalidCount: invalidCount, state: state, controller: api })
        : { html: '<span class="err">' + invalidCount + ' invalid</span>', display: 'block' };
      if (!summary || !summary.html) {
        el.textContent = '';
        el.style.display = 'none';
        return;
      }
      el.innerHTML = summary.html;
      el.style.display = summary.display || 'block';
    }

    function renderTags(id, renderOpts) {
      var wrap = resolveElement(id);
      if (!wrap) return;
      var inputEl = wrap.querySelector('.ms-input');
      if (!inputEl) return;
      wrap.querySelectorAll('.ms-tag').forEach(function(tagEl) { tagEl.remove(); });

      var state = getState(id);
      if (!state) return;

      state.selected.forEach(function(value) {
        var meta = getCoinMeta(id, value);
        var ctx = { id: id, value: value, meta: meta, state: state, controller: api };
        var tag = document.createElement('span');
        tag.className = formatTagClass(ctx);
        var title = formatTagTitle(ctx);
        if (title) tag.title = title;
        tag.appendChild(document.createTextNode(formatTagLabel(ctx) + ' '));
        var close = document.createElement('span');
        close.className = 'ms-x';
        close.setAttribute('data-val', value);
        close.textContent = '×';
        tag.appendChild(close);
        wrap.insertBefore(tag, inputEl);
      });

      wrap.querySelectorAll('.ms-x').forEach(function(closeEl) {
        closeEl.addEventListener('click', function(e) {
          e.stopPropagation();
          var state = getState(id);
          if (!state) return;
          var value = this.getAttribute('data-val');
          state.selected = state.selected.filter(function(selectedValue) { return selectedValue !== value; });
          renderTags(id);
        });
      });

      updateStatusSummary(id);
      if (onAfterRender && !(renderOpts && renderOpts.silent)) {
        onAfterRender(id, state, renderOpts || {});
      }
    }

    function toggleValue(id, value) {
      var state = getState(id);
      if (!state) return;
      var idx = state.selected.indexOf(value);
      var exclusive = isExclusiveValue(id, value);
      if (exclusive) {
        state.selected = idx >= 0 ? [] : [value];
      } else if (idx >= 0) {
        state.selected.splice(idx, 1);
      } else {
        state.selected = state.selected.filter(function(selectedValue) {
          return !isExclusiveValue(id, selectedValue);
        });
        state.selected.push(value);

        var cpId = counterpartMap[id];
        if (cpId && stateMap[cpId]) {
          var cpIdx = stateMap[cpId].selected.indexOf(value);
          if (cpIdx >= 0) {
            stateMap[cpId].selected.splice(cpIdx, 1);
            renderTags(cpId);
          }
        }
      }
      state.highlightIdx = -1;
      renderTags(id);
    }

    function showDropdown(id, filter) {
      var wrap = resolveElement(id);
      if (!wrap) return;
      var dd = wrap.querySelector('.ms-dropdown');
      if (!dd) return;
      var state = getState(id);
      if (!state) return;

      var cpId = counterpartMap[id];
      var cpSelected = (cpId && stateMap[cpId]) ? stateMap[cpId].selected : [];
      var upperFilter = String(filter || '').toUpperCase();
      var html = '';
      var count = 0;
      var hiIdx = state.highlightIdx !== undefined ? state.highlightIdx : -1;
      var visibleIdx = 0;

      state.options.forEach(function(opt) {
        if (upperFilter && String(opt).toUpperCase().indexOf(upperFilter) < 0) return;
        var selected = state.selected.indexOf(opt) >= 0;
        var exclusive = isExclusiveValue(id, opt);
        var inOther = !exclusive && cpSelected.indexOf(opt) >= 0;
        var highlighted = !selected && (visibleIdx === hiIdx);
        var cls = 'ms-option' +
          (selected ? ' selected' : '') +
          (inOther ? ' in-other' : '') +
          (highlighted ? ' highlighted' : '') +
          (exclusive ? ' ms-opt-all' : '');
        var label = formatOptionLabel({
          id: id,
          value: opt,
          selected: selected,
          inOther: inOther,
          highlighted: highlighted,
          exclusive: exclusive,
          state: state,
          controller: api,
        });
        html += '<div class="' + cls + '" data-val="' + escapeHtml(opt) + '">' + escapeHtml(label) + '</div>';
        if (!selected) visibleIdx += 1;
        count += 1;
      });

      if (!count) {
        html = '<div style="padding:4px 8px;color:var(--text-dim);font-size:var(--fs-xs)">No matches</div>';
      }

      dd.innerHTML = html;
      dd.classList.add('open');
      var highlightedEl = dd.querySelector('.highlighted');
      if (highlightedEl) highlightedEl.scrollIntoView({ block: 'nearest' });

      dd.querySelectorAll('.ms-option').forEach(function(optionEl) {
        optionEl.addEventListener('mousedown', function(e) {
          e.preventDefault();
          toggleValue(id, this.getAttribute('data-val'));
          var inputEl = wrap.querySelector('.ms-input');
          if (inputEl) inputEl.value = '';
          showDropdown(id, '');
        });
      });
    }

    function wire(id) {
      var wrap = resolveElement(id);
      if (!wrap) return;
      var input = wrap.querySelector('.ms-input');
      if (!input) return;

      var boundKey = controllerName + ':' + id;
      if (input.getAttribute(bindAttr) === boundKey) return;
      input.setAttribute(bindAttr, boundKey);

      input.addEventListener('focus', function() {
        showDropdown(id, this.value);
      });
      input.addEventListener('input', function() {
        var state = getState(id);
        if (state) state.highlightIdx = -1;
        showDropdown(id, this.value);
      });
      input.addEventListener('blur', function() {
        var dd = wrap.querySelector('.ms-dropdown');
        this.value = '';
        if (dd) {
          setTimeout(function() {
            dd.classList.remove('open');
            var state = getState(id);
            if (state) state.highlightIdx = -1;
          }, 150);
        }
      });
      input.addEventListener('keydown', function(e) {
        var dd = wrap.querySelector('.ms-dropdown');
        if (!dd || !dd.classList.contains('open')) return;
        var items = dd.querySelectorAll('.ms-option:not(.selected)');
        var state = getState(id);
        if (!state) return;

        if (e.key === 'ArrowDown' || e.key === 'ArrowUp') {
          e.preventDefault();
          if (!items.length) return;
          if (e.key === 'ArrowDown') {
            state.highlightIdx = state.highlightIdx < items.length - 1 ? state.highlightIdx + 1 : 0;
          } else {
            state.highlightIdx = state.highlightIdx > 0 ? state.highlightIdx - 1 : items.length - 1;
          }
          items.forEach(function(itemEl, index) {
            if (index === state.highlightIdx) {
              itemEl.classList.add('highlighted');
              itemEl.scrollIntoView({ block: 'nearest' });
            } else {
              itemEl.classList.remove('highlighted');
            }
          });
        } else if (e.key === 'Enter') {
          e.preventDefault();
          var value = null;
          if (items.length === 1) {
            value = items[0].getAttribute('data-val');
          } else if (state.highlightIdx >= 0 && state.highlightIdx < items.length) {
            value = items[state.highlightIdx].getAttribute('data-val');
          }
          if (value) {
            toggleValue(id, value);
            this.value = '';
            showDropdown(id, '');
          }
        }
      });
    }

    function rebuild(id, options, selected, rebuildOpts) {
      stateMap[id] = {
        options: options.slice(),
        selected: selected.slice(),
        highlightIdx: -1,
        coinMeta: {},
      };
      renderTags(id, rebuildOpts);
      if (!rebuildOpts || rebuildOpts.wire !== false) wire(id);
    }

    function getValues(id) {
      var state = getState(id);
      return state ? state.selected.slice() : [];
    }

    function setValues(id, values, renderOpts) {
      var state = getState(id);
      if (!state) return;
      state.selected = values.slice();
      renderTags(id, renderOpts);
    }

    function clear(id, renderOpts) {
      var state = getState(id);
      if (!state) return;
      state.selected = [];
      renderTags(id, renderOpts);
    }

    function selectAll(id, renderOpts) {
      var state = getState(id);
      if (!state) return;
      var cpId = counterpartMap[id];
      var cpSelected = (cpId && stateMap[cpId]) ? stateMap[cpId].selected : [];
      state.selected = state.options.filter(function(opt) {
        return !isExclusiveValue(id, opt) && cpSelected.indexOf(opt) < 0;
      });
      renderTags(id, renderOpts);
    }

    function clearCoinMeta(id) {
      var state = getState(id);
      if (!state) return;
      state.coinMeta = {};
      renderTags(id, { silent: true });
    }

    function applyCoinStatus(id, statuses) {
      var state = getState(id);
      if (!state) return;

      var nextSelected = [];
      var nextMeta = {};
      state.selected.forEach(function(value) {
        var info = statuses ? statuses[value] : null;
        var nextValue = value;
        if (info && info.status !== 'invalid' && info.normalized) {
          nextValue = info.normalized;
        }
        if (nextSelected.indexOf(nextValue) >= 0) return;
        nextSelected.push(nextValue);
        if (info) {
          nextMeta[nextValue] = {
            status: info.status,
            normalized: info.normalized || nextValue,
            raw: value,
            changed: (info.normalized || nextValue) !== value,
          };
        }
      });

      state.selected = nextSelected;
      state.coinMeta = nextMeta;
      renderTags(id);
    }

    function removeInvalid(ids) {
      var removed = 0;
      var targetIds = Array.isArray(ids) ? ids.slice() : Object.keys(coinIds);
      targetIds.forEach(function(id) {
        var state = getState(id);
        if (!state) return;

        var nextSelected = [];
        var nextMeta = {};
        state.selected.forEach(function(value) {
          var meta = getCoinMeta(id, value);
          if (meta && meta.status === 'invalid') {
            removed += 1;
            return;
          }
          nextSelected.push(value);
          if (meta) nextMeta[value] = meta;
        });

        state.selected = nextSelected;
        state.coinMeta = nextMeta;
        renderTags(id);
      });
      return removed;
    }

    api = {
      state: stateMap,
      counterpartMap: counterpartMap,
      coinIds: coinIds,
      rebuild: rebuild,
      wire: wire,
      showDropdown: showDropdown,
      renderTags: renderTags,
      getValues: getValues,
      setValues: setValues,
      clear: clear,
      selectAll: selectAll,
      getCoinMeta: getCoinMeta,
      clearCoinMeta: clearCoinMeta,
      applyCoinStatus: applyCoinStatus,
      removeInvalid: removeInvalid,
    };

    return api;
  }

  var fixedValidationEntries = {};
  var fixedValidationOrder = [];

  function ensureFixedValidationOverlay() {
    var style = document.getElementById('pb-json-fixed-validation-style');
    if (!style) {
      style = document.createElement('style');
      style.id = 'pb-json-fixed-validation-style';
      style.textContent = [
        '#pb-json-fixed-validation{display:none;position:fixed;top:64px;right:16px;z-index:1400;width:min(520px,calc(100vw - 32px));max-height:calc(100vh - 80px);padding:10px 12px;border:1px solid rgba(255,95,95,0.65);border-left-width:4px;border-radius:6px;background:rgba(43,12,12,0.985);box-shadow:0 14px 36px rgba(0,0,0,0.45);box-sizing:border-box;overflow:auto;font-size:13px;line-height:1.35;color:#ffd7d7}',
        '#pb-json-fixed-validation.pb-json-fixed-open{display:block}',
        '#pb-json-fixed-validation .pb-json-fixed-main{font-weight:700;color:#fff3f3}',
        '#pb-json-fixed-validation .pb-json-fixed-meta{margin-top:4px;color:#ffb3b3}',
        '#pb-json-fixed-validation .pb-json-fixed-actions{margin-top:8px}',
        '#pb-json-fixed-validation .pb-json-fixed-btn{height:26px;padding:0 10px;border:1px solid rgba(255,95,95,0.45);border-radius:4px;background:rgba(255,255,255,0.08);color:#fff1f1;cursor:pointer;font-size:13px}',
        '#pb-json-fixed-validation .pb-json-fixed-btn:hover{background:rgba(255,255,255,0.08)}'
      ].join('');
      document.head.appendChild(style);
    }
    var overlay = document.getElementById('pb-json-fixed-validation');
    if (!overlay) {
      overlay = document.createElement('div');
      overlay.id = 'pb-json-fixed-validation';
      overlay.setAttribute('aria-live', 'polite');
      document.body.appendChild(overlay);
    }
    return overlay;
  }

  function renderFixedValidationOverlay(preferredOwnerId) {
    var overlay = ensureFixedValidationOverlay();
    var ownerId = preferredOwnerId && fixedValidationEntries[preferredOwnerId]
      ? preferredOwnerId
      : (fixedValidationOrder.length ? fixedValidationOrder[fixedValidationOrder.length - 1] : null);
    overlay.textContent = '';
    if (!ownerId) {
      overlay.classList.remove('pb-json-fixed-open');
      return;
    }
    var entry = fixedValidationEntries[ownerId];
    if (!entry) {
      overlay.classList.remove('pb-json-fixed-open');
      return;
    }

    var main = document.createElement('div');
    main.className = 'pb-json-fixed-main';
    main.textContent = entry.summary || 'Invalid JSON';
    overlay.appendChild(main);

    if (entry.message) {
      var meta = document.createElement('div');
      meta.className = 'pb-json-fixed-meta';
      meta.textContent = entry.message;
      overlay.appendChild(meta);
    }

    if (entry.action) {
      var actions = document.createElement('div');
      actions.className = 'pb-json-fixed-actions';
      var button = document.createElement('button');
      button.type = 'button';
      button.className = 'pb-json-fixed-btn';
      button.textContent = entry.actionLabel || 'Reveal line in editor';
      button.addEventListener('click', function() {
        entry.action();
      });
      actions.appendChild(button);
      overlay.appendChild(actions);
    }

    overlay.classList.add('pb-json-fixed-open');
  }

  function setFixedValidationStatus(ownerId, opts) {
    if (!ownerId) return;
    fixedValidationEntries[ownerId] = {
      summary: opts && opts.summary ? opts.summary : 'Invalid JSON',
      message: opts && opts.message ? opts.message : '',
      actionLabel: opts && opts.actionLabel ? opts.actionLabel : 'Reveal line in editor',
      action: opts && typeof opts.action === 'function' ? opts.action : null,
    };
    fixedValidationOrder = fixedValidationOrder.filter(function(id) { return id !== ownerId; });
    fixedValidationOrder.push(ownerId);
    renderFixedValidationOverlay(ownerId);
  }

  function clearFixedValidationStatus(ownerId) {
    if (!ownerId) {
      fixedValidationEntries = {};
      fixedValidationOrder = [];
      renderFixedValidationOverlay(null);
      return;
    }
    delete fixedValidationEntries[ownerId];
    fixedValidationOrder = fixedValidationOrder.filter(function(id) { return id !== ownerId; });
    renderFixedValidationOverlay(null);
  }

  global.PBGuiEditorShared = {
    autoResizeTextarea: autoResizeTextarea,
    captureTextareaAnchor: captureTextareaAnchor,
    restoreTextareaAnchor: restoreTextareaAnchor,
    openModal: openModal,
    closeModal: closeModal,
    createDebouncedRunner: createDebouncedRunner,
    clearInlineStatus: clearInlineStatus,
    setInlineStatusError: setInlineStatusError,
    resolveJsonResult: resolveJsonResult,
    normalizeEditorConfigPayload: normalizeEditorConfigPayload,
    resolveEditorConfigPayload: resolveEditorConfigPayload,
    getBalanceCalcApiBase: getBalanceCalcApiBase,
    createBalanceCalcDraft: createBalanceCalcDraft,
    openBalanceCalcPage: openBalanceCalcPage,
    requestBalanceCalculation: requestBalanceCalculation,
    createMultiselectController: createMultiselectController,
    setFixedValidationStatus: setFixedValidationStatus,
    clearFixedValidationStatus: clearFixedValidationStatus,
  };
})(window);