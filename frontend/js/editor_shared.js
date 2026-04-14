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
    setFixedValidationStatus: setFixedValidationStatus,
    clearFixedValidationStatus: clearFixedValidationStatus,
  };
})(window);