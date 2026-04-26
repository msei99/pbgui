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

  function formatJsonParseMessage(message) {
    if (!message) return 'Invalid JSON';
    return String(message)
      .replace(/\s+at position \d+(?:\s+\(line \d+ column \d+\))?/i, '')
      .replace(/^JSON\.parse:\s*/i, '')
      .trim();
  }

  function getLineColumnFromJsonPos(raw, pos) {
    if (!Number.isFinite(pos) || pos < 0) return { line: null, column: null };
    var text = typeof raw === 'string' ? raw : '';
    var line = 1;
    var column = 1;
    var limit = Math.min(pos, text.length);
    for (var i = 0; i < limit; i++) {
      if (text.charCodeAt(i) === 10) {
        line += 1;
        column = 1;
      } else {
        column += 1;
      }
    }
    return { line: line, column: column };
  }

  function getJsonErrorLocation(raw, error) {
    var message = error && error.message ? error.message : String(error || '');
    var lineColMatch = message.match(/line\s+(\d+)\s+column\s+(\d+)/i);
    if (lineColMatch) {
      return {
        line: parseInt(lineColMatch[1], 10),
        column: parseInt(lineColMatch[2], 10),
      };
    }
    var posMatch = message.match(/position\s+(\d+)/i);
    if (!posMatch) return { line: null, column: null };
    return getLineColumnFromJsonPos(raw, parseInt(posMatch[1], 10));
  }

  function findJsonSyntaxError(raw) {
    var text = typeof raw === 'string' ? raw : '';
    var idx = 0;
    var len = text.length;

    function fail(pos, message) {
      throw { pos: pos, message: message };
    }

    function skipWhitespace() {
      while (idx < len) {
        var ch = text.charCodeAt(idx);
        if (ch === 9 || ch === 10 || ch === 13 || ch === 32) idx += 1;
        else break;
      }
    }

    function parseValue() {
      skipWhitespace();
      if (idx >= len) fail(idx, 'Unexpected end of JSON');
      var ch = text[idx];
      if (ch === '{') return parseObject();
      if (ch === '[') return parseArray();
      if (ch === '"') return parseString();
      if (ch === '-' || (ch >= '0' && ch <= '9')) return parseNumber();
      if (text.slice(idx, idx + 4) === 'true') { idx += 4; return; }
      if (text.slice(idx, idx + 5) === 'false') { idx += 5; return; }
      if (text.slice(idx, idx + 4) === 'null') { idx += 4; return; }
      fail(idx, 'Unexpected token');
    }

    function parseString() {
      idx += 1;
      while (idx < len) {
        var ch = text[idx];
        if (ch === '"') {
          idx += 1;
          return;
        }
        if (ch === '\\') {
          idx += 1;
          if (idx >= len) fail(idx, 'Unterminated escape sequence in string');
          var escCh = text[idx];
          if ('"\\/bfnrt'.indexOf(escCh) >= 0) {
            idx += 1;
            continue;
          }
          if (escCh === 'u') {
            idx += 1;
            for (var i = 0; i < 4; i++) {
              var code = text[idx + i];
              if (!code || !/[0-9a-fA-F]/.test(code)) fail(idx + i, 'Invalid Unicode escape in string');
            }
            idx += 4;
            continue;
          }
          fail(idx, 'Invalid escape sequence in string');
        }
        if (ch === '\n' || ch === '\r') fail(idx, 'Unterminated string literal');
        idx += 1;
      }
      fail(idx, 'Unterminated string literal');
    }

    function parseNumber() {
      if (text[idx] === '-') idx += 1;
      if (idx >= len) fail(idx, 'Invalid number');
      if (text[idx] === '0') {
        idx += 1;
      } else if (text[idx] >= '1' && text[idx] <= '9') {
        while (idx < len && text[idx] >= '0' && text[idx] <= '9') idx += 1;
      } else {
        fail(idx, 'Invalid number');
      }
      if (text[idx] === '.') {
        idx += 1;
        if (idx >= len || text[idx] < '0' || text[idx] > '9') fail(idx, 'Invalid number');
        while (idx < len && text[idx] >= '0' && text[idx] <= '9') idx += 1;
      }
      if (text[idx] === 'e' || text[idx] === 'E') {
        idx += 1;
        if (text[idx] === '+' || text[idx] === '-') idx += 1;
        if (idx >= len || text[idx] < '0' || text[idx] > '9') fail(idx, 'Invalid number exponent');
        while (idx < len && text[idx] >= '0' && text[idx] <= '9') idx += 1;
      }
    }

    function parseArray() {
      idx += 1;
      skipWhitespace();
      if (idx >= len) fail(idx, 'Unterminated array');
      if (text[idx] === ']') {
        idx += 1;
        return;
      }
      while (idx < len) {
        parseValue();
        skipWhitespace();
        if (idx >= len) fail(idx, 'Unterminated array');
        if (text[idx] === ',') {
          idx += 1;
          skipWhitespace();
          if (idx >= len) fail(idx, 'Unexpected end of JSON after array comma');
          if (text[idx] === ']') fail(idx, 'Expected value after array comma');
          continue;
        }
        if (text[idx] === ']') {
          idx += 1;
          return;
        }
        fail(idx, 'Expected comma or closing bracket after array element');
      }
      fail(idx, 'Unterminated array');
    }

    function parseObject() {
      idx += 1;
      skipWhitespace();
      if (idx >= len) fail(idx, 'Unterminated object');
      if (text[idx] === '}') {
        idx += 1;
        return;
      }
      while (idx < len) {
        skipWhitespace();
        if (text[idx] !== '"') fail(idx, 'Expected double-quoted property name in JSON');
        parseString();
        skipWhitespace();
        if (idx >= len || text[idx] !== ':') fail(idx, 'Expected colon after property name');
        idx += 1;
        parseValue();
        skipWhitespace();
        if (idx >= len) fail(idx, 'Unterminated object');
        if (text[idx] === ',') {
          idx += 1;
          skipWhitespace();
          if (idx >= len) fail(idx, 'Unexpected end of JSON after object comma');
          if (text[idx] === '}') fail(idx, 'Expected property name after object comma');
          continue;
        }
        if (text[idx] === '}') {
          idx += 1;
          return;
        }
        fail(idx, 'Expected comma or closing brace after property value');
      }
      fail(idx, 'Unterminated object');
    }

    try {
      skipWhitespace();
      parseValue();
      skipWhitespace();
      if (idx < len) fail(idx, 'Unexpected token after end of JSON');
      return null;
    } catch (error) {
      return error && Number.isFinite(error.pos) ? error : null;
    }
  }

  function getJsonLineDetail(raw, line, column) {
    if (!raw || !line || line < 1) return null;
    var lines = raw.split('\n');
    if (line > lines.length) return null;
    var lineText = lines[line - 1].replace(/\r$/, '');
    var safeColumn = Math.max(1, Math.min(column || 1, lineText.length + 1));
    var lineStart = 0;
    for (var i = 0; i < line - 1; i++) lineStart += lines[i].length + 1;
    var lineEnd = lineStart + lineText.length;
    return {
      lineText: lineText,
      lineStart: lineStart,
      lineEnd: lineEnd,
      selectionStart: Math.max(lineStart, Math.min(lineEnd, lineStart + safeColumn - 1)),
      selectionEnd: lineEnd,
      column: safeColumn,
      line: line,
    };
  }

  function validateJsonText(raw, opts) {
    opts = opts || {};
    var text = typeof raw === 'string' ? raw : '';
    if (!text.trim()) {
      return {
        parsed: null,
        error: { line: 1, column: 1, message: opts.emptyMessage || 'JSON cannot be empty' }
      };
    }
    try {
      var parsed = JSON.parse(text);
      if (opts.expectObject && (!parsed || typeof parsed !== 'object' || Array.isArray(parsed))) {
        return {
          parsed: null,
          error: { line: null, column: null, message: 'Top-level JSON value must be an object' }
        };
      }
      return { parsed: parsed, error: null };
    } catch (error) {
      var location = getJsonErrorLocation(text, error);
      var fallbackError = (!location.line || !location.column) ? findJsonSyntaxError(text) : null;
      if (fallbackError) {
        var fallbackLocation = getLineColumnFromJsonPos(text, fallbackError.pos);
        location = {
          line: fallbackLocation.line,
          column: fallbackLocation.column,
        };
      }
      return {
        parsed: null,
        error: {
          line: location.line,
          column: location.column,
          message: fallbackError && fallbackError.message
            ? fallbackError.message
            : formatJsonParseMessage(error && error.message ? error.message : error),
        }
      };
    }
  }

  function buildLineHighlightHtml(text, errorLine) {
    return String(text || '').split('\n').map(function(line, index) {
      var lineEsc = escapeHtml(line);
      if (!lineEsc) lineEsc = '&nbsp;';
      var style = 'display:block';
      if (errorLine === index + 1) {
        style += ';background:rgba(255,75,75,0.16);box-shadow:inset 3px 0 0 rgba(255,75,75,0.95);border-radius:2px';
      }
      return '<span style="' + style + '">' + lineEsc + '</span>';
    }).join('');
  }

  function ensureExistingHighlightOverlay(textareaTarget, overlayTarget) {
    var textarea = resolveElement(textareaTarget);
    var overlay = resolveElement(overlayTarget);
    if (!textarea || !overlay) return null;
    var cs = window.getComputedStyle(textarea);
    overlay.style.cssText = [
      'display:block',
      'position:absolute', 'top:0', 'left:0', 'right:0', 'bottom:0',
      'margin:0',
      'padding:' + cs.padding,
      'font-family:' + cs.fontFamily,
      'font-size:' + cs.fontSize,
      'line-height:' + cs.lineHeight,
      'white-space:pre-wrap',
      'word-wrap:break-word',
      'overflow:hidden',
      'pointer-events:none',
      'background:transparent',
      'border:1px solid transparent',
      'box-sizing:border-box',
      'color:transparent',
      'tab-size:' + (cs.tabSize || '4'),
      'z-index:2'
    ].join(';');
    if (!textarea.dataset.pbExistingHighlightBound) {
      textarea.dataset.pbExistingHighlightBound = '1';
      textarea.addEventListener('scroll', function() {
        overlay.scrollTop = textarea.scrollTop;
      });
    }
    overlay.scrollTop = textarea.scrollTop;
    return overlay;
  }

  function syncExistingHighlightOverlay(opts) {
    opts = opts || {};
    var textarea = resolveElement(opts.textarea);
    var overlay = ensureExistingHighlightOverlay(textarea, opts.overlay);
    if (!textarea || !overlay) return null;
    if (!opts.errorLine) {
      overlay.innerHTML = '';
      overlay.style.display = 'none';
      return overlay;
    }
    overlay.innerHTML = buildLineHighlightHtml(opts.text != null ? opts.text : textarea.value, opts.errorLine);
    overlay.style.display = 'block';
    return overlay;
  }

  function ensureWrappedHighlightOverlay(target, opts) {
    var textarea = resolveElement(target);
    opts = opts || {};
    if (!textarea) return null;
    var anchor = captureTextareaAnchor(textarea);
    var wrapperClass = opts.wrapperClass || 'json-highlight-wrap';
    var preClass = opts.preClass || 'json-highlight-pre';
    var wrapper = textarea.parentNode;
    if (!wrapper || !wrapper.classList || !wrapper.classList.contains(wrapperClass)) {
      var parent = textarea.parentNode;
      var nextWrapper = document.createElement('div');
      nextWrapper.className = wrapperClass;
      parent.insertBefore(nextWrapper, textarea);
      nextWrapper.appendChild(textarea);
      wrapper = nextWrapper;
    }
    var pre = textarea._jsonHighlightPre || wrapper.querySelector('.' + preClass);
    if (!pre) {
      var cs = window.getComputedStyle(textarea);
      pre = document.createElement('pre');
      pre.className = preClass;
      pre.setAttribute('aria-hidden', 'true');
      pre.style.cssText = [
        'position:absolute', 'top:0', 'left:0', 'right:0', 'bottom:0',
        'margin:0',
        'padding:' + cs.padding,
        'font-family:' + cs.fontFamily,
        'font-size:' + cs.fontSize,
        'line-height:' + cs.lineHeight,
        'white-space:pre-wrap',
        'word-wrap:break-word',
        'overflow:hidden',
        'pointer-events:none',
        'background:transparent',
        'border:1px solid transparent',
        'box-sizing:border-box',
        'color:transparent',
        'z-index:0'
      ].join(';');
      wrapper.insertBefore(pre, textarea);
      textarea.addEventListener('scroll', function() {
        if (textarea._jsonHighlightPre) textarea._jsonHighlightPre.scrollTop = textarea.scrollTop;
      });
    }
    textarea._jsonHighlightPre = pre;
    restoreTextareaAnchor(textarea, anchor);
    return pre;
  }

  function syncWrappedHighlightOverlay(target, opts) {
    var textarea = resolveElement(target);
    opts = opts || {};
    if (!textarea) return null;
    var pre = ensureWrappedHighlightOverlay(textarea, opts);
    if (!pre) return null;
    if (!opts.errorLine) {
      pre.innerHTML = '';
      pre.style.display = 'none';
      textarea.style.background = '';
      textarea.style.color = '';
      textarea.style.caretColor = '';
      textarea.style.position = '';
      textarea.style.zIndex = '';
      return pre;
    }
    try {
      var cs = window.getComputedStyle(textarea);
      pre.innerHTML = buildLineHighlightHtml(opts.text != null ? opts.text : textarea.value, opts.errorLine);
      textarea.style.position = 'relative';
      textarea.style.zIndex = '1';
      textarea.style.background = 'transparent';
      textarea.style.color = cs.color;
      textarea.style.caretColor = cs.color;
      pre.style.display = 'block';
      pre.style.height = textarea.offsetHeight + 'px';
      pre.scrollTop = textarea.scrollTop;
    } catch (e) {
      pre.innerHTML = '';
      pre.style.display = 'none';
      textarea.style.color = '';
      textarea.style.background = '';
      textarea.style.caretColor = '';
      textarea.style.position = '';
      textarea.style.zIndex = '';
    }
    return pre;
  }

  function focusJsonErrorLocation(target, detail) {
    var textarea = resolveElement(target);
    if (!textarea || !detail) return;
    var resolved = detail;
    if ((!Number.isFinite(resolved.selectionStart) || !Number.isFinite(resolved.selectionEnd)) && resolved.line != null) {
      resolved = getJsonLineDetail(textarea.value || '', resolved.line, resolved.column);
      if (!resolved) return;
    }
    textarea.focus();
    if (Number.isFinite(resolved.selectionStart) && Number.isFinite(resolved.selectionEnd)) {
      try {
        textarea.setSelectionRange(resolved.selectionStart, resolved.selectionEnd);
      } catch (e) {}
    }
    if (!Number.isFinite(resolved.line) || resolved.line < 1) return;
    var style = window.getComputedStyle(textarea);
    var lineHeight = parseFloat(style.lineHeight) || 20;
    var paddingTop = parseFloat(style.paddingTop) || 0;
    var targetTop = window.scrollY + textarea.getBoundingClientRect().top + paddingTop + Math.max(0, resolved.line - 2) * lineHeight - 120;
    window.scrollTo({ top: Math.max(0, targetTop), behavior: 'smooth' });
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

  function normalizePreviewPath(value) {
    return String(value || '').trim().replace(/\\/g, '/').replace(/\/+$/g, '');
  }

  function uniqPreviewStrings(values) {
    var out = [];
    var seen = Object.create(null);
    (values || []).forEach(function(value) {
      var normalized = String(value || '').trim();
      if (!normalized) return;
      if (seen[normalized]) return;
      seen[normalized] = true;
      out.push(normalized);
    });
    return out;
  }

  function summarizePreviewList(values, limit) {
    var items = uniqPreviewStrings(values || []);
    if (!items.length) return 'None';
    var maxItems = Math.max(1, limit || 3);
    if (items.length <= maxItems) return items.join(', ');
    return items.slice(0, maxItems).join(', ') + ' +' + (items.length - maxItems);
  }

  function parsePreviewDate(value) {
    var raw = String(value || '').trim();
    if (!raw) return null;
    if (raw.toLowerCase() === 'now') {
      var today = new Date();
      return new Date(today.getFullYear(), today.getMonth(), today.getDate());
    }
    var parsed = new Date(raw);
    if (isNaN(parsed.getTime())) return null;
    return new Date(parsed.getFullYear(), parsed.getMonth(), parsed.getDate());
  }

  function getPreviewDaySpan(startValue, endValue) {
    var startDate = parsePreviewDate(startValue);
    var endDate = parsePreviewDate(endValue);
    if (!startDate || !endDate) return null;
    var diff = endDate.getTime() - startDate.getTime();
    if (diff < 0) return null;
    return Math.floor(diff / 86400000) + 1;
  }

  function formatPreviewRange(startValue, endValue) {
    var startText = String(startValue || '').trim() || 'unset';
    var endRaw = String(endValue || '').trim();
    var endText = endRaw || 'unset';
    return startText + ' -> ' + endText;
  }

  function normalizePreviewCoinList(value) {
    if (!Array.isArray(value)) return [];
    return value.map(function(item) {
      return String(item || '').trim();
    }).filter(function(item) {
      return !!item;
    });
  }

  function summarizeApprovedCoins(approvedCoins) {
    var longAll = false;
    var shortAll = false;
    var longCoins = [];
    var shortCoins = [];

    if (typeof approvedCoins === 'string') {
      if (approvedCoins.trim().toLowerCase() === 'all') {
        longAll = true;
        shortAll = true;
      }
    } else if (Array.isArray(approvedCoins)) {
      longCoins = normalizePreviewCoinList(approvedCoins);
    } else if (approvedCoins && typeof approvedCoins === 'object') {
      if (typeof approvedCoins.long === 'string' && approvedCoins.long.trim().toLowerCase() === 'all') {
        longAll = true;
      } else {
        longCoins = normalizePreviewCoinList(approvedCoins.long);
      }
      if (typeof approvedCoins.short === 'string' && approvedCoins.short.trim().toLowerCase() === 'all') {
        shortAll = true;
      } else {
        shortCoins = normalizePreviewCoinList(approvedCoins.short);
      }
    }

    var uniqueCoins = uniqPreviewStrings(longCoins.concat(shortCoins));
    var label = 'Derived from filters/defaults';
    if (longAll && shortAll) {
      label = 'All approved coins';
    } else if (longAll || shortAll) {
      label = (longAll ? 'long: all approved' : 'long: ' + longCoins.length + ' explicit') +
        ' | ' +
        (shortAll ? 'short: all approved' : 'short: ' + shortCoins.length + ' explicit');
    } else if (uniqueCoins.length) {
      label = uniqueCoins.length + ' explicit approved coin' + (uniqueCoins.length === 1 ? '' : 's');
    }

    return {
      label: label,
      count: uniqueCoins.length,
      longAll: longAll,
      shortAll: shortAll,
      longCount: longCoins.length,
      shortCount: shortCoins.length,
      explicitCoins: uniqueCoins,
    };
  }

  function summarizeIgnoredCoins(ignoredCoins) {
    if (!ignoredCoins || typeof ignoredCoins !== 'object') {
      return { count: 0, coins: [], label: 'None' };
    }
    var longCoins = normalizePreviewCoinList(ignoredCoins.long);
    var shortCoins = normalizePreviewCoinList(ignoredCoins.short);
    var uniqueCoins = uniqPreviewStrings(longCoins.concat(shortCoins));
    return {
      count: uniqueCoins.length,
      coins: uniqueCoins,
      label: uniqueCoins.length ? uniqueCoins.length + ' ignored coin' + (uniqueCoins.length === 1 ? '' : 's') : 'None',
    };
  }

  function summarizePreviewFilters(pbgui, live) {
    var filters = [];
    var marketCap = pbgui && pbgui.market_cap;
    var volMcap = pbgui && pbgui.vol_mcap;
    var tags = normalizePreviewCoinList(pbgui && pbgui.tags);
    var minCoinAgeDays = live && live.minimum_coin_age_days;

    if (marketCap != null && marketCap !== '' && Number(marketCap) > 0) {
      filters.push('market cap >= ' + Number(marketCap) + 'M');
    }
    if (volMcap != null && volMcap !== '' && isFinite(Number(volMcap))) {
      filters.push('vol/mcap <= ' + Number(volMcap));
    }
    if (tags.length) {
      filters.push(tags.length + ' tag' + (tags.length === 1 ? '' : 's'));
    }
    if (pbgui && pbgui.only_cpt) {
      filters.push('copy-trading only');
    }
    if (pbgui && pbgui.notices_ignore) {
      filters.push('notices ignored');
    }
    if (minCoinAgeDays != null && minCoinAgeDays !== '' && Number(minCoinAgeDays) > 0) {
      filters.push('min coin age ' + Number(minCoinAgeDays) + 'd');
    }
    return filters;
  }

  function buildOhlcvPreviewModel(config, opts) {
    var cfg = config && typeof config === 'object' && !Array.isArray(config) ? config : {};
    var backtest = cfg.backtest && typeof cfg.backtest === 'object' ? cfg.backtest : {};
    var live = cfg.live && typeof cfg.live === 'object' ? cfg.live : {};
    var pbgui = cfg.pbgui && typeof cfg.pbgui === 'object' ? cfg.pbgui : {};
    var pageLabel = String((opts && opts.pageLabel) || 'Config').trim();
    var previewNote = String((opts && opts.note) || '').trim();
    var configuredSource = String(backtest.ohlcv_source_dir || '').trim();
    var pbguiDataPath = String((opts && opts.pbguiDataPath) || '').trim();
    var configuredSourceNorm = normalizePreviewPath(configuredSource);
    var pbguiDataPathNorm = normalizePreviewPath(pbguiDataPath);
    var sourceKind = 'default';
    var sourceLabel = 'PB7 Default';
    var sourcePath = configuredSource || 'caches/ohlcv';

    if (configuredSourceNorm) {
      if (pbguiDataPathNorm && configuredSourceNorm === pbguiDataPathNorm) {
        sourceKind = 'pbgui';
        sourceLabel = 'PBGui Market Data';
      } else {
        sourceKind = 'custom';
        sourceLabel = 'Custom Directory';
      }
    }

    var exchanges = uniqPreviewStrings(
      Array.isArray(backtest.exchanges)
        ? backtest.exchanges
        : (backtest.exchange ? [backtest.exchange] : [])
    );
    var approvedSummary = summarizeApprovedCoins(live.approved_coins);
    var ignoredSummary = summarizeIgnoredCoins(live.ignored_coins);
    var filters = summarizePreviewFilters(pbgui, live);
    var coinSources = backtest.coin_sources && typeof backtest.coin_sources === 'object'
      ? Object.keys(backtest.coin_sources)
      : [];
    var marketSettingsSources = backtest.market_settings_sources && typeof backtest.market_settings_sources === 'object'
      ? Object.keys(backtest.market_settings_sources)
      : [];
    var daySpan = getPreviewDaySpan(backtest.start_date, backtest.end_date);
    var suiteScenarios = Array.isArray(backtest.scenarios) ? backtest.scenarios.length : 0;

    var notes = [];
    if (!configuredSourceNorm) {
      notes.push('No explicit ohlcv_source_dir is set; preview uses the default PB7 local source path.');
    } else if (sourceKind === 'pbgui') {
      notes.push('This config currently points at the PBGui market data root.');
    }
    if (!approvedSummary.count && !approvedSummary.longAll && !approvedSummary.shortAll) {
      notes.push('Approved coins are not pinned explicitly; the final universe is resolved from filters and market data at run time.');
    }
    notes.push('Preview shows requested scope from the editor, not on-disk completeness.');
    if (previewNote) notes.push(previewNote);

    var sections = [
      {
        title: 'Source',
        items: [
          { label: 'Type', value: sourceLabel },
          { label: 'Directory', value: sourcePath, multiline: true },
        ]
      },
      {
        title: 'Coverage',
        items: [
          { label: 'Range', value: formatPreviewRange(backtest.start_date, backtest.end_date) },
          { label: 'Days', value: daySpan == null ? 'n/a' : String(daySpan) },
          { label: 'Candle', value: String(backtest.candle_interval_minutes || 1) + ' min' },
          { label: 'Warmup', value: backtest.max_warmup_minutes ? String(backtest.max_warmup_minutes) + ' min cap' : 'default' },
          { label: 'Gap Tol.', value: backtest.gap_tolerance_ohlcvs_minutes ? String(backtest.gap_tolerance_ohlcvs_minutes) + ' min' : 'default' },
        ]
      },
      {
        title: 'Universe',
        items: [
          { label: 'Exchanges', value: summarizePreviewList(exchanges, 3) },
          { label: 'Approved', value: approvedSummary.label },
          { label: 'Ignored', value: ignoredSummary.label },
          { label: 'coin_sources', value: coinSources.length ? String(coinSources.length) + ' override' + (coinSources.length === 1 ? '' : 's') : 'None' },
          { label: 'market_settings', value: marketSettingsSources.length ? String(marketSettingsSources.length) + ' override' + (marketSettingsSources.length === 1 ? '' : 's') : 'None' },
        ]
      }
    ];

    if (filters.length) {
      sections.push({
        title: 'Filters',
        list: filters
      });
    }
    if (suiteScenarios > 0) {
      sections.push({
        title: 'Suite',
        items: [
          { label: 'Scenarios', value: String(suiteScenarios) },
        ]
      });
    }

    return {
      title: 'OHLCV Preview',
      subtitle: pageLabel + ' editor',
      badge: sourceLabel,
      badgeKind: sourceKind,
      sections: sections,
      notes: notes,
    };
  }

  function renderOhlcvPreviewPanel(target, model) {
    var panel = resolveElement(target);
    if (!panel) return null;
    if (!model) {
      panel.innerHTML = '';
      return panel;
    }

    var html = '';
    html += '<div class="sb-preview-head">';
    html += '<div>';
    html += '<div class="sb-preview-title">' + escapeHtml(model.title || 'OHLCV Preview') + '</div>';
    if (model.subtitle) {
      html += '<div class="sb-preview-subtitle">' + escapeHtml(model.subtitle) + '</div>';
    }
    html += '</div>';
    if (model.badge) {
      html += '<span class="sb-preview-chip ' + (model.badgeKind ? ('kind-' + escapeHtml(model.badgeKind)) : '') + '">' + escapeHtml(model.badge) + '</span>';
    }
    html += '</div>';

    (model.sections || []).forEach(function(section) {
      html += '<div class="sb-preview-section">';
      html += '<div class="sb-preview-section-title">' + escapeHtml(section.title || '') + '</div>';
      if (Array.isArray(section.items) && section.items.length) {
        html += '<div class="sb-preview-grid">';
        section.items.forEach(function(item) {
          html += '<div class="sb-preview-label">' + escapeHtml(item.label || '') + '</div>';
          html += '<div class="sb-preview-value' + (item.multiline ? ' multiline' : '') + '">' + escapeHtml(item.value == null ? '' : item.value) + '</div>';
        });
        html += '</div>';
      }
      if (Array.isArray(section.list) && section.list.length) {
        html += '<div class="sb-preview-list">';
        section.list.forEach(function(line) {
          html += '<div class="sb-preview-list-item">' + escapeHtml(line) + '</div>';
        });
        html += '</div>';
      }
      html += '</div>';
    });

    if (Array.isArray(model.notes) && model.notes.length) {
      html += '<div class="sb-preview-notes">';
      model.notes.forEach(function(note) {
        html += '<div class="sb-preview-note">' + escapeHtml(note) + '</div>';
      });
      html += '</div>';
    }

    panel.innerHTML = html;
    return panel;
  }

  function createOhlcvPreviewController(opts) {
    opts = opts || {};
    var button = resolveElement(opts.button);
    var panel = resolveElement(opts.panel);
    var syncRoot = resolveElement(opts.syncRoot);
    var loadConfig = typeof opts.loadConfig === 'function' ? opts.loadConfig : function() { return {}; };
    var loadPbguiDataPath = typeof opts.loadPbguiDataPath === 'function' ? opts.loadPbguiDataPath : function() { return ''; };
    var pageLabel = String(opts.pageLabel || 'Config').trim() || 'Config';
    var isOpen = false;
    var cachedPbguiDataPath;
    var hasPbguiDataPath = false;

    function setOpen(nextOpen) {
      isOpen = !!nextOpen;
      if (button) {
        button.classList.toggle('active', isOpen);
        button.setAttribute('aria-expanded', isOpen ? 'true' : 'false');
      }
      if (panel) {
        panel.style.display = isOpen ? 'block' : 'none';
        panel.classList.toggle('open', isOpen);
      }
    }

    function renderMessage(kind, title, message) {
      if (!panel) return;
      panel.innerHTML = '' +
        '<div class="sb-preview-head">' +
          '<div>' +
            '<div class="sb-preview-title">' + escapeHtml(title) + '</div>' +
            '<div class="sb-preview-subtitle">' + escapeHtml(pageLabel + ' editor') + '</div>' +
          '</div>' +
        '</div>' +
        '<div class="sb-preview-note ' + escapeHtml(kind || '') + '">' + escapeHtml(message || '') + '</div>';
    }

    var scheduleRefresh = createDebouncedRunner(function() {
      if (!isOpen) return;
      api.refresh().catch(function() {});
    }, opts.refreshDelay || 180);

    async function getPbguiDataPath() {
      if (hasPbguiDataPath) return cachedPbguiDataPath;
      try {
        cachedPbguiDataPath = await Promise.resolve(loadPbguiDataPath());
      } catch (error) {
        cachedPbguiDataPath = '';
      }
      hasPbguiDataPath = true;
      return cachedPbguiDataPath;
    }

    var api = {
      isOpen: function() {
        return isOpen;
      },
      open: function() {
        setOpen(true);
        return api.refresh();
      },
      close: function() {
        scheduleRefresh.cancel();
        setOpen(false);
      },
      refresh: async function() {
        if (!panel) return null;
        renderMessage('loading', 'OHLCV Preview', 'Refreshing preview...');
        try {
          var loaded = await Promise.resolve(loadConfig());
          var config = loaded;
          var note = '';
          if (loaded && typeof loaded === 'object' && !Array.isArray(loaded) && loaded.config && typeof loaded.config === 'object') {
            config = loaded.config;
            note = String(loaded.note || '').trim();
          }
          var model = buildOhlcvPreviewModel(config, {
            pageLabel: pageLabel,
            pbguiDataPath: await getPbguiDataPath(),
            note: note,
          });
          renderOhlcvPreviewPanel(panel, model);
          return model;
        } catch (error) {
          renderMessage('error', 'OHLCV Preview', error && error.message ? error.message : String(error || 'Preview failed'));
          return null;
        }
      },
      invalidatePbguiDataPath: function() {
        hasPbguiDataPath = false;
        cachedPbguiDataPath = '';
      }
    };

    if (button && !button.dataset.pbOhlcvPreviewBound) {
      button.dataset.pbOhlcvPreviewBound = '1';
      button.addEventListener('click', function() {
        if (isOpen) {
          api.close();
          return;
        }
        api.open().catch(function() {});
      });
    }

    if (syncRoot && !syncRoot.dataset.pbOhlcvPreviewBound) {
      syncRoot.dataset.pbOhlcvPreviewBound = '1';
      syncRoot.addEventListener('input', scheduleRefresh);
      syncRoot.addEventListener('change', scheduleRefresh);
    }

    setOpen(false);
    return api;
  }

  var _ohlcvPreflightStatusOrder = [
    'missing_local',
    'legacy_importable',
    'blocked_by_persistent_gap',
    'store_complete',
    'missing_market',
    'coin_too_young'
  ];

  function getOhlcvPreflightChipKind(status) {
    switch (String(status || '').trim()) {
      case 'ready': return 'ready';
      case 'preload': return 'preload';
      case 'blocked': return 'blocked';
      case 'legacy': return 'legacy';
      case 'mixed': return 'mixed';
      case 'empty': return 'default';
      default: return 'default';
    }
  }

  function getOhlcvPreflightTone(status) {
    switch (String(status || '').trim()) {
      case 'store_complete': return 'tone-ready';
      case 'legacy_importable': return 'tone-legacy';
      case 'missing_local': return 'tone-warn';
      case 'blocked_by_persistent_gap': return 'tone-danger';
      default: return 'tone-neutral';
    }
  }

  function renderOhlcvPreflightCountPills(counts) {
    var html = '';
    var data = counts && typeof counts === 'object' ? counts : {};
    _ohlcvPreflightStatusOrder.forEach(function(status) {
      var count = Number(data[status] || 0);
      if (!count) return;
      html += '<span class="sb-preview-pill ' + getOhlcvPreflightTone(status) + '">' +
        '<span class="sb-preview-pill-label">' + escapeHtml(status.replace(/_/g, ' ')) + '</span>' +
        '<span class="sb-preview-pill-value">' + escapeHtml(String(count)) + '</span>' +
      '</span>';
    });
    return html || '<span class="sb-preview-pill tone-neutral"><span class="sb-preview-pill-label">no data</span><span class="sb-preview-pill-value">0</span></span>';
  }

  function renderOhlcvPreflightEntries(entries) {
    if (!Array.isArray(entries) || !entries.length) return '';
    var html = '<div class="sb-preview-list">';
    entries.forEach(function(entry) {
      var sides = Array.isArray(entry.sides)
        ? entry.sides.filter(function(side) { return !!side; })
        : [];
      var sideLabel = sides.length ? (' [' + sides.join('/') + ']') : '';
      var title = escapeHtml((entry.coin || '?') + sideLabel + (entry.exchange ? (' on ' + entry.exchange) : ''));
      var meta = [];
      if (entry.symbol) meta.push(String(entry.symbol));
      if (entry.effective_start_date) meta.push('start ' + entry.effective_start_date);
      html += '<div class="sb-preview-list-item">';
      html += '<div class="sb-preview-mini-title">' + title + '</div>';
      html += '<div class="sb-preview-mini-body">' + escapeHtml(entry.note || entry.status_label || '') + '</div>';
      if (meta.length) {
        html += '<div class="sb-preview-mini-meta">' + escapeHtml(meta.join(' | ')) + '</div>';
      }
      if (entry.persistent_gap && entry.persistent_gap.reason) {
        html += '<div class="sb-preview-mini-meta">' +
          escapeHtml('gap ' + entry.persistent_gap.start + ' -> ' + entry.persistent_gap.end + ' (' + entry.persistent_gap.reason + ')') +
        '</div>';
      }
      html += '</div>';
    });
    html += '</div>';
    return html;
  }

  function renderOhlcvPreloadProgress(progressInfo, job) {
    var progress = progressInfo && typeof progressInfo === 'object' ? progressInfo : {};
    var tracker = progress.tracker && typeof progress.tracker === 'object' ? progress.tracker : null;
    var tasks = Array.isArray(progress.tasks)
      ? progress.tasks.filter(function(task) { return task && task.pct != null; })
      : [];
    if (!tasks.length && !(tracker && tracker.total)) return '';

    var html = '<div class="sb-preload-progress-list">';
    tasks.forEach(function(task) {
      var pct = Math.max(0, Math.min(100, Number(task.pct) || 0));
      if (job && job.status === 'completed' && task.kind === 'ccxt') pct = 100;
      var titleBits = [];
      if (task.exchange) titleBits.push(String(task.exchange));
      if (task.symbol) titleBits.push(String(task.symbol));
      var metaBits = [];
      if (task.kind === 'archive') {
        if (task.completed != null && task.total != null) metaBits.push('Archive ' + task.completed + '/' + task.total);
        if (task.batch) metaBits.push('Batch ' + String(task.batch));
      } else if (task.kind === 'ccxt') {
        if (task.cursor_iso) metaBits.push('Cursor ' + String(task.cursor_iso));
        if (task.response_ignored_cursor && task.response_first_iso && task.last_iso) {
          metaBits.push('Exchange returned ' + String(task.response_first_iso) + ' .. ' + String(task.last_iso));
        } else if (task.last_iso) {
          metaBits.push('Fetched through ' + String(task.last_iso));
        } else if (task.since_iso) {
          metaBits.push('Request started at ' + String(task.since_iso));
        }
        if (job && job.target_end_iso) metaBits.push('Target ' + String(job.target_end_iso));
      }
      if (!metaBits.length && task.detail) metaBits.push(String(task.detail));
      html += '<div class="sb-preload-progress-row">';
      html += '<div class="sb-preload-progress-head">';
      html += '<div class="sb-preload-progress-title">' + escapeHtml(titleBits.join(' | ') || 'Download progress') + '</div>';
      html += '<div class="sb-preload-progress-pct">' + escapeHtml(String(pct)) + '%</div>';
      html += '</div>';
      html += '<div class="sb-preload-progress-track"><div class="sb-preload-progress-fill" style="width:' + String(pct) + '%"></div></div>';
      if (metaBits.length) {
        html += '<div class="sb-preload-progress-meta">' + escapeHtml(metaBits.join(' | ')) + '</div>';
      }
      html += '</div>';
    });
    if (tracker && tracker.total) {
      var trackerText = 'PB7 progress ' + tracker.processed + '/' + tracker.total;
      if (tracker.current) trackerText += ' | current=' + tracker.current;
      if (tracker.eta_seconds != null) trackerText += ' | ETA ' + tracker.eta_seconds + 's';
      html += '<div class="sb-preview-note">' + escapeHtml(trackerText) + '</div>';
    }
    html += '</div>';
    return html;
  }

  function renderOhlcvPreflightPanel(target, model) {
    var panel = resolveElement(target);
    if (!panel) return null;
    if (!model || !model.payload) {
      panel.innerHTML = '';
      return panel;
    }

    var payload = model.payload;
    var summary = payload.summary || {};
    var request = payload.request || {};
    var universe = payload.universe || {};
    var bestSamples = payload.best_samples || {};
    var exchanges = Array.isArray(payload.exchanges) ? payload.exchanges : [];
    var notes = Array.isArray(payload.notes) ? payload.notes.slice() : [];
    if (model.clientNote) notes.unshift(String(model.clientNote));
    if (model.stale) notes.unshift('Editor values changed. Refresh the OHLCV check to re-run the PB7 planner.');

    var job = model.job || null;
    var jobRunning = job && (job.status === 'queued' || job.status === 'running');
    var preloadDisabled = !summary.preload_supported || jobRunning;
    var refreshBtnClass = model.stale ? 'sb-btn accent' : 'sb-btn';
    var preloadBtnClass = jobRunning ? 'sb-btn info' : 'sb-btn info';
    var sourceLabel = request.source_dir || 'PB7 default caches/ohlcv';
    var chipKind = getOhlcvPreflightChipKind(summary.overall_status);

    var html = '';
    html += '<div class="sb-preview-head">';
    html += '<div>';
    html += '<div class="sb-preview-title">OHLCV Readiness</div>';
    html += '<div class="sb-preview-subtitle">' + escapeHtml(String((model.pageLabel || 'Config')) + ' editor') + '</div>';
    html += '</div>';
    html += '<span class="sb-preview-chip kind-' + escapeHtml(chipKind) + '">' + escapeHtml(summary.headline || 'OHLCV check') + '</span>';
    html += '</div>';

    html += '<div class="sb-preview-section">';
    html += '<div class="sb-preview-section-title">Summary</div>';
    html += '<div class="sb-preview-note">' + escapeHtml(summary.detail || '') + '</div>';
    html += '<div class="sb-preview-pill-row">' + renderOhlcvPreflightCountPills(summary.counts || {}) + '</div>';
    html += '</div>';

    html += '<div class="sb-preview-actions">';
    html += '<button type="button" class="' + refreshBtnClass + '" data-action="refresh">↻ Refresh Check</button>';
    html += '<button type="button" class="' + preloadBtnClass + '" data-action="preload"' + (preloadDisabled ? ' disabled' : '') + '>' +
      escapeHtml(jobRunning ? '⏳ Preload running...' : (summary.preload_label || 'Preload OHLCV Data')) +
    '</button>';
    if (summary.preload_detail) {
      html += '<div class="sb-preview-note">' + escapeHtml(summary.preload_detail) + '</div>';
    }
    html += '</div>';

    html += '<div class="sb-preview-section">';
    html += '<div class="sb-preview-section-title">Request</div>';
    html += '<div class="sb-preview-grid">';
    html += '<div class="sb-preview-label">Requested</div><div class="sb-preview-value">' + escapeHtml(request.requested_start_date || 'n/a') + ' → ' + escapeHtml(request.end_date || 'n/a') + '</div>';
    html += '<div class="sb-preview-label">Effective</div><div class="sb-preview-value">' + escapeHtml(request.effective_start_date || 'n/a') + '</div>';
    html += '<div class="sb-preview-label">Warmup</div><div class="sb-preview-value">' + escapeHtml(String(request.warmup_minutes == null ? 'n/a' : request.warmup_minutes)) + ' min</div>';
    html += '<div class="sb-preview-label">Min age</div><div class="sb-preview-value">' + escapeHtml(String(request.minimum_coin_age_days == null ? 'n/a' : request.minimum_coin_age_days)) + ' d</div>';
    html += '<div class="sb-preview-label">Source</div><div class="sb-preview-value multiline">' + escapeHtml(sourceLabel) + '</div>';
    html += '<div class="sb-preview-label">Catalog</div><div class="sb-preview-value multiline">' + escapeHtml(request.catalog_present ? (request.catalog_path || 'present') : 'missing') + '</div>';
    html += '</div>';
    html += '</div>';

    html += '<div class="sb-preview-section">';
    html += '<div class="sb-preview-section-title">Universe</div>';
    html += '<div class="sb-preview-grid">';
    html += '<div class="sb-preview-label">Coins</div><div class="sb-preview-value">' + escapeHtml(String(universe.coin_count || 0)) + '</div>';
    html += '<div class="sb-preview-label">Mode</div><div class="sb-preview-value">' + escapeHtml((universe.coins_mode || 'explicit').replace(/_/g, ' ')) + '</div>';
    html += '<div class="sb-preview-label">Exchanges</div><div class="sb-preview-value">' + escapeHtml(String(universe.exchange_count || 0)) + '</div>';
    html += '</div>';
    html += '</div>';

    _ohlcvPreflightStatusOrder.forEach(function(status) {
      var entries = bestSamples[status];
      if (!Array.isArray(entries) || !entries.length) return;
      html += '<div class="sb-preview-section">';
      html += '<div class="sb-preview-section-title">Best Per Coin: ' + escapeHtml((entries[0] && entries[0].status_label) || status.replace(/_/g, ' ')) + '</div>';
      html += renderOhlcvPreflightEntries(entries);
      html += '</div>';
    });

    exchanges.forEach(function(exchangePayload) {
      var exchangeName = exchangePayload && (exchangePayload.input_exchange || exchangePayload.exchange);
      if (!exchangeName) return;
      html += '<div class="sb-preview-section">';
      html += '<div class="sb-preview-section-title">Exchange: ' + escapeHtml(exchangeName) + '</div>';
      html += '<div class="sb-preview-pill-row">' + renderOhlcvPreflightCountPills((exchangePayload && exchangePayload.counts) || {}) + '</div>';
      var exchangeSamples = exchangePayload && exchangePayload.samples && typeof exchangePayload.samples === 'object'
        ? exchangePayload.samples
        : {};
      ['missing_local', 'legacy_importable', 'blocked_by_persistent_gap'].forEach(function(status) {
        var entries = exchangeSamples[status];
        if (!Array.isArray(entries) || !entries.length) return;
        html += '<div class="sb-preview-mini-title" style="margin-top:var(--sp-sm)">' + escapeHtml((entries[0] && entries[0].status_label) || status.replace(/_/g, ' ')) + '</div>';
        html += renderOhlcvPreflightEntries(entries);
      });
      html += '</div>';
    });

    if (job) {
      var progressInfo = job.progress && typeof job.progress === 'object' ? job.progress : {};
      var currentTask = progressInfo.current_task && typeof progressInfo.current_task === 'object' ? progressInfo.current_task : null;
      var jobStatus = String(job.status || 'unknown');
      var isJobRunning = jobStatus === 'queued' || jobStatus === 'running';
      var isJobStopped = jobStatus === 'stopped';
      var isJobCompleted = jobStatus === 'completed';
      var isJobError = jobStatus === 'error';
      var jobStatusClass = ' sb-preload-job-queued';
      var jobStatusLabel = 'Queued';
      var jobStatusIcon = '○';
      var jobActivity = 'Waiting for the PB7 OHLCV downloader to start.';
      if (jobStatus === 'running') {
        jobStatusClass = ' sb-preload-job-running';
        jobStatusLabel = 'Running';
        jobStatusIcon = '<span class="sb-preload-pulse" aria-hidden="true"></span>';
        if (currentTask) {
          var activityBits = [];
          if (currentTask.exchange) activityBits.push(String(currentTask.exchange));
          if (currentTask.symbol) activityBits.push(String(currentTask.symbol));
          if (currentTask.detail) activityBits.push(String(currentTask.detail));
          if (currentTask.batch) activityBits.push('batch ' + String(currentTask.batch));
          jobActivity = activityBits.join(' | ') || (job.last_log_line || 'PB7 is downloading missing OHLCV ranges.');
        } else {
          jobActivity = job.last_log_line || 'PB7 is downloading missing OHLCV ranges.';
        }
      } else if (jobStatus === 'queued') {
        jobStatusClass = ' sb-preload-job-queued';
        jobStatusLabel = 'Queued';
        jobStatusIcon = '<span class="sb-preload-pulse" aria-hidden="true"></span>';
      } else if (isJobCompleted) {
        jobStatusClass = ' sb-preload-job-completed';
        jobStatusLabel = 'Completed';
        jobStatusIcon = '✓';
        jobActivity = job.last_log_line || 'Preload finished successfully.';
      } else if (isJobStopped) {
        jobStatusClass = ' sb-preload-job-stopped';
        jobStatusLabel = 'Stopped';
        jobStatusIcon = '◼';
        jobActivity = 'Preload was stopped before completion.';
      } else if (isJobError) {
        jobStatusClass = ' sb-preload-job-error';
        jobStatusLabel = 'Error';
        jobStatusIcon = '!';
        jobActivity = job.error || job.last_log_line || 'The preload job failed.';
      }
      var elapsedMs = 0;
      if (job.started_at) {
        var endMs = job.finished_at || Date.now();
        elapsedMs = Math.max(0, endMs - job.started_at);
      }
      var elapsedStr = '0s';
      if (elapsedMs > 0) {
        var totalSec = Math.floor(elapsedMs / 1000);
        var hours = Math.floor(totalSec / 3600);
        var min = Math.floor((totalSec % 3600) / 60);
        var sec = totalSec % 60;
        if (hours > 0) elapsedStr = hours + 'h ' + min + 'm';
        else if (min > 0) elapsedStr = min + 'm ' + sec + 's';
        else elapsedStr = sec + 's';
      }
      html += '<div class="sb-preview-section sb-preload-job' + jobStatusClass + '" data-preload-job="1">';
      html += '<div class="sb-preview-section-title">Preload Job <span class="sb-preload-status-text">' + jobStatusIcon + ' ' + escapeHtml(jobStatusLabel) + '</span></div>';
      html += '<div class="sb-preview-note">' + escapeHtml(jobActivity) + '</div>';
      html += renderOhlcvPreloadProgress(progressInfo, job);
      html += '<div class="sb-preview-grid">';
      html += '<div class="sb-preview-label">Duration</div><div class="sb-preview-value">' + escapeHtml(elapsedStr) + '</div>';
      html += '<div class="sb-preview-label">Started</div><div class="sb-preview-value">' + escapeHtml(job.started_at_iso || 'n/a') + '</div>';
      html += '<div class="sb-preview-label">Finished</div><div class="sb-preview-value">' + escapeHtml(job.finished_at_iso || '—') + '</div>';
      html += '<div class="sb-preview-label">PID</div><div class="sb-preview-value">' + escapeHtml(job.pid == null ? '—' : String(job.pid)) + '</div>';
      html += '<div class="sb-preview-label">Log lines</div><div class="sb-preview-value">' + escapeHtml(String(job.log_line_count || 0)) + '</div>';
      html += '<div class="sb-preview-label">Observed tasks</div><div class="sb-preview-value">' + escapeHtml(String(progressInfo.observed_tasks || 0)) + '</div>';
      html += '<div class="sb-preview-label">Active tasks</div><div class="sb-preview-value">' + escapeHtml(String(progressInfo.active_tasks || 0)) + '</div>';
      html += '<div class="sb-preview-label">Finished tasks</div><div class="sb-preview-value">' + escapeHtml(String(progressInfo.finished_tasks || 0)) + '</div>';
      html += '<div class="sb-preview-label">Last update</div><div class="sb-preview-value">' + escapeHtml(job.log_updated_at_iso || '—') + '</div>';
      html += '</div>';
      if (job.error) {
        html += '<div class="sb-preview-note error">' + escapeHtml(job.error) + '</div>';
      }
      if (isJobRunning) {
        html += '<div class="sb-preview-actions" style="margin-top:var(--sp-sm)">';
        html += '<button type="button" class="sb-btn danger" data-action="stop">⏹ Stop Preload</button>';
        html += '</div>';
      }
      if (Array.isArray(job.log_tail) && job.log_tail.length) {
        html += '<pre class="sb-preview-log" data-preload-log="1">' + escapeHtml(job.log_tail.join('\n')) + '</pre>';
      }
      html += '</div>';
    }

    if (notes.length) {
      html += '<div class="sb-preview-notes">';
      notes.forEach(function(note) {
        html += '<div class="sb-preview-note">' + escapeHtml(note) + '</div>';
      });
      html += '</div>';
    }

    panel.innerHTML = html;
    return panel;
  }

  function bindFloatingPanel(opts) {
    opts = opts || {};
    var panel = resolveElement(opts.panel);
    var header = resolveElement(opts.header);
    var closeButton = resolveElement(opts.closeButton);
    var fullscreenButton = resolveElement(opts.fullscreenButton);
    var handlesSelector = String(opts.handlesSelector || '.fp-resize');
    var minWidth = Math.max(160, Number(opts.minWidth) || 360);
    var minHeight = Math.max(120, Number(opts.minHeight) || 220);
    if (!panel || !header) return null;

    var restoreBounds = null;

    function isHeaderControlTarget(target) {
      if (!target) return false;
      if (closeButton && (target === closeButton || (closeButton.contains && closeButton.contains(target)))) {
        return true;
      }
      if (fullscreenButton && (target === fullscreenButton || (fullscreenButton.contains && fullscreenButton.contains(target)))) {
        return true;
      }
      return false;
    }

    function syncFullscreenButton() {
      if (!fullscreenButton) return;
      var isMaximized = panel.classList.contains('is-maximized');
      fullscreenButton.setAttribute('aria-pressed', isMaximized ? 'true' : 'false');
      fullscreenButton.setAttribute('title', isMaximized ? 'Restore window size' : 'Fit to browser window');
      fullscreenButton.textContent = isMaximized ? '❐' : '⛶';
    }

    function setMaximized(nextValue) {
      var shouldMaximize = !!nextValue;
      var isMaximized = panel.classList.contains('is-maximized');
      if (shouldMaximize === isMaximized) {
        syncFullscreenButton();
        return;
      }
      if (shouldMaximize) {
        restoreBounds = {
          left: panel.style.left || '',
          top: panel.style.top || '',
          right: panel.style.right || '',
          bottom: panel.style.bottom || '',
          width: panel.style.width || '',
          height: panel.style.height || ''
        };
        panel.classList.add('is-maximized');
        panel.style.left = '12px';
        panel.style.top = '76px';
        panel.style.right = '12px';
        panel.style.bottom = '12px';
        panel.style.width = 'auto';
        panel.style.height = 'auto';
      } else {
        panel.classList.remove('is-maximized');
        var saved = restoreBounds || {};
        panel.style.left = saved.left || '';
        panel.style.top = saved.top || '';
        panel.style.right = saved.right || '';
        panel.style.bottom = saved.bottom || '';
        panel.style.width = saved.width || '';
        panel.style.height = saved.height || '';
      }
      syncFullscreenButton();
    }

    syncFullscreenButton();

    if (!header.dataset.pbFloatingDragBound) {
      header.dataset.pbFloatingDragBound = '1';
      header.addEventListener('mousedown', function(event) {
        if (panel.classList.contains('is-maximized') || isHeaderControlTarget(event.target)) {
          return;
        }
        var rect = panel.getBoundingClientRect();
        panel.style.left = rect.left + 'px';
        panel.style.top = rect.top + 'px';
        panel.style.right = 'auto';
        panel.style.bottom = 'auto';
        var startX = event.clientX;
        var startY = event.clientY;
        var startL = rect.left;
        var startT = rect.top;
        function onMove(moveEvent) {
          panel.style.left = (startL + moveEvent.clientX - startX) + 'px';
          panel.style.top = (startT + moveEvent.clientY - startY) + 'px';
        }
        function onUp() {
          document.removeEventListener('mousemove', onMove);
          document.removeEventListener('mouseup', onUp);
        }
        document.addEventListener('mousemove', onMove);
        document.addEventListener('mouseup', onUp);
        event.preventDefault();
      });
    }

    if (!panel.dataset.pbFloatingResizeBound) {
      panel.dataset.pbFloatingResizeBound = '1';
      panel.querySelectorAll(handlesSelector).forEach(function(handle) {
        handle.addEventListener('mousedown', function(event) {
          if (panel.classList.contains('is-maximized')) return;
          event.preventDefault();
          event.stopPropagation();
          var dir = handle.dataset.dir;
          var rect = panel.getBoundingClientRect();
          panel.style.left = rect.left + 'px';
          panel.style.top = rect.top + 'px';
          panel.style.right = 'auto';
          panel.style.bottom = 'auto';
          panel.style.width = rect.width + 'px';
          panel.style.height = rect.height + 'px';
          var startX = event.clientX;
          var startY = event.clientY;
          var startL = rect.left;
          var startT = rect.top;
          var startW = rect.width;
          var startH = rect.height;
          function onMove(moveEvent) {
            var dx = moveEvent.clientX - startX;
            var dy = moveEvent.clientY - startY;
            var nextL = startL;
            var nextT = startT;
            var nextW = startW;
            var nextH = startH;
            if (dir.indexOf('w') >= 0) {
              nextL = startL + dx;
              nextW = startW - dx;
            }
            if (dir.indexOf('e') >= 0) nextW = startW + dx;
            if (dir.indexOf('n') >= 0) {
              nextT = startT + dy;
              nextH = startH - dy;
            }
            if (dir.indexOf('s') >= 0) nextH = startH + dy;
            if (nextW < minWidth) {
              if (dir.indexOf('w') >= 0) nextL = startL + startW - minWidth;
              nextW = minWidth;
            }
            if (nextH < minHeight) {
              if (dir.indexOf('n') >= 0) nextT = startT + startH - minHeight;
              nextH = minHeight;
            }
            panel.style.left = nextL + 'px';
            panel.style.top = nextT + 'px';
            panel.style.width = nextW + 'px';
            panel.style.height = nextH + 'px';
          }
          function onUp() {
            document.removeEventListener('mousemove', onMove);
            document.removeEventListener('mouseup', onUp);
          }
          document.addEventListener('mousemove', onMove);
          document.addEventListener('mouseup', onUp);
        });
      });
    }

    if (fullscreenButton && !fullscreenButton.dataset.pbFloatingFullscreenBound) {
      fullscreenButton.dataset.pbFloatingFullscreenBound = '1';
      fullscreenButton.addEventListener('click', function(event) {
        event.preventDefault();
        setMaximized(!panel.classList.contains('is-maximized'));
      });
    }

    return panel;
  }

  function createOhlcvPreflightController(opts) {
    opts = opts || {};
    var button = resolveElement(opts.button);
    var panel = resolveElement(opts.panel);
    var container = resolveElement(opts.container);
    var closeButton = resolveElement(opts.closeButton);
    var syncRoot = resolveElement(opts.syncRoot);
    var loadConfig = typeof opts.loadConfig === 'function' ? opts.loadConfig : function() { return {}; };
    var pageLabel = String(opts.pageLabel || 'Config').trim() || 'Config';
    var apiBase = String(opts.apiBase || '').trim();
    var token = String(opts.token || '').trim();
    var containerOpenClass = String(opts.containerOpenClass || 'visible').trim() || 'visible';
    var isOpen = false;
    var isStale = false;
    var cachedPayload = null;
    var cachedClientNote = '';
    var currentJob = null;
    var currentJobId = '';
    var pollTimerId = null;
    var pendingScrollTarget = '';

    function captureRenderState() {
      if (!panel) return null;
      var state = {
        panelScrollTop: panel.scrollTop,
      };
      var log = panel.querySelector('[data-preload-log]');
      if (log) {
        state.log = {
          scrollTop: log.scrollTop,
          atBottom: (log.scrollHeight - log.clientHeight - log.scrollTop) <= 12,
        };
      }
      return state;
    }

    function restoreRenderState(state) {
      if (!panel || !state) return;
      panel.scrollTop = Math.max(0, Number(state.panelScrollTop) || 0);
      if (!state.log) return;
      var log = panel.querySelector('[data-preload-log]');
      if (!log) return;
      if (state.log.atBottom) {
        log.scrollTop = log.scrollHeight;
      } else {
        log.scrollTop = Math.max(0, Number(state.log.scrollTop) || 0);
      }
    }

    function stopPolling() {
      clearTimeout(pollTimerId);
      pollTimerId = null;
    }

    function setOpen(nextOpen) {
      isOpen = !!nextOpen;
      if (button) {
        button.classList.toggle('active', isOpen);
        button.setAttribute('aria-expanded', isOpen ? 'true' : 'false');
      }
      if (container) {
        container.classList.toggle(containerOpenClass, isOpen);
        container.setAttribute('aria-hidden', isOpen ? 'false' : 'true');
      } else if (panel) {
        panel.style.display = isOpen ? 'block' : 'none';
        panel.classList.toggle('open', isOpen);
      }
      if (!isOpen) stopPolling();
    }

    function renderMessage(kind, title, message) {
      if (!panel) return;
      panel.innerHTML = '' +
        '<div class="sb-preview-head">' +
          '<div>' +
            '<div class="sb-preview-title">' + escapeHtml(title) + '</div>' +
            '<div class="sb-preview-subtitle">' + escapeHtml(pageLabel + ' editor') + '</div>' +
          '</div>' +
        '</div>' +
        '<div class="sb-preview-note ' + escapeHtml(kind || '') + '">' + escapeHtml(message || '') + '</div>';
    }

    async function requestJson(path, init) {
      if (!apiBase) throw new Error('Missing API base');
      var options = init ? Object.assign({}, init) : {};
      options.headers = Object.assign({ 'Authorization': 'Bearer ' + token }, options.headers || {});
      return resolveJsonResult(fetch(apiBase + path, options));
    }

    function normalizeLoadedConfig(loaded) {
      var config = loaded;
      var note = '';
      if (loaded && typeof loaded === 'object' && !Array.isArray(loaded) && loaded.config && typeof loaded.config === 'object') {
        config = loaded.config;
        note = String(loaded.note || '').trim();
      }
      if (!config || typeof config !== 'object' || Array.isArray(config)) {
        throw new Error('Config must be a JSON object');
      }
      return { config: config, note: note };
    }

    function renderPanel() {
      if (!panel) return;
      var renderState = captureRenderState();
      renderOhlcvPreflightPanel(panel, {
        payload: cachedPayload,
        job: currentJob,
        pageLabel: pageLabel,
        stale: isStale,
        clientNote: cachedClientNote,
      });
      if (pendingScrollTarget === 'preload-log') {
        pendingScrollTarget = '';
        requestAnimationFrame(function() {
          if (!panel) return;
          var target = panel.querySelector('[data-preload-log]') || panel.querySelector('[data-preload-job]');
          if (!target) return;
          panel.scrollTop = Math.max(0, target.offsetTop - 8);
          if (target.matches && target.matches('[data-preload-log]')) {
            target.scrollTop = target.scrollHeight;
            return;
          }
          var log = panel.querySelector('[data-preload-log]');
          if (log) log.scrollTop = log.scrollHeight;
        });
        return;
      }
      requestAnimationFrame(function() {
        restoreRenderState(renderState);
      });
    }

    function schedulePoll(delayMs) {
      stopPolling();
      if (!isOpen || !currentJobId) return;
      pollTimerId = setTimeout(function() {
        api.refreshJob(true).catch(function() {});
      }, Math.max(500, Number(delayMs) || 1500));
    }

    var api = {
      isOpen: function() {
        return isOpen;
      },
      open: function() {
        setOpen(true);
        return api.refresh();
      },
      close: function() {
        setOpen(false);
      },
      markStale: function() {
        if (!isOpen) return;
        isStale = true;
        renderPanel();
      },
      refresh: async function() {
        if (!panel) return null;
        stopPolling();
        currentJob = null;
        currentJobId = '';
        pendingScrollTarget = '';
        renderMessage('loading', 'OHLCV Readiness', 'Running PB7 OHLCV preflight...');
        try {
          var normalized = normalizeLoadedConfig(await Promise.resolve(loadConfig()));
          isStale = false;
          cachedClientNote = normalized.note;
          cachedPayload = await requestJson('/ohlcv-preflight', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ config: normalized.config })
          });
          renderPanel();
          if (currentJobId) {
            api.refreshJob(false).catch(function() {});
          }
          return cachedPayload;
        } catch (error) {
          renderMessage('error', 'OHLCV Readiness', error && error.message ? error.message : String(error || 'OHLCV preflight failed'));
          return null;
        }
      },
      refreshJob: async function(refreshAfterDone) {
        if (!currentJobId) return null;
        try {
          currentJob = await requestJson('/ohlcv-preload/' + encodeURIComponent(currentJobId));
          renderPanel();
          if (currentJob && (currentJob.status === 'queued' || currentJob.status === 'running')) {
            schedulePoll(1500);
          } else if (refreshAfterDone !== false && currentJob && currentJob.status === 'stopped') {
            schedulePoll(1200);
          }
          return currentJob;
        } catch (error) {
          renderMessage('error', 'OHLCV Readiness', error && error.message ? error.message : String(error || 'Could not load preload job status'));
          return null;
        }
      },
      startPreload: async function() {
        if (!panel) return null;
        try {
          var normalized = normalizeLoadedConfig(await Promise.resolve(loadConfig()));
          cachedClientNote = normalized.note;
          currentJob = await requestJson('/ohlcv-preload', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ config: normalized.config })
          });
          currentJobId = currentJob && currentJob.job_id ? String(currentJob.job_id) : '';
          pendingScrollTarget = 'preload-log';
          renderPanel();
          if (currentJobId) schedulePoll(1000);
          return currentJob;
        } catch (error) {
          renderMessage('error', 'OHLCV Readiness', error && error.message ? error.message : String(error || 'Could not start OHLCV preload'));
          return null;
        }
      },
      stopPreload: async function() {
        if (!currentJobId) return null;
        try {
          currentJob = await requestJson('/ohlcv-preload/' + encodeURIComponent(currentJobId), {
            method: 'DELETE'
          });
          renderPanel();
          schedulePoll(1200);
          return currentJob;
        } catch (error) {
          renderMessage('error', 'OHLCV Readiness', error && error.message ? error.message : String(error || 'Could not stop OHLCV preload'));
          return null;
        }
      }
    };

    if (button && !button.dataset.pbOhlcvPreflightBound) {
      button.dataset.pbOhlcvPreflightBound = '1';
      button.addEventListener('click', function() {
        if (isOpen) {
          api.close();
          return;
        }
        api.open().catch(function() {});
      });
    }

    if (closeButton && !closeButton.dataset.pbOhlcvPreflightBound) {
      closeButton.dataset.pbOhlcvPreflightBound = '1';
      closeButton.addEventListener('click', function(event) {
        event.preventDefault();
        api.close();
      });
    }

    if (panel && !panel.dataset.pbOhlcvPreflightBound) {
      panel.dataset.pbOhlcvPreflightBound = '1';
      panel.addEventListener('click', function(event) {
        var target = event.target && event.target.closest ? event.target.closest('[data-action]') : null;
        if (!target) return;
        var action = target.getAttribute('data-action');
        if (action === 'refresh') {
          event.preventDefault();
          api.refresh().catch(function() {});
        } else if (action === 'preload' && !target.disabled) {
          event.preventDefault();
          api.startPreload().catch(function() {});
        } else if (action === 'stop') {
          event.preventDefault();
          api.stopPreload().catch(function() {});
        }
      });
    }

    if (syncRoot && !syncRoot.dataset.pbOhlcvPreflightBound) {
      syncRoot.dataset.pbOhlcvPreflightBound = '1';
      syncRoot.addEventListener('input', function() { api.markStale(); });
      syncRoot.addEventListener('change', function() { api.markStale(); });
    }

    setOpen(false);
    return api;
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

  function bindStructuredSyncRoot(rootTarget, opts) {
    var root = resolveElement(rootTarget);
    opts = opts || {};
    if (!root) return;
    var boundAttr = 'data-pb-structured-sync-bound';
    var boundKey = opts.boundKey || 'default';
    if (root.getAttribute(boundAttr) === boundKey) return;
    root.setAttribute(boundAttr, boundKey);

    function shouldIgnore(target, phase) {
      return typeof opts.shouldIgnoreTarget === 'function' ? !!opts.shouldIgnoreTarget(target, phase) : false;
    }

    function schedule() {
      if (typeof opts.scheduleStructured === 'function') opts.scheduleStructured();
    }

    root.addEventListener('input', function(e) {
      if (shouldIgnore(e.target, 'input')) return;
      schedule();
    });
    root.addEventListener('change', function(e) {
      if (shouldIgnore(e.target, 'change')) return;
      schedule();
    });
    if (opts.clickSelector) {
      root.addEventListener('click', function(e) {
        var target = e.target && e.target.closest ? e.target.closest(opts.clickSelector) : null;
        if (!target) return;
        setTimeout(schedule, 0);
      });
    }
  }

  function createJsonSyncController(opts) {
    opts = opts || {};

    function getRawElement() {
      return resolveElement(opts.rawTextarea || 'cfg-raw-json');
    }

    function autoResizeRaw(rawEl) {
      if (typeof opts.autoResizeRaw === 'function') opts.autoResizeRaw(rawEl);
      else autoResizeTextarea(rawEl);
    }

    function getRawLastApplied() {
      return typeof opts.getRawLastApplied === 'function' ? opts.getRawLastApplied() : '';
    }

    function setRawLastApplied(value) {
      if (typeof opts.setRawLastApplied === 'function') opts.setRawLastApplied(value);
    }

    function isRawSyncing() {
      return typeof opts.isRawSyncing === 'function' ? !!opts.isRawSyncing() : false;
    }

    function setRawSyncing(value) {
      if (typeof opts.setRawSyncing === 'function') opts.setRawSyncing(!!value);
    }

    function isStructuredSyncing() {
      return typeof opts.isStructuredSyncing === 'function' ? !!opts.isStructuredSyncing() : false;
    }

    function setStructuredSyncing(value) {
      if (typeof opts.setStructuredSyncing === 'function') opts.setStructuredSyncing(!!value);
    }

    function setRawValidationError(error) {
      if (typeof opts.setRawValidationError === 'function') opts.setRawValidationError(error);
    }

    function validateRaw(raw) {
      if (typeof opts.validateRaw === 'function') return opts.validateRaw(raw);
      return { parsed: null, error: null };
    }

    function handleRawSyncError(error) {
      if (typeof opts.onRawSyncError === 'function') opts.onRawSyncError(error);
      else console.error('Raw JSON sync failed:', error);
    }

    var api = {
      scheduleRaw: null,
      scheduleStructured: null,
      applyRaw: async function() {
        if (isRawSyncing()) return;
        var rawEl = getRawElement();
        if (!rawEl) return;

        var raw = rawEl.value || '';
        var rawValidation = validateRaw(raw);
        setRawValidationError(rawValidation.error);
        if (rawValidation.error || raw === getRawLastApplied()) return;

        var parsed = rawValidation.parsed;
        if (!parsed) return;

        setRawSyncing(true);
        try {
          if (typeof opts.applyParsed === 'function') {
            await Promise.resolve(opts.applyParsed(parsed, raw));
          }
          setRawLastApplied(raw);
          setRawValidationError(null);
        } finally {
          setRawSyncing(false);
        }
      },
      applyStructured: function() {
        if (isRawSyncing() || isStructuredSyncing()) return;
        var rawEl = getRawElement();
        if (!rawEl) return;
        if (document.activeElement === rawEl) return;

        var currentRaw = rawEl.value || '';
        if (currentRaw.trim()) {
          var structuredValidation = validateRaw(currentRaw);
          if (structuredValidation.error) {
            setRawValidationError(structuredValidation.error);
            return;
          }
        }

        setStructuredSyncing(true);
        try {
          var nextCfg = typeof opts.collectConfig === 'function' ? opts.collectConfig() : {};
          if (typeof opts.onStructuredConfigCollected === 'function') opts.onStructuredConfigCollected(nextCfg);
          var nextRaw = JSON.stringify(nextCfg, null, 2);
          if (rawEl.value === nextRaw) {
            setRawLastApplied(nextRaw);
            return nextCfg;
          }
          var rawScrollTop = rawEl.scrollTop;
          rawEl.value = nextRaw;
          rawEl.scrollTop = Math.min(rawScrollTop, rawEl.scrollHeight);
          autoResizeRaw(rawEl);
          setRawLastApplied(nextRaw);
          setRawValidationError(null);
          return nextCfg;
        } finally {
          setStructuredSyncing(false);
        }
      },
      cancel: function() {
        rawRunner.cancel();
        structuredRunner.cancel();
      },
      setRawLastApplied: setRawLastApplied,
      getRawLastApplied: getRawLastApplied,
    };

    var rawRunner = createDebouncedRunner(function() {
      Promise.resolve(api.applyRaw()).catch(handleRawSyncError);
    }, opts.rawDelay || 250);
    var structuredRunner = createDebouncedRunner(function() {
      try {
        api.applyStructured();
      } catch (error) {
        console.error('Structured JSON sync failed:', error);
      }
    }, opts.structuredDelay || 150);

    api.scheduleRaw = function() {
      if (isRawSyncing()) return;
      rawRunner();
    };
    api.scheduleStructured = function() {
      if (isRawSyncing() || isStructuredSyncing()) return;
      structuredRunner();
    };

    return api;
  }

  global.PBGuiEditorShared = {
    escapeHtml: escapeHtml,
    formatJsonParseMessage: formatJsonParseMessage,
    getJsonErrorLocation: getJsonErrorLocation,
    getLineColumnFromJsonPos: getLineColumnFromJsonPos,
    findJsonSyntaxError: findJsonSyntaxError,
    getJsonLineDetail: getJsonLineDetail,
    validateJsonText: validateJsonText,
    ensureExistingHighlightOverlay: ensureExistingHighlightOverlay,
    syncExistingHighlightOverlay: syncExistingHighlightOverlay,
    ensureWrappedHighlightOverlay: ensureWrappedHighlightOverlay,
    syncWrappedHighlightOverlay: syncWrappedHighlightOverlay,
    focusJsonErrorLocation: focusJsonErrorLocation,
    autoResizeTextarea: autoResizeTextarea,
    captureTextareaAnchor: captureTextareaAnchor,
    restoreTextareaAnchor: restoreTextareaAnchor,
    openModal: openModal,
    closeModal: closeModal,
    createDebouncedRunner: createDebouncedRunner,
    bindStructuredSyncRoot: bindStructuredSyncRoot,
    createJsonSyncController: createJsonSyncController,
    clearInlineStatus: clearInlineStatus,
    setInlineStatusError: setInlineStatusError,
    resolveJsonResult: resolveJsonResult,
    normalizeEditorConfigPayload: normalizeEditorConfigPayload,
    resolveEditorConfigPayload: resolveEditorConfigPayload,
    getBalanceCalcApiBase: getBalanceCalcApiBase,
    createBalanceCalcDraft: createBalanceCalcDraft,
    openBalanceCalcPage: openBalanceCalcPage,
    requestBalanceCalculation: requestBalanceCalculation,
    bindFloatingPanel: bindFloatingPanel,
    buildOhlcvPreviewModel: buildOhlcvPreviewModel,
    renderOhlcvPreviewPanel: renderOhlcvPreviewPanel,
    createOhlcvPreviewController: createOhlcvPreviewController,
    renderOhlcvPreflightPanel: renderOhlcvPreflightPanel,
    createOhlcvPreflightController: createOhlcvPreflightController,
    createMultiselectController: createMultiselectController,
    setFixedValidationStatus: setFixedValidationStatus,
    clearFixedValidationStatus: clearFixedValidationStatus,
  };
})(window);