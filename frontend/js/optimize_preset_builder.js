(function () {
  'use strict';

  var DIRECTIONS = [
    'Balanced (keep run scoring)',
    'More profit (risk can be higher)',
    'Safer (lower drawdowns)',
    'Smoother equity curve',
    'Fewer/shorter holds (less time in market)',
    'Lower exposure (safer sizing)'
  ];
  var active = null;

  function esc(value) {
    return String(value == null ? '' : value)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#39;');
  }

  function sanitizeName(value, fallback) {
    var name = String(value || '').trim() || (fallback || 'optimize_refine');
    name = name.replace(/[ \/\\:*?"<>|\u0000]+/g, '_').replace(/^[._]+|[._]+$/g, '');
    return (name || fallback || 'optimize_refine').slice(0, 64);
  }

  function parseIntSafe(value, fallback) {
    var parsed = parseInt(value, 10);
    return Number.isFinite(parsed) ? parsed : fallback;
  }

  function injectStyles() {
    if (document.getElementById('opb-style')) return;
    var style = document.createElement('style');
    style.id = 'opb-style';
    style.textContent = '' +
      '.opb-overlay{position:fixed;inset:0;z-index:10000;background:rgba(0,0,0,.62);display:flex;align-items:center;justify-content:center;padding:24px;}' +
      '.opb-inline{height:100%;min-height:0;overflow:auto;padding:0;}' +
      '.opb-card{width:min(1180px,96vw);max-height:92vh;overflow:auto;background:var(--bg2,#1a1d24);color:var(--text,#fafafa);border:1px solid var(--border,#333640);border-radius:10px;box-shadow:0 20px 70px rgba(0,0,0,.55);padding:16px;}' +
      '.opb-card-inline{width:100%;max-height:none;min-height:100%;box-shadow:none;}' +
      '.opb-head{display:flex;gap:12px;align-items:flex-start;justify-content:space-between;margin-bottom:10px;}' +
      '.opb-title{font-size:16px;font-weight:700;}' +
      '.opb-muted{color:var(--text-dim,#a0a4ab);font-size:13px;line-height:1.45;}' +
      '.opb-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:12px;margin-top:12px;}' +
      '.opb-field{display:flex;flex-direction:column;gap:5px;min-width:0;}' +
      '.opb-field-full{grid-column:1/-1;}' +
      '.opb-field label,.opb-label{font-size:11px;color:var(--text-dim,#a0a4ab);text-transform:uppercase;letter-spacing:.04em;}' +
      '.opb-field input[type=text],.opb-field select{height:32px;background:var(--bg3,#262730);color:var(--text,#fafafa);border:1px solid var(--border,#333640);border-radius:6px;padding:0 10px;}' +
      '.opb-field input[type=range]{width:100%;accent-color:var(--accent,#4da6ff);}' +
      '.opb-check{display:flex;align-items:flex-start;gap:8px;font-size:13px;color:var(--text,#fafafa);}' +
      '.opb-check input{width:16px;height:16px;accent-color:var(--accent,#4da6ff);}' +
      '.opb-table{width:100%;border-collapse:collapse;font-size:12px;}' +
      '.opb-table th,.opb-table td{padding:6px 8px;border-bottom:1px solid var(--border,#333640);text-align:left;vertical-align:top;}' +
      '.opb-table th{color:var(--text,#fafafa);font-weight:700;background:var(--bg2,#1a1d24);}' +
      '.opb-placeholder{border:1px dashed var(--border,#333640);border-radius:8px;padding:12px;color:var(--text-dim,#a0a4ab);}' +
      '.opb-code{margin:0;max-height:260px;overflow:auto;background:var(--bg,#0e1117);border:1px solid var(--border,#333640);border-radius:8px;padding:10px;color:var(--text,#fafafa);font-size:12px;}' +
      '.opb-details{border:1px solid var(--border,#333640);border-radius:8px;padding:10px;}' +
      '.opb-details summary{cursor:pointer;font-weight:700;}' +
      '.opb-buttons{display:flex;gap:8px;align-items:center;flex-wrap:wrap;margin-top:8px;}' +
      '.opb-btn{height:32px;border:1px solid var(--border,#333640);border-radius:7px;background:var(--bg3,#262730);color:var(--text,#fafafa);padding:0 12px;cursor:pointer;}' +
      '.opb-btn:hover{border-color:var(--accent,#4da6ff);}' +
      '.opb-btn-primary{background:var(--blue,var(--accent,#4da6ff));border-color:var(--blue,var(--accent,#4da6ff));color:#fff;}' +
      '.opb-btn:disabled{opacity:.55;cursor:not-allowed;}' +
      '.opb-status{min-height:18px;margin-top:8px;}' +
      '@media(max-width:780px){.opb-overlay{padding:8px;align-items:stretch;}.opb-card{max-height:calc(100vh - 16px);}.opb-grid{grid-template-columns:1fr;}}';
    document.head.appendChild(style);
  }

  function apiJson(url, token, options) {
    options = options || {};
    var headers = Object.assign({ 'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json' }, options.headers || {});
    return fetch(url, Object.assign({}, options, { headers: headers })).then(function (res) {
      if (!res.ok) {
        return res.text().then(function (text) {
          var detail = text || ('HTTP ' + res.status);
          try {
            var parsed = JSON.parse(text);
            detail = parsed.detail || detail;
          } catch (_err) {}
          throw new Error(detail);
        });
      }
      return res.json();
    });
  }

  function extractConfigSections(raw) {
    var out = {};
    ['backtest', 'bot', 'live', 'optimize', 'pbgui', 'coin_overrides'].forEach(function (key) {
      if (raw && Object.prototype.hasOwnProperty.call(raw, key)) out[key] = raw[key];
    });
    return out;
  }

  function saveOptimizePresetConfig(token, name, config) {
    var encoded = encodeURIComponent(name);
    return apiJson(window.location.origin + '/api/optimize-v7/configs/' + encoded, token, {
      method: 'PUT',
      body: JSON.stringify(config)
    }).then(function () {
      return apiJson(window.location.origin + '/api/optimize-v7/configs/' + encoded, token);
    }).then(function (saved) {
      if (!saved || !saved.config) throw new Error('Saved optimize config could not be reloaded.');
      return saved.config;
    });
  }

  function queueOptimizePreset(token, name) {
    return apiJson(window.location.origin + '/api/optimize-v7/queue', token, {
      method: 'POST',
      body: JSON.stringify({ name: name })
    });
  }

  function openOptimizeSeedDraft(token, config, draftName) {
    return apiJson(window.location.origin + '/api/backtest-v7/optimize-draft', token, {
      method: 'POST',
      body: JSON.stringify({ config: extractConfigSections(config) })
    }).then(function (draft) {
      var params = new URLSearchParams();
      params.set('token', token);
      params.set('opt_draft_id', draft && draft.draft_id ? draft.draft_id : '');
      if (draftName) params.set('draft_name', draftName);
      window.location.href = window.location.origin + '/api/optimize-v7/main_page?' + params.toString();
    });
  }

  function updateRangeFill(input) {
    if (!input) return;
    var min = Number(input.min || 0);
    var max = Number(input.max || 100);
    var val = Number(input.value || 0);
    var pct = max === min ? 0 : ((val - min) / (max - min)) * 100;
    pct = Math.max(0, Math.min(100, pct));
    input.style.background = 'linear-gradient(90deg,var(--accent,#4da6ff) 0%,var(--accent,#4da6ff) ' + pct + '%,rgba(255,255,255,.18) ' + pct + '%,rgba(255,255,255,.18) 100%)';
  }

  function renderRows(root, rows, emptyText, columns) {
    if (!rows || !rows.length) {
      root.className = 'opb-placeholder';
      root.innerHTML = esc(emptyText);
      return;
    }
    root.className = '';
    var visibleRows = rows.slice(0, 140);
    var html = '<div style="overflow:auto;max-height:320px"><table class="opb-table"><thead><tr>';
    columns.forEach(function (column) { html += '<th>' + esc(column.label) + '</th>'; });
    html += '</tr></thead><tbody>';
    visibleRows.forEach(function (row) {
      html += '<tr>';
      columns.forEach(function (column) { html += '<td>' + esc(row[column.key] == null ? '' : row[column.key]) + '</td>'; });
      html += '</tr>';
    });
    html += '</tbody></table></div>';
    if (rows.length > visibleRows.length) html += '<div class="opb-muted" style="margin-top:6px">Showing first ' + visibleRows.length + ' of ' + rows.length + ' rows.</div>';
    root.innerHTML = html;
  }

  function scoringLabel(entry) {
    if (entry && typeof entry === 'object') {
      return entry.goal ? String(entry.metric || '') + ' (' + String(entry.goal || '') + ')' : String(entry.metric || '');
    }
    return String(entry == null ? '' : entry);
  }

  function notify(options, message, type) {
    if (typeof options.notify === 'function') options.notify(message, type || 'info');
  }

  function resolveMount(options) {
    var mount = options && (options.mount || options.mountEl || options.container);
    if (typeof mount === 'string') return document.getElementById(mount);
    return mount && mount.nodeType === 1 ? mount : null;
  }

  function closeActive(silent) {
    var previous = active;
    if (active && active.timer) window.clearTimeout(active.timer);
    if (active && active.root && active.root.parentNode) active.root.parentNode.removeChild(active.root);
    if (!active) {
      var overlay = document.getElementById('opb-overlay');
      if (overlay) overlay.remove();
    }
    active = null;
    if (!silent && previous && previous.options && typeof previous.options.onClose === 'function') previous.options.onClose();
  }

  function open(options) {
    options = options || {};
    if (typeof options.buildPreset !== 'function') throw new Error('buildPreset callback is required.');
    closeActive(true);
    injectStyles();

    var defaultName = sanitizeName(options.defaultName || 'optimize_refine', 'optimize_refine');
    var mount = resolveMount(options);
    var directions = options.directions || DIRECTIONS;
    var directionOptions = directions.map(function (direction) {
      return '<option value="' + esc(direction) + '">' + esc(direction) + '</option>';
    }).join('');
    var root = document.createElement('div');
    root.id = mount ? 'opb-inline-root' : 'opb-overlay';
    root.className = mount ? 'opb-inline' : 'opb-overlay';
    if (!mount) {
      root.setAttribute('role', 'dialog');
      root.setAttribute('aria-modal', 'true');
    }
    root.innerHTML = '' +
      '<div class="opb-card' + (mount ? ' opb-card-inline' : '') + '">' +
        '<div class="opb-head"><div><div class="opb-title">Create PBv7 Optimize Preset</div><div class="opb-muted">' + esc(options.sourceLabel || 'Source') + (options.sourceName ? ': ' + esc(options.sourceName) : '') + '</div></div><button class="opb-btn" data-opb="close" type="button">' + esc(options.closeLabel || (mount ? 'Back to Results' : 'Close')) + '</button></div>' +
        '<p class="opb-muted">Create a follow-up Optimize preset with bounds tightened around this result config. Use it for focused fine-tuning instead of broad exploration.</p>' +
        '<div class="opb-grid">' +
          '<div class="opb-field"><label>Optimization goal</label><select data-opb="direction">' + directionOptions + '</select></div>' +
          '<div class="opb-field"><label>Preset name</label><input data-opb="name" type="text" maxlength="64" value="' + esc(defaultName) + '"></div>' +
          '<div class="opb-field opb-field-full"><label class="opb-check"><input data-opb="only-near" type="checkbox" checked><span>Only adjust parameters near optimize bounds</span></label></div>' +
          '<div class="opb-field"><label>Bounds window (%)</label><input data-opb="bounds-window" type="range" min="0" max="100" step="5" value="0"><div class="opb-muted" data-opb="bounds-window-value">0%</div></div>' +
          '<div class="opb-field"><label>Risk adjustment</label><input data-opb="risk-adjust" type="range" min="-50" max="50" step="5" value="0"><div class="opb-muted" data-opb="risk-adjust-value">0</div></div>' +
          '<div class="opb-field opb-field-full"><div class="opb-muted" data-opb="bounds-hint">Bounds unchanged.</div></div>' +
          '<div class="opb-field opb-field-full"><h4 style="margin:0">Preset summary</h4><div data-opb="summary" class="opb-placeholder">Building preset preview...</div></div>' +
          '<div class="opb-field opb-field-full"><details class="opb-details"><summary>Advanced preview details</summary><div class="opb-grid"><div class="opb-field opb-field-full"><h4 style="margin:0">Planned optimize defaults</h4><pre data-opb="json" class="opb-code">Building preset preview...</pre></div><div class="opb-field opb-field-full"><h4 style="margin:0">Bounds changes preview</h4><div data-opb="bounds" class="opb-placeholder">No preview loaded.</div></div></div></details></div>' +
          '<div class="opb-field opb-field-full"><div class="opb-buttons"><button class="opb-btn opb-btn-primary" data-opb="create" type="button">Create Optimize Preset</button><button class="opb-btn" data-opb="queue" type="button">Create &amp; Queue</button></div><div class="opb-status opb-muted" data-opb="status"></div></div>' +
        '</div>' +
      '</div>';
    if (mount) {
      mount.innerHTML = '';
      mount.appendChild(root);
    } else {
      document.body.appendChild(root);
    }

    var state = active = { root: root, seq: 0, timer: null, options: options };
    function q(name) { return root.querySelector('[data-opb="' + name + '"]'); }

    function updateLabels() {
      var windowPct = parseIntSafe(q('bounds-window').value, 0);
      var riskAdjust = parseIntSafe(q('risk-adjust').value, 0);
      q('bounds-window-value').textContent = String(windowPct) + '%';
      q('risk-adjust-value').textContent = String(riskAdjust);
      q('bounds-hint').textContent = windowPct === 0
        ? 'Bounds unchanged.'
        : 'Effective bounds window: +/-' + String(windowPct) + '% around selected values' + (q('only-near').checked ? ' for near-bound parameters only.' : '.');
      updateRangeFill(q('bounds-window'));
      updateRangeFill(q('risk-adjust'));
    }

    function buildBody(includeConfig) {
      var body = {
        include_config: !!includeConfig,
        preset: {
          preset_name: q('name').value.trim() || defaultName,
          only_adjust_near_bounds: !!q('only-near').checked,
          bounds_window_pct: parseIntSafe(q('bounds-window').value, 0),
          direction: q('direction').value,
          risk_adjust: parseIntSafe(q('risk-adjust').value, 0),
          show_near_bounds: false,
          expand_near_bounds: false,
          hide_hard_limited_near: false
        }
      };
      if (typeof options.extendRequest === 'function') {
        var extended = options.extendRequest(body, { includeConfig: !!includeConfig });
        if (extended) body = extended;
      }
      return body;
    }

    function renderSummary(payload) {
      var root = q('summary');
      if (!payload || !payload.ok) {
        root.className = 'opb-placeholder';
        root.innerHTML = 'No preset preview available.';
        return;
      }
      var scoring = Array.isArray(payload.scoring) ? payload.scoring : [];
      var limits = Array.isArray(payload.limits) ? payload.limits : [];
      var rows = payload.bounds_preview_rows || [];
      var nearCount = parseIntSafe(payload.near_bounds_count || 0, 0);
      var summaryRows = [
        { label: 'Name', value: payload.preset_name || q('name').value || defaultName },
        { label: 'Goal', value: payload.direction || q('direction').value },
        { label: 'Bounds scope', value: payload.only_adjust_near_bounds ? ('near-bound parameters only' + (nearCount ? ' (' + nearCount + ')' : ' (none detected)')) : 'all optimized parameters' },
        { label: 'Bounds window', value: (payload.window_pct || 0) > 0 ? ('+/-' + String(payload.window_pct) + '%') : 'unchanged' },
        { label: 'Scoring', value: scoring.map(scoringLabel).filter(Boolean).join(', ') || 'unchanged' },
        { label: 'Limits', value: String(limits.length) + ' configured' },
        { label: 'Bounds changes', value: String(rows.length) }
      ];
      var html = '<table class="opb-table"><tbody>';
      summaryRows.forEach(function (row) { html += '<tr><th>' + esc(row.label) + '</th><td>' + esc(row.value) + '</td></tr>'; });
      html += '</tbody></table>';
      root.className = '';
      root.innerHTML = html;
    }

    function renderPreview(payload) {
      if (!payload || !payload.ok) {
        renderSummary(null);
        q('json').textContent = 'No preset preview available.';
        renderRows(q('bounds'), [], 'No bounds changes detected.', []);
        return;
      }
      if (payload.preset_name) q('name').value = String(payload.preset_name);
      renderSummary(payload);
      q('json').textContent = JSON.stringify({ scoring: payload.scoring || [], limits: payload.limits || [] }, null, 2);
      renderRows(q('bounds'), payload.bounds_preview_rows || [], 'No bounds changes detected.', [
        { key: 'param', label: 'Param' },
        { key: 'change', label: 'Change' },
        { key: 'before', label: 'Before' },
        { key: 'expand', label: 'Expand' },
        { key: 'window', label: 'Window' },
        { key: 'risk', label: 'Risk' },
        { key: 'result', label: 'Result' },
        { key: 'expand_note', label: 'Note' }
      ]);
    }

    function setWorking(working) {
      q('create').disabled = !!working;
      q('queue').disabled = !!working;
    }

    function loadPreview(includeConfig) {
      var seq = ++state.seq;
      q('status').textContent = includeConfig ? 'Building preset config...' : 'Building preset preview...';
      return options.buildPreset(buildBody(!!includeConfig)).then(function (payload) {
        if (seq === state.seq) {
          renderPreview(payload);
          q('status').textContent = '';
        }
        return payload;
      }).catch(function (err) {
        if (seq === state.seq) q('status').textContent = 'Preset preview failed: ' + err.message;
        throw err;
      });
    }

    function schedulePreview(delay) {
      if (state.timer) window.clearTimeout(state.timer);
      state.timer = window.setTimeout(function () {
        state.timer = null;
        loadPreview(false).catch(function () {});
      }, delay == null ? 250 : delay);
    }

    function createPreset(queueAfter) {
      setWorking(true);
      loadPreview(true).then(function (payload) {
        if (!payload || !payload.preset_config) throw new Error('Preset config was not generated.');
        var name = sanitizeName(payload.preset_name || q('name').value || defaultName, defaultName);
        q('name').value = name;
        if (typeof options.saveConfig !== 'function') throw new Error('saveConfig callback is required.');
        q('status').textContent = 'Saving optimize preset...';
        return options.saveConfig(name, payload.preset_config).then(function (savedConfig) {
          notify(options, 'Optimize preset created: ' + name + '.json', 'ok');
          if (queueAfter) {
            if (typeof options.queueConfig !== 'function') throw new Error('queueConfig callback is required.');
            q('status').textContent = 'Queueing optimize preset...';
            return options.queueConfig(name).then(function (queueData) {
              var suffix = queueData && queueData.filename ? ' (' + queueData.filename + ')' : '';
              q('status').textContent = 'Optimize preset queued: ' + name + '.json' + suffix;
              notify(options, 'Optimize preset queued: ' + name + '.json', 'ok');
              return savedConfig;
            });
          }
          if (typeof options.openOptimize === 'function') {
            q('status').textContent = 'Opening PBv7 Optimize...';
            return options.openOptimize(savedConfig, name);
          }
          q('status').textContent = 'Optimize preset saved: ' + name + '.json';
          return savedConfig;
        });
      }).catch(function (err) {
        q('status').textContent = 'Create preset failed: ' + err.message;
        notify(options, 'Create preset failed: ' + err.message, 'err');
      }).finally(function () {
        setWorking(false);
      });
    }

    q('close').addEventListener('click', function () { closeActive(false); });
    q('bounds-window').addEventListener('input', updateLabels);
    q('risk-adjust').addEventListener('input', updateLabels);
    ['bounds-window', 'risk-adjust', 'direction', 'only-near', 'name'].forEach(function (name) {
      q(name).addEventListener('change', function () {
        updateLabels();
        schedulePreview(0);
      });
    });
    q('create').addEventListener('click', function () { createPreset(false); });
    q('queue').addEventListener('click', function () { createPreset(true); });

    updateLabels();
    schedulePreview(0);
  }

  window.PBGuiOptimizePresetBuilder = {
    directions: DIRECTIONS.slice(),
    open: open,
    close: function () { closeActive(false); },
    sanitizeName: sanitizeName,
    saveOptimizePresetConfig: saveOptimizePresetConfig,
    queueOptimizePreset: queueOptimizePreset,
    openOptimizeSeedDraft: openOptimizeSeedDraft,
    extractConfigSections: extractConfigSections
  };
})();
