/**
 * Suite Editor Module — reusable component for passivbot suite mode.
 *
 * Usage:
 *   suiteInit('suite-container', { apiBase: API_BASE })
 *   suiteLoad(cfg)        // populate from config
 *   suiteCollect()        // → { suite_enabled, scenarios, aggregate }
 *
 * Depends on parent page providing: apiFetch(), toast(), esc()
 */

/* ── State ──────────────────────────────────────────────────── */
var _suiteState = {
  enabled: false,
  scenarios: [],       // array of scenario objects
  editIdx: -1,         // index of scenario being edited (-1 = none)
  aggregate: { default: 'mean' },
  botParams: [],       // [{key, type, default}] loaded from API
  aggregateMetrics: [],
  containerId: '',
  apiBase: '',
  exchanges: ['binance','bybit','bitget','okx','hyperliquid','kucoin'],
  preserveMarketIdentifiers: false,
  scenarioGeneratorEnabled: false,
  getScenarioContext: null,
  scenarioPreview: null,
  scenarioPreviewContext: '',
  scenarioGeneratorDraft: null,
  scenarioRequestId: 0,
  scenarioTemplate: null,
  applyingGeneratedTemplate: false,
  expanded: false,
  onApplyScenarioPreview: null,
};

/* ── Templates ──────────────────────────────────────────────── */
var _suiteTemplates = {
  'Exchange Comparison': {
    scenarios: [
      { label: 'binance_only', exchanges: ['binance'] },
      { label: 'bybit_only', exchanges: ['bybit'] },
    ],
    aggregate: { default: 'mean' },
  },
  'Date Windows': {
    scenarios: [
      { label: '2021', start_date: '2021-01-01', end_date: '2021-12-31' },
      { label: '2022', start_date: '2022-01-01', end_date: '2022-12-31' },
      { label: '2023', start_date: '2023-01-01', end_date: '2023-12-31' },
      { label: '2024', start_date: '2024-01-01', end_date: '2024-12-31' },
    ],
    aggregate: { default: 'mean' },
  },
  'TWE Sensitivity': {
    scenarios: [
      { label: 'twe_0.5', overrides: { 'bot.long.total_wallet_exposure_limit': 0.5, 'bot.short.total_wallet_exposure_limit': 0.5 } },
      { label: 'twe_1.0', overrides: { 'bot.long.total_wallet_exposure_limit': 1.0, 'bot.short.total_wallet_exposure_limit': 1.0 } },
      { label: 'twe_1.5', overrides: { 'bot.long.total_wallet_exposure_limit': 1.5, 'bot.short.total_wallet_exposure_limit': 1.5 } },
      { label: 'twe_2.0', overrides: { 'bot.long.total_wallet_exposure_limit': 2.0, 'bot.short.total_wallet_exposure_limit': 2.0 } },
    ],
    aggregate: { default: 'mean', drawdown_worst_strategy_eq: 'max' },
  },
  'n_positions Sensitivity': {
    scenarios: [
      { label: 'npos_5',  overrides: { 'bot.long.n_positions': 5,  'bot.short.n_positions': 5 } },
      { label: 'npos_10', overrides: { 'bot.long.n_positions': 10, 'bot.short.n_positions': 10 } },
      { label: 'npos_15', overrides: { 'bot.long.n_positions': 15, 'bot.short.n_positions': 15 } },
      { label: 'npos_20', overrides: { 'bot.long.n_positions': 20, 'bot.short.n_positions': 20 } },
    ],
    aggregate: { default: 'mean', drawdown_worst_strategy_eq: 'max' },
  },
};

/* ── Aggregate metric names ─────────────────────────────────── */
var _suiteAggMetricFallbacks = [
  'adg_strategy_eq', 'drawdown_worst_strategy_eq', 'drawdown_worst_mean_1pct_strategy_eq',
  'peak_recovery_days_strategy_eq', 'peak_recovery_hours_strategy_eq', 'position_held_days_max',
  'position_held_hours_max', 'sharpe_ratio_strategy_eq', 'sortino_ratio_strategy_eq',
  'backtest_completion_ratio',
];

var _suiteMsState = {};
var _suiteMsCounterpart = {
  'suite-sc-coins-ms': 'suite-sc-ign-ms',
  'suite-sc-ign-ms': 'suite-sc-coins-ms',
};
var _suiteCoinSourcesPrefix = 'suite-kv-cs';
var _suiteInlineInputStyle = 'height:24px;font-size:var(--fs-xs);background:var(--bg2);color:var(--text);border:1px solid var(--border);border-radius:4px;padding:0 var(--sp-xs);outline:none;font-family:var(--font)';
var _suiteInlineSelectStyle = _suiteInlineInputStyle + ';appearance:none;-webkit-appearance:none;-moz-appearance:none;padding-right:22px';

/* ── Init ───────────────────────────────────────────────────── */
function suiteInit(containerId, opts) {
  opts = opts || {};
  _suiteState.containerId = containerId;
  _suiteState.apiBase = opts.apiBase || '';
  _suiteState.aggregateMetrics = _suiteNormalizeAggMetrics(opts.aggregateMetrics);
  _suiteState.preserveMarketIdentifiers = String(opts.version || '').toLowerCase() === 'v8';
  _suiteState.scenarioGeneratorEnabled = _suiteState.preserveMarketIdentifiers && opts.scenarioGenerator === true;
  _suiteState.getScenarioContext = typeof opts.getScenarioContext === 'function' ? opts.getScenarioContext : null;
  _suiteState.onApplyScenarioPreview = typeof opts.onApplyScenarioPreview === 'function' ? opts.onApplyScenarioPreview : null;
  _suiteState.scenarioPreview = null;
  _suiteState.scenarioPreviewContext = '';
  _suiteState.scenarioGeneratorDraft = null;
  _suiteState.scenarioRequestId += 1;
  _suiteState.scenarioTemplate = null;
  _suiteState.expanded = false;
  if (Array.isArray(opts.exchanges) && opts.exchanges.length) {
    _suiteState.exchanges = opts.exchanges.map(function(value) { return String(value || '').trim(); })
      .filter(function(value, index, values) { return value && values.indexOf(value) === index; });
  } else {
    _suiteState.exchanges = ['binance','bybit','bitget','okx','hyperliquid','kucoin'];
  }
  if (String(opts.version || '').toLowerCase() === 'v8') {
    ['TWE Sensitivity', 'n_positions Sensitivity'].forEach(function(name) {
      var template = _suiteTemplates[name];
      (template && template.scenarios || []).forEach(function(scenario) {
        var migrated = {};
        Object.keys(scenario.overrides || {}).forEach(function(key) {
          migrated[key.replace(/^(bot\.(?:long|short))\.(total_wallet_exposure_limit|n_positions)$/, '$1.risk.$2')] = scenario.overrides[key];
        });
        scenario.overrides = migrated;
      });
    });
  }
  _suiteLoadBotParams();
}

function _suiteNormalizeAggMetrics(metrics) {
  var source = Array.isArray(metrics) && metrics.length ? metrics : _suiteAggMetricFallbacks;
  var seen = {};
  var result = [];
  for (var i = 0; i < source.length; i++) {
    var value = String(source[i] == null ? '' : source[i]).trim();
    if (!value || seen[value]) continue;
    seen[value] = true;
    result.push(value);
  }
  return result.length ? result : _suiteAggMetricFallbacks.slice();
}

function _suiteAggMetricOptions(extraMetrics) {
  var source = (_suiteState.aggregateMetrics || []).concat(extraMetrics || []);
  return _suiteNormalizeAggMetrics(source);
}

/* ── Load config into suite editor ──────────────────────────── */
function suiteLoad(cfg, opts) {
  opts = opts || {};
  var bt = cfg.backtest || {};
  var prevEditIdx = _suiteState.editIdx;
  var prevLabel = '';
  if (prevEditIdx >= 0 && _suiteState.scenarios[prevEditIdx]) {
    prevLabel = String(_suiteState.scenarios[prevEditIdx].label || '');
  }
  var nextScenarios = Array.isArray(bt.scenarios) ? JSON.parse(JSON.stringify(bt.scenarios)) : [];
  _suiteState.enabled = !!bt.suite_enabled;
  var existingExpander = typeof document !== 'undefined' ? document.getElementById('exp-suite') : null;
  _suiteState.expanded = existingExpander
    ? existingExpander.classList.contains('open')
    : _suiteState.enabled;
  _suiteState.scenarios = nextScenarios;
  var reducer = bt.reducer || bt.aggregate;
  _suiteState.aggregate = reducer ? JSON.parse(JSON.stringify(reducer)) : { default: 'mean' };
  _suiteState.scenarioPreview = null;
  _suiteState.scenarioPreviewContext = '';
  _suiteState.scenarioGeneratorDraft = null;
  _suiteState.scenarioRequestId += 1;
  _suiteState.scenarioTemplate = cfg.pbgui && cfg.pbgui.scenario_template
    ? JSON.parse(JSON.stringify(cfg.pbgui.scenario_template))
    : null;
  if (opts.preserveEdit && _suiteState.enabled && prevEditIdx >= 0) {
    var nextEditIdx = -1;
    if (prevLabel) {
      for (var i = 0; i < nextScenarios.length; i++) {
        if (String((nextScenarios[i] || {}).label || '') === prevLabel) {
          nextEditIdx = i;
          break;
        }
      }
    }
    if (nextEditIdx < 0 && prevEditIdx < nextScenarios.length) nextEditIdx = prevEditIdx;
    _suiteState.editIdx = nextEditIdx;
  } else {
    _suiteState.editIdx = -1;
  }
  _suiteRender();
}

/* ── Collect current state → config fragment ────────────────── */
function suiteCollect() {
  // If editing a scenario, auto-save it first
  if (_suiteState.editIdx >= 0) _suiteSaveEditingScenario();

  var result = {};
  result.suite_enabled = _suiteState.enabled;

  if (_suiteState.enabled) {
    result.scenarios = JSON.parse(JSON.stringify(_suiteState.scenarios));
    result.aggregate = JSON.parse(JSON.stringify(_suiteState.aggregate));
    if (_suiteState.scenarioTemplate) {
      result.scenario_template = JSON.parse(JSON.stringify(_suiteState.scenarioTemplate));
    }
  }
  return result;
}

/* ── Fetch bot params from API ──────────────────────────────── */
function _suiteLoadBotParams() {
  apiFetch('/bot-params').then(function(data) {
    _suiteState.botParams = data.params || [];
  }).catch(function() {
    // Fallback: use a hardcoded list of common override params
    _suiteState.botParams = [
      'total_wallet_exposure_limit', 'n_positions',
      'entry_initial_qty_pct', 'entry_initial_ema_dist',
      'entry_grid_spacing_pct', 'entry_grid_double_down_factor',
      'close_grid_markup_start', 'close_grid_markup_end',
      'close_grid_qty_pct', 'close_trailing_grid_ratio',
      'unstuck_close_pct', 'unstuck_ema_dist', 'unstuck_threshold',
      'ema_span_0', 'ema_span_1',
    ].map(function(k) { return { key: k }; });
  });
}

/* ── Structured editor sync hook ───────────────────────────── */
function _suiteNotifyStructuredSync() {
  if (!_suiteState.applyingGeneratedTemplate) _suiteState.scenarioTemplate = null;
  if (typeof scheduleStructuredEditorSync === 'function') {
    scheduleStructuredEditorSync();
  }
}

function _suiteAvailableCoins() {
  var seen = {};
  if (typeof _cfgMs !== 'undefined') {
    ['ms-cfg-app-long', 'ms-cfg-app-short', 'ms-cfg-ign-long', 'ms-cfg-ign-short'].forEach(function(id) {
      var state = _cfgMs[id];
      if (!state) return;
      (state.options || []).forEach(function(value) {
        if (value && value !== 'all') seen[value] = true;
      });
      (state.selected || []).forEach(function(value) {
        if (value && value !== 'all') seen[value] = true;
      });
    });
  }
  return Object.keys(seen).sort();
}

function _suiteBuildCoinMsField(id, label, tip, placeholder) {
  return '\x3Cdiv class="form-group">\x3Clabel>\x3Cspan data-tip="' + esc(tip) + '">' + label + '\x3C/span> ' +
    '\x3Cspan class="ms-clear-btn" onclick="_suiteClearCoinMs(\'' + id + '\')" title="Clear all">\u00d7\x3C/span>\x3C/label>' +
    '\x3Cdiv class="ms-wrap" id="' + id + '">' +
      '\x3Cinput class="ms-input" id="' + id + '-input" placeholder="' + esc(placeholder) + '" autocomplete="off">' +
      '\x3Cdiv class="ms-dropdown" id="' + id + '-dd">\x3C/div>' +
    '\x3C/div>\x3C/div>';
}

function _suiteBuildDateField(id, label, value, tip, placeholder) {
  var dateVal = value || '';
  return '\x3Cdiv class="form-group">\x3Clabel>\x3Cspan data-tip="' + esc(tip) + '">' + label + '\x3C/span>\x3C/label>' +
    '\x3Cdiv style="position:relative">' +
      '\x3Cinput type="text" id="' + id + '" value="' + esc(dateVal) + '" data-prev="' + esc(dateVal) + '" placeholder="' + esc(placeholder || '') + '" ' +
      'style="width:100%;box-sizing:border-box;padding-right:26px" onchange="this.dataset.prev=this.value">' +
      '\x3Cbutton type="button" data-dp="' + id + '" onclick="window.__dp.show(\'' + id + '\',this)" ' +
      'style="position:absolute;right:2px;top:50%;transform:translateY(-50%);background:transparent;border:none;padding:0 3px;font-size:var(--fs-sm);line-height:1;cursor:pointer" title="Open calendar">📅\x3C/button>' +
    '\x3C/div>\x3C/div>';
}

function _suiteBuildCoinSourcesEditor(data) {
  var count = Object.keys(data || {}).length;
  var exchangeOptions = '';
  for (var i = 0; i < _suiteState.exchanges.length; i++) {
    exchangeOptions += '\x3Coption value="' + _suiteState.exchanges[i] + '">' + _suiteState.exchanges[i] + '\x3C/option>';
  }
  return '\x3Cdiv class="expander" id="suite-exp-csrc">' +
    '\x3Cdiv class="expander-header" onclick="toggleExpander(\'suite-exp-csrc\')">' +
      '\x3Cspan class="arrow">▶\x3C/span> coin_sources (' + count + ' configured)' +
    '\x3C/div>' +
    '\x3Cdiv class="expander-body">' +
      '\x3Cdiv style="display:flex;align-items:center;justify-content:flex-end;min-height:18px;margin-bottom:4px">' +
        '\x3Cspan class="ms-clear-btn" onclick="kvClearAll(\'' + _suiteCoinSourcesPrefix + '\')" title="Clear all">\u00d7 all\x3C/span>' +
      '\x3C/div>' +
      '\x3Cdiv class="kv-chips" id="' + _suiteCoinSourcesPrefix + '">\x3C/div>' +
      '\x3Cdiv style="display:flex;gap:var(--sp-sm);align-items:end;margin-top:var(--sp-xs)">' +
        '\x3Cdiv class="form-group" style="width:140px">\x3Clabel>Exchange\x3C/label>' +
          '\x3Cselect id="' + _suiteCoinSourcesPrefix + '-exchange" class="form-input" onchange="kvLoadCoins(\'' + _suiteCoinSourcesPrefix + '\')">' +
            exchangeOptions +
          '\x3C/select>\x3C/div>' +
        '\x3Cdiv class="form-group" style="flex:1">\x3Clabel>Coin\x3C/label>' +
          '\x3Cdiv class="ms-wrap" id="' + _suiteCoinSourcesPrefix + '-coin">' +
            '\x3Cinput class="ms-input" id="' + _suiteCoinSourcesPrefix + '-coin-input" placeholder="Type to search..." autocomplete="off">' +
            '\x3Cdiv class="ms-dropdown" id="' + _suiteCoinSourcesPrefix + '-coin-dd">\x3C/div>' +
          '\x3C/div>\x3C/div>' +
      '\x3C/div>' +
    '\x3C/div>' +
  '\x3C/div>';
}

function _suiteGetCoinMsSelected(id) {
  return _suiteMsState[id] ? _suiteMsState[id].selected.slice() : [];
}

function _suiteClearCoinMs(id) {
  if (!_suiteMsState[id]) return;
  _suiteMsState[id].selected = [];
  _suiteRenderCoinMs(id);
  _suiteNotifyStructuredSync();
}

function _suiteEnsureCoinMsState(id, selected) {
  var options = _suiteAvailableCoins();
  var chosen = (selected || []).filter(Boolean).map(function(value) {
    var identifier = String(value).trim();
    return _suiteState.preserveMarketIdentifiers ? identifier : identifier.toUpperCase();
  });
  chosen.forEach(function(value) {
    if (options.indexOf(value) < 0) options.push(value);
  });
  options.sort();
  var prev = _suiteMsState[id] || {};
  _suiteMsState[id] = {
    options: options,
    selected: chosen,
    highlightIdx: prev.highlightIdx || -1,
  };
}

function _suiteInitCoinMs(id, selected) {
  _suiteEnsureCoinMsState(id, selected);
  var wrap = document.getElementById(id);
  if (!wrap) return;
  var input = wrap.querySelector('.ms-input');
  if (!input) return;
  if (!input.dataset.wired) {
    input.dataset.wired = '1';
    input.addEventListener('focus', function() { _suiteShowCoinDd(id, this.value); });
    input.addEventListener('input', function() {
      if (_suiteMsState[id]) _suiteMsState[id].highlightIdx = -1;
      _suiteShowCoinDd(id, this.value);
    });
    input.addEventListener('blur', function() {
      var dd = wrap.querySelector('.ms-dropdown');
      this.value = '';
      if (dd) setTimeout(function() {
        dd.classList.remove('open');
        if (_suiteMsState[id]) _suiteMsState[id].highlightIdx = -1;
      }, 150);
    });
    input.addEventListener('keydown', function(e) {
      var dd = wrap.querySelector('.ms-dropdown');
      if (!dd || !dd.classList.contains('open')) return;
      var items = dd.querySelectorAll('.ms-option:not(.selected)');
      var state = _suiteMsState[id];
      if (!state) return;
      if (e.key === 'ArrowDown' || e.key === 'ArrowUp') {
        e.preventDefault();
        if (!items.length) return;
        if (e.key === 'ArrowDown') state.highlightIdx = state.highlightIdx < items.length - 1 ? state.highlightIdx + 1 : 0;
        else state.highlightIdx = state.highlightIdx > 0 ? state.highlightIdx - 1 : items.length - 1;
        items.forEach(function(el, idx) {
          if (idx === state.highlightIdx) { el.classList.add('highlighted'); el.scrollIntoView({ block: 'nearest' }); }
          else el.classList.remove('highlighted');
        });
      } else if (e.key === 'Enter') {
        e.preventDefault();
        var value = null;
        if (items.length === 1) value = items[0].getAttribute('data-val');
        else if (state.highlightIdx >= 0 && state.highlightIdx < items.length) value = items[state.highlightIdx].getAttribute('data-val');
        if (value) {
          _suiteAddCoinMsValue(id, value);
          this.value = '';
          _suiteShowCoinDd(id, '');
        }
      }
    });
  }
  _suiteRenderCoinMs(id);
}

function _suiteRenderCoinMs(id) {
  var wrap = document.getElementById(id);
  if (!wrap) return;
  var input = wrap.querySelector('.ms-input');
  if (!input) return;
  wrap.querySelectorAll('.ms-tag').forEach(function(tag) { tag.remove(); });
  var state = _suiteMsState[id];
  if (!state) return;
  state.selected.forEach(function(value) {
    var tag = document.createElement('span');
    tag.className = 'ms-tag';
    tag.appendChild(document.createTextNode(value + ' '));
    var close = document.createElement('span');
    close.className = 'ms-x';
    close.setAttribute('data-val', value);
    close.textContent = '×';
    tag.appendChild(close);
    wrap.insertBefore(tag, input);
  });
  wrap.querySelectorAll('.ms-x').forEach(function(close) {
    close.addEventListener('click', function(e) {
      e.stopPropagation();
      var value = this.getAttribute('data-val');
      state.selected = state.selected.filter(function(selectedValue) { return selectedValue !== value; });
      _suiteRenderCoinMs(id);
      _suiteNotifyStructuredSync();
    });
  });
}

function _suiteAddCoinMsValue(id, value) {
  var state = _suiteMsState[id];
  if (!state) return;
  value = String(value || '').trim();
  if (!_suiteState.preserveMarketIdentifiers) value = value.toUpperCase();
  if (!value) return;
  if (state.selected.indexOf(value) < 0) state.selected.push(value);
  var counterpartId = _suiteMsCounterpart[id];
  if (counterpartId && _suiteMsState[counterpartId]) {
    _suiteMsState[counterpartId].selected = _suiteMsState[counterpartId].selected.filter(function(selectedValue) {
      return selectedValue !== value;
    });
    _suiteRenderCoinMs(counterpartId);
  }
  _suiteRenderCoinMs(id);
  _suiteNotifyStructuredSync();
}

function _suiteShowCoinDd(id, filter) {
  var wrap = document.getElementById(id);
  if (!wrap) return;
  var dd = wrap.querySelector('.ms-dropdown');
  if (!dd) return;
  var state = _suiteMsState[id];
  if (!state) return;
  _suiteEnsureCoinMsState(id, state.selected);
  state = _suiteMsState[id];
  var counterpartId = _suiteMsCounterpart[id];
  var counterpartSelected = (counterpartId && _suiteMsState[counterpartId]) ? _suiteMsState[counterpartId].selected : [];
  var query = (filter || '').toUpperCase();
  var html = '';
  var visibleIdx = 0;
  state.options.forEach(function(option) {
    if (query && option.toUpperCase().indexOf(query) < 0) return;
    var selected = state.selected.indexOf(option) >= 0;
    var inOther = counterpartSelected.indexOf(option) >= 0;
    var highlighted = !selected && visibleIdx === state.highlightIdx;
    html += '\x3Cdiv class="ms-option' + (selected ? ' selected' : '') + (inOther ? ' in-other' : '') + (highlighted ? ' highlighted' : '') + '" data-val="' + esc(option) + '">' +
      esc(inOther ? (option + ' \u21c4') : option) + '\x3C/div>';
    if (!selected) visibleIdx++;
  });
  dd.innerHTML = html;
  dd.classList.add('open');
  dd.querySelectorAll('.ms-option').forEach(function(optionEl) {
    optionEl.addEventListener('mousedown', function(e) {
      e.preventDefault();
      _suiteAddCoinMsValue(id, this.getAttribute('data-val'));
      var input = wrap.querySelector('.ms-input');
      if (input) input.value = '';
      _suiteShowCoinDd(id, '');
    });
  });
}

/* ── Main render ────────────────────────────────────────────── */
function _suiteRender() {
  var el = document.getElementById(_suiteState.containerId);
  if (!el) return;
  var existingExpander = document.getElementById('exp-suite');
  if (existingExpander) _suiteState.expanded = existingExpander.classList.contains('open');

  var h = '';
  h += '\x3Cdiv class="expander' + (_suiteState.expanded ? ' open' : '') + '" id="exp-suite">';
  h += '\x3Cdiv class="expander-header" onclick="_suiteToggleExpander()">';
  h += '\x3Cspan class="arrow">\u25B6\x3C/span> Suite Mode';
  if (_suiteState.enabled) {
    h += ' \x3Cspan style="color:var(--green);font-size:var(--fs-xs);margin-left:6px">ENABLED (' +
         _suiteState.scenarios.length + ' scenario' + (_suiteState.scenarios.length !== 1 ? 's' : '') + ')\x3C/span>';
  }
  h += '\x3C/div>';
  h += '\x3Cdiv class="expander-body">';

  h += '\x3Cdiv style="display:flex;align-items:center;gap:var(--sp-md);margin-bottom:var(--sp-md)">';
  h += '\x3Cdiv class="chk-row">\x3Cinput type="checkbox" id="suite-enabled"' +
       (_suiteState.enabled ? ' checked' : '') +
       ' onchange="_suiteToggle(this.checked)">';
  h += '\x3Clabel for="suite-enabled">\x3Cspan data-tip="Run multiple scenarios with different parameters,\ncoin sets, date ranges, or exchanges.\nResults are aggregated for comparison.">Enable Suite Mode\x3C/span>\x3C/label>\x3C/div>';
  h += '\x3C/div>';

  if (_suiteState.scenarioGeneratorEnabled) {
    h += _suiteRenderScenarioGenerator();
  }

  if (_suiteState.enabled) {
    h += '\x3Cdiv style="display:flex;gap:var(--sp-sm);flex-wrap:wrap;margin-bottom:var(--sp-md)">';
    h += '\x3Cspan style="font-size:var(--fs-xs);color:var(--text-dim);align-self:center">Templates:\x3C/span>';
    var tKeys = Object.keys(_suiteTemplates);
    for (var ti = 0; ti < tKeys.length; ti++) {
      h += '\x3Cbutton type="button" class="act-btn" onclick="_suiteApplyTemplate(\'' +
           tKeys[ti].replace(/'/g, "\\'") + '\')">' + tKeys[ti] + '\x3C/button>';
    }
    h += '\x3Cbutton type="button" class="act-btn" onclick="_suiteResetToBase()" title="Clear all scenarios and reset to a single base scenario" ' +
         'style="border-color:var(--orange);color:var(--orange)">Reset to Base\x3C/button>';
    h += '\x3C/div>';

    h += _suiteRenderScenariosTable();

    if (_suiteState.editIdx >= 0) {
      h += _suiteRenderScenarioEditor();
    }

    h += _suiteRenderAggregate();
  }

  h += '\x3C/div>\x3C/div>';
  el.innerHTML = h;
  if (_suiteState.enabled && _suiteState.editIdx >= 0) {
    var current = _suiteState.scenarios[_suiteState.editIdx] || {};
    _suiteInitCoinMs('suite-sc-coins-ms', current.coins || []);
    _suiteInitCoinMs('suite-sc-ign-ms', current.ignored_coins || []);
    _suiteInitCoinSources(current);
    if (typeof cfgWireDateRange === 'function') cfgWireDateRange('suite-sc-start', 'suite-sc-end');
  }
}

function _suiteToggleExpander() {
  toggleExpander('exp-suite');
  var expander = document.getElementById('exp-suite');
  if (expander) _suiteState.expanded = expander.classList.contains('open');
}

function _suiteScenarioContext() {
  if (!_suiteState.getScenarioContext) return {};
  var context = _suiteState.getScenarioContext();
  return context && typeof context === 'object' ? context : {};
}

function _suiteRenderScenarioGenerator() {
  var context = _suiteScenarioContext();
  var preview = _suiteState.scenarioPreview;
  var contextBalance = Number(context.starting_balance);
  var defaultBalance = isFinite(contextBalance) && contextBalance >= 1 ? contextBalance : 1000;
  var draft = Object.assign({
    template: 'rolling_windows', window_days: 90, stride_days: 30, training_windows: 4,
    holdout_windows: 1, exchange_mode: 'inherit', balance_multiplier: 2,
    starting_balance: defaultBalance, refill_cost: 0, cooldown_days: 0,
  }, _suiteState.scenarioGeneratorDraft || {});
  var multiplier = Number(draft.balance_multiplier);
  if (!isFinite(multiplier) || multiplier < 1.01 || multiplier > 100) draft.balance_multiplier = 2;
  var startingBalance = Number(draft.starting_balance);
  if (!isFinite(startingBalance) || startingBalance < 1) draft.starting_balance = defaultBalance;
  var refillCost = Number(draft.refill_cost);
  if (!isFinite(refillCost) || refillCost < 0) draft.refill_cost = 0;
  var cooldownDays = Number(draft.cooldown_days);
  if (!isFinite(cooldownDays) || cooldownDays < 0) draft.cooldown_days = 0;
  var isSweep = draft.template === 'sweep_cycles';
  var h = '\x3Cdiv style="border:1px solid var(--border);border-radius:6px;padding:var(--sp-md);margin-bottom:var(--sp-md);background:rgba(77,166,255,.035)">';
  h += '\x3Cdiv style="display:flex;align-items:start;justify-content:space-between;gap:var(--sp-md);margin-bottom:var(--sp-sm)">';
  h += '\x3Cdiv>\x3Cdiv style="font-weight:650">PB8 Scenario Generator\x3C/div>';
  h += '\x3Cdiv style="font-size:var(--fs-xs);color:var(--text-dim);margin-top:2px">Deterministic preview using base dates ' +
    esc(context.start_date || 'unset') + ' to ' + esc(context.end_date || 'unset') + '.\x3C/div>\x3C/div>';
  h += '\x3Cdiv style="display:flex;gap:var(--sp-xs)">';
  h += '\x3Cbutton type="button" class="act-btn" onclick="_suiteOpenScenarioGeneratorGuide()">Guide\x3C/button>';
  h += '\x3Cbutton type="button" class="act-btn" onclick="_suiteRecalculateScenarioGenerator()">Recalculate\x3C/button>';
  h += '\x3Cbutton type="button" class="act-btn" id="suite-generator-preview-btn" onclick="_suitePreviewScenarioTemplate()">Preview\x3C/button>\x3C/div>\x3C/div>';
  h += '\x3Cdiv style="display:grid;grid-template-columns:repeat(auto-fit,minmax(125px,1fr));gap:var(--sp-sm);align-items:end">';
  h += '\x3Cdiv class="form-group">\x3Clabel>\x3Cspan data-tip="Choose rolling training windows, chronological walk-forward training with untouched holdout windows, or walk-forward windows with sweep evaluation provenance.">Template\x3C/span>\x3C/label>\x3Cselect id="suite-generator-template" onchange="_suiteUpdateGeneratorFields(this.value)">';
  h += '\x3Coption value="rolling_windows"' + (draft.template === 'rolling_windows' ? ' selected' : '') + '>Rolling Windows\x3C/option>';
  h += '\x3Coption value="walk_forward"' + (draft.template === 'walk_forward' ? ' selected' : '') + '>Walk-Forward\x3C/option>';
  h += '\x3Coption value="sweep_cycles"' + (isSweep ? ' selected' : '') + '>Sweep Cycles\x3C/option>\x3C/select>\x3C/div>';
  h += '\x3Cdiv class="form-group">\x3Clabel>\x3Cspan data-tip="Number of calendar days included in each generated scenario.">Window days\x3C/span>\x3C/label>\x3Cinput type="number" id="suite-generator-window" min="1" max="3650" value="' + esc(draft.window_days) + '" onchange="_suiteAlignSweepStride()">\x3C/div>';
  h += '\x3Cdiv class="form-group">\x3Clabel>\x3Cspan data-tip="Distance between consecutive window end dates. Sweep Cycles calculates this automatically as Window days plus Cooldown days.">Stride days\x3C/span>\x3C/label>\x3Cinput type="number" id="suite-generator-stride" min="1" max="3650" value="' + esc(draft.stride_days) + '"' + (isSweep ? ' readonly' : '') + '>\x3C/div>';
  h += '\x3Cdiv class="form-group">\x3Clabel>\x3Cspan data-tip="Windows applied to optimizer training. Sweep Cycles calculates the maximum complete count automatically after reserving holdouts.">Training windows\x3C/span>\x3C/label>\x3Cinput type="number" id="suite-generator-training" min="1" max="48" value="' + esc(draft.training_windows) + '"' + (isSweep ? ' readonly' : '') + '>\x3C/div>';
  h += '\x3Cdiv class="form-group" id="suite-generator-holdout-wrap" style="' + (draft.template === 'rolling_windows' ? 'display:none' : '') + '">\x3Clabel>\x3Cspan data-tip="Untouched windows shown separately for later validation. Holdout windows are never applied to backtest.scenarios.">Holdout windows\x3C/span>\x3C/label>\x3Cinput type="number" id="suite-generator-holdout" min="0" max="16" value="' + esc(draft.holdout_windows) + '" onchange="_suiteAlignSweepStride()">\x3C/div>';
  h += '\x3Cdiv class="form-group">\x3Clabel>\x3Cspan data-tip="Inherit base evaluates the selected exchanges together. One per exchange creates a separate scenario for every selected base exchange.">Exchange mode\x3C/span>\x3C/label>\x3Cselect id="suite-generator-exchange-mode">';
  h += '\x3Coption value="inherit"' + (draft.exchange_mode === 'inherit' ? ' selected' : '') + '>Inherit base\x3C/option>\x3Coption value="per_exchange"' + (draft.exchange_mode === 'per_exchange' ? ' selected' : '') + '>One per exchange\x3C/option>\x3C/select>\x3C/div>';
  h += '\x3Cdiv class="form-group suite-generator-sweep" style="' + (isSweep ? '' : 'display:none') + '">\x3Clabel>\x3Cspan data-tip="Absolute sweep target equals Starting balance multiplied by this value. When reached at a window end, all balance above Starting balance is swept and the next window resets.">Balance multiplier\x3C/span>\x3C/label>\x3Cinput type="number" id="suite-generator-multiplier" min="1.01" max="100" step="0.01" value="' + esc(draft.balance_multiplier) + '">\x3C/div>';
  h += '\x3Cdiv class="form-group suite-generator-sweep" style="' + (isSweep ? '' : 'display:none') + '">\x3Clabel>\x3Cspan data-tip="Working capital at the beginning of the first cycle and after every sweep or refill reset.">Starting balance\x3C/span>\x3C/label>\x3Cinput type="number" id="suite-generator-balance" min="1" value="' + esc(draft.starting_balance) + '">\x3C/div>';
  h += '\x3Cdiv class="form-group suite-generator-sweep" style="' + (isSweep ? '' : 'display:none') + '">\x3Clabel>\x3Cspan data-tip="Additional external cost booked whenever a loss window is refilled back to Starting balance.">Refill cost\x3C/span>\x3C/label>\x3Cinput type="number" id="suite-generator-refill" min="0" value="' + esc(draft.refill_cost) + '">\x3C/div>';
  h += '\x3Cdiv class="form-group suite-generator-sweep" style="' + (isSweep ? '' : 'display:none') + '">\x3Clabel>\x3Cspan data-tip="Minimum no-trading gap between sweep-cycle windows. Stride must be at least window days plus cooldown days.">Cooldown days\x3C/span>\x3C/label>\x3Cinput type="number" id="suite-generator-cooldown" min="0" max="3650" value="' + esc(draft.cooldown_days) + '" onchange="_suiteAlignSweepStride()">\x3C/div>';
  h += '\x3C/div>';

  if (preview) {
    h += '\x3Cdiv style="margin-top:var(--sp-md);padding-top:var(--sp-sm);border-top:1px solid var(--border)">';
    h += '\x3Cdiv style="display:flex;align-items:center;justify-content:space-between;gap:var(--sp-md);margin-bottom:var(--sp-sm)">';
    h += '\x3Cspan style="font-size:var(--fs-sm);font-weight:600">Preview: ' + preview.training_scenarios.length +
      ' training, ' + preview.holdout_scenarios.length + ' holdout\x3C/span>';
    h += '\x3Cbutton type="button" class="act-btn" onclick="_suiteApplyScenarioPreview()">Apply Training Scenarios\x3C/button>\x3C/div>';
    h += '\x3Cdiv style="max-height:190px;overflow:auto">\x3Ctable class="tbl" style="font-size:var(--fs-sm)">';
    h += '\x3Cthead>\x3Ctr>\x3Cth>Use\x3C/th>\x3Cth>Label\x3C/th>\x3Cth>Period\x3C/th>\x3C/tr>\x3C/thead>\x3Ctbody>';
    preview.training_scenarios.concat(preview.holdout_scenarios).forEach(function(scenario, index) {
      var training = index < preview.training_scenarios.length;
      h += '\x3Ctr>\x3Ctd>' + (training ? 'Train' : 'Holdout') + '\x3C/td>\x3Ctd>' + esc(scenario.label) +
        '\x3C/td>\x3Ctd>' + esc(scenario.start_date) + ' to ' + esc(scenario.end_date) + '\x3C/td>\x3C/tr>';
    });
    h += '\x3C/tbody>\x3C/table>\x3C/div>';
    (preview.warnings || []).forEach(function(warning) {
      h += '\x3Cdiv style="font-size:var(--fs-sm);line-height:1.45;color:var(--orange);margin-top:4px">' + esc(warning) + '\x3C/div>';
    });
    h += '\x3C/div>';
  }
  h += '\x3C/div>';
  return h;
}

function _suiteUpdateGeneratorFields(template) {
  var holdout = document.getElementById('suite-generator-holdout-wrap');
  if (holdout) holdout.style.display = template === 'rolling_windows' ? 'none' : '';
  document.querySelectorAll('.suite-generator-sweep').forEach(function(field) {
    field.style.display = template === 'sweep_cycles' ? '' : 'none';
  });
  if (template === 'sweep_cycles') _suiteAlignSweepStride();
}

function _suiteAlignSweepStride() {
  var template = document.getElementById('suite-generator-template');
  var windowInput = document.getElementById('suite-generator-window');
  var strideInput = document.getElementById('suite-generator-stride');
  var cooldownInput = document.getElementById('suite-generator-cooldown');
  var trainingInput = document.getElementById('suite-generator-training');
  var holdoutInput = document.getElementById('suite-generator-holdout');
  if (!template || template.value !== 'sweep_cycles' || !windowInput || !strideInput || !cooldownInput || !trainingInput || !holdoutInput) return;
  var minimum = Math.max(1, parseInt(windowInput.value, 10) || 1) + Math.max(0, parseInt(cooldownInput.value, 10) || 0);
  strideInput.value = String(minimum);
  var context = _suiteScenarioContext();
  var startMs = Date.parse(String(context.start_date || '') + 'T00:00:00Z');
  var endMs = Date.parse(String(context.end_date || '') + 'T00:00:00Z');
  if (!isFinite(startMs) || !isFinite(endMs) || endMs < startMs) return;
  var availableDays = Math.floor((endMs - startMs) / 86400000) + 1;
  var windowDays = Math.max(1, parseInt(windowInput.value, 10) || 1);
  var totalWindows = availableDays < windowDays ? 0 : 1 + Math.floor((availableDays - windowDays) / minimum);
  var holdouts = Math.max(0, parseInt(holdoutInput.value, 10) || 0);
  trainingInput.value = String(Math.max(0, totalWindows - holdouts));
}

function _suiteGeneratorInteger(id) {
  var input = document.getElementById(id);
  return input ? parseInt(input.value, 10) : 0;
}

function _suiteGeneratorNumber(id) {
  var input = document.getElementById(id);
  return input ? Number(input.value) : 0;
}

function _suiteScenarioContextSignature(context) {
  return JSON.stringify({
    start_date: context.start_date || null,
    end_date: context.end_date || null,
    exchanges: Array.isArray(context.exchanges) ? context.exchanges : [],
  });
}

function _suiteCaptureScenarioGeneratorDraft() {
  var templateNode = document.getElementById('suite-generator-template');
  if (!templateNode) return null;
  var template = templateNode.value;
  var draft = {
    template: template,
    window_days: _suiteGeneratorInteger('suite-generator-window'),
    stride_days: _suiteGeneratorInteger('suite-generator-stride'),
    training_windows: _suiteGeneratorInteger('suite-generator-training'),
    holdout_windows: template === 'rolling_windows' ? 0 : _suiteGeneratorInteger('suite-generator-holdout'),
    exchange_mode: document.getElementById('suite-generator-exchange-mode').value,
    auto_windows: template === 'sweep_cycles',
  };
  if (template === 'sweep_cycles') {
    draft.balance_multiplier = _suiteGeneratorNumber('suite-generator-multiplier');
    draft.starting_balance = _suiteGeneratorNumber('suite-generator-balance');
    draft.refill_cost = _suiteGeneratorNumber('suite-generator-refill');
    draft.cooldown_days = _suiteGeneratorInteger('suite-generator-cooldown');
  }
  return draft;
}

function _suiteRecalculateScenarioGenerator() {
  var template = document.getElementById('suite-generator-template');
  var balanceInput = document.getElementById('suite-generator-balance');
  var contextBalance = Number(_suiteScenarioContext().starting_balance);
  if (template && template.value === 'sweep_cycles' && balanceInput && isFinite(contextBalance) && contextBalance >= 1) {
    balanceInput.value = String(contextBalance);
  }
  _suiteAlignSweepStride();
  var draft = _suiteCaptureScenarioGeneratorDraft();
  if (!draft) return;
  _suiteState.scenarioGeneratorDraft = draft;
  _suiteState.scenarioPreview = null;
  _suiteState.scenarioPreviewContext = '';
  _suiteState.scenarioRequestId += 1;
  _suiteRender();
  toast('Scenario Generator recalculated from current dates, exchanges, and base balance', 'ok');
}

function _suiteOpenScenarioGeneratorGuide() {
  if (typeof window.PBGUI_SCENARIO_GENERATOR_HELP === 'function') {
    window.PBGUI_SCENARIO_GENERATOR_HELP();
    return;
  }
  toast('Scenario Generator guide is unavailable', 'err');
}

function _suitePreviewScenarioTemplate() {
  _suiteAlignSweepStride();
  var context = _suiteScenarioContext();
  var draft = _suiteCaptureScenarioGeneratorDraft();
  if (!draft) return;
  var template = draft.template;
  var payload = Object.assign({}, draft, {
    start_date: context.start_date,
    end_date: context.end_date,
    exchanges: Array.isArray(context.exchanges) ? context.exchanges : [],
  });
  _suiteState.scenarioGeneratorDraft = JSON.parse(JSON.stringify(payload));
  var contextSignature = _suiteScenarioContextSignature(context);
  var requestId = ++_suiteState.scenarioRequestId;
  var button = document.getElementById('suite-generator-preview-btn');
  if (button) { button.disabled = true; button.textContent = 'Generating...'; }
  apiFetch('/scenario-templates/preview', { method: 'POST', body: JSON.stringify(payload) }).then(function(preview) {
    if (requestId !== _suiteState.scenarioRequestId) return;
    _suiteState.scenarioPreview = preview;
    _suiteState.scenarioPreviewContext = contextSignature;
    _suiteRender();
  }).catch(function(error) {
    if (requestId !== _suiteState.scenarioRequestId) return;
    toast(error.message || 'Scenario preview failed', 'err');
    if (button) { button.disabled = false; button.textContent = 'Preview'; }
  });
}

function _suiteApplyScenarioPreview() {
  var preview = _suiteState.scenarioPreview;
  if (!preview || !Array.isArray(preview.training_scenarios)) return;
  if (_suiteScenarioContextSignature(_suiteScenarioContext()) !== _suiteState.scenarioPreviewContext) {
    toast('Base dates or exchanges changed. Preview the scenario template again before applying.', 'err');
    return;
  }
  _suiteState.enabled = true;
  _suiteState.scenarios = JSON.parse(JSON.stringify(preview.training_scenarios));
  _suiteState.aggregate = JSON.parse(JSON.stringify(preview.reducer || { default: 'mean' }));
  _suiteState.scenarioTemplate = JSON.parse(JSON.stringify(preview.provenance || null));
  _suiteState.editIdx = -1;
  if (_suiteState.onApplyScenarioPreview) _suiteState.onApplyScenarioPreview(preview);
  _suiteState.applyingGeneratedTemplate = true;
  _suiteNotifyStructuredSync();
  _suiteState.applyingGeneratedTemplate = false;
  _suiteRender();
  toast('Applied ' + _suiteState.scenarios.length + ' generated training scenarios', 'ok');
}

/* ── Toggle enabled ─────────────────────────────────────────── */
function _suiteToggle(on) {
  _suiteState.enabled = on;
  if (on) _suiteState.expanded = true;
  if (on && _suiteState.scenarios.length === 0) {
    _suiteState.scenarios.push({ label: 'base' });
  }
  _suiteRender();
  _suiteNotifyStructuredSync();
}

/* ── Apply built-in template ────────────────────────────────── */
function _suiteApplyTemplate(name) {
  var t = _suiteTemplates[name];
  if (!t) return;
  _suiteState.scenarios = JSON.parse(JSON.stringify(t.scenarios));
  _suiteState.aggregate = JSON.parse(JSON.stringify(t.aggregate));
  _suiteState.editIdx = -1;

  if (typeof cfgGetMs === 'function' && typeof cfgSetMs === 'function') {
    var baseEx = cfgGetMs('ms-cfg-exchanges');
    var needed = {};
    for (var i = 0; i < _suiteState.scenarios.length; i++) {
      var se = _suiteState.scenarios[i].exchanges;
      if (se) for (var j = 0; j < se.length; j++) needed[se[j]] = true;
    }
    var added = [];
    for (var ex in needed) {
      if (baseEx.indexOf(ex) < 0) { baseEx.push(ex); added.push(ex); }
    }
    if (added.length) {
      cfgSetMs('ms-cfg-exchanges', baseEx);
      toast('Added exchange(s) ' + added.join(', ') + ' to base config', 'ok');
    }
  }

  _suiteRender();
  _suiteNotifyStructuredSync();
  toast('Template "' + name + '" applied', 'ok');
}

/* ── Reset to single base scenario ──────────────────────────── */
function _suiteResetToBase() {
  _suiteState.scenarios = [{ label: 'base' }];
  _suiteState.aggregate = { default: 'mean' };
  _suiteState.editIdx = -1;
  _suiteRender();
  _suiteNotifyStructuredSync();
  toast('Reset to base scenario', 'ok');
}

/* ── Scenarios table ────────────────────────────────────────── */
function _suiteRenderScenariosTable() {
  var s = _suiteState.scenarios;
  var h = '\x3Cdiv style="margin-bottom:var(--sp-md)">';
  h += '\x3Cdiv style="display:flex;align-items:center;justify-content:space-between;margin-bottom:var(--sp-sm)">';
  h += '\x3Cspan style="font-size:var(--fs-sm);font-weight:600">Scenarios (' + s.length + ')\x3C/span>';
  h += '\x3Cbutton type="button" class="act-btn" onclick="_suiteAddScenario()">+ Add Scenario\x3C/button>';
  h += '\x3C/div>';

  if (s.length === 0) {
    h += '\x3Cdiv style="color:var(--text-dim);font-size:var(--fs-sm);padding:var(--sp-sm)">No scenarios defined. Add one or use a template.\x3C/div>';
  } else {
    h += '\x3Ctable class="tbl" style="font-size:var(--fs-sm)">';
    h += '\x3Cthead>\x3Ctr>\x3Cth style="width:30%">Label\x3C/th>\x3Cth>Details\x3C/th>\x3Cth style="width:120px">Actions\x3C/th>\x3C/tr>\x3C/thead>';
    h += '\x3Ctbody>';
    for (var i = 0; i < s.length; i++) {
      var sc = s[i];
      var isEditing = (_suiteState.editIdx === i);
      h += '\x3Ctr' + (isEditing ? ' style="background:rgba(77,166,255,.06)"' : '') + '>';
      h += '\x3Ctd style="font-weight:600">' + esc(sc.label || '(unnamed)') + '\x3C/td>';
      h += '\x3Ctd>' + _suiteScenarioSummary(sc) + '\x3C/td>';
      h += '\x3Ctd>';
      h += '\x3Cbutton type="button" class="act-btn" onclick="_suiteEditScenario(' + i + ')" title="Edit">' +
           (isEditing ? 'Editing' : 'Edit') + '\x3C/button> ';
      h += '\x3Cbutton type="button" class="act-btn act-btn-danger" onclick="_suiteRemoveScenario(' + i + ')" title="Remove">\u00d7\x3C/button>';
      if (i > 0) {
        h += ' \x3Cbutton type="button" class="act-btn" onclick="_suiteMoveScenario(' + i + ',-1)" title="Move up">\u2191\x3C/button>';
      }
      if (i < s.length - 1) {
        h += ' \x3Cbutton type="button" class="act-btn" onclick="_suiteMoveScenario(' + i + ',1)" title="Move down">\u2193\x3C/button>';
      }
      h += '\x3C/td>\x3C/tr>';
    }
    h += '\x3C/tbody>\x3C/table>';
  }
  h += '\x3C/div>';
  return h;
}

/* ── Short summary of scenario ──────────────────────────────── */
function _suiteScenarioSummary(sc) {
  var parts = [];
  if (sc.exchanges && sc.exchanges.length > 0) parts.push('ex: ' + sc.exchanges.join(','));
  if (sc.start_date || sc.end_date) parts.push((sc.start_date || '...') + ' \u2192 ' + (sc.end_date || '...'));
  if (sc.coins && sc.coins.length > 0) parts.push('coins: ' + sc.coins.length);
  if (sc.ignored_coins && sc.ignored_coins.length > 0) parts.push('ignored: ' + sc.ignored_coins.length);
  if (sc.coin_sources && Object.keys(sc.coin_sources).length > 0) parts.push('coin_src: ' + Object.keys(sc.coin_sources).length);
  if (sc.overrides && Object.keys(sc.overrides).length > 0) parts.push('overrides: ' + Object.keys(sc.overrides).length);
  return parts.length > 0
    ? '\x3Cspan style="color:var(--text-dim);font-size:var(--fs-xs)">' + esc(parts.join(' | ')) + '\x3C/span>'
    : '\x3Cspan style="color:var(--text-dim);font-size:var(--fs-xs)">base config\x3C/span>';
}

/* ── Scenario CRUD ──────────────────────────────────────────── */
function _suiteAddScenario() {
  _suiteState.scenarios.push({ label: 'scenario_' + (_suiteState.scenarios.length + 1) });
  _suiteState.editIdx = _suiteState.scenarios.length - 1;
  _suiteRender();
  _suiteNotifyStructuredSync();
}

function _suiteEditScenario(idx) {
  // Save current editing scenario before switching
  if (_suiteState.editIdx >= 0 && _suiteState.editIdx !== idx) {
    _suiteSaveEditingScenario();
  }
  _suiteState.editIdx = (_suiteState.editIdx === idx) ? -1 : idx;
  _suiteRender();
  _suiteNotifyStructuredSync();
}

function _suiteRemoveScenario(idx) {
  _suiteState.scenarios.splice(idx, 1);
  if (_suiteState.editIdx === idx) _suiteState.editIdx = -1;
  else if (_suiteState.editIdx > idx) _suiteState.editIdx--;
  _suiteRender();
  _suiteNotifyStructuredSync();
}

function _suiteMoveScenario(idx, dir) {
  var newIdx = idx + dir;
  if (newIdx < 0 || newIdx >= _suiteState.scenarios.length) return;
  var tmp = _suiteState.scenarios[idx];
  _suiteState.scenarios[idx] = _suiteState.scenarios[newIdx];
  _suiteState.scenarios[newIdx] = tmp;
  if (_suiteState.editIdx === idx) _suiteState.editIdx = newIdx;
  else if (_suiteState.editIdx === newIdx) _suiteState.editIdx = idx;
  _suiteRender();
  _suiteNotifyStructuredSync();
}

/* ── Scenario editor form ───────────────────────────────────── */
function _suiteRenderScenarioEditor() {
  var sc = _suiteState.scenarios[_suiteState.editIdx];
  if (!sc) return '';

  var h = '\x3Cdiv style="border:1px solid var(--accent);border-radius:6px;padding:var(--sp-md);margin-bottom:var(--sp-md);background:rgba(77,166,255,.03)">';
  h += '\x3Cdiv style="display:flex;justify-content:space-between;align-items:center;margin-bottom:var(--sp-sm)">';
  h += '\x3Cspan style="font-size:var(--fs-sm);font-weight:600;color:var(--accent)">Edit Scenario: ' + esc(sc.label || '') + '\x3C/span>';
  h += '\x3Cbutton type="button" class="act-btn" onclick="_suiteSaveAndClose()">Done\x3C/button>';
  h += '\x3C/div>';

  /* Label */
  h += '\x3Cdiv class="form-row cols-4">';
  h += '\x3Cdiv class="form-group">\x3Clabel>\x3Cspan data-tip="Unique label for this scenario.\nUsed in result filenames.">label\x3C/span>\x3C/label>';
  h += '\x3Cinput type="text" id="suite-sc-label" value="' + esc(sc.label || '') + '">\x3C/div>';

  /* Start / End dates */
  h += _suiteBuildDateField('suite-sc-start', 'start_date', sc.start_date || '', 'Override start date for this scenario.\nLeave empty to use base config.', 'e.g. 2023-01-01');
  h += _suiteBuildDateField('suite-sc-end', 'end_date', sc.end_date || '', 'Override end date for this scenario.\nLeave empty to use base config.', 'e.g. now');
  h += '\x3C/div>';

  /* Exchanges checkboxes */
  var scEx = sc.exchanges || [];
  h += '\x3Cdiv class="form-group" style="margin-bottom:var(--sp-md)">';
  h += '\x3Clabel>\x3Cspan data-tip="Override exchanges for this scenario.\nLeave all unchecked to use base config.">exchanges\x3C/span>\x3C/label>';
  h += '\x3Cdiv style="display:flex;gap:var(--sp-md);flex-wrap:wrap">';
  for (var ei = 0; ei < _suiteState.exchanges.length; ei++) {
    var ex = _suiteState.exchanges[ei];
    h += '\x3Cdiv class="chk-row">\x3Cinput type="checkbox" id="suite-sc-ex-' + ex + '"' +
         (scEx.indexOf(ex) >= 0 ? ' checked' : '') + '>\x3Clabel for="suite-sc-ex-' + ex + '">' + ex + '\x3C/label>\x3C/div>';
  }
  h += '\x3C/div>\x3C/div>';

  /* Coins */
  h += '\x3Cdiv class="form-row cols-2">';
  h += _suiteBuildCoinMsField('suite-sc-coins-ms', 'coins', 'Override approved coins for this scenario.\nSelect one or more coins from the currently loaded exchange universe.\nLeave empty to use base config.', 'Type to search...');
  h += _suiteBuildCoinMsField('suite-sc-ign-ms', 'ignored_coins', 'Override ignored coins for this scenario.\nSelect one or more coins from the currently loaded exchange universe.\nLeave empty to use base config.', 'Type to search...');
  h += '\x3C/div>';

  /* coin_sources */
  h += _suiteBuildCoinSourcesEditor(sc.coin_sources || {});

  /* Overrides */
  h += _suiteRenderOverrides(sc);

  h += '\x3C/div>';
  return h;
}

/* ── Overrides editor ───────────────────────────────────────── */
function _suiteRenderOverrides(sc) {
  var ov = sc.overrides || {};
  var keys = Object.keys(ov);

  var h = '\x3Cdiv style="margin-top:var(--sp-sm)">';
  h += '\x3Cdiv style="display:flex;align-items:center;justify-content:space-between;margin-bottom:var(--sp-xs)">';
  h += '\x3Clabel style="font-size:var(--fs-xs);color:var(--text-dim)">\x3Cspan data-tip="Override bot parameters for this scenario.\nFormat: bot.side.param_name = value\nOverrides are applied on top of the base config.">overrides (' + keys.length + ')\x3C/span>\x3C/label>';
  h += '\x3Cbutton type="button" class="act-btn" onclick="_suiteAddOverride()">+ Override\x3C/button>';
  h += '\x3C/div>';

  if (keys.length > 0) {
    h += '\x3Ctable class="tbl" style="font-size:var(--fs-xs);margin-bottom:var(--sp-xs)">';
    h += '\x3Cthead>\x3Ctr>\x3Cth>Side\x3C/th>\x3Cth>Parameter\x3C/th>\x3Cth>Value\x3C/th>\x3Cth style="width:40px">\x3C/th>\x3C/tr>\x3C/thead>';
    h += '\x3Ctbody>';
    for (var ki = 0; ki < keys.length; ki++) {
      var k = keys[ki];
      // Parse dotted path: "bot.long.param" → side=long, param=param
      var kParts = k.split('.');
      var side, param;
      if (kParts.length >= 3 && kParts[0] === 'bot') {
        side = kParts[1]; param = kParts.slice(2).join('.');
      } else if (kParts.length >= 2) {
        side = kParts[0]; param = kParts.slice(1).join('.');
      } else {
        side = ''; param = k;
      }
      h += '\x3Ctr>';
      h += '\x3Ctd>' + esc(side) + '\x3C/td>';
      h += '\x3Ctd>' + esc(param) + '\x3C/td>';
       h += '\x3Ctd>\x3Cinput type="text" id="suite-ov-val-' + ki + '" value="' + esc(String(ov[k])) + '" ' +
         'style="' + _suiteInlineInputStyle + ';width:100px" ' +
           'onchange="_suiteUpdateOverrideVal(' + ki + ')">\x3C/td>';
      h += '\x3Ctd>\x3Cbutton type="button" class="act-btn act-btn-danger" onclick="_suiteRemoveOverride(\'' +
           k.replace(/'/g, "\\'") + '\')">\u00d7\x3C/button>\x3C/td>';
      h += '\x3C/tr>';
    }
    h += '\x3C/tbody>\x3C/table>';
  }

  /* Add override row */
  h += '\x3Cdiv id="suite-add-ov-row" style="display:none;margin-top:var(--sp-xs)">';
  h += '\x3Cdiv class="form-row cols-4" style="align-items:end">';
  h += '\x3Cdiv class="form-group">\x3Clabel>Side\x3C/label>\x3Cselect id="suite-ov-side">';
  h += '\x3Coption value="long">long\x3C/option>\x3Coption value="short">short\x3C/option>\x3C/select>\x3C/div>';
  h += '\x3Cdiv class="form-group">\x3Clabel>Parameter\x3C/label>\x3Cselect id="suite-ov-param">';
  var bp = _suiteState.botParams;
  for (var pi = 0; pi < bp.length; pi++) {
    var pk = typeof bp[pi] === 'string' ? bp[pi] : bp[pi].key;
    h += '\x3Coption value="' + esc(pk) + '">' + esc(pk) + '\x3C/option>';
  }
  h += '\x3C/select>\x3C/div>';
  h += '\x3Cdiv class="form-group">\x3Clabel>Value\x3C/label>\x3Cinput type="text" id="suite-ov-value" placeholder="0.5">\x3C/div>';
  h += '\x3Cdiv class="form-group">\x3Cbutton type="button" class="act-btn" onclick="_suiteConfirmOverride()" style="height:var(--btn-h)">Add\x3C/button>\x3C/div>';
  h += '\x3C/div>\x3C/div>';

  h += '\x3C/div>';
  return h;
}

function _suiteAddOverride() {
  var row = document.getElementById('suite-add-ov-row');
  if (row) row.style.display = '';
}

function _suiteConfirmOverride() {
  var side = document.getElementById('suite-ov-side').value;
  var param = document.getElementById('suite-ov-param').value;
  var val = document.getElementById('suite-ov-value').value.trim();
  if (!param || val === '') { toast('Parameter and value required', 'err'); return; }

  var key = 'bot.' + side + '.' + param;
  // Parse value: try number, then boolean, fallback string
  var parsed = val;
  if (val === 'true') parsed = true;
  else if (val === 'false') parsed = false;
  else if (!isNaN(Number(val)) && val !== '') parsed = Number(val);

  _suiteSaveEditingScenario();
  var sc = _suiteState.scenarios[_suiteState.editIdx];
  if (!sc) return;
  if (!sc.overrides) sc.overrides = {};
  sc.overrides[key] = parsed;
  _suiteRender();
  _suiteNotifyStructuredSync();
}

function _suiteRemoveOverride(key) {
  _suiteSaveEditingScenario();
  var sc = _suiteState.scenarios[_suiteState.editIdx];
  if (!sc || !sc.overrides) return;
  delete sc.overrides[key];
  if (Object.keys(sc.overrides).length === 0) delete sc.overrides;
  _suiteRender();
  _suiteNotifyStructuredSync();
}

function _suiteUpdateOverrideVal(ki) {
  _suiteSaveEditingScenario();
  _suiteNotifyStructuredSync();
}

/* ── Save editing scenario from form → state ─────────────── */
function _suiteSaveEditingScenario() {
  var idx = _suiteState.editIdx;
  if (idx < 0 || idx >= _suiteState.scenarios.length) return;

  var labelEl = document.getElementById('suite-sc-label');
  if (!labelEl) return;  // editor form not rendered

  var sc = _suiteState.scenarios[idx];
  sc.label = labelEl.value.trim() || 'unnamed';

  // Start / end dates
  var sd = (document.getElementById('suite-sc-start') || {}).value || '';
  var ed = (document.getElementById('suite-sc-end') || {}).value || '';
  if (sd.trim()) sc.start_date = sd.trim(); else delete sc.start_date;
  if (ed.trim()) sc.end_date = ed.trim(); else delete sc.end_date;

  // Exchanges
  var exArr = [];
  for (var ei = 0; ei < _suiteState.exchanges.length; ei++) {
    var cb = document.getElementById('suite-sc-ex-' + _suiteState.exchanges[ei]);
    if (cb && cb.checked) exArr.push(_suiteState.exchanges[ei]);
  }
  if (exArr.length > 0) sc.exchanges = exArr; else delete sc.exchanges;

  // Coins
  var coins = _suiteGetCoinMsSelected('suite-sc-coins-ms');
  if (coins.length > 0) sc.coins = coins; else delete sc.coins;

  // Ignored coins
  var ign = _suiteGetCoinMsSelected('suite-sc-ign-ms');
  if (ign.length > 0) sc.ignored_coins = ign; else delete sc.ignored_coins;

  // coin_sources
  var csObj = _suiteCollectCoinSources();
  if (Object.keys(csObj).length > 0) sc.coin_sources = csObj; else delete sc.coin_sources;

  // Overrides: re-read values from inputs
  if (sc.overrides) {
    var oKeys = Object.keys(sc.overrides);
    for (var oi = 0; oi < oKeys.length; oi++) {
      var valEl = document.getElementById('suite-ov-val-' + oi);
      if (valEl) {
        var v = valEl.value.trim();
        if (v === 'true') sc.overrides[oKeys[oi]] = true;
        else if (v === 'false') sc.overrides[oKeys[oi]] = false;
        else if (!isNaN(Number(v)) && v !== '') sc.overrides[oKeys[oi]] = Number(v);
        else sc.overrides[oKeys[oi]] = v;
      }
    }
  }
  _suiteNotifyStructuredSync();
}

function _suiteSaveAndClose() {
  _suiteSaveEditingScenario();
  _suiteState.editIdx = -1;
  _suiteRender();
  _suiteNotifyStructuredSync();
}

/* ── Aggregate settings ─────────────────────────────────────── */
function _suiteAggregateMethods() {
  return _suiteState.preserveMarketIdentifiers
    ? ['mean', 'min', 'max', 'std', 'median']
    : ['mean', 'min', 'max'];
}

function _suiteAggregateMethodOptions(selected, defaultMethod) {
  var methods = _suiteAggregateMethods();
  var h = '';
  for (var index = 0; index < methods.length; index++) {
    var method = methods[index];
    h += '\x3Coption value="' + method + '"' + (method === selected || (!selected && method === defaultMethod) ? ' selected' : '') + '>' + method + '\x3C/option>';
  }
  return h;
}

function _suiteRenderAggregate() {
  var agg = _suiteState.aggregate;
  var h = '\x3Cdiv class="expander" id="exp-suite-agg">';
  h += '\x3Cdiv class="expander-header" onclick="toggleExpander(\'exp-suite-agg\')">';
  h += '\x3Cspan class="arrow">\u25B6\x3C/span> Aggregate Settings';
  h += '\x3C/div>';
  h += '\x3Cdiv class="expander-body">';

  /* Default method */
  h += '\x3Cdiv class="form-row cols-4" style="margin-bottom:var(--sp-sm)">';
  h += '\x3Cdiv class="form-group">\x3Clabel>\x3Cspan data-tip="Default aggregation method for all metrics.\nPB8 supports mean, min, max, std, and median.">default method\x3C/span>\x3C/label>';
  h += '\x3Cselect id="suite-agg-default" onchange="_suiteUpdateAggDefault()">';
  h += _suiteAggregateMethodOptions(agg.default, 'mean');
  h += '\x3C/select>\x3C/div>';
  h += '\x3C/div>';

  /* Per-metric overrides */
  var metricKeys = Object.keys(agg).filter(function(k) { return k !== 'default'; });
  var metricOptions = _suiteAggMetricOptions(metricKeys);
  h += '\x3Cdiv style="margin-bottom:var(--sp-xs)">';
  h += '\x3Cdiv style="display:flex;align-items:center;justify-content:space-between;margin-bottom:var(--sp-xs)">';
  h += '\x3Clabel style="font-size:var(--fs-xs);color:var(--text-dim)">Metric overrides (' + metricKeys.length + ')\x3C/label>';
  h += '\x3Cbutton type="button" class="act-btn" onclick="_suiteAddAggMetric()">+ Metric\x3C/button>';
  h += '\x3C/div>';

  if (metricKeys.length > 0) {
    for (var mi = 0; mi < metricKeys.length; mi++) {
      var mk = metricKeys[mi];
      h += '\x3Cdiv style="display:flex;gap:var(--sp-sm);align-items:center;margin-bottom:2px">';
      h += '\x3Cspan style="font-size:var(--fs-xs);flex:1">' + esc(mk) + '\x3C/span>';
      h += '\x3Cselect id="suite-agg-m-' + mi + '" style="' + _suiteInlineSelectStyle + ';width:90px" onchange="_suiteUpdateAggMetric(\'' + mk.replace(/'/g, "\\'") + '\',' + mi + ')">';
      h += _suiteAggregateMethodOptions(agg[mk], 'mean');
      h += '\x3C/select>';
      h += '\x3Cbutton type="button" class="act-btn act-btn-danger" onclick="_suiteRemoveAggMetric(\'' + mk.replace(/'/g, "\\'") + '\')">\u00d7\x3C/button>';
      h += '\x3C/div>';
    }
  }

  /* Add metric row */
  h += '\x3Cdiv id="suite-add-agg-row" style="display:none;margin-top:var(--sp-xs)">';
  h += '\x3Cdiv style="display:flex;gap:var(--sp-sm);align-items:end">';
  h += '\x3Cdiv class="form-group" style="flex:1">\x3Clabel>Metric\x3C/label>\x3Cselect id="suite-agg-sel">';
  for (var ami = 0; ami < metricOptions.length; ami++) {
    h += '\x3Coption value="' + esc(metricOptions[ami]) + '">' + esc(metricOptions[ami]) + '\x3C/option>';
  }
  h += '\x3C/select>\x3C/div>';
  h += '\x3Cdiv class="form-group">\x3Clabel>Method\x3C/label>\x3Cselect id="suite-agg-method">';
  h += _suiteAggregateMethodOptions('', 'max');
  h += '\x3C/select>\x3C/div>';
  h += '\x3Cbutton type="button" class="act-btn" onclick="_suiteConfirmAggMetric()" style="height:var(--btn-h)">Add\x3C/button>';
  h += '\x3C/div>\x3C/div>';

  h += '\x3C/div>';
  h += '\x3C/div>\x3C/div>';
  return h;
}

function _suiteUpdateAggDefault() {
  var sel = document.getElementById('suite-agg-default');
  if (sel) _suiteState.aggregate.default = sel.value;
  _suiteNotifyStructuredSync();
}

function _suiteUpdateAggMetric(key, idx) {
  var sel = document.getElementById('suite-agg-m-' + idx);
  if (sel) _suiteState.aggregate[key] = sel.value;
  _suiteNotifyStructuredSync();
}

function _suiteAddAggMetric() {
  var row = document.getElementById('suite-add-agg-row');
  if (row) row.style.display = '';
}

function _suiteConfirmAggMetric() {
  var metric = document.getElementById('suite-agg-sel').value;
  var method = document.getElementById('suite-agg-method').value;
  if (!metric) return;
  _suiteState.aggregate[metric] = method;
  _suiteRender();
  _suiteNotifyStructuredSync();
}

function _suiteRemoveAggMetric(key) {
  delete _suiteState.aggregate[key];
  _suiteRender();
  _suiteNotifyStructuredSync();
}

/* ── coin_sources helpers ─────────────────────────────────── */
function _suiteInitCoinSources(sc) {
  if (typeof kvInit !== 'function') return;
  var prefix = _suiteCoinSourcesPrefix;
  kvInit(prefix, (sc && sc.coin_sources) || {});
  var exchangeEl = document.getElementById(prefix + '-exchange');
  if (exchangeEl) {
    var scenarioEx = Array.isArray(sc && sc.exchanges) ? sc.exchanges : [];
    var nextExchange = exchangeEl.value;
    if (scenarioEx.length && _suiteState.exchanges.indexOf(scenarioEx[0]) >= 0) nextExchange = scenarioEx[0];
    else if (!nextExchange || _suiteState.exchanges.indexOf(nextExchange) < 0) nextExchange = _suiteState.exchanges[0] || 'binance';
    exchangeEl.value = nextExchange;
  }
  kvUpdateExpanderCount(prefix);
  kvLoadCoins(prefix);
}

function _suiteCollectCoinSources() {
  if (typeof kvCollect === 'function') return kvCollect(_suiteCoinSourcesPrefix);
  return {};
}
