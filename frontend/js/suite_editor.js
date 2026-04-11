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
  containerId: '',
  apiBase: '',
  exchanges: ['binance','bybit','bitget','okx','hyperliquid'],
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
    aggregate: { default: 'mean', drawdown_worst_hsl: 'max' },
  },
  'n_positions Sensitivity': {
    scenarios: [
      { label: 'npos_5',  overrides: { 'bot.long.n_positions': 5,  'bot.short.n_positions': 5 } },
      { label: 'npos_10', overrides: { 'bot.long.n_positions': 10, 'bot.short.n_positions': 10 } },
      { label: 'npos_15', overrides: { 'bot.long.n_positions': 15, 'bot.short.n_positions': 15 } },
      { label: 'npos_20', overrides: { 'bot.long.n_positions': 20, 'bot.short.n_positions': 20 } },
    ],
    aggregate: { default: 'mean', drawdown_worst_hsl: 'max' },
  },
};

/* ── Aggregate metric names ─────────────────────────────────── */
var _suiteAggMetrics = [
  'adg_per_exposure', 'drawdown_worst_hsl', 'drawdown_worst_mean_1pct_hsl',
  'peak_recovery_hours_hsl', 'position_held_hours_max', 'sharpe_ratio',
  'sortino_ratio', 'profit_factor', 'total_pnl',
];

/* ── Init ───────────────────────────────────────────────────── */
function suiteInit(containerId, opts) {
  _suiteState.containerId = containerId;
  _suiteState.apiBase = (opts && opts.apiBase) || '';
  _suiteLoadBotParams();
}

/* ── Load config into suite editor ──────────────────────────── */
function suiteLoad(cfg) {
  var bt = cfg.backtest || {};
  _suiteState.enabled = !!bt.suite_enabled;
  _suiteState.scenarios = Array.isArray(bt.scenarios) ? JSON.parse(JSON.stringify(bt.scenarios)) : [];
  _suiteState.aggregate = bt.aggregate ? JSON.parse(JSON.stringify(bt.aggregate)) : { default: 'mean' };
  _suiteState.editIdx = -1;
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

/* ── Main render ────────────────────────────────────────────── */
function _suiteRender() {
  var el = document.getElementById(_suiteState.containerId);
  if (!el) return;

  var h = '';
  h += '\x3Cdiv class="expander' + (_suiteState.enabled ? ' open' : '') + '" id="exp-suite">';
  h += '\x3Cdiv class="expander-header" onclick="toggleExpander(\'exp-suite\')">';
  h += '\x3Cspan class="arrow">\u25B6\x3C/span> Suite Mode';
  if (_suiteState.enabled) {
    h += ' \x3Cspan style="color:var(--green);font-size:var(--fs-xs);margin-left:6px">ENABLED (' +
         _suiteState.scenarios.length + ' scenario' + (_suiteState.scenarios.length !== 1 ? 's' : '') + ')\x3C/span>';
  }
  h += '\x3C/div>';
  h += '\x3Cdiv class="expander-body">';

  /* Enable toggle */
  h += '\x3Cdiv style="display:flex;align-items:center;gap:var(--sp-md);margin-bottom:var(--sp-md)">';
  h += '\x3Cdiv class="chk-row">\x3Cinput type="checkbox" id="suite-enabled"' +
       (_suiteState.enabled ? ' checked' : '') +
       ' onchange="_suiteToggle(this.checked)">';
  h += '\x3Clabel for="suite-enabled">\x3Cspan data-tip="Run multiple scenarios with different parameters,\ncoin sets, date ranges, or exchanges.\nResults are aggregated for comparison.">Enable Suite Mode\x3C/span>\x3C/label>\x3C/div>';
  h += '\x3C/div>';

  if (_suiteState.enabled) {
    /* Template buttons */
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

    /* Scenarios table */
    h += _suiteRenderScenariosTable();

    /* Scenario editor (inline form) */
    if (_suiteState.editIdx >= 0) {
      h += _suiteRenderScenarioEditor();
    }

    /* Aggregate settings */
    h += _suiteRenderAggregate();
  }

  h += '\x3C/div>\x3C/div>';
  el.innerHTML = h;
}

/* ── Toggle enabled ─────────────────────────────────────────── */
function _suiteToggle(on) {
  _suiteState.enabled = on;
  if (on && _suiteState.scenarios.length === 0) {
    _suiteState.scenarios.push({ label: 'base' });
  }
  _suiteRender();
}

/* ── Apply built-in template ────────────────────────────────── */
function _suiteApplyTemplate(name) {
  var t = _suiteTemplates[name];
  if (!t) return;
  _suiteState.scenarios = JSON.parse(JSON.stringify(t.scenarios));
  _suiteState.aggregate = JSON.parse(JSON.stringify(t.aggregate));
  _suiteState.editIdx = -1;

  // Auto-merge scenario exchanges into base config exchanges
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
  toast('Template "' + name + '" applied', 'ok');
}

/* ── Reset to single base scenario ──────────────────────────── */
function _suiteResetToBase() {
  _suiteState.scenarios = [{ label: 'base' }];
  _suiteState.aggregate = { default: 'mean' };
  _suiteState.editIdx = -1;
  _suiteRender();
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
}

function _suiteEditScenario(idx) {
  // Save current editing scenario before switching
  if (_suiteState.editIdx >= 0 && _suiteState.editIdx !== idx) {
    _suiteSaveEditingScenario();
  }
  _suiteState.editIdx = (_suiteState.editIdx === idx) ? -1 : idx;
  _suiteRender();
}

function _suiteRemoveScenario(idx) {
  _suiteState.scenarios.splice(idx, 1);
  if (_suiteState.editIdx === idx) _suiteState.editIdx = -1;
  else if (_suiteState.editIdx > idx) _suiteState.editIdx--;
  _suiteRender();
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
  h += '\x3Cdiv class="form-group">\x3Clabel>\x3Cspan data-tip="Override start date for this scenario.\nLeave empty to use base config.">start_date\x3C/span>\x3C/label>';
  h += '\x3Cinput type="text" id="suite-sc-start" value="' + esc(sc.start_date || '') + '" placeholder="e.g. 2023-01-01">\x3C/div>';
  h += '\x3Cdiv class="form-group">\x3Clabel>\x3Cspan data-tip="Override end date for this scenario.\nLeave empty to use base config.">end_date\x3C/span>\x3C/label>';
  h += '\x3Cinput type="text" id="suite-sc-end" value="' + esc(sc.end_date || '') + '" placeholder="e.g. now">\x3C/div>';
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

  /* Coins (comma-separated text) */
  h += '\x3Cdiv class="form-row cols-2">';
  h += '\x3Cdiv class="form-group">\x3Clabel>\x3Cspan data-tip="Override approved coins for this scenario.\nComma-separated list (e.g. BTC,ETH,SOL).\nLeave empty to use base config.">coins\x3C/span>\x3C/label>';
  h += '\x3Cinput type="text" id="suite-sc-coins" value="' + esc((sc.coins || []).join(',')) + '" placeholder="BTC,ETH,SOL">\x3C/div>';
  h += '\x3Cdiv class="form-group">\x3Clabel>\x3Cspan data-tip="Override ignored coins for this scenario.\nComma-separated list.\nLeave empty to use base config.">ignored_coins\x3C/span>\x3C/label>';
  h += '\x3Cinput type="text" id="suite-sc-ign" value="' + esc((sc.ignored_coins || []).join(',')) + '" placeholder="DOGE,SHIB">\x3C/div>';
  h += '\x3C/div>';

  /* coin_sources */
  var scCs = sc.coin_sources || {};
  var csStr = _suiteCoinSourcesToStr(scCs);
  h += '\x3Cdiv class="form-group" style="margin-bottom:var(--sp-md)">';
  h += '\x3Clabel>\x3Cspan data-tip="Override coin data sources for this scenario.\nFormat: COIN:exchange (comma-separated).\nExample: BTC:binance,ETH:bybit\nLeave empty to use base config.">coin_sources\x3C/span>\x3C/label>';
  h += '\x3Cinput type="text" id="suite-sc-csrc" value="' + esc(csStr) + '" placeholder="BTC:binance,ETH:bybit">';
  h += '\x3C/div>';

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
           'style="height:24px;font-size:var(--fs-xs);width:100px" ' +
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
}

function _suiteRemoveOverride(key) {
  _suiteSaveEditingScenario();
  var sc = _suiteState.scenarios[_suiteState.editIdx];
  if (!sc || !sc.overrides) return;
  delete sc.overrides[key];
  if (Object.keys(sc.overrides).length === 0) delete sc.overrides;
  _suiteRender();
}

function _suiteUpdateOverrideVal(ki) {
  _suiteSaveEditingScenario();
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
  var coinsStr = (document.getElementById('suite-sc-coins') || {}).value || '';
  var coins = coinsStr.split(',').map(function(c) { return c.trim().toUpperCase(); }).filter(Boolean);
  if (coins.length > 0) sc.coins = coins; else delete sc.coins;

  // Ignored coins
  var ignStr = (document.getElementById('suite-sc-ign') || {}).value || '';
  var ign = ignStr.split(',').map(function(c) { return c.trim().toUpperCase(); }).filter(Boolean);
  if (ign.length > 0) sc.ignored_coins = ign; else delete sc.ignored_coins;

  // coin_sources
  var csrcStr = (document.getElementById('suite-sc-csrc') || {}).value || '';
  var csObj = _suiteStrToCoinSources(csrcStr);
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
}

function _suiteSaveAndClose() {
  _suiteSaveEditingScenario();
  _suiteState.editIdx = -1;
  _suiteRender();
}

/* ── Aggregate settings ─────────────────────────────────────── */
function _suiteRenderAggregate() {
  var agg = _suiteState.aggregate;
  var h = '\x3Cdiv class="expander" id="exp-suite-agg">';
  h += '\x3Cdiv class="expander-header" onclick="toggleExpander(\'exp-suite-agg\')">';
  h += '\x3Cspan class="arrow">\u25B6\x3C/span> Aggregate Settings';
  h += '\x3C/div>';
  h += '\x3Cdiv class="expander-body">';

  /* Default method */
  h += '\x3Cdiv class="form-row cols-4" style="margin-bottom:var(--sp-sm)">';
  h += '\x3Cdiv class="form-group">\x3Clabel>\x3Cspan data-tip="Default aggregation method for all metrics.\nmean = average across scenarios.\nmax = worst-case across scenarios.">default method\x3C/span>\x3C/label>';
  h += '\x3Cselect id="suite-agg-default" onchange="_suiteUpdateAggDefault()">';
  h += '\x3Coption value="mean"' + (agg.default === 'mean' ? ' selected' : '') + '>mean\x3C/option>';
  h += '\x3Coption value="max"' + (agg.default === 'max' ? ' selected' : '') + '>max\x3C/option>';
  h += '\x3C/select>\x3C/div>';
  h += '\x3C/div>';

  /* Per-metric overrides */
  var metricKeys = Object.keys(agg).filter(function(k) { return k !== 'default'; });
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
      h += '\x3Cselect id="suite-agg-m-' + mi + '" style="width:80px;height:24px;font-size:var(--fs-xs)" onchange="_suiteUpdateAggMetric(\'' + mk.replace(/'/g, "\\'") + '\',' + mi + ')">';
      h += '\x3Coption value="mean"' + (agg[mk] === 'mean' ? ' selected' : '') + '>mean\x3C/option>';
      h += '\x3Coption value="max"' + (agg[mk] === 'max' ? ' selected' : '') + '>max\x3C/option>';
      h += '\x3C/select>';
      h += '\x3Cbutton type="button" class="act-btn act-btn-danger" onclick="_suiteRemoveAggMetric(\'' + mk.replace(/'/g, "\\'") + '\')">\u00d7\x3C/button>';
      h += '\x3C/div>';
    }
  }

  /* Add metric row */
  h += '\x3Cdiv id="suite-add-agg-row" style="display:none;margin-top:var(--sp-xs)">';
  h += '\x3Cdiv style="display:flex;gap:var(--sp-sm);align-items:end">';
  h += '\x3Cdiv class="form-group" style="flex:1">\x3Clabel>Metric\x3C/label>\x3Cselect id="suite-agg-sel">';
  for (var ami = 0; ami < _suiteAggMetrics.length; ami++) {
    h += '\x3Coption value="' + _suiteAggMetrics[ami] + '">' + _suiteAggMetrics[ami] + '\x3C/option>';
  }
  h += '\x3C/select>\x3C/div>';
  h += '\x3Cdiv class="form-group">\x3Clabel>Method\x3C/label>\x3Cselect id="suite-agg-method">';
  h += '\x3Coption value="mean">mean\x3C/option>\x3Coption value="max" selected>max\x3C/option>';
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
}

function _suiteUpdateAggMetric(key, idx) {
  var sel = document.getElementById('suite-agg-m-' + idx);
  if (sel) _suiteState.aggregate[key] = sel.value;
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
}

function _suiteRemoveAggMetric(key) {
  delete _suiteState.aggregate[key];
  _suiteRender();
}

/* ── coin_sources helpers: {COIN: "exchange"} ↔ "COIN:exchange,..." ── */
function _suiteCoinSourcesToStr(obj) {
  var parts = [];
  var keys = Object.keys(obj).sort();
  for (var i = 0; i < keys.length; i++) {
    parts.push(keys[i] + ':' + obj[keys[i]]);
  }
  return parts.join(',');
}

function _suiteStrToCoinSources(str) {
  var result = {};
  if (!str || !str.trim()) return result;
  var parts = str.split(',');
  for (var i = 0; i < parts.length; i++) {
    var p = parts[i].trim();
    if (!p) continue;
    var ci = p.indexOf(':');
    if (ci > 0) {
      var coin = p.substring(0, ci).trim().toUpperCase();
      var ex = p.substring(ci + 1).trim().toLowerCase();
      if (coin && ex) result[coin] = ex;
    }
  }
  return result;
}
