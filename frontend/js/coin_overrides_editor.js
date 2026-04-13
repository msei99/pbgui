/**
 * Coin Overrides Editor Module — per-coin parameter overrides for passivbot.
 *
 * Usage:
 *   coinOvInit('coin-overrides-container', { apiBase: API_BASE })
 *   coinOvLoad(cfg)          // populate from config
 *   coinOvCollect()          // → { coin_overrides } or {}
 *   coinOvSetCoins(coins)    // update available coins from approved list
 *
 * Depends on parent page providing: apiFetch(), toast(), esc(), toggleExpander()
 * Design pattern: mirrors suite_editor.js — uses .tbl, .act-btn, .form-group
 */

/* ── State ──────────────────────────────────────────────────── */
var _covState = {
  overrides: {},        // { COIN: { bot: { long: {...}, short: {...} }, live: {...} } }
  editCoin: null,       // coin currently being edited (null = none)
  allowedParams: null,  // { bot: { long: {...}, short: {...} }, live: {...} } from API
  availableCoins: [],   // populated from approved coins
  containerId: '',
  apiBase: '',
  configName: '',       // current config folder name (for override_config_path lookups)
  overrideConfigs: {},  // cache: { COIN: { long: {...}, short: {...} } } loaded from override files
};

function _covNotifyStructuredSync() {
  if (typeof scheduleStructuredEditorSync === 'function') {
    scheduleStructuredEditorSync();
  }
}

/* ── Init ───────────────────────────────────────────────────── */
function coinOvInit(containerId, opts) {
  _covState.containerId = containerId;
  _covState.apiBase = (opts && opts.apiBase) || '';
  _fetchAllowedParams();
}

function _fetchAllowedParams() {
  apiFetch('/override-params').then(function(data) {
    _covState.allowedParams = data.params || {};
  }).catch(function() {
    _covState.allowedParams = {};
  });
}

/* ── Load from config ───────────────────────────────────────── */
function coinOvLoad(cfg) {
  _covState.overrides = {};
  _covState.editCoin = null;
  _covState.overrideConfigs = {};
  var co = (cfg && cfg.coin_overrides) || {};
  for (var coin in co) {
    if (!co.hasOwnProperty(coin)) continue;
    var norm = _covNormalizeCoin(coin);
    if (_covState.overrides[norm]) {
      // Duplicate after normalization — short name wins, merge missing keys only
      var existing = _covState.overrides[norm];
      var incoming = co[coin];
      // Only add keys from the long-name entry that don't already exist
      if (incoming.bot) {
        if (!existing.bot) existing.bot = {};
        ['long','short'].forEach(function(s) {
          if (incoming.bot[s] && !existing.bot[s]) existing.bot[s] = JSON.parse(JSON.stringify(incoming.bot[s]));
        });
      }
      if (incoming.live && !existing.live) existing.live = JSON.parse(JSON.stringify(incoming.live));
    } else {
      var data = JSON.parse(JSON.stringify(co[coin]));
      // Normalize override_config_path to match the normalized coin name
      if (data.override_config_path) {
        data.override_config_path = norm + '.json';
      }
      _covState.overrides[norm] = data;
    }
  }
  _covRender();
}

/* ── Coin name normalizer ───────────────────────────────────── */
/** Normalize exchange symbol to short coin name (e.g. HYPEUSDT → HYPE, 1000BONKUSDT → BONK). */
function _covNormalizeCoin(symbol) {
  if (!symbol) return symbol;
  var s = symbol.toUpperCase();
  // Strip quote suffixes
  var quotes = ['USDT', 'USDC', 'BUSD', 'USD'];
  for (var i = 0; i < quotes.length; i++) {
    if (s.length > quotes[i].length && s.endsWith(quotes[i])) {
      s = s.slice(0, -quotes[i].length);
      break;
    }
  }
  // Strip powers-of-ten prefix (1000, 100, 10)
  var m = s.match(/^(10+)([A-Z].*)/);
  if (m) s = m[2];
  // Strip Hyperliquid k-prefix (kSHIB → SHIB)
  if (s.length > 1 && s[0] === 'K' && s[1] !== 'K') {
    var tail = s.slice(1);
    // Only strip if the tail looks like a pure uppercase coin name
    if (/^[A-Z]+$/.test(tail)) s = tail;
  }
  return s;
}

/* ── Update available coins ─────────────────────────────────── */
function coinOvSetCoins(coins) {
  _covState.availableCoins = (coins || []).filter(function(c) { return c !== 'all'; }).sort();
  _covRender();
}

/** Set the current config folder name (needed for loading override_config_path files) */
function coinOvSetConfigName(name) {
  _covState.configName = name || '';
}

/** Load an override config file for a coin. Returns a promise resolving to the raw file dict.
 *  Auto-filters to allowed override params on load. */
function _covLoadOverrideFile(coin) {
  var data = _covState.overrides[coin];
  if (!data || !data.override_config_path || !_covState.configName) {
    return Promise.resolve(null);
  }
  // Check cache
  if (_covState.overrideConfigs[coin] !== undefined) return Promise.resolve(_covState.overrideConfigs[coin]);
  var filename = data.override_config_path;
  return apiFetch('/override-config/' + encodeURIComponent(_covState.configName) + '/' + encodeURIComponent(filename))
    .then(function(resp) {
      var cfg = resp.config || {};
      cfg = _covFilterOverrideConfig(cfg);
      _covState.overrideConfigs[coin] = cfg;
      return cfg;
    })
    .catch(function() {
      _covState.overrideConfigs[coin] = null;
      return null;
    });
}

/** Filter an override config dict to only allowed bot.long/bot.short params. */
function _covFilterOverrideConfig(cfg) {
  var allowed = _covState.allowedParams;
  if (!cfg || !allowed || !allowed.bot) return cfg;
  var result = {};
  var sides = ['long', 'short'];
  for (var i = 0; i < sides.length; i++) {
    var side = sides[i];
    var src = (cfg.bot && cfg.bot[side]) ? cfg.bot[side] : null;
    if (!src) continue;
    /* unwrap: if a side contains a full passivbot config (has bot.long/bot.short)
       extract the actual side params from within */
    if (src.bot && typeof src.bot === 'object' && src.bot[side]) {
      src = src.bot[side];
    }
    var keys = (allowed.bot[side]) ? allowed.bot[side] : {};
    var filtered = {};
    for (var k in src) {
      if (src.hasOwnProperty(k) && keys.hasOwnProperty(k)) {
        filtered[k] = src[k];
      }
    }
    if (Object.keys(filtered).length > 0) {
      if (!result.bot) result.bot = {};
      result.bot[side] = filtered;
    }
  }
  return result;
}

/* ── Collect → config ───────────────────────────────────────── */
function coinOvCollect() {
  if (_covState.editCoin) _covSaveEdit();
  var ov = _covState.overrides;
  if (Object.keys(ov).length === 0) return {};
  return { coin_overrides: JSON.parse(JSON.stringify(ov)) };
}

/* ── Render ──────────────────────────────────────────────────── */
function _covRender() {
  var el = document.getElementById(_covState.containerId);
  if (!el) return;
  var ov = _covState.overrides;
  var count = Object.keys(ov).length;
  var isOpen = count > 0 ? ' open' : '';

  var h = '\x3Cdiv class="expander' + isOpen + '" id="exp-coin-ov">';
  h += '\x3Cdiv class="expander-header" onclick="toggleExpander(\'exp-coin-ov\')">';
  h += '\x3Cspan class="arrow">\u25B6\x3C/span> Coin Overrides';
  if (count > 0) {
    h += ' \x3Cspan style="color:var(--text-dim);font-size:var(--fs-xs);margin-left:6px">(' +
         count + ' coin' + (count > 1 ? 's' : '') + ')\x3C/span>';
  }
  h += '\x3C/div>';
  h += '\x3Cdiv class="expander-body">';

  /* Summary table */
  if (count > 0) {
    h += '\x3Ctable class="tbl" style="font-size:var(--fs-sm)">';
    h += '\x3Cthead>\x3Ctr>\x3Cth>Coin\x3C/th>\x3Cth>Overrides\x3C/th>\x3Cth style="width:100px">Actions\x3C/th>\x3C/tr>\x3C/thead>';
    h += '\x3Ctbody>';
    var coins = Object.keys(ov).sort();
    for (var i = 0; i < coins.length; i++) {
      var c = coins[i];
      var isEditing = (_covState.editCoin === c);
      var badge = _covBadge(ov[c]);
      var tooltipHtml = _covTooltipHtml(ov[c]);
      h += '\x3Ctr' + (isEditing ? ' style="background:rgba(77,166,255,.06)"' : '') + '>';
      h += '\x3Ctd style="font-weight:600">' + esc(c) + '\x3C/td>';
      h += '\x3Ctd>\x3Cspan class="cov-badge" data-tooltip="' + tooltipHtml.replace(/"/g, '&quot;') + '">' + badge + '\x3C/span>\x3C/td>';
      h += '\x3Ctd>';
      h += '\x3Cbutton type="button" class="act-btn" onclick="coinOvEdit(\'' + esc(c) + '\')">' +
           (isEditing ? 'Editing' : 'Edit') + '\x3C/button> ';
      h += '\x3Cbutton type="button" class="act-btn act-btn-danger" onclick="coinOvRemove(\'' + esc(c) + '\')">\u00d7\x3C/button>';
      h += '\x3C/td>\x3C/tr>';
    }
    h += '\x3C/tbody>\x3C/table>';
  }

  /* Add coin — searchable dropdown (same as approved_coins) */
  h += '\x3Cdiv class="form-group" style="margin-top:var(--sp-sm);max-width:300px">';
  h += '\x3Clabel>\x3Cspan data-tip="Select a coin to add per-coin overrides.\nType to search the list.">Add coin\x3C/span>\x3C/label>';
  h += '\x3Cdiv class="ms-wrap" id="cov-coin-picker">';
  h += '\x3Cinput class="ms-input" id="cov-coin-input" placeholder="Type to search\u2026">';
  h += '\x3Cdiv class="ms-dropdown" id="cov-coin-dd">\x3C/div>';
  h += '\x3C/div>\x3C/div>';

  /* Edit area */
  if (_covState.editCoin) {
    h += _covEditHtml(_covState.editCoin);
  }

  h += '\x3C/div>\x3C/div>';
  el.innerHTML = h;

  /* Wire up coin picker search dropdown */
  _covWireCoinPicker();
  /* Wire up param picker search dropdowns */
  _covWireParamPickers();
  /* Auto-resize config file textareas if visible */
  ['cov-cfg-long','cov-cfg-short'].forEach(function(id) {
    var ta = document.getElementById(id);
    if (ta && ta.offsetParent !== null) covAutoResizeCfgTa(ta);
  });
}

/* ── Coin picker: searchable dropdown ────────────────────────── */
function _covWireCoinPicker() {
  var input = document.getElementById('cov-coin-input');
  var dd = document.getElementById('cov-coin-dd');
  if (!input || !dd) return;
  var hiIdx = -1;

  input.addEventListener('focus', function() { _covShowCoinDd(''); });
  input.addEventListener('input', function() { hiIdx = -1; _covShowCoinDd(this.value); });
  input.addEventListener('blur', function() {
    setTimeout(function() { dd.classList.remove('open'); hiIdx = -1; }, 150);
  });
  input.addEventListener('keydown', function(e) {
    if (!dd.classList.contains('open')) return;
    var items = dd.querySelectorAll('.ms-option');
    if (e.key === 'ArrowDown' || e.key === 'ArrowUp') {
      e.preventDefault();
      if (!items.length) return;
      if (e.key === 'ArrowDown') hiIdx = hiIdx < items.length - 1 ? hiIdx + 1 : 0;
      else hiIdx = hiIdx > 0 ? hiIdx - 1 : items.length - 1;
      items.forEach(function(el, i) {
        if (i === hiIdx) { el.classList.add('highlighted'); el.scrollIntoView({block:'nearest'}); }
        else el.classList.remove('highlighted');
      });
    } else if (e.key === 'Enter') {
      e.preventDefault();
      var val = null;
      if (items.length === 1) val = items[0].getAttribute('data-val');
      else if (hiIdx >= 0 && hiIdx < items.length) val = items[hiIdx].getAttribute('data-val');
      if (val) _covPickCoin(val);
    }
  });
}

function _covShowCoinDd(filter) {
  var dd = document.getElementById('cov-coin-dd');
  if (!dd) return;
  var ov = _covState.overrides;
  var f = (filter || '').toUpperCase();
  var html = '';
  var avail = _covState.availableCoins;
  // Also allow coins not in available list (typed custom coins already added don't repeat)
  var shown = 0;
  for (var i = 0; i < avail.length; i++) {
    var c = avail[i];
    if (ov[c]) continue; // already has overrides
    if (f && c.toUpperCase().indexOf(f) < 0) continue;
    html += '\x3Cdiv class="ms-option" data-val="' + esc(c) + '">' + esc(c) + '\x3C/div>';
    shown++;
    if (shown > 200) break;
  }
  if (shown === 0 && f) {
    // Allow adding custom coin if typed and not already present
    var custom = f.toUpperCase();
    if (!ov[custom]) {
      html += '\x3Cdiv class="ms-option" data-val="' + esc(custom) + '">' + esc(custom) + ' (custom)\x3C/div>';
    }
  }
  dd.innerHTML = html;
  dd.classList.add('open');
  dd.querySelectorAll('.ms-option').forEach(function(el) {
    el.addEventListener('mousedown', function(e) {
      e.preventDefault();
      _covPickCoin(this.getAttribute('data-val'));
    });
  });
}

function _covPickCoin(coin) {
  if (!coin) return;
  if (_covState.overrides[coin]) { toast(coin + ' already has overrides', 'err'); return; }
  _covState.overrides[coin] = {};
  _covState.editCoin = coin;
  _covRender();
  _covNotifyStructuredSync();
}

/* ── Param picker: searchable dropdowns per section ──────────── */
var _covParamPickerState = {}; // secId → { selected: '' }

function _covWireParamPickers() {
  var secIds = ['bot-long', 'bot-short', 'live'];
  for (var i = 0; i < secIds.length; i++) {
    _covWireOneParamPicker(secIds[i]);
  }
}

function _covWireOneParamPicker(secId) {
  var input = document.getElementById('cov-ps-' + secId + '-input');
  var dd = document.getElementById('cov-ps-' + secId + '-dd');
  if (!input || !dd) return;
  _covParamPickerState[secId] = { selected: '', hiIdx: -1 };

  input.addEventListener('focus', function() { _covShowParamDd(secId, ''); });
  input.addEventListener('input', function() {
    _covParamPickerState[secId].hiIdx = -1;
    _covParamPickerState[secId].selected = '';
    _covShowParamDd(secId, this.value);
  });
  input.addEventListener('blur', function() {
    setTimeout(function() { dd.classList.remove('open'); }, 150);
  });
  input.addEventListener('keydown', function(e) {
    if (!dd.classList.contains('open')) return;
    var items = dd.querySelectorAll('.ms-option');
    var st = _covParamPickerState[secId];
    if (e.key === 'ArrowDown' || e.key === 'ArrowUp') {
      e.preventDefault();
      if (!items.length) return;
      if (e.key === 'ArrowDown') st.hiIdx = st.hiIdx < items.length - 1 ? st.hiIdx + 1 : 0;
      else st.hiIdx = st.hiIdx > 0 ? st.hiIdx - 1 : items.length - 1;
      items.forEach(function(el, i) {
        if (i === st.hiIdx) { el.classList.add('highlighted'); el.scrollIntoView({block:'nearest'}); }
        else el.classList.remove('highlighted');
      });
    } else if (e.key === 'Enter') {
      e.preventDefault();
      var val = null;
      if (items.length === 1) val = items[0].getAttribute('data-val');
      else if (st.hiIdx >= 0 && st.hiIdx < items.length) val = items[st.hiIdx].getAttribute('data-val');
      if (val) _covSelectParam(secId, val);
    }
  });
}

function _covShowParamDd(secId, filter) {
  var dd = document.getElementById('cov-ps-' + secId + '-dd');
  if (!dd) return;
  var coin = _covState.editCoin;
  if (!coin) return;
  var allowed = _covState.allowedParams || {};
  // Map secId back to path
  var akey = secId === 'live' ? ['live'] : ['bot', secId.replace('bot-', '')];
  var secAllowed = _covGetNested(allowed, akey) || {};
  var data = _covState.overrides[coin] || {};
  var secData = _covGetNested(data, akey) || {};
  var f = (filter || '').toUpperCase();
  var html = '';
  var paramKeys = Object.keys(secAllowed).filter(function(k) { return secAllowed[k] === true; }).sort();
  var st = _covParamPickerState[secId] || { hiIdx: -1 };
  var visIdx = 0;
  for (var i = 0; i < paramKeys.length; i++) {
    var pk = paramKeys[i];
    if (secData.hasOwnProperty(pk)) continue; // already added
    if (f && pk.toUpperCase().indexOf(f) < 0) continue;
    var isHi = (visIdx === st.hiIdx);
    var isSel = (st.selected === pk);
    var cls = 'ms-option' + (isHi ? ' highlighted' : '') + (isSel ? ' selected' : '');
    html += '\x3Cdiv class="' + cls + '" data-val="' + esc(pk) + '">' + esc(pk) + '\x3C/div>';
    visIdx++;
  }
  dd.innerHTML = html;
  dd.classList.add('open');
  dd.querySelectorAll('.ms-option').forEach(function(el) {
    el.addEventListener('mousedown', function(e) {
      e.preventDefault();
      _covSelectParam(secId, this.getAttribute('data-val'));
    });
  });
}

function _covSelectParam(secId, param) {
  if (!param) return;
  var input = document.getElementById('cov-ps-' + secId + '-input');
  if (input) { input.value = param; }
  _covParamPickerState[secId] = { selected: param, hiIdx: -1 };
  var dd = document.getElementById('cov-ps-' + secId + '-dd');
  if (dd) dd.classList.remove('open');
  // Focus the value input
  var valInput = document.getElementById('cov-pv-' + secId);
  if (valInput) valInput.focus();
}

/* ── Describe overrides for summary ──────────────────────────── */
function _covDescribe(data) {
  var parts = [];
  if (data.bot) {
    if (data.bot.long) {
      var lk = Object.keys(data.bot.long);
      if (lk.length) parts.push('long: ' + lk.join(', '));
    }
    if (data.bot.short) {
      var sk = Object.keys(data.bot.short);
      if (sk.length) parts.push('short: ' + sk.join(', '));
    }
  }
  if (data.live) {
    var lvk = Object.keys(data.live);
    if (lvk.length) parts.push('live: ' + lvk.join(', '));
  }
  if (data.override_config_path) {
    parts.push('file: ' + data.override_config_path);
  }
  return parts.join(' | ') || '(empty)';
}

/* Returns a compact badge string: "long: 12" or "long: 5 · short: 3" etc. */
function _covBadge(data) {
  var parts = [];
  if (data.bot) {
    if (data.bot.long && Object.keys(data.bot.long).length)
      parts.push('long\u00a0' + Object.keys(data.bot.long).length);
    if (data.bot.short && Object.keys(data.bot.short).length)
      parts.push('short\u00a0' + Object.keys(data.bot.short).length);
  }
  if (data.live && Object.keys(data.live).length)
    parts.push('live\u00a0' + Object.keys(data.live).length);
  if (data.override_config_path) parts.push('file');
  if (!parts.length) return '(empty)';
  return parts.join('\u2002·\u2002');
}

/* Returns an HTML string for the tooltip table (param | value rows per section). */
function _covTooltipHtml(data) {
  var rows = [];
  function addSection(label, obj) {
    if (!obj || !Object.keys(obj).length) return;
    var keys = Object.keys(obj).sort();
    rows.push('<tr class="cov-tt-hdr"><td colspan="2">' + label + '</td></tr>');
    for (var i = 0; i < keys.length; i++) {
      var v = obj[keys[i]];
      var vs = (typeof v === 'object' && v !== null) ? JSON.stringify(v) : String(v);
      rows.push('<tr><td>' + escHtml(keys[i]) + '</td><td>' + escHtml(vs) + '</td></tr>');
    }
  }
  if (data.bot) {
    addSection('long', data.bot.long);
    addSection('short', data.bot.short);
  }
  addSection('live', data.live);
  if (data.override_config_path) {
    rows.push('<tr class="cov-tt-hdr"><td colspan="2">file</td></tr>');
    rows.push('<tr><td colspan="2">' + escHtml(data.override_config_path) + '</td></tr>');
  }
  if (!rows.length) return '(empty)';
  return '<table class="cov-tt-tbl">' + rows.join('') + '</table>';
}

function escHtml(s) {
  return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}

/* ── Remove coin ─────────────────────────────────────────────── */
function coinOvRemove(coin) {
  delete _covState.overrides[coin];
  if (_covState.editCoin === coin) _covState.editCoin = null;
  _covRender();
  _covNotifyStructuredSync();
}

/* ── Edit coin ───────────────────────────────────────────────── */
function coinOvEdit(coin) {
  if (_covState.editCoin && _covState.editCoin !== coin) _covSaveEdit();
  if (_covState.editCoin === coin) {
    _covState.editCoin = null;
    _covRender();
    return;
  }
  _covState.editCoin = coin;
  // If coin has override_config_path, load the file before rendering
  var data = _covState.overrides[coin] || {};
  if (data.override_config_path && _covState.configName && !_covState.overrideConfigs[coin]) {
    _covLoadOverrideFile(coin).then(function() { _covRender(); });
  } else {
    _covRender();
  }
}

/* ── Build edit HTML ─────────────────────────────────────────── */
function _covEditHtml(coin) {
  var data = _covState.overrides[coin] || {};
  var allowed = _covState.allowedParams || {};

  var h = '\x3Cdiv style="border:1px solid var(--accent);border-radius:6px;padding:var(--sp-md);margin-top:var(--sp-sm);background:rgba(77,166,255,.03)">';
  h += '\x3Cdiv style="display:flex;justify-content:space-between;align-items:center;margin-bottom:var(--sp-sm)">';
  h += '\x3Cspan style="font-size:var(--fs-sm);font-weight:600;color:var(--accent)">Edit: ' + esc(coin) + '\x3C/span>';
  h += '\x3Cbutton type="button" class="act-btn" onclick="coinOvCloseEdit()">Done\x3C/button>';
  h += '\x3C/div>';

  /* Bot Long / Bot Short / Live sections — inline parameter overrides */
  var sections = [
    { key: 'bot.long', label: 'Bot Long', color: 'var(--green)', akey: ['bot','long'] },
    { key: 'bot.short', label: 'Bot Short', color: 'var(--red)', akey: ['bot','short'] },
    { key: 'live', label: 'Live', color: 'var(--blue)', akey: ['live'] },
  ];

  for (var s = 0; s < sections.length; s++) {
    var sec = sections[s];
    var secData = _covGetNested(data, sec.akey) || {};
    var secAllowed = _covGetNested(allowed, sec.akey) || {};
    var paramKeys = Object.keys(secAllowed).filter(function(k) { return secAllowed[k] === true; }).sort();

    h += '\x3Cdiv style="margin-bottom:var(--sp-sm);padding-bottom:var(--sp-sm);border-bottom:1px solid var(--border)">';
    h += '\x3Cdiv style="display:flex;align-items:center;justify-content:space-between;margin-bottom:var(--sp-xs)">';
    h += '\x3Cspan style="font-weight:600;font-size:var(--fs-sm);color:' + sec.color + '">' + sec.label + '\x3C/span>';
    h += '\x3C/div>';

    /* Existing overrides as table */
    var existingKeys = Object.keys(secData).sort();
    if (existingKeys.length > 0) {
      h += '\x3Ctable class="tbl" style="font-size:var(--fs-xs);margin-bottom:var(--sp-xs)">';
      h += '\x3Cthead>\x3Ctr>\x3Cth>Parameter\x3C/th>\x3Cth>Value\x3C/th>\x3Cth style="width:40px">\x3C/th>\x3C/tr>\x3C/thead>';
      h += '\x3Ctbody>';
      for (var e = 0; e < existingKeys.length; e++) {
        var pk = existingKeys[e];
        var pv = secData[pk];
        var inputId = 'cov-' + sec.key.replace('.', '-') + '-' + pk;
        h += '\x3Ctr>';
        h += '\x3Ctd>' + esc(pk) + '\x3C/td>';
        h += '\x3Ctd>' + _covInputHtml(inputId, pk, pv) + '\x3C/td>';
        h += '\x3Ctd>\x3Cbutton type="button" class="act-btn act-btn-danger" onclick="covRemoveParam(\'' +
             esc(coin) + '\',\'' + sec.key + '\',\'' + esc(pk) + '\')">\u00d7\x3C/button>\x3C/td>';
        h += '\x3C/tr>';
      }
      h += '\x3C/tbody>\x3C/table>';
    }

    /* Add parameter row — searchable dropdown + value input */
    var unusedParams = paramKeys.filter(function(k) { return !secData.hasOwnProperty(k); });
    if (unusedParams.length > 0) {
      var secId = sec.key.replace('.', '-');
      h += '\x3Cdiv class="form-row cols-4" style="align-items:end;margin-bottom:0">';
      h += '\x3Cdiv class="form-group" style="grid-column:span 2">\x3Clabel>Parameter\x3C/label>';
      h += '\x3Cdiv class="ms-wrap" id="cov-ps-' + secId + '">';
      h += '\x3Cinput class="ms-input" id="cov-ps-' + secId + '-input" placeholder="Type to search\u2026">';
      h += '\x3Cdiv class="ms-dropdown" id="cov-ps-' + secId + '-dd">\x3C/div>';
      h += '\x3C/div>\x3C/div>';
      h += '\x3Cdiv class="form-group">\x3Clabel>Value\x3C/label>';
      h += '\x3Cinput type="text" id="cov-pv-' + secId + '" placeholder="0.5">\x3C/div>';
      h += '\x3Cdiv class="form-group">';
      h += '\x3Cbutton type="button" class="act-btn" onclick="covAddParam(\'' + esc(coin) + '\',\'' + sec.key + '\')" ' +
           'style="height:var(--input-h)">Add\x3C/button>';
      h += '\x3C/div>\x3C/div>';
    }

    h += '\x3C/div>';
  }

  /* Config File — long/short JSON textareas that map directly to COIN.json on disk */
  /* Shows ONLY the file content (override_config_path), NOT merged with base config. */
  /* Passivbot merge order: base config → file overrides → inline overrides (inline wins). */
  var ovFile = _covState.overrideConfigs[coin];
  var fileLong = (ovFile && ovFile.bot && ovFile.bot.long) ? ovFile.bot.long : {};
  var fileShort = (ovFile && ovFile.bot && ovFile.bot.short) ? ovFile.bot.short : {};
  var fileLongJson = JSON.stringify(fileLong, null, 4);
  var fileShortJson = JSON.stringify(fileShort, null, 4);
  var hasFile = ovFile && (Object.keys(fileLong).length > 0 || Object.keys(fileShort).length > 0);

  h += '\x3Cdiv style="margin-top:var(--sp-xs)">';
  h += '\x3Cdiv style="display:flex;align-items:center;gap:var(--sp-sm);margin-bottom:var(--sp-xs);cursor:pointer" onclick="covToggleConfig()">';
  h += '\x3Cspan id="cov-cfg-arrow" style="font-size:var(--fs-xs);color:var(--text-dim);transition:transform .15s;display:inline-block' +
       (hasFile ? ';transform:rotate(90deg)' : '') + '">\u25B6\x3C/span>';
  h += '\x3Cspan style="font-size:var(--fs-sm);font-weight:600;color:var(--text-dim)">Config File\x3C/span>';
  h += '\x3Cspan style="font-size:var(--fs-xs);color:var(--text-dim)">(' + esc(coin) + '.json \u2014 bot.long/bot.short overrides as JSON file)\x3C/span>';
  h += '\x3C/div>';
  h += '\x3Cdiv id="cov-cfg-area" style="display:' + (hasFile ? 'block' : 'none') + '">';
  h += '\x3Cdiv style="display:grid;grid-template-columns:1fr 1fr;gap:var(--sp-sm)">';
  /* Long textarea */
  h += '\x3Cdiv class="form-group">';
  h += '\x3Clabel style="color:var(--green)">long\x3C/label>';
  h += '\x3Ctextarea id="cov-cfg-long" rows="12" ' +
       'style="font-family:monospace;font-size:var(--fs-xs);resize:vertical;overflow:hidden" ' +
       'oninput="covAutoResizeCfgTa(this)" onpaste="covFilterCfgPaste(event,\'long\')">' + esc(fileLongJson) + '\x3C/textarea>';
  h += '\x3C/div>';
  /* Short textarea */
  h += '\x3Cdiv class="form-group">';
  h += '\x3Clabel style="color:var(--red)">short\x3C/label>';
  h += '\x3Ctextarea id="cov-cfg-short" rows="12" ' +
       'style="font-family:monospace;font-size:var(--fs-xs);resize:vertical;overflow:hidden" ' +
       'oninput="covAutoResizeCfgTa(this)" onpaste="covFilterCfgPaste(event,\'short\')">' + esc(fileShortJson) + '\x3C/textarea>';
  h += '\x3C/div>';
  h += '\x3C/div>';
  h += '\x3Cspan style="font-size:var(--fs-xs);color:var(--text-dim);margin-top:2px;display:block">' +
       'Saved as ' + esc(coin) + '.json alongside backtest.json. Passivbot applies these after the base config. ' +
       'Inline overrides above take precedence over file values.\x3C/span>';
  h += '\x3C/div>';
  h += '\x3C/div>';

  h += '\x3C/div>';
  return h;
}

/* ── Input HTML for a parameter ──────────────────────────────── */
var _covInputStyle = 'height:24px;font-size:var(--fs-xs);background:var(--bg2);color:var(--text);border:1px solid var(--border);border-radius:4px;padding:0 var(--sp-xs);outline:none;font-family:var(--font)';

function _covInputHtml(id, key, value) {
  if (key === 'forced_mode_long' || key === 'forced_mode_short') {
    var modes = ['normal', 'graceful_stop', 'manual', 'panic', 'tp_only'];
    var h = '\x3Cselect id="' + id + '" style="' + _covInputStyle + ';width:140px">';
    for (var m = 0; m < modes.length; m++) {
      h += '\x3Coption value="' + modes[m] + '"' + (value === modes[m] ? ' selected' : '') + '>' + modes[m] + '\x3C/option>';
    }
    h += '\x3C/select>';
    return h;
  }
  var v = (value !== undefined && value !== null) ? value : '';
  return '\x3Cinput type="text" id="' + id + '" value="' + v + '" ' +
         'style="' + _covInputStyle + ';width:100px">';
}

/* ── Add/remove parameter actions ────────────────────────────── */
function covAddParam(coin, secKey) {
  _covSaveEdit();
  var secId = secKey.replace('.', '-');
  var st = _covParamPickerState[secId];
  var param = (st && st.selected) ? st.selected : '';
  // Fallback: read directly from the input field
  if (!param) {
    var inp = document.getElementById('cov-ps-' + secId + '-input');
    if (inp) param = inp.value.trim();
  }
  if (!param) { toast('Select a parameter first', 'err'); return; }
  var valInput = document.getElementById('cov-pv-' + secId);
  var rawVal = valInput ? valInput.value.trim() : '';
  var data = _covState.overrides[coin];
  if (!data) return;
  var parts = secKey.split('.');
  _covEnsureNested(data, parts);
  var target = _covGetNested(data, parts);
  if (param === 'forced_mode_long' || param === 'forced_mode_short') {
    target[param] = rawVal || 'normal';
  } else if (rawVal !== '') {
    var num = parseFloat(rawVal);
    target[param] = isNaN(num) ? rawVal : num;
  } else if (param === 'leverage') {
    target[param] = 7;
  } else {
    target[param] = 0;
  }
  _covRender();
  _covNotifyStructuredSync();
}

function covRemoveParam(coin, secKey, param) {
  _covSaveEdit();
  var data = _covState.overrides[coin];
  if (!data) return;
  var parts = secKey.split('.');
  var target = _covGetNested(data, parts);
  if (target) {
    delete target[param];
    // Clean up empty nested objects
    _covCleanEmpty(data);
  }
  _covRender();
  _covNotifyStructuredSync();
}

/* ── Save edit form values back to state ─────────────────────── */
function _covSaveEdit() {
  var coin = _covState.editCoin;
  if (!coin) return;
  var data = _covState.overrides[coin];
  if (!data) return;

  // Read all section values from inline inputs
  var sections = [
    { key: 'bot.long', akey: ['bot','long'] },
    { key: 'bot.short', akey: ['bot','short'] },
    { key: 'live', akey: ['live'] },
  ];
  for (var s = 0; s < sections.length; s++) {
    var sec = sections[s];
    var target = _covGetNested(data, sec.akey);
    if (!target) continue;
    var keys = Object.keys(target);
    for (var k = 0; k < keys.length; k++) {
      var param = keys[k];
      var inputId = 'cov-' + sec.key.replace('.', '-') + '-' + param;
      var el = document.getElementById(inputId);
      if (!el) continue;
      if (param === 'forced_mode_long' || param === 'forced_mode_short') {
        target[param] = el.value;
      } else {
        var v = parseFloat(el.value);
        if (!isNaN(v)) target[param] = v;
      }
    }
  }

  // Config File textareas → save to COIN.json via API (NOT merged into inline overrides)
  _covSaveConfigFile(coin);

  _covCleanEmpty(data);
  _covNotifyStructuredSync();
}

/** Save the Config File textareas to COIN.json via the API. */
function _covSaveConfigFile(coin) {
  var cfgArea = document.getElementById('cov-cfg-area');
  if (!cfgArea) return;
  var data = _covState.overrides[coin];
  if (!data) return;

  var fileContent = {};
  var hasContent = false;
  var sides = ['long', 'short'];
  for (var i = 0; i < sides.length; i++) {
    var side = sides[i];
    var ta = document.getElementById('cov-cfg-' + side);
    if (!ta) continue;
    var raw = ta.value.trim();
    if (!raw || raw === '{}') continue;
    var parsed;
    try { parsed = JSON.parse(raw); }
    catch (e) { toast('Invalid JSON in ' + side + ': ' + e.message, 'err'); return; }
    if (typeof parsed === 'object' && parsed !== null && Object.keys(parsed).length > 0) {
      if (!fileContent.bot) fileContent.bot = {};
      fileContent.bot[side] = parsed;
      hasContent = true;
    }
  }

  var filename = coin + '.json';
  if (hasContent) {
    // Set override_config_path and save file
    data.override_config_path = filename;
    // Update cache
    _covState.overrideConfigs[coin] = fileContent;
    // Save via API if config name is known
    if (_covState.configName) {
      apiFetch('/override-config/' + encodeURIComponent(_covState.configName) + '/' + encodeURIComponent(filename), {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(fileContent)
      }).catch(function(e) { toast('Save ' + filename + ' failed: ' + e.message, 'err'); });
    }
  } else {
    // No file content → remove override_config_path
    delete data.override_config_path;
    delete _covState.overrideConfigs[coin];
  }
}

/* ── Config JSON toggle + auto-resize + save from textareas ── */
function covToggleConfig() {
  var area = document.getElementById('cov-cfg-area');
  var arrow = document.getElementById('cov-cfg-arrow');
  if (!area) return;
  var open = area.style.display !== 'none';
  area.style.display = open ? 'none' : 'block';
  if (arrow) arrow.style.transform = open ? '' : 'rotate(90deg)';
  if (!open) {
    // Auto-resize textareas on open
    ['cov-cfg-long','cov-cfg-short'].forEach(function(id) {
      var ta = document.getElementById(id);
      if (ta) covAutoResizeCfgTa(ta);
    });
  }
}

function covAutoResizeCfgTa(el) {
  el.style.height = 'auto';
  el.style.height = Math.max(el.scrollHeight, 100) + 'px';
}

/** On paste into Config File textarea: auto-filter to allowed override params.
 *  Detects config structure and extracts the relevant side:
 *    { bot: { long: {...}, short: {...} } }  → extract bot[side]
 *    { long: {...}, short: {...} }           → extract [side]
 *    { close_grid_markup_end: ... }          → flat override params (use as-is)
 *  Then filters to only allowed override parameters. */
function covFilterCfgPaste(evt, side) {
  var clip = (evt.clipboardData || window.clipboardData);
  if (!clip) return;
  var text = clip.getData('text');
  if (!text || text.trim().length < 2) return;
  var trimmed = text.trim();
  if (trimmed[0] !== '{') return;
  var parsed;
  try { parsed = JSON.parse(trimmed); } catch (_) {
    var fixed = trimmed
      .replace(/,(\s*[}\]])/g, '$1')
      .replace(/:\s*(\d+),(\d+)/g, ':$1.$2');
    try { parsed = JSON.parse(fixed); } catch (_2) { return; }
  }
  if (typeof parsed !== 'object' || parsed === null || Array.isArray(parsed)) return;
  var allowed = _covState.allowedParams;
  var keys = (allowed && allowed.bot && allowed.bot[side]) ? allowed.bot[side] : null;
  if (!keys) return;
  /* Detect structured configs and extract the relevant side */
  var flat = parsed;
  if (parsed.bot && typeof parsed.bot === 'object' && parsed.bot[side] && typeof parsed.bot[side] === 'object') {
    flat = parsed.bot[side]; /* { bot: { long: {...}, short: {...} } } */
  } else if (parsed[side] && typeof parsed[side] === 'object' && !Array.isArray(parsed[side])) {
    flat = parsed[side]; /* { long: {...}, short: {...} } */
  }
  /* Only filter if the paste contains non-override keys */
  var hasNonOverride = false;
  for (var k in flat) {
    if (flat.hasOwnProperty(k) && !keys.hasOwnProperty(k)) { hasNonOverride = true; break; }
  }
  if (!hasNonOverride && flat === parsed) return; /* already clean flat — let default paste */
  var filtered = {};
  var removed = [];
  for (var k in flat) {
    if (!flat.hasOwnProperty(k)) continue;
    if (keys.hasOwnProperty(k)) {
      filtered[k] = flat[k];
    } else {
      removed.push(k);
    }
  }
  evt.preventDefault();
  var ta = evt.target;
  ta.value = JSON.stringify(filtered, null, 4);
  covAutoResizeCfgTa(ta);
  _covNotifyStructuredSync();
  var msg = flat !== parsed ? 'Extracted ' + side + ' side' : '';
  if (removed.length > 0) {
    msg += (msg ? ', filtered ' : 'Filtered ') + removed.length + ' non-override param(s)';
  }
  if (msg) toast(msg, 'info');
}

/* ── Close edit ──────────────────────────────────────────────── */
function coinOvCloseEdit() {
  _covSaveEdit();
  _covState.editCoin = null;
  _covRender();
}

/* ── Helpers ─────────────────────────────────────────────────── */

function _covGetNested(obj, path) {
  var cur = obj;
  for (var i = 0; i < path.length; i++) {
    if (!cur || typeof cur !== 'object') return undefined;
    cur = cur[path[i]];
  }
  return cur;
}

function _covEnsureNested(obj, path) {
  var cur = obj;
  for (var i = 0; i < path.length; i++) {
    if (!cur[path[i]] || typeof cur[path[i]] !== 'object') {
      cur[path[i]] = {};
    }
    cur = cur[path[i]];
  }
  return cur;
}

function _covCleanEmpty(data) {
  // Remove empty nested objects
  if (data.bot) {
    if (data.bot.long && Object.keys(data.bot.long).length === 0) delete data.bot.long;
    if (data.bot.short && Object.keys(data.bot.short).length === 0) delete data.bot.short;
    if (Object.keys(data.bot).length === 0) delete data.bot;
  }
  if (data.live && Object.keys(data.live).length === 0) delete data.live;
}
