/* Storage page: disk usage, cleanup categories, retention, history and system. */
(function () {
  'use strict';

  var API = window.API_BASE || '';
  var GROUPS = { logs: 'Logs', results: 'Jobs & Results', caches: 'Caches & Market Data', backups: 'Backups & Leftovers' };
  var TITLES = { overview: 'Overview', retention: 'Retention', history: 'History', system: 'System' };
  var TIER_LABELS = { safe: 'Safe', optional: 'Optional', review: 'Review', protected: 'Protected' };
  var TIER_TIPS = {
    safe: 'No useful data is lost. Can be cleaned automatically.',
    optional: 'Recreated when needed or of low value. Not cleaned unless you choose it.',
    review: 'Your own results and leftovers. Select the entries to delete.',
    protected: 'Shown for information only. Never deleted here.'
  };
  var COLORS = ['#63b3ed', '#a78bfa', '#f472b6', '#34d399', '#fbbf24', '#fb923c'];
  var FILTER_KEY = 'pbgui.storage.filters';
  var SELECTION_KEY = 'pbgui.storage.selection';
  var EXPANDED_KEY = 'pbgui.storage.expanded';
  var MAX_STORED_SELECTION = 5000;

  var state = {
    view: 'overview',
    category: '',
    status: null,
    snapshot: null,
    generation: -1,
    items: null,
    itemsKey: '',
    selected: {},
    planRows: [],
    planExcluded: {},
    filters: readStore(localStorage, FILTER_KEY, {}),
    expanded: readStore(sessionStorage, EXPANDED_KEY, {}),
    matches: {},
    plan: null,
    settings: null,
    settingsDirty: false,
    autoState: null,
    history: null,
    system: null,
    wasBusy: false,
    req: { status: 0, snapshot: 0, items: 0, system: 0, history: 0, settings: 0, match: {} },
    timers: {}
  };

  // ── helpers ──

  function readStore(store, key, fallback) {
    try {
      var value = JSON.parse(store.getItem(key) || 'null');
      return value && typeof value === 'object' ? value : fallback;
    } catch (_) {
      return fallback;
    }
  }

  function writeStore(store, key, value) {
    try { store.setItem(key, JSON.stringify(value)); } catch (_) {}
  }

  function $(id) { return document.getElementById(id); }

  function el(tag, attrs, children) {
    var node = document.createElement(tag);
    Object.keys(attrs || {}).forEach(function (key) {
      var value = attrs[key];
      if (value == null || value === false) return;
      if (key === 'text') node.textContent = String(value);
      else if (key === 'className') node.className = value;
      else if (key === 'style') node.setAttribute('style', value);
      else if (key.slice(0, 2) === 'on') node.addEventListener(key.slice(2), value);
      else if (key === 'dataset') Object.keys(value).forEach(function (k) { node.dataset[k] = value[k]; });
      else node.setAttribute(key, value === true ? '' : String(value));
    });
    (children || []).forEach(function (child) {
      if (child == null) return;
      node.appendChild(typeof child === 'string' ? document.createTextNode(child) : child);
    });
    return node;
  }

  function clear(node) { while (node && node.firstChild) node.removeChild(node.firstChild); }

  function fmtBytes(value) {
    var n = Number(value) || 0;
    var units = ['B', 'KB', 'MB', 'GB', 'TB'];
    var i = 0;
    while (n >= 1024 && i < units.length - 1) { n /= 1024; i += 1; }
    return (i === 0 ? String(Math.round(n)) : n.toFixed(n >= 100 ? 0 : 1)) + ' ' + units[i];
  }

  function fmtCount(value) {
    var n = Math.round(Number(value) || 0);
    return String(n).replace(/\B(?=(\d{3})+(?!\d))/g, "'");
  }

  function fmtAge(ts) {
    if (!ts) return '—';
    var s = Math.max(0, Date.now() / 1000 - Number(ts));
    if (s < 90) return 'just now';
    if (s < 5400) return Math.round(s / 60) + ' min ago';
    if (s < 172800) return Math.round(s / 3600) + ' h ago';
    return Math.round(s / 86400) + ' d ago';
  }

  function fmtDate(ts) {
    if (!ts) return '';
    var d = new Date(Number(ts) * 1000);
    function pad(n) { return (n < 10 ? '0' : '') + n; }
    return d.getFullYear() + '-' + pad(d.getMonth() + 1) + '-' + pad(d.getDate()) + ' ' + pad(d.getHours()) + ':' + pad(d.getMinutes());
  }

  function fmtDuration(seconds) {
    var s = Math.max(0, Math.round(Number(seconds) || 0));
    var d = Math.floor(s / 86400), h = Math.floor((s % 86400) / 3600), m = Math.floor((s % 3600) / 60);
    if (d) return d + 'd ' + h + 'h';
    if (h) return h + 'h ' + m + 'm';
    return m + 'm';
  }

  function api(path, options) {
    options = options || {};
    var init = { method: options.method || 'GET', credentials: 'same-origin', headers: {} };
    if (options.body !== undefined) {
      init.headers['Content-Type'] = 'application/json';
      init.body = JSON.stringify(options.body);
    }
    return fetch(API + path, init).then(function (res) {
      return res.json().catch(function () { return {}; }).then(function (data) {
        if (!res.ok) {
          var error = new Error(data && data.detail ? String(data.detail) : 'Request failed (' + res.status + ')');
          error.status = res.status;
          throw error;
        }
        return data;
      });
    });
  }

  function notify(title, message) {
    if (window.PBGuiDialogs) window.PBGuiDialogs.alert({ title: title, message: message });
  }

  function schedule(name, fn, ms) {
    clearTimeout(state.timers[name]);
    state.timers[name] = setTimeout(fn, ms);
  }

  function categories() { return (state.snapshot && state.snapshot.categories) || []; }

  function categoryById(id) {
    return categories().filter(function (c) { return c.id === id; })[0] || null;
  }

  function filterFor(category) {
    var stored = state.filters[category.id] || {};
    return {
      days: stored.days != null ? stored.days : (category.default_days || 0),
      keep: stored.keep != null ? stored.keep : (category.default_keep || 0)
    };
  }

  function isDefaultFilter(category) {
    var f = filterFor(category);
    return f.days === (category.default_days || 0) && f.keep === (category.default_keep || 0);
  }

  function tierBadge(tier) {
    return el('span', { className: 'badge ' + tier, tabindex: '0', 'data-tip': TIER_TIPS[tier] || '', text: TIER_LABELS[tier] || tier });
  }

  // ── navigation ──

  function parseHash() {
    var raw = decodeURIComponent(String(window.location.hash || '').replace(/^#/, ''));
    var parts = raw.split('/');
    var view = parts[0] || 'overview';
    if (!(view in TITLES) && !(view in GROUPS)) view = 'overview';
    return { view: view, category: view in GROUPS ? (parts[1] || '') : '' };
  }

  function writeHash() {
    var hash = '#' + encodeURIComponent(state.view) + (state.category ? '/' + encodeURIComponent(state.category) : '');
    if (window.location.hash !== hash) window.history.replaceState(null, '', window.location.pathname + window.location.search + hash);
  }

  function setView(view, category, options) {
    options = options || {};
    var changed = view !== state.view;
    state.view = view;
    if (category !== undefined) state.category = category || '';
    if (!(view in GROUPS)) state.category = '';
    if (state.plan && options.keepPlan !== true) state.plan = null;
    writeHash();
    document.querySelectorAll('#sidebar .sb-section[data-view]').forEach(function (btn) {
      var active = btn.getAttribute('data-view') === view;
      btn.classList.toggle('active', active);
      if (active) btn.setAttribute('aria-current', 'page'); else btn.removeAttribute('aria-current');
    });
    if (changed && options.scroll !== false) $('main-content').scrollTop = 0;
    render();
    if (view === 'system') loadSystem();
    if (view === 'history') loadHistory();
    if (view === 'retention') loadSettings();
    if (view in GROUPS) { loadMatches(); if (state.category) loadItems(); }
  }

  // ── data loading ──

  function pollStatus(viewing) {
    var id = ++state.req.status;
    return api('/status' + (viewing ? '?viewing=true' : '')).then(function (data) {
      if (id !== state.req.status) return;
      state.status = data;
      var finished = state.wasBusy && !data.busy;
      state.wasBusy = !!data.busy;
      renderStatus();
      // Mount values are live in every status response, the snapshot is not.
      renderSidebar();
      if (!state.plan && state.view === 'overview') renderDisk();
      if (data.generation !== state.generation) loadSnapshot();
      else if (finished) { state.matches = {}; render(); if (state.view in GROUPS) { loadMatches(); if (state.category) loadItems(true); } }
      if (finished && state.view === 'history') loadHistory();
    }).catch(function (error) {
      if (id === state.req.status) $('scan-state').textContent = error.message;
    }).then(function () {
      if (id !== state.req.status) return;
      var busy = state.status && state.status.busy;
      if (!document.hidden) schedule('status', function () { pollStatus(false); }, busy ? 1500 : 10000);
    });
  }

  function loadSnapshot() {
    var id = ++state.req.snapshot;
    return api('/snapshot').then(function (data) {
      if (id !== state.req.snapshot) return;
      state.snapshot = data;
      state.generation = data.generation;
      state.matches = {};
      render();
      if (state.view in GROUPS) { loadMatches(); if (state.category) loadItems(true); }
      if (state.view === 'history') loadHistory();
    }).catch(function (error) {
      if (id === state.req.snapshot) $('scan-state').textContent = error.message;
    });
  }

  function loadMatches() {
    categories().forEach(function (category) {
      if (category.group !== state.view || category.tier === 'protected' || isDefaultFilter(category)) return;
      var f = filterFor(category);
      var key = category.id + ':' + f.days + ':' + f.keep + ':' + state.generation;
      if (state.matches[category.id] && state.matches[category.id].key === key) return;
      var id = (state.req.match[category.id] || 0) + 1;
      state.req.match[category.id] = id;
      api('/categories/' + encodeURIComponent(category.id) + '/items?limit=0&days=' + f.days + '&keep=' + f.keep).then(function (data) {
        if (state.req.match[category.id] !== id) return;
        state.matches[category.id] = { key: key, count: data.match_count, bytes: data.match_bytes };
        if (state.view === category.group) renderCategoryTable();
      }).catch(function () {});
    });
  }

  function loadItems(preserve) {
    var category = categoryById(state.category);
    if (!category) { state.items = null; renderItems(); return; }
    var f = filterFor(category);
    var key = category.id + ':' + f.days + ':' + f.keep;
    var id = ++state.req.items;
    if (!preserve || state.itemsKey.split(':')[0] !== category.id) {
      state.items = null;
      state.selected = (readStore(sessionStorage, SELECTION_KEY, {})[category.id] || []).reduce(function (acc, item) { acc[item] = true; return acc; }, {});
      if (state.itemSelection) state.itemSelection.reset();
      renderItems();
    }
    api('/categories/' + encodeURIComponent(category.id) + '/items?days=' + f.days + '&keep=' + f.keep).then(function (data) {
      if (id !== state.req.items || state.category !== category.id) return;
      state.items = data;
      state.itemsKey = key;
      var valid = {};
      data.items.forEach(function (item) { if (!item.blocked && state.selected[item.id]) valid[item.id] = true; });
      state.selected = valid;
      persistSelection();
      renderItems();
    }).catch(function (error) {
      if (id !== state.req.items) return;
      state.items = { error: error.message, items: [] };
      renderItems();
    });
  }

  function persistSelection() {
    var stored = readStore(sessionStorage, SELECTION_KEY, {});
    var ids = Object.keys(state.selected);
    if (ids.length && ids.length <= MAX_STORED_SELECTION) stored[state.category] = ids;
    else delete stored[state.category];
    writeStore(sessionStorage, SELECTION_KEY, stored);
  }

  function loadSystem() {
    var id = ++state.req.system;
    api('/system').then(function (data) {
      if (id !== state.req.system) return;
      state.system = data;
      renderSystem();
    }).catch(function (error) {
      if (id === state.req.system) { clear($('system-grid')); $('system-grid').appendChild(el('div', { className: 'empty', text: error.message })); }
    }).then(function () {
      if (id === state.req.system && state.view === 'system' && !document.hidden) schedule('system', loadSystem, 5000);
    });
  }

  function loadHistory() {
    var id = ++state.req.history;
    api('/history').then(function (data) {
      if (id !== state.req.history) return;
      state.history = data.history || [];
      renderHistory();
    }).catch(function () {});
  }

  function loadSettings() {
    if (state.settingsDirty) return;
    var id = ++state.req.settings;
    api('/settings').then(function (data) {
      if (id !== state.req.settings || state.settingsDirty) return;
      state.settings = data.settings;
      state.autoState = data.auto_state || {};
      state.ruleCategories = data.categories || [];
      renderSettings();
    }).catch(function (error) { $('settings-state').textContent = error.message; });
  }

  // ── rendering ──

  function render() {
    var view = state.plan ? 'preview' : (state.view in GROUPS ? 'group' : state.view);
    document.querySelectorAll('[data-view-panel]').forEach(function (panel) {
      panel.classList.toggle('is-active', panel.getAttribute('data-view-panel') === view);
    });
    $('view-title').textContent = state.plan ? 'Cleanup preview' : (GROUPS[state.view] || TITLES[state.view] || 'Storage');
    renderSidebar();
    renderStatus();
    if (state.plan) { renderPreview(); return; }
    if (view === 'overview') renderOverview();
    if (view === 'group') { renderCategoryTable(); renderItems(); }
  }

  function renderSidebar() {
    var totals = {};
    categories().forEach(function (c) {
      if (c.tier !== 'protected') totals[c.group] = (totals[c.group] || 0) + c.bytes;
    });
    document.querySelectorAll('[data-group-size]').forEach(function (node) {
      var value = totals[node.getAttribute('data-group-size')];
      node.textContent = value ? fmtBytes(value) : '';
    });
    var mount = pbguiMount();
    var badge = $('sb-disk');
    if (mount) {
      badge.hidden = false;
      badge.textContent = Math.round(mount.percent) + '%';
      badge.title = 'Disk ' + mount.mount + ': ' + fmtBytes(mount.free) + ' free';
    }
  }

  function pbguiMount() {
    var mounts = (state.status && state.status.mounts) || (state.snapshot && state.snapshot.mounts) || [];
    return mounts.filter(function (m) { return (m.roots || []).indexOf('pbgui') >= 0; })[0] || mounts[0] || null;
  }

  function renderStatus() {
    var status = state.status;
    var text;
    if (!status) text = 'Loading…';
    else if (status.busy && status.job && status.job.mode === 'scan') text = 'Scanning…';
    else if (status.scanned_at) text = 'Scanned ' + fmtAge(status.scanned_at) + (status.updated_at && status.updated_at !== status.scanned_at ? ' · updated ' + fmtAge(status.updated_at) : '');
    else text = 'The first scan starts automatically.';
    $('scan-state').textContent = text;
    $('scan-state').title = status && status.scanned_at ? fmtDate(status.scanned_at) : '';
    var box = $('progress');
    var progress = status && status.busy ? (status.progress || {}) : null;
    box.classList.toggle('visible', !!progress);
    if (!progress) return;
    var mode = (status.job && status.job.mode) || progress.mode;
    $('progress-title').textContent = mode === 'scan' ? 'Scanning folders' : mode === 'auto' ? 'Automatic cleanup' : 'Deleting';
    var meta = progress.phase || 'Starting';
    var pct = 100;
    if (progress.total_items) {
      meta = fmtCount(progress.done_items) + ' / ' + fmtCount(progress.total_items) + ' entries · ' + fmtBytes(progress.freed_bytes) + ' of ' + fmtBytes(progress.total_bytes);
      pct = progress.total_bytes ? Math.min(100, 100 * progress.freed_bytes / progress.total_bytes) : 100 * progress.done_items / progress.total_items;
    }
    $('progress-meta').textContent = meta;
    $('progress-fill').style.width = pct.toFixed(1) + '%';
    $('progress-current').textContent = progress.current || '';
  }

  function renderOverview() {
    renderDisk();
    renderReclaim();
    renderLastResult();
    renderTree();
  }

  function renderDisk() {
    var body = $('disk-body');
    clear(body);
    var mounts = (state.status && state.status.mounts) || [];
    var snapshot = state.snapshot;
    if (!mounts.length) body.appendChild(el('div', { className: 'empty', text: 'Disk usage is loading…' }));
    mounts.forEach(function (mount) {
      var cls = mount.percent >= 95 ? 'bad' : mount.percent >= 85 ? 'warn' : '';
      body.appendChild(el('div', { className: 'stat-grid' }, [
        el('div', { className: 'stat' }, [
          el('span', { className: 'stat-label', text: 'Disk ' + mount.mount }),
          el('span', { className: 'stat-value', text: Math.round(mount.percent) + '% used' }),
          el('div', { className: 'meter' }, [el('div', { className: 'meter-fill ' + cls, style: 'width:' + Math.min(100, mount.percent) + '%' })]),
          el('span', { className: 'stat-detail', text: fmtBytes(mount.used) + ' of ' + fmtBytes(mount.total) + ' · ' + fmtBytes(mount.free) + ' free' })
        ])
      ]));
      if (snapshot && snapshot.ready) body.appendChild(composition(mount, snapshot));
    });
  }

  function composition(mount, snapshot) {
    var segments = [];
    var known = 0;
    (snapshot.roots || []).forEach(function (root, index) {
      if ((mount.roots || []).indexOf(root.id) < 0) return;
      var size = (root.tree && root.tree.size) || 0;
      known += size;
      segments.push({ label: root.label, size: size, color: COLORS[index % COLORS.length] });
    });
    segments.push({ label: 'Other', size: Math.max(0, mount.used - known), color: '#475569' });
    segments.push({ label: 'Free', size: mount.free, color: '#1e293b' });
    var total = mount.total || 1;
    var bar = el('div', { className: 'composition', role: 'img', 'aria-label': 'Disk composition' });
    var legend = el('div', { className: 'legend' });
    segments.forEach(function (segment) {
      bar.appendChild(el('span', { title: segment.label + ': ' + fmtBytes(segment.size), style: 'width:' + (100 * segment.size / total).toFixed(2) + '%;background:' + segment.color }));
      legend.appendChild(el('span', {}, [el('span', { className: 'legend-dot', style: 'background:' + segment.color }), segment.label + ' ' + fmtBytes(segment.size)]));
    });
    return el('div', { style: 'display:grid;gap:8px' }, [bar, legend]);
  }

  function renderReclaim() {
    var grid = $('reclaim-grid');
    clear(grid);
    var snapshot = state.snapshot;
    if (!snapshot || !snapshot.ready) {
      grid.appendChild(el('div', { className: 'empty', text: state.status && state.status.busy ? 'Waiting for the first scan…' : 'No scan yet.' }));
      return;
    }
    function breakdown(tier) {
      var rows = categories().filter(function (c) { return c.tier === tier; }).map(function (c) {
        return { category: c, bytes: tier === 'protected' ? c.bytes : c.default_bytes, count: tier === 'protected' ? c.count : c.default_count };
      }).filter(function (row) { return row.bytes > 0 || row.category.blocked; });
      rows.sort(function (a, b) { return b.bytes - a.bytes; });
      return el('div', { className: 'tier-list' }, rows.map(function (row) {
        var c = row.category;
        return el('button', { className: 'tier-row', type: 'button', title: c.blocked || c.tip,
          'aria-label': 'Open ' + c.label + ' in ' + GROUPS[c.group],
          onclick: function () { setView(c.group, c.id); } }, [
          el('span', { className: 'tier-row-name', text: c.label }),
          el('span', { className: 'tier-row-size', text: c.blocked ? 'blocked' : fmtBytes(row.bytes) })
        ]);
      }));
    }
    function tile(tier, detail, action) {
      var t = snapshot.tiers[tier] || { bytes: 0, count: 0 };
      return el('div', { className: 'stat ' + tier }, [
        el('span', { className: 'stat-label' }, [tierBadge(tier)]),
        el('span', { className: 'stat-value', text: fmtBytes(t.bytes) }),
        el('span', { className: 'stat-detail', text: detail.replace('{n}', fmtCount(t.count)) }),
        breakdown(tier),
        action
      ]);
    }
    var busy = state.status && state.status.busy;
    grid.appendChild(tile('safe', '{n} entries with default filters',
      el('button', { className: 'btn primary small', type: 'button', disabled: busy || !snapshot.tiers.safe.count, onclick: function () { previewTier('safe'); }, text: 'Preview safe cleanup' })));
    grid.appendChild(tile('optional', '{n} entries with default filters',
      el('button', { className: 'btn small', type: 'button', disabled: busy || !snapshot.tiers.optional.count, onclick: function () { previewTier('optional'); }, text: 'Preview optional cleanup' })));
    grid.appendChild(tile('review', '{n} entries to choose from', null));
    grid.appendChild(tile('protected', 'Never deleted here', null));
  }

  function renderLastResult() {
    var box = $('last-result');
    clear(box);
    var result = state.status && state.status.last_result;
    if (!result) return;
    var freed = result.freed_bytes || 0;
    var status = result.status || 'done';
    var title = (result.mode === 'auto' ? 'Last automatic cleanup' : result.mode === 'scan' ? 'Last scan' : 'Last cleanup') +
      ' ' + fmtAge(result.finished_at) + ': ' + (status === 'done' ? '' : status + ' · ') +
      fmtCount(result.done_items || 0) + ' entries deleted, ' + fmtBytes(freed) + ' freed';
    var errors = result.errors || [];
    var node = el('div', { className: 'result' + (status === 'failed' ? ' err' : '') }, [el('strong', { text: title })]);
    errors.slice(0, 5).forEach(function (text) { node.appendChild(el('span', { className: 'muted', text: text })); });
    if (errors.length > 5) node.appendChild(el('span', { className: 'muted', text: '… ' + (errors.length - 5) + ' more' }));
    box.appendChild(node);
  }

  function renderTree() {
    var body = $('tree-table').tBodies[0];
    clear(body);
    var snapshot = state.snapshot;
    if (!snapshot || !snapshot.ready) return;
    (snapshot.roots || []).forEach(function (root) {
      addTreeRows(body, root.tree, root.id, 0, (root.tree && root.tree.size) || 1);
    });
  }

  function addTreeRows(body, node, key, depth, rootSize) {
    if (!node) return;
    var children = node.children || [];
    var open = !!state.expanded[key];
    var toggle = children.length
      ? el('button', { className: 'tree-toggle', type: 'button', 'aria-expanded': open ? 'true' : 'false', 'aria-label': (open ? 'Collapse ' : 'Expand ') + node.name, text: open ? '▾' : '▸',
          onclick: function () { state.expanded[key] = !open; if (!state.expanded[key]) delete state.expanded[key]; writeStore(sessionStorage, EXPANDED_KEY, state.expanded); renderTree(); } })
      : el('span', { className: 'tree-toggle' });
    var share = Math.min(100, 100 * (node.size || 0) / rootSize);
    body.appendChild(el('tr', { className: 'tree-row' }, [
      el('td', { title: node.path || '' }, [el('span', { style: 'display:inline-block;width:' + (depth * 16) + 'px' }), toggle, depth === 0 ? el('strong', { text: node.name }) : (node.other ? el('span', { className: 'muted', text: node.name }) : node.name)]),
      el('td', { className: 'num', text: fmtBytes(node.size) }),
      el('td', { className: 'num', text: node.files ? fmtCount(node.files) : '' }),
      el('td', {}, [el('span', { className: 'tree-bar', style: 'width:' + Math.max(1, share * 1.2).toFixed(1) + 'px' }), el('span', { className: 'muted', text: ' ' + share.toFixed(share < 10 ? 1 : 0) + '%' })]),
      el('td', { className: 'muted', title: fmtDate(node.mtime), text: node.mtime ? fmtAge(node.mtime) : '' })
    ]));
    if (open) children.forEach(function (child, index) { addTreeRows(body, child, key + '/' + (child.name || index), depth + 1, rootSize); });
  }

  function renderCategoryTable() {
    $('group-title').textContent = GROUPS[state.view] || 'Categories';
    var body = $('category-table').tBodies[0];
    var focused = document.activeElement && body.contains(document.activeElement) ? document.activeElement.getAttribute('data-focus-key') : null;
    clear(body);
    var snapshot = state.snapshot;
    if (!snapshot || !snapshot.ready) {
      body.appendChild(el('tr', {}, [el('td', { colspan: '8', className: 'empty', text: 'Waiting for the first scan…' })]));
      return;
    }
    var busy = state.status && state.status.busy;
    categories().filter(function (c) { return c.group === state.view; }).forEach(function (category) {
      var f = filterFor(category);
      var deletable = category.tier !== 'protected';
      var match = isDefaultFilter(category) ? { count: category.default_count, bytes: category.default_bytes } : state.matches[category.id];
      var filterCell = el('div', { className: 'filter-cell' });
      if (deletable) {
        filterCell.appendChild(numberInput(category, 'days', f.days, 'Older than (days)'));
        if (category.default_keep != null) filterCell.appendChild(numberInput(category, 'keep', f.keep, 'Keep newest'));
      }
      var actions = el('div', { className: 'actions' });
      if (deletable && category.tier !== 'review') {
        actions.appendChild(el('button', { className: 'btn small primary', type: 'button', disabled: busy || !!category.blocked || !(match && match.count),
          onclick: function () { createPreview([{ category: category.id, days: f.days, keep: f.keep }]); }, text: 'Preview' }));
      }
      if (category.count) {
        actions.appendChild(el('button', { className: 'btn small', type: 'button', 'aria-pressed': state.category === category.id ? 'true' : 'false',
          onclick: function () { setView(state.view, state.category === category.id ? '' : category.id); },
          text: state.category === category.id ? 'Hide entries' : (category.tier === 'review' ? 'Choose entries' : 'Show entries') }));
      }
      var name = el('td', { className: 'name' }, [el('span', { tabindex: '0', 'data-tip': category.tip, text: category.label })]);
      if (category.blocked) name.appendChild(el('div', { className: 'faint', text: category.blocked }));
      else if (!category.available) name.appendChild(el('div', { className: 'faint', text: 'Not present' }));
      body.appendChild(el('tr', { className: state.category === category.id ? 'is-open' : '' }, [
        name,
        el('td', {}, [tierBadge(category.tier)]),
        el('td', { className: 'num', text: fmtCount(category.count) }),
        el('td', { className: 'num', text: fmtBytes(category.bytes) }),
        el('td', { className: 'muted', title: fmtDate(category.oldest), text: category.oldest ? fmtAge(category.oldest) : '—' }),
        el('td', {}, [filterCell]),
        el('td', { className: 'num', text: !deletable ? '—' : match ? fmtCount(match.count) + ' · ' + fmtBytes(match.bytes) : '…' }),
        el('td', {}, [actions])
      ]));
    });
    if (focused) {
      var again = body.querySelector('[data-focus-key="' + focused.replace(/"/g, '') + '"]');
      if (again) again.focus();
    }
  }

  function numberInput(category, field, value, label) {
    var key = category.id + ':' + field;
    var input = el('input', { type: 'number', min: '0', max: field === 'days' ? '3650' : '1000', step: '1', value: String(value), 'aria-label': category.label + ': ' + label, 'data-focus-key': key });
    input.addEventListener('change', function () {
      var n = Math.max(0, Math.min(Number(input.max), Math.round(Number(input.value) || 0)));
      input.value = String(n);
      var stored = state.filters[category.id] || {};
      stored[field] = n;
      state.filters[category.id] = stored;
      writeStore(localStorage, FILTER_KEY, state.filters);
      loadMatches();
      renderCategoryTable();
      if (state.category === category.id) loadItems(true);
    });
    return el('label', {}, [el('span', { className: 'faint', text: field === 'days' ? '> ' : 'keep ' }), input, field === 'days' ? el('span', { className: 'faint', text: ' d' }) : null]);
  }

  function renderItems() {
    var panel = $('items-panel');
    var category = categoryById(state.category);
    panel.hidden = !category;
    if (!category) return;
    $('items-title').textContent = category.label;
    var blocked = $('items-blocked');
    clear(blocked);
    if (category.blocked) blocked.appendChild(el('div', { className: 'notice', text: category.blocked }));
    var body = $('items-table').tBodies[0];
    clear(body);
    var data = state.items;
    if (!data) { body.appendChild(el('tr', {}, [el('td', { colspan: '5', className: 'empty', text: 'Loading…' })])); updateSelectionInfo(); return; }
    if (data.error) { body.appendChild(el('tr', {}, [el('td', { colspan: '5', className: 'empty', text: data.error })])); updateSelectionInfo(); return; }
    if (!data.items.length) body.appendChild(el('tr', {}, [el('td', { colspan: '5', className: 'empty', text: 'No entries.' })]));
    var deletable = category.tier !== 'protected';
    data.items.forEach(function (item, index) {
      var row = el('tr', { className: (item.blocked || !deletable ? 'is-blocked' : '') + (state.selected[item.id] ? ' selected' : ''), dataset: { index: String(index) },
        'aria-selected': state.selected[item.id] ? 'true' : 'false', tabindex: item.blocked || !deletable ? null : '0' }, [
        el('td', { className: 'name', title: (item.paths || []).join('\n') }, [item.label]),
        el('td', { className: 'num', text: fmtBytes(item.size) }),
        el('td', { className: 'num', text: item.files ? fmtCount(item.files) : '' }),
        el('td', { className: 'muted', title: fmtDate(item.mtime), text: item.mtime ? fmtAge(item.mtime) : '' }),
        el('td', { className: item.blocked ? 'faint' : 'muted', text: item.blocked || (item.match ? 'Matches filter' : '') })
      ]);
      body.appendChild(row);
    });
    updateSelectionInfo();
  }

  function selectable(item) {
    var category = categoryById(state.category);
    return item && !item.blocked && category && category.tier !== 'protected';
  }

  function setRowSelected(index, value) {
    var item = state.items.items[index];
    if (!selectable(item)) return;
    if (value) state.selected[item.id] = true; else delete state.selected[item.id];
    markRow($('items-table').tBodies[0].rows[index], value);
  }

  function updateSelectionInfo() {
    var ids = Object.keys(state.selected);
    var bytes = 0;
    ((state.items && state.items.items) || []).forEach(function (item) { if (state.selected[item.id]) bytes += item.size || 0; });
    $('items-selection').textContent = ids.length ? fmtCount(ids.length) + ' selected · ' + fmtBytes(bytes) : '';
    var category = categoryById(state.category);
    var busy = state.status && state.status.busy;
    $('items-preview').disabled = busy || !ids.length || !category || category.tier === 'protected' || !!category.blocked;
    $('items-select-matching').disabled = !category || category.tier === 'protected';
    $('items-clear').disabled = !ids.length;
  }

  // Shared row selection: click toggles, drag or Shift-click selects a range,
  // Space/Enter toggles the focused row. `model` adapts it to one table.
  function attachRowSelection(body, model) {
    var anchor = null, drag = null;
    function apply(from, to, value) {
      for (var i = Math.min(from, to); i <= Math.max(from, to); i += 1) if (model.canSelect(i)) model.set(i, value);
    }
    body.addEventListener('mousedown', function (event) {
      var row = event.target.closest('tr[data-index]');
      if (!row || event.button !== 0) return;
      var index = Number(row.dataset.index);
      if (!model.canSelect(index)) return;
      event.preventDefault();
      row.focus();
      if (event.shiftKey && anchor != null) {
        apply(anchor, index, true);
      } else {
        var value = !model.isSelected(index);
        model.set(index, value);
        anchor = index;
        drag = { value: value };
      }
      model.changed();
    });
    body.addEventListener('mouseover', function (event) {
      if (!drag) return;
      var row = event.target.closest('tr[data-index]');
      if (!row) return;
      apply(anchor, Number(row.dataset.index), drag.value);
      model.changed();
    });
    document.addEventListener('mouseup', function () {
      if (drag) { drag = null; model.changed(true); }
    });
    body.addEventListener('keydown', function (event) {
      var row = event.target.closest('tr[data-index]');
      if (!row || (event.key !== ' ' && event.key !== 'Enter')) return;
      var index = Number(row.dataset.index);
      if (!model.canSelect(index)) return;
      event.preventDefault();
      model.set(index, !model.isSelected(index));
      anchor = index;
      model.changed(true);
    });
    return { reset: function () { anchor = null; drag = null; } };
  }

  function markRow(row, value) {
    if (!row) return;
    row.classList.toggle('selected', !!value);
    row.setAttribute('aria-selected', value ? 'true' : 'false');
  }

  function setupItemSelection() {
    state.itemSelection = attachRowSelection($('items-table').tBodies[0], {
      canSelect: function (i) { return !!state.items && selectable(state.items.items[i]); },
      isSelected: function (i) { return !!state.selected[state.items.items[i].id]; },
      set: setRowSelected,
      changed: function () { persistSelection(); updateSelectionInfo(); }
    });
    $('items-select-matching').addEventListener('click', function () {
      ((state.items && state.items.items) || []).forEach(function (item, index) { if (item.match) setRowSelected(index, true); });
      persistSelection();
      updateSelectionInfo();
    });
    $('items-clear').addEventListener('click', function () {
      state.selected = {};
      persistSelection();
      renderItems();
    });
    $('items-preview').addEventListener('click', function () {
      createPreview([{ category: state.category, item_ids: Object.keys(state.selected) }]);
    });
  }

  // ── preview and cleanup ──

  function previewTier(tier) {
    var selections = categories().filter(function (c) {
      return c.tier === tier && !c.blocked && c.default_count;
    }).map(function (c) { var f = filterFor(c); return { category: c.id, days: f.days, keep: f.keep }; });
    if (selections.length) createPreview(selections);
  }

  function createPreview(selections) {
    api('/preview', { method: 'POST', body: { selections: selections } }).then(function (plan) {
      state.plan = plan;
      state.planExcluded = {};
      state.planRows = [];
      plan.categories.forEach(function (entry) {
        entry.items.forEach(function (item) { state.planRows.push({ category: entry.category, label: entry.label, item: item }); });
      });
      if (state.planSelection) state.planSelection.reset();
      render();
      $('main-content').scrollTop = 0;
    }).catch(function (error) { notify('Preview failed', error.message); });
  }

  function planKey(row) { return row.category + '\u0000' + row.item.id; }

  function planTotals() {
    var count = 0, bytes = 0, byCategory = {};
    state.plan.categories.forEach(function (entry) {
      var hidden = entry.count - entry.items.length;
      byCategory[entry.category] = { count: hidden, bytes: entry.bytes - entry.items.reduce(function (sum, item) { return sum + (item.size || 0); }, 0) };
    });
    state.planRows.forEach(function (row) {
      if (state.planExcluded[planKey(row)]) return;
      byCategory[row.category].count += 1;
      byCategory[row.category].bytes += row.item.size || 0;
    });
    Object.keys(byCategory).forEach(function (key) { count += byCategory[key].count; bytes += byCategory[key].bytes; });
    return { count: count, bytes: bytes, byCategory: byCategory };
  }

  function updatePreviewTotals() {
    var plan = state.plan;
    if (!plan) return;
    var totals = planTotals();
    var button = $('preview-delete');
    button.disabled = !totals.count || !!(state.status && state.status.busy);
    button.textContent = totals.count ? 'Delete ' + fmtCount(totals.count) + ' entries (' + fmtBytes(totals.bytes) + ')' : 'Delete';
    plan.categories.forEach(function (entry) {
      var node = document.querySelector('[data-plan-total="' + entry.category + '"]');
      var value = totals.byCategory[entry.category];
      if (node) node.textContent = fmtBytes(value.bytes);
      var detail = document.querySelector('[data-plan-detail="' + entry.category + '"]');
      if (detail) detail.textContent = entry.blocked ? entry.blocked : fmtCount(value.count) + ' of ' + fmtCount(entry.count) + ' entries selected';
    });
    var excluded = Object.keys(state.planExcluded).length;
    $('preview-selection').textContent = fmtCount(state.planRows.length - excluded) + ' of ' + fmtCount(state.planRows.length) + ' listed entries selected';
  }

  function renderPreview() {
    var plan = state.plan;
    var body = $('preview-body');
    clear(body);
    body.appendChild(el('div', { className: 'stat-grid' }, plan.categories.map(function (entry) {
      return el('div', { className: 'stat ' + entry.tier }, [
        el('span', { className: 'stat-label', text: entry.label }),
        el('span', { className: 'stat-value', 'data-plan-total': entry.category, text: fmtBytes(entry.bytes) }),
        el('span', { className: 'stat-detail', 'data-plan-detail': entry.category, text: entry.blocked ? entry.blocked : fmtCount(entry.count) + ' entries' })
      ]);
    })));
    if (!plan.count) {
      body.appendChild(el('div', { className: 'empty', text: 'Nothing matches the selected filters.' }));
      updatePreviewTotals();
      return;
    }
    body.appendChild(el('div', { className: 'actions' }, [
      el('span', { className: 'muted', id: 'preview-selection' }),
      el('button', { className: 'btn small', type: 'button', text: 'Select all', onclick: function () { state.planExcluded = {}; renderPreview(); } }),
      el('button', { className: 'btn small', type: 'button', text: 'Select none', onclick: function () {
        state.planExcluded = {};
        state.planRows.forEach(function (row) { state.planExcluded[planKey(row)] = true; });
        renderPreview();
      } })
    ]));
    var tbody = el('tbody', {}, state.planRows.map(function (row, index) {
      var selected = !state.planExcluded[planKey(row)];
      return el('tr', { className: selected ? 'selected' : '', tabindex: '0', 'aria-selected': selected ? 'true' : 'false', dataset: { index: String(index) } }, [
        el('td', { className: 'muted', text: row.label }),
        el('td', { className: 'name', title: (row.item.paths || []).join('\n'), text: row.item.label }),
        el('td', { className: 'num', text: fmtBytes(row.item.size) }),
        el('td', { className: 'muted', title: fmtDate(row.item.mtime), text: row.item.mtime ? fmtAge(row.item.mtime) : '' })
      ]);
    }));
    plan.categories.forEach(function (entry) {
      if (entry.count > entry.items.length) {
        tbody.appendChild(el('tr', {}, [el('td', { colspan: '4', className: 'muted', text: entry.label + ': ' + fmtCount(entry.count - entry.items.length) + ' smaller entries not listed (always included)' })]));
      }
    });
    body.appendChild(el('div', { className: 'table-wrap tall' }, [el('table', { className: 'tbl selectable', id: 'preview-table' }, [
      el('thead', {}, [el('tr', {}, [el('th', { text: 'Category' }), el('th', { text: 'Entry' }), el('th', { className: 'num', text: 'Size' }), el('th', { text: 'Last change' })])]),
      tbody
    ])]));
    state.planSelection = attachRowSelection(tbody, {
      canSelect: function (i) { return i < state.planRows.length; },
      isSelected: function (i) { return !state.planExcluded[planKey(state.planRows[i])]; },
      set: function (i, value) {
        var key = planKey(state.planRows[i]);
        if (value) delete state.planExcluded[key]; else state.planExcluded[key] = true;
        markRow(tbody.rows[i], value);
      },
      changed: updatePreviewTotals
    });
    updatePreviewTotals();
  }

  function planExclude() {
    var exclude = {};
    state.planRows.forEach(function (row) {
      if (!state.planExcluded[planKey(row)]) return;
      (exclude[row.category] = exclude[row.category] || []).push(row.item.id);
    });
    return exclude;
  }

  function setupPreview() {
    $('preview-discard').addEventListener('click', function () { state.plan = null; render(); });
    $('preview-delete').addEventListener('click', function () {
      var plan = state.plan;
      if (!plan || !window.PBGuiDialogs) return;
      var totals = planTotals();
      if (!totals.count) return;
      var exclude = planExclude();
      window.PBGuiDialogs.confirm({
        title: 'Delete files',
        message: 'Delete ' + fmtCount(totals.count) + ' entries (' + fmtBytes(totals.bytes) + ')?',
        detail: 'This cannot be undone. Every entry is checked again before deletion; entries that are now in use or no longer match are skipped.',
        confirmText: 'Delete'
      }).then(function (ok) {
        if (!ok || state.plan !== plan) return;
        return api('/cleanup', { method: 'POST', body: { plan_id: plan.id, exclude: exclude } }).then(function () {
          state.plan = null;
          if (state.category) {
            state.selected = {};
            persistSelection();
          }
          render();
          pollStatus(false);
        });
      }).catch(function (error) { notify('Cleanup failed', error.message); });
    });
    $('cancel-btn').addEventListener('click', function () {
      api('/cancel', { method: 'POST', body: {} }).then(function () { pollStatus(false); }).catch(function (error) { notify('Stop failed', error.message); });
    });
  }

  // ── retention ──

  function renderSettings() {
    var settings = state.settings;
    if (!settings) return;
    $('scan-interval').value = String(settings.scan_interval_hours);
    $('auto-enabled').checked = !!settings.auto.enabled;
    $('auto-hour').value = String(settings.auto.hour);
    var body = $('rules-table').tBodies[0];
    clear(body);
    (state.ruleCategories || []).forEach(function (category) {
      var rule = settings.auto.rules[category.id] || { enabled: false, days: 0, keep: 0 };
      function input(field, max) {
        var node = el('input', { type: 'number', min: '0', max: String(max), step: '1', value: String(rule[field] || 0), 'aria-label': category.label + ': ' + field });
        node.addEventListener('input', function () { rule[field] = Math.max(0, Math.round(Number(node.value) || 0)); settings.auto.rules[category.id] = rule; markDirty(); });
        return node;
      }
      var check = el('input', { type: 'checkbox', 'aria-label': category.label + ': enabled' });
      check.checked = !!rule.enabled;
      check.addEventListener('change', function () { rule.enabled = check.checked; settings.auto.rules[category.id] = rule; markDirty(); });
      body.appendChild(el('tr', {}, [
        el('td', { className: 'name' }, [el('span', { tabindex: '0', 'data-tip': category.tip, text: category.label })]),
        el('td', {}, [tierBadge(category.tier)]),
        el('td', {}, [check]),
        el('td', {}, [input('days', 3650)]),
        el('td', {}, [category.default_keep != null ? input('keep', 1000) : el('span', { className: 'faint', text: '—' })])
      ]));
    });
    var last = state.autoState && state.autoState.last_run;
    $('auto-last').textContent = last ? 'Last automatic cleanup started ' + fmtDate(last) + '.' : 'Automatic cleanup has not run yet.';
    $('settings-save').disabled = !state.settingsDirty;
    $('settings-state').textContent = state.settingsDirty ? 'Unsaved changes' : '';
  }

  function markDirty() {
    state.settingsDirty = true;
    $('settings-save').disabled = false;
    $('settings-state').textContent = 'Unsaved changes';
  }

  function setupSettings() {
    $('scan-interval').addEventListener('input', function () { if (state.settings) { state.settings.scan_interval_hours = Math.round(Number($('scan-interval').value) || 0); markDirty(); } });
    $('auto-enabled').addEventListener('change', function () { if (state.settings) { state.settings.auto.enabled = $('auto-enabled').checked; markDirty(); } });
    $('auto-hour').addEventListener('input', function () { if (state.settings) { state.settings.auto.hour = Math.round(Number($('auto-hour').value) || 0); markDirty(); } });
    $('settings-save').addEventListener('click', function () {
      if (!state.settings) return;
      $('settings-save').disabled = true;
      api('/settings', { method: 'PUT', body: state.settings }).then(function (data) {
        state.settings = data.settings;
        state.settingsDirty = false;
        renderSettings();
        $('settings-state').textContent = 'Saved';
      }).catch(function (error) {
        $('settings-save').disabled = false;
        notify('Settings not saved', error.message);
      });
    });
  }

  // ── history ──

  function renderHistory() {
    var history = state.history || [];
    var chart = $('history-chart');
    var body = $('growth-table').tBodies[0];
    clear(body);
    if (!history.length) {
      if (window.Plotly) window.Plotly.purge(chart);
      clear(chart);
      chart.appendChild(el('div', { className: 'empty', text: 'History starts with the first scan. One point is kept per day.' }));
      return;
    }
    var x = history.map(function (point) { return new Date(point.ts * 1000); });
    var traces = [];
    var mounts = {};
    history.forEach(function (point) { (point.mounts || []).forEach(function (m) { mounts[m.mount] = true; }); });
    Object.keys(mounts).forEach(function (mount) {
      traces.push({ x: x, y: history.map(function (p) { var m = (p.mounts || []).filter(function (v) { return v.mount === mount; })[0]; return m ? m.used / Math.pow(1024, 3) : null; }),
        name: 'Disk ' + mount + ' used', mode: 'lines', line: { width: 3, color: '#e2e8f0' } });
    });
    var roots = {};
    history.forEach(function (point) { Object.keys(point.roots || {}).forEach(function (key) { roots[key] = true; }); });
    Object.keys(roots).forEach(function (key, index) {
      traces.push({ x: x, y: history.map(function (p) { return p.roots && p.roots[key] != null ? p.roots[key] / Math.pow(1024, 3) : null; }),
        name: rootLabel(key), mode: 'lines', line: { width: 2, color: COLORS[index % COLORS.length] } });
    });
    if (window.Plotly) {
      clear(chart);
      window.Plotly.react(chart, traces, {
        paper_bgcolor: '#131b2b', plot_bgcolor: '#131b2b', font: { color: '#94a3b8', size: 12 },
        margin: { l: 56, r: 16, t: 12, b: 40 }, legend: { orientation: 'h', y: -0.18 },
        yaxis: { title: 'GB', gridcolor: '#1e2736', rangemode: 'tozero' }, xaxis: { gridcolor: '#1e2736' }, hovermode: 'x unified'
      }, { displayModeBar: false, responsive: true });
    }
    var latest = history[history.length - 1];
    function before(days) {
      var limit = latest.ts - days * 86400;
      var candidates = history.filter(function (p) { return p.ts <= limit; });
      return candidates.length ? candidates[candidates.length - 1] : null;
    }
    var week = before(7), month = before(30);
    function delta(point, getter) {
      if (!point) return '—';
      var value = getter(latest) - getter(point);
      return (value >= 0 ? '+' : '−') + fmtBytes(Math.abs(value));
    }
    var rows = [];
    Object.keys(latest.roots || {}).forEach(function (key) {
      rows.push({ label: rootLabel(key), get: function (p) { return (p.roots || {})[key] || 0; }, strong: true });
    });
    Object.keys(latest.areas || {}).sort(function (a, b) { return latest.areas[b] - latest.areas[a]; }).forEach(function (key) {
      rows.push({ label: key, get: function (p) { return (p.areas || {})[key] || 0; } });
    });
    rows.forEach(function (row) {
      body.appendChild(el('tr', {}, [
        el('td', { className: 'name' }, [row.strong ? el('strong', { text: row.label }) : row.label]),
        el('td', { className: 'num', text: fmtBytes(row.get(latest)) }),
        el('td', { className: 'num', text: delta(week, row.get) }),
        el('td', { className: 'num', text: delta(month, row.get) })
      ]));
    });
  }

  function rootLabel(key) {
    var root = ((state.snapshot && state.snapshot.roots) || []).filter(function (r) { return r.id === key; })[0];
    return root ? root.label : key;
  }

  // ── system ──

  function renderSystem() {
    var data = state.system;
    var grid = $('system-grid');
    clear(grid);
    if (!data) return;
    var s = data.system || {};
    function stat(label, value, detail, meter) {
      var children = [el('span', { className: 'stat-label', text: label }), el('span', { className: 'stat-value', text: value })];
      if (meter != null) {
        var cls = meter >= 90 ? 'bad' : meter >= 75 ? 'warn' : '';
        children.push(el('div', { className: 'meter' }, [el('div', { className: 'meter-fill ' + cls, style: 'width:' + Math.min(100, meter) + '%' })]));
      }
      (Array.isArray(detail) ? detail : [detail]).forEach(function (line) { if (line) children.push(el('span', { className: 'stat-detail', text: line })); });
      return el('div', { className: 'stat' }, children);
    }
    var mem = s.memory || {}, swap = s.swap || {};
    grid.appendChild(stat('CPU', Math.round(s.cpu_percent || 0) + '%', [s.cpu_model, (s.cpu_physical || '?') + ' cores · ' + (s.cpu_logical || '?') + ' threads' + (s.cpu_freq_mhz ? ' · ' + s.cpu_freq_mhz + ' MHz' : ''), s.load && s.load.length ? 'Load ' + s.load.join(' / ') : ''], s.cpu_percent || 0));
    grid.appendChild(stat('Memory', fmtBytes(mem.used) + ' / ' + fmtBytes(mem.total), fmtBytes(mem.available) + ' available', mem.percent || 0));
    grid.appendChild(stat('Swap', swap.total ? fmtBytes(swap.used) + ' / ' + fmtBytes(swap.total) : 'None', swap.total ? Math.round(swap.percent || 0) + '% used' : '', swap.total ? swap.percent : null));
    grid.appendChild(stat('Uptime', fmtDuration(s.uptime_s), 'Since ' + fmtDate(s.boot_time)));
    grid.appendChild(stat('Operating system', s.os || '', [s.hostname, 'Kernel ' + (s.kernel || '') + ' · ' + (s.arch || '')]));
    var pythons = data.pythons || {};
    grid.appendChild(stat('Python', pythons.PBGui || '', Object.keys(pythons).filter(function (k) { return k !== 'PBGui'; }).map(function (k) { return k + ': ' + (pythons[k] || 'not configured'); })));
    (data.gpus || []).forEach(function (gpu, index) {
      grid.appendChild(stat('GPU ' + index, gpu.name, [
        gpu.memory_total_mb ? fmtBytes(gpu.memory_used_mb * 1048576) + ' / ' + fmtBytes(gpu.memory_total_mb * 1048576) + ' memory' : '',
        gpu.utilization != null ? Math.round(gpu.utilization) + '% busy' + (gpu.temperature != null ? ' · ' + Math.round(gpu.temperature) + ' °C' : '') : ''
      ], gpu.utilization));
    });
    (data.mounts || []).forEach(function (mount) {
      grid.appendChild(stat('Disk ' + mount.mount, Math.round(mount.percent) + '%', fmtBytes(mount.free) + ' free of ' + fmtBytes(mount.total), mount.percent));
    });
    var processes = data.processes || { items: [] };
    grid.appendChild(stat('PBGui processes', String(processes.count || 0), fmtBytes(processes.rss) + ' memory'));
    var body = $('process-table').tBodies[0];
    clear(body);
    processes.items.forEach(function (proc) {
      body.appendChild(el('tr', {}, [
        el('td', { text: proc.name }),
        el('td', { className: 'num mono', text: String(proc.pid) }),
        el('td', { className: 'num', text: fmtBytes(proc.rss) }),
        el('td', { className: 'num', text: (proc.cpu || 0).toFixed(1) + '%' })
      ]));
    });
  }

  // ── startup ──

  function onVisibility() {
    if (document.hidden) {
      Object.keys(state.timers).forEach(function (name) { clearTimeout(state.timers[name]); });
      return;
    }
    pollStatus(true);
    if (state.view === 'system') loadSystem();
    if (state.view === 'retention') loadSettings();
  }

  function init() {
    document.querySelectorAll('#sidebar .sb-section[data-view]').forEach(function (btn) {
      btn.addEventListener('click', function () { setView(btn.getAttribute('data-view'), ''); });
    });
    window.addEventListener('hashchange', function () {
      var target = parseHash();
      if (target.view !== state.view || target.category !== state.category) setView(target.view, target.category, { scroll: false });
    });
    document.addEventListener('visibilitychange', onVisibility);
    window.addEventListener('pagehide', function () {
      Object.keys(state.timers).forEach(function (name) { clearTimeout(state.timers[name]); });
    });
    setupItemSelection();
    setupPreview();
    setupSettings();
    var target = parseHash();
    setView(target.view, target.category, { scroll: false });
    pollStatus(true);
    if (window.PBGuiSidebarResize && typeof window.PBGuiSidebarResize.init === 'function') window.PBGuiSidebarResize.init();
  }

  window.PBGuiStorage = { state: state, fmtBytes: fmtBytes, parseHash: parseHash };
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
  else init();
})();
