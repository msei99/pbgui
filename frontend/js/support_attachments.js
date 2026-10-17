/* One shared selection model for preview, downloads and encrypted attachments. */
(function () {
  'use strict';
  var groups = [['browser', 'Browser diagnostics'], ['services', 'Services and jobs'], ['ai', 'AI Loops / chat'],
    ['backtests', 'Backtests'], ['optimize', 'Optimization'], ['run', 'Bot runtime'], ['market', 'Market / coin data'],
    ['vps', 'VPS / monitoring / cluster'], ['vast', 'Vast workers'], ['databases', 'Database health'],
    ['other', 'Other application data'], ['coverage', 'Collection details']];
  function group(source) {
    var name = String(source).toLowerCase();
    if (/vast/.test(name)) return 'vast';
    if (/loop|ai[\/_. -]|chat|research/.test(name)) return 'ai';
    if (/backtest|bt_v[78]/.test(name)) return 'backtests';
    if (/optimiz|opt_v[78]|pareto/.test(name)) return 'optimize';
    if (/vps|monitor|cluster|remote|ssh/.test(name)) return 'vps';
    if (/coin|ohlcv|market|preload/.test(name)) return 'market';
    if (/database|sqlite|db_sync|dbsync/.test(name)) return 'databases';
    if (/pbrun|run_v[78]|instance|bot[\/_. -]/.test(name)) return 'run';
    if (/pbgui|pbapi|service|task|job/.test(name)) return 'services';
    return 'other';
  }
  function defaultGroups(report) {
    var all = groups.map(function (pair) { return pair[0]; });
    var area = report && report.error_context && report.error_context.area;
    if (!area || group(area) === 'other') return all;
    var related = group(area), dependencies = {ai:['optimize','backtests','vast'], optimize:['backtests','market'],
      backtests:['market'], run:['vps','market']};
    return Array.from(new Set(['browser','services','coverage',related].concat(dependencies[related] || [])));
  }
  function create(onChange, options) {
    options = options || {};
    var full = null, selected = new Set(groups.map(function (pair) { return pair[0]; })), presetApplied = false, cache = null, cachedSize = null, pickers = [];
    try {
      var saved = JSON.parse(sessionStorage.getItem(options.storageKey) || 'null');
      // Legacy arrays cannot distinguish the old minimal default from user choices.
      if (saved && saved.version === 2 && Array.isArray(saved.groups)) { selected = new Set(saved.groups.filter(function (key) { return groups.some(function (pair) { return pair[0] === key; }); })); presetApplied = true; }
    } catch (_) {}
    function subset() {
      if (!full) return null;
      if (cache) return cache;
      var output = {};
      ['format', 'created_at', 'system', 'error_context'].forEach(function (key) { if (key in full) output[key] = full[key]; });
      if (selected.has('browser') && full.browser) output.browser = full.browser;
      ['records', 'logs'].forEach(function (key) {
        output[key] = {}; Object.entries(full[key] || {}).forEach(function (pair) { if (selected.has(group(pair[0]))) output[key][pair[0]] = pair[1]; });
      });
      output.databases = selected.has('databases') ? full.databases || [] : [];
      Object.keys(full).forEach(function (key) {
        if (!['format','created_at','system','error_context','browser','records','logs','databases','coverage'].includes(key) && selected.has('other')) output[key] = full[key];
      });
      var coverage = full.coverage || {};
      output.coverage = {limits: coverage.limits || {}, registered_routes: [], sources: [], omitted: [],
        attachment_selection: {groups: Array.from(selected), excluded_records: Object.keys(full.records || {}).length - Object.keys(output.records).length,
          excluded_logs: Object.keys(full.logs || {}).length - Object.keys(output.logs).length}};
      if (selected.has('coverage')) {
        output.coverage.registered_routes = coverage.registered_routes || [];
        output.coverage.data_areas = coverage.data_areas || [];
        output.coverage.excluded = coverage.excluded || [];
        ['sources', 'omitted'].forEach(function (key) { output.coverage[key] = (coverage[key] || []).filter(function (item) { return selected.has(group(item.source || '')); }); });
      }
      cache = output; return output;
    }
    function size() { if (!full) return 0; if (cachedSize === null) cachedSize = new Blob([JSON.stringify(subset(), null, 2)]).size; return cachedSize; }
    function stats() {
      var counts = Object.fromEntries(groups.map(function (pair) { return [pair[0], 0]; }));
      if (full) {
        ['records','logs'].forEach(function (key) { Object.keys(full[key] || {}).forEach(function (name) { counts[group(name)]++; }); });
        counts.browser += full.browser ? 1 : 0; counts.databases += (full.databases || []).length;
        counts.coverage = (full.coverage || {}).sources ? full.coverage.sources.length : 0;
      }
      return counts;
    }
    function render(root) {
      root.replaceChildren(); var counts = stats();
      var summary = document.createElement('p'); summary.className = 'support-attachment-size';
      summary.textContent = full ? 'Selected JSON: ' + Math.ceil(size() / 1024) + ' KiB · system/version' + (full.error_context ? ' and error context' : '') + ' always included' : 'Collecting attachment choices…'; root.appendChild(summary);
      var grid = document.createElement('div'); grid.className = 'support-attachment-grid'; root.appendChild(grid);
      groups.forEach(function (pair) {
        var button = document.createElement('button'); button.type = 'button'; button.textContent = pair[1] + ' (' + counts[pair[0]] + ')';
        button.dataset.attachmentGroup = pair[0]; button.setAttribute('aria-pressed', String(selected.has(pair[0]))); button.disabled = !full;
        button.addEventListener('click', function () { if (selected.has(pair[0])) selected.delete(pair[0]); else selected.add(pair[0]); changed(); }); grid.appendChild(button);
      });
    }
    function changed() { cache = null; cachedSize = null; if (full) { try { sessionStorage.setItem(options.storageKey, JSON.stringify({version:2, groups:Array.from(selected)})); } catch (_) {} } pickers.forEach(render); onChange(); }
    return {
      getData: subset, getSize: size,
      attach: function (root) { pickers.push(root); render(root); },
      setReport: function (report) {
        full = report;
        if (report && !presetApplied) {
          selected = new Set(defaultGroups(report));
          presetApplied = true;
        }
        changed();
      }
    };
  }
  window.PBGuiSupportAttachments = {create: create, group: group};
})();
