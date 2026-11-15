/* Native PB9 config binding for the existing PB7/PB8 Backtest shell. */
;(function () {
  'use strict';
  var metadataPromise, metadata, renderer, source, jsonShown = {}, generation = 0;
  var paths = function () { return window.PBGuiEditorShared.configPath; };
  var copy = function(value) { return JSON.parse(JSON.stringify(value)); };
  var basic = {
    'cfg-ohlcv-dir': ['backtest', 'ohlcv_source_dir'],
    'cfg-start-date': ['backtest', 'start_date'], 'cfg-end-date': ['backtest', 'end_date'],
    'cfg-starting-balance': ['backtest', 'starting_balance'], 'cfg-native-strategy': ['bot', 'portfolio', 'strategy_kind']
  };
  var shown = {}, configRevision = null, configName = null, selectionRestored = false;

  function loadMetadata() {
    if (!metadataPromise) metadataPromise = window.apiFetch('/editor/metadata').then(function(value) {
      metadata = value; return value;
    }).catch(function(error) { metadataPromise = null; throw error; });
    return metadataPromise;
  }

  function resolve(config, field) {
    var cp = paths(), path = field.path, fallback;
    if (path[0] === 'bot' && ['long', 'short'].indexOf(path[1]) >= 0 && path[2] !== 'enabled') {
      var globalPath = ['bot', 'global'].concat(path.slice(2));
      fallback = cp.has(config, globalPath) ? cp.get(config, globalPath) : cp.get(metadata.prototype, globalPath);
    }
    if (fallback === undefined) fallback = cp.get(metadata.prototype, path);
    if (fallback === undefined) fallback = field.default;
    return {explicit: cp.has(config, path), value: cp.has(config, path) ? cp.get(config, path) : fallback, inherited: fallback};
  }

  function fields(config) {
    var kind = paths().get(config, ['bot', 'portfolio', 'strategy_kind']);
    var fixed = Object.keys(basic).map(function(id) { return basic[id].join('.'); }).concat(['backtest.base_dir', 'backtest.exchanges']);
    return metadata.fields.filter(function(field) {
      var path = field.path;
      return ['backtest', 'bot', 'logging'].indexOf(path[0]) >= 0 && fixed.indexOf(path.join('.')) < 0 &&
        !(path[0] === 'bot' && path[2] === 'strategy' && path[3] !== kind);
    });
  }

  function mount(config) {
    var groupDefs = [
      ['backtest', 'Backtest Settings', ['backtest']], ['portfolio', 'Portfolio', ['bot', 'portfolio']],
      ['global', 'Global Defaults', ['bot', 'global']], ['long', 'Long Overrides', ['bot', 'long']],
      ['short', 'Short Overrides', ['bot', 'short']], ['logging', 'Logging', ['logging']]
    ];
    if (!renderer) renderer = window.PBGuiEditorShared.createMetadataFieldRenderer({idPrefix:'bt9-field', resolve:resolve});
    var open = renderer.clear(), available = fields(config);
    var previousReport = document.getElementById('bt9-migration-report');
    if (previousReport) previousReport.remove();
    if (window._nativeMigrationReport && window.editingConfig === '__new__') {
      var report = document.createElement('details'); report.id = 'bt9-migration-report';
      var summary = document.createElement('summary'); summary.textContent = 'PB8 → PB9 migration report';
      var explanation = document.createElement('p'); explanation.textContent = 'Official best-effort conversion. Review inferred settings before saving or running the backtest.';
      var text = document.createElement('pre'); text.style.whiteSpace = 'pre-wrap'; text.style.overflowWrap = 'anywhere';
      text.textContent = JSON.stringify(window._nativeMigrationReport, null, 2);
      report.append(summary, explanation, text);
      document.getElementById('configs-editor').prepend(report);
    }
    var sections = groupDefs.map(function(definition) {
      var prefix = definition[2];
      return {id:definition[0], title:definition[1], expander:true, open:definition[0] === 'backtest', groups:[{title:'', fields:available.filter(function(field) {
        return prefix.every(function(part, index) { return field.path[index] === part; });
      }).map(function(field) { return {field:field, label:field.path.slice(prefix.length).join('.')}; })}]};
    });
    renderer.mount('bt9-native-fields', sections, open);
    renderer.populate(config);
  }

  function populate(config) {
    source = copy(config); window._cfgEditorCfg = source;
    Object.keys(basic).forEach(function(id) {
      var input = document.getElementById(id), value = paths().get(source, basic[id]);
      if (id === 'cfg-end-date' && value === 'now') {
        input.dataset.semanticValue = 'now'; value = new Date().toISOString().slice(0,10);
      } else if (id === 'cfg-end-date') delete input.dataset.semanticValue;
      input.value = value == null ? '' : String(value);
      shown[id] = input.value;
    });
    var exchanges=Array.from(new Set(((metadata.template.backtest || {}).exchanges || []).concat(source.backtest.exchanges || [])));
    window.cfgRebuildMs('ms-cfg-exchanges', exchanges, source.backtest.exchanges || []);
    ['long', 'short'].forEach(function(side) {
      var textarea = document.getElementById('cfg-bot-' + side);
      textarea.value = JSON.stringify((source.bot || {})[side] || {}, null, 2);
      jsonShown[side] = textarea.value;
      window.autoResizeTa(textarea);
      window._applyBotJsonHighlight(textarea, {});
    });
    mount(source);
  }

  async function render(name, config) {
    var mine = ++generation;
    try {
      await loadMetadata();
      if (mine !== generation || window.currentPanel !== 'configs') return;
      window.resetBacktestEditorUiState();
      window._extraBtKeys = [];
      document.getElementById('configs-list').style.display = 'none';
      document.getElementById('configs-toolbar').style.display = 'none';
      var host = document.getElementById('configs-editor'); host.style.display = '';
      host.innerHTML = '<div class="form-row cols-4">' +
        '<div class="form-group"><label for="cfg-name">Name</label><input id="cfg-name"></div>' +
        '<div class="form-group"><label for="cfg-start-date">Start date</label><input id="cfg-start-date" type="date"></div>' +
        '<div class="form-group"><label for="cfg-end-date">End date</label><input id="cfg-end-date" type="date"></div>' +
        '<div class="form-group"><label for="cfg-starting-balance">Starting balance</label><input id="cfg-starting-balance" type="number" min="0" step="any"></div></div>' +
        '<div class="form-row cols-2"><div class="form-group"><label>Exchanges</label><div class="ms-wrap" id="ms-cfg-exchanges"><div class="ms-tags"></div><input class="ms-input" placeholder="Select exchanges..."><div class="ms-dropdown"></div></div></div>' +
        '<div class="form-group"><label for="cfg-native-strategy">strategy_kind</label><select id="cfg-native-strategy"></select></div></div>' +
        '<div class="form-row cols-8">' + window.fieldOhlcvSource(config.backtest && config.backtest.ohlcv_source_dir) + '</div>' +
        '<div id="bt9-native-fields"></div><div class="form-row cols-2">' +
        '<div class="form-group"><label for="cfg-bot-long">Long overrides (JSON)</label><textarea id="cfg-bot-long" class="json-editor" rows="16"></textarea><div id="cfg-bot-long-status"></div></div>' +
        '<div class="form-group"><label for="cfg-bot-short">Short overrides (JSON)</label><textarea id="cfg-bot-short" class="json-editor" rows="16"></textarea><div id="cfg-bot-short-status"></div></div></div>' +
        '<div class="expander" id="exp-raw"><div class="expander-header"><span class="arrow">▶</span> Raw JSON</div><div class="expander-body"><div class="raw-json-wrap" id="cfg-raw-json-wrap"><div id="cfg-raw-json-status"></div><pre id="cfg-raw-json-highlight" class="raw-json-highlight" aria-hidden="true"></pre><textarea id="cfg-raw-json"></textarea></div></div></div>';
      document.getElementById('cfg-name').value = name;
      var select = document.getElementById('cfg-native-strategy');
      metadata.strategies.forEach(function(kind) { var option=document.createElement('option');option.value=kind;option.textContent=kind;select.appendChild(option); });
      configRevision = window._nativeConfigRevision || null; configName = name;
      if (window.editingConfig !== '__new__' && name) {
        var location = new URL(window.location.href); location.searchParams.set('config', name);
        location.searchParams.delete('panel');history.replaceState(null,'',location);
      }
      populate(config);
      var raw = document.getElementById('cfg-raw-json'); raw.value = JSON.stringify(source, null, 2);
      window._rawEditorLastApplied = raw.value;
      raw.addEventListener('input', window.scheduleRawJsonEditorSync);
      document.getElementById('exp-raw').querySelector('.expander-header').addEventListener('click', function() { window.toggleExpander('exp-raw'); });
      document.getElementById('cfg-end-date').addEventListener('change', function() { delete this.dataset.semanticValue; });
      select.addEventListener('change', function() {
        source = collect(); mount(source); window.scheduleStructuredEditorSync();
      });
      ['long', 'short'].forEach(function(side) {
        document.getElementById('cfg-bot-' + side).addEventListener('change', function() {
          if (validate()) { source = collect(); renderer.populate(source); }
        });
      });
      window.coinOvInit('unused-bt9-overrides', {apiBase:window.API_BASE, inlineOnly:true});
      window.showEditorSidebar();
      window.bindStructuredJsonFieldValidation(); window.bindStructuredEditorSync();
      window.configEditorGeneration += 1; window.configEditRevision = 0;
      window.setRawJsonValidationError(null);
    } catch (error) { window.toast('PB9 editor: ' + error.message, 'err'); }
  }

  function collect() {
    var raw = document.getElementById('cfg-raw-json'), config = JSON.parse(raw.value);
    Object.keys(basic).forEach(function(id) {
      var input = document.getElementById(id);
      var value = input.type === 'number' ? Number(input.value) : input.value;
      if (id === 'cfg-end-date') value = input.dataset.semanticValue || value;
      if (id === 'cfg-ohlcv-dir') value = value.trim() || null;
      // Raw JSON may already contain an earlier edit. Returning a visible field
      // to its original value must still replace that intermediate value.
      if (input.value !== shown[id] || paths().has(config, basic[id]) && paths().get(config, basic[id]) !== value) {
        paths().set(config, basic[id], value);
      }
    });
    var exchanges = window.cfgGetMs('ms-cfg-exchanges');
    config.backtest.exchanges = exchanges;
    renderer.apply(config);
    ['long', 'short'].forEach(function(side) {
      var input = document.getElementById('cfg-bot-' + side);
      if (input.value !== jsonShown[side]) {
        var result = window.PBGuiEditorShared.validateJsonText(input.value, {expectObject:true});
        if (!result.error) {
          config.bot[side] = result.parsed; jsonShown[side] = input.value; renderer.populate(config);
        }
      } else if (document.activeElement !== input) {
        input.value = JSON.stringify(config.bot[side] || {}, null, 2); jsonShown[side] = input.value;
      }
    });
    return config;
  }

  function validate() {
    var invalid = renderer && renderer.firstInvalid();
    if (invalid) {
      var section = invalid.control.closest('.expander'); if (section) section.classList.add('open');
      invalid.control.focus(); window.toast(invalid.key + ' ' + invalid.message, 'err'); return false;
    }
    for (var side of ['long', 'short']) {
      var input = document.getElementById('cfg-bot-' + side);
      var result = window.PBGuiEditorShared.validateJsonText(input.value, {expectObject:true});
      if (result.error) { input.focus(); window.toast(side + ' overrides: ' + result.error.message, 'err'); return false; }
    }
    var balance = document.getElementById('cfg-starting-balance');
    if (!balance.value || !Number.isFinite(Number(balance.value)) || Number(balance.value) <= 0) {
      balance.focus(); window.toast('Starting balance must be positive', 'err'); return false;
    }
    return true;
  }

  async function save(queue) {
    var beforeSourceLookup = generation;
    if (window._pbguiDataPathRequest) await window._pbguiDataPathRequest;
    if (beforeSourceLookup !== generation) return;
    if (!validate() || !window.ensureRawJsonValidForSave()) return;
    var name = document.getElementById('cfg-name').value.trim();
    if (!name) { window.toast('Config name is required', 'err'); return; }
    var config = collect(), revision = window.configEditRevision, editorGeneration = window.configEditorGeneration, mine = generation;
    var dateError = window.cfgValidateDateRanges(config);
    if (dateError) { window.toast(dateError, 'err'); return; }
    try {
      var saved = await window.apiFetch('/configs/' + encodeURIComponent(name), {method:'PUT', headers:{'Content-Type':'application/json'},body:JSON.stringify({config:config, expected_revision:name === configName ? configRevision : null})});
      if (editorGeneration === window.configEditorGeneration) { configRevision=saved.revision; configName=name; }
      if (queue) await window.apiFetch('/queue', {method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({name:name,config:config})});
      if (mine !== generation) return;
      window.loadConfigs();
      if (editorGeneration === window.configEditorGeneration && revision === window.configEditRevision) {
        if (queue) window.closeEditor(); else {window.editingConfig = name;window.showEditorSidebar();}
        var url = new URL(window.location.href); url.searchParams.delete('opt_draft_id'); url.searchParams.delete('draft_id');
        if (queue) {url.searchParams.delete('config');url.searchParams.set('panel','queue');} else {url.searchParams.set('config', name);url.searchParams.delete('panel');} history.replaceState(null,'',url);
      }
      if (queue) { window.selectPanel('queue'); window.wsRefresh(); }
      window.toast(queue ? 'Saved and queued PB9 backtest' : 'Saved PB9 backtest', 'ok');
    } catch (error) { window.toast('PB9 save: ' + error.message, 'err'); }
  }

  var viewKey = 'pbgui:backtest:v9:results-view';
  function readView() {
    try { return JSON.parse(sessionStorage.getItem(viewKey) || '{}'); } catch(error) { return {}; }
  }
  function preferredPrice(path) {
    var prices=readView().prices;
    var entry=Array.isArray(prices) && prices.find(function(item){return item.path===path;});
    return entry && typeof entry.value==='string' ? entry.value : '';
  }
  function rememberPrice(path, value) {
    var state=readView(), prices=Array.isArray(state.prices)?state.prices:[];
    state.prices=prices.filter(function(item){return item.path!==path;}).concat([{path:path,value:value}]).slice(-100);
    try {sessionStorage.setItem(viewKey,JSON.stringify(state));} catch(error) { /* Optional navigation state. */ }
  }
  function rememberResults() {
    if (window.currentPanel !== 'results') return;
    var rows=Array.from(document.querySelectorAll('#results-list tr[data-path]'));
    if (!rows.length) return;
    var state={search:document.getElementById('results-filter').value, actions:[], selected:Array.from(window._selectedResultPaths).slice(0,100), prices:readView().prices || [], version:window.selectedResultsVersion(), compare:readView().compare===true};
    rows.forEach(function(row) {
      row.querySelectorAll('button[data-action].active').forEach(function(button) {
        state.actions.push({path:row.dataset.path, action:button.dataset.action});
      });
    });
    state.actions=state.actions.slice(0,100);state.selected=state.selected.slice(0,100);
    try {sessionStorage.setItem(viewKey,JSON.stringify(state));} catch(error) { /* Storage is optional. */ }
  }
  function rememberComparison(selected, visible, version) {
    var state=readView();state.selected=selected.slice(0,100);state.compare=visible===true;
    state.search=document.getElementById('results-filter').value.slice(0,500);
    state.version=version || window.selectedResultsVersion();
    try {sessionStorage.setItem(viewKey,JSON.stringify(state));} catch(error) { /* Optional navigation state. */ }
  }
  function restoreComparison() {
    var state=readView(), area=document.getElementById('compare-chart-area');
    if (!state.compare || !area || document.getElementById('compare-chart-div')) return;
    var selected=window.getSelectedResults();
    if(selected.length>=2)window._compareResultPaths(selected,window.results,area,'compare-chart-div');
  }
  function restoreResults() {
    var state=readView(), changed=false;
    if(!selectionRestored) {
      (Array.isArray(state.selected)?state.selected:[]).forEach(function(path){window._selectedResultPaths.add(path);});
      selectionRestored=true;
    }
    document.querySelectorAll('#results-list tr[data-path]').forEach(function(row) {
      var selected=window._selectedResultPaths.has(row.dataset.path);
      row.classList.toggle('selected',selected);
      if (selected) window._selectedResultPaths.add(row.dataset.path);
      row.querySelectorAll('button[data-action]').forEach(function(button) {
        var enabled=Array.isArray(state.actions)&&state.actions.some(function(item){return item.path===row.dataset.path&&item.action===button.dataset.action;});
        if(enabled){button.classList.add('active');changed=true;}
      });
    });
    if(changed)window.onResultActionsChanged();
  }
  function restoreSearch() {
    var state=readView(), input=document.getElementById('results-filter');
    if(input&&typeof state.search==='string')input.value=state.search.slice(0,500);
    var version=document.getElementById('results-version-filter');
    if(version && ['v7','v8','v9','both'].indexOf(state.version)>=0)version.value=state.version;
  }
  window.PBGuiBacktestV9 = {rememberComparison:rememberComparison, restoreComparison:restoreComparison, preferredPrice:preferredPrice, rememberPrice:rememberPrice, rememberResults:rememberResults, restoreResults:restoreResults, restoreSearch:restoreSearch, active:function(){return !!(window.backtestEditorAdapter && window.backtestEditorAdapter.metadataDriven);}, render:render, collect:collect, populate:populate, validate:validate, save:save, close:function(){generation++;var url=new URL(window.location.href);['config','draft_id','opt_draft_id'].forEach(function(key){url.searchParams.delete(key);});history.replaceState(null,'',url);}};
}());
