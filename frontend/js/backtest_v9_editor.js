/* Native PB9 config binding for the existing PB7/PB8 Backtest shell. */
;(function () {
  'use strict';
  var metadataPromise, metadata, renderer, source, jsonShown = {}, generation = 0;
  var paths = function () { return window.PBGuiEditorShared.configPath; };
  var copy = function(value) { return JSON.parse(JSON.stringify(value)); };
  var basic = {
    'cfg-ohlcv-dir': ['backtest', 'ohlcv_source_dir'],
    'cfg-start-date': ['backtest', 'start_date'], 'cfg-end-date': ['backtest', 'end_date'],
    'cfg-native-strategy': ['bot', 'portfolio', 'strategy_kind']
  };
  var shown = {}, configRevision = null, configName = null, selectionRestored = false;
  var jsonBaseline = {}, jsonMode = {}, coinShown = {}, coinAuthored = {}, filterShown = {}, filterDirty = new Set(), componentShown = {}, componentDirty = new Set();
  var editorViewKey = '', editorView = {}, editorListeners;
  var extraShown = {}, extraExplicit = new Set(), extraDirty = new Set();
  var saveInFlight = false, enqueueAttempt = null;

  function additionalKeys() {
    var known = new Set(['base_dir','exchanges','coin_sources','market_settings_sources','market_settings','scenarios','suite_enabled','reducer']);
    Object.keys(basic).forEach(function(id) {if (basic[id][0] === 'backtest') known.add(basic[id][1]);});
    var primary=primaryRows();
    Object.keys(primary).forEach(function(row) {primary[row].forEach(function(entry) {if (entry.field.path[0] === 'backtest') known.add(entry.field.path[1]);});});
    metadata.fields.forEach(function(field) {if (field.path[0] === 'backtest' && field.path.length > 2 && field.path.indexOf('*') < 0) known.add(field.path[1]);});
    return known;
  }

  function syncAdditional(config) {
    var cp=paths(), bt=cp.merge(cp.merge(cp.merge({},metadata.prototype.backtest),metadata.template.backtest),config.backtest);
    var meta=Object.create(null);
    metadata.fields.forEach(function(field) {
      if (field.path[0] !== 'backtest' || field.path.length !== 2) return;
      if (!Object.prototype.hasOwnProperty.call(bt,field.path[1])) Object.defineProperty(bt,field.path[1],{value:Object.prototype.hasOwnProperty.call(field,'default') ? copy(field.default) : null,enumerable:true,writable:true,configurable:true});
      var shared=field.path[1] === 'hlcvs_data_dir' ? {} : window.EXTRA_BT_META[field.path[1]] || {};
      var entry=Object.assign({},shared,{tip:field.description || shared.tip || ''}), choices=field.domain && field.domain.choices;
      if (Array.isArray(choices) && choices.length && choices.every(function(value){return typeof value === 'string';})) entry.options=choices;
      meta[field.path[1]]=entry;
    });
    var old=document.getElementById('exp-extra-bt'), open=old && old.classList.contains('open');
    var result=window.buildExtraBtExpanderHtml(bt,document.getElementById('cfg-name').value,{knownKeys:additionalKeys(),fieldMeta:meta,resultMetrics:false});
    document.getElementById('bt9-additional-fields').innerHTML=result.html;
    window._extraBtKeys=result.keys;
    extraShown=copy(window.collectExtraBtParams({},result.keys));
    extraExplicit=new Set(Object.keys(config.backtest || {}));extraDirty=new Set();
    if (open || editorView.open && editorView.open['exp-extra-bt']) document.getElementById('exp-extra-bt').classList.add('open');
    window.bindStructuredJsonFieldValidation();
  }

  function rememberEditorView() {
    var open = {};
    document.querySelectorAll('#configs-editor .expander[id]').forEach(function(element) {open[element.id]=element.classList.contains('open');});
    editorView = {open:open,views:{long:document.getElementById('cfg-bot-long-view').value,short:document.getElementById('cfg-bot-short-view').value}};
    try {sessionStorage.setItem(editorViewKey,JSON.stringify(editorView));} catch(error) { /* Navigation storage is optional. */ }
  }

  function item(key, id, label, step, span) {
    var path = key.split('.');
    var field = metadata.fields.find(function(value) { return value.path.join('.') === key; });
    if (!field) {
      var value = paths().get(metadata.prototype, path);
      if (value === undefined) value = paths().get(metadata.template, path);
      field = {path:path, types:[typeof value === 'boolean' ? 'boolean' : typeof value === 'number' || step ? 'float' : 'string'], nullable:value == null, default:value};
    }
    if (key === 'logging.level') field = Object.assign({},field,{domain:Object.assign({},field.domain,{choices:[0,1,2,3]})});
    return {field:field, id:id, label:label, step:step, span:span, showExplicit:false, hideUnit:true};
  }

  function primaryRows() {
    return {
      balance: [
        item('backtest.starting_balance','cfg-starting-balance','starting_balance',500),
        item('backtest.balance_sample_divider','cfg-balance-divider','balance_sample_divider',1),
        item('backtest.btc_collateral_target_ratio','cfg-btc-cap','btc_collateral_target_ratio',0.1),
        item('backtest.max_usd_debt_to_wallet_value_ratio','cfg-btc-ltv','max_usd_debt_to_wallet_value_ratio',0.1),
        item('bot.long.universe.minimum_coin_age_days','cfg-min-age-long','minimum_coin_age_days (Long)',1),
        item('bot.short.universe.minimum_coin_age_days','cfg-min-age-short','minimum_coin_age_days (Short)',1),
        item('backtest.equity_floor_fraction','cfg-liq-thresh','equity_floor_fraction',0.01),
        item('backtest.dynamic_wel_by_tradability','cfg-dyn-wel','dynamic_wel_by_tradability')
      ],
      fees: [
        item('backtest.maker_fee_override_fraction','cfg-maker-fee','maker_fee_override_fraction',0.00001,2),
        item('backtest.taker_fee_override_fraction','cfg-taker-fee','taker_fee_override_fraction',0.00001,2),
        item('backtest.market_order_slippage_fraction','cfg-slippage','market_order_slippage_fraction',0.0001),
        item('backtest.filter_by_min_effective_cost','cfg-filter-cost','filter_by_min_effective_cost'),
        item('bot.portfolio.hsl.scope','cfg-hsl-scope','hsl.scope'),
        item('logging.level','cfg-logging-level','logging_level')
      ],
      candles: [
        item('backtest.candle_interval_minutes','cfg-candle-interval','candle_interval_minutes',1),
        item('backtest.gap_tolerance_ohlcvs_minutes','cfg-gap-tol','gap_tolerance_ohlcvs_minutes',1),
        item('backtest.compress_cache','cfg-compress','compress_cache'),
        item('backtest.volume_normalization','cfg-vol-norm','volume_normalization')
      ],
      long: [
        item('bot.long.risk.total_wallet_exposure_limit_ratio','cfg-twe-long','total_wallet_exposure_limit_ratio',0.05),
        item('bot.long.risk.max_positions','cfg-npos-long','max_positions',1)
      ],
      short: [
        item('bot.short.risk.total_wallet_exposure_limit_ratio','cfg-twe-short','total_wallet_exposure_limit_ratio',0.05),
        item('bot.short.risk.max_positions','cfg-npos-short','max_positions',1)
      ]
    };
  }

  function sideProjection(config, side) {
    var cp = paths(), result = cp.merge({}, cp.get(metadata.prototype, ['bot',side]));
    cp.merge(result, cp.get(metadata.prototype, ['bot','global']));
    cp.merge(result, cp.get(config, ['bot','global']));
    cp.merge(result, cp.get(config, ['bot',side]));
    result.enabled = cp.get(config, ['bot',side,'enabled']) === true;
    ['approved_coins','ignored_coins'].forEach(function(key) {
      if (cp.get(result,['universe',key]) === null) cp.set(result,['universe',key],coinValue(config,side,key));
    });
    var kind = cp.get(config,['bot','portfolio','strategy_kind']);
    Object.keys(result.strategy || {}).forEach(function(key) { if (key !== kind) delete result.strategy[key]; });
    return result;
  }

  function renderSideJson(config) {
    ['long','short'].forEach(function(side) {
      var input = document.getElementById('cfg-bot-' + side);
      var mode = document.getElementById('cfg-bot-' + side + '-view').value;
      jsonMode[side] = mode;
      jsonBaseline[side] = copy(mode === 'overrides' ? config.bot[side] || {} : sideProjection(config,side));
      input.value = JSON.stringify(jsonBaseline[side],null,2); jsonShown[side] = input.value;
      window.autoResizeTa(input); window._applyBotJsonHighlight(input,{});
    });
  }

  function coinValue(config, side, key) {
    var value = resolve(config,{path:['bot',side,'universe',key]}).value;
    if (value === null) value = resolve(config,{path:['bot','global','universe',key]}).value;
    return value === 'all' ? ['all'] : Array.isArray(value) ? value.slice() : [];
  }

  function populateCoins(config, lookup) {
    var view = {live:{approved_coins:{},ignored_coins:{}},pbgui:config.pbgui || {}};
    ['long','short'].forEach(function(side) {
      ['approved_coins','ignored_coins'].forEach(function(key) {
        var id = 'ms-cfg-' + (key === 'approved_coins' ? 'app-' : 'ign-') + side;
        var values = coinValue(config,side,key); coinShown[id] = values;
        var nativeValue=paths().get(config,['bot',side,'universe',key]);
        coinAuthored[id] = nativeValue === 'all' || Array.isArray(nativeValue);
        view.live[key][side] = values;
        window.cfgRebuildMs(id,Array.from(new Set(window.cfgGetMs(id).concat(values, key === 'approved_coins' ? ['all'] : []))),values);
      });
    });
    if (lookup) window.loadCfgSymbols(view);
  }

  function allowedOverrides() {
    var allowed = {bot:{long:{},short:{}},live:{}};
    metadata.fields.forEach(function(field) {
      var permission = field.coinOverride || field.coin_override;
      if (!permission || /forbidden|unsupported|never|none/.test(permission) || field.path.indexOf('*') >= 0) return;
      var target = field.path[0] === 'live' ? allowed.live : field.path[0] === 'bot' ? allowed.bot[field.path[1]] : null;
      if (!target) return;
      var types = field.types || [], type = types.some(function(t){return t === 'object' || t === 'array';}) ? 'json' : types[0] === 'boolean' ? 'boolean' : types.some(function(t){return t === 'float' || t === 'integer';}) ? (field.nullable ? 'number_or_null' : 'number') : 'string';
      target[field.path.slice(field.path[0] === 'live' ? 1 : 2).join('.')] = {type:type,default:field.default};
    });
    return allowed;
  }

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
    var concrete = metadata.fields.filter(function(field) { return field.path.indexOf('*') < 0; });
    // Registry wildcards describe map/array members, never literal config keys.
    // Keep dynamic maps editable as one JSON value, like the existing array fields.
    metadata.fields.forEach(function(field) {
      var wildcard = field.path.indexOf('*');
      if (wildcard < 1) return;
      var prefix = field.path.slice(0, wildcard);
      if (concrete.some(function(parent) { return parent.path.join('.') === prefix.join('.'); })) return;
      concrete.push({path:prefix, types:['object'], nullable:false, description:field.description || ''});
    });
    // A new template/source field may precede its native registry entry.
    // Stop at registered JSON containers so dynamic map keys stay data.
    var known=new Set(concrete.map(function(field){return field.path.join('.');}));
    Object.keys(basic).forEach(function(id){known.add(basic[id].join('.'));});
    var primary=primaryRows();
    Object.keys(primary).forEach(function(row){primary[row].forEach(function(entry){known.add(entry.field.path.join('.'));});});
    ['backtest.coin_sources','backtest.market_settings_sources','backtest.market_settings.overrides','backtest.market_settings.overrides_by_exchange','backtest.scenarios','backtest.reducer'].forEach(function(key){known.add(key);});
    ['long','short'].forEach(function(side){['approved_coins','ignored_coins'].forEach(function(key){known.add('bot.'+side+'.universe.'+key);});});
    function discover(value,path) {
      var key=path.join('.');
      if (known.has(key)) return;
      var children=Array.from(known).some(function(child){return child.indexOf(key+'.') === 0;}) || path.length === 2 && path[0] === 'bot' && ['global','long','short','portfolio'].indexOf(path[1]) >= 0;
      if (value && typeof value === 'object' && !Array.isArray(value) && children) {
        Object.keys(value).forEach(function(part){if (['__proto__','constructor','prototype'].indexOf(part) < 0) discover(value[part],path.concat(part));});
      } else {
        var type=Array.isArray(value) ? 'array' : value !== null && typeof value === 'object' ? 'object' : typeof value === 'number' ? 'float' : typeof value;
        var inferred={path:path,types:value === null ? ['object','array','string','float','boolean'] : [type],nullable:value === null};
        var nativeDefault=paths().get(metadata.prototype,path);
        if (nativeDefault === undefined) nativeDefault=paths().get(metadata.template,path);
        if (nativeDefault !== undefined) inferred.default=copy(nativeDefault);
        concrete.push(inferred);known.add(key);
      }
    }
    ['backtest','bot','logging'].forEach(function(root) {
      var merged=paths().merge(paths().merge(paths().merge({},metadata.prototype[root]),metadata.template[root]),config[root]);
      Object.keys(merged).forEach(function(key){if (['__proto__','constructor','prototype'].indexOf(key) < 0) discover(merged[key],[root,key]);});
    });
    return concrete.filter(function(field) {
      var path = field.path;
      return ['backtest', 'bot', 'logging'].indexOf(path[0]) >= 0 && fixed.indexOf(path.join('.')) < 0 &&
        !(path[0] === 'bot' && path[2] === 'strategy' && path[3] !== kind);
    });
  }

  function mount(config) {
    var groupDefs = [
      ['backtest', 'Backtest Settings', ['backtest']], ['portfolio', 'Portfolio', ['bot', 'portfolio']],
      ['global', 'Global Defaults', ['bot', 'global']], ['long', 'Long Overrides', ['bot', 'long']],
      ['short', 'Short Overrides', ['bot', 'short']], ['logging', 'Logging', ['logging']],
      ['otherbot', 'Other Bot Parameters', ['bot']]
    ];
    if (!renderer) renderer = window.PBGuiEditorShared.createMetadataFieldRenderer({idPrefix:'bt9-field', resolve:resolve, steppers:true,choiceLabel:function(field,value) {return field.path.join('.') === 'logging.level' ? ['warning','info','debug','trace'][value] || String(value) : null;}});
    var open = Object.assign({},editorView.open || {},renderer.clear()), available = fields(config);
    var fixed = new Set();
    var primary = primaryRows();
    Object.keys(primary).forEach(function(key) {
      primary[key].forEach(function(entry) { fixed.add(entry.field.path.join('.')); });
      renderer.mount('bt9-' + key + '-fields',[{id:key,groups:[{columns:key === 'long' || key === 'short' ? 2 : key === 'candles' ? 4 : 8,fields:primary[key]}]}],open);
    });
    ['backtest.coin_sources','backtest.market_settings_sources','backtest.market_settings.overrides','backtest.market_settings.overrides_by_exchange','backtest.scenarios','backtest.suite_enabled','backtest.reducer'].forEach(function(key){fixed.add(key);});
    ['long','short'].forEach(function(side) { ['approved_coins','ignored_coins'].forEach(function(key){fixed.add('bot.'+side+'.universe.'+key);}); });
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
    var groups = groupDefs.map(function(definition) {
      var prefix = definition[2];
      return {title:definition[1], fields:available.filter(function(field) {
        return !(definition[0] === 'otherbot' && ['portfolio','global','long','short'].indexOf(field.path[1]) >= 0) && !(field.path[0] === 'backtest' && field.path.length === 2) && !fixed.has(field.path.join('.')) && prefix.every(function(part, index) { return field.path[index] === part; });
      }).map(function(field) { return {field:field, label:field.path.slice(prefix.length).join('.')}; })};
    }).filter(function(group){return group.fields.length > 0;});
    renderer.mount('bt9-native-fields', [{id:'advanced',title:'Advanced Settings',expander:true,open:false,groups:groups}], open);
    renderer.populate(config);
  }

  function populate(config) {
    source = copy(config); window._cfgEditorCfg = source;
    mount(source);
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
    renderSideJson(source);
    window.kvInit('kv-cs',source.backtest.coin_sources || {});
    window.kvInit('kv-mss',source.backtest.market_settings_sources || {});
    window.kvLoadCoins('kv-cs'); window.kvLoadCoins('kv-mss');
    window.marketSettingsLoad(source.backtest.market_settings || {}); window.marketSettingsRender();
    var pbgui = source.pbgui || {};
    [['cfg-market-cap','market_cap',0],['cfg-vol-mcap','vol_mcap',10]].forEach(function(definition) { document.getElementById(definition[0]).value=pbgui[definition[1]] == null ? definition[2] : pbgui[definition[1]]; });
    ['only_cpt','notices_ignore'].forEach(function(key) { document.getElementById('cfg-'+key.replace(/_/g,'-')).checked = pbgui[key] === true; });
    window.cfgRebuildMs('ms-cfg-tags',pbgui.tags || [],pbgui.tags || []);
    filterShown = filters(); filterDirty = new Set(); populateCoins(source,true);
    window.coinOvLoad(source); window.suiteLoad(source,{preserveEdit:false});
    componentShown = components(); componentDirty = new Set();
    syncAdditional(source);
  }

  function editorHtml(name, config) {
    var html = '<div class="form-row cols-8"><div class="form-group span-4"><label>exchanges</label><div class="ms-wrap" id="ms-cfg-exchanges"><input class="ms-input" id="ms-cfg-exchanges-input" placeholder="Select exchanges..." autocomplete="off"><div class="ms-dropdown" id="ms-cfg-exchanges-dd"></div></div></div>';
    html += window.fieldText2('cfg-name','config_name',name,true,null,'',2);
    html += window.fieldBacktestStartDate('cfg-start-date','start_date','', 'Find the earliest or common start date for selected coins using native PB9 rules.',1) + window.fieldDate2('cfg-end-date','end_date','',null,1) + '</div>';
    html += '<div id="bt9-balance-fields"></div><div id="bt9-fees-fields"></div>';
    html += '<div class="form-row cols-8">' + window.fieldOhlcvSource(config.backtest && config.backtest.ohlcv_source_dir) + '<div class="span-4" id="bt9-candles-fields"></div></div>';
    html += '<div id="bt9-native-fields"></div>';
    html += window.buildBacktestSourcesHtml(config.backtest || {},Array.from(new Set((metadata.template.backtest.exchanges || []).concat(config.backtest.exchanges || []))));
    html += window.buildBacktestCoinFiltersHtml(config.pbgui || {});
    html += '<div id="coin-overrides-container"></div><div id="suite-container"></div>';
    html += '<div class="section-title">Bot Configuration</div><div class="form-row cols-8"><div class="form-group span-2" style="grid-column:-3 / -1"><label for="cfg-native-strategy">strategy_kind</label><select id="cfg-native-strategy"></select></div></div>';
    html += '<div class="form-row cols-2">';
    ['long','short'].forEach(function(side) {
      html += '<div><div style="font-weight:600;font-size:var(--fs-sm);margin-bottom:var(--sp-sm);color:var(--' + (side === 'long' ? 'green' : 'red') + ')">' + (side === 'long' ? 'Long' : 'Short') + '</div><div id="bt9-' + side + '-fields"></div>';
      html += '<div class="form-group"><label for="cfg-bot-' + side + '">Full Config JSON <select id="cfg-bot-' + side + '-view" aria-label="' + side + ' JSON view" style="width:auto;height:22px;min-height:22px;margin-left:auto"><option value="effective">Full Config JSON</option><option value="overrides">Overrides only</option></select></label><textarea id="cfg-bot-' + side + '" class="json-editor" rows="16"></textarea><div id="cfg-bot-' + side + '-status"></div></div></div>';
    });
    html += '</div><div id="bt9-additional-fields"></div><div class="expander" id="exp-raw"><div class="expander-header"><span class="arrow">▶</span> Raw JSON</div><div class="expander-body"><div class="raw-json-wrap" id="cfg-raw-json-wrap"><div id="cfg-raw-json-status"></div><pre id="cfg-raw-json-highlight" class="raw-json-highlight" aria-hidden="true"></pre><textarea id="cfg-raw-json"></textarea></div></div></div>';
    return html;
  }

  function filters() {
    return {market_cap:Number(document.getElementById('cfg-market-cap').value),vol_mcap:Number(document.getElementById('cfg-vol-mcap').value),tags:window.cfgGetMs('ms-cfg-tags'),only_cpt:document.getElementById('cfg-only-cpt').checked,notices_ignore:document.getElementById('cfg-notices-ignore').checked};
  }

  function components() {
    return {coin_sources:window.kvCollect('kv-cs'),market_settings_sources:window.kvCollect('kv-mss'),market_settings:window.marketSettingsCollect(),coin_overrides:window.coinOvCollect().coin_overrides || {},suite:window.suiteCollect()};
  }

  async function render(name, config) {
    var mine = ++generation;
    try {
      await loadMetadata();
      if (mine !== generation || window.currentPanel !== 'configs') return;
      var navigation = new URL(window.location.href);
      editorViewKey='pbgui:backtest:v9:editor:'+(navigation.searchParams.get('config') || navigation.searchParams.get('opt_draft_id') || navigation.searchParams.get('draft_id') || name || 'new');
      try {editorView=JSON.parse(sessionStorage.getItem(editorViewKey) || '{}') || {};} catch(error) {editorView={};}
      if (editorListeners) editorListeners.abort();
      editorListeners = new AbortController();
      if (renderer) renderer.clear();
      window.resetBacktestEditorUiState();
      window._extraBtKeys = [];
      document.getElementById('configs-list').style.display = 'none';
      document.getElementById('configs-toolbar').style.display = 'none';
      var host = document.getElementById('configs-editor'); host.style.display = '';
      host.innerHTML = editorHtml(name,config);
      ['long','short'].forEach(function(side) {document.getElementById('cfg-bot-'+side+'-view').value=editorView.views && editorView.views[side] === 'overrides' ? 'overrides' : 'effective';});
      document.getElementById('cfg-name').value = name;
      var select = document.getElementById('cfg-native-strategy');
      metadata.strategies.forEach(function(kind) { var option=document.createElement('option');option.value=kind;option.textContent=kind;select.appendChild(option); });
      configRevision = window._nativeConfigRevision || null; configName = name;
      if (window.editingConfig !== '__new__' && name) {
        var location = new URL(window.location.href); location.searchParams.set('config', name);
        location.searchParams.delete('panel');history.replaceState(null,'',location);
      }
      window.coinOvInit('coin-overrides-container',{apiBase:window.API_BASE,inlineOnly:true,preserveMarketIdentifiers:true,runtimeLabel:'PB9',allowedParams:allowedOverrides()});
      window.coinOvSetConfigName(name);
      var params = metadata.fields.filter(function(field){return field.path[0] === 'bot' && field.path[1] === 'long' && field.path.indexOf('*') < 0;}).map(function(field){return {key:field.path.slice(2).join('.'),type:field.types[0]};});
      var optimize=metadata.template.optimize || {};
      var metrics=Array.from(new Set((optimize.scoring || []).concat(optimize.limits || []).map(function(entry){return entry.metric;}).filter(function(metric){return typeof metric === 'string';})));
      window.suiteInit('suite-container',{apiBase:window.API_BASE,version:'v9',exchanges:metadata.template.backtest.exchanges,botParams:params,aggregateMetrics:metrics});
      populate(config);
      Object.keys(editorView.open || {}).forEach(function(id) {var element=document.getElementById(id);if(element && host.contains(element)) element.classList.toggle('open',editorView.open[id] === true);});
      host.addEventListener('click',function(event) {if(event.target.closest('.expander-header')) rememberEditorView();},{signal:editorListeners.signal});
      var raw = document.getElementById('cfg-raw-json'); raw.value = JSON.stringify(source, null, 2);
      window._rawEditorLastApplied = raw.value;
      raw.addEventListener('input', window.scheduleRawJsonEditorSync);
      document.getElementById('exp-raw').querySelector('.expander-header').addEventListener('click', function() { window.toggleExpander('exp-raw'); });
      document.getElementById('cfg-end-date').addEventListener('change', function() { delete this.dataset.semanticValue; });
      document.getElementById('cfg-end-date').addEventListener('input', function() { delete this.dataset.semanticValue; });
      document.getElementById('cfg-name').addEventListener('input',window.syncManagedBacktestBaseDirField);
      select.addEventListener('change', function() {
        source = collect(); mount(source); renderSideJson(source); window.scheduleStructuredEditorSync();
      });
      ['long', 'short'].forEach(function(side) {
        document.getElementById('cfg-bot-' + side).addEventListener('change', function() {
          if (validate()) { source = collect(); renderer.populate(source); }
        });
        var view = document.getElementById('cfg-bot-' + side + '-view');
        view.dataset.previous = view.value;
        view.addEventListener('change',function() {
          if (!validate()) {view.value=view.dataset.previous;return;}
          source=collect();view.dataset.previous=view.value;renderSideJson(source);rememberEditorView();
        });
      });
      window.cfgWireDateRange('cfg-start-date','cfg-end-date');
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
      if (id === 'cfg-end-date') value = input.value === shown[id] && input.dataset.semanticValue || value;
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
    var sideChanged = false;
    ['long', 'short'].forEach(function(side) {
      var input = document.getElementById('cfg-bot-' + side);
      if (input.value !== jsonShown[side]) {
        var result = window.PBGuiEditorShared.validateJsonText(input.value, {expectObject:true});
        if (!result.error) {
          if (jsonMode[side] === 'overrides') config.bot[side] = result.parsed;
          else paths().applyDelta(config,jsonBaseline[side],result.parsed,['bot',side]);
          sideChanged = true;
        }
      }
    });
    if (sideChanged) {renderer.populate(config); populateCoins(config,false);}
    ['long','short'].forEach(function(side) {
      ['approved_coins','ignored_coins'].forEach(function(key) {
        var id='ms-cfg-'+(key === 'approved_coins' ? 'app-' : 'ign-')+side, current=window.cfgGetMs(id);
        if (JSON.stringify(current.slice().sort()) !== JSON.stringify(coinShown[id].slice().sort())) coinAuthored[id]=true;
        if (coinAuthored[id]) paths().set(config,['bot',side,'universe',key],key === 'approved_coins' && current.indexOf('all') >= 0 ? 'all' : current.filter(function(coin){return coin !== 'all';}));
      });
    });
    var currentFilters=filters();
    Object.keys(currentFilters).forEach(function(key) {
      if (JSON.stringify(currentFilters[key]) !== JSON.stringify(filterShown[key])) filterDirty.add(key);
      if (filterDirty.has(key)) {config.pbgui=config.pbgui || {};config.pbgui[key]=currentFilters[key];}
    });
    var currentComponents=components();
    Object.keys(currentComponents).forEach(function(key) {
      if (JSON.stringify(currentComponents[key]) !== JSON.stringify(componentShown[key])) componentDirty.add(key);
      if (!componentDirty.has(key)) return;
      if (key === 'coin_overrides') config.coin_overrides=currentComponents[key];
      else if (key === 'suite') {
        var suite=currentComponents.suite;config.backtest.suite_enabled=suite.suite_enabled;
        if (suite.suite_enabled) {config.backtest.scenarios=suite.scenarios;config.backtest.reducer=suite.aggregate;}
      } else if (key === 'market_settings') {
        var settings=Object.assign({},config.backtest.market_settings || {});
        ['overrides','overrides_by_exchange'].forEach(function(name){settings[name]=currentComponents.market_settings[name];});
        config.backtest.market_settings=settings;
      } else config.backtest[key]=currentComponents[key];
    });
    var extras=window.collectExtraBtParams({},window._extraBtKeys);
    Object.keys(extras).forEach(function(key) {
      if (JSON.stringify(extras[key]) !== JSON.stringify(extraShown[key])) extraDirty.add(key);
      if (extraExplicit.has(key) || extraDirty.has(key)) Object.defineProperty(config.backtest,key,{value:extras[key],enumerable:true,writable:true,configurable:true});
    });
    ['long','short'].forEach(function(side) {
      var input=document.getElementById('cfg-bot-'+side);
      if (sideChanged || document.activeElement !== input) {
        var value=jsonMode[side] === 'overrides' ? config.bot[side] || {} : sideProjection(config,side);
        jsonBaseline[side]=copy(value);input.value=JSON.stringify(value,null,2);jsonShown[side]=input.value;
      }
    });
    return config;
  }

  function validate() {
    for (var key of window._extraBtKeys || []) {
      var extra=document.getElementById('extra-bt-'+key), type=extra && extra.dataset.extraBtType;
      var error=type === 'json' ? window.PBGuiEditorShared.validateJsonText(extra.value,{expectObject:false}).error : type === 'number' && (!extra.value.trim() || !Number.isFinite(Number(extra.value))) ? {message:'must be a finite number'} : null;
      if (error) {document.getElementById('exp-extra-bt').classList.add('open');extra.focus();window.toast(key+': '+error.message,'err');return false;}
    }
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
    var marketError = window.marketSettingsValidate();
    if (marketError) {window.toast(marketError,'err');return false;}
    return true;
  }

  async function save(queue) {
    if (saveInFlight) return;
    saveInFlight=true;
    try { await saveOnce(queue); } finally {saveInFlight=false;}
  }

  async function saveOnce(queue) {
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
      if (queue) {
        var fingerprint=JSON.stringify({name:name,config:config});
        if (!enqueueAttempt || enqueueAttempt.fingerprint !== fingerprint) enqueueAttempt={fingerprint:fingerprint,id:'enqueue-'+Array.from(crypto.getRandomValues(new Uint32Array(4))).map(function(value){return value.toString(16);}).join('-')};
        await window.apiFetch('/queue', {method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({name:name,config:config,operation_id:enqueueAttempt.id})});
        enqueueAttempt=null;
      }
      if (mine !== generation) return;
      window.loadConfigs();
      if (editorGeneration === window.configEditorGeneration && revision === window.configEditRevision) {
        if (queue) window.closeEditor(); else {window.editingConfig = name;window.showEditorSidebar();editorViewKey='pbgui:backtest:v9:editor:'+name;rememberEditorView();}
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
  async function balanceConfig(config) {
    await loadMetadata();
    return window.PBGuiEditorShared.nativeV9BalanceConfig(config,function(path) {return resolve(config,{path:path}).value;});
  }
  window.PBGuiBacktestV9 = {balanceConfig:balanceConfig,rememberComparison:rememberComparison, restoreComparison:restoreComparison, preferredPrice:preferredPrice, rememberPrice:rememberPrice, rememberResults:rememberResults, restoreResults:restoreResults, restoreSearch:restoreSearch, active:function(){return !!(window.backtestEditorAdapter && window.backtestEditorAdapter.metadataDriven);}, render:render, collect:collect, populate:populate, validate:validate, save:save, close:function(){generation++;if(editorListeners)editorListeners.abort();var url=new URL(window.location.href);['config','draft_id','opt_draft_id'].forEach(function(key){url.searchParams.delete(key);});history.replaceState(null,'',url);}};
}());
