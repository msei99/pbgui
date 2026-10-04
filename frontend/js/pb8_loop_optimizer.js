/* Autonomous PB8 loops; mounted only by the PB8 optimizer adapter. */
(function () {
  'use strict';
  var root, timer, generation = 0, optionsGeneration = 0, rows = [], selected = '', options = {}, hoursEdited = false;
  var base = '/api/optimize-v8/loops';
  var watchGeneration = 0, stopped = false, listSignature = '', detailSignature = '', renderedLoop = '';
  var expandedEvidence = new Map();
  var starting = false, formEdited = false, setupGeneration = 0, initialSetupRestored = false, optionsReady;
  var jevEnabled=true;
  var uiError = '', uiErrorPanel = 'loops-config', definitions = [], editingName = '', editingRevision = null, editingQueue = '';
  var editorSource = null, sourceDefaults = null, sourceDigest = null, editorVisible = false, editorLoading = false, editorGeneration = 0, queueing = false;
  var selectedDefinitions = new Set(), selectedRuns = new Set(), expanded = new Set();
  var logGeneration=0, logOpening=false, logRun='', logOperation='';
  var workflow = savedWorkflow();
  var knowledgeEntries=[], knowledgeLoaded=false, knowledgeError='', knowledgeSignature='';
  function savedWorkflow() { try { return JSON.parse(localStorage.getItem('pbgui.pb8.loops.workflow') || '{}') || {}; } catch (_) { return {}; } }
  function persistWorkflow() {
    workflow.selected = selected; workflow.config_selection = Array.from(selectedDefinitions); workflow.run_selection = Array.from(selectedRuns); workflow.expanded = Array.from(expanded);
    try { localStorage.setItem('pbgui.pb8.loops.workflow', JSON.stringify(workflow)); } catch (_) {}
  }
  function isLoopPanel() { return window.state && ['loops-config','loops-queue','loops-results','loops-knowledge','loops-instructions'].includes(state.panel); }
  function saveDraft() {
    if (!editorVisible) return;
    var settings = body(); delete settings.authorization;
    workflow.draft = {name:byId('name').value,settings:settings,source:editorSource,defaults:sourceDefaults,digest:sourceDigest,editing_name:editingName,revision:editingRevision,queue_id:editingQueue}; persistWorkflow();
  }

  function savedNavigation() {
    try { var value = JSON.parse(localStorage.getItem('pbgui.pb8.loops.setup') || '{}'); return value && typeof value === 'object' ? value : {}; }
    catch (_) { return {}; }
  }
  function saveNavigation() {
    try { localStorage.setItem('pbgui.pb8.loops.setup', JSON.stringify({config_name: byId('config').value, execution: byId('execution').value})); }
    catch (_) {}
  }
  function executionAvailable() {
    var input = byId('execution'), option = Array.from(input.options).find(function (item) { return item.value === input.value; });
    return Boolean(option && !option.disabled);
  }
  function syncStart() { byId('start').disabled = starting || editorLoading || !byId('config').value || !executionAvailable(); if (byId('save')) byId('save').disabled = starting || editorLoading || !byId('config').value; }
  function syncDirection() { var both = byId('direction').value === 'both'; byId('long').disabled = byId('short').disabled = !both; byId('position-split').hidden = !both; }
  async function loadSettings(row) {
    var token = ++setupGeneration;
    await optionsReady;
    if (token !== setupGeneration) return false;
    var settings = row.settings, goals = settings.goals;jevEnabled=settings.jev_enabled!==false;
    if (!options.configs.includes(settings.config_name)) { var option = element('option',settings.config_name + ' · snapshot'); option.value=settings.config_name; byId('config').appendChild(option); }
    byId('config').value = settings.config_name;
    await loadOptions();
    if (token !== setupGeneration) return false;
    byId('execution').value = settings.execution;
    byId('direction').value = goals.direction || 'config';
    byId('coins').value = (goals.coins || []).join(', ');
    byId('positions').value = goals.positions == null ? '' : goals.positions;
    ['long','short'].forEach(function (side) { byId(side).value = (goals.positions_by_side || {})[side] == null ? '' : goals.positions_by_side[side]; });
    byId('scenario').checked = Boolean(settings.scenario_enabled);
    byId('strategy').checked = Boolean(settings.strategy_enabled);
    ['gain','drawdown','uptrend','consistency','trades','robustness'].forEach(function (goal) {
      byId(goal).checked = (goals.presets || []).includes(goal);
      byId(goal + '-target').value = (goals.targets || {})[goal] == null ? '' : goals.targets[goal];
      byId(goal + '-target').disabled = !byId(goal).checked;
    });
    byId('goals').value = goals.text || '';
    [['runs','max_runs'],['hours','hours'],['parallel','parallel'],['patience','patience'],['validations','max_validations'],['candidates','candidates']].forEach(function (pair) { byId(pair[0]).value = settings[pair[1]]; });
    byId('run-mode').value = settings.run_limit_mode || 'config';
    ['iters','proxy','hours'].forEach(function (name) { if (settings['run_' + name] != null) byId('run-' + name).value = settings['run_' + name]; });
    hoursEdited = true;
    executionSummary();
    var shortened = Number(byId('hours').value) > Number(byId('hours').max);
    if (shortened) byId('hours').value = byId('hours').max;
    if (settings.execution === 'vast') byId('parallel').value = Math.min(settings.parallel, options.vast.max_rentals);
    if (byId('run-mode').value === 'hours' && Number(byId('run-hours').value) > Number(byId('hours').value)) byId('run-hours').value = byId('hours').value;
    syncDirection(); syncRunLimit(); showConfigValues(); syncStart(); saveNavigation();
    formEdited = false; uiError = '';
    byId('setup-source').textContent = 'Source: ' + settings.config_name + (shortened ? ' · duration reduced to current rental limit' : '');
    if (!executionAvailable()) uiError='The saved execution target is unavailable. Connect Vast.ai or choose an available execution target before queueing.';
    return true;
  }
  function defaultSettings(name, execution) {
    return {config_name:name,execution:execution || 'cpu',scenario_enabled:false,strategy_enabled:false,goals:{coins:[],direction:'config',positions:null,positions_by_side:{},presets:['gain','drawdown','uptrend'],targets:{},text:''},run_limit_mode:'config',run_iters:null,run_proxy:null,run_hours:null,max_runs:10,hours:options.vast ? options.vast.hours : 24,parallel:1,max_validations:30,candidates:3,patience:3};
  }
  function editorLocation() {
    var url=new URL(location.href); if(editorVisible)url.searchParams.set('loop_edit',byId('name').value || 'new');else url.searchParams.delete('loop_edit');history.replaceState(null,'',url);
  }
  async function openDefinition(row, duplicate) {
    var token=++editorGeneration, navigation=state.navigationSeq;editorLoading=true;editorVisible=false;syncContext();syncStart();
    try {
    closeReport(); editorSource={kind:'definition',id:row.name}; sourceDefaults=row.config_defaults;sourceDigest=row.bundle_digest || null;
    editingName=duplicate?'':row.name; editingRevision=duplicate?null:row.revision; editingQueue='';
    byId('name').value=duplicate?row.name+'_copy':row.name;
    var loaded=await loadSettings({settings:row.settings});if(!loaded||token!==editorGeneration||navigation!==state.navigationSeq)return;showEditor();saveDraft();
    } finally {if(token===editorGeneration){editorLoading=false;syncStart();}}
  }
  function showEditor(activate) {
    editorVisible=true; byId('form').hidden=false; byId('config-list').hidden=true;
    byId('config').disabled=false;if(activate!==false)selectPanel('loops-config'); editorLocation(); renderWorkflow();
  }
  async function newDefinition() {
    var token=++editorGeneration,navigation=state.navigationSeq;editorLoading=true;editorVisible=false;syncContext();syncStart();
    try {
    await optionsReady;closeReport();editingName='';editingRevision=null;editingQueue='';editorSource=null;sourceDefaults=null;sourceDigest=null;if(!options.configs.includes(byId('config').value))byId('config').value=options.configs[0]||'';await loadOptions();if(token!==editorGeneration||navigation!==state.navigationSeq)return;
    byId('name').value='';var loaded=await loadSettings({settings:defaultSettings(byId('config').value || options.config_name)});if(!loaded||token!==editorGeneration||navigation!==state.navigationSeq)return;showEditor();saveDraft();byId('name').focus();
    } finally {if(token===editorGeneration){editorLoading=false;syncStart();}}
  }
  async function createFromSource(source) {
    var token=++editorGeneration,navigation=state.navigationSeq;editorLoading=true;editorVisible=false;syncContext();syncStart();
    try {
    await optionsReady;var draft=await request('/sources',source);if(token!==editorGeneration||navigation!==state.navigationSeq)return;
    closeReport();editorSource=source;sourceDefaults=draft.config_defaults;sourceDigest=draft.bundle_digest || null;editingName='';editingRevision=null;editingQueue='';
    byId('name').value=draft.config_name+'_ai_loop';var values=draft.settings || defaultSettings(draft.config_name,draft.execution);if(!draft.settings)values.hours='';var loaded=await loadSettings({settings:values});if(!loaded||token!==editorGeneration||navigation!==state.navigationSeq)return;showEditor();saveDraft();byId('name').focus();return true;
    } finally {if(token===editorGeneration){editorLoading=false;syncStart();}}
  }
  async function editRun(row) {
    if(!await createFromSource({kind:'run',id:row.id}))return;
    var definition=definitions.find(function(item){return item.name===row.definition_name;});
    byId('name').value=row.name || row.settings.config_name;
    editingName=definition?definition.name:(row.name||row.settings.config_name); editingRevision=definition?definition.revision:null;
    editingQueue=row.status==='queued'?row.id:''; saveDraft();editorLocation();
  }
  async function queueDefinitions(names) {
    if(queueing)return;queueing=true;var navigation=state.navigationSeq,selection=selected,created=[];syncContext();
    try {
    if(!window.PBGuiAI || !PBGuiAI.ensureSelection)throw new Error('PBGui AI is still loading. Try again shortly.');
    var ai=await PBGuiAI.ensureSelection();
    for(var name of names) { var row=await request('/configs/'+encodeURIComponent(name)+'/queue',Object.assign({authorization:true},ai)); created.push(row); }
    if(navigation===state.navigationSeq && selection===selected && created.length){selected=created[created.length-1].id;selectedRuns=new Set(created.map(function(row){expanded.add(row.id);return row.id;}));persistWorkflow();var url=new URL(location.href);url.searchParams.set('loop_id',selected);history.replaceState(null,'',url);selectPanel('loops-queue');}await poll();
    } finally{queueing=false;syncContext();}
  }
  async function saveDefinition(andQueue) {
    if(starting)return; starting=true;syncStart();uiError='';
    try {
      if(!byId('form').reportValidity())return;
      var name=byId('name').value.trim(), settings=body();delete settings.authorization;
      var saved=await request('/configs/'+encodeURIComponent(name),{settings:settings,source:editorSource || {kind:'config',id:settings.config_name},revision:name===editingName?editingRevision:null,queue_id:name===editingName && editingQueue?editingQueue:null,source_digest:sourceDigest},'PUT');
      definitions=[saved].concat(definitions.filter(function(row){return row.name!==saved.name;}));
      editingName=saved.name;editingRevision=saved.revision;editorSource={kind:'definition',id:saved.name};sourceDefaults=saved.config_defaults;sourceDigest=saved.bundle_digest || null;
      selectedDefinitions=new Set([saved.name]); formEdited=false;editorVisible=false;workflow.draft=null;persistWorkflow();editorLocation();
      if(andQueue)await queueDefinitions([saved.name]);else{byId('form').hidden=true;byId('config-list').hidden=false;renderWorkflow();}
    } catch(exc){error(exc);} finally{starting=false;syncStart();renderWorkflow();}
  }
  async function deleteDefinitions() {
    var names=Array.from(selectedDefinitions);if(!names.length)return;
    var accepted=await PBGuiDialogs.confirm({title:'Delete loop configurations',message:'Delete '+names.length+' selected configuration(s)? Run history will be retained.',confirmText:'Delete',danger:true});if(!accepted)return;
    for(var name of names)await request('/configs/'+encodeURIComponent(name),null,'DELETE');selectedDefinitions.clear();await poll();
  }
  function forgetRun(id) {
    if(logRun===id || new URL(location.href).searchParams.get('loop_log_run')===id){clearNativeLog();if(typeof closeLogPanel==='function')closeLogPanel();}
    selectedRuns.delete(id);
    expanded.forEach(function(key){if(key===id || key.startsWith(id+':'))expanded.delete(key);});
    if(workflow.focus===id || String(workflow.focus||'').startsWith(id+':'))workflow.focus='';
    if(selected===id){closeReport();selected='';var url=new URL(location.href);if(url.searchParams.get('loop_id')===id){url.searchParams.delete('loop_id');history.replaceState(null,'',url);}}
    persistWorkflow();
  }
  async function deleteRuns(kind) {
    var targets=rows.filter(function(row){return selectedRuns.has(row.id) && (kind==='results'?row.ended:!row.ended);});
    if(!targets.length || targets.some(function(row){return !row.deletable;}))return;
    var accepted=await PBGuiDialogs.confirm({title:'Delete loop runs',message:'Delete '+targets.length+' selected loop run(s) and their round history? Saved configurations and learned findings will be retained.',confirmText:'Delete',danger:true});
    if(!accepted)return;
    try {
      for(var row of targets){await request('/'+row.id,null,'DELETE');generation++;rows=rows.filter(function(item){return item.id!==row.id;});forgetRun(row.id);render();}
      uiError='';
    } finally {await poll();}
  }
  function byId(id) { return document.getElementById('loop-' + id); }
  function element(tag, text, cls) { var node = document.createElement(tag); if (text !== undefined) node.textContent = text; if (cls) node.className = cls; return node; }
  async function request(path, body, method) {
    var response = await fetch(base + path, {credentials: 'same-origin', cache: 'no-store', headers: body ? {'Content-Type': 'application/json'} : {}, method: method || (body ? 'POST' : 'GET'), body: body ? JSON.stringify(body) : undefined});
    var data = await response.json();
    if (!response.ok) { if (response.status === 401) { clearTimeout(timer); generation++; } throw new Error(typeof data.detail === 'string' ? data.detail : Array.isArray(data.detail) ? data.detail.map(function(item){return item.msg;}).join('; ') : 'Loop request failed'); }
    return data;
  }
  function showMessages(){[byId('message'),document.getElementById('loops-queue-message'),document.getElementById('loops-results-message'),document.getElementById('loops-knowledge-message')].forEach(function(node){if(node){var panel=node===byId('message')?'loops-config':node.id.replace('-message','');node.textContent=panel===uiErrorPanel?uiError:'';node.className='loop-error';}});}
  function error(exc) { uiErrorPanel=(window.state && state.panel)||'loops-config';uiError=exc.message || String(exc);showMessages(); }
  function clearNativeLog() {
    logGeneration++;logOpening=false;logRun='';logOperation='';
    var url=new URL(location.href);url.searchParams.delete('loop_log');url.searchParams.delete('loop_log_run');history.replaceState(null,'',url);
    if(reportSuspended){reportSuspended=false;reportSignature='';if(isLoopPanel())updateReport();}
  }
  async function openNativeLog(run,job,restore) {
    if(!job || !job.log_available)return;
    if(restore && job.kind==='optimizer' && run.settings.execution==='vast' && (!window.PBGuiVast || !PBGuiVast.openJobLog))return;
    if(!restore)select(run.id);
    var token=++logGeneration,navigation=state.navigationSeq,selection=selected,reportLocation=[reportView,reportRound,reportTab].join(':');logOpening=true;
    function current(){return !stopped && token===logGeneration && navigation===state.navigationSeq && selection===selected && reportLocation===[reportView,reportRound,reportTab].join(':');}
    try {
      var target=await request('/'+run.id+'/log-target?operation='+encodeURIComponent(job.operation));
      if(!current())return;
      if(reportView&&reportOverlay){reportSuspended=true;reportOverlay.classList.remove('is-open');reportOverlay.setAttribute('aria-hidden','true');}
      if(target.cloud){if(!window.PBGuiVast || !PBGuiVast.openJobLog)throw new Error('Vast.ai log controls are still loading. Try again shortly.');if(!await PBGuiVast.openJobLog(target.id,current)){if(current())clearNativeLog();return;}}
      else if(target.kind && target.kind!=='optimizer')openLogPanel(target.id,target.name,{backtest:true});
      else openLogPanel(target.id,target.name);
      if(!current())return;
      logRun=run.id;logOperation=job.operation;uiError='';showMessages();
      var url=new URL(location.href);url.searchParams.set('loop_log_run',run.id);url.searchParams.set('loop_log',job.operation);history.replaceState(null,'',url);
    } catch(exc){if(current()){clearNativeLog();error(exc);}} finally{if(token===logGeneration)logOpening=false;}
  }
  function nativeLogButton(cell,run,jobs) {
    var available=jobs.filter(function(job){return job.log_available;});
    var log=button(cell,'Log',function(){if(available.length===1)openNativeLog(run,available[0],false);});log.className='act-btn loop-open';log.disabled=available.length!==1;
    log.title=available.length>1?'Expand the round to choose a job.':available.length?'Open this job’s native log.':'Available after the native job is created.';
  }
  async function restoreNativeLog() {
    var url=new URL(location.href),id=url.searchParams.get('loop_log_run'),operation=url.searchParams.get('loop_log');
    if(!operation || logOpening || logOperation)return;
    var run=rows.find(function(row){return row.id===id;}),job=run && run.jobs.concat(run.observer_jobs||[]).find(function(job){return job.operation===operation && job.log_available;});
    if(!job){clearNativeLog();error(new Error('The selected optimizer log is no longer available.'));return;}
    await openNativeLog(run,job,true);
  }
  function field(form, id, label, type, value, extra, span) {
    var wrap = element('div', undefined, 'form-group loop-field span-' + (span || 2));
    var caption = element('label', label); caption.htmlFor = 'loop-' + id;
    var input = element(type === 'select' ? 'select' : type === 'textarea' ? 'textarea' : 'input'); input.id = 'loop-' + id;
    if (input.tagName === 'INPUT') input.type = type;
    if (value !== undefined) input.value = value;
    Object.keys(extra || {}).forEach(function (key) { input.setAttribute(key, extra[key]); });
    if (type === 'checkbox') {
      wrap.classList.add('loop-toggle');
      var line = element('div', undefined, 'chk-row'); line.appendChild(input); line.appendChild(caption); wrap.appendChild(line);
    } else { wrap.appendChild(caption); wrap.appendChild(input); }
    form.appendChild(wrap); return input;
  }
  function section(form, title) {
    var container = element('section', undefined, 'loop-section');
    container.appendChild(element('h3', title, 'section-title')); form.appendChild(container); return container;
  }
  function grid(parent) { var row = element('div', undefined, 'form-row cols-8 loop-grid'); parent.appendChild(row); return row; }
  function executionSummary() {
    if (!options.vast) return;
    var target = byId('execution').value, hours = byId('hours');
    hours.max = target === 'vast' ? options.vast.hours : 168;
    if (!hoursEdited) hours.value = options.vast.hours;
    syncRunLimit();
    byId('resources').textContent = target === 'vast' ? (options.vast.gpu_name || 'Matching GPU') + ' · $' + options.vast.budget + ' per rental · up to ' + options.vast.max_rentals + ' GPUs · ' + options.vast.hours + ' h maximum'
      : target === 'gpu' ? (options.gpu_available ? 'Compatible local GPU' : options.gpu_reason) : '';
  }
  function choices(input, values, previous) {
    input.replaceChildren(); values.forEach(function (row) { var option = element('option', row.label || row.id); option.value = row.id; option.disabled = Boolean(row.disabled); input.appendChild(option); });
    if (Array.from(input.options).some(function (row) { return row.value === previous; })) input.value = previous;
  }
  function showConfigValues() {
    var values = options.config_defaults;
    var direction = byId('direction').querySelector('option[value="config"]');
    var mode = byId('run-mode').querySelector('option[value="config"]');
    function range(value) { if (!value) return 'unavailable'; return value[0] === value[1] ? String(value[0]) : value[0] + '–' + value[1]; }
    function coinList(value) { return Array.isArray(value) ? (value.length ? value.join(', ') : 'no explicit list') : String(value || 'no explicit list'); }
    function hint(id, text) {
      var input = byId(id), node = byId(id + '-config');
      if (!node) { node = element('div', '', 'loop-config-value'); node.id = 'loop-' + id + '-config'; input.parentNode.appendChild(node); }
      node.textContent = text;
      node.hidden = input.value.trim() !== '';
    }
    if (!values) {
      direction.textContent = mode.textContent = 'Use config · unavailable';
      hint('coins', 'Config coins unavailable'); hint('positions', 'Config positions unavailable');
      return;
    }
    direction.textContent = 'Use config · ' + ({long:'Long', short:'Short', both:'Long and short', disabled:'both disabled'}[values.direction] || 'unavailable');
    mode.textContent = 'Use config · ' + (values.iters == null ? 'iterations unavailable' : Number(values.iters).toLocaleString() + ' iters');
    var sides = ['long', 'short'], names = {long:'Long', short:'Short'};
    var coins = sides.map(function (side) { return names[side] + ': ' + coinList(values.coins[side]); }).join(' · ');
    var ignored = sides.filter(function (side) { return values.ignored_coins && (values.ignored_coins[side] || []).length; });
    if (ignored.length) coins += ' · Ignored: ' + ignored.map(function (side) { return names[side] + ': ' + coinList(values.ignored_coins[side]); }).join(' · ');
    hint('coins', 'Config · ' + coins);
    byId('positions').placeholder = 'From config: ' + range(values.total_positions);
    hint('positions', 'Config · ' + sides.map(function (side) { return names[side] + ': ' + range(values.positions[side]); }).join(' · '));
  }
  async function loadOptions(listOnly) {
    var token = ++optionsGeneration, navigation = savedNavigation(), config = byId('config').value || navigation.config_name;
    var query=new URLSearchParams();if(listOnly)query.set('list_only','true');if(config)query.set('config_name',config);if(editorSource){query.set('source_kind',editorSource.kind);query.set('source_id',editorSource.id);}var value=await request('/options'+(query.size?'?'+query.toString():''));
    if (token !== optionsGeneration) return;
    if (listOnly && options.configs) {
      options.configs = value.configs;
      options.vast = value.vast;
      options.vast_connected = value.vast_connected;
      return;
    }
    options = value;
    if(!editorSource)sourceDigest=value.bundle_digest || null;
    if(value.missing_config)uiError='Starting config '+value.missing_config+' is no longer available. Choose a configuration.';
    if (sourceDefaults) options.config_defaults=sourceDefaults;
    var configNames=value.configs.slice();if(editorSource && config && !configNames.includes(config))configNames.push(config);
    choices(byId('config'), configNames.map(function (id) { return {id: id}; }), config || value.config_name);
    choices(byId('execution'), [{id: 'cpu', label: 'Local CPU'}, {id: 'gpu', label: 'Local GPU', disabled: !value.gpu_available}, {id: 'vast', label: 'Vast.ai', disabled: !value.vast_connected}], byId('execution').value || navigation.execution);
    showConfigValues();
    executionSummary();
    syncStart();
  }
  function syncRunLimit() {
    var mode = byId('run-mode'), cpu = byId('execution').value === 'cpu';
    Array.from(mode.options).forEach(function (option) { if (option.value === 'proxy') option.disabled = cpu; });
    if (cpu && mode.value === 'proxy') mode.value = 'config';
    ['iters', 'proxy', 'hours'].forEach(function (key) { var input = byId('run-' + key), active = mode.value === key; input.disabled = !active; input.required = active; input.parentNode.hidden = !active; });
    byId('run-iters').min = byId('execution').value === 'vast' ? 256 : 1;
    byId('run-hours').max = byId('hours').value || byId('hours').max;
  }
  function number(id, fallback) { var raw = byId(id).value.trim(); return raw === '' ? fallback : Number(raw); }
  function body() {
    var presets = [], targets = {};
    ['gain', 'drawdown', 'uptrend', 'consistency', 'trades', 'robustness'].forEach(function (name) { if (byId(name).checked) { presets.push(name); var target = number(name + '-target', null); if (target !== null) targets[name] = target; } });
    var sides = {}, total = number('positions', null);
    if (byId('direction').value === 'both' && (byId('long').value !== '' || byId('short').value !== '')) { sides = {long: number('long', 0), short: number('short', 0)}; if (total === null) total = sides.long + sides.short; }
    return {config_name: byId('config').value, execution: byId('execution').value, scenario_enabled: byId('scenario').checked, strategy_enabled: byId('strategy').checked,
      goals: {coins: byId('coins').value.split(/[\s,;]+/).filter(Boolean), direction: byId('direction').value, positions: total, positions_by_side: sides, presets: presets, targets: targets, text: byId('goals').value},
      run_limit_mode: byId('run-mode').value, run_iters: byId('run-mode').value === 'iters' ? number('run-iters', null) : null, run_proxy: byId('run-mode').value === 'proxy' ? number('run-proxy', null) : null, run_hours: byId('run-mode').value === 'hours' ? number('run-hours', null) : null,
      max_runs: number('runs', 10), hours: number('hours', 24), parallel: number('parallel', 1), max_validations: number('validations', 30), candidates: number('candidates', 3), patience: number('patience', 3),
      jev_enabled:jevEnabled,authorization: true};
  }
  function select(id) {
    if (selected !== id) { setupGeneration++; closeReport(); }
    selected = id;workflow.focus=id;selectedRuns=new Set(id?[id]:[]);persistWorkflow();var url = new URL(location.href); if (id) url.searchParams.set('loop_id', id); else url.searchParams.delete('loop_id'); history.replaceState(null, '', url); render();
  }
  function button(parent, label, action) { var node = element('button', label, 'btn'); node.type = 'button'; node.addEventListener('click', action); parent.appendChild(node); return node; }
  var reportWindow, reportOverlay, reportDialog, reportBody, reportTabs, reportTitle, reportView = '', reportRound = 0, reportTab = 'overview';
  var reportSignature = '', reportSuspended=false, diffRequest, diffGeneration = 0, diffOperation = '', diffBaseline = '', reportReturnFocus,reportReturnKey='';
  var tabNames = {overview:'Overview', goals:'Goals', backtests:'Backtests', performance:'User evaluation', changes:'Changes', analysis:'AI analysis'};
  function numeric(value) { return typeof value === 'number' && Number.isFinite(value) ? Number(value.toPrecision(6)).toLocaleString() : '—'; }
  function duration(value) { if (value === null || value === undefined) return '—'; value=Math.max(0,Math.floor(value)); return Math.floor(value/3600)+'h '+Math.floor(value%3600/60)+'m '+value%60+'s'; }
  function dateText(value) { return value ? new Date(value*1000).toLocaleString() : 'Not recorded'; }
  function cycleRows(row) {
    if (Array.isArray(row.cycles)) return row.cycles;
    return Array.from(new Set(row.jobs.map(function (job) { return job.round; }))).sort(function (a,b) { return a-b; }).map(function (round) {
      var jobs=row.jobs.filter(function (job) { return job.round===round; }), checks=jobs.filter(function (job) { return job.kind==='validation'; });
      return {round:round,status:'Not validated',backtests:checks.length,completed_backtests:checks.filter(function (job) { return ['complete','completed'].includes(job.status); }).length,goals:[],observations:[],duration_seconds:null};
    });
  }
  function reportNavigation() {
    var url=new URL(location.href);
    ['loop_view','loop_cycle','loop_tab','loop_variant','loop_compare'].forEach(function (key) { url.searchParams.delete(key); });
    if (reportView) { url.searchParams.set('loop_view',reportView); url.searchParams.set('loop_tab',reportTab); if(reportView==='cycle')url.searchParams.set('loop_cycle',String(reportRound)); if(reportTab==='changes'){if(diffOperation)url.searchParams.set('loop_variant',diffOperation);if(diffBaseline)url.searchParams.set('loop_compare',diffBaseline);} }
    history.replaceState(null,'',url);
  }
  function closeReport(clearNavigation) {
    if(diffRequest)diffRequest.abort(); diffGeneration++; reportSignature=''; reportView='';reportSuspended=false;
    if(reportWindow)reportWindow.cancel();
    if(reportOverlay){reportOverlay.classList.remove('is-open');reportOverlay.setAttribute('aria-hidden','true');}
    if(clearNavigation!==false)reportNavigation();
    root.querySelectorAll('.loop-cycle-table tbody tr').forEach(function(line){line.classList.remove('selected');});
    var focus=reportReturnFocus&&reportReturnFocus.isConnected?reportReturnFocus:document.getElementById(reportReturnFocus&&reportReturnFocus.id)||document.getElementById('loop-cycle-row-'+reportRound)||Array.from(document.querySelectorAll('.loop-panel tr[data-key]')).find(function(row){return row.dataset.key===(reportReturnKey||selected);});
    if(focus)focus.focus({preventScroll:true});
  }
  function openReport(view,round,tab) {
    reportReturnFocus=document.activeElement;var origin=reportReturnFocus&&reportReturnFocus.closest('tr[data-key]');reportReturnKey=origin?origin.dataset.key:'';reportView=view; reportRound=round||0; reportTab=tab||'overview';workflow.focus=view==='cycle'?selected+':'+reportRound:view==='initial'?selected+':initial':selected;persistWorkflow();renderWorkflow();
    diffOperation='';diffBaseline='';reportSignature=''; reportNavigation(); updateReport();
    root.querySelectorAll('.loop-cycle-table tbody tr').forEach(function(line){line.classList.toggle('selected',view==='cycle'&&Number(line.dataset.cycle)===reportRound);});
  }
  function makeReportDialog() {
    reportOverlay=element('div',undefined,'pbg-modal-overlay');reportOverlay.id='loop-report-overlay';reportOverlay.setAttribute('aria-hidden','true');
    reportDialog=element('div',undefined,'pbg-modal-window loop-report-dialog'); reportDialog.id='loop-report-dialog';
    reportDialog.setAttribute('role','dialog');reportDialog.setAttribute('aria-modal','true');
    reportDialog.setAttribute('aria-labelledby','loop-report-title');
    var header=element('div',undefined,'pbg-modal-head'); reportTitle=element('h3');reportTitle.id='loop-report-title';header.appendChild(reportTitle);
    var close=button(header,'×',function(){closeReport();});close.id='loop-report-close';close.className='pbg-modal-close';close.setAttribute('aria-label','Close');close.title='Close';reportDialog.appendChild(header);
    reportTabs=element('div',undefined,'pbg-modal-tabs');reportTabs.setAttribute('role','tablist');
    Object.keys(tabNames).forEach(function(tab){var item=button(reportTabs,tabNames[tab],function(){reportTab=tab;reportSignature='';reportNavigation();updateReport();});item.className='pbg-modal-tab';item.dataset.tab=tab;item.id='loop-report-tab-'+tab;item.setAttribute('role','tab');item.setAttribute('aria-controls','loop-report-body');});
    reportTabs.addEventListener('keydown',function(event){var tabs=Object.keys(tabNames).filter(function(tab){return !document.getElementById('loop-report-tab-'+tab).hidden;}),index=tabs.indexOf(reportTab);if(event.key==='ArrowRight')index=(index+1)%tabs.length;else if(event.key==='ArrowLeft')index=(index+tabs.length-1)%tabs.length;else if(event.key==='Home')index=0;else if(event.key==='End')index=tabs.length-1;else return;event.preventDefault();document.getElementById('loop-report-tab-'+tabs[index]).click();document.getElementById('loop-report-tab-'+tabs[index]).focus();});
    reportDialog.appendChild(reportTabs);reportBody=element('div',undefined,'pbg-modal-body');reportBody.id='loop-report-body';reportBody.setAttribute('role','tabpanel');reportDialog.appendChild(reportBody);
    reportOverlay.appendChild(reportDialog);document.body.appendChild(reportOverlay);
    reportWindow=window.PBGuiFloatingWindow.attach(reportDialog,header,{storage_key:'pbgui.pb8.loops.report_window'});
    reportOverlay.addEventListener('keydown',function(event){
      if(event.key==='Escape'){event.preventDefault();event.stopPropagation();closeReport();return;}
      if(event.key!=='Tab')return;
      var controls=Array.from(reportDialog.querySelectorAll('button:not(:disabled),select:not(:disabled),a[href],input:not(:disabled),[tabindex="0"]')).filter(function(node){return node.tabIndex>=0&&node.getClientRects().length;});
      if(!controls.length){event.preventDefault();return;}
      var first=controls[0],last=controls[controls.length-1];
      if(event.shiftKey&&document.activeElement===first){event.preventDefault();last.focus();}
      else if(!event.shiftKey&&document.activeElement===last){event.preventDefault();first.focus();}
    });
  }
  function reportTable(parent,headers,values) {
    var wrap=element('div',undefined,'loop-table-scroll'), table=element('table',undefined,'tbl loop-table'),head=element('thead'),line=element('tr');
    headers.forEach(function(text){line.appendChild(element('th',text));});head.appendChild(line);table.appendChild(head);var body=element('tbody');
    values.forEach(function(values){var row=element('tr');values.forEach(function(value){row.appendChild(element('td',value===undefined||value===null?'—':String(value)));});body.appendChild(row);});table.appendChild(body);wrap.appendChild(table);parent.appendChild(wrap);return table;
  }
  function goalTable(parent,goals) {
    if(!goals.length){parent.appendChild(element('p','No successful exact comparison recorded.','muted-line'));return;}
    reportTable(parent,['Goal / metric','Target','Previous validated','Current','Change','Result'],goals.map(function(goal){
      var change=goal.delta===null||goal.delta===undefined?'—':(goal.delta>0?'+':'')+metricValue(goal.delta,goal)+(goal.improved===true?' · improved':goal.improved===false&&goal.delta!==0?' · worse':'');
      return [goal.goal+' · '+(goal.metrics||[goal.metric]).join(', ')+' · '+(goal.direction==='min'?'worst window':'mean'),goal.target==null?(goal.goal==='custom'?'Qualitative requirement':'Ranking preference'):metricValue(goal.target,goal),metricValue(goal.previous,goal),metricValue(goal.value,goal),change,goal.value===null||goal.value===undefined?'Not measured':goal.target==null?(goal.goal==='custom'?'Not confirmed':'Measured preference'):goal.confirmed?'Target met':'Target violated'];
    }));
  }
  function observerName(job) {
    var name=job.kind==='observer_holdout'?'Holdout':'Full Time Range';
    if(job.round>=0&&job.comparison_label)name+=' · '+job.comparison_label;
    if(job.selected_for_full_range)name+=' · Best Holdout';
    if(job.previous_selection)name+=' · Previous selection';
    return name;
  }
  function selectedObserver(run,round,kind) {
    var jobs=(run.observer_jobs||[]).filter(function(item){return item.round===round&&item.kind===kind;}),selection=(run.observer_selections||[]).find(function(item){return item.round===round;});
    if(!selection)return jobs[0];
    if(selection.status!=='selected')return undefined;
    return jobs.find(function(item){return kind==='observer_holdout'?item.operation===selection.operation:item.candidate_id===selection.candidate_id;});
  }
  function observerValue(value,metric) {
    if(value==null)return '—';
    if(metric==='gain')return numeric(value)+' ×';
    if(['drawdown','adg'].includes(metric))return numeric(value*100)+' %';
    if(metric==='recovery_days')return numeric(value*24)+' h';
    return numeric(value);
  }
  function observerSummary(run,round,kind) {
    var job=selectedObserver(run,round,kind),selection=(run.observer_selections||[]).find(function(item){return item.round===round;});
    if(!job){if(selection&&selection.status==='waiting')return 'Holdouts '+selection.completed+'/'+selection.expected+' finished';if(selection&&selection.status==='unavailable')return 'No valid Holdout';return run.observer_enabled?'Pending':'Not recorded';}
    if(!job.simulation_complete)return job.display_status||job.status;
    return observerValue((job.metrics||{}).gain,'gain')+' / '+observerValue((job.metrics||{}).drawdown,'drawdown');
  }
  function overviewValue(value,metric,delta) {
    if(value==null)return '—';
    if(/gain/.test(metric))return numeric(value)+' ×';
    if(/drawdown|underwater_pct|^adg/.test(metric))return numeric(value*100)+(delta?' pp':' %');
    if(/days/.test(metric))return numeric(value*24)+' h';
    return numeric(value);
  }
  function overviewChange(parent,value,direction,metric,reference) {
    var known=typeof value==='number'&&Number.isFinite(value),change=known&&Math.abs(value)<1e-12?0:value;
    var improved=known&&change!==0&&(direction==='min'?change<0:change>0);
    var node=element('span',known?(change===0?'= ':improved?'↑ ':'↓ ')+(change>0?'+':'')+overviewValue(change,metric,true):'—','loop-metric-change '+(!known||change===0?'is-neutral':improved?'is-better':'is-worse'));
    node.title=known?(change===0?'Unchanged':improved?'Improved':'Worse')+' · '+reference:'No complete reference result';
    parent.appendChild(node);
  }
  function completeComparison(cycle) {
    var observations=cycle.observations||[];
    return !['incomplete','failed','not validated'].includes(cycle.status)&&(!observations.length||observations.some(function(item){return item.assessment&&item.assessment.comparable!==false&&item.assessment.simulation_complete!==false;}));
  }
  function comparisonReference(run,cycles,cycle) {
    var previous=cycles.filter(function(item){return item.round<cycle.round&&item.score!=null&&completeComparison(item);}).pop();
    if(previous)return {goals:previous.goals||[],label:'vs Loop '+(previous.round+1)+' (best exact comparison)'};
    var baseline=run.baseline&&run.baseline.assessment;
    return {goals:baseline&&baseline.comparable!==false&&baseline.simulation_complete!==false?baseline.goals||[]:[],label:'vs unchanged start'};
  }
  function overviewGoals(cell,goals,reference,complete) {
    cell.replaceChildren();cell.classList.add('loop-metric-cell');
    if(!goals.length){cell.appendChild(element('span','—','loop-metric-change is-neutral'));return;}
    goals.forEach(function(goal){
      var metric=goal.metric||(goal.metrics||[])[0]||'',name={gain:'Gain',drawdown:'Drawdown',uptrend:'Uptrend',consistency:'Consistency',trades:'Trades',robustness:'Robustness',custom:'Custom'}[goal.goal]||goal.goal;
      var item=element('div',undefined,'loop-metric-line'),label=element('span',name+' '+overviewValue(goal.value,metric,false));
      label.title=metric+' · '+(goal.direction==='min'?'worst window':'mean');item.appendChild(label);
      var prior=(reference.goals||[]).find(function(other){return other.goal===goal.goal&&(other.metric||(other.metrics||[])[0])===metric;});
      var delta=complete&&prior&&typeof prior.value==='number'&&typeof goal.value==='number'?goal.value-prior.value:null;
      overviewChange(item,delta,goal.direction,metric,reference.label);cell.appendChild(item);
    });
  }
  function overviewObserver(cell,run,round,kind,specificJob) {
    var job=specificJob||selectedObserver(run,round,kind);
    cell.replaceChildren();cell.classList.add('loop-metric-cell');
    if(!job||!job.simulation_complete){cell.textContent=specificJob?(specificJob.display_status||specificJob.status):observerSummary(run,round,kind);return;}
    if(job.comparison_label)cell.title=job.comparison_label+(job.selected_for_full_range?' · Best Holdout':'');
    ['gain','drawdown'].forEach(function(metric){var item=element('div',undefined,'loop-metric-line');item.appendChild(element('span',(metric==='gain'?'Gain ':'DD ')+overviewValue((job.metrics||{})[metric],metric,false)));overviewChange(item,(job.delta_previous||{})[metric],metric==='gain'?'max':'min',metric,round===0?'vs unchanged start':'vs Loop '+round);cell.appendChild(item);});
  }
  function userEvaluation(parent,run,round) {
    var compareActions=element('div',undefined,'panel-toolbar');parent.appendChild(compareActions);
    ['observer_holdout','observer_full_range'].forEach(function(kind){var control=button(compareActions,kind==='observer_holdout'?'Compare all Holdouts':'Compare all Full Time Ranges',function(){openEvaluationComparison(run,kind);});control.disabled=!hasEvaluationResults(run,kind);});
    parent.appendChild(element('p','Automatic backtests for your evaluation. These results are excluded from AI, JEV, goal scores, stopping decisions and learned knowledge.','muted-line'));
    parent.appendChild(element('p','Holdout tests every successful comparison candidate. Full Time Range tests only the best Holdout, selected by target compliance and Holdout score.','muted-line'));
    var jobs=(run.observer_jobs||[]).filter(function(job){return round==null||job.round===round;}).sort(function(a,b){return a.round-b.round||a.kind.localeCompare(b.kind);});
    if(!jobs.length){parent.appendChild(element('p',run.observer_enabled?'User evaluations are queued after the start comparison and each round’s exact comparison.':'No automatic user evaluations were recorded for this older run.','muted-line'));return;}
    var table=reportTable(parent,['Round / period','Status','Holdout score','Duration','Gain','Drawdown','Recovery','ADG','Sharpe','Sortino','Fills'],jobs.map(function(job){var metrics=job.metrics||{};return [job.round<0?'Unchanged start · '+observerName(job):'Loop '+(job.round+1)+' · '+observerName(job),job.display_status||job.status,numeric(job.holdout_score),duration(job.run_started_at&&job.ended_at?job.ended_at-job.run_started_at:null)].concat(['gain','drawdown','recovery_days','adg','sharpe','sortino','trades'].map(function(metric){return observerValue(metrics[metric],metric);}));}));
    Array.from(table.tBodies[0].rows).forEach(function(line,index){var job=jobs[index];backtestButton(line.cells[0],run,job);nativeLogButton(line.cells[0],run,[job]);});
    jobs.forEach(function(job){
      parent.appendChild(element('h4',(job.round<0?'Unchanged start':'Loop '+(job.round+1))+' · '+observerName(job)));
      if((job.windows||[]).length)reportTable(parent,['Window','Start','End','Exchanges'],job.windows.map(function(window){return [window.label||observerName(job),window.start_date,window.end_date,(window.exchanges||[]).join(', ')];}));
      if(job.error)parent.appendChild(element('p',job.error,'loop-error'));
      if(!job.simulation_complete){parent.appendChild(element('p',job.report_done?'No complete simulation available; deltas are unavailable.':'Waiting for complete simulation results.','muted-line'));return;}
      if(job.round<0)return;
      reportTable(parent,['Metric','Current','Change vs start','Change vs previous round'],['gain','drawdown','recovery_days','adg','sharpe','sortino','trades'].map(function(metric){return [{gain:'Gain',drawdown:'Drawdown',recovery_days:'Recovery time',adg:'ADG',sharpe:'Sharpe',sortino:'Sortino',trades:'Fills'}[metric],observerValue((job.metrics||{})[metric],metric),observerValue((job.delta_start||{})[metric],metric),observerValue((job.delta_previous||{})[metric],metric)];}));
    });
  }
  function renderDiff(row,cycle) {
    var jobs=row.jobs.filter(function(job){return job.kind==='optimizer';}), variants=jobs.filter(function(job){return job.round===cycle.round;});
    if(!variants.length){reportBody.appendChild(element('p','No optimizer snapshot for this round.'));return;}
    if(!variants.some(function(job){return job.operation===diffOperation;}))diffOperation=variants[0].operation;
    var prior=jobs.filter(function(job){return job.round<cycle.round;});
    if(!diffBaseline||diffBaseline===diffOperation||diffBaseline!=='initial'&&!jobs.some(function(job){return job.operation===diffBaseline;}))diffBaseline=prior.length?prior[prior.length-1].operation:'initial';
    var selectors=element('div',undefined,'loop-diff-selectors');
    function selector(label,id,values,selectedValue,change){var field=element('label',label),input=element('select');input.id=id;values.forEach(function(value){var option=element('option',value.label);option.value=value.value;input.appendChild(option);});input.value=selectedValue;input.addEventListener('change',change);field.appendChild(input);selectors.appendChild(field);return input;}
    selector('Optimizer variant','loop-diff-operation',variants.map(function(job){return {value:job.operation,label:job.name};}),diffOperation,function(){diffOperation=this.value;reportSignature='';reportNavigation();updateReport();});
    selector('Compare with','loop-diff-baseline',[{value:'initial',label:'Starting config'}].concat(jobs.filter(function(job){return job.operation!==diffOperation;}).map(function(job){return {value:job.operation,label:'Round '+(job.round+1)+' · '+job.name};})),diffBaseline,function(){diffBaseline=this.value;reportSignature='';reportNavigation();updateReport();});
    reportNavigation();
    reportBody.appendChild(selectors);var output=element('div');reportBody.appendChild(output);output.appendChild(element('p','Loading parameter diff…','muted-line'));
    var token=++diffGeneration;if(diffRequest)diffRequest.abort();diffRequest=new AbortController();
    fetch(base+'/'+encodeURIComponent(row.id)+'/diff?operation='+encodeURIComponent(diffOperation)+'&baseline='+encodeURIComponent(diffBaseline),{credentials:'same-origin',cache:'no-store',signal:diffRequest.signal}).then(async function(response){var data=await response.json();if(!response.ok)throw new Error(data.detail||'Diff unavailable');return data;}).then(function(data){
      if(token!==diffGeneration||!reportOverlay.classList.contains('is-open'))return;output.replaceChildren();
      if(!data.fields.length)output.appendChild(element('p','No parameter changes.'));
      else reportTable(output,['Parameter','Before','After'],data.fields.map(function(field){return [field.path,JSON.stringify(field.before),JSON.stringify(field.after)];}));
      if(data.truncated)output.appendChild(element('p','Diff exceeds 4096 fields.','loop-error'));
    }).catch(function(exc){if(exc.name !== 'AbortError' && token === diffGeneration){output.replaceChildren(element('p',exc.message,'loop-error'));}});
  }
  function updateReport() {
    if(!reportView||!reportDialog||reportSuspended)return;
    var row=rows.find(function(item){return item.id===selected;});if(!row)return;
    var cycles=cycleRows(row),cycle=cycles.find(function(item){return item.round===reportRound;});
    if(reportView==='run'&&['backtests','changes'].includes(reportTab)){reportTab='overview';reportNavigation();}
    if(reportView==='cycle'&&!cycle){closeReport();uiError='The selected round is no longer available.';return;}
    var signature=JSON.stringify([reportView,reportRound,reportTab,diffOperation,diffBaseline,row],function(key,value){return ['updated_at','duration_seconds','last_observed_at'].includes(key)?undefined:value;});
    if(signature===reportSignature){var clock=reportBody.querySelector('[data-report-duration]');if(clock&&cycle)clock.textContent=duration(cycle.duration_seconds);return;}
    reportSignature=signature;if(diffRequest)diffRequest.abort();diffGeneration++;
    var scroll=reportBody.scrollTop,focus=document.activeElement&&document.activeElement.id;
    var continueRound=reportView==='cycle'?reportRound:null;
    var continueKey=(reportView==='run'&&row.best||reportView==='cycle'&&canContinueRound(cycle))?JSON.stringify([row.id,reportView,continueRound]):'';
    var controls=reportBody.firstElementChild;
    if(!continueKey||!controls||controls.dataset.continuationKey!==continueKey)controls=null;
    // Keep the live form attached: removing/reinserting even the same input loses focus.
    Array.from(reportBody.children).forEach(function(node){if(node!==controls)node.remove();});
    if(continueKey&&!controls)continuationControls(reportBody,row,continueRound,continueKey);
    if(reportTab==='analysis')button(reportBody,'View instructions',function(){closeReport();PBGuiLoopInstructions.openRun(row.id);});
    if(reportView==='run'&&reportTab==='overview')resultSummary(reportBody,row);
    reportTitle.textContent=reportView==='cycle'?'Round '+(reportRound+1)+' · '+cycle.status:reportView==='initial'?'Initial result evaluation':'Loop details';
    reportTabs.querySelectorAll('button').forEach(function(item){item.hidden=reportView==='run'&&['backtests','changes'].includes(item.dataset.tab);var selectedTab=item.dataset.tab===reportTab;item.classList.toggle('selected',selectedTab);item.setAttribute('aria-selected',String(selectedTab));item.tabIndex=selectedTab?0:-1;});
    reportBody.setAttribute('aria-labelledby','loop-report-tab-'+reportTab);
    if(reportTab==='performance')userEvaluation(reportBody,row,reportView==='cycle'?reportRound:reportView==='initial'?-1:null);
    else if(reportView==='cycle'){
      var jobs=row.jobs.filter(function(job){return job.round===reportRound;});
      if(reportTab==='overview'){
        var overview=element('div',undefined,'form-row cols-4 loop-summary');[['Status',cycle.status],['Duration',duration(cycle.duration_seconds)],['Backtests',cycle.completed_backtests+' / '+cycle.backtests],['Validated score',numeric(cycle.score)]].forEach(function(pair){var card=element('div');card.appendChild(element('span',pair[0]));var val=element('strong',pair[1]);if(pair[0]==='Duration')val.dataset.reportDuration='';card.appendChild(val);overview.appendChild(card);});reportBody.appendChild(overview);
        reportTable(reportBody,['Started','Finished','Timing'],[[dateText(cycle.started_at),cycle.active?'Running':dateText(cycle.ended_at),cycle.duration_estimated?'Estimated from legacy timestamps':'Recorded']]);
        reportTable(reportBody,['Job','Kind','Status','Duration'],jobs.map(function(job){return [job.name,job.kind,job.status,duration(job.run_started_at&&job.ended_at?job.ended_at-job.run_started_at:null)];}));
        jobs.filter(function(job){return job.error||job.result_error;}).forEach(function(job){reportBody.appendChild(element('p',job.name+': '+(job.error||job.result_error),'loop-error'));});
      } else if(reportTab==='goals')goalTable(reportBody,cycle.goals||[]);
      else if(reportTab==='changes')renderDiff(row,cycle);
      else if(reportTab==='backtests'){
        var checks=jobs.filter(function(job){return ['validation','holdout'].includes(job.kind);});
        var comparison=row.comparison||{};reportTable(reportBody,['Comparison period','Exchanges'],[[[comparison.start_date,comparison.end_date].filter(Boolean).join(' → ')||'Not recorded',(comparison.exchanges||[]).join(', ')||'Not recorded']]);
        if(!checks.length)reportBody.appendChild(element('p','No comparison backtests. This round cannot authorize the next optimizer round.','loop-error'));
        else {checks.forEach(function(job){var actions=element('div',undefined,'panel-toolbar');actions.appendChild(element('span',job.name));nativeLogButton(actions,row,[job]);backtestButton(actions,row,job);reportBody.appendChild(actions);});reportTable(reportBody,['Backtest','Type','Status','Started','Finished','Score'],checks.map(function(job){var obs=(cycle.observations||[]).find(function(item){return item.operation===job.operation;});return [job.name,job.kind,job.status,dateText(job.run_started_at),dateText(job.ended_at),numeric(obs&&obs.assessment?obs.assessment.score:null)];}));}
        (cycle.observations||[]).forEach(function(obs){reportBody.appendChild(element('h4',obs.candidate_id));goalTable(reportBody,(obs.assessment.goals||[]).map(function(goal){return Object.assign({},goal,{metrics:goal.metric?[goal.metric]:[]});}));});
      } else {
        if(cycle.reason){reportBody.appendChild(element('h4','Cycle evaluation'));reportBody.appendChild(element('p',cycle.reason,'loop-analysis'));}
        if(cycle.knowledge){reportBody.appendChild(element('h4','Findings'));reportBody.appendChild(element('p',cycle.knowledge,'loop-analysis'));}
        jobs.filter(function(job){return job.kind==='optimizer';}).forEach(function(job){reportBody.appendChild(element('h4',job.name));reportBody.appendChild(element('p',job.reason||'No AI rationale recorded.','loop-analysis'));reportBody.appendChild(element('p','Applied native iters: '+numeric(job.native_iters),'muted-line'));});
      }
    } else if(reportView==='initial'){
      var initial=row.bootstrap;
      row.jobs.filter(function(job){return job.kind==='baseline';}).forEach(function(job){var actions=element('div',undefined,'panel-toolbar');actions.appendChild(element('span','Unchanged starting baseline · '+job.status));nativeLogButton(actions,row,[job]);backtestButton(actions,row,job);reportBody.appendChild(actions);});
      if(row.baseline)goalTable(reportBody,row.baseline.assessment.goals||[]);
      if(!initial)reportBody.appendChild(element('p','No initial saved-result evaluation was recorded.'));
      else if(reportTab==='analysis'){reportBody.appendChild(element('p',initial.reason||'Evaluation pending.','loop-analysis'));if(initial.knowledge)reportBody.appendChild(element('p',initial.knowledge,'loop-analysis'));}
      else if(reportTab==='changes'){var initialCycle=cycles[0];if(initialCycle){renderDiff(row,initialCycle);}else reportBody.appendChild(element('p','No optimizer snapshot yet.'));}
      else if(reportTab==='backtests')reportBody.appendChild(element('p','Historical optimizer results require separate exact comparison backtests. Open a round to inspect those jobs.'));
      else {reportBody.appendChild(element('p','Status: '+initial.status));reportBody.appendChild(element('p',(initial.candidate_count||0)+' historical candidates · pending exact comparison'));reportTable(reportBody,['Saved result run'],(initial.sources||[]).map(function(source){return [source];}));if(reportTab==='goals')reportTable(reportBody,['Goal','Metrics','Target'],(row.rubric||[]).map(function(rule){return [rule.goal,rule.metrics.join(', '),numeric(rule.target)];}));}
    } else {
      if(reportTab==='analysis'){reportBody.appendChild(element('h4','Goal interpretation'));reportBody.appendChild(element('p',row.interpretation||'Not recorded.','loop-analysis'));if(row.status!=='failed'){if(row.reason)reportBody.appendChild(element('p',row.reason,'loop-analysis'));if(row.last_error&&row.last_error!==row.reason)reportBody.appendChild(element('p',row.last_error,'loop-error'));}}
      else if(reportTab==='goals') {
        var measured=cycles.filter(function(cycle){return cycle.score!=null;}), latest=measured[measured.length-1];
        if(!latest) reportBody.appendChild(element('p','No confirmed exact-backtest comparison yet.'));
        else reportTable(reportBody,['Goal / metric'].concat(measured.map(function(cycle){return 'Loop '+(cycle.round+1);})), latest.goals.map(function(goal){return [goal.goal+' · '+goal.metrics.join(', ')].concat(measured.map(function(cycle){var item=(cycle.goals||[]).find(function(value){return value.goal===goal.goal&&value.metrics.join(',')===goal.metrics.join(',');});return item?metricValue(item.value,item)+(item.delta==null?'':item.improved?' ↑':item.delta!==0?' ↓':' ='):'—';}));}));
      }
      else {if(row.best)button(reportBody,'Download best comparison config',async function(){try{var payload=await request('/'+row.id+'/best-config'),url=URL.createObjectURL(new Blob([JSON.stringify(payload,null,4)],{type:'application/json'})),link=element('a');link.href=url;link.download='pb8-loop-'+row.id+'.json';link.click();setTimeout(function(){URL.revokeObjectURL(url);},1000);}catch(exc){error(exc);}});reportTable(reportBody,['Field','Value'],[['Status',row.status],['Execution',({cpu:'Local CPU',gpu:'Local GPU',vast:'Vast.ai'}[row.settings.execution]||row.settings.execution)],['AI calls',row.ai_calls],['Token reserve',row.ai_tokens_reserved],['JEV',row.jev_status||'Not recorded'],['JEV reserved cost','$'+Number(row.jev_usd_reserved||0).toFixed(4)],['Deadline',dateText(row.deadline)],['Final holdout',row.holdout_used?'Used':row.holdout_available?'Available':'Unavailable'],['PB8 revision',(row.fingerprint||{}).pb8||'Unknown'],['Data status',(row.fingerprint||{}).data_status||'Unknown'],['AI USD reserve',row.usd_status==='subscription'?'Subscription / unavailable':'$'+Number(row.ai_usd_reserved||0).toFixed(4)]]);}
    }
    if(row.status==='failed'&&['overview','analysis'].includes(reportTab)&&(reportView==='run'||reportView==='initial'&&row.phase==='bootstrap'||reportView==='cycle'&&reportRound===((row.failure||{}).round==null?row.round:row.failure.round)))terminationReport(reportBody,row);
    reportSignature=JSON.stringify([reportView,reportRound,reportTab,diffOperation,diffBaseline,row],function(key,value){return ['updated_at','duration_seconds','last_observed_at'].includes(key)?undefined:value;});
    reportBody.scrollTop=scroll;if(!reportOverlay.classList.contains('is-open')){reportOverlay.classList.add('is-open');reportOverlay.setAttribute('aria-hidden','false');reportWindow.fit();document.getElementById('loop-report-close').focus();}else if(focus){var focused=document.getElementById(focus);if(focused&&reportDialog.contains(focused))focused.focus({preventScroll:true});}
  }
  function executionName(value){return {cpu:'Local CPU',gpu:'Local GPU',vast:'Vast.ai',validation:'Local CPU'}[value]||'Unknown';}
  function metricValue(value,goal){
    if(value==null)return '—';var metric=goal.metric||(goal.metrics||[])[0]||'';
    if(metric.includes('days'))return numeric(value)+' days ('+numeric(value*24)+' h)';
    if(/drawdown|underwater_pct|gain|^adg/.test(metric))return numeric(value*100)+' %';
    return numeric(value);
  }
  var resultsGeneration=0;
  function hasEvaluationResults(run,kind){return !!run&&(run.observer_jobs||[]).some(function(job){return job.kind===kind&&job.simulation_complete&&['complete','completed'].includes(job.status)&&job.log_available;});}
  function openEvaluationComparison(run,kind){
    if(!hasEvaluationResults(run,kind))return;
    try{sessionStorage.removeItem('pbgui.loopEvaluationComparison');}catch(_){}
    select(run.id);
    var origin=new URL(location.href),url=new URL(origin);
    url.pathname=url.pathname.replace(/\/optimize-v8\/main_page$/, '/backtest-v8/main_page');url.search='';url.hash='';
    url.searchParams.set('panel','results');url.searchParams.set('result_loop',run.id);url.searchParams.set('result_evaluation',kind);
    var back=new URL(origin);back.search='';['loop_id','loop_view','loop_cycle','loop_tab','loop_variant','loop_compare','loop_log_run','loop_log'].forEach(function(key){if(origin.searchParams.has(key))back.searchParams.set(key,origin.searchParams.get(key));});back.hash=state.panel;
    url.searchParams.set('loop_return',back.pathname+back.search+back.hash);location.assign(url.href);
  }
  function backtestButton(parent,run,job){
    if(!job.log_available)return;
    var control=button(parent,'Results',async function(){
      select(run.id);
      var token=++resultsGeneration,navigation=state.navigationSeq,panel=state.panel,view=[reportView,reportRound,reportTab].join(':');
      function current(){return token===resultsGeneration&&navigation===state.navigationSeq&&panel===state.panel&&selected===run.id&&view===[reportView,reportRound,reportTab].join(':');}
      try{
        var target=await request('/'+run.id+'/log-target?operation='+encodeURIComponent(job.operation));
        if(!current())return;
        var url=new URL('/api/backtest-v8/main_page',location.origin);
        url.searchParams.set('panel','results');url.searchParams.set('result_filter',target.name);url.searchParams.set('result_compare','1');
        var origin=new URL(location.href),back=new URL('/api/optimize-v8/main_page',location.origin);
        ['loop_id','loop_view','loop_cycle','loop_tab','loop_variant','loop_compare','loop_log_run','loop_log'].forEach(function(key){if(origin.searchParams.has(key))back.searchParams.set(key,origin.searchParams.get(key));});
        back.hash=panel;url.searchParams.set('loop_return',back.pathname+back.search+back.hash);location.assign(url.href);
      }catch(exc){if(current())error(exc);}
    });control.className='act-btn loop-open';
  }
  function canContinueRound(cycle){return (cycle.observations||[]).some(function(item){return item.assessment&&item.assessment.comparable!==false&&item.assessment.simulation_complete!==false;});}
  function continueButton(parent,run,round){
    var control=button(parent,'Continue',function(){select(run.id);openReport(round==null?'run':'cycle',round==null?0:round,'overview');var count=reportBody.querySelector('input[aria-label="Additional optimizer runs"]');if(count){count.focus();count.scrollIntoView({block:'nearest'});}});control.className='act-btn loop-open';control.title=round==null?'Continue from the best exact comparison with 5, 10 or a custom number of runs.':'Continue from this round with 5, 10 or a custom number of runs.';
  }
  var continueRounds=5,continuing=false,continuationBusyReset;
  function continuationControls(parent,row,round,key){
    var form=element('div');form.dataset.continuationKey=key;parent.appendChild(form);
    var actions=element('div',undefined,'panel-toolbar');actions.appendChild(element('span',round==null?'Continue from best exact comparison':'Continue from this round’s best exact comparison'));
    var count=element('input');count.type='number';count.min='1';count.max='200';count.required=true;count.value=continueRounds;count.className='panel-input';count.style.width='90px';count.setAttribute('aria-label','Additional optimizer runs');count.oninput=function(){continueRounds=count.value;};actions.appendChild(count);
    var presets=[5,10].map(function(n){return button(actions,String(n)+' more',function(){continueRounds=n;count.value=n;});});
    var status=element('p','','muted-line');status.setAttribute('role','status');status.hidden=true;
    function busy(value){action.disabled=value;count.disabled=value;presets.forEach(function(control){control.disabled=value;});action.textContent=value?'Starting…':'Continue';form.setAttribute('aria-busy',String(value));}
    continuationBusyReset=function(){busy(false);};
    var action=button(actions,'Continue',async function(){
      if(continuing||!count.reportValidity())return;
      var rounds=Number(count.value);continuing=true;busy(true);status.hidden=false;status.className='muted-line';status.textContent='Preparing the linked run…';
      try{var ai=await PBGuiAI.ensureSelection();status.textContent='Starting the linked run…';var result=await request('/'+row.id+'/continue',{rounds:rounds,round:round,selection:ai,authorization:true});closeReport();selected=result.id;selectedRuns=new Set([result.id]);expanded.add(result.id);persistWorkflow();var url=new URL(location.href);url.searchParams.set('loop_id',result.id);history.replaceState(null,'',url);selectPanel('loops-queue');await poll();}
      catch(exc){status.className='loop-error';status.textContent=exc.message;error(exc);}
      finally{continuing=false;busy(false);continuationBusyReset();}
    });busy(continuing);form.appendChild(actions);
    form.appendChild(element('p','Starts a new linked run. Goals and current rental limits still apply; a used final holdout is not reused.','muted-line'));form.appendChild(status);
  }
  function failureReason(row){return (row.failure||{}).reason||row.reason||row.last_error||'Failure reason was not recorded for this older run. See PBGui.log.';}
  function terminationReport(parent,row){
    var failure=row.failure||{},settings=row.settings||{},stage=failure.stage||failure.phase||row.phase;
    var stages={interpret:'Goal interpretation',bootstrap:'Initial evaluation',baseline:'Starting baseline backtest',optimize:'Optimization',select:'Candidate selection',validate:'Comparison backtests',evaluate:'Cycle evaluation',holdout:'Final holdout'};
    parent.appendChild(element('h4','Termination details'));
    var fields=[['Reason',failureReason(row)],['Failed at',failure.at?dateText(failure.at):'Not recorded'],['Step',stages[stage]||stage||'Not recorded'],['Round',(failure.round==null?row.round:failure.round)==null?'Not recorded':'Loop '+((failure.round==null?row.round:failure.round)+1)],['Error type',failure.error_type||'Not recorded'],['AI provider',failure.provider||settings.provider||'Not recorded'],['AI model',failure.model||settings.model||'Not recorded'],['Reasoning',failure.effort||settings.effort||'Not recorded'],['Request attempts',failure.attempts==null?'Not recorded':failure.attempts]];
    if(failure.detail&&failure.detail!==failureReason(row))fields.push(['Error detail',failure.detail]);
    reportTable(parent,['Field','Value'],fields);
    if(failure.history&&failure.history.length){parent.appendChild(element('h4','Failed request attempts'));reportTable(parent,['Attempt','Failed at','Error','Next retry pause'],failure.history.map(function(item){return [item.attempt,dateText(item.at),item.error,item.retry_delay==null?'No further retry':item.retry_delay+' seconds'];}));}
  }
  function resultSummary(parent,row){
    var best=row.best,baseline=row.baseline,assessment=best&&best.assessment;
    var finalAssessment=row.final_validation&&row.final_validation[0]&&row.final_validation[0].assessment;
    var confirmation=row.status==='completed'?'Confirmed':row.final_validation&&row.final_validation.length?'Unconfirmed — inspect holdout goals':'Not confirmed';
    if(row.status==='completed'&&finalAssessment&&finalAssessment.confirmation_source==='observer_holdout')confirmation=(finalAssessment.goals||[]).some(function(goal){return goal.requirement;})?'Holdout checked · numeric targets met':'Holdout checked';
    if(row.holdout_retryable)button(parent,'Retry unchanged final holdout',async function(){try{await request('/'+row.id+'/retry-holdout',{});await poll();}catch(exc){error(exc);}});
    reportTable(parent,['Outcome','Details'],[['Search stopped',row.status==='failed'?failureReason(row):row.stop_reason||row.reason||row.status],['Best comparison',best?'Round '+(best.round==null?'unknown':best.round+1)+' · '+numeric(assessment.score):'None'],['Hard numeric targets',assessment?(assessment.hard_targets_met===false?'Violated':assessment.hard_targets_met===true?'Met':'See goal evidence'):'Unknown'],['Final confirmation',confirmation],['Start comparison',baseline?numeric(baseline.assessment.score):'Not recorded for this older run']]);
    if(best){reportTable(parent,['Goal / metric','Unchanged start','Best comparison','Change vs start'],assessment.goals.map(function(goal){var initial=baseline&&(baseline.assessment.goals||[]).find(function(item){return item.goal===goal.goal&&item.metric===goal.metric;});return [goal.goal+' · '+goal.metric,metricValue(initial&&initial.value,goal),metricValue(goal.value,goal),initial&&initial.value!=null?metricValue(goal.value-initial.value,goal):'Unknown'];}));}
    if(row.final_validation&&row.final_validation.length){parent.appendChild(element('h4','Final holdout'));row.final_validation.forEach(function(item){goalTable(parent,item.assessment.goals||[]);});}
    (row.jev_answers||[]).forEach(function(answer){parent.appendChild(element('p',typeof answer==='string'?answer:JSON.stringify(answer),'loop-analysis'));});
    if(row.settings.run_limit_mode==='proxy')parent.appendChild(element('p','Proxy stopping is supervised from native progress. An already dispatched GPU batch can finish beyond the requested threshold.','muted-line'));
  }
  function render() { renderWorkflow(); updateReport(); }
  function tableSelection(line, key, getSelection, repaint) {
    line.tabIndex=0;line.classList.toggle('selected',getSelection().has(key));
    function paint(){var selection=getSelection();line.closest('table').querySelectorAll('tr[data-selection]').forEach(function(row){row.classList.toggle('selected',selection.has(row.dataset.selection));});}
    function pick(event){var table=line.closest('table');if(event.type==='click'&&table.dataset.dragged==='1'){delete table.dataset.dragged;repaint();return;}if(event.target.closest('button'))return;var selection=getSelection();if(!event.ctrlKey&&!event.metaKey)selection.clear();if(event.ctrlKey||event.metaKey){if(selection.has(key))selection.delete(key);else selection.add(key);}else selection.add(key);persistWorkflow();paint();repaint();}
    line.addEventListener('click',pick);line.addEventListener('keydown',function(event){if(event.key===' '){event.preventDefault();pick(event);}});
    line.addEventListener('pointerdown',function(event){if(event.button!==0||event.target.closest('button'))return;var table=line.closest('table'),selection=getSelection(),lines=Array.from(table.querySelectorAll('tbody tr[data-selection]'));delete table.dataset.dragged;table.dataset.selecting='1';table._loop_range={from:lines.indexOf(line),baseline:new Set(event.ctrlKey||event.metaKey?selection:[]),remove:(event.ctrlKey||event.metaKey)&&selection.has(key)};});
    line.addEventListener('pointerenter',function(){var table=line.closest('table'),range=table._loop_range;if(table.dataset.selecting==='1'&&range){table.dataset.dragged='1';var selection=getSelection(),lines=Array.from(table.querySelectorAll('tbody tr[data-selection]')),to=lines.indexOf(line);selection.clear();range.baseline.forEach(function(value){selection.add(value);});lines.slice(Math.min(range.from,to),Math.max(range.from,to)+1).forEach(function(row){if(range.remove)selection.delete(row.dataset.selection);else selection.add(row.dataset.selection);});persistWorkflow();paint();}});
    line.dataset.selection=key;
  }
  function tableShell(parent, headings) { var wrap=element('div',undefined,'loop-table-scroll');var table=element('table',undefined,'tbl loop-table');var head=element('thead'),line=element('tr');headings.forEach(function(title){line.appendChild(element('th',title));});head.appendChild(line);table.appendChild(head);var body=element('tbody');table.appendChild(body);wrap.appendChild(table);parent.appendChild(wrap);return body; }
  function disclosure(cell,key,callback) {var toggle=button(cell,expanded.has(key)?'▾':'▸',function(){if(expanded.has(key))expanded.delete(key);else expanded.add(key);persistWorkflow();callback();});toggle.className='act-btn loop-disclosure';toggle.setAttribute('aria-expanded',String(expanded.has(key)));toggle.setAttribute('aria-label',(expanded.has(key)?'Collapse ':'Expand ')+(cell.firstChild.textContent||'group'));}
  function renderWorkflow() {
    if(!root)return;
    showMessages();
    ['config','queue','results'].forEach(function(kind){byId('nav-count-'+kind).textContent=String(kind==='config'?definitions.length:rows.filter(function(row){return kind==='results'?row.ended:!row.ended;}).length);});
    renderConfigurations();renderRunTree('loops-queue',false);renderRunTree('loops-results',true);renderKnowledge();syncContext();
  }
  function renderKnowledge(){
    var host=document.getElementById('loop-knowledge-list');if(!host)return;
    byId('nav-count-knowledge').textContent=knowledgeLoaded?String(knowledgeEntries.length):'—';
    var message=byId('knowledge-status');message.textContent=knowledgeError||(!knowledgeLoaded?'Loading findings…':'');message.className=knowledgeError?'loop-error':'muted-line';
    var query=(workflow.knowledge_filter||'').toLowerCase();
    var entries=knowledgeEntries.slice().reverse().filter(function(item){var source=rows.find(function(row){return row.id===item.loop;});return JSON.stringify([item.knowledge,item.hypothesis,item.coins,item.loop,source&&source.name,item.id,item.status]).toLowerCase().includes(query);});
    var signature=JSON.stringify([knowledgeLoaded,entries,rows.map(function(row){return [row.id,row.name,row.ended];})]);if(signature===knowledgeSignature)return;knowledgeSignature=signature;
    var kept=new Set(), cursor=host.firstElementChild, scroll=document.getElementById('panel-loops-knowledge').scrollTop;
    entries.forEach(function(item){
      var key=String(item.id), run=rows.find(function(row){return row.id===item.loop;}), entrySignature=JSON.stringify([item,run&&[run.name,run.ended]]), node=Array.from(host.children).find(function(child){return child.dataset.evidenceId===key;});
      if(!node||node.dataset.signature!==entrySignature){
        var previous=node;node=element('article',undefined,'loop-knowledge-entry');node.dataset.evidenceId=key;node.dataset.signature=entrySignature;
        var title=item.id&&item.id.endsWith(':bootstrap')?'Starting evaluation':/\:round:\d+$/.test(item.id||'')?'Loop '+(Number(item.id.split(':').pop())+1):'Run finding';
        node.appendChild(element('h3',(run?run.name:String(item.loop||'').slice(0,8))+' · '+title));
        var status={historical_optimizer_guidance:'Historical guidance',observed_exact:'Exact comparison',hypothesis:'Hypothesis',unconfirmed:'Unconfirmed',confirmed:'Confirmed'}[item.status]||item.status||'Not recorded';
        node.appendChild(element('p',dateText(item.created_at)+' · '+status+(item.superseded_by?' · Superseded':''),'muted-line'));
        node.appendChild(element('p',item.knowledge||'No finding text recorded.','loop-analysis'));
        var evidence=element('details',undefined,'loop-advanced');evidence.appendChild(element('summary','Evidence and source'));
        evidence.open=(workflow.knowledge_open||[]).includes(key);
        evidence.addEventListener('toggle',function(){var opened=new Set(workflow.knowledge_open||[]);if(evidence.open)opened.add(key);else opened.delete(key);workflow.knowledge_open=Array.from(opened);persistWorkflow();});
        var source=element('div',undefined,'panel-toolbar');
        source.appendChild(element('span','Source run: '+(run?run.name+' · ': '')+String(item.loop||'Not recorded')));
        if(run)button(source,'Open run',function(){select(run.id);selectPanel(run.ended?'loops-results':'loops-queue');openReport('run',0,'overview');});
        evidence.appendChild(source);
        if(item.hypothesis)evidence.appendChild(element('p',item.hypothesis,'loop-analysis'));
        reportTable(evidence,['Field','Value'],[['Evidence ID',key],['Coins',(item.coins||[]).join(', ')||'Inherited from config'],['PB8 revision',(item.fingerprint||{}).pb8||'Not recorded'],['Comparison period',[(item.comparison||{}).start_date,(item.comparison||{}).end_date].filter(Boolean).join(' → ')||'Not recorded'],['Uncertainty',item.uncertainty||'Not recorded'],['Superseded by',item.superseded_by||'—']]);
        var observations=item.observations||[];
        if(observations.length)reportTable(evidence,['Backtest','Score','Numeric targets'],observations.map(function(obs){var assessment=obs.assessment||{};return [obs.operation||'Not recorded',numeric(assessment.score),assessment.hard_targets_met===true?'Met':assessment.hard_targets_met===false?'Violated':'Not recorded'];}));
        var raw=element('details',undefined,'loop-advanced');raw.appendChild(element('summary','Raw evidence'));raw.appendChild(element('pre',JSON.stringify(item,null,2)));evidence.appendChild(raw);node.appendChild(evidence);
        if(previous){if(cursor===previous)cursor=node;previous.replaceWith(node);}
      }
      kept.add(node);if(node!==cursor)host.insertBefore(node,cursor);else cursor=cursor.nextElementSibling;
    });
    Array.from(host.children).forEach(function(node){if(!kept.has(node))node.remove();});
    if(!entries.length&&knowledgeLoaded)host.appendChild(element('p',knowledgeEntries.length?'No matching findings.':'No saved findings yet.','muted-line'));
    document.getElementById('panel-loops-knowledge').scrollTop=scroll;
  }
  function renderConfigurations() {
    var host=byId('config-list');var sig=JSON.stringify([definitions,workflow.config_filter]);if(host.querySelector('table[data-selecting]'))return;if(host.dataset.signature===sig){host.querySelectorAll('tr[data-selection]').forEach(function(line){line.classList.toggle('selected',selectedDefinitions.has(line.dataset.selection));});return;}host.dataset.signature=sig;
    host.replaceChildren();var body=tableShell(host,['Name','Starting config','Execution','Updated']);
    var query=(workflow.config_filter || '').toLowerCase();definitions.filter(function(row){return !query || (row.name+' '+row.settings.config_name).toLowerCase().includes(query);}).forEach(function(row){
      var line=element('tr');[row.name,row.settings.config_name,{cpu:'Local CPU',gpu:'Local GPU',vast:'Vast.ai'}[row.settings.execution],dateText(row.updated_at)].forEach(function(value){line.appendChild(element('td',value));});body.appendChild(line);
      tableSelection(line,row.name,function(){return selectedDefinitions;},renderWorkflow);line.addEventListener('dblclick',function(){openDefinition(row,false).catch(error);});line.addEventListener('keydown',function(event){if(event.key==='Enter')openDefinition(row,false).catch(error);});
    });
    if(!body.children.length){var line=element('tr'),cell=element('td','No loop configurations.');cell.colSpan=4;line.appendChild(cell);body.appendChild(line);}
  }
  function renderRunTree(panel, ended) {
    var host=document.getElementById(panel+'-table');var query=(workflow[panel+'_filter'] || '').toLowerCase(),status=workflow[panel+'_status'] || '';
    var visible=rows.filter(function(row){return !!row.ended===ended && (!query || ((row.name||row.settings.config_name)+' '+row.id).toLowerCase().includes(query)) && (!status || row.status===status);});
    var sig=JSON.stringify([visible,Array.from(expanded)],function(key,value){return ['updated_at','duration_seconds'].includes(key)?undefined:value;});
    if(host.querySelector('table[data-selecting]'))return;
    if(host.dataset.signature===sig){host.querySelectorAll('tr[data-key]').forEach(function(line){line.classList.toggle('selected',selectedRuns.has(line.dataset.selection));line.classList.toggle('is-selected',line.dataset.key===workflow.focus && !line.dataset.selection);});host.querySelectorAll('[data-clock]').forEach(function(clock){var pair=clock.dataset.clock.split(':'),row=rows.find(function(row){return row.id===pair[0];}),cycle=row && cycleRows(row).find(function(cycle){return cycle.round===Number(pair[1]);});if(cycle)clock.textContent=duration(cycle.duration_seconds);});return;}host.dataset.signature=sig;var oldScroll=host.querySelector('.loop-table-scroll'),scrollLeft=oldScroll?oldScroll.scrollLeft:0;var focused=document.activeElement,focusedRow=focused&&focused.closest('tr[data-key]'),focusKey=focusedRow&&host.contains(focusedRow)?focusedRow.dataset.key:'';host.replaceChildren();
    var body=tableShell(host,['Name / round / job','Status','Execution','Duration','Backtests','Comparison metrics · Δ prior round','Goal score','Score change','Created','Holdout gain / DD','Full range gain / DD']);
    function line(values, depth, key, report) {values=values.slice();values.splice(5,0,'');var row=element('tr');values.forEach(function(value,index){var cell=element('td',String(value==null?'—':value));if(index===0)cell.style.paddingLeft='calc(var(--sp-md) + '+depth+' * 20px)';row.appendChild(cell);});while(row.cells.length<11)row.appendChild(element('td',''));body.appendChild(row);row.dataset.key=key;row.classList.toggle('is-selected',key===workflow.focus);row.tabIndex=0;if(report){row.addEventListener('dblclick',report);row.addEventListener('keydown',function(event){if(event.key==='Enter'){event.preventDefault();report();}});var open=button(row.cells[0],'Open',report);open.className='act-btn loop-open';}return row;}
    visible.forEach(function(run){
      var cycles=cycleRows(run), latest=cycles.filter(function(cycle){return cycle.score!=null;}).pop(),checks=run.jobs.filter(function(job){return job.kind!=='optimizer';});
      var parent=line([(run.name||run.settings.config_name)+' · '+run.id.slice(0,8),run.status,{cpu:'Local CPU',gpu:'Local GPU',vast:'Vast.ai'}[run.settings.execution],run.deadline==null?'—':duration((run.ended?Math.max.apply(null,run.jobs.map(function(job){return job.ended_at||run.created_at;})):Date.now()/1000)-(run.started_at||run.created_at)),checks.filter(function(job){return ['completed','complete'].includes(job.status);}).length+' / '+checks.length,run.best?numeric(run.best.assessment.score):latest?numeric(latest.score):'—','—',dateText(run.created_at)],0,run.id,function(){select(run.id);openReport('run',0,'overview');});
      var bestCycle=cycles.find(function(cycle){return run.best&&cycle.round===run.best.round;})||latest;
      overviewObserver(parent.cells[9],run,latest?latest.round:-1,'observer_holdout');overviewObserver(parent.cells[10],run,latest?latest.round:-1,'observer_full_range');
      if(bestCycle){parent.cells[5].title='Best comparison · Loop '+(bestCycle.round+1);overviewGoals(parent.cells[5],bestCycle.goals||[],comparisonReference(run,cycles,bestCycle),completeComparison(bestCycle));}
      tableSelection(parent,run.id,function(){return selectedRuns;},function(){selected=run.id;workflow.focus=run.id;persistWorkflow();var url=new URL(location.href);url.searchParams.set('loop_id',selected);history.replaceState(null,'',url);renderWorkflow();});disclosure(parent.cells[0],run.id,renderWorkflow);
      if(run.ended&&run.best)continueButton(parent.cells[0],run,null);
      var latestRound=Math.max.apply(null,run.jobs.filter(function(job){return job.kind==='optimizer';}).map(function(job){return job.round;}));nativeLogButton(parent.cells[0],run,run.jobs.filter(function(job){return job.kind==='optimizer'&&job.round===latestRound;}));
      function observerLines(number,depth){(run.observer_jobs||[]).filter(function(job){return job.round===number;}).forEach(function(job){var native=line([(number<0?'Start · ':'')+observerName(job)+' · user evaluation',job.display_status||job.status,'Local CPU',duration(job.ended_at&&job.run_started_at?job.ended_at-job.run_started_at:null),'','','',dateText(job.created_at)],depth,run.id+':observer:'+job.operation,function(){select(run.id);openReport(number<0?'initial':'cycle',Math.max(0,number),'performance');});overviewObserver(native.cells[job.kind==='observer_holdout'?9:10],run,number,job.kind,job);nativeLogButton(native.cells[0],run,[job]);backtestButton(native.cells[0],run,job);});}
      if(!expanded.has(run.id))return;
      line(['Initial evaluation',run.bootstrap?run.bootstrap.status:'Not recorded','','','','','',''],1,run.id+':initial',function(){select(run.id);openReport('initial',0,'overview');});
      observerLines(-1,1);
      run.jobs.filter(function(job){return job.kind==='baseline';}).forEach(function(job){var baseline=line(['Start comparison',job.display_status||job.status,'Local CPU',duration(job.ended_at&&job.created_at?job.ended_at-job.created_at:null),'','','',''],1,run.id+':baseline',function(){select(run.id);openReport('initial',0,'backtests');});if(run.baseline&&run.baseline.assessment)overviewGoals(baseline.cells[5],run.baseline.assessment.goals||[],{goals:[],label:'Unchanged start'},false);nativeLogButton(baseline.cells[0],run,[job]);backtestButton(baseline.cells[0],run,job);});
      cycles.forEach(function(cycle){var jobs=run.jobs.filter(function(job){return job.round===cycle.round;}),optimizers=jobs.filter(function(job){return job.kind==='optimizer';}),validations=jobs.filter(function(job){return job.kind!=='optimizer';}),key=run.id+':'+cycle.round;
        var round=line(['Loop '+(cycle.round+1),cycle.status,'',duration(cycle.duration_seconds),cycle.completed_backtests+' / '+cycle.backtests,numeric(cycle.score),numeric(cycle.score_delta),''],1,key,function(){select(run.id);openReport('cycle',cycle.round,'overview');});overviewObserver(round.cells[9],run,cycle.round,'observer_holdout');overviewObserver(round.cells[10],run,cycle.round,'observer_full_range');var reference=comparisonReference(run,cycles,cycle);overviewGoals(round.cells[5],cycle.goals||[],reference,completeComparison(cycle));round.cells[7].replaceChildren();overviewChange(round.cells[7],completeComparison(cycle)?cycle.score_delta:null,'max','score',reference.label);round.cells[3].dataset.clock=key;disclosure(round.cells[0],key,renderWorkflow);
        nativeLogButton(round.cells[0],run,optimizers);
        if(run.ended&&canContinueRound(cycle))continueButton(round.cells[0],run,cycle.round);
        if(!expanded.has(key))return;
        optimizers.forEach(function(job,index){var native=line([optimizers.length>1?'Optimizer Variant '+(index+1):'Optimizer',job.display_status||job.status,executionName(job.execution||run.settings.execution),duration(job.ended_at&&job.created_at?job.ended_at-job.created_at:null),'', '', '',dateText(job.created_at)],2,key+':'+job.operation,function(){select(run.id);openReport('cycle',cycle.round,'changes');diffOperation=job.operation;reportSignature='';updateReport();});nativeLogButton(native.cells[0],run,[job]);});
        observerLines(cycle.round,2);
        validations.forEach(function(job,index){var native=line([(job.kind==='holdout'?'Final holdout':'Comparison backtest')+' '+(index+1),job.display_status||job.status,executionName(job.execution||'cpu'),duration(job.ended_at&&job.created_at?job.ended_at-job.created_at:null),'','','',dateText(job.created_at)],2,key+':'+job.operation,function(){select(run.id);openReport('cycle',cycle.round,'backtests');});var observation=(cycle.observations||[]).find(function(item){return item.operation===job.operation;});if(observation&&observation.assessment)overviewGoals(native.cells[5],observation.assessment.goals||[],reference,observation.assessment.comparable!==false&&observation.assessment.simulation_complete!==false);nativeLogButton(native.cells[0],run,[job]);backtestButton(native.cells[0],run,job);});
      });
    });
    Array.from(body.rows).forEach(function(row){
      var cell=row.cells[0],controls=Array.from(cell.children).filter(function(node){return node.tagName==='BUTTON';});
      if(!controls.length)return;
      var actions=element('div',undefined,'loop-row-actions');
      controls.forEach(function(control){
        if(control.classList.contains('loop-disclosure'))actions.prepend(control);
        else actions.appendChild(control);
      });
      controls.filter(function(control){return control.textContent==='Continue';}).forEach(function(control){actions.appendChild(control);});
      cell.appendChild(actions);
    });
    host.querySelector('.loop-table-scroll').scrollLeft=scrollLeft;if(focusKey){var replacement=Array.from(body.children).find(function(row){return row.dataset.key===focusKey;});if(replacement)replacement.focus({preventScroll:true});}
    if(!body.children.length){var empty=element('tr'),cell=element('td',ended?'No finished loops.':'No queued or active loops.');cell.colSpan=11;empty.appendChild(cell);body.appendChild(empty);}
  }
  function syncContext() {
    ['loops-config','loops-queue','loops-results','loops-knowledge'].forEach(function(panel){var context=document.getElementById('ctx-'+panel);if(context){context.hidden=state.panel!==panel;context.style.display=context.hidden?'none':'';}});
    byId('nav-count-knowledge').hidden=state.panel!=='loops-knowledge';
    selectedDefinitions.forEach(function(name){if(!definitions.some(function(row){return row.name===name;}))selectedDefinitions.delete(name);});
    var count=selectedDefinitions.size,single=definitions.find(function(row){return selectedDefinitions.has(row.name);});
    byId('edit-config').disabled=editorVisible||count!==1;byId('duplicate-config').disabled=editorVisible||count!==1;byId('queue-config').disabled=editorVisible||queueing||!count;byId('delete-config').disabled=editorVisible||!count;
    var currentRuns=rows.filter(function(row){return selectedRuns.has(row.id) && (state.panel==='loops-results'?row.ended:!row.ended);});var run=currentRuns[0];['queue','results'].forEach(function(panel){byId(panel+'-edit').disabled=currentRuns.length!==1||!run;});
    ['queue','results'].forEach(function(panel){['observer_holdout','observer_full_range'].forEach(function(kind){byId(panel+'-compare-'+kind).disabled=currentRuns.length!==1||!hasEvaluationResults(run,kind);});});
    ['queue','results'].forEach(function(panel){var targets=rows.filter(function(row){return selectedRuns.has(row.id) && (panel==='results'?row.ended:!row.ended);});var node=byId(panel+'-delete');node.disabled=!targets.length||targets.some(function(row){return !row.deletable;});node.title=targets.some(function(row){return !row.deletable;})?'Stop the loop and wait for collection to finish before deleting it.':'';});
    ['start','pause','resume','stop'].forEach(function(action){var eligible=rows.filter(function(row){return selectedRuns.has(row.id) && (action==='start'?row.status==='queued':action==='pause'?['running','finishing'].includes(row.status):action==='resume'?row.status==='paused':!row.ended && row.status!=='stopping');});byId('queue-'+action).disabled=!eligible.length;});
    document.querySelectorAll('[data-create-ai-loop]').forEach(function(button){button.disabled=!nativeSource(button.dataset.createAiLoop);});
    document.querySelectorAll('.ctx-actions [data-busy]').forEach(function(button){button.disabled=true;});byId('form').hidden=!editorVisible;byId('config-list').hidden=editorVisible;root.querySelector('.panel-toolbar').hidden=editorVisible;
  }
  function nativeSource(panel) {
    var selected=panel==='configs'?Array.from(state.selectedConfigs):panel==='queue'?Array.from(state.selectedQueue):Array.from(state.selectedResults);
    if(selected.length!==1)return null;var id=selected[0];return panel==='queue' && id.startsWith('vast:')?{kind:'cloud',id:id.slice(5)}:{kind:{configs:'config',queue:'queue',results:'result'}[panel],id:id};
  }
  function onPanel() { if(!root)return;syncContext();if(isLoopPanel())watch(); }
  async function action(id, name) { try { var body={action:name};if(name==='start')body.selection=await PBGuiAI.ensureSelection();await request('/' + id + '/action', body);uiError='';await poll(); } catch (exc) { error(exc); } }
  async function poll() {
    var token=++generation;
    if(state.panel==='loops-instructions'){await PBGuiLoopInstructions.refresh();return;}
    var loadKnowledge=state.panel==='loops-knowledge';
    try { var values=await Promise.all([request(''),request('/configs'),loadKnowledge?request('/knowledge?limit=200').then(function(data){return {data:data};},function(exc){return {error:exc.message};}):null]);if(token!==generation)return;rows=values[0].loops;definitions=values[1].configs;
      if(loadKnowledge){knowledgeError=values[2].error||'';if(values[2].data){knowledgeEntries=values[2].data.entries||[];knowledgeLoaded=true;}}
      if(selected&&!rows.some(function(row){return row.id===selected;})){forgetRun(selected);uiError='The selected loop is no longer available.';}selectedRuns.forEach(function(id){if(!rows.some(function(row){return row.id===id;}))forgetRun(id);});render();await restoreNativeLog(); }
    catch(exc){if(token===generation)error(exc);}
  }
  async function watch() {
    clearTimeout(timer);
    var token = ++watchGeneration;
    if (stopped) return;
    if (!document.hidden && window.state && isLoopPanel()) await poll();
    if (!stopped && token === watchGeneration) timer = setTimeout(watch, 3000);
  }
  function mount() {
    if (!window.optimizeEditorAdapter || !optimizeEditorAdapter.isV8 || root) return;
    base = String(API_BASE).replace(/\/$/, '') + '/loops';
    PBGuiLoopInstructions.mount(base);
    ['config','queue','results'].forEach(function(kind){var panel='loops-'+kind;PANEL_META[panel]={title:'AI Loops '+({config:'Config',queue:'Queue',results:'Results'}[kind]),subtitle:''};var view=element('div',undefined,'view-panel loop-panel');view.id='panel-'+panel;document.getElementById('panel-configs').parentNode.appendChild(view);if(kind==='config')root=view;
      var nav=element('button','AI Loops '+({config:'Config',queue:'Queue',results:'Results'}[kind]),'sb-section');nav.dataset.panel=panel;nav.addEventListener('click',function(){selectPanel(panel);watch();});var count=element('span','0','sb-count');count.id='loop-nav-count-'+kind;count.style.marginLeft='auto';nav.appendChild(count);document.getElementById('sidebar-inner').insertBefore(nav,document.querySelector('#sidebar-inner .sb-sep'));
      var context=element('div',undefined,'ctx-actions');context.id='ctx-'+panel;context.hidden=true;document.getElementById('sidebar-inner').appendChild(context);
      var toolbar=element('div',undefined,'panel-toolbar');view.appendChild(toolbar);var filter=element('input',undefined,'panel-input');filter.type='text';filter.style.maxWidth='260px';filter.placeholder='Search loop name...';filter.setAttribute('aria-label','Search AI Loops '+kind);filter.value=workflow[kind==='config'?'config_filter':panel+'_filter']||'';toolbar.appendChild(filter);filter.addEventListener('input',function(){workflow[kind==='config'?'config_filter':panel+'_filter']=filter.value;persistWorkflow();renderWorkflow();});
      if(kind!=='config'){var status=element('select',undefined,'panel-input');status.style.width='auto';choices(status,[{id:'',label:'All statuses'}].concat((kind==='queue'?['queued','running','paused','finishing','stopping']:['completed','failed','unconfirmed','stopped']).map(function(value){return {id:value,label:value};})),workflow[panel+'_status']);status.setAttribute('aria-label','Status filter');toolbar.appendChild(status);status.addEventListener('change',function(){workflow[panel+'_status']=status.value;persistWorkflow();renderWorkflow();});var message=element('p','','loop-error');message.id=panel+'-message';view.appendChild(message);var table=element('div');table.id=panel+'-table';view.appendChild(table);}
    });
    PANEL_META['loops-knowledge']={title:'AI Loops Knowledge',subtitle:''};
    var knowledgeView=element('div',undefined,'view-panel loop-panel');knowledgeView.id='panel-loops-knowledge';root.parentNode.appendChild(knowledgeView);
    var knowledgeNav=element('button','AI Loops Knowledge','sb-section');knowledgeNav.dataset.panel='loops-knowledge';knowledgeNav.addEventListener('click',function(){selectPanel('loops-knowledge');watch();});
    var knowledgeCount=element('span','—','sb-count');knowledgeCount.id='loop-nav-count-knowledge';knowledgeCount.style.marginLeft='auto';knowledgeNav.appendChild(knowledgeCount);document.getElementById('sidebar-inner').insertBefore(knowledgeNav,document.querySelector('#sidebar-inner .sb-sep'));
    var knowledgeToolbar=element('div',undefined,'panel-toolbar'), knowledgeSearch=element('input',undefined,'panel-input');knowledgeSearch.type='search';knowledgeSearch.placeholder='Search findings...';knowledgeSearch.setAttribute('aria-label','Search learned findings');knowledgeSearch.style.maxWidth='360px';knowledgeSearch.value=workflow.knowledge_filter||'';knowledgeSearch.addEventListener('input',function(){workflow.knowledge_filter=knowledgeSearch.value;persistWorkflow();renderKnowledge();});knowledgeToolbar.appendChild(knowledgeSearch);knowledgeView.appendChild(knowledgeToolbar);
    knowledgeView.appendChild(element('p','Loading findings…','muted-line')).id='loop-knowledge-status';knowledgeView.appendChild(element('p','','loop-error')).id='loops-knowledge-message';knowledgeView.appendChild(element('div')).id='loop-knowledge-list';
    function side(kind,id,label,callback){var node=button(document.getElementById('ctx-loops-'+kind),label,function(){if(node.dataset.busy)return;node.dataset.busy='1';node.disabled=true;Promise.resolve().then(callback).catch(error).finally(function(){delete node.dataset.busy;syncContext();});});node.className='sb-btn';node.id='loop-'+id;return node;}
    side('config','new-config','New Config',newDefinition);
    side('config','edit-config','Edit Selected',function(){return openDefinition(definitions.find(function(row){return selectedDefinitions.has(row.name);}),false);});
    side('config','duplicate-config','Duplicate',function(){return openDefinition(definitions.find(function(row){return selectedDefinitions.has(row.name);}),true);});
    side('config','queue-config','Queue Selected',function(){return queueDefinitions(Array.from(selectedDefinitions));});
    side('config','delete-config','Delete Selected',deleteDefinitions).classList.add('danger');
    ['queue','results'].forEach(function(kind){side(kind,kind+'-edit','Edit Selected',function(){return editRun(rows.find(function(row){return selectedRuns.has(row.id) && (kind==='results'?row.ended:!row.ended);}));});side(kind,kind+'-delete','Delete Selected',function(){return deleteRuns(kind);}).classList.add('danger');});
    ['queue','results'].forEach(function(panel){['observer_holdout','observer_full_range'].forEach(function(kind){side(panel,panel+'-compare-'+kind,kind==='observer_holdout'?'Compare all Holdouts':'Compare all Full Time Ranges',function(){var run=rows.find(function(row){return selectedRuns.has(row.id)&&(panel==='results'?row.ended:!row.ended);});openEvaluationComparison(run,kind);});});});
    ['start','pause','resume','stop'].forEach(function(name){side('queue','queue-'+name,name[0].toUpperCase()+name.slice(1)+' Selected',async function(){for(var row of rows.filter(function(row){return selectedRuns.has(row.id);})) {if(name==='start'&&row.status!=='queued'||name==='pause'&&!['running','finishing'].includes(row.status)||name==='resume'&&row.status!=='paused'||name==='stop'&&(row.ended||row.status==='stopping'))continue;await action(row.id,name);}});});
    ['configs','queue','results'].forEach(function(panel){var node=button(document.getElementById('ctx-'+panel),'Create AI Loop',function(){var source=nativeSource(panel);if(source)createFromSource(source).catch(error);});node.className='sb-btn info';node.dataset.createAiLoop=panel;node.disabled=true;});
    root.appendChild(element('div')).id='loop-config-list';
    var form = element('form', undefined, 'loop-form editor-shell'); root.appendChild(form);form.id='loop-form';form.hidden=true;
    var setup = section(form, 'Starting configuration');
    setup.appendChild(element('p', 'New loop setup', 'muted-line')).id = 'loop-setup-source';
    var row=grid(setup);field(row,'name','Name','text','',{required:'',maxlength:120},8);
    row = grid(setup);
    field(row, 'config', 'PB8 configuration', 'select', undefined, undefined, 4);
    field(row, 'execution', 'Execution', 'select', undefined, undefined, 4);
    setup.appendChild(element('p', '', 'muted-line loop-resources')).id = 'loop-resources';

    var trading = section(form, 'Coins and positions'); row = grid(trading);
    field(row, 'coins', 'Coins', 'text', '', {placeholder: 'Enter coins or leave blank to use config'}, 4);
    var direction = field(row, 'direction', 'Direction', 'select');
    choices(direction, [{id:'config', label:'Use config'}, {id:'both', label:'Long and short'}, {id:'long', label:'Long'}, {id:'short', label:'Short'}]);
    field(row, 'positions', 'Total position capacity', 'number', '', {min:1, max:1000, placeholder:'From config'});
    row = grid(trading);
    var split = element('div', undefined, 'loop-position-split span-4'); split.id = 'loop-position-split'; row.appendChild(split);
    field(split, 'long', 'Long capacity', 'number', '', {min:0, placeholder:'Optional'});
    field(split, 'short', 'Short capacity', 'number', '', {min:0, placeholder:'Optional'});
    field(row, 'scenario', 'Allow AI to change Scenario Editor', 'checkbox', undefined, undefined, 2).checked = false;
    field(row, 'strategy', 'Allow AI to change strategy (ema_anchor / trailing_martingale)', 'checkbox', undefined, undefined, 2).checked = false;

    var goals = section(form, 'Result goals');
    var goalGrid = element('div', undefined, 'loop-goal-grid'); goals.appendChild(goalGrid);
    ['gain','drawdown','uptrend','consistency','trades','robustness'].forEach(function (name) {
      var label = {gain:'Gain',drawdown:'Low drawdown',uptrend:'Stable uptrend',consistency:'Consistent time windows',trades:'Enough trades',robustness:'Robust across coins'}[name];
      var pair = element('div', undefined, 'loop-goal'); goalGrid.appendChild(pair);
      var input = field(pair, name, label, 'checkbox'); input.checked = ['gain','drawdown','uptrend'].includes(name);
      var target = field(pair, name + '-target', 'Target', 'number', '', {step:'any', placeholder:'Optional · metric units'});
      target.disabled = !input.checked;
      input.addEventListener('change', function () { target.disabled = !input.checked; });
    });
    row = grid(goals); field(row, 'goals', 'Additional goals and priorities', 'textarea', '', {maxlength:8000, placeholder:'Describe additional objectives or priorities…'}, 8);

    var limits = section(form, 'Run limits'); row = grid(limits);
    var runMode = field(row, 'run-mode', 'Limit per optimizer run', 'select', undefined, undefined, 4);
    choices(runMode, [{id:'config', label:'Use config'}, {id:'iters', label:'Iterations (iters)'}, {id:'proxy', label:'GPU proxy evaluations'}, {id:'hours', label:'Hours'}]);
    field(row, 'run-iters', 'Iterations per run', 'number', 100000, {min:1, max:10000000, step:1}, 4);
    field(row, 'run-proxy', 'Proxy evaluations per run', 'number', 100000, {min:1, max:1000000000, step:1}, 4);
    field(row, 'run-hours', 'Hours per run', 'number', 1, {min:.001, max:168, step:'any'}, 4);
    row = grid(limits);
    field(row, 'runs', 'Maximum optimizer attempts', 'number', 10, {min:1, max:200});
    field(row, 'hours', 'Total loop hours', 'number', '', {min:.1, max:168, step:'any'});
    field(row, 'parallel', 'Parallel variants', 'number', 1, {min:1, max:16});
    field(row, 'patience', 'Cycles without improvement', 'number', 3, {min:1, max:100});
    row = grid(limits);
    field(row, 'validations', 'Maximum validation backtests', 'number', 30, {min:1, max:1000}, 4);
    field(row, 'candidates', 'Candidates validated per cycle', 'number', 3, {min:1, max:12}, 4);

    var startBar = element('div', undefined, 'loop-start-bar'); form.appendChild(startBar);
    startBar.appendChild(element('span','Queue authorizes automatic config changes, jobs and AI usage.','muted-line'));var save=button(startBar,'Save',function(){saveDefinition(false);});save.id='loop-save';var cancel=button(startBar,'Cancel',function(){editorVisible=false;workflow.draft=null;persistWorkflow();editorLocation();renderWorkflow();});
    var start = element('button','Save & Queue','btn primary'); start.id = 'loop-start'; start.type = 'submit'; startBar.appendChild(start);
    root.appendChild(element('p', '', 'loop-error')).id = 'loop-message';
    makeReportDialog();
    form.addEventListener('submit', function (event) { event.preventDefault(); saveDefinition(true); });
    ['input','change'].forEach(function (event) { form.addEventListener(event, function () { formEdited = true; setupGeneration++; saveNavigation();saveDraft();byId('setup-source').textContent='Edited'; }); });
    byId('config').addEventListener('change', function () { editorSource=null;sourceDefaults=null;sourceDigest=null;loadOptions().then(saveDraft).catch(error); });
    byId('coins').addEventListener('input', showConfigValues); byId('positions').addEventListener('input', showConfigValues);
    window.addEventListener('pbgui:ai-action-completed', function () { if (isLoopPanel()) loadOptions(true).catch(error); });
    direction.addEventListener('change', syncDirection); syncDirection();
    byId('hours').addEventListener('input', function () { hoursEdited = true; syncRunLimit(); });
    runMode.addEventListener('change', syncRunLimit); syncRunLimit();
    byId('execution').addEventListener('change', function () { executionSummary(); if (this.value === 'vast' && Number(byId('hours').value) > Number(byId('hours').max)) byId('hours').value = byId('hours').max; syncRunLimit(); syncStart(); });
    selected = new URL(location.href).searchParams.get('loop_id') || workflow.selected || '';selectedDefinitions=new Set(workflow.config_selection||[]);selectedRuns=new Set(workflow.run_selection|| (selected?[selected]:[]));expanded=new Set(workflow.expanded||[]);
    var reportUrl=new URL(location.href);reportView=['cycle','initial','run'].includes(reportUrl.searchParams.get('loop_view'))?reportUrl.searchParams.get('loop_view'):'';reportRound=/^\d{1,4}$/.test(reportUrl.searchParams.get('loop_cycle')||'')?Number(reportUrl.searchParams.get('loop_cycle')):0;reportTab=Object.keys(tabNames).includes(reportUrl.searchParams.get('loop_tab'))?reportUrl.searchParams.get('loop_tab'):'overview';diffOperation=/^[0-9a-f]{32}$/.test(reportUrl.searchParams.get('loop_variant')||'')?reportUrl.searchParams.get('loop_variant'):'';diffBaseline=/^(initial|[0-9a-f]{32})$/.test(reportUrl.searchParams.get('loop_compare')||'')?reportUrl.searchParams.get('loop_compare'):'';
    document.addEventListener('visibilitychange', function () { if (!document.hidden) { if (isLoopPanel()) loadOptions(true).catch(error); watch(); } }); window.addEventListener('hashchange', function () { if (isLoopPanel()) { loadOptions(true).catch(error); watch(); } });
    window.addEventListener('pagehide', function (event) { if(reportWindow){if(event.persisted)reportWindow.cancel();else reportWindow.dispose();} stopped = true; if(diffRequest)diffRequest.abort(); diffGeneration++; clearTimeout(timer); watchGeneration++; generation++; optionsGeneration++; });
    window.addEventListener('pageshow', function (event) { if (event.persisted) { stopped = false; watch(); } });
    optionsReady = loadOptions(true);
    optionsReady.then(async function(){
      if(workflow.draft && new URL(location.href).searchParams.has('loop_edit')){var draft=workflow.draft;editorSource=draft.source;sourceDefaults=draft.defaults;sourceDigest=draft.digest || null;editingName=draft.editing_name;editingRevision=draft.revision;editingQueue=draft.queue_id||'';byId('name').value=draft.name;await loadSettings({settings:draft.settings});showEditor(false);}
      await poll();
    }).catch(error);watch();
    function finishSelection(){document.querySelectorAll('[data-selecting]').forEach(function(table){delete table.dataset.selecting;delete table._loop_range;});persistWorkflow();syncContext();}
    document.addEventListener('pointerup',finishSelection);document.addEventListener('pointercancel',finishSelection);window.addEventListener('blur',finishSelection);
    var nativeObserver=new MutationObserver(syncContext);['configs-tbody','queue-tbody','results-tbody'].forEach(function(id){var node=document.getElementById(id);if(node)nativeObserver.observe(node,{childList:true,subtree:true,attributes:true,attributeFilter:['class']});});window.addEventListener('pagehide',function(event){if(!event.persisted)nativeObserver.disconnect();});
    document.addEventListener('click',function(){queueMicrotask(syncContext);});
    document.addEventListener('keydown',function(){queueMicrotask(syncContext);});
    var nativeLogPanel=document.getElementById('log-panel'),nativeLogObserver=new MutationObserver(function(){if((logOperation || logOpening) && !nativeLogPanel.classList.contains('visible'))clearNativeLog();});
    nativeLogObserver.observe(nativeLogPanel,{attributes:true,attributeFilter:['class']});
    window.addEventListener('pagehide',function(event){logGeneration++;logOpening=false;if(!event.persisted)nativeLogObserver.disconnect();});

  }
  window.PBGuiLoopOptimizer = {mount:mount,onPanel:onPanel,createFromSource:createFromSource};
}());
