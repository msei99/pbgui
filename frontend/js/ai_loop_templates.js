(function () {
  'use strict';
  function node(tag, text) {
    var n = document.createElement(tag);
    if (text != null) n.textContent = text;
    return n;
  }
  var marketsUrl = new URL('../../api/v8/coins/status', document.currentScript.src).href;
  var optionsUrl = new URL('../../api/ai/loop-template-options', document.currentScript.src).href;
  var optimizeUrl = new URL('../../api/optimize-v8/', document.currentScript.src).href;
  async function requestOptions() {
    var response = await fetch(optionsUrl, {credentials: 'same-origin', cache: 'no-store'});
    var data = await response.json();
    if (!response.ok) throw new Error(data.detail || 'Cannot load template choices');
    return data;
  }
  var instance;
  function attach() {
    var root = node('div'); root.className = 'pbg-modal-overlay pai-template-overlay';
    root.setAttribute('aria-hidden', 'true');
    var dialog = node('section'); dialog.className = 'pbg-modal-window pai-template-window';
    dialog.setAttribute('role', 'dialog'); dialog.setAttribute('aria-modal', 'true');
    dialog.setAttribute('aria-labelledby', 'pai-template-title');
    var head = node('div'); head.className = 'pbg-modal-head';
    var title = node('h3', 'AI Loop Template'); title.id = 'pai-template-title';
    var dismiss = node('button', '×'); dismiss.type = 'button'; dismiss.className = 'pbg-modal-close';
    dismiss.setAttribute('aria-label', 'Close AI Loop Template'); head.append(title, dismiss);
    var panel = node('form'); panel.id = 'pai-loop-template-form';
    panel.className = 'pbg-modal-body pai-loop-template';
    dialog.append(head, panel); root.appendChild(dialog); document.body.appendChild(root);
    var trigger = null, sending = false, sendError = '';
    var floating = window.PBGuiFloatingWindow.attach(dialog, head, {storage_key: 'pbgui.ai.loop-template.geometry'});
    var fields = {}, labels = {}, verified = false, marketReady = false, marketGeneration = 0, configs = [], loaded = false, generation = 0;
    var marketAbort = null, marketRetryTimer = null, marketError = '';
    var grid = node('div'); grid.className = 'form-row pai-template-grid'; panel.appendChild(grid);
    function field(key, label, choices, value, type, min, max) {
      var wrap = node('div'), caption = node('label', label), input = node(choices ? 'select' : 'input');
      wrap.className = 'form-group'; input.className = 'panel-input';
      input.id = 'pai-template-' + key; caption.htmlFor = input.id; fields[key] = input;
      if (choices) choices.forEach(function (entry) {
        var option = node('option', entry[1]); option.value = entry[0]; input.appendChild(option);
      });
      else { input.type = type || 'text'; input.required = true; }
      if (min != null) input.min = min;
      if (max != null) input.max = max;
      if (type === 'number') input.step = 'any';
      if (value != null) input.value = value;
      wrap.append(caption, input); grid.appendChild(wrap); return input;
    }
    field('source', 'Starting configuration', [['', 'PB8 defaults']]);
    field('direction', 'Direction', [['long', 'Long'], ['short', 'Short'], ['both', 'Both']], 'long');
    field('strategy', 'Strategy', [['ema_anchor', 'EMA Anchor'], ['trailing', 'Trailing Martingale'], ['both', 'Both — AI chooses']], 'both');
    field('hsl', 'HSL', [['off', 'Off'], ['on', 'On'], ['source', 'From starting configuration']], 'off');
    field('twe', 'Maximum TWE per active side', null, 8, 'number', .01, 1000);
    field('positions', 'Maximum simultaneous coins', null, '', 'number', 1, 1000).required = false;
    fields.positions.placeholder = 'All selected coins';
    field('rounds', 'Rounds', null, 25, 'number', 1, 200).step = '1';
    field('validations', 'CPU training validation budget', null, 100, 'number', 1, 1000).step = '1';
    field('proxy', 'GPU proxy evaluations / round', null, 100000, 'number', 1, 1000000000).step = '1';
    field('execution', 'Execution', [['vast', 'Vast GPU'], ['gpu', 'Existing GPU worker']], 'vast');
    field('hours', 'Time limit (hours)', null, 12, 'number', .1, 168);
    field('budget', 'Rental budget (USD)', null, 5, 'number', .1, 100);
    field('hourly', 'Maximum rental USD / hour', null, .3, 'number', .01, 100);
    field('windows', 'Training windows', [['auto', 'Automatic from selected coins'], ['custom', 'Manual date range'], ['source', 'From starting configuration']], 'auto');
    field('timeframes', 'Number of training timeframes', null, 8, 'number', 1, 48).step = '1';
    field('holdout', 'Use holdout', null, 'on', 'checkbox').required = false;
    fields.holdout.checked = true;
    fields.holdout.addEventListener('change', function () { fields.holdout.value = fields.holdout.checked ? 'on' : 'off'; });
    field('start', 'Available period start (UTC)', null, '', 'date');
    field('end', 'Available period end (UTC)', null, '', 'date');
    field('objective', 'Optimization goal', [['ai', 'AI choice'], ['gain', 'Maximum gain'], ['balanced', 'Gain (25% DD)'], ['adg', 'Average daily growth (ADG)'], ['custom', 'Custom']], 'ai');
    field('drawdown', 'Maximum drawdown (%)', null, 50, 'number', .01, 100);
    field('custom', 'Custom objective', null, '').required = false;
    fields.custom.maxLength = 2000;
    field('notes', 'Additional instructions', null, '').required = false;
    fields.notes.maxLength = 2000;
    var defaultValues = {};
    Object.keys(fields).forEach(function (key) { defaultValues[key] = fields[key].value; });
    function multiselect(id, label, parent) {
      var group = node('div'); group.className = 'form-group';
      var caption = node('label', label);
      var clear = node('button', '×'); clear.type = 'button'; clear.className = 'ms-clear-btn';
      clear.setAttribute('aria-label', 'Clear ' + label.toLowerCase());
      clear.addEventListener('click', function () { controller.clear(id); }); caption.appendChild(clear);
      var wrap = node('div'); wrap.id = id; wrap.className = 'ms-wrap';
      var input = node('input'); input.className = 'ms-input'; input.placeholder = 'Select ' + label.toLowerCase() + '...';
      input.setAttribute('aria-label', label); input.autocomplete = 'off';
      var dropdown = node('div'); dropdown.className = 'ms-dropdown';
      wrap.append(input, dropdown); group.append(caption, wrap); parent.appendChild(group);
    }
    multiselect('pai-template-exchanges', 'Exchanges', grid);
    grid.insertBefore(document.getElementById('pai-template-exchanges').parentNode, grid.children[1]);
    multiselect('pai-template-coins', 'Coins', panel);
    var count = node('span'); count.className = 'pai-template-count'; panel.appendChild(count);
    var dateStatus = node('p'); dateStatus.className = 'pai-template-date-status'; dateStatus.setAttribute('role', 'status'); panel.appendChild(dateStatus);
    var windowPlan = null, windowSignature = '', previewError = '', previewEditor = null, sourceGeneration = 0, sourceSignature = '', windowContext = '', windowEdited = false;
    var dateBasis = null, restoredEdits = null;
    function marketContext() { return JSON.stringify([exchanges().slice().sort(),coins().slice().sort(),fields.source.value]); }
    function hasDateBasis() {
      return !!dateBasis && dateBasis.context === marketContext() &&
        dateBasis.start_date === fields.start.value && dateBasis.end_date === fields.end.value;
    }
    var previewTitle = node('h4', 'Window preview (inclusive UTC dates)'); panel.appendChild(previewTitle);
    var previewStatus = node('p'); previewStatus.id = 'pai-template-window-status'; previewStatus.className = 'pai-template-date-status'; previewStatus.setAttribute('role', 'status'); panel.appendChild(previewStatus);
    var previewHost = node('div'); previewHost.id = 'pai-template-window-preview'; panel.appendChild(previewHost);
    function signature() { return JSON.stringify([fields.windows.value,fields.source.value,fields.start.value,fields.end.value,fields.timeframes.value,fields.holdout.checked]); }
    function disposePreview() { if (previewEditor) previewEditor.dispose(); previewEditor = null; }
    function checkWindows() {
      if (fields.windows.value === 'auto' && !hasDateBasis()) throw new Error(dateError || 'Wait for available dates for the current market selection.');
      if (!windowPlan || windowSignature !== signature()) throw new Error(previewError || 'Wait for a current window preview.');
      windowPlan.use_holdout = fields.holdout.checked; windowPlan.training_windows = Number(fields.timeframes.value);
      return window.PBGuiAILoopWindows.validate(windowPlan);
    }
    function renderPreview() {
      disposePreview(); previewHost.replaceChildren();
      if (!windowPlan) return;
      previewEditor = window.PBGuiScenarioVisual.mount(previewHost, {fixedWindows:true,windows:windowPlan.windows,
        context:{start_date:windowPlan.start_date,end_date:windowPlan.end_date,exchanges:exchanges(),coins:coins()},
        apiBase:optimizeUrl.replace(/\/$/,''),
        validate:function(windows){window.PBGuiAILoopWindows.validate(Object.assign({},windowPlan,{windows:windows}));},
        change:function(windows){windowPlan.windows=windows;windowEdited=true;persist();updateValidation();},apply:async function(){return true;}});
    }
    function updatePreview() {
      if (fields.windows.value === 'source') {
        if(windowPlan && sourceSignature===fields.source.value){windowSignature=signature();windowPlan.use_holdout=fields.holdout.checked;}
        loadSourceWindows(); return;
      }
      sourceGeneration++; sourceSignature = '';
      if (fields.windows.value === 'auto' && !hasDateBasis()) {
        windowPlan = null; windowSignature = ''; previewError = '';
        previewStatus.textContent = dateError || 'Waiting for available training dates…';
        renderPreview(); return;
      }
      var next = signature();
      if (windowSignature === next && windowPlan) return;
      try {
        windowPlan = window.PBGuiAILoopWindows.generate(fields.start.value,fields.end.value,Number(fields.timeframes.value),fields.holdout.checked);
        windowContext=marketContext();windowEdited=false;
        windowSignature = next; previewError = ''; previewStatus.textContent = 'Windows updated for the selected period and holdout choice.';
      } catch (exc) { windowPlan = null; windowSignature = ''; previewError = exc.message; previewStatus.textContent = previewError; }
      renderPreview();
    }
    async function loadSourceWindows() {
      var key = fields.source.value;
      if (!key || sourceSignature === key) return;
      sourceSignature = key; var current = ++sourceGeneration;
      disposePreview(); previewHost.replaceChildren(); windowPlan = null; previewError = 'Loading starting configuration windows…';
      previewStatus.textContent = previewError; updateValidation();
      try {
        var data = await dateRequest('configs/' + encodeURIComponent(key));
        if (current !== sourceGeneration || fields.windows.value !== 'source') return;
        var config = data.config || {}, bt = config.backtest || {}, metadata = (config.pbgui || {}).scenario_template || {};
        var parameters = metadata.parameters || {};
        var windows = parameters.windows || (bt.suite_enabled ? bt.scenarios || [] : [bt]).map(function(row,i){return {
          id:'source_'+i,label:row.label||'train_'+(i+1),role:'training',start_date:row.start_date||bt.start_date,end_date:row.end_date||bt.end_date,scenario:bt.suite_enabled?Object.assign({},row):{}};
        }).concat((metadata.holdout_scenarios || []).map(function(row,i){return {id:'holdout_'+i,label:row.label||'holdout_'+(i+1),role:'holdout',start_date:row.start_date,end_date:row.end_date,scenario:Object.assign({},row)};}));
        fields.start.value = parameters.start_date || bt.start_date || ''; fields.end.value = parameters.end_date || bt.end_date || '';
        fields.timeframes.value = windows.filter(function(w){return w.role==='training';}).length;
        windowPlan = {version:1,start_date:fields.start.value,end_date:fields.end.value,training_windows:Number(fields.timeframes.value),use_holdout:fields.holdout.checked,windows:windows};
        windowSignature = signature(); previewError = ''; previewStatus.textContent = 'Starting configuration windows loaded.';
        renderPreview(); persist(); updateValidation();
      } catch(exc) { if(current===sourceGeneration){previewError=exc.message;previewStatus.textContent=previewError;updateValidation();} }
    }
    var dateGeneration = 0, dateTimer = null, dateJob = null, dateSignature = '', dateReady = false, dateBusy = false;
    var dateError = '', dateDeadline = 0, dateDeadlineTimer = null, dateRequests = new Set();
    var DATE_REQUEST_MS = 15000, DATE_LOOKUP_MS = 210000;
    var controller = window.PBGuiEditorShared.createMultiselectController({
      controllerName: 'ai-loop-template', coinIds: {'pai-template-coins': true},
      formatTagLabel: function (ctx) { return labels[ctx.value] || ctx.value; },
      formatOptionLabel: function (ctx) { return labels[ctx.value] || ctx.value; },
      onAfterRender: function (id) {
        count.textContent = coins().length + ' selected'; persist();
        if (id === 'pai-template-exchanges') loadMarkets();
        else if (marketReady) { updateValidation(); queueDates(); }
      }
    });
    function exchanges() { return controller.getValues('pai-template-exchanges'); }
    function coins() { return controller.getValues('pai-template-coins'); }
    controller.rebuild('pai-template-exchanges', optimizeExchangeOptions([]), ['binance'], {silent: true});
    controller.rebuild('pai-template-coins', [], [], {silent: true});
    var error = node('div'); error.className = 'pai-template-error'; error.setAttribute('role', 'status');
    var actions = node('div'); actions.className = 'pai-template-actions';
    var generate = node('button', 'Send to AI Chat'); generate.type = 'submit'; generate.setAttribute('form', panel.id); generate.className = 'btn primary';
    var close = node('button', 'Close'); close.type = 'button'; close.className = 'btn';
    var reset = node('button', 'Reset to defaults'); reset.type = 'button'; reset.className = 'btn';
    var retryDates = node('button', 'Retry date lookup'); retryDates.type = 'button'; retryDates.className = 'btn'; retryDates.hidden = true;
    retryDates.addEventListener('click', function () { queueDates(true); });
    reset.addEventListener('click', resetDefaults);
    actions.append(reset, retryDates, generate, close);
    var footer = node('div'); footer.className = 'pai-template-footer'; footer.append(error, actions); dialog.appendChild(footer);
    function updateValidation() {
      var state = controller.state['pai-template-coins'];
      verified = !coins().some(function (coin) {
        var meta = controller.getCoinMeta('pai-template-coins', coin);
        return (meta && meta.status === 'invalid') || !state.options.includes(coin);
      });
      var windowError = '';
      try { checkWindows(); } catch(exc) { windowError = exc.message; }
      if(!windowError){
        var used=windowPlan.windows.reduce(function(total,w){return total+(Date.parse(w.end_date)-Date.parse(w.start_date))/86400000+1;},0);
        var available=(Date.parse(windowPlan.end_date)-Date.parse(windowPlan.start_date))/86400000+1;
        previewStatus.textContent=available>used?(available-used)+' unused calendar days between or outside windows.':'Windows cover the selected period without gaps.';
      }
      var automatic = fields.windows.value === 'auto';
      var waiting = automatic && (!dateReady || !hasDateBasis()) && !dateError;
      if (automatic && dateReady) dateStatus.textContent = windowError || 'Common market start including warmup → last complete UTC day.';
      if (waiting) previewStatus.textContent = dateStatus.textContent || 'Waiting for available training dates…';
      if (automatic && dateError) previewStatus.textContent = dateError;
      error.textContent = sendError || marketError || (automatic && dateError) || (!verified ? 'Some selected coins are unavailable; review the selection.' : waiting ? '' : windowError);
      retryDates.hidden = !automatic || !dateError;
      retryDates.disabled = !marketReady || !verified || !coins().length || !exchanges().length;
      generate.disabled = !!windowError || !verified || !state.options.length || !marketReady || (automatic && (!dateReady || !hasDateBasis()));
    }
    async function loadMarkets(retrying) {
      clearTimeout(marketRetryTimer); marketRetryTimer = null;
      if (marketAbort) marketAbort.abort();
      var current = ++marketGeneration;
      checkDateDeadline();
      invalidateDateContext();
      marketReady = false; verified = false; generate.disabled = true; marketError = '';
      if (!exchanges().length) { error.textContent = 'Select at least one exchange.'; return; }
      error.textContent = sendError || 'Loading coins…';
      var abort = new AbortController(), timedOut = false, received = false;
      marketAbort = abort;
      var timeout = setTimeout(function () { timedOut = true; abort.abort(); }, DATE_REQUEST_MS);
      try {
        var response = await fetch(marketsUrl, {method: 'POST', credentials: 'same-origin', cache: 'no-store',
          signal: abort.signal, headers: {'Content-Type': 'application/json'}, body: JSON.stringify({exchanges: exchanges(), coins: coins()})});
        received = true;
        var data = await response.json();
        if (current !== marketGeneration) return;
        if (!response.ok) throw new Error(data.detail || 'Cannot load coins');
        labels = {};
        (data.catalog || []).forEach(function (entry) { labels[entry.config_id] = entry.display || entry.coin || entry.config_id; });
        var input = document.querySelector('#pai-template-coins .ms-input');
        controller.rebuild('pai-template-coins', data.symbols || [], coins(), {silent: true});
        controller.applyCoinStatus('pai-template-coins', data.statuses || {});
        if (document.activeElement === input) controller.showDropdown('pai-template-coins', input.value);
        marketReady = true; updateValidation(); queueDates();
      } catch (e) {
        if (current === marketGeneration) {
          marketError = timedOut ? 'Market availability request timed out after 15 seconds.' : e.message;
          error.textContent = marketError;
          if (retrying !== true && (timedOut || !received) && root.classList.contains('is-open')) {
            marketRetryTimer = setTimeout(function () { loadMarkets(true); }, 1000);
          }
        }
      } finally {
        clearTimeout(timeout);
        if (marketAbort === abort) marketAbort = null;
      }
    }
    async function dateRequest(path, options, current) {
      var controller = new AbortController(), timedOut = false;
      if (current != null) dateRequests.add(controller);
      var timer = setTimeout(function () { timedOut = true; controller.abort(); }, DATE_REQUEST_MS);
      try {
        var response = await fetch(optimizeUrl + path, Object.assign({credentials:'same-origin',cache:'no-store'}, options || {}, {signal:controller.signal}));
        var data = await response.json();
        if (!response.ok) throw new Error(data.detail || 'Cannot determine training dates');
        return data;
      } catch (exc) {
        if (timedOut) throw new Error('Date lookup request timed out after 15 seconds.');
        throw exc;
      } finally { clearTimeout(timer); dateRequests.delete(controller); }
    }
    function stopDateJob(id) {
      if (id) dateRequest('ohlcv-start-dates/' + encodeURIComponent(id), {method:'DELETE',keepalive:true}).catch(function () {});
    }
    function cancelDates() {
      dateGeneration++; clearTimeout(dateTimer); dateTimer = null;
      clearTimeout(dateDeadlineTimer); dateDeadlineTimer = null; dateDeadline = 0;
      dateRequests.forEach(function (controller) { controller.abort(); }); dateRequests.clear();
      stopDateJob(dateJob); dateJob = null; dateSignature = ''; dateBusy = false; dateReady = false;
      dateError = ''; retryDates.hidden = true;
      generate.disabled = true;
    }
    function lastCompletedDay() { return new Date(Date.now() - 86400000).toISOString().slice(0,10); }
    function dateKey() { return JSON.stringify([exchanges().slice().sort(),coins().slice().sort(),fields.source.value,lastCompletedDay()]); }
    function invalidateDateContext() {
      if (fields.windows.value !== 'auto') return;
      var changed = (dateBasis && dateBasis.context !== marketContext()) ||
        (dateSignature && dateSignature !== dateKey()) || (windowPlan && windowContext !== marketContext());
      if (!changed || (hasDateBasis() && windowEdited && windowPlan && windowSignature === signature())) return;
      cancelDates(); dateBasis = null; restoredEdits = null;
      fields.start.value = ''; fields.end.value = ''; dateStatus.textContent = 'Waiting for valid market selection…';
      windowPlan = null; windowSignature = ''; windowEdited = false; updatePreview();
    }
    function checkDateDeadline() {
      if (dateBusy && dateDeadline && Date.now() >= dateDeadline) {
        failDates(new Error('Date lookup did not finish within 210 seconds.'), dateGeneration); return false;
      }
      return true;
    }
    function queueDates(force) {
      if (fields.windows.value !== 'auto') { cancelDates(); dateStatus.textContent = ''; updateValidation(); return; }
      if (!root.classList.contains('is-open')) return;
      invalidateDateContext();
      if (!marketReady) return;
      if (!verified || !coins().length || !exchanges().length) {
        cancelDates(); dateStatus.textContent = 'Select valid coins and exchanges to determine training dates.';
        updateValidation(); return;
      }
      if (!checkDateDeadline()) return;
      if(!force && hasDateBasis() && windowPlan && windowSignature===signature() && windowContext===marketContext() && (windowEdited || fields.end.value===lastCompletedDay())) {
        dateReady=true; updateValidation(); return;
      }
      var key = dateKey();
      if (!force && key === dateSignature && (dateBusy || dateReady || dateError)) return;
      cancelDates(); dateSignature = key; dateBusy = true;
      dateBasis = null;
      fields.start.value = ''; fields.end.value = ''; generate.disabled = true;
      windowPlan = null; windowSignature = ''; windowEdited = false; previewError = ''; renderPreview();
      dateStatus.textContent = 'Determining available training dates…';
      var current = dateGeneration;
      dateDeadline = Date.now() + DATE_LOOKUP_MS;
      dateDeadlineTimer = setTimeout(checkDateDeadline, DATE_LOOKUP_MS);
      updateValidation();
      dateTimer = setTimeout(function () { resolveDates(current); }, 250);
    }
    async function resolveDates(current) {
      try {
        var data = await dateRequest(fields.source.value ? 'configs/' + encodeURIComponent(fields.source.value) : 'configs/new-config', null, current);
        if (current !== dateGeneration) return;
        if (!data.config || typeof data.config !== 'object') throw new Error('PB8 configuration defaults are unavailable.');
        var config = data.config;
        config.backtest = Object.assign({}, config.backtest, {exchanges:exchanges(),start_date:'2010-01-01',end_date:lastCompletedDay(),suite_enabled:false,scenarios:[]});
        config.live = Object.assign({}, config.live, {approved_coins:{long:coins(),short:[]},ignored_coins:{long:[],short:[]}});
        var job = await dateRequest('ohlcv-start-dates', {method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({config:config})}, current);
        if (current !== dateGeneration) { stopDateJob(job.job_id); return; }
        if (!/^[a-f0-9]{12}$/.test(job.job_id || '')) throw new Error('Date lookup returned no valid job ID.');
        dateJob = job.job_id;
        handleDateJob(job, current);
      } catch (exc) { failDates(exc, current); }
    }
    function failDates(exc, current) {
      if (current !== dateGeneration) return;
      var key = dateSignature;
      cancelDates(); dateSignature = key;
      dateError = exc.message + ' Retry date lookup or choose Manual date range.';
      dateStatus.textContent = dateError;
      updateValidation();
    }
    function handleDateJob(job, current) {
      if (current !== dateGeneration) return;
      if (!checkDateDeadline()) return;
      if (dateSignature !== dateKey()) { queueDates(); return; }
      if (job.status === 'complete' || job.status === 'completed') {
        var result = (job.result || {}).start_date_options || {}, option = result.all_markets;
        if (!option || option.available !== true) {
          var detail = (option || {}).detail || 'No common start date is available for the selected markets.';
          [['missing_market_pairs','Unavailable markets'],['unknown_start_pairs','Unknown OHLCV start']].forEach(function (entry) {
            var pairs = result[entry[0]];
            if (Array.isArray(pairs) && pairs.length) detail += ' ' + entry[1] + ': ' + pairs.slice(0,20).map(function (pair) {
              return String(pair.exchange || '').slice(0,80) + '/' + String(pair.coin || '').slice(0,80);
            }).join(', ') + '.';
          });
          failDates(new Error(detail), current); return;
        }
        var end = lastCompletedDay(), plan;
        var keepEdits = restoredEdits && restoredEdits.context === marketContext();
        try {
          plan = window.PBGuiAILoopWindows.generate(option.start_date,end,Number(fields.timeframes.value),fields.holdout.checked);
          if (keepEdits) {
            plan.windows = restoredEdits.windows;
            window.PBGuiAILoopWindows.validate(plan);
          }
        }
        catch (exc) { failDates(exc, current); return; }
        fields.start.value = plan.start_date; fields.end.value = plan.end_date;
        dateBasis = {context:marketContext(),start_date:plan.start_date,end_date:plan.end_date};
        windowPlan = plan; windowSignature = signature(); windowContext = marketContext(); windowEdited = !!keepEdits; previewError = ''; restoredEdits = null;
        clearTimeout(dateDeadlineTimer); dateDeadlineTimer = null; dateDeadline = 0;
        renderPreview();
        dateJob = null; dateBusy = false; dateReady = true;
        dateStatus.textContent = 'Common market start including warmup → last complete UTC day.';
        persist(); updateValidation(); return;
      }
      if (!['queued','running','stopping'].includes(job.status)) { failDates(new Error(job.error || 'Date lookup did not complete.'), current); return; }
      dateStatus.textContent = (job.progress || {}).phase === 'complete' ? 'Finalizing available training dates…' : (job.progress || {}).message || 'Determining available training dates…';
      updateValidation();
      var id = dateJob;
      dateTimer = setTimeout(async function () {
        if (current !== dateGeneration || !checkDateDeadline()) return;
        try { handleDateJob(await dateRequest('ohlcv-start-dates/' + encodeURIComponent(id), null, current), current); }
        catch (exc) { failDates(exc, current); }
      }, 750);
    }
    function persist() {
      var values = {};
      Object.keys(fields).forEach(function (key) { if (!['notes', 'custom'].includes(key)) values[key] = fields[key].value; });
      try { sessionStorage.setItem('pbgui.ai.loop-template', JSON.stringify({schema_version: 4, values: values, exchanges: exchanges(), coins: coins(), window_plan:windowPlan, window_signature:windowSignature, window_context:windowContext, window_edited:windowEdited,date_basis:dateBasis,restored_edits:restoredEdits})); } catch (_) {}
    }
    function sync() {
      var defaults = !fields.source.value;
      fields.windows.querySelector('option[value="source"]').disabled = defaults;
      if (defaults && fields.windows.value === 'source') fields.windows.value = 'auto';
      var customDates = fields.windows.value !== 'source';
      ['start', 'end','timeframes'].forEach(function (key) { fields[key].required = customDates; fields[key].parentNode.hidden = !customDates; });
      fields.custom.parentNode.hidden = fields.objective.value !== 'custom'; fields.custom.required = fields.objective.value === 'custom';
      ['budget', 'hourly'].forEach(function (key) { fields[key].parentNode.hidden = fields.execution.value !== 'vast'; fields[key].required = fields.execution.value === 'vast'; });
    }
    fields.objective.addEventListener('change', function () {
      if (fields.objective.value !== 'custom') fields.drawdown.value = fields.objective.value === 'balanced' ? 25 : 50;
    });
    fields.source.addEventListener('change', function () {
      dateBasis = null; restoredEdits = null;
      windowPlan=null;windowSignature='';sourceSignature='';sourceGeneration++;disposePreview();
      var source = configs.find(function (c) { return c.name === fields.source.value; });
      fields.windows.value = source ? 'source' : 'auto';
      if (source) { fields.start.value = source.start_date || ''; fields.end.value = source.end_date || ''; }
      else { fields.start.value='';fields.end.value=''; }
    });
    ['start','end'].forEach(function (key) { fields[key].addEventListener('input', function () {
      if (fields.windows.value === 'auto') { fields.windows.value = 'custom'; cancelDates(); dateStatus.textContent = ''; sync(); updateValidation(); }
    }); });
    panel.addEventListener('change', function (event) {
      sendError='';
      if(previewHost.contains(event.target))return;
      sync(); updatePreview(); persist(); queueDates(); if (marketReady) updateValidation();
    });
    panel.addEventListener('input', function(event){
      if(['start','end','timeframes'].some(function(key){return event.target===fields[key];})){
        sendError='';
        updatePreview();persist();if(marketReady)updateValidation();
      }
    });
    function resetDefaults() {
      if (sending) return;
      cancelDates(); generation++; marketGeneration++;
      sendError = ''; marketError = ''; error.textContent = ''; dateStatus.textContent = '';
      Object.keys(defaultValues).forEach(function (key) { fields[key].value = defaultValues[key]; });
      fields.holdout.checked=true;windowPlan=null;windowSignature='';windowEdited=false;sourceSignature='';sourceGeneration++;
      dateBasis=null;restoredEdits=null;
      previewError='';previewStatus.textContent='';windowContext='';
      disposePreview();previewHost.replaceChildren();
      controller.rebuild('pai-template-exchanges', optimizeExchangeOptions([]), ['binance'], {silent: true});
      controller.rebuild('pai-template-coins', [], [], {silent: true});
      labels = {}; marketReady = false; verified = false;
      count.textContent = '0 selected'; sync(); persist();
      loadOptions(); loadMarkets();
    }
    function hide() {
      cancelDates();
      clearTimeout(marketRetryTimer); marketRetryTimer = null;
      if (marketAbort) marketAbort.abort();
      sourceGeneration++;if(!windowPlan)sourceSignature='';disposePreview();
      generation++; marketGeneration++; root.classList.remove('is-open'); root.setAttribute('aria-hidden', 'true');
      try { sessionStorage.removeItem('pbgui.ai.loop-template.open'); } catch (_) {}
      floating.cancel(); if (trigger && trigger.isConnected) trigger.focus();
    }
    window.addEventListener('pagehide', function () {
      cancelDates(); marketGeneration++; clearTimeout(marketRetryTimer);
      if (marketAbort) marketAbort.abort();
    });
    dismiss.addEventListener('click', hide);
    close.addEventListener('click', hide);
    root.addEventListener('keydown', function (event) {
      if (event.key === 'Escape') { event.preventDefault(); event.stopPropagation(); hide(); return; }
      if (event.key !== 'Tab') return;
      var focusable = Array.from(dialog.querySelectorAll('button,input,select,textarea,[tabindex]')).filter(function (n) {
        return !n.disabled && n.tabIndex >= 0 && n.getClientRects().length;
      });
      var first = focusable[0], last = focusable[focusable.length - 1];
      if (event.shiftKey && document.activeElement === first) { event.preventDefault(); last.focus(); }
      else if (!event.shiftKey && document.activeElement === last) { event.preventDefault(); first.focus(); }
    });
    document.addEventListener('focusin', function (event) {
      if (root.classList.contains('is-open') && !dialog.contains(event.target)) dismiss.focus();
    });
    async function loadOptions() {
      var current = ++generation;
      try {
        var data = await requestOptions();
        if (current !== generation) return;
        configs = data.configs || [];
        var sourceValue = fields.source.value;
        fields.source.replaceChildren(new Option('PB8 defaults', ''));
        configs.forEach(function (c) { fields.source.add(new Option(c.name, c.name)); });
        if (Array.from(fields.source.options).some(function (o) { return o.value === sourceValue; })) fields.source.value = sourceValue;
        else error.textContent = 'Starting configuration is unavailable; choose a new source.';
        sync();

      } catch (e) { if (current === generation) error.textContent = e.message; }
    }
    function open(button) {
      trigger = button || trigger; root.classList.add('is-open'); root.setAttribute('aria-hidden', 'false');
      try { sessionStorage.setItem('pbgui.ai.loop-template.open', '1'); } catch (_) {}
      if (!loaded) {
        try {
          var saved = JSON.parse(sessionStorage.getItem('pbgui.ai.loop-template') || 'null');
          if (saved) {
            Object.keys(saved.values || {}).forEach(function (key) {
              if (fields[key] && !['notes', 'custom', 'source'].includes(key)) fields[key].value = saved.values[key];
            });
            var values = saved.values || {};
            if (!['ai', 'gain', 'adg', 'balanced', 'custom'].includes(fields.objective.value)) fields.objective.value = 'ai';
            fields.holdout.checked = values.holdout !== 'off';
            if(saved.schema_version>=3 && saved.window_plan){windowPlan=saved.window_plan;windowSignature=saved.window_signature||'';windowContext=saved.window_context||'';windowEdited=!!saved.window_edited;}
            if(saved.schema_version>=4){dateBasis=saved.date_basis||null;restoredEdits=saved.restored_edits||null;}
            else if(fields.windows.value==='auto' && windowPlan && windowEdited){restoredEdits={context:windowContext,windows:windowPlan.windows};}
            if (Number(saved.schema_version || 0) < 2 && fields.windows.value === 'custom' && (!fields.start.value || !fields.end.value)) {
              fields.windows.value = 'auto';
            }
            var chosen = saved.exchanges || (values.exchange ? [values.exchange] : ['binance']);
            controller.rebuild('pai-template-exchanges', optimizeExchangeOptions(chosen), chosen, {silent: true});
            controller.rebuild('pai-template-coins', saved.coins || [], saved.coins || [], {silent: true});
            if (saved.values.source) fields.source.add(new Option(saved.values.source, saved.values.source, true, true));
            if(windowPlan && fields.windows.value==='source' && windowSignature===signature())sourceSignature=fields.source.value;
          }
        } catch (_) {}
        loaded = true;
      }
      sync(); updatePreview(); if(windowPlan&&!previewEditor)renderPreview(); count.textContent = coins().length + ' selected';
      floating.fit(); dismiss.focus(); loadOptions(); loadMarkets();
    }
    window.addEventListener('focus', function () { if (root.classList.contains('is-open') && !sending) { loadOptions(); loadMarkets(); } });
    document.addEventListener('visibilitychange', function () { if (!document.hidden && root.classList.contains('is-open') && !sending) { loadOptions(); loadMarkets(); } });
    panel.addEventListener('submit', async function (event) {
      event.preventDefault(); sendError = ''; error.textContent = '';
      if (sending) return;
      if (!panel.reportValidity()) return;
      if (!verified || !exchanges().length) { error.textContent = 'Wait for a valid market selection.'; return; }
      var selectedCoins = coins().sort(), rounds = Number(fields.rounds.value), positions = Number(fields.positions.value || selectedCoins.length);
      if (!selectedCoins.length || selectedCoins.length > 200) { error.textContent = 'Select between 1 and 200 coins.'; return; }
      if (!Number.isInteger(positions) || positions > selectedCoins.length) { error.textContent = 'Maximum simultaneous coins must not exceed selected coins.'; return; }
      var reviewed;
      try { reviewed = checkWindows(); } catch(exc) { error.textContent=exc.message;return; }
      reviewed = JSON.parse(JSON.stringify(reviewed));
      delete reviewed.objective;
      reviewed.max_twe = Number(fields.twe.value);
      reviewed.max_validations = Number(fields.validations.value);
      var timeframeCount = reviewed.training_windows;
      var trainingWindows = reviewed.windows.filter(function(w){return w.role==='training';}).map(function(w){return w.start_date+' → '+w.end_date;});
      var source = fields.source.value;
      var text = 'Create a new optimization loop ' + (source ? 'using optimizer configuration ' + JSON.stringify(source) : 'from PB8 defaults, creating a new optimizer configuration') + '.\n\n';
      text += 'Optimize ' + selectedCoins.join(', ') + ' on ' + exchanges().join(', ') + ', direction ' + fields.direction.value + '. ';
      if (fields.direction.value !== 'both') text += 'Keep ' + (fields.direction.value === 'long' ? 'Short' : 'Long') + ' disabled. ';
      text += 'Maximum simultaneous coins: ' + positions + '. Maximum TWE per active side: ' + fields.twe.value + '.\n';
      text += fields.strategy.value === 'both' ? 'Allow EMA Anchor and Trailing Martingale; choose the strategy using measured training results.\n' : 'Use ' + (fields.strategy.value === 'ema_anchor' ? 'EMA Anchor' : 'Trailing Martingale') + '.\n';
      text += 'HSL: ' + (fields.hsl.value === 'source' ? 'preserve the starting configuration setting' : fields.hsl.value) + '.\n';
      text += 'Use exactly ' + timeframeCount + ' independent, non-overlapping CPU training timeframes (inclusive UTC dates), equally weighted:\n' + trainingWindows.map(function (value, index) { return (index + 1) + '. ' + value; }).join('\n') + '\nKeep these training windows and comparison conditions fixed across all rounds.\n';
      var holdoutWindow=reviewed.windows.find(function(w){return w.role==='holdout';});
      text += holdoutWindow ? 'Use holdout: enabled. Holdout: '+holdoutWindow.start_date+' → '+holdoutWindow.end_date+'. Always run Holdout and Full Time Range backtests as independent user-only background reports for validated training candidates. Start the next optimization round without waiting for these reports. Never expose their results to AI training, adaptive search, historical advice or incumbent selection. Prior use in another Loop must not block these reports.\n' : 'Use holdout: disabled. Create no holdout validation. Always run Full Time Range as an independent user-only background report; never expose its results to AI training, adaptive search or incumbent selection.\n';
      text += 'For preview_pb8_scenario_template and the optimizer save proposal, pass scenario_template with template="ai_loop_windows" and the following exact reviewed parameters. Do not regenerate or change any window. Keep the full period in backtest.start_date/end_date; use only training scenarios and reducer={"default":"mean"}; set optimize.write_all_results=true.\nPBGUI_LOOP_WINDOWS_V1:'+JSON.stringify(reviewed)+'\n';
      text += '\nComplete ' + rounds + ' sequential rounds with ' + fields.proxy.value + ' completed GPU proxy evaluations per round. Execution: ' + (fields.execution.value === 'vast' ? 'Vast GPU' : 'existing GPU worker') + '. Time limit: ' + fields.hours.value + ' hours.\n';
      if (fields.execution.value === 'vast') text += 'Rental budget: USD ' + fields.budget.value + '; maximum hourly price: USD ' + fields.hourly.value + '.\n';
      text += 'Store these as technical Loop settings: max_rounds=' + rounds + ', max_runs=' + rounds + ', max_validations=' + fields.validations.value + ', parallel=1, run_limit_mode="proxy", run_proxy=' + fields.proxy.value + ', hours=' + fields.hours.value + ', execution="' + fields.execution.value + '".\n';
      text += 'Maximum TWE is an upper ceiling, not a fixed search value. Store goals.max_twe=' + fields.twe.value + ' and choose the positive exposure search interval freely below or at that ceiling. Explain the interval and justify fixing TWE for a controlled experiment. Choose how many distinct Pareto candidates deserve independent CPU training validation in each round within the total budget of ' + fields.validations.value + ' validation jobs, including the baseline. There is no per-round candidate cap. Explain coverage, duplicate avoidance and allocation to future rounds. Holdout and Full Time Range reports do not consume this training budget.\n';
      var objectiveText = fields.objective.value === 'custom' ? fields.custom.value.trim() :
        fields.objective.value === 'ai' ? 'Find configurations with convincing return within the requested risk limits. Choose and explain suitable measurable evaluation criteria from the training evidence and user instructions; no fixed evaluation metric is prescribed.' :
        fields.objective.value === 'adg' ? 'Seek strong daily growth. Choose and explain suitable evaluation metrics; no GPU scoring formula is prescribed.' :
        'Seek high total return within the requested risk limits. Choose and explain suitable evaluation metrics; no GPU scoring formula is prescribed.';
      text += '\n' + objectiveText + '\n';
      text += 'Every training report must be complete, liquidation-free and have drawdown ≤' + fields.drawdown.value + '%.\n';
      text += 'Consult the original manual of the installed PB8 version before choosing evaluation metrics, scoring combinations, limits or bot changes. Investigate different GPU scorings over the experiment program, including simultaneous multi-objective combinations and justified single-objective comparisons. Choose metrics, combinations, objective count, directions, training reducers, scenario bases and experiment order freely from the complete supported GPU catalog. Multiple scoring entries define Pareto objectives, not a weighted sum. No scoring switch or optional-limit change is required in every round. Use controlled comparisons and repeated seeds when useful; explain hypotheses, tradeoffs, measured CPU training outcomes and uncertainty. Never claim a robust improvement from a single small training increase. Never use exact-only metrics directly in GPU scoring or limits. Preserve explicit user goals and hard safety filters; report untested variants if budgets prevent coverage. Adapt optimizer and bot settings from training evidence. Retain the incumbent unless independent CPU evidence supports a better candidate under the explained criteria.\n';
      text += 'Do not stop early because of stagnation. Report any hard limit that prevents completing all ' + rounds + ' rounds.';
      if (fields.notes.value.trim()) text += '\n\nAdditional instructions:\n' + fields.notes.value.trim();
      if (text.length > 8000) { error.textContent = 'Prompt exceeds 8000 characters; reduce timeframe count, additional instructions or the coin selection.'; return; }
      sending = true; reset.disabled = true; generate.disabled = true; panel.inert = true;
      var current = generation;
      try {
        await window.PBGuiAI.ensureSelection();
        if (current !== generation || !root.classList.contains('is-open')) return;
        if (window.PBGuiAI.isBusy()) throw new Error('Wait for the current AI turn to finish.');
        persist(); hide();
        await window.PBGuiAI.sendLoopPrompt(text);
      } catch (e) {
        sendError = e.message; open(trigger); error.textContent = sendError;
      } finally { sending = false; reset.disabled = false; generate.disabled = !marketReady || !verified || (fields.windows.value === 'auto' && !dateReady); panel.inert = false; }
    });
    sync();
    return {open: open};
  }
  window.PBGuiAILoopTemplates = {open: function (button) { if (!instance) instance = attach(); instance.open(button); }};
})();
