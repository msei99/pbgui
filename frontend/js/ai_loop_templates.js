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
    field('proxy', 'GPU proxy evaluations / round', null, 100000, 'number', 1, 1000000000).step = '1';
    field('execution', 'Execution', [['vast', 'Vast GPU'], ['gpu', 'Existing GPU worker']], 'vast');
    field('hours', 'Time limit (hours)', null, 12, 'number', .1, 168);
    field('budget', 'Rental budget (USD)', null, 5, 'number', .1, 100);
    field('hourly', 'Maximum rental USD / hour', null, .3, 'number', .01, 100);
    field('windows', 'Training windows', [['auto', 'Automatic from selected coins'], ['custom', 'Manual date range'], ['source', 'From starting configuration']], 'auto');
    field('timeframes', 'Number of training timeframes', null, 8, 'number', 1, 200).step = '1';
    field('start', 'Training start', null, '', 'date');
    field('end', 'Training end', null, '', 'date');
    field('objective', 'Objective', [['gain', 'Maximum gain'], ['balanced', 'Balanced'], ['custom', 'Custom']], 'gain');
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
    var dateGeneration = 0, dateTimer = null, dateJob = null, dateSignature = '', dateReady = false, dateBusy = false;
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
    reset.addEventListener('click', resetDefaults);
    actions.append(reset, generate, close);
    var footer = node('div'); footer.className = 'pai-template-footer'; footer.append(error, actions); dialog.appendChild(footer);
    function updateValidation() {
      var state = controller.state['pai-template-coins'];
      verified = !coins().some(function (coin) {
        var meta = controller.getCoinMeta('pai-template-coins', coin);
        return (meta && meta.status === 'invalid') || !state.options.includes(coin);
      });
      error.textContent = sendError || (verified ? '' : 'Some selected coins are unavailable; review the selection.');
      generate.disabled = !verified || !state.options.length || !marketReady || (fields.windows.value === 'auto' && !dateReady);
    }
    async function loadMarkets() {
      var current = ++marketGeneration;
      if (fields.windows.value === 'auto') {
        cancelDates(); fields.start.value = ''; fields.end.value = ''; dateStatus.textContent = 'Waiting for valid market selection…';
      }
      marketReady = false; verified = false; generate.disabled = true;
      if (!exchanges().length) { error.textContent = 'Select at least one exchange.'; return; }
      error.textContent = sendError || 'Loading coins…';
      try {
        var response = await fetch(marketsUrl, {method: 'POST', credentials: 'same-origin', cache: 'no-store',
          headers: {'Content-Type': 'application/json'}, body: JSON.stringify({exchanges: exchanges(), coins: coins()})});
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
      } catch (e) { if (current === marketGeneration) error.textContent = e.message; }
    }
    async function dateRequest(path, options) {
      var response = await fetch(optimizeUrl + path, Object.assign({credentials:'same-origin',cache:'no-store'}, options || {}));
      var data = await response.json();
      if (!response.ok) throw new Error(data.detail || 'Cannot determine training dates');
      return data;
    }
    function stopDateJob(id) {
      if (id) dateRequest('ohlcv-start-dates/' + encodeURIComponent(id), {method:'DELETE',keepalive:true}).catch(function () {});
    }
    function cancelDates() {
      dateGeneration++; clearTimeout(dateTimer); dateTimer = null;
      stopDateJob(dateJob); dateJob = null; dateSignature = ''; dateBusy = false; dateReady = false;
    }
    function lastCompletedDay() { return new Date(Date.now() - 86400000).toISOString().slice(0,10); }
    function queueDates() {
      if (fields.windows.value !== 'auto') { cancelDates(); dateStatus.textContent = ''; return; }
      if (!root.classList.contains('is-open') || !marketReady || !verified || !coins().length || !exchanges().length) {
        cancelDates(); dateStatus.textContent = 'Select valid coins and exchanges to determine training dates.'; return;
      }
      var signature = JSON.stringify([exchanges().slice().sort(),coins().slice().sort(),fields.source.value,lastCompletedDay()]);
      if (signature === dateSignature && (dateBusy || dateReady)) return;
      cancelDates(); dateSignature = signature; dateBusy = true;
      fields.start.value = ''; fields.end.value = ''; generate.disabled = true;
      dateStatus.textContent = 'Determining available training dates…';
      var current = dateGeneration;
      dateTimer = setTimeout(function () { resolveDates(current); }, 250);
    }
    async function resolveDates(current) {
      try {
        var data = await dateRequest(fields.source.value ? 'configs/' + encodeURIComponent(fields.source.value) : 'configs/new-config');
        if (current !== dateGeneration) return;
        if (!data.config || typeof data.config !== 'object') throw new Error('PB8 configuration defaults are unavailable.');
        var config = data.config;
        config.backtest = Object.assign({}, config.backtest, {exchanges:exchanges(),start_date:'2010-01-01',end_date:lastCompletedDay(),suite_enabled:false,scenarios:[]});
        config.live = Object.assign({}, config.live, {approved_coins:{long:coins(),short:[]},ignored_coins:{long:[],short:[]}});
        var job = await dateRequest('ohlcv-start-dates', {method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({config:config})});
        if (current !== dateGeneration) { stopDateJob(job.job_id); return; }
        if (!/^[a-f0-9]{12}$/.test(job.job_id || '')) throw new Error('Date lookup returned no valid job ID.');
        dateJob = job.job_id;
        handleDateJob(job, current);
      } catch (exc) { failDates(exc, current); }
    }
    function failDates(exc, current) {
      if (current !== dateGeneration) return;
      dateBusy = false; dateReady = false; stopDateJob(dateJob); dateJob = null;
      dateStatus.textContent = exc.message + ' Choose Set date range to enter dates manually.';
      updateValidation();
    }
    function handleDateJob(job, current) {
      if (current !== dateGeneration) return;
      if (job.status === 'complete' || job.status === 'completed') {
        var option = ((job.result || {}).start_date_options || {}).all_markets;
        if (!option || !option.available || !/^\d{4}-\d{2}-\d{2}$/.test(option.start_date || '')) {
          failDates(new Error((option || {}).detail || 'No common start date is available for the selected markets.'), current); return;
        }
        fields.start.value = option.start_date; fields.end.value = lastCompletedDay();
        dateJob = null; dateBusy = false; dateReady = true;
        dateStatus.textContent = 'Common market start including warmup → last complete UTC day.';
        persist(); updateValidation(); return;
      }
      if (!['queued','running','stopping'].includes(job.status)) { failDates(new Error(job.error || 'Date lookup did not complete.'), current); return; }
      dateStatus.textContent = (job.progress || {}).message || 'Determining available training dates…';
      dateTimer = setTimeout(async function () {
        try { handleDateJob(await dateRequest('ohlcv-start-dates/' + encodeURIComponent(dateJob)), current); }
        catch (exc) { failDates(exc, current); }
      }, 750);
    }
    function persist() {
      var values = {};
      Object.keys(fields).forEach(function (key) { if (!['notes', 'custom'].includes(key)) values[key] = fields[key].value; });
      try { sessionStorage.setItem('pbgui.ai.loop-template', JSON.stringify({schema_version: 2, values: values, exchanges: exchanges(), coins: coins()})); } catch (_) {}
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
      var source = configs.find(function (c) { return c.name === fields.source.value; });
      fields.windows.value = source ? 'source' : 'auto';
      if (source) { fields.start.value = source.start_date || ''; fields.end.value = source.end_date || ''; }
    });
    ['start','end'].forEach(function (key) { fields[key].addEventListener('input', function () {
      if (fields.windows.value === 'auto') { fields.windows.value = 'custom'; cancelDates(); dateStatus.textContent = ''; sync(); updateValidation(); }
    }); });
    panel.addEventListener('change', function () { sync(); persist(); queueDates(); if (marketReady) updateValidation(); });
    function resetDefaults() {
      if (sending) return;
      cancelDates(); generation++; marketGeneration++;
      sendError = ''; error.textContent = ''; dateStatus.textContent = '';
      Object.keys(defaultValues).forEach(function (key) { fields[key].value = defaultValues[key]; });
      controller.rebuild('pai-template-exchanges', optimizeExchangeOptions([]), ['binance'], {silent: true});
      controller.rebuild('pai-template-coins', [], [], {silent: true});
      labels = {}; marketReady = false; verified = false;
      count.textContent = '0 selected'; sync(); persist();
      loadOptions(); loadMarkets();
    }
    function hide() {
      cancelDates();
      generation++; marketGeneration++; root.classList.remove('is-open'); root.setAttribute('aria-hidden', 'true');
      try { sessionStorage.removeItem('pbgui.ai.loop-template.open'); } catch (_) {}
      floating.cancel(); if (trigger && trigger.isConnected) trigger.focus();
    }
    window.addEventListener('pagehide', cancelDates);
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
            if (Number(saved.schema_version || 0) < 2 && fields.windows.value === 'custom' && (!fields.start.value || !fields.end.value)) {
              fields.windows.value = 'auto';
            }
            var chosen = saved.exchanges || (values.exchange ? [values.exchange] : ['binance']);
            controller.rebuild('pai-template-exchanges', optimizeExchangeOptions(chosen), chosen, {silent: true});
            controller.rebuild('pai-template-coins', saved.coins || [], saved.coins || [], {silent: true});
            if (saved.values.source) fields.source.add(new Option(saved.values.source, saved.values.source, true, true));
          }
        } catch (_) {}
        loaded = true;
      }
      sync(); count.textContent = coins().length + ' selected';
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
      var timeframeCount = Number(fields.timeframes.value), trainingWindows = [];
      if (fields.windows.value !== 'source') {
        if (fields.windows.value === 'auto' && !dateReady) { error.textContent = 'Wait for training dates to be determined.'; return; }
        var firstDay = Date.parse(fields.start.value + 'T00:00:00Z') / 86400000;
        var lastDay = Date.parse(fields.end.value + 'T00:00:00Z') / 86400000;
        var days = lastDay - firstDay + 1;
        if (!Number.isInteger(timeframeCount) || timeframeCount < 1 || !Number.isFinite(days) || days < timeframeCount * 2) {
          error.textContent = 'Each training timeframe needs at least two calendar days; reduce the count or extend the date range.'; return;
        }
        for (var index = 0; index < timeframeCount; index++) {
          var startDay = firstDay + Math.floor(index * days / timeframeCount), endDay = firstDay + Math.floor((index + 1) * days / timeframeCount) - 1;
          trainingWindows.push(new Date(startDay * 86400000).toISOString().slice(0,10) + ' → ' + new Date(endDay * 86400000).toISOString().slice(0,10));
        }
      }
      var source = fields.source.value;
      var text = 'Create a new optimization loop ' + (source ? 'using optimizer configuration ' + JSON.stringify(source) : 'from PB8 defaults, creating a new optimizer configuration') + '.\n\n';
      text += 'Optimize ' + selectedCoins.join(', ') + ' on ' + exchanges().join(', ') + ', direction ' + fields.direction.value + '. ';
      if (fields.direction.value !== 'both') text += 'Keep ' + (fields.direction.value === 'long' ? 'Short' : 'Long') + ' disabled. ';
      text += 'Maximum simultaneous coins: ' + positions + '. Maximum TWE per active side: ' + fields.twe.value + '.\n';
      text += fields.strategy.value === 'both' ? 'Allow EMA Anchor and Trailing Martingale; choose the strategy using measured training results.\n' : 'Use ' + (fields.strategy.value === 'ema_anchor' ? 'EMA Anchor' : 'Trailing Martingale') + '.\n';
      text += 'HSL: ' + (fields.hsl.value === 'source' ? 'preserve the starting configuration setting' : fields.hsl.value) + '.\n';
      text += fields.windows.value === 'source' ? 'Preserve the starting configuration training windows and comparison conditions.\n' : 'Use exactly ' + timeframeCount + ' independent, non-overlapping CPU training timeframes (inclusive UTC dates), equally weighted:\n' + trainingWindows.map(function (value, index) { return (index + 1) + '. ' + value; }).join('\n') + '\nKeep these training windows and comparison conditions fixed across all rounds.\n';
      text += '\nComplete ' + rounds + ' sequential rounds with ' + fields.proxy.value + ' completed GPU proxy evaluations per round. Execution: ' + (fields.execution.value === 'vast' ? 'Vast GPU' : 'existing GPU worker') + '. Time limit: ' + fields.hours.value + ' hours.\n';
      if (fields.execution.value === 'vast') text += 'Rental budget: USD ' + fields.budget.value + '; maximum hourly price: USD ' + fields.hourly.value + '.\n';
      text += 'Store these as technical Loop settings: max_rounds=' + rounds + ', max_runs=' + rounds + ', max_validations=' + (rounds * 2) + ', parallel=1, run_limit_mode="proxy", run_proxy=' + fields.proxy.value + ', hours=' + fields.hours.value + ', execution="' + fields.execution.value + '".\n';
      text += '\n' + (fields.objective.value === 'custom' ? fields.custom.value.trim() : 'Maximize the equally weighted arithmetic mean of gain_strategy_eq across all independent CPU training reports.') + '\n';
      text += 'Every training report must be complete, liquidation-free and have drawdown ≤' + fields.drawdown.value + '%.\n';
      if (fields.objective.value !== 'custom') text += 'Use smoothness only to break equal mean-gain ties.\n';
      text += 'Adapt search bounds, proxy scoring, optional optimizer limits and optimizer settings using measured training results. All other bot parameters may change. Treat source settings as a starting point; add no constraints beyond this request. Retain the incumbent unless independent CPU validation proves another candidate is better.\n';
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
