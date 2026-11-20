/* Native source bindings for the shared Optimize controls; projections never execute. */
;(function () {
  'use strict';
  var metadata = null, renderer = null, policyRenderer = null, prefilterEditor = null, gpuScenarios = null, runOptionsEditor = null, sideJsonBaseline = {};
  var clone = function (value) { return JSON.parse(JSON.stringify(value)); };
  var cp = function () { return window.PBGuiEditorShared.configPath; };
  var same = function (a,b) { return JSON.stringify(a) === JSON.stringify(b); };
  var renames = {
    'backtest.btc_collateral_cap':'backtest.btc_collateral_target_ratio',
    'backtest.btc_collateral_ltv_cap':'backtest.max_usd_debt_to_wallet_value_ratio',
    'backtest.liquidation_threshold':'backtest.equity_floor_fraction',
    'backtest.maker_fee_override':'backtest.maker_fee_override_fraction',
    'backtest.taker_fee_override':'backtest.taker_fee_override_fraction',
    'backtest.market_order_slippage_pct':'backtest.market_order_slippage_fraction',
    'backtest.limit_order_fill_buffer_pct':'backtest.limit_order_fill_buffer_fraction',
    'live.strategy_kind':'bot.portfolio.strategy_kind',
    'live.hsl_signal_mode':'bot.portfolio.hsl.scope',
    'live.minimum_coin_age_days':'bot.global.universe.minimum_coin_age_days',
    'optimize.pymoo.shared.crossover_eta':'optimize.pymoo.shared.crossover_distribution_index',
    'optimize.pymoo.shared.crossover_prob_var':'optimize.pymoo.shared.crossover_probability_per_variable',
    'optimize.pymoo.shared.mutation_eta':'optimize.pymoo.shared.mutation_distribution_index',
    'optimize.pymoo.shared.mutation_prob_per_variable':'optimize.pymoo.shared.mutation_probability_per_variable',
    'optimize.pymoo.shared.mutation_prob':'optimize.pymoo.shared.mutation_probability'
  };
  ['long','short'].forEach(function(side) {
    ['approved_coins','ignored_coins'].forEach(function(key) {
      renames['live.'+key+'.'+side]='bot.'+side+'.universe.'+key;
    });
  });

  function effective(source) {
    var result=cp().merge({},metadata ? metadata.prototype : {});
    cp().merge(result, source);
    ['long','short'].forEach(function(side) {
      var values=cp().merge({},cp().get(metadata && metadata.prototype,['bot',side]) || {});
      cp().merge(values,cp().get(result,['bot','global']) || {});
      cp().merge(values,cp().get(source,['bot',side]) || {});
      cp().set(result,['bot',side],values);
    });
    return result;
  }

  function toView(source) {
    var result=effective(source), getter=cp();
    // Runtime defaults describe suggested domains. Only authored bounds own a search range.
    result.optimize.bounds = clone(getter.get(source, ['optimize', 'bounds']) || {});
    Object.keys(renames).forEach(function(path) {
      var value=getter.get(result,renames[path].split('.'));
      if(value !== undefined)getter.set(result,path.split('.'),clone(value));
    });
    // Shared probability controls remain display-only for native Pymoo settings.
    result.optimize.crossover_probability=result.optimize.pymoo.shared.crossover_probability_per_variable;
    result.optimize.mutation_probability=result.optimize.pymoo.shared.mutation_probability;
    return result;
  }

  function accepted(path, original) {
    var parts=path.split('.');
    if(parts[0]==='pbgui')return true;
    if (parts[0]==='optimize' && parts[1]==='bounds') {
      if (metadata && Object.hasOwn(metadata.bound_catalog || {}, parts.slice(2).join('.'))) return true;
      if (cp().get(metadata && metadata.all_bounds,parts.slice(2))!==undefined) return true;
      if (parts[2]==='long' || parts[2]==='short') {
        // PB9 supports the same bound paths on both sides. A shared authored
        // range can be overridden even when that side has no suggested range.
        var globalPath=['global'].concat(parts.slice(3));
        if (cp().has(original,['optimize','bounds'].concat(globalPath))) return true;
      }
    }
    if(cp().get(original,parts)!==undefined || cp().get(metadata && metadata.prototype,parts)!==undefined)return true;
    return (metadata && metadata.fields || []).some(function(field) {
      return field.path.length===parts.length && field.path.every(function(value,index){return value==='*' || value===parts[index];});
    });
  }

  function fromView(view, original, strict) {
    var result=clone(original), baseline=toView(original), getter=cp();
    // Side JSON shows effective values, including values also edited by the
    // universe pickers. Apply only edits since that JSON was rendered; stale
    // display values must not overwrite a newer structured/Raw JSON change.
    var adjusted=clone(view);
    ['long','short'].forEach(function(side) {
      if(!sideJsonBaseline[side])return;
      getter.set(adjusted,['bot',side],clone(baseline.bot[side]));
      getter.applyDelta(adjusted,sideJsonBaseline[side],view.bot[side],['bot',side]);
    });
    function walk(before, after, parts) {
      if(same(before,after))return;
      var path=parts.join('.'), target=renames[path] || path;
      if(after && typeof after==='object' && !Array.isArray(after)) {
        var keys=new Set(Object.keys(before && typeof before==='object' ? before : {}).concat(Object.keys(after)));
        keys.forEach(function(key){walk(before && before[key],after[key],parts.concat(key));});
        return;
      }
      if(!accepted(target,original))return;
      if(after===undefined)getter.remove(result,target.split('.'));
      else getter.set(result,target.split('.'),target==='optimize.pymoo.shared.mutation_probability_per_variable' && after===null ? 'auto' : clone(after));
    }
    var errors=[];
    // Policy-panel edits apply first and only for fields edited since the
    // panel was populated. A newer Side JSON edit of the same path then wins
    // instead of being reverted or removed by the panel's displayed state.
    if(policyRenderer) errors=errors.concat(policyRenderer.apply(result,{onlyTouched:true}));
    walk(baseline,adjusted,[]);
    if(policyRenderer) policyRenderer.refresh(result);
    if(renderer) errors=errors.concat(renderer.apply(result));
    if (prefilterEditor) errors = errors.concat(prefilterEditor.apply(result));
    if (gpuScenarios) errors = errors.concat(gpuScenarios.apply(result));
    if (runOptionsEditor) errors = errors.concat(runOptionsEditor.apply(result));
    if(strict && errors.length)throw new Error('Correct the highlighted native optimizer parameters.');
    return result;
  }

  function decorate(source) {
    var section=document.getElementById('optimize-gpu-section');
    if(!section || !metadata)return;
    prefilterEditor = mountPrefilter(source);
    if (runOptionsEditor && runOptionsEditor.dispose) runOptionsEditor.dispose();
    runOptionsEditor = mountRunOptions(document.getElementById('opt9-run-options'), source);
    sideJsonBaseline={};
    ['long','short'].forEach(function(side) {
      var node=document.getElementById('opted-bot-'+side);
      if(node)sideJsonBaseline[side]=JSON.parse(node.value);
    });
    var previous=document.getElementById('opt9-gpu-fields');if(previous)previous.remove();
    Array.from(section.children).forEach(function(child){child.hidden=true;});
    var host=document.createElement('div');host.id='opt9-gpu-fields';section.appendChild(host);
    if(renderer)renderer.clear();
    renderer=window.PBGuiEditorShared.createMetadataFieldRenderer({idPrefix:'opt9-field',steppers:true,
      resolve:function(config,field){
        var inherited=cp().get(metadata.prototype,field.path);
        return {explicit:cp().has(config,field.path),value:cp().get(effective(config),field.path),inherited:inherited};
      }});
    var fields=metadata.fields.filter(function(field) {
      return field.path[0]==='optimize' && field.path[1]==='gpu' && field.path.indexOf('*')<0
        && field.path.join('.') !== 'optimize.gpu.screening.scenarios';
    });
    fields=fields.filter(function(field){return !fields.some(function(child){return child.path.length>field.path.length && field.path.every(function(part,index){return child.path[index]===part;});});});
    renderer.mount(host.id,[{id:'native-gpu',groups:[{columns:4,fields:fields.map(function(field){return {field:field,showExplicit:false};})}]}],{});
    renderer.populate(source);
    gpuScenarios = mountGpuScenarios(host, source);
    var policyHost=document.getElementById('opted-runtime-overrides-panel');
    if(policyHost) {
      if(policyRenderer)policyRenderer.clear();
      policyRenderer=window.PBGuiEditorShared.createMetadataFieldRenderer({idPrefix:'opt9-policy',steppers:true,booleanLayout:'label-above',
        resolve:function(config,field){var value=cp().get(effective(config),field.path);return {explicit:cp().has(config,field.path),value:value,inherited:cp().get(metadata.prototype,field.path)};}});
      policyHost.replaceChildren();
      var surface=document.createElement('div');surface.className='bounds-surface';policyHost.appendChild(surface);
      var titles={portfolio:'Portfolio policy (account-wide)',global:'Shared defaults (Long and Short)',long:'Long policy',short:'Short policy'};
      ['portfolio','global','long','short'].forEach(function(scope){
        var card=document.createElement('div');card.className='bounds-side-card';surface.appendChild(card);
        if (scope==='portfolio' || scope==='global') card.classList.add('bounds-side-card-wide');
        var title=document.createElement('div');title.className='bounds-side-title';
        title.textContent=titles[scope];card.appendChild(title);
        if (scope==='long' || scope==='short') card.appendChild(sideOverrideList(scope, source));
        var body=document.createElement('div');body.id='opt9-policy-'+scope;card.appendChild(body);
        var fields=metadata.fields.filter(function(field){
          return field.path[0]==='bot' && field.path[1]===scope && field.path.indexOf('*')<0
            && field.path.join('.')!=='bot.portfolio.strategy_kind' && field.path.join('.')!=='bot.portfolio.hsl.scope'
            && (field.types.some(function(type){return type==='boolean' || type==='string';})
              || (scope==='portfolio' && ['hsl','loss_gate','unstuck'].includes(field.path[2]))
              || fixedOnlyNumber(field))
            && field.path.indexOf('universe')<0;
        }).map(function(field){return {field:field,label:field.path.slice(2).join('.'),showExplicit:false,hideUnit:true};});
        policyRenderer.mount(body.id,[{id:scope,groups:[{columns:2,fields:fields}]}],{});
      });
      policyRenderer.populate(source);

    }
    Object.keys(renames).forEach(function(oldPath) {
      var oldKey=oldPath.split('.').pop(), target=renames[oldPath].split('.').pop();
      if(oldKey===target)return;
      document.querySelectorAll('#configs-editor label span').forEach(function(label) {
        if(label.textContent.trim()===oldKey)label.textContent=target;
      });
    });
  }

  // Show policy-panel changes in an unedited Side JSON without re-rendering
  // the panel being typed in. Pending Side JSON edits are never replaced.
  function syncSideJson(config, format) {
    if(!metadata)return;
    var view=toView(config);
    ['long','short'].forEach(function(side) {
      var node=document.getElementById('opted-bot-'+side);
      if(!node || !sideJsonBaseline[side] || document.activeElement===node)return;
      var current;
      try { current=JSON.parse(node.value); } catch (error) { return; }
      if(!same(current,sideJsonBaseline[side]))return;
      var next=(view.bot && view.bot[side]) || {};
      if(same(next,current))return;
      node.value=format(next);
      sideJsonBaseline[side]=JSON.parse(node.value);
    });
  }

  // The value a fixed bound freezes: authored, inherited from global or a native default.
  function effectiveBotValue(source, key) {
    var path=['bot'].concat(String(key || '').replace(/^bot\./,'').split('.').filter(Boolean));
    return cp().get(effective(source || {}), path);
  }

  // Authored Long/Short leaves override Shared defaults. Each can be reset
  // so the side inherits again; the page applies it through Raw JSON sync.
  function sideOverridePaths(source, side) {
    var paths=[];
    (function walk(value, prefix) {
      if (value && typeof value==='object' && !Array.isArray(value) && Object.keys(value).length) {
        Object.keys(value).sort().forEach(function(key){walk(value[key], prefix.concat(key));});
      } else if (prefix.length) paths.push(prefix.join('.'));
    }(cp().get(source,['bot',side]) || {}, []));
    return paths;
  }

  function sideOverrideList(side, source) {
    var label=side==='long' ? 'Long' : 'Short';
    var wrap=document.createElement('div');
    wrap.className='side-override-list';wrap.id='opt9-side-overrides-'+side;
    var paths=sideOverridePaths(source, side);
    var title=document.createElement('div');title.className='side-override-title';
    var titleText=document.createElement('span');
    titleText.textContent=label+' overrides ('+paths.length+')';
    titleText.dataset.tip=label+' values set here override Shared defaults. Reset removes the '+label
      +' value so it inherits the shared value again (or the native default for '+label+'-only settings).';
    titleText.tabIndex=0;
    title.appendChild(titleText);wrap.appendChild(title);
    if(!paths.length) {
      var empty=document.createElement('div');empty.className='side-override-empty';
      empty.textContent='None. All '+label+' values inherit Shared defaults.';
      wrap.appendChild(empty);
      return wrap;
    }
    paths.forEach(function(path) {
      var row=document.createElement('div');row.className='side-override-row';
      var key=document.createElement('code');key.textContent=path;row.appendChild(key);
      var button=document.createElement('button');button.type='button';button.className='inline-btn';
      button.textContent='Reset';button.dataset.resetSideOverride=side+'.'+path;
      button.setAttribute('aria-label','Reset '+label+' '+path+' to inherited value');
      button.addEventListener('click',function(){
        if(typeof window.resetOptimizeSideOverride==='function')window.resetOptimizeSideOverride(side,path);
      });
      row.appendChild(button);wrap.appendChild(row);
    });
    return wrap;
  }

  // Numeric policy values that cannot be optimized have no bound row; they
  // are regular fields here (for example forager.score_hysteresis_margin).
  function fixedOnlyNumber(field) {
    var numeric=field.types.some(function(type){return ['float','integer','number'].includes(type);});
    return numeric && !Object.hasOwn((metadata && metadata.bound_catalog) || {}, field.path.slice(1).join('.'));
  }

  function hasSideJsonEdits() {
    return Object.keys(sideJsonBaseline).some(function(side) {
      var node=document.getElementById('opted-bot-'+side);
      return node && !same(JSON.parse(node.value),sideJsonBaseline[side]);
    });
  }

  function mountPrefilter(source) {
    var host = document.getElementById('opt9-prefilter');
    if (!host) return null;
    host.replaceChildren();
    var path = ['optimize', 'prefilter'];
    var inherited = cp().get(metadata.prototype, path) || {};
    var sourcePolicy = cp().get(source, path) || {};
    var policy = Object.assign({}, clone(inherited), clone(sourcePolicy));
    var fields = metadata.fields.filter(function(field) {
      return field.path[0] === 'optimize' && field.path[1] === 'prefilter';
    });
    var scalars = window.PBGuiEditorShared.createMetadataFieldRenderer({
      idPrefix:'opt9-prefilter-field', steppers:true,
      resolve:function(config, field) {
        return {explicit:cp().has(config, field.path), value:cp().get(effective(config), field.path),
          inherited:cp().get(metadata.prototype, field.path)};
      }
    });
    scalars.mount(host, [{id:'assumptions', title:'Probe assumptions', groups:[{columns:4,
      fields:fields.filter(function(field) {
        return field.path.length === 3 && !field.types.includes('array');
      }).map(function(field) { return {field:field, showExplicit:false}; })
    }]}], {});
    scalars.populate(source);
    var fractionRows = [], checkRows = [], nextId = 0;

    function node(tag, parent, text) {
      var element = document.createElement(tag);
      if (text !== undefined) element.textContent = text;
      parent.appendChild(element);
      return element;
    }
    function button(parent, text, action) {
      var element = node('button', parent, text);
      element.type = 'button';
      element.className = 'btn';
      element.addEventListener('click', function() {
        action();
        host.dispatchEvent(new Event('input', {bubbles:true}));
      });
      return element;
    }
    function heading(text, fieldName) {
      var label = node('div', host);
      label.className = 'subsection-title';
      var span = node('span', label, text);
      var field = fields.find(function(item) { return item.path.join('.') === 'optimize.prefilter.' + fieldName; });
      if (field?.description) { span.dataset.tip = field.description; span.tabIndex = 0; }
    }
    heading('Exposure probe fractions', 'exposure_probe_fractions');
    var fractions = node('div', host);
    fractions.className = 'form-row cols-4';
    function addFraction(value) {
      var row = node('div', fractions);
      row.className = 'form-group';
      var label = node('label', row, 'Exposure fraction');
      var input = node('input', row);
      input.type = 'number'; input.step = 'any'; input.min = '0'; input.max = '0.9989999999999999';
      input.required = true; input.value = value;
      input.id = 'opt9-prefilter-fraction-' + nextId++;
      label.htmlFor = input.id;
      var entry = {row:row, input:input}; fractionRows.push(entry);
      button(row, 'Remove fraction', function() {
        fractionRows.splice(fractionRows.indexOf(entry), 1); row.remove();
      });
    }
    (policy.exposure_probe_fractions || []).forEach(addFraction);
    button(host, 'Add exposure fraction', function() { addFraction(0.5); });
    heading('Checks', 'checks');
    var checks = node('div', host);
    var template = fields.filter(function(field) {
      return field.path[2] === 'checks' && field.path.length === 5 && field.path[4] !== 'sides';
    });
    var metrics = template.find(function(field) { return field.path[4] === 'metric'; })?.domain?.choices || [];

    function addCheck(value) {
      var row = node('div', checks);
      row.className = 'limits-panel';
      row.dataset.prefilterCheck = String(nextId++);
      var body = node('div', row);
      var defaults = {enabled:false, min:null, max:null, scale:1, weight:1};
      var checkRenderer = window.PBGuiEditorShared.createMetadataFieldRenderer({
        idPrefix:'opt9-prefilter-check-' + row.dataset.prefilterCheck, steppers:true,
        booleanLayout:'label-above',
        resolve:function(config, field) {
          var key = field.path[1];
          return {explicit:Object.hasOwn(config.check, key),
            value:Object.hasOwn(config.check, key) ? config.check[key] : defaults[key], inherited:defaults[key]};
        }
      });
      checkRenderer.mount(body, [{id:'check', groups:[{columns:4, fields:template.map(function(field) {
        var local = clone(field); local.path = ['check', field.path[4]];
        if (['min','max','scale','weight'].includes(local.path[1])) {
          local.domain = {minimum:0, exclusive_minimum:['scale','weight'].includes(local.path[1]), finite:true};
        }
        return {field:local, showExplicit:false};
      })}]}], {});
      checkRenderer.populate({check:value});
      var sides = node('div', row); sides.className = 'form-row cols-4';
      var sideValues = {};
      ['long', 'short'].forEach(function(side) {
        var label = node('label', sides); label.className = 'chk-row';
        var checkbox = node('input', label);
        checkbox.type = 'checkbox'; checkbox.checked = (value.sides || ['long','short']).includes(side);
        node('span', label, side === 'long' ? 'Long' : 'Short');
        sideValues[side] = checkbox;
      });
      var entry = {row:row, source:clone(value), renderer:checkRenderer, sides:sideValues};
      checkRows.push(entry);
      button(row, 'Remove check', function() {
        checkRows.splice(checkRows.indexOf(entry), 1); checkRenderer.clear(); row.remove();
      });
    }
    (policy.checks || []).forEach(addCheck);
    var add = button(host, 'Add check', function() { addCheck({metric:metrics[0], enabled:false}); });
    add.disabled = !metrics.length;
    var status = node('div', host); status.className = 'field-status'; status.setAttribute('aria-live', 'polite');
    return {apply:function(target) {
      var errors = scalars.apply(target);
      var probeValues = fractionRows.map(function(entry) { return entry.input.valueAsNumber; });
      var messages = [];
      if (!probeValues.length || probeValues.length > 64 || probeValues.some(function(value) {
        return !Number.isFinite(value) || value < 0 || value >= 0.999;
      })) messages.push('Use 1–64 exposure fractions from 0 up to 0.999 (exclusive).');
      var values = checkRows.map(function(entry) {
        var local = {check:clone(entry.source)};
        errors = errors.concat(entry.renderer.apply(local));
        var sides = ['long','short'].filter(function(side) { return entry.sides[side].checked; });
        if (!same(sides, entry.source.sides || ['long','short'])) local.check.sides = sides;
        if (!sides.length) messages.push('Each check needs Long and/or Short.');
        if (local.check.enabled && local.check.min == null && local.check.max == null) {
          messages.push('An enabled check needs a minimum or maximum.');
        }
        if (local.check.min != null && local.check.max != null && local.check.min > local.check.max) {
          messages.push('A check minimum must not exceed its maximum.');
        }
        return local.check;
      });
      if (values.length > 64) messages.push('Use at most 64 checks.');
      if (values.some(function(check) { return check.enabled; }) && target.bot.portfolio.strategy_kind !== 'trailing_martingale') {
        messages.push('Enabled prefilter checks require trailing_martingale.');
      }
      var grid = cp().get(target, path.concat('grid_max_entries')) ?? inherited.grid_max_entries;
      var penalty = cp().get(target, path.concat('penalty_weight')) ?? inherited.penalty_weight;
      if (!Number.isInteger(grid) || grid < 2 || grid > 4096) messages.push('grid_max_entries must be an integer from 2 to 4096.');
      if (!(penalty > 0)) messages.push('penalty_weight must be positive.');
      status.textContent = Array.from(new Set(messages)).join(' ');
      if (!messages.length && !errors.length) {
        if (cp().has(target, path.concat('exposure_probe_fractions')) || !same(probeValues, policy.exposure_probe_fractions)) {
          cp().set(target, path.concat('exposure_probe_fractions'), probeValues);
        }
        if (cp().has(target, path.concat('checks')) || !same(values, policy.checks)) {
          cp().set(target, path.concat('checks'), values);
        }
      }
      return errors.concat(messages);
    }};
  }

  // Screening scenarios are a subset of the Suite scenario labels. Only
  // current labels can be selected; stale saved labels stay visible as errors.
  function mountGpuScenarios(host, source, launchSelection) {
    var path = launchSelection ? ['pbgui','optimize_runtime','scenarios'] : ['optimize','gpu','screening','scenarios'];
    var baseline = clone(cp().get(effective(source), path) || []);
    var selected = baseline.slice();
    var title = document.createElement('div');
    title.className = 'subsection-title';
    var titleText = document.createElement('span');
    titleText.textContent = launchSelection ? 'Run scenarios' : 'Screening scenarios';
    titleText.dataset.tip = launchSelection ? 'Run only the selected Suite scenarios (--scenarios). Select none to run all scenarios.' : 'Suite scenarios evaluated in the partial GPU screening stage. '
      + 'Select none to disable partial screening. Labels come from the Suite scenarios.';
    titleText.tabIndex = 0;
    title.appendChild(titleText); host.appendChild(title);
    var list = document.createElement('div');
    list.className = 'form-row cols-4'; list.id = launchSelection ? 'opt9-run-scenarios' : 'opt9-gpu-screening-scenarios';
    host.appendChild(list);
    var status = document.createElement('div');
    status.className = 'field-status'; status.setAttribute('aria-live', 'polite');
    host.appendChild(status);
    function suiteLabels(config) {
      var backtest = cp().get(config, ['backtest']) || {};
      if (!backtest.suite_enabled || !Array.isArray(backtest.scenarios)) return [];
      return backtest.scenarios.map(function(scenario) {
        return String((scenario && scenario.label) || '').trim();
      }).filter(function(label, index, labels) { return label && labels.indexOf(label) === index; });
    }
    var renderedLabels = null;
    function report(labels) {
      var unknown = selected.filter(function(label) { return labels.indexOf(label) < 0; });
      status.textContent = unknown.length ? (launchSelection ? 'Run scenarios' : 'Screening scenarios') + ' must be Suite labels: ' + unknown.join(', ') + '.' : '';
      status.classList.toggle('error', !!unknown.length);
      return unknown;
    }
    function render(labels) {
      renderedLabels = JSON.stringify(labels);
      report(labels);
      list.replaceChildren();
      var shown = labels.concat(selected.filter(function(label) { return labels.indexOf(label) < 0; }));
      if (!shown.length) {
        var empty = document.createElement('div');
        empty.className = 'side-override-empty';
        empty.textContent = launchSelection ? 'Enable Suite mode and add scenarios to select a run subset. None selected runs all scenarios.' : 'Enable Suite mode and add scenarios to select screening scenarios.';
        list.appendChild(empty);
        return;
      }
      shown.forEach(function(label) {
        var row = document.createElement('label'); row.className = 'chk-row';
        var checkbox = document.createElement('input'); checkbox.type = 'checkbox';
        checkbox.checked = selected.indexOf(label) >= 0;
        if (launchSelection) checkbox.dataset.runScenario = label;
        else checkbox.dataset.screeningLabel = label;
        checkbox.addEventListener('change', function() {
          selected = selected.filter(function(item) { return item !== label; });
          if (checkbox.checked) selected.push(label);
          host.dispatchEvent(new Event('input', {bubbles:true}));
        });
        var text = document.createElement('span');
        text.textContent = labels.indexOf(label) >= 0 ? label : label + ' (not in Suite)';
        row.appendChild(checkbox); row.appendChild(text);
        if (labels.indexOf(label) < 0) row.classList.add('is-invalid');
        list.appendChild(row);
      });
    }
    render(suiteLabels(source));
    return {apply:function(target) {
      var labels = suiteLabels(target);
      if (JSON.stringify(labels) !== renderedLabels) render(labels);
      var unknown = report(labels);
      var ordered = labels.filter(function(label) { return selected.indexOf(label) >= 0; }).concat(unknown);
      if (unknown.length) return [status.textContent];
      if (cp().has(target, path) || !same(ordered, baseline)) cp().set(target, path, ordered);
      return [];
    }};
  }

  function mountRunOptions(host, source) {
    if (!host) return null;
    host.replaceChildren();
    var runtime = clone(source.pbgui?.optimize_runtime || {});
    var scenarios = mountGpuScenarios(host, source, true);
    var row = document.createElement('div'); row.className = 'form-row cols-4'; host.appendChild(row);
    function field(label, tip, input) {
      var group = document.createElement('div'); group.className = 'form-group';
      var caption = document.createElement('label'); caption.htmlFor = input.id;
      var text = document.createElement('span'); text.textContent = label; text.dataset.tip = tip;
      text.tabIndex = 0; caption.appendChild(text); group.appendChild(caption);
      if (input.type === 'checkbox') {
        var checkRow = document.createElement('div'); checkRow.className = 'chk-row';
        checkRow.appendChild(input); group.appendChild(checkRow);
      } else group.appendChild(input);
      row.appendChild(group); return group;
    }
    var filtering = document.createElement('input'); filtering.id = 'opt9-filter-starting-configs';
    filtering.type = 'checkbox'; filtering.checked = runtime.filter_starting_configs === true;
    field('Filter starting configs', 'Pareto seed only: filter metric-bearing saved seeds using the final active limits. Saved metrics are not recalculated.', filtering);
    var maximum = document.createElement('input'); maximum.id = 'opt9-compress-starting-configs';
    maximum.type = 'number'; maximum.min = '1'; maximum.step = '1';
    maximum.value = runtime.compress_starting_configs == null ? '' : String(runtime.compress_starting_configs);
    field('Maximum starting configs', 'Pareto seed only: compress the starting population to at most N configs. Leave empty for no compression.', maximum);
    var upload = document.createElement('input'); upload.id = 'opt9-preparation-manifest'; upload.type = 'file'; upload.accept = '.json,application/json';
    var group = field('Preparation manifest', 'Optional completed download --for-optimize report. PBGui freezes this report in the job; Passivbot verifies the actual prepared inputs before evaluation. Maximum 4 MiB.', upload);
    group.classList.add('span-2');
    var status = document.createElement('div'); status.className = 'field-status'; status.setAttribute('aria-live', 'polite'); group.appendChild(status);
    var seedStatus = document.createElement('div'); seedStatus.className = 'field-status error';
    seedStatus.setAttribute('aria-live', 'polite'); host.appendChild(seedStatus);
    var remove = document.createElement('button'); remove.type = 'button'; remove.className = 'btn';
    remove.id = 'opt9-remove-preparation-manifest'; remove.textContent = 'Remove manifest'; group.appendChild(remove);
    var manifest = runtime.preparation_manifest || null, pending = false, failure = '', generation = 0;
    var requestController = null, requestTimer = null;
    function cancelRequest() {
      if (requestController) requestController.abort();
      if (requestTimer) clearTimeout(requestTimer);
      requestController = null; requestTimer = null;
    }
    function report() {
      status.textContent = failure || (pending ? 'Reading manifest…' : manifest ? manifest.name : 'No preparation manifest selected.');
      status.classList.toggle('error', !!failure); remove.disabled = !manifest && !pending && !failure;
    }
    upload.addEventListener('change', async function() {
      var ticket = ++generation, file = upload.files[0];
      if (!file) return;
      cancelRequest();
      pending = true; failure = ''; report();
      try {
        if (file.size > 4 * 1024 * 1024) throw new Error('Preparation manifest exceeds 4 MiB.');
        var text = await file.text();
        if (ticket !== generation || !host.isConnected) return;
        var parsed = JSON.parse(text.replace(/^\uFEFF/, ''));
        if (parsed?.kind !== 'prepared-inputs' || parsed?.consumer !== 'optimize') throw new Error('Choose a completed optimizer preparation report.');
        requestController = new AbortController();
        requestTimer = setTimeout(function() { if (ticket === generation) requestController?.abort(); }, 15000);
        var response = await fetch(window.API_BASE + '/preparation-manifests', {method:'POST',
          headers:{'Content-Type':'application/json'}, body:JSON.stringify({name:file.name, text:text}),
          signal:requestController.signal});
        var payload;
        try { payload = await response.json(); }
        catch (_) { throw new Error('Manifest import failed (HTTP ' + response.status + '). Retry.'); }
        if (ticket !== generation || !host.isConnected) return;
        if (!response.ok) throw new Error(typeof payload.detail === 'string' ? payload.detail : 'Cannot import preparation manifest.');
        if (!payload.manifest || !/^[0-9a-f]{64}$/.test(payload.manifest.sha256)
          || payload.manifest.artifact !== payload.manifest.sha256) throw new Error('The server returned an invalid manifest reference. Retry.');
        manifest = payload.manifest;
      } catch (error) {
        if (ticket !== generation || !host.isConnected) return;
        failure = error.name === 'AbortError' ? 'Manifest import timed out. Retry.' : error.message;
      } finally {
        if (ticket === generation) {
          cancelRequest();
          if (host.isConnected) {
            upload.value = '';
            pending = false; report(); host.dispatchEvent(new Event('input', {bubbles:true}));
          }
        }
      }
    });
    remove.addEventListener('click', function() {
      ++generation; cancelRequest(); pending = false; manifest = null; failure = ''; upload.value = ''; report();
      host.dispatchEvent(new Event('input', {bubbles:true}));
    });
    report();
    return {dispose:function() { ++generation; cancelRequest(); }, apply:function(target) {
      var errors = scenarios.apply(target);
      target.pbgui = target.pbgui || {};
      target.pbgui.optimize_runtime = target.pbgui.optimize_runtime || {};
      var options = target.pbgui.optimize_runtime;
      options.filter_starting_configs = filtering.checked;
      options.compress_starting_configs = maximum.value === '' ? null : maximum.valueAsNumber;
      options.preparation_manifest = manifest;
      var mode = options.mode || 'fresh';
      var seeded = mode === 'pareto_seed' || (mode !== 'checkpoint_resume' && ['self','path'].includes(target.pbgui.optimize_seed_mode));
      seedStatus.textContent = (filtering.checked || maximum.value !== '') && !seeded
        ? 'Starting-config filtering and compression require seed_mode self or path (Pareto seed).' : '';
      if (seedStatus.textContent) errors.push(seedStatus.textContent);
      if (pending || failure) errors.push(failure || 'Wait for the preparation manifest to finish loading.');
      if (maximum.value !== '' && (!Number.isInteger(maximum.valueAsNumber) || maximum.valueAsNumber <= 0)) errors.push('Maximum starting configs must be a positive integer.');
      return errors;
    }};
  }

  function boundChoices(scope, strategy, hslScope) {
    return Object.keys(metadata?.bound_catalog || {}).filter(function(key) {
      var parts = key.split('.');
      if (parts[0] !== scope) return false;
      if (parts[1] === 'strategy' && parts[2] !== strategy) return false;
      return parts[1] !== 'hsl' || (scope === 'portfolio') === (hslScope === 'portfolio');
    });
  }

  function initialBound(key, source) {
    var range = metadata?.bound_catalog?.[key];
    if (Array.isArray(range)) return clone(range);
    var path = ['bot'].concat(key.split('.'));
    var current = cp().get(effective(source || {}), path);
    if (Number.isFinite(current)) return [current, current];
    var field = (metadata?.fields || []).find(function(field) {
      return field.path.join('.') === path.join('.');
    });
    if (Number.isFinite(field?.default)) return [field.default, field.default];
    var domain = field?.domain || {};
    var initial = Number.isFinite(domain.minimum) ? domain.minimum : 0;
    if (domain.exclusive_minimum) {
      initial = Number.isFinite(domain.maximum) ? (initial + domain.maximum) / 2 : initial + 1;
    } else if (domain.maximum < initial || (domain.exclusive_maximum && domain.maximum === initial)) {
      initial = domain.maximum - (domain.exclusive_maximum ? 1 : 0);
    }
    return [initial, initial];
  }

  window.PBGuiOptimizeV9Editor={configure:function(value){metadata=clone(value);},toView:toView,fromView:fromView,decorate:decorate,hasSideJsonEdits:hasSideJsonEdits,syncSideJson:syncSideJson,effectiveBotValue:effectiveBotValue,boundChoices:boundChoices,initialBound:initialBound};
}());
