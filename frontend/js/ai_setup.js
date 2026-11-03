/* AI provider setup, default model selection and isolated fixed-input tests. */
(function () {
  'use strict';
  var $ = function (id) { return document.getElementById(id); };
  var params = new URL(window.location.href).searchParams;
  var sections = ['models','chatgpt','opencode','claude-code','openrouter','deepseek','github'];
  var state = {section:sections.includes(params.get('section')) ? params.get('section') : 'models',
    profile:params.get('chatgpt_profile') || 'default', provider:params.get('model_provider') || '',
    model:params.get('model') || '', effort:params.get('effort') || '', speed:params.get('speed') || '',
    profiles:[], providers:{}, models:{}, claudeLoginId:'', statusGeneration:0, modelGeneration:0,
    githubGeneration:0, testGeneration:0, policyReadAt:0, writing:false, testing:false, nameDirty:false, nameCleared:false, nameRevision:0,
    loginProfile:'', loginGeneration:0, loginDeadline:0, loginTimer:null, timer:null, stopped:false};
  var controllers = new Map();
  var connectionTests = {};
  var modelSearches = {model:window.PBGuiAIModelSearch.attach($('model-select'),$('model-search'),'pbgui.ai.setupModelSearch'),openrouter:window.PBGuiAIModelSearch.attach($('openrouter-test-model'),$('openrouter-test-search'),'pbgui.ai.setupTestModelSearch')};
  ['chatgpt','opencode','openrouter','claude-code','deepseek'].forEach(function(key){connectionTests[key]={generation:0,choices:new Map(),models:new Map(),fingerprint:'',loading:false};});
  function notice(message,error) { $('setup-status').textContent=message || ''; $('setup-status').classList.toggle('error',!!error); }
  function report(error) { if(error.name!=='AbortError') notice(error.message,true); }
  function option(select,value,label) { var node=document.createElement('option');node.value=value;node.textContent=label;select.appendChild(node);return node; }
  async function api(path,options,key,debug) {
    options=options || {};key=key || path;
    if(controllers.has(key))controllers.get(key).abort();
    var controller=new AbortController();controllers.set(key,controller);
    var timeout=setTimeout(function(){controller.abort();},path==='/test-model' ? 185000 : 30000);
    try {
      var base=debug ? window.API_BASE.replace(/\/ai\/?$/,'/debug') : window.API_BASE;
      var response=await fetch(base+path,Object.assign({credentials:'same-origin',cache:'no-store'},options,{signal:controller.signal}));
      var data=await response.json();
      if(!response.ok){if(response.status===401)clearSecrets();var error=new Error(typeof data.detail==='string' ? data.detail : 'Request failed.');error.status=response.status;throw error;}
      return data;
    } finally {clearTimeout(timeout);if(controllers.get(key)===controller)controllers.delete(key);}
  }
  function json(method,value) {return {method:method,headers:{'Content-Type':'application/json'},body:JSON.stringify(value)};}
  function remember() {
    var url=new URL(window.location.href);
    url.searchParams.set('section',state.section);url.searchParams.set('chatgpt_profile',state.profile);
    [['model_provider',state.provider],['model',state.model],['effort',state.effort],['speed',state.speed]].forEach(function(pair){if(pair[1])url.searchParams.set(pair[0],pair[1]);else url.searchParams.delete(pair[0]);});
    url.searchParams.delete('conversation');history.replaceState(null,'',url);
  }
  function clearSecrets() {['github-token','go-key','openrouter-key','deepseek-key','claude-code-login-code'].forEach(function(id){$(id).value='';});}
  function selectSection(section) {
    state.section=sections.includes(section) ? section : 'models';
    document.querySelectorAll('[data-view]').forEach(function(node){node.hidden=node.dataset.view!==state.section;});
    document.querySelectorAll('[data-section]').forEach(function(node){node.classList.toggle('active',node.dataset.section===state.section);node.setAttribute('aria-current',node.dataset.section===state.section ? 'page' : 'false');});
    clearSecrets();remember();
    if(state.section==='github')refreshGitHub().catch(report);
    if(connectionTests[state.section])loadConnectionModels(state.section);
  }
  function provider() { return state.provider.startsWith('chatgpt:') ? 'chatgpt' : state.provider; }
  function selection() {return {provider:provider(),profile:state.provider.startsWith('chatgpt:') ? state.provider.slice(8) : 'default',model:state.model,effort:state.effort,service_tier:state.speed};}
  function controls() {
    var model=state.models[state.model],blocked=state.writing || state.testing;
    ['provider-select','model-select','chatgpt-profile','chatgpt-profile-add','chatgpt-profile-rename','chatgpt-profile-remove','github-connect','github-disconnect','go-connect','go-disconnect','openrouter-connect','openrouter-disconnect','deepseek-connect','deepseek-disconnect','jev-max-cost-save'].forEach(function(id){$(id).disabled=blocked;});
    $('provider-select').disabled=blocked || !$('provider-select').options.length;
    $('github-check').disabled=blocked;
    Object.keys(connectionTests).forEach(function(key){var slot=connectionTests[key],chosen=slot.choices.get($(key+'-test-model').value);$(key+'-test').disabled=blocked || slot.loading || !chosen || !(state.providers[chosen.provider] || {}).connected || key==='chatgpt' && chosen.profile!==state.profile;$(key+'-test-model').disabled=blocked || slot.loading || !slot.choices.size;});
    $('model-select').disabled=blocked || !Object.keys(state.models).length;
    $('save-model').disabled=$('test-model').disabled=blocked || !model;
    ['effort-select','speed-select'].forEach(function(id){$(id).disabled=blocked || !model;});
    var loggingIn=!!state.loginProfile;
    ['chatgpt-profile','chatgpt-profile-add','chatgpt-profile-rename','chatgpt-profile-remove'].forEach(function(id){$(id).disabled=blocked || loggingIn;});
    $('chatgpt-connect').disabled=$('chatgpt-device-connect').disabled=blocked || loggingIn;
    var claude=state.providers['claude-code'] || {};
    $('claude-code-connect').disabled=blocked || !claude.available || !!(claude.login && claude.login.active);
    ['claude-code-disconnect','claude-code-login-submit','claude-code-login-cancel'].forEach(function(id){$(id).disabled=blocked;});
    privacyControl('model',provider()==='openrouter' ? state.models[state.model] : null);
    privacyControl('openrouter',connectionTests.openrouter.models.get($('openrouter-test-model').value));
  }
  function privacyControl(prefix,model) {
    var allowed=!!model && model.data_collection_allowed===true, visible=!!model && !model.decision && allowed;
    $(prefix+'-privacy').hidden=!visible;
    $(prefix+'-privacy-status').textContent=allowed ? 'Provider data collection: allowed' : 'Provider data collection: blocked';
    $(prefix+'-privacy-toggle').textContent=allowed ? 'Disable provider data collection' : 'Allow provider data collection';
    $(prefix+'-privacy-toggle').disabled=!visible || state.writing || state.testing;
  }
  async function changePrivacy(prefix) {
    if(state.writing || state.testing)return;
    var model=prefix==='model' ? (provider()==='openrouter' && state.models[state.model]) : connectionTests.openrouter.models.get($('openrouter-test-model').value);
    if(!model || model.decision)return;
    if(model.data_collection_allowed!==true)return;
    if(state.stopped || state.writing || state.testing)return;
    await mutate(async function(){
      await api('/providers/openrouter/privacy',json('PUT',{model:model.id,allow_data_collection:false}));
      window.dispatchEvent(new CustomEvent('pbgui:ai-models-changed'));
    },'Data collection blocked for this model. Test the connection again.');
  }
  ['model','openrouter'].forEach(function(prefix){$(prefix+'-privacy-toggle').addEventListener('click',function(){changePrivacy(prefix).catch(report);});});
  function modelDetails() {
    var model=state.models[state.model];
    $('model-info').textContent=model ? [model.decision ? 'Structured decisions' : model.tool_capable || model.tools ? 'Tools' : 'Text',model.provider==='openrouter' ? (model.image_input ? 'Images' : 'Text only') : '',model.reasoning_mandatory ? 'Reasoning required' : '',provider()==='openrouter' ? window.PBGuiAIModelSearch.priceText(model) : '',model.retention ? 'Retention: '+model.retention : '',model.training ? 'Training enabled' : '',model.health && model.health.status ? 'Availability: '+model.health.status : ''].filter(Boolean).join(' · ') : 'Selected model unavailable. Connect its provider or select another model.';
    [['effort-select','effort-label',model && model.reasoning_variants,state.effort,'Standard'],['speed-select','speed-label',provider()==='chatgpt' && model && model.service_tiers,state.speed,'Model default']].forEach(function(row){
      var select=$(row[0]);select.replaceChildren();option(select,'',row[4]);
      var variants=Array.isArray(row[2]) ? row[2] : [];
      if(row[0]==='speed-select' && variants.length)option(select,'default','Standard');
      variants.forEach(function(item){if(item && typeof item.id==='string')option(select,item.id,String(item.label || item.id));});
      if(Array.from(select.options).some(function(item){return item.value===row[3];}))select.value=row[3];
      else if(row[3])option(select,row[3],row[3]+' · unavailable').selected=true;
      $(row[1]).hidden=!variants.length && !row[3];
    });
    controls();
  }
  async function loadModels(background) {
    var generation=++state.modelGeneration, selected=selection();
    if(!background){state.models={};$('model-select').replaceChildren();modelSearches.model.refresh();modelDetails();}
    if(!(state.providers[selected.provider] || {}).connected)return;
    try {
      var result=await api('/models?provider='+encodeURIComponent(selected.provider)+'&profile='+encodeURIComponent(selected.profile),{},'models');
      if(generation!==state.modelGeneration || state.stopped)return;
      if(selected.provider==='openrouter')state.policyReadAt=Date.now();
      state.models={};$('model-select').replaceChildren();
      (result.models || []).forEach(function(model){state.models[model.id]=model;option($('model-select'),model.id,String(model.name || model.id)+(model.free ? ' · Free' : '')+(model.decision ? ' · Decision' : ''));if(model.catalog_error)notice(model.catalog_error,true);});
      if(!state.model){var preferred=(result.models || []).find(function(model){return model.default;}) || (result.models || [])[0];state.model=preferred ? preferred.id : '';}
      if(state.model && !state.models[state.model])option($('model-select'),state.model,state.model+' · unavailable');
      $('model-select').value=state.model;modelSearches.model.refresh(state.models);modelDetails();remember();
    } catch(error){if(generation===state.modelGeneration)report(error);}
  }
  function useTestedSelection(chosen) {
    var tested = Object.assign({}, chosen);
    window.PBGuiAI = window.PBGuiAI || {};
    window.PBGuiAI.pendingTestedSelection = tested;
    window.dispatchEvent(new CustomEvent('pbgui:ai-model-tested', {detail: tested}));
  }
  async function testSelectedModel(chosen,generation) {
    var completed=null;
    try {
      var result=await api('/test-model',json('POST',chosen),'test');
      completed=result;
      if(chosen.provider==='openrouter' && result.tool_probe && result.tool_probe.status==='failed' &&
        String(result.tool_probe.message || '').startsWith('OpenRouter reports that the privacy/data-policy filters'))throw new Error(result.tool_probe.message);
      return result;
    }
    catch(error) {
      var model=state.models[chosen.model];
      connectionTests.openrouter.models.forEach(function(row){if(row.id===chosen.model)model=row;});
      if(chosen.provider!=='openrouter' || !model || model.decision || model.data_collection_allowed ||
        !error.message.startsWith('OpenRouter reports that the privacy/data-policy filters') ||
        generation!==state.testGeneration || state.stopped)throw error;
      var approved=await window.PBGuiDialogs.confirm({title:'Allow data collection for this model?',
        message:'OpenRouter rejected '+String(model.name || model.id)+' under the current privacy policy. Allow providers to store submitted chat content, images and tool results and potentially use them for training? Permission applies only to this model version and your current connection. The model test will then be retried once and may consume credits. OpenRouter account restrictions still apply.',
        confirmText:'Allow and test again',cancelText:'Keep blocked'});
      if(!approved && completed && generation===state.testGeneration && !state.stopped)return completed;
      if(!approved || generation!==state.testGeneration || state.stopped)throw error;
      await api('/providers/openrouter/privacy',json('PUT',{model:chosen.model,allow_data_collection:true}));
      if(state.models[chosen.model])state.models[chosen.model].data_collection_allowed=true;
      connectionTests.openrouter.models.forEach(function(row){if(row.id===chosen.model)row.data_collection_allowed=true;});
      controls();window.dispatchEvent(new CustomEvent('pbgui:ai-models-changed'));
      if(generation!==state.testGeneration || state.stopped)throw error;
      return await api('/test-model',json('POST',chosen),'test');
    }
  }
  function probeSummary(result) {return result.tool_probe ? (result.tool_probe.status==='failed' ? 'PBGui tools disabled: ' : 'PBGui tools: ')+result.tool_probe.message : '';}
  async function refreshAfterProbe(result) {
    if(!result.tool_probe)return;
    window.dispatchEvent(new CustomEvent('pbgui:ai-models-changed'));
    if(provider()==='openrouter')await loadModels();
  }
  function buildProviders() {
    var select=$('provider-select');select.replaceChildren();
    state.profiles.forEach(function(profile){option(select,'chatgpt:'+profile.id,'ChatGPT · '+profile.name);});
    [['opencode-zen','OpenCode Zen'],['opencode-go','OpenCode Go'],['claude-code','Claude Agent'],['openrouter','OpenRouter'],['deepseek','DeepSeek']].forEach(function(pair){option(select,pair[0],pair[1]);});
    if(!state.provider){var connected=['chatgpt','opencode-go','opencode-zen','openrouter','claude-code','deepseek'].find(function(name){return (state.providers[name] || {}).connected;});state.provider=connected==='chatgpt' ? 'chatgpt:'+state.profile : connected || 'chatgpt:'+state.profile;}
    if(!Array.from(select.options).some(function(item){return item.value===state.provider;}))option(select,state.provider,state.provider+' · unavailable');
    select.value=state.provider;controls();
  }
  function providerStatus(name,connected,label,error) {var node=$(name+'-status');node.textContent=label;node.classList.toggle('connected',!!connected);node.classList.toggle('error',!!error);}
  function connectionModelPrice() {
    var slot=connectionTests.openrouter, node=$('openrouter-model-cost');
    var model=slot.models.get($('openrouter-test-model').value);
    node.textContent=model ? window.PBGuiAIModelSearch.priceText(model) : '';
    node.hidden=!model || $('openrouter-test-model').hidden;
  }
  async function loadConnectionModels(key,force,background) {
    var slot=connectionTests[key];
    if(!slot || state.stopped || state.testing)return;
    var providers=key==='opencode' ? ['opencode-zen','opencode-go'] : [key];
    var profile=key==='chatgpt' ? state.profile : 'default';
    var fingerprint=JSON.stringify([profile,providers.map(function(name){var value=state.providers[name] || {};return [name,!!value.connected,value.catalog || [],value.privacy_revision || ''];})]);
    if(!force && fingerprint===slot.fingerprint && !(key==='openrouter' && Date.now()-(slot.policyReadAt || 0)>=300000))return;
    var select=$(key+'-test-model'),previous=select.value || params.get('test_'+key) || '';
    var generation=++slot.generation;slot.fingerprint=fingerprint;slot.loading=!background;
    if(!background){slot.choices.clear();slot.models.clear();select.replaceChildren();}
    if(!background){$(key+'-test-result').hidden=true;if(key==='openrouter')connectionModelPrice();option(select,'','Loading models…');}controls();
    try {
      var available=providers.filter(function(name){return (state.providers[name] || {}).connected;});
      var results=await Promise.allSettled(available.map(async function(name){return {provider:name,payload:await api('/models?provider='+encodeURIComponent(name)+'&profile='+encodeURIComponent(profile),{},'connection-models:'+name)};}));
      if(generation!==slot.generation || state.stopped)return;
      if(background)previous=select.value;
      slot.choices.clear();slot.models.clear();select.replaceChildren();var preferred='',defaultModel='',failures=[];
      results.forEach(function(result,index){
        if(result.status!=='fulfilled'){if(result.reason.name==='TypeError' || result.reason.name==='AbortError' || result.reason.status>=500 || result.reason.status===429)slot.fingerprint='';if(result.reason.name!=='AbortError')failures.push(available[index]+': '+result.reason.message);return;}
        var name=result.value.provider;
        (result.value.payload.models || []).forEach(function(model){
          var value=JSON.stringify([name,model.id]);slot.models.set(value,model);slot.choices.set(value,{provider:name,profile:profile,model:model.id,effort:'',service_tier:''});
          option(select,value,(key==='opencode' ? (name==='opencode-go' ? 'Go · ' : 'Zen · ') : '')+String(model.name || model.id));
          if(model.default && !defaultModel)defaultModel=value;
          if(name===provider() && model.id===state.model)preferred=value;
        });
      });
      select.value=slot.choices.has(previous) ? previous : preferred || defaultModel || (select.options[0] || {}).value || '';
      if(!slot.choices.size)option(select,'',available.length ? 'No models available' : 'Connect this provider first');
      if(modelSearches[key])modelSearches[key].refresh(slot.models);if(key==='openrouter'){slot.policyReadAt=Date.now();connectionModelPrice();}
      if(failures.length){var output=$(key+'-test-result');output.hidden=false;output.classList.add('error');output.textContent=failures.join('\n');}
    } finally {if(generation===slot.generation){slot.loading=false;controls();}}
  }
  async function usage(providerName,target,generation) {
    if(!(state.providers[providerName] || {}).connected){$(target).replaceChildren();return;}
    try {var result=await api('/usage?provider='+providerName,{},'usage:'+providerName);if(generation===state.statusGeneration){if(providerName==='openrouter')window.PBGuiAIUsage.renderOpenRouter($(target),result);else window.PBGuiAIUsage.render($(target),result);}}
    catch(error){if(generation===state.statusGeneration && error.name!=='AbortError')$(target).textContent='Usage currently unavailable';}
  }
  async function refreshStatus(forceModels) {
    if(state.stopped || document.hidden || state.writing)return;
    var generation=++state.statusGeneration,profile=state.profile;
    try {
      var result=await api('/status?profile='+encodeURIComponent(profile),{},'status');
      if(generation!==state.statusGeneration || profile!==state.profile || state.stopped || state.writing)return;
      function inventory(value) {return JSON.stringify(Object.entries(value).map(function(pair){return [pair[0],!!pair[1].connected,pair[1].profiles || [],pair[1].catalog || [],pair[1].privacy_revision || ''];}));}
      var fingerprint=inventory(state.providers);state.providers=result.providers || {};
      var chat=state.providers.chatgpt || {}, go=state.providers['opencode-go'] || {}, router=state.providers.openrouter || {}, deepseek=state.providers.deepseek || {};
      state.profiles=chat.profiles || [{id:'default',name:'Default'}];
      $('chatgpt-profile').replaceChildren();state.profiles.forEach(function(item){option($('chatgpt-profile'),item.id,item.name);});
      var current=state.profiles.find(function(item){return item.id===profile;});
      if(!current)option($('chatgpt-profile'),profile,'Removed profile');$('chatgpt-profile').value=profile;
      if(!state.nameDirty && !state.nameCleared)$('chatgpt-profile-name').value=current ? current.name : '';
      $('chatgpt-account').textContent=chat.email || '';window.PBGuiAIUsage.render($('chatgpt-limits'),chat);
      providerStatus('chatgpt',chat.connected,chat.connected ? 'Connected'+(chat.plan ? ' · '+chat.plan : '') : chat.available ? 'Not connected' : 'Runtime missing',!chat.available);
      providerStatus('go',go.connected,go.connected ? 'Connected' : 'Not connected');providerStatus('openrouter',router.connected,router.connected ? 'Connected' : 'Not connected');
      providerStatus('deepseek',deepseek.connected,deepseek.connected ? 'Connected' : 'Not connected');
      var claude=state.providers['claude-code'] || {}, login=claude.login || {};
      if(state.claudeLoginId!==(login.id || ''))$('claude-code-login-code').value='';
      state.claudeLoginId=login.id || '';
      providerStatus('claude-code',claude.connected,claude.error || login.error || (claude.connected ? 'Connected'+(claude.plan ? ' · '+claude.plan : '') : login.active ? 'Waiting for sign-in' : claude.available ? 'Not connected' : 'Runtime missing'),!!claude.error || !!login.error || !claude.available);
      $('claude-code-account').textContent=claude.email || '';
      $('claude-code-connect').hidden=!!claude.connected;
      $('claude-code-disconnect').hidden=!claude.connected;
      $('claude-code-login').hidden=!!claude.connected || !login.active;
      var link=$('claude-code-login-link');link.hidden=!login.url;
      if(login.url)link.setAttribute('href',login.url);else link.removeAttribute('href');
      if(claude.connected || !login.active)$('claude-code-login-code').value='';
      $('chatgpt-connect').hidden=$('chatgpt-device-connect').hidden=!current || chat.connected || !chat.available;
      $('chatgpt-disconnect').hidden=!chat.connected;
      $('go-connect-row').hidden=!!go.connected;$('go-disconnect').hidden=!go.connected;
      $('openrouter-connect-row').hidden=!!router.connected;$('openrouter-disconnect').hidden=!router.connected;
      $('deepseek-connect-row').hidden=!!deepseek.connected;$('deepseek-disconnect').hidden=!deepseek.connected;
      usage('opencode-go','go-usage',generation);usage('openrouter','openrouter-usage',generation);
      buildProviders();
      if(forceModels || !state.testing && (fingerprint!==inventory(state.providers) || provider()==='openrouter' && Date.now()-state.policyReadAt>=300000))await loadModels();
      if(connectionTests[state.section])await loadConnectionModels(state.section,forceModels);
      if(state.loginProfile===profile && chat.connected){stopLogin();notice('ChatGPT connected.');}
    } catch(error){if(generation===state.statusGeneration)report(error);}
  }
  async function refreshGitHub() {
    if(state.stopped || document.hidden || state.writing)return;
    var generation=++state.githubGeneration;
    var result=await api('/github',{},'github-status',true);
    if(generation!==state.githubGeneration || state.stopped)return;
    if(state.githubLogin!==result.login)$('github-check-result').hidden=true;
    state.githubLogin=result.login;
    providerStatus('github',result.configured,result.configured ? 'Connected · '+String(result.login || '') : 'Not connected');
    $('github-connect-row').hidden=!!result.configured;$('github-disconnect').hidden=!result.configured;
    $('github-check').hidden=!result.configured;
    if(!result.configured)$('github-check-result').hidden=true;
  }
  async function mutate(action,message) {
    if(state.writing || state.testing)return;
    ++state.statusGeneration;
    state.writing=true;controls();
    try {await action();notice(message);}
    catch(error){report(error);}
    finally {state.writing=false;controls();if(!state.stopped){await refreshStatus(true);await refreshGitHub().catch(report);}}
  }
  function stopLogin() {
    ++state.loginGeneration;clearTimeout(state.loginTimer);state.loginTimer=null;state.loginProfile='';state.loginDeadline=0;
    $('login-box').classList.remove('visible');$('login-link').removeAttribute('href');$('login-link').textContent='';$('login-code').textContent='';controls();
  }
  function pollLogin() {
    clearTimeout(state.loginTimer);
    if(!state.loginProfile || state.stopped)return;
    if(Date.now()>=state.loginDeadline){stopLogin();notice('ChatGPT login timed out. Start it again.',true);return;}
    state.loginTimer=setTimeout(async function(){await refreshStatus(false);pollLogin();},2000);
  }
  async function startLogin(mode) {
    if(state.loginProfile || state.writing)return;
    state.loginProfile=state.profile;var generation=++state.loginGeneration;controls();
    try {
      var result=await api('/providers/chatgpt/'+(mode==='device' ? 'device-login' : 'browser-login')+'?profile='+encodeURIComponent(state.loginProfile),{method:'POST'},'login');
      if(generation!==state.loginGeneration || state.stopped)return;
      var raw=mode==='device' ? result.verification_url : result.auth_url,url=new URL(raw);
      if(!['http:','https:'].includes(url.protocol))throw new Error('Invalid login address.');
      $('login-instructions').textContent=mode==='device' ? 'Open this address and enter the code:' : 'Open this address to sign in:';
      $('login-link').href=url.href;$('login-link').textContent=url.href;$('login-code').textContent=mode==='device' ? result.user_code : '';
      $('login-code').hidden=mode!=='device';$('login-box').classList.add('visible');state.loginDeadline=Date.now()+600000;pollLogin();
    } catch(error){if(generation===state.loginGeneration){stopLogin();report(error);}}
  }
  async function changeProfile(profile) {
    ++state.statusGeneration;++state.modelGeneration;++state.testGeneration;
    state.profile=profile;state.nameDirty=false;state.nameCleared=false;++state.nameRevision;$('chatgpt-account').textContent='';$('chatgpt-limits').replaceChildren();
    if(state.provider.startsWith('chatgpt:')){state.provider='chatgpt:'+profile;state.model='';state.effort='';state.speed='';}
    remember();await refreshStatus(true);
  }
  document.querySelectorAll('[data-section]').forEach(function(node){node.addEventListener('click',function(){selectSection(node.dataset.section);});});
  $('open-assistant').addEventListener('click',function(){var button=document.getElementById('pbgui-ai-btn');if(button)button.click();});
  $('github-check').addEventListener('click',async function(){
    if(state.writing || state.testing)return;
    state.writing=true;controls();var output=$('github-check-result');output.hidden=false;output.classList.remove('error');output.textContent='Checking GitHub permissions…';
    try {
      var result=await api('/github/check-permissions',{method:'POST'},'github-check',true);
      if(state.stopped)return;
      var message=result.permissions==='missing' ? 'Token valid, but public_repo permission is missing. Create a classic token with public_repo and reconnect.' : result.permissions==='unknown' ? 'Token valid, but GitHub did not expose classic token scopes. Permissions could not be verified.' : !result.issue_target_available ? 'Token and public_repo permission verified, but the issue repository is unavailable or does not accept issues.' : 'Account '+result.login+' verified. Required public_repo permission and issue repository verified. No issue or attachment was created.';
      output.textContent='Checked '+new Date(result.checked_at ? result.checked_at*1000 : Date.now()).toLocaleTimeString()+': '+message;output.classList.toggle('error',!result.ready);
    } catch(error){if(!state.stopped){output.textContent=error.name==='AbortError' ? 'Permission check stopped or timed out.' : error.message;output.classList.add('error');}}
    finally {state.writing=false;controls();}
  });
  $('chatgpt-profile-name').addEventListener('input',function(){state.nameDirty=true;state.nameCleared=false;++state.nameRevision;});
  $('chatgpt-profile').addEventListener('change',function(){changeProfile($('chatgpt-profile').value).catch(report);});
  ['add','rename'].forEach(function(action){$('chatgpt-profile-'+action).addEventListener('click',function(){
    var name=$('chatgpt-profile-name').value.trim(),profile=state.profile,revision=state.nameRevision;
    if(!name){notice('Enter a profile name.',true);return;}
    var current=state.profiles.find(function(item){return item.id===profile;});
    var unchanged=action==='rename' && current && current.name.trim()===name;
    if(!unchanged && state.profiles.some(function(item){return (action==='add' || item.id!==profile) && item.name.trim()===name;})){
      notice('A profile named "'+name+'" already exists. Choose a different name.',true);return;
    }
    mutate(async function(){
      var result=await api('/providers/chatgpt/profiles',json('POST',{name:name,profile:action==='add' ? null : profile}));
      if(state.stopped || profile!==state.profile)return;
      if(revision===state.nameRevision){
        state.nameDirty=false;state.nameCleared=action==='add';
        $('chatgpt-profile-name').value=action==='add' ? '' : result.name;
      }
      if(action==='add'){state.profile=result.id;if(state.provider.startsWith('chatgpt:')){state.provider='chatgpt:'+result.id;state.model='';}}
      remember();
    },'Profile saved.');
  });});
  $('chatgpt-profile-remove').addEventListener('click',async function(){var profile=state.profile;if(!await window.PBGuiDialogs.confirm({title:'Remove ChatGPT profile',message:'Disconnect and remove this profile? Existing conversation history remains.',confirmText:'Remove profile'}) || profile!==state.profile)return;var revision=state.nameRevision;mutate(async function(){await api('/providers/chatgpt/profiles/'+encodeURIComponent(profile),{method:'DELETE'});state.profile=(state.profiles.find(function(item){return item.id!==profile;}) || {id:'default'}).id;state.nameCleared=false;if(revision===state.nameRevision)state.nameDirty=false;if(state.provider.startsWith('chatgpt:')){state.provider='chatgpt:'+state.profile;state.model='';}remember();},'Profile removed.');});
  $('chatgpt-connect').addEventListener('click',function(){startLogin('browser');});$('chatgpt-device-connect').addEventListener('click',function(){startLogin('device');});
  $('login-cancel').addEventListener('click',async function(){var profile=state.loginProfile;stopLogin();if(profile)try{await api('/providers/chatgpt/login-cancel?profile='+encodeURIComponent(profile),{method:'POST'});}catch(error){report(error);}});
  $('chatgpt-disconnect').addEventListener('click',function(){mutate(function(){return api('/providers/chatgpt/connection?profile='+encodeURIComponent(state.profile),{method:'DELETE'});},'ChatGPT disconnected.');});
  [['go','opencode-go'],['openrouter','openrouter'],['deepseek','deepseek'],['github','github']].forEach(function(pair){
    var name=pair[0],remote=pair[1],github=name==='github',input=$(github ? 'github-token' : name+'-key');
    $(name+'-connect').addEventListener('click',function(){var secret=input.value.trim();input.value='';if(!secret){notice('Enter the '+(github ? 'GitHub token.' : 'API key.'),true);return;}mutate(function(){return api(github ? '/github/connect' : '/providers/'+remote+'/connect',json('POST',github ? {token:secret} : {api_key:secret}),name+'-connect',github);},'Connection saved.');});
    $(name+'-disconnect').addEventListener('click',function(){input.value='';mutate(function(){return api(github ? '/github' : '/providers/'+remote+'/connection',{method:'DELETE'},name+'-disconnect',github);},'Disconnected.');});
  });
  $('jev-max-cost-save').addEventListener('click',function(){var cost=Number($('jev-max-cost').value);if(!Number.isFinite(cost) || cost<.000001 || cost>1){notice('Enter a limit between $0.000001 and $1.',true);return;}mutate(function(){return api('/preferences',json('PUT',{jev_max_cost_usd:cost}));},'Jev limit saved.');});
  $('provider-select').addEventListener('change',async function(){++state.testGeneration;state.provider=$('provider-select').value;state.model='';state.effort='';state.speed='';$('test-result').hidden=true;if(state.provider.startsWith('chatgpt:') && state.profile!==state.provider.slice(8)){await changeProfile(state.provider.slice(8));return;}remember();await refreshStatus(true);});
  $('model-select').addEventListener('change',function(){++state.testGeneration;state.model=$('model-select').value;state.effort='';state.speed='';$('test-result').hidden=true;modelDetails();remember();});
  [['effort-select','effort'],['speed-select','speed']].forEach(function(pair){$(pair[0]).addEventListener('change',function(){state[pair[1]]=$(pair[0]).value;remember();});});
  $('save-model').addEventListener('click',function(){var chosen=selection();mutate(async function(){var result=await api('/preferences',json('PUT',{selection:chosen}));if(window.PBGuiAI)delete window.PBGuiAI.pendingTestedSelection;window.dispatchEvent(new CustomEvent('pbgui:ai-setup-saved',{detail:result.selection || chosen}));},'Default model saved for new chats.');});
  $('test-model').addEventListener('click',async function(){
    if(state.testing || state.writing)return;var chosen=selection(),generation=++state.testGeneration;
    state.testing=true;controls();$('test-result').hidden=true;notice('Testing selected model…');
    try {var result=await testSelectedModel(chosen,generation);if(generation!==state.testGeneration || state.stopped)return;$('test-result').hidden=false;$('test-summary').textContent=result.provider+' · '+result.model+' · '+result.elapsed_seconds+' s';$('test-output').textContent=[result.reply || '',probeSummary(result)].filter(Boolean).join('\n');notice('Model responded successfully.');useTestedSelection(chosen);await refreshAfterProbe(result);}
    catch(error){if(generation===state.testGeneration)notice(error.name==='AbortError' ? 'Model test stopped or timed out.' : error.message,true);}
    finally {if(generation===state.testGeneration){state.testing=false;controls();}}
  });
  Object.keys(connectionTests).forEach(function(key){
    $(key+'-test-model').addEventListener('change',function(){
      $(key+'-test-result').hidden=true;if(key==='openrouter')connectionModelPrice();
      var url=new URL(location.href);url.searchParams.set('test_'+key,$(key+'-test-model').value);history.replaceState(null,'',url);controls();
    });
    $(key+'-test').addEventListener('click',async function(){
      var chosen=connectionTests[key].choices.get($(key+'-test-model').value);
      if(!chosen || state.testing || state.writing)return;
      var generation=++state.testGeneration,output=$(key+'-test-result');state.testing=true;controls();output.hidden=false;output.classList.remove('error');output.textContent='Testing '+chosen.provider+' · '+chosen.model+'…';
      try {
        var result=await testSelectedModel(chosen,generation);
        if(generation!==state.testGeneration || state.stopped)return;
        output.textContent=['Connection verified: '+result.provider+' · '+result.model+' · '+result.elapsed_seconds+' s',String(result.reply || ''),probeSummary(result)].filter(Boolean).join('\n');
        useTestedSelection(chosen);
        await refreshAfterProbe(result);
      } catch(error){if(generation===state.testGeneration && !state.stopped){output.classList.add('error');output.textContent=error.name==='AbortError' ? 'Connection test stopped or timed out.' : error.message;}}
      finally {if(generation===state.testGeneration){state.testing=false;controls();}}
    });
  });
  async function refreshAll() {await Promise.allSettled([refreshStatus(false),refreshGitHub()]);}
  $('claude-code-connect').addEventListener('click',function(){mutate(function(){return api('/providers/claude-code/connect',{method:'POST'});},'Claude sign-in started. Open the sign-in link when it appears.');});
  $('claude-code-disconnect').addEventListener('click',function(){clearSecrets();mutate(function(){return api('/providers/claude-code/connection',{method:'DELETE'});},'Claude disconnected.');});
  $('claude-code-login-cancel').addEventListener('click',function(){clearSecrets();mutate(function(){return api('/providers/claude-code/login',{method:'DELETE'});},'Claude sign-in cancelled.');});
  $('claude-code-login-submit').addEventListener('click',function(){var code=$('claude-code-login-code').value.trim(),loginId=state.claudeLoginId;$('claude-code-login-code').value='';if(!code)return;mutate(function(){return api('/providers/claude-code/login-code',json('POST',{code:code,login_id:loginId}));},'Sign-in code submitted.');});
  function schedule() {clearTimeout(state.timer);if(state.stopped || document.hidden)return;state.timer=setTimeout(async function(){await refreshAll();schedule();},15000);}
  function suspend() {state.stopped=true;++state.statusGeneration;++state.modelGeneration;++state.githubGeneration;++state.testGeneration;++state.loginGeneration;Object.keys(connectionTests).forEach(function(key){var slot=connectionTests[key];++slot.generation;slot.loading=false;slot.fingerprint='';if(state.testing){$(key+'-test-result').hidden=true;}});clearTimeout(state.timer);clearTimeout(state.loginTimer);controllers.forEach(function(controller){controller.abort();});controllers.clear();state.testing=false;clearSecrets();controls();}
  document.addEventListener('visibilitychange',function(){if(document.hidden)suspend();else {state.stopped=false;refreshAll().then(schedule);pollLogin();}});
  window.addEventListener('pagehide',suspend);
  window.addEventListener('pageshow',function(event){if(event.persisted){state.stopped=false;refreshAll().then(schedule);pollLogin();}});
  window.addEventListener('popstate',function(){var current=new URL(window.location.href).searchParams;selectSection(current.get('section'));});
  if(window.PBGuiAI && window.PBGuiAI.registerPageContext)window.PBGuiAI.registerPageContext({id:'ai-setup',getContext:function(){return {section:state.section};}});
  (async function(){
    try {var preferences=await api('/preferences');var saved=preferences.selection || {};if(!state.provider){state.provider=saved.provider==='chatgpt' ? 'chatgpt:'+(saved.profile || 'default') : saved.provider || '';if(saved.provider==='chatgpt' && !params.has('chatgpt_profile'))state.profile=saved.profile || 'default';state.model=state.model || saved.model || '';if(!params.has('effort'))state.effort=saved.effort || '';if(!params.has('speed'))state.speed=saved.service_tier || '';}$('jev-max-cost').value=String(preferences.jev_max_cost_usd || .01);}
    catch(error){report(error);}
    selectSection(state.section);await Promise.allSettled([refreshStatus(true),refreshGitHub()]);schedule();
  })();
})();
