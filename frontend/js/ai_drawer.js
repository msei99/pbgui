(function () {
  'use strict';
  var apiBase = window.location.origin + '/api/ai';
  var state = {
    open: false,
    conversations: [],
    current: '',
    providers: {},
    models: {},
    poll: null,
    pollGeneration: 0,
    requestGeneration: 0,
    modelGeneration: 0,
    listGeneration: 0,
    proposalGeneration: 0,
    history: false,
    busy: false,
    resizing: false,
    drawerWidth: 460,
    retryMessages: {},
    uiActionIds: new Set(),
    contextTimer: null,
    contextSignature: ''
  };
  var root;

  function el(tag, className, text) {
    var node = document.createElement(tag);
    if (className) node.className = className;
    if (text != null) node.textContent = String(text);
    return node;
  }

  async function api(path, options) {
    var response = await fetch(apiBase + path, Object.assign({ credentials: 'same-origin', cache: 'no-store' }, options || {}));
    var data = {};
    try { data = await response.json(); } catch (_) {}
    if (!response.ok) throw new Error(data.detail || 'AI request failed');
    return data;
  }

  function build() {
    if (root) return;
    root = el('aside');
    root.id = 'pbgui-ai-drawer';
    root.setAttribute('aria-hidden', 'true');
    var resize = el('div', 'pai-resize');
    resize.setAttribute('role', 'separator');
    resize.setAttribute('aria-orientation', 'vertical');
    resize.setAttribute('aria-label', 'Resize AI drawer');
    root.appendChild(resize);
    bindResize(resize);

    var head = el('div', 'pai-head');
    var history = el('button', '', 'History');
    history.type = 'button';
    history.addEventListener('click', function () {
      state.history = !state.history;
      root.querySelector('.pai-body').classList.toggle('history-open', state.history);
    });
    head.appendChild(history);
    head.appendChild(el('div', 'pai-title', 'PBGui AI'));
    var full = el('button', '', 'Full');
    full.type = 'button';
    full.title = 'Open full AI Chat';
    full.addEventListener('click', function () { window.location.assign(window.location.origin + '/api/ai/main_page'); });
    head.appendChild(full);
    var remove = el('button', '', 'Delete');
    remove.type = 'button';
    remove.addEventListener('click', deleteConversation);
    head.appendChild(remove);
    var close = el('button', '', 'X');
    close.type = 'button';
    close.setAttribute('aria-label', 'Collapse AI assistant');
    close.addEventListener('click', closeDrawer);
    head.appendChild(close);
    root.appendChild(head);

    var toolbar = el('div', 'pai-toolbar');
    var provider = el('select');
    provider.id = 'pai-provider';
    provider.addEventListener('change', function () { loadModels(); });
    toolbar.appendChild(provider);
    var model = el('select');
    model.id = 'pai-model';
    model.addEventListener('change', function () { rebuildEfforts(); });
    toolbar.appendChild(model);
    var effort = el('select');
    effort.id = 'pai-effort';
    toolbar.appendChild(effort);
    var fresh = el('button', 'pai-new', 'New');
    fresh.type = 'button';
    fresh.addEventListener('click', newConversation);
    toolbar.appendChild(fresh);
    var health = el('button', '', 'Health');
    health.type = 'button';
    health.title = 'Refresh free-model availability';
    health.addEventListener('click', refreshHealth);
    toolbar.appendChild(health);
    root.appendChild(toolbar);

    var context = el('div', 'pai-context');
    var contextToggle = el('label', 'pai-context-toggle');
    var include = document.createElement('input');
    include.type = 'checkbox';
    include.checked = true;
    include.id = 'pai-context-toggle';
    contextToggle.appendChild(include);
    contextToggle.appendChild(el('span', '', 'Include page context'));
    context.appendChild(contextToggle);
    context.appendChild(el('div', 'pai-context-chips'));
    root.appendChild(context);

    var statusRow = el('div', 'pai-status-row');
    statusRow.appendChild(el('div', 'pai-status'));
    var retry = el('button', 'pai-retry', 'Retry');
    retry.type = 'button';
    retry.hidden = true;
    retry.addEventListener('click', retryTurn);
    statusRow.appendChild(retry);

    var body = el('div', 'pai-body');
    var historyPane = el('div', 'pai-history');
    historyPane.appendChild(el('div', 'pai-history-list'));
    body.appendChild(historyPane);
    var chat = el('div', 'pai-chat');
    chat.appendChild(el('div', 'pai-messages'));
    chat.appendChild(el('div', 'pai-proposals'));
    var reasoning = el('details', 'pai-reasoning'); reasoning.hidden = true; reasoning.appendChild(el('summary', '', 'Reasoning summary')); reasoning.appendChild(el('pre', 'pai-reasoning-text')); chat.appendChild(reasoning);
    var activity = el('details', 'pai-reasoning'); activity.hidden = true; activity.appendChild(el('summary', '', 'Activity')); activity.appendChild(el('pre', 'pai-activity-history')); chat.appendChild(activity);
    chat.appendChild(statusRow);
    var compose = el('div', 'pai-compose');
    var prompt = document.createElement('textarea');
    prompt.maxLength = 12000;
    prompt.placeholder = 'Ask about this page...';
    prompt.addEventListener('keydown', function (event) {
      if (event.key === 'Enter' && !event.shiftKey) { event.preventDefault(); sendMessage(); }
    });
    compose.appendChild(prompt);
    var send = el('button', 'primary', 'Send');
    send.type = 'button';
    send.addEventListener('click', function () { sendMessage(); });
    compose.appendChild(send);
    var stop = el('button', 'danger', 'Stop');
    stop.type = 'button';
    stop.hidden = true;
    stop.addEventListener('click', stopTurn);
    compose.appendChild(stop);
    chat.appendChild(compose);
    body.appendChild(chat);
    root.appendChild(body);
    document.body.appendChild(root);
    document.body.appendChild(buildReviewOverlay());
    renderContext(collectContext());
    refreshAll();
  }

  function collectContext() {
    return window.PBGuiAI && typeof window.PBGuiAI.collectContext === 'function' ? window.PBGuiAI.collectContext() : {};
  }

  function renderContext(context) {
    if (!root) return;
    var chips = root.querySelector('.pai-context-chips');
    chips.textContent = '';
    context = context || {};
    var values = [];
    if (context.title || context.page_key) values.push(context.title || context.page_key);
    if (context.section) values.push('Section: ' + context.section);
    (context.entities || []).slice(0, 4).forEach(function (entity) {
      values.push(String(entity.kind || 'item') + ': ' + String(entity.name || ''));
    });
    if (context.focused_field) values.push('Field: ' + String(context.focused_field.label || context.focused_field.path || ''));
    if (!values.length) values.push('Current page');
    values.forEach(function (value) { chips.appendChild(el('span', 'pai-context-chip', value)); });
  }

  function refreshLiveContext() {
    var context = collectContext();
    var signature = '';
    try { signature = JSON.stringify(context); } catch (_) {}
    if (signature === state.contextSignature) return;
    state.contextSignature = signature;
    renderContext(context);
  }

  function startContextWatch() {
    stopContextWatch();
    refreshLiveContext();
    state.contextTimer = window.setInterval(refreshLiveContext, 500);
  }

  function stopContextWatch() {
    if (state.contextTimer) window.clearInterval(state.contextTimer);
    state.contextTimer = null;
  }

  function setStatus(text, error) {
    var node = root.querySelector('.pai-status');
    node.textContent = text || '';
    node.classList.toggle('error', !!error);
  }

  function setBusy(busy) {
    state.busy = !!busy;
    root.querySelector('.primary').hidden = busy;
    root.querySelector('.danger').hidden = !busy;
    root.querySelector('textarea').disabled = busy;
    root.querySelector('.pai-retry').disabled = busy;
    root.querySelector('#pai-provider').disabled = busy || !root.querySelector('#pai-provider').options.length;
    root.querySelector('#pai-model').disabled = busy || !root.querySelector('#pai-model').options.length;
    root.querySelector('#pai-effort').disabled = busy || root.querySelector('#pai-effort').hidden;
    root.querySelector('.pai-new').disabled = busy;
  }

  async function refreshAll() {
    try {
      var preferences = await api('/preferences');
      if (!state.resizing) applyWidth(preferences.drawer_width);
      var status = await api('/status');
      state.providers = status.providers || {};
      rebuildProviders();
      await loadConversations();
    } catch (error) { setStatus(error.message, true); }
  }

  function applyWidth(value) {
    state.drawerWidth = Math.max(180, Number(value || 460));
    if (!root || window.innerWidth <= 760) return;
    root.style.width = Math.round(Math.min(state.drawerWidth, window.innerWidth)) + 'px';
  }

  function saveDrawerPreferences(drawerOpen) {
    var width = root ? root.getBoundingClientRect().width : state.drawerWidth;
    return api('/preferences', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        drawer_width: Math.round(width),
        drawer_open: drawerOpen == null ? state.open : !!drawerOpen
      })
    });
  }

  function bindResize(handle) {
    handle.addEventListener('mousedown', function (event) {
      if (window.innerWidth <= 760 || event.button !== 0 || state.resizing) return;
      event.preventDefault();
      event.stopPropagation();
      state.resizing = true;
      handle.classList.add('active');
      var shield = el('div', 'pai-resize-shield');
      shield.style.cssText = 'position:fixed;inset:0;z-index:2399;cursor:ew-resize;background:transparent;';
      document.body.appendChild(shield);
      var previousUserSelect = document.body.style.userSelect;
      document.body.style.userSelect = 'none';
      var finished = false;
      function move(moveEvent) { applyWidth(window.innerWidth - moveEvent.clientX); }
      async function finish() {
        if (finished) return;
        finished = true;
        document.removeEventListener('mousemove', move);
        document.removeEventListener('mouseup', finish);
        window.removeEventListener('blur', finish);
        shield.remove();
        document.body.style.userSelect = previousUserSelect;
        state.resizing = false;
        handle.classList.remove('active');
        try {
          await saveDrawerPreferences();
        } catch (error) { setStatus(error.message, true); }
      }
      document.addEventListener('mousemove', move);
      document.addEventListener('mouseup', finish);
      window.addEventListener('blur', finish);
    });
  }

  function rebuildProviders(preferred) {
    var select = root.querySelector('#pai-provider');
    var current = preferred || select.value;
    select.textContent = '';
    [['chatgpt', 'ChatGPT'], ['opencode-zen', 'OpenCode Zen'], ['opencode-go', 'OpenCode Go']].forEach(function (provider) {
      if (!(state.providers[provider[0]] || {}).connected) return;
      var option = el('option', '', provider[1]);
      option.value = provider[0];
      select.appendChild(option);
    });
    if (current && Array.from(select.options).some(function (option) { return option.value === current; })) select.value = current;
    loadModels();
  }

  async function loadModels(preferred) {
    var provider = root.querySelector('#pai-provider').value;
    var select = root.querySelector('#pai-model');
    var current = preferred || select.value;
    var generation = ++state.modelGeneration;
    select.textContent = '';
    if (!provider) return;
    try {
      var data = await api('/models?provider=' + encodeURIComponent(provider));
      if (generation !== state.modelGeneration || provider !== root.querySelector('#pai-provider').value) return;
      state.models = {};
      (data.models || []).forEach(function (model) {
        state.models[model.id] = model;
        var healthStatus = model.health && model.health.status ? ' - ' + String(model.health.status).replace(/_/g, ' ') : '';
        var option = el('option', '', model.name + (model.tools ? ' - PBGui tools' : ' - Chat only') + healthStatus);
        option.value = model.id;
        if (model.default) option.selected = true;
        select.appendChild(option);
      });
      if (current && Array.from(select.options).some(function (option) { return option.value === current; })) select.value = current;
      rebuildEfforts();
    } catch (error) { setStatus(error.message, true); }
  }

  function rebuildEfforts(preferred) {
    var model = state.models[root.querySelector('#pai-model').value] || {};
    var select = root.querySelector('#pai-effort');
    var current = preferred == null ? select.value : preferred;
    select.textContent = '';
    var standard = el('option', '', 'Standard' + (model.default_effort ? ' - ' + model.default_effort : ''));
    standard.value = '';
    select.appendChild(standard);
    (model.reasoning_variants || []).forEach(function (variant) {
      var option = el('option', '', variant.label || variant.id);
      option.value = variant.id;
      option.title = variant.description || '';
      select.appendChild(option);
    });
    if (current && Array.from(select.options).some(function (option) { return option.value === current; })) select.value = current;
    select.hidden = select.options.length < 2;
  }

  async function loadConversations(preferredId) {
    var generation = ++state.listGeneration;
    try {
      var data = await api('/conversations');
      if (generation !== state.listGeneration) return;
      state.conversations = data.conversations || [];
      var available = state.conversations.some(function (item) { return item.conversation_id === state.current; });
      if (preferredId) state.current = preferredId;
      else if (!available) state.current = state.conversations.length ? state.conversations[0].conversation_id : '';
      renderHistory();
      if (state.current) await loadConversation(state.current);
      else {
        stopPoll();
        renderMessages([]);
        renderReasoningSummary('');
        renderActivityHistory([]);
        renderProposals([]);
        renderContext(collectContext());
        setBusy(false);
        setStatus('', false);
      }
    } catch (error) { setStatus(error.message, true); }
  }

  function renderHistory() {
    var list = root.querySelector('.pai-history-list');
    list.textContent = '';
    state.conversations.forEach(function (conversation) {
      var button = el('button', conversation.conversation_id === state.current ? 'selected' : '');
      button.type = 'button';
      button.appendChild(el('span', '', conversation.title || 'New chat'));
      var meta = conversation.model || '';
      if (conversation.busy) meta += ' - working';
      else if (conversation.last_error) meta += ' - needs attention';
      button.appendChild(el('small', '', meta));
      button.addEventListener('click', function () {
        state.current = conversation.conversation_id;
        renderHistory();
        loadConversation(state.current);
      });
      list.appendChild(button);
    });
  }

  async function loadConversation(id) {
    var generation = ++state.requestGeneration;
    try {
      var conversation = await api('/conversations/' + encodeURIComponent(id));
      if (id !== state.current || generation !== state.requestGeneration) return;
      renderMessages(conversation.messages || []);
      renderReasoningSummary(conversation.reasoning_summary || '');
      renderActivityHistory(conversation.activity_history || []);
      var uiActions = conversation.ui_actions || [];
      dispatchUiActions(id, uiActions);
      if (conversation.retry_message) state.retryMessages[id] = conversation.retry_message;
      renderContext(conversation.context && Object.keys(conversation.context).length ? conversation.context : collectContext());
      setBusy(!!conversation.busy);
      var retry = root.querySelector('.pai-retry');
      retry.hidden = !conversation.last_error || !state.retryMessages[id] || conversation.busy;
      setStatus(conversation.busy ? (conversation.activity || 'Model is working...') : (conversation.last_error || ''), !!conversation.last_error);
      if (root.querySelector('#pai-provider').value !== conversation.provider) {
        rebuildProviders(conversation.provider);
        await loadModels(conversation.model);
      } else if (root.querySelector('#pai-model').value !== conversation.model) {
        await loadModels(conversation.model);
      }
      if (id !== state.current || generation !== state.requestGeneration) return;
      rebuildEfforts(conversation.effort || '');
      await reconcileProposals(id, generation);
      var pendingPageAction = uiActions.some(function (action) {
        return action && action.type === 'page.perform_action';
      });
      if (conversation.busy || pendingPageAction) schedulePoll(id); else stopPoll();
      if (!conversation.busy && !conversation.last_error) delete state.retryMessages[id];
      var summary = state.conversations.find(function (item) { return item.conversation_id === id; });
      if (summary) Object.assign(summary, conversation);
      renderHistory();
    } catch (error) {
      if (id === state.current && generation === state.requestGeneration) setStatus(error.message, true);
    }
  }

  function dispatchUiActions(conversationId, actions) {
    (actions || []).forEach(function (action) {
      var actionId = String((action || {}).action_id || '');
      if (!actionId || state.uiActionIds.has(actionId)) return;
      if (action.type === 'chat.quick_replies') {
        renderQuickReplies(conversationId, action);
        return;
      }
      var event = new CustomEvent('pbgui:ai-ui-action', {
        cancelable: true,
        detail: action
      });
      window.dispatchEvent(event);
      if (!event.defaultPrevented) return;
      state.uiActionIds.add(actionId);
      api('/conversations/' + encodeURIComponent(conversationId) + '/ui-actions/' + encodeURIComponent(actionId) + '/ack', {
        method: 'POST'
      }).catch(function () {
        state.uiActionIds.delete(actionId);
      });
    });
  }

  function renderQuickReplies(conversationId, action) {
    var payload = action && action.payload && typeof action.payload === 'object' ? action.payload : {};
    var choices = Array.isArray(payload.choices) ? payload.choices : [];
    if (!choices.length) return;
    var box = root.querySelector('.pai-messages');
    Array.from(box.querySelectorAll('.pai-inline-choices')).forEach(function (item) { item.remove(); });
    var existing = box.querySelector('.pai-quick-replies');
    if (existing) existing.remove();
    var row = el('div', 'pai-message assistant pai-quick-replies');
    var content = el('div', 'pai-bubble');
    content.appendChild(el('div', 'pai-quick-question', String(payload.question || 'Choose an option:')));
    var options = el('div', 'pai-quick-options');
    choices.forEach(function (choice) {
      var button = el('button', '', String((choice || {}).label || ''));
      button.type = 'button';
      button.addEventListener('click', function () {
        var value = String((choice || {}).value || '').trim();
        if (!value) return;
        Array.from(options.querySelectorAll('button')).forEach(function (item) { item.disabled = true; });
        api('/conversations/' + encodeURIComponent(conversationId) + '/ui-actions/' + encodeURIComponent(action.action_id) + '/ack', {
          method: 'POST'
        }).then(function () {
          row.remove();
          sendMessage(value);
        }).catch(function (error) {
          Array.from(options.querySelectorAll('button')).forEach(function (item) { item.disabled = false; });
          setStatus(error.message, true);
        });
      });
      options.appendChild(button);
    });
    content.appendChild(options);
    row.appendChild(content);
    box.appendChild(row);
    box.scrollTop = box.scrollHeight;
  }

  function renderMessages(messages) {
    var box = root.querySelector('.pai-messages');
    box.textContent = '';
    if (!messages.length) {
      box.appendChild(el('div', 'pai-empty', 'Ask about the current PBGui page, selected resource, or installed Passivbot source.'));
      return;
    }
    messages.forEach(function (message, index) {
      var row = el('div', 'pai-message ' + (message.role === 'user' ? 'user' : 'assistant'));
      var bubble = el('div', 'pai-bubble', message.content || '');
      if (message.role !== 'user') appendDetectedQuickReplies(bubble, message.content || '');
      row.appendChild(bubble);
      var actions = el('div', 'pai-message-actions');
      var copy = el('button', '', 'Copy'); copy.type = 'button'; copy.addEventListener('click', function () { copyMessage(message.content || ''); }); actions.appendChild(copy);
      if (message.role === 'user') {
        var rewind = el('button', '', 'Rewind'); rewind.type = 'button'; rewind.addEventListener('click', function () { rewindMessage(index); }); actions.appendChild(rewind);
      }
      row.appendChild(actions);
      box.appendChild(row);
    });
    box.scrollTop = box.scrollHeight;
  }

  function detectedQuickReplies(text) {
    var value = String(text || '');
    if (!/(soll ich|möchtest du|welche option|choose|should i|would you like)/i.test(value)) return [];
    var choices = [];
    value.split('\n').forEach(function (line) {
      var match = line.match(/^\s*\d+[.)]\s*(?:\*\*)?(.+?)(?:\*\*)?\s*$/);
      if (!match) return;
      var choice = match[1].replace(/\*\*/g, '').trim();
      if (choice && choices.length < 5) choices.push(choice);
    });
    return choices.length >= 2 ? choices : [];
  }

  function appendDetectedQuickReplies(bubble, text) {
    var choices = detectedQuickReplies(text);
    if (!choices.length) return;
    var options = el('div', 'pai-quick-options pai-inline-choices');
    choices.forEach(function (choice) {
      var label = choice.length > 80 ? choice.slice(0, 77) + '...' : choice;
      var button = el('button', '', label); button.type = 'button'; button.title = choice;
      button.addEventListener('click', function () { sendMessage(choice); });
      options.appendChild(button);
    });
    bubble.appendChild(options);
  }

  function renderReasoningSummary(summary) {
    var details = root.querySelector('.pai-reasoning');
    var text = root.querySelector('.pai-reasoning-text');
    text.textContent = String(summary || '');
    details.hidden = !text.textContent;
  }

  function renderActivityHistory(items) {
    var text = root.querySelector('.pai-activity-history');
    var details = text.closest('details');
    text.textContent = (items || []).map(function (item) { return String(item.message || ''); }).filter(Boolean).join('\n');
    details.hidden = !text.textContent;
  }

  async function copyMessage(text) {
    try { await navigator.clipboard.writeText(String(text || '')); setStatus('Message copied.', false); }
    catch (_) { setStatus('Clipboard access was denied by the browser.', true); }
  }

  async function rewindMessage(messageIndex) {
    if (!state.current) return;
    var confirmed = typeof window.PBGuiConfirm === 'function'
      ? await window.PBGuiConfirm({ title: 'Rewind AI chat', message: 'Remove this message and every response after it?', confirmText: 'Rewind' })
      : false;
    if (!confirmed) return;
    try {
      var result = await api('/conversations/' + encodeURIComponent(state.current) + '/rewind', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ message_index: messageIndex }) });
      root.querySelector('textarea').value = String(result.restored_prompt || '');
      delete state.retryMessages[state.current];
      await loadConversation(state.current);
      setStatus('Chat rewound. Edit or resend the restored prompt.', false);
    } catch (error) { setStatus(error.message, true); }
  }

  async function reconcileProposals(conversationId, generation) {
    var proposalGeneration = ++state.proposalGeneration;
    try {
      var data = await api('/proposals?conversation_id=' + encodeURIComponent(conversationId));
      if (proposalGeneration !== state.proposalGeneration || conversationId !== state.current || (generation != null && generation !== state.requestGeneration)) return;
      renderProposals(data.proposals || []);
    } catch (error) {
      if (conversationId === state.current) setStatus(error.message, true);
    }
  }

  function proposalActionLabel(action) {
    if (action === 'save') return 'Save PB8 optimizer config';
    if (action === 'save_and_queue') return 'Save PB8 config and add to queue';
    if (action === 'queue') return 'Add PB8 config to optimizer queue';
    if (action === 'queue_backtests') return 'Queue PB8 Pareto backtests';
    if (action === 'create_dashboard') return 'Create PBGui dashboard';
    if (action === 'save_dashboard_layout') return 'Save PBGui dashboard layout';
    if (action === 'python_analysis') return 'Run sandboxed Python analysis';
    return 'PBGui action';
  }

  function proposalDetail(preview) {
    if (preview.action === 'python_analysis') return String(preview.code_bytes || 0) + ' bytes of code - ' + String((preview.input_summary || {}).bytes || 0) + ' bytes of sanitized JSON input';
    if (preview.action === 'queue_backtests') return String(preview.job_count || 0) + ' backtest jobs across ' + String((preview.exchanges || []).length) + ' exchanges' + (preview.may_start_immediately ? ' - may start immediately' : '');
    if (preview.action === 'create_dashboard') return 'Create from template ' + String(preview.template || '');
    if (preview.action === 'save_dashboard_layout') return String((preview.layout || {}).rows || 0) + ' rows x ' + String((preview.layout || {}).columns || 0) + ' columns - ' + String(preview.changed_count || 0) + ' changes';
    return String(preview.changed_count || 0) + ' changes' + (preview.may_start_immediately ? ' - may start immediately' : '');
  }

  function proposalReviewText(proposal) {
    var preview = proposal.preview || {};
    if (preview.action === 'python_analysis') {
      var inputReview = preview.input_resource
        ? 'Bound PBGui input resource:\n' + JSON.stringify(preview.input_resource, null, 2)
        : 'Sanitized JSON input:\n' + JSON.stringify(preview.input_data, null, 2);
      return 'Code:\n' + String(preview.code || '') + '\n\nInput summary:\n' + JSON.stringify(preview.input_summary || {}, null, 2) + '\n\n' + inputReview + '\n\nPayload digest:\n' + String(proposal.payload_digest || '');
    }
    return JSON.stringify({ preview: preview, payload_digest: proposal.payload_digest || '' }, null, 2);
  }

  function diffValue(value) {
    if (value === undefined) return 'Not set';
    if (value === null) return 'None';
    if (typeof value === 'string') return value;
    try { return JSON.stringify(value, null, 2); } catch (_) { return String(value); }
  }

  function humanizeDiffName(value) {
    var abbreviations = { adg: 'ADG', eq: 'equity', pct: '%', usd: 'USD', id: 'ID', w: 'weighted' };
    return String(value == null ? '' : value).replace(/_/g, ' ').split(' ').filter(Boolean).map(function (part) {
      return abbreviations[part.toLowerCase()] || part;
    }).join(' ').replace(/^./, function (letter) { return letter.toUpperCase(); });
  }

  function diffPathParts(path) {
    var raw = String(path || 'config');
    var sectionMatch = raw.match(/^[^.[]+/);
    var section = sectionMatch ? sectionMatch[0] : 'config';
    var detail = raw.slice(section.length).replace(/^\./, '');
    var labels = [];
    detail.split('.').filter(Boolean).forEach(function (part) {
      var base = part.replace(/\[[^\]]+\]/g, '');
      if (base) labels.push(humanizeDiffName(base));
      var matches = part.matchAll(/\[([^\]]+)\]/g);
      Array.from(matches).forEach(function (match) { labels.push(humanizeDiffName(match[1])); });
    });
    return { section: humanizeDiffName(section), label: labels.join(' / ') || 'Value' };
  }

  function changeKind(change) {
    if (change.kind === 'added' || change.kind === 'removed' || change.kind === 'changed') return change.kind;
    if (!Object.prototype.hasOwnProperty.call(change, 'before') || change.before === null) return 'added';
    if (!Object.prototype.hasOwnProperty.call(change, 'after') || change.after === null) return 'removed';
    return 'changed';
  }

  function appendDiffValue(parent, value, item) {
    if (value && typeof value === 'object' && !Array.isArray(value)) {
      var fields = el('dl', 'pai-change-fields');
      Object.keys(value).forEach(function (key) {
        if (item && String(value[key]) === String(item)) return;
        fields.appendChild(el('dt', '', humanizeDiffName(key)));
        fields.appendChild(el('dd', '', typeof value[key] === 'string' ? humanizeDiffName(value[key]) : diffValue(value[key])));
      });
      if (fields.childNodes.length) parent.appendChild(fields);
      return;
    }
    var rendered = typeof value === 'string' ? humanizeDiffName(value) : diffValue(value);
    parent.appendChild(el('div', 'pai-change-value', rendered));
  }

  function buildChangeCard(change) {
    var path = diffPathParts(change.path);
    var kind = changeKind(change);
    var card = el('div', 'pai-change-card ' + kind);
    var heading = el('div', 'pai-change-heading');
    var label = path.label + (change.item ? ' / ' + humanizeDiffName(change.item) : '');
    heading.appendChild(el('strong', 'pai-change-label', label));
    heading.appendChild(el('span', 'pai-change-kind', kind === 'added' ? 'Added' : kind === 'removed' ? 'Removed' : 'Changed'));
    card.appendChild(heading);
    var values = el('div', 'pai-change-values');
    if (kind === 'changed') {
      var before = el('div', 'pai-change-side before');
      before.appendChild(el('small', '', 'Before'));
      appendDiffValue(before, change.before, change.item);
      values.appendChild(before);
      values.appendChild(el('span', 'pai-change-arrow', '→'));
      var after = el('div', 'pai-change-side after');
      after.appendChild(el('small', '', 'After'));
      appendDiffValue(after, change.after, change.item);
      values.appendChild(after);
    } else {
      appendDiffValue(values, kind === 'added' ? change.after : change.before, change.item);
    }
    card.appendChild(values);
    return { section: path.section, card: card };
  }

  function buildProposalDiff(preview) {
    var diff = el('div', 'pai-diff');
    var groups = new Map();
    (preview.changes || []).forEach(function (change) {
      var rendered = buildChangeCard(change);
      if (!groups.has(rendered.section)) groups.set(rendered.section, []);
      groups.get(rendered.section).push(rendered.card);
    });
    groups.forEach(function (cards, section) {
      var group = el('section', 'pai-change-group');
      group.appendChild(el('h4', '', section));
      var list = el('div', 'pai-change-list');
      cards.forEach(function (card) { list.appendChild(card); });
      group.appendChild(list);
      diff.appendChild(group);
    });
    if (!(preview.changes || []).length) diff.appendChild(el('div', 'pai-proposal-detail', 'No field-level differences were returned.'));
    return diff;
  }

  function buildReviewOverlay() {
    var overlay = el('div', 'pai-review-overlay'); overlay.hidden = true;
    var dialog = el('div', 'pai-review-dialog'); dialog.setAttribute('role', 'dialog'); dialog.setAttribute('aria-modal', 'true');
    var head = el('div', 'pai-review-head'); head.appendChild(el('strong', '', 'Review proposed changes'));
    var close = el('button', '', 'X'); close.type = 'button'; close.setAttribute('aria-label', 'Close proposal review'); close.addEventListener('click', closeProposalReview); head.appendChild(close); dialog.appendChild(head);
    dialog.appendChild(el('div', 'pai-review-content'));
    dialog.appendChild(el('div', 'pai-review-actions'));
    overlay.appendChild(dialog); return overlay;
  }

  function closeProposalReview() {
    var overlay = document.querySelector('.pai-review-overlay');
    if (!overlay) return;
    overlay.hidden = true; overlay.querySelector('.pai-review-content').textContent = ''; overlay.querySelector('.pai-review-actions').textContent = '';
  }

  function openProposalReview(proposal, card) {
    var preview = proposal.preview || {}, overlay = document.querySelector('.pai-review-overlay'), content = overlay.querySelector('.pai-review-content'), actions = overlay.querySelector('.pai-review-actions');
    content.textContent = ''; actions.textContent = '';
    content.appendChild(el('h3', '', proposalActionLabel(preview.action) + (preview.name ? ': ' + String(preview.name) : '')));
    content.appendChild(el('div', 'pai-proposal-detail', proposalDetail(preview)));
    if (preview.action === 'python_analysis') content.appendChild(el('pre', 'pai-review-raw', proposalReviewText(proposal)));
    else {
      content.appendChild(buildProposalDiff(preview));
      var raw = el('details', 'pai-proposal-raw'); raw.appendChild(el('summary', '', 'Raw JSON')); raw.appendChild(el('pre', 'pai-review-raw', proposalReviewText(proposal))); content.appendChild(raw);
    }
    var reject = el('button', '', 'Reject'); reject.type = 'button'; reject.addEventListener('click', async function () { closeProposalReview(); await resolveProposal(proposal, false, card); }); actions.appendChild(reject);
    var approve = el('button', 'primary', 'Approve'); approve.type = 'button'; approve.addEventListener('click', async function () { closeProposalReview(); await resolveProposal(proposal, true, card); }); actions.appendChild(approve);
    overlay.hidden = false;
    var closeButton = overlay.querySelector('[aria-label="Close proposal review"]');
    if (closeButton) closeButton.focus();
  }

  function appendAnalysisResult(result) {
    var output = result.output || {};
    var rendered = output.format === 'json' ? JSON.stringify(output.value, null, 2) : String(output.text || '');
    var stderr = String(result.stderr || '');
    var text = 'Python analysis ' + String(result.analysis_status || 'completed') + ' (exit ' + String(result.exit_code) + ').\n\n' + rendered + (stderr ? '\n\nstderr:\n' + stderr : '') + ((result.stdout_truncated || result.stderr_truncated) ? '\n\nOutput was truncated by PBGui limits.' : '');
    var box = root.querySelector('.pai-messages');
    var row = el('div', 'pai-message assistant');
    row.appendChild(el('div', 'pai-bubble', text));
    box.appendChild(row);
    box.scrollTop = box.scrollHeight;
  }

  function renderProposals(proposals) {
    var list = root.querySelector('.pai-proposals');
    list.textContent = '';
    (proposals || []).forEach(function (proposal) {
      var preview = proposal.preview || {};
      var card = el('div', 'pai-proposal');
      var main = el('div', 'pai-proposal-main');
      main.appendChild(el('div', 'pai-proposal-title', proposalActionLabel(preview.action) + (preview.name ? ': ' + String(preview.name) : '')));
      main.appendChild(el('div', 'pai-proposal-detail', proposalDetail(preview)));
      card.appendChild(main);
      var actions = el('div', 'pai-proposal-actions');
      var reject = el('button', '', 'Reject');
      reject.type = 'button';
      reject.addEventListener('click', function () { resolveProposal(proposal, false, card); });
      actions.appendChild(reject);
      var approve = el('button', 'primary', 'Review changes');
      approve.type = 'button';
      approve.addEventListener('click', function () { openProposalReview(proposal, card); });
      actions.appendChild(approve);
      card.appendChild(actions);
      list.appendChild(card);
    });
  }

  async function resolveProposal(proposal, approve, card) {
    var conversationId = state.current;
    var preview = proposal.preview || {};
    var buttons = Array.from(card.querySelectorAll('button'));
    buttons.forEach(function (button) { button.disabled = true; });
    if (approve) {
      var approvalDetail = preview.action === 'python_analysis'
        ? 'The reviewed code and sanitized input will run without network or host-data access. Proposal integrity is verified before execution.'
        : 'Apply ' + String(preview.changed_count || 0) + ' reviewed changes. ' + (preview.may_start_immediately ? 'Queue autostart is enabled; this may start immediately. ' : '') + 'Proposal integrity is verified before execution.';
      var confirmed = typeof window.PBGuiConfirm === 'function' && await window.PBGuiConfirm({
        title: 'Approve PBGui action',
        message: proposalActionLabel(preview.action) + ' ' + String(preview.name || ''),
        detail: approvalDetail,
        confirmText: 'Approve'
      });
      if (!confirmed) { buttons.forEach(function (button) { button.disabled = false; }); return; }
    }
    if (!conversationId || conversationId !== state.current) return;
    try {
      var result = await api('/proposals/' + encodeURIComponent(proposal.proposal_id) + (approve ? '/approve' : '/reject'), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ payload_digest: proposal.payload_digest, conversation_id: conversationId })
      });
      if (result.status === 'executed' && result.action === 'python_analysis') appendAnalysisResult(result);
      if (result.status === 'executed') {
        window.dispatchEvent(new CustomEvent('pbgui:ai-action-completed', { detail: result }));
      }
      setStatus(result.status === 'executed' ? 'Approved action completed.' : 'Proposal ' + String(result.status || 'resolved') + '.', false);
    } catch (error) { setStatus(error.message, true); }
    finally { await reconcileProposals(conversationId); }
  }

  async function newConversation() {
    var provider = root.querySelector('#pai-provider').value;
    var model = root.querySelector('#pai-model').value;
    if (!provider || !model) return;
    try {
      var context = root.querySelector('#pai-context-toggle').checked ? collectContext() : null;
      var data = await api('/conversations', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ provider: provider, model: model, effort: root.querySelector('#pai-effort').value, context: context })
      });
      state.current = data.conversation_id;
      await loadConversations(state.current);
      root.querySelector('textarea').focus();
    } catch (error) { setStatus(error.message, true); }
  }

  async function sendMessage(override) {
    var prompt = root.querySelector('textarea');
    var message = String(override == null ? prompt.value : override).trim();
    if (!message) return;
    if (!state.current) await newConversation();
    if (!state.current) return;
    var conversationId = state.current;
    state.retryMessages[conversationId] = message;
    if (override == null) prompt.value = '';
    var turnContext = root.querySelector('#pai-context-toggle').checked ? collectContext() : null;
    renderContext(turnContext || {});
    var localResult = window.PBGuiAI && typeof window.PBGuiAI.tryLocalCommand === 'function'
      ? window.PBGuiAI.tryLocalCommand(message)
      : { handled: false };
    if (localResult.handled) {
      setBusy(true);
      setStatus(localResult.message || 'PBGui action completed.', false);
      try {
        await api('/conversations/' + encodeURIComponent(conversationId) + '/local-action', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ message: message, context: turnContext })
        });
        delete state.retryMessages[conversationId];
        if (conversationId === state.current) await loadConversation(conversationId);
      } catch (error) {
        if (conversationId === state.current) {
          setBusy(false);
          setStatus('PBGui completed the action, but could not record it: ' + error.message, true);
        }
      }
      return;
    }
    setBusy(true);
    setStatus('Starting model...', false);
    try {
      await api('/conversations/' + encodeURIComponent(conversationId) + '/turns', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          message: message,
          context: turnContext,
          effort: root.querySelector('#pai-effort').value,
          model: root.querySelector('#pai-model').value,
          provider: root.querySelector('#pai-provider').value
        })
      });
      if (conversationId === state.current) {
        schedulePoll(conversationId);
        await loadConversation(conversationId);
      }
    } catch (error) {
      if (conversationId === state.current) {
        setBusy(false);
        root.querySelector('.pai-retry').hidden = false;
        setStatus(error.message, true);
      }
    }
  }

  function retryTurn() {
    var message = state.retryMessages[state.current];
    if (message) sendMessage(message);
  }

  async function stopTurn() {
    if (!state.current) return;
    try {
      await api('/conversations/' + encodeURIComponent(state.current) + '/cancel', { method: 'POST' });
      await loadConversation(state.current);
    } catch (error) { setStatus(error.message, true); }
  }

  async function refreshHealth() {
    try {
      await api('/models/health-refresh', { method: 'POST' });
      setStatus('Free-model health refresh queued.', false);
    } catch (error) { setStatus(error.message, true); }
  }

  async function deleteConversation() {
    if (!state.current) return;
    var confirmed = typeof window.PBGuiConfirm === 'function' && await window.PBGuiConfirm({
      title: 'Delete AI chat',
      message: 'Delete this conversation and its history?',
      confirmText: 'Delete'
    });
    if (!confirmed) return;
    var conversationId = state.current;
    try {
      await api('/conversations/' + encodeURIComponent(conversationId), { method: 'DELETE' });
      delete state.retryMessages[conversationId];
      state.current = '';
      await loadConversations();
    } catch (error) { setStatus(error.message, true); }
  }

  function schedulePoll(conversationId) {
    stopPoll();
    var generation = state.pollGeneration;
    state.poll = setTimeout(async function tick() {
      if (generation !== state.pollGeneration || conversationId !== state.current) return;
      await loadConversation(conversationId);
    }, 1000);
  }

  function stopPoll() {
    state.pollGeneration += 1;
    if (state.poll) clearTimeout(state.poll);
    state.poll = null;
  }

  function openDrawer() {
    build();
    state.open = true;
    root.classList.add('open');
    root.setAttribute('aria-hidden', 'false');
    renderContext(collectContext());
    startContextWatch();
    var button = document.getElementById('pbgui-ai-btn');
    if (button) button.setAttribute('aria-expanded', 'true');
    saveDrawerPreferences(true).catch(function (error) { setStatus(error.message, true); });
    loadConversations();
  }

  function closeDrawer() {
    if (!root) return;
    state.open = false;
    closeProposalReview();
    stopContextWatch();
    root.classList.remove('open');
    root.setAttribute('aria-hidden', 'true');
    var button = document.getElementById('pbgui-ai-btn');
    if (button) { button.setAttribute('aria-expanded', 'false'); button.focus(); }
    saveDrawerPreferences(false).catch(function (error) { setStatus(error.message, true); });
  }

  var facade = window.PBGuiAI || {};
  facade.open = openDrawer;
  facade.close = closeDrawer;
  facade.toggle = function () { state.open ? closeDrawer() : openDrawer(); };
  window.PBGuiAI = facade;
  window.addEventListener('resize', function () { if (root && window.innerWidth > 760) applyWidth(state.drawerWidth); });
  window.addEventListener('pagehide', function () { stopPoll(); stopContextWatch(); });
}());
