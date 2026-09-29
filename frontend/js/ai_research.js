/* Inline research approvals and inert evidence rendering. */
(function () {
  'use strict';
  if (window.PBGuiAIResearch) return;
  var assetBase = document.currentScript && document.currentScript.src ? document.currentScript.src.split('/app/')[0] : '';
  var views = new WeakMap(), liveViews = new Set();
  function node(tag, text) {
    var element = document.createElement(tag);
    if (text != null) element.textContent = String(text);
    return element;
  }
  async function request(view, path, body) {
    var response = await fetch(view.apiBase + path, {method: body === undefined ? 'GET' : 'POST',
      credentials: 'same-origin', cache: 'no-store', headers: {'Content-Type': 'application/json'},
      body: body === undefined ? undefined : JSON.stringify(body)});
    var result = await response.json();
    if (!response.ok) {
      var error = new Error(typeof result.detail === 'string' ? result.detail : 'Research request failed');
      error.status = response.status; throw error;
    }
    return result;
  }
  function resultText(target, text) {
    target.textContent = '';
    String(text || '').split(/(https:\/\/[^\s<>"']+)/g).forEach(function (part) {
      if (/^https:\/\//.test(part)) {
        try {
          var url = new URL(part.replace(/[),.;]+$/, ''));
          if (!url.username && !url.password) {
            var link = node('a', url.href); link.href = url.href; link.target = '_blank';
            link.rel = 'noopener noreferrer'; link.referrerPolicy = 'no-referrer';
            target.appendChild(link); target.appendChild(document.createTextNode(part.slice(url.href.length))); return;
          }
        } catch (_) {}
      }
      target.appendChild(document.createTextNode(part));
    });
  }
  function update(card, record) {
    // A delayed conversation snapshot must not revive an already approved preview.
    var rank = function (status) { return status === 'preview' ? 0 : (status === 'running' || status === 'budget_review') ? 1 : 2; };
    if (card.record && Number(record.revision || 0) < Number(card.record.revision || 0)) return;
    if (card.record && rank(record.status) < rank(card.record.status)) return;
    if (card.record && card.record.status === 'running' && record.status === 'running' && card.record.phase === 'jev' && !record.phase) record = Object.assign({}, record, {phase: 'jev'});
    var box = card.element.isConnected ? card.element.parentElement : null;
    var savedScroll = box && window.PBGuiAIMessageView ? window.PBGuiAIMessageView.capture(box) : null;
    card.record = record;
    card.prompt.textContent = record.prompt || '';
    card.instructions.textContent = record.instructions || '';
    card.jevPlan.textContent = record.jev_questions ? 'Jev follow-up · max USD ' + record.jev_max_cost_usd + '\n' + JSON.stringify(record.jev_questions, null, 2) : '';
    card.jevPlan.hidden = !record.jev_questions;
    card.approve.textContent = record.status === 'budget_review' ? 'Approve Jev up to USD ' + Number(record.jev_max_cost_usd).toFixed(6) : record.kind === 'jev' ? 'Approve Jev analysis' : record.jev_questions ? 'Approve research + Jev' : 'Approve research';
    card.selection.textContent = String(record.provider || '') + ' · ' + String(record.model || '');
    var jevRunning = record.kind === 'jev' || record.phase === 'jev' || record.resume_jev === true;
    card.cancel.textContent = record.phase === 'summary' ? 'Cancel final analysis' : jevRunning ? 'Cancel Jev analysis' : 'Cancel research';
    card.footer.dataset.state = record.error || record.jev_error || record.summary_error ? 'error' : record.status;
    card.status.textContent = record.error ? 'Stopped · ' + record.error : ({preview: 'Review the prompt, then approve here. Nothing has been sent for research.',
      budget_review: 'Jev estimate USD ' + Number(record.jev_estimated_cost_usd || 0).toFixed(6) + ' exceeds approved USD ' + Number(record.jev_previous_budget_usd || 0).toFixed(6) + '. Awaiting approval.',
      running: record.phase === 'summary' ? 'Preparing final analysis…' : record.phase === 'jev_check' ? 'Checking Jev connection…' : jevRunning ? 'Jev analysis running…' : record.jev_questions ? 'Web research running… · Jev next' : 'Web research running…', completed: record.summary_error ? 'Research and Jev completed · Final analysis failed — ' + record.summary_error : record.summary ? 'Research, Jev and final analysis completed' : record.jev_error ? 'Research completed · Jev failed — ' + record.jev_error : record.jev_answer ? 'Research and Jev completed' : 'Research completed',
      cancelled: 'Research cancelled.', superseded: 'Replaced by a newer research prompt.', expired: 'Preview expired. Ask for a new one in this chat.'}[record.status] || record.status);
    card.approve.hidden = !['preview', 'budget_review'].includes(record.status); card.reject.hidden = card.approve.hidden;
    card.cancel.hidden = record.status !== 'running';
    card.approve.disabled = card.pending; card.reject.disabled = card.pending; card.cancel.disabled = card.pending;
    resultText(card.answer, record.answer);
    card.answer.hidden = !record.answer;
    card.jevAnswer.textContent = record.jev_answer ? 'Jev analysis\n' + record.jev_answer : '';
    card.jevAnswer.hidden = !record.jev_answer;
    card.summary.hidden = !record.summary;
    if (card.summaryText !== record.summary) {
      card.summaryText = record.summary;
      if (window.PBGuiAIMessageView) window.PBGuiAIMessageView.render(card.summary, record.summary || '');
      else card.summary.textContent = record.summary || '';
    }
    if (savedScroll) window.PBGuiAIMessageView.restore(box, savedScroll);
  }
  function schedule(view, card) {
    clearTimeout(card.timer);
    if (card.record.status !== 'running' || !card.element.isConnected || document.hidden) return;
    var generation = view.generation;
    card.timer = setTimeout(async function () {
      if (generation !== view.generation || !card.element.isConnected) return;
      try {
        var record = await request(view, '/research/' + card.record.id);
        if (generation !== view.generation || !card.element.isConnected) return;
        update(card, record);
      } catch (error) {
        if (generation !== view.generation) return;
        if (error.status === 400 || error.status === 404) {
          update(card, Object.assign({}, card.record, {status: 'expired', error: error.message}));
        } else card.status.textContent = error.message + ' — checking the existing request again; no new research is sent.';
      }
      schedule(view, card);
    }, 1500);
  }
  async function act(view, card, action, event) {
    if (!event.isTrusted || card.pending) return;
    card.pending = true; update(card, card.record);
    var generation = view.generation;
    try {
      var record = await request(view, '/research/' + card.record.id + '/' + action,
        action === 'start' ? {digest: card.record.digest} : {});
      if (generation !== view.generation) return;
      update(card, record);
    } catch (error) {
      if (generation !== view.generation) return;
      card.status.textContent = error.message;
      // A lost start response must not create a second job. Poll the same immutable ID.
      if (action === 'start' && !error.status) update(card, Object.assign({}, card.record, {status: 'running'}));
      card.status.textContent = error.message;
    } finally {
      card.pending = false;
      if (generation === view.generation) {
        card.approve.disabled = false; card.reject.disabled = false; card.cancel.disabled = false;
        schedule(view, card);
      }
    }
  }
  function create(view, record) {
    var card = {element: node('section'), record: null, pending: false, timer: null};
    card.element.className = 'pbgui-research-card';
    card.element.dataset.researchId = record.id;
    card.element.setAttribute('aria-label', 'Web research');
    card.element.appendChild(node('strong', record.kind === 'jev' ? 'Jev research analysis' : 'Web research'));
    card.selection = node('div'); card.element.appendChild(card.selection);
    card.prompt = node('pre'); card.prompt.className = 'pbgui-research-prompt'; card.element.appendChild(card.prompt);
    var details = node('details'); details.appendChild(node('summary', 'Fixed instructions and research limits'));
    card.instructions = node('pre'); details.appendChild(card.instructions);
    details.appendChild(node('p', 'Only this prompt and these instructions are sent. No chat history or PBGui action tools. Provider charges/limits apply; 180 seconds without observable progress; no fixed total runtime. To revise the prompt, ask in the normal chat.'));
    card.element.appendChild(details);
    card.jevPlan = node('pre'); card.element.appendChild(card.jevPlan);
    card.footer = node('div'); card.footer.className = 'pbgui-research-statusbar';
    card.status = node('p'); card.status.setAttribute('role', 'status'); card.footer.appendChild(card.status);
    var actions = node('div'); actions.className = 'pbgui-research-actions';
    card.approve = node('button', 'Approve research'); card.reject = node('button', 'Reject'); card.cancel = node('button', 'Cancel research');
    [[card.approve, 'start'], [card.reject, 'cancel'], [card.cancel, 'cancel']].forEach(function (pair) {
      pair[0].type = 'button'; pair[0].onclick = function (event) { act(view, card, pair[1], event); }; actions.appendChild(pair[0]);
    });
    card.footer.appendChild(actions);
    card.answer = node('pre'); card.answer.className = 'pbgui-research-answer'; card.element.appendChild(card.answer);
    card.jevAnswer = node('pre'); card.jevAnswer.className = 'pbgui-jev-answer'; card.element.appendChild(card.jevAnswer);
    card.summary = node('div'); card.summary.className = 'pbgui-research-summary'; card.element.appendChild(card.summary);
    card.element.appendChild(card.footer);
    update(card, record);
    return card;
  }
  function dispose(view) {
    view.generation += 1;
    view.cards.forEach(function (card) { clearTimeout(card.timer); card.element.remove(); });
    view.cards.clear();
  }
  window.PBGuiAIResearch = {render: function (container, conversation, apiBase) {
    var view = views.get(container);
    if (!view) {
      view = {conversation: '', generation: 0, cards: new Map(), apiBase: assetBase ? assetBase + '/api/ai' : apiBase};
      views.set(container, view); liveViews.add(view);
    }
    if (view.conversation !== conversation.conversation_id) { dispose(view); view.conversation = conversation.conversation_id; }
    var records = Array.isArray(conversation.research_items) ? conversation.research_items : [];
    var ids = new Set(records.map(function (record) {return record.id;}));
    view.cards.forEach(function (card, id) { if (!ids.has(id)) {clearTimeout(card.timer); card.element.remove(); view.cards.delete(id);} });
    // Preserve card nodes across transcript refreshes so open details and focus survive.
    var messages = Array.from(container.children).filter(function (item) {return item.classList.contains('pai-message') || item.classList.contains('message');});
    records.slice().reverse().forEach(function (record) {
      if (!record || !/^[a-f0-9]{32}$/.test(record.id || '')) return;
      var card = view.cards.get(record.id);
      if (!card) {card = create(view, record); view.cards.set(record.id, card);} else update(card, record);
      var index = Number.isInteger(record.message_index) ? record.message_index : messages.length - 1;
      var anchor = messages[Math.max(0, Math.min(index, messages.length - 1))];
      if (anchor) {if (anchor.nextElementSibling !== card.element) anchor.after(card.element);}
      else if (container.lastElementChild !== card.element) container.appendChild(card.element);
      schedule(view, card);
    });
  }};
  document.addEventListener('visibilitychange', function () {
    liveViews.forEach(function (view) { view.cards.forEach(function (card) {schedule(view, card);}); });
  });
  window.addEventListener('pagehide', function () {liveViews.forEach(dispose); liveViews.clear();});
  // Retire the previous separate research-window navigation, never restore that UI.
  try {sessionStorage.removeItem('pbgui.ai.research.navigation');} catch (_) {}
}());
