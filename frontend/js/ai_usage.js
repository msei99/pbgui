(function () {
  'use strict';
  function render(container, data) {
    container.replaceChildren();
    if (data.email) {
      var account = document.createElement('div');
      account.textContent = 'Signed in as ' + data.email;
      container.appendChild(account);
    }
    (data.limits || []).forEach(function (limit) {
      if (typeof limit.usedPercent !== 'number' || !Number.isFinite(limit.usedPercent)) return;
      var remaining = Math.max(0, Math.min(100, 100 - limit.usedPercent));
      var names = {300: '5-hour usage limit', 1440: 'Daily usage limit', 10080: 'Weekly usage limit', 43200: 'Monthly usage limit'};
      var label = names[limit.windowDurationMins] || 'Usage limit';
      var row = document.createElement('div'); row.className = 'usage-limit';
      var title = document.createElement('div'); title.textContent = label;
      var value = document.createElement('strong'); value.textContent = Number(remaining.toFixed(1)) + '% remaining';
      var bar = document.createElement('progress'); bar.max = 100; bar.value = remaining;
      bar.setAttribute('aria-label', label + ': ' + value.textContent);
      row.append(title, value, bar);
      if (limit.resetsAt && Number.isFinite(limit.resetsAt)) {
        var reset = document.createElement('div'); reset.textContent = 'Resets ' + new Date(limit.resetsAt * 1000).toLocaleString(); row.appendChild(reset);
      }
      container.appendChild(row);
    });
    if (!(data.limits || []).length) {
      var note = document.createElement('div'); note.textContent = data.message || 'Usage limits currently unavailable'; container.appendChild(note);
    }
  }
  function renderOpenRouter(container, data) {
    container.replaceChildren();
    if (!data.connected) {
      container.textContent = 'Connect OpenRouter to view key usage.';
      return;
    }
    var title = document.createElement('div');
    title.className = 'openrouter-usage-title';
    title.textContent = 'API key spend (USD)';
    container.appendChild(title);
    var spend = data.spend || {};
    if (Object.keys(spend).length) {
      var cards = document.createElement('div');
      cards.className = 'openrouter-usage-cards';
      [['daily', 'Today'], ['weekly', 'This week'], ['monthly', 'This month']].forEach(function (entry) {
        var card = document.createElement('div');
        card.className = 'openrouter-usage-card';
        var label = document.createElement('span');
        label.textContent = entry[1];
        var value = document.createElement('strong');
        var amount = spend[entry[0]];
        value.textContent = typeof amount === 'number' && Number.isFinite(amount)
          ? '$' + new Intl.NumberFormat('en-US', {minimumFractionDigits: 2, maximumFractionDigits: 6}).format(amount)
          : '—';
        card.append(label, value);
        cards.appendChild(card);
      });
      container.appendChild(cards);
    }
    var limit = document.createElement('div');
    limit.className = 'openrouter-usage-note';
    if (typeof data.key_limit_usd === 'number' && Number.isFinite(data.key_limit_usd)) {
      var format = function (value) { return '$' + new Intl.NumberFormat('en-US', {minimumFractionDigits: 2, maximumFractionDigits: 4}).format(value); };
      limit.textContent = 'Key limit: ' + format(data.key_limit_usd)
        + (typeof data.key_limit_remaining_usd === 'number' && Number.isFinite(data.key_limit_remaining_usd)
          ? ' · ' + format(data.key_limit_remaining_usd) + ' remaining' : '')
        + (['daily', 'weekly', 'monthly'].includes(data.key_limit_reset) ? ' · resets ' + data.key_limit_reset : '');
    } else {
      limit.textContent = 'No key spending limit reported.';
    }
    if (!data.message) container.appendChild(limit);
    if (data.message) {
      var note = document.createElement('div');
      note.className = 'openrouter-usage-note';
      note.textContent = data.message;
      container.appendChild(note);
    }
    var link = document.createElement('a');
    link.href = 'https://openrouter.ai/activity';
    link.target = '_blank';
    link.rel = 'noopener noreferrer';
    link.textContent = 'View full usage on OpenRouter ↗';
    container.appendChild(link);
  }
  window.PBGuiAIUsage = {render: render, renderOpenRouter: renderOpenRouter};
}());
