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
  window.PBGuiAIUsage = {render: render};
}());
