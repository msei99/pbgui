(function () {
  'use strict';
  var sheet = document.createElement('link');sheet.rel = 'stylesheet';
  sheet.href = new URL('../css/ai_model_search.css?v=8', document.currentScript.src).href;
  document.head.appendChild(sheet);
  function read(key) { try { return sessionStorage.getItem(key) || ''; } catch (_) { return ''; } }
  function write(key, value) { try { sessionStorage.setItem(key, value); } catch (_) {} }
  function node(tag, cls, text) {
    var item = document.createElement(tag);if (cls) item.className = cls;
    if (text !== undefined) item.textContent = text;return item;
  }
  function priced(row) { return row && row.cost && ['input', 'output'].every(function (key) { return typeof row.cost[key] === 'number' && Number.isFinite(row.cost[key]) && row.cost[key] >= 0; }); }
  function priceText(row) { return priced(row) ? 'Per 1M tokens: Input USD ' + row.cost.input + ' · Output USD ' + row.cost.output : 'Model pricing unavailable'; }
  function author(row) { return String(row.id || '').split('/')[0]; }
  function variant(row) {
    var suffix = String(row.id || '').split(':').slice(1).join(':');
    return suffix || 'standard';
  }
  function released(row) {
    return Number.isFinite(row.created) && row.created > 0 ? new Date(row.created * 1000).toLocaleDateString('en-US', {year: 'numeric', month: 'short', day: 'numeric'}) : '';
  }
  function field(label, choices) {
    var select = node('select');select.setAttribute('aria-label', label);
    choices.forEach(function (pair) { var option = node('option', '', pair[1]);option.value = pair[0];select.appendChild(option); });return select;
  }
  function dropdown(select) {
    var wrap = node('div', 'pbg-model-select'), trigger = node('button', 'pbg-model-control');
    trigger.type = 'button';trigger.setAttribute('aria-haspopup', 'listbox');
    var menu = node('div', 'pbg-model-menu');menu.popover = 'auto';menu.setAttribute('role', 'listbox');
    menu.setAttribute('aria-label', select.getAttribute('aria-label'));
    select.insertAdjacentElement('beforebegin', wrap);wrap.append(select, trigger, menu);select.hidden = true;
    function label() {
      trigger.textContent = ((select.selectedOptions[0] || {}).textContent || select.getAttribute('aria-label')) + ' ▾';
      trigger.setAttribute('aria-expanded', String(menu.matches(':popover-open')));
    }
    function open() {
      menu.replaceChildren();
      Array.from(select.options).forEach(function (option) {
        var choice = node('button', '', option.textContent);choice.type = 'button';choice.setAttribute('role', 'option');
        choice.setAttribute('aria-selected', String(option.selected));
        choice.addEventListener('click', function () { select.value = option.value;menu.hidePopover();select.dispatchEvent(new Event('change', {bubbles: true}));trigger.focus(); });
        menu.appendChild(choice);
      });
      var box = trigger.getBoundingClientRect(), width = Math.min(Math.max(box.width, 210), innerWidth - 16);
      menu.style.width = width + 'px';menu.style.left = Math.max(8, Math.min(box.left, innerWidth - width - 8)) + 'px';
      menu.style.top = Math.max(8, Math.min(box.bottom + 5, innerHeight - 200)) + 'px';
      menu.style.maxHeight = Math.max(80, Math.min(280, innerHeight - parseFloat(menu.style.top) - 8)) + 'px';
      menu.showPopover();label();
    }
    trigger.addEventListener('click', function () { if (menu.matches(':popover-open')) menu.hidePopover();else open(); });
    trigger.addEventListener('keydown', function (event) { if (event.key === 'ArrowDown') { event.preventDefault();open();var first = menu.querySelector('[aria-selected=true]') || menu.firstElementChild;if (first) first.focus(); } });
    menu.addEventListener('keydown', function (event) {
      if (event.key === 'Escape') { menu.hidePopover();trigger.focus();event.preventDefault();event.stopPropagation();return; }
      if (['ArrowDown', 'ArrowUp'].includes(event.key)) {
        var options = Array.from(menu.children), index = options.indexOf(document.activeElement);
        index = (index + (event.key === 'ArrowDown' ? 1 : -1) + options.length) % options.length;
        if (options[index]) { options[index].focus();event.preventDefault(); }
      }
    });
    menu.addEventListener('toggle', label);select.addEventListener('change', label);
    new MutationObserver(label).observe(select, {childList: true});label();
  }
  function attach(select, input, key) {
    var metadata = {}, active = false, category = read(key + '.category') || '';
    var view = read(key + '.view') === 'table' ? 'table' : 'list';
    var pinKey = 'pbgui.ai.openrouter.pins';
    var favorites;
    try { favorites = new Set(JSON.parse(read(pinKey) || '[]').filter(function (v) { return typeof v === 'string'; }).slice(0, 500)); }
    catch (_) { favorites = new Set(); }
    var host = node('div', 'pbg-model-browser');host.dataset.modelSelect = select.id;host.hidden = true;
    (select.closest('label') || select).insertAdjacentElement('afterend', host);
    var compact = !!select.closest('.pai-toolbar');
    var selected = node('div', 'pbg-model-current');
    var toggle = node('button', 'pbg-model-toggle', 'Browse models');toggle.type = 'button';toggle.hidden = !compact;
    var panel = node('div', 'pbg-model-panel');panel.hidden = compact;
    var toolbar = node('div', 'pbg-model-filters');
    var sorting = field('Sort models', [['newest', 'Newest'], ['oldest', 'Oldest'], ['price-asc', 'Pricing: Low to High'], ['price-desc', 'Pricing: High to Low'], ['input-asc', 'Input price: Low to High'], ['input-desc', 'Input price: High to Low'], ['output-asc', 'Output price: Low to High'], ['output-desc', 'Output price: High to Low'], ['context-asc', 'Context: Low to High'], ['context', 'Context: High to Low'], ['name', 'Name: A to Z'], ['name-desc', 'Name: Z to A']]);
    sorting.value = read(key + '.sort') || 'newest';
    if (!sorting.value) sorting.value = 'newest';
    var variants = field('Model variant', [['', 'All variants']]);
    var pinned = node('button', 'pbg-model-control', '☆ Pinned');pinned.type = 'button';
    var pinnedOnly = read(key + '.pinnedOnly') === 'true';
    var views = node('div', 'pbg-model-views');
    var listButton = node('button', 'pbg-model-control', 'List');listButton.type = 'button';
    var tableButton = node('button', 'pbg-model-control', 'Table');tableButton.type = 'button';
    views.append(listButton, tableButton);toolbar.append(sorting, variants, pinned, views);
    dropdown(sorting);dropdown(variants);
    var tabs = node('div', 'pbg-model-tabs');tabs.setAttribute('role', 'group');tabs.setAttribute('aria-label', 'Model types');
    var count = node('div', 'pbg-model-count');count.setAttribute('role', 'status');
    var results = node('div', 'pbg-model-results');results.setAttribute('aria-label', 'Available models');
    panel.append(toolbar, tabs, count, results);host.append(selected, toggle, panel);
    input.value = read(key);
    function modelFor(option) {
      var row = metadata instanceof Map ? metadata.get(option.value) : metadata[option.value];
      return row || {id: option.value, name: option.textContent, unavailable: true};
    }
    function choose(value) {
      if (select.disabled) return;
      select.value = value;select.dispatchEvent(new Event('change', {bubbles: true}));
      var current = results.querySelector('.pbg-model-row.selected');if (current) current.focus();
    }
    function pin(row) {
      var button = node('button', 'pbg-model-pin', favorites.has(row.id) ? '★' : '☆');button.type = 'button';
      button.setAttribute('aria-label', (favorites.has(row.id) ? 'Unpin ' : 'Pin ') + (row.name || row.id));
      button.setAttribute('aria-pressed', String(favorites.has(row.id)));
      button.addEventListener('click', function (event) {
        event.stopPropagation();
        if (favorites.has(row.id)) favorites.delete(row.id);else if (favorites.size < 500) favorites.add(row.id);
        write(pinKey, JSON.stringify(Array.from(favorites)));window.dispatchEvent(new Event('pbgui:ai-model-pins-changed'));
      });return button;
    }
    function features(row) {
      var parts = [];
      if (row.unavailable) parts.push('Unavailable');
      else if (row.decision) parts.push('Decision');else parts.push('Text');
      if (row.image_input) parts.push('Image input');
      if (row.tool_capable || row.tools) parts.push('Tools');
      if (row.reasoning) parts.push('Reasoning');return parts.join(' · ');
    }
    function row(option, table) {
      var model = modelFor(option), current = option.value === select.value;
      var action = node(table ? 'tr' : 'button', 'pbg-model-row' + (current ? ' selected' : ''));
      action.dataset.modelValue = option.value;
      if (table) { action.tabIndex = select.disabled ? -1 : 0;action.setAttribute('aria-selected', String(current)); }
      else { action.type = 'button';action.disabled = select.disabled || option.disabled;action.setAttribute('aria-pressed', String(current)); }
      action.addEventListener('click', function () { choose(option.value); });
      if (table) action.addEventListener('keydown', function (event) { if (event.target === action && ['Enter', ' '].includes(event.key)) { event.preventDefault();choose(option.value); } });
      var name = node(table ? 'td' : 'strong', 'pbg-model-name', model.name || model.id);
      if (table) name.appendChild(node('span', 'pbg-model-id', features(model)));
      action.appendChild(name);
      if (table) {
        [priced(model) ? '$' + model.cost.input : '—', priced(model) ? '$' + model.cost.output : '—',
          model.context ? model.context.toLocaleString('en-US') : '—', released(model) || '—'].forEach(function (value) { action.appendChild(node('td', '', value)); });
        var star = node('td');star.appendChild(pin(model));action.appendChild(star);return action;
      }
      action.appendChild(node('span', 'pbg-model-id', model.id + ' · ' + features(model)));
      if (model.description) action.appendChild(node('span', 'pbg-model-description', model.description));
      var details = ['by ' + author(model)];
      if (released(model)) details.push(released(model));
      if (model.context) details.push(model.context.toLocaleString('en-US') + ' context');
      details.push(priceText(model));action.appendChild(node('span', 'pbg-model-meta', details.join(' | ')));
      var item = node('div', 'pbg-model-item');item.append(action, pin(model));return item;
    }
    function typeMatches(model, type) {
      return !type || (type === 'text' ? !model.decision : type === 'vision' ? model.image_input : !!model.decision);
    }
    var renderedSignature='';
    function render() {
      if (!active) return;
      var scroll = results.scrollTop, term = input.value.trim().toLowerCase();
      var signature=JSON.stringify([term,category,view,sorting.value,variants.value,pinnedOnly,Array.from(favorites),select.value,select.disabled,Array.from(select.options).map(function(option){return [option.value,option.disabled,modelFor(option)];})]);
      if(signature===renderedSignature)return;renderedSignature=signature;
      var all = Array.from(select.options).filter(function (option) { return !!option.value; });
      var matches = all.filter(function (option) {
        var model = modelFor(option);
        return (!term || (String(model.name || '') + ' ' + String(model.id || '') + ' ' + String(model.description || '')).toLowerCase().includes(term)) &&
          typeMatches(model, category) && (!variants.value || variant(model) === variants.value) &&
          (!pinnedOnly || favorites.has(model.id));
      });
      function sortValue(model) {
        if (sorting.value === 'newest' || sorting.value === 'oldest') return Number.isFinite(model.created) && model.created > 0 ? model.created : null;
        if (sorting.value.startsWith('context')) return Number.isFinite(model.context) && model.context > 0 ? model.context : null;
        if (sorting.value.startsWith('price')) return priced(model) ? model.cost.input + model.cost.output : null;
        if (sorting.value.startsWith('input') || sorting.value.startsWith('output')) return priced(model) ? model.cost[sorting.value.split('-')[0]] : null;
        return String(model.name || model.id);
      }
      matches.sort(function (a, b) {
        var left = modelFor(a), right = modelFor(b), x = sortValue(left), y = sortValue(right);
        if (x === null && y !== null) return 1;
        if (y === null && x !== null) return -1;
        var diff = x === null ? 0 : typeof x === 'string' ? x.localeCompare(y) : x - y;
        if (sorting.value.endsWith('-desc') || sorting.value === 'newest' || sorting.value === 'context') diff = -diff;
        return diff || String(left.name || left.id).localeCompare(String(right.name || right.id)) || String(left.id).localeCompare(String(right.id));
      });
      count.textContent = matches.length + ' of ' + all.length + ' models';
      tabs.replaceChildren();
      [['', 'All'], ['text', 'Text'], ['vision', 'Image input'], ['decision', 'Decisions']].forEach(function (pair) {
        var amount = all.filter(function (option) { return typeMatches(modelFor(option), pair[0]); }).length;
        if (!amount && pair[0]) return;
        var button = node('button', 'pbg-model-tab', pair[1] + ' ' + amount);button.type = 'button';
        button.setAttribute('aria-pressed', String(category === pair[0]));
        button.addEventListener('click', function () { category = pair[0];write(key + '.category', category);render(); });tabs.appendChild(button);
      });
      pinned.setAttribute('aria-pressed', String(pinnedOnly));listButton.setAttribute('aria-pressed', String(view === 'list'));tableButton.setAttribute('aria-pressed', String(view === 'table'));
      var current = all.find(function (option) { return option.value === select.value; });
      selected.textContent = current ? 'Selected: ' + (modelFor(current).name || modelFor(current).id) : '';
      results.replaceChildren();
      var shown = matches;
      if (view === 'table') {
        var table = node('table', 'pbg-model-table'), head = node('thead'), heading = node('tr'), body = node('tbody');
        [['Model name', 'name', 'name-desc'], ['Input / 1M', 'input-asc', 'input-desc'], ['Output / 1M', 'output-asc', 'output-desc'], ['Context', 'context-asc', 'context'], ['Released', 'oldest', 'newest']].forEach(function (column) {
          var cell = node('th'), ascending = sorting.value === column[1], descending = sorting.value === column[2];cell.scope = 'col';
          cell.setAttribute('aria-sort', ascending ? 'ascending' : descending ? 'descending' : 'none');
          var button = node('button', 'pbg-model-sort', column[0]);button.type = 'button';button.dataset.sortColumn = column[1];
          button.setAttribute('aria-label', 'Sort by ' + column[0]);
          var arrow = node('span', '', ascending ? ' ↑' : descending ? ' ↓' : ' ↕');arrow.setAttribute('aria-hidden', 'true');button.appendChild(arrow);
          button.addEventListener('click', function () {
            sorting.value = ascending ? column[2] : column[1];sorting.dispatchEvent(new Event('change', {bubbles: true}));
            var replacement = results.querySelector('[data-sort-column="' + column[1] + '"]');if (replacement) replacement.focus({preventScroll: true});
          });
          cell.appendChild(button);heading.appendChild(cell);
        });
        var pinHeading = node('th');pinHeading.scope = 'col';pinHeading.setAttribute('aria-label', 'Pinned');heading.appendChild(pinHeading);
        head.appendChild(heading);shown.forEach(function (option) { body.appendChild(row(option, true)); });table.append(head, body);results.appendChild(table);
      } else shown.forEach(function (option) { results.appendChild(row(option, false)); });
      if (!matches.length) results.appendChild(node('p', 'pbg-model-empty', 'No matching models'));
      results.scrollTop = scroll;
    }
    function refresh(rows) {
      if (rows) metadata = rows;
      var options = Array.from(select.options);
      active = options.some(function (option) { var model = modelFor(option);return model.provider === 'openrouter' || model.decision; });
      if(!active)renderedSignature='';
      host.hidden = !active;select.hidden = active;
      if (active) {
        if (input.parentNode !== toolbar) toolbar.prepend(input);input.hidden = false;
        var previous = variants.value || read(key + '.variant');variants.replaceChildren();
        var option = node('option', '', 'All variants');option.value = '';variants.appendChild(option);
        var available = new Set(options.filter(function (item) { return !!item.value; }).map(function (item) { return variant(modelFor(item)); }));
        ['standard', 'free', 'extended', 'thinking'].forEach(function (value) {
          if (!available.has(value)) return;
          var item = node('option', '', value[0].toUpperCase() + value.slice(1));item.value = value;variants.appendChild(item);
        });
        variants.value = previous;render();
      } else {
        if (input.parentNode === toolbar) select.insertAdjacentElement('beforebegin', input);
        input.hidden = options.length < 12;
        var term = input.hidden ? '' : input.value.trim().toLowerCase();options.forEach(function (option) { option.hidden = !!term && !option.selected && !(option.textContent + ' ' + option.value).toLowerCase().includes(term); });
      }
    }
    toggle.setAttribute('aria-expanded', String(!panel.hidden));
    toggle.addEventListener('click', function () { panel.hidden = !panel.hidden;toggle.setAttribute('aria-expanded', String(!panel.hidden));toggle.textContent = panel.hidden ? 'Browse models' : 'Close model browser';if (!panel.hidden) input.focus(); });
    input.addEventListener('input', function () { write(key, input.value);refresh(); });
    [[sorting, 'sort'], [variants, 'variant']].forEach(function (pair) { pair[0].addEventListener('change', function () { write(key + '.' + pair[1], pair[0].value);if (pair[1] === 'sort') results.scrollTop = 0;render(); }); });
    pinned.addEventListener('click', function () { pinnedOnly = !pinnedOnly;write(key + '.pinnedOnly', String(pinnedOnly));render(); });
    [[listButton, 'list'], [tableButton, 'table']].forEach(function (pair) { pair[0].addEventListener('click', function () { view = pair[1];write(key + '.view', view);render(); }); });
    window.addEventListener('pbgui:ai-model-pins-changed', function () {
      try { favorites = new Set(JSON.parse(read(pinKey) || '[]').filter(function (value) { return typeof value === 'string'; }).slice(0, 500)); }
      catch (_) { favorites = new Set(); }
      render();
    });
    select.addEventListener('change', function () { refresh(); });
    new MutationObserver(render).observe(select, {attributes: true, attributeFilter: ['disabled']});
    host.addEventListener('keydown', function (event) {
      if (event.key === 'Escape' && compact && !panel.hidden) { panel.hidden = true;toggle.setAttribute('aria-expanded', 'false');toggle.textContent = 'Browse models';toggle.focus();event.preventDefault();return; }
      if (!['ArrowDown', 'ArrowUp'].includes(event.key) || !event.target.classList.contains('pbg-model-row')) return;
      var buttons = Array.from(host.querySelectorAll('.pbg-model-row:not(:disabled)'));
      var index = buttons.indexOf(event.target) + (event.key === 'ArrowDown' ? 1 : -1);if (buttons[index]) { event.preventDefault();buttons[index].focus(); }
    });
    return {refresh: refresh};
  }
  window.PBGuiAIModelSearch = {attach: attach, priceText: priceText};
})();
