/* Bounded, lazy structured preview of the exact diagnostic snapshot. */
(function () {
  'use strict';
  var PAGE_SIZE = 40, TEXT_SIZE = 4000;
  function node(tag, text, cls) {
    var result = document.createElement(tag);
    if (text != null) result.textContent = text;
    if (cls) result.className = cls;
    return result;
  }
  function button(label, action) {
    var result = node('button', label); result.type = 'button'; result.addEventListener('click', action); return result;
  }
  function summary(value) {
    if (value === null) return 'null';
    if (Array.isArray(value)) return value.length + ' items';
    if (typeof value === 'object') return Object.keys(value).length + ' fields' + (typeof value.status === 'string' ? ' · ' + value.status.slice(0, 80) : '');
    var text = String(value); return text.length > 180 ? text.slice(0, 180) + '…' : text;
  }
  function pager(total, size, page, change) {
    var pages = Math.max(1, Math.ceil(total / size)), row = node('div', null, 'support-report-pager');
    var previous = button('Previous', function () { change(page - 1); }), next = button('Next', function () { change(page + 1); });
    previous.disabled = page <= 0; next.disabled = page >= pages - 1;
    row.append(previous, node('span', total ? (page * size + 1) + '–' + Math.min(total, (page + 1) * size) + ' of ' + total : '0 items'), next);
    return row;
  }
  function renderValue(container, value) {
    if (value === null || typeof value !== 'object') {
      var text = value === null ? 'null' : String(value);
      function chunk(page) {
        container.replaceChildren(node('pre', text.slice(page * TEXT_SIZE, (page + 1) * TEXT_SIZE), 'support-report-value'));
        if (text.length > TEXT_SIZE) container.appendChild(pager(text.length, TEXT_SIZE, page, chunk));
      }
      chunk(0); return;
    }
    var keys = Object.keys(value);
    function page(index) {
      container.replaceChildren();
      keys.slice(index * PAGE_SIZE, (index + 1) * PAGE_SIZE).forEach(function (key) {
        var child = value[key], complex = child !== null && (typeof child === 'object' || String(child).length > 300);
        if (complex) {
          var details = node('details', null, 'support-report-field');
          var caption = node('summary'); caption.append(node('strong', key), node('span', summary(child)));
          var content = node('div', null, 'support-report-nested');
          details.append(caption, content);
          details.addEventListener('toggle', function () {
            if (details.open && !content.childNodes.length) renderValue(content, child);
            if (!details.open) content.replaceChildren();
          });
          container.appendChild(details);
        } else {
          var row = node('div', null, 'support-report-field support-report-leaf');
          row.append(node('strong', key), node('span', child === null ? 'null' : String(child))); container.appendChild(row);
        }
      });
      if (!keys.length) container.appendChild(node('p', Array.isArray(value) ? 'Empty list' : 'No fields'));
      if (keys.length > PAGE_SIZE) container.appendChild(pager(keys.length, PAGE_SIZE, index, page));
    }
    page(0);
  }
  function create(root, options) {
    options = options || {};
    root.classList.add('support-report-view');
    var state = {category: 'system', selected: '', page: 0, filter: ''}, data = null, entries = [];
    if (options.storageKey) { try {
      var saved = JSON.parse(sessionStorage.getItem(options.storageKey) || '{}');
      ['category', 'selected', 'filter'].forEach(function (key) { if (typeof saved[key] === 'string') state[key] = saved[key].slice(0, 1200); });
      if (Number.isSafeInteger(saved.page) && saved.page >= 0) state.page = saved.page;
    } catch (_) {} }
    function save() { if (options.storageKey) { try { sessionStorage.setItem(options.storageKey, JSON.stringify(state)); } catch (_) {} } }
    var empty = node('p', 'Collect a report to preview its contents.', 'support-report-empty');
    var tabs = node('div', null, 'support-report-categories'); tabs.setAttribute('role', 'tablist');
    var layout = node('div', null, 'support-report-layout'), sources = node('section', null, 'support-report-sources');
    var search = node('input'); search.type = 'search'; search.placeholder = 'Filter sources'; search.setAttribute('aria-label', 'Filter report sources');
    search.maxLength = 1200; search.value = state.filter;
    var list = node('div', null, 'support-report-list'), listPager = node('div');
    sources.append(search, list, listPager);
    var detail = node('section', null, 'support-report-detail'), heading = node('h2'), content = node('div', null, 'support-report-content');
    detail.append(heading, content); layout.append(sources, detail); root.append(empty, tabs, layout);
    var definitions = [['system', 'System and versions'], ['records', 'Operational records'], ['logs', 'Log excerpts'],
      ['databases', 'Databases'], ['coverage', 'Coverage and limits'], ['browser', 'Browser'], ['metadata', 'Report metadata']];
    function categoryData(key) {
      if (key === 'metadata') {
        var rest = {}; Object.keys(data || {}).forEach(function (name) { if (!definitions.some(function (pair) { return pair[0] === name; })) rest[name] = data[name]; }); return rest;
      }
      return (data || {})[key] || {};
    }
    function select(key, value) {
      state.selected = key; save(); heading.textContent = key;
      content.replaceChildren(); detail.scrollTop = 0; renderValue(content, value);
      list.querySelectorAll('button').forEach(function (item) { item.setAttribute('aria-pressed', String(item.dataset.source === key)); });
    }
    function renderList() {
      var query = search.value.toLowerCase();
      var filtered = entries.filter(function (entry) { return entry[0].toLowerCase().includes(query); });
      state.page = Math.max(0, Math.min(Number(state.page) || 0, Math.max(0, Math.ceil(filtered.length / PAGE_SIZE) - 1)));
      list.replaceChildren();
      filtered.slice(state.page * PAGE_SIZE, (state.page + 1) * PAGE_SIZE).forEach(function (entry) {
        var item = button('', function () { select(entry[0], entry[1]); });
        item.dataset.source = entry[0]; item.setAttribute('aria-pressed', String(state.selected === entry[0]));
        item.append(node('strong', entry[0]), node('span', summary(entry[1]))); list.appendChild(item);
      });
      if (!filtered.length) list.appendChild(node('p', 'No matching sources.'));
      listPager.replaceChildren(pager(filtered.length, PAGE_SIZE, state.page, function (page) { state.page = page; save(); renderList(); list.scrollTop = 0; }));
    }
    function activate(key, preserve) {
      state.category = key;
      if (!preserve) { state.page = 0; state.selected = ''; state.filter = ''; search.value = ''; }
      entries = Object.entries(categoryData(key));
      tabs.querySelectorAll('button').forEach(function (item) { item.setAttribute('aria-selected', String(item.dataset.category === key)); });
      var previous = entries.find(function (entry) { return entry[0] === state.selected; });
      empty.hidden = true;
      if (!previous && state.selected) { empty.textContent = 'Previous source is unavailable in this report. Showing the nearest available source.'; empty.hidden = false; }
      if (previous || entries.length) { var choice = previous || entries[0]; select(choice[0], choice[1]); }
      else { heading.textContent = 'No data in this section'; content.replaceChildren(); }
      renderList(); save();
    }
    search.addEventListener('input', function () { state.page = 0; state.filter = search.value; save(); renderList(); });
    function setReport(value) {
      data = value; empty.hidden = !!value; tabs.hidden = !value; layout.hidden = !value;
      if (!value) { empty.textContent = 'Collect a report to preview its contents.'; tabs.replaceChildren(); list.replaceChildren(); content.replaceChildren(); heading.textContent = ''; return; }
      tabs.replaceChildren();
      definitions.forEach(function (pair) {
        var count = Object.keys(categoryData(pair[0])).length;
        var item = button(pair[1] + ' (' + count + ')', function () { activate(pair[0]); });
        item.dataset.category = pair[0]; item.setAttribute('role', 'tab'); tabs.appendChild(item);
      });
      if (!definitions.some(function (pair) { return pair[0] === state.category; })) state.category = 'system';
      activate(state.category, true);
    }
    setReport(null);
    return {setReport: setReport};
  }
  window.PBGuiSupportReportView = {create: create};
})();
