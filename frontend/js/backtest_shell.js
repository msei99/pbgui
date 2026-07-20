;(function () {
  'use strict';

  function element(tag, className, text) {
    var node = document.createElement(tag);
    if (className) node.className = className;
    if (text !== undefined) node.textContent = text;
    return node;
  }

  function invoke(callbacks, name) {
    var callback = callbacks && callbacks[name];
    if (typeof callback !== 'function' && typeof window[name] === 'function') callback = window[name];
    if (typeof callback === 'function') return callback();
  }

  function actionButton(spec, callbacks) {
    var button = element('button', spec.className || 'sb-btn', spec.label);
    button.type = 'button';
    if (spec.id) button.id = spec.id;
    if (spec.title) button.title = spec.title;
    button.addEventListener('click', function () { invoke(callbacks, spec.action); });
    return button;
  }

  function appendActions(host, actions, callbacks) {
    (actions || []).forEach(function (spec) {
      if (spec.separator) {
        host.appendChild(element('hr', 'sb-sep'));
      } else {
        host.appendChild(actionButton(spec, callbacks));
      }
    });
  }

  function commonPanelMarkup() {
    return ''
      + '<div id="panel-configs" class="view-panel active">'
      +   '<div id="configs-toolbar" class="bt-panel-toolbar">'
      +     '<input type="text" id="configs-filter" class="sb-input" style="max-width:240px" placeholder="Search name...">'
      +     '<span class="bt-toolbar-spacer"></span>'
      +     '<button type="button" class="act-btn" data-bt-action="selectAllConfigs">Select All</button>'
      +     '<button type="button" class="act-btn" data-bt-action="deselectAllConfigs">Deselect</button>'
      +   '</div>'
      +   '<div id="configs-list"></div><div id="configs-editor" style="display:none"></div>'
      + '</div>'
      + '<div id="panel-queue" class="view-panel">'
      +   '<div id="queue-toolbar" class="bt-panel-toolbar"><span class="bt-toolbar-spacer"></span>'
      +     '<button type="button" class="act-btn" data-bt-action="selectAllQueue">Select All</button>'
      +     '<button type="button" class="act-btn" data-bt-action="deselectAllQueue">Deselect</button>'
      +   '</div><div id="queue-list"></div>'
      + '</div>'
      + '<div id="panel-results" class="view-panel">'
      +   '<div id="results-fixed-top"><div id="results-toolbar">'
      +     '<label style="font-size:var(--fs-sm);color:var(--text-dim)">Version:</label>'
      +     '<select id="results-version-filter" class="sb-input" style="max-width:100px"><option value="v7">PBv7</option><option value="v8">PBv8</option><option value="both">Both</option></select>'
      +     '<label style="font-size:var(--fs-sm);color:var(--text-dim)">Config:</label>'
      +     '<select id="results-config-filter" class="sb-input" style="max-width:200px"><option value="">All configs</option></select>'
      +     '<input type="text" id="results-filter" class="sb-input" style="max-width:200px" placeholder="Search name...">'
      +     '<span id="results-count-label" class="results-count-label"></span><span class="bt-toolbar-spacer"></span>'
      +     '<button type="button" class="act-btn" data-bt-action="selectAllResults">Select All</button>'
      +     '<button type="button" class="act-btn" data-bt-action="deselectAllResults">Deselect</button>'
      +     '<button type="button" id="results-pin-btn" class="act-btn" data-bt-action="toggleResultsSticky" title="Pin table">📌</button>'
      +   '</div><div id="results-list-wrap"><div id="results-list"></div></div>'
      +   '<div id="results-resize-handle" title="Drag to resize list"><span></span></div></div>'
      +   '<div id="results-scroll-area"><div id="compare-chart-area" style="display:none"></div><div id="results-charts" style="display:none"></div><div id="results-detail"></div></div>'
      + '</div>';
  }

  function create(options) {
    options = options || {};
    var callbacks = options.callbacks || {};
    var root = typeof options.root === 'string' ? document.getElementById(options.root) : options.root;
    if (!root) throw new Error('Backtest shell root not found');
    root.replaceChildren();

    var pageBody = element('div'); pageBody.id = 'page-body';
    var sidebar = element('div'); sidebar.id = 'sidebar';
    var sidebarInner = element('div'); sidebarInner.id = 'sidebar-inner';
    var navItems = options.navItems || [
      { panel: 'configs', icon: '📋', label: 'Configs' },
      { panel: 'queue', icon: '⏳', label: 'Queue', badge: true },
      { panel: 'results', icon: '📊', label: 'Results' }
    ];
    navItems.forEach(function (item, index) {
      var button = element('button', 'sb-section' + (index === 0 ? ' active' : ''));
      button.type = 'button'; button.dataset.panel = item.panel;
      button.appendChild(element('span', 'sb-icon', item.icon));
      button.appendChild(document.createTextNode(' ' + item.label));
      if (item.badge) {
        var badge = element('span'); badge.id = 'queue-count-badge';
        badge.style.cssText = 'margin-left:auto;font-size:var(--fs-xs);color:var(--text-dim)';
        button.appendChild(badge);
      }
      button.addEventListener('click', function () { invoke(callbacks, 'selectPanel:' + item.panel); });
      sidebarInner.appendChild(button);
    });
    sidebarInner.appendChild(element('hr', 'sb-sep'));

    var contexts = options.contexts || {};
    ['configs', 'queue', 'results'].forEach(function (name, index) {
      var context = element('div', 'ctx-actions'); context.id = 'ctx-' + name;
      if (index) context.style.display = 'none';
      appendActions(context, contexts[name], callbacks);
      sidebarInner.appendChild(context);
    });
    if (options.runtimeStatus) {
      var runtime = element('div', 'bt-runtime-status', 'Checking PB8 runtime...'); runtime.id = 'bt-runtime-status';
      sidebarInner.appendChild(runtime);
    }
    sidebar.appendChild(sidebarInner);

    var editorSidebar = element('div', 'sidebar-sticky'); editorSidebar.id = 'sidebar-editor'; editorSidebar.style.display = 'none';
    var editorHeader = element('div', 'sidebar-header'); editorHeader.appendChild(element('span', 'sb-title', options.editorTitle || 'EDIT BACKTEST'));
    var editorToolbar = element('div', 'sidebar-toolbar');
    appendActions(editorToolbar, options.editorActions || [], callbacks);
    editorSidebar.appendChild(editorHeader); editorSidebar.appendChild(editorToolbar); sidebar.appendChild(editorSidebar);
    var resize = element('div'); resize.id = 'sidebar-resize'; sidebar.appendChild(resize);

    var main = element('div'); main.id = 'main-content'; main.innerHTML = commonPanelMarkup();
    pageBody.appendChild(sidebar); pageBody.appendChild(main); root.appendChild(pageBody);
    if (options.overlays !== false) {
      var toast = element('div'); toast.id = 'toast'; root.appendChild(toast);
      var modalRoot = element('div'); modalRoot.id = 'modal-root'; modalRoot.appendChild(element('div', 'modal-box')); root.appendChild(modalRoot);
      var logPanel = element('div'); logPanel.id = 'log-panel';
      var logHeader = element('div'); logHeader.id = 'log-panel-header';
      var logTitle = element('span', '', 'Log'); logTitle.id = 'log-panel-title';
      var logClose = element('button', '', '×'); logClose.id = 'log-panel-close'; logClose.type = 'button';
      logHeader.appendChild(logTitle); logHeader.appendChild(logClose);
      var logTarget = element('div'); logTarget.id = 'log-viewer-target'; logTarget.style.cssText = 'flex:1;min-height:0;overflow:hidden';
      logPanel.appendChild(logHeader); logPanel.appendChild(logTarget); root.appendChild(logPanel);
    }

    root.querySelectorAll('[data-bt-action]').forEach(function (button) {
      button.addEventListener('click', function () { invoke(callbacks, button.dataset.btAction); });
    });
    var configFilter = document.getElementById('configs-filter');
    var resultFilter = document.getElementById('results-filter');
    var resultConfigFilter = document.getElementById('results-config-filter');
    var resultVersionFilter = document.getElementById('results-version-filter');
    configFilter.addEventListener('input', function () { invoke(callbacks, 'filterConfigs'); });
    resultFilter.addEventListener('input', function () { invoke(callbacks, 'filterResults'); });
    resultConfigFilter.addEventListener('change', function () { invoke(callbacks, 'filterResults'); });
    resultVersionFilter.addEventListener('change', function () { invoke(callbacks, 'changeResultsVersion'); });

    var instance = adopt({ root: root, callbacks: callbacks });
    bindVerticalResize(document.getElementById('results-resize-handle'), document.getElementById('results-list-wrap'));
    return instance;
  }

  function upgradeLegacy(options) {
    options = options || {};
    var source = options.source;
    if (!source || !source.parentNode) throw new Error('Legacy backtest shell source not found');
    var sourceSidebar = source.querySelector('#sidebar');
    var preservedContexts = Array.from(sourceSidebar.querySelectorAll('#sidebar-inner > [id^="ctx-"]'));
    var preservedEditor = sourceSidebar.querySelector('#sidebar-editor');
    var preservedPanels = Array.from(source.querySelectorAll('#main-content > .view-panel')).filter(function (panel) {
      return ['panel-configs', 'panel-queue', 'panel-results'].indexOf(panel.id) < 0;
    });
    var root = element('div'); root.id = options.rootId || 'shared-backtest-shell-root';
    source.parentNode.insertBefore(root, source);
    var instance = create({
      root: root,
      overlays: false,
      callbacks: options.callbacks || {},
      navItems: options.navItems,
      contexts: { configs: [], queue: [], results: [] },
      editorActions: []
    });
    var newSidebarInner = root.querySelector('#sidebar-inner');
    root.querySelectorAll('#sidebar-inner > [id^="ctx-"]').forEach(function (node) { node.remove(); });
    preservedContexts.forEach(function (node) { newSidebarInner.appendChild(node); });
    var newEditor = root.querySelector('#sidebar-editor');
    if (preservedEditor) newEditor.replaceWith(preservedEditor);
    var main = root.querySelector('#main-content');
    preservedPanels.forEach(function (panel) { main.appendChild(panel); });
    source.remove();
    return instance;
  }

  function adopt(options) {
    options = options || {};
    var root = options.root || document;
    return {
      selectPanel: function (id, selectOptions) {
        selectOptions = selectOptions || {};
        var navPanel = selectOptions.navPanel || id;
        root.querySelectorAll('.sb-section').forEach(function (button) {
          button.classList.toggle('active', button.getAttribute('data-panel') === navPanel);
        });
        root.querySelectorAll('.view-panel').forEach(function (panel) {
          panel.classList.toggle('active', panel.id === 'panel-' + id);
        });
        root.querySelectorAll('.ctx-actions').forEach(function (context) {
          context.style.display = context.id === 'ctx-' + id ? '' : 'none';
        });
      },
      setEditorMode: function (open) {
        var sidebarInner = document.getElementById('sidebar-inner');
        var sidebarEditor = document.getElementById('sidebar-editor');
        var list = document.getElementById('configs-list');
        var toolbar = document.getElementById('configs-toolbar');
        var editor = document.getElementById('configs-editor');
        if (sidebarInner) sidebarInner.style.display = open ? 'none' : '';
        if (sidebarEditor) sidebarEditor.style.display = open ? '' : 'none';
        if (list) list.style.display = open ? 'none' : '';
        if (toolbar) toolbar.style.display = open ? 'none' : '';
        if (editor) editor.style.display = open ? '' : 'none';
      },
      statusClass: function (status) {
        return 'badge-' + String(status || 'unknown').replace(/[^a-z_-]/gi, '').toLowerCase();
      }
    };
  }

  function statusBadge(status) {
    return element('span', 'badge ' + adopt({}).statusClass(status), String(status || 'unknown'));
  }

  function renderTable(host, definition) {
    host = typeof host === 'string' ? document.getElementById(host) : host;
    host.replaceChildren();
    var rows = definition.rows || [];
    if (!rows.length) {
      var empty = element('div', 'empty-state');
      empty.appendChild(element('div', 'empty-icon', definition.emptyIcon || '📋'));
      empty.appendChild(document.createTextNode(definition.emptyText || 'No items.'));
      host.appendChild(empty); return;
    }
    var table = element('table', 'tbl');
    var thead = element('thead'); var headRow = element('tr');
    (definition.columns || []).forEach(function (column) { headRow.appendChild(element('th', '', column.label)); });
    thead.appendChild(headRow); table.appendChild(thead);
    var tbody = element('tbody');
    var selecting = false;
    var selectValue = true;
    rows.forEach(function (item) {
      var row = element('tr');
      if (definition.rowDataset) Object.keys(definition.rowDataset(item)).forEach(function (key) { row.dataset[key] = definition.rowDataset(item)[key]; });
      if (definition.isSelected && definition.isSelected(item)) row.classList.add('selected');
      (definition.columns || []).forEach(function (column) {
        var cell = element('td', column.className || '');
        if (column.render) {
          var rendered = column.render(item);
          if (rendered instanceof Node) cell.appendChild(rendered); else cell.textContent = rendered == null || rendered === '' ? '—' : String(rendered);
        } else {
          var value = typeof column.value === 'function' ? column.value(item) : item[column.value];
          cell.textContent = value == null || value === '' ? '—' : String(value);
        }
        row.appendChild(cell);
      });
      if (definition.onClick) row.addEventListener('click', function (event) {
        if (event.detail > 1) return;
        window.setTimeout(function () { if (row.isConnected) definition.onClick(item); }, 180);
      });
      if (definition.onDoubleClick) row.addEventListener('dblclick', function () { definition.onDoubleClick(item); });
      if (definition.selection) {
        function applySelection() {
          definition.selection.setSelected(item, selectValue);
          row.classList.toggle('selected', selectValue);
        }
        row.addEventListener('mousedown', function (event) {
          if (event.button !== 0 || event.target.closest('.actions-cell')) return;
          selecting = true;
          selectValue = !definition.selection.isSelected(item);
          applySelection();
          document.addEventListener('mouseup', function () { selecting = false; }, { once: true });
        });
        row.addEventListener('mouseenter', function () { if (selecting) applySelection(); });
      }
      tbody.appendChild(row);
    });
    table.appendChild(tbody); host.appendChild(table);
  }

  function tableAction(label, callback, options) {
    options = options || {};
    var button = element('button', 'act-btn' + (options.danger ? ' act-btn-danger' : ''), label);
    button.type = 'button'; button.title = options.title || label;
    button.addEventListener('click', function (event) { event.stopPropagation(); callback(); });
    return button;
  }

  function openSettings(options) {
    var root = document.getElementById('modal-root');
    var box = root.querySelector('.modal-box'); box.replaceChildren();
    var header = element('div', 'modal-header'); header.appendChild(element('span', 'modal-title', options.title || 'Backtest Settings'));
    var close = element('button', 'modal-close', '×'); close.type = 'button'; header.appendChild(close); box.appendChild(header);
    var body = element('div', 'modal-body');
    var cpuGroup = element('div', 'form-group'); cpuGroup.appendChild(element('label', '', 'Backtest CPU Slots'));
    var cpu = element('input'); cpu.type = 'number'; cpu.min = '1'; cpu.max = String(options.cpuMax || 1); cpu.value = String(options.cpu || 1); cpu.id = 'bt-settings-cpu'; cpuGroup.appendChild(cpu); body.appendChild(cpuGroup);
    var autoGroup = element('label', 'form-group'); autoGroup.style.marginTop = 'var(--sp-md)';
    var auto = element('input'); auto.type = 'checkbox'; auto.checked = !!options.autostart; auto.id = 'bt-settings-autostart'; auto.style.width = 'auto';
    autoGroup.appendChild(auto); autoGroup.appendChild(document.createTextNode(' Start queued jobs automatically')); body.appendChild(autoGroup); box.appendChild(body);
    var actions = element('div', 'modal-actions'); var cancel = element('button', 'modal-btn', 'Cancel'); var save = element('button', 'modal-btn modal-btn-primary', 'Save');
    actions.appendChild(cancel); actions.appendChild(save); box.appendChild(actions);
    function closeModal() { root.classList.remove('open'); }
    close.addEventListener('click', closeModal); cancel.addEventListener('click', closeModal);
    save.addEventListener('click', function () { options.onSave({ cpu: Number(cpu.value) || 1, autostart: auto.checked }); closeModal(); });
    root.classList.add('open');
  }

  function bindVerticalResize(handle, target) {
    if (!handle || !target) return;
    handle.addEventListener('mousedown', function (event) {
      event.preventDefault(); var startY = event.clientY; var startHeight = target.getBoundingClientRect().height;
      function move(moveEvent) { target.style.height = Math.max(80, startHeight + moveEvent.clientY - startY) + 'px'; }
      function up() { document.removeEventListener('mousemove', move); document.removeEventListener('mouseup', up); }
      document.addEventListener('mousemove', move); document.addEventListener('mouseup', up);
    });
  }

  function toggleResultsSticky() {
    var panel = document.getElementById('panel-results');
    var button = document.getElementById('results-pin-btn');
    var sticky = panel.classList.contains('unpinned');
    panel.classList.toggle('unpinned', !sticky);
    button.style.opacity = sticky ? '1' : '0.4';
    button.title = sticky ? 'Unpin table' : 'Pin table';
    return sticky;
  }

  window.PBGuiBacktestShell = {
    create: create,
    upgradeLegacy: upgradeLegacy,
    adopt: adopt,
    renderTable: renderTable,
    statusBadge: statusBadge,
    tableAction: tableAction,
    openSettings: openSettings,
    toggleResultsSticky: toggleResultsSticky
  };
}());
