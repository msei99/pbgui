/* Shared JSON panel viewer for read-only config/analysis views. */
(function() {
  'use strict';

  if (window.PBGuiJsonPanel) return;

  var STYLE_ID = 'pbgui-json-panel-styles';

  function escapeHtml(value) {
    return String(value == null ? '' : value)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#39;');
  }

  function ensureStyles() {
    if (document.getElementById(STYLE_ID)) return;
    var style = document.createElement('style');
    style.id = STYLE_ID;
    style.textContent = ''
      + '.json-panel-wrap{position:relative;margin-top:var(--sp-md);background:var(--bg2);border:1px solid var(--border);border-radius:6px;padding:var(--sp-md);padding-bottom:0;overflow:hidden;}'
      + '.json-panel-hdr{display:flex;align-items:center;gap:var(--sp-sm);margin-bottom:var(--sp-sm);flex-wrap:wrap;}'
      + '.json-panel-title{margin:0;color:var(--text-dim);font-size:var(--fs-xs);text-transform:uppercase;letter-spacing:.05em;}'
      + '.json-panel-actions{display:flex;align-items:center;gap:var(--sp-xs);flex-wrap:wrap;}'
      + '.json-panel-btn{background:transparent;border:1px solid var(--border);color:var(--text-dim);cursor:pointer;border-radius:4px;padding:2px 8px;font-size:var(--fs-xs);line-height:1.4;}'
      + '.json-panel-btn:hover{color:var(--text);border-color:var(--accent);}'
      + '.json-panel-close{background:none;border:none;color:var(--text-dim);cursor:pointer;font-size:13px;margin-left:auto;padding:0 4px;line-height:1;}'
      + '.json-panel-close:hover{color:var(--red);}'
      + '.json-pre{font-size:var(--fs-xs);background:var(--bg);padding:var(--sp-md);border-radius:4px;overflow:auto;height:400px;min-height:80px;color:var(--green);margin:0;white-space:pre-wrap;word-break:break-word;}'
      + '.json-panel-wrap .chart-resize-handle{height:6px;cursor:row-resize;background:var(--border);display:flex;align-items:center;justify-content:center;}'
      + '.json-panel-wrap .chart-resize-handle span{width:32px;height:2px;background:var(--text-dim);border-radius:2px;opacity:.4;}';
    document.head.appendChild(style);
  }

  function createPanelHtml(options) {
    options = options || {};
    ensureStyles();
    var wrapId = String(options.wrapId || '');
    var preId = String(options.preId || '');
    var title = escapeHtml(options.title || 'Config');
    var closeOnclick = String(options.closeOnclick || '');
    var resizeHandler = String(options.resizeHandler || '');
    var collapsedHeight = String(options.collapsedHeight || '400px');
    var initialText = escapeHtml(options.initialText || '');
    return ''
      + '<div class="json-panel-wrap"' + (wrapId ? ' id="' + escapeHtml(wrapId) + '"' : '') + '>'
      +   '<div class="json-panel-hdr">'
      +     '<span class="json-panel-title">' + title + '</span>'
      +     '<div class="json-panel-actions">'
      +       '<button class="json-panel-btn" onclick="copyJsonPanel(\'' + escapeHtml(preId) + '\',this)" title="Copy to clipboard">⧉ Copy</button>'
      +       '<button class="json-panel-btn json-panel-expand-btn" onclick="expandJsonPanel(\'' + escapeHtml(preId) + '\',this)" title="Expand / Collapse">⬌ Expand</button>'
      +       '<button class="json-panel-btn" onclick="zoomJsonPanel(\'' + escapeHtml(preId) + '\',-1)" title="Smaller font">A−</button>'
      +       '<button class="json-panel-btn" onclick="zoomJsonPanel(\'' + escapeHtml(preId) + '\',1)" title="Larger font">A+</button>'
      +     '</div>'
      +     (closeOnclick ? '<button class="json-panel-close" onclick="' + closeOnclick + '" title="Close">✕</button>' : '')
      +   '</div>'
      +   '<pre id="' + escapeHtml(preId) + '" class="json-pre" data-collapsed-height="' + escapeHtml(collapsedHeight) + '">' + initialText + '</pre>'
      +   (resizeHandler ? '<div class="chart-resize-handle" onmousedown="' + resizeHandler + '" title="Drag to resize"><span></span></div>' : '')
      + '</div>';
  }

  function syncExpandButton(pre) {
    var wrap = pre && pre.closest ? pre.closest('.json-panel-wrap') : null;
    var btn = wrap ? wrap.querySelector('.json-panel-expand-btn') : null;
    if (!btn) return;
    btn.textContent = pre.dataset.expanded === '1' ? '⬍ Collapse' : '⬌ Expand';
  }

  function setExpanded(preId, expanded) {
    var el = document.getElementById(preId);
    if (!el) return;
    var collapsedHeight = String(el.dataset.collapsedHeight || '400px');
    el.style.height = expanded ? 'auto' : collapsedHeight;
    el.dataset.expanded = expanded ? '1' : '0';
    syncExpandButton(el);
  }

  function copyJsonPanel(preId, btn) {
    var el = document.getElementById(preId);
    if (!el) return;
    navigator.clipboard.writeText(el.textContent || '').then(function() {
      if (!btn) return;
      var original = btn.textContent;
      btn.textContent = '✓ Copied';
      window.setTimeout(function() { btn.textContent = original; }, 1400);
    }).catch(function() {});
  }

  function expandJsonPanel(preId, btn) {
    var el = document.getElementById(preId);
    if (!el) return;
    setExpanded(preId, el.dataset.expanded !== '1');
    if (btn) syncExpandButton(el);
  }

  function zoomJsonPanel(preId, dir) {
    var el = document.getElementById(preId);
    if (!el) return;
    var current = parseFloat(el.dataset.fontSize) || parseFloat(getComputedStyle(el).fontSize) || 11;
    var next = Math.min(24, Math.max(8, current + dir));
    el.style.fontSize = next + 'px';
    el.dataset.fontSize = String(next);
  }

  function setContent(preId, value, options) {
    var el = document.getElementById(preId);
    if (!el) return;
    var text = value;
    if (value && typeof value === 'object') text = JSON.stringify(value, null, 4);
    el.textContent = String(text == null ? '' : text);
    setExpanded(preId, !(options && options.expanded === false));
  }

  window.PBGuiJsonPanel = {
    ensureStyles: ensureStyles,
    createPanelHtml: createPanelHtml,
    setExpanded: setExpanded,
    setContent: setContent,
    copyJsonPanel: copyJsonPanel,
    expandJsonPanel: expandJsonPanel,
    zoomJsonPanel: zoomJsonPanel,
  };

  window.copyJsonPanel = copyJsonPanel;
  window.expandJsonPanel = expandJsonPanel;
  window.zoomJsonPanel = zoomJsonPanel;
}());
