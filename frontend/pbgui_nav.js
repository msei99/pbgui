/* pbgui_nav.js — Shared navigation bar for PBGui standalone FastAPI pages.
 *
 * HOW TO USE IN A NEW STANDALONE PAGE:
 *   1. Add an empty placeholder in your HTML body:        <nav id="topnav"></nav>
 *   2. Near the end of <body>, set config and include this script:
 *        <script>
 *          window.PBGUI_NAV_CONFIG = { subtitle: 'MY PAGE', current: 'page_key' };
 *        </script>
 *        <script src="/app/pbgui_nav.js"></script>
 *   3. The following globals must be set anywhere before this script runs:
 *        TOKEN, ST_BASE, API_BASE, PBGUI_VERSION
 *
 * The 'current' value in PBGUI_NAV_CONFIG must match a 'page' key in NAV_GROUPS below.
 * The active nav group is highlighted automatically.
 *
 * Guide button: navigates to the Streamlit Help page.
 * About button: opens an in-page modal with version info and links.
 */
(function () {
  'use strict';

  /* ── config (read at runtime so global vars are already set) ── */
  function cfg() {
    var c = window.PBGUI_NAV_CONFIG || {};
    return {
      token:    c.token    !== undefined ? c.token    : (window.TOKEN    || ''),
      stBase:   c.stBase   !== undefined ? c.stBase   : (window.ST_BASE  || ''),
      apiBase:  c.apiBase  !== undefined ? c.apiBase  : (window.API_BASE || ''),
      version:  c.version  !== undefined ? c.version  : (window.PBGUI_VERSION || ''),
      serial:   c.serial   !== undefined ? c.serial   : (window.PBGUI_SERIAL  || ''),
      subtitle: c.subtitle || 'PBGui',
      current:  c.current  || ''
    };
  }

  /* ════════════════════════════════════
     NAV STRUCTURE
     ════════════════════════════════════ */
  var NAV_GROUPS = [
    { id: 'system', label: 'System', items: [
      { page: '/',                    icon: '&#128682;', label: 'Welcome'           },
      { page: 'system_api_keys',      icon: '&#128273;', label: 'API-Keys'          },
      { page: 'system_services',      icon: '&#128295;', label: 'PBGUI Services'    },
      { page: 'system_vps_manager',   icon: '&#128421;', label: 'VPS Manager'       },
      { page: 'system_vps_monitor',   icon: '&#128223;', label: 'VPS Monitor'       },
      { page: 'system_logging',       icon: '&#128196;', label: 'Logging'           }
    ]},
    { id: 'information', label: 'Information', items: [
      { page: 'dashboards',           icon: '&#128202;', label: 'Dashboards'        },
      { page: 'info_coin_data',       icon: '&#129689;', label: 'Coin Data'         },
      { page: 'info_market_data',     icon: '&#128452;', label: 'Market Data'       },
      { page: 'help',                 icon: '&#10067;',  label: 'Help'              }
    ]},
    { id: 'pbv7', label: 'PBv7', items: [
      { page: 'v7_run',               icon: '&#9654;',   label: 'Run'               },
      { page: 'v7_backtest',          icon: '&#9194;',   label: 'Backtest'          },
      { page: 'v7_live_vs_backtest',  icon: '&#8644;',   label: 'Live vs Backtest'  },
      { page: 'v7_optimize',          icon: '&#9881;',   label: 'Optimize'          },
      { page: 'v7_strategy_explorer', icon: '&#128065;', label: 'Strategy Explorer' },
      { page: 'v7_balance_calc',      icon: '&#128176;', label: 'Balance Calculator'},
      { page: 'v7_pareto_explorer',   icon: '&#127919;', label: 'Pareto Explorer'   }
    ]}
  ];

  /* ════════════════════════════════════
     CSS (injected into <head>)
     ════════════════════════════════════ */
  var NAV_CSS = [
    'html,body{height:100%;}',
    'body{padding:0!important;overflow:hidden;}',

    '#topnav{display:flex;align-items:center;height:52px;background:#111827;',
    'border-bottom:1px solid #1e2736;flex-shrink:0;position:relative;z-index:200;',
    'padding:0 0.5rem;gap:0.25rem;user-select:none;}',

    '#nav-logo{display:flex;align-items:center;padding:0 0.75rem 0 0.25rem;',
    'flex-shrink:0;cursor:pointer;text-decoration:none;border-right:1px solid #1e2736;',
    'margin-right:0.25rem;height:100%;}',

    '.nav-group{position:relative;height:100%;display:flex;align-items:center;}',

    '.nav-group-btn{display:flex;align-items:center;gap:0.3rem;padding:0.3rem 0.7rem;',
    'height:100%;background:transparent;border:none;color:#94a3b8;font-size:var(--fs-base);',
    'font-weight:500;cursor:pointer;border-bottom:2px solid transparent;',
    'transition:color .15s,border-color .15s,background .15s;white-space:nowrap;}',
    '.nav-group-btn:hover{color:#e2e8f0;background:rgba(255,255,255,.04);}',
    '.nav-group-btn.active{color:#63b3ed;border-bottom-color:#63b3ed;}',

    '.nav-arrow{font-size:0.58rem;opacity:0.6;transition:transform .15s;}',
    '.nav-group.open .nav-arrow{transform:rotate(180deg);}',

    '.nav-dropdown{display:none;position:absolute;top:calc(100% + 1px);left:0;',
    'min-width:190px;background:#1a202c;border:1px solid #2d3748;',
    'border-radius:0 0 8px 8px;box-shadow:0 8px 24px rgba(0,0,0,.5);',
    'flex-direction:column;padding:0.3rem 0;z-index:300;}',
    '.nav-group.open .nav-dropdown{display:flex;}',

    '.nav-item{display:flex;align-items:center;gap:0.55rem;padding:0.42rem 1rem;',
    'color:#94a3b8;font-size:var(--fs-base);text-decoration:none;cursor:pointer;',
    'transition:background .1s,color .1s;white-space:nowrap;}',
    '.nav-item:hover{background:#243047;color:#e2e8f0;}',
    '.nav-item.current{color:#63b3ed;font-weight:600;background:rgba(99,179,237,.07);cursor:default;}',
    '.nav-item-icon{font-size:1rem;width:1.1rem;text-align:center;flex-shrink:0;}',

    '#nav-spacer{flex:1;}',

    '#nav-right{display:flex;align-items:center;gap:0.25rem;padding-right:0.5rem;}',
    '.nav-action-btn{display:flex;align-items:center;gap:0.35rem;padding:0.3rem 0.75rem;',
    'border-radius:6px;background:transparent;border:1px solid transparent;',
    'color:#64748b;font-size:var(--fs-sm);font-weight:500;cursor:pointer;',
    'transition:all .15s;white-space:nowrap;height:32px;}',
    '.nav-action-btn:hover{background:rgba(255,255,255,.05);border-color:#2d3748;color:#e2e8f0;}',
    '.nav-action-btn.accent{color:#63b3ed;border-color:rgba(99,179,237,.25);background:rgba(99,179,237,.04);}',
    '.nav-action-btn.accent:hover{background:rgba(99,179,237,.12);border-color:#63b3ed;}',
    '.nav-action-btn.restart{color:#f59e0b;border-color:rgba(245,158,11,.3);background:rgba(245,158,11,.06);display:none;}',
    '.nav-action-btn.restart:hover{background:rgba(245,158,11,.15);border-color:#f59e0b;}',
    '.nav-restart-dot{display:inline-block;width:7px;height:7px;border-radius:50%;',
    'background:#f59e0b;margin-right:2px;animation:nav-blink 1.4s ease-in-out infinite;}',
    '@keyframes nav-blink{0%,100%{opacity:1;}50%{opacity:.3;}}',
    '.nav-action-btn.notify{color:#64748b;}',
    '.nav-action-btn.notify:hover{color:#e2e8f0;}',

    /* notification log panel */
    '#pbgui-notify-panel{position:fixed;bottom:0;right:0;width:50%;height:40vh;min-width:240px;min-height:150px;',
    'background:var(--bg2,#131b2b);border:2px solid var(--accent,#3182ce);',
    'z-index:2500;display:none;flex-direction:column;overflow:hidden;border-radius:6px 6px 0 0;}',
    '#pbgui-notify-panel.visible{display:flex;}',
    '#pbgui-notify-hdr{display:flex;align-items:center;justify-content:space-between;',
    'padding:6px 12px;background:var(--bg3,#1a2744);border-bottom:1px solid var(--border,#2d3748);',
    'flex-shrink:0;cursor:move;user-select:none;}',
    '#pbgui-notify-title{font-size:var(--fs-sm,0.82rem);font-weight:600;color:var(--text,#e2e8f0);}',
    '#pbgui-notify-close{background:none;border:none;color:var(--text-dim,#64748b);cursor:pointer;font-size:var(--fs-lg,1.15rem);}',
    '#pbgui-notify-close:hover{color:var(--text,#e2e8f0);}',
    '#pbgui-notify-target{flex:1;min-height:0;overflow:hidden;}',
    '.pnr{position:absolute;z-index:2;}',
    '.pnr-n{top:-4px;left:6px;right:6px;height:8px;cursor:n-resize;}',
    '.pnr-s{bottom:-4px;left:6px;right:6px;height:8px;cursor:s-resize;}',
    '.pnr-w{left:-4px;top:6px;bottom:6px;width:8px;cursor:w-resize;}',
    '.pnr-e{right:-4px;top:6px;bottom:6px;width:8px;cursor:e-resize;}',
    '.pnr-nw{top:-4px;left:-4px;width:12px;height:12px;cursor:nw-resize;}',
    '.pnr-ne{top:-4px;right:-4px;width:12px;height:12px;cursor:ne-resize;}',
    '.pnr-sw{bottom:-4px;left:-4px;width:12px;height:12px;cursor:sw-resize;}',
    '.pnr-se{bottom:-4px;right:-4px;width:12px;height:12px;cursor:se-resize;}',

    /* about overlay */
    '#pbgui-about-ovl{display:none;position:fixed;inset:0;',
    'background:rgba(0,0,0,.72);z-index:3000;align-items:center;justify-content:center;',
    'backdrop-filter:blur(2px);}',
    '#pbgui-about-ovl.visible{display:flex;}',
    '#pbgui-about-box{background:#131b2b;border:1px solid #2d3748;border-radius:14px;',
    'box-shadow:0 20px 70px rgba(0,0,0,.9);overflow:hidden;width:min(440px,92vw);}',
    '.pbgui-ovl-header{display:flex;align-items:center;justify-content:space-between;',
    'padding:0.85rem 1.1rem;border-bottom:1px solid #1e2736;background:#111827;}',
    '.pbgui-ovl-title{font-size:var(--fs-md);font-weight:700;color:#e2e8f0;}',
    '.pbgui-ovl-close{background:transparent;border:none;color:#64748b;font-size:var(--fs-lg);',
    'cursor:pointer;padding:0.2rem 0.35rem;border-radius:5px;line-height:1;',
    'transition:color .12s,background .12s;}',
    '.pbgui-ovl-close:hover{color:#e2e8f0;background:rgba(255,255,255,.06);}',
    '#pbgui-about-body{padding:2rem 2rem 1.5rem;text-align:center;}',
    '#pbgui-about-ver{font-size:var(--fs-xl);font-weight:800;color:#e2e8f0;margin-bottom:0.2rem;}',
    '#pbgui-about-serial{font-size:var(--fs-xs);color:#64748b;margin-bottom:0.25rem;}',
    '#pbgui-about-tag{font-size:var(--fs-sm);color:#64748b;letter-spacing:.06em;',
    'text-transform:uppercase;margin-bottom:1.5rem;}',
    '.pbgui-about-divider{width:100%;height:1px;',
    'background:linear-gradient(90deg,transparent,#2d3748,transparent);margin:0 0 1.3rem;}',
    '.pbgui-about-links{display:flex;flex-direction:column;gap:0.6rem;margin-bottom:1.3rem;}',
    '.pbgui-about-link{display:flex;align-items:center;justify-content:center;gap:0.55rem;',
    'padding:0.55rem 1.2rem;border-radius:8px;text-decoration:none;font-size:var(--fs-sm);',
    'font-weight:600;transition:all .15s;}',
    '.pbgui-about-link.kofi{background:rgba(255,94,20,.1);border:1px solid rgba(255,94,20,.35);color:#ff6a30;}',
    '.pbgui-about-link.kofi:hover{background:rgba(255,94,20,.2);border-color:#ff6a30;}',
    '.pbgui-about-link.github{background:rgba(99,179,237,.07);border:1px solid rgba(99,179,237,.25);color:#63b3ed;}',
    '.pbgui-about-link.github:hover{background:rgba(99,179,237,.15);border-color:#63b3ed;}',
    '.pbgui-about-link.readme{background:rgba(72,187,120,.07);border:1px solid rgba(72,187,120,.25);color:#48bb78;}',
    '.pbgui-about-link.readme:hover{background:rgba(72,187,120,.15);border-color:#48bb78;}',
    '#pbgui-about-footer{padding:0.75rem 2rem;border-top:1px solid #1e2736;',
    'text-align:center;font-size:var(--fs-xs);color:#4a5568;background:#0e1117;}',

    /* page content wrapper — used when page wraps its content in #page-content */
    '#page-content{height:calc(100vh - 52px);overflow-y:auto;padding:20px;}'
  ].join('');

  /* ════════════════════════════════════
     BUILD
     ════════════════════════════════════ */
  function injectCSS() {
    if (document.getElementById('pbgui-nav-css')) return;
    var s = document.createElement('style');
    s.id  = 'pbgui-nav-css';
    s.textContent = NAV_CSS;
    document.head.appendChild(s);
  }

  function buildNav() {
    var nav = document.getElementById('topnav');
    if (!nav) return;
    var c = cfg();
    var CURRENT = c.current;
    var navGroups = NAV_GROUPS.map(function (group) {
      return {
        id: group.id,
        label: group.label,
        items: group.items.slice()
      };
    });

    /* find which group contains the current page */
    var activeGroup = '';
    navGroups.forEach(function (g) {
      g.items.forEach(function (item) {
        if (item.page === CURRENT) activeGroup = g.id;
      });
    });

    var html = '';

    /* logo */
    html += '<a id="nav-logo" href="#" title="PBGui">'
          + '<svg width="112" height="36" viewBox="0 0 112 36" xmlns="http://www.w3.org/2000/svg">'
          + '<rect x="1" y="1" width="34" height="34" rx="7" fill="#1a2744" stroke="#3182ce" stroke-width="1.5"/>'
          + '<rect x="7" y="21" width="5" height="9" rx="1.5" fill="#63b3ed"/>'
          + '<rect x="14.5" y="15" width="5" height="15" rx="1.5" fill="#4299e1"/>'
          + '<rect x="22" y="9" width="5" height="21" rx="1.5" fill="#3182ce"/>'
          + '<text x="42" y="15" font-family="\'Segoe UI\',system-ui,sans-serif" font-size="13" font-weight="700" fill="#e2e8f0" letter-spacing="0.3">PBGui</text>'
          + '<text x="42" y="28" font-family="\'Segoe UI\',system-ui,sans-serif" font-size="7.5" font-weight="400" fill="#4299e1" letter-spacing="1.2">' + esc(c.subtitle) + '</text>'
          + '</svg></a>';

    /* groups */
    navGroups.forEach(function (group) {
      var isActive = (group.id === activeGroup);
      html += '<div class="nav-group">';
      html += '<button class="nav-group-btn' + (isActive ? ' active' : '') + '" data-group="' + group.id + '">';
      html += esc(group.label) + ' <span class="nav-arrow">&#9660;</span></button>';
      html += '<div class="nav-dropdown">';
      group.items.forEach(function (item) {
        var isCurrent = (item.page === CURRENT);
        if (isCurrent) {
          html += '<span class="nav-item current"><span class="nav-item-icon">' + item.icon + '</span>' + esc(item.label) + '</span>';
        } else {
          html += '<a class="nav-item" data-page="' + item.page + '"><span class="nav-item-icon">' + item.icon + '</span>' + esc(item.label) + '</a>';
        }
      });
      html += '</div></div>';
    });

    /* spacer + right buttons */
    html += '<div id="nav-spacer"></div>';
    html += '<div id="nav-right">'
          + '<button class="nav-action-btn restart" id="pbgui-restart-btn"><span class="nav-restart-dot"></span>Restart</button>'
          + '<button class="nav-action-btn notify" id="pbgui-notify-btn" title="Notification log">&#128276;</button>'
          + '<button class="nav-action-btn accent" id="pbgui-guide-btn">&#128218; Guide</button>'
          + '<button class="nav-action-btn" id="pbgui-about-btn">&#x2139;&#xFE0F; About</button>'
          + '</div>';

    nav.innerHTML = html;
  }

  /* ════════════════════════════════════
     NOTIFICATION LOG PANEL
     ════════════════════════════════════ */
  var _notifyViewer = null;

  function buildNotifyPanel() {
    if (document.getElementById('pbgui-notify-panel')) return;
    var d = document.createElement('div');
    d.id = 'pbgui-notify-panel';
    d.innerHTML =
      '<div class="pnr pnr-n" data-dir="n"></div>' +
      '<div class="pnr pnr-s" data-dir="s"></div>' +
      '<div class="pnr pnr-w" data-dir="w"></div>' +
      '<div class="pnr pnr-e" data-dir="e"></div>' +
      '<div class="pnr pnr-nw" data-dir="nw"></div>' +
      '<div class="pnr pnr-ne" data-dir="ne"></div>' +
      '<div class="pnr pnr-sw" data-dir="sw"></div>' +
      '<div class="pnr pnr-se" data-dir="se"></div>' +
      '<div id="pbgui-notify-hdr">' +
        '<span id="pbgui-notify-title">Notifications</span>' +
        '<button id="pbgui-notify-close">\u2715</button>' +
      '</div>' +
      '<div id="pbgui-notify-target" style="flex:1;min-height:0;overflow:hidden"></div>';
    document.body.appendChild(d);
  }

  function _ensureLogViewer(cb) {
    if (typeof window.LogViewerPanel === 'function') { cb(); return; }
    var s = document.createElement('script');
    s.src = '/app/js/log_viewer_panel.js?v=8';
    s.onload = cb;
    s.onerror = function() { console.warn('Failed to load log_viewer_panel.js'); };
    document.head.appendChild(s);
  }

  function _getWsBase() {
    if (window.WS_BASE) return window.WS_BASE;
    var proto = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    return proto + '//' + window.location.host;
  }

  function toggleNotifyPanel() {
    var panel = document.getElementById('pbgui-notify-panel');
    if (!panel) return;
    if (panel.classList.contains('visible')) {
      closeNotifyPanel();
      return;
    }
    _ensureLogViewer(function() {
      if (_notifyViewer) { _notifyViewer.close(); _notifyViewer = null; }
      _notifyViewer = new LogViewerPanel({
        containerId: 'pbgui-notify-target',
        wsBase: _getWsBase(),
        token: cfg().token,
        defaultHost: 'local',
        defaultFile: 'PBV7UI.log',
        presets: 'system',
        showRestart: false,
        height: '100%'
      });
      _notifyViewer.open();
      if (!panel.style.left) {
        panel.style.right = '0'; panel.style.bottom = '0';
        panel.style.left = ''; panel.style.top = '';
      }
      panel.classList.add('visible');
      _bindNotifyDrag(panel);
      _bindNotifyResize(panel);
    });
  }

  function closeNotifyPanel() {
    var panel = document.getElementById('pbgui-notify-panel');
    if (panel) panel.classList.remove('visible');
    if (_notifyViewer) { _notifyViewer.close(); _notifyViewer = null; }
  }

  function _bindNotifyDrag(panel) {
    var hdr = document.getElementById('pbgui-notify-hdr');
    if (!hdr || hdr._dragBound) return;
    hdr._dragBound = true;
    hdr.addEventListener('mousedown', function(e) {
      if (e.target.id === 'pbgui-notify-close') return;
      var rect = panel.getBoundingClientRect();
      panel.style.left = rect.left + 'px'; panel.style.top = rect.top + 'px';
      panel.style.right = 'auto'; panel.style.bottom = 'auto';
      var sX = e.clientX, sY = e.clientY, sL = rect.left, sT = rect.top;
      function onMove(e) {
        panel.style.left = (sL + e.clientX - sX) + 'px';
        panel.style.top  = (sT + e.clientY - sY) + 'px';
      }
      function onUp() {
        document.removeEventListener('mousemove', onMove);
        document.removeEventListener('mouseup', onUp);
      }
      document.addEventListener('mousemove', onMove);
      document.addEventListener('mouseup', onUp);
      e.preventDefault();
    });
  }

  function _bindNotifyResize(panel) {
    if (panel._resizeBound) return;
    panel._resizeBound = true;
    panel.querySelectorAll('.pnr').forEach(function(handle) {
      handle.addEventListener('mousedown', function(e) {
        e.preventDefault(); e.stopPropagation();
        var dir = handle.dataset.dir;
        var rect = panel.getBoundingClientRect();
        panel.style.left = rect.left + 'px'; panel.style.top = rect.top + 'px';
        panel.style.right = 'auto'; panel.style.bottom = 'auto';
        panel.style.width = rect.width + 'px'; panel.style.height = rect.height + 'px';
        var sX = e.clientX, sY = e.clientY;
        var sL = rect.left, sT = rect.top, sW = rect.width, sH = rect.height;
        function onMove(e) {
          var dx = e.clientX - sX, dy = e.clientY - sY;
          var nL = sL, nT = sT, nW = sW, nH = sH;
          if (dir.indexOf('w') >= 0) { nL = sL + dx; nW = sW - dx; }
          if (dir.indexOf('e') >= 0) { nW = sW + dx; }
          if (dir.indexOf('n') >= 0) { nT = sT + dy; nH = sH - dy; }
          if (dir.indexOf('s') >= 0) { nH = sH + dy; }
          if (nW < 240) { if (dir.indexOf('w') >= 0) nL = sL + sW - 240; nW = 240; }
          if (nH < 150) { if (dir.indexOf('n') >= 0) nT = sT + sH - 150; nH = 150; }
          panel.style.left = nL + 'px'; panel.style.top = nT + 'px';
          panel.style.width = nW + 'px'; panel.style.height = nH + 'px';
        }
        function onUp() {
          document.removeEventListener('mousemove', onMove);
          document.removeEventListener('mouseup', onUp);
        }
        document.addEventListener('mousemove', onMove);
        document.addEventListener('mouseup', onUp);
      });
    });
  }

  function buildAbout() {
    if (document.getElementById('pbgui-about-ovl')) return;
    var c = cfg();
    var ver    = esc(c.version || '');
    var serial = esc(c.serial  || '');
    var html = '<div id="pbgui-about-ovl">'
      + '<div id="pbgui-about-box">'
      +   '<div class="pbgui-ovl-header">'
      +     '<span class="pbgui-ovl-title">&#x2139;&#xFE0F; About PBGui</span>'
      +     '<button class="pbgui-ovl-close" id="pbgui-about-close">&#x2715;</button>'
      +   '</div>'
      +   '<div id="pbgui-about-body">'
      +     '<svg width="72" height="72" viewBox="0 0 36 36" xmlns="http://www.w3.org/2000/svg" style="margin-bottom:1.1rem">'
      +       '<rect x="1" y="1" width="34" height="34" rx="7" fill="#1a2744" stroke="#3182ce" stroke-width="1.5"/>'
      +       '<rect x="7" y="21" width="5" height="9" rx="1.5" fill="#63b3ed"/>'
      +       '<rect x="14.5" y="15" width="5" height="15" rx="1.5" fill="#4299e1"/>'
      +       '<rect x="22" y="9" width="5" height="21" rx="1.5" fill="#3182ce"/>'
      +     '</svg>'
      +     '<div id="pbgui-about-ver">PBGui ' + ver + '</div>'
      +     (serial ? '<div id="pbgui-about-serial">API Serial ' + serial + '</div>' : '')
      +     '<div id="pbgui-about-tag">Passivbot GUI &mdash; by msei99</div>'
      +     '<div class="pbgui-about-divider"></div>'
      +     '<div class="pbgui-about-links">'
      +       '<a class="pbgui-about-link kofi" href="https://ko-fi.com/Y8Y216Q3QS" target="_blank" rel="noopener">&#9749; Support on Ko-fi</a>'
      +       '<a class="pbgui-about-link github" href="https://github.com/msei99/pbgui" target="_blank" rel="noopener">&#128279; GitHub Repository</a>'
      +       '<a class="pbgui-about-link readme" href="https://github.com/msei99/pbgui#readme" target="_blank" rel="noopener">&#128196; README</a>'
      +     '</div>'
      +   '</div>'
      +   '<div id="pbgui-about-footer">Open-source &bull; MIT License</div>'
      + '</div></div>';

    var wrapper = document.createElement('div');
    wrapper.innerHTML = html;
    document.body.appendChild(wrapper.firstChild);
  }

  /* ════════════════════════════════════
     FASTAPI DIRECT ROUTES
     Pages served directly by FastAPI — navigate without Streamlit detour.
     Key = nav page id, value = path under the API origin.
     ════════════════════════════════════ */
  var FASTAPI_PAGES = {
    'dashboards':        '/api/dashboard/main_page',
    'system_api_keys':   '/api/api-keys/main_page',
    'system_logging':     '/api/logging/main_page',
    'system_vps_monitor': '/api/vps/main_page',
    'system_services':    '/api/services/main_page',
    'help':               '/app/help.html',
    'v7_run':             '/api/v7/main_page',
    'v7_backtest':        '/api/backtest-v7/main_page',
    'v7_optimize':        '/api/optimize-v7/main_page',
    'v7_balance_calc':    '/api/balance-calc/main_page'
  };

  /* ════════════════════════════════════
     EVENT HANDLERS
     ════════════════════════════════════ */
  function setupHandlers() {
    var c = cfg();
    var TOKEN   = c.token;
    var ST_BASE = c.stBase;

    /* Derive API origin (scheme + host + port) from apiBase or current location */
    var apiOrigin = '';
    if (c.apiBase) {
      var m = c.apiBase.match(/^(https?:\/\/[^/]+)/);
      if (m) apiOrigin = m[1];
    }
    if (!apiOrigin) {
      apiOrigin = window.location.origin;
    }

    function navTo(page) {
      if (!page) return;

      /* Direct FastAPI page — navigate without Streamlit relay */
      if (FASTAPI_PAGES[page] && apiOrigin) {
        var faUrl = apiOrigin + FASTAPI_PAGES[page]
                  + '?token=' + encodeURIComponent(TOKEN)
                  + '&st_base=' + encodeURIComponent(ST_BASE);
        window.location.href = faUrl;
        return;
      }

      if (!ST_BASE) {
        console.warn('[pbgui_nav] ST_BASE is empty — cannot navigate to Streamlit page "' + page + '".');
        var msg = document.createElement('div');
        msg.style.cssText = 'position:fixed;top:60px;left:50%;transform:translateX(-50%);z-index:9999;background:#ef444480;color:#fff;padding:.6rem 1.2rem;border-radius:8px;font-size:.85rem;pointer-events:none;';
        msg.textContent = 'Navigation unavailable — reload the page from the PBGui menu.';
        document.body.appendChild(msg);
        setTimeout(function() { msg.remove(); }, 4000);
        return;
      }
      var pageKey = page === '/' ? 'SYSTEM_LOGIN' : page.toUpperCase();
      window.location.href = ST_BASE + '/?target=' + pageKey + '&token=' + TOKEN;
    }

    /* nav item clicks */
    document.querySelectorAll('.nav-item[data-page]').forEach(function (el) {
      el.addEventListener('click', function (e) {
        e.preventDefault();
        navTo(el.getAttribute('data-page'));
      });
    });

    /* group dropdown toggles */
    document.querySelectorAll('.nav-group-btn').forEach(function (btn) {
      btn.addEventListener('click', function (e) {
        e.stopPropagation();
        var grp = btn.closest('.nav-group');
        var wasOpen = grp.classList.contains('open');
        document.querySelectorAll('.nav-group.open').forEach(function (g) { g.classList.remove('open'); });
        if (!wasOpen) grp.classList.add('open');
      });
    });

    /* close dropdowns on outside click */
    document.addEventListener('click', function () {
      document.querySelectorAll('.nav-group.open').forEach(function (g) { g.classList.remove('open'); });
    });

    /* logo click → navigate to Welcome */
    var logo = document.getElementById('nav-logo');
    if (logo) logo.addEventListener('click', function (e) { e.preventDefault(); navTo('/'); });

    /* Guide button → navigate to Streamlit Help page */
    var guideBtn = document.getElementById('pbgui-guide-btn');
    if (guideBtn) guideBtn.addEventListener('click', function () { navTo('help'); });

    /* Notify button → open inline floating log panel */
    var notifyBtn = document.getElementById('pbgui-notify-btn');
    if (notifyBtn) notifyBtn.addEventListener('click', function () { toggleNotifyPanel(); });
    var notifyClose = document.getElementById('pbgui-notify-close');
    if (notifyClose) notifyClose.addEventListener('click', function () { closeNotifyPanel(); });

    /* About button → show overlay */
    var aboutBtn = document.getElementById('pbgui-about-btn');
    var aboutOvl = document.getElementById('pbgui-about-ovl');
    var aboutClose = document.getElementById('pbgui-about-close');
    if (aboutBtn && aboutOvl) {
      aboutBtn.addEventListener('click', function () { aboutOvl.classList.add('visible'); });
      if (aboutClose) aboutClose.addEventListener('click', function () { aboutOvl.classList.remove('visible'); });
      aboutOvl.addEventListener('click', function (e) {
        if (e.target === aboutOvl) aboutOvl.classList.remove('visible');
      });
    }

    /* Esc key closes about overlay */
    document.addEventListener('keydown', function (e) {
      if (e.key === 'Escape' && aboutOvl) aboutOvl.classList.remove('visible');
    });

    /* Restart button */
    var restartBtn = document.getElementById('pbgui-restart-btn');
    if (restartBtn) {
      restartBtn.addEventListener('click', function () {
        if (!confirm('Restart the PBGui API server now?\nThe page will reload automatically.')) return;
        var c2 = cfg();
        var origin2 = '';
        if (c2.apiBase) { var m2 = c2.apiBase.match(/^(https?:\/\/[^/]+)/); if (m2) origin2 = m2[1]; }
        if (!origin2) origin2 = window.location.origin;
        fetch(origin2 + '/api/server-restart', {
          method: 'POST',
          headers: { 'Authorization': 'Bearer ' + c2.token, 'Content-Type': 'application/json' },
          body: JSON.stringify({ token: c2.token })
        }).then(function() {
          showRestartOverlay(origin2, c2.token);
        }).catch(function() {
          showRestartOverlay(origin2, c2.token);
        });
      });
    }

    /* Restart button: fetch once immediately, then keep live via SSE */
    fetchRestartStatus(TOKEN, apiOrigin);
    setInterval(function () { fetchRestartStatus(TOKEN, apiOrigin); }, 30000);

    /* SSE: watch for needs_restart */
    setupRestartSSE(TOKEN, apiOrigin);
  }

  function showRestartOverlay(origin, token) {
    /* Remove any existing overlay first */
    var existing = document.getElementById('pbgui-restart-overlay');
    if (existing) existing.remove();

    var ov = document.createElement('div');
    ov.id = 'pbgui-restart-overlay';
    ov.style.cssText = 'position:fixed;inset:0;z-index:99999;background:rgba(9,14,26,.92);display:flex;align-items:center;justify-content:center;flex-direction:column;gap:1rem;font-family:sans-serif;';
    ov.innerHTML =
      '<div style="color:#e2e8f0;font-size:1.1rem;font-weight:600;">Restarting API Server\u2026</div>' +
      '<div id="pbgui-restart-status" style="color:#64748b;font-size:0.85rem;">Waiting for server\u2026</div>';
    document.body.appendChild(ov);

    var attempts = 0;
    var maxAttempts = 30;
    var statusEl = document.getElementById('pbgui-restart-status');
    var apiBase = (origin || window.location.origin);

    function probe() {
      attempts++;
      if (statusEl) statusEl.textContent = 'Reconnecting\u2026 (' + attempts + '/' + maxAttempts + ')';
      fetch(apiBase + '/api/services/status?token=' + encodeURIComponent(token || ''), { cache: 'no-store' })
        .then(function (r) {
          if (r.ok) { window.location.reload(); }
          else { if (attempts < maxAttempts) setTimeout(probe, 2000); else _overlayFail(); }
        })
        .catch(function () {
          if (attempts < maxAttempts) setTimeout(probe, 2000); else _overlayFail();
        });
    }

    function _overlayFail() {
      if (statusEl) statusEl.textContent = 'Server did not respond \u2014 please refresh manually.';
    }

    /* First probe after PBGUI_RESTART_DELAY (3s) + a small buffer */
    setTimeout(probe, 4000);
  }

  function updateRestartBtnVisible(visible) {
    var btn = document.getElementById('pbgui-restart-btn');
    if (btn) btn.style.display = visible ? 'flex' : 'none';
  }

  function fetchRestartStatus(token, apiOrigin) {
    if (!token || !apiOrigin) return;
    fetch(apiOrigin + '/api/server-status?token=' + encodeURIComponent(token), { cache: 'no-store' })
      .then(function (resp) {
        if (!resp.ok) throw new Error('server-status failed');
        return resp.json();
      })
      .then(function (data) {
        updateRestartBtnVisible(!!(data && data.needs_restart));
      })
      .catch(function () {});
  }

  function setupRestartSSE(token, apiOrigin) {
    if (!token || !apiOrigin) return;
    var url = apiOrigin + '/api/server-status/stream?token=' + encodeURIComponent(token);
    var es = new EventSource(url);
    es.onmessage = function (e) {
      try {
        var data = JSON.parse(e.data);
        updateRestartBtnVisible(!!data.needs_restart);
      } catch (_) {}
    };
    es.onerror = function () {
      es.close();
      fetchRestartStatus(token, apiOrigin);
      /* retry after 15s */
      setTimeout(function() { setupRestartSSE(token, apiOrigin); }, 15000);
    };
  }

  /* ── html escape helper ── */
  function esc(s) {
    return String(s || '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
  }

  /* ════════════════════════════════════
     TOKEN KEEP-ALIVE & 401 REDIRECT
     ════════════════════════════════════ */

  /* Redirect to Streamlit login when token is invalid/expired. */
  function redirectToLogin() {
    var base = cfg().stBase || '';
    if (base) {
      window.location.replace(base);
    } else {
      window.location.replace('/');
    }
  }

  /* Periodically call /api/token-refresh to extend token expiry.
     Interval: 30 minutes.  If the refresh itself returns 401 we redirect. */
  var _refreshTimer = null;
  function startTokenRefresh() {
    if (_refreshTimer) return;
    var c = cfg();
    if (!c.token) return;
    /* Derive the API root from known page-specific API_BASE values.
       API_BASE is e.g. "http://host:port/api/services" or "/api/services".
       Token-refresh lives at /api/token-refresh.  */
    var apiRoot = '';
    if (window.API_BASE) {
      var m = String(window.API_BASE).match(/^(https?:\/\/[^/]+)/);
      apiRoot = m ? m[1] : '';
    }
    function doRefresh() {
      fetch(apiRoot + '/api/token-refresh?token=' + encodeURIComponent(c.token), { method: 'POST' })
        .then(function (r) {
          if (r.status === 401) { redirectToLogin(); }
        })
        .catch(function () { /* network error — ignore, will retry next cycle */ });
    }
    doRefresh();  /* immediate first refresh on page load */
    _refreshTimer = setInterval(doRefresh, 30 * 60 * 1000);  /* every 30 min */
  }

  /* Global 401 interceptor — monkey-patch window.fetch so ANY fetch returning 401
     triggers a redirect.  This catches background polling, WebSocket auth, etc. */
  var _origFetch = window.fetch;
  window.fetch = function () {
    return _origFetch.apply(this, arguments).then(function (response) {
      if (response.status === 401) {
        redirectToLogin();
      }
      return response;
    });
  };

  /* ════════════════════════════════════
     INIT
     ════════════════════════════════════ */
  function init() {
    injectCSS();
    buildNav();
    buildNotifyPanel();
    buildAbout();
    setupHandlers();
    startTokenRefresh();
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }

  /* Expose overlay helper so other scripts on the same page (e.g. services_monitor.html)
     can call it without requiring closure access to this IIFE. */
  window.showRestartOverlay = showRestartOverlay;

}());
