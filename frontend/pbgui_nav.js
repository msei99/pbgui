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
 *        TOKEN, API_BASE, PBGUI_VERSION
 *
 * The 'current' value in PBGUI_NAV_CONFIG must match a 'page' key in NAV_GROUPS below.
 * The active nav group is highlighted automatically.
 *
 * Guide button: opens a page-local help overlay when the page exposes
 * `window.PBGUI_HELP_OPENER`; otherwise it navigates to the shared Help page.
 * About button: opens an in-page modal with version info and links.
 */
(function () {
  'use strict';

  /* ── config (read at runtime so global vars are already set) ── */
  function cfg() {
    var c = window.PBGUI_NAV_CONFIG || {};
    return {
      token:    c.token    !== undefined ? c.token    : (window.TOKEN    || ''),
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
      { page: 'system_db_tools',      icon: '&#128736;', label: 'DB Tools'          },
      { page: 'system_vps_manager_fastapi', icon: '&#128421;', label: 'VPS Manager' },
      { page: 'system_vps_monitor',   icon: '&#128223;', label: 'VPS Monitor'       },
      { page: 'system_logging',       icon: '&#128196;', label: 'Logging'           }
    ]},
    { id: 'information', label: 'Information', items: [
      { page: 'dashboards',           icon: '&#128202;', label: 'Dashboards'        },
      { page: 'info_coin_data',       icon: '&#129689;', label: 'Coin Data'         },
      { page: 'info_market_data_fastapi', icon: '&#128187;', label: 'Market Data' },
      { page: 'help',                 icon: '&#10067;',  label: 'Help'              }
    ]},
    { id: 'pbv7', label: 'PBv7', items: [
      { page: 'v7_run',               icon: '&#9654;',   label: 'Run'               },
      { page: 'v7_backtest',          icon: '&#9194;',   label: 'Backtest'          },
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
    ':root{--fs-xs:11px;--fs-sm:13px;--fs-base:14px;--fs-md:15px;--fs-lg:18px;--fs-xl:28px;--sp-xs:4px;--sp-sm:8px;--sp-md:12px;--sp-lg:20px;--btn-h:32px;}',
    'html,body{height:100%;}',
    'body{padding:0!important;overflow:hidden;}',

    '#topnav{display:flex;align-items:center;height:52px;background:#111827;',
    'border-bottom:1px solid #1e2736;flex-shrink:0;position:relative;z-index:200;',
    'padding:0 0.5rem;gap:0.25rem;user-select:none;}',

    '#nav-logo{display:flex;align-items:center;padding:0 0.75rem 0 0.25rem;',
    'flex-shrink:0;cursor:pointer;text-decoration:none;border-right:1px solid #1e2736;',
    'margin-right:0.25rem;height:100%;}',

    '.nav-group{position:relative;height:100%;display:flex;align-items:center;}',

    'button.nav-group-btn{display:flex;align-items:center;gap:0.3rem;padding:0.3rem 0.7rem;',
    'height:100%;background:transparent;border:none;border-radius:0;color:#94a3b8;font-size:var(--fs-base);',
    'font-weight:500;cursor:pointer;border-bottom:2px solid transparent;',
    'transition:color .15s,border-color .15s,background .15s;white-space:nowrap;}',
    'button.nav-group-btn:hover:not(:disabled){color:#e2e8f0;background:rgba(255,255,255,.04);transform:none;}',
    '.nav-group-btn.active{color:#63b3ed;border-bottom-color:#63b3ed;}',
    '.nav-group-btn.disabled{opacity:.45;cursor:not-allowed;}',
    '.nav-group-btn.disabled:hover{color:#94a3b8;background:transparent;border-bottom-color:transparent;}',

    '.nav-arrow{font-size:0.58rem;opacity:0.6;transition:transform .15s;}',
    '.nav-group.open .nav-arrow{transform:rotate(180deg);}',

    '.nav-dropdown{display:none;position:absolute;top:calc(100% + 1px);left:0;',
    'min-width:190px;background:#1a202c;border:1px solid #2d3748;',
    'border-radius:0 0 8px 8px;box-shadow:0 8px 24px rgba(0,0,0,.5);',
    'flex-direction:column;padding:0.3rem 0;z-index:300;}',
    '.nav-group.open .nav-dropdown{display:flex;}',
    'body.pbgui-help-open #topnav{z-index:3200;}',
    'body.pbgui-help-open #topnav .nav-group.open{z-index:3300;}',
    'body.pbgui-help-open #topnav .nav-dropdown{z-index:3301;}',

    '.nav-item{display:flex;align-items:center;gap:0.55rem;padding:0.42rem 1rem;',
    'color:#94a3b8;font-size:var(--fs-base);text-decoration:none;cursor:pointer;',
    'transition:background .1s,color .1s;white-space:nowrap;}',
    '.nav-item:hover{background:#243047;color:#e2e8f0;}',
    '.nav-item.current{color:#63b3ed;font-weight:600;background:rgba(99,179,237,.07);cursor:default;}',
    '.nav-item-icon{font-size:1rem;width:1.1rem;text-align:center;flex-shrink:0;}',

    '#nav-spacer{flex:1;}',

    '#nav-right{display:flex;align-items:center;gap:0.25rem;padding-right:0.5rem;}',
    '.nav-divider{width:1px;height:18px;background:#1e2736;margin:0 0.15rem;}',
    '.nav-action-btn{display:flex;align-items:center;gap:0.35rem;padding:0.3rem 0.75rem;',
    'border-radius:6px;background:transparent;border:1px solid transparent;',
    'color:#64748b;font-size:var(--fs-sm);font-weight:500;cursor:pointer;',
    'transition:all .15s;white-space:nowrap;height:32px;}',
    '.nav-action-btn:hover{background:rgba(255,255,255,.05);border-color:#2d3748;color:#e2e8f0;}',
    '.nav-action-btn.icon-only{justify-content:center;gap:0;padding:0;width:32px;min-width:32px;}',
    '.nav-action-btn.icon-only svg{width:16px;height:16px;display:block;stroke:currentColor;}',
    '.nav-action-btn.accent{color:#63b3ed;border-color:rgba(99,179,237,.25);background:rgba(99,179,237,.04);}',
    '.nav-action-btn.accent:hover{background:rgba(99,179,237,.12);border-color:#63b3ed;}',
    '.nav-action-btn.restart{color:#f59e0b;border-color:rgba(245,158,11,.3);background:rgba(245,158,11,.06);display:none;}',
    '.nav-action-btn.restart:hover{background:rgba(245,158,11,.15);border-color:#f59e0b;}',
    '.nav-restart-dot{display:inline-block;width:7px;height:7px;border-radius:50%;',
    'background:#f59e0b;margin-right:2px;animation:nav-blink 1.4s ease-in-out infinite;}',
    '@keyframes nav-blink{0%,100%{opacity:1;}50%{opacity:.3;}}',
    '.nav-action-btn.notify{color:#64748b;}',
    '.nav-action-btn.notify:hover{color:#e2e8f0;}',
    '.nav-action-btn.alerts{color:#f59e0b;border-color:rgba(245,158,11,.24);background:rgba(245,158,11,.05);display:inline-flex;}',
    '.nav-action-btn.alerts:hover{color:#fcd34d;border-color:rgba(245,158,11,.45);background:rgba(245,158,11,.12);}',
    '.nav-badge{display:inline-flex;align-items:center;justify-content:center;min-width:18px;height:18px;padding:0 0.35rem;border-radius:999px;font-size:10px;font-weight:700;line-height:1;background:#243047;color:#cbd5e1;}',
    '.nav-badge.has-new{background:#b91c1c;color:#fff;}',
    '.nav-action-btn.logout{color:#94a3b8;}',
    '.nav-action-btn.logout:hover{color:#f8fafc;}',

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
    '#pbgui-alert-ovl{display:none;position:fixed;inset:0;background:rgba(0,0,0,.72);z-index:3060;backdrop-filter:blur(2px);}',
    '#pbgui-alert-ovl.visible{display:flex;}',
    '#pbgui-alert-box{position:absolute;background:#131b2b;border:1px solid #2d3748;border-radius:14px;box-shadow:0 20px 70px rgba(0,0,0,.9);overflow:hidden;width:min(880px,94vw);max-width:94vw;height:min(640px,78vh);max-height:90vh;min-width:320px;min-height:220px;display:flex;flex-direction:column;}',
    '#pbgui-alert-body{display:flex;flex-direction:column;gap:0.8rem;padding:1rem 1.1rem 1.1rem;min-height:0;}',
    '#pbgui-alert-toolbar{display:flex;align-items:center;justify-content:space-between;gap:0.75rem;flex-wrap:wrap;}',
    '#pbgui-alert-summary{font-size:var(--fs-sm);color:#94a3b8;}',
    '#pbgui-alert-list{display:flex;flex-direction:column;gap:0.65rem;overflow:auto;min-height:0;padding-right:0.2rem;}',
    '.pbgui-alert-section-sep{height:1px;background:#1e2736;margin:0.35rem 0 0.2rem;}',
    '.pbgui-alert-history-title{font-size:var(--fs-xs);letter-spacing:0.05em;text-transform:uppercase;color:#64748b;margin-top:0.2rem;}',
    '.pbgui-alert-item{border:1px solid #243047;border-radius:10px;background:#0f1724;padding:0.8rem 0.9rem;display:grid;gap:0.45rem;}',
    '.pbgui-alert-item.new{border-color:rgba(239,68,68,.45);}',
    '.pbgui-alert-item.history{background:#0d1420;border-color:#1a2537;}',
    '.pbgui-alert-head{display:flex;align-items:flex-start;justify-content:space-between;gap:0.75rem;}',
    '.pbgui-alert-title{font-size:var(--fs-base);font-weight:600;color:#e2e8f0;}',
    '.pbgui-alert-meta{display:flex;align-items:center;gap:0.45rem;flex-wrap:wrap;font-size:var(--fs-xs);color:#64748b;}',
    '.pbgui-alert-pill{display:inline-flex;align-items:center;justify-content:center;padding:0.18rem 0.45rem;border-radius:999px;font-size:10px;font-weight:700;letter-spacing:0.02em;text-transform:uppercase;}',
    '.pbgui-alert-pill.new{background:rgba(239,68,68,.18);color:#fca5a5;}',
    '.pbgui-alert-pill.ack{background:rgba(99,179,237,.15);color:#93c5fd;}',
    '.pbgui-alert-pill.kind{background:rgba(148,163,184,.12);color:#cbd5e1;}',
    '.pbgui-alert-details{font-size:var(--fs-sm);line-height:1.45;color:#94a3b8;}',
    '.pbgui-alert-actions{display:flex;align-items:center;justify-content:flex-end;gap:0.5rem;}',
    '.pbgui-alert-empty{padding:1rem 0.2rem;color:#64748b;font-size:var(--fs-sm);text-align:center;}',
    '.pbgui-alert-link{display:inline-flex;align-items:center;justify-content:center;height:var(--btn-h);padding:0 var(--sp-md);border-radius:8px;border:1px solid rgba(99,179,237,.25);font-size:var(--fs-base);font-weight:600;color:#e2e8f0;background:rgba(99,179,237,.08);cursor:pointer;transition:background .15s,border-color .15s,color .15s;text-decoration:none;}',
    '.pbgui-alert-link:hover{background:rgba(99,179,237,.16);border-color:#63b3ed;}',
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

    /* shared confirm overlay */
    '#pbgui-confirm-ovl{display:none;position:fixed;inset:0;background:rgba(0,0,0,.72);',
    'z-index:7000;align-items:center;justify-content:center;backdrop-filter:blur(2px);}',
    '#pbgui-confirm-ovl.visible{display:flex;}',
    '#pbgui-confirm-box{background:#131b2b;border:1px solid #2d3748;border-radius:14px;',
    'box-shadow:0 20px 70px rgba(0,0,0,.9);overflow:hidden;width:min(460px,92vw);}',
    '#pbgui-confirm-body{display:grid;gap:var(--sp-md);padding:var(--sp-lg);}',
    '#pbgui-confirm-msg{font-size:var(--fs-base);line-height:1.5;color:#e2e8f0;}',
    '#pbgui-confirm-detail{font-size:var(--fs-sm);line-height:1.45;color:#94a3b8;}',
    '#pbgui-confirm-actions{display:flex;justify-content:flex-end;gap:var(--sp-sm);flex-wrap:wrap;}',
    '.pbgui-modal-btn{display:inline-flex;align-items:center;justify-content:center;height:var(--btn-h);',
    'padding:0 var(--sp-md);border-radius:8px;border:1px solid transparent;font-size:var(--fs-base);',
    'font-weight:600;cursor:pointer;transition:background .15s,border-color .15s,color .15s;}',
    '.pbgui-modal-btn.secondary{background:rgba(99,179,237,.08);border-color:rgba(99,179,237,.25);color:#e2e8f0;}',
    '.pbgui-modal-btn.secondary:hover{background:rgba(99,179,237,.16);border-color:#63b3ed;}',
    '.pbgui-modal-btn.primary{background:#63b3ed;border-color:#63b3ed;color:#0b1220;}',
    '.pbgui-modal-btn.primary:hover{background:#7cc4f5;}',

    /* shared help overlay chrome */
    '#help-ovl.is-maximized{max-width:none;max-height:none;resize:none;}',
    '#help-ovl.is-maximized #help-drag-handle{cursor:default;pointer-events:none;}',
    '.ovl-tool{display:inline-flex;align-items:center;justify-content:center;width:28px;height:28px;',
    'background:transparent;border:1px solid transparent;border-radius:4px;color:#64748b;',
    'cursor:pointer;font-size:var(--fs-md);line-height:1;padding:0;transition:color .12s,background .12s,border-color .12s;}',
    '.ovl-tool[aria-pressed="true"]{color:#e2e8f0;border-color:rgba(148,163,184,.2);background:rgba(255,255,255,.06);}',
    '.ovl-tool:hover{color:#e2e8f0;border-color:rgba(148,163,184,.18);background:rgba(255,255,255,.06);}',

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
    var canNavigate = !!c.token;
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
      html += '<button class="nav-group-btn' + (isActive ? ' active' : '') + (canNavigate ? '' : ' disabled') + '" data-group="' + group.id + '"' + (canNavigate ? '' : ' disabled aria-disabled="true"') + '>';
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
          + '<span class="nav-divider" aria-hidden="true"></span>'
          + '<button class="nav-action-btn alerts" id="pbgui-alert-btn" title="VPSMonitor alerts">&#128737; <span class="nav-badge" id="pbgui-alert-badge">0/0</span></button>'
          + '<button class="nav-action-btn accent" id="pbgui-guide-btn">&#128218; Guide</button>'
          + '<button class="nav-action-btn" id="pbgui-about-btn">&#x2139;&#xFE0F; About</button>'
          + '<button class="nav-action-btn icon-only logout" id="pbgui-logout-btn" title="Logout" aria-label="Logout">'
          +   '<svg viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg" aria-hidden="true">'
          +     '<path d="M4 4.75C4 4.33579 4.33579 4 4.75 4H13.25C13.6642 4 14 4.33579 14 4.75V19.25C14 19.6642 13.6642 20 13.25 20H4.75C4.33579 20 4 19.6642 4 19.25V4.75Z" stroke-width="1.7" stroke-linejoin="round"/>'
          +     '<path d="M14 6.25L18.75 8V16L14 17.75V6.25Z" stroke-width="1.7" stroke-linejoin="round"/>'
          +     '<circle cx="15.8" cy="12" r="0.85" fill="currentColor" stroke="none"/>'
          +   '</svg>'
          + '</button>'
          + '</div>';

    nav.innerHTML = html;
  }

  /* ════════════════════════════════════
     NOTIFICATION LOG PANEL
     ════════════════════════════════════ */
  var _notifyViewer = null;
  var _navAlerts = { items: [], history: [], summary: { new_count: 0, ack_count: 0, total_active: 0 } };
  var _alertsTimer = null;
  var _navConfirmResolve = null;
  var _navConfirmReturnFocus = null;

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

  function buildAlertOverlay() {
    if (document.getElementById('pbgui-alert-ovl')) return;
    var wrapper = document.createElement('div');
    wrapper.innerHTML = ''
      + '<div id="pbgui-alert-ovl" aria-hidden="true">'
      +   '<div id="pbgui-alert-box" role="dialog" aria-modal="true" aria-labelledby="pbgui-alert-title">'
      +     '<div class="pnr pnr-n" data-dir="n"></div>'
      +     '<div class="pnr pnr-s" data-dir="s"></div>'
      +     '<div class="pnr pnr-w" data-dir="w"></div>'
      +     '<div class="pnr pnr-e" data-dir="e"></div>'
      +     '<div class="pnr pnr-nw" data-dir="nw"></div>'
      +     '<div class="pnr pnr-ne" data-dir="ne"></div>'
      +     '<div class="pnr pnr-sw" data-dir="sw"></div>'
      +     '<div class="pnr pnr-se" data-dir="se"></div>'
      +     '<div class="pbgui-ovl-header" id="pbgui-alert-hdr">'
      +       '<span class="pbgui-ovl-title" id="pbgui-alert-title">VPSMonitor Alerts</span>'
      +       '<button class="pbgui-ovl-close" id="pbgui-alert-close">&#x2715;</button>'
      +     '</div>'
      +     '<div id="pbgui-alert-body">'
      +       '<div id="pbgui-alert-toolbar">'
      +         '<div id="pbgui-alert-summary"></div>'
      +         '<div style="display:flex;gap:0.5rem;flex-wrap:wrap;">'
      +           '<button type="button" class="pbgui-modal-btn secondary" id="pbgui-alert-ack-all">Ack all</button>'
      +           '<a class="pbgui-alert-link" id="pbgui-alert-open-monitor" href="#">Open VPS Monitor</a>'
      +         '</div>'
      +       '</div>'
      +       '<div id="pbgui-alert-list"></div>'
      +     '</div>'
      +   '</div>'
      + '</div>';
    document.body.appendChild(wrapper.firstChild);
    var ovl = document.getElementById('pbgui-alert-ovl');
    var box = document.getElementById('pbgui-alert-box');
    var closeBtn = document.getElementById('pbgui-alert-close');
    var ackAllBtn = document.getElementById('pbgui-alert-ack-all');
    if (closeBtn) closeBtn.addEventListener('click', closeAlertOverlay);
    if (ackAllBtn) ackAllBtn.addEventListener('click', function () { ackAllAlerts(); });
    if (box) {
      _bindPanelDrag(box, 'pbgui-alert-hdr', ['pbgui-alert-close']);
      _bindPanelResize(box, 320, 220);
    }
  }

  function closeAlertOverlay() {
    var ovl = document.getElementById('pbgui-alert-ovl');
    if (!ovl) return;
    ovl.classList.remove('visible');
    ovl.setAttribute('aria-hidden', 'true');
  }

  function openAlertOverlay() {
    buildAlertOverlay();
    renderAlertOverlay();
    var ovl = document.getElementById('pbgui-alert-ovl');
    var box = document.getElementById('pbgui-alert-box');
    if (!ovl) return;
    if (box && !box.dataset.positioned) {
      var width = Math.min(Math.max(Math.round(window.innerWidth * 0.78), 320), 880);
      var height = Math.min(Math.max(Math.round(window.innerHeight * 0.78), 220), 640);
      box.style.width = width + 'px';
      box.style.height = height + 'px';
      box.style.left = Math.max(16, Math.round((window.innerWidth - width) / 2)) + 'px';
      box.style.top = Math.max(16, Math.round((window.innerHeight - height) / 2)) + 'px';
      box.dataset.positioned = 'true';
    }
    ovl.classList.add('visible');
    ovl.setAttribute('aria-hidden', 'false');
  }

  function _alertKindLabel(kind) {
    return ({ offline: 'Offline', service: 'Service', system: 'System', instance: 'Instance' })[String(kind || '')] || 'Alert';
  }

  function _formatTs(ts) {
    var value = Number(ts || 0);
    if (!value) return 'n/a';
    try { return new Date(value * 1000).toLocaleString(); } catch (_) { return 'n/a'; }
  }

  function renderAlertOverlay() {
    var summaryEl = document.getElementById('pbgui-alert-summary');
    var listEl = document.getElementById('pbgui-alert-list');
    var openMonitor = document.getElementById('pbgui-alert-open-monitor');
    if (openMonitor) {
      openMonitor.onclick = function (e) {
        e.preventDefault();
        closeAlertOverlay();
        var c = cfg();
        var apiOrigin = '';
        if (c.apiBase) {
          var m = c.apiBase.match(/^(https?:\/\/[^/]+)/);
          if (m) apiOrigin = m[1];
        }
        if (!apiOrigin) apiOrigin = window.location.origin;
        var url = apiOrigin + '/api/vps/main_page?token=' + encodeURIComponent(c.token || '');
        window.location.href = url;
      };
    }
    if (summaryEl) {
      var s = _navAlerts.summary || { new_count: 0, ack_count: 0, total_active: 0 };
      summaryEl.textContent = s.total_active + ' active alerts, ' + s.new_count + ' new, ' + s.ack_count + ' acknowledged';
    }
    if (!listEl) return;
    var items = Array.isArray(_navAlerts.items) ? _navAlerts.items : [];
    var history = Array.isArray(_navAlerts.history) ? _navAlerts.history : [];
    var html = '';
    if (!items.length) {
      html += '<div class="pbgui-alert-empty">No active VPSMonitor alerts.</div>';
    }
    items.forEach(function (item) {
      var ack = !!item.acknowledged;
      var host = esc(item.host || '');
      var title = esc(item.summary || 'Alert');
      var details = esc(item.details || '');
      html += '<div class="pbgui-alert-item' + (ack ? '' : ' new') + '">';
      html +=   '<div class="pbgui-alert-head">';
      html +=     '<div>';
      html +=       '<div class="pbgui-alert-title">' + title + '</div>';
      html +=       '<div class="pbgui-alert-meta">';
      html +=         '<span class="pbgui-alert-pill kind">' + esc(_alertKindLabel(item.kind)) + '</span>';
      html +=         '<span class="pbgui-alert-pill ' + (ack ? 'ack' : 'new') + '">' + (ack ? 'ACK' : 'NEW') + '</span>';
      html +=         '<span>' + host + (item.name ? ' / ' + esc(item.name) : '') + '</span>';
      html +=         '<span>Seen ' + esc(_formatTs(item.first_seen_ts)) + '</span>';
      html +=       '</div>';
      html +=     '</div>';
      if (!ack) {
        html +=   '<div class="pbgui-alert-actions"><button type="button" class="pbgui-modal-btn secondary" data-alert-ack="' + escAttr(item.id) + '">Ack</button></div>';
      }
      html +=   '</div>';
      html +=   '<div class="pbgui-alert-details">' + details + '</div>';
      html += '</div>';
    });
    if (history.length) {
      html += '<div class="pbgui-alert-section-sep"></div>';
      html += '<div class="pbgui-alert-history-title">History</div>';
      history.forEach(function (item) {
        var host = esc(item.host || '');
        var title = esc(item.summary || 'Alert');
        var details = esc(item.details || '');
        var seenTs = esc(_formatTs(item.first_seen_ts));
        var resolvedTs = esc(_formatTs(item.resolved_ts || item.last_seen_ts));
        html += '<div class="pbgui-alert-item history">';
        html +=   '<div class="pbgui-alert-head">';
        html +=     '<div>';
        html +=       '<div class="pbgui-alert-title">' + title + '</div>';
        html +=       '<div class="pbgui-alert-meta">';
        html +=         '<span class="pbgui-alert-pill kind">' + esc(_alertKindLabel(item.kind)) + '</span>';
        html +=         '<span class="pbgui-alert-pill ack">' + (item.acknowledged ? 'ACK' : 'DONE') + '</span>';
        html +=         '<span>' + host + (item.name ? ' / ' + esc(item.name) : '') + '</span>';
        html +=         '<span>Seen ' + seenTs + '</span>';
        html +=         '<span>Resolved ' + resolvedTs + '</span>';
        html +=       '</div>';
        html +=     '</div>';
        html +=   '</div>';
        html +=   '<div class="pbgui-alert-details">' + details + '</div>';
        html += '</div>';
      });
    }
    listEl.innerHTML = html;
    listEl.querySelectorAll('[data-alert-ack]').forEach(function (btn) {
      btn.addEventListener('click', function () {
        ackAlert(btn.getAttribute('data-alert-ack') || '');
      });
    });
  }

  function updateAlertButton() {
    var btn = document.getElementById('pbgui-alert-btn');
    var badge = document.getElementById('pbgui-alert-badge');
    if (!btn || !badge) return;
    var summary = _navAlerts.summary || { new_count: 0, ack_count: 0, total_active: 0 };
    badge.textContent = String(summary.new_count || 0) + '/' + String(summary.ack_count || 0);
    badge.classList.toggle('has-new', !!summary.new_count);
  }

  function fetchAlerts() {
    var c = cfg();
    if (!c.token) return;
    var apiOrigin = '';
    if (c.apiBase) {
      var m = c.apiBase.match(/^(https?:\/\/[^/]+)/);
      if (m) apiOrigin = m[1];
    }
    if (!apiOrigin) apiOrigin = window.location.origin;
    fetch(apiOrigin + '/api/vps/alerts?token=' + encodeURIComponent(c.token), { cache: 'no-store' })
      .then(function (resp) {
        if (!resp.ok) throw new Error('alerts failed');
        return resp.json();
      })
      .then(function (data) {
        _navAlerts = data || { items: [], history: [], summary: { new_count: 0, ack_count: 0, total_active: 0 } };
        updateAlertButton();
        var ovl = document.getElementById('pbgui-alert-ovl');
        if (ovl && ovl.classList.contains('visible')) renderAlertOverlay();
      })
      .catch(function () {});
  }

  function scheduleAlerts() {
    clearInterval(_alertsTimer);
    _alertsTimer = setInterval(fetchAlerts, 10000);
  }

  function ackAlert(alertId) {
    var c = cfg();
    var apiOrigin = '';
    if (c.apiBase) {
      var m = c.apiBase.match(/^(https?:\/\/[^/]+)/);
      if (m) apiOrigin = m[1];
    }
    if (!apiOrigin) apiOrigin = window.location.origin;
    fetch(apiOrigin + '/api/vps/alerts/ack?token=' + encodeURIComponent(c.token || ''), {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ id: alertId })
    })
      .then(function (resp) {
        if (!resp.ok) throw new Error('ack failed');
        return resp.json();
      })
      .then(function (data) {
        _navAlerts = data || _navAlerts;
        updateAlertButton();
        renderAlertOverlay();
      })
      .catch(function () {});
  }

  function ackAllAlerts() {
    var c = cfg();
    var apiOrigin = '';
    if (c.apiBase) {
      var m = c.apiBase.match(/^(https?:\/\/[^/]+)/);
      if (m) apiOrigin = m[1];
    }
    if (!apiOrigin) apiOrigin = window.location.origin;
    fetch(apiOrigin + '/api/vps/alerts/ack-all?token=' + encodeURIComponent(c.token || ''), { method: 'POST' })
      .then(function (resp) {
        if (!resp.ok) throw new Error('ack-all failed');
        return resp.json();
      })
      .then(function (data) {
        _navAlerts = data || _navAlerts;
        updateAlertButton();
        renderAlertOverlay();
      })
      .catch(function () {});
  }

  function _bindPanelDrag(panel, headerId, closeIds) {
    var hdr = document.getElementById(headerId);
    if (!hdr || hdr._dragBound) return;
    hdr._dragBound = true;
    var ignoreIds = Array.isArray(closeIds) ? closeIds : [];
    hdr.addEventListener('mousedown', function(e) {
      if (ignoreIds.indexOf(e.target.id) >= 0) return;
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

  function _bindPanelResize(panel, minWidth, minHeight) {
    if (panel._resizeBound) return;
    panel._resizeBound = true;
    var minW = Number(minWidth || 240);
    var minH = Number(minHeight || 150);
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
          if (nW < minW) { if (dir.indexOf('w') >= 0) nL = sL + sW - minW; nW = minW; }
          if (nH < minH) { if (dir.indexOf('n') >= 0) nT = sT + sH - minH; nH = minH; }
          nL = Math.max(0, Math.min(nL, window.innerWidth - minW));
          nT = Math.max(0, Math.min(nT, window.innerHeight - minH));
          nW = Math.min(nW, window.innerWidth - nL);
          nH = Math.min(nH, window.innerHeight - nT);
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

  function _bindNotifyDrag(panel) {
    _bindPanelDrag(panel, 'pbgui-notify-hdr', ['pbgui-notify-close']);
  }

  function _bindNotifyResize(panel) {
    _bindPanelResize(panel, 240, 150);
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

  function buildConfirmOverlay() {
    if (document.getElementById('pbgui-confirm-ovl')) return;
    var wrapper = document.createElement('div');
    wrapper.innerHTML = ''
      + '<div id="pbgui-confirm-ovl" aria-hidden="true">'
      +   '<div id="pbgui-confirm-box" role="dialog" aria-modal="true" aria-labelledby="pbgui-confirm-title">'
      +     '<div class="pbgui-ovl-header">'
      +       '<span class="pbgui-ovl-title" id="pbgui-confirm-title">Confirm action</span>'
      +       '<button class="pbgui-ovl-close" id="pbgui-confirm-close">&#x2715;</button>'
      +     '</div>'
      +     '<div id="pbgui-confirm-body">'
      +       '<div id="pbgui-confirm-msg"></div>'
      +       '<div id="pbgui-confirm-detail" hidden></div>'
      +       '<div id="pbgui-confirm-actions">'
      +         '<button type="button" class="pbgui-modal-btn secondary" id="pbgui-confirm-cancel">Cancel</button>'
      +         '<button type="button" class="pbgui-modal-btn primary" id="pbgui-confirm-accept">Confirm</button>'
      +       '</div>'
      +     '</div>'
      +   '</div>'
      + '</div>';
    document.body.appendChild(wrapper.firstChild);

    var overlay = document.getElementById('pbgui-confirm-ovl');
    var closeBtn = document.getElementById('pbgui-confirm-close');
    var cancelBtn = document.getElementById('pbgui-confirm-cancel');
    var acceptBtn = document.getElementById('pbgui-confirm-accept');
    if (closeBtn) closeBtn.addEventListener('click', function () { closeNavConfirm(false); });
    if (cancelBtn) cancelBtn.addEventListener('click', function () { closeNavConfirm(false); });
    if (acceptBtn) acceptBtn.addEventListener('click', function () { closeNavConfirm(true); });
  }

  function closeNavConfirm(confirmed) {
    var overlay = document.getElementById('pbgui-confirm-ovl');
    if (overlay) {
      overlay.classList.remove('visible');
      overlay.setAttribute('aria-hidden', 'true');
    }
    var resolver = _navConfirmResolve;
    var returnFocus = _navConfirmReturnFocus;
    _navConfirmResolve = null;
    _navConfirmReturnFocus = null;
    if (returnFocus && typeof returnFocus.focus === 'function') {
      try { returnFocus.focus(); } catch (_) {}
    }
    if (typeof resolver === 'function') resolver(Boolean(confirmed));
  }

  function showNavConfirm(options) {
    options = options || {};
    buildConfirmOverlay();
    var overlay = document.getElementById('pbgui-confirm-ovl');
    var title = document.getElementById('pbgui-confirm-title');
    var message = document.getElementById('pbgui-confirm-msg');
    var detail = document.getElementById('pbgui-confirm-detail');
    var cancelBtn = document.getElementById('pbgui-confirm-cancel');
    var acceptBtn = document.getElementById('pbgui-confirm-accept');
    if (!overlay || !title || !message || !detail || !cancelBtn || !acceptBtn) {
      return Promise.resolve(window.confirm(String(options.message || 'Are you sure?')));
    }

    if (typeof _navConfirmResolve === 'function') {
      var previousResolve = _navConfirmResolve;
      _navConfirmResolve = null;
      previousResolve(false);
    }

    title.textContent = String(options.title || 'Confirm action');
    message.textContent = String(options.message || 'Are you sure?');
    acceptBtn.textContent = String(options.confirmText || 'Confirm');
    cancelBtn.textContent = String(options.cancelText || 'Cancel');
    var detailText = String(options.detail || '').trim();
    detail.textContent = detailText;
    detail.hidden = !detailText;
    _navConfirmReturnFocus = document.activeElement;

    return new Promise(function (resolve) {
      _navConfirmResolve = resolve;
      overlay.classList.add('visible');
      overlay.setAttribute('aria-hidden', 'false');
      acceptBtn.focus();
    });
  }

  /* ════════════════════════════════════
     FASTAPI DIRECT ROUTES
     Pages served directly by FastAPI.
     Key = nav page id, value = path under the API origin.
     ════════════════════════════════════ */
  var FASTAPI_PAGES = {
    '/':                 '/api/auth/main_page',
    'dashboards':        '/api/dashboard/main_page',
    'info_coin_data':    '/api/coin-data/main_page',
    'info_market_data_fastapi': '/api/market-data/main_page',
    'system_api_keys':   '/api/api-keys/main_page',
    'system_vps_manager_fastapi': '/api/vps-manager/main_page',
    'system_logging':     '/api/logging/main_page',
    'system_vps_monitor': '/api/vps/main_page',
    'system_services':    '/api/services/main_page',
    'system_db_tools':    '/api/db-tools/main_page',
    'help':               '/app/help.html',
    'v7_run':             '/api/v7/main_page',
    'v7_backtest':        '/api/backtest-v7/main_page',
    'v7_optimize':        '/api/optimize-v7/main_page',
    'v7_pareto_explorer': '/api/pareto-explorer/main_page',
    'v7_strategy_explorer': '/api/strategy-explorer/main_page',
    'v7_balance_calc':    '/api/balance-calc/main_page'
  };

  function syncHelpOverlayState() {
    var legacyHelpOvl = document.getElementById('help-ovl');
    var sharedHelpOvl = document.getElementById('pbgui-shared-help-ovl');
    var isVisible = !!(
      (legacyHelpOvl && legacyHelpOvl.classList.contains('visible')) ||
      (sharedHelpOvl && sharedHelpOvl.classList.contains('visible'))
    );
    document.body.classList.toggle('pbgui-help-open', isVisible);
  }

  function ensureSharedHelpOverlay() {
    var helpOvl = document.getElementById('help-ovl');
    if (!helpOvl) {
      syncHelpOverlayState();
      return null;
    }

    var actions = helpOvl.querySelector('.ovl-header-actions');
    var closeBtn = document.getElementById('help-close') || helpOvl.querySelector('.ovl-close');
    var maxBtn = document.getElementById('help-maximize') || helpOvl.querySelector('.ovl-tool[data-role="maximize"]');

    if (actions && !maxBtn) {
      maxBtn = document.createElement('button');
      maxBtn.type = 'button';
      maxBtn.id = 'help-maximize';
      maxBtn.className = 'ovl-tool';
      maxBtn.setAttribute('data-role', 'maximize');
      maxBtn.setAttribute('aria-pressed', 'false');
      maxBtn.setAttribute('title', 'Fit to browser window');
      maxBtn.textContent = '⛶';
      if (closeBtn && closeBtn.parentNode === actions) actions.insertBefore(maxBtn, closeBtn);
      else actions.appendChild(maxBtn);
    }

    function syncMaximizeButton() {
      if (!maxBtn) return;
      var isMaximized = helpOvl.classList.contains('is-maximized');
      maxBtn.setAttribute('aria-pressed', isMaximized ? 'true' : 'false');
      maxBtn.setAttribute('title', isMaximized ? 'Restore window size' : 'Fit to browser window');
      maxBtn.textContent = isMaximized ? '❐' : '⛶';
    }

    function setMaximized(nextValue) {
      var shouldMaximize = !!nextValue;
      var isMaximized = helpOvl.classList.contains('is-maximized');
      if (shouldMaximize === isMaximized) {
        syncMaximizeButton();
        return;
      }
      if (shouldMaximize) {
        helpOvl._pbguiHelpRestoreBounds = {
          left: helpOvl.style.left || '',
          top: helpOvl.style.top || '',
          right: helpOvl.style.right || '',
          bottom: helpOvl.style.bottom || '',
          width: helpOvl.style.width || '',
          height: helpOvl.style.height || '',
          transform: helpOvl.style.transform || ''
        };
        helpOvl.classList.add('is-maximized');
        if (window.innerWidth <= 720) {
          helpOvl.style.left = '7px';
          helpOvl.style.top = '59px';
          helpOvl.style.right = '7px';
          helpOvl.style.bottom = '7px';
        } else {
          helpOvl.style.left = '12px';
          helpOvl.style.top = '64px';
          helpOvl.style.right = '12px';
          helpOvl.style.bottom = '12px';
        }
        helpOvl.style.width = 'auto';
        helpOvl.style.height = 'auto';
        helpOvl.style.transform = 'none';
      } else {
        helpOvl.classList.remove('is-maximized');
        var saved = helpOvl._pbguiHelpRestoreBounds || {};
        helpOvl.style.left = saved.left || '';
        helpOvl.style.top = saved.top || '';
        helpOvl.style.right = saved.right || '';
        helpOvl.style.bottom = saved.bottom || '';
        helpOvl.style.width = saved.width || '';
        helpOvl.style.height = saved.height || '';
        helpOvl.style.transform = saved.transform || '';
      }
      syncMaximizeButton();
    }

    if (maxBtn && !maxBtn.dataset.pbguiHelpMaxBound) {
      maxBtn.dataset.pbguiHelpMaxBound = '1';
      maxBtn.addEventListener('click', function (event) {
        event.preventDefault();
        setMaximized(!helpOvl.classList.contains('is-maximized'));
      });
    }

    if (!helpOvl.dataset.pbguiHelpStateObserved) {
      helpOvl.dataset.pbguiHelpStateObserved = '1';
      new MutationObserver(function () {
        syncHelpOverlayState();
      }).observe(helpOvl, { attributes: true, attributeFilter: ['class'] });
    }

    syncMaximizeButton();
    syncHelpOverlayState();
    return helpOvl;
  }

  /* ════════════════════════════════════
     EVENT HANDLERS
     ════════════════════════════════════ */
  function setupHandlers() {
    var c = cfg();
    var TOKEN   = c.token;

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

      if (!TOKEN && page !== '/') {
        return;
      }

      /* Direct FastAPI page */
      if (FASTAPI_PAGES[page] && apiOrigin) {
        var faUrl = apiOrigin + FASTAPI_PAGES[page]
                  + '?token=' + encodeURIComponent(TOKEN);
        window.location.href = faUrl;
        return;
      }

      console.warn('[pbgui_nav] Unknown PBGui page "' + page + '".');
      var msg = document.createElement('div');
      msg.style.cssText = 'position:fixed;top:60px;left:50%;transform:translateX(-50%);z-index:9999;background:#ef444480;color:#fff;padding:.6rem 1.2rem;border-radius:8px;font-size:.85rem;pointer-events:none;';
      msg.textContent = 'Navigation unavailable — page is not registered.';
      document.body.appendChild(msg);
      setTimeout(function() { msg.remove(); }, 4000);
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

    ensureSharedHelpOverlay();

    /* Guide button → open page-local help when available, else navigate to Help page */
    var guideBtn = document.getElementById('pbgui-guide-btn');
    if (guideBtn) guideBtn.addEventListener('click', function () {
      var opener = window.PBGUI_HELP_OPENER;
      if (typeof opener === 'function') {
        ensureSharedHelpOverlay();
        opener();
        ensureSharedHelpOverlay();
        return;
      }
      navTo('help');
    });

    /* Notify button → open inline floating log panel */
    var notifyBtn = document.getElementById('pbgui-notify-btn');
    if (notifyBtn) notifyBtn.addEventListener('click', function () { toggleNotifyPanel(); });
    var notifyClose = document.getElementById('pbgui-notify-close');
    if (notifyClose) notifyClose.addEventListener('click', function () { closeNotifyPanel(); });
    buildAlertOverlay();
    var alertBtn = document.getElementById('pbgui-alert-btn');
    if (alertBtn) alertBtn.addEventListener('click', function () { openAlertOverlay(); });
    fetchAlerts();
    scheduleAlerts();

    /* About button → show overlay */
    var aboutBtn = document.getElementById('pbgui-about-btn');
    var aboutOvl = document.getElementById('pbgui-about-ovl');
    var aboutClose = document.getElementById('pbgui-about-close');
    if (aboutBtn && aboutOvl) {
      aboutBtn.addEventListener('click', function () { aboutOvl.classList.add('visible'); });
      if (aboutClose) aboutClose.addEventListener('click', function () { aboutOvl.classList.remove('visible'); });
    }

    var logoutBtn = document.getElementById('pbgui-logout-btn');
    if (logoutBtn) {
      logoutBtn.style.display = TOKEN ? 'inline-flex' : 'none';
      logoutBtn.addEventListener('click', function () { performLogout(); });
    }

    /* Esc key closes about overlay */
    document.addEventListener('keydown', function (e) {
      var confirmOvl = document.getElementById('pbgui-confirm-ovl');
      if (e.key === 'Escape') {
        if (confirmOvl && confirmOvl.classList.contains('visible')) {
          closeNavConfirm(false);
          return;
        }
        var alertOvl = document.getElementById('pbgui-alert-ovl');
        if (alertOvl && alertOvl.classList.contains('visible')) {
          closeAlertOverlay();
          return;
        }
        if (aboutOvl) aboutOvl.classList.remove('visible');
      }
      if (e.key === 'Enter' && confirmOvl && confirmOvl.classList.contains('visible')) {
        if (e.target && e.target.id === 'pbgui-confirm-cancel') return;
        e.preventDefault();
        closeNavConfirm(true);
      }
    });

    /* Restart button */
    var restartBtn = document.getElementById('pbgui-restart-btn');
    if (restartBtn) {
      restartBtn.addEventListener('click', function () {
        var blocked = restartBtn.getAttribute('data-restart-blocked') === '1';
        var blockReason = restartBtn.getAttribute('data-restart-block-reason') || '';
        if (blocked) {
          showNavConfirm({
            title: 'Restart blocked',
            message: 'The PBGui API server cannot restart while VPS tasks are still running.',
            detail: blockReason || 'Wait until the active VPS task finishes or is marked interrupted.',
            confirmText: 'OK',
            cancelText: '',
            hideCancel: true
          });
          return;
        }
        showNavConfirm({
          title: 'Restart API server',
          message: 'Restart the PBGui API server now?',
          detail: 'The page will reload automatically.',
          confirmText: 'Restart'
        }).then(function (confirmed) {
          if (!confirmed) return;
          var c2 = cfg();
          var origin2 = '';
          if (c2.apiBase) { var m2 = c2.apiBase.match(/^(https?:\/\/[^/]+)/); if (m2) origin2 = m2[1]; }
          if (!origin2) origin2 = window.location.origin;
          restartBtn.disabled = true;
          restartBtn.classList.add('disabled');
          restartBtn.innerHTML = '<span class="nav-restart-dot"></span>Restarting...';
          fetch(origin2 + '/api/server-restart', {
            method: 'POST',
            headers: { 'Authorization': 'Bearer ' + c2.token, 'Content-Type': 'application/json' },
            body: JSON.stringify({ token: c2.token })
          }).then(function(resp) {
            if (!resp.ok) {
              return resp.json().catch(function () { return {}; }).then(function (data) {
                var detail = (data && data.detail) ? String(data.detail) : 'Restart failed.';
                throw new Error(detail);
              });
            }
            showRestartOverlay(origin2, c2.token);
          }).catch(function(err) {
            restartBtn.disabled = false;
            restartBtn.classList.remove('disabled');
            restartBtn.innerHTML = '<span class="nav-restart-dot"></span>Restart';
            fetchRestartStatus(c2.token, origin2);
            showNavConfirm({
              title: 'Restart failed',
              message: 'The PBGui API server restart request was rejected.',
              detail: err && err.message ? err.message : 'Restart failed.',
              confirmText: 'OK',
              cancelText: '',
              hideCancel: true
            });
          });
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

  function updateRestartButtonState(state) {
    var btn = document.getElementById('pbgui-restart-btn');
    if (!btn) return;
    var visible = !!(state && state.needs_restart);
    var blocked = !!(state && state.restart_blocked);
    var reason = state && state.restart_block_reason ? String(state.restart_block_reason) : '';
    btn.style.display = visible ? 'flex' : 'none';
    btn.setAttribute('data-restart-blocked', blocked ? '1' : '0');
    btn.setAttribute('data-restart-block-reason', reason);
    btn.disabled = false;
    btn.setAttribute('aria-disabled', blocked ? 'true' : 'false');
    btn.classList.toggle('disabled', blocked);
    btn.title = blocked ? ('Restart blocked: ' + (reason || 'Active VPS tasks are still running.')) : 'Restart API server';
  }

  function fetchRestartStatus(token, apiOrigin) {
    if (!token || !apiOrigin) return;
    fetch(apiOrigin + '/api/server-status?token=' + encodeURIComponent(token), { cache: 'no-store' })
      .then(function (resp) {
        if (!resp.ok) throw new Error('server-status failed');
        return resp.json();
      })
      .then(function (data) {
        updateRestartButtonState(data || {});
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
        updateRestartButtonState(data || {});
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

  function escAttr(s) {
    return esc(s).replace(/'/g, '&#39;');
  }

  /* ════════════════════════════════════
     TOKEN KEEP-ALIVE & 401 REDIRECT
     ════════════════════════════════════ */

  /* Redirect to the standalone root login when token is invalid/expired. */
  function redirectToLogin() {
    var c = cfg();
    var origin = '';
    if (c.apiBase) {
      var match = String(c.apiBase).match(/^(https?:\/\/[^/]+)/);
      if (match) origin = match[1];
    }
    if (!origin) origin = window.location.origin;
    var url = new URL(origin + '/');
    window.location.replace(url.toString());
  }

  function performLogout() {
    var c = cfg();
    var origin = '';
    if (c.apiBase) {
      var match = String(c.apiBase).match(/^(https?:\/\/[^/]+)/);
      if (match) origin = match[1];
    }
    if (!origin) origin = window.location.origin;

    var redirect = function () {
      var url = new URL(origin + '/');
      window.location.replace(url.toString());
    };

    if (!c.token) {
      redirect();
      return;
    }

    fetch(origin + '/api/auth/logout', {
      method: 'POST',
      headers: { 'Authorization': 'Bearer ' + c.token }
    }).finally(function () {
      redirect();
    });
  }

  /* Periodically call /api/token-refresh to extend token expiry.
     Interval: 30 minutes.  If the refresh itself returns 401 we redirect. */
  var _refreshTimer = null;
  var _authCheckPending = false;
  function tokenRefreshUrl(token) {
    var apiRoot = '';
    if (window.API_BASE) {
      var m = String(window.API_BASE).match(/^(https?:\/\/[^/]+)/);
      apiRoot = m ? m[1] : '';
    }
    return apiRoot + '/api/token-refresh?token=' + encodeURIComponent(token || '');
  }

  function confirmTokenStillValid() {
    if (_authCheckPending) return;
    var c = cfg();
    if (!c.token) {
      redirectToLogin();
      return;
    }
    _authCheckPending = true;
    _origFetch(tokenRefreshUrl(c.token), { method: 'POST' })
      .then(function (r) {
        if (r.status === 401) {
          redirectToLogin();
          return;
        }
        _authCheckPending = false;
      })
      .catch(function () { _authCheckPending = false; });
  }

  function startTokenRefresh() {
    if (_refreshTimer) return;
    var c = cfg();
    if (!c.token) return;
    function doRefresh() {
      _origFetch(tokenRefreshUrl(c.token), { method: 'POST' })
        .then(function (r) {
          if (r.status === 401) { redirectToLogin(); }
        })
        .catch(function () { /* network error — ignore, will retry next cycle */ });
    }
    doRefresh();  /* immediate first refresh on page load */
    _refreshTimer = setInterval(doRefresh, 30 * 60 * 1000);  /* every 30 min */
  }

  /* Global 401 interceptor — confirm the session token is actually invalid before
     redirecting, so one transient background 401 does not drop the whole page. */
  var _origFetch = window.fetch;
  window.fetch = function () {
    return _origFetch.apply(this, arguments).then(function (response) {
      if (response.status === 401) {
        confirmTokenStillValid();
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
    buildConfirmOverlay();
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
  window.PBGuiConfirm = showNavConfirm;

}());
