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
    ]},
    { id: 'pbv6m', label: 'PBv6 Multi', items: [
      { page: 'v6_multi_run',         icon: '&#9654;',   label: 'Run'               },
      { page: 'v6_multi_backtest',    icon: '&#9194;',   label: 'Backtest'          },
      { page: 'v6_multi_optimize',    icon: '&#9881;',   label: 'Optimize'          }
    ]},
    { id: 'pbv6s', label: 'PBv6 Single', items: [
      { page: 'v6_single_run',        icon: '&#9654;',   label: 'Run'               },
      { page: 'v6_single_backtest',   icon: '&#9194;',   label: 'Backtest'          },
      { page: 'v6_single_optimize',   icon: '&#9881;',   label: 'Optimize'          },
      { page: 'v6_spot_view',         icon: '&#128065;', label: 'Spot View'         }
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
    '#pbgui-about-ver{font-size:var(--fs-xl);font-weight:800;color:#e2e8f0;margin-bottom:0.25rem;}',
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

    /* find which group contains the current page */
    var activeGroup = '';
    NAV_GROUPS.forEach(function (g) {
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
    NAV_GROUPS.forEach(function (group) {
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
          + '<button class="nav-action-btn accent" id="pbgui-guide-btn">&#128218; Guide</button>'
          + '<button class="nav-action-btn" id="pbgui-about-btn">&#x2139;&#xFE0F; About</button>'
          + '</div>';

    nav.innerHTML = html;
  }

  function buildAbout() {
    if (document.getElementById('pbgui-about-ovl')) return;
    var c = cfg();
    var ver = esc(c.version || '');
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
    'system_api_keys':   '/api/api-keys/main_page'
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
          setTimeout(function() { window.location.reload(); }, 3000);
        }).catch(function() {
          setTimeout(function() { window.location.reload(); }, 3000);
        });
      });
    }

    /* SSE: watch for needs_restart */
    setupRestartSSE(TOKEN, apiOrigin);
  }

  function setupRestartSSE(token, apiOrigin) {
    if (!token || !apiOrigin) return;
    var url = apiOrigin + '/api/server-status/stream?token=' + encodeURIComponent(token);
    var es = new EventSource(url);
    es.onmessage = function (e) {
      try {
        var data = JSON.parse(e.data);
        var btn = document.getElementById('pbgui-restart-btn');
        if (btn) btn.style.display = data.needs_restart ? 'flex' : 'none';
      } catch (_) {}
    };
    es.onerror = function () {
      es.close();
      /* retry after 15s */
      setTimeout(function() { setupRestartSSE(token, apiOrigin); }, 15000);
    };
  }

  /* ── html escape helper ── */
  function esc(s) {
    return String(s || '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
  }

  /* ════════════════════════════════════
     INIT
     ════════════════════════════════════ */
  function init() {
    injectCSS();
    buildNav();
    buildAbout();
    setupHandlers();
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }

}());
