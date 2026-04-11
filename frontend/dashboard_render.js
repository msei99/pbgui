/**
 * dashboard_render.js
 * Shared rendering module for dashboard widgets.
 *
 * Served by FastAPI at /app/dashboard_render.js.
 * Loaded dynamically in dashboard_balance.html and dashboard_top.html
 * (via document.createElement('script')) to bypass DOMPurify.
 * Loaded via <script src=""> in dashboard_editor.html (no DOMPurify there on
 * the src attribute path — external src with empty body is not stripped).
 *
 * Exported as  window.DashRender = { ... }
 *
 * API:
 *   DashRender.injectCSS()                          – inject shared CSS once
 *   DashRender.tweColor(v)                          – TWE colour string
 *   DashRender.upnlColor(v)                         – uPnL colour string
 *   DashRender.tweBarPct(v)                         – TWE bar width %
 *   DashRender.signedFmt(v)                         – "+1.23" / "-1.23"
 *   DashRender.renderBalanceRows(tbody, rows)        – fill <tbody> with rows
 *   DashRender.buildBalance(container, data)         – build full balance widget DOM
 *   DashRender.renderTop(chartDiv, data, opts)       – Plotly.react into chartDiv
 *   DashRender.buildTop(container, data, opts)       – build full top widget DOM + chart
 *
 * opts for renderTop / buildTop:
 *   { users, topN, period, height, displayModeBar, responsive }
 *
 * NOTE: This file is served as plain JavaScript — no DOMPurify constraints.
 *       Angle brackets in string literals do NOT need \x3C / \x3E escaping.
 */
(function (global) {
    'use strict';

    /* ──────────────────────────────── CSS ──────────────────────────────── */

    var _CSS = [
        /* ── Design tokens — single source of truth for all widget colours ── */
        ':root{',
        '  --db-bg:#0e1117;',
        '  --db-surface:#1a202c;',
        '  --db-surface2:#2d3748;',
        '  --db-surface3:#4a5568;',
        '  --db-text:#e2e8f0;',
        '  --db-text-muted:#94a3b8;',
        '  --db-text-dim:#64748b;',
        '  --db-title:#63b3ed;',
        '  --db-pos:#48bb78;',
        '  --db-neg:#f56565;',
        '  --db-warn:#fbd38d;',
        '  --db-accent:#5a8dee;',
        '  --db-green:#22c55e;',
        '  --db-red:#ef4444;',
        '  --db-orange:#f59e0b;',
        '  --db-radius:6px;',
        '  --db-font:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}',

        /* ── Balance widget ── */
        '.db-root{font-family:var(--db-font);font-size:0.875rem;color:var(--db-text);background:var(--db-bg);}',

        '.db-header{display:flex;justify-content:flex-start;align-items:center;',
        '  padding:0.5rem 0.75rem;background:var(--db-surface);border-bottom:1px solid var(--db-surface2);',
        '  border-radius:var(--db-radius) var(--db-radius) 0 0;flex-wrap:nowrap;gap:0.5rem;}',

        '.db-totals{display:flex;gap:1.5rem;flex-wrap:wrap;}',

        '.db-total-item label{color:var(--db-text-dim);font-size:0.68rem;text-transform:uppercase;',
        '  letter-spacing:0.05em;display:block;}',
        '.db-total-item span{font-weight:600;font-size:0.88rem;}',

        '.db-user-sel{display:flex;align-items:center;gap:0.4rem;position:relative;margin-left:auto;}',
        '.db-user-sel label{color:var(--db-text-muted);font-size:0.73rem;}',

        '.db-msel-btn{background:var(--db-surface2);color:var(--db-text);border:1px solid var(--db-surface3);',
        '  border-radius:4px;padding:0.25rem 0.5rem;font-size:0.78rem;cursor:pointer;',
        '  min-width:120px;text-align:left;display:flex;justify-content:space-between;',
        '  align-items:center;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;',
        '  max-width:260px;}',
        '.db-msel-btn:hover{border-color:var(--db-title);}',
        '.db-msel-arrow{font-size:0.55rem;margin-left:0.4rem;flex-shrink:0;}',

        '.db-msel-drop{display:none;position:absolute;top:100%;right:0;background:var(--db-surface);',
        '  border:1px solid var(--db-surface3);border-radius:4px;min-width:180px;max-height:260px;',
        '  overflow:hidden;z-index:100;box-shadow:0 4px 12px rgba(0,0,0,0.4);margin-top:2px;}',
        '.db-msel-drop.open{display:block;}',

        '.db-msel-item{display:flex;align-items:center;gap:0.4rem;padding:0.3rem 0.6rem;',
        '  cursor:pointer;font-size:0.78rem;color:var(--db-text);white-space:nowrap;}',
        '.db-msel-item:hover{background:var(--db-surface2);}',
        '.db-msel-item input[type="checkbox"]{accent-color:var(--db-title);margin:0;cursor:pointer;}',
        '.db-msel-sep{border-top:1px solid var(--db-surface2);margin:0.15rem 0;}',

        '.db-msel-filter{width:100%;box-sizing:border-box;background:var(--db-surface2);color:var(--db-text);',
        '  border:none;border-bottom:1px solid var(--db-surface3);padding:0.35rem 0.5rem;',
        '  font-size:0.76rem;outline:none;}',
        '.db-msel-filter::placeholder{color:var(--db-text-dim);}',
        '.db-msel-list{max-height:180px;overflow-y:auto;}',

        '.db-status{font-size:0.68rem;color:var(--db-text-dim);padding:0.15rem 0.75rem;',
        '  background:var(--db-bg);border-bottom:1px solid var(--db-surface);}',

        '.db-table-wrap{overflow-x:auto;}',
        '.db-table{width:100%;border-collapse:collapse;}',
        '.db-table thead th{background:var(--db-surface);color:var(--db-text-muted);padding:0.4rem 0.7rem;',
        '  text-align:left;font-weight:500;font-size:0.75rem;text-transform:uppercase;',
        '  letter-spacing:0.04em;border-bottom:1px solid var(--db-surface2);',
        '  cursor:pointer;user-select:none;}',
        '.db-table thead th:hover{color:var(--db-text);}',
        '.db-sort-arrow{font-size:0.6rem;margin-left:0.25rem;color:var(--db-text-dim);}',
        '.db-table thead th.db-sorted .db-sort-arrow{color:var(--db-text);}',
        '.db-table tbody td{padding:0.35rem 0.7rem;border-bottom:1px solid var(--db-surface);}',
        '.db-table tbody tr:hover td{background:#1e2a3a;}',

        '.db-green{color:var(--db-green);}',
        '.db-red{color:var(--db-red);}',
        '.db-orange{color:var(--db-orange);}',
        '.db-muted{color:var(--db-text-muted);font-size:0.76rem;}',

        '.db-twe-cell{display:flex;align-items:center;gap:0.4rem;}',
        '.db-twe-track{flex:1;height:7px;border-radius:3px;background:var(--db-surface2);',
        '  overflow:hidden;min-width:50px;}',
        '.db-twe-fill{height:100%;border-radius:3px;transition:width .3s,background .3s;}',
        '.db-twe-lbl{font-size:0.76rem;min-width:44px;text-align:right;}',
        '.db-nodata{padding:1.5rem;text-align:center;color:var(--db-surface3);font-size:0.85rem;}',

        /* ── Shared widget chrome (.dt-root and .di-root) ──────────────────
           All widgets share .dt-header, .dt-title, .dt-meta* classes.
           Never add widget-specific overrides for these — use var() tokens. */
        '.dt-root,.di-root{font-family:var(--db-font);font-size:0.875rem;color:var(--db-text);',
        '  background:var(--db-bg);border-radius:var(--db-radius);overflow:hidden;position:relative;}',
        '.di-root{width:100%;display:flex;flex-direction:column;}',

        '.dt-header{padding:0.45rem 0.75rem;background:var(--db-surface);border-bottom:1px solid var(--db-surface2);',
        '  display:flex;align-items:center;justify-content:flex-start;',
        '  flex-wrap:nowrap;gap:0.4rem;}',
        '.dt-title{font-size:0.88rem;font-weight:600;color:var(--db-title);white-space:nowrap;flex-shrink:0;}',
        '.dt-meta{font-size:0.73rem;color:var(--db-text-muted);white-space:nowrap;margin-left:auto;}',
        '.dt-meta-user{color:var(--db-text);}',

        '.dt-daterange{font-size:0.68rem;color:var(--db-text-dim);padding:0.2rem 0.75rem;',
        '  background:var(--db-bg);border-bottom:1px solid var(--db-surface);min-height:1.1em;}',
        '.dt-status{font-size:0.68rem;color:var(--db-text-dim);padding:0.2rem 0.75rem;min-height:1.1em;}',
        '.dt-nodata{padding:1.5rem;text-align:center;color:var(--db-surface3);font-size:0.85rem;}',
        '.dt-chart{width:100%;position:relative;}',

        /* inline editable controls inside the header */
        '.dt-meta-controls{display:flex;align-items:center;gap:0.3rem;flex-wrap:nowrap;flex-shrink:0;margin-left:auto;}',
        '.dt-meta-lbl{color:var(--db-text-muted);font-size:0.73rem;white-space:nowrap;flex-shrink:0;}',
        '.dt-meta-sep{color:var(--db-surface3);font-size:0.73rem;padding:0 0.1rem;flex-shrink:0;}',
        '.dt-meta-controls input.dt-ctrl-num{width:52px!important;flex-shrink:0;background:var(--db-surface2);color:var(--db-text);',
        '  border:1px solid var(--db-surface3);border-radius:4px;',
        '  padding:0.2rem 0.3rem;font-size:0.76rem;outline:none;}',
        '.dt-meta-controls select.dt-ctrl-sel{width:auto!important;flex-shrink:0;background:var(--db-surface2);color:var(--db-text);border:1px solid var(--db-surface3);',
        '  border-radius:4px;padding:0.2rem 0.3rem;font-size:0.76rem;',
        '  outline:none;cursor:pointer;max-width:160px;}',
        '.dt-meta-controls input.dt-ctrl-date{width:112px!important;flex-shrink:0;background:var(--db-surface2);color:var(--db-text);',
        '  border:1px solid var(--db-surface3);border-radius:4px;',
        '  padding:0.2rem 0.3rem;font-size:0.76rem;outline:none;cursor:pointer;}',
        '.dt-meta-controls input.dt-ctrl-date:disabled{opacity:0.4;cursor:not-allowed;}',
        '.dt-ctrl-now-wrap{display:flex;align-items:center;gap:0.25rem;flex-shrink:0;',
        '  color:var(--db-text-muted);font-size:0.73rem;white-space:nowrap;cursor:pointer;user-select:none;}',
        '.dt-ctrl-now-wrap input[type=checkbox]{cursor:pointer;accent-color:var(--db-accent);width:13px;height:13px;flex-shrink:0;}',
        '.dt-meta-controls .msel-wrap{width:auto!important;flex-shrink:0;}',
        '.dt-meta-controls .msel-btn{min-width:80px;max-width:120px;font-size:0.73rem;padding:0.2rem 0.35rem;}',

        /* widget icon before title */
        '.dt-icon{font-size:0.85rem;line-height:1;flex-shrink:0;}',
        /* trash button inside dt-header */
        '.dt-trash{display:inline-flex;align-items:center;justify-content:center;',
        '  width:22px;height:22px;border:none;background:transparent;',
        '  color:var(--db-text-dim,#4a5568);font-size:0.85rem;cursor:pointer;border-radius:4px;',
        '  transition:color 0.12s,background 0.12s;flex-shrink:0;padding:0;margin-left:0.25rem;line-height:1;}',
        '.dt-trash:hover{color:#fc8181;background:rgba(252,129,129,0.1);}',

        /* Plotly modebar — shared rule covering both widget roots */
        '.dt-root .modebar-container .modebar,.di-root .modebar-container .modebar{display:flex!important;flex-direction:row!important;flex-wrap:nowrap!important;}',
        '.dt-root .modebar-container .modebar-group,.di-root .modebar-container .modebar-group{display:flex!important;flex-direction:row!important;flex-wrap:nowrap!important;}',
        '.dt-root .modebar-container,.di-root .modebar-container{position:absolute!important;right:0!important;top:0!important;}',

        /* fullscreen — shared */
        '.dt-root:fullscreen,.di-root:fullscreen{border-radius:0;width:100vw;height:100vh;display:flex;flex-direction:column;}',
        '.dt-root:fullscreen .dt-chart,.di-root:fullscreen .di-chart{flex:1;}',
        '.dt-root:-webkit-full-screen,.di-root:-webkit-full-screen{border-radius:0;width:100vw;height:100vh;display:flex;flex-direction:column;}',
        '.dt-root:-webkit-full-screen .dt-chart,.di-root:-webkit-full-screen .di-chart{flex:1;}',

        /* X close button (fullscreen) */
        '.dt-fs-close{display:none;position:absolute;top:8px;left:8px;z-index:9999;',
        '  background:rgba(45,55,72,0.85);color:var(--db-text);',
        '  border:1px solid var(--db-surface3);border-radius:4px;',
        '  padding:0.2rem 0.55rem;font-size:0.82rem;line-height:1.5;cursor:pointer;}',
        '.dt-fs-close:hover{background:#e53e3e;border-color:#e53e3e;color:#fff;}',

        /* ── Income widget specific ── */
        '.di-nav-btn{background:var(--db-surface2);color:var(--db-text);border:1px solid var(--db-surface3);border-radius:4px;',
        '  padding:0.18rem 0.55rem;font-size:0.82rem;cursor:pointer;white-space:nowrap;flex-shrink:0;}',
        '.di-nav-btn:hover{background:var(--db-surface3);border-color:var(--db-title);color:var(--db-title);}',
        '.di-table-wrap{overflow-x:auto;overflow-y:auto;flex:1;min-height:0;}',
        '.di-table{width:100%;border-collapse:collapse;font-size:0.78rem;}',
        '.di-table th{position:sticky;top:0;background:var(--db-surface);color:var(--db-text-muted);font-weight:600;',
        '  padding:0.35rem 0.5rem;text-align:left;border-bottom:1px solid var(--db-surface2);white-space:nowrap;cursor:pointer;user-select:none;}',
        '.di-table th:hover{color:var(--db-text);}',
        '.di-table th .di-sort{font-size:0.65rem;margin-left:0.2rem;color:var(--db-text-dim);}',
        '.di-table td{padding:0.3rem 0.5rem;border-bottom:1px solid var(--db-surface);white-space:nowrap;}',
        '.di-table tr:hover{background:var(--db-surface);}',
        '.di-table tr.di-sel{background:#2a3a5c;}',
        '.di-inc-pos{color:var(--db-pos);}',
        '.di-inc-neg{color:var(--db-neg);}',
        '.di-table input[type=checkbox]{cursor:pointer;accent-color:var(--db-accent);width:13px;height:13px;}',
        '.di-jump-input{margin-left:0.4rem;background:var(--db-surface2);color:var(--db-text);',
        '  border:1px solid var(--db-surface3);border-radius:4px;padding:0.1rem 0.25rem;',
        '  font-size:0.72rem;outline:none;cursor:pointer;width:108px;font-weight:400;}',
        '.di-actions{display:flex;gap:0.5rem;padding:0.4rem 0.75rem;background:var(--db-bg);',
        '  border-top:1px solid var(--db-surface);flex-wrap:wrap;align-items:center;}',
        '.di-btn{background:var(--db-surface2);color:var(--db-text);border:1px solid var(--db-surface3);border-radius:4px;',
        '  padding:0.3rem 0.7rem;font-size:0.76rem;cursor:pointer;white-space:nowrap;}',
        '.di-btn:hover{background:var(--db-surface3);}',
        '.di-btn-danger{background:#742a2a;border-color:#9b2c2c;}',
        '.di-btn-danger:hover{background:#9b2c2c;}',
        '.di-btn:disabled{opacity:0.4;cursor:not-allowed;}',
        '.di-confirm{position:absolute;top:0;left:0;right:0;bottom:0;background:rgba(0,0,0,0.85);',
        '  z-index:100;display:flex;flex-direction:column;align-items:center;justify-content:center;gap:0.8rem;',
        '  padding:1.5rem;text-align:center;}',
        '.di-confirm-msg{color:var(--db-warn);font-size:0.85rem;max-width:90%;}',
        '.di-confirm-btns{display:flex;gap:0.8rem;}',
        '.di-btn-yes{background:#276749;border-color:#38a169;color:#fff;}',
        '.di-btn-yes:hover{background:#38a169;}',
        '.di-btn-no{background:#742a2a;border-color:#e53e3e;color:#fff;}',
        '.di-btn-no:hover{background:#9b2c2c;}',
        '.di-backup{padding:0.5rem 0.75rem;background:var(--db-surface);border-top:1px solid var(--db-surface2);}',
        '.di-backup select{background:var(--db-surface2);color:var(--db-text);border:1px solid var(--db-surface3);border-radius:4px;',
        '  padding:0.25rem 0.4rem;font-size:0.76rem;margin:0 0.4rem;}',
        '.di-status{font-size:0.73rem;color:var(--db-pos);padding:0.3rem 0.75rem;}',
        '.di-chart{width:100%;position:relative;}',

        /* ── Positions widget ── */
        '.dp-table-wrap{overflow-x:auto;overflow-y:auto;max-height:70vh;}',
        '.dp-table{width:100%;border-collapse:collapse;font-size:0.78rem;}',
        '.dp-table th{position:sticky;top:0;background:var(--db-surface);color:var(--db-text-muted);font-weight:600;',
        '  padding:0.35rem 0.5rem;text-align:left;border-bottom:1px solid var(--db-surface2);white-space:nowrap;',
        '  cursor:pointer;user-select:none;}',
        '.dp-table th:hover{color:var(--db-text);}',
        '.dp-table th .dp-sort{font-size:0.65rem;margin-left:0.2rem;color:var(--db-text-dim);}',
        '.dp-table td{padding:0.3rem 0.5rem;border-bottom:1px solid var(--db-surface);white-space:nowrap;}',
        '.dp-table tr{cursor:pointer;}',
        '.dp-table tr:hover td{background:#1e2a3a;}',
        '.dp-table tr.dp-sel td{background:#2a3a5c;}',
        '.dp-upnl-pos{color:var(--db-pos);}',
        '.dp-upnl-neg{color:var(--db-neg);}',

        /* ── Orders widget (Lightweight Charts) ── */
        '.do-tf-bar{display:flex;gap:0.25rem;align-items:center;flex-wrap:wrap;}',
        '.do-tf-btn{background:var(--db-surface2);color:var(--db-text-dim);border:1px solid var(--db-surface3);border-radius:4px;',
        '  padding:0.15rem 0.5rem;font-size:0.78rem;cursor:pointer;white-space:nowrap;transition:all 0.15s;}',
        '.do-tf-btn:hover{background:var(--db-surface3);color:var(--db-text);}',
        '.do-tf-btn.do-tf-active{background:var(--db-accent);color:#fff;border-color:var(--db-accent);}',
        '.do-chart-wrap{position:relative;width:100%;height:580px;background:#0e1117;border-radius:6px;overflow:hidden;}',
        '.do-chart-wrap.do-fullscreen,.do-chart-wrap:fullscreen{position:fixed;top:0;left:0;width:100vw !important;height:100vh !important;z-index:99999;border-radius:0;background:#0e1117;}',
        '.dt-root:fullscreen .do-chart-wrap,.dt-root:-webkit-full-screen .do-chart-wrap{flex:1;height:auto !important;position:relative !important;}',        '.do-chart-toolbar{position:absolute;top:4px;left:4px;z-index:20;display:none;gap:2px;background:rgba(14,17,23,0.85);border:1px solid #2d3748;border-radius:5px;padding:3px 4px;}',,
        '.do-chart-wrap:hover .do-chart-toolbar{display:flex;}',
        '.do-fs-btn{background:transparent;color:#94a3b8;border:none;padding:2px 5px;font-size:15px;cursor:pointer;line-height:1;border-radius:3px;}',
        '  border:1px solid #4a5568;border-radius:4px;padding:0.2rem 0.5rem;font-size:0.75rem;cursor:pointer;}',
        '.do-fs-btn:hover{color:#e2e8f0;background:rgba(45,55,72,0.9);}',
        '.dt-pos{color:#22c55e;}',
        '.dt-neg{color:#ef4444;}',
        '.do-legend{position:absolute;top:6px;left:40px;z-index:10;display:flex;gap:0.75rem;font-size:0.65rem;color:#94a3b8;pointer-events:none;}',
        '.do-leg-item{display:inline-flex;align-items:center;gap:4px;}',
        '.do-leg-solid{display:inline-block;width:18px;border-bottom:2px solid;}',
        '.do-leg-dotted{display:inline-block;width:18px;border-bottom:1px dotted;}',
        '.do-leg-dashed{display:inline-block;width:18px;border-bottom:1px dashed;}'
    ].join('');

    var _cssInjected = false;

    function injectCSS() {
        if (_cssInjected || document.getElementById('dr-css')) {
            _cssInjected = true;
            return;
        }
        _cssInjected = true;
        var s = document.createElement('style');
        s.id = 'dr-css';
        s.textContent = _CSS;
        (document.head || document.documentElement).appendChild(s);
    }

    /* ──────────────────────── Balance helpers ───────────────────────────── */

    function tweColor(v)  { return v < 100 ? '#22c55e' : v < 200 ? '#f59e0b' : '#ef4444'; }
    function upnlColor(v) { return v >= 0  ? '#22c55e' : '#ef4444'; }
    function tweBarPct(v) { return Math.min(100, (v / 300) * 100).toFixed(1); }
    function signedFmt(v) { return (v >= 0 ? '+' : '') + v.toFixed(2); }

    /** Prepend an icon span before titleSpan and append a trash button to hdr.
     *  @param {HTMLElement} hdr  — the .dt-header or .db-header element
     *  @param {string|null} icon — emoji string (e.g. '\ud83d\udcca')
     *  @param {HTMLElement|null} titleSpan — the .dt-title span (insert icon before it)
     *  @param {Function|null} onDelete — callback when trash is clicked
     */
    function _decorateHeader(hdr, icon, titleSpan, onDelete) {
        if (icon && titleSpan) {
            var ic = document.createElement('span');
            ic.className = 'dt-icon';
            ic.textContent = icon;
            hdr.insertBefore(ic, titleSpan);
        }
        if (typeof onDelete === 'function') {
            var trash = document.createElement('button');
            trash.className = 'dt-trash';
            trash.innerHTML = '&#128465;';
            trash.title = 'Remove widget';
            trash.addEventListener('click', function (e) { e.stopPropagation(); onDelete(); });
            hdr.appendChild(trash);
        }
    }

    /**
     * Fill a <tbody> element with balance rows.
     * @param {HTMLTableSectionElement} tbody
     * @param {Array<{user,date,balance,upnl,we}>} rows
     */
    function renderBalanceRows(tbody, rows) {
        tbody.innerHTML = '';
        for (var i = 0; i < rows.length; i++) {
            var row = rows[i];
            var tc  = tweColor(row.we);
            var uc  = upnlColor(row.upnl);
            var pct = tweBarPct(row.we);
            var tr  = document.createElement('tr');
            tr.innerHTML =
                '<td>' + row.user + '</td>' +
                '<td class="db-muted">' + row.date + '</td>' +
                '<td>' + row.balance.toFixed(2) + '</td>' +
                '<td style="color:' + uc + '">' + signedFmt(row.upnl) + '</td>' +
                '<td><div class="db-twe-cell">' +
                  '<div class="db-twe-track">' +
                    '<div class="db-twe-fill" style="width:' + pct + '%;background:' + tc + '"></div>' +
                  '</div>' +
                  '<span class="db-twe-lbl" style="color:' + tc + '">' + row.we.toFixed(2) + '</span>' +
                '</div></td>';
            tbody.appendChild(tr);
        }
    }

    /**
     * Build a complete balance widget DOM inside container.
     * Used by the editor inline preview.
     * @param {HTMLElement} container
     * @param {{rows:Array, totals:{balance,upnl,we}}} data
     * @param {{users?:Array}} opts  optional — pass users to show label top-right (matches live view)
     */
    function buildBalance(container, data, opts) {
        injectCSS();
        opts = opts || {};
        /* Fast-path: update existing balance widget in-place (avoids blank-frame flicker) */
        var _dbRoot = container.querySelector('.db-root');
        var _dbTbody = _dbRoot && _dbRoot.querySelector('.db-table tbody');
        if (_dbRoot && _dbTbody && data && (data.rows || []).length > 0) {
            var _t = (data.totals) ? data.totals : {};
            var _totDiv = _dbRoot.querySelector('.db-totals');
            if (_totDiv) {
                _totDiv.innerHTML =
                    '<div class="db-total-item"><label>Total Balance</label>' +
                    '<span class="db-green">$' + (_t.balance || 0).toFixed(2) + ' USDT</span></div>' +
                    '<div class="db-total-item"><label>Total uPnl</label>' +
                    '<span style="color:' + upnlColor(_t.upnl || 0) + '">' + signedFmt(_t.upnl || 0) + '</span></div>' +
                    '<div class="db-total-item"><label>Total TWE</label>' +
                    '<span style="color:' + tweColor(_t.we || 0) + '">' + (_t.we || 0).toFixed(2) + ' %</span></div>';
            }
            var _st = _dbRoot.querySelector('.db-status');
            if (_st) _st.textContent = 'Updated: ' + new Date().toLocaleTimeString();
            renderBalanceRows(_dbTbody, data.rows);
            return;
        }
        container.innerHTML = '';

        var t    = (data && data.totals) ? data.totals : {};
        var rows = (data && data.rows)   ? data.rows   : [];

        var root = document.createElement('div');
        root.className = 'db-root';

        /* header: totals left, optional users label right (same position as live view) */
        var hdr = document.createElement('div');
        hdr.className = 'db-header';
        var totDiv = document.createElement('div');
        totDiv.className = 'db-totals';
        totDiv.innerHTML =
            '<div class="db-total-item"><label>Total Balance</label>' +
            '<span class="db-green">$' + (t.balance || 0).toFixed(2) + ' USDT</span></div>' +
            '<div class="db-total-item"><label>Total uPnl</label>' +
            '<span style="color:' + upnlColor(t.upnl || 0) + '">' + signedFmt(t.upnl || 0) + '</span></div>' +
            '<div class="db-total-item"><label>Total TWE</label>' +
            '<span style="color:' + tweColor(t.we || 0) + '">' + (t.we || 0).toFixed(2) + ' %</span></div>';
        /* optional icon before totals */
        hdr.appendChild(totDiv);
        if (opts.icon) {
            var ic = document.createElement('span');
            ic.className = 'dt-icon';
            ic.textContent = opts.icon;
            hdr.insertBefore(ic, totDiv);
        }
        /* top-right: either a pre-built interactive control (editor) or a static label (live view) */
        if (opts.usersControl) {
            /* interactive dropdown element passed in by the editor */
            var userSelCtrl = document.createElement('div');
            userSelCtrl.className = 'db-user-sel';
            userSelCtrl.innerHTML = '<label>Users:</label>';
            userSelCtrl.appendChild(opts.usersControl);
            hdr.appendChild(userSelCtrl);
        } else if (opts.users !== undefined) {
            /* static label for live view */
            var uLabel = (opts.users && opts.users.length > 0 &&
                          !(opts.users.length === 1 && opts.users[0] === 'ALL'))
                ? opts.users.join(', ') : 'ALL';
            var userSel = document.createElement('div');
            userSel.className = 'db-user-sel';
            var userLbl = document.createElement('label');
            userLbl.textContent = 'Users:';
            userSel.appendChild(userLbl);
            var userSpan = document.createElement('span');
            userSpan.style.cssText = 'color:#e2e8f0;font-size:0.78rem;';
            userSpan.textContent = uLabel;
            userSel.appendChild(userSpan);
            hdr.appendChild(userSel);
        }
        if (typeof opts.onDelete === 'function') {
            var trash = document.createElement('button');
            trash.className = 'dt-trash';
            trash.innerHTML = '&#128465;';
            trash.title = 'Remove widget';
            trash.addEventListener('click', function (e) { e.stopPropagation(); opts.onDelete(); });
            hdr.appendChild(trash);
        }
        root.appendChild(hdr);

        /* status / updated timestamp */
        var statusDiv = document.createElement('div');
        statusDiv.className = 'db-status';
        statusDiv.textContent = 'Updated: ' + new Date().toLocaleTimeString();
        root.appendChild(statusDiv);

        if (rows.length === 0) {
            var noData = document.createElement('div');
            noData.className = 'db-nodata';
            noData.textContent = 'No balance data.';
            root.appendChild(noData);
            container.appendChild(root);
            return;
        }

        /* table */
        var wrap = document.createElement('div');
        wrap.className = 'db-table-wrap';
        var tbl = document.createElement('table');
        tbl.className = 'db-table';
        tbl.innerHTML =
            '<thead><tr>' +
            '<th>User</th><th>Date</th>' +
            '<th>Balance USDT</th><th>uPnl</th><th>TWE %</th>' +
            '</tr></thead>';
        var tbody = document.createElement('tbody');
        renderBalanceRows(tbody, rows);
        tbl.appendChild(tbody);
        wrap.appendChild(tbl);
        root.appendChild(wrap);
        container.appendChild(root);
    }

    /* ──────────────────────── Top Symbols helpers ───────────────────────── */

    /**
     * Render / update a Plotly bar chart into chartDiv.
     * Requires global Plotly (or opts.Plotly).
     * @param {HTMLElement} chartDiv
     * @param {{rows:Array, from_date:string, to_date:string}} data
     * @param {{height?:number, displayModeBar?:boolean, responsive?:boolean, Plotly?:object}} opts
     */
    function renderTop(chartDiv, data, opts) {
        opts = opts || {};
        var P = opts.Plotly || global.Plotly;
        if (!P) { chartDiv.textContent = 'Plotly not loaded'; return; }

        var rows    = (data && data.rows) ? data.rows : [];
        var symbols = rows.map(function (r) { return r[1]; });
        var incomes = rows.map(function (r) { return parseFloat(r[2]); });
        var colors  = incomes.map(function (v) { return v < 0 ? '#fc8181' : '#68d391'; });

        var trace = {
            x: symbols,
            y: incomes,
            type: 'bar',
            marker: { color: colors },
            hovertemplate: '<b>%{x}</b><br>Income: %{y:.4f}<extra></extra>'
        };
        var layout = {
            paper_bgcolor: '#0e1117',
            plot_bgcolor:  '#0e1117',
            font:   { color: '#e2e8f0', size: 11 },
            margin: { l: 50, r: 20, t: 40, b: 60 },
            xaxis:  { tickangle: -45, gridcolor: '#2d3748', color: '#e2e8f0' },
            yaxis:  { gridcolor: '#2d3748', color: '#e2e8f0',
                      zeroline: true, zerolinecolor: '#4a5568' },
            bargap: 0.3,
            autosize: true
        };
        var origHeight = opts.height || null;
        if (origHeight) { layout.height = origHeight; }

        /* fullscreen change: relayout to fill full screen or restore original size */
        var fschangeHandler = function () {
            var root = chartDiv.closest ? chartDiv.closest('.dt-root') : null;
            var isFull = root
                ? (document.fullscreenElement === root || document.webkitFullscreenElement === root)
                : !!(document.fullscreenElement || document.webkitFullscreenElement);
            /* show/hide the X close button */
            var closeBtn = root ? root.querySelector('.dt-fs-close') : null;
            if (closeBtn) { closeBtn.style.display = isFull ? 'block' : 'none'; }
            if (isFull) {
                /* header (~40px) + daterange (~22px) = ~62px overhead */
                var fsW = window.screen.width  || window.innerWidth;
                var fsH = (window.screen.availHeight || window.innerHeight) - 62;
                P.relayout(chartDiv, { width: fsW, height: fsH });
            } else {
                /* restore original size — null lets Plotly infer from container */
                P.relayout(chartDiv, { width: null, height: origHeight || null });
                setTimeout(function () { P.Plots.resize(chartDiv); }, 100);
            }
        };
        document.addEventListener('fullscreenchange', fschangeHandler);
        document.addEventListener('webkitfullscreenchange', fschangeHandler);

        var cfg = {
            displayModeBar: opts.displayModeBar !== undefined ? opts.displayModeBar : false,
            responsive:     opts.responsive     !== undefined ? opts.responsive     : true,
            modeBarButtonsToAdd: [
                {
                    name:  'fullscreen',
                    title: 'Fullscreen',
                    icon:  {
                        width: 857.1, height: 857.1,
                        path: 'M0 0v285.7h142.9V142.9H285.7V0H0zm571.4 0v142.9h142.9v142.9H857.1V0H571.4zM0 571.4v285.7h285.7V714.3H142.9V571.4H0zm714.3 142.9v142.9H571.4v142.9H857.1V571.4H714.3z'
                    },
                    click: function (gd) {
                        var root = gd.closest ? gd.closest('.dt-root') : gd.parentElement;
                        var isFull = (document.fullscreenElement === root ||
                                      document.webkitFullscreenElement === root);
                        if (!isFull) {
                            if (root.requestFullscreen) root.requestFullscreen();
                            else if (root.webkitRequestFullscreen) root.webkitRequestFullscreen();
                        } else {
                            if (document.exitFullscreen) document.exitFullscreen();
                            else if (document.webkitExitFullscreen) document.webkitExitFullscreen();
                        }
                    }
                }
            ]
        };
        layout.transition = { duration: 0, easing: 'linear' };
        P.react(chartDiv, [trace], layout, cfg);
        if (!opts.noResize) { setTimeout(function () { P.Plots.resize(chartDiv); }, 80); }
    }

    /**
     * Build a complete top-symbols widget DOM inside container.
     * Used by the editor inline preview.
     * @param {HTMLElement} container
     * @param {{rows:Array, from_date:string, to_date:string}} data
     * @param {{users?:Array, topN?:number, period?:string,
     *           height?:number, displayModeBar?:boolean, responsive?:boolean}} opts
     */
    function buildTop(container, data, opts) {
        injectCSS();
        opts = opts || {};
        /* Fast-path: if chart already rendered, update in-place via Plotly.react */
        var _fc = container.querySelector('.dt-chart');
        if (_fc && data && (data.rows || []).length > 0) {
            var _dr = container.querySelector('.dt-daterange');
            if (_dr) _dr.textContent = (data.from_date && data.to_date) ? 'From: ' + data.from_date + '  To: ' + data.to_date : '';
            renderTop(_fc, data, { noResize: true });
            return;
        }
        container.innerHTML = '';

        var root = document.createElement('div');
        root.className = 'dt-root';

        /* header */
        var hdr = document.createElement('div');
        hdr.className = 'dt-header';
        var titleSpan = document.createElement('span');
        titleSpan.className = 'dt-title';
        titleSpan.textContent = 'Top Symbols';
        hdr.appendChild(titleSpan);

        if (opts.topNControl || opts.periodControl || opts.usersControl) {
            /* editor mode: interactive controls on the right */
            var metaDiv = document.createElement('div');
            metaDiv.className = 'dt-meta dt-meta-controls';
            if (opts.topNControl) {
                var lTop = document.createElement('span');
                lTop.className = 'dt-meta-lbl'; lTop.textContent = 'Top';
                metaDiv.appendChild(lTop);
                metaDiv.appendChild(opts.topNControl);
            }
            if (opts.periodControl) {
                var sepP = document.createElement('span');
                sepP.className = 'dt-meta-sep'; sepP.innerHTML = '&middot;';
                metaDiv.appendChild(sepP);
                var lPrd = document.createElement('span');
                lPrd.className = 'dt-meta-lbl'; lPrd.textContent = 'Period';
                metaDiv.appendChild(lPrd);
                metaDiv.appendChild(opts.periodControl);
            }
            if (opts.fromControl) {
                var sepF = document.createElement('span');
                sepF.className = 'dt-meta-sep'; sepF.innerHTML = '&middot;';
                metaDiv.appendChild(sepF);
                var lFrom = document.createElement('span');
                lFrom.className = 'dt-meta-lbl'; lFrom.textContent = 'From';
                metaDiv.appendChild(lFrom);
                metaDiv.appendChild(opts.fromControl);
            }
            if (opts.toControl) {
                var lTo = document.createElement('span');
                lTo.className = 'dt-meta-lbl'; lTo.textContent = 'To';
                metaDiv.appendChild(lTo);
                metaDiv.appendChild(opts.toControl);                if (opts.toNowControl) {
                    metaDiv.appendChild(opts.toNowControl);
                }            }
            if (opts.usersControl) {
                var sepU = document.createElement('span');
                sepU.className = 'dt-meta-sep'; sepU.innerHTML = '&middot;';
                metaDiv.appendChild(sepU);
                var lUsr = document.createElement('span');
                lUsr.className = 'dt-meta-lbl'; lUsr.textContent = 'Users';
                metaDiv.appendChild(lUsr);
                metaDiv.appendChild(opts.usersControl);
            } else if (opts.users && opts.users.length > 0) {
                var sepU2 = document.createElement('span');
                sepU2.className = 'dt-meta-sep'; sepU2.innerHTML = '&middot;';
                metaDiv.appendChild(sepU2);
                var lUsr2 = document.createElement('span');
                lUsr2.className = 'dt-meta-lbl'; lUsr2.textContent = 'Users';
                metaDiv.appendChild(lUsr2);
                var usrVal = document.createElement('span');
                usrVal.className = 'dt-meta-user';
                var _uLabel = (opts.users.length === 1 && opts.users[0] === 'ALL')
                    ? 'ALL' : opts.users.join(', ');
                usrVal.textContent = _uLabel;
                metaDiv.appendChild(usrVal);
            }
            hdr.appendChild(metaDiv);
        } else {
            /* live view: static meta text */
            var uLabel = (opts.users && opts.users.length > 0 &&
                          !(opts.users.length === 1 && opts.users[0] === 'ALL'))
                ? opts.users.join(', ') : 'ALL';
            var _rawPeriod = (opts.period || '');
            var _periodDisplay = (_rawPeriod.indexOf('CUSTOM:') === 0)
                ? (function () {
                      var _pp = _rawPeriod.split(':');
                      var _toDisp = (_pp[2] === 'NOW' || _pp[2] === '') ? 'Now' : _pp[2];
                      return _pp[1] + ' \u2192 ' + _toDisp;
                  }())
                : _rawPeriod;
            var metaSpan = document.createElement('span');
            metaSpan.className = 'dt-meta';
            metaSpan.innerHTML =
                'Top:&nbsp;' + (opts.topN || 10) +
                '&nbsp;&middot;&nbsp;Period:&nbsp;' + _periodDisplay +
                '&nbsp;&middot;&nbsp;Users:&nbsp;<span class="dt-meta-user">' + uLabel + '</span>';
            hdr.appendChild(metaSpan);
        }
        _decorateHeader(hdr, opts.icon, titleSpan, opts.onDelete);
        root.appendChild(hdr);

        /* date range */
        var dr = document.createElement('div');
        dr.className = 'dt-daterange';
        if (data && data.from_date && data.to_date) {
            dr.textContent = 'From: ' + data.from_date + '  To: ' + data.to_date;
        }
        root.appendChild(dr);

        var rows = (data && data.rows) ? data.rows : [];
        if (rows.length === 0) {
            var noData = document.createElement('div');
            noData.className = 'dt-nodata';
            noData.textContent = 'No data for the selected period.';
            root.appendChild(noData);
            container.appendChild(root);
            return;
        }

        /* chart container */
        var chartDiv = document.createElement('div');
        chartDiv.className = 'dt-chart';
        if (opts.height) { chartDiv.style.height = opts.height + 'px'; }

        /* X close button overlaid inside the chart, shown only in fullscreen */
        var closeBtn = document.createElement('button');
        closeBtn.className = 'dt-fs-close';
        closeBtn.textContent = '\u2715';  /* × */
        closeBtn.title = 'Exit Fullscreen';
        closeBtn.addEventListener('click', function () {
            if (document.exitFullscreen) document.exitFullscreen();
            else if (document.webkitExitFullscreen) document.webkitExitFullscreen();
        });
        chartDiv.appendChild(closeBtn);

        root.appendChild(chartDiv);
        container.appendChild(root);

        renderTop(chartDiv, data, opts);
    }

    /* ──────────────────────── Income widget ────────────────────────────── */

    /** Shared click handler factory for the Live vs BT nav button. */
    function _makeNavHandler(opts) {
        return function () {
            var navUrl = (opts.apiBase || '') + '/nav/request';
            fetch(navUrl, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'Authorization': 'Bearer ' + (opts.token || '')
                },
                body: JSON.stringify({
                    page:   'V7_LIVE_VS_BACKTEST',
                    params: { v7_live_vs_backtest_prefill_user: opts.lvbUser },
                    token:  opts.token || ''
                })
            });
        };
    }

    /**
     * Build a complete income widget DOM inside container.
     * Supports two modes:
     *   1) Chart mode  (data.mode === 'chart')  — cumulative line chart with per-symbol traces
     *   2) Table mode  (data.mode === 'table')  — sortable table with row selection + delete
     *
     * @param {HTMLElement} container
     * @param {object} data  — JSON from /income_data endpoint
     * @param {object} opts  — configuration:
     *   opts.periodControl, opts.usersControl, opts.lastNControl, opts.filterControl
     *     — interactive controls (editor/live mode)
     *   opts.fromControl, opts.toControl, opts.toNowControl — CUSTOM date controls
     *   opts.height        — chart height in px
     *   opts.displayModeBar, opts.responsive — Plotly options
     *   opts.apiBase, opts.token — for delete/backup API calls
     *   opts.onReload      — callback to reload after delete/restore
     *   opts.users, opts.period, opts.lastN, opts.filterVal — static display values
     */
    function buildIncome(container, data, opts) {
        injectCSS();
        opts = opts || {};
        /* Fast-path for chart mode: update existing Plotly chart in-place */
        var _incMode = (data && data.mode) || 'chart';
        if (_incMode === 'chart') {
            var _diChart = container.querySelector('.di-chart');
            if (_diChart && data && (data.traces || []).length > 0) {
                var _dr = container.querySelector('.dt-daterange');
                if (_dr) _dr.textContent = (data.from_date && data.to_date) ? 'From: ' + data.from_date + '  To: ' + data.to_date : '';
                var _plotTraces = (data.traces || []).map(function(t) {
                    return { x: t.x, y: t.y, name: t.name, type: 'scatter', mode: 'lines', showlegend: true };
                });
                var _layout = {
                    paper_bgcolor: '#0e1117', plot_bgcolor: '#0e1117',
                    font: { color: '#e2e8f0', size: 11 }, margin: { l: 55, r: 15, t: 40, b: 40 },
                    autosize: true,
                    xaxis: { gridcolor: '#2d3748', color: '#e2e8f0' },
                    yaxis: { gridcolor: '#2d3748', color: '#e2e8f0', zeroline: true, zerolinecolor: '#4a5568' },
                    legend: { bgcolor: 'rgba(0,0,0,0)', font: { size: 10, color: '#e2e8f0' } },
                    transition: { duration: 0, easing: 'linear' }
                };
                /* Preserve current zoom state across WS-triggered data updates */
                if (_diChart.layout) {
                    var _xa = _diChart.layout.xaxis || {}, _ya = _diChart.layout.yaxis || {};
                    if (_xa.autorange === false && _xa.range) {
                        _layout.xaxis.range = _xa.range.slice();
                        _layout.xaxis.autorange = false;
                    }
                    if (_ya.autorange === false && _ya.range) {
                        _layout.yaxis.range = _ya.range.slice();
                        _layout.yaxis.autorange = false;
                    }
                }
                if (typeof Plotly !== 'undefined') { Plotly.react(_diChart, _plotTraces, _layout, { responsive: true, displayModeBar: false }); }
                return;
            }
        }
        /* Save table scroll position before rebuilding DOM */
        var _savedScroll = 0;
        var _oldWrap = container.querySelector('.di-table-wrap');
        if (_oldWrap) _savedScroll = _oldWrap.scrollTop;
        container.innerHTML = '';

        var root = document.createElement('div');
        root.className = 'di-root';

        /* ── header ── */
        var hdr = document.createElement('div');
        hdr.className = 'dt-header';
        var titleSpan = document.createElement('span');
        titleSpan.className = 'dt-title';
        titleSpan.textContent = 'Income';
        hdr.appendChild(titleSpan);

        if (opts.periodControl || opts.usersControl || opts.lastNControl || opts.filterControl) {
            /* interactive controls */
            var metaDiv = document.createElement('div');
            metaDiv.className = 'dt-meta dt-meta-controls';
            /* Live vs BT button — leftmost item, before Period */
            if (opts.lvbUser) {
                var navBtnI = document.createElement('button');
                navBtnI.className = 'di-nav-btn';
                navBtnI.title = 'Open Live vs Backtest';
                navBtnI.textContent = '\u21C4 Live vs BT';
                navBtnI.addEventListener('click', _makeNavHandler(opts));
                metaDiv.appendChild(navBtnI);
                var sepNav = document.createElement('span');
                sepNav.className = 'dt-meta-sep'; sepNav.innerHTML = '&middot;';
                metaDiv.appendChild(sepNav);
            }
            if (opts.periodControl) {
                var lPrd = document.createElement('span');
                lPrd.className = 'dt-meta-lbl'; lPrd.textContent = 'Period';
                metaDiv.appendChild(lPrd);
                metaDiv.appendChild(opts.periodControl);
            }
            if (opts.fromControl) {
                var sepF = document.createElement('span');
                sepF.className = 'dt-meta-sep'; sepF.innerHTML = '&middot;';
                metaDiv.appendChild(sepF);
                var lFrom = document.createElement('span');
                lFrom.className = 'dt-meta-lbl'; lFrom.textContent = 'From';
                metaDiv.appendChild(lFrom);
                metaDiv.appendChild(opts.fromControl);
            }
            if (opts.toControl) {
                var lTo = document.createElement('span');
                lTo.className = 'dt-meta-lbl'; lTo.textContent = 'To';
                metaDiv.appendChild(lTo);
                metaDiv.appendChild(opts.toControl);
                if (opts.toNowControl) metaDiv.appendChild(opts.toNowControl);
            }
            if (opts.lastNControl) {
                var sepL = document.createElement('span');
                sepL.className = 'dt-meta-sep'; sepL.innerHTML = '&middot;';
                metaDiv.appendChild(sepL);
                var lLast = document.createElement('span');
                lLast.className = 'dt-meta-lbl'; lLast.textContent = 'Last N';
                metaDiv.appendChild(lLast);
                metaDiv.appendChild(opts.lastNControl);
            }
            if (opts.filterControl) {
                var sepFi = document.createElement('span');
                sepFi.className = 'dt-meta-sep'; sepFi.innerHTML = '&middot;';
                metaDiv.appendChild(sepFi);
                var lFilt = document.createElement('span');
                lFilt.className = 'dt-meta-lbl'; lFilt.textContent = 'Filter';
                metaDiv.appendChild(lFilt);
                metaDiv.appendChild(opts.filterControl);
            }
            if (opts.usersControl) {
                var sepU = document.createElement('span');
                sepU.className = 'dt-meta-sep'; sepU.innerHTML = '&middot;';
                metaDiv.appendChild(sepU);
                var lUsr = document.createElement('span');
                lUsr.className = 'dt-meta-lbl'; lUsr.textContent = 'Users';
                metaDiv.appendChild(lUsr);
                metaDiv.appendChild(opts.usersControl);
            } else if (opts.users && opts.users.length > 0) {
                var sepU2 = document.createElement('span');
                sepU2.className = 'dt-meta-sep'; sepU2.innerHTML = '&middot;';
                metaDiv.appendChild(sepU2);
                var lUsr2 = document.createElement('span');
                lUsr2.className = 'dt-meta-lbl'; lUsr2.textContent = 'Users';
                metaDiv.appendChild(lUsr2);
                var usrVal = document.createElement('span');
                usrVal.className = 'dt-meta-user';
                var _uLabel = (opts.users.length === 1 && opts.users[0] === 'ALL')
                    ? 'ALL' : opts.users.join(', ');
                usrVal.textContent = _uLabel;
                metaDiv.appendChild(usrVal);
            }
            hdr.appendChild(metaDiv);
        } else {
            /* static meta text */
            var uLabel = (opts.users && opts.users.length > 0 &&
                          !(opts.users.length === 1 && opts.users[0] === 'ALL'))
                ? opts.users.join(', ') : 'ALL';
            var _rawPeriod = (opts.period || '');
            var _periodDisplay = (_rawPeriod.indexOf('CUSTOM:') === 0)
                ? (function () {
                      var _pp = _rawPeriod.split(':');
                      var _toDisp = (_pp[2] === 'NOW' || _pp[2] === '') ? 'Now' : _pp[2];
                      return _pp[1] + ' \u2192 ' + _toDisp;
                  }())
                : _rawPeriod;
            var metaSpan = document.createElement('span');
            metaSpan.className = 'dt-meta';
            var metaHtml = '';
            if (opts.lvbUser) {
                metaHtml += '<button class="di-nav-btn di-nav-btn-static" title="Open Live vs Backtest">\u21C4 Live vs BT</button><span class="dt-meta-sep">&middot;</span>';
            }
            metaSpan.innerHTML = metaHtml +
                'Period:&nbsp;' + _periodDisplay +
                '&nbsp;&middot;&nbsp;Last&nbsp;N:&nbsp;' + (opts.lastN || 0) +
                '&nbsp;&middot;&nbsp;Filter:&nbsp;' + (opts.filterVal || 0) +
                '&nbsp;&middot;&nbsp;Users:&nbsp;<span class="dt-meta-user">' + uLabel + '</span>';
            if (opts.lvbUser) {
                var _staticBtn = metaSpan.querySelector('.di-nav-btn-static');
                if (_staticBtn) _staticBtn.addEventListener('click', _makeNavHandler(opts));
            }
            hdr.appendChild(metaSpan);
        }
        _decorateHeader(hdr, opts.icon, titleSpan, opts.onDelete);
        root.appendChild(hdr);

        /* date range */
        var dr = document.createElement('div');
        dr.className = 'dt-daterange';
        if (data && data.from_date && data.to_date) {
            dr.textContent = 'From: ' + data.from_date + '  To: ' + data.to_date;
        }
        root.appendChild(dr);

        var mode = (data && data.mode) || 'chart';
        var origHeight = opts.height || null;

        if (mode === 'table') {
            _buildIncomeTable(root, data, opts);
        } else {
            _buildIncomeChart(root, data, opts);
        }

        container.appendChild(root);

        /* Restore table scroll position after DOM rebuild */
        if (_savedScroll && mode === 'table') {
            var _newWrap = container.querySelector('.di-table-wrap');
            if (_newWrap) _newWrap.scrollTop = _savedScroll;
        }
    }

    /* ── Income: Table mode ── */
    function _buildIncomeTable(root, data, opts) {
        var rows = (data && data.rows) || [];
        if (rows.length === 0) {
            var noData = document.createElement('div');
            noData.className = 'dt-nodata';
            noData.textContent = 'No data for the selected period.';
            root.appendChild(noData);
            return;
        }

        /* state */
        var selected = {};      /* id → true */
        var sortCol = 'date';   /* current sort column key */
        var sortAsc = false;    /* current sort direction */
        var sortedRows = rows.slice(); /* working copy */

        var wrap = document.createElement('div');
        wrap.className = 'di-table-wrap';
        var table = document.createElement('table');
        table.className = 'di-table';

        /* columns definition */
        var cols = [
            { key: 'sel',    label: '', sortable: false },
            { key: 'date',   label: 'Date', sortable: true },
            { key: 'user',   label: 'User', sortable: true },
            { key: 'symbol', label: 'Symbol', sortable: true },
            { key: 'income', label: 'Income', sortable: true }
        ];

        function renderTable() {
            table.innerHTML = '';
            /* thead */
            var thead = document.createElement('thead');
            var hrow = document.createElement('tr');
            cols.forEach(function (c) {
                var th = document.createElement('th');
                if (c.key === 'sel') {
                    var cbAll = document.createElement('input');
                    cbAll.type = 'checkbox';
                    cbAll.checked = sortedRows.length > 0 && sortedRows.every(function (r) { return !!selected[r.id]; });
                    cbAll.addEventListener('change', function () {
                        sortedRows.forEach(function (r) {
                            if (cbAll.checked) selected[r.id] = true;
                            else delete selected[r.id];
                        });
                        renderTable();
                        updateActions();
                    });
                    th.appendChild(cbAll);
                } else {
                    th.textContent = c.label;
                    if (c.key === 'date') {
                        var jumpTh = th;
                        var jumpInput = document.createElement('input');
                        jumpInput.type = 'date';
                        jumpInput.className = 'di-jump-input';
                        jumpInput.title = 'Go to date';
                        jumpInput.addEventListener('click', function (e) { e.stopPropagation(); });
                        var _jumpDebounce = null;
                        jumpInput.addEventListener('change', (function (ji) {
                            return function (e) {
                                e.stopPropagation();
                                var target = ji.value;
                                if (!target) return;
                                var trs = table.querySelectorAll('tbody tr');
                                var theadH = table.querySelector('thead') ? table.querySelector('thead').offsetHeight : 0;
                                function scrollToRow(tr) {
                                    var rTop = tr.getBoundingClientRect().top;
                                    var wTop = wrap.getBoundingClientRect().top;
                                    wrap.scrollTop = wrap.scrollTop + (rTop - wTop) - theadH;
                                }
                                /* exact match → scroll immediately */
                                for (var ii = 0; ii < sortedRows.length; ii++) {
                                    if (sortedRows[ii].date.slice(0, 10) === target) {
                                        scrollToRow(trs[ii]);
                                        if (_jumpDebounce) { clearTimeout(_jumpDebounce); _jumpDebounce = null; }
                                        return;
                                    }
                                }
                                /* date not in current rows → debounced reload */
                                if (typeof opts.onJumpToDate === 'function') {
                                    if (_jumpDebounce) clearTimeout(_jumpDebounce);
                                    _jumpDebounce = setTimeout(function () {
                                        _jumpDebounce = null;
                                        if (ji.value === target) opts.onJumpToDate(target);
                                    }, 600);
                                    return;
                                }
                                /* fallback: scroll to closest entry */
                                var targetMs = new Date(target).getTime();
                                var bestIdx = 0, bestDiff = Infinity;
                                for (var jj = 0; jj < sortedRows.length; jj++) {
                                    var diff2 = Math.abs(new Date(sortedRows[jj].date.slice(0, 10)).getTime() - targetMs);
                                    if (diff2 < bestDiff) { bestDiff = diff2; bestIdx = jj; }
                                }
                                if (trs[bestIdx]) scrollToRow(trs[bestIdx]);
                            };
                        })(jumpInput));
                        /* appended after sort arrow below */
                    }
                    if (c.sortable) {
                        var arrow = document.createElement('span');
                        arrow.className = 'di-sort';
                        if (sortCol === c.key) arrow.textContent = sortAsc ? ' \u25B2' : ' \u25BC';
                        th.appendChild(arrow);
                        if (c.key === 'date' && jumpInput) th.appendChild(jumpInput);
                        th.addEventListener('click', (function (ck) {
                            return function () {
                                if (sortCol === ck) sortAsc = !sortAsc;
                                else { sortCol = ck; sortAsc = true; }
                                doSort();
                                renderTable();
                            };
                        })(c.key));
                    }
                }
                hrow.appendChild(th);
            });
            thead.appendChild(hrow);
            table.appendChild(thead);

            /* tbody */
            var tbody = document.createElement('tbody');
            sortedRows.forEach(function (r) {
                var tr = document.createElement('tr');
                if (selected[r.id]) tr.className = 'di-sel';
                /* checkbox */
                var tdCb = document.createElement('td');
                var cb = document.createElement('input');
                cb.type = 'checkbox';
                cb.checked = !!selected[r.id];
                cb.addEventListener('change', function () {
                    if (cb.checked) selected[r.id] = true;
                    else delete selected[r.id];
                    tr.className = cb.checked ? 'di-sel' : '';
                    updateActions();
                    /* update header checkbox */
                    var hcb = table.querySelector('thead input[type=checkbox]');
                    if (hcb) hcb.checked = sortedRows.every(function (r2) { return !!selected[r2.id]; });
                });
                tdCb.appendChild(cb);
                tr.appendChild(tdCb);
                /* date */
                var tdD = document.createElement('td');
                tdD.textContent = r.date;
                tr.appendChild(tdD);
                /* user */
                var tdU = document.createElement('td');
                tdU.textContent = r.user;
                tr.appendChild(tdU);
                /* symbol */
                var tdS = document.createElement('td');
                tdS.textContent = r.symbol;
                tr.appendChild(tdS);
                /* income */
                var tdI = document.createElement('td');
                tdI.textContent = r.income.toFixed(2);
                tdI.className = r.income >= 0 ? 'di-inc-pos' : 'di-inc-neg';
                tr.appendChild(tdI);

                tbody.appendChild(tr);
            });
            table.appendChild(tbody);
        }

        function doSort() {
            sortedRows.sort(function (a, b) {
                var va = a[sortCol], vb = b[sortCol];
                if (typeof va === 'string') {
                    return sortAsc ? va.localeCompare(vb) : vb.localeCompare(va);
                }
                return sortAsc ? va - vb : vb - va;
            });
        }

        renderTable();

        wrap.appendChild(table);
        root.appendChild(wrap);

        /* ── action bar ── */
        var actionsDiv = document.createElement('div');
        actionsDiv.className = 'di-actions';
        actionsDiv.style.display = 'none';

        var btnDeleteSel = document.createElement('button');
        btnDeleteSel.className = 'di-btn di-btn-danger';
        btnDeleteSel.textContent = 'Delete selected\u2026';
        btnDeleteSel.addEventListener('click', function () {
            var ids = Object.keys(selected).map(Number);
            showConfirm('Delete ' + ids.length + ' selected income row(s)?', function () {
                apiPost('/income/delete_ids', { ids: ids });
            });
        });
        actionsDiv.appendChild(btnDeleteSel);

        var btnDeleteOlder = document.createElement('button');
        btnDeleteOlder.className = 'di-btn di-btn-danger';
        btnDeleteOlder.textContent = 'Delete older than selected\u2026';
        btnDeleteOlder.addEventListener('click', function () {
            var ids = Object.keys(selected).map(Number);
            var minMs = Infinity;
            var selUsers = {};
            sortedRows.forEach(function (r) {
                if (selected[r.id]) {
                    if (r.date_ms < minMs) minMs = r.date_ms;
                    selUsers[r.user] = true;
                }
            });
            var cutoffDate = new Date(minMs).toISOString().replace('T', ' ').slice(0, 19);
            var userList = Object.keys(selUsers);
            var usersParam = (opts.users && opts.users.indexOf('ALL') >= 0) ? ['ALL'] : userList;
            showConfirm(
                'Delete all income for ' + (usersParam[0] === 'ALL' ? 'ALL users' : usersParam.join(', ')) +
                ' with timestamp \u2264 ' + cutoffDate + '?',
                function () {
                    apiPost('/income/delete_older', { users: usersParam, cutoff_ms: minMs });
                }
            );
        });
        actionsDiv.appendChild(btnDeleteOlder);

        /* backup/restore toggle */
        var btnBackup = document.createElement('button');
        btnBackup.className = 'di-btn';
        btnBackup.textContent = 'Backup / Restore\u2026';
        btnBackup.addEventListener('click', function () {
            loadBackups();
        });
        actionsDiv.appendChild(btnBackup);

        root.appendChild(actionsDiv);

        /* backup panel (hidden by default) */
        var backupDiv = document.createElement('div');
        backupDiv.className = 'di-backup';
        backupDiv.style.display = 'none';
        root.appendChild(backupDiv);

        /* confirm overlay (hidden) */
        var confirmDiv = document.createElement('div');
        confirmDiv.className = 'di-confirm';
        confirmDiv.style.display = 'none';
        root.appendChild(confirmDiv);

        /* status message */
        var statusDiv = document.createElement('div');
        statusDiv.className = 'di-status';
        statusDiv.style.display = 'none';
        root.appendChild(statusDiv);

        function updateActions() {
            var count = Object.keys(selected).length;
            actionsDiv.style.display = count > 0 ? 'flex' : 'none';
            if (count === 0) {
                backupDiv.style.display = 'none';
            }
        }

        function showConfirm(msg, onYes) {
            confirmDiv.style.display = 'flex';
            confirmDiv.innerHTML = '';
            var msgEl = document.createElement('div');
            msgEl.className = 'di-confirm-msg';
            msgEl.textContent = '\u26a0\ufe0f ' + msg;
            confirmDiv.appendChild(msgEl);
            var btns = document.createElement('div');
            btns.className = 'di-confirm-btns';
            var yesBtn = document.createElement('button');
            yesBtn.className = 'di-btn di-btn-yes';
            yesBtn.textContent = 'Yes';
            yesBtn.addEventListener('click', function () {
                confirmDiv.style.display = 'none';
                onYes();
            });
            var noBtn = document.createElement('button');
            noBtn.className = 'di-btn di-btn-no';
            noBtn.textContent = 'No';
            noBtn.addEventListener('click', function () {
                confirmDiv.style.display = 'none';
            });
            btns.appendChild(yesBtn);
            btns.appendChild(noBtn);
            confirmDiv.appendChild(btns);
        }

        function showStatus(msg) {
            statusDiv.textContent = msg;
            statusDiv.style.display = 'block';
            setTimeout(function () { statusDiv.style.display = 'none'; }, 4000);
        }

        function apiPost(path, body) {
            var url = (opts.apiBase || '') + '/dashboard' + path
                + '?token=' + encodeURIComponent(opts.token || '');
            fetch(url, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'Authorization': 'Bearer ' + (opts.token || '')
                },
                body: JSON.stringify(body)
            })
            .then(function (r) { return r.json(); })
            .then(function (d) {
                showStatus('Deleted ' + (d.deleted || 0) + ' row(s). Backup created.');
                selected = {};
                updateActions();
                if (opts.onReload) setTimeout(opts.onReload, 500);
            })
            .catch(function (e) {
                showStatus('Error: ' + e.message);
            });
        }

        function loadBackups() {
            backupDiv.style.display = 'block';
            backupDiv.innerHTML = '<span style="color:#94a3b8;font-size:0.73rem;">Loading backups\u2026</span>';
            var url = (opts.apiBase || '') + '/dashboard/income/backups'
                + '?token=' + encodeURIComponent(opts.token || '');
            fetch(url, { headers: { 'Authorization': 'Bearer ' + (opts.token || '') } })
            .then(function (r) { return r.json(); })
            .then(function (d) {
                backupDiv.innerHTML = '';
                var backups = d.backups || [];
                if (backups.length === 0) {
                    backupDiv.innerHTML = '<span style="color:#94a3b8;font-size:0.73rem;">No backups available.</span>';
                    return;
                }
                var lbl = document.createElement('span');
                lbl.className = 'dt-meta-lbl';
                lbl.textContent = 'Restore from:';
                backupDiv.appendChild(lbl);
                var sel = document.createElement('select');
                backups.forEach(function (b) {
                    var opt = document.createElement('option');
                    opt.value = b.path;
                    opt.textContent = b.name + ' \u2014 ' + b.date;
                    sel.appendChild(opt);
                });
                backupDiv.appendChild(sel);
                var restoreBtn = document.createElement('button');
                restoreBtn.className = 'di-btn';
                restoreBtn.textContent = 'Restore';
                restoreBtn.addEventListener('click', function () {
                    showConfirm('Restore database from ' + sel.options[sel.selectedIndex].textContent + '?', function () {
                        var url2 = (opts.apiBase || '') + '/dashboard/income/restore'
                            + '?token=' + encodeURIComponent(opts.token || '');
                        fetch(url2, {
                            method: 'POST',
                            headers: {
                                'Content-Type': 'application/json',
                                'Authorization': 'Bearer ' + (opts.token || '')
                            },
                            body: JSON.stringify({ path: sel.value })
                        })
                        .then(function (r) { return r.json(); })
                        .then(function (d) {
                            if (d.ok) {
                                showStatus('Database restored successfully.');
                                backupDiv.style.display = 'none';
                                selected = {};
                                updateActions();
                                if (opts.onReload) setTimeout(opts.onReload, 500);
                            } else {
                                showStatus('Restore failed.');
                            }
                        })
                        .catch(function (e) { showStatus('Error: ' + e.message); });
                    });
                });
                backupDiv.appendChild(restoreBtn);
                var closeBtn2 = document.createElement('button');
                closeBtn2.className = 'di-btn';
                closeBtn2.textContent = '\u2715';
                closeBtn2.style.marginLeft = '0.3rem';
                closeBtn2.addEventListener('click', function () { backupDiv.style.display = 'none'; });
                backupDiv.appendChild(closeBtn2);
            })
            .catch(function (e) {
                backupDiv.textContent = '';
                var errSpan = document.createElement('span');
                errSpan.style.cssText = 'color:#f56565;font-size:0.73rem;';
                errSpan.textContent = 'Error loading backups';
                backupDiv.appendChild(errSpan);
            });
        }
    }

    /* ── Income: Chart mode ── */
    function _buildIncomeChart(root, data, opts) {
        var traces = (data && data.traces) || [];
        if (traces.length === 0) {
            var noData = document.createElement('div');
            noData.className = 'dt-nodata';
            noData.textContent = 'No data for the selected period.';
            root.appendChild(noData);
            return;
        }

        var chartDiv = document.createElement('div');
        chartDiv.className = 'di-chart';
        if (opts.height) chartDiv.style.height = opts.height + 'px';

        /* fullscreen close button */
        var closeBtn = document.createElement('button');
        closeBtn.className = 'dt-fs-close';
        closeBtn.textContent = '\u2715';
        closeBtn.title = 'Exit Fullscreen';
        closeBtn.addEventListener('click', function () {
            if (document.exitFullscreen) document.exitFullscreen();
            else if (document.webkitExitFullscreen) document.webkitExitFullscreen();
        });
        chartDiv.appendChild(closeBtn);

        root.appendChild(chartDiv);

        /* Plotly traces */
        var plotTraces = traces.map(function (t, i) {
            return {
                x: t.x,
                y: t.y,
                name: t.name,
                type: 'scatter',
                mode: 'lines',
                showlegend: true
            };
        });

        var origHeight = opts.height || null;
        var layout = {
            paper_bgcolor: '#0e1117',
            plot_bgcolor:  '#0e1117',
            font:          { color: '#e2e8f0', size: 11 },
            margin:        { l: 55, r: 15, t: 40, b: 40 },
            autosize:      true,
            xaxis:         { gridcolor: '#2d3748', color: '#e2e8f0' },
            yaxis:         { gridcolor: '#2d3748', color: '#e2e8f0',
                             zeroline: true, zerolinecolor: '#4a5568' },
            legend:        { bgcolor: 'rgba(0,0,0,0)', font: { size: 10, color: '#e2e8f0' } }
        };
        if (origHeight) { layout.height = origHeight; }

        /* fullscreen change: relayout or restore (same pattern as renderTop) */
        var fsHandler = function () {
            var root = chartDiv.closest ? chartDiv.closest('.di-root') : null;
            var isFull = root
                ? (document.fullscreenElement === root || document.webkitFullscreenElement === root)
                : !!(document.fullscreenElement || document.webkitFullscreenElement);
            closeBtn.style.display = isFull ? 'block' : 'none';
            if (isFull) {
                var fsW = window.screen.width  || window.innerWidth;
                var fsH = (window.screen.availHeight || window.innerHeight) - 62;
                Plotly.relayout(chartDiv, { width: fsW, height: fsH });
            } else {
                Plotly.relayout(chartDiv, { width: null, height: origHeight || null });
                setTimeout(function () { Plotly.Plots.resize(chartDiv); }, 100);
            }
        };
        document.addEventListener('fullscreenchange', fsHandler);
        document.addEventListener('webkitfullscreenchange', fsHandler);

        /* fullscreen button — same icon as renderTop */
        var plotCfg = {
            displayModeBar: opts.displayModeBar !== undefined ? opts.displayModeBar : false,
            responsive:     opts.responsive     !== undefined ? opts.responsive     : true,
            modeBarButtonsToAdd: [
                {
                    name:  'fullscreen',
                    title: 'Fullscreen',
                    icon:  {
                        width: 857.1, height: 857.1,
                        path: 'M0 0v285.7h142.9V142.9H285.7V0H0zm571.4 0v142.9h142.9v142.9H857.1V0H571.4zM0 571.4v285.7h285.7V714.3H142.9V571.4H0zm714.3 142.9v142.9H571.4v142.9H857.1V571.4H714.3z'
                    },
                    click: function (gd) {
                        var root = gd.closest ? gd.closest('.di-root') : gd.parentElement;
                        var isFull = (document.fullscreenElement === root ||
                                      document.webkitFullscreenElement === root);
                        if (!isFull) {
                            if (root.requestFullscreen) root.requestFullscreen();
                            else if (root.webkitRequestFullscreen) root.webkitRequestFullscreen();
                        } else {
                            if (document.exitFullscreen) document.exitFullscreen();
                            else if (document.webkitExitFullscreen) document.webkitExitFullscreen();
                        }
                    }
                }
            ]
        };

        layout.transition = { duration: 0, easing: 'linear' };
        if (typeof Plotly !== 'undefined') {
            Plotly.react(chartDiv, plotTraces, layout, plotCfg);
            /* Ensure chart fills container after layout settles */
            if (!opts.noResize) { setTimeout(function () { Plotly.Plots.resize(chartDiv); }, 80); }
        }
    }


    /* ──────────────────────── PNL widget ───────────────────────────────── */

    function renderPnl(chartDiv, data, opts) {
        opts = opts || {};
        var P = opts.Plotly || global.Plotly;
        if (!P) { chartDiv.textContent = 'Plotly not loaded'; return; }

        var bars = (data && data.bars) ? data.bars : [];
        var mode = (data && data.mode) || 'bar';

        var dates  = bars.map(function (b) { return b.date; });
        var values = bars.map(function (b) { return b.income; });
        var colors = values.map(function (v) { return v < 0 ? '#fc8181' : '#68d391'; });

        var trace;
        if (mode === 'line') {
            trace = {
                x: dates, y: values, type: 'scatter', mode: 'lines+markers',
                line: { color: '#63b3ed', width: 1 },
                marker: { color: colors, size: 6 },
                hovertemplate: '<b>%{x}</b><br>Income: %{y:.2f}<extra></extra>'
            };
        } else {
            trace = {
                x: dates, y: values, type: 'bar',
                marker: { color: colors },
                text: values.map(function (v) { return v.toFixed(2); }),
                textposition: 'auto',
                hovertemplate: '<b>%{x}</b><br>Income: %{y:.2f}<extra></extra>'
            };
        }

        var layout = {
            paper_bgcolor: '#0e1117',
            plot_bgcolor:  '#0e1117',
            font:   { color: '#e2e8f0', size: 11 },
            margin: { l: 50, r: 20, t: 40, b: 50 },
            xaxis:  { tickangle: -45, gridcolor: '#2d3748', color: '#e2e8f0',
                      type: 'date' },
            yaxis:  { gridcolor: '#2d3748', color: '#e2e8f0',
                      zeroline: true, zerolinecolor: '#4a5568' },
            bargap: 0.3,
            autosize: true
        };
        var origHeight = opts.height || null;
        if (origHeight) { layout.height = origHeight; }

        /* restore zoom if user had zoomed before a mode switch */
        if (opts.savedZoom) {
            if (opts.savedZoom.xrange) {
                layout.xaxis.range      = opts.savedZoom.xrange;
                layout.xaxis.autorange  = false;
            }
            if (opts.savedZoom.yrange) {
                layout.yaxis.range      = opts.savedZoom.yrange;
                layout.yaxis.autorange  = false;
            }
        }

        var fschangeHandler = function () {
            var root = chartDiv.closest ? chartDiv.closest('.dt-root') : null;
            var isFull = root
                ? (document.fullscreenElement === root || document.webkitFullscreenElement === root)
                : !!(document.fullscreenElement || document.webkitFullscreenElement);
            var closeBtn = root ? root.querySelector('.dt-fs-close') : null;
            if (closeBtn) { closeBtn.style.display = isFull ? 'block' : 'none'; }
            if (isFull) {
                var fsW = window.screen.width  || window.innerWidth;
                var fsH = (window.screen.availHeight || window.innerHeight) - 62;
                P.relayout(chartDiv, { width: fsW, height: fsH });
            } else {
                P.relayout(chartDiv, { width: null, height: origHeight || null });
                setTimeout(function () { P.Plots.resize(chartDiv); }, 100);
            }
        };
        document.addEventListener('fullscreenchange', fschangeHandler);
        document.addEventListener('webkitfullscreenchange', fschangeHandler);

        var cfg = {
            displayModeBar: opts.displayModeBar !== undefined ? opts.displayModeBar : false,
            responsive:     opts.responsive     !== undefined ? opts.responsive     : true,
            modeBarButtonsToAdd: [
                {
                    name:  'fullscreen',
                    title: 'Fullscreen',
                    icon:  {
                        width: 857.1, height: 857.1,
                        path: 'M0 0v285.7h142.9V142.9H285.7V0H0zm571.4 0v142.9h142.9v142.9H857.1V0H571.4zM0 571.4v285.7h285.7V714.3H142.9V571.4H0zm714.3 142.9v142.9H571.4v142.9H857.1V571.4H714.3z'
                    },
                    click: function (gd) {
                        var root = gd.closest ? gd.closest('.dt-root') : gd.parentElement;
                        var isFull = (document.fullscreenElement === root ||
                                      document.webkitFullscreenElement === root);
                        if (!isFull) {
                            if (root.requestFullscreen) root.requestFullscreen();
                            else if (root.webkitRequestFullscreen) root.webkitRequestFullscreen();
                        } else {
                            if (document.exitFullscreen) document.exitFullscreen();
                            else if (document.webkitExitFullscreen) document.webkitExitFullscreen();
                        }
                    }
                }
            ]
        };
        layout.transition = { duration: 0, easing: 'linear' };
        P.react(chartDiv, [trace], layout, cfg);
        if (!opts.noResize) { setTimeout(function () { P.Plots.resize(chartDiv); }, 80); }
    }

    function buildPnl(container, data, opts) {
        injectCSS();
        opts = opts || {};
        /* Fast-path: if chart already rendered, update in-place via Plotly.react */
        var _fc = container.querySelector('.dt-chart');
        if (_fc && data && (data.bars || []).length > 0) {
            var _dr = container.querySelector('.dt-daterange');
            if (_dr) _dr.textContent = (data.from_date && data.to_date) ? 'From: ' + data.from_date + '  To: ' + data.to_date : '';
            /* Preserve current zoom state across WS-triggered data updates */
            var _z = opts.savedZoom || null;
            if (!_z && _fc.layout) {
                var _xa = _fc.layout.xaxis || {}, _ya = _fc.layout.yaxis || {};
                var _xr = (_xa.autorange === false && _xa.range) ? _xa.range.slice() : null;
                var _yr = (_ya.autorange === false && _ya.range) ? _ya.range.slice() : null;
                if (_xr || _yr) _z = { xrange: _xr, yrange: _yr };
            }
            renderPnl(_fc, data, { noResize: true, savedZoom: _z });
            return;
        }
        container.innerHTML = '';

        var root = document.createElement('div');
        root.className = 'dt-root';

        /* header */
        var hdr = document.createElement('div');
        hdr.className = 'dt-header';
        var titleSpan = document.createElement('span');
        titleSpan.className = 'dt-title';
        titleSpan.textContent = 'Daily PNL';
        hdr.appendChild(titleSpan);

        if (opts.modeControl || opts.periodControl || opts.usersControl) {
            var metaDiv = document.createElement('div');
            metaDiv.className = 'dt-meta dt-meta-controls';
            if (opts.modeControl) {
                var lMode = document.createElement('span');
                lMode.className = 'dt-meta-lbl'; lMode.textContent = 'Mode';
                metaDiv.appendChild(lMode);
                metaDiv.appendChild(opts.modeControl);
            }
            if (opts.periodControl) {
                var sepP = document.createElement('span');
                sepP.className = 'dt-meta-sep'; sepP.innerHTML = '&middot;';
                metaDiv.appendChild(sepP);
                var lPrd = document.createElement('span');
                lPrd.className = 'dt-meta-lbl'; lPrd.textContent = 'Period';
                metaDiv.appendChild(lPrd);
                metaDiv.appendChild(opts.periodControl);
            }
            if (opts.fromControl) {
                var sepF = document.createElement('span');
                sepF.className = 'dt-meta-sep'; sepF.innerHTML = '&middot;';
                metaDiv.appendChild(sepF);
                var lFrom = document.createElement('span');
                lFrom.className = 'dt-meta-lbl'; lFrom.textContent = 'From';
                metaDiv.appendChild(lFrom);
                metaDiv.appendChild(opts.fromControl);
            }
            if (opts.toControl) {
                var lTo = document.createElement('span');
                lTo.className = 'dt-meta-lbl'; lTo.textContent = 'To';
                metaDiv.appendChild(lTo);
                metaDiv.appendChild(opts.toControl);
                if (opts.toNowControl) {
                    metaDiv.appendChild(opts.toNowControl);
                }
            }
            if (opts.usersControl) {
                var sepU = document.createElement('span');
                sepU.className = 'dt-meta-sep'; sepU.innerHTML = '&middot;';
                metaDiv.appendChild(sepU);
                var lUsr = document.createElement('span');
                lUsr.className = 'dt-meta-lbl'; lUsr.textContent = 'Users';
                metaDiv.appendChild(lUsr);
                metaDiv.appendChild(opts.usersControl);
            } else if (opts.users && opts.users.length > 0) {
                var sepU2 = document.createElement('span');
                sepU2.className = 'dt-meta-sep'; sepU2.innerHTML = '&middot;';
                metaDiv.appendChild(sepU2);
                var lUsr2 = document.createElement('span');
                lUsr2.className = 'dt-meta-lbl'; lUsr2.textContent = 'Users';
                metaDiv.appendChild(lUsr2);
                var usrVal = document.createElement('span');
                usrVal.className = 'dt-meta-user';
                var _uLabel = (opts.users.length === 1 && opts.users[0] === 'ALL')
                    ? 'ALL' : opts.users.join(', ');
                usrVal.textContent = _uLabel;
                metaDiv.appendChild(usrVal);
            }
            hdr.appendChild(metaDiv);
        } else {
            var uLabel = (opts.users && opts.users.length > 0 &&
                          !(opts.users.length === 1 && opts.users[0] === 'ALL'))
                ? opts.users.join(', ') : 'ALL';
            var _rawPeriod = (opts.period || '');
            var _periodDisplay = (_rawPeriod.indexOf('CUSTOM:') === 0)
                ? (function () {
                      var _pp = _rawPeriod.split(':');
                      var _toDisp = (_pp[2] === 'NOW' || _pp[2] === '') ? 'Now' : _pp[2];
                      return _pp[1] + ' \u2192 ' + _toDisp;
                  }())
                : _rawPeriod;
            var metaSpan = document.createElement('span');
            metaSpan.className = 'dt-meta';
            metaSpan.innerHTML =
                'Mode:&nbsp;' + ((data && data.mode) || 'bar') +
                '&nbsp;&middot;&nbsp;Period:&nbsp;' + _periodDisplay +
                '&nbsp;&middot;&nbsp;Users:&nbsp;<span class="dt-meta-user">' + uLabel + '</span>';
            hdr.appendChild(metaSpan);
        }
        _decorateHeader(hdr, opts.icon, titleSpan, opts.onDelete);
        root.appendChild(hdr);

        /* date range */
        var dr = document.createElement('div');
        dr.className = 'dt-daterange';
        if (data && data.from_date && data.to_date) {
            dr.textContent = 'From: ' + data.from_date + '  To: ' + data.to_date;
        }
        root.appendChild(dr);

        var bars = (data && data.bars) ? data.bars : [];
        if (bars.length === 0) {
            var noData = document.createElement('div');
            noData.className = 'dt-nodata';
            noData.textContent = 'No data for the selected period.';
            root.appendChild(noData);
            container.appendChild(root);
            return;
        }

        /* chart container */
        var chartDiv = document.createElement('div');
        chartDiv.className = 'dt-chart';
        if (opts.height) { chartDiv.style.height = opts.height + 'px'; }

        var closeBtn = document.createElement('button');
        closeBtn.className = 'dt-fs-close';
        closeBtn.textContent = '\u2715';
        closeBtn.title = 'Exit Fullscreen';
        closeBtn.addEventListener('click', function () {
            if (document.exitFullscreen) document.exitFullscreen();
            else if (document.webkitExitFullscreen) document.webkitExitFullscreen();
        });
        chartDiv.appendChild(closeBtn);

        root.appendChild(chartDiv);
        container.appendChild(root);

        renderPnl(chartDiv, data, opts);
    }


    /* ───────── P+L (Profits & Losses) — stacked bar chart ─────────── */

    function renderPpl(chartDiv, data, opts) {
        opts = opts || {};
        var P = opts.Plotly || global.Plotly;
        if (!P) { chartDiv.textContent = 'Plotly not loaded'; return; }

        var bars = (data && data.bars) ? data.bars : [];

        var periods = bars.map(function (b) { return b.period; });
        var profits = bars.map(function (b) { return b.profits; });
        var losses  = bars.map(function (b) { return b.losses; });

        var profitTrace = {
            x: periods, y: profits, type: 'bar', name: 'Profits',
            marker: { color: '#48bb78' },
            text: profits.map(function (v) { return v === 0 ? '' : v.toFixed(2); }),
            textposition: 'outside',
            hovertemplate: '<b>%{x}</b><br>Profits: %{y:.2f}<extra></extra>'
        };
        var lossTrace = {
            x: periods, y: losses, type: 'bar', name: 'Losses',
            marker: { color: '#f56565' },
            text: losses.map(function (v) { return v === 0 ? '' : v.toFixed(2); }),
            textposition: 'outside',
            hovertemplate: '<b>%{x}</b><br>Losses: %{y:.2f}<extra></extra>'
        };

        /* Y-axis range with 10% padding; guard against yRange=0 */
        var allVals = profits.concat(losses);
        var yMin = Math.min.apply(null, allVals);
        var yMax = Math.max.apply(null, allVals);
        var yRange = yMax - yMin;
        var padding = yRange > 0 ? yRange * 0.10
                                 : (Math.max(Math.abs(yMin), Math.abs(yMax)) * 0.2 || 1);

        var layout = {
            paper_bgcolor: '#0e1117',
            plot_bgcolor:  '#0e1117',
            font:   { color: '#e2e8f0', size: 11 },
            margin: { l: 50, r: 20, t: 40, b: 50 },
            barmode: 'relative',
            xaxis:  { tickangle: -45, gridcolor: '#2d3748', color: '#e2e8f0',
                      type: 'category', nticks: 20 },
            yaxis:  { gridcolor: '#2d3748', color: '#e2e8f0',
                      zeroline: true, zerolinecolor: '#4a5568',
                      range: [yMin - padding, yMax + padding] },
            bargap: 0.3,
            legend: { font: { color: '#e2e8f0' } },
            autosize: true
        };

        var origHeight = opts.height || null;
        if (origHeight) { layout.height = origHeight; }

        /* restore zoom */
        if (opts.savedZoom) {
            if (opts.savedZoom.fracRange && periods.length > 0) {
                /* Proportional remap — used on sum-period switch (DAY↔WEEK↔MONTH).
                   fracRange stores [xr[0]/n, xr[1]/n]; multiply back by new n. */
                var m = periods.length;
                var newLo = opts.savedZoom.fracRange[0] * m;
                var newHi = opts.savedZoom.fracRange[1] * m;
                if (newHi > newLo && newHi > 0 && newLo < m) {
                    layout.xaxis.range     = [Math.max(-0.5, newLo), Math.min(m - 0.5, newHi)];
                    layout.xaxis.autorange = false;
                }
                /* Y-axis: not restored — aggregation level changes values scale */
            } else {
                if (opts.savedZoom.xrange) {
                    layout.xaxis.range     = opts.savedZoom.xrange;
                    layout.xaxis.autorange = false;
                }
                if (opts.savedZoom.yrange) {
                    layout.yaxis.range     = opts.savedZoom.yrange;
                    layout.yaxis.autorange = false;
                }
            }
        }

        var fschangeHandler = function () {
            var root = chartDiv.closest ? chartDiv.closest('.dt-root') : null;
            var isFull = root
                ? (document.fullscreenElement === root || document.webkitFullscreenElement === root)
                : !!(document.fullscreenElement || document.webkitFullscreenElement);
            var closeBtn = root ? root.querySelector('.dt-fs-close') : null;
            if (closeBtn) { closeBtn.style.display = isFull ? 'block' : 'none'; }
            if (isFull) {
                var fsW = window.screen.width  || window.innerWidth;
                var fsH = (window.screen.availHeight || window.innerHeight) - 62;
                P.relayout(chartDiv, { width: fsW, height: fsH });
            } else {
                P.relayout(chartDiv, { width: null, height: origHeight || null });
                setTimeout(function () { P.Plots.resize(chartDiv); }, 100);
            }
        };
        document.addEventListener('fullscreenchange', fschangeHandler);
        document.addEventListener('webkitfullscreenchange', fschangeHandler);

        var cfg = {
            displayModeBar: opts.displayModeBar !== undefined ? opts.displayModeBar : false,
            responsive:     opts.responsive     !== undefined ? opts.responsive     : true,
            modeBarButtonsToAdd: [
                {
                    name:  'fullscreen',
                    title: 'Fullscreen',
                    icon:  {
                        width: 857.1, height: 857.1,
                        path: 'M0 0v285.7h142.9V142.9H285.7V0H0zm571.4 0v142.9h142.9v142.9H857.1V0H571.4zM0 571.4v285.7h285.7V714.3H142.9V571.4H0zm714.3 142.9v142.9H571.4v142.9H857.1V571.4H714.3z'
                    },
                    click: function (gd) {
                        var root = gd.closest ? gd.closest('.dt-root') : gd.parentElement;
                        var isFull = (document.fullscreenElement === root ||
                                      document.webkitFullscreenElement === root);
                        if (!isFull) {
                            if (root.requestFullscreen) root.requestFullscreen();
                            else if (root.webkitRequestFullscreen) root.webkitRequestFullscreen();
                        } else {
                            if (document.exitFullscreen) document.exitFullscreen();
                            else if (document.webkitExitFullscreen) document.webkitExitFullscreen();
                        }
                    }
                }
            ]
        };
        layout.transition = { duration: 0, easing: 'linear' };
        P.react(chartDiv, [profitTrace, lossTrace], layout, cfg);
        if (!opts.noResize) { setTimeout(function () { P.Plots.resize(chartDiv); }, 80); }
    }

    function buildPpl(container, data, opts) {
        injectCSS();
        opts = opts || {};
        /* Fast-path: if chart already rendered, update in-place via Plotly.react */
        var _fc = container.querySelector('.dt-chart');
        if (_fc && data && (data.bars || []).length > 0) {
            var _dr = container.querySelector('.dt-daterange');
            if (_dr) _dr.textContent = (data.from_date && data.to_date) ? 'From: ' + data.from_date + '  To: ' + data.to_date : '';
            /* Preserve current zoom state across WS-triggered data updates */
            var _z = opts.savedZoom || null;
            if (!_z && _fc.layout) {
                var _xa = _fc.layout.xaxis || {}, _ya = _fc.layout.yaxis || {};
                var _xr = (_xa.autorange === false && _xa.range) ? _xa.range.slice() : null;
                var _yr = (_ya.autorange === false && _ya.range) ? _ya.range.slice() : null;
                if (_xr || _yr) _z = { xrange: _xr, yrange: _yr };
            }
            renderPpl(_fc, data, { noResize: true, savedZoom: _z });
            return;
        }
        container.innerHTML = '';

        var root = document.createElement('div');
        root.className = 'dt-root';

        /* header */
        var hdr = document.createElement('div');
        hdr.className = 'dt-header';
        var titleSpan = document.createElement('span');
        titleSpan.className = 'dt-title';
        titleSpan.textContent = 'Profits and Losses';
        hdr.appendChild(titleSpan);

        if (opts.sumPeriodControl || opts.periodControl || opts.usersControl) {
            var metaDiv = document.createElement('div');
            metaDiv.className = 'dt-meta dt-meta-controls';
            if (opts.sumPeriodControl) {
                var lSum = document.createElement('span');
                lSum.className = 'dt-meta-lbl'; lSum.textContent = 'Sum Period';
                metaDiv.appendChild(lSum);
                metaDiv.appendChild(opts.sumPeriodControl);
            }
            if (opts.periodControl) {
                var sepP = document.createElement('span');
                sepP.className = 'dt-meta-sep'; sepP.innerHTML = '&middot;';
                metaDiv.appendChild(sepP);
                var lPrd = document.createElement('span');
                lPrd.className = 'dt-meta-lbl'; lPrd.textContent = 'Period';
                metaDiv.appendChild(lPrd);
                metaDiv.appendChild(opts.periodControl);
            }
            if (opts.fromControl) {
                var sepF = document.createElement('span');
                sepF.className = 'dt-meta-sep'; sepF.innerHTML = '&middot;';
                metaDiv.appendChild(sepF);
                var lFrom = document.createElement('span');
                lFrom.className = 'dt-meta-lbl'; lFrom.textContent = 'From';
                metaDiv.appendChild(lFrom);
                metaDiv.appendChild(opts.fromControl);
            }
            if (opts.toControl) {
                var lTo = document.createElement('span');
                lTo.className = 'dt-meta-lbl'; lTo.textContent = 'To';
                metaDiv.appendChild(lTo);
                metaDiv.appendChild(opts.toControl);
                if (opts.toNowControl) {
                    metaDiv.appendChild(opts.toNowControl);
                }
            }
            if (opts.usersControl) {
                var sepU = document.createElement('span');
                sepU.className = 'dt-meta-sep'; sepU.innerHTML = '&middot;';
                metaDiv.appendChild(sepU);
                var lUsr = document.createElement('span');
                lUsr.className = 'dt-meta-lbl'; lUsr.textContent = 'Users';
                metaDiv.appendChild(lUsr);
                metaDiv.appendChild(opts.usersControl);
            } else if (opts.users && opts.users.length > 0) {
                var sepU2 = document.createElement('span');
                sepU2.className = 'dt-meta-sep'; sepU2.innerHTML = '&middot;';
                metaDiv.appendChild(sepU2);
                var lUsr2 = document.createElement('span');
                lUsr2.className = 'dt-meta-lbl'; lUsr2.textContent = 'Users';
                metaDiv.appendChild(lUsr2);
                var usrVal = document.createElement('span');
                usrVal.className = 'dt-meta-user';
                var _uLabel = (opts.users.length === 1 && opts.users[0] === 'ALL')
                    ? 'ALL' : opts.users.join(', ');
                usrVal.textContent = _uLabel;
                metaDiv.appendChild(usrVal);
            }
            hdr.appendChild(metaDiv);
        } else {
            var uLabel = (opts.users && opts.users.length > 0 &&
                          !(opts.users.length === 1 && opts.users[0] === 'ALL'))
                ? opts.users.join(', ') : 'ALL';
            var _rawPeriod = (opts.period || '');
            var _periodDisplay = (_rawPeriod.indexOf('CUSTOM:') === 0)
                ? (function () {
                      var _pp = _rawPeriod.split(':');
                      var _toDisp = (_pp[2] === 'NOW' || _pp[2] === '') ? 'Now' : _pp[2];
                      return _pp[1] + ' \u2192 ' + _toDisp;
                  }())
                : _rawPeriod;
            var metaSpan = document.createElement('span');
            metaSpan.className = 'dt-meta';
            metaSpan.innerHTML =
                'Sum:&nbsp;' + ((data && data.sum_period) || 'MONTH') +
                '&nbsp;&middot;&nbsp;Period:&nbsp;' + _periodDisplay +
                '&nbsp;&middot;&nbsp;Users:&nbsp;<span class="dt-meta-user">' + uLabel + '</span>';
            hdr.appendChild(metaSpan);
        }
        _decorateHeader(hdr, opts.icon, titleSpan, opts.onDelete);
        root.appendChild(hdr);

        /* date range */
        var dr = document.createElement('div');
        dr.className = 'dt-daterange';
        if (data && data.from_date && data.to_date) {
            dr.textContent = 'From: ' + data.from_date + '  To: ' + data.to_date;
        }
        root.appendChild(dr);

        var bars = (data && data.bars) ? data.bars : [];
        if (bars.length === 0) {
            var noData = document.createElement('div');
            noData.className = 'dt-nodata';
            noData.textContent = 'No data for the selected period.';
            root.appendChild(noData);
            container.appendChild(root);
            return;
        }

        /* chart container */
        var chartDiv = document.createElement('div');
        chartDiv.className = 'dt-chart';
        if (opts.height) { chartDiv.style.height = opts.height + 'px'; }

        var closeBtn = document.createElement('button');
        closeBtn.className = 'dt-fs-close';
        closeBtn.textContent = '\u2715';
        closeBtn.title = 'Exit Fullscreen';
        closeBtn.addEventListener('click', function () {
            if (document.exitFullscreen) document.exitFullscreen();
            else if (document.webkitExitFullscreen) document.webkitExitFullscreen();
        });
        chartDiv.appendChild(closeBtn);

        root.appendChild(chartDiv);
        container.appendChild(root);

        renderPpl(chartDiv, data, opts);
    }


    /* ──────────── Positions — interactive table ──────────────────────── */

    function buildPositions(container, data, opts) {
        injectCSS();
        opts = opts || {};
        /* Fast-path: update existing positions widget in-place (avoids blank-frame flicker) */
        if (typeof container._dpUpdate === 'function' && data && data.positions) {
            container._dpUpdate(data.positions);
            return;
        }
        container.innerHTML = '';

        var root = document.createElement('div');
        root.className = 'dt-root';

        /* header */
        var hdr = document.createElement('div');
        hdr.className = 'dt-header';
        var titleSpan = document.createElement('span');
        titleSpan.className = 'dt-title';
        titleSpan.textContent = 'Positions';
        hdr.appendChild(titleSpan);

        if (opts.usersControl) {
            var metaDiv = document.createElement('div');
            metaDiv.className = 'dt-meta dt-meta-controls';
            var lUsr = document.createElement('span');
            lUsr.className = 'dt-meta-lbl'; lUsr.textContent = 'Users';
            metaDiv.appendChild(lUsr);
            metaDiv.appendChild(opts.usersControl);
            hdr.appendChild(metaDiv);
        } else if (opts.users && opts.users.length > 0) {
            var metaSpan = document.createElement('span');
            metaSpan.className = 'dt-meta';
            var _uLabel = (opts.users.length === 1 && opts.users[0] === 'ALL')
                ? 'ALL' : opts.users.join(', ');
            metaSpan.innerHTML = 'Users:&nbsp;<span class="dt-meta-user">' + _uLabel + '</span>';
            hdr.appendChild(metaSpan);
        }
        _decorateHeader(hdr, opts.icon, titleSpan, opts.onDelete);
        root.appendChild(hdr);

        /* status */
        var statusDiv = document.createElement('div');
        statusDiv.className = 'dt-status';
        statusDiv.textContent = 'Updated: ' + new Date().toLocaleTimeString();
        root.appendChild(statusDiv);

        var rows = (data && data.positions) ? data.positions : [];
        if (rows.length === 0) {
            var noData = document.createElement('div');
            noData.className = 'dt-nodata';
            noData.textContent = 'No open positions.';
            root.appendChild(noData);
            container.appendChild(root);
            return;
        }

        /* table */
        var COLS = [
            { key: 'user',      label: 'User',      fmt: null },
            { key: 'symbol',    label: 'Symbol',     fmt: null },
            { key: 'side',      label: 'Side',       fmt: null },
            { key: 'size',      label: 'Size',       fmt: function (v) { return v.toFixed(3); } },
            { key: 'upnl',      label: 'uPnl',       fmt: function (v) { return v.toFixed(4); } },
            { key: 'entry',     label: 'Entry',      fmt: function (v) { return v.toFixed(5); } },
            { key: 'price',     label: 'Price',      fmt: function (v) { return v.toFixed(5); } },
            { key: 'dca',       label: 'DCA',        fmt: null },
            { key: 'next_dca',  label: 'Next DCA',   fmt: function (v) { return v.toFixed(5); } },
            { key: 'next_tp',   label: 'Next TP',    fmt: function (v) { return v.toFixed(5); } },
            { key: 'pos_value', label: 'Pos Value',  fmt: function (v) { return v.toFixed(2); } }
        ];

        var sortCol = null, sortAsc = true;

        var wrap = document.createElement('div');
        wrap.className = 'dp-table-wrap';
        var tbl = document.createElement('table');
        tbl.className = 'dp-table';

        var thead = document.createElement('thead');
        var hrow = document.createElement('tr');
        COLS.forEach(function (col) {
            var th = document.createElement('th');
            th.textContent = col.label;
            var arrow = document.createElement('span');
            arrow.className = 'dp-sort';
            th.appendChild(arrow);
            th.addEventListener('click', function () {
                if (sortCol === col.key) { sortAsc = !sortAsc; }
                else { sortCol = col.key; sortAsc = true; }
                renderRows();
            });
            hrow.appendChild(th);
        });
        thead.appendChild(hrow);
        tbl.appendChild(thead);

        var tbody = document.createElement('tbody');
        tbl.appendChild(tbody);
        wrap.appendChild(tbl);
        root.appendChild(wrap);

        /* apply height constraint if provided */
        if (opts.height && opts.height > 0) {
            root.style.maxHeight = opts.height + 'px';
            root.style.overflow = 'auto';
        }

        container.appendChild(root);

        var selectedIdx = -1;

        function renderRows() {
            var sorted = rows.slice();
            if (sortCol) {
                sorted.sort(function (a, b) {
                    var va = a[sortCol], vb = b[sortCol];
                    if (typeof va === 'string') {
                        va = va.toLowerCase(); vb = (vb || '').toLowerCase();
                    }
                    if (va < vb) return sortAsc ? -1 : 1;
                    if (va > vb) return sortAsc ? 1 : -1;
                    return 0;
                });
            }
            /* update sort arrows */
            var ths = thead.querySelectorAll('th');
            for (var ti = 0; ti < ths.length; ti++) {
                var arr = ths[ti].querySelector('.dp-sort');
                if (COLS[ti].key === sortCol) {
                    arr.textContent = sortAsc ? ' \u25B2' : ' \u25BC';
                } else {
                    arr.textContent = '';
                }
            }
            tbody.innerHTML = '';
            for (var i = 0; i < sorted.length; i++) {
                var row = sorted[i];
                var tr = document.createElement('tr');
                if (i === selectedIdx) tr.className = 'dp-sel';
                for (var ci = 0; ci < COLS.length; ci++) {
                    var td = document.createElement('td');
                    var val = row[COLS[ci].key];
                    var text = COLS[ci].fmt ? COLS[ci].fmt(val) : String(val);
                    td.textContent = text;
                    /* color uPnl */
                    if (COLS[ci].key === 'upnl') {
                        td.className = val >= 0 ? 'dp-upnl-pos' : 'dp-upnl-neg';
                    }
                    tr.appendChild(td);
                }
                (function (idx, rowData) {
                    tr.addEventListener('click', function () {
                        selectedIdx = idx;
                        renderRows();
                        /* Notify Orders widgets */
                        var pos = opts.position || '';
                        window['_dashPosSelected_' + pos] = rowData;
                        document.dispatchEvent(new CustomEvent('dash-pos-selected', {
                            detail: { pos: pos, data: rowData }
                        }));
                    });
                })(i, row);
                tbody.appendChild(tr);
            }
        }
        renderRows();
        /* Expose in-place update hook for WS fast-path (preserves sort state, avoids DOM rebuild) */
        container._dpUpdate = function (newPositions) {
            rows.length = 0;
            Array.prototype.push.apply(rows, newPositions);
            var _s = container.querySelector('.dt-status');
            if (_s) _s.textContent = 'Updated: ' + new Date().toLocaleTimeString();
            renderRows();
        };
    }


    /* ───────── Orders — candlestick chart with order lines ─────────── */

    function renderOrders(chartDiv, data, opts) {
        /* Lightweight Charts implementation.
           Expects window.LightweightCharts to be loaded.
           chartDiv: a container <div> with explicit width/height.
           data: { candles: [{t,o,h,l,c,v}], orders: [...], position: {...}, current_price: N }
           opts: { onTimeframeChange, onVisibleRangeChange }
           Returns controller with live-update methods (Phase 2).
        */
        var LWC = window.LightweightCharts;
        if (!LWC) { chartDiv.textContent = 'Lightweight Charts not loaded'; return null; }

        var candles = (data && data.candles) ? data.candles : [];
        if (candles.length === 0) {
            chartDiv.textContent = 'No candle data';
            return null;
        }

        /* Convert candle timestamps (ms) to seconds for LW Charts */
        var lwData = candles.map(function (c) {
            return { time: Math.floor(c.t / 1000), open: c.o, high: c.h, low: c.l, close: c.c };
        });

        var _tfShowTime = !(opts.timeframe === '1d' || opts.timeframe === '1w');

        var chart = LWC.createChart(chartDiv, {
            autoSize: true,  /* fill container — no manual width/height needed */
            layout: {
                background: { type: 'solid', color: '#0e1117' },
                textColor: '#94a3b8',
                fontSize: 12
            },
            grid: {
                vertLines: { color: '#1e2d3d' },
                horzLines: { color: '#1e2d3d' }
            },
            crosshair: {
                mode: LWC.CrosshairMode.Normal
            },
            rightPriceScale: {
                borderColor: '#2d3748',
                scaleMargins: { top: 0.1, bottom: 0.15 }  /* some bottom padding for volume without going negative on wide-range charts */
            },
            timeScale: {
                borderColor: '#2d3748',
                timeVisible: _tfShowTime,
                secondsVisible: false,
                rightOffset: 30
            },
            handleScroll: true,
            handleScale: true
        });

        var csOpts = {
            upColor: '#48bb78',
            downColor: '#f56565',
            borderUpColor: '#48bb78',
            borderDownColor: '#f56565',
            wickUpColor: '#48bb78',
            wickDownColor: '#f56565'
        };
        /* v4+ uses addSeries(type, opts); v3 uses addCandlestickSeries(opts) */
        var series = (typeof chart.addCandlestickSeries === 'function')
            ? chart.addCandlestickSeries(csOpts)
            : chart.addSeries(LWC.CandlestickSeries, csOpts);

        series.setData(lwData);

        /* ── Auto precision: show enough decimal places for the price magnitude ── */
        var _lastPrice = candles[candles.length - 1].c;
        var _prec = _lastPrice < 0.0001 ? 8
                  : _lastPrice < 0.001  ? 6
                  : _lastPrice < 0.01   ? 5
                  : _lastPrice < 0.1    ? 4
                  : _lastPrice < 1      ? 4
                  : _lastPrice < 10     ? 3
                  : _lastPrice < 100    ? 2 : 2;
        var _minMove = parseFloat(Math.pow(10, -_prec).toFixed(_prec));
        series.applyOptions({ priceFormat: { type: 'price', precision: _prec, minMove: _minMove } });

        /* ── Tracked price lines (Phase 2: updatable via controller methods) ── */
        var _entryLine = null;
        var _priceLine = null;
        var _orderLines = [];

        /* Create initial entry-price line (Solid) */
        var pos = data.position;
        var _lastClose = candles[candles.length - 1].c;
        if (pos && pos.entry) {
            var posColor = _lastClose >= pos.entry ? '#48bb78' : '#f56565';
            _entryLine = series.createPriceLine({
                price: pos.entry,
                color: posColor,
                lineWidth: 2,
                lineStyle: LWC.LineStyle.Solid,
                axisLabelVisible: true,
                title: 'Entry',
                autoscaleInfoProvider: function () { return null; }
            });
        }

        /* Create market-price line (Dotted — follows last close) */
        if (_lastClose > 0) {
            _priceLine = series.createPriceLine({
                price: _lastClose,
                color: '#a0aec0',
                lineWidth: 1,
                lineStyle: LWC.LineStyle.Dotted,
                axisLabelVisible: true,
                title: 'Price',
                autoscaleInfoProvider: function () { return null; }
            });
        }

        /* Create initial order lines (Dashed) */
        var orders = (data && data.orders) ? data.orders : [];
        for (var oi = 0; oi < orders.length; oi++) {
            var o = orders[oi];
            _orderLines.push(series.createPriceLine({
                price: o.price,
                color: o.side === 'sell' ? '#f56565' : '#48bb78',
                lineWidth: 1,
                lineStyle: LWC.LineStyle.Dashed,
                axisLabelVisible: true,
                title: '',
                autoscaleInfoProvider: function () { return null; }
            }));
        }

        /* ── Volume histogram ── */
        var _volData = candles.map(function (c) {
            return {
                time: Math.floor(c.t / 1000),
                value: c.v,
                color: c.c >= c.o ? 'rgba(72,187,120,0.35)' : 'rgba(245,101,101,0.35)'
            };
        });
        var _volOpts = { priceFormat: { type: 'volume' }, priceScaleId: 'vol' };
        var volSeries = typeof chart.addHistogramSeries === 'function'
            ? chart.addHistogramSeries(_volOpts)
            : chart.addSeries(LWC.HistogramSeries, _volOpts);
        try {
            chart.priceScale('vol').applyOptions({
                scaleMargins: { top: 0.8, bottom: 0 },
                visible: false
            });
        } catch (_) {}
        volSeries.setData(_volData);

        /* Fit content after the browser has laid out the container.
           Two-phase: rAF for first paint, setTimeout for autoSize ResizeObserver settling. */
        requestAnimationFrame(function () {
            chart.timeScale().fitContent();
        });
        setTimeout(function () {
            chart.timeScale().fitContent();
        }, 200);

        /* ── Lazy history loading: fire onLoadMore when user scrolls near left edge ── */
        var _loadingMore = false;
        var _dataGen = 0;  /* generation counter — incremented by setData to invalidate stale prepends */
        var _loadMoreHandler = null;
        function _armLoadMore() {
            if (!opts.onLoadMore) return;
            if (_loadMoreHandler) {
                try { chart.timeScale().unsubscribeVisibleLogicalRangeChange(_loadMoreHandler); } catch (_) {}
            }
            _loadMoreHandler = function (range) {
                if (!range) return;
                if (_loadingMore) return;
                if (range.from < 20) {
                    _loadingMore = true;
                    var gen = _dataGen;
                    opts.onLoadMore(lwData[0].time * 1000, function () {
                        if (gen === _dataGen) _loadingMore = false;
                    });
                }
            };
            chart.timeScale().subscribeVisibleLogicalRangeChange(_loadMoreHandler);
        }
        _armLoadMore();

        /* autoSize:true handles responsive resizing — no manual ResizeObserver needed */

        /* Return controller object for live updates */
        return {
            chart: chart,
            series: series,
            updateCandle: function (candle) {
                /* candle: [t, o, h, l, c, v] (raw exchange format) */
                var t = Math.floor(candle[0] / 1000);
                _lastClose = candle[4];
                series.update({
                    time: t, open: candle[1], high: candle[2],
                    low: candle[3], close: candle[4]
                });
                volSeries.update({
                    time: t, value: candle[5],
                    color: candle[4] >= candle[1] ? 'rgba(72,187,120,0.35)' : 'rgba(245,101,101,0.35)'
                });
                /* Update market-price line to follow latest close */
                if (_priceLine) {
                    _priceLine.applyOptions({ price: _lastClose });
                } else if (_lastClose > 0) {
                    _priceLine = series.createPriceLine({
                        price: _lastClose, color: '#a0aec0', lineWidth: 1,
                        lineStyle: LWC.LineStyle.Dotted, axisLabelVisible: true, title: 'Price',
                        autoscaleInfoProvider: function () { return null; }
                    });
                }
                /* Update entry line color based on latest close vs entry */
                if (_entryLine && pos && pos.entry) {
                    _entryLine.applyOptions({
                        color: candle[4] >= pos.entry ? '#48bb78' : '#f56565'
                    });
                }
            },
            updatePosition: function (posData) {
                /* posData: {entry, size, upnl, side} or null */
                if (_entryLine) {
                    try { series.removePriceLine(_entryLine); } catch (_) {}
                    _entryLine = null;
                }
                if (posData && posData.entry && posData.entry > 0) {
                    var pc = _lastClose >= posData.entry ? '#48bb78' : '#f56565';
                    _entryLine = series.createPriceLine({
                        price: posData.entry,
                        color: pc,
                        lineWidth: 2,
                        lineStyle: LWC.LineStyle.Solid,
                        axisLabelVisible: true,
                        title: 'Entry',
                        autoscaleInfoProvider: function () { return null; }
                    });
                }
            },
            updateOrders: function (ordersList) {
                /* ordersList: [{price, amount, side}, ...] */
                /* Remove all existing order lines */
                for (var i = 0; i < _orderLines.length; i++) {
                    try { series.removePriceLine(_orderLines[i]); } catch (_) {}
                }
                _orderLines = [];
                /* Create new lines */
                if (ordersList && ordersList.length > 0) {
                    for (var j = 0; j < ordersList.length; j++) {
                        var ord = ordersList[j];
                        _orderLines.push(series.createPriceLine({
                            price: ord.price,
                            color: ord.side === 'sell' ? '#f56565' : '#48bb78',
                            lineWidth: 1,
                            lineStyle: LWC.LineStyle.Dashed,
                            axisLabelVisible: true,
                            title: '',
                            autoscaleInfoProvider: function () { return null; }
                        }));
                    }
                }
            },
            prependData: function (olderCandles, gen) {
                /* Skip stale calls from a previous timeframe */
                if (gen !== undefined && gen !== _dataGen) return;
                /* Prepend older candles to front of chart without losing current view */
                var newLw = olderCandles.map(function (c) {
                    return { time: Math.floor(c.t / 1000), open: c.o, high: c.h, low: c.l, close: c.c };
                });
                var newVol = olderCandles.map(function (c) {
                    return { time: Math.floor(c.t / 1000), value: c.v,
                        color: c.c >= c.o ? 'rgba(72,187,120,0.35)' : 'rgba(245,101,101,0.35)' };
                });
                /* Merge: newLw first, then existing lwData (deduplicate by time) */
                var existing = lwData.reduce(function (m, c) { m[c.time] = c; return m; }, {});
                newLw.forEach(function (c) { if (!existing[c.time]) { existing[c.time] = c; } });
                lwData = Object.values(existing).sort(function (a, b) { return a.time - b.time; });
                series.setData(lwData);
                /* same for volume */
                var existingVol = _volData.reduce(function (m, c) { m[c.time] = c; return m; }, {});
                newVol.forEach(function (c) { if (!existingVol[c.time]) { existingVol[c.time] = c; } });
                _volData = Object.values(existingVol).sort(function (a, b) { return a.time - b.time; });
                volSeries.setData(_volData);
            },
            setData: function (newCandles) {
                /* Full candle replacement (e.g. timeframe switch) — no chart rebuild */
                _dataGen++;          /* invalidate any in-flight prependData calls */
                _loadingMore = false; /* allow fresh onLoadMore triggers */
                lwData = newCandles.map(function (c) {
                    return { time: Math.floor(c.t / 1000), open: c.o, high: c.h, low: c.l, close: c.c };
                });
                _volData = newCandles.map(function (c) {
                    return { time: Math.floor(c.t / 1000), value: c.v,
                        color: c.c >= c.o ? 'rgba(72,187,120,0.35)' : 'rgba(245,101,101,0.35)' };
                });
                /* Recalibrate precision for new price range */
                if (newCandles.length > 0) {
                    var _lp2 = newCandles[newCandles.length - 1].c;
                    var _pr2 = _lp2 < 0.0001 ? 8 : _lp2 < 0.001 ? 7 : _lp2 < 0.01 ? 6
                             : _lp2 < 0.1 ? 5 : _lp2 < 1 ? 4 : _lp2 < 10 ? 3 : 2;
                    series.applyOptions({ priceFormat: { type: 'price', precision: _pr2,
                        minMove: parseFloat(Math.pow(10, -_pr2).toFixed(_pr2)) } });
                    _lastClose = _lp2;
                    /* Refresh price-line position */
                    if (_priceLine) _priceLine.applyOptions({ price: _lp2 });
                }
                /* Auto-detect timeframe from data spacing and update time axis */
                if (newCandles.length >= 2) {
                    var _span = newCandles[1].t - newCandles[0].t;
                    var _showTime = _span < 86400000; /* less than 1-day interval → show time */
                    chart.applyOptions({ timeScale: { timeVisible: _showTime, secondsVisible: false } });
                }
                series.setData(lwData);
                volSeries.setData(_volData);
                chart.timeScale().fitContent();
                /* Re-arm load-more handler for the new dataset */
                _armLoadMore();
            },
            _gen: function () { return _dataGen; },
            destroy: function () {
                chart.remove();
            },
            chartInstance: chart
        };
    }

    function buildOrders(container, data, opts) {
        injectCSS();
        opts = opts || {};
        container.innerHTML = '';

        var root = document.createElement('div');
        root.className = 'dt-root';

        /* header */
        var hdr = document.createElement('div');
        hdr.className = 'dt-header';
        var titleSpan = document.createElement('span');
        titleSpan.className = 'dt-title';
        titleSpan.textContent = 'Orders';
        hdr.appendChild(titleSpan);

        if (opts.message) {
            /* placeholder when no position selected or loading */
            var msgSpan = document.createElement('span');
            msgSpan.className = 'dt-meta';
            msgSpan.textContent = opts.message;
            hdr.appendChild(msgSpan);
            _decorateHeader(hdr, opts.icon, titleSpan, opts.onDelete);
            root.appendChild(hdr);
            var noData = document.createElement('div');
            noData.className = 'dt-nodata';
            noData.textContent = opts.message;
            root.appendChild(noData);
            container.appendChild(root);
            return null;
        }

        /* meta: Timeframe buttons + user/symbol info */
        var metaDiv = document.createElement('div');
        metaDiv.className = 'dt-meta dt-meta-controls';

        /* Timeframe button bar */
        var TIMEFRAMES = ['1m','5m','15m','30m','1h','2h','4h','6h','12h','1d','1w'];
        var currentTf = opts.timeframe || '4h';
        var tfBar = document.createElement('div');
        tfBar.className = 'do-tf-bar';

        TIMEFRAMES.forEach(function (tf) {
            var btn = document.createElement('button');
            btn.className = 'do-tf-btn' + (tf === currentTf ? ' do-tf-active' : '');
            btn.textContent = tf;
            btn.addEventListener('click', function () {
                /* Update active highlight immediately */
                var allBtns = tfBar.querySelectorAll('.do-tf-btn');
                for (var bi = 0; bi < allBtns.length; bi++) {
                    allBtns[bi].classList.remove('do-tf-active');
                }
                btn.classList.add('do-tf-active');
                if (opts.onTimeframeChange) opts.onTimeframeChange(tf);
            });
            tfBar.appendChild(btn);
        });

        var lTf = document.createElement('span');
        lTf.className = 'dt-meta-lbl'; lTf.textContent = 'Timeframe';
        metaDiv.appendChild(lTf);
        metaDiv.appendChild(tfBar);

        /* user / symbol / time / live uPnL display */
        var _clockInterval = null;
        if (data) {
            var sep2 = document.createElement('span');
            sep2.className = 'dt-meta-sep'; sep2.innerHTML = '&middot;';
            metaDiv.appendChild(sep2);
            var infoSpan = document.createElement('span');
            infoSpan.className = 'dt-meta';
            var _clockSpan = document.createElement('span');
            _clockSpan.textContent = new Date().toLocaleTimeString();
            /* uPnL info span — updated live via WS position updates */
            var _posInfoSpan = document.createElement('span');
            _posInfoSpan.className = 'do-pos-info';
            if (data.position && data.position.upnl !== undefined) {
                var _upnl = data.position.upnl || 0;
                var _cls = _upnl >= 0 ? 'dt-pos' : 'dt-neg';
                _posInfoSpan.innerHTML = ' \u00b7 uPnL: \x3Cspan class="' + _cls + '">'
                    + (_upnl >= 0 ? '+' : '') + _upnl.toFixed(2) + '\x3C/span>';
            }
            infoSpan.innerHTML =
                'User:&nbsp;\x3Cspan class="dt-meta-user">' + (data.user || '') + '\x3C/span>' +
                '&nbsp;\u00b7&nbsp;Symbol:&nbsp;\x3Cspan class="dt-meta-user">' + (data.symbol || '') + '\x3C/span>' +
                '&nbsp;\u00b7&nbsp;';
            infoSpan.appendChild(_clockSpan);
            infoSpan.appendChild(_posInfoSpan);
            metaDiv.appendChild(infoSpan);
            /* Tick every second */
            _clockInterval = setInterval(function () {
                _clockSpan.textContent = new Date().toLocaleTimeString();
            }, 1000);
        }
        hdr.appendChild(metaDiv);
        _decorateHeader(hdr, opts.icon, titleSpan, opts.onDelete);
        root.appendChild(hdr);

        /* ── Chart legend ── */
        var legendDiv = document.createElement('div');
        legendDiv.className = 'do-legend';
        var legendItems = [
            { style: 'do-leg-solid', color: '#a0aec0', label: 'Entry' },
            { style: 'do-leg-dotted', color: '#a0aec0', label: 'Price' },
            { style: 'do-leg-dashed', color: '#48bb78', label: 'Buy Order' },
            { style: 'do-leg-dashed', color: '#f56565', label: 'Sell Order' }
        ];
        legendItems.forEach(function (li) {
            var item = document.createElement('span');
            item.className = 'do-leg-item';
            var swatch = document.createElement('span');
            swatch.className = li.style;
            swatch.style.borderColor = li.color;
            item.appendChild(swatch);
            item.appendChild(document.createTextNode(li.label));
            legendDiv.appendChild(item);
        });
        /* legend will be appended to chartWrap below */

        var candles = (data && data.candles) ? data.candles : [];

        /* fullscreen button — Plotly-style: hover toolbar floated over chart */
        if (candles.length === 0) {
            var noData2 = document.createElement('div');
            noData2.className = 'dt-nodata';
            noData2.textContent = 'No candle data for this symbol.';
            root.appendChild(noData2);
            container.appendChild(root);
            return null;
        }

        /* fullscreen button — Plotly-style: hover toolbar floated over chart */
        var fsBtn = document.createElement('button');
        fsBtn.className = 'do-fs-btn';
        fsBtn.textContent = '\u26F6';
        fsBtn.title = 'Fullscreen';

        var chartToolbar = document.createElement('div');
        chartToolbar.className = 'do-chart-toolbar';
        chartToolbar.appendChild(fsBtn);

        /* chart container */
        var chartWrap = document.createElement('div');
        chartWrap.className = 'do-chart-wrap';
        chartWrap.appendChild(chartToolbar);
        chartWrap.appendChild(legendDiv);
        root.appendChild(chartWrap);
        container.appendChild(root);

        var ctrl = renderOrders(chartWrap, data, opts);

        /* ── Live uPnL tracking: recalculate on every candle/position update ── */
        var _posState = (data && data.position) ? {
            entry: data.position.entry || 0,
            size:  data.position.size  || 0,
            side:  data.position.side  || 'long'
        } : { entry: 0, size: 0, side: 'long' };

        function _refreshUpnl(closePrice) {
            /* Approximate uPnL from entry, size, close price */
            if (!_posState.entry || !_posState.size || !closePrice) {
                _posInfoSpan.textContent = '';
                return;
            }
            var diff = (_posState.side === 'short')
                     ? (_posState.entry - closePrice)
                     : (closePrice - _posState.entry);
            var upnl = diff * Math.abs(_posState.size);
            var cls = upnl >= 0 ? 'dt-pos' : 'dt-neg';
            _posInfoSpan.innerHTML = ' \u00b7 uPnL: \x3Cspan class="' + cls + '">'
                + (upnl >= 0 ? '+' : '') + upnl.toFixed(2) + '\x3C/span>';
        }

        /* Wrap updateCandle to also refresh uPnL from latest close */
        if (ctrl) {
            var _origUpdateCandle = ctrl.updateCandle;
            ctrl.updateCandle = function (candle) {
                _origUpdateCandle(candle);
                /* candle[4] = close price */
                _refreshUpnl(candle[4]);
            };
            /* Wrap updatePosition to track state + refresh uPnL */
            var _origUpdatePosition = ctrl.updatePosition;
            ctrl.updatePosition = function (posData) {
                _origUpdatePosition(posData);
                if (posData && posData.entry) {
                    _posState.entry = posData.entry;
                    _posState.size  = posData.size || 0;
                    _posState.side  = posData.side || 'long';
                    if (posData.upnl !== undefined) {
                        /* Use exact exchange uPnL when available */
                        var cls = posData.upnl >= 0 ? 'dt-pos' : 'dt-neg';
                        _posInfoSpan.innerHTML = ' \u00b7 uPnL: \x3Cspan class="' + cls + '">'
                            + (posData.upnl >= 0 ? '+' : '') + posData.upnl.toFixed(2) + '\x3C/span>';
                    }
                } else {
                    _posState.entry = 0;
                    _posState.size = 0;
                    _posInfoSpan.textContent = '';
                }
            };
        }

        /* Wire up fullscreen after ctrl is available */
        function _syncFs() {
            var isFull = !!(document.fullscreenElement === root || document.webkitFullscreenElement === root);
            if (isFull) {
                fsBtn.textContent = '\u2715';
                root.classList.add('do-fullscreen');
            } else {
                fsBtn.textContent = '\u26F6';
                root.classList.remove('do-fullscreen');
            }
            if (ctrl) {
                /* autoSize:true reflows automatically; just re-fit the visible range */
                setTimeout(function () {
                    ctrl.chart.timeScale().fitContent();
                }, 150);
            }
        }
        document.addEventListener('fullscreenchange', _syncFs);
        document.addEventListener('webkitfullscreenchange', _syncFs);

        fsBtn.addEventListener('click', function () {
            if (document.fullscreenElement || document.webkitFullscreenElement) {
                if (document.exitFullscreen) document.exitFullscreen();
                else if (document.webkitExitFullscreen) document.webkitExitFullscreen();
            } else {
                if (root.requestFullscreen) root.requestFullscreen();
                else if (root.webkitRequestFullscreen) root.webkitRequestFullscreen();
            }
        });

        /* Patch destroy to clean up fullscreen listeners + clock */
        if (ctrl) {
            var _origDestroy = ctrl.destroy;
            ctrl.destroy = function () {
                document.removeEventListener('fullscreenchange', _syncFs);
                document.removeEventListener('webkitfullscreenchange', _syncFs);
                if (_clockInterval) clearInterval(_clockInterval);
                _origDestroy();
            };
        }

        return ctrl;
    }


    /* ───────── ADG — Average Daily Growth (%) chart ─────────── */

    function renderAdg(chartDiv, data, opts) {
        opts = opts || {};
        var P = opts.Plotly || global.Plotly;
        if (!P) { chartDiv.textContent = 'Plotly not loaded'; return; }

        var bars = (data && data.bars) ? data.bars : [];
        var mode = (data && data.mode) || 'bar';

        var dates  = bars.map(function (b) { return b.date; });
        var values = bars.map(function (b) { return b.adg; });
        var colors = values.map(function (v) { return v < 0 ? '#fc8181' : '#68d391'; });

        var trace;
        if (mode === 'line') {
            trace = {
                x: dates, y: values, type: 'scatter', mode: 'lines+markers',
                line: { color: '#63b3ed', width: 1 },
                marker: { color: colors, size: 6 },
                hovertemplate: '\x3Cb>%{x}\x3C/b>\x3Cbr>ADG: %{y:.2f}%\x3Cextra>\x3C/extra>'
            };
        } else {
            trace = {
                x: dates, y: values, type: 'bar',
                marker: { color: colors },
                text: values.map(function (v) { return v.toFixed(2); }),
                textposition: 'auto',
                hovertemplate: '\x3Cb>%{x}\x3C/b>\x3Cbr>ADG: %{y:.2f}%\x3Cextra>\x3C/extra>'
            };
        }

        var layout = {
            paper_bgcolor: '#0e1117',
            plot_bgcolor:  '#0e1117',
            font:   { color: '#e2e8f0', size: 11 },
            margin: { l: 50, r: 20, t: 40, b: 50 },
            xaxis:  { tickangle: -45, gridcolor: '#2d3748', color: '#e2e8f0',
                      type: 'date' },
            yaxis:  { gridcolor: '#2d3748', color: '#e2e8f0',
                      zeroline: true, zerolinecolor: '#4a5568' },
            bargap: 0.3,
            autosize: true
        };
        var origHeight = opts.height || null;
        if (origHeight) { layout.height = origHeight; }

        if (opts.savedZoom) {
            if (opts.savedZoom.xrange) {
                layout.xaxis.range     = opts.savedZoom.xrange;
                layout.xaxis.autorange = false;
            }
            if (opts.savedZoom.yrange) {
                layout.yaxis.range     = opts.savedZoom.yrange;
                layout.yaxis.autorange = false;
            }
        }

        var fschangeHandler = function () {
            var root = chartDiv.closest ? chartDiv.closest('.dt-root') : null;
            var isFull = root
                ? (document.fullscreenElement === root || document.webkitFullscreenElement === root)
                : !!(document.fullscreenElement || document.webkitFullscreenElement);
            var closeBtn = root ? root.querySelector('.dt-fs-close') : null;
            if (closeBtn) { closeBtn.style.display = isFull ? 'block' : 'none'; }
            if (isFull) {
                var fsW = window.screen.width  || window.innerWidth;
                var fsH = (window.screen.availHeight || window.innerHeight) - 62;
                P.relayout(chartDiv, { width: fsW, height: fsH });
            } else {
                P.relayout(chartDiv, { width: null, height: origHeight || null });
                setTimeout(function () { P.Plots.resize(chartDiv); }, 100);
            }
        };
        document.addEventListener('fullscreenchange', fschangeHandler);
        document.addEventListener('webkitfullscreenchange', fschangeHandler);

        var cfg = {
            displayModeBar: opts.displayModeBar !== undefined ? opts.displayModeBar : false,
            responsive:     opts.responsive     !== undefined ? opts.responsive     : true,
            modeBarButtonsToAdd: [
                {
                    name:  'fullscreen',
                    title: 'Fullscreen',
                    icon:  {
                        width: 857.1, height: 857.1,
                        path: 'M0 0v285.7h142.9V142.9H285.7V0H0zm571.4 0v142.9h142.9v142.9H857.1V0H571.4zM0 571.4v285.7h285.7V714.3H142.9V571.4H0zm714.3 142.9v142.9H571.4v142.9H857.1V571.4H714.3z'
                    },
                    click: function (gd) {
                        var root = gd.closest ? gd.closest('.dt-root') : gd.parentElement;
                        var isFull = (document.fullscreenElement === root ||
                                      document.webkitFullscreenElement === root);
                        if (!isFull) {
                            if (root.requestFullscreen) root.requestFullscreen();
                            else if (root.webkitRequestFullscreen) root.webkitRequestFullscreen();
                        } else {
                            if (document.exitFullscreen) document.exitFullscreen();
                            else if (document.webkitExitFullscreen) document.webkitExitFullscreen();
                        }
                    }
                }
            ]
        };
        layout.transition = { duration: 0, easing: 'linear' };
        P.react(chartDiv, [trace], layout, cfg);
        if (!opts.noResize) { setTimeout(function () { P.Plots.resize(chartDiv); }, 80); }
    }


    function buildAdg(container, data, opts) {
        injectCSS();
        opts = opts || {};
        /* Fast-path: if chart already rendered, update in-place via Plotly.react */
        var _fc = container.querySelector('.dt-chart');
        if (_fc && data && (data.bars || []).length > 0) {
            var _dr = container.querySelector('.dt-daterange');
            if (_dr) _dr.textContent = (data.from_date && data.to_date) ? 'From: ' + data.from_date + '  To: ' + data.to_date : '';
            /* Preserve current zoom state across WS-triggered data updates */
            var _z = opts.savedZoom || null;
            if (!_z && _fc.layout) {
                var _xa = _fc.layout.xaxis || {}, _ya = _fc.layout.yaxis || {};
                var _xr = (_xa.autorange === false && _xa.range) ? _xa.range.slice() : null;
                var _yr = (_ya.autorange === false && _ya.range) ? _ya.range.slice() : null;
                if (_xr || _yr) _z = { xrange: _xr, yrange: _yr };
            }
            renderAdg(_fc, data, { noResize: true, savedZoom: _z });
            return;
        }
        container.innerHTML = '';

        var root = document.createElement('div');
        root.className = 'dt-root';

        /* header */
        var hdr = document.createElement('div');
        hdr.className = 'dt-header';
        var titleSpan = document.createElement('span');
        titleSpan.className = 'dt-title';
        titleSpan.textContent = 'ADG';
        hdr.appendChild(titleSpan);

        if (opts.modeControl || opts.periodControl || opts.usersControl) {
            var metaDiv = document.createElement('div');
            metaDiv.className = 'dt-meta dt-meta-controls';
            if (opts.modeControl) {
                var lMode = document.createElement('span');
                lMode.className = 'dt-meta-lbl'; lMode.textContent = 'Mode';
                metaDiv.appendChild(lMode);
                metaDiv.appendChild(opts.modeControl);
            }
            if (opts.periodControl) {
                var sepP = document.createElement('span');
                sepP.className = 'dt-meta-sep'; sepP.innerHTML = '\x26middot;';
                metaDiv.appendChild(sepP);
                var lPrd = document.createElement('span');
                lPrd.className = 'dt-meta-lbl'; lPrd.textContent = 'Period';
                metaDiv.appendChild(lPrd);
                metaDiv.appendChild(opts.periodControl);
            }
            if (opts.fromControl) {
                var sepF = document.createElement('span');
                sepF.className = 'dt-meta-sep'; sepF.innerHTML = '\x26middot;';
                metaDiv.appendChild(sepF);
                var lFrom = document.createElement('span');
                lFrom.className = 'dt-meta-lbl'; lFrom.textContent = 'From';
                metaDiv.appendChild(lFrom);
                metaDiv.appendChild(opts.fromControl);
            }
            if (opts.toControl) {
                var lTo = document.createElement('span');
                lTo.className = 'dt-meta-lbl'; lTo.textContent = 'To';
                metaDiv.appendChild(lTo);
                metaDiv.appendChild(opts.toControl);
                if (opts.toNowControl) {
                    metaDiv.appendChild(opts.toNowControl);
                }
            }
            if (opts.usersControl) {
                var sepU = document.createElement('span');
                sepU.className = 'dt-meta-sep'; sepU.innerHTML = '\x26middot;';
                metaDiv.appendChild(sepU);
                var lUsr = document.createElement('span');
                lUsr.className = 'dt-meta-lbl'; lUsr.textContent = 'Users';
                metaDiv.appendChild(lUsr);
                metaDiv.appendChild(opts.usersControl);
            } else if (opts.users && opts.users.length > 0) {
                var sepU2 = document.createElement('span');
                sepU2.className = 'dt-meta-sep'; sepU2.innerHTML = '\x26middot;';
                metaDiv.appendChild(sepU2);
                var lUsr2 = document.createElement('span');
                lUsr2.className = 'dt-meta-lbl'; lUsr2.textContent = 'Users';
                metaDiv.appendChild(lUsr2);
                var usrVal = document.createElement('span');
                usrVal.className = 'dt-meta-user';
                var _uLabel = (opts.users.length === 1 && opts.users[0] === 'ALL')
                    ? 'ALL' : opts.users.join(', ');
                usrVal.textContent = _uLabel;
                metaDiv.appendChild(usrVal);
            }
            hdr.appendChild(metaDiv);
        } else {
            var uLabel = (opts.users && opts.users.length > 0 &&
                          !(opts.users.length === 1 && opts.users[0] === 'ALL'))
                ? opts.users.join(', ') : 'ALL';
            var _rawPeriod = (opts.period || '');
            var _periodDisplay = (_rawPeriod.indexOf('CUSTOM:') === 0)
                ? (function () {
                      var _pp = _rawPeriod.split(':');
                      var _toDisp = (_pp[2] === 'NOW' || _pp[2] === '') ? 'Now' : _pp[2];
                      return _pp[1] + ' \u2192 ' + _toDisp;
                  }())
                : _rawPeriod;
            var metaSpan = document.createElement('span');
            metaSpan.className = 'dt-meta';
            metaSpan.innerHTML =
                'Mode:\x26nbsp;' + ((data && data.mode) || 'bar') +
                '\x26nbsp;\x26middot;\x26nbsp;Period:\x26nbsp;' + _periodDisplay +
                '\x26nbsp;\x26middot;\x26nbsp;Users:\x26nbsp;\x3Cspan class="dt-meta-user">' + uLabel + '\x3C/span>';
            hdr.appendChild(metaSpan);
        }
        _decorateHeader(hdr, opts.icon, titleSpan, opts.onDelete);
        root.appendChild(hdr);

        /* balance summary line */
        if (data && data.starting_balance !== undefined) {
            var sumDiv = document.createElement('div');
            sumDiv.className = 'dt-daterange';
            sumDiv.innerHTML = 'Starting Balance: \x3Cb>' + data.starting_balance.toFixed(2) + '\x3C/b>'
                + ' \x26middot; Total PNL: \x3Cb>' + data.total_pnl.toFixed(2) + '\x3C/b>'
                + ' \x26middot; Current Balance: \x3Cb>' + data.current_balance.toFixed(2) + '\x3C/b>';
            root.appendChild(sumDiv);
        }

        /* date range */
        var dr = document.createElement('div');
        dr.className = 'dt-daterange';
        if (data && data.from_date && data.to_date) {
            dr.textContent = 'From: ' + data.from_date + '  To: ' + data.to_date;
        }
        root.appendChild(dr);

        var bars = (data && data.bars) ? data.bars : [];
        if (bars.length === 0) {
            var noData = document.createElement('div');
            noData.className = 'dt-nodata';
            noData.textContent = 'No data for the selected period.';
            root.appendChild(noData);
            container.appendChild(root);
            return;
        }

        /* chart container */
        var chartDiv = document.createElement('div');
        chartDiv.className = 'dt-chart';
        if (opts.height) { chartDiv.style.height = opts.height + 'px'; }

        var closeBtn = document.createElement('button');
        closeBtn.className = 'dt-fs-close';
        closeBtn.textContent = '\u2715';
        closeBtn.title = 'Exit Fullscreen';
        closeBtn.addEventListener('click', function () {
            if (document.exitFullscreen) document.exitFullscreen();
            else if (document.webkitExitFullscreen) document.webkitExitFullscreen();
        });
        chartDiv.appendChild(closeBtn);

        root.appendChild(chartDiv);
        container.appendChild(root);

        renderAdg(chartDiv, data, opts);
    }


    /* ──────────────────────────── Export ───────────────────────────────── */

    global.DashRender = {
        VERSION:            '20260410a',
        injectCSS:          injectCSS,
        tweColor:           tweColor,
        upnlColor:          upnlColor,
        tweBarPct:          tweBarPct,
        signedFmt:          signedFmt,
        renderBalanceRows:  renderBalanceRows,
        buildBalance:       buildBalance,
        renderTop:          renderTop,
        buildTop:           buildTop,
        buildIncome:        buildIncome,
        renderPnl:          renderPnl,
        buildPnl:           buildPnl,
        renderPpl:          renderPpl,
        buildPpl:           buildPpl,
        renderAdg:          renderAdg,
        buildAdg:           buildAdg,
        buildPositions:     buildPositions,
        renderOrders:       renderOrders,
        buildOrders:        buildOrders
    };

}(window));
