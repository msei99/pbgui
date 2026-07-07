/**
 * LogViewerPanel v2 — unified log viewer component
 *
 * Replaces all inline log viewer implementations with a single reusable class.
 * Supports both local (file-based) and remote (VPS service-based) log viewing.
 *
 * Usage:
 *   const viewer = new LogViewerPanel({
 *       containerId   : 'myDiv',
 *       wsBase        : 'ws://host:port',
 *       token         : 'TOKEN',
 *       defaultHost   : 'local',               // 'local' | VPS hostname
 *       defaultFile   : 'PBRun.log',           // local mode default
 *       defaultService: 'Bot:bybit_BTC:7',     // VPS mode default
 *       presets       : 'system',              // 'system' | 'trading'
 *       showRestart   : false,                 // restart button (VPS)
 *       height        : 'calc(100vh - 200px)', // optional
 *   });
 *   viewer.open();       // connect WS and start
 *   viewer.close();      // disconnect
 *   viewer.setHost(h);   // programmatic host switch
 *   viewer.setService(s);// programmatic service switch (VPS)
 *   viewer.setFile(f);   // programmatic file switch (local)
 *   viewer.fetchFile(f); // one-shot fetch, no streaming (e.g. rotated files)
 */
class LogViewerPanel {

    /* ── Preset sets ──────────────────────────────────────────── */
    static PRESETS = {
        system: [
            { l: 'Errors',            v: 'error|traceback|exception' },
            { l: 'Warnings',          v: 'warning|warn' },
            { l: 'Errors + Warnings', v: 'error|warning|traceback' },
            { l: 'Connection',        v: 'connect|disconnect|timeout|reconnect' },
            { l: 'Restart / Stop',    v: 'restart|kill|stop|shutdown' },
            { l: 'Traceback',         v: 'traceback|exception|raise' },
        ],
        trading: [
            { l: 'Errors',            v: 'error|traceback|exception' },
            { l: 'Warnings',          v: 'warning|warn' },
            { l: 'Errors + Warnings', v: 'error|warning|traceback' },
            { l: 'Orders / Fills',    v: 'order|fill|entry|close' },
            { l: 'Balance / PnL',     v: 'balance|pnl|profit|loss|equity' },
            { l: 'Positions',         v: 'position|pos_size|wallet' },
            { l: 'Startup',           v: 'start|running|initialized|listening' },
            { l: 'Connection',        v: 'connect|disconnect|timeout|reconnect' },
            { l: 'Restart / Stop',    v: 'restart|kill|stop|shutdown' },
            { l: 'Traceback',         v: 'traceback|exception|raise' },
        ],
    };

    /* ── CSS (injected once per page) ─────────────────────────── */
    static _injectStyles() {
        if (document.getElementById('lvp-global-styles')) return;
        var s = document.createElement('style');
        s.id = 'lvp-global-styles';
        s.textContent = `
/* ── LogViewerPanel v2 ────────────────────────────────────────────── */
.lvp-root{display:flex;flex-direction:row;height:100%;min-height:0;overflow:hidden}

/* sidebar */
.lvp-sidebar{
    width:auto;min-width:140px;
    background:#111827;border-right:1px solid #1e293b;
    display:flex;flex-direction:column;overflow:hidden;flex-shrink:0;
    position:relative;
}
.lvp-sidebar.lvp-collapsed{width:0;min-width:0;border-right:none;overflow:hidden}
.lvp-sidebar-resize{
    position:absolute;top:0;right:-4px;bottom:0;width:8px;
    z-index:10;cursor:col-resize;
    user-select:none;
}
.lvp-sidebar-header{
    padding:4px 10px;border-bottom:1px solid #1e293b;
    flex-shrink:0;white-space:nowrap;display:flex;align-items:center;gap:6px;
}
.lvp-sb-hdr-toggle{
    display:inline-flex;align-items:center;gap:4px;flex:1;min-width:0;padding:4px 0;
    background:none;border:none;color:#64748b;
    font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.6px;
    cursor:pointer;text-align:left;transition:color .15s;user-select:none;
}
.lvp-sb-hdr-toggle:hover{color:#e2e8f0}
.lvp-sb-hdr-toggle-arrow{font-size:9px;margin-left:auto;opacity:.6}
.lvp-sort-btn{
    display:inline-flex;align-items:center;justify-content:center;
    min-width:18px;height:18px;padding:0 4px;
    background:#1e293b;border:1px solid #334155;border-radius:4px;
    color:#94a3b8;font-size:10px;cursor:pointer;transition:all .15s;
}
.lvp-sort-btn:hover{border-color:#64748b;color:#e2e8f0}
.lvp-item-list{flex:1;overflow-y:auto;padding:4px 0}
.lvp-item-btn{
    display:block;width:100%;text-align:left;padding:5px 10px;
    background:none;border:none;border-left:3px solid transparent;
    color:#e2e8f0;font-family:'Cascadia Code','Fira Code','Consolas',monospace;
    font-size:11px;cursor:pointer;white-space:nowrap;
    overflow:hidden;text-overflow:ellipsis;transition:background .12s;
}
.lvp-item-btn.lvp-subitem{padding-left:26px;color:#cbd5e1;font-size:10px}
.lvp-item-btn:hover{background:#1e293b}
.lvp-item-btn.lvp-active{
    background:#1e293b;border-left-color:#4da6ff;color:#4da6ff;font-weight:600;
}
.lvp-item-size{font-size:9px;color:#64748b;margin-left:6px}

/* viewer */
.lvp-viewer{flex:1;min-width:0;display:flex;flex-direction:column;gap:6px;overflow:hidden}

/* toolbar */
.lvp-toolbar{display:flex;align-items:center;gap:5px;flex-shrink:0;flex-wrap:wrap}
.lvp-host-sel,.lvp-lines-sel{
    font-size:12px;background:#1e293b;color:#e2e8f0;
    border:1px solid #334155;border-radius:4px;padding:2px 4px;cursor:pointer;
}
.lvp-sb-toggle{
    display:inline-flex;align-items:center;gap:4px;padding:3px 8px;
    background:#1e293b;border:1px solid #334155;border-radius:4px;
    color:#94a3b8;font-size:11px;cursor:pointer;transition:all .15s;user-select:none;
}
.lvp-sb-toggle:hover{border-color:#64748b;color:#e2e8f0}
.lvp-sb-arrow{transition:transform .18s;display:inline-block;font-size:9px;line-height:1}
.lvp-sb-toggle.lvp-sb-open .lvp-sb-arrow{transform:rotate(90deg)}
.lvp-item-badge{
    font-size:11px;color:#94a3b8;
    font-family:'Cascadia Code','Fira Code','Consolas',monospace;
    white-space:nowrap;overflow:hidden;text-overflow:ellipsis;
}
.lvp-sep{width:1px;height:18px;background:#334155;flex-shrink:0}
.lvp-file-size{font-size:11px;color:#888;min-width:40px}
.lvp-host-label{
    font-size:12px;color:#94a3b8;display:flex;align-items:center;gap:4px;white-space:nowrap;
}
.lvp-lines-label{
    font-size:12px;color:#94a3b8;display:flex;align-items:center;gap:4px;white-space:nowrap;
}

/* level buttons */
.lvp-lvl-btn{
    padding:3px 7px;border-radius:3px;border:1px solid #333640;
    background:#262730;font-size:11px;font-weight:700;cursor:pointer;
    font-family:monospace;transition:all .15s;opacity:.4;color:#aaa;
}
.lvp-lvl-btn.on{opacity:1.0}
.lvp-lvl-btn[data-lvl="DEBUG"].on   {background:#3a3f4b;border-color:#555;color:#e2e8f0}
.lvp-lvl-btn[data-lvl="INFO"].on    {background:#0d3b20;border-color:#21c354;color:#21c354}
.lvp-lvl-btn[data-lvl="WARNING"].on {background:#3b2700;border-color:#ff8c00;color:#ff8c00}
.lvp-lvl-btn[data-lvl="ERROR"].on   {background:#3b0d0d;border-color:#ff4b4b;color:#ff4b4b}
.lvp-lvl-btn[data-lvl="CRITICAL"].on{background:#2d0040;border-color:#b39ddb;color:#b39ddb}

/* control buttons */
.lvp-ctrl-btn{
    padding:3px 9px;background:#262730;border:1px solid #333640;
    border-radius:4px;color:#e2e8f0;font-size:12px;cursor:pointer;
    white-space:nowrap;transition:all .15s;line-height:1.6;
}
.lvp-ctrl-btn:hover{background:#4da6ff;color:#000;border-color:#4da6ff}
.lvp-ctrl-btn.lvp-stream-on{background:#21c354;color:#000;border-color:#21c354}
.lvp-ctrl-btn.lvp-active{background:#4da6ff;color:#000}

/* terminal */
.lvp-terminal{
    overflow-y:auto;
    font-family:'Cascadia Code','Fira Code','Consolas',monospace;
    font-size:12px;line-height:1.45;
    background:#000;color:#b0b0b0;
    padding:10px 12px;border:1px solid #1e293b;border-radius:5px;
    white-space:pre-wrap;word-break:break-all;
    flex:1;min-height:0;
}
.lvp-terminal.show-line-nums > div{padding-left:52px;position:relative}
.lvp-terminal.show-line-nums > div::before{
    content:attr(data-ln);position:absolute;left:0;width:44px;
    text-align:right;color:#555;font-size:11px;user-select:none;pointer-events:none;
}
.lvp-terminal.show-line-nums > div.lvp-separator::before{content:''}

/* log line classes */
.lvp-log-debug   {color:#808080}
.lvp-log-info    {color:#b0b0b0}
.lvp-log-warning {color:#ff8c00}
.lvp-log-error   {color:#ff4b4b}
.lvp-log-critical{color:#b39ddb;font-weight:600}
.lvp-ansi-bold{font-weight:600}
.lvp-ansi-fg-30{color:#8b95a7}
.lvp-ansi-fg-31{color:#ff4b4b}
.lvp-ansi-fg-32{color:#21c354}
.lvp-ansi-fg-33{color:#f4b942}
.lvp-ansi-fg-34{color:#7cc7ff}
.lvp-ansi-fg-35{color:#d28cff}
.lvp-ansi-fg-36{color:#58c7e6}
.lvp-ansi-fg-37{color:#e2e8f0}
.lvp-ansi-fg-90{color:#64748b}
.lvp-ansi-fg-91{color:#ff7a7a}
.lvp-ansi-fg-92{color:#69e69c}
.lvp-ansi-fg-93{color:#ffd36b}
.lvp-ansi-fg-94{color:#9bd7ff}
.lvp-ansi-fg-95{color:#ebb0ff}
.lvp-ansi-fg-96{color:#86eaff}
.lvp-ansi-fg-97{color:#f8fafc}
.lvp-hidden      {display:none !important}
.lvp-level-hidden{display:none !important}
.lvp-highlight      {background:rgba(255,200,0,.18)}
.lvp-highlight mark {background:#e8a620;color:#000;border-radius:2px;padding:0 1px}
.lvp-current-match  {background:rgba(255,160,0,.40);outline:1px solid #e8a620}

/* context / blocks */
.lvp-context{opacity:.5}
.lvp-separator{
    text-align:center;color:#64748b;font-size:11px;padding:2px 0;
    user-select:none;border-top:1px dotted #334155;border-bottom:1px dotted #334155;margin:2px 0;
}
.lvp-group-first{cursor:pointer;padding-left:20px !important;position:relative}
.lvp-group-first .grp-arrow{
    position:absolute;left:2px;top:0;color:#fff;font-size:11px;
    text-shadow:0 0 2px rgba(0,0,0,.8);pointer-events:none;
}
.lvp-terminal.show-line-nums .lvp-group-first{padding-left:72px !important}
.lvp-terminal.show-line-nums .lvp-group-first .grp-arrow{left:52px}
.lvp-group-first .grp-count{font-size:10px;color:#64748b;margin-left:8px}

/* search bar */
.lvp-searchbar{display:flex;gap:6px;align-items:center;flex-shrink:0;flex-wrap:wrap}
.lvp-searchbar select{
    font-size:12px;background:#1e293b;color:#e2e8f0;
    border:1px solid #334155;border-radius:4px;padding:3px 6px;
}
.lvp-searchbar input[type="text"]{
    flex:1;min-width:140px;font-size:12px;background:#1e293b;color:#e2e8f0;
    border:1px solid #334155;border-radius:4px;padding:4px 8px;
}
.lvp-searchbar label{
    font-size:12px;color:#94a3b8;display:inline-flex;align-items:center;
    gap:4px;cursor:pointer;white-space:nowrap;
}
.lvp-ctx-sel{padding:3px 6px;font-size:11px}
.lvp-grp-link{
    font-size:11px;color:#64748b;text-decoration:underline;cursor:pointer;
    background:none;border:none;font-family:inherit;
}
.lvp-grp-link:hover{color:#e2e8f0}
.lvp-nav-btn{
    background:#262730;border:1px solid #333640;border-radius:3px;
    color:#94a3b8;cursor:pointer;font-size:12px;padding:2px 6px;line-height:1;
}
.lvp-nav-btn:hover{color:#e2e8f0;border-color:#94a3b8}
.lvp-match-count{font-size:11px;color:#888;min-width:70px}
.lvp-conn-badge{font-size:11px;color:#64748b}
/* ── end LogViewerPanel v2 ─────────────────────────────────────────── */
`;
        document.head.appendChild(s);
    }

    /* ═══════════════════════════════════════════════════════════
       Constructor
       ═══════════════════════════════════════════════════════ */
    constructor(opts) {
        this._cid     = opts.containerId;
        this._wsBase  = opts.wsBase;
        this._token   = opts.token;
        this._height  = opts.height || null;

        /* defaults */
        this._host     = (opts.defaultHost || 'local').toLowerCase() === 'local' ? 'local' : opts.defaultHost;
        this._file     = opts.defaultFile  || '';
        this._service  = opts.defaultService || 'PBRun';
        this._presetSet = LogViewerPanel.PRESETS[opts.presets] || LogViewerPanel.PRESETS.system;
        this._showRestart = !!opts.showRestart;
        this._onFileChange = opts.onFileChange || null;
        this._localFileFilter = typeof opts.localFileFilter === 'function' ? opts.localFileFilter : null;
        this._startLocalAtEnd = !!opts.startLocalAtEnd;
        this._serviceListOverride = typeof opts.serviceListOverride === 'function' ? opts.serviceListOverride : null;
        this._serviceStatusProvider = typeof opts.serviceStatusProvider === 'function' ? opts.serviceStatusProvider : null;
        this._taskBrowseMode = !!opts.taskBrowseMode;
        this._taskListSortMode = opts.taskListSortMode || 'newest';

        /* runtime state */
        this._ws           = null;
        this._sid          = 0;
        this._streaming    = false;
        this._lines        = [];
        this._lineBase     = 0;
        this._pending      = [];
        this._rafPending   = false;
        this._renderAbort  = 0;
        this._visLevels    = new Set(['DEBUG','INFO','WARNING','ERROR','CRITICAL']);
        this._searchTerm   = '';
        this._searchRegex  = false;
        this._filterMode   = true;
        this._matchEls     = [];
        this._matchIdx     = -1;
        this._searchAbort  = 0;
        this._searchTimer  = null;
        this._showLineNums = false;
        this._sidebarOpen  = true;
        this._fileList     = [];
        this._vpState      = null;
        this._contextLines = 5;
        this._blocksCollapsed = true;
        this._fileSize     = null;
        this._restartResetTimer = 0;
        this._restartTargetService = null;
        this._pendingRestartCommand = null;
        this._startRemoteAtEnd = false;
        this._closed = false;
        this._reconnectTimer = 0;

        this._MAX    = 5000;
        this._CHUNK  = 500;
        this._SCHUNK = 400;
        this._MAXLINES = 5000;  /* tracks dropdown; 0 = unlimited */

        LogViewerPanel._injectStyles();
        this._build();
        this._bindEvents();
    }

    _normalizeIncomingLines(lines) {
        var normalized = [];
        for (var i = 0; i < (lines || []).length; i++) {
            var expanded = this._expandIncomingLine(lines[i]);
            if (expanded.length) normalized.push.apply(normalized, expanded);
            else normalized.push('');
        }
        return normalized;
    }

    _prettyFormatStructuredPayload(text) {
        var source = String(text == null ? '' : text);
        if (!source) return source;

        var prefix = '';
        var payload = source;
        var match = source.match(/^(.*?=>\s*)([{[][^]*)$/);
        if (match) {
            prefix = match[1] || '';
            payload = match[2] || '';
        } else if (!/^\s*[{[]/.test(source)) {
            return source;
        }

        try {
            return prefix + JSON.stringify(JSON.parse(this._stripAnsi(payload)), null, 2);
        } catch (error) {
            return source;
        }
    }

    _expandIncomingLine(line) {
        var text = String(line == null ? '' : line);
        if (!text) return [''];
        var structuredPayload = /=>\s*[{[]/.test(text);

        if (structuredPayload) {
            text = this._prettyFormatStructuredPayload(text);
            text = text
                .replace(/\\r\\n/g, '\n')
                .replace(/\\n/g, '\n')
                .replace(/\\r/g, '\n')
                .replace(/\\t/g, '    ');
        }

        text = text.replace(/\r\n?/g, '\n');

        var ansi = '(?:\\x1B(?:[@-Z\\\\-_]|\\[[0-?]*[ -/]*[@-~]))*';
        var markers = '(?:\\[WARNING\\]:|ok: \\[|changed: \\[|fatal: \\[|failed: \\[|skipping: \\[|PLAY \\[|TASK \\[|RUNNING HANDLER \\[|PLAY RECAP)';
        text = text.replace(new RegExp('(\\*{5,})(?=' + ansi + markers + ')', 'g'), '$1\n');
        text = text.replace(new RegExp('([^\\n])(' + ansi + markers + ')', 'g'), '$1\n$2');
        text = text.replace(/\n{3,}/g, '\n\n');

        var parts = text.split('\n');
        if (!structuredPayload) return parts;

        var compact = [];
        for (var i = 0; i < parts.length; i++) {
            if (parts[i].trim()) compact.push(parts[i]);
        }
        return compact.length ? compact : [''];
    }

    /* ── DOM helper ───────────────────────────────────────────── */
    _q(suffix) { return document.getElementById(this._cid + '-lvp-' + suffix); }

    /* ═══════════════════════════════════════════════════════════
       Build HTML
       ═══════════════════════════════════════════════════════ */
    _build() {
        var c = document.getElementById(this._cid);
        if (!c) return;
        if (this._height) c.style.height = this._height;
        var p = this._cid + '-lvp-';
        var esc = function(s) { return s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;'); };

        /* preset options */
        var preOpts = '<option value="">\u2014 Preset \u2014</option>';
        for (var i = 0; i < this._presetSet.length; i++) {
            var pr = this._presetSet[i];
            preOpts += '<option value="' + esc(pr.v) + '">' + esc(pr.l) + '</option>';
        }

        c.innerHTML =
'<div class="lvp-root">' +
  '<!-- sidebar -->' +
  '<div class="lvp-sidebar" id="' + p + 'sidebar">' +
    '<div class="lvp-sidebar-header">' +
      '<button class="lvp-sb-hdr-toggle" id="' + p + 'sb-hdr-toggle">' +
        'Files <span class="lvp-sb-hdr-toggle-arrow">&#9664;</span>' +
      '</button>' +
      '<button class="lvp-sort-btn lvp-hidden" type="button" id="' + p + 'sort-btn" title="Toggle sort order" aria-label="Toggle sort order">&#8595;</button>' +
    '</div>' +
    '<div class="lvp-item-list" id="' + p + 'item-list"></div>' +
    '<div class="lvp-sidebar-resize" id="' + p + 'sidebar-resize"></div>' +
  '</div>' +
  '<!-- viewer -->' +
  '<div class="lvp-viewer">' +
    '<!-- toolbar -->' +
    '<div class="lvp-toolbar">' +
      '<label class="lvp-host-label">Host:' +
        '<select class="lvp-host-sel" id="' + p + 'host-sel">' +
          '<option value="local" selected>Local</option>' +
        '</select>' +
      '</label>' +
      '<div class="lvp-sep"></div>' +
      '<button class="lvp-sb-toggle lvp-sb-open" id="' + p + 'sb-toggle" style="display:none">' +
        '<span class="lvp-sb-arrow">&#9658;</span> Files' +
      '</button>' +
      '<span class="lvp-item-badge" id="' + p + 'item-badge"></span>' +
      '<div class="lvp-sep"></div>' +
      '<div style="display:flex;gap:3px;">' +
        '<button class="lvp-lvl-btn on" data-lvl="DEBUG"    id="' + p + 'lvl-DEBUG">DBG</button>' +
        '<button class="lvp-lvl-btn on" data-lvl="INFO"     id="' + p + 'lvl-INFO">INF</button>' +
        '<button class="lvp-lvl-btn on" data-lvl="WARNING"  id="' + p + 'lvl-WARNING">WRN</button>' +
        '<button class="lvp-lvl-btn on" data-lvl="ERROR"    id="' + p + 'lvl-ERROR">ERR</button>' +
        '<button class="lvp-lvl-btn on" data-lvl="CRITICAL" id="' + p + 'lvl-CRITICAL">CRT</button>' +
      '</div>' +
      '<div class="lvp-sep"></div>' +
      '<label class="lvp-lines-label">Lines:' +
        '<select class="lvp-lines-sel" id="' + p + 'lines-sel">' +
          '<option value="200" selected>200</option>' +
          '<option value="500">500</option>' +
          '<option value="1000">1000</option>' +
          '<option value="2000">2000</option>' +
          '<option value="5000">5000</option>' +
          '<option value="0">All</option>' +
        '</select>' +
      '</label>' +
      '<span class="lvp-file-size" id="' + p + 'file-size"></span>' +
      '<div class="lvp-sep"></div>' +
      '<button class="lvp-ctrl-btn lvp-stream-on" id="' + p + 'stream-btn">&#9208; Pause</button>' +
      '<button class="lvp-ctrl-btn" id="' + p + 'fetch-btn">&#128229; Fetch</button>' +
      '<button class="lvp-ctrl-btn" id="' + p + 'clear-btn">&#128465; Clear</button>' +
      '<button class="lvp-ctrl-btn" id="' + p + 'dl-btn">\u2B07 Download</button>' +
      (this._showRestart
        ? '<button class="lvp-ctrl-btn" id="' + p + 'restart-btn">&#128260; Restart</button>'
        : '') +
      '<button class="lvp-ctrl-btn" id="' + p + 'ln-btn"># Lines</button>' +
      '<div class="lvp-sep"></div>' +
      '<span class="lvp-conn-badge" id="' + p + 'conn">connecting\u2026</span>' +
    '</div>' +
    '<!-- search bar -->' +
    '<div class="lvp-searchbar">' +
      '<select id="' + p + 'preset">' + preOpts + '</select>' +
      '<input type="text" id="' + p + 'search" placeholder="Search logs\u2026">' +
      '<label><input type="checkbox" id="' + p + 'filter-chk" checked> Filter</label>' +
      '<select class="lvp-ctx-sel" id="' + p + 'ctx-sel" style="display:none">' +
        '<option value="3">\u00b13 lines</option>' +
        '<option value="5" selected>\u00b15 lines</option>' +
        '<option value="10">\u00b110 lines</option>' +
        '<option value="20">\u00b120 lines</option>' +
      '</select>' +
      '<span id="' + p + 'grp-actions" style="display:none">' +
        '<button class="lvp-grp-link" id="' + p + 'expand-all">Expand all</button>' +
        '<button class="lvp-grp-link" id="' + p + 'collapse-all">Collapse all</button>' +
      '</span>' +
      '<span id="' + p + 'nav-btns" style="display:none;gap:2px;">' +
        '<button class="lvp-nav-btn" id="' + p + 'nav-up" title="Prev (Shift+Enter)">&#9650;</button>' +
        '<button class="lvp-nav-btn" id="' + p + 'nav-dn" title="Next (Enter)">&#9660;</button>' +
      '</span>' +
      '<span class="lvp-match-count" id="' + p + 'match-count"></span>' +
    '</div>' +
    '<!-- terminal -->' +
    '<div id="' + p + 'terminal" class="lvp-terminal"></div>' +
  '</div>' +
'</div>';
    }

    /* ═══════════════════════════════════════════════════════════
       Event binding
       ═══════════════════════════════════════════════════════ */
    _bindEvents() {
        var me = this;
        var levels = ['DEBUG','INFO','WARNING','ERROR','CRITICAL'];
        for (var i = 0; i < levels.length; i++)
            (function(lvl) {
                me._q('lvl-' + lvl).addEventListener('click', function() { me._toggleLevel(lvl); });
            })(levels[i]);

        this._q('host-sel').addEventListener('change',   function() { me._onHostChange(); });
        this._q('lines-sel').addEventListener('change',  function() { var v = parseInt(me._q('lines-sel').value, 10); me._MAXLINES = v; me._MAX = v > 0 ? v : Infinity; me._subscribe(); });
        this._q('stream-btn').addEventListener('click',  function() { me._toggleStream(); });
        this._q('fetch-btn').addEventListener('click',   function() { me._fetchOnce(); });
        this._q('clear-btn').addEventListener('click',   function() { me._clear(); });
        this._q('dl-btn').addEventListener('click',      function() { me._download(); });
        this._q('ln-btn').addEventListener('click',      function() { me._toggleLineNums(); });
        this._q('sb-toggle').addEventListener('click',   function() { me._toggleSidebar(); });
        this._q('sb-hdr-toggle').addEventListener('click', function() { me._toggleSidebar(); });
        this._q('sort-btn').addEventListener('click', function(e) { e.preventDefault(); e.stopPropagation(); me._toggleTaskListSort(); });
        this._q('sidebar-resize').addEventListener('mousedown', function(e) { me._initResize(e); });
        this._q('preset').addEventListener('change',     function() { me._onPresetChange(); });
        this._q('search').addEventListener('input',      function() { me._onSearchInput(); });
        this._q('search').addEventListener('keydown',    function(e) { me._onSearchKeydown(e); });
        this._q('filter-chk').addEventListener('change', function() { me._onFilterToggle(); });
        this._q('ctx-sel').addEventListener('change',    function() { me._onContextChange(); });
        this._q('expand-all').addEventListener('click',  function(e) { e.preventDefault(); me._toggleAllGroups(true); });
        this._q('collapse-all').addEventListener('click',function(e) { e.preventDefault(); me._toggleAllGroups(false); });
        this._q('nav-up').addEventListener('click',      function() { me._searchNav(-1); });
        this._q('nav-dn').addEventListener('click',      function() { me._searchNav(1); });

        if (this._showRestart) {
            var rb = this._q('restart-btn');
            if (rb) rb.addEventListener('click', function() { me._restart(); });
        }

        /* block toggle: event delegation on terminal */
        this._q('terminal').addEventListener('click', function(e) {
            var first = e.target.closest('.lvp-group-first');
            if (!first) return;
            e.preventDefault();
            var blk = first.dataset.blk;
            if (blk === undefined) return;
            first.classList.toggle('collapsed');
            var show = !first.classList.contains('collapsed');
            var arrow = first.querySelector('.grp-arrow');
            if (arrow) arrow.textContent = show ? '\u25bc ' : '\u25b6 ';
            var details = me._q('terminal').querySelectorAll('.lvp-grp-detail[data-blk="' + blk + '"]');
            for (var j = 0; j < details.length; j++)
                details[j].style.display = show ? '' : 'none';
        });
    }

    /* ═══════════════════════════════════════════════════════════
       Public API
       ═══════════════════════════════════════════════════════ */
    open()  { this._closed = false; this._connect(); }
    close() { this._closed = true; this._disconnect(); }

    /** Currently selected file (local mode) */
    get currentFile() { return this._file; }

    setHost(host) {
        var sel = this._q('host-sel');
        var h = (!host || host === 'local') ? 'local' : host;
        this._host = h;
        if (sel) {
            /* if host not yet in dropdown, add it temporarily */
            var found = false;
            for (var i = 0; i < sel.options.length; i++) {
                if (sel.options[i].value === h) { found = true; break; }
            }
            if (!found && h !== 'local') {
                var o = document.createElement('option');
                o.value = h; o.textContent = h;
                sel.appendChild(o);
            }
            sel.value = h;
        }
        this._onHostChange();
    }

    setService(svc) {
        if (this._host !== 'local') {
            this._selectItem(svc);
        } else {
            this._service = svc;
        }
    }

    setFile(file) {
        if (this._host === 'local') {
            this._selectItem(file);
        } else {
            this._file = file;
        }
    }

    /** One-shot fetch of a specific file (e.g. rotated .log.1). No streaming. */
    fetchFile(filename) {
        this._unsubscribe();
        this._clear();
        var sid = ++this._sid;
        this._send({ cmd: 'get_local_logs', file: filename, lines: this._getLines(), sid: sid });
        this._streaming = false;
        this._updateStreamBtn();
    }

    /* ═══════════════════════════════════════════════════════════
       WebSocket
       ═══════════════════════════════════════════════════════ */
    _connect() {
        if (this._reconnectTimer) {
            clearTimeout(this._reconnectTimer);
            this._reconnectTimer = 0;
        }
        this._disconnect();
        var url = this._wsBase + '/ws/vps?token=' + encodeURIComponent(this._token);
        var ws  = new WebSocket(url);
        this._ws  = ws;
        var me  = this;

        ws.onopen = function() {
            var ce = me._q('conn');
            if (ce) ce.textContent = 'connected';
            ws.send(JSON.stringify({ cmd: 'list_local_logs' }));
            me._subscribe();
            me._flushPendingRestart(ws);
        };
        ws.onmessage = function(evt) {
            try { me._handleMsg(JSON.parse(evt.data)); } catch(e) { /* ignore parse errors */ }
        };
        ws.onerror = function() {
            var ce = me._q('conn');
            if (ce) ce.textContent = 'error';
        };
        ws.onclose = function() {
            if (me._ws !== ws) return;
            me._ws = null;
            me._streaming = false;
            me._updateStreamBtn();
            var ce = me._q('conn');
            if (ce) ce.textContent = 'disconnected';
            if (!me._closed && !me._reconnectTimer) {
                me._reconnectTimer = setTimeout(function() {
                    me._reconnectTimer = 0;
                    if (!me._closed) me._connect();
                }, 2000);
            }
        };
    }

    _disconnect() {
        if (this._ws) {
            try { this._ws.close(); } catch(e) { /* ignore */ }
            this._ws = null;
        }
        this._streaming = false;
    }

    _send(obj) {
        if (this._ws && this._ws.readyState === WebSocket.OPEN)
            this._ws.send(JSON.stringify(obj));
    }

    _sendRestart(obj) {
        if (this._ws && this._ws.readyState === WebSocket.OPEN) {
            this._ws.send(JSON.stringify(obj));
            return true;
        }
        this._pendingRestartCommand = obj;
        if (!this._ws || this._ws.readyState === WebSocket.CLOSING || this._ws.readyState === WebSocket.CLOSED) {
            this._connect();
        }
        return false;
    }

    _flushPendingRestart(ws) {
        if (!this._pendingRestartCommand || this._ws !== ws || ws.readyState !== WebSocket.OPEN) return;
        var cmd = this._pendingRestartCommand;
        this._pendingRestartCommand = null;
        ws.send(JSON.stringify(cmd));
    }

    /* ── Message handling ─────────────────────────────────────── */
    _handleMsg(msg) {
        switch (msg.type) {
        case 'state':
            this._vpState = msg.data || msg;
            this._updateHostDropdown();
            if (this._host === 'local' && this._vpState.local_logs)
                this._updateFileList(this._vpState.local_logs);
            if (this._host !== 'local') {
                var nextServices = this._getServices();
                if (!this._arrayEq(this._lastServiceList || [], nextServices)) {
                    this._lastServiceList = nextServices;
                    this._buildServiceList();
                }
            }
            break;

        case 'local_logs_list':
            this._updateFileList(msg.files || []);
            break;

        case 'local_logs':
            if (msg.sid !== undefined && msg.sid !== this._sid) return;
            this._lines    = this._normalizeIncomingLines(msg.lines || []);
            this._lineBase = 0;
            this._streaming = !!msg.streaming;
            if (msg.file_size !== undefined) this._showFileSize(msg.file_size);
            this._updateStreamBtn();
            this._renderFull();
            break;

        case 'local_log_lines':
            if (msg.sid !== undefined && msg.sid !== this._sid) return;
            this._ingestLines(msg.lines || []);
            break;

        case 'logs':
            if (msg.sid !== undefined && msg.sid !== this._sid) return;
            this._lines    = this._normalizeIncomingLines(msg.lines || []);
            this._lineBase = 0;
            if (msg.streaming) this._streaming = true;
            this._updateStreamBtn();
            this._renderFull();
            break;

        case 'log_lines':
            if (msg.sid !== undefined && msg.sid !== this._sid) return;
            this._ingestLines(msg.lines || []);
            break;

        case 'log_info':
            if (msg.size !== undefined) this._showFileSize(msg.size);
            break;

        case 'result':
            if (this._showRestart && (msg.cmd === 'restart_service' || msg.cmd === 'kill_instance')) {
                var rb = this._q('restart-btn');
                if (rb) {
                    rb.disabled = false;
                    rb.textContent = msg.success ? '\u2705 Restarted' : '\u274c Failed';
                    setTimeout(function() { rb.textContent = '\ud83d\udd04 Restart'; }, 3000);
                }
                if (msg.success) this._resetAfterRestart(this._restartTargetService || this._activeItem());
                this._restartTargetService = null;
            }
            break;
        }
    }

    _resetAfterRestart(nextItem) {
        var me = this;
        if (this._restartResetTimer) {
            clearTimeout(this._restartResetTimer);
            this._restartResetTimer = 0;
        }
        this._unsubscribe();
        this._clear();
        this._showFileSize(null);
        this._streaming = false;
        if (!this._isLocal() && nextItem) {
            this._service = nextItem;
            this._startRemoteAtEnd = true;
            this._buildServiceList();
        }
        this._updateStreamBtn();
        this._updateBadge();
        this._updateRestartBtn();
        this._restartResetTimer = setTimeout(function() {
            me._restartResetTimer = 0;
            me._subscribe();
        }, 2500);
    }

    _ingestLines(newLines) {
        var normalized = this._normalizeIncomingLines(newLines);
        this._lines.push.apply(this._lines, normalized);
        if (this._MAX !== Infinity && this._lines.length > this._MAX) {
            var trim = this._lines.length - this._MAX;
            this._lines = this._lines.slice(-this._MAX);
            this._lineBase += trim;
        }
        this._appendLines(normalized);
    }

    /* ═══════════════════════════════════════════════════════════
       Host / sidebar
       ═══════════════════════════════════════════════════════ */
    _isLocal() { return this._host === 'local'; }

    _activeItem() { return this._isLocal() ? this._file : this._service; }

    _onHostChange() {
        var sel = this._q('host-sel');
        this._host = sel ? sel.value : 'local';
        this._unsubscribe();
        this._clear();

        var label = this._isLocal() ? 'Files' : 'Services';
        var sbToggle = this._q('sb-toggle');
        if (sbToggle)
            sbToggle.innerHTML = '<span class="lvp-sb-arrow">&#9658;</span> ' + label;
        var hdrToggle = this._q('sb-hdr-toggle');
        if (hdrToggle)
            hdrToggle.innerHTML = label + ' <span class="lvp-sb-hdr-toggle-arrow">&#9664;</span>';

        if (this._isLocal()) this._buildFileList();
        else this._buildServiceList();

        this._updateBadge();
        this._updateRestartBtn();
        this._subscribe();
    }

    _updateHostDropdown() {
        var sel = this._q('host-sel');
        if (!sel) return;
        var hosts = ['local'];
        if (this._vpState && this._vpState.connections) {
            var conns = this._vpState.connections.connections || this._vpState.connections || {};
            for (var h in conns) {
                if (conns.hasOwnProperty(h) && conns[h] &&
                    String(conns[h].status).toUpperCase() === 'CONNECTED')
                    hosts.push(h);
            }
        }
        hosts.sort(function(a, b) {
            if (a === 'local') return -1;
            if (b === 'local') return 1;
            return a.localeCompare(b);
        });
        if (this._arrayEq(this._lastHostList || [], hosts)) {
            if (hosts.indexOf(this._host) >= 0 && sel.value !== this._host) sel.value = this._host;
            return;
        }
        this._lastHostList = hosts;
        var prev = sel.value;
        sel.innerHTML = '';
        for (var i = 0; i < hosts.length; i++) {
            var o = document.createElement('option');
            o.value = hosts[i];
            o.textContent = hosts[i] === 'local' ? 'Local' : hosts[i];
            sel.appendChild(o);
        }
        if (hosts.indexOf(prev) >= 0) sel.value = prev;
        else if (hosts.indexOf(this._host) >= 0) sel.value = this._host;
        /* if the originally requested host just appeared, auto-select */
        if (this._host !== 'local' && sel.value !== this._host && hosts.indexOf(this._host) >= 0) {
            sel.value = this._host;
            this._onHostChange();
        }
    }

    _arrayEq(a, b) {
        if (a.length !== b.length) return false;
        for (var i = 0; i < a.length; i++) if (a[i] !== b[i]) return false;
        return true;
    }

    _updateFileList(files) {
        var nextFiles = (files || []).slice();
        if (this._localFileFilter)
            nextFiles = nextFiles.filter(this._localFileFilter);
        nextFiles.sort();
        if (this._arrayEq(this._fileList, nextFiles)) return;
        this._fileList = nextFiles;
        var hadFile = !!this._file;
        if (!hadFile && this._fileList.length) {
            this._file = this._fileList[0];
        }
        if (this._file && this._fileList.indexOf(this._file) < 0) {
            this._fileList.unshift(this._file);
        }
        if (this._isLocal()) {
            this._buildFileList();
        }
        if (this._isLocal() && !hadFile && this._file) {
            this._subscribe();
        }
    }

    _taskLabel(rawAction) {
        var action = rawAction || '';
        var match = action.match(/^(.*?)(?:\.(\d+))?$/);
        var command = match ? (match[1] || '') : action;
        var history = match && match[2] ? parseInt(match[2], 10) : 0;
        var runMatch = command.match(/^(.*?)(?:--run[-_](\d+)|\s+Run\s+(\d+))$/i);
        var runId = runMatch ? parseInt(runMatch[2] || runMatch[3] || '0', 10) : 0;
        if (runMatch) command = runMatch[1] || command;
        var labels = {
            'init': 'Initialize',
            'setup': 'Setup VPS',
            'update': 'Update',
            'vps_init': 'Initialize',
            'vps_setup': 'Setup VPS',
            'vps_update': 'Update',
            'vps-init': 'Initialize',
            'vps-setup': 'Setup VPS',
            'vps-update': 'Update Linux',
            'vps-update-pbgui': 'Update PBGui',
            'vps-update-pb': 'Update PBGui and PB7',
            'vps-pb7-python312': 'Update PB7 venv',
            'vps-pbgui-python312': 'Update PBGui venv',
            'vps-reboot': 'Reboot VPS',
            'vps-cleanup': 'Cleanup VPS',
            'vps-resize-swap': 'Resize Swap',
            'vps-update-firewall': 'Update Firewall Settings',
            'vps-update-coindata': 'Update CoinData API',
            'master-update-pb': 'Update PBGui and PB7',
            'master-update-pbgui': 'Update PBGui',
            'master-update-pb7': 'Update PB7',
            'master-install-rustup': 'Install or Update rustup',
            'master-install-rclone': 'Install or Update rclone'
        };
        var label = labels[command] || command
            .replace(/^vps[-_]/, '')
            .replace(/^master[-_]/, '')
            .split(/[-_]+/)
            .filter(Boolean)
            .map(function(part) {
                return part.charAt(0).toUpperCase() + part.slice(1);
            })
            .join(' ');
        return {
            command: command,
            history: history,
            runId: runId,
            label: label || 'Task'
        };
    }

    _taskHistoryBase(file) {
        var f = file || '';
        if (f.indexOf('VPSAction:') === 0) {
            var parts = f.split(':');
            var host = parts[1] || '';
            var info = this._taskLabel(parts.slice(2).join(':'));
            return 'VPSAction:' + host + ':' + info.command;
        }
        if (f.indexOf('MasterAction:') === 0) {
            var masterInfo = this._taskLabel(f.split(':').slice(1).join(':'));
            return 'MasterAction:' + masterInfo.command;
        }
        return '';
    }

    _taskHistoryIndex(file) {
        var f = file || '';
        if (f.indexOf('VPSAction:') === 0) {
            return this._taskLabel(f.split(':').slice(2).join(':')).runId;
        }
        if (f.indexOf('MasterAction:') === 0) {
            return this._taskLabel(f.split(':').slice(1).join(':')).runId;
        }
        return 0;
    }

    _taskHistoryTieBreaker(file) {
        var f = file || '';
        if (f.indexOf('VPSAction:') === 0) {
            return this._taskLabel(f.split(':').slice(2).join(':')).history;
        }
        if (f.indexOf('MasterAction:') === 0) {
            return this._taskLabel(f.split(':').slice(1).join(':')).history;
        }
        return 0;
    }

    _orderedFileList() {
        var files = this._fileList.slice();
        if (!this._taskBrowseMode) {
            return this._serviceEntries(files).map(function(entry) { return entry.value; });
        }
        if (this._taskBrowseMode) {
            var mode = this._taskListSortMode || 'newest';
            if (mode === 'alphabetical') {
                return files.sort(function(a, b) {
                    return String(this._fileLabel(a) || a).localeCompare(String(this._fileLabel(b) || b));
                }.bind(this));
            }
            files.sort(function(a, b) {
                var ai = this._taskHistoryIndex(a);
                var bi = this._taskHistoryIndex(b);
                if (ai !== bi) return mode === 'oldest' ? ai - bi : bi - ai;
                var at = this._taskHistoryTieBreaker(a);
                var bt = this._taskHistoryTieBreaker(b);
                if (at !== bt) return mode === 'oldest' ? at - bt : bt - at;
                return String(this._fileLabel(a) || a).localeCompare(String(this._fileLabel(b) || b));
            }.bind(this));
            return files;
        }
        var base = this._taskHistoryBase(this._file);
        if (!base) return files;
        var me = this;
        var related = files.filter(function(item) { return me._taskHistoryBase(item) === base; });
        if (!related.length) return files;
        related.sort(function(a, b) {
            return me._taskHistoryIndex(a) - me._taskHistoryIndex(b);
        });
        var rest = files.filter(function(item) { return me._taskHistoryBase(item) !== base; });
        return related.concat(rest);
    }

    _fileLabel(file) {
        var f = file || '';
        if (!f) return '';
        if (f.indexOf('Bot:') === 0) return '\ud83e\udd16 ' + f.substring(4);
        if (f.indexOf('BotErr:') === 0) return '\u26a0\ufe0f ' + f.substring(7) + ' error';
        if (f.indexOf('pb7/logs/') === 0 || f.indexOf('software/pb7/logs/') === 0) {
            return this._svcLabel(f);
        }
        if (f.indexOf('VPSAction:') === 0) {
            var vpsParts = f.split(':');
            var host = vpsParts[1] || 'VPS';
            var vpsInfo = this._taskLabel(vpsParts.slice(2).join(':'));
            return host + ' ' + vpsInfo.label + (vpsInfo.history ? ' (History ' + vpsInfo.history + ')' : '');
        }
        if (f.indexOf('MasterAction:') === 0) {
            var masterInfo = this._taskLabel(f.split(':').slice(1).join(':'));
            return 'Master ' + masterInfo.label + (masterInfo.history ? ' (History ' + masterInfo.history + ')' : '');
        }
        if (f.indexOf('/') >= 0) {
            var pathParts = f.split('/').filter(Boolean);
            return pathParts[pathParts.length - 1] || f;
        }
        return f;
    }

    _buildFileList() {
        var list = this._q('item-list');
        var sortBtn = this._q('sort-btn');
        if (!list) return;
        if (sortBtn) {
            if (this._taskBrowseMode) {
                sortBtn.classList.remove('lvp-hidden');
                sortBtn.textContent = this._taskListSortMode === 'alphabetical' ? 'A' : (this._taskListSortMode === 'oldest' ? '\u2191' : '\u2193');
                sortBtn.title = 'Sort: ' + (this._taskListSortMode || 'newest');
                sortBtn.setAttribute('aria-label', 'Sort: ' + (this._taskListSortMode || 'newest'));
            } else {
                sortBtn.classList.add('lvp-hidden');
            }
        }
        list.innerHTML = '';
        var me = this;
        var files = this._orderedFileList();
        var entries = this._serviceEntries(files);
        for (var i = 0; i < entries.length; i++) {
            (function(entry) {
                var btn = document.createElement('button');
                btn.className = 'lvp-item-btn' + (entry.value === me._file ? ' lvp-active' : '') + (entry.className ? ' ' + entry.className : '');
                btn.textContent = entry.label;
                btn.title = entry.title || entry.value;
                btn.addEventListener('click', function() { me._selectItem(entry.value); });
                list.appendChild(btn);
            })(entries[i]);
        }
    }

    _toggleTaskListSort() {
        if (!this._taskBrowseMode) return;
        var modes = ['newest', 'oldest', 'alphabetical'];
        var current = modes.indexOf(this._taskListSortMode || 'newest');
        this._taskListSortMode = modes[(current + 1) % modes.length];
        this._buildFileList();
    }

    _collectKnownBotNames(services) {
        var names = [];
        var seen = {};
        function add(name) {
            var n = String(name || '');
            if (!n || seen[n]) return;
            seen[n] = true;
            names.push(n);
        }

        for (var i = 0; i < services.length; i++) {
            var svc = String(services[i] || '');
            if (svc.indexOf('Bot:') === 0) add(svc.substring(4).split(':')[0]);
            else if (svc.indexOf('BotErr:') === 0) add(svc.substring(7));
        }

        if (this._vpState && this._host !== 'local') {
            var v7 = this._vpState.v7_instances || {};
            var v7Insts = v7[this._host] || [];
            for (var j = 0; j < v7Insts.length; j++) add(v7Insts[j] && v7Insts[j].name);

            var botLogsByHost = this._vpState.bot_logs || {};
            var botLogs = botLogsByHost[this._host] || {};
            for (var botName in botLogs) {
                if (Object.prototype.hasOwnProperty.call(botLogs, botName)) add(botName);
            }
        }

        names.sort(function(a, b) { return b.length - a.length; });
        return names;
    }

    _archiveLogMeta(path, knownBots) {
        var svc = String(path || '');
        if (svc.indexOf('pb7/logs/') !== 0 && svc.indexOf('software/pb7/logs/') !== 0) return null;

        var base = svc.split('/').filter(Boolean).pop() || svc;
        var tsMatch = base.match(/^(\d{4})(\d{2})(\d{2})_(\d{2})(\d{2})(\d{2})/);
        var timestamp = '';
        var sortKey = '';
        if (tsMatch) {
            timestamp = tsMatch[1] + '-' + tsMatch[2] + '-' + tsMatch[3] + ' ' + tsMatch[4] + ':' + tsMatch[5] + ':' + tsMatch[6];
            sortKey = tsMatch[1] + tsMatch[2] + tsMatch[3] + tsMatch[4] + tsMatch[5] + tsMatch[6];
        }

        var bot = '';
        var inferred = base.match(/data_run_v7_(.+?)(?:_config_run(?:\.[^.]+)?|_con(?:fig(?:_run)?)?)\.log$/);
        if (inferred) bot = inferred[1];

        if (!bot) {
            var candidates = Array.isArray(knownBots) ? knownBots : [];
            for (var i = 0; i < candidates.length; i++) {
                if (base.indexOf(candidates[i]) >= 0) {
                    bot = candidates[i];
                    break;
                }
            }
        }

        return {
            bot: bot,
            timestamp: timestamp,
            sortKey: sortKey,
        };
    }

    _botErrorLogMeta(path) {
        var svc = String(path || '');
        var aliasMatch = svc.match(/^BotErr:([^:]+)$/);
        if (aliasMatch) {
            return {
                bot: aliasMatch[1],
                filename: 'passivbot_err.log',
                isOld: false,
            };
        }
        var match = svc.match(/^data\/run_v7\/([^/]+)\/(passivbot_err\.log(?:\.old)?)$/);
        if (!match) return null;
        return {
            bot: match[1],
            filename: match[2],
            isOld: match[2].endsWith('.old'),
        };
    }

    _serviceEntries(services) {
        var entries = [];
        var knownBots = this._collectKnownBotNames(services);
        var botOrder = [];
        var botGroups = {};
        var fallbackEntries = [];
        var me = this;

        function ensureGroup(botName) {
            if (!botGroups[botName]) {
                botGroups[botName] = { live: null, history: [] };
                botOrder.push(botName);
            }
            return botGroups[botName];
        }

        for (var i = 0; i < services.length; i++) {
            var svc = String(services[i] || '');
            if (!svc) continue;

            if (svc.indexOf('Bot:') === 0) {
                var botName = svc.substring(4).split(':')[0];
                ensureGroup(botName).live = svc;
                continue;
            }

            var errMeta = this._botErrorLogMeta(svc);
            if (errMeta) {
                ensureGroup(errMeta.bot).history.push({
                    value: svc,
                    label: errMeta.isOld ? 'error.old' : 'error',
                    title: svc,
                    className: 'lvp-subitem',
                    sortKey: errMeta.isOld ? '1' : '2',
                });
                continue;
            }

            var archive = this._archiveLogMeta(svc, knownBots);
            if (archive && archive.bot) {
                ensureGroup(archive.bot).history.push({
                    value: svc,
                    label: archive.timestamp || me._svcLabel(svc),
                    title: svc,
                    className: 'lvp-subitem',
                    sortKey: archive.sortKey || '',
                });
                continue;
            }

            fallbackEntries.push({ value: svc, label: this._svcLabel(svc), title: svc, className: '' });
        }

        entries = entries.concat(fallbackEntries);

        for (var j = 0; j < botOrder.length; j++) {
            var bot = botOrder[j];
            var group = botGroups[bot];
            if (!group) continue;

            if (group.live) {
                entries.push({ value: group.live, label: this._svcLabel(group.live), title: group.live, className: '' });
            }

            group.history.sort(function(a, b) {
                if (a.sortKey && b.sortKey && a.sortKey !== b.sortKey) return b.sortKey.localeCompare(a.sortKey);
                if (a.sortKey && !b.sortKey) return -1;
                if (!a.sortKey && b.sortKey) return 1;
                return a.title.localeCompare(b.title);
            });
            entries = entries.concat(group.history);
        }

        return entries;
    }

    _buildServiceList() {
        var list = this._q('item-list');
        if (!list) return;
        list.innerHTML = '';
        var services = this._serviceEntries(this._getServices());
        var me = this;
        for (var i = 0; i < services.length; i++) {
            (function(entry) {
                var btn = document.createElement('button');
                btn.className = 'lvp-item-btn' + (entry.value === me._service ? ' lvp-active' : '') + (entry.className ? ' ' + entry.className : '');
                btn.textContent = entry.label;
                btn.title = entry.title || entry.value;
                btn.addEventListener('click', function() { me._selectItem(entry.value); });
                list.appendChild(btn);
            })(services[i]);
        }
    }

    _getServices() {
        if (this._serviceListOverride) {
            /* Allow local overrides; return [] to fall back to built-in host-specific services. */
            var overridden = this._serviceListOverride(this._host, this._vpState);
            if (Array.isArray(overridden) && overridden.length) {
                var next = Array.from(new Set(overridden.filter(function(item) { return !!item; })));
                if (this._service && next.indexOf(this._service) < 0) next.unshift(this._service);
                return next;
            }
        }
        var svcs = ['PBRun', 'PBRemote', 'PBCoinData', 'PBData', 'PBGui', 'PBApiServer', 'FastAPI', 'VPSMonitor', 'VPSManagerApi', 'data/logs/sync.log'];
        if (this._vpState && this._host !== 'local') {
            var meta = (this._vpState.host_meta || {})[this._host] || {};
            var available = meta.available_logs;
            if (Array.isArray(available) && available.length > 0) {
                /* available_logs contains relative paths like "data/logs/PBRun.log" */
                var availSet = {};
                for (var i = 0; i < available.length; i++) availSet[available[i]] = true;
                /* keep known services that have a matching log file */
                svcs = svcs.filter(function(s) { return !!availSet[s]; });
                /* add extra log files not in the known list */
                for (var j = 0; j < available.length; j++) {
                    if (svcs.indexOf(available[j]) < 0) svcs.push(available[j]);
                }
            } else if (String(meta.role) === 'slave') {
                /* fallback: filter by role when available_logs not yet collected */
                var masterOnly = ['PBGui', 'PBApiServer', 'FastAPI', 'VPSMonitor', 'VPSManagerApi'];
                svcs = svcs.filter(function(s) { return masterOnly.indexOf(s) < 0; });
            }
            /* PB7 instances (config-status): {name, running, cv, eo, rv} */
            var v7 = this._vpState.v7_instances || {};
            var v7Insts = v7[this._host] || [];
            for (var k = 0; k < v7Insts.length; k++) {
                var v7i = v7Insts[k];
                if (v7i.name && v7i.running) svcs.push('Bot:' + v7i.name + ':7');
            }
            var botLogsByHost = this._vpState.bot_logs || {};
            var botLogs = botLogsByHost[this._host] || {};
            for (var botName in botLogs) {
                if (!Object.prototype.hasOwnProperty.call(botLogs, botName)) continue;
                var botFiles = botLogs[botName] || [];
                for (var m = 0; m < botFiles.length; m++) {
                    var file = String(botFiles[m] || '');
                    if (!file) continue;
                    svcs.push(file);
                }
            }
        }
        if (this._service && svcs.indexOf(this._service) < 0) svcs.unshift(this._service);
        return Array.from(new Set(svcs));
    }

    _svcLabel(s) {
        if (s.indexOf('BotErr:') === 0) return '\u26a0\ufe0f ' + s.substring(7) + ' error';
        if (s.indexOf('Bot:') === 0) {
            var parts = s.substring(4).split(':');
            return '\ud83e\udd16 ' + parts[0];
        }
        if (s.indexOf('/') >= 0) {
            var pathParts = s.split('/').filter(Boolean);
            var errMeta = this._botErrorLogMeta(s);
            if (errMeta) return errMeta.bot + ' ' + (errMeta.isOld ? 'error.old' : 'error');
            if (s.indexOf('pb7/logs/') === 0 || s.indexOf('software/pb7/logs/') === 0) {
                var archive = this._archiveLogMeta(s);
                if (archive && archive.bot && archive.timestamp) return '\ud83d\udcdc ' + archive.bot + ' ' + archive.timestamp;
                if (archive && archive.bot) return '\ud83d\udcdc ' + archive.bot + ' history';
                if (archive && archive.timestamp) return '\ud83d\udcdc ' + archive.timestamp;
            }
            if (pathParts.length >= 2 && pathParts[pathParts.length - 1] === 'passivbot.log') {
                return '\ud83e\udd16 ' + pathParts[pathParts.length - 2];
            }
            return pathParts[pathParts.length - 1] || s;
        }
        return s;
    }

    _selectItem(item) {
        if (this._isLocal()) {
            if (item === this._file && this._streaming) return;
            this._file = item;
        } else {
            if (item === this._service && this._streaming) return;
            this._service = item;
        }
        /* highlight in sidebar */
        var list = this._q('item-list');
        if (list) {
            var btns = list.querySelectorAll('.lvp-item-btn');
            for (var i = 0; i < btns.length; i++)
                btns[i].classList.toggle('lvp-active', btns[i].title === item);
        }
        this._updateBadge();
        this._updateRestartBtn();
        this._subscribe();
        if (this._isLocal() && this._onFileChange) this._onFileChange(this._file);
    }

    _updateRestartBtn() {
        var rb = this._q('restart-btn');
        if (!rb) return;
        var svc = this._restartableService();
        var blocker = this._restartBlockerFor(svc);
        rb.style.display = svc && !blocker ? '' : 'none';
        rb.title = blocker || '';
    }

    _updateBadge() {
        var badge = this._q('item-badge');
        if (!badge) return;
        if (this._isLocal()) badge.textContent = this._fileLabel(this._file) || '(no file)';
        else badge.textContent = this._svcLabel(this._service);
    }

    _toggleSidebar() {
        this._sidebarOpen = !this._sidebarOpen;
        var sidebar  = this._q('sidebar');
        var tbToggle = this._q('sb-toggle');
        var badge    = this._q('item-badge');
        if (this._sidebarOpen) {
            sidebar.classList.remove('lvp-collapsed');
            sidebar.style.flex = '';
            sidebar.style.width = '';
            if (tbToggle) tbToggle.style.display = 'none';
            if (badge) badge.style.display = 'none';
            if (this._isLocal() && this._ws && this._ws.readyState === WebSocket.OPEN)
                this._ws.send(JSON.stringify({ cmd: 'list_local_logs' }));
            else if (!this._isLocal())
                this._buildServiceList();
        } else {
            sidebar.classList.add('lvp-collapsed');
            if (tbToggle) tbToggle.style.display = '';
            if (badge) badge.style.display = '';
        }
    }

    _initResize(e) {
        e.preventDefault();
        var sidebar = this._q('sidebar');
        if (sidebar.classList.contains('lvp-collapsed')) return;
        var me = this;
        var startX = e.clientX;
        var startW = sidebar.offsetWidth;
        function onMove(e2) {
            var newW = Math.max(100, startW + e2.clientX - startX);
            sidebar.style.flex = 'none';
            sidebar.style.width = newW + 'px';
        }
        function onUp() {
            document.removeEventListener('mousemove', onMove);
            document.removeEventListener('mouseup', onUp);
            document.body.style.cursor = '';
            document.body.style.userSelect = '';
        }
        document.addEventListener('mousemove', onMove);
        document.addEventListener('mouseup', onUp);
        document.body.style.cursor = 'col-resize';
        document.body.style.userSelect = 'none';
    }

    /* ═══════════════════════════════════════════════════════════
       Subscribe / Unsubscribe / Fetch
       ═══════════════════════════════════════════════════════ */
    _getLines() {
        return parseInt((this._q('lines-sel') || {}).value || '200', 10);
    }

    _subscribe() {
        if (!this._ws || this._ws.readyState !== WebSocket.OPEN) return;
        this._unsubscribe();
        this._clear();
        var sid = ++this._sid;
        if (this._isLocal()) {
            if (!this._file) return;
            this._send({ cmd: 'subscribe_local_logs', file: this._file, lines: this._getLines(), sid: sid, start_at_end: !!this._startLocalAtEnd });
            this._startLocalAtEnd = false;
        } else {
            if (!this._host || !this._service) return;
            var startAtEnd = !!this._startRemoteAtEnd;
            this._startRemoteAtEnd = false;
            this._send({ cmd: 'subscribe_logs', host: this._host, service: this._service, lines: this._getLines(), sid: sid, start_at_end: startAtEnd });
            this._send({ cmd: 'get_log_info', host: this._host, service: this._service });
        }
        this._streaming = true;
        this._updateStreamBtn();
    }

    _unsubscribe() {
        if (!this._ws || this._ws.readyState !== WebSocket.OPEN) return;
        if (this._streaming) {
            if (this._isLocal()) this._send({ cmd: 'unsubscribe_local_logs' });
            else this._send({ cmd: 'unsubscribe_logs' });
        }
        this._streaming = false;
    }

    _fetchOnce() {
        if (!this._ws || this._ws.readyState !== WebSocket.OPEN) return;
        this._unsubscribe();
        this._clear();
        var sid = ++this._sid;
        if (this._isLocal()) {
            if (!this._file) return;
            this._send({ cmd: 'get_local_logs', file: this._file, lines: this._getLines(), sid: sid });
        } else {
            if (!this._host || !this._service) return;
            this._send({ cmd: 'get_logs', host: this._host, service: this._service, lines: this._getLines(), sid: sid });
        }
    }

    /* ═══════════════════════════════════════════════════════════
       Controls
       ═══════════════════════════════════════════════════════ */
    _toggleStream() {
        if (this._streaming) this._unsubscribe();
        else this._subscribe();
        this._updateStreamBtn();
    }

    _updateStreamBtn() {
        var btn = this._q('stream-btn');
        if (!btn) return;
        if (this._streaming) {
            btn.textContent = '\u23f8 Pause';
            btn.className   = 'lvp-ctrl-btn lvp-stream-on';
        } else {
            btn.textContent = '\u25b6 Stream';
            btn.className   = 'lvp-ctrl-btn';
        }
    }

    _clear() {
        this._lines      = [];
        this._lineBase   = 0;
        this._pending    = [];
        this._rafPending = false;
        ++this._renderAbort;
        var term = this._q('terminal');
        if (term) term.innerHTML = '';
        var ctx = this._q('ctx-sel');
        var grp = this._q('grp-actions');
        if (ctx) ctx.style.display = 'none';
        if (grp) grp.style.display = 'none';
        this._matchEls = [];
        this._matchIdx = -1;
        this._updateMatchCount(0);
    }

    _download() {
        if (!this._lines.length) return;
        var blob = new Blob([this._lines.join('\n')], { type: 'text/plain' });
        var url  = URL.createObjectURL(blob);
        var a    = document.createElement('a');
        a.href   = url;
        if (this._isLocal()) {
            var localName = (this._file || 'log.log').replace(/[/:]+/g, '_');
            a.download = /\.log$/i.test(localName) ? localName : (localName + '.log');
        } else {
            var sn = this._service.indexOf('Bot:') === 0
                ? this._service.substring(4).split(':')[0]
                : this._service;
            a.download = this._host + '_' + sn + '.log';
        }
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        URL.revokeObjectURL(url);
    }

    _toggleLineNums() {
        this._showLineNums = !this._showLineNums;
        var term = this._q('terminal');
        var btn  = this._q('ln-btn');
        if (term) {
            term.style.display = 'none';
            term.classList.toggle('show-line-nums', this._showLineNums);
            void term.offsetHeight;
            term.style.display = '';
        }
        if (btn) btn.className = this._showLineNums ? 'lvp-ctrl-btn lvp-active' : 'lvp-ctrl-btn';
    }

    /* Map local log filename → service name for restart */
    static _LOCAL_SVC_MAP = {
        'PBRun.log': 'PBRun', 'PBRemote.log': 'PBRemote',
        'PBCoinData.log': 'PBCoinData', 'PBData.log': 'PBData',
    };

    _restartableService() {
        if (this._isLocal()) {
            /* Derive service from log filename */
            if (!this._file) return null;
            if (this._file.indexOf('Bot:') === 0) {
                return this._file + ':7';
            }
            var svc = LogViewerPanel._LOCAL_SVC_MAP[this._file];
            if (svc) return svc;
            /* Bot instance logs: {name}.log in run_v7 dirs */
            if (this._file.endsWith('.log') && this._file.indexOf('/') < 0) {
                var name = this._file.replace('.log', '');
                return 'Bot:' + name + ':7';
            }
            return null;
        }
        if (this._service && (this._service.indexOf('pb7/logs/') === 0 || this._service.indexOf('software/pb7/logs/') === 0)) {
            var archive = this._archiveLogMeta(this._service);
            if (archive && archive.bot) return 'Bot:' + archive.bot + ':7';
        }
        return this._service || null;
    }

    _restartBlockerFor(svc) {
        if (!svc || svc.indexOf('Bot:') === 0 || !this._serviceStatusProvider) return '';
        var check = null;
        try {
            check = this._serviceStatusProvider(this._host || 'local', svc, this._vpState);
        } catch (_e) {
            check = null;
        }
        if (!check) return '';
        if (check.expected === false || check.status === 'disabled') {
            return check.reason || 'Service is not configured';
        }
        return '';
    }

    _restart() {
        var svc = this._restartableService();
        if (!svc) return;
        if (this._restartBlockerFor(svc)) return;
        var host = this._host || 'local';
        var rb = this._q('restart-btn');
        this._restartTargetService = (!this._isLocal() && svc.indexOf('Bot:') === 0) ? svc : this._activeItem();
        if (rb) { rb.disabled = true; rb.textContent = '\u231b Restarting\u2026'; }

        if (svc.indexOf('Bot:') === 0) {
            var parts = svc.substring(4).split(':');
            if (!this._sendRestart({ cmd: 'kill_instance', host: host, name: parts[0], pb_version: parts[1] || '7' }) && rb) {
                rb.textContent = 'Connecting...';
            }
        } else {
            if (!this._sendRestart({ cmd: 'restart_service', host: host, service: svc }) && rb) {
                rb.textContent = 'Connecting...';
            }
        }
        var me = this;
        setTimeout(function() {
            if (rb) { rb.disabled = false; rb.textContent = '\ud83d\udd04 Restart'; }
        }, 4000);
    }

    _showFileSize(bytes) {
        this._fileSize = bytes;
        var el = this._q('file-size');
        if (!el) return;
        if (bytes == null) { el.textContent = ''; return; }
        el.textContent = this._formatSize(bytes);
    }

    /* ═══════════════════════════════════════════════════════════
       Level filter
       ═══════════════════════════════════════════════════════ */
    _stripAnsi(line) {
        return String(line == null ? '' : line)
            .replace(/\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])/g, '');
    }

    _hasAnsi(line) {
        return /\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])/.test(String(line == null ? '' : line));
    }

    _applyAnsiCodes(state, codes) {
        for (var i = 0; i < codes.length; i++) {
            var code = parseInt(codes[i], 10);
            if (isNaN(code)) continue;
            if (code === 0) {
                state.bold = false;
                state.fg = null;
            } else if (code === 1) {
                state.bold = true;
            } else if (code === 22) {
                state.bold = false;
            } else if (code === 39) {
                state.fg = null;
            } else if ((code >= 30 && code <= 37) || (code >= 90 && code <= 97)) {
                state.fg = code;
            }
        }
    }

    _ansiStateClass(state) {
        var classes = [];
        if (state.bold) classes.push('lvp-ansi-bold');
        if (state.fg != null) classes.push('lvp-ansi-fg-' + state.fg);
        return classes.join(' ');
    }

    _parseAnsiSegments(line) {
        var text = String(line == null ? '' : line);
        var re = /\x1B(?:\[([0-9;]*)m|\[[0-?]*[ -/]*[@-~]|[@-Z\\-_])/g;
        var state = { bold: false, fg: null };
        var segments = [];
        var lastIndex = 0;
        var match;
        var me = this;

        function pushSegment(segmentText) {
            if (!segmentText) return;
            segments.push({
                text: segmentText,
                className: me._ansiStateClass(state)
            });
        }

        while ((match = re.exec(text)) !== null) {
            if (match.index > lastIndex) {
                pushSegment(text.slice(lastIndex, match.index));
            }
            if (match[1] !== undefined) {
                me._applyAnsiCodes(state, match[1] ? match[1].split(';') : ['0']);
            }
            lastIndex = re.lastIndex;
        }
        if (lastIndex < text.length) {
            pushSegment(text.slice(lastIndex));
        }
        return segments;
    }

    _renderAnsiHtml(line, re) {
        var segments = this._parseAnsiSegments(line);
        var parts = [];
        for (var i = 0; i < segments.length; i++) {
            var seg = segments[i];
            var html = this._esc(seg.text);
            if (re) html = html.replace(re, '<mark>$1</mark>');
            if (seg.className) parts.push('<span class="' + seg.className + '">' + html + '</span>');
            else parts.push(html);
        }
        return parts.join('');
    }

    _extractLevel(line) {
        var clean = this._stripAnsi(line);
        if (/\b(fatal|failed)\s*:/i.test(clean) || /\b(unreachable|failed)=\s*[1-9]\d*\b/i.test(clean))
            return 'ERROR';
        if (/\[WARNING\]:/i.test(clean) || /\bWARNING\b/i.test(clean))
            return 'WARNING';
        if (/\bchanged\s*:/i.test(clean) || /\bchanged=\s*[1-9]\d*\b/i.test(clean))
            return 'WARNING';
        var m = clean.match(/\[(DEBUG|INFO|WARNING|ERROR|CRITICAL)\]/i);
        if (m) return m[1].toUpperCase();
        var m2 = clean.match(/\b(DEBUG|INFO|WARNING|WARN|ERROR|CRITICAL)\b/i);
        if (m2) { var l = m2[1].toUpperCase(); return l === 'WARN' ? 'WARNING' : l; }
        return 'INFO';
    }

    _levelClass(level) {
        var map = { DEBUG:'lvp-log-debug', INFO:'lvp-log-info', WARNING:'lvp-log-warning',
                    ERROR:'lvp-log-error', CRITICAL:'lvp-log-critical' };
        return map[level] || 'lvp-log-info';
    }

    _toggleLevel(lvl) {
        var btn = this._q('lvl-' + lvl);
        if (this._visLevels.has(lvl)) {
            this._visLevels.delete(lvl);
            if (btn) btn.classList.remove('on');
        } else {
            this._visLevels.add(lvl);
            if (btn) btn.classList.add('on');
        }
        var term = this._q('terminal');
        if (!term) return;
        for (var i = 0; i < term.children.length; i++) {
            var div = term.children[i];
            var lv = div.dataset.level || 'INFO';
            if (this._visLevels.has(lv)) div.classList.remove('lvp-level-hidden');
            else div.classList.add('lvp-level-hidden');
        }
        this._updateMatchCount(0);
    }

    /* ═══════════════════════════════════════════════════════════
       Rendering
       ═══════════════════════════════════════════════════════ */
    _esc(s) { return s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;'); }

    _getSearchRe() {
        if (!this._searchTerm) return null;
        try {
            var pat = this._searchRegex
                ? this._searchTerm
                : this._searchTerm.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
            return new RegExp('(' + pat + ')', 'gi');
        } catch(e) { return null; }
    }

    _testMatch(text) {
        var re = this._getSearchRe();
        return re ? re.test(text) : false;
    }

    _buildDiv(line, lineNum) {
        var div = document.createElement('div');
        if (lineNum != null) div.dataset.ln = lineNum;
        var rawLine = String(line == null ? '' : line);
        var cleanLine = this._stripAnsi(rawLine);
        var hasAnsi = this._hasAnsi(rawLine);
        var level = this._extractLevel(cleanLine);
        div.dataset.level = level;
        div.dataset.text  = cleanLine;
        var cls = hasAnsi ? 'lvp-log-info' : this._levelClass(level);
        if (!this._visLevels.has(level)) cls += ' lvp-level-hidden';

        var re = this._getSearchRe();
        if (this._searchTerm && this._testMatch(cleanLine)) {
            cls += ' lvp-highlight';
            if (hasAnsi) div.innerHTML = this._renderAnsiHtml(rawLine, re);
            else div.innerHTML = re ? this._esc(cleanLine).replace(re, '<mark>$1</mark>') : this._esc(cleanLine);
        } else if (this._searchTerm && this._filterMode) {
            div.textContent = cleanLine;
            cls += ' lvp-hidden';
        } else {
            if (hasAnsi) div.innerHTML = this._renderAnsiHtml(rawLine, null);
            else div.textContent = cleanLine;
        }
        div.className = cls;
        return div;
    }

    _renderFull() {
        var term = this._q('terminal');
        if (!term) return;
        this._pending    = [];
        this._rafPending = false;
        term.innerHTML   = '';
        var total = this._lines.length;
        if (!total) { this._updateMatchCount(0); return; }

        if (total <= this._CHUNK) {
            var frag = document.createDocumentFragment();
            for (var i = 0; i < total; i++)
                frag.appendChild(this._buildDiv(this._lines[i], this._lineBase + i + 1));
            term.appendChild(frag);
            term.scrollTop = term.scrollHeight;
            var me = this;
            if (this._searchTerm) setTimeout(function() { me._applySearch(); }, 0);
            else this._updateMatchCount(0);
            return;
        }

        var me = this;
        var rid = ++this._renderAbort;
        var idx = 0;
        var status = document.createElement('div');
        status.style.cssText = 'color:#888;padding:8px;font-size:12px';
        status.textContent = 'Rendering\u2026';
        term.appendChild(status);

        function rChunk() {
            if (rid !== me._renderAbort) return;
            var end  = Math.min(idx + me._CHUNK, total);
            var frag = document.createDocumentFragment();
            for (; idx < end; idx++)
                frag.appendChild(me._buildDiv(me._lines[idx], me._lineBase + idx + 1));
            if (idx >= total) {
                status.remove();
                term.appendChild(frag);
                term.scrollTop = term.scrollHeight;
                if (me._searchTerm) setTimeout(function() { me._applySearch(); }, 0);
                else me._updateMatchCount(0);
            } else {
                term.insertBefore(frag, status);
                status.textContent = 'Rendering\u2026 ' + Math.round(idx / total * 100) + '%';
                requestAnimationFrame(rChunk);
            }
        }
        requestAnimationFrame(rChunk);
    }

    _appendLines(newLines) {
        if (!newLines.length) return;
        var startNum = this._lineBase + this._lines.length - newLines.length + 1;
        for (var i = 0; i < newLines.length; i++)
            this._pending.push({ line: newLines[i], num: startNum + i });
        if (this._rafPending) return;
        this._rafPending = true;
        var me = this;
        requestAnimationFrame(function() {
            var term = me._q('terminal');
            if (!term) { me._pending = []; me._rafPending = false; return; }
            var atBottom = term.scrollTop + term.clientHeight >= term.scrollHeight - 40;
            var frag = document.createDocumentFragment();
            for (var j = 0; j < me._pending.length; j++) {
                var item = me._pending[j];
                frag.appendChild(me._buildDiv(item.line, item.num));
            }
            me._pending    = [];
            me._rafPending = false;
            term.appendChild(frag);
            while (me._MAX !== Infinity && term.childElementCount > me._MAX) term.removeChild(term.firstChild);
            if (atBottom) term.scrollTop = term.scrollHeight;
            me._updateMatchCount(0);
        });
    }

    /* ═══════════════════════════════════════════════════════════
       Search UI
       ═══════════════════════════════════════════════════════ */
    _onPresetChange() {
        var val = this._q('preset').value;
        if (val) {
            this._searchTerm  = val;
            this._searchRegex = true;
            this._q('search').value = val;
        } else {
            this._searchTerm  = '';
            this._searchRegex = false;
            this._q('search').value = '';
        }
        var me = this;
        setTimeout(function() { me._applySearch(); }, 0);
    }

    _onSearchInput() {
        this._searchTerm  = (this._q('search').value || '').trim();
        this._searchRegex = false;
        this._q('preset').value = '';
        if (this._searchTimer) clearTimeout(this._searchTimer);
        var me = this;
        this._searchTimer = setTimeout(function() { me._applySearch(); }, 300);
    }

    _onFilterToggle() {
        this._filterMode = this._q('filter-chk').checked;
        var me = this;
        setTimeout(function() { me._applySearch(); }, 0);
    }

    _onContextChange() {
        this._contextLines = parseInt(this._q('ctx-sel').value || '5', 10);
        var me = this;
        setTimeout(function() { me._applySearch(); }, 0);
    }

    _onSearchKeydown(e) {
        if (e.key !== 'Enter') return;
        e.preventDefault();
        this._searchNav(e.shiftKey ? -1 : 1);
    }

    /* ═══════════════════════════════════════════════════════════
       Search engine + block grouping
       ═══════════════════════════════════════════════════════ */
    _applySearch() {
        var terminal   = this._q('terminal');
        var ctxSel     = this._q('ctx-sel');
        var grpActions = this._q('grp-actions');
        var navBtns    = this._q('nav-btns');
        var countEl    = this._q('match-count');
        if (!terminal) return;

        this._cleanupGroups(terminal);
        var children = Array.from(terminal.children);
        var total = children.length;

        this._matchEls = [];
        this._matchIdx = -1;

        if (!this._searchTerm) {
            for (var i = 0; i < children.length; i++) {
                var div = children[i];
                var text = div.dataset.text || div.textContent;
                div.textContent = text;
                var cls = div.className
                    .replace(/ lvp-highlight| lvp-hidden| lvp-level-hidden| lvp-current-match/g, '');
                var lv = div.dataset.level || 'INFO';
                if (!this._visLevels.has(lv)) cls += ' lvp-level-hidden';
                div.className = cls;
            }
            if (ctxSel) ctxSel.style.display = 'none';
            if (grpActions) grpActions.style.display = 'none';
            if (navBtns) navBtns.style.display = 'none';
            if (countEl) countEl.textContent = '';
            return;
        }

        /* build isMatch bitmap */
        var isMatch = new Uint8Array(total);
        for (var i = 0; i < total; i++)
            isMatch[i] = this._testMatch(children[i].dataset.text || children[i].textContent || '') ? 1 : 0;

        var re = this._getSearchRe();

        /* non-filter mode: highlight only */
        if (!this._filterMode) {
            if (ctxSel) ctxSel.style.display = 'none';
            if (grpActions) grpActions.style.display = 'none';
            this._applyHighlightsOnly(children, isMatch, total, re);
            return;
        }

        /* filter + context mode */
        if (ctxSel) ctxSel.style.display = '';
        if (grpActions) grpActions.style.display = '';

        var result   = this._computeBlocks(isMatch, total, this._contextLines);
        var blocks   = result.blocks;
        var blockOf  = result.blockOf;
        var isVisible = result.isVisible;
        var me = this;
        var searchId = ++this._searchAbort;

        if (total <= this._SCHUNK) {
            for (var i = 0; i < total; i++)
                this._applyBlockDiv(children[i], i, isMatch, isVisible, blockOf, blocks, re);
            this._insertSeparators(terminal, children, blocks);
            if (this._blocksCollapsed) this._toggleAllGroups(false);
            this._updateMatchCount(blocks.length);
            return;
        }

        /* chunked processing */
        for (var i = 0; i < total; i++) children[i].style.display = 'none';
        var idx = 0;

        function processChunk() {
            if (searchId !== me._searchAbort) return;
            var end = Math.min(idx + me._SCHUNK, total);
            for (; idx < end; idx++)
                me._applyBlockDiv(children[idx], idx, isMatch, isVisible, blockOf, blocks, re);
            if (idx < total) {
                if (countEl) countEl.textContent = 'Filtering\u2026 ' + Math.round(idx / total * 100) + '%';
                requestAnimationFrame(processChunk);
            } else {
                me._insertSeparators(terminal, children, blocks);
                if (me._blocksCollapsed) me._toggleAllGroups(false);
                me._updateMatchCount(blocks.length);
            }
        }
        requestAnimationFrame(processChunk);
    }

    _applyHighlightsOnly(children, isMatch, total, re) {
        var me = this;
        var searchId = ++this._searchAbort;
        var countEl = this._q('match-count');

        if (total <= this._SCHUNK) {
            for (var i = 0; i < total; i++)
                this._applyHighlightDiv(children[i], isMatch[i], re);
            this._cacheNavMatches();
            this._updateMatchCount(0);
            return;
        }

        var idx = 0;
        function processChunk() {
            if (searchId !== me._searchAbort) return;
            var end = Math.min(idx + me._SCHUNK, total);
            for (; idx < end; idx++)
                me._applyHighlightDiv(children[idx], isMatch[idx], re);
            if (idx < total) {
                if (countEl) countEl.textContent = 'Filtering\u2026 ' + Math.round(idx / total * 100) + '%';
                requestAnimationFrame(processChunk);
            } else {
                me._cacheNavMatches();
                me._updateMatchCount(0);
            }
        }
        requestAnimationFrame(processChunk);
    }

    _applyHighlightDiv(div, match, re) {
        var text = div.dataset.text || div.textContent || '';
        var cls = div.className.replace(/ lvp-highlight| lvp-hidden| lvp-current-match/g, '');
        if (match) {
            cls += ' lvp-highlight';
            div.innerHTML = re ? this._esc(text).replace(re, '<mark>$1</mark>') : this._esc(text);
        } else {
            div.textContent = text;
        }
        div.className = cls;
    }

    _applyBlockDiv(div, idx, isMatch, isVisible, blockOf, blocks, re) {
        var text = div.dataset.text || div.textContent || '';
        var cls = div.className
            .replace(/ lvp-highlight| lvp-hidden| lvp-context| lvp-group-first| lvp-grp-detail| lvp-current-match/g, '');
        div.style.display = '';

        if (!isVisible[idx]) {
            cls += ' lvp-hidden';
            div.textContent = text;
            div.className = cls;
            return;
        }

        var b = blockOf[idx];
        div.dataset.blk = b;

        if (isMatch[idx] && idx === blocks[b].firstMatch) {
            /* first match = collapsible header */
            cls += ' lvp-highlight lvp-group-first';
            var arrow = document.createElement('span');
            arrow.className = 'grp-arrow';
            arrow.textContent = '\u25bc ';
            div.innerHTML = re ? this._esc(text).replace(re, '<mark>$1</mark>') : this._esc(text);
            div.insertBefore(arrow, div.firstChild);
            var detailCount = blocks[b].end - blocks[b].start;
            if (detailCount > 0) {
                var span = document.createElement('span');
                span.className = 'grp-count';
                span.textContent = ' (+' + detailCount + ' lines)';
                div.appendChild(span);
            }
        } else if (isMatch[idx]) {
            cls += ' lvp-highlight lvp-grp-detail';
            div.innerHTML = re ? this._esc(text).replace(re, '<mark>$1</mark>') : this._esc(text);
        } else {
            cls += ' lvp-context lvp-grp-detail';
            div.textContent = text;
        }
        div.className = cls;
    }

    /* ── Block computation ────────────────────────────────────── */
    _computeBlocks(isMatch, total, contextLines) {
        var isVisible = new Uint8Array(total);
        for (var i = 0; i < total; i++) {
            if (isMatch[i]) {
                isVisible[i] = 1;
                for (var d = 1; d <= contextLines; d++) {
                    if (i - d >= 0) isVisible[i - d] = 1;
                    if (i + d < total) isVisible[i + d] = 1;
                }
            }
        }
        var blocks = [];
        var bs = -1, fm = -1, mc = 0;
        for (var i = 0; i < total; i++) {
            if (isVisible[i]) {
                if (bs === -1) { bs = i; fm = -1; mc = 0; }
                if (isMatch[i]) { if (fm === -1) fm = i; mc++; }
            } else {
                if (bs !== -1) {
                    blocks.push({ start: bs, end: i - 1, firstMatch: fm, matchCount: mc });
                    bs = -1;
                }
            }
        }
        if (bs !== -1) blocks.push({ start: bs, end: total - 1, firstMatch: fm, matchCount: mc });

        var blockOf = new Int16Array(total).fill(-1);
        for (var b = 0; b < blocks.length; b++)
            for (var i = blocks[b].start; i <= blocks[b].end; i++) blockOf[i] = b;

        return { blocks: blocks, blockOf: blockOf, isVisible: isVisible };
    }

    _cleanupGroups(terminal) {
        if (!terminal) return;
        var seps = terminal.querySelectorAll('.lvp-separator');
        for (var i = 0; i < seps.length; i++) seps[i].remove();
        for (var i = 0; i < terminal.children.length; i++) {
            var div = terminal.children[i];
            div.className = div.className
                .replace(/ lvp-context| lvp-group-first| lvp-grp-detail| collapsed| lvp-current-match/g, '');
            if (div.dataset.text !== undefined) div.textContent = div.dataset.text;
            delete div.dataset.blk;
            div.style.display = '';
        }
    }

    _insertSeparators(terminal, children, blocks) {
        for (var b = 0; b < blocks.length - 1; b++) {
            var endDiv = children[blocks[b].end];
            if (endDiv && endDiv.nextSibling) {
                var sep = document.createElement('div');
                sep.className = 'lvp-separator';
                sep.textContent = '\u00b7\u00b7\u00b7';
                terminal.insertBefore(sep, endDiv.nextSibling);
            }
        }
    }

    _toggleAllGroups(expand) {
        this._blocksCollapsed = !expand;
        var terminal = this._q('terminal');
        if (!terminal) return;
        var firsts = terminal.querySelectorAll('.lvp-group-first');
        for (var i = 0; i < firsts.length; i++) {
            if (expand) firsts[i].classList.remove('collapsed');
            else firsts[i].classList.add('collapsed');
            var arrow = firsts[i].querySelector('.grp-arrow');
            if (arrow) arrow.textContent = expand ? '\u25bc ' : '\u25b6 ';
        }
        var details = terminal.querySelectorAll('.lvp-grp-detail');
        for (var i = 0; i < details.length; i++)
            details[i].style.display = expand ? '' : 'none';
        var separators = terminal.querySelectorAll('.lvp-separator');
        for (var i = 0; i < separators.length; i++)
            separators[i].style.display = expand ? '' : 'none';
    }

    _cacheNavMatches() {
        var terminal = this._q('terminal');
        this._matchEls = terminal ? Array.from(terminal.querySelectorAll('.lvp-highlight')) : [];
        this._matchIdx = -1;
        var navBtns = this._q('nav-btns');
        if (navBtns)
            navBtns.style.display = (!this._filterMode && this._matchEls.length > 0)
                ? 'inline-flex' : 'none';
    }

    /* ── Search navigation ────────────────────────────────────── */
    _searchNav(dir) {
        if (this._filterMode || !this._matchEls.length) return;
        if (this._matchIdx >= 0 && this._matchIdx < this._matchEls.length)
            this._matchEls[this._matchIdx].classList.remove('lvp-current-match');
        this._matchIdx += dir;
        if (this._matchIdx >= this._matchEls.length) this._matchIdx = 0;
        if (this._matchIdx < 0) this._matchIdx = this._matchEls.length - 1;
        var el = this._matchEls[this._matchIdx];
        el.classList.add('lvp-current-match');
        var terminal = this._q('terminal');
        terminal.scrollTop = el.offsetTop - terminal.offsetTop - terminal.clientHeight / 2 + el.offsetHeight / 2;
        this._updateMatchCount(0);
    }

    _updateMatchCount(blockCount) {
        var el = this._q('match-count');
        if (!el) return;
        if (!this._searchTerm) { el.textContent = ''; return; }
        var terminal = this._q('terminal');
        var count = terminal ? terminal.querySelectorAll('.lvp-highlight').length : 0;
        if (this._filterMode && blockCount > 0)
            el.textContent = count + ' matches in ' + blockCount + ' blocks';
        else if (!this._filterMode && this._matchIdx >= 0)
            el.textContent = (this._matchIdx + 1) + ' / ' + count;
        else
            el.textContent = count + ' matches';
    }

    /* ── Utility ──────────────────────────────────────────────── */
    _formatSize(bytes) {
        if (bytes < 1024) return bytes + ' B';
        if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + ' KB';
        return (bytes / (1024 * 1024)).toFixed(1) + ' MB';
    }
}
