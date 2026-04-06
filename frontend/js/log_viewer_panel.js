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
    width:170px;min-width:120px;max-width:240px;
    background:#111827;border-right:1px solid #1e293b;
    display:flex;flex-direction:column;overflow:hidden;flex-shrink:0;
    transition:width .18s ease;
}
.lvp-sidebar.lvp-collapsed{width:0;min-width:0;border-right:none}
.lvp-sidebar-header{
    padding:4px 6px 4px 0;border-bottom:1px solid #1e293b;
    flex-shrink:0;white-space:nowrap;
}
.lvp-sb-hdr-toggle{
    display:inline-flex;align-items:center;gap:4px;width:100%;padding:4px 10px;
    background:none;border:none;color:#64748b;
    font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.6px;
    cursor:pointer;text-align:left;transition:color .15s;user-select:none;
}
.lvp-sb-hdr-toggle:hover{color:#e2e8f0}
.lvp-sb-hdr-toggle-arrow{font-size:9px;margin-left:auto;opacity:.6}
.lvp-item-list{flex:1;overflow-y:auto;padding:4px 0}
.lvp-item-btn{
    display:block;width:100%;text-align:left;padding:5px 10px;
    background:none;border:none;border-left:3px solid transparent;
    color:#e2e8f0;font-family:'Cascadia Code','Fira Code','Consolas',monospace;
    font-size:11px;cursor:pointer;white-space:nowrap;
    overflow:hidden;text-overflow:ellipsis;transition:background .12s;
}
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
    white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:200px;
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
        this._sidebarOpen  = false;
        this._fileList     = [];
        this._vpState      = null;
        this._contextLines = 5;
        this._blocksCollapsed = true;
        this._fileSize     = null;

        this._MAX    = 5000;
        this._CHUNK  = 500;
        this._SCHUNK = 400;

        LogViewerPanel._injectStyles();
        this._build();
        this._bindEvents();
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
  '<div class="lvp-sidebar lvp-collapsed" id="' + p + 'sidebar">' +
    '<div class="lvp-sidebar-header">' +
      '<button class="lvp-sb-hdr-toggle" id="' + p + 'sb-hdr-toggle">' +
        'Files <span class="lvp-sb-hdr-toggle-arrow">&#9664;</span>' +
      '</button>' +
    '</div>' +
    '<div class="lvp-item-list" id="' + p + 'item-list"></div>' +
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
      '<button class="lvp-sb-toggle" id="' + p + 'sb-toggle">' +
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
        this._q('lines-sel').addEventListener('change',  function() { me._subscribe(); });
        this._q('stream-btn').addEventListener('click',  function() { me._toggleStream(); });
        this._q('fetch-btn').addEventListener('click',   function() { me._fetchOnce(); });
        this._q('clear-btn').addEventListener('click',   function() { me._clear(); });
        this._q('dl-btn').addEventListener('click',      function() { me._download(); });
        this._q('ln-btn').addEventListener('click',      function() { me._toggleLineNums(); });
        this._q('sb-toggle').addEventListener('click',   function() { me._toggleSidebar(); });
        this._q('sb-hdr-toggle').addEventListener('click', function() { me._toggleSidebar(); });
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
    open()  { this._connect(); }
    close() { this._disconnect(); }

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

    /* ── Message handling ─────────────────────────────────────── */
    _handleMsg(msg) {
        switch (msg.type) {
        case 'state':
            this._vpState = msg.data || msg;
            this._updateHostDropdown();
            if (this._host === 'local' && this._vpState.local_logs)
                this._updateFileList(this._vpState.local_logs);
            if (this._host !== 'local')
                this._buildServiceList();
            break;

        case 'local_logs_list':
            this._updateFileList(msg.files || []);
            break;

        case 'local_logs':
            if (msg.sid !== undefined && msg.sid !== this._sid) return;
            this._lines    = msg.lines || [];
            this._lineBase = 0;
            this._streaming = true;
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
            this._lines    = msg.lines || [];
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
            }
            break;
        }
    }

    _ingestLines(newLines) {
        this._lines.push.apply(this._lines, newLines);
        if (this._lines.length > this._MAX) {
            var trim = this._lines.length - this._MAX;
            this._lines = this._lines.slice(-this._MAX);
            this._lineBase += trim;
        }
        this._appendLines(newLines);
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

    _updateFileList(files) {
        this._fileList = (files || []).slice().sort();
        if (this._isLocal()) this._buildFileList();
    }

    _buildFileList() {
        var list = this._q('item-list');
        if (!list) return;
        list.innerHTML = '';
        var me = this;
        for (var i = 0; i < this._fileList.length; i++) {
            (function(f) {
                var btn = document.createElement('button');
                btn.className = 'lvp-item-btn' + (f === me._file ? ' lvp-active' : '');
                btn.textContent = f;
                btn.title = f;
                btn.addEventListener('click', function() { me._selectItem(f); });
                list.appendChild(btn);
            })(this._fileList[i]);
        }
    }

    _buildServiceList() {
        var list = this._q('item-list');
        if (!list) return;
        list.innerHTML = '';
        var services = this._getServices();
        var me = this;
        for (var i = 0; i < services.length; i++) {
            (function(s) {
                var btn = document.createElement('button');
                btn.className = 'lvp-item-btn' + (s === me._service ? ' lvp-active' : '');
                btn.textContent = me._svcLabel(s);
                btn.title = s;
                btn.addEventListener('click', function() { me._selectItem(s); });
                list.appendChild(btn);
            })(services[i]);
        }
    }

    _getServices() {
        var svcs = ['PBRun', 'PBRemote', 'PBCoinData'];
        if (this._vpState && this._host !== 'local') {
            /* PB7 instances (config-status): {name, running, cv, eo, rv} */
            var v7 = this._vpState.v7_instances || {};
            var v7Insts = v7[this._host] || [];
            for (var j = 0; j < v7Insts.length; j++) {
                var v7i = v7Insts[j];
                if (v7i.name && v7i.running) svcs.push('Bot:' + v7i.name + ':7');
            }
        }
        return svcs;
    }

    _svcLabel(s) {
        if (s.indexOf('Bot:') === 0) {
            var parts = s.substring(4).split(':');
            return '\ud83e\udd16 ' + parts[0];
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
        this._subscribe();
        if (this._isLocal() && this._onFileChange) this._onFileChange(this._file);
    }

    _updateBadge() {
        var badge = this._q('item-badge');
        if (!badge) return;
        if (this._isLocal()) badge.textContent = this._file || '(no file)';
        else badge.textContent = this._svcLabel(this._service);
    }

    _toggleSidebar() {
        this._sidebarOpen = !this._sidebarOpen;
        var sidebar  = this._q('sidebar');
        var tbToggle = this._q('sb-toggle');
        var badge    = this._q('item-badge');
        if (this._sidebarOpen) {
            sidebar.classList.remove('lvp-collapsed');
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
            this._send({ cmd: 'subscribe_local_logs', file: this._file, lines: this._getLines(), sid: sid });
        } else {
            if (!this._host || !this._service) return;
            this._send({ cmd: 'subscribe_logs', host: this._host, service: this._service, sid: sid });
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
            a.download = this._file || 'log.log';
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

    _restart() {
        if (!this._host || this._isLocal() || !this._service) return;
        var rb = this._q('restart-btn');
        if (rb) { rb.disabled = true; rb.textContent = '\u231b Restarting\u2026'; }

        if (this._service.indexOf('Bot:') === 0) {
            var parts = this._service.substring(4).split(':');
            this._send({ cmd: 'kill_instance', host: this._host, name: parts[0], pb_version: parts[1] || '7' });
        } else {
            this._send({ cmd: 'restart_service', host: this._host, service: this._service });
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
    _extractLevel(line) {
        var m = line.match(/\[(DEBUG|INFO|WARNING|ERROR|CRITICAL)\]/i);
        if (m) return m[1].toUpperCase();
        var m2 = line.match(/\b(DEBUG|INFO|WARNING|WARN|ERROR|CRITICAL)\b/i);
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
        var level = this._extractLevel(line);
        div.dataset.level = level;
        div.dataset.text  = line;
        var cls = this._levelClass(level);
        if (!this._visLevels.has(level)) cls += ' lvp-level-hidden';

        var re = this._getSearchRe();
        if (this._searchTerm && this._testMatch(line)) {
            cls += ' lvp-highlight';
            div.innerHTML = re ? this._esc(line).replace(re, '<mark>$1</mark>') : this._esc(line);
        } else if (this._searchTerm && this._filterMode) {
            div.textContent = line;
            cls += ' lvp-hidden';
        } else {
            div.textContent = line;
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
            while (term.childElementCount > me._MAX) term.removeChild(term.firstChild);
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
