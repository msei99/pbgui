/**
 * LogViewerPanel — reusable live-log widget
 *
 * Usage:
 *   const viewer = new LogViewerPanel({
 *       containerId : 'myTargetDiv',
 *       wsBase      : 'ws://host:port',
 *       token       : 'YOUR_TOKEN',
 *       defaultFile : 'ApiKeys.log',   // optional
 *       height      : 'calc(100vh - 290px)',  // optional CSS height
 *   });
 *   viewer.open();   // connect WS and start streaming
 *   viewer.close();  // disconnect
 *
 * WS commands:
 *   → list_local_logs
 *   → subscribe_local_logs  { file, lines, sid }
 *   → unsubscribe_local_logs
 *
 * WS messages:
 *   ← local_logs_list  { files: [...] }
 *   ← local_logs       { sid, lines: [...] }
 *   ← local_log_lines  { sid, lines: [...] }
 *   ← state            { data: { local_logs: [...] } }
 */
class LogViewerPanel {
    constructor({ containerId, wsBase, token, defaultFile = 'ApiKeys.log', height = 'calc(100vh - 290px)' }) {
        this._cid     = containerId;
        this._wsBase  = wsBase;
        this._token   = token;
        this._file    = defaultFile;
        this._height  = height;

        this._ws           = null;
        this._sid          = 0;
        this._streaming    = false;
        this._lines        = [];
        this._lineBase     = 0;
        this._pending      = [];
        this._rafPending   = false;
        this._visLevels    = new Set(['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']);
        this._searchTerm   = '';
        this._filterMode   = true;
        this._searchRegex  = false;
        this._matchEls     = [];
        this._matchIdx     = -1;
        this._searchAbort  = 0;
        this._searchTimer  = null;
        this._renderAbort  = 0;
        this._showLineNums = false;
        this._sidebarOpen  = false;   // collapsed by default
        this._fileList     = [];

        this._MAX    = 5000;
        this._CHUNK  = 500;
        this._SCHUNK = 400;

        LogViewerPanel._injectStyles();
        this._build();
    }

    // ─── CSS (injected once per page) ───────────────────────────────────────
    static _injectStyles() {
        if (document.getElementById('lvp-global-styles')) return;
        const s = document.createElement('style');
        s.id = 'lvp-global-styles';
        s.textContent = `
/* ── LogViewerPanel ───────────────────────────────────────────────────── */
.lvp-root {
    display: flex;
    flex-direction: row;
    height: 100%;
    min-height: 0;
    overflow: hidden;
}
/* left sidebar */
.lvp-sidebar {
    width: 160px;
    min-width: 120px;
    max-width: 220px;
    background: #111827;
    border-right: 1px solid #1e293b;
    display: flex;
    flex-direction: column;
    overflow: hidden;
    flex-shrink: 0;
    transition: width 0.18s ease;
}
.lvp-sidebar.lvp-collapsed {
    width: 0;
    min-width: 0;
    border-right: none;
}
.lvp-sidebar-header {
    padding: 4px 6px 4px 0;
    border-bottom: 1px solid #1e293b;
    flex-shrink: 0;
    white-space: nowrap;
}
.lvp-sb-hdr-toggle {
    display: inline-flex;
    align-items: center;
    gap: 4px;
    width: 100%;
    padding: 4px 10px;
    background: none;
    border: none;
    color: #64748b;
    font-size: 10px;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.6px;
    cursor: pointer;
    white-space: nowrap;
    text-align: left;
    transition: color 0.15s;
    user-select: none;
}
.lvp-sb-hdr-toggle:hover { color: #e2e8f0; }
.lvp-sb-hdr-toggle-arrow {
    font-size: 9px;
    margin-left: auto;
    opacity: 0.6;
}
.lvp-file-list {
    flex: 1;
    overflow-y: auto;
    padding: 4px 0;
}
.lvp-file-btn {
    display: block;
    width: 100%;
    text-align: left;
    padding: 5px 10px;
    background: none;
    border: none;
    border-left: 3px solid transparent;
    color: #e2e8f0;
    font-family: 'Cascadia Code','Fira Code','Consolas',monospace;
    font-size: 11px;
    cursor: pointer;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    transition: background 0.12s;
}
.lvp-file-btn:hover  { background: #1e293b; }
.lvp-file-btn.lvp-file-active {
    background: #1e293b;
    border-left-color: #4da6ff;
    color: #4da6ff;
    font-weight: 600;
}
/* right viewer area */
.lvp-viewer {
    flex: 1;
    min-width: 0;
    display: flex;
    flex-direction: column;
    gap: 6px;
    overflow: hidden;
}
/* sidebar toggle strip */
.lvp-toggle-strip {
    display: flex;
    align-items: center;
    gap: 5px;
    flex-shrink: 0;
    flex-wrap: wrap;
}
.lvp-sb-toggle {
    display: inline-flex;
    align-items: center;
    gap: 4px;
    padding: 3px 8px;
    background: #1e293b;
    border: 1px solid #334155;
    border-radius: 4px;
    color: #94a3b8;
    font-size: 11px;
    cursor: pointer;
    white-space: nowrap;
    transition: border-color 0.15s, color 0.15s;
    user-select: none;
}
.lvp-sb-toggle:hover { border-color: #64748b; color: #e2e8f0; }
.lvp-sb-arrow {
    transition: transform 0.18s;
    display: inline-block;
    font-size: 9px;
    line-height: 1;
}
.lvp-sb-toggle.lvp-sb-open .lvp-sb-arrow { transform: rotate(90deg); }
.lvp-fname-badge {
    font-size: 11px;
    color: #94a3b8;
    font-family: 'Cascadia Code','Fira Code','Consolas',monospace;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    max-width: 200px;
}
/* terminal */
.lvp-terminal {
    overflow-y: auto;
    font-family: 'Cascadia Code','Fira Code','Consolas',monospace;
    font-size: 12px;
    line-height: 1.45;
    background: #000;
    color: #b0b0b0;
    padding: 10px 12px;
    border: 1px solid #1e293b;
    border-radius: 5px;
    white-space: pre-wrap;
    word-break: break-all;
    flex: 1;
    min-height: 0;
}
.lvp-terminal.show-line-nums > div { padding-left: 52px; position: relative; }
.lvp-terminal.show-line-nums > div::before {
    content: attr(data-ln);
    position: absolute; left: 0; width: 44px;
    text-align: right; color: #555; font-size: 11px;
    user-select: none; pointer-events: none;
}
.lvp-log-debug    { color: #808080; }
.lvp-log-info     { color: #b0b0b0; }
.lvp-log-warning  { color: #ff8c00; }
.lvp-log-error    { color: #ff4b4b; }
.lvp-log-critical { color: #b39ddb; font-weight: 600; }
.lvp-hidden       { display: none !important; }
.lvp-level-hidden { display: none !important; }
.lvp-highlight { background: rgba(255,200,0,0.18); }
.lvp-highlight mark { background: #e8a620; color: #000; border-radius: 2px; padding: 0 1px; }
.lvp-current-match { background: rgba(255,160,0,0.40); outline: 1px solid #e8a620; }
.lvp-lvl-btn {
    padding: 3px 7px; border-radius: 3px; border: 1px solid #333640;
    background: #262730; font-size: 11px; font-weight: 700; cursor: pointer;
    font-family: monospace; transition: all 0.15s; opacity: 0.4; color: #aaa;
}
.lvp-lvl-btn.on { opacity: 1.0; }
.lvp-lvl-btn[data-lvl="DEBUG"].on    { background: #3a3f4b; border-color: #555;    color: #e2e8f0; }
.lvp-lvl-btn[data-lvl="INFO"].on     { background: #0d3b20; border-color: #21c354; color: #21c354; }
.lvp-lvl-btn[data-lvl="WARNING"].on  { background: #3b2700; border-color: #ff8c00; color: #ff8c00; }
.lvp-lvl-btn[data-lvl="ERROR"].on    { background: #3b0d0d; border-color: #ff4b4b; color: #ff4b4b; }
.lvp-lvl-btn[data-lvl="CRITICAL"].on { background: #2d0040; border-color: #b39ddb; color: #b39ddb; }
.lvp-ctrl-btn {
    padding: 3px 9px; background: #262730; border: 1px solid #333640;
    border-radius: 4px; color: #e2e8f0; font-size: 12px; cursor: pointer;
    white-space: nowrap; transition: all 0.15s; line-height: 1.6;
}
.lvp-ctrl-btn:hover { background: #4da6ff; color: #000; border-color: #4da6ff; }
.lvp-ctrl-btn.lvp-stream-on { background: #21c354; color: #000; border-color: #21c354; }
.lvp-ctrl-btn.lvp-active { background: #4da6ff; color: #000; }
.lvp-nav-btn {
    background: #262730; border: 1px solid #333640; border-radius: 3px;
    color: #94a3b8; cursor: pointer; font-size: 12px; padding: 2px 6px; line-height: 1;
}
.lvp-nav-btn:hover { color: #e2e8f0; border-color: #94a3b8; }
/* ── end LogViewerPanel ───────────────────────────────────────────────── */
`;
        document.head.appendChild(s);
    }

    // ─── DOM helper ─────────────────────────────────────────────────────────
    _q(suffix) { return document.getElementById(this._cid + '-lvp-' + suffix); }

    // ─── Build HTML ─────────────────────────────────────────────────────────
    _build() {
        const container = document.getElementById(this._cid);
        if (!container) return;
        const p = this._cid + '-lvp-';
        const esc = s => s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
        container.innerHTML = `
<div class="lvp-root">
  <!-- left sidebar: file list -->
  <div class="lvp-sidebar lvp-collapsed" id="${p}sidebar">
    <div class="lvp-sidebar-header">
      <button class="lvp-sb-hdr-toggle" id="${p}sb-hdr-toggle"
              title="Collapse file list">Files <span class="lvp-sb-hdr-toggle-arrow">&#9664;</span></button>
    </div>
    <div class="lvp-file-list" id="${p}file-list"></div>
  </div>
  <!-- right viewer -->
  <div class="lvp-viewer">
    <!-- toolbar row -->
    <div style="display:flex;align-items:center;gap:5px;flex-shrink:0;flex-wrap:wrap;">
      <!-- sidebar toggle (toolbar, visible only when sidebar is collapsed) -->
      <button class="lvp-sb-toggle" id="${p}sb-toggle">
        <span class="lvp-sb-arrow">&#9658;</span> Files
      </button>
      <span class="lvp-fname-badge" id="${p}fname-badge">${esc(this._file)}</span>
      <div style="width:1px;height:18px;background:#334155;flex-shrink:0;"></div>
      <div style="display:flex;gap:3px;">
        <button class="lvp-lvl-btn on" data-lvl="DEBUG"    id="${p}lvl-DEBUG">DBG</button>
        <button class="lvp-lvl-btn on" data-lvl="INFO"     id="${p}lvl-INFO">INF</button>
        <button class="lvp-lvl-btn on" data-lvl="WARNING"  id="${p}lvl-WARNING">WRN</button>
        <button class="lvp-lvl-btn on" data-lvl="ERROR"    id="${p}lvl-ERROR">ERR</button>
        <button class="lvp-lvl-btn on" data-lvl="CRITICAL" id="${p}lvl-CRITICAL">CRT</button>
      </div>
      <div style="width:1px;height:18px;background:#334155;flex-shrink:0;"></div>
      <label style="font-size:12px;color:#94a3b8;display:flex;align-items:center;gap:4px;white-space:nowrap;">Lines:
        <select id="${p}lines-sel" style="font-size:12px;background:#1e293b;color:#e2e8f0;border:1px solid #334155;border-radius:4px;padding:2px 4px;">
          <option value="200">200</option>
          <option value="500">500</option>
          <option value="1000">1000</option>
          <option value="2000">2000</option>
          <option value="5000">5000</option>
        </select>
      </label>
      <button id="${p}stream-btn" class="lvp-ctrl-btn lvp-stream-on">&#9208; Pause</button>
      <button id="${p}clear-btn"  class="lvp-ctrl-btn">&#128465; Clear</button>
      <button id="${p}dl-btn"     class="lvp-ctrl-btn">&#8595; Download</button>
      <button id="${p}ln-btn"     class="lvp-ctrl-btn"># Lines</button>
      <div style="width:1px;height:18px;background:#334155;flex-shrink:0;"></div>
      <span id="${p}conn" style="font-size:11px;color:#64748b;">connecting\u2026</span>
    </div>
    <!-- search bar -->
    <div style="display:flex;gap:6px;align-items:center;flex-shrink:0;flex-wrap:wrap;">
      <select id="${p}preset" style="font-size:12px;background:#1e293b;color:#e2e8f0;border:1px solid #334155;border-radius:4px;padding:3px 6px;">
        <option value="">\u2014 Preset \u2014</option>
        <option value="error|traceback|exception">Errors</option>
        <option value="warning|warn">Warnings</option>
        <option value="error|warning|traceback">Errors + Warnings</option>
        <option value="connect|disconnect|timeout|reconnect">Connection</option>
        <option value="restart|kill|stop|shutdown">Restart / Stop</option>
        <option value="traceback|exception|raise">Traceback</option>
      </select>
      <input type="text" id="${p}search" placeholder="Search logs\u2026"
             style="flex:1;min-width:140px;font-size:12px;background:#1e293b;color:#e2e8f0;border:1px solid #334155;border-radius:4px;padding:4px 8px;">
      <label style="font-size:12px;color:#94a3b8;display:inline-flex;align-items:center;gap:4px;cursor:pointer;white-space:nowrap;">
        <input type="checkbox" id="${p}filter-chk" checked> Filter
      </label>
      <span id="${p}nav-btns" style="display:none;gap:2px;">
        <button class="lvp-nav-btn" id="${p}nav-up" title="Prev (Shift+Enter)">&#9650;</button>
        <button class="lvp-nav-btn" id="${p}nav-dn" title="Next (Enter)">&#9660;</button>
      </span>
      <span id="${p}match-count" style="font-size:11px;color:#888;min-width:70px;"></span>
    </div>
    <!-- terminal -->
    <div id="${p}terminal" class="lvp-terminal"></div>
  </div>
</div>`;
        this._bindEvents();
    }

    // ─── Event binding ───────────────────────────────────────────────────────
    _bindEvents() {
        for (const lvl of ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']) {
            this._q('lvl-' + lvl).addEventListener('click', () => this._toggleLevel(lvl));
        }
        this._q('lines-sel').addEventListener('change',  () => this._resubscribe());
        this._q('stream-btn').addEventListener('click',   () => this._toggleStream());
        this._q('clear-btn').addEventListener('click',    () => this._clear());
        this._q('dl-btn').addEventListener('click',       () => this._download());
        this._q('ln-btn').addEventListener('click',       () => this._toggleLineNums());
        this._q('sb-toggle').addEventListener('click',    () => this._toggleSidebar());
        this._q('sb-hdr-toggle').addEventListener('click', () => this._toggleSidebar());
        this._q('preset').addEventListener('change',      () => this._onPresetChange());
        this._q('search').addEventListener('input',       () => this._onSearchInput());
        this._q('search').addEventListener('keydown',     e  => this._onSearchKeydown(e));
        this._q('filter-chk').addEventListener('change',  () => this._onFilterToggle());
        this._q('nav-up').addEventListener('click',       () => this._searchNav(-1));
        this._q('nav-dn').addEventListener('click',       () => this._searchNav(1));
    }

    // ─── Public API ──────────────────────────────────────────────────────────
    open()  { this._connect(); }
    close() { this._disconnect(); }

    // ─── Sidebar ─────────────────────────────────────────────────────────────
    _toggleSidebar() {
        this._sidebarOpen = !this._sidebarOpen;
        const sidebar      = this._q('sidebar');
        const tbToggle     = this._q('sb-toggle');   // toolbar button
        const fnameBadge   = this._q('fname-badge');
        if (this._sidebarOpen) {
            sidebar.classList.remove('lvp-collapsed');
            tbToggle.style.display = 'none';          // hide toolbar button when open
            if (fnameBadge) fnameBadge.style.display = 'none';  // filename shown in sidebar
            // refresh file list on open
            if (this._ws && this._ws.readyState === WebSocket.OPEN) {
                this._ws.send(JSON.stringify({ cmd: 'list_local_logs' }));
            }
        } else {
            sidebar.classList.add('lvp-collapsed');
            tbToggle.style.display = '';              // show toolbar button when collapsed
            if (fnameBadge) fnameBadge.style.display = '';      // show filename badge
        }
    }

    _updateFileList(files) {
        this._fileList = files;
        const list = this._q('file-list');
        if (!list) return;
        list.innerHTML = '';
        for (const f of files) {
            const btn = document.createElement('button');
            btn.className = 'lvp-file-btn' + (f === this._file ? ' lvp-file-active' : '');
            btn.textContent = f;
            btn.title = f;
            btn.addEventListener('click', () => this._switchFile(f));
            list.appendChild(btn);
        }
    }

    _switchFile(f) {
        if (f === this._file) return;
        this._file = f;
        const list = this._q('file-list');
        if (list) {
            for (const btn of list.querySelectorAll('.lvp-file-btn')) {
                btn.classList.toggle('lvp-file-active', btn.textContent === f);
            }
        }
        const fnameBadge = this._q('fname-badge');
        if (fnameBadge) fnameBadge.textContent = f;
        this._clear();
        this._resubscribe();
    }

    // ─── WebSocket ───────────────────────────────────────────────────────────
    _connect() {
        this._disconnect();
        const ws = new WebSocket(this._wsBase + '/ws/vps?token=' + encodeURIComponent(this._token));
        this._ws = ws;
        ws.onopen = () => {
            const connEl = this._q('conn');
            if (connEl) connEl.textContent = 'connected';
            this._resubscribe();
            ws.send(JSON.stringify({ cmd: 'list_local_logs' }));
        };
        ws.onmessage = evt => {
            try { this._handleMsg(JSON.parse(evt.data)); } catch (e) { /* ignore */ }
        };
        ws.onerror = () => {
            const connEl = this._q('conn');
            if (connEl) connEl.textContent = 'error';
        };
        ws.onclose = () => {
            if (this._ws !== ws) return;
            this._ws = null;
            this._streaming = false;
            this._updateStreamBtn();
            const connEl = this._q('conn');
            if (connEl) connEl.textContent = 'disconnected';
        };
    }

    _disconnect() {
        if (this._ws) {
            try { this._ws.close(); } catch (e) { /* ignore */ }
            this._ws = null;
        }
        this._streaming = false;
    }

    // ─── Message handling ────────────────────────────────────────────────────
    _handleMsg(msg) {
        if (msg.type === 'local_logs') {
            if (msg.sid !== undefined && msg.sid !== this._sid) return;
            this._lines    = msg.lines || [];
            this._lineBase = 0;
            this._streaming = true;
            this._updateStreamBtn();
            this._renderFull();
        } else if (msg.type === 'local_log_lines') {
            if (msg.sid !== undefined && msg.sid !== this._sid) return;
            const nl = msg.lines || [];
            this._lines.push(...nl);
            if (this._lines.length > this._MAX) {
                const trim = this._lines.length - this._MAX;
                this._lines    = this._lines.slice(-this._MAX);
                this._lineBase += trim;
            }
            this._appendLines(nl);
        } else if (msg.type === 'local_logs_list') {
            this._updateFileList(msg.files || []);
        } else if (msg.type === 'state' && msg.data && msg.data.local_logs) {
            this._updateFileList(msg.data.local_logs);
        }
    }

    // ─── Level filter ─────────────────────────────────────────────────────────
    _extractLevel(line) {
        const m = line.match(/\[(DEBUG|INFO|WARNING|ERROR|CRITICAL)\]/i);
        if (m) return m[1].toUpperCase();
        const m2 = line.match(/\b(DEBUG|INFO|WARNING|WARN|ERROR|CRITICAL)\b/i);
        if (m2) { const l = m2[1].toUpperCase(); return l === 'WARN' ? 'WARNING' : l; }
        return 'INFO';
    }

    _levelClass(level) {
        const map = { DEBUG: 'lvp-log-debug', INFO: 'lvp-log-info', WARNING: 'lvp-log-warning', ERROR: 'lvp-log-error', CRITICAL: 'lvp-log-critical' };
        return map[level] || 'lvp-log-info';
    }

    _toggleLevel(lvl) {
        const btn = this._q('lvl-' + lvl);
        if (this._visLevels.has(lvl)) {
            this._visLevels.delete(lvl);
            if (btn) btn.classList.remove('on');
        } else {
            this._visLevels.add(lvl);
            if (btn) btn.classList.add('on');
        }
        const term = this._q('terminal');
        for (const div of term.children) {
            const lv = div.dataset.level || 'INFO';
            if (this._visLevels.has(lv)) div.classList.remove('lvp-level-hidden');
            else                         div.classList.add('lvp-level-hidden');
        }
        this._updateMatchCount();
    }

    // ─── Stream control ───────────────────────────────────────────────────────
    _toggleStream() {
        if (this._streaming) {
            if (this._ws && this._ws.readyState === WebSocket.OPEN) {
                this._ws.send(JSON.stringify({ cmd: 'unsubscribe_local_logs' }));
            }
            this._streaming = false;
        } else {
            this._resubscribe();
        }
        this._updateStreamBtn();
    }

    _updateStreamBtn() {
        const btn = this._q('stream-btn');
        if (!btn) return;
        if (this._streaming) {
            btn.textContent = '\u23F8 Pause';
            btn.className   = 'lvp-ctrl-btn lvp-stream-on';
        } else {
            btn.textContent = '\u25B6 Stream';
            btn.className   = 'lvp-ctrl-btn';
        }
    }

    // ─── Rendering ────────────────────────────────────────────────────────────
    _esc(s) { return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;'); }

    _getSearchRe() {
        if (!this._searchTerm) return null;
        try {
            const pat = this._searchRegex
                ? this._searchTerm
                : this._searchTerm.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
            return new RegExp('(' + pat + ')', 'gi');
        } catch (e) { return null; }
    }

    _buildDiv(line, lineNum) {
        const div   = document.createElement('div');
        if (lineNum != null) div.dataset.ln = lineNum;
        const level = this._extractLevel(line);
        div.dataset.level = level;
        div.dataset.text  = line;
        let cls = this._levelClass(level);
        if (!this._visLevels.has(level)) cls += ' lvp-level-hidden';
        const re = this._getSearchRe();
        if (re) {
            if (re.test(line)) {
                re.lastIndex = 0;
                cls += ' lvp-highlight';
                div.innerHTML = this._esc(line).replace(this._getSearchRe(), '<mark>$1</mark>');
            } else {
                div.textContent = line;
                if (this._filterMode) cls += ' lvp-hidden';
            }
        } else if (this._searchTerm) {
            div.textContent = line;
            if (this._filterMode) cls += ' lvp-hidden';
        } else {
            div.textContent = line;
        }
        div.className = cls;
        return div;
    }

    _renderFull() {
        const term  = this._q('terminal');
        this._pending    = [];
        this._rafPending = false;
        term.innerHTML   = '';
        const total = this._lines.length;
        if (!total) { this._updateMatchCount(); return; }

        if (total <= this._CHUNK) {
            const frag = document.createDocumentFragment();
            for (let i = 0; i < total; i++) {
                frag.appendChild(this._buildDiv(this._lines[i], this._lineBase + i + 1));
            }
            term.appendChild(frag);
            term.scrollTop = term.scrollHeight;
            if (this._searchTerm) setTimeout(() => this._applySearch(), 0);
            else this._updateMatchCount();
            return;
        }

        const rid = ++this._renderAbort;
        let idx = 0;
        const status = document.createElement('div');
        status.style.cssText = 'color:#888;padding:8px;font-size:12px;';
        status.textContent   = 'Rendering\u2026';
        term.appendChild(status);
        const rChunk = () => {
            if (rid !== this._renderAbort) return;
            const end  = Math.min(idx + this._CHUNK, total);
            const frag = document.createDocumentFragment();
            for (; idx < end; idx++) {
                frag.appendChild(this._buildDiv(this._lines[idx], this._lineBase + idx + 1));
            }
            if (idx >= total) {
                status.remove();
                term.appendChild(frag);
                term.scrollTop = term.scrollHeight;
                if (this._searchTerm) setTimeout(() => this._applySearch(), 0);
                else this._updateMatchCount();
            } else {
                term.insertBefore(frag, status);
                status.textContent = 'Rendering\u2026 ' + Math.round(idx / total * 100) + '%';
                requestAnimationFrame(rChunk);
            }
        };
        requestAnimationFrame(rChunk);
    }

    _appendLines(newLines) {
        if (!newLines.length) return;
        const startNum = this._lineBase + this._lines.length - newLines.length + 1;
        for (let i = 0; i < newLines.length; i++) {
            this._pending.push({ line: newLines[i], num: startNum + i });
        }
        if (this._rafPending) return;
        this._rafPending = true;
        requestAnimationFrame(() => {
            const term     = this._q('terminal');
            const atBottom = term.scrollTop + term.clientHeight >= term.scrollHeight - 40;
            const frag     = document.createDocumentFragment();
            for (const item of this._pending) {
                frag.appendChild(this._buildDiv(item.line, item.num));
            }
            this._pending    = [];
            this._rafPending = false;
            term.appendChild(frag);
            while (term.childElementCount > this._MAX) term.removeChild(term.firstChild);
            if (atBottom) term.scrollTop = term.scrollHeight;
            this._updateMatchCount();
        });
    }

    // ─── Search ───────────────────────────────────────────────────────────────
    _onPresetChange() {
        const val = this._q('preset').value;
        if (val) {
            this._searchTerm   = val;
            this._searchRegex  = true;
            this._q('search').value = val;
        } else {
            this._searchTerm   = '';
            this._searchRegex  = false;
            this._q('search').value = '';
        }
        setTimeout(() => this._applySearch(), 0);
    }

    _onSearchInput() {
        this._searchTerm  = (this._q('search').value || '').trim();
        this._searchRegex = false;
        this._q('preset').value = '';
        if (this._searchTimer) clearTimeout(this._searchTimer);
        this._searchTimer = setTimeout(() => this._applySearch(), 300);
    }

    _onFilterToggle() {
        this._filterMode = this._q('filter-chk').checked;
        setTimeout(() => this._applySearch(), 0);
    }

    _onSearchKeydown(e) {
        if (e.key !== 'Enter') return;
        e.preventDefault();
        this._searchNav(e.shiftKey ? -1 : 1);
    }

    _searchNav(dir) {
        if (this._filterMode || !this._matchEls.length) return;
        if (this._matchIdx >= 0 && this._matchIdx < this._matchEls.length) {
            this._matchEls[this._matchIdx].classList.remove('lvp-current-match');
        }
        this._matchIdx += dir;
        if (this._matchIdx >= this._matchEls.length) this._matchIdx = 0;
        if (this._matchIdx < 0) this._matchIdx = this._matchEls.length - 1;
        const el   = this._matchEls[this._matchIdx];
        const term = this._q('terminal');
        el.classList.add('lvp-current-match');
        term.scrollTop = el.offsetTop - term.offsetTop - term.clientHeight / 2 + el.offsetHeight / 2;
        this._updateMatchCount();
    }

    _applySearch() {
        const term    = this._q('terminal');
        const navBtns = this._q('nav-btns');
        const re       = this._getSearchRe();
        const children = Array.from(term.children);
        this._matchEls = [];
        this._matchIdx = -1;

        if (!this._searchTerm) {
            for (const div of children) {
                const text = div.dataset.text || div.textContent;
                div.textContent = text;
                let cls = div.className.replace(/ lvp-highlight| lvp-hidden| lvp-level-hidden| lvp-current-match/g, '');
                const lv = div.dataset.level || 'INFO';
                if (!this._visLevels.has(lv)) cls += ' lvp-level-hidden';
                div.className = cls;
            }
            if (navBtns) navBtns.style.display = 'none';
            this._updateMatchCount();
            return;
        }

        const sid   = ++this._searchAbort;
        const total = children.length;
        let idx = 0;
        const sChunk = () => {
            if (sid !== this._searchAbort) return;
            const end = Math.min(idx + this._SCHUNK, total);
            for (; idx < end; idx++) {
                const div  = children[idx];
                const text = div.dataset.text || div.textContent;
                let cls = div.className.replace(/ lvp-highlight| lvp-hidden| lvp-level-hidden| lvp-current-match/g, '');
                const lv = div.dataset.level || 'INFO';
                if (!this._visLevels.has(lv)) {
                    div.className = cls + ' lvp-level-hidden';
                    continue;
                }
                if (re && re.test(text)) {
                    re.lastIndex = 0;
                    cls += ' lvp-highlight';
                    div.innerHTML = this._esc(text).replace(this._getSearchRe(), '<mark>$1</mark>');
                    this._matchEls.push(div);
                } else {
                    div.textContent = text;
                    if (this._filterMode) cls += ' lvp-hidden';
                }
                div.className = cls;
            }
            if (idx < total) {
                requestAnimationFrame(sChunk);
            } else {
                if (navBtns) {
                    navBtns.style.display = (!this._filterMode && this._matchEls.length) ? 'inline-flex' : 'none';
                }
                this._updateMatchCount();
            }
        };
        requestAnimationFrame(sChunk);
    }

    _updateMatchCount() {
        const el = this._q('match-count');
        if (!el) return;
        if (!this._searchTerm) { el.textContent = ''; return; }
        const count = this._q('terminal').querySelectorAll('.lvp-highlight').length;
        if (!this._filterMode && this._matchIdx >= 0) {
            el.textContent = (this._matchIdx + 1) + ' / ' + count;
        } else {
            el.textContent = count + ' matches';
        }
    }

    // ─── Controls ─────────────────────────────────────────────────────────────
    _clear() {
        this._lines      = [];
        this._lineBase   = 0;
        this._pending    = [];
        this._rafPending = false;
        ++this._renderAbort;
        const term = this._q('terminal');
        if (term) term.innerHTML = '';
        this._updateMatchCount();
    }

    _resubscribe() {
        if (!this._ws || this._ws.readyState !== WebSocket.OPEN) return;
        this._clear();
        const sid   = ++this._sid;
        const lines = parseInt(this._q('lines-sel').value || '200', 10);
        this._ws.send(JSON.stringify({ cmd: 'subscribe_local_logs', file: this._file, lines: lines, sid: sid }));
        this._streaming = true;
        this._updateStreamBtn();
    }

    _download() {
        if (!this._lines.length) return;
        const blob = new Blob([this._lines.join('\n')], { type: 'text/plain' });
        const url  = URL.createObjectURL(blob);
        const a    = document.createElement('a');
        a.href     = url;
        a.download = this._file;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        URL.revokeObjectURL(url);
    }

    _toggleLineNums() {
        this._showLineNums = !this._showLineNums;
        const term = this._q('terminal');
        const btn  = this._q('ln-btn');
        term.classList.toggle('show-line-nums', this._showLineNums);
        if (btn) btn.className = this._showLineNums ? 'lvp-ctrl-btn lvp-active' : 'lvp-ctrl-btn';
    }
}
