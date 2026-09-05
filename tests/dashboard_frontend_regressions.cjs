/* Offline dashboard regressions. Only versioned frontend source is read. */
'use strict';
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const source = name => fs.readFileSync(path.join(__dirname, '..', 'frontend', name), 'utf8');
const tick = () => new Promise(setImmediate);
const location = {origin: 'https://pbgui.test:8443', protocol: 'https:', host: 'pbgui.test:8443'};

function functionCode(html, name) {
    const match = html.match(new RegExp('^([ \\t]*)function ' + name + '\\(', 'm'));
    assert.ok(match, name);
    const end = html.indexOf('\n' + match[1] + '}', match.index);
    assert.ok(end > match.index, name);
    return html.slice(match.index, end + match[1].length + 2);
}

class Element {
    constructor(tag = 'div') {
        this.tagName = tag;
        this.style = {};
        this.listeners = {};
        this.children = [];
        this.attrs = {};
        this.className = '';
        this.src = '';
        this.value = '';
        this.textContent = '';
        this.classList = {
            contains: name => this.className.split(' ').includes(name),
            add: name => { if (!this.classList.contains(name)) this.className += ' ' + name; },
            remove: name => { this.className = this.className.split(' ').filter(x => x !== name).join(' '); },
            toggle: (name, yes) => yes ? this.classList.add(name) : this.classList.remove(name)
        };
    }
    addEventListener(type, fn) { (this.listeners[type] ||= new Set()).add(fn); }
    removeEventListener(type, fn) { this.listeners[type]?.delete(fn); }
    dispatch(type, event = {}) {
        for (const fn of [...(this.listeners[type] || [])]) {
            fn({...event, type, currentTarget: this, preventDefault() {}, stopPropagation() {}});
        }
    }
    count(type) { return this.listeners[type]?.size || 0; }
    appendChild(child) { this.children.push(child); child.parentElement = this; return child; }
    set innerHTML(value) {
        this.children.forEach(child => { child.parentElement = null; });
        this.children = [];
        this.html = value;
    }
    get innerHTML() { return this.html || ''; }
    setAttribute(key, value) { this.attrs[key] = value; }
    getAttribute(key) { return this.attrs[key]; }
    get isConnected() { return this.connected || !!this.parentElement?.isConnected; }
    matches(selector) {
        return selector.startsWith('.') ? this.classList.contains(selector.slice(1)) : this.tagName === selector;
    }
    closest(selector) {
        return selector.split(',').some(s => this.matches(s.trim())) ? this : this.parentElement?.closest(selector) || null;
    }
    querySelectorAll(selector) {
        return this.children.flatMap(child => [
            ...(selector.split(',').some(s => child.matches(s.trim())) ? [child] : []),
            ...child.querySelectorAll(selector)
        ]);
    }
    querySelector(selector) { return this.querySelectorAll(selector)[0] || null; }
    getBoundingClientRect() { return {height: parseFloat(this.style.height) || 300}; }
}

function mainContext(html = source('dashboard_main.html')) {
    const window = new Element();
    const document = {documentElement: new Element(), body: new Element()};
    const requests = [];
    const ctx = vm.createContext({
        window, document, location, encodeURIComponent, Date,
        currentDash: 'old', selectedDashboards: ['old'], DASHBOARDS: ['old'],
        editMode: false, viewDirty: false, API_BASE: '/api', dashboardCreatedGeneration: 0,
        contentFrame: new Element(), contentLoading: new Element(), editBanner: new Element(),
        tplOverlay: new Element(), tplIframe: new Element(),
        renderToolbar() {}, renderList() {}, updateCount() {}, updateViewSaveBtn() {},
        pruneSelectedDashboards() {},
        apiFetch(url) { assert.equal(url, '/dashboards'); return new Promise(resolve => requests.push(resolve)); }
    });
    for (const name of ['loadView', 'refreshList']) {
        const start = html.indexOf('  function ' + name + '(');
        vm.runInContext(html.slice(start, html.indexOf('\n  }', start) + 4), ctx);
    }
    for (const listener of html.matchAll(/  window\.addEventListener\('message', function \(e\) \{[\s\S]*?^  \}\);/gm)) {
        vm.runInContext(listener[0], ctx);
    }
    ctx.contentFrame.src = '/old';
    ctx.contentFrame.contentWindow = {};
    ctx.tplIframe.contentWindow = {};
    ctx.tplOverlay.classList.add('visible');
    ctx.tplIframe.src = '/templates';
    return {ctx, requests, send: (data, event = {}) => window.dispatch('message', {
        data, origin: location.origin,
        source: ['pbgui_dashboard_created', 'pbgui_close_templates'].includes(data?.type)
            ? ctx.tplIframe.contentWindow : ctx.contentFrame.contentWindow,
        ...event
    })};
}

async function creation() {
    const {ctx, requests, send} = mainContext();
    send({type: 'pbgui_dashboard_created', name: 'new dash'});
    assert.equal(requests.length, 1, 'exactly one list request per creation');
    assert.equal(ctx.tplOverlay.classList.contains('visible'), false);
    assert.equal(ctx.tplIframe.src, '');
    assert.equal(ctx.currentDash, 'old');
    await tick(); // The old view remains until the delayed list response arrives.
    requests.shift()({ok: true, json: async () => ({dashboards: ['old', 'new dash']})});
    await tick();
    assert.equal(ctx.currentDash, 'new dash');
    assert.deepEqual([...ctx.selectedDashboards], ['new dash']);
    assert.match(ctx.contentFrame.src, /name=new%20dash.*refresh=/);
    assert.equal(requests.length, 0);

    send({type: 'pbgui_dashboard_created', name: 'later'});
    ctx.loadView('manual');
    requests.shift()({ok: true, json: async () => ({dashboards: ['later', 'manual']})});
    await tick();
    assert.equal(ctx.currentDash, 'manual', 'later navigation wins');

    send({type: 'pbgui_dashboard_created', name: 'first'});
    send({type: 'pbgui_dashboard_created', name: 'second'});
    requests[1]({ok: true, json: async () => ({dashboards: ['first', 'second']})});
    await tick();
    requests[0]({ok: true, json: async () => ({dashboards: ['first', 'second']})});
    await tick();
    assert.equal(ctx.currentDash, 'second', 'older completion cannot overwrite newer creation');
}

async function templates() {
    const html = source('dashboard_templates.html');
    const start = html.indexOf('    /* Create dashboard(s) */');
    const code = html.slice(start, html.indexOf('\n  }\n', start));
    for (const scenario of [
        {users: [], name: 'single', exists: false, expected: 'single'},
        {users: [], name: 'overwrite', exists: true, expected: 'overwrite'},
        {users: ['one', 'two', 'failed'], name: '', expected: 'two'},
        {users: ['one', 'two', 'failed'], name: '', exists: true, expected: 'two'},
        {users: ['failed'], name: '', expected: null},
        {users: ['one'], name: '', exists: true, skipOverwrite: true, expected: null}
    ]) {
        const elements = {'btn-create': new Element(), 'tpl-select': new Element(), 'dash-name': new Element()};
        elements['tpl-select'].value = 'template';
        elements['dash-name'].value = scenario.name;
        const messages = [];
        const ctx = vm.createContext({
            document: {getElementById: id => elements[id]},
            location,
            window: {parent: {postMessage: (message, origin) => {
                assert.equal(origin, location.origin);
                messages.push(message);
            }}},
            getEffectiveUsers: () => scenario.users,
            confirmDialog: async opts => !(opts.title === 'Overwrite dashboard' && scenario.skipOverwrite),
            showMsg() {},
            apiGet: (url, cb) => cb(scenario.exists ? {config: {}} : null),
            apiPost: (url, body, cb) => cb({status: body.name === 'failed' ? 'error' : 'ok'})
        });
        vm.runInContext(code, ctx);
        elements['btn-create'].dispatch('click');
        await tick();
        assert.equal(messages.length, scenario.expected ? 1 : 0);
        if (scenario.expected) assert.equal(messages[0].name, scenario.expected);
        assert.equal(elements['btn-create'].disabled, false);
    }
}

async function cancel() {
    for (const name of ['', 'unsaved name', 'old']) {
        const {ctx, requests, send} = mainContext();
        ctx.currentDash = name;
        ctx.editMode = true;
        ctx.contentLoading.style.display = 'flex';
        ctx.contentFrame.onload = () => { throw Error('stale onload'); };
        send({type: 'pbgui_editor_cancelled', original_name: name});
        assert.equal(ctx.editMode, false);
        assert.equal(requests.length, 0);
        if (name === 'old') {
            assert.match(ctx.contentFrame.src, /name=old/);
            ctx.contentFrame.onload();
        } else {
            assert.equal(ctx.currentDash, '');
            assert.equal(ctx.contentFrame.src, '');
            assert.equal(ctx.contentFrame.onload, null);
            assert.equal(ctx.contentFrame.classList.contains('visible'), false);
        }
        assert.equal(ctx.contentLoading.style.display, 'none');
    }
}

async function resize() {
    const html = source('dashboard_editor.html');
    const move = html.indexOf('          function onMouseMove(e) {');
    const down = html.indexOf("          handle.addEventListener('mousedown', function (e) {");
    const code = html.slice(move, html.indexOf('          /* Double-click', move)) +
        html.slice(down, html.indexOf('          /* \u2500\u2500 Min button', down));
    for (const end of ['local', 'parent', 'blur', 'parent-blur', 'pagehide', 'standalone']) {
        const document = new Element();
        const window = new Element();
        window.document = document;
        const parent = end === 'standalone' ? window : new Element();
        if (parent !== window) parent.document = new Element();
        const messages = [];
        parent.postMessage = (msg, origin) => {
            assert.equal(origin, location.origin);
            messages.push(msg.type);
        };
        const cellEl = new Element();
        const table = cellEl.appendChild(new Element());
        table.className = 'di-table-wrap';
        table.style.overflowY = 'auto';
        const handle = new Element();
        let saves = 0;
        const ctx = vm.createContext({
            document, window, parent, location, cellEl, handle,
            state: {}, hKey: 'height', startH: 0, startY: 0, _resizeRaf: null, VIEW_ONLY: false,
            requestAnimationFrame: () => 1, cancelAnimationFrame() {},
            resizePlotsInCell() {}, reportHeight() {}, scheduleSync() { saves++; }
        });
        vm.runInContext(code, ctx);
        handle.dispatch('mousedown', {button: 2, clientY: 100});
        assert.equal(messages.length, 0);
        for (let round = 0; round < 2; round++) {
            handle.dispatch('mousedown', {button: 0, clientY: 100});
            assert.equal(table.style.overflowY, 'hidden');
            document.dispatch('mousemove', {clientY: 150});
            const lastHeight = cellEl.getBoundingClientRect().height;
            const target = end === 'parent' ? parent.document : end === 'parent-blur' ? parent :
                ['blur', 'pagehide', 'standalone'].includes(end) ? window : document;
            const type = ['blur', 'parent-blur', 'standalone'].includes(end) ? 'blur' :
                end === 'pagehide' ? 'pagehide' : 'mouseup';
            target.dispatch(type, {clientY: end === 'local' ? 150 : 900});
            assert.equal(ctx.state.height, lastHeight, 'parent coordinates must not inflate height');
            assert.equal(handle.classList.contains('active'), false);
            assert.equal(table.style.overflowY, 'auto');
            assert.equal(table._prevOverflowY, undefined);
            for (const emitter of new Set([document, window, parent, parent.document])) {
                for (const type of ['mousemove', 'mouseup', 'blur', 'pagehide']) assert.equal(emitter.count(type), 0);
            }
            assert.equal(saves, round + 1);
            assert.deepEqual(messages.slice(-2), ['pbgui_resize_start', 'pbgui_resize_end']);
        }
    }
}

async function charts() {
    const document = new Element();
    document.createElement = tag => new Element(tag);
    document.getElementById = () => new Element(); // CSS already installed.
    const window = {screen: {width: 1200, availHeight: 900}};
    const timers = new Map();
    let timerId = 0;
    const layouts = [], resizes = [];
    const Plotly = {
        react(chart) { chart._fullLayout = {}; },
        newPlot(chart) { chart._fullLayout = {}; },
        relayout(chart, layout) { assert.ok(chart.isConnected); layouts.push({chart, layout}); },
        Plots: {resize(chart) { assert.ok(chart.isConnected); resizes.push(chart); }}
    };
    window.Plotly = Plotly;
    const ctx = vm.createContext({window, document, Plotly,
        setTimeout(fn) { timers.set(++timerId, fn); return timerId; },
        clearTimeout(id) { timers.delete(id); }
    });
    vm.runInContext(source('dashboard_render.js'), ctx);
    const data = {rows: [['user', 'BTC', 2]],
        bars: [{date: '2026-09-01', period: '2026-09-01', income: 2, adg: 1, profits: 2, losses: -1}],
        traces: [{name: 'user', x: ['2026-09-01'], y: [2]}]};
    for (const kind of ['Top', 'Pnl', 'Ppl', 'Adg', 'Income']) {
        const container = new Element();
        container.connected = true;
        for (let i = 0; i < 4; i++) window.DashRender['build' + kind](container, data, {});
        const root = container.children[0];
        const chart = root.querySelector(kind === 'Income' ? '.di-chart' : '.dt-chart');
        assert.ok(chart, kind);
        assert.equal(document.count('fullscreenchange'), 0, 'no document-owned chart handlers');
        assert.equal(document.count('webkitfullscreenchange'), 0);
        assert.equal(root.count('fullscreenchange'), 1, 'one handler after in-place refreshes');
        assert.equal(root.count('webkitfullscreenchange'), 1);
        timers.clear(); // Discard initial sizing timers, test fullscreen exit timers separately.
        const before = layouts.length;
        document.fullscreenElement = root;
        root.dispatch('fullscreenchange');
        assert.equal(layouts.length, before + 1);
        assert.equal(layouts.at(-1).layout.width, 1200);
        document.fullscreenElement = null;
        root.dispatch('fullscreenchange');
        assert.equal(layouts.at(-1).layout.width, null);
        const delayed = [...timers.values()];
        container.innerHTML = '';
        const afterDetach = layouts.length;
        document.dispatch('fullscreenchange');
        root.dispatch('fullscreenchange'); // Even an already queued old event is harmless.
        delayed.forEach(fn => fn());
        assert.equal(layouts.length, afterDetach);
        assert.equal(resizes.length, 0);
        window.DashRender['build' + kind](container, data, {});
        document.webkitFullscreenElement = container.children[0];
        container.children[0].dispatch('webkitfullscreenchange');
        assert.equal(layouts.length, afterDetach + 1);
        document.webkitFullscreenElement = null;
    }

    const container = new Element();
    container.connected = true;
    const rows = [1, 3, 2].map(day => ({id: day, date: '2026-09-0' + day, user: 'u', symbol: 'BTC', income: day}));
    window.DashRender.buildIncome(container, {mode: 'table', rows}, {});
    const table = container.querySelector('table');
    const dates = () => table.querySelector('tbody').children.map(row => row.children[0].textContent);
    const arrow = () => table.querySelector('th').querySelector('.di-sort').textContent;
    assert.deepEqual(dates(), ['2026-09-03', '2026-09-02', '2026-09-01']);
    assert.equal(arrow(), ' \u25bc');
    table.querySelector('th').dispatch('click');
    assert.deepEqual(dates(), ['2026-09-01', '2026-09-02', '2026-09-03']);
    assert.equal(arrow(), ' \u25b2');
    assert.deepEqual(rows.map(row => row.id), [1, 3, 2], 'sorting must not mutate API rows');
    for (const name of fs.readdirSync(path.join(__dirname, '..', 'frontend')).filter(n => /^dashboard.*\.html$/.test(n))) {
        const html = source(name);
        if (!html.includes('/app/dashboard_render.js?v=')) continue;
        assert.equal(html.match(/var DR_VERSION = '([^']+)'/)[1], window.DashRender.VERSION, name);
    }
}

async function messages() {
    const {ctx, requests, send} = mainContext();
    const types = ['editor_saved', 'editor_cancelled', 'view_dirty', 'view_saved',
        'resize_start', 'resize_end', 'dashboard_created', 'close_templates'];
    for (const type of types) {
        const blocked = event => {
            ctx.viewDirty = type === 'view_saved';
            ctx.editMode = true;
            ctx.document.body.style.overflow = type === 'resize_end' ? 'hidden' : '';
            ctx.document.documentElement.style.overflow = ctx.document.body.style.overflow;
            const snapshot = JSON.stringify([ctx.viewDirty, ctx.editMode, ctx.document.body.style,
                ctx.document.documentElement.style, ctx.currentDash, ctx.contentFrame.src, ctx.tplIframe.src]);
            send({type: 'pbgui_' + type, name: 'evil'}, event);
            assert.equal(JSON.stringify([ctx.viewDirty, ctx.editMode, ctx.document.body.style,
                ctx.document.documentElement.style, ctx.currentDash, ctx.contentFrame.src, ctx.tplIframe.src]), snapshot, type);
            assert.equal(requests.length, 0, type);
        };
        for (const origin of ['https://evil.test', 'null', 'http://pbgui.test:8443', 'https://pbgui.test']) {
            blocked({origin});
        }
        for (const source of [{}, null, ctx.window,
            type.endsWith('templates') || type === 'dashboard_created'
                ? ctx.contentFrame.contentWindow : ctx.tplIframe.contentWindow]) {
            blocked({source});
        }
    }
    assert.equal(requests.length, 0, 'untrusted messages cannot refresh or navigate');
    assert.equal(ctx.currentDash, 'old');
    assert.equal(ctx.contentFrame.src, '/old');
    assert.equal(ctx.tplIframe.src, '/templates');
    assert.equal(ctx.tplOverlay.classList.contains('visible'), true);
    assert.equal(ctx.viewDirty, false);
    assert.equal(ctx.document.body.style.overflow, '');
    assert.equal(ctx.dashboardCreatedGeneration, 0);
    send({type: 'pbgui_view_dirty'});
    assert.equal(ctx.viewDirty, true);
    send({type: 'pbgui_view_saved'});
    assert.equal(ctx.viewDirty, false);
    send({type: 'pbgui_resize_start'});
    assert.equal(ctx.document.body.style.overflow, 'hidden');
    send({type: 'pbgui_resize_end'});
    assert.equal(ctx.document.body.style.overflow, '');
    send({type: 'pbgui_editor_saved', name: 'saved'});
    requests.shift()({ok: true, json: async () => ({dashboards: ['saved']})});
    await tick();
    assert.equal(ctx.currentDash, 'saved');
    send({type: 'pbgui_close_templates'});
    assert.equal(ctx.tplIframe.src, '');

    const html = source('dashboard_editor.html');
    for (const standalone of [false, true]) {
        const window = new Element();
        window.parent = standalone ? window : new Element();
        const saves = [], replies = [];
        window.parent.postMessage = (message, origin) => {
            assert.equal(origin, location.origin);
            replies.push(message.type);
        };
        const editor = vm.createContext({window, location, state: {name: 'saved'}, ORIG_NAME: 'old',
            VIEW_ONLY: true, closeAllUsersDropdowns() {}, setStatus() {},
            apiFetch(url, opts) { saves.push({url, opts}); return Promise.resolve({ok: true}); }
        });
        for (const name of ['doSave', 'doCancel', 'saveViewLayout', 'markViewDirty']) {
            vm.runInContext(functionCode(html, name), editor);
        }
        const listener = html.match(/  window\.addEventListener\('message', function \(e\) \{[\s\S]*?^  \}\);/m)[0];
        vm.runInContext(listener, editor);
        for (const type of ['pbgui_trigger_save', 'pbgui_trigger_cancel', 'pbgui_trigger_view_save']) {
            for (const origin of ['null', 'https://evil.test', 'http://pbgui.test:8443']) {
                window.dispatch('message', {data: {type}, origin, source: window.parent});
            }
            for (const source of [{}, null, standalone ? new Element() : window]) {
                window.dispatch('message', {data: {type}, origin: location.origin, source});
            }
        }
        await tick();
        assert.equal(saves.length, 0, 'untrusted messages cannot save');
        assert.equal(replies.length, 0, 'untrusted messages cannot cancel');
        for (const type of ['pbgui_trigger_save', 'pbgui_trigger_cancel', 'pbgui_trigger_view_save']) {
            window.dispatch('message', {data: {type}, origin: location.origin, source: window.parent});
        }
        editor.markViewDirty();
        await tick();
        assert.deepEqual(saves.map(s => s.url), ['/dashboards/saved', '/dashboards/old']);
        assert.ok(saves.every(s => s.opts.method === 'POST'));
        assert.deepEqual(replies.sort(), ['pbgui_editor_cancelled', 'pbgui_editor_saved', 'pbgui_view_dirty', 'pbgui_view_saved']);
    }

    const box = {isConnected: true, remove() { this.isConnected = false; }};
    const iframe = {contentWindow: {}};
    let refreshes = 0;
    const sidebar = vm.createContext({location, box, iframe,
        window: {parent: new Element()}, sendAction(action) { assert.equal(action, 'refresh'); refreshes++; }
    });
    vm.runInContext(functionCode(source('dashboard_sidebar.html'), 'onMsg'), sidebar);
    for (const type of ['pbgui_close_templates', 'pbgui_dashboard_created']) {
        sidebar.onMsg({data: {type}, origin: 'https://evil.test', source: iframe.contentWindow});
        sidebar.onMsg({data: {type}, origin: location.origin, source: {}});
    }
    assert.equal(refreshes, 0);
    assert.equal(box.isConnected, true);
    sidebar.onMsg({data: {type: 'pbgui_dashboard_created'}, origin: location.origin, source: iframe.contentWindow});
    assert.equal(refreshes, 1);
    sidebar.onMsg({data: {type: 'pbgui_close_templates'}, origin: location.origin, source: iframe.contentWindow});
    assert.equal(box.isConnected, false);
    sidebar.onMsg({data: {type: 'pbgui_dashboard_created'}, origin: location.origin, source: iframe.contentWindow});
    assert.equal(refreshes, 1, 'detached popup cannot revive navigation');

    // Execute every outgoing dashboard message, including toolbar and popup close buttons.
    for (const file of ['dashboard_main.html', 'dashboard_editor.html', 'dashboard_templates.html']) {
        const replies = [];
        const recipient = {postMessage(message, origin) { assert.equal(origin, location.origin); replies.push(message); }};
        const ctx = vm.createContext({location, window: {parent: recipient}, parent: recipient,
            contentFrame: {contentWindow: recipient}, name: 'n', ORIG_NAME: 'o', lastCreatedName: 'last', nameInput: 'input'});
        for (const call of source(file).matchAll(/(?:window\.parent|parent|contentFrame\.contentWindow)\.postMessage\([^\n]+?\);/g)) {
            vm.runInContext(call[0], ctx);
        }
        assert.ok(replies.length >= 3, file);
    }
}

async function assets() {
  for (const prefix of ['', '/pbgui', '/team/pbgui']) {
    for (const file of fs.readdirSync(path.join(__dirname, '..', 'frontend')).filter(n => /^dashboard.*\.html$/.test(n))) {
        const html = source(file);
        if (!html.includes('var BASE_PREFIX')) continue;
        const pageLocation = new URL(location.origin + prefix + '/app/' + file + '?api_base=https://evil.test/api');
        const document = {head: new Element(), createElement: tag => new Element(tag)};
        const ctx = vm.createContext({window: {location: pageLocation}, document, location: pageLocation, URL,
            API_BASE: 'https://evil.test/api', API_HOST: 'evil.test', DR_VERSION: 'test'});
        const declaration = html.match(/var BASE_PREFIX\s*=[^\n]+/)[0];
        vm.runInContext(declaration.replace('"%%BASE_PREFIX%%"', JSON.stringify(prefix)), ctx);
        assert.equal(ctx.BASE_PREFIX, prefix, file);
        if (html.includes('/app/dashboard_render.js?v=')) {
            const name = file === 'dashboard_editor.html' ? '_ensureRenderScript' : '_loadRenderScript';
            vm.runInContext(functionCode(html, name), ctx);
            ctx[name](() => {});
            assert.equal(document.head.children[0].src, prefix + '/app/dashboard_render.js?v=test', file);
        }
        if (file !== 'dashboard_editor.html') {
            vm.runInContext(html.match(/var API_BASE\s*=[^\n]+/)[0], ctx);
            assert.equal(ctx.API_BASE, prefix + '/api');
            for (const script of html.matchAll(/<script src="([^"]+)"/g)) {
                const url = new URL(script[1], pageLocation);
                assert.equal(url.origin, location.origin);
                assert.ok(url.pathname.startsWith(prefix + '/app/'), file);
            }
            if (html.includes('var API_HOST')) {
                const sockets = [];
                vm.runInContext(html.match(/var API_HOST\s*=[^\n]+/)[0], ctx);
                Object.assign(ctx, {isCurrentGeneration: () => true, isStale: () => false,
                    reconnTimer: null, clearTimeout() {}, _wsKey: 'ws',
                    WebSocket: class { constructor(url) { sockets.push(url); } }});
                vm.runInContext(functionCode(html, 'connect'), ctx);
                ctx.connect();
                assert.deepEqual(sockets, ['wss://' + location.host + prefix + '/ws/dashboard']);
                if (file === 'dashboard_orders.html') {
                    Object.assign(ctx, {candleReconnTimer: null, candleWs: null, _candleTimerKey: 'ct', _candleWsKey: 'cw',
                        selectedPosition: {user: 'u', symbol: 'BTC'}, currentTimeframe: '1m'});
                    vm.runInContext(functionCode(html, 'connectCandleWs'), ctx);
                    ctx.connectCandleWs();
                    assert.ok(sockets[1].startsWith('wss://' + location.host + prefix + '/ws/candles?'));
                }
            }
        }
    }
  }
}

async function page() {
    const {html, page, prefix} = JSON.parse(fs.readFileSync(0, 'utf8'));
    const requests = [], sockets = [];
    const document = {head: new Element(), createElement: tag => new Element(tag)};
    const ctx = vm.createContext({window: {location}, document, location,
        DR_VERSION: 'test', _reconnTimer: null, clearTimeout() {},
        WebSocket: class { constructor(url) { sockets.push(url); } },
        fetch(url) { requests.push(url); return Promise.resolve({ok: true, json: async () => ({})}); }
    });
    vm.runInContext(html.match(/var API_BASE\s*=[^\n]+/)[0], ctx);
    assert.equal(ctx.API_BASE, prefix + '/api');
    for (const script of html.matchAll(/<script src="([^"]+)"/g)) {
        assert.ok(script[1].startsWith(prefix + '/app/'), script[1]);
        assert.equal(new URL(script[1], location.origin).origin, location.origin);
    }
    if (page === 'templates_page') {
        vm.runInContext(functionCode(html, 'apiGet'), ctx);
        ctx.apiGet('/dashboards/templates', () => {});
    } else {
        vm.runInContext(functionCode(html, 'apiFetch'), ctx);
        await ctx.apiFetch('/dashboards');
    }
    assert.ok(requests.every(url => new URL(url, location.origin).origin === location.origin));
    assert.ok(requests.every(url => url.startsWith(prefix + '/api/dashboards')));
    if (page === 'editor_page') {
        vm.runInContext(html.match(/var BASE_PREFIX\s*=[^\n]+/)[0], ctx);
        vm.runInContext(functionCode(html, '_ensureRenderScript'), ctx);
        ctx._ensureRenderScript(() => {});
        assert.equal(document.head.children[0].src, prefix + '/app/dashboard_render.js?v=test');
        vm.runInContext(functionCode(html, '_connectWs'), ctx);
        ctx._connectWs();
        assert.deepEqual(sockets, ['wss://pbgui.test:8443' + prefix + '/ws/dashboard']);
    } else if (page === 'main_page') {
        const {ctx: main} = mainContext(html);
        main.API_BASE = ctx.API_BASE;
        main.loadView('mounted');
        assert.ok(main.contentFrame.src.startsWith(prefix + '/api/dashboard/editor_page?'));
        vm.runInContext(functionCode(html, 'loadEditor'), main);
        main.loadEditor('mounted');
        assert.ok(main.contentFrame.src.startsWith(prefix + '/api/dashboard/editor_page?'));
        vm.runInContext(functionCode(html, 'openTemplates'), main);
        main.openTemplates();
        assert.ok(main.tplIframe.src.startsWith(prefix + '/api/dashboard/templates_page?'));
    }
}

({creation, templates, cancel, resize, charts, messages, assets, page})[process.argv[2]]().catch(error => {
    console.error(error);
    process.exitCode = 1;
});
