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
    const match = html.match(new RegExp('^([ \\t]*)(?:async )?function ' + name + '\\(', 'm'));
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
        this.captureListeners = {};
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
    addEventListener(type, fn, options = false) {
        const capture = options === true || !!options.capture;
        ((capture ? this.captureListeners : this.listeners)[type] ||= new Set()).add(fn);
    }
    removeEventListener(type, fn, options = false) {
        const capture = options === true || !!options.capture;
        (capture ? this.captureListeners : this.listeners)[type]?.delete(fn);
    }
    dispatch(type, event = {}) {
        const dispatched = {...event, type, currentTarget: this, target: event.target || this,
            defaultPrevented: !!event.defaultPrevented,
            preventDefault() { this.defaultPrevented = true; }, stopPropagation() {},
            stopImmediatePropagation() { this.immediateStopped = true; }};
        for (const fn of [...(this.captureListeners[type] || []), ...(this.listeners[type] || [])]) {
            if (dispatched.immediateStopped) break;
            fn(dispatched);
        }
        return dispatched;
    }
    count(type) { return (this.listeners[type]?.size || 0) + (this.captureListeners[type]?.size || 0); }
    appendChild(child) { this.children.push(child); child.parentElement = this; return child; }
    get parentNode() { return this.parentElement; }
    removeChild(child) { this.children.splice(this.children.indexOf(child), 1); child.parentElement = null; return child; }
    contains(child) { return child === this || this.children.some(el => el.contains(child)); }
    get ownerDocument() { return this.document || this.parentElement?.ownerDocument; }
    focus() { if (this.ownerDocument) this.ownerDocument.activeElement = this; }
    click() { this.dispatch('click'); }
    get tabIndex() { return this._tabIndex ?? (['button', 'input', 'select', 'textarea', 'a'].includes(this.tagName) ? 0 : -1); }
    set tabIndex(value) { this._tabIndex = value; }
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
        const match = selector.match(/^([\w-]+)?(?:\[([\w-]+)(?:="([^"]*)")?\])?$/);
        if (selector.startsWith('#')) return this.id === selector.slice(1);
        if (match) return (!match[1] || this.tagName === match[1]) &&
            (!match[2] || (this.attrs[match[2]] !== undefined && (match[3] === undefined || this.attrs[match[2]] === match[3])));
        return selector.startsWith('.') && this.classList.contains(selector.slice(1));
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
    getBoundingClientRect() { return {height: parseFloat(this.style.height) || 300,
        width: parseFloat(this.style.width) || 800, left: parseFloat(this.style.left) || 0, top: parseFloat(this.style.top) || 0}; }
    getClientRects() { return this.isConnected && !this.hidden && this.style.display !== 'none' ? [this.getBoundingClientRect()] : []; }
}

function mainContext(html = source('dashboard_main.html')) {
    const window = new Element();
    const document = {documentElement: new Element(), body: new Element()};
    const requests = [];
    const ctx = vm.createContext({
        window, document, location, encodeURIComponent, Date,
        currentDash: 'old', selectedDashboards: ['old'], DASHBOARDS: ['old'],
        editMode: false, viewDirty: false, API_BASE: '/api', dashboardCreatedGeneration: 0,
        listGeneration: 0, persistNavigation() {},
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

function helpRequests() {
    const requests = [], errors = [];
    return {
        requests, errors,
        console: {error: (...args) => errors.push(args)},
        fetch(url, options) {
            return new Promise((resolve, reject) => requests.push({url, options, resolve, reject}));
        }
    };
}

function legacyHelpContext() {
    const {ctx} = mainContext();
    const http = helpRequests();
    Object.assign(ctx, http, {
        helpTopics: [{file: '33_dashboard.md', title: 'Dashboards'}],
        helpLang: 'EN', helpSel: 0, helpContent: new Element(), helpFilter: new Element(), helpTocList: new Element(),
        mdToHtml: text => '<p>' + text + '</p>'
    });
    ctx.document.createElement = tag => new Element(tag);
    for (const name of ['renderToc', 'loadTopic']) {
        vm.runInContext(functionCode(source('dashboard_main.html'), name), ctx);
    }
    ctx.renderToc();
    return {...http, content: ctx.helpContent, load: () => ctx.helpTocList.children[0].click()};
}

async function sharedHelpContext() {
    const code = source('js/shared_help_overlay.js');
    const nodes = {};
    for (const match of code.matchAll(/id="(pbgui-shared-help-[^"]+)"/g)) nodes[match[1]] = new Element();
    nodes['pbgui-shared-help-css'] = new Element();
    const document = new Element();
    document.body = new Element();
    document.getElementById = id => nodes[id] || null;
    document.createElement = tag => {
        const element = new Element(tag);
        if (tag === 'template') element.content = new Element();
        return element;
    };
    const http = helpRequests();
    const ctx = vm.createContext({
        ...http, document, URL, encodeURIComponent,
        PBGUI_BASE_PREFIX: '/pbgui', location: {...location, pathname: '/pbgui/api/dashboard/main_page'},
        localStorage: {getItem: () => null}, setTimeout: fn => { fn(); },
        marked: {parse: text => '<p>' + text + '</p>'}, DOMPurify: {sanitize: html => html}
    });
    ctx.window = ctx;
    vm.runInContext(code, ctx);
    // Use the actual Dashboard Guide opener and public shared overlay, with inert DOM/network.
    const opener = source('dashboard_main.html').match(
        /    window\._openDashboardHelp = function \(\) \{[\s\S]*?window\.PBGUI_HELP_OPENER = window\._openDashboardHelp;/
    );
    assert.ok(opener);
    vm.runInContext(opener[0], ctx);
    ctx.PBGUI_HELP_OPENER();
    await tick();
    const index = http.requests.shift();
    assert.equal(index.url, '/pbgui/api/help/index?lang=EN');
    assert.equal(index.options.credentials, 'same-origin');
    index.resolve({ok: true, json: async () => [
        {file: '33_dashboard.md', title: 'Dashboards'}, {file: '00_overview.md', title: 'Overview'}
    ]});
    await tick();
    assert.equal(nodes['pbgui-shared-help-ovl'].classList.contains('visible'), true);
    assert.equal(ctx.location.pathname, '/pbgui/api/dashboard/main_page');
    const initial = http.requests.shift();
    assert.equal(initial.url, '/pbgui/api/help/content?file=33_dashboard.md&lang=EN');
    assert.equal(initial.options.credentials, 'same-origin');
    initial.resolve({ok: true, json: async () => ({content: 'Initial guide'})});
    await tick();
    assert.equal(nodes['pbgui-shared-help-content'].innerHTML, '<p>Initial guide</p>');
    return {...http, content: nodes['pbgui-shared-help-content'],
        load: index => nodes['pbgui-shared-help-toc-list'].children[index].click()};
}

async function checkHelpFailures(createContext) {
    for (const scenario of ['network', 404, 500, 'json', 'success']) {
        const help = await createContext();
        help.load(0);
        assert.match(help.content.innerHTML, /Loading\.\.\./);
        assert.equal(help.requests.length, 1);
        const request = help.requests.shift();
        const failure = scenario === 'network' ? new TypeError('Failed to fetch') : new SyntaxError('Invalid JSON');
        let jsonRead = false;
        if (scenario === 'network') request.reject(failure);
        else request.resolve({
            ok: typeof scenario !== 'number', status: typeof scenario === 'number' ? scenario : 200,
            json: async () => {
                jsonRead = true;
                if (scenario === 'json') throw failure;
                return {content: 'Loaded guide'};
            }
        });
        await tick();
        if (scenario === 'success') {
            assert.equal(help.content.innerHTML, '<p>Loaded guide</p>');
            assert.equal(help.content.scrollTop, 0);
            assert.equal(help.errors.length, 0, 'successful content never logs an error');
        } else {
            assert.match(help.content.innerHTML, />Failed to load content\.<\/div>$/);
            assert.equal(help.errors.length, 1);
            assert.equal(help.errors[0][0], 'Error loading help topic:');
            if (typeof scenario === 'number') {
                assert.equal(help.errors[0][1].message, 'HTTP ' + scenario);
                assert.equal(jsonRead, false, 'HTTP errors are rejected before reading even valid JSON');
            } else assert.equal(help.errors[0][1], failure, 'the original error object is logged');
            help.load(0);
            help.requests.shift().resolve({ok: true, json: async () => ({content: 'Recovered guide'})});
            await tick();
            assert.equal(help.content.innerHTML, '<p>Recovered guide</p>');
            assert.equal(help.errors.length, 1, 'successful retry adds no error log');
        }
    }
}

async function helpTopicLegacy() {
    await checkHelpFailures(legacyHelpContext);
}

async function helpTopicShared() {
    await checkHelpFailures(sharedHelpContext);
}

async function helpTopicGenerations() {
    for (const stale of ['network', 404, 500, 'json', 'success', 'pending-json']) {
        const help = await sharedHelpContext();
        help.load(0);
        help.load(1);
        const [older, newer] = help.requests.splice(0);
        let finishJson;
        if (stale === 'pending-json') {
            older.resolve({ok: true, json: () => new Promise(resolve => { finishJson = resolve; })});
            await tick();
        }
        newer.resolve({ok: true, json: async () => ({content: 'Newest guide'})});
        await tick();
        assert.equal(help.content.innerHTML, '<p>Newest guide</p>');
        help.content.scrollTop = 123;
        if (stale === 'network') older.reject(new TypeError('Old network failure'));
        else if (stale === 'pending-json') finishJson({content: 'Old guide'});
        else older.resolve({
            ok: typeof stale !== 'number', status: stale,
            json: async () => {
                if (stale === 'json') throw new SyntaxError('Old invalid JSON');
                return {content: 'Old guide'};
            }
        });
        await tick();
        assert.equal(help.content.innerHTML, '<p>Newest guide</p>', 'stale results cannot replace current content');
        assert.equal(help.content.scrollTop, 123, 'stale results preserve the current reading position');
        assert.equal(help.errors.length, 0, 'superseded requests never log errors');
    }
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


async function pnl() {
    const mode = process.argv[3];
    const scenario = process.argv[4];
    const cumulative = mode === 'Cumulative' || mode === 'line';
    const invalid = [null, undefined, NaN, Infinity, -Infinity, '', '  ', true, false, 'bad', {}, [], 'Infinity', 'NaN'];
    const cases = {
        mixed: {raw: [5, ...invalid, ' 3.25 ', -2, '-1.5', 0, '0'],
                expected: [5, ...invalid.map(() => null), 3.25, -2, -1.5, 0, 0]},
        invalid: {raw: invalid, expected: invalid.map(() => null)},
        empty: {raw: [], expected: []},
        missing: {raw: [null, undefined, 8], expected: [null, null, 8]},
    };
    const {raw, expected} = cases[scenario];
    const bars = raw.map((value, index) => {
        const bar = {date: '2026-09-' + String(index + 1).padStart(2, '0'), income: value, cum: value};
        if (scenario === 'missing') {
            bar.income = 99; // Neither null nor missing cum may fall back to daily income.
            if (index === 1) delete bar.cum;
        }
        return Object.freeze(bar);
    });
    Object.freeze(bars);
    const data = Object.freeze({mode, bars, from_date: '2026-09-01', to_date: '2026-09-30'});
    const before = structuredClone(data);
    const wanted = scenario === 'missing' && !cumulative ? [99, 99, 99] : expected;
    const document = new Element();
    document.createElement = tag => new Element(tag);
    document.getElementById = () => new Element();
    const Plotly = {
        react(chart, traces, layout) { chart.data = traces; chart.layout = layout; },
        Plots: {resize() {}}
    };
    const window = {Plotly};
    const ctx = vm.createContext({window, document, Plotly, setTimeout() {}, clearTimeout() {}});
    vm.runInContext(source('dashboard_render.js'), ctx);
    const container = new Element();
    container.connected = true;
    const opts = {noResize: true, savedZoom: {xrange: ['2026-09-01', '2026-09-20'], yrange: [-3, 10]}};
    // Direct rendering also checks empty input; buildPnl presents an empty-state message.
    const direct = new Element();
    window.DashRender.renderPnl(direct, data, opts);
    const check = chart => {
        const trace = chart.data[0];
        assert.deepEqual([...trace.x], bars.map(bar => bar.date));
        assert.deepEqual([...trace.y], wanted);
        assert.deepEqual([...trace.marker.color], wanted.map(value => value === null
            ? 'rgba(0,0,0,0)' : value < 0 ? '#fc8181' : '#68d391'));
        if (cumulative) {
            assert.equal(trace.type, 'scatter');
            assert.equal(trace.connectgaps, false);
        } else {
            assert.equal(trace.type, 'bar');
            assert.deepEqual([...trace.text], wanted.map(value => value === null ? '' : value.toFixed(2)));
            assert.deepEqual([...trace.textposition], wanted.map(value => value === 0 ? 'outside' : 'auto'));
        }
        assert.deepEqual([...chart.layout.xaxis.range], opts.savedZoom.xrange);
        assert.deepEqual([...chart.layout.yaxis.range], opts.savedZoom.yrange);
    };
    check(direct);
    window.DashRender.buildPnl(container, data, opts);
    if (bars.length) {
        const chart = container.querySelector('.dt-chart');
        check(chart); // First build.
        const header = container.querySelector('.dt-header');
        window.DashRender.buildPnl(container, data, {noResize: true});
        assert.equal(container.querySelector('.dt-chart'), chart);
        assert.equal(container.querySelector('.dt-header'), header);
        check(chart); // Existing-chart fast path retains zoom and header.
    } else {
        assert.equal(container.querySelector('.dt-chart'), null);
        assert.equal(container.querySelector('.dt-nodata').textContent, 'No data for the selected period.');
    }
    assert.deepEqual(data, before, 'raw input remains unchanged, including missing properties and NaN');
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
        clearTimeout(id) { timers.delete(id); },
        setInterval() { return ++timerId; },
        clearInterval() {}
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
    const positions = new Element();
    positions.connected = true;
    window.DashRender.buildPositions(positions, {positions: [], source: 'db'}, {});
    assert.equal(positions.querySelector('.dt-nodata').textContent, 'No open positions.');
    const livePosition = {user: 'u', symbol: 'AAVEUSDC', side: 'long', size: 0.24,
        upnl: 0.29, entry: 164.25, price: 165.47, dca: 0, next_dca: 0,
        next_tp: 0, pos_value: 39.71};
    positions._dpUpdate([livePosition], 'live');
    assert.equal(positions.querySelector('.dt-nodata'), null);
    assert.equal(positions.querySelector('tbody').children.length, 1);
    assert.equal(positions.querySelector('tbody').children[0].children[1].textContent, 'AAVEUSDC');
    for (const name of fs.readdirSync(path.join(__dirname, '..', 'frontend')).filter(n => /^dashboard.*\.html$/.test(n))) {
        const html = source(name);
        if (!html.includes('/app/dashboard_render.js?v=')) continue;
        assert.equal(html.match(/var DR_VERSION = '([^']+)'/)[1], window.DashRender.VERSION, name);
    }
}

async function cards() {
    const html = source('dashboard_editor.html');
    let dirty = 0, synced = 0;
    const persistence = vm.createContext({
        VIEW_ONLY: true,
        markViewDirty() { dirty++; },
        scheduleSync() { synced++; }
    });
    vm.runInContext(functionCode(html, 'persistCardControlChange'), persistence);
    persistence.persistCardControlChange();
    persistence.VIEW_ONLY = false;
    persistence.persistCardControlChange();
    assert.equal(dirty, 1);
    assert.equal(synced, 1);

    const builders = [
        'buildBalanceInline', 'buildTopInline', 'buildIncomeInline', 'buildPnlInline',
        'buildAdgInline', 'buildPplInline', 'buildPositionsInline'
    ];
    for (const name of builders) {
        const code = functionCode(html, name);
        assert.ok(!code.includes('scheduleSync()'), name + ' controls must use mode-aware persistence');
        const document = new Element();
        document.createElement = tag => new Element(tag);
        const ctx = vm.createContext({
            document, state: {}, _buildGen: {}, API_BASE: '/api', encodeURIComponent,
            fetch() { return Promise.reject(new Error('offline')); },
            reportHeight() {}, persistCardControlChange() {}
        });
        vm.runInContext(code, ctx);

        const initial = new Element();
        ctx[name](initial, 1, 1);
        ctx[name](initial, 1, 1);
        assert.ok(initial.querySelector('.dashboard-loading'), name);
        await tick();
        await tick();
        assert.equal(initial.children.length, 1, name);
        assert.match(initial.children[0].textContent, /Data unavailable/, name);
        assert.equal(initial.querySelector('.dashboard-loading'), null, name);

        const rendered = new Element();
        const existing = rendered.appendChild(new Element());
        existing.className = 'rendered-card';
        ctx[name](rendered, 2, 1);
        await tick();
        await tick();
        assert.equal(rendered.children[0], existing, name + ' must preserve existing data on refresh failure');
    }
}


async function ppl() {
    const html = source('dashboard_editor.html');
    const allowed = ['DAY', 'WEEK', 'MONTH', 'YEAR', 'ALL_TIME'];
    const labels = {
        DAY: ['2023-12-31', '2024-01-01', '2024-06-01', '2025-01-01'],
        WEEK: ['2023-52', '2024-01', '2024-22', '2025-00'],
        MONTH: ['2023-12', '2024-01', '2024-06', '2025-01'],
        YEAR: ['2023', '2024', '2025'], ALL_TIME: ['ALL_TIME']
    };
    const key = 'dashboard_ppl_sum_period_1_1';
    for (const viewOnly of [false, true]) for (const savedSum of ['YEAR', 'ALL_TIME']) {
        let saved = {name: 'portfolio', rows: 1, cols: 1, [key]: savedSum,
            dashboard_ppl_users_1_1: ['alice'], dashboard_ppl_period_1_1: 'CUSTOM:2023-01-01:2025-12-31'};
        const requests = [], messages = [];
        let revision = 0;
        function open() {
            const document = new Element();
            document.createElement = tag => new Element(tag);
            document.createTextNode = value => Object.assign(new Element('text'), {textContent: value});
            document.getElementById = () => new Element();
            const window = {screen: {width: 1200, availHeight: 900},
                parent: {postMessage: message => messages.push(message)}};
            const Plotly = {
                react(chart, traces, layout) {chart.data = traces; chart.layout = layout;},
                Plots: {resize() {}}
            };
            window.Plotly = Plotly;
            const fetch = async (url, options = {}) => {
                if (url.startsWith('/api/dashboard/ppl_data')) {
                    const query = new URL(url, location.origin).searchParams;
                    requests.push(Object.fromEntries(query));
                    return {ok: true, json: async () => ({sum_period: query.get('sum_period'),
                        bars: labels[query.get('sum_period')].map(period => ({period, profits: 10 + revision, losses: -2}))})};
                }
                assert.equal(url, '/api/dashboards/portfolio');
                saved = JSON.parse(options.body);
                return {ok: true};
            };
            const ctx = vm.createContext({window, document, Plotly, location, encodeURIComponent,
                state: JSON.parse(JSON.stringify(saved)), _buildGen: {}, API_BASE: '/api',
                VIEW_ONLY: viewOnly, ORIG_NAME: 'portfolio', PERIODS_TOP: ['CUSTOM'],
                setTimeout() {}, clearTimeout() {}, setInterval() {}, clearInterval() {},
                fetch, apiFetch: (url, opts) => fetch('/api' + url, opts),
                _ensureRenderScript: cb => cb(), makeUsersDropdown: () => new Element(),
                _widgetIcon() {}, _makeDeleteCb() {}, _attachViewDrag() {},
                reportHeight() {}, scheduleSync() {}, setStatus() {}, closeAllUsersDropdowns() {}
            });
            vm.runInContext(source('dashboard_render.js'), ctx);
            ctx.DashRender = window.DashRender;
            vm.runInContext(html.match(/var PPL_SUM = \[[^;]+;/)[0], ctx);
            for (const name of ['persistCardControlChange', 'markViewDirty', 'buildPplInline', 'doSave', 'saveViewLayout']) {
                vm.runInContext(functionCode(html, name), ctx);
            }
            const container = new Element();
            container.connected = true;
            ctx.buildPplInline(container, 1, 1);
            return {ctx, container};
        }
        let {ctx, container} = open();
        await tick();
        function sumControl() {return container.querySelector('select');}
        function chart() {return container.querySelector('.dt-chart');}
        assert.deepEqual(sumControl().children.map(option => option.value), allowed);
        assert.equal(sumControl().children.find(option => option.selected).value, savedSum);
        for (const aggregation of allowed) {
            if (aggregation === 'YEAR') {
                chart().layout.xaxis.range = [1, 2];
                chart().layout.xaxis.autorange = false;
                chart().layout.yaxis.range = [-1, 5];
                chart().layout.yaxis.autorange = false;
            }
            const control = sumControl();
            control.value = aggregation;
            for (const handler of control.listeners.change) handler.call(control, {});
            await tick();
            assert.deepEqual(requests.at(-1), {users: 'alice', period: saved.dashboard_ppl_period_1_1, sum_period: aggregation});
            assert.deepEqual([...chart().data[0].x], labels[aggregation]);
            if (aggregation === 'ALL_TIME') {
                const range = chart().layout.xaxis.range;
                assert.ok(chart().layout.xaxis.autorange !== false || (range[0] <= 0 && range[1] >= 0),
                    'ALL_TIME must stay visible after changing aggregation from a zoomed chart');
                assert.ok(chart().layout.yaxis.range[1] > 10, 'totals must not be clipped by the old value zoom');
            }
        }
        for (const aggregation of ['YEAR', 'ALL_TIME']) {
            const control = sumControl();
            control.value = aggregation;
            for (const handler of control.listeners.change) handler.call(control, {});
            await tick();
            ctx[viewOnly ? 'saveViewLayout' : 'doSave']();
            await tick();
            assert.equal(saved[key], aggregation);
            ({ctx, container} = open());
            await tick();
            assert.equal(sumControl().children.find(option => option.selected).value, aggregation);
            revision++;
            const existingChart = chart();
            chart().layout.xaxis.range = [0, 0.3];
            chart().layout.xaxis.autorange = false;
            chart().layout.yaxis.range = [-4, 14];
            chart().layout.yaxis.autorange = false;
            // Run the same builder used by income_updated without discarding state.
            ctx.buildPplInline(container, 1, 1);
            await tick();
            assert.equal(chart(), existingChart, 'live updates must reuse the existing chart');
            assert.deepEqual([...chart().layout.xaxis.range], [0, 0.3]);
            assert.deepEqual([...chart().layout.yaxis.range], [-4, 14]);
            assert.equal(chart().data[0].y[0], 10 + revision);
            assert.equal(ctx.state[key], aggregation);
            assert.equal(requests.at(-1).sum_period, aggregation);
        }
        assert.ok(messages.some(message => message.type === (viewOnly ? 'pbgui_view_saved' : 'pbgui_editor_saved')));
    }
    // The legacy template has no runtime loader, but must retain supported values.
    const legacy = source('dashboard_ppl.html');
    const normalization = legacy.slice(legacy.indexOf('    var PPL_SUM'), legacy.indexOf('\n', legacy.indexOf('    var currentSumPeriod')));
    for (const value of [...allowed, 'invalid']) {
        const ctx = vm.createContext({SUM_PERIOD: value});
        vm.runInContext(normalization, ctx);
        assert.equal(ctx.currentSumPeriod, value === 'invalid' ? 'DAY' : value);
    }
}


async function emptyOrders() {
    const position = {user: 'u', symbol: 'AAVEUSDC', side: 'long'};
    const responses = [[], [{t: 1, o: 1, h: 1, l: 1, c: 1, v: 1}]];
    async function fetchOrders() {
        const candles = responses.shift();
        assert.ok(candles, 'unexpected Orders fetch');
        return {ok: true, json: async () => ({candles})};
    }
    const timers = new Map();
    let nextTimer = 0;
    const setTimeoutFake = fn => { const id = ++nextTimer; timers.set(id, fn); return id; };
    const clearTimeoutFake = id => timers.delete(id);
    const runRetry = () => {
        assert.equal(timers.size, 1, 'empty Orders must schedule one retry');
        const [id, fn] = [...timers][0];
        timers.delete(id);
        fn();
    };
    let charts = 0;
    const DashRender = {buildOrders(_container, data) {
        if (data && data.candles && data.candles.length) { charts++; return {destroy() {}}; }
        return null;
    }};
    const standalone = vm.createContext({
        window: {}, container: new Element(), selectedPosition: position,
        chartCtrl: null, loadSeq: 0, _tfFetchId: 0, currentTimeframe: '4h',
        emptyRetryTimer: null, _emptyTimerKey: 'empty',
        isCurrentGeneration: () => true, reportHeight() {}, connectCandleWs() {},
        _tfLimit: () => 500, API_BASE: '/api', POSITION: '1_1',
        encodeURIComponent, fetch: fetchOrders, DashRender,
        setTimeout: setTimeoutFake, clearTimeout: clearTimeoutFake
    });
    standalone.container.connected = true;
    vm.runInContext(functionCode(source('dashboard_orders.html'), 'load'), standalone);
    standalone.load();
    await tick(); await tick();
    runRetry();
    await tick(); await tick();
    assert.equal(charts, 1, 'standalone Orders must render candles after an empty first response');

    responses.push([], [{t: 2, o: 2, h: 2, l: 2, c: 2, v: 2}]);
    const document = new Element();
    const container = new Element();
    container.connected = true;
    const inline = vm.createContext({
        window: {}, document, state: {dashboard_orders_1_1: 'view_orders_1_2'},
        API_BASE: '/api', encodeURIComponent, fetch: fetchOrders, DashRender,
        _ensureRenderScript: fn => fn(), _widgetIcon: () => '',
        _makeDeleteCb: () => () => {}, _attachViewDrag() {}, reportHeight() {},
        setTimeout: setTimeoutFake, clearTimeout: clearTimeoutFake
    });
    vm.runInContext(functionCode(source('dashboard_editor.html'), 'buildOrdersInline'), inline);
    inline.buildOrdersInline(container, 1, 1);
    document.dispatch('dash-pos-selected', {detail: {pos: '1_2', data: position}});
    await tick(); await tick();
    runRetry();
    await tick(); await tick();
    assert.equal(charts, 2, 'inline Orders must render candles after an empty first response');
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
            VIEW_ONLY: true, cancelling: false, syncInFlight: Promise.resolve(), syncTimer: null, clearTimeout, closeAllUsersDropdowns() {}, setStatus() {},
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
        assert.deepEqual(saves.map(s => s.url), ['/dashboards/saved', '/dashboards/old', '/dashboard/pending_full?name=old']);
        assert.deepEqual(saves.map(s => s.opts.method), ['POST', 'POST', 'DELETE']);
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
    let responseData = {};
    const document = {head: new Element(), createElement: tag => new Element(tag)};
    const ctx = vm.createContext({window: {location}, document, location,
        DR_VERSION: 'test', _reconnTimer: null, clearTimeout() {},
        WebSocket: class { constructor(url) { sockets.push(url); } },
        fetch(url) { requests.push(url); return Promise.resolve({ok: true, json: async () => responseData}); }
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
        const renders = [];
        Object.assign(ctx, {
            listGeneration: 0, DASHBOARDS: [],
            pruneSelectedDashboards() { renders.push('selection'); },
            renderList() { renders.push('list'); },
            renderToolbar() { renders.push('toolbar'); }
        });
        vm.runInContext(functionCode(html, 'refreshList'), ctx);
        for (const names of [['Zulu', 'alpha', 'middle'], []]) {
            responseData = {dashboards: names};
            requests.length = 0;
            renders.length = 0;
            let loaded = false;
            ctx.refreshList(() => { loaded = true; });
            await tick();
            assert.equal(loaded, true, 'the complete list request chain must reach its callback');
            assert.deepEqual(requests, [prefix + '/api/dashboards']);
            assert.deepEqual([...ctx.DASHBOARDS], names.length ? ['alpha', 'middle', 'Zulu'] : []);
            assert.deepEqual(renders, ['selection', 'list', 'toolbar']);
        }
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

async function candles() {
    const document = new Element();
    document.createElement = tag => tag === 'canvas' ? {getContext: () => ({measureText: text => ({width: text.length * 6})})} : new Element(tag);
    document.getElementById = () => new Element();
    document.createTextNode = text => Object.assign(new Element(), {textContent: text});
    const timers = new Map(), frames = new Map(), requests = [], candleRows = [], volumeRows = [];
    const rangeListeners = new Set(), callbacks = [];
    let timerId = 0, removed = false, fitCount = 0, rangeWrites = 0, widthReads = 0;
    let plotWidth = 1000, resizeCallback, disconnected = false;
    let visible = {from: 0.25, to: 2.5};
    let restored, chartOptions;
    const emit = () => { const range = {...visible}; callbacks.push(() => rangeListeners.forEach(fn => fn(range))); };
    const flush = () => { while (callbacks.length) callbacks.shift()(); };
    const scale = {logicalToCoordinate(i) {return i * plotWidth / (visible.to - visible.from + 1);}, width() {widthReads++; return plotWidth;}, options() {return {minBarSpacing: 0.5};}, fitContent() {fitCount++; visible = {from: 0, to: candleRows.length - 1}; emit();},
        getVisibleLogicalRange() {return {...visible};},
        setVisibleLogicalRange(range) {rangeWrites++; restored = {...range}; visible = {...range}; emit();},
        subscribeVisibleLogicalRangeChange(fn) {rangeListeners.add(fn);},
        unsubscribeVisibleLogicalRangeChange(fn) {rangeListeners.delete(fn);}};
    const line = {applyOptions() {}};
    const price = {setData(rows) {candleRows.splice(0, candleRows.length, ...rows);},
        update(row) {const i = candleRows.findIndex(c => c.time === row.time);
            if (i < 0) candleRows.push(row); else candleRows[i] = row;},
        applyOptions() {}, createPriceLine() {return line;}};
    const volume = {setData(rows) {volumeRows.splice(0, volumeRows.length, ...rows);},
        update(row) {const i = volumeRows.findIndex(c => c.time === row.time);
            if (i < 0) volumeRows.push(row); else volumeRows[i] = row;}};
    const chart = {addCandlestickSeries() {return price;}, addHistogramSeries() {return volume;},
        timeScale() {return scale;}, priceScale() {return {applyOptions() {}};},
        options() {return {layout: {fontSize: 12, fontFamily: 'Arial'}};},
        applyOptions() {}, remove() {removed = true;}};
    const window = {LightweightCharts: {createChart(div, options) {chartOptions = options; return chart;}, CrosshairMode: {Normal: 0},
        LineStyle: {Solid: 0, Dotted: 1, Dashed: 2}}};
    const ctx = vm.createContext({window, document, AbortController, console, encodeURIComponent,
        ResizeObserver: class {
            constructor(fn) {resizeCallback = fn;}
            observe() {}
            disconnect() {disconnected = true;}
        },
        requestAnimationFrame(fn) {frames.set(++timerId, fn); return timerId;},
        cancelAnimationFrame(id) {frames.delete(id);},
        setInterval() {return 1;}, clearInterval() {},
        setTimeout(fn, delay) {timers.set(++timerId, {fn, delay}); return timerId;},
        clearTimeout(id) {timers.delete(id);},
        fetch(url, options) {return new Promise(resolve => requests.push({url, options, resolve}));}
    });
    vm.runInContext(source('dashboard_render.js'), ctx);
    const container = new Element(); container.connected = true;
    const row = (t, close, vol) => ({t, o: 5, h: 6, l: 4, c: close, v: vol});
    const ctrl = window.DashRender.renderOrders(container, {
        user: 'alice', symbol: 'NEARUSDC', candles: [row(300000, 5, 10), row(600000, 5, 20)]
    }, {apiBase: '/pbgui/api', timeframe: '5m', onLoadMore(t, done) {loadCount++; done();}});
    let loadCount = 0;
    assert.equal(chartOptions.timeScale.lockVisibleTimeRangeOnResize, true);
    visible = {from: 0.25, to: 2.5}; emit(); flush();
    // Early input cancels both layout passes before any snapshot or frame runs.
    container.dispatch('wheel');
    assert.equal(frames.size, 0);
    assert.equal([...timers.values()].filter(t => t.delay === 200).length, 0);
    const runReconcile = delay => {
        const match = [...timers].find(([, timer]) => timer.delay === delay);
        assert.ok(match, 'automatic candle reconciliation scheduled');
        timers.delete(match[0]); match[1].fn();
    };
    runReconcile(0);
    assert.ok(requests[0].url.startsWith('/pbgui/api/dashboard/candles_data?'));
    assert.ok(requests[0].url.includes('timeframe=5m'));
    ctrl.updateCandle([600000, 5, 7, 4, 6, 25]);
    ctrl.updateCandle([900000, 6, 8, 5, 7, 30]);
    const beforePrepend = {...visible};
    ctrl.prependData([row(0, 4, 3)]);
    assert.deepEqual(visible, {from: beforePrepend.from + 1, to: beforePrepend.to + 1});
    assert.equal(candleRows.at(-1).close, 7, 'history loading must retain appended live candle');
    assert.equal(volumeRows.at(-1).value, 30, 'history loading must retain live volume');
    requests[0].resolve({ok: true, json: async () => ({candles: [row(300000, 5.2, 12), row(600000, 5.1, 21)]})});
    await tick();
    assert.equal(candleRows.find(c => c.time === 300).close, 5.2, 'missed final closed value repaired');
    assert.equal(volumeRows.find(c => c.time === 300).value, 12);
    assert.equal(candleRows.find(c => c.time === 600).close, 6, 'in-flight snapshot cannot undo newer WS update');
    assert.equal(candleRows[0].time, 0, 'loaded older history retained');
    flush();
    assert.deepEqual(restored, {from: 1.25, to: 3.5}, 'fractional logical range retained with latest candle still visible');
    assert.equal(loadCount, 0, 'deferred programmatic callbacks cannot trigger history loops');
    assert.equal(fitCount, 0, 'reconciliation must not fit/reset zoom');
    // Repeated/overlapping history and an inserted missing candle use a time anchor.
    ctrl.prependData([row(0, 99, 3), row(150000, 4, 3)]);
    flush();
    assert.deepEqual(visible, {from: 2.25, to: 4.5});
    ctrl.prependData([row(0, 99, 3), row(150000, 4, 3)]);
    flush();
    assert.deepEqual(visible, {from: 2.25, to: 4.5}, 'overlap cannot shift range twice');
    assert.equal(candleRows[0].close, 4, 'history must not overwrite existing candles');
    runReconcile(30000);
    const oldRequest = requests[1];
    ctrl.setData([row(1800000, 8, 50)], '30m');
    assert.equal(oldRequest.options.signal.aborted, true);
    oldRequest.resolve({ok: true, json: async () => ({candles: [row(300000, 99, 999)]})});
    await tick();
    assert.deepEqual(candleRows.map(c => c.time), [1800], 'old timeframe response discarded');
    assert.equal(visible.to, 8, 'timeframe replacement starts with eight empty slots');
    const span = visible.to - visible.from;
    ctrl.updateCandle([3600000, 8, 9, 7, 8.5, 60]);
    flush();
    assert.equal(visible.to, 9, 'latest view follows live append');
    assert.equal(visible.to - visible.from, span);
    // Merely seeing the last candle does not mean following it.
    visible = {from: 0.125, to: 4.75}; emit(); flush();
    ctrl.updateCandle([5400000, 8, 9, 7, 8.5, 60]); flush();
    assert.deepEqual(visible, {from: 0.125, to: 4.75});
    visible = {from: candleRows.length - 1 + 8 + 0.25 - 80, to: candleRows.length - 1 + 8 + 0.25}; emit(); flush();
    const followSpan = visible.to - visible.from;
    runReconcile(0);
    assert.ok(requests[2].url.includes('timeframe=30m'));
    requests[2].resolve({ok: true, json: async () => ({candles: [row(7200000, 9, 70)]})});
    await tick(); flush();
    assert.equal(visible.to, candleRows.length - 1 + 8, 'return within six pixels resumes following snapshots');
    assert.equal(visible.to - visible.from, followSpan);
    runReconcile(30000);
    ctrl.destroy();
    assert.equal(requests[3].options.signal.aborted, true);
    assert.equal(rangeListeners.size, 0);
    assert.ok(disconnected);
    assert.equal(container.count('wheel'), 0);
    assert.equal(frames.size, 0);
    assert.equal(document.count('visibilitychange'), 0);
    assert.ok(removed);
    assert.equal([...timers].filter(([, t]) => [0, 30000].includes(t.delay)).length, 0);
    requests[3].resolve({ok: true, json: async () => ({candles: [row(1800000, 99, 999)]})});
    await tick();
    assert.equal(candleRows.at(-1).close, 9, 'destroyed chart ignores pending snapshot');

    // Real buildOrders wrapper owns fullscreen timers and cancels them before destroy.
    removed = false;
    const built = window.DashRender.buildOrders(container, {candles: [row(300000, 5, 10)]}, {});
    for (const [id, fn] of [...frames]) {frames.delete(id); fn();}
    runReconcile(200); flush();
    assert.equal(visible.to, 8, 'initial fit explicitly restores margin');
    const fitBeforeFullscreen = fitCount;
    document.dispatch('fullscreenchange');
    document.dispatch('webkitfullscreenchange');
    assert.equal([...timers.values()].filter(t => t.delay === 150).length, 1);
    visible = {from: -0.25, to: 3.5}; emit(); flush();
    runReconcile(150);
    for (const [id, fn] of [...frames]) {frames.delete(id); fn();}
    flush();
    assert.deepEqual(visible, {from: -0.25, to: 3.5}, 'intervening gesture survives delayed fullscreen resize');
    assert.equal(fitCount, fitBeforeFullscreen);
    document.dispatch('fullscreenchange');
    built.destroy();
    assert.equal([...timers.values()].filter(t => [150, 200].includes(t.delay)).length, 0);
    assert.equal(document.count('fullscreenchange'), 0);
    assert.equal(document.count('webkitfullscreenchange'), 0);
    assert.equal(rangeListeners.size, 0);
    assert.equal(frames.size, 0);
    // Density, public-coordinate conversion, bounded refinement and resize ownership.
    plotWidth = 1128;
    const denseRows = Array.from({length: 500}, (_, i) => row(i * 14400000, 81926, 10));
    const dense = window.DashRender.renderOrders(container, {candles: denseRows,
        position: {entry: 84183, side: 'long'}}, {timeframe: '4h'});
    const drainFrames = () => {
        for (let n = 0; frames.size && n < 6; n++) {
            for (const [id, fn] of [...frames]) {frames.delete(id); fn();}
            flush();
        }
        assert.equal(frames.size, 0, 'layout work is bounded');
    };
    let initialWrites = rangeWrites;
    drainFrames();
    runReconcile(200); drainFrames();
    assert.ok(rangeWrites - initialWrites <= 3, 'at most three initial corrections including delayed timer');
    const wide = {...visible};
    const clearance = () => (visible.to - 499) * plotWidth / (visible.to - visible.from + 1);
    assert.ok(clearance() >= 64, 'dense history reserves label pixels rather than just eight slots');
    const wideSpan = wide.to - wide.from;
    plotWidth = 668; resizeCallback(); drainFrames();
    assert.ok(Math.abs(visible.to - visible.from - wideSpan) < 1e-8, 'following resize retains feasible zoom span');
    assert.ok(clearance() >= 64);
    plotWidth = 188; resizeCallback(); drainFrames();
    assert.ok(visible.to - visible.from <= plotWidth / 0.5 - 1, 'minimum spacing limits narrow fitted history');
    assert.ok(visible.from > 0, 'narrow chart shows less history instead of covering the latest candle');
    assert.ok(clearance() >= 64);
    // A tight explicit zoom can follow with fewer than eight slots; its span is retained.
    visible = {from: 498, to: 502}; emit(); flush();
    dense.preserveViewport(); drainFrames();
    assert.equal(visible.to - visible.from, 4);
    assert.ok(visible.to - 499 < 8);
    dense.updateCandle([500 * 14400000, 81900, 82000, 81800, 81926, 10]); flush();
    assert.equal(visible.to, 503, 'tight zoom keeps following with a feasible margin below eight');
    // Resize work queued before a gesture must not restore the previous following view.
    const manual = {from: 30.25, to: 100.75};
    plotWidth = 1128; resizeCallback();
    container.dispatch('pointerdown');
    visible = {...manual}; emit(); flush();
    drainFrames();
    assert.deepEqual(visible, manual);
    dense.prependData([row(-14400000, 81926, 10)]); flush();
    assert.deepEqual(visible, {from: 31.25, to: 101.75});
    dense.preserveViewport();
    assert.equal(frames.size, 1);
    dense.destroy();
    assert.equal(frames.size, 0, 'destroy cancels pending layout work');
    const readsBeforeLateResize = widthReads;
    plotWidth = 188; resizeCallback();
    assert.equal(widthReads, readsBeforeLateResize, 'dequeued observer callback cannot access a destroyed chart');
    assert.equal(rangeListeners.size, 0);
    const pendingInitial = window.DashRender.renderOrders(container, {candles: [row(0, 1, 1)]}, {});
    assert.equal(frames.size, 1);
    pendingInitial.destroy();
    assert.equal(frames.size, 0);
    assert.equal([...timers.values()].filter(t => t.delay === 200).length, 0);
}

async function manageModal() {
    const document = new Element();
    document.body = new Element('body');
    document.body.document = document;
    document.body.connected = true;
    document.head = new Element('head');
    document.createElement = tag => new Element(tag);
    document.getElementById = id => document.body.querySelector('#' + id) || document.head.querySelector('#' + id);
    const window = {innerWidth: 1280, innerHeight: 900,
        getComputedStyle: el => ({visibility: el.style.visibility || 'visible'})};
    const timers = new Map();
    const requests = [];
    let timerId = 0;
    const ctx = vm.createContext({window, document, console, encodeURIComponent,
        setInterval(fn) { timers.set(++timerId, fn); return timerId; },
        clearInterval(id) { timers.delete(id); }, setTimeout() { throw Error('Unexpected timer'); },
        fetch(url, options) {
            const body = JSON.parse(options.body);
            assert.equal(url, '/api/dashboard/positions/manage');
            assert.equal(body.dry_run, true, 'only isolated preview requests are permitted');
            return new Promise(resolve => requests.push(() => resolve({ok: true,
                json: async () => ({dry_run: true, config: {bot: {long: {}}}})})));
        }
    });
    vm.runInContext(source('dashboard_render.js'), ctx);
    const container = document.body.appendChild(new Element());
    const position = {user: 'test-user', symbol: 'BTCUSDT', side: 'long', size: 1,
        upnl: 2, entry: 100, price: 102, dca: 0, next_dca: 0, next_tp: 0, pos_value: 102};
    const build = data => window.DashRender.buildPositions(container, data, {apiBase: '/api'});
    build({positions: [{...position}]});
    let trigger = container.querySelector('.dp-manage-btn');
    const current = () => document.getElementById('dp-manage-modal');
    const key = (key, extra = {}) => document.dispatch('keydown', {key, target: document.activeElement, ...extra});
    const button = (overlay, label) => overlay.querySelectorAll('button').find(el => el.textContent === label);
    const open = () => { trigger.focus(); trigger.dispatch('click'); return current(); };
    const clean = () => {
        assert.equal(current(), null);
        for (const type of ['keydown', 'mousemove', 'mouseup']) assert.equal(document.count(type), 0, type + ' leaked');
        assert.equal(container._dpCloseManage, undefined);
    };
    for (const action of ['Escape', 'Close', 'cross']) {
        const overlay = open();
        const modal = overlay.querySelector('.dp-modal');
        assert.equal(modal.getAttribute('role'), 'dialog');
        assert.equal(modal.getAttribute('aria-modal'), 'true');
        assert.equal(document.getElementById(modal.getAttribute('aria-labelledby')).textContent, 'Manage positions');
        assert.equal(document.activeElement, overlay.querySelector('.dp-modal-close'));
        overlay.dispatch('click', {target: overlay});
        assert.equal(current(), overlay, 'backdrop is deliberately inert');
        key('Escape', {defaultPrevented: true});
        assert.equal(current(), overlay);
        if (action === 'Escape') key('Escape');
        else if (action === 'Close') button(overlay, 'Close').dispatch('click');
        else overlay.querySelector('.dp-modal-close').dispatch('click');
        clean();
        assert.equal(document.activeElement, trigger);
    }
    let overlay = open();
    const first = overlay.querySelector('.dp-modal-close');
    const last = button(overlay, 'Close');
    key('Tab', {shiftKey: true});
    assert.equal(document.activeElement, last);
    key('Tab');
    assert.equal(document.activeElement, first);
    const focusable = overlay.querySelectorAll('button, input, select, textarea').filter(el =>
        !el.disabled && el.getClientRects().length && el.style.visibility !== 'hidden');
    for (let i = 1; i <= focusable.length; i++) {
        key('Tab');
        assert.equal(document.activeElement, focusable[i % focusable.length], 'Tab skips disabled controls');
    }
    const amount = overlay.querySelector('.dp-manage-amount');
    amount.focus(); amount.value = '0.125'; amount.dispatch('input');
    build({positions: [{...position, price: 103}]});
    assert.equal(current(), overlay);
    assert.equal(document.activeElement, amount);
    assert.equal(amount.value, '0.125', 'live updates preserve an amount draft');
    first.focus();
    const staleSelect = overlay.querySelector('select');
    build({positions: [{...position, symbol: 'ETHUSDT'}]});
    assert.equal(staleSelect.isConnected, false);
    key('Tab');
    assert.equal(document.activeElement, overlay.querySelector('select'), 'Tab discovers replacement controls');
    key('Tab');
    assert.equal(document.activeElement, overlay.querySelector('.dp-manage-amount'));

    // Exercise real shared confirmation key handling in both registration orders.
    key('Escape'); clean();
    const confirmCode = source('js/pbgui_dialogs.js').match(/    document.addEventListener\('keydown', function \(event\) \{[\s\S]*?\n    \}\);/)[0];
    for (const order of ['before', 'after']) {
        const upper = document.body.appendChild(new Element());
        upper.id = 'pbgui-dialog-ovl';
        upper.className = 'visible';
        ctx.overlay = upper;
        ctx.currentMode = 'confirm';
        ctx.close = () => { upper.classList.remove('visible'); trigger.focus(); };
        if (order === 'before') vm.runInContext(confirmCode, ctx);
        overlay = open();
        if (order === 'after') vm.runInContext(confirmCode, ctx);
        key('Escape');
        assert.equal(upper.classList.contains('visible'), false);
        assert.equal(current(), overlay, 'one Escape closes only the confirmation');
        document.listeners.keydown.clear();
        document.body.removeChild(upper);
        key('Escape'); clean();
    }
    for (const id of ['pbgui-shared-help-ovl', 'pbgui-confirm-ovl']) {
        const upper = document.body.appendChild(new Element());
        upper.id = id; upper.className = 'visible';
        // Like Guide/navigation, this older handler need not preventDefault.
        const dismiss = () => upper.classList.remove('visible');
        document.addEventListener('keydown', dismiss);
        overlay = open(); key('Escape');
        assert.equal(current(), overlay);
        document.removeEventListener('keydown', dismiss);
        document.body.removeChild(upper);
        key('Escape'); clean();
    }
    // The real Dashboard shell owns these overlays in the parent document.
    const parentDocument = new Element();
    parentDocument.body = new Element('body');
    parentDocument.body.document = parentDocument;
    parentDocument.body.connected = true;
    parentDocument.getElementById = id => parentDocument.body.querySelector('#' + id);
    const parentWindow = {document: parentDocument};
    parentWindow.parent = parentWindow;
    window.parent = parentWindow;
    const parentOverlays = [
        ['pbgui-dialog-ovl', 'pbgui-dialog-close'],
        ['pbgui-confirm-ovl', 'pbgui-confirm-cancel'],
        ['pbgui-alert-ovl', 'pbgui-alert-close'],
        ['pbgui-about-ovl', 'pbgui-about-close'],
        ['pbgui-shared-help-ovl', 'pbgui-shared-help-close']
    ].map(([id, closeId]) => {
        const upper = parentDocument.body.appendChild(new Element());
        upper.id = id;
        const close = upper.appendChild(new Element('button'));
        close.id = closeId;
        close.addEventListener('click', () => upper.classList.remove('visible'));
        return {upper, close};
    });
    for (const {upper, close} of parentOverlays) {
        overlay = open();
        upper.classList.add('visible');
        key('Escape', {defaultPrevented: true});
        assert.ok(upper.classList.contains('visible'));
        key('Tab');
        assert.equal(parentDocument.activeElement, close, 'Tab yields focus to the ancestor overlay');
        overlay.querySelector('.dp-modal-close').focus();
        key('Escape');
        assert.equal(upper.classList.contains('visible'), false);
        assert.equal(current(), overlay, 'ancestor Escape never closes Manage');
        key('Escape'); clean();
    }
    overlay = open();
    parentOverlays[0].upper.classList.add('visible');
    parentOverlays[4].upper.classList.add('visible');
    key('Escape');
    assert.equal(parentOverlays[0].upper.classList.contains('visible'), false);
    assert.equal(parentOverlays[4].upper.classList.contains('visible'), true, 'only one ancestor layer closes');
    key('Escape');
    assert.equal(current(), overlay);
    key('Escape'); clean();

    // Do not cross a browser origin boundary or let it break local dismissal.
    window.parent = {get document() { throw Object.assign(new Error('Cross-origin'), {name: 'SecurityError'}); }};
    open(); key('Escape'); clean();
    window.parent = parentWindow;
    overlay = open();
    const previewTrigger = button(overlay, 'Preview Panic');
    previewTrigger.focus(); previewTrigger.dispatch('click');
    requests.shift()(); await tick();
    const preview = document.getElementById('dp-preview-modal');
    assert.ok(preview);
    preview.dispatch('click'); assert.ok(preview.isConnected);
    parentOverlays[4].upper.classList.add('visible');
    key('Escape');
    assert.equal(parentOverlays[4].upper.classList.contains('visible'), false);
    assert.equal(document.getElementById('dp-preview-modal'), preview, 'ancestor dismissal also preserves the preview');
    assert.equal(current(), overlay);
    key('Tab', {shiftKey: true});
    assert.equal(document.activeElement, button(preview, 'Close'));
    key('Escape');
    assert.equal(document.getElementById('dp-preview-modal'), null);
    assert.equal(current(), overlay);
    assert.equal(document.activeElement, previewTrigger);
    key('Escape'); clean();

    for (const action of ['Close', 'cross', 'replaceManage']) {
        overlay = open();
        const previewButton = button(overlay, 'Preview Panic');
        previewButton.focus(); previewButton.dispatch('click');
        requests.shift()(); await tick();
        const child = document.getElementById('dp-preview-modal');
        assert.equal(child.querySelector('.dp-preview-modal').getAttribute('role'), 'dialog');
        if (action === 'Close') button(child, 'Close').dispatch('click');
        else if (action === 'cross') child.querySelector('.dp-modal-close').dispatch('click');
        else trigger.dispatch('click');
        assert.equal(document.getElementById('dp-preview-modal'), null);
        assert.equal(document.count('keydown'), 1, 'preview listeners are disposed');
        if (action !== 'replaceManage') assert.equal(document.activeElement, previewButton);
        key('Escape'); clean();
    }

    overlay = open();
    const oldClose = container._dpCloseManage;
    trigger.dispatch('click');
    assert.notEqual(current(), overlay);
    oldClose();
    assert.ok(current(), 'old cleanup cannot erase the replacement');
    assert.equal(document.count('keydown'), 1);
    assert.equal(document.count('mousemove'), 2);
    assert.equal(document.count('mouseup'), 2);
    key('Escape'); clean();

    overlay = open();
    const head = overlay.querySelector('.dp-modal-head');
    head.dispatch('mousedown', {clientX: 100, clientY: 100});
    document.dispatch('mousemove', {clientX: 130, clientY: 140});
    document.dispatch('mouseup');
    const modal = overlay.querySelector('.dp-modal');
    const left = modal.style.left;
    overlay.querySelector('.dp-resize-se').dispatch('mousedown', {clientX: 500, clientY: 500});
    document.dispatch('mousemove', {clientX: 520, clientY: 520});
    document.dispatch('mouseup');
    const width = modal.style.width;
    build({positions: [{...position}]});
    assert.equal(modal.style.left, left);
    assert.equal(modal.style.width, width);
    build(null); // The slow rebuild path must dispose the body-owned dialog.
    clean();
    build({positions: [{...position}]}); // Empty-to-populated rebuild.
    trigger = container.querySelector('.dp-manage-btn');
    overlay = open();
    container._dpUpdate([], 'live');
    assert.equal(current(), overlay);
    assert.match(overlay.querySelector('tbody').children[0].children[0].textContent, /No open positions/);
    key('Escape'); clean();
    container._dpUpdate([{...position}], 'live');
    overlay = open();
    button(overlay, 'Preview Panic').dispatch('click');
    document.body.removeChild(container);
    const other = document.body.appendChild(new Element('button')); other.focus();
    [...timers.values()].forEach(fn => fn());
    clean();
    assert.equal(document.activeElement, other, 'widget removal never steals focus');
    requests.shift()(); await tick();
    assert.equal(document.getElementById('dp-preview-modal'), null, 'late previews cannot resurrect a removed widget');
    assert.equal(timers.size, 0);
}

({creation, templates, cancel, resize, charts, cards, pnl, ppl, emptyOrders, messages, assets, candles, page, manageModal,
    helpTopicLegacy, helpTopicShared, helpTopicGenerations})[process.argv[2]]().catch(error => {
    console.error(error);
    process.exitCode = 1;
});
