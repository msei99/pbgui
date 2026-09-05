/* Run the complete shared nav in a VM with inert DOM, network and timers. */
'use strict';
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const source = fs.readFileSync(path.join(__dirname, '../frontend/pbgui_nav.js'), 'utf8');
const prefixArg = process.argv[2];
const prefix = prefixArg === '<absent>' ? '' : prefixArg;
const origin = process.argv[3];
const flush = () => new Promise(resolve => setImmediate(resolve));

function browser(apiBase = prefix + '/api/balance-calc', mount = prefixArg) {
  const requests = [], assets = [], streams = [], redirects = [], assigned = [];
  const timeouts = [], intervals = [], listeners = {}, documentListeners = {}, nodes = {};
  let navItems = [];
  const stored = new Map();
  function element(id = '') {
    const handlers = {}, attributes = {}, classes = new Set();
    return {
      id, style: {}, dataset: {}, handlers, innerHTML: '',
      classList: {
        contains: name => classes.has(name),
        add: name => classes.add(name), remove: name => classes.delete(name),
        toggle(name, value) { if (value) classes.add(name); else classes.delete(name); }
      },
      setAttribute: (name, value) => { attributes[name] = value; },
      getAttribute: name => attributes[name] || '',
      addEventListener(name, handler) { (handlers[name] ||= []).push(handler); },
      click() { for (const handler of handlers.click || []) handler({preventDefault() {}, isTrusted: true}); },
      querySelectorAll: () => [], querySelector: () => null, remove() {}, focus() {},
      appendChild(child) { if (child && child.id) nodes[child.id] = child; }
    };
  }
  for (const id of ['topnav', 'nav-logo', 'pbgui-guide-btn', 'pbgui-ai-btn', 'pbgui-logout-btn',
    'pbgui-notify-btn', 'pbgui-notify-panel', 'pbgui-alert-ovl', 'pbgui-alert-open-monitor',
    'pbgui-about-ovl', 'pbgui-confirm-ovl', 'pbgui-restart-btn', 'pbgui-confirm-title',
    'pbgui-confirm-msg', 'pbgui-confirm-detail', 'pbgui-confirm-cancel', 'pbgui-confirm-accept']) nodes[id] = element(id);
  const location = new URL(origin + prefix + '/api/balance-calc/main_page');
  location.assign = value => assigned.push(value);
  location.replace = value => redirects.push(value);
  location.reload = () => {};
  const context = {
    URL, console, WeakMap, Map, Set,
    location, API_BASE: apiBase,
    PBGUI_NAV_CONFIG: {current: 'info_balance_calc', authenticated: true},
    sessionStorage: {getItem: key => stored.get(key), setItem: (key, value) => stored.set(key, value), removeItem: key => stored.delete(key)},
    addEventListener(name, handler) { (listeners[name] ||= []).push(handler); },
    setInterval(fn, delay) { intervals.push({fn, delay}); return intervals.length; },
    clearInterval() {},
    setTimeout(fn, delay) { timeouts.push({fn, delay}); return timeouts.length; },
    clearTimeout() {},
    fetch(input, options) {
      requests.push({input, options});
      return Promise.resolve({status: context.responseStatus || 200, ok: !context.responseStatus, json: async () => ({})});
    },
    EventSource: class {
      constructor(url, options) { this.url = url; this.options = options; streams.push(this); }
      close() { this.closed = true; }
    },
    document: {
      readyState: 'loading', body: element(), head: {appendChild: item => assets.push(item)},
      createElement: () => element(), getElementById: id => nodes[id] || null,
      querySelectorAll: selector => selector === '.nav-item[data-page]' ? navItems : [],
      addEventListener(name, handler) { documentListeners[name] = handler; }
    }
  };
  context.window = context;
  context.top = {location: {replace: value => redirects.push(value)}};
  if (mount !== '<absent>') context.PBGUI_BASE_PREFIX = mount;
  vm.createContext(context);
  // Test-only bridge; production code has no testing exports or alternate logic.
  const bridge = `window.testNav = {_getApiOrigin, _getAppBase, _getWsBase, _appPath,
    tokenRefreshUrl, redirectToLogin, confirmTokenStillValid, ackAlert, ackAllAlerts,
    renderAlertOverlay, FASTAPI_PAGES};`;
  vm.runInContext(source.replace(/\}\(\)\);\s*$/, bridge + '\n}());'), context);
  navItems = Object.keys(context.testNav.FASTAPI_PAGES).map(page => {
    const item = element();
    item.setAttribute('data-page', page);
    return item;
  });
  return {context, requests, assets, streams, redirects, assigned, timeouts, intervals, listeners, nodes, navItems,
    start: () => documentListeners.DOMContentLoaded(), keydown: event => documentListeners.keydown(event)};
}

async function main() {
  // Relative and absolute API bases both retain exactly one explicit mount.
  for (const apiBase of [prefix + '/api/balance-calc', origin + prefix + '/api/balance-calc']) {
    const b = browser(apiBase);
    const c = b.context;
    b.start();
    await flush();
    const app = origin + prefix;
    assert.equal(c.testNav._getApiOrigin(), origin);
    assert.equal(c.testNav._getAppBase(), app);
    const requested = b.requests.map(request => new URL(request.input, origin).href);
    for (const endpoint of ['/api/token-refresh', '/api/vps/alerts', '/api/server-status', '/api/ai/preferences']) {
      assert.ok(requested.includes(app + endpoint), endpoint);
    }
    assert.equal(b.streams[0].url, app + '/api/server-status/stream');
    assert.ok(b.requests.every(request => !request.options?.headers?.Authorization));
    assert.ok(b.streams.every(stream => !stream.url.includes('token=')));

    for (const item of b.navItems) {
      item.click();
      assert.equal(c.location.href, app + c.testNav.FASTAPI_PAGES[item.getAttribute('data-page')]);
    }
    b.nodes['nav-logo'].click();
    assert.equal(c.location.href, app + '/api/auth/main_page');
    c.testNav.renderAlertOverlay();
    b.nodes['pbgui-alert-open-monitor'].onclick({preventDefault() {}});
    assert.equal(c.location.href, app + '/api/vps/main_page');
    c.testNav.ackAlert('test-alert');
    c.testNav.ackAllAlerts();
    assert.equal(b.requests.at(-2).input, app + '/api/vps/alerts/ack');
    assert.equal(b.requests.at(-1).input, app + '/api/vps/alerts/ack-all');

    b.nodes['pbgui-guide-btn'].click();
    assert.equal(b.assets.at(-1).src, prefix + '/app/js/shared_help_overlay.js?v=6');
    b.assets.at(-1).onerror();
    assert.equal(c.location.href, app + '/app/help.html?v=1766');
    b.nodes['pbgui-ai-btn'].click();
    assert.equal(b.assets.at(-2).href, prefix + '/app/css/ai_drawer.css?v=13');
    assert.equal(b.assets.at(-1).src, prefix + '/app/js/ai_drawer.js?v=39');
    b.nodes['pbgui-notify-btn'].click();
    assert.equal(b.assets.at(-1).src, prefix + '/app/js/log_viewer_panel.js?v=29');
    let viewerOptions;
    c.LogViewerPanel = class {constructor(options) {viewerOptions = options;} open() {} close() {}};
    b.assets.at(-1).onload();
    const ws = origin.replace(/^http/, 'ws') + prefix;
    assert.equal(viewerOptions.wsBase, ws);
    c.WS_BASE = ws;
    assert.equal(c.testNav._getWsBase(), ws); // Server already included the mount.

    c.TOKEN = 'test-only-bearer';
    c.PBGuiNotify.log('test notification', 'info');
    assert.equal(b.requests.at(-1).input, app + '/api/notify_log');
    assert.equal(b.requests.at(-1).options.headers.Authorization, 'Bearer test-only-bearer');
    c.TOKEN = '';

    // Origin allowlisting must not compare an origin to origin-plus-prefix.
    assert.equal(c.PBGuiAI.continuePageAction(app + '/api/v8/main_page', 'safe'), true);
    for (const target of ['https://evil.test/path', '//evil.test/path', 'javascript:alert(1)', 'data:text/plain,bad', origin + '.evil.test/path']) {
      assert.equal(c.PBGuiAI.continuePageAction(target, 'unsafe'), false, target);
    }
    const event = {detail: {type: 'page.perform_action', target: {page_key: 'system_transfers'}, action_id: 'transfer'}, preventDefault() {}};
    b.listeners['pbgui:ai-ui-action'][0](event);
    assert.equal(b.assigned.at(-1), app + '/api/profit-sweep/transfers/main_page?pbgui_ai_action=1');
    assert.equal(c.PBGuiAI.openQueuedBacktestCompare({action: 'queue_backtests', compare_after_completion: true,
      queued: [{filename: 'a.json'}, {filename: 'b.json'}]}), true);
    assert.equal(b.assigned.at(-1), app + '/api/backtest-v8/main_page?panel=queue');

    // Restart and subsequent reconnect probes use the same application base.
    b.nodes['pbgui-restart-btn'].click();
    b.keydown({key: 'Enter', preventDefault() {}});
    await flush();
    assert.equal(b.requests.at(-1).input, app + '/api/server-restart');
    assert.equal(b.requests.at(-1).options.credentials, 'same-origin');
    b.timeouts.find(timer => timer.delay === 4000).fn();
    assert.equal(b.requests.at(-1).input, app + '/api/server-status');

    // The interceptor must pass arbitrary strings, URL/Request inputs and
    // request options through unchanged, with no prefix or credential injection.
    c.TOKEN = 'test-only-bearer';
    const options = {method: 'PUT', headers: {'X-Test': 'test'}, credentials: 'omit'};
    for (const input of ['/api/custom', prefix + '/api/custom', 'relative', 'https://external.test/api/custom',
      new URL('https://external.test/api/custom'), new Request('https://external.test/api/custom', {method: 'POST', body: 'test'})]) {
      await c.fetch(input, options);
      assert.equal(b.requests.at(-1).input, input);
      assert.equal(b.requests.at(-1).options, options);
    }
    c.TOKEN = '';
    b.nodes['pbgui-logout-btn'].click();
    await flush();
    assert.equal(b.requests.at(-1).input, app + '/api/auth/logout');
    assert.equal(b.requests.at(-1).options.credentials, 'same-origin');
    assert.deepEqual(b.redirects, [app + '/']);
    for (const listener of b.listeners.pagehide) listener();
    assert.ok(b.streams[0].closed);
  }

  // Initial/periodic refresh and the existing 401 confirmation path bypass the
  // fetch interceptor but must use the mounted auth endpoint and login redirect.
  for (const mode of ['bootstrap', 'periodic', '401']) {
    const b = browser();
    if (mode === 'bootstrap') b.context.responseStatus = 401;
    b.start();
    await flush();
    if (mode === 'periodic') {
      b.context.responseStatus = 401;
      b.intervals.find(timer => timer.delay === 30 * 60 * 1000).fn();
    }
    if (mode === '401') {
      b.context.responseStatus = 401;
      await b.context.fetch('https://external.test/denied', {credentials: 'omit'});
    }
    await flush();
    assert.equal(new URL(b.requests.at(-1).input, origin).href, origin + prefix + '/api/token-refresh');
    assert.deepEqual(b.redirects, [origin + prefix + '/']);
  }

  const legacy = browser('https://api.example.test:9443/api/v7');
  assert.equal(legacy.context.testNav._getApiOrigin(), 'https://api.example.test:9443');
  assert.equal(legacy.context.PBGuiAI.continuePageAction('https://api.example.test:9443/api/v8/main_page', 'legacy'), true);
  assert.equal(legacy.context.testNav.tokenRefreshUrl(), 'https://api.example.test:9443' + prefix + '/api/token-refresh');

  for (const invalid of [null, 5, {}, '//evil.test', 'https://evil.test', 'relative', '/a/../b', '/a/./b',
    '/%2e%2e', '/.%2E', '/%2f%2fevil.test', '/%5c', '/%00', '/%0a', '/%7f', '/bad%',
    '/%ff', '/a?b', '/a#b', '/a\\b', '/a\nb', '/a b']) {
    const b = browser(undefined, invalid);
    assert.throws(() => b.context.testNav._appPath('/api/token-refresh'), /Invalid PBGui mount path/);
    assert.equal(b.requests.length, 0);
  }
}

main().catch(error => {console.error(error); process.exitCode = 1;});
