"""Executable Welcome regressions for cookie-only browser authentication."""

from pathlib import Path
import subprocess


def test_welcome_navigation_and_requests_use_cookie_auth() -> None:
    """Exercise actual inline functions without exposing session credentials to JavaScript."""
    source = Path("frontend/welcome.html").read_text(encoding="utf-8")
    assert "TOKEN" not in source
    assert "Authorization" not in source
    script = r"""
const assert = require('node:assert/strict');
const vm = require('node:vm');
let source = '';
process.stdin.setEncoding('utf8');
process.stdin.on('data', chunk => { source += chunk; });
process.stdin.on('end', async () => {
    try {
        for (const match of source.matchAll(/<script\b[^>]*>([\s\S]*?)<\/script>/g)) {
            new vm.Script(match[1]);
        }
        const requests = [];
        const helpCalls = [];
        const banners = [];
        const sections = [];
        let payload = {auth: {authenticated: true}};
        let sessionPayload = {auth: {authenticated: true, password_required: false}};
        let sessionError = false;
        let pauseFetch = null;
        const context = vm.createContext({
            URL,
            API_ORIGIN: 'https://pbgui.test', API_BASE: 'https://pbgui.test/api',
            PBGUI_VERSION: 'test', PBGUI_SERIAL: 'test',
            state: {bootstrap: null},
            window: {
                location: {origin: 'https://pbgui.test', href: 'https://pbgui.test/api/auth/main_page'},
                PBGuiSharedHelp: {open: (...args) => helpCalls.push(args)},
            },
            setBanner: (...args) => banners.push(args),
            focusSection: section => sections.push(section),
            fetch: async (url, options) => {
                requests.push({url, options});
                if (pauseFetch) await pauseFetch;
                if (url.endsWith('/passwordless-session')) {
                    return {ok: !sessionError, status: sessionError ? 403 : 200,
                        text: async () => JSON.stringify(sessionError ? {detail: 'Origin rejected'} : sessionPayload)};
                }
                return {ok: true, text: async () => JSON.stringify(payload)};
            },
        });
        context.window.self = context.window;
        context.window.top = context.window;
        context.render = data => { context.state.bootstrap = data; };
        for (const name of ['syncNavConfig', 'initWelcomeHelp', 'authHeaders', 'fetchJson', 'navigateTo', 'loadBootstrap']) {
            const match = source.match(new RegExp('        (?:async )?function ' + name + '\\([\\s\\S]*?\\n        }'));
            assert.ok(match, `Missing function ${name}`);
            vm.runInContext(match[0], context);
        }
        context.syncNavConfig();
        assert.equal('token' in context.window.PBGUI_NAV_CONFIG, false);
        context.initWelcomeHelp();
        context.window.PBGUI_HELP_OPENER();
        assert.deepEqual(helpCalls, [['welcome']]);
        assert.deepEqual({...context.authHeaders(false)}, {});
        assert.deepEqual({...context.authHeaders(true)}, {'Content-Type': 'application/json'});

        const entry = context.window.location.href;
        context.navigateTo('/api/v7/main_page');
        assert.equal(context.window.location.href, entry);
        assert.equal(sections.at(-1), 'overview');
        assert.match(banners.at(-1)[0], /Login required/);

        await context.loadBootstrap();
        assert.equal(requests[0].url, 'https://pbgui.test/api/auth/bootstrap');
        assert.deepEqual({...requests[0].options}, {});
        context.navigateTo('/api/v7/main_page');
        assert.equal(context.window.location.href, 'https://pbgui.test/api/v7/main_page');

        payload = {auth: {authenticated: false}};
        await context.loadBootstrap();
        context.window.location.href = entry;
        context.navigateTo('/api/v7/main_page');
        assert.equal(context.window.location.href, entry);
        assert.match(banners.at(-1)[0], /Login required/);
        assert.equal('TOKEN' in context, false);

        for (const auth of [
            {authenticated: false, password_required: true},
            {authenticated: true, password_required: false},
            {authenticated: false, password_required: false, error: 'Invalid auth state'},
            {authenticated: false}, {},
        ]) {
            payload = {auth};
            const before = requests.length;
            await context.loadBootstrap();
            assert.equal(requests.length, before + 1, 'Passwordless POST requires explicit safe bootstrap flags');
        }

        // Simulate an HTTP LAN browser: GET has no origin evidence, POST is same-origin.
        context.API_ORIGIN = 'http://192.0.2.8:8000';
        payload = {auth: {authenticated: false, password_required: false, error: null}};
        let before = requests.length;
        await context.loadBootstrap();
        assert.equal(requests.length, before + 2);
        assert.equal(requests.at(-1).url, 'http://192.0.2.8:8000/api/auth/passwordless-session');
        assert.deepEqual({...requests.at(-1).options}, {
            method: 'POST', credentials: 'same-origin', headers: context.authHeaders(true), body: '{}',
        });
        assert.equal(context.state.bootstrap.auth.authenticated, true);
        assert.equal(context.state.bootstrapLoading, false);

        payload = sessionPayload;
        before = requests.length;
        await context.loadBootstrap();
        assert.equal(requests.length, before + 1, 'An existing session must not trigger another issuance');

        payload = {auth: {authenticated: false, password_required: false}};
        sessionError = true;
        before = requests.length;
        await context.loadBootstrap();
        assert.equal(requests.length, before + 2, 'A failed POST must not create a retry loop');
        assert.deepEqual(banners.at(-1), ['Origin rejected', 'error']);
        assert.equal(context.state.bootstrapLoading, false);
        sessionError = false;
        sessionPayload = payload;
        before = requests.length;
        await context.loadBootstrap();
        assert.equal(requests.length, before + 2, 'An unauthenticated POST response must not recurse');

        let resume;
        pauseFetch = new Promise(resolve => { resume = resolve; });
        before = requests.length;
        const pending = context.loadBootstrap();
        await context.loadBootstrap();
        assert.equal(requests.length, before + 1, 'Concurrent bootstrap calls must share the in-flight operation');
        resume();
        await pending;
        pauseFetch = null;
        assert.equal(requests.length, before + 2);
        assert.equal(context.state.bootstrapLoading, false);
        assert.equal(requests.some(request => request.url.endsWith('/login')), false, 'Password login must stay separate');

        context.window.top = {};
        before = requests.length;
        await context.loadBootstrap();
        assert.equal(requests.length, before + 1, 'An embedded static Welcome shell must not issue a session');
    } catch (error) {
        console.error(error);
        process.exitCode = 1;
    }
});
"""
    result = subprocess.run(["node", "-e", script], input=source, text=True, capture_output=True, timeout=15)
    assert result.returncode == 0, result.stderr
