"""Non-regression checks for Python-only project browser tooling."""
import ast
import json
from pathlib import Path
import subprocess

import pytest

from browser_driver import Error, expect, sync_browser
from javascript_checks import run_javascript
from tools.code_impact import impact, inventory


ROOT = Path(__file__).resolve().parents[1]


def test_python_impact_inventory_never_executes_inspected_modules(tmp_path):
    """A source file with import-time side effects is parsed, never imported."""
    source = tmp_path / "example.py"
    source.write_text("raise RuntimeError('must not execute')\n\ndef target(): pass\n\ndef caller(): target()\n")
    value = impact(inventory(tmp_path, [source.name]), "target")
    assert value["possible_callers"][0]["owner"] == "caller"
    assert value["risk"] == "MEDIUM"
    assert impact(inventory(tmp_path, [source.name]), "absent")["risk"] == "UNKNOWN"


def test_repository_has_no_javascript_runtime_or_browser_driver_dependency():
    """Reject executable Node/automation-driver dependencies without banning DOM nodes."""
    names = subprocess.check_output(["git", "ls-files", "--cached", "--others", "--exclude-standard"], cwd=ROOT, text=True).splitlines()
    violations = []
    forbidden = {"node", "nodejs", "npm", "npx", "pnpm", "yarn", "bun"}
    for name in names:
        path = ROOT / name
        if not path.is_file():
            continue
        if path.name in {"package.json", "package-lock.json", "yarn.lock", "pnpm-lock.yaml", ".gitnexusrc"} or path.suffix in {".cjs", ".mjs"}:
            violations.append(name)
        if path.suffix != ".py":
            continue
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and (node.module or "").split(".")[0] == "playwright":
                violations.append(name)
            if isinstance(node, ast.Import) and any(item.name.split(".")[0] == "playwright" for item in node.names):
                violations.append(name)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr in {"run", "Popen", "check_call", "check_output", "which"} and node.args:
                command = node.args[0]
                first = command.elts[0] if isinstance(command, (ast.List, ast.Tuple)) and command.elts else command
                if isinstance(first, ast.Constant) and isinstance(first.value, str) and Path(first.value).name in forbidden:
                    violations.append(name)
    assert not violations, sorted(set(violations))
    assert not (ROOT / ".gitnexus").exists()


def test_browser_fixture_assertions_fail_closed_and_cannot_read_private_files():
    """A failed browser assertion or an undeclared source access must fail its test."""
    for script in ("browserAssert.equal(1,2)", "testSources.readFileSync('pbgui.ini')",
                   "browserAssert.deepEqual(new Set([1]),new Set([2]))",
                   "browserAssert.deepEqual(new Map([['key',1]]),new Map([['key',2]]))",
                   "browserAssert.deepEqual(Object.create(null),{})",
                   "setImmediate(()=>{throw Error('asynchronous failure')})",
                   "'use strict'; undeclaredFixtureValue=1;"):
        result = run_javascript(["-e", script], capture_output=True, text=True)
        assert result.returncode == 1
        assert result.stderr
    result = run_javascript(["-e", "console.log(JSON.stringify({ok:true}))"], text=True, check=True)
    assert json.loads(result.stdout) == {"ok": True}


def test_direct_python_browser_routes_are_offline_and_assertions_observe_real_dom():
    """Route a fake origin, retain pending replies and exercise actual browser input."""
    with sync_browser() as runner:
        browser = runner.chromium.launch()
        page = browser.new_page()
        pending, errors = [], []
        page.on("pageerror", lambda error: errors.append(str(error)))

        def route(request):
            """Serve an isolated fixture and deliberately delay one request."""
            if request.request.url.endswith("/value"):
                pending.append(request)
            else:
                request.fulfill(content_type="text/html", body='<button id="go" onclick="fetch(\'/value\').then(r=>r.json()).then(x=>this.textContent=x.text)">Start</button>')

        page.route("**/*", route)
        page.goto("http://offline.test/check")
        page.locator("#go").click()
        page._wait(lambda: pending)
        assert page.locator("#go").inner_text() == "Start"
        pending[0].fulfill(json={"text": "Complete"})
        expect(page.locator("#go")).to_have_text("Complete")
        assert page.evaluate("async()=>({ready:await Promise.resolve(true)})") == {"ready": True}
        messages = []
        page.route_web_socket("ws://offline.test/*", lambda socket: socket.on_message(messages.append))
        page.evaluate("()=>{const socket=new WebSocket('ws://offline.test/check');socket.onopen=()=>socket.send('ready');}")
        page._wait(lambda: messages)
        assert messages == ["ready"]
        page.evaluate("()=>{document.querySelector('#go').onpointerdown=e=>window.pointerFixture={button:e.button,id:e.pointerId};}")
        page.dispatch_event("#go", "pointerdown", {"button": 0, "pointerId": 7})
        assert page.evaluate("window.pointerFixture") == {"button": 0, "id": 7}
        page.evaluate("()=>{const e=document.createElement('div');e.id='empty-area';e.style.width='100px';document.body.appendChild(e);}")
        assert not page.locator("#empty-area").is_visible()
        pending.clear()
        page.route("**/value", lambda route: route.fulfill(json={"text": "Once"}), times=1)
        assert page.evaluate("async()=>await(await fetch('/value')).json()") == {"text": "Once"}
        page.locator("#go").click()
        page._wait(lambda: len(pending) == 1)
        pending.pop().fulfill(json={"text": "Complete again"})
        expect(page.locator("#go")).to_have_text("Complete again")
        with pytest.raises(AssertionError):
            expect(page.locator("#go")).to_have_text("Incorrect", timeout=20)
        page.clock.install(time=1770000000000)
        page.reload()
        assert 1770000000000 <= page.evaluate("Date.now()") < 1770000010000
        page.clock.pause_at(1770000005000)
        page.reload()
        page.wait_for_timeout(50)
        assert page.evaluate("Date.now()") == 1770000005000
        assert not errors
        profile = Path(browser.profile.name)
    assert not profile.exists()
    assert browser.process.poll() is not None
