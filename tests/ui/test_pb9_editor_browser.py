"""PB9 editor real-menu/browser regressions with only intercepted fixture APIs."""

import copy
import json
import mimetypes
from pathlib import Path
import time
from types import SimpleNamespace
from urllib.parse import urlparse

import pytest

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(params=["", "/mounted"], ids=["root", "mounted"])
def editor(request):
    """Run the actual page/nav assets with a bounded, isolated in-memory library."""
    browser_api = __import__("browser_driver")
    prefix = request.param
    template = {"pbgui": {"name": "Template", "number": 9007199254740993},
                "config_version": "v9.0.0", "bot": {"long": {}, "short": {}}}
    fixture = SimpleNamespace(prefix=prefix, files={}, requests=[], errors=[], violations=[],
                              delay="", save_error="", load_error="", restart=False, pending=[], saved=[], raw=[], runtime={"configured": True, "detected": True,
                              "config_schema": "v9.0.0", "pb9dir": "/fixture/pb9", "pb9venv": "/fixture/env/bin/python"})
    with browser_api.sync_browser() as driver:
        browser = driver.chromium.launch(headless=True)
        context = browser.new_context(viewport={"width": 1280, "height": 900}, service_workers="block")
        context.set_default_timeout(6000)
        context.route_web_socket("**/*", lambda socket: socket.close())

        def reject(route, status, code):
            """Send only the same fixed error envelope used by the production API."""
            route.fulfill(status=status, json={"detail": "Fixture rejection"},
                          headers={"X-PBGUI-Error-Codes": code, "Cache-Control": "no-store"})

        def respond(route):
            """Intercept every API/asset request; never contact a real application."""
            url = urlparse(route.request.url)
            if url.hostname != "pb9.test" or not url.path.startswith(prefix + "/"):
                fixture.violations.append(route.request.url)
                route.fulfill(status=404, body="Unapproved request")
                return
            path = url.path.removeprefix(prefix)
            fixture.requests.append((path, route.request.method))
            if route.request.post_data is not None:
                fixture.raw.append((path, route.request.post_data))
            if path in {"/api/services/main_page", "/api/pb9/main_page", "/app/help.html"}:
                name = "services_monitor.html" if "services" in path else "pb9_editor.html" if "pb9" in path else "help.html"
                html = (ROOT / "frontend" / name).read_text()
                api = prefix + ("/api/pb9" if "pb9" in path else "/api/services")
                for marker, value in {"API_BASE": api, "BASE_PREFIX": prefix, "VERSION": "fixture", "SERIAL": "1", "CURRENT": "", "NAV_HASH": "fixture"}.items():
                    html = html.replace('"%%' + marker + '%%"', json.dumps(value)).replace("%%" + marker + "%%", value)
                html = html.replace('src="/app/', 'src="' + prefix + '/app/').replace('href="/app/', 'href="' + prefix + '/app/')
                route.fulfill(body=html, content_type="text/html")
            elif path.startswith("/app/"):
                asset = ROOT / "frontend" / path.removeprefix("/app/")
                assert asset.resolve().is_relative_to(ROOT / "frontend")
                if asset.is_file():
                    route.fulfill(body=asset.read_bytes(), content_type=mimetypes.guess_type(asset)[0] or "application/octet-stream")
                else:
                    route.fulfill(status=404, body="")
            elif path == "/api/help/index":
                route.fulfill(json=[{"file": "52_pbv9_editor", "title": "PB9 Config Editor"}])
            elif path == "/api/help/content":
                route.fulfill(json={"content": "# PB9 editor guide\n\nFixture documentation."})
            elif path == "/api/pb9/runtime":
                route.fulfill(json=fixture.runtime)
            elif path == "/api/pb9/configs":
                route.fulfill(json={"configs": sorted(fixture.files)})
            elif path == "/api/pb9/template":
                route.fulfill(json={"config": template, "config_text": json.dumps(template, indent=4) + "\n", "source": {}})
            elif path == "/api/pb9/runtime/paths":
                fixture.runtime.update(route.request.post_data_json)
                route.fulfill(json=fixture.runtime)
            elif path == "/api/pb9/validate":
                if fixture.delay == "validate":
                    fixture.pending.append(route)
                else:
                    route.fulfill(json={"valid": True})
            elif path == "/api/pb9/configs/revision":
                record = fixture.files.get(route.request.post_data_json["name"])
                if record:
                    route.fulfill(json={"revision": record["revision"]})
                else:
                    reject(route, 404, "config_file_not_found")
            elif path == "/api/pb9/configs/reconcile":
                body = route.request.post_data_json
                stored = fixture.files.get(body["name"])
                if fixture.load_error:
                    reject(route, 503, fixture.load_error)
                elif not stored:
                    reject(route, 404, "config_file_not_found")
                else:
                    matches = json.dumps(stored["config"], sort_keys=True) == json.dumps(body["config"], sort_keys=True)
                    route.fulfill(json={"matches": matches, "revision": stored["revision"],
                                        "config_text": json.dumps(stored["config"], indent=4) + "\n"})
            elif path == "/api/pb9/configs/load":
                name = route.request.post_data_json["name"]
                if fixture.load_error:
                    reject(route, 503, fixture.load_error)
                elif name not in fixture.files:
                    reject(route, 404, "config_file_not_found")
                elif fixture.delay == "load":
                    fixture.pending.append(route)
                else:
                    data = fixture.files[name]
                    route.fulfill(json={**data, "config_text": json.dumps(data["config"], indent=4) + "\n"})
            elif path == "/api/pb9/configs/save":
                if fixture.save_error == "before-network":
                    route.abort(); return
                if fixture.save_error == "unauthenticated":
                    reject(route, 401, ""); return
                body = route.request.post_data_json
                previous = fixture.files.get(body["name"])
                if fixture.restart:
                    reject(route, 409, "api_restart_pending")
                elif previous and body["revision"] != previous["revision"]:
                    reject(route, 409, "config_revision_conflict")
                elif not previous and body["revision"] is not None:
                    reject(route, 409, "config_revision_conflict")
                else:
                    fixture.saved.append(copy.deepcopy(body))
                    config = copy.deepcopy(body["config"])
                    gui = config.pop("pbgui", None)
                    if gui is not None:
                        config["pbgui"] = gui
                    fixture.files[body["name"]] = {"config": config, "revision": str(len(fixture.saved)).zfill(64)}
                    if fixture.save_error == "network":
                        route.abort()
                    elif fixture.save_error:
                        reject(route, 503, fixture.save_error)
                    elif fixture.delay == "save":
                        fixture.pending.append(route)
                    else:
                        route.fulfill(json={"saved": True, "name": body["name"]})
            elif path.endswith("/stream"):
                route.fulfill(status=204, body="")
            else:
                route.fulfill(json={})

        context.route("**/*", respond)
        page = context.new_page()
        page.on("pageerror", lambda error: fixture.errors.append(str(error)))
        fixture.page, fixture.context, fixture.expect = page, context, browser_api.expect
        try:
            yield fixture
        finally:
            context.close()
            browser.close()


def _enter(editor):
    """Open the retained JSON-library preview; the PBv9 menu now opens Run."""
    page = editor.page
    page.goto("http://pb9.test" + editor.prefix + "/api/pb9/main_page")
    editor.expect(page).to_have_url("http://pb9.test" + editor.prefix + "/api/pb9/main_page")
    editor.expect(page.locator("#runtime-status")).to_contain_text("PB9 detected")
    editor.expect(page.locator("#new-template")).to_be_enabled()


def _draft(editor, name="example.json"):
    """Load an actual template through the UI and choose its create-only name."""
    editor.page.locator("#new-template").click()
    editor.page.wait_for_function("document.getElementById('config-json').value.includes('v9.0.0')")
    editor.page.locator("#config-name").fill(name)


def _pending(editor):
    """Pump browser events until this fixture owns the deliberately paused request."""
    deadline = time.monotonic() + 5
    while not editor.pending:
        assert time.monotonic() < deadline
        editor.page.wait_for_timeout(20)
    return editor.pending.pop(0)


def test_library_preview_template_save_open_and_reload_preserve_navigation(editor):
    """A real first-editor cycle keeps authored values, revisions and mounted routes."""
    _enter(editor)
    _draft(editor)
    page = editor.page
    assert "9007199254740993" in page.locator("#config-json").input_value()
    page.locator("#validate-config").click()
    editor.expect(page.locator("#message")).to_contain_text("Configuration is valid")
    page.locator("#save-config").click()
    editor.expect(page.locator("#message")).to_contain_text("Saved example.json")
    assert editor.saved[0]["revision"] is None
    assert editor.saved[0]["config"]["pbgui"]["number"] == 9007199254740993
    page.locator("#config-filter").fill("example")
    page.reload()
    page.wait_for_function("document.getElementById('config-json').value.includes('v9.0.0')")
    assert page.locator("#config-filter").input_value() == "example"
    assert page.locator("#config-name").input_value() == "example.json"
    assert "file=example.json" in page.url
    assert "9007199254740993" in page.locator("#config-json").input_value()
    assert not editor.violations
    assert not editor.errors


def test_typing_during_save_and_validation_keeps_newer_text(editor):
    """Completion messages identify their snapshot without replacing newer local edits."""
    _enter(editor)
    _draft(editor)
    page = editor.page
    editor.delay = "validate"
    page.locator("#validate-config").click()
    pending = _pending(editor)
    newer = '{"config_version":"v9.0.0","bot":{"long":{"changed":1}}}'
    page.locator("#config-json").fill(newer)
    pending.fulfill(json={"valid": True})
    editor.expect(page.locator("#message")).to_contain_text("newer edits are not validated")
    editor.delay = "save"
    page.locator("#save-config").click()
    pending = _pending(editor)
    newest = newer.replace('"changed":1', '"changed":2')
    page.locator("#config-json").fill(newest)
    editor.delay = ""
    pending.fulfill(json={"saved": True, "name": "example.json"})
    editor.expect(page.locator("#message")).to_contain_text("newer edits are kept")
    assert page.locator("#config-json").input_value() == newest
    editor.expect(page.locator("#document-status")).to_contain_text("Unsaved changes")
    assert not editor.errors


def test_automatic_change_and_conflict_keep_draft(editor):
    """Background file changes update a clean document, and never overwrite a draft."""
    editor.files["example.json"] = {"config": {"config_version": "v9.0.0", "pbgui": {"name": "Original"}}, "revision": "1" * 64}
    _enter(editor)
    page = editor.page
    page.locator('#sidebar-inner button').click()
    page.wait_for_function("document.getElementById('config-json').value.includes('Original')")
    editor.files["example.json"] = {"config": {"config_version": "v9.0.0", "pbgui": {"name": "External"}}, "revision": "2" * 64}
    page.evaluate("window.dispatchEvent(new Event('focus'))")
    page.wait_for_function("document.getElementById('config-json').value.includes('External')")
    draft = '{"config_version":"v9.0.0","pbgui":{"name":"My draft"}}'
    page.locator("#config-json").fill(draft)
    editor.files["example.json"]["revision"] = "3" * 64
    page.evaluate("window.dispatchEvent(new Event('focus'))")
    editor.expect(page.locator("#message")).to_contain_text("file changed on disk")
    assert page.locator("#config-json").input_value() == draft
    page.locator("#save-config").click()
    editor.expect(page.locator("#message")).to_contain_text("already exists or changed")
    assert page.locator("#config-json").input_value() == draft
    assert not editor.saved
    assert not editor.errors


def test_runtime_paths_and_separate_guide_preserve_editor(editor):
    """Path controls are explicit settings, and Guide stays above the current page."""
    _enter(editor)
    _draft(editor)
    page = editor.page
    original = page.locator("#config-json").input_value()
    page.locator("#runtime-toggle").click()
    page.locator("#pb9-source").fill("/fixture/new-pb9")
    page.locator("#pb9-python").fill("/fixture/new-env/bin/python")
    page.locator("#save-paths").click()
    editor.expect(page.locator("#message")).to_contain_text("paths saved")
    assert editor.runtime["pb9dir"] == "/fixture/new-pb9"
    assert page.locator("#config-json").input_value() == original
    before = page.url
    page.locator("#pbgui-guide-btn").click()
    editor.expect(page.locator("#pbgui-shared-help-content")).to_contain_text("Fixture documentation")
    assert page.url == before
    assert page.locator("#config-json").input_value() == original
    assert not editor.errors


@pytest.mark.parametrize("failure", ["network", "config_file_created_io_failed"])
def test_uncertain_save_is_reconciled_without_repeating_write(editor, failure):
    """A lost acknowledgement checks the stored revision and keeps the submitted bytes."""
    _enter(editor)
    _draft(editor)
    editor.save_error = failure
    page = editor.page
    page.locator("#save-config").click()
    if failure == "network":
        editor.expect(page.locator("#message")).to_contain_text("may have completed")
        editor.expect(page.locator("#save-config")).to_be_disabled()
        page.evaluate("window.dispatchEvent(new Event('focus'))")
        editor.expect(page.locator("#message")).to_contain_text("Stored saved version found")
    else:
        editor.expect(page.locator("#message")).to_contain_text("storage acknowledgement failed")
    assert len(editor.saved) == 1
    assert "9007199254740993" in page.locator("#config-json").input_value()
    editor.expect(page.locator("#document-status")).to_contain_text("Saved")
    assert not editor.errors


def test_late_load_cannot_replace_typing(editor):
    """Even a requested file open preserves text entered while native loading runs."""
    editor.files["example.json"] = {"config": {"config_version": "v9.0.0"}, "revision": "1" * 64}
    _enter(editor)
    page = editor.page
    editor.delay = "load"
    page.locator('#sidebar-inner button').click()
    pending = _pending(editor)
    draft = '{"config_version":"v9.0.0","pbgui":{"name":"Newer local text"}}'
    page.locator("#config-json").fill(draft)
    pending.fulfill(json=editor.files["example.json"])
    editor.expect(page.locator("#message")).to_contain_text("newer edits were kept")
    assert page.locator("#config-json").input_value() == draft
    assert not editor.errors


def test_raw_json_request_preserves_duplicate_keys_for_server_rejection(editor):
    """Client parsing must not silently choose one duplicate field before validation."""
    _enter(editor)
    _draft(editor)
    page = editor.page
    text = '{"config_version":"v9.0.0","pbgui":{"x":1,"x":2}}'
    page.locator("#config-json").fill(text)
    page.locator("#validate-config").click()
    editor.expect(page.locator("#message")).to_contain_text("Configuration is valid")
    sent = [body for path, body in editor.raw if path == "/api/pb9/validate"]
    assert sent == ['{"config":' + text + '}']
    assert page.locator("#config-json").input_value() == text
    # The fixture acknowledges transport; strict duplicate rejection is covered
    # against the actual backend in test_api_pb9.py, without reimplementing it here.


def test_sidebar_bounds_and_focus_remain_current_across_automatic_update(editor):
    """Minimum/maximum widths, mobile layout and focused text survive genuine renderers."""
    long_name = "a" * 240 + ".json"
    editor.files[long_name] = {"config": {"config_version": "v9.0.0", "pbgui": {"name": "Old"}}, "revision": "1" * 64}
    _enter(editor)
    page = editor.page
    page.locator('#sidebar-inner button').click()
    page.wait_for_function("document.getElementById('config-json').value.includes('Old')")
    page.locator("#config-json").focus()
    page.evaluate("document.getElementById('config-json').setSelectionRange(8,12)")
    editor.files[long_name] = {"config": {"config_version": "v9.0.0", "pbgui": {"name": "New"}}, "revision": "2" * 64}
    page.evaluate("window.dispatchEvent(new Event('focus'))")
    page.wait_for_function("document.getElementById('config-json').value.includes('New')")
    assert page.evaluate("document.activeElement.id") == "config-json"
    assert page.evaluate("[document.getElementById('config-json').selectionStart,document.getElementById('config-json').selectionEnd]") == [8, 12]
    for key, expected in (("Home", 160), ("End", 420)):
        page.locator("#sidebar-resize").focus()
        page.keyboard.press(key)
        assert abs(page.locator("#sidebar").bounding_box()["width"] - expected) < 2
        page.reload()
        page.wait_for_function("document.getElementById('config-json').value.includes('New')")
        assert abs(page.locator("#sidebar").bounding_box()["width"] - expected) < 2
    for width in (1440, 850, 740, 390):
        page.set_viewport_size({"width": width, "height": 750})
        bounds = page.evaluate("""() => {const sb=document.getElementById('sidebar'), main=document.getElementById('main-content');
          return {overflow:sb.scrollWidth>sb.clientWidth+1, controls:[...document.querySelectorAll('#sidebar button')].some(b=>b.scrollWidth>b.clientWidth+1),
            mainWidth:main.getBoundingClientRect().width, mainHeight:main.getBoundingClientRect().height};}""")
        assert not bounds["overflow"]
        assert not bounds["controls"]
        assert bounds["mainWidth"] > 100 and bounds["mainHeight"] > 100
    assert not editor.errors


def test_create_conflict_opens_the_conflicting_filename(editor):
    """A draft conflict can explicitly open its target instead of an unrelated source."""
    editor.files["existing.json"] = {"config": {"config_version": "v9.0.0", "pbgui": {"name": "Stored target"}}, "revision": "1" * 64}
    _enter(editor)
    _draft(editor, "existing.json")
    page = editor.page
    page.locator("#save-config").click()
    editor.expect(page.locator("#message")).to_contain_text("already exists or changed")
    page.locator("#open-stored").click()
    page.locator("#pbgui-dialog-accept").click()
    page.wait_for_function("document.getElementById('config-json').value.includes('Stored target')")
    assert page.locator("#config-name").input_value() == "existing.json"
    assert not editor.saved


def test_restart_409_does_not_offer_discard_as_file_conflict(editor):
    """A coordinator restart is operational state, not an update revision mismatch."""
    _enter(editor)
    _draft(editor)
    editor.restart = True
    page = editor.page
    original = page.locator("#config-json").input_value()
    page.locator("#save-config").click()
    editor.expect(page.locator("#message")).to_contain_text("API restart is pending")
    editor.expect(page.locator("#open-stored")).to_be_hidden()
    assert page.locator("#config-json").input_value() == original
    assert not editor.saved


def test_saved_file_reconciles_when_initial_revision_load_fails(editor):
    """A successful save remains acknowledged when its independent verification is busy."""
    _enter(editor)
    _draft(editor)
    editor.load_error = "pb9_busy"
    page = editor.page
    page.locator("#save-config").click()
    editor.expect(page.locator("#message")).to_contain_text("Saved; the stored revision could not be confirmed")
    editor.expect(page.locator("#save-config")).to_be_disabled()
    assert len(editor.saved) == 1
    editor.load_error = ""
    page.evaluate("window.dispatchEvent(new Event('focus'))")
    editor.expect(page.locator("#message")).to_contain_text("Stored saved version found")
    editor.expect(page.locator("#document-status")).to_contain_text("Saved")
    assert len(editor.saved) == 1


def test_unchanged_open_document_uses_revision_reads_not_native_loads(editor):
    """Repeated automatic/return checks never reload unchanged files through native validation."""
    editor.files["example.json"] = {"config": {"config_version": "v9.0.0"}, "revision": "1" * 64}
    _enter(editor)
    page = editor.page
    page.locator('#sidebar-inner button').click()
    page.wait_for_function("document.getElementById('config-json').value.includes('v9.0.0')")
    original = len([path for path, _method in editor.requests if path == "/api/pb9/configs/load"])
    for _ in range(3):
        before = len([path for path, _method in editor.requests if path == "/api/pb9/configs/revision"])
        page.evaluate("window.dispatchEvent(new Event('focus'))")
        deadline = time.monotonic() + 5
        while len([path for path, _method in editor.requests if path == "/api/pb9/configs/revision"]) == before:
            assert time.monotonic() < deadline
            page.wait_for_timeout(30)
        page.wait_for_timeout(30)
    assert len([path for path, _method in editor.requests if path == "/api/pb9/configs/load"]) == original
    assert not editor.errors


@pytest.mark.parametrize("existing", [False, True])
def test_save_that_never_arrives_finishes_reconciliation_and_allows_manual_retry(editor, existing):
    """Missing writes stop polling natively and never trap the edited filename."""
    if existing:
        editor.files["example.json"] = {"config": {"config_version": "v9.0.0", "pbgui": {"name": "Before"}}, "revision": "1" * 64}
    _enter(editor)
    page = editor.page
    if existing:
        page.locator('#sidebar-inner button').click()
        page.wait_for_function("document.getElementById('config-json').value.includes('Before')")
        page.locator("#config-json").fill('{"config_version":"v9.0.0","pbgui":{"name":"My draft"}}')
    else:
        _draft(editor)
    original = page.locator("#config-json").input_value()
    editor.save_error = "before-network"
    page.locator("#save-config").click()
    editor.expect(page.locator("#message")).to_contain_text("may have completed")
    page.evaluate("window.dispatchEvent(new Event('focus'))")
    editor.expect(page.locator("#message")).to_contain_text("differs from the submitted" if existing else "No saved target was found")
    editor.expect(page.locator("#save-config")).to_be_enabled()
    assert page.locator("#config-json").input_value() == original
    load_count = len([path for path, _ in editor.requests if path == "/api/pb9/configs/load"])
    for _ in range(3):
        page.evaluate("window.dispatchEvent(new Event('focus'))")
        page.wait_for_timeout(70)
    assert len([path for path, _ in editor.requests if path == "/api/pb9/configs/load"]) == load_count
    assert not editor.saved
    assert not editor.errors


def test_save_401_keeps_session_message_and_does_not_enter_uncertain_state(editor):
    """A known auth rejection never masquerades as a possibly committed save."""
    _enter(editor)
    _draft(editor)
    editor.save_error = "unauthenticated"
    page = editor.page
    original = page.locator("#config-json").input_value()
    page.locator("#save-config").click()
    editor.expect(page.locator("#message")).to_contain_text("session expired")
    editor.expect(page.locator("#save-config")).to_be_enabled()
    assert page.locator("#config-json").input_value() == original
    assert not editor.saved


def test_save_as_definitive_rejection_does_not_orphan_previous_uncertainty(editor):
    """A later explicit filename cannot leave an old ambiguous-name lock stranded."""
    _enter(editor)
    _draft(editor, "a.json")
    page = editor.page
    editor.save_error = "before-network"
    page.locator("#save-config").click()
    editor.expect(page.locator("#message")).to_contain_text("may have completed")
    page.locator("#config-name").fill("b.json")
    editor.save_error = "unauthenticated"
    page.locator("#save-config").click()
    editor.expect(page.locator("#message")).to_contain_text("session expired")
    page.locator("#config-name").fill("a.json")
    editor.expect(page.locator("#save-config")).to_be_enabled()
    assert not editor.saved
