"""Notification navigation regressions with local assets and synthetic log sockets."""

import json
import re
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import urlparse

import pytest


ROOT = Path(__file__).resolve().parents[2]
SOCKET_FIXTURE = """
window.__notifySockets = [];
window.__notifyViewers = [];
class FixtureSocket {
  static CONNECTING = 0; static OPEN = 1; static CLOSING = 2; static CLOSED = 3;
  constructor(url) {
    this.url = url; this.readyState = 0; this.messages = [];
    window.__notifySockets.push(this);
    setTimeout(() => {
      if (this.readyState !== 0) return;
      this.readyState = 1;
      if (this.onopen) this.onopen({});
    }, 0);
  }
  send(value) { this.messages.push(JSON.parse(value)); }
  close() { this.readyState = 3; }
}
window.WebSocket = FixtureSocket;
"""
VIEWER_PROBE = """
const Issue420Viewer = LogViewerPanel;
LogViewerPanel = class extends Issue420Viewer {
  constructor(options) { super(options); window.__notifyViewers.push(this); }
};
"""


@pytest.fixture
def nav(request):
    """Serve only repository assets and synthetic API replies; hold viewer script delivery."""
    browser_api = __import__('browser_driver')
    parameters = getattr(
        request, "param", ("", "info_balance_calc", "/api/balance-calc/main_page")
    )
    prefix, current, destination = parameters[:3]
    dialogs_before_nav = len(parameters) > 3 and parameters[3]
    held, viewer_requests, errors, requests = [], [], [], []
    config = json.dumps({"authenticated": True, "current": current})
    html = (
        '<nav id="topnav"></nav><main style="padding-top:80px">'
        '<input id="draft" aria-label="Test draft"><button id="outside">Outside</button></main>'
        '<script>window.PBGUI_BASE_PREFIX=' + json.dumps(prefix) + ";"
        "window.PBGUI_NAV_CONFIG=" + config + ";</script>"
        + (
            '<script src="' + prefix + '/app/js/pbgui_dialogs.js?v=12"></script>'
            '<script>PBGuiDialogs.confirm({title:"Isolated setup",message:"No action"});'
            'document.getElementById("pbgui-dialog-cancel").click();</script>'
            if dialogs_before_nav else ""
        )
        +
        '<script src="' + prefix + '/app/pbgui_nav.js?v=test"></script>'
    )
    with browser_api.sync_browser() as driver:
        browser = driver.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": 1280, "height": 800})
        page.add_init_script(SOCKET_FIXTURE)
        page.on("pageerror", lambda error: errors.append(str(error)))

        def respond(route):
            """Intercept all requests so no application, remote host or user data is touched."""
            url = urlparse(route.request.url)
            requests.append(url.path)
            assert url.hostname == "notify.test"
            assert url.path.startswith(prefix + "/")
            path = url.path.removeprefix(prefix)
            if path == destination:
                route.fulfill(content_type="text/html", body=html)
            elif path == "/app/help.html":
                source = (ROOT / "frontend/help.html").read_text()
                for marker, value in {
                    "API_BASE": prefix + "/api", "WS_BASE": "ws://notify.test" + prefix,
                    "VERSION": "fixture", "SERIAL": "1",
                }.items():
                    source = source.replace("%%" + marker + "%%", value)
                route.fulfill(content_type="text/html", body=source)
            elif path == "/app/js/log_viewer_panel.js":
                held.append(route)
                viewer_requests.append(url.path)
            elif path.startswith("/app/"):
                asset = ROOT / "frontend" / path.removeprefix("/app/")
                assert asset.resolve().is_relative_to(ROOT / "frontend")
                route.fulfill(
                    body=asset.read_bytes(),
                    content_type="text/css" if asset.suffix == ".css" else "text/javascript",
                )
            elif path == "/api/help/index":
                route.fulfill(json=[{"file": "00_overview", "title": "Help Overview"}])
            elif path == "/api/help/content":
                route.fulfill(json={"content": "# Help Overview\n\nIsolated guide content."})
            elif path.endswith("/stream"):
                route.fulfill(status=204, body="")
            else:
                route.fulfill(json={})

        def release(fail=False):
            """Finish held script requests, optionally simulating an unavailable local asset."""
            assert len(held) == 1
            route = held.pop()
            if fail:
                route.abort("failed")
            else:
                route.fulfill(
                    body=(ROOT / "frontend/js/log_viewer_panel.js").read_text() + VIEWER_PROBE,
                    content_type="text/javascript",
                )

        def begin():
            """Request the lazy viewer script and return before its delivery."""
            with page.expect_request("**/log_viewer_panel.js?*"):
                page.get_by_role("button", name="Notification log", exact=True).click()
            assert len(held) == 1

        page.route("**/*", respond)
        page.goto("http://notify.test" + prefix + destination)
        yield SimpleNamespace(
            page=page, begin=begin, release=release, viewer_requests=viewer_requests,
            requests=requests, prefix=prefix, expect=browser_api.expect,
        )
        browser.close()
        assert not errors, errors


def _assert_closed(nav):
    """Assert visual/ARIA closure, actual viewer teardown and zero connected log sockets."""
    nav.expect(nav.page.locator("#pbgui-notify-panel")).not_to_be_visible()
    nav.expect(nav.page.locator("#pbgui-notify-panel")).to_have_attribute("aria-hidden", "true")
    nav.expect(nav.page.locator("#pbgui-notify-btn")).to_have_attribute("aria-expanded", "false")
    state = nav.page.evaluate("""() => ({
        viewers: __notifyViewers.map(v => ({closed:v._closed, socket:!!v._ws, reconnect:!!v._reconnectTimer})),
        sockets: __notifySockets.map(s => ({state:s.readyState, url:s.url}))
    })""")
    assert all(v == {"closed": True, "socket": False, "reconnect": False} for v in state["viewers"]), state
    assert all(s["state"] == 3 for s in state["sockets"]), state


def _open(nav):
    """Open the real viewer after serving the one pending lazy script."""
    nav.begin()
    nav.release()
    nav.expect(nav.page.get_by_role("dialog", name="Notifications", exact=True)).to_be_visible()
    nav.page.wait_for_function("__notifySockets.length === 1 && __notifySockets[0].readyState === 1")


@pytest.mark.parametrize("nav", [
    ("", "info_balance_calc", "/api/balance-calc/main_page"),
    ("/prefix", "system_services", "/api/services/main_page"),
], indirect=True)
def test_notification_escape_accessibility_cleanup_and_reopen(nav):
    """Escape from log contents restores bell focus and closes the real viewer on any nav."""
    page = nav.page
    bell = page.get_by_role("button", name="Notification log", exact=True)
    nav.expect(bell).to_have_attribute("aria-controls", "pbgui-notify-panel")
    nav.expect(bell).to_have_attribute("aria-haspopup", "dialog")
    _assert_closed(nav)
    page.locator("#draft").fill("Unsaved isolated draft")
    destination = page.url
    _open(nav)
    nav.expect(bell).to_have_attribute("aria-expanded", "true")
    panel = page.get_by_role("dialog", name="Notifications", exact=True)
    nav.expect(panel).to_have_attribute("aria-hidden", "false")
    assert panel.get_attribute("aria-modal") != "true"
    nav.expect(page.get_by_role("button", name="Close notification log")).to_be_focused()
    panel.locator("input").first.focus()
    page.evaluate("() => { window.__staleSocketClose = __notifySockets[0].onclose; }")
    page.keyboard.press("Escape")
    _assert_closed(nav)
    nav.expect(bell).to_be_focused()
    # A callback captured before closing must not revive an obsolete connection.
    page.evaluate("__staleSocketClose({code: 1006})")
    _assert_closed(nav)
    assert page.evaluate("__notifySockets.length") == 1
    nav.expect(page.locator("#draft")).to_have_value("Unsaved isolated draft")
    assert page.url == destination
    bell.click()
    nav.expect(panel).to_be_visible()
    assert page.evaluate("__notifyViewers.length") == 2
    assert len(nav.viewer_requests) == 1
    page.keyboard.press("Escape")
    _assert_closed(nav)


@pytest.mark.parametrize("close_action", ["bell", "close", "api"])
def test_notification_all_close_paths_are_idempotent(nav, close_action):
    """Every close path shares cleanup; repeated API calls never move unrelated focus."""
    _open(nav)
    if close_action == "bell":
        nav.page.locator("#pbgui-notify-btn").click()
    elif close_action == "close":
        nav.page.get_by_role("button", name="Close notification log").click()
    else:
        nav.page.evaluate("PBGuiNotify.closePanel()")
    _assert_closed(nav)
    nav.page.locator("#draft").focus()
    nav.page.evaluate("PBGuiNotify.closePanel(); PBGuiNotify.closePanel()")
    nav.page.keyboard.press("Escape")
    nav.expect(nav.page.locator("#draft")).to_be_focused()
    _assert_closed(nav)


def test_notification_escape_cancels_loading_without_focus_theft(nav):
    """A cancelled script completion creates neither a viewer nor a log connection."""
    nav.begin()
    nav.page.locator("#draft").focus()
    nav.page.keyboard.press("Escape")
    nav.release()
    nav.page.wait_for_function("typeof LogViewerPanel === 'function'")
    _assert_closed(nav)
    nav.expect(nav.page.locator("#draft")).to_be_focused()
    assert nav.page.evaluate("__notifyViewers.length + __notifySockets.length") == 0


@pytest.mark.parametrize("clicks", [2, 3, 4, 5])
def test_notification_pending_toggles_keep_only_latest_opening(nav, clicks):
    """Even/odd toggle bursts reuse one script and create at most one live viewer."""
    nav.begin()
    for _ in range(clicks - 1):
        nav.page.locator("#pbgui-notify-btn").click()
    assert len(nav.viewer_requests) == 1
    nav.release()
    nav.page.wait_for_function("typeof LogViewerPanel === 'function'")
    if clicks % 2:
        nav.expect(nav.page.locator("#pbgui-notify-panel")).to_be_visible()
        assert nav.page.evaluate("__notifyViewers.length") == 1
        nav.page.keyboard.press("Escape")
    else:
        assert nav.page.evaluate("__notifyViewers.length") == 0
    _assert_closed(nav)


def test_notification_duplicate_and_stale_script_callbacks_are_ignored(nav):
    """Duplicate load events and obsolete completions cannot create another viewer."""
    nav.begin()
    nav.page.evaluate("""() => {
      window.__oldLoad = document.querySelector('script[src*="log_viewer_panel.js"]').onload;
      PBGuiNotify.closePanel();
    }""")
    nav.page.locator("#pbgui-notify-btn").click()
    nav.release()
    nav.expect(nav.page.locator("#pbgui-notify-panel")).to_be_visible()
    nav.page.evaluate("__oldLoad(); __oldLoad()")
    assert nav.page.evaluate("__notifyViewers.length") == 1
    nav.page.keyboard.press("Escape")
    nav.page.evaluate("__oldLoad()")
    _assert_closed(nav)
    assert nav.page.evaluate("__notifyViewers.length") == 1


def test_notification_failed_load_can_retry(nav):
    """An isolated asset failure settles closed and allows the next opening to succeed."""
    nav.begin()
    nav.release(fail=True)
    nav.page.wait_for_function("!document.querySelector('script[src*=\"log_viewer_panel.js\"]')")
    _assert_closed(nav)
    assert nav.page.evaluate("__notifyViewers.length") == 0
    _open(nav)
    assert len(nav.viewer_requests) == 2
    nav.page.keyboard.press("Escape")
    _assert_closed(nav)


def test_notification_overlay_priority(nav):
    """One Escape closes Confirm, Alert, About, then the underlying notification panel."""
    _open(nav)
    page = nav.page
    page.locator("#pbgui-about-btn").click()
    # Test-only stacking avoids invoking any restart/delete/production action.
    page.locator("#pbgui-alert-btn").dispatch_event("click")
    page.evaluate("() => { window.__confirmation = PBGuiConfirm({title:'Isolated confirmation',message:'No action'}).then(v => { window.__answer=v; }); }")
    for selector in ("#pbgui-confirm-ovl", "#pbgui-alert-ovl", "#pbgui-about-ovl"):
        nav.expect(page.locator(selector)).to_be_visible()
        page.keyboard.press("Escape")
        nav.expect(page.locator(selector)).not_to_be_visible()
        nav.expect(page.locator("#pbgui-notify-panel")).to_be_visible()
    assert page.evaluate("window.__answer") is False
    page.keyboard.press("Escape")
    _assert_closed(nav)
    nav.expect(page.locator("#pbgui-notify-btn")).to_be_focused()


def test_notification_outside_click_and_separate_guide(nav):
    """Outside clicks retain the nonmodal panel; the separate Guide owns its Escape."""
    _open(nav)
    page = nav.page
    page.locator("#draft").fill("Keep this draft")
    page.locator("#outside").click()
    nav.expect(page.locator("#pbgui-notify-panel")).to_be_visible()
    destination = page.url
    page.locator("#pbgui-guide-btn").click()
    guide = page.locator("#pbgui-shared-help-ovl")
    nav.expect(guide).to_be_visible()
    nav.expect(page.locator("#pbgui-shared-help-content")).to_contain_text("Isolated guide content")
    page.locator("#pbgui-shared-help-close").focus()
    page.keyboard.press("Escape")
    nav.expect(guide).not_to_be_visible()
    nav.expect(page.locator("#pbgui-notify-panel")).to_be_visible()
    assert page.evaluate("__notifyViewers.length") == 1
    page.keyboard.press("Escape")
    _assert_closed(nav)
    nav.expect(page.locator("#draft")).to_have_value("Keep this draft")
    assert page.url == destination


@pytest.mark.parametrize("nav", [
    ("", "info_balance_calc", "/api/balance-calc/main_page"),
    ("/prefix", "system_services", "/api/services/main_page"),
], indirect=True)
def test_notification_on_help_through_real_menu(nav):
    """Information → Help loads the versioned real page and its updated shared nav."""
    page = nav.page
    page.locator('.nav-group-btn[data-group="information"]').click()
    page.locator('.nav-item[data-page="help"]').click()
    nav.expect(page).to_have_url(re.compile(
        re.escape("http://notify.test" + nav.prefix + "/app/help.html?v=1780") + r"(?:&.*)?$"
    ))
    nav.expect(page.locator("#help-content")).to_contain_text("Isolated guide content")
    destination = page.url
    _open(nav)
    page.keyboard.press("Escape")
    _assert_closed(nav)
    nav.expect(page.locator("#pbgui-notify-btn")).to_be_focused()
    assert page.url == destination


def _show_shared_confirmation(nav):
    """Open an inert real shared confirmation without invoking an application action."""
    if not nav.page.evaluate("typeof PBGuiDialogs === 'object'"):
        nav.page.add_script_tag(url="http://notify.test" + nav.prefix + "/app/js/pbgui_dialogs.js?v=12")
    nav.page.evaluate("""() => {
        PBGuiDialogs.confirm({title:'Isolated confirmation',message:'No action'})
            .then(value => { window.__sharedAnswer=value; });
    }""")
    nav.expect(nav.page.locator("#pbgui-dialog-accept")).to_be_focused()


@pytest.mark.parametrize("nav", [
    ("", "info_balance_calc", "/api/balance-calc/main_page", True),
    ("", "info_balance_calc", "/api/balance-calc/main_page", False),
], indirect=True)
def test_notification_shared_dialog_owns_escape_in_either_listener_order(nav):
    """Shared confirmation consumes only its Escape regardless of handler registration order."""
    _open(nav)
    _show_shared_confirmation(nav)
    nav.page.keyboard.press("Escape")
    nav.expect(nav.page.locator("#pbgui-dialog-ovl")).not_to_be_visible()
    assert nav.page.evaluate("window.__sharedAnswer") is False
    nav.expect(nav.page.locator("#pbgui-notify-panel")).to_be_visible()
    nav.expect(nav.page.locator("#pbgui-notify-btn")).to_have_attribute("aria-expanded", "true")
    nav.expect(nav.page.locator("#pbgui-notify-close")).to_be_focused()
    assert nav.page.evaluate("__notifySockets.length === 1 && !__notifyViewers[0]._closed")
    nav.page.keyboard.press("Escape")
    _assert_closed(nav)
    nav.expect(nav.page.locator("#pbgui-notify-btn")).to_be_focused()


@pytest.mark.parametrize("overlay", ["nav-confirm", "shared-confirm", "about", "alert", "guide", "draft"])
def test_notification_delayed_open_preserves_newer_focus(nav, overlay):
    """Finishing a pending nonmodal opening must not take focus from a newer interaction."""
    nav.begin()
    page = nav.page
    if overlay == "nav-confirm":
        page.evaluate("() => { PBGuiConfirm({title:'Isolated confirmation',message:'No action'}); }")
        focus = page.locator("#pbgui-confirm-accept")
    elif overlay == "shared-confirm":
        _show_shared_confirmation(nav)
        focus = page.locator("#pbgui-dialog-accept")
    elif overlay == "about":
        page.locator("#pbgui-about-btn").click()
        focus = page.locator("#pbgui-about-close")
    elif overlay == "alert":
        page.locator("#pbgui-alert-btn").click()
        focus = page.locator("#pbgui-alert-close")
    elif overlay == "guide":
        page.locator("#pbgui-guide-btn").click()
        focus = page.locator("#pbgui-shared-help-search")
        nav.expect(page.locator("#pbgui-shared-help-content")).to_contain_text("Isolated guide content")
    else:
        focus = page.locator("#draft")
        focus.fill("Keep typing here")
    focus.focus()
    nav.release()
    nav.expect(page.locator("#pbgui-notify-panel")).to_be_visible()
    nav.expect(focus).to_be_focused()
    # Closing the companion through its public API must also leave the newer focus alone.
    page.evaluate("PBGuiNotify.closePanel()")
    _assert_closed(nav)
    nav.expect(focus).to_be_focused()
    if overlay == "draft":
        nav.expect(focus).to_have_value("Keep typing here")
