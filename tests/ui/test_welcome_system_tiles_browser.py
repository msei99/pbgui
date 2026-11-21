"""Welcome system tiles, storage line and storage warnings in a real browser."""

from __future__ import annotations

from pathlib import Path
from urllib.parse import urlparse

import pytest

from browser_driver import sync_browser

ROOT = Path(__file__).resolve().parents[2]

SUMMARY = {
    "system": {"cpu_percent": 23.4, "cpu_logical": 16, "cpu_physical": 8, "load": [2.1, 1.8, 1.5],
               "memory": {"total": 64 * 1024 ** 3, "used": 11 * 1024 ** 3, "available": 53 * 1024 ** 3, "percent": 17.2},
               "swap": {"total": 8 * 1024 ** 3, "used": 0, "percent": 0}, "uptime_s": 4 * 86400 + 3 * 3600},
    "processes": {"count": 9, "rss": int(1.8 * 1024 ** 3)},
    "disk": {"mount": "/", "total": 915 * 1024 ** 3, "used": 780 * 1024 ** 3, "free": 135 * 1024 ** 3, "percent": 85.2, "roots": ["pbgui"]},
    "mounts": [],
    "scanned_at": 1_700_000_000,
    "busy": False,
    "reclaimable": {"safe": int(3.8 * 1024 ** 3), "optional": 100 * 1024 ** 3, "review": 0},
    "warnings": [{"level": "warning", "text": "Disk / is 85% full (135.0 GB free)"}],
}


def _bootstrap(authenticated: bool) -> dict:
    return {
        "version": "test", "serial": "1",
        "auth": {"authenticated": authenticated, "password_required": True, "auth_mode": "password", "login_security": {}},
        "setup": {"ready": True, "import_ready": True, "master": True, "pbname": "host", "role": "master",
                  "pb8": {"ready": True, "configured": True}, "errors": [], "warnings": []},
    }


@pytest.fixture
def welcome():
    """Serve the real Welcome page with synthetic bootstrap and summary data."""
    control = {"authenticated": True, "summary_calls": 0, "runtime_calls": 0, "save_calls": [], "save_error": False,
               "runtime": {"configured": True, "detected": True, "source_detected": True,
                           "pb9dir": "/fixtures/pb9", "pb9venv": "/fixtures/venv_pb9/bin/python",
                           "config_schema": "v9.0.0", "package_version": "9.0.0.dev0",
                           "python_version_supported": True, "interpreter_executable": True,
                           "cli_executable": True, "rust_exists": True, "commit_available": True,
                           "commit": "b" * 40, "errors": [], "warnings": [], "installation_incomplete": False}}
    errors = []
    with sync_browser() as driver:
        browser = driver.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": 1400, "height": 900})
        page.set_default_timeout(7000)
        page.on("pageerror", lambda error: errors.append(str(error)))

        def respond(route):
            path = urlparse(route.request.url).path
            if path in {"/welcome", "/api/auth/main_page"}:
                html = (ROOT / "frontend/welcome.html").read_text().replace("%%NAV_HASH%%", "test")
                route.fulfill(body=html, content_type="text/html")
            elif path.startswith("/app/"):
                asset = ROOT / "frontend" / path.removeprefix("/app/")
                if asset.is_file():
                    route.fulfill(body=asset.read_bytes(), content_type={".js": "text/javascript", ".css": "text/css"}.get(asset.suffix, "application/octet-stream"))
                else:
                    route.fulfill(status=404, body="")
            elif path == "/api/auth/bootstrap":
                route.fulfill(json=_bootstrap(control["authenticated"]))
            elif path == "/api/pb9/runtime":
                control["runtime_calls"] += 1
                route.fulfill(json=control["runtime"])
            elif path == "/api/pb9/runtime/paths":
                values = route.request.post_data_json
                control["save_calls"].append(values)
                if control["save_error"]:
                    route.fulfill(status=422, json={"detail": "PB9 paths rejected"})
                else:
                    control["runtime"].update(values)
                    route.fulfill(json=control["runtime"])
            elif path == "/api/storage/summary":
                control["summary_calls"] += 1
                route.fulfill(json=SUMMARY) if control["authenticated"] else route.fulfill(status=401, json={"detail": "no"})
            elif path.endswith("/stream"):
                route.fulfill(status=204, body="")
            else:
                route.fulfill(json={})

        page.route("**/*", respond)
        yield page, control, errors
        browser.close()


def test_tiles_storage_line_and_warning(welcome):
    """Authenticated sessions see live tiles, the storage line and the disk warning."""
    page, control, errors = welcome
    page.goto("http://welcome.test/welcome")
    page.wait_for_function("!document.getElementById('system-tiles').hidden")
    text = page.evaluate("document.getElementById('system-tiles').textContent")
    assert "23 %" in text and "16 threads" in text and "11.0 GB / 64.0 GB" in text and "4d 3h" in text
    assert "PBGui 1.8 GB in 9 processes" in text
    line = page.evaluate("document.getElementById('storage-line').textContent")
    assert "135 GB free" in line and "3.8 GB safe to clean" in line
    assert page.evaluate("document.getElementById('storage-line').getAttribute('href')").endswith("/api/storage/main_page#overview")
    assert page.evaluate("document.getElementById('tile-cpu-link').getAttribute('href')").endswith("/api/storage/main_page#system")
    page.wait_for_function("document.getElementById('issues').textContent.includes('Disk / is 85% full')")
    issues = page.evaluate("document.getElementById('issues').textContent")
    assert "No setup issues detected." not in issues
    assert page.evaluate("document.querySelector('#issues .issue a').textContent") == "Open Storage"
    assert page.evaluate("document.getElementById('tile-disk-bar').className") == "warn"
    assert not errors


def test_guest_sees_no_tiles(welcome):
    """Guests never request the authenticated summary."""
    page, control, errors = welcome
    control["authenticated"] = False
    page.goto("http://welcome.test/welcome")
    page.wait_for_function("document.getElementById('summary-auth').textContent === 'Guest'")
    assert page.evaluate("document.getElementById('system-tiles').hidden") is True
    assert page.evaluate("document.getElementById('storage-line').hidden") is True
    assert control["summary_calls"] == 0
    assert control["runtime_calls"] == 0


def test_pb9_status_and_setup_keep_edits_and_restore_location(welcome):
    """Real controls use the existing PB9 writer; polling keeps both setup forms intact."""
    page, control, errors = welcome
    page.goto("http://welcome.test/api/auth/main_page#setup")
    page.wait_for_function("document.getElementById('summary-pb9').textContent === 'PB9 detected'")
    assert page.locator("#section-setup").is_visible()
    assert page.locator("#pb9dir").input_value() == "/fixtures/pb9"
    assert "PB9 revision" in page.locator("#status-list").text_content()
    assert "b" * 40 in page.locator("#status-list").text_content()
    page.locator("#pb9dir").fill("/fixtures/edited-pb9")
    page.locator("#pbname").fill("unsaved-name")
    control["runtime"]["installation_incomplete"] = True
    page.evaluate("loadPb9Runtime()")
    page.wait_for_function("document.getElementById('summary-pb9').textContent === 'PB9 incomplete'")
    assert page.locator("#pb9dir").input_value() == "/fixtures/edited-pb9"
    assert page.locator("#pbname").input_value() == "unsaved-name"
    assert "Installation or update is incomplete" in page.locator("#issues").text_content()
    page.locator("#save-pb9-setup-btn").click()
    page.wait_for_function("document.getElementById('banner').textContent.includes('PB9 paths saved')")
    assert control["save_calls"] == [{"pb9dir": "/fixtures/edited-pb9", "pb9venv": "/fixtures/venv_pb9/bin/python"}]
    assert page.locator("#pb9dir").input_value() == "/fixtures/edited-pb9"
    assert page.locator("#pbname").input_value() == "unsaved-name"
    page.reload()
    page.wait_for_function("document.getElementById('summary-pb9').textContent === 'PB9 incomplete'")
    assert page.locator("#section-setup").is_visible()
    assert page.url.endswith("#setup")
    assert not errors


def test_pb9_optional_and_rejected_path_save(welcome):
    """Optional PB9 is honest and a rejected write retains correctable user values."""
    page, control, errors = welcome
    control["runtime"] = {"configured": False, "detected": False, "errors": [], "warnings": []}
    control["save_error"] = True
    page.goto("http://welcome.test/api/auth/main_page#setup")
    page.wait_for_function("document.getElementById('summary-pb9').textContent === 'PB9 optional'")
    page.locator("#pb9dir").fill("/fixtures/rejected")
    page.locator("#pb9venv").fill("/fixtures/bad/bin/python")
    page.locator("#save-pb9-setup-btn").click()
    page.wait_for_function("document.getElementById('banner').textContent.includes('PB9 paths rejected')")
    assert page.locator("#pb9dir").input_value() == "/fixtures/rejected"
    assert page.locator("#pb9venv").input_value() == "/fixtures/bad/bin/python"
    assert page.locator("#save-pb9-setup-btn").is_enabled()
    assert not errors


def test_pb9_auth_expiry_clears_detected_status(welcome):
    """A prior authenticated inspection cannot remain presented as current after expiry."""
    page, control, errors = welcome
    page.goto("http://welcome.test/api/auth/main_page")
    page.wait_for_function("document.getElementById('summary-pb9').textContent === 'PB9 detected'")
    page.evaluate("render({auth: {authenticated: false, password_required: true}, setup: {}})")
    assert page.locator("#summary-pb9").text_content() == "PB9 login required"
    assert "b" * 40 not in page.locator("#status-list").text_content()
    assert page.locator("#save-pb9-setup-btn").is_disabled()
    assert not errors


@pytest.mark.parametrize("width,sidebar_width", [(1400, 160), (1400, 420), (700, 240)])
def test_pb9_welcome_layout(welcome, width, sidebar_width):
    """PB9 status and fields fit the shared sidebar limits and mobile viewport."""
    page, control, errors = welcome
    page.set_viewport_size({"width": width, "height": 900})
    page.goto("http://welcome.test/api/auth/main_page#setup")
    page.wait_for_function("document.getElementById('summary-pb9').textContent === 'PB9 detected'")
    page.evaluate("width => document.getElementById('sidebar').style.width = width + 'px'", sidebar_width)
    assert page.evaluate("document.getElementById('main-content').scrollWidth <= document.getElementById('main-content').clientWidth")
    assert page.evaluate("document.getElementById('sidebar-toolbar').scrollWidth <= document.getElementById('sidebar-toolbar').clientWidth")
    assert not errors
