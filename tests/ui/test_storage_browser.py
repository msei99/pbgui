"""Storage page through the real menu, router, selection, preview and reload."""

from __future__ import annotations

import functools
import json
import time
import traceback
from pathlib import Path
from urllib.parse import urlparse

import pytest
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

import api.storage as storage_api
import storage_registry as registry
from api.page_templates import render_page_urls
from browser_driver import sync_browser
from storage_controller import StorageController

ROOT = Path(__file__).resolve().parents[2]
PREFIX = "/mounted"
DAY = 86400


def _concise(function):
    """Keep driver diagnostics short."""
    @functools.wraps(function)
    def wrapped(*args, **kwargs):
        try:
            return function(*args, **kwargs)
        except Exception:
            pytest.fail(traceback.format_exc()[-1600:], pytrace=False)
    return wrapped


def _item(item_id, size, age_days=40, blocked=None):
    stamp = time.time() - age_days * DAY
    return {"id": item_id, "label": item_id, "paths": [f"/pb7/optimize_results/{item_id}"], "size": size, "files": 3,
            "mtime": stamp, "sort_mtime": stamp, "blocked": blocked, "meta": {}}


def _snapshot():
    tree = {"name": "PB7", "path": "/pb7", "size": 5 * 1024 ** 3, "files": 10, "mtime": time.time(), "children": [
        {"name": "optimize_results", "path": "/pb7/optimize_results", "size": 3 * 1024 ** 3, "files": 5, "mtime": time.time(),
         "children": [{"name": "run_a", "path": "/pb7/optimize_results/run_a", "size": 2 * 1024 ** 3, "files": 2, "mtime": 0}]},
    ]}
    return {
        "version": registry.SNAPSHOT_VERSION, "started_at": time.time() - 30, "finished_at": time.time() - 20,
        "roots": [{"id": "pb7", "label": "PB7", "path": "/pb7", "tree": tree}],
        "mounts": [],
        "categories": {
            "logs_jobs": {"items": [_item("old.log", 2048), _item("new.log", 1024, age_days=1)]},
            "pb7_optimize_results": {"items": [
                _item("run_a", 2 * 1024 ** 3), _item("run_b", 1024 ** 3), _item("run_c", 512 * 1024 ** 2),
                _item("run_live", 1024, age_days=0, blocked="Changed within the last 24 hours"),
            ]},
            "protected_core": {"items": [_item("PBGui database", 4096, blocked="Protected")]},
        },
    }


@pytest.fixture
def storage_page(tmp_path, monkeypatch):
    """Serve the real page and router with a synthetic snapshot; never spawn a worker."""
    controller = StorageController(tmp_path / "pbgui")
    controller._snapshot = _snapshot()
    controller._loaded = True
    controller._generation = 1
    started = []

    async def fake_start(mode, payload):
        started.append((mode, payload))
        return {"id": "job", "mode": mode, "started_at": time.time()}

    monkeypatch.setattr(controller, "_start_job", fake_start)
    monkeypatch.setattr(storage_api, "_controller", controller)
    monkeypatch.setattr(storage_api, "live_mounts", lambda ctrl: [
        {"mount": "/", "total": 100 * 1024 ** 3, "used": 86 * 1024 ** 3, "free": 14 * 1024 ** 3, "percent": 86.0, "roots": ["pbgui", "pb7"]}])
    storage_api._cache.clear()
    app = FastAPI()
    app.include_router(storage_api.router, prefix="/api/storage")
    app.dependency_overrides[storage_api.require_auth] = lambda: object()
    client = TestClient(app)
    calls, errors = [], []
    with sync_browser() as driver:
        browser = driver.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": 1280, "height": 900})
        page.set_default_timeout(7000)
        page.on("pageerror", lambda error: errors.append(str(error)))
        page.add_init_script("window.WebSocket=class {constructor(){this.readyState=1;}close(){}send(){}};")

        def respond(route):
            url = urlparse(route.request.url)
            path = url.path
            calls.append((route.request.method, path))
            if not path.startswith(PREFIX):
                route.fulfill(status=404, body="")
                return
            path = path.removeprefix(PREFIX)
            if path.startswith("/app/"):
                asset = ROOT / "frontend" / path.removeprefix("/app/")
                if asset.is_file():
                    route.fulfill(body=asset.read_bytes(), content_type={".js": "text/javascript", ".css": "text/css"}.get(asset.suffix, "application/octet-stream"))
                else:
                    route.fulfill(status=404, body="")
            elif path == "/start":
                route.fulfill(body='<nav id="topnav"></nav><script>window.PBGUI_NAV_CONFIG={authenticated:true,apiBase:"' + PREFIX + '/api/auth",current:"/"};</script><script src="' + PREFIX + '/app/pbgui_nav.js?v=test"></script>', content_type="text/html")
            elif path == "/api/storage/main_page":
                html = render_page_urls(Request({"type": "http", "root_path": PREFIX}), (ROOT / "frontend/storage.html").read_text(), "/api/storage")
                route.fulfill(body=html.replace("%%NAV_HASH%%", "test"), content_type="text/html")
            elif path.startswith("/api/storage/"):
                body = route.request.post_data
                response = client.request(route.request.method, path + ("?" + url.query if url.query else ""),
                                          content=body, headers={"Content-Type": "application/json"} if body else {})
                route.fulfill(status=response.status_code, body=response.content, content_type="application/json")
            elif path == "/api/help/index":
                route.fulfill(json=[{"file": "51_storage.md", "title": "Storage"}])
            elif path == "/api/help/content":
                route.fulfill(json={"content": (ROOT / "docs/help/51_storage.md").read_text()})
            elif path.endswith("/stream"):
                route.fulfill(status=204, body="")
            else:
                route.fulfill(json={})

        page.route("**/*", respond)
        page.goto("http://storage.test" + PREFIX + "/start")
        page.locator('.nav-group-btn[data-group="system"]').click()
        page.locator('.nav-item[data-page="system_storage"]').click()
        try:
            page.wait_for_function('window.PBGuiStorage && document.querySelectorAll("#reclaim-grid .stat").length === 4')
        except Exception as exc:
            pytest.fail(str(exc) + " page errors: " + repr(errors), pytrace=False)
        yield page, controller, started, calls, errors
        browser.close()


@_concise
def test_menu_overview_and_prefix(storage_page):
    """The real menu opens the mounted page; overview shows disk, tiers and tree."""
    page, _controller, _started, calls, errors = storage_page
    assert page.evaluate("location.pathname") == PREFIX + "/api/storage/main_page"
    assert "86% used" in page.evaluate("document.getElementById('disk-body').textContent")
    assert page.evaluate("document.getElementById('sb-disk').textContent") == "86%"
    assert page.evaluate("document.querySelectorAll('#tree-table tbody tr').length") == 1
    page.locator("#tree-table .tree-toggle").first.click()
    page.wait_for_function("document.querySelectorAll('#tree-table tbody tr').length === 2")
    storage_calls = [path for _method, path in calls if "/api/storage/" in path]
    assert storage_calls and all(path.startswith(PREFIX + "/api/storage/") for path in storage_calls)
    assert not [text for text in errors if "Plotly" not in text]
    assert "Refresh" not in page.evaluate("document.getElementById('page-body').innerText")
    assert page.evaluate("""Array.from(document.querySelectorAll('script[src],link[rel=stylesheet]'))
        .every(node => new URL(node.src || node.href).origin === location.origin)""")


@_concise
def test_review_selection_preview_and_delete(storage_page):
    """Drag-select review entries, skip blocked ones, preview and confirm deletion."""
    page, controller, started, calls, _errors = storage_page
    page.locator('#sidebar .sb-section[data-view="results"]').click()
    page.wait_for_function("document.querySelectorAll('#category-table tbody tr').length >= 1")
    row = page.locator("#category-table tbody tr", has_text="PB7 optimize results")
    row.locator("button", has_text="Choose entries").click()
    page.wait_for_function("document.querySelectorAll('#items-table tbody tr[data-index]').length === 4")
    assert page.evaluate("location.hash") == "#results/pb7_optimize_results"
    rows = page.locator("#items-table tbody tr")
    first = rows.nth(0).bounding_box()
    second = rows.nth(1).bounding_box()
    page.mouse.move(first["x"] + 20, first["y"] + first["height"] / 2)
    page.mouse.down()
    page.mouse.move(second["x"] + 20, second["y"] + second["height"] / 2)
    page.mouse.up()
    rows.nth(3).click()
    page.wait_for_function("document.querySelectorAll('#items-table tbody tr.selected').length === 2")
    assert "2 selected · 3.0 GB" == page.evaluate("document.getElementById('items-selection').textContent")
    page.reload()
    page.wait_for_function("document.querySelectorAll('#items-table tbody tr.selected').length === 2")
    assert page.evaluate("document.querySelector('#sidebar .sb-section.active').dataset.view") == "results"
    page.locator("#items-preview").click()
    page.wait_for_function("document.querySelector('[data-view-panel=\"preview\"]').classList.contains('is-active')")
    assert "Delete 2 entries" in page.evaluate("document.getElementById('preview-delete').textContent")
    page.locator("#preview-table tbody tr", has_text="run_b").click()
    page.wait_for_function("document.getElementById('preview-delete').textContent === 'Delete 1 entries (2.0 GB)'")
    assert page.evaluate("document.getElementById('preview-selection').textContent") == "1 of 2 listed entries selected"
    page.locator("#preview-delete").click()
    page.wait_for_function("document.getElementById('pbgui-dialog-ovl')?.classList.contains('visible')")
    page.locator("#pbgui-dialog-cancel").click()
    assert started == []
    page.locator("#preview-delete").click()
    page.wait_for_function("document.getElementById('pbgui-dialog-ovl')?.classList.contains('visible')")
    page.locator("#pbgui-dialog-accept").click()
    page.wait_for_function("!document.querySelector('[data-view-panel=\"preview\"]').classList.contains('is-active')")
    assert started and started[0][0] == "cleanup"
    selection = started[0][1]["selections"][0]
    assert selection["category"] == "pb7_optimize_results" and selection["item_ids"] == ["run_a"]


@_concise
def test_safe_preview_and_protected_rows(storage_page):
    """Safe categories preview with filters; protected categories offer no action."""
    page, _controller, _started, _calls, _errors = storage_page
    page.locator('#sidebar .sb-section[data-view="logs"]').click()
    row = page.locator("#category-table tbody tr", has_text="Market data job logs")
    page.wait_for_function("document.querySelector('#category-table tbody tr input[type=number]') !== null")
    row.locator("button", has_text="Preview").click()
    page.wait_for_function("document.getElementById('preview-delete').textContent.includes('Delete 1 entries')")
    page.locator("#preview-discard").click()
    page.locator('#sidebar .sb-section[data-view="backups"]').click()
    page.wait_for_function("document.getElementById('group-title').textContent === 'Backups & Leftovers'")
    protected = page.locator("#category-table tbody tr", has_text="Core data")
    assert protected.locator("button", has_text="Preview").count() == 0


@_concise
def test_retention_settings_save(storage_page):
    """Settings edits are saved through the real validation."""
    page, controller, _started, _calls, _errors = storage_page
    page.locator('#sidebar .sb-section[data-view="retention"]').click()
    page.wait_for_function("document.getElementById('scan-interval').value === '6'")
    page.locator("#scan-interval").fill("12")
    page.locator("#settings-save").click()
    page.wait_for_function("document.getElementById('settings-state').textContent === 'Saved'")
    assert controller.load_settings()["scan_interval_hours"] == 12


@_concise
def test_guide_and_sidebar_widths(storage_page):
    """Guide opens as overlay; the sidebar has no horizontal overflow at shared widths."""
    page, _controller, _started, _calls, _errors = storage_page
    page.locator("#pbgui-guide-btn").click()
    page.wait_for_function('document.getElementById("pbgui-shared-help-ovl")?.getAttribute("aria-hidden") === "false"')
    page.locator("#pbgui-shared-help-close").click()
    assert page.evaluate("location.pathname").endswith("/api/storage/main_page")
    for viewport, width in [(1280, 160), (1280, 240), (1280, 420), (750, 240)]:
        page.set_viewport_size({"width": viewport, "height": 900})
        page.evaluate('(width)=>document.getElementById("sidebar").style.width=width+"px"', width)
        assert page.evaluate("Array.from(document.querySelectorAll('#sidebar .sb-section')).every(b => b.scrollWidth <= b.clientWidth + 1)")
        assert page.evaluate("document.getElementById('main-content').scrollWidth <= document.getElementById('main-content').clientWidth + 1")


@_concise
def test_system_view_updates(storage_page):
    """The System view renders live values and process rows."""
    page, _controller, _started, _calls, _errors = storage_page
    page.locator('#sidebar .sb-section[data-view="system"]').click()
    page.wait_for_function("document.querySelectorAll('#system-grid .stat').length >= 6")
    assert "threads" in page.evaluate("document.getElementById('system-grid').textContent")
    assert page.evaluate("location.hash") == "#system"


@_concise
def test_history_chart_and_growth(storage_page):
    """History renders the chart and 7/30-day growth from daily points."""
    page, controller, _started, _calls, _errors = storage_page
    now = time.time()
    points = [
        {"date": "a", "ts": now - 40 * DAY, "mounts": [{"mount": "/", "used": 50 * 1024 ** 3, "total": 100 * 1024 ** 3}], "roots": {"pb7": 1024 ** 3}, "areas": {"PB7/caches": 512 * 1024 ** 2}},
        {"date": "b", "ts": now - 8 * DAY, "mounts": [{"mount": "/", "used": 60 * 1024 ** 3, "total": 100 * 1024 ** 3}], "roots": {"pb7": 2 * 1024 ** 3}, "areas": {"PB7/caches": 1024 ** 3}},
        {"date": "c", "ts": now, "mounts": [{"mount": "/", "used": 70 * 1024 ** 3, "total": 100 * 1024 ** 3}], "roots": {"pb7": 5 * 1024 ** 3}, "areas": {"PB7/caches": 1024 ** 3}},
    ]
    history = controller.state_dir / "history.json"
    history.parent.mkdir(parents=True, exist_ok=True)
    history.write_text(json.dumps(points))
    page.locator('#sidebar .sb-section[data-view="history"]').click()
    page.wait_for_function("document.querySelectorAll('#growth-table tbody tr').length === 2")
    rows = page.evaluate("Array.from(document.querySelectorAll('#growth-table tbody tr')).map(r => Array.from(r.cells).map(c => c.textContent))")
    assert rows[0] == ["PB7", "5.0 GB", "+3.0 GB", "+4.0 GB"]
    assert rows[1] == ["PB7/caches", "1.0 GB", "+0 B", "+512 MB"]
    page.wait_for_function("document.querySelector('#history-chart .main-svg') !== null")


@_concise
def test_tier_tiles_open_their_categories(storage_page):
    """Every tier tile lists its categories; a click opens the category entries."""
    page, _controller, _started, _calls, _errors = storage_page
    review = page.locator("#reclaim-grid .stat.review .tier-row", has_text="PB7 optimize results")
    assert "3.5 GB" in review.text_content()
    review.click()
    page.wait_for_function("document.querySelectorAll('#items-table tbody tr[data-index]').length === 4")
    assert page.evaluate("location.hash") == "#results/pb7_optimize_results"
    page.locator('#sidebar .sb-section[data-view="overview"]').click()
    protected = page.locator("#reclaim-grid .stat.protected .tier-row", has_text="Core data")
    protected.click()
    page.wait_for_function("location.hash === '#backups/protected_core'")


@_concise
def test_live_disk_values_update_without_new_snapshot(storage_page, monkeypatch):
    """Status polls refresh disk usage and the sidebar badge even when the snapshot is unchanged."""
    page, _controller, _started, _calls, _errors = storage_page
    page.locator("#tree-table .tree-toggle").first.click()
    page.wait_for_function("document.querySelectorAll('#tree-table tbody tr').length === 2")
    monkeypatch.setattr(storage_api, "live_mounts", lambda ctrl: [
        {"mount": "/", "total": 100 * 1024 ** 3, "used": 91 * 1024 ** 3, "free": 9 * 1024 ** 3, "percent": 91.0, "roots": ["pbgui", "pb7"]}])
    storage_api._cache.clear()
    page.evaluate("document.dispatchEvent(new Event('visibilitychange'))")
    page.wait_for_function("document.getElementById('disk-body').textContent.includes('91% used')")
    assert page.evaluate("document.getElementById('sb-disk').textContent") == "91%"
    assert "9.0 GB free" in page.evaluate("document.getElementById('disk-body').textContent")
    assert page.evaluate("PBGuiStorage.state.generation") == 1
    assert page.evaluate("document.querySelectorAll('#tree-table tbody tr').length") == 2
