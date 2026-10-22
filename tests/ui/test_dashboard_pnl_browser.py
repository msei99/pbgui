"""Real-menu PNL rendering with local Plotly and synthetic HTTP/WebSocket data."""

import json
import re
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pytest
from playwright.sync_api import expect, sync_playwright

ROOT = Path(__file__).resolve().parents[2]


def assert_daily_svg_labels(chart, first_value):
    """Check rendered SVG labels and their date mapping, including the true zero day."""
    expect(chart.locator(".barlayer text.bartext").filter(has_text=re.compile(r"^0\.00$"))).to_be_visible()
    assert chart.evaluate("""e => Array.from(e.querySelectorAll('.barlayer .point')).map(p => ({
        date: e.data[0].x[p.__data__.i], text: p.textContent
    }))""") == [
        {"date": "2026-09-01", "text": f"{first_value:.2f}"},
        {"date": "2026-09-02", "text": ""},
        {"date": "2026-09-03", "text": "3.00"},
        {"date": "2026-09-04", "text": "0.00"},
    ]


@pytest.mark.parametrize("editing", [False, True])
def test_pnl_gaps_mode_changes_and_live_updates(editing):
    """Keep invalid days as gaps in view and editor, including in-place updates."""
    from starlette.requests import Request
    from api import dashboard

    name = "PNL gaps"
    names = ["Alpha", name, "Beta", "Gamma"]
    config = {
        "name": name, "rows": 1, "cols": 1, "dashboard_type_1_1": "PNL",
        "dashboard_pnl_users_1_1": ["alice"],
        "dashboard_pnl_period_1_1": "CUSTOM:2026-09-01:2026-09-04",
        "dashboard_pnl_mode_1_1": "bar",
    }
    errors, sockets, queries, asset_versions = [], [], [], []
    pending = {}
    revision = 0
    with sync_playwright() as driver:
        browser = driver.chromium.launch()
        page = browser.new_page(viewport={"width": 1280, "height": 900})
        page.set_default_timeout(10000)
        page.on("pageerror", lambda error: errors.append(str(error)))

        def respond(route):
            """Intercept every request; no runtime data or real service is contacted."""
            url = urlparse(route.request.url)
            path, query = url.path, parse_qs(url.query)
            if path == "/app/start.html":
                route.fulfill(content_type="text/html", body='<nav id="topnav"></nav><script>window.PBGUI_NAV_CONFIG={current:"welcome",authenticated:true};</script><script src="/app/pbgui_nav.js"></script>')
            elif path == "/api/dashboard/main_page":
                source = (ROOT / "frontend/dashboard_main.html").read_text()
                for key, value in {
                    "API_BASE": "/api", "VERSION": "test", "CURRENT": "",
                    "DASHBOARDS_JSON": json.dumps(names), "SERIAL": "1", "NAV_HASH": "test",
                }.items():
                    source = source.replace("%%" + key + "%%", value)
                route.fulfill(content_type="text/html", body=source)
            elif path == "/api/dashboard/editor_page":
                response = dashboard.get_editor_page(
                    Request({"type": "http", "root_path": ""}), name=name, api_base="",
                    view_only=query.get("view_only") == ["1"],
                    standalone=query.get("standalone") == ["1"], session=object(),
                )
                route.fulfill(content_type="text/html", body=response.body)
            elif path == "/api/dashboards":
                route.fulfill(json={"dashboards": names})
            elif path in ("/api/dashboards/PNL%20gaps", "/api/dashboards/PNL gaps"):
                route.fulfill(json={"name": name, "config": config})
            elif path == "/api/dashboard/pending_full":
                if route.request.method == "POST":
                    pending.update(route.request.post_data_json)
                    route.fulfill(json={"status": "ok"})
                else:
                    route.fulfill(json={"found": bool(pending), "config": pending})
            elif path == "/api/dashboard/users":
                route.fulfill(json={"users": ["alice", "bob"]})
            elif path == "/api/dashboard/pnl_data":
                queries.append(query)
                route.fulfill(json={
                    "mode": query["mode"][0], "from_date": "2026-09-01", "to_date": "2026-09-04",
                    "bars": [
                        {"date": "2026-09-01", "income": 5 + revision, "cum": 5 + revision},
                        {"date": "2026-09-02", "income": None, "cum": None},
                        {"date": "2026-09-03", "income": "3", "cum": str(8 + revision)},
                        {"date": "2026-09-04", "income": 0, "cum": 8 + revision},
                    ],
                })
            elif path == "/api/help/index":
                route.fulfill(json=[{"file": "33_dashboard", "title": "Dashboard"}])
            elif path == "/api/help/content":
                lang = "help_de" if query.get("lang") == ["DE"] else "help"
                route.fulfill(json={"content": (ROOT / "docs" / lang / "33_dashboard.md").read_text()})
            elif path.startswith("/app/"):
                asset = ROOT / "frontend" / path.removeprefix("/app/")
                if asset.name == "dashboard_render.js":
                    asset_versions.append(query.get("v"))
                route.fulfill(body=asset.read_bytes() if asset.is_file() else b"",
                              content_type="text/css" if asset.suffix == ".css" else "text/javascript")
            elif path.endswith("/stream"):
                route.fulfill(status=204, body="")
            else:
                route.fulfill(json={})

        page.route("**/*", respond)
        page.route_web_socket("**/ws/dashboard", lambda socket: sockets.append(socket))
        page.goto("http://dashboard.test/app/start.html")
        page.locator(".nav-group-btn", has_text="Information").click()
        page.locator('.nav-item[data-page="dashboards"]').click()
        page.locator('.sb-item[data-name="PNL gaps"]').click()
        page.locator("#sidebar-search").fill("PNL")
        if editing:
            page.locator("#sb-edit").click()
        frame = page.frame_locator("#content-frame")
        chart = frame.locator(".dt-chart")
        mode = frame.locator(".dt-meta-controls select").first
        expect(chart).to_have_class("dt-chart js-plotly-plot")
        expect(mode).to_have_value("bar")
        assert chart.evaluate("e => e.data[0].y") == [5, None, 3, 0]
        assert chart.evaluate("e => e.data[0].text") == ["5.00", "", "3.00", "0.00"]
        expect(chart.locator(".barlayer")).to_contain_text("5.00")
        expect(chart.locator(".barlayer")).to_contain_text("3.00")
        assert_daily_svg_labels(chart, 5)
        mode.select_option("line")
        page.wait_for_function('() => document.querySelector("#content-frame").contentDocument.querySelector(".dt-chart").data[0].type === "scatter"')
        assert chart.evaluate("e => e.data[0].y") == [5, None, 8, 8]
        assert chart.evaluate("e => e.data[0].connectgaps") is False
        expect(chart.locator(".scatterlayer")).to_be_visible()
        expect(chart.locator(".scatterlayer path.js-line")).to_have_count(2)
        chart.evaluate('(e) => {window.pnlChartBefore=e; Plotly.relayout(e, {"xaxis.range":["2026-09-01","2026-09-04"],"yaxis.range":[-1,12]});}')
        mode.blur()
        revision = 1
        sockets[-1].send(json.dumps({"type": "income_updated", "user": "alice"}))
        page.wait_for_function('() => document.querySelector("#content-frame").contentDocument.querySelector(".dt-chart").data[0].y[0] === 6')
        assert chart.evaluate("e => e === window.pnlChartBefore")
        assert chart.evaluate("e => e.data[0].y") == [6, None, 9, 9]
        assert chart.evaluate("e => e.layout.yaxis.range") == [-1, 12]
        expect(mode).to_have_value("line")
        mode.select_option("bar")
        assert_daily_svg_labels(chart, 6)
        assert chart.evaluate("e => e.data[0].y") == [6, None, 3, 0]
        assert chart.evaluate("e => e.layout.yaxis.range") == [-1, 12]
        # Exercise Plotly.react in daily mode as well as the first bar render.
        revision = 2
        mode.blur()
        sockets[-1].send(json.dumps({"type": "income_updated", "user": "alice"}))
        page.wait_for_function('() => document.querySelector("#content-frame").contentDocument.querySelector(".dt-chart").data[0].y[0] === 7')
        assert_daily_svg_labels(chart, 7)
        assert chart.evaluate("e => e === window.pnlChartBefore")
        assert chart.evaluate("e => e.layout.yaxis.range") == [-1, 12]
        expect(page.locator("#sidebar-search")).to_have_value("PNL")
        assert queries[-1]["users"] == ["alice"]
        assert queries[-1]["period"] == ["CUSTOM:2026-09-01:2026-09-04"]
        assert parse_qs(urlparse(page.url).query)["current"] == [name]
        version = frame.locator("body").evaluate("() => DashRender.VERSION")
        assert asset_versions and all(value == [version] for value in asset_versions)

        # Guide opens independently of the real navigation destination.
        destination = page.url
        page.locator("#pbgui-guide-btn").click()
        expect(page.locator("#pbgui-shared-help-ovl")).to_have_class("visible")
        assert page.url == destination
        page.locator("#pbgui-shared-help-ovl .ovl-close").click()
        # Browser reload restores the selected dashboard; new response still contains the gap.
        page.reload()
        expect(chart).to_have_class("dt-chart js-plotly-plot")
        expect(page.locator("#sidebar-search")).to_have_value("PNL")
        expect(page.locator('.sb-item.active')).to_have_attribute("data-name", name)
        assert chart.evaluate("e => e.data[0].y[1]") is None
        assert_daily_svg_labels(chart, 7)
        assert not errors
        browser.close()
