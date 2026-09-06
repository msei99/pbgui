"""Regression tests for browser cookie authentication and safe page URLs."""

from __future__ import annotations

import re
from pathlib import Path

import pytest
from starlette.requests import Request

import api.coin_data as coin_data_api
import api.logging as logging_api
import api.market_data as market_data_api
import api.services as services_api


ROOT = Path(__file__).resolve().parents[1]
FRONTEND_FILES = sorted((ROOT / "frontend").glob("*.html")) + sorted(
    (ROOT / "frontend" / "js").glob("*.js")
) + [ROOT / "frontend" / "pbgui_nav.js"]


def _request(root_path: str = "/mounted") -> Request:
    """Build a request whose untrusted authority must never enter page output."""
    return Request({
        "type": "http",
        "method": "GET",
        "scheme": "https",
        "server": ("127.0.0.1", 8000),
        "client": ("127.0.0.1", 1234),
        "path": f"{root_path}/page",
        "root_path": root_path,
        "query_string": b"",
        "headers": [(b"host", b"attacker.example")],
    })


class _CookieOnlySession:
    """Fail a rendering test if server code attempts to read session contents."""

    def __getattr__(self, name: str) -> object:
        """Reject access to every server-side session field."""
        raise AssertionError(f"session field accessed: {name}")


@pytest.mark.parametrize("path", FRONTEND_FILES, ids=lambda path: str(path.relative_to(ROOT)))
def test_frontend_never_transports_pbgui_session_tokens(path: Path) -> None:
    """Browser sources contain no legacy token globals, placeholders, DOM transport, or bearer headers."""
    source = path.read_text(encoding="utf-8")
    forbidden = {
        "token placeholder": re.compile(r"%%TOKEN%%", re.IGNORECASE),
        "token global": re.compile(r"\b(?:var|let|const)\s+(?:TOKEN|API_TOKEN)\b|window\.(?:TOKEN|API_TOKEN)\b"),
        "token DOM attribute": re.compile(r"data-token\s*=|getAttribute\(\s*['\"]data-token", re.IGNORECASE),
        "bearer header": re.compile(r"Authorization[^\n]{0,100}Bearer|Bearer[^\n]{0,100}Authorization", re.IGNORECASE),
    }
    violations = [label for label, pattern in forbidden.items() if pattern.search(source)]
    assert not violations, f"{path.relative_to(ROOT)} contains: {', '.join(violations)}"


def test_backend_never_injects_session_tokens_into_browser_content() -> None:
    """API source must not place a session token in HTML replacements or query parameters."""
    violations: list[str] = []
    html_injection = re.compile(r"\.replace\([^)]*session\.token", re.IGNORECASE | re.DOTALL)
    token_query = re.compile(r"['\"]token['\"]\s*:\s*session\.token|[?&]token=", re.IGNORECASE)
    for path in sorted((ROOT / "api").glob("*.py")):
        source = path.read_text(encoding="utf-8")
        if html_injection.search(source) or token_query.search(source):
            violations.append(str(path.relative_to(ROOT)))
    assert not violations, "Browser-visible session token transport found in: " + ", ".join(violations)


@pytest.mark.parametrize(("renderer", "api_path"), [
    (logging_api.get_main_page, "/mounted/api/logging"),
    (services_api.get_main_page, "/mounted/api/services"),
    (coin_data_api.get_main_page, "/mounted/api/coin-data"),
])
def test_issue_pages_use_cookie_only_mount_aware_same_origin_urls(renderer, api_path: str) -> None:
    """Affected page routes ignore Host and never inspect or render the authenticated token."""
    response = renderer(_request(), _CookieOnlySession())
    html = response.body.decode("utf-8")
    assert api_path in html
    assert "attacker.example" not in html
    assert "%%TOKEN%%" not in html
    assert "Authorization" not in html
    assert response.headers["cache-control"] == "no-store"


def test_market_data_fragments_use_cookie_only_mount_aware_urls() -> None:
    """Embedded Market Data views retain no token or request-authority transport."""
    status_html = market_data_api._render_market_data_status_html(_request(), "binance")
    actions_html = market_data_api._render_hl_data_actions_html(_request())
    for html in (status_html, actions_html):
        assert 'data-api-base="/mounted/api"' in html
        assert "data-token" not in html
        assert "attacker.example" not in html
        assert "Authorization" not in html
        assert "Bearer" not in html


def test_affected_frontend_requests_use_same_origin_credentials() -> None:
    """Shared and page-local request helpers explicitly retain the HttpOnly cookie."""
    contracts = {
        "frontend/pbgui_nav.js": "opts.credentials = 'same-origin'",
        "frontend/js/pbgui_dialogs.js": "credentials: 'same-origin'",
        "frontend/js/editor_shared.js": "options.credentials = 'same-origin'",
        "frontend/js/optimize_preset_builder.js": "credentials: 'same-origin'",
        "frontend/js/shared_help_overlay.js": "credentials: 'same-origin'",
        "frontend/services_monitor.html": "opts.credentials = 'same-origin'",
        "frontend/coin_data.html": "credentials: 'same-origin'",
        "frontend/db_tools.html": "credentials: 'same-origin'",
        "frontend/market_data_main.html": "credentials: 'same-origin'",
        "frontend/market_data_status.html": "credentials: 'same-origin'",
        "frontend/hl_data_actions.html": "opts.credentials = 'same-origin'",
        "frontend/v7_backtest.html": "opts.credentials = 'same-origin'",
        "frontend/v7_optimize.html": "opts.credentials = 'same-origin'",
        "frontend/help.html": "credentials: 'same-origin'",
        "frontend/gap_heatmap.html": "credentials: 'same-origin'",
    }
    for relative_path, marker in contracts.items():
        source = (ROOT / relative_path).read_text(encoding="utf-8")
        assert marker in source, f"{relative_path} is missing its cookie-auth request contract"


def test_help_renderers_reject_active_urls_and_build_search_results_with_dom_nodes() -> None:
    """Help documents and index metadata cannot become executable search-result markup."""
    for relative_path in ("frontend/help.html", "frontend/js/shared_help_overlay.js"):
        source = (ROOT / relative_path).read_text(encoding="utf-8")
        assert "parsed.protocol === 'http:' || parsed.protocol === 'https:'" in source
        assert "title.textContent = String(result.title || '')" in source
        assert "appendHighlighted(snippetElement" in source
        assert "replaceChildren(container)" in source
        assert "snippet.replace(expr, '<mark>$1</mark>')" not in source
        assert "' + result.title + '" not in source


def test_shared_clients_derive_mounted_paths_without_request_authorities() -> None:
    """Navigation, Help, and Optimize clients keep the trusted ASGI prefix on browser requests."""
    nav = (ROOT / "frontend" / "pbgui_nav.js").read_text(encoding="utf-8")
    help_overlay = (ROOT / "frontend" / "js" / "shared_help_overlay.js").read_text(encoding="utf-8")
    presets = (ROOT / "frontend" / "js" / "optimize_preset_builder.js").read_text(encoding="utf-8")
    assert "if (prefix === undefined) prefix = window.BASE_PREFIX" in nav
    assert "window.location.pathname.lastIndexOf('/app/')" in nav
    assert "appPath('/api/help/" in help_overlay
    assert "appPath('/app/vendor/" in help_overlay
    assert "basePrefix() + '/api/optimize-'" in presets
    assert "basePrefix() + '/api/backtest-'" in presets
    assert "window.location.origin + '/api/" not in presets
