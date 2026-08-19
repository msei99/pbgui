"""Frontend contracts for the shared PB7/PB8 Balance Calculator."""

from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def test_balance_calculator_lives_under_information_navigation() -> None:
    """The shared calculator must have one Information menu identity and route."""
    nav = (ROOT / "frontend" / "pbgui_nav.js").read_text(encoding="utf-8")
    page = (ROOT / "frontend" / "balance_calc.html").read_text(encoding="utf-8")
    api = (ROOT / "api" / "balance_calc.py").read_text(encoding="utf-8")
    information = nav[nav.index("{ id: 'information'") : nav.index("{ id: 'pbv7'")]
    pbv7 = nav[nav.index("{ id: 'pbv7'") : nav.index("{ id: 'pbv8'")]

    assert "info_balance_calc" in information
    assert "info_balance_calc" not in pbv7
    assert "v7_balance_calc" not in nav
    assert "'info_balance_calc':  '/api/balance-calc/main_page'" in nav
    assert "'info_balance_calc':           '38_balance_calc'" in nav
    assert "current: 'info_balance_calc'" in page
    assert "PBv7 Balance Calculator" not in page
    assert "PB7 / PB8 Instance:" in page
    assert "%%TOKEN%%" not in page
    assert "session.token" not in api
    assert "credentials: 'same-origin'" in page
    assert "JSON.stringify({ name: inst.name, version: inst.version })" in page
    assert "inst.version === 'v8' ? 'PB8' : 'PB7'" in page
    assert "config_file" not in page


def test_pb8_backtest_handoffs_use_shared_balance_calculator() -> None:
    """PB8 editor and result actions must remain visible and resolve the canonical API."""
    shared = (ROOT / "frontend" / "js" / "editor_shared.js").read_text(encoding="utf-8")
    adapter = (ROOT / "frontend" / "js" / "backtest_editor_adapter.js").read_text(encoding="utf-8")
    page = (ROOT / "frontend" / "v7_backtest.html").read_text(encoding="utf-8")

    assert ".replace(/\\/api\\/backtest-v8$/, '/api/balance-calc')" in shared
    for action in ("goBalanceCalc", "goCalcBalance", "goBalanceCalculatorFromResult", "calcBalanceFromResult"):
        assert f"'{action}'" not in adapter
    assert "openBalanceCalculatorWithConfig(cfg" in page
    assert "/app/js/editor_shared.js?v=14" in page
    assert "/app/js/backtest_editor_adapter.js?v=10" in page


def test_pb8_run_sidebar_uses_shared_balance_calculator() -> None:
    """PB8 Run must show both calculator actions and resolve its API base."""
    shared = (ROOT / "frontend" / "js" / "editor_shared.js").read_text(encoding="utf-8")
    adapter = (ROOT / "frontend" / "js" / "run_editor_adapter.js").read_text(encoding="utf-8")
    page = (ROOT / "frontend" / "v7_edit.html").read_text(encoding="utf-8")

    assert ".replace(/\\/api\\/v8$/, '/api/balance-calc')" in shared
    assert 'data-v7-only onclick="goBalanceCalc()"' not in page
    assert 'data-v7-only onclick="calculateBalance()"' not in page
    assert 'onclick="goBalanceCalc()"' in page
    assert 'onclick="calculateBalance()"' in page
    assert "supportsBalanceCalculator: true" in adapter
    assert "/app/js/editor_shared.js?v=14" in page
    assert "/app/js/run_editor_adapter.js?v=9" in page


def test_pb7_run_links_directly_to_shared_calculator() -> None:
    """PB7 Run must provide a direct instance handoff to the Information page."""
    source = (ROOT / "frontend" / "v7_run.html").read_text(encoding="utf-8")

    assert 'data-balance="' in source
    assert "function openBalanceCalculator(name)" in source
    assert "'/api/balance-calc/main_page?'" in source
    assert "instance_version: 'v7'" in source
