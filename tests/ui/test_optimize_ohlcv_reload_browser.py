"""Verify the latest authoritative optimizer edits across isolated browser reloads.

The previous two-conflicting-drafts expectation was superseded by the user's
single-current-config contract. Its original artifact is retained under
/tmp/pbgui-495-496-retained/single-state-PBFnDm/.
"""

import importlib.util
import json
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[2]
_fixture_spec = importlib.util.spec_from_file_location(
    "optimizer_reload_browser_fixture", ROOT / "tests/ui/test_optimizer_log_layout_browser.py"
)
_fixture_module = importlib.util.module_from_spec(_fixture_spec)
_fixture_spec.loader.exec_module(_fixture_module)
log_page = _fixture_module.log_page


@pytest.mark.parametrize("order", ["raw_then_form", "form_then_raw"])
@pytest.mark.parametrize("same_field", [False, True])
@pytest.mark.parametrize("settle", [False, True])
def test_conflicting_editor_draft_survives_browser_reload(log_page, order, same_field, settle):
    """Latest same-field edits and disjoint edits reload as one coherent config."""
    page, _, _, errors, _, _, _ = log_page
    config = json.loads((ROOT / "tests/fixtures/pb8_october_template.json").read_text())["config"]
    config["backtest"].update(
        starting_balance=1000,
        exchanges=["binance", "bybit"],
        start_date="2021-04-20",
        end_date="2026-07-21",
    )
    config["live"]["approved_coins"] = {"long": ["HYPE"], "short": []}
    page.route("**/metadata", lambda route: route.fulfill(json={
        "backend_options": ["pymoo"], "optimize": {"backend": "pymoo"},
        "pymoo_algorithm_options": ["nsga2"],
    }))
    page.route("**/configs/new-config", lambda route: route.fulfill(json={"config": config}))
    page.route("**/coins/status", lambda route: route.fulfill(json={
        "symbols": ["HYPE"], "catalog": [], "statuses": {},
    }))
    page.reload()
    page.wait_for_function("!state.initialLoadPending")
    page.locator('[data-panel="configs"]').click()
    page.locator("#btn-new-config").click()
    page.wait_for_function("document.getElementById('opted-starting-balance')?.value === '1000'")
    page.wait_for_timeout(500)
    before = page.evaluate('''({order, sameField}) => {
        const raw = document.getElementById('opted-raw-json');
        const form = document.getElementById(sameField ? 'opted-starting-balance' : 'opted-start-date');
        const editRaw = () => {
            const config = JSON.parse(raw.value);
            config.backtest.starting_balance = 2345;
            raw.value = JSON.stringify(config, null, 2);
            raw.dispatchEvent(new Event('input', {bubbles: true}));
        };
        const editForm = () => {
            form.value = sameField ? '3456' : '2022-02-02';
            form.dispatchEvent(new Event('input', {bubbles: true}));
            form.dispatchEvent(new Event('change', {bubbles: true}));
        };
        if (order === 'raw_then_form') { editRaw(); editForm(); }
        else { editForm(); editRaw(); }
        return {raw: JSON.parse(raw.value).backtest,
            balance: document.getElementById('opted-starting-balance').value,
            date: document.getElementById('opted-start-date').value};
    }''', {"order": order, "sameField": same_field})
    expected_balance = 3456 if same_field and order == "raw_then_form" else 2345
    expected_date = "2021-04-20" if same_field else "2022-02-02"
    assert before["raw"]["starting_balance"] == expected_balance
    assert before["balance"] == str(expected_balance)
    assert before["raw"]["start_date"] == before["date"] == expected_date
    if settle:
        page.wait_for_timeout(600)
    assert page.locator("#opted-starting-balance").input_value() == str(expected_balance)
    location = page.url
    assert "open_draft=1" in location and location.endswith("#configs")
    page.reload()
    page.wait_for_function(
        "document.getElementById('opted-starting-balance')?.value === " + json.dumps(str(expected_balance))
    )
    assert page.url == location
    assert page.locator("#opted-starting-balance").is_visible()
    current = page.evaluate("JSON.parse(document.getElementById('opted-raw-json').value).backtest")
    assert current["starting_balance"] == expected_balance
    assert current["start_date"] == page.locator("#opted-start-date").input_value() == expected_date
    assert "conflicting edits" not in page.locator("#opted-ohlcv-start-status").inner_text()
    assert not errors
