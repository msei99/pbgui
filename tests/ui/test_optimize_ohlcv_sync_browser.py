"""Exercise pending optimizer editor changes with isolated browser requests."""

import asyncio
import copy
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path

import pytest

from api import pb8_ohlcv_runtime_helper


ROOT = Path(__file__).resolve().parents[2]
_fixture_spec = importlib.util.spec_from_file_location(
    "optimizer_log_browser_fixture", ROOT / "tests/ui/test_optimizer_log_layout_browser.py"
)
_fixture_module = importlib.util.module_from_spec(_fixture_spec)
_fixture_spec.loader.exec_module(_fixture_module)
log_page = _fixture_module.log_page


def _open_sync_editor(log_page, hsl_draft=False):
    """Open a disposable PB8 editor using only intercepted requests."""
    page, _, _, errors, _, _, _ = log_page
    config = json.loads((ROOT / "tests/fixtures/pb8_october_template.json").read_text())["config"]
    config["backtest"].update(starting_balance=1000, exchanges=["binance", "bybit"],
                              start_date="2021-04-20", end_date="2026-07-21")
    config["live"].update(strategy_kind="ema_anchor", approved_coins={"long": ["HYPE"], "short": []})
    param_status = {}
    if hsl_draft:
        config["bot"]["long"]["hsl"].update(enabled=True, restart_after_red_policy=None)
        config["bot"]["short"].pop("hsl", None)
        config["optimize"]["fixed_runtime_overrides"] = {}
        # Representative migration annotations, never production configuration.
        def migration_leaves(value, prefix=""):
            """Use real template paths for representative added-default annotations."""
            if isinstance(value, dict):
                for key, item in value.items():
                    yield from migration_leaves(item, prefix + "." + key if prefix else key)
            else:
                yield prefix, value

        leaves = [(path, value) for path, value in migration_leaves(config)
                  if path != "bot.long.hsl.restart_after_red_policy"]
        changes = {path: {"added": True, "after": value} for path, value in leaves[:95]}
        changes["bot.long.hsl.restart_after_red_policy"] = {"before": "threshold", "after": None}
        param_status = {"long": {"hsl.restart_after_red_policy": "review_line"}, "config_changes": changes}
    page.route("**/metadata", lambda route: route.fulfill(json={
        "backend_options": ["pymoo"], "optimize": {"backend": "pymoo"},
        "pymoo_algorithm_options": ["nsga2"],
    }))
    page.route("**/configs/new-config", lambda route: route.fulfill(json={"config": config, "param_status": param_status}))
    page.route("**/coins/status", lambda route: route.fulfill(json={"symbols": ["HYPE"], "catalog": [], "statuses": {}}))
    page.reload()
    page.wait_for_function("!state.initialLoadPending")
    page.locator('[data-panel="configs"]').click()
    page.locator("#btn-new-config").click()
    page.wait_for_function("document.getElementById('opted-start-date')?.value === '2021-04-20' && _optMarketStatusVerified")
    return page, errors


@pytest.mark.parametrize("edit", ["bot", "coin", "lookup"])
def test_completed_edit_survives_immediate_disjoint_raw_input(log_page, edit):
    """Bot/coin callbacks and a successful lookup commit before the next Raw edit."""
    page, errors = _open_sync_editor(log_page)
    observed = page.evaluate('''async edit => {
        if (edit === 'bot') {
            const bot = document.getElementById('opted-bot-long');
            const config = JSON.parse(bot.value);
            config.risk.n_positions = 3;
            bot.value = JSON.stringify(config);
            bot.dispatchEvent(new Event('input', {bubbles: true}));
        } else if (edit === 'coin') {
            // The real tag's user handler invokes the multiselect callback.
            document.querySelector('#ms-opt-app-long .ms-x').click();
        } else {
            const config = await collectOptimizeStartLookupConfig();
            const run = {mode: 'earliest', signature: optimizeOhlcvStartLookupSignature(config)};
            _optOhlcvStartDateRun = run;
            await applyOptimizeStartDateResult(run, {result: {start_date_options: {
                earliest: {available: true, start_date: '2025-01-02'}, known_pair_count: 2, pair_count: 2
            }}});
            clearOptimizeStartDateProgress();
        }
        const raw = document.getElementById('opted-raw-json');
        const config = JSON.parse(raw.value);
        config.backtest.starting_balance = 2345;
        raw.value = JSON.stringify(config);
        raw.dispatchEvent(new Event('input', {bubbles: true}));
        return JSON.parse(raw.value);
    }''', edit)
    assert observed["backtest"]["starting_balance"] == 2345
    if edit == "bot":
        assert observed["bot"]["long"]["risk"]["n_positions"] == 3
    elif edit == "coin":
        assert observed["live"]["approved_coins"] == {"long": [], "short": []}
    else:
        assert observed["backtest"]["start_date"] == "2025-01-02"
    page.wait_for_timeout(400)
    assert page.evaluate("JSON.parse(document.getElementById('opted-raw-json').value)") == observed
    assert not errors


@pytest.mark.parametrize("action", ["earliest", "all_markets", "readiness", "save", "queue"])
def test_invalid_raw_is_retained_and_blocks_editor_actions(log_page, action):
    """Incomplete JSON cannot be applied, submitted, or stored as arbitrary text."""
    page, errors = _open_sync_editor(log_page)
    submitted = []

    def capture(route):
        """Reject every attempted stateful request inside the isolated browser."""
        submitted.append(route.request.url)
        route.fulfill(status=400, json={"detail": "Synthetic rejection"})

    page.route("**/ohlcv-start-dates", capture)
    page.route("**/ohlcv-preflight", capture)
    page.route("**/configs/sync_test", capture)
    page.route("**/queue/sync_test", capture)
    page.evaluate('''() => {
        const name = document.getElementById('opted-name');
        name.value = 'sync_test'; name.dispatchEvent(new Event('input', {bubbles: true}));
        const raw = document.getElementById('opted-raw-json');
        raw.value = '{"unfinished":'; raw.dispatchEvent(new Event('input', {bubbles: true}));
        const date = document.getElementById('opted-start-date');
        date.value = '2022-02-02'; date.dispatchEvent(new Event('input', {bubbles: true}));
    }''')
    if action in {"earliest", "all_markets"}:
        label = "Set earliest OHLCV start date" if action == "earliest" else "Set common OHLCV start date"
        page.get_by_role("button", name=label, exact=True).click()
    else:
        page.locator({"readiness": "#opted-sidebar-ohlcv-preflight-btn", "save": "#btn-cloud-save",
                      "queue": "#btn-cloud-save-queue"}[action]).click()
    page.wait_for_timeout(400)
    assert page.locator("#opted-raw-json").input_value() == '{"unfinished":'
    assert page.locator("#opted-start-date").input_value() == "2022-02-02"
    assert submitted == []
    assert "Raw JSON is invalid" in page.locator("#opted-ohlcv-start-status").inner_text()
    recovery = page.evaluate('''() => {
        captureOptimizeDraftForRefresh();
        return sessionStorage.getItem(optimizeDraftStorageKey());
    }''')
    assert recovery is None
    assert not errors


def test_late_symbol_rebuild_keeps_latest_user_selection_and_date(log_page):
    """An older market response cannot revive selections, edits or verified state."""
    page, errors = _open_sync_editor(log_page)
    held = []
    page.route("**/coins/status", lambda route: held.append(route))
    page.evaluate("() => { window.oldLoad = loadOptSymbols(state.editorLastConfig); }")
    # Do not await the intentionally held request from page.evaluate.
    page.wait_for_timeout(50)
    assert len(held) == 1
    page.evaluate('''() => {
        document.querySelector('#ms-opt-app-long .ms-x').click();
        const raw = document.getElementById('opted-raw-json');
        const config = JSON.parse(raw.value); config.backtest.start_date = '2023-03-03';
        raw.value = JSON.stringify(config); raw.dispatchEvent(new Event('input', {bubbles: true}));
    }''')
    page.wait_for_function("_optSymbolsLoadSeq > 1")
    page.wait_for_timeout(50)
    assert len(held) == 2
    held[0].fulfill(json={"symbols": ["HYPE"], "catalog": [], "statuses": {}})
    page.evaluate("() => window.oldLoad")
    assert page.evaluate("_optMarketStatusVerified") is False
    held[1].fulfill(json={"symbols": ["HYPE"], "catalog": [], "statuses": {}})
    page.wait_for_function("_optMarketStatusVerified")
    assert page.evaluate("collectApprovedCoinsFromMs()") == {"long": [], "short": []}
    assert page.locator("#opted-start-date").input_value() == "2023-03-03"
    assert page.evaluate("JSON.parse(document.getElementById('opted-raw-json').value).backtest.start_date") == "2023-03-03"
    assert not errors


def test_correcting_incomplete_raw_keeps_intervening_form_edit(log_page):
    """Corrected Raw text contributes its actual changes without losing a newer form field."""
    page, errors = _open_sync_editor(log_page)
    observed = page.evaluate('''async () => {
        const raw = document.getElementById('opted-raw-json');
        const lastValid = JSON.parse(raw.value);
        raw.value = '{"unfinished":'; raw.dispatchEvent(new Event('input', {bubbles: true}));
        const date = document.getElementById('opted-start-date');
        date.value = '2022-02-02'; date.dispatchEvent(new Event('input', {bubbles: true}));
        if (raw.value !== '{"unfinished":') throw Error('Incomplete text was replaced');
        lastValid.backtest.starting_balance = 2345;
        raw.value = JSON.stringify(lastValid); raw.dispatchEvent(new Event('input', {bubbles: true}));
        return await collectOptimizeStartLookupConfig();
    }''')
    assert observed["backtest"]["start_date"] == "2022-02-02"
    assert observed["backtest"]["starting_balance"] == 2345
    assert page.locator("#opted-ohlcv-start-status").inner_text() == ""
    assert not errors


@pytest.mark.parametrize("field", ["start_date", "starting_balance"])
@pytest.mark.parametrize("order", ["disjoint", "raw_then_form", "form_then_raw"])
def test_valid_raw_delta_survives_invalid_bot_json_and_reload(log_page, field, order):
    """Keep accepted field deltas and their actual ordering while Bot JSON is incomplete."""
    page, errors = _open_sync_editor(log_page)
    page.locator("#opted-seed-mode").select_option("self")
    assert page.locator("#opted-bot-long").is_visible()
    observed = page.evaluate('''async ({field, order}) => {
        const raw = document.getElementById('opted-raw-json');
        const bot = document.getElementById('opted-bot-long');
        const validBot = JSON.parse(bot.value);
        const form = document.getElementById(field === 'start_date' ? 'opted-start-date' : 'opted-starting-balance');
        bot.value = '{'; bot.dispatchEvent(new Event('input', {bubbles: true}));
        const editRaw = () => {
            const config = JSON.parse(raw.value);
            config.backtest[field] = field === 'start_date' ? '2023-03-03' : 2345;
            raw.value = JSON.stringify(config); raw.dispatchEvent(new Event('input', {bubbles: true}));
        };
        const editForm = () => {
            form.value = field === 'start_date' ? '2022-02-02' : '3456';
            form.dispatchEvent(new Event('input', {bubbles: true}));
        };
        if (order === 'form_then_raw') { editForm(); editRaw(); }
        else { editRaw(); if (order === 'raw_then_form') editForm(); }
        let blocked = false;
        try { await collectOptimizeStartLookupConfig(); }
        catch (error) { blocked = error.message.includes('Invalid JSON in bot-long'); }
        if (bot.value !== '{') throw Error('Incomplete Bot JSON was replaced');
        validBot.risk.n_positions = 3;
        bot.value = JSON.stringify(validBot);
        bot.focus(); bot.setSelectionRange(10, 10);
        bot.dispatchEvent(new Event('input', {bubbles: true}));
        return {blocked, config: JSON.parse(raw.value), form: form.value,
            caret: bot.selectionStart, focused: document.activeElement === bot};
    }''', {"field": field, "order": order})
    expected = ("2022-02-02" if field == "start_date" else 3456) if order == "raw_then_form" else (
        "2023-03-03" if field == "start_date" else 2345
    )
    assert observed["blocked"] is True
    assert observed["config"]["backtest"][field] == expected
    assert observed["form"] == str(expected)
    assert observed["config"]["bot"]["long"]["risk"]["n_positions"] == 3
    assert observed["focused"] is True and observed["caret"] == 10
    page.wait_for_timeout(400)
    assert page.evaluate("JSON.parse(document.getElementById('opted-raw-json').value).backtest")[field] == expected
    assert page.locator("#opted-ohlcv-start-status").inner_text() == ""
    location = page.url
    page.reload()
    input_id = "opted-start-date" if field == "start_date" else "opted-starting-balance"
    page.wait_for_function("document.getElementById('" + input_id + "')?.value === " + json.dumps(str(expected)))
    assert page.url == location
    assert page.evaluate("JSON.parse(document.getElementById('opted-raw-json').value).backtest")[field] == expected
    assert not errors


def test_incomplete_extra_json_never_enters_current_config_or_draft(log_page):
    """Defer rendering without converting invalid extra JSON text into config data."""
    page, errors = _open_sync_editor(log_page)
    observed = page.evaluate('''() => {
        const raw = document.getElementById('opted-raw-json');
        const config = JSON.parse(raw.value);
        config.optimize.sync_test_fixture = {value: 1};
        raw.value = JSON.stringify(config); raw.dispatchEvent(new Event('input', {bubbles: true}));
        const extra = document.querySelector('[data-extra-param-section="optimize"][data-extra-param-key="sync_test_fixture"]');
        if (!extra) throw Error('Synthetic extra JSON editor was not rendered');
        extra.value = '{"unfinished":'; extra.dispatchEvent(new Event('input', {bubbles: true}));
        const changed = JSON.parse(raw.value); changed.backtest.start_date = '2023-03-03';
        changed.optimize.sync_second_fixture = {value: 9};
        raw.value = JSON.stringify(changed); raw.dispatchEvent(new Event('input', {bubbles: true}));
        const accepted = state.editorLastConfig.backtest;
        const before = {date: accepted.start_date, extra: state.editorLastConfig.optimize.sync_test_fixture, text: extra.value};
        captureOptimizeDraftForRefresh();
        const recovery = sessionStorage.getItem(optimizeDraftStorageKey());
        document.getElementById('exp-opt-extra').classList.add('open');
        extra.focus(); extra.value = '{"value":2}'; extra.setSelectionRange(5, 5);
        extra.dispatchEvent(new Event('input', {bubbles: true}));
        const replacement = document.getElementById(extra.id);
        return {before, recovery, current: JSON.parse(raw.value),
            replaced: replacement !== extra, focused: document.activeElement === replacement,
            caret: replacement.selectionStart};
    }''')
    assert observed["before"] == {"date": "2023-03-03", "extra": {"value": 1}, "text": '{"unfinished":'}
    assert observed["recovery"] is None
    assert observed["current"]["backtest"]["start_date"] == "2023-03-03"
    assert observed["current"]["optimize"]["sync_test_fixture"] == {"value": 2}
    assert observed["current"]["optimize"]["sync_second_fixture"] == {"value": 9}
    assert observed["replaced"] is True and observed["focused"] is True
    assert observed["caret"] == 5
    assert not errors


@pytest.mark.parametrize("action", ["readiness", "save", "queue"])
@pytest.mark.parametrize("order", ["raw_then_form", "form_then_raw"])
def test_editor_actions_submit_one_current_snapshot(log_page, action, order):
    """Readiness and Save/Queue review the same latest config; all writes are mocked."""
    page, errors = _open_sync_editor(log_page)
    posts = []

    def capture(route):
        """Capture the snapshot and reject it before any queue/run can be created."""
        posts.append(route.request.post_data_json)
        route.fulfill(status=400, json={"detail": "Synthetic test rejection after snapshot capture"})

    page.route("**/ohlcv-preflight", capture)
    page.route("**/configs/sync_test", capture)
    page.evaluate('''order => {
        const name = document.getElementById('opted-name');
        name.value = 'sync_test'; name.dispatchEvent(new Event('input', {bubbles: true}));
        const editRaw = () => {
            const raw = document.getElementById('opted-raw-json');
            const config = JSON.parse(raw.value); config.backtest.starting_balance = 2345;
            raw.value = JSON.stringify(config); raw.dispatchEvent(new Event('input', {bubbles: true}));
        };
        const editForm = () => {
            const date = document.getElementById('opted-start-date');
            date.value = '2022-02-02'; date.dispatchEvent(new Event('input', {bubbles: true}));
        };
        if (order === 'raw_then_form') { editRaw(); editForm(); }
        else { editForm(); editRaw(); }
    }''', order)
    page.wait_for_function("_optMarketStatusVerified")
    page.locator({"readiness": "#opted-sidebar-ohlcv-preflight-btn", "save": "#btn-cloud-save",
                  "queue": "#btn-cloud-save-queue"}[action]).click()
    page.wait_for_timeout(400)
    assert len(posts) == 1
    config = posts[0]["config"]
    assert config["backtest"]["starting_balance"] == 2345
    assert config["backtest"]["start_date"] == "2022-02-02"
    assert config["live"]["approved_coins"] == {"long": ["HYPE"], "short": []}
    assert page.locator("#opted-start-date").input_value() == "2022-02-02"
    assert not errors


@pytest.mark.parametrize("order", ["raw_then_date", "date_then_raw"])
@pytest.mark.parametrize("wait_for_sync", [False, True])
def test_ohlcv_lookup_preserves_pending_raw_and_structured_edits(log_page, order, wait_for_sync):
    """A lookup retains both edits before and after the synchronization timers."""
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
    page.wait_for_function("document.getElementById('opted-start-date')?.value === '2021-04-20'")
    page.wait_for_timeout(500)
    posts = []

    def start_lookup(route):
        """Capture the request and return a synthetic job."""
        posts.append(route.request.post_data_json)
        route.fulfill(json={"job_id": "mock-job", "status": "queued"})

    page.route("**/ohlcv-start-dates", start_lookup)
    page.route("**/ohlcv-start-dates/mock-job", lambda route: route.fulfill(json={
        "status": "error", "error": "Synthetic failure",
    }))
    before = page.evaluate('''async ({order, waitForSync}) => {
        const raw = document.getElementById('opted-raw-json');
        const date = document.getElementById('opted-start-date');
        const changeRaw = () => {
            const config = JSON.parse(raw.value);
            config.backtest.starting_balance = 2345;
            raw.value = JSON.stringify(config, null, 2);
            raw.dispatchEvent(new Event('input', {bubbles: true}));
        };
        const changeDate = () => {
            date.value = '2022-02-02';
            date.dispatchEvent(new Event('input', {bubbles: true}));
            date.dispatchEvent(new Event('change', {bubbles: true}));
        };
        if (order === 'raw_then_date') { changeRaw(); changeDate(); }
        else { changeDate(); changeRaw(); }
        if (waitForSync) await new Promise(resolve => setTimeout(resolve, 600));
        const observed = {date: date.value, balance: JSON.parse(raw.value).backtest.starting_balance};
        document.querySelector('[aria-label="Set earliest OHLCV start date"]').click();
        return observed;
    }''', {"order": order, "waitForSync": wait_for_sync})
    page.wait_for_function("document.getElementById('opted-ohlcv-start-status').textContent.includes('Synthetic failure')")
    assert before == {"date": "2022-02-02", "balance": 2345}
    assert len(posts) == 1
    assert posts[0]["config"]["backtest"]["start_date"] == "2022-02-02"
    assert posts[0]["config"]["backtest"]["starting_balance"] == 2345
    assert posts[0]["config"]["live"]["approved_coins"] == {"long": ["HYPE"], "short": []}
    assert page.locator("#opted-start-date").input_value() == "2022-02-02"
    assert not errors


def _synthetic_hype_start_dates(config, root):
    """Run the real helper with synthetic PB8 dependencies, without runtime data."""
    observed = {}

    def timestamp(value):
        """Convert a synthetic UTC date to milliseconds."""
        return int(datetime.fromisoformat(value).replace(tzinfo=timezone.utc).timestamp() * 1000)

    def date_string(value):
        """Render helper dates as UTC ISO strings."""
        return datetime.fromtimestamp(value / 1000, timezone.utc).isoformat()

    def require(config, path):
        """Read a required dotted path from the isolated config."""
        value = config
        for key in path.split("."):
            value = value[key]
        return value

    def prepare(config, **kwargs):
        """Record the input at the mocked PB8 preparation boundary."""
        observed["prepared"] = copy.deepcopy(config["live"]["approved_coins"])
        return copy.deepcopy(config)

    async def format_coins(config, exchanges):
        """Record the input at the mocked PB8 coin-formatting boundary."""
        assert exchanges == ["binance", "bybit"]
        observed["formatted"] = copy.deepcopy(config["live"]["approved_coins"])

    def warmup(config):
        """Supply a synthetic warmup crossing UTC midnight on the newer market."""
        assert config["live"]["strategy_kind"] == "ema_anchor"
        observed["warmup"] = copy.deepcopy(config["live"]["approved_coins"])
        return 61

    class Manager:
        """Provide invented market timestamps, independent of real HYPE data."""

        def __init__(self, exchange, *args, **kwargs):
            """Keep only the selected exchange and isolated market metadata."""
            self.exchange, self.cc, self.markets = exchange, None, {}

        async def load_markets(self):
            """No network or cache access is needed."""

        def has_coin(self, coin):
            """Expose just the selected synthetic market."""
            return coin == "HYPE"

        def get_symbol(self, coin):
            """Return the synthetic market symbol."""
            return "HYPE/USDT:USDT"

        def load_first_timestamp(self, coin):
            """Force the helper to exercise its asynchronous lookup path."""
            return 0

        async def get_first_timestamp(self, coin):
            """Return invented timestamps unrelated to local candle boundaries."""
            return timestamp({
                "binance": "2025-03-04T23:30:00", "bybit": "2024-02-03T12:00:00",
            }[self.exchange])

        async def aclose(self):
            """Record deterministic manager cleanup."""
            observed.setdefault("closed", []).append(self.exchange)

    modules = dict(
        prepare_config=prepare, require_config_value=require,
        require_live_value=lambda config, key: config["live"][key],
        format_end_date=lambda value: value, date_to_ts=timestamp, ts_to_date=date_string,
        compute_backtest_warmup_minutes=warmup, format_approved_ignored_coins=format_coins,
        to_ccxt_exchange_id=lambda value: value, to_standard_exchange_name=lambda value: value,
        HLCVManager=Manager,
    )
    result = asyncio.run(pb8_ohlcv_runtime_helper._build(dict(
        config=config, pb8_dir=str(root), include_start_date_options=True,
    ), modules=modules))
    approved = config["live"]["approved_coins"]
    assert all(observed[key] == approved for key in ("prepared", "formatted", "warmup"))
    assert observed["closed"] == ["binance", "bybit"]
    options = result["start_date_options"]
    assert options["earliest"]["start_date"] == "2024-02-04"
    assert options["all_markets"]["start_date"] == "2025-03-06"
    assert options["known_pair_count"] == options["pair_count"] == 2
    return result


@pytest.mark.parametrize("with_short", [False, True])
@pytest.mark.parametrize("mode,label", [
    ("earliest", "Set earliest OHLCV start date"),
    ("all_markets", "Set common OHLCV start date"),
])
def test_hype_lookup_connects_editor_helper_and_date_apply(log_page, tmp_path, with_short, mode, label):
    """Both date modes apply helper results with stable long-only or two-sided inputs."""
    page, _, _, errors, _, _, _ = log_page
    config = json.loads((ROOT / "tests/fixtures/pb8_october_template.json").read_text())["config"]
    config["backtest"].update(
        starting_balance=1000, exchanges=["binance", "bybit"],
        start_date="2021-04-20", end_date="2026-07-21",
    )
    config["live"].update(
        strategy_kind="ema_anchor",
        approved_coins={"long": ["HYPE"], "short": ["HYPE"] if with_short else []},
    )
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
    page.wait_for_function("document.getElementById('opted-start-date')?.value === '2021-04-20'")
    page.wait_for_function("_optMarketStatusVerified")
    page.evaluate('''() => {
        window.testLookupSignatures = [];
        const collect = collectOptimizeStartLookupConfig;
        collectOptimizeStartLookupConfig = async function() {
            const config = await collect();
            window.testLookupSignatures.push(optimizeOhlcvStartLookupSignature(config));
            return config;
        };
    }''')
    posts = []

    def start_lookup(route):
        """Capture the actual reconciled editor request before helper execution."""
        posts.append(route.request.post_data_json)
        route.fulfill(json={"job_id": "synthetic", "status": "queued"})

    page.route("**/ohlcv-start-dates", start_lookup)
    page.route("**/ohlcv-start-dates/synthetic", lambda route: route.fulfill(json={"status": "running"}))
    page.get_by_role("button", name=label, exact=True).click()
    page.wait_for_function("_optOhlcvStartDateRun?.jobId === 'synthetic'")
    sent = posts[0]["config"]
    assert sent["live"]["approved_coins"] == config["live"]["approved_coins"]
    # Keep the async helper separate from synchronous browser callbacks.
    with ThreadPoolExecutor(max_workers=1) as executor:
        result = executor.submit(_synthetic_hype_start_dates, sent, tmp_path).result(timeout=5)
    page.unroute("**/ohlcv-start-dates/synthetic")
    page.route("**/ohlcv-start-dates/synthetic", lambda route: route.fulfill(json={
        "status": "completed", "result": result,
    }))
    page.wait_for_function("document.getElementById('opted-ohlcv-start-status').textContent.includes('Lookup succeeded')")
    signatures = page.evaluate("window.testLookupSignatures")
    assert len(signatures) == 2 and signatures[0] == signatures[1]
    expected = result["start_date_options"][mode]["start_date"]
    assert page.locator("#opted-start-date").input_value() == expected
    page.wait_for_function(
        "JSON.parse(document.getElementById('opted-raw-json').value).backtest.start_date === " + json.dumps(expected)
    )
    assert page.evaluate("collectApprovedCoinsFromMs()") == config["live"]["approved_coins"]
    assert not errors


def test_categorized_lookup_failure_is_persistent_and_retry_replaces_it(log_page):
    """A safe backend category is visible beside the unchanged date until retry succeeds."""
    page, errors = _open_sync_editor(log_page)
    posts = []

    def start_lookup(route):
        """Create only synthetic browser jobs; no actual runtime is contacted."""
        posts.append(route.request.post_data_json)
        route.fulfill(json={"job_id": "diagnostic_test", "status": "queued"})

    def poll_lookup(route):
        """Return a bounded diagnosis first and a valid synthetic result on retry."""
        if len(posts) == 1:
            route.fulfill(json={"status": "error", "error_stage": "warmup", "error_category": "invalid_input",
                                "error": "[warmup/invalid_input] Check the strategy and EMA warmup settings."})
        else:
            route.fulfill(json={"status": "completed", "result": {"start_date_options": {
                "earliest": {"available": True, "start_date": "2025-01-02"},
                "known_pair_count": 2, "pair_count": 2}}})

    page.route("**/ohlcv-start-dates", start_lookup)
    page.route("**/ohlcv-start-dates/diagnostic_test", poll_lookup)
    page.get_by_role("button", name="Set earliest OHLCV start date", exact=True).click()
    status = page.locator("#opted-ohlcv-start-status")
    page.wait_for_function("document.getElementById('opted-ohlcv-start-status').textContent.includes('[warmup/invalid_input]')")
    assert page.locator("#opted-start-date").input_value() == "2021-04-20"
    page.wait_for_timeout(600)
    assert "[warmup/invalid_input]" in status.inner_text()
    page.get_by_role("button", name="Set earliest OHLCV start date", exact=True).click()
    page.wait_for_function("document.getElementById('opted-ohlcv-start-status').textContent.includes('Lookup succeeded')")
    assert page.locator("#opted-start-date").input_value() == "2025-01-02"
    assert "warmup/invalid_input" not in status.inner_text()
    assert len(posts) == 2 and not errors


@pytest.mark.parametrize("policy", ["always", "never"])
@pytest.mark.parametrize("mode,label", [
    ("earliest", "Set earliest OHLCV start date"),
    ("all_markets", "Set common OHLCV start date"),
])
def test_hsl_restart_normal_form_lookup(log_page, tmp_path, policy, mode, label):
    """A migrated Long-only draft has a real valid selection before either date action."""
    page, errors = _open_sync_editor(log_page, hsl_draft=True)
    selector = page.locator('select[data-runtime-override="bot.long.hsl.restart_after_red_policy"]')
    assert selector.count() == 1
    assert selector.input_value() == "always"
    assert "96 fields" in page.locator("#opted-config-changes summary").inner_text()
    assert page.locator('select[data-runtime-override="bot.short.hsl.restart_after_red_policy"]').count() == 0
    if policy == "never":
        selector.select_option("never")
    page.locator("#opted-starting-balance").fill("2345")
    page.evaluate("renderOptimizeRuntimeOverridesEditor()")
    assert selector.input_value() == policy
    for signal_mode in ("pside", "coin"):
        page.locator("#opted-hsl-signal-mode").select_option(signal_mode)
        assert selector.input_value() == policy
    current = page.evaluate("JSON.parse(document.getElementById('opted-raw-json').value)")
    assert current["bot"]["long"]["hsl"]["restart_after_red_policy"] == policy
    assert current["bot"]["long"]["hsl"]["enabled"] is True
    assert "hsl" not in current["bot"]["short"]
    posts = []

    def start_lookup(route):
        """Capture only isolated test requests."""
        posts.append(route.request.post_data_json)
        route.fulfill(json={"job_id": "hsl_choice", "status": "queued"})

    def poll_lookup(route):
        """Run date aggregation/warmup code with synthetic market inputs."""
        sent = posts[-1]["config"]
        assert sent["bot"]["long"]["hsl"]["restart_after_red_policy"] == policy
        with ThreadPoolExecutor(max_workers=1) as executor:
            result = executor.submit(_synthetic_hype_start_dates, sent, tmp_path).result(timeout=5)
        route.fulfill(json={"status": "completed", "result": result})

    page.route("**/ohlcv-start-dates", start_lookup)
    page.route("**/ohlcv-start-dates/hsl_choice", poll_lookup)
    page.get_by_role("button", name=label, exact=True).click()
    page.wait_for_function("document.getElementById('opted-ohlcv-start-status').textContent.includes('Lookup succeeded')")
    expected = "2024-02-04" if mode == "earliest" else "2025-03-06"
    assert page.locator("#opted-start-date").input_value() == expected
    after = page.evaluate("JSON.parse(document.getElementById('opted-raw-json').value)")
    assert after["bot"]["long"]["hsl"]["restart_after_red_policy"] == policy
    assert after["bot"]["long"]["hsl"]["enabled"] is True
    assert after["backtest"]["starting_balance"] == 2345
    assert after["backtest"]["end_date"] == "2026-07-21"
    assert len(posts) == 1
    page.evaluate("captureOptimizeDraftForRefresh()")
    page.reload()
    page.wait_for_function("document.getElementById('opted-start-date')?.value === " + json.dumps(expected))
    assert selector.input_value() == policy
    assert page.locator("#opted-starting-balance").input_value() == "2345"
    assert not errors


@pytest.mark.parametrize("policy", ["always", "never"])
@pytest.mark.parametrize("action", ["readiness", "save", "queue"])
def test_hsl_restart_normal_form_actions(log_page, policy, action):
    """Readiness and mocked Save/Queue receive the policy displayed by the real form."""
    page, errors = _open_sync_editor(log_page, hsl_draft=True)
    selector = page.locator('select[data-runtime-override="bot.long.hsl.restart_after_red_policy"]')
    assert selector.input_value() == "always"
    if policy == "never":
        selector.select_option(policy)
    page.locator("#opted-name").fill("hsl_form_test")
    page.locator("#opted-starting-balance").fill("2345")
    submitted, queued = [], []

    def snapshot(route):
        """Persist only in this test's memory, never in a server or runtime config."""
        if route.request.method == "GET":
            route.fulfill(json={"config": submitted[-1]["config"]})
        else:
            submitted.append(route.request.post_data_json)
            route.fulfill(json={"ok": True})

    def preflight(route):
        """Capture Readiness before any runtime dependency can be touched."""
        submitted.append(route.request.post_data_json)
        route.fulfill(status=400, json={"detail": "Isolated readiness snapshot captured"})

    def queue(route):
        """A queue acknowledgement is a mock; it never creates a job."""
        if route.request.method == "POST":
            queued.append(route.request.post_data_json)
            route.fulfill(json={"ok": True})
        else:
            route.fulfill(json={"items": []})

    page.route("**/configs/hsl_form_test", snapshot)
    page.route("**/ohlcv-preflight", preflight)
    page.route("**/optimize-v8/queue", queue)
    page.wait_for_function("_optMarketStatusVerified")
    page.locator({"readiness": "#opted-sidebar-ohlcv-preflight-btn", "save": "#btn-cloud-save",
                  "queue": "#btn-cloud-save-queue"}[action]).click()
    page.wait_for_function("!state.editorSaving")
    assert len(submitted) == 1
    config = submitted[0]["config"]
    assert config["bot"]["long"]["hsl"]["restart_after_red_policy"] == policy
    assert config["bot"]["long"]["hsl"]["enabled"] is True
    assert config["backtest"]["starting_balance"] == 2345
    assert config["live"]["approved_coins"] == {"long": ["HYPE"], "short": []}
    assert "hsl" not in config["bot"]["short"]
    assert "bot.long.hsl.restart_after_red_policy" not in config["optimize"]["fixed_runtime_overrides"]
    if action != "readiness":
        assert queued == ([{"name": "hsl_form_test"}] if action == "queue" else [])
        page.evaluate("openConfigEditor('hsl_form_test')")
        page.wait_for_function("document.getElementById('opted-name')?.value === 'hsl_form_test'")
        assert selector.input_value() == policy
        assert page.locator("#opted-starting-balance").input_value() == "2345"
    assert not errors


def test_hsl_restart_form_unified_and_invalid_json(log_page):
    """Scope switching keeps independent policies; invalid Bot JSON cannot fake a selection."""
    page, errors = _open_sync_editor(log_page)
    page.evaluate('''() => {
        const config = structuredClone(state.editorLastConfig);
        config.live.hsl_signal_mode = 'unified';
        config.bot.hsl = {...config.bot.long.hsl, enabled:false, restart_after_red_policy:null};
        config.bot.long.hsl.restart_after_red_policy = 'never';
        config.bot.short.hsl.restart_after_red_policy = 'always';
        config.optimize.fixed_runtime_overrides = {'bot.hsl.restart_after_red_policy':'never'};
        openEditorWithConfig({config}, '', '');
    }''')
    portfolio = page.locator('select[data-runtime-override="bot.hsl.restart_after_red_policy"]')
    assert portfolio.input_value() == "never"
    portfolio.select_option("always")
    page.locator("#opted-hsl-signal-mode").select_option("coin")
    long_policy = page.locator('select[data-runtime-override="bot.long.hsl.restart_after_red_policy"]')
    assert long_policy.input_value() == "never"
    page.locator("#opted-hsl-signal-mode").select_option("unified")
    assert portfolio.input_value() == "always"
    current = page.evaluate("JSON.parse(document.getElementById('opted-raw-json').value)")
    assert current["bot"]["hsl"]["enabled"] is False
    assert current["bot"]["long"]["hsl"]["restart_after_red_policy"] == "never"
    assert "bot.hsl.restart_after_red_policy" not in current["optimize"]["fixed_runtime_overrides"]
    page.locator("#opted-hsl-signal-mode").select_option("coin")
    valid_bot = page.locator("#opted-bot-long").input_value()
    page.evaluate('''() => {
        const bot = document.getElementById('opted-bot-long');
        bot.value = '{'; bot.dispatchEvent(new Event('input', {bubbles:true}));
    }''')
    assert page.evaluate("setOptimizeBotHslField('long', 'restart_after_red_policy', 'always')") is False
    assert page.locator("#opted-bot-long").input_value() == "{"
    assert "invalid Bot JSON" in page.locator("#cfg-editor-status").inner_text()
    assert long_policy.is_disabled() and long_policy.input_value() == ""
    page.evaluate('''value => {
        const bot = document.getElementById('opted-bot-long');
        bot.value = value; bot.dispatchEvent(new Event('input', {bubbles:true}));
    }''', valid_bot)
    page.evaluate("renderOptimizeRuntimeOverridesEditor()")
    assert long_policy.is_enabled() and long_policy.input_value() == "never"
    assert not errors


@pytest.mark.parametrize("unavailable_status", [403, 404])
def test_named_hsl_draft_recovers_after_authorized_reload(log_page, unavailable_status):
    """Named drafts retain unsaved policy and disjoint edits after a fresh source read."""
    page, errors = _open_sync_editor(log_page, hsl_draft=True)
    config = page.evaluate('structuredClone(state.editorLastConfig)')
    config['bot']['long']['hsl']['restart_after_red_policy'] = None
    reads = []

    def saved_config(route):
        """Only serve an immutable test source; no config is written."""
        reads.append(route.request.method)
        route.fulfill(json={'config': config})

    page.route('**/configs/hsl_saved_test', saved_config)
    page.evaluate("openConfigEditor('hsl_saved_test')")
    selector = page.locator('select[data-runtime-override="bot.long.hsl.restart_after_red_policy"]')
    selector.select_option('never')
    page.locator('#opted-starting-balance').fill('2345')
    location = page.url
    page.reload()
    page.wait_for_function("document.getElementById('opted-name')?.value === 'hsl_saved_test'")
    assert selector.input_value() == 'never'
    assert page.locator('#opted-starting-balance').input_value() == '2345'
    assert page.url == location and reads == ['GET', 'GET']
    assert page.evaluate('state.editorSourceName') == 'hsl_saved_test'
    assert page.evaluate("JSON.parse(document.getElementById('opted-raw-json').value).bot.long.hsl.enabled") is True
    # A cached draft must not bypass a source that was removed or lost access.
    page.route('**/configs/hsl_saved_test', lambda route: route.fulfill(status=unavailable_status, json={'detail': 'Test config unavailable'}))
    page.reload()
    page.wait_for_function('!state.initialLoadPending')
    assert page.locator('#opted-name').count() == 0
    assert not errors


def test_hsl_restart_keyboard_keeps_focus_and_label_help(log_page):
    """The real select and its label stay keyboard operable during automatic redraws."""
    page, errors = _open_sync_editor(log_page, hsl_draft=True)
    selector = page.locator('select[data-runtime-override="bot.long.hsl.restart_after_red_policy"]')
    selector.focus()
    selector.press('ArrowDown')
    assert selector.input_value() == 'never'
    assert selector.evaluate('(node) => document.activeElement === node')
    label = selector.locator('..').locator('.runtime-override-label')
    assert label.evaluate('(node) => node.tabIndex') == 0
    label.focus()
    tooltip = page.locator('#pb8-parameter-tooltip')
    assert tooltip.is_visible()
    assert 'configured lookback' in tooltip.inner_text()
    page.keyboard.press('Escape')
    assert tooltip.is_hidden()
    assert page.evaluate("JSON.parse(document.getElementById('opted-raw-json').value).bot.long.hsl.restart_after_red_policy") == 'never'
    assert not errors
