"""Static and executable frontend contracts for the shared PB7/PB8 Optimize page."""

from pathlib import Path
import subprocess
import textwrap


ROOT = Path(__file__).resolve().parents[2]


def test_pb8_optimize_enables_the_shared_scenario_generator_only_for_v8() -> None:
    """The shared Optimize page exposes deterministic generation only through its PB8 adapter."""
    page = (ROOT / "frontend" / "v7_optimize.html").read_text(encoding="utf-8")

    assert "scenarioGenerator: optimizeEditorAdapter.isV8" in page
    assert "getScenarioContext: function()" in page
    assert "pbgui.scenario_template = suite.scenario_template" in page
    assert "onApplyScenarioPreview: function(preview)" in page
    assert "balanceInput.value = String(policy.starting_balance)" in page


def test_sweep_apply_sets_balanced_high_risk_scoring_preset() -> None:
    """Explicit Sweep Apply replaces scoring, limits, objective basis, and base balance together."""
    page = (ROOT / "frontend" / "v7_optimize.html").read_text(encoding="utf-8")
    function_source = _page_function(page, "applyOptimizeSweepPreset")
    script = textwrap.dedent(
        f"""
        const assert = require('node:assert/strict');
        const nodes = {{
          'opted-starting-balance': {{value: '100000'}},
          'opted-objective-scenario-mode': {{value: 'named'}},
          'opted-objective-scenario-name': {{value: 'old'}}
        }};
        const el = id => nodes[id] || null;
        let scoring = null;
        let limits = null;
        let toggled = 0;
        const setScoringEntries = value => {{ scoring = value; }};
        const setLimitEntries = value => {{ limits = value; }};
        const applyOptimizeSweepCoinSymmetry = () => {{}};
        const applyOptimizeSweepLongBoundsPreset = () => {{}};
        const toggleOptimizeObjectiveScenarioInput = () => {{ toggled += 1; }};
        {function_source}

        applyOptimizeSweepPreset({{
          template: 'sweep_cycles',
          parameters: {{sweep_policy: {{starting_balance: 1000}}}}
        }});

        assert.equal(nodes['opted-starting-balance'].value, '1000');
        assert.deepEqual(scoring, [
          {{metric: 'gain_strategy_eq', goal: 'max'}},
          {{metric: 'sortino_ratio_strategy_eq', goal: 'max'}},
          {{metric: 'drawdown_worst_strategy_eq', goal: 'min'}}
        ]);
        assert.deepEqual(limits, [
          {{metric: 'drawdown_worst_strategy_eq', penalize_if: 'greater_than', value: 0.8}},
          {{metric: 'backtest_completion_ratio', penalize_if: 'less_than', value: 0.99}}
        ]);
        assert.equal(nodes['opted-objective-scenario-mode'].value, 'aggregate');
        assert.equal(nodes['opted-objective-scenario-name'].value, '');
        assert.equal(toggled, 1);
        """
    )
    _run_node(script)


def test_sweep_apply_sizes_long_positions_to_coins_and_sets_twe_range() -> None:
    """One explicit Long coin becomes one fixed position while TWE searches 6 through 10."""
    page = (ROOT / "frontend" / "v7_optimize.html").read_text(encoding="utf-8")
    function_source = _page_function(page, "applyOptimizeSweepLongBoundsPreset")
    script = textwrap.dedent(
        f"""
        const assert = require('node:assert/strict');
        const inputs = {{
          'opted-long-npos': {{value: '7'}},
          'opted-long-twe': {{value: '1.5'}}
        }};
        const state = {{
          optimizeBounds: [
            {{key: 'long.risk.n_positions', group: 'long', suffix: 'risk.n_positions', stepValue: 1}},
            {{key: 'long.risk.total_wallet_exposure_limit', group: 'long', suffix: 'risk.total_wallet_exposure_limit', stepValue: 0.01}},
            {{key: 'long.forager.score_weights_volume', group: 'long', suffix: 'forager.score_weights_volume', lowValue: 0, highValue: 1}},
            {{key: 'long.strategy.trailing_martingale.ema_span_0', group: 'long', suffix: 'strategy.trailing_martingale.ema_span_0', lowValue: 100, highValue: 2880}},
            {{key: 'long.risk.position_exposure_enforcer_threshold', group: 'long', suffix: 'risk.position_exposure_enforcer_threshold', lowValue: 1, highValue: 1}},
            {{key: 'short.risk.n_positions', lowValue: 5, highValue: 5}}
          ],
          optimizeFixedParams: ['long.strategy.trailing_martingale.ema_span_0', 'short.keep.fixed']
        }};
        const el = id => inputs[id] || null;
        const optGetMs = id => id === 'ms-opt-app-long' ? ['HYPE'] : [];
        const uniqStrings = values => Array.from(new Set(values));
        const optimizeEditorAdapter = {{canonicalFixedParam: key => 'bot.' + key}};
        const getOptimizeBoundMeta = (_key, _low, _high, step) => ({{decimals: step === 1 ? 0 : 2}});
        const formatOptimizeBoundValue = (value, decimals) => Number(value).toFixed(decimals).replace(/\\.00$/, '');
        const constrainOptimizeBoundEntry = entry => entry;
        const optimizeBoundNumbersMatch = (left, right) => left === right;
        const isOptimizeHslAutoFixedBound = () => false;
        let synced = 0;
        let rendered = 0;
        const optBotSyncFromFields = side => {{ assert.equal(side, 'long'); synced += 1; }};
        const renderOptimizeBoundsEditor = () => {{ rendered += 1; }};
        const updateOptimizeBoundsHeader = () => {{}};
        {function_source}

        applyOptimizeSweepLongBoundsPreset();

        assert.deepEqual(
          state.optimizeBounds.slice(0, 2).map(entry => [entry.lowValue, entry.highValue]),
          [[1, 1], [6, 10]]
        );
        assert.deepEqual(state.optimizeBounds[5], {{key: 'short.risk.n_positions', lowValue: 5, highValue: 5}});
        assert.deepEqual(state.optimizeFixedParams, [
          'long.forager.score_weights_volume',
          'long.risk.n_positions',
          'long.risk.position_exposure_enforcer_threshold',
          'short.keep.fixed'
        ]);
        assert.equal(inputs['opted-long-npos'].value, '1');
        assert.equal(inputs['opted-long-twe'].value, '6');
        assert.equal(synced, 1);
        assert.equal(rendered, 1);
        """
    )
    _run_node(script)


def test_sweep_apply_mirrors_approved_coins_without_enabling_short() -> None:
    """PB8 Suite receives symmetric coin lists while Short activation remains a TWE concern."""
    page = (ROOT / "frontend" / "v7_optimize.html").read_text(encoding="utf-8")
    function_source = _page_function(page, "applyOptimizeSweepCoinSymmetry")
    script = textwrap.dedent(
        f"""
        const assert = require('node:assert/strict');
        const values = {{
          'ms-opt-app-long': ['HYPE/USDC:USDC'],
          'ms-opt-app-short': [],
          'ms-opt-ign-short': ['HYPE/USDC:USDC', 'DOGE/USDC:USDC']
        }};
        const optGetMs = id => (values[id] || []).slice();
        const optSetMs = (id, next) => {{ values[id] = next.slice(); }};
        {function_source}
        applyOptimizeSweepCoinSymmetry();
        assert.deepEqual(values['ms-opt-app-long'], ['HYPE/USDC:USDC']);
        assert.deepEqual(values['ms-opt-app-short'], ['HYPE/USDC:USDC']);
        assert.deepEqual(values['ms-opt-ign-short'], ['DOGE/USDC:USDC']);
        """
    )
    _run_node(script)


def test_suite_generator_settings_expose_inline_help_tooltips() -> None:
    """Every generated-scenario setting must use the standard underlined data-tip help."""
    source = (ROOT / "frontend" / "js" / "suite_editor.js").read_text(encoding="utf-8")

    labels = (
        "Template",
        "Window days",
        "Stride days",
        "Training windows",
        "Holdout windows",
        "Exchange mode",
        "Balance multiplier",
        "Starting balance",
        "Refill cost",
        "Cooldown days",
    )
    for label in labels:
        assert f'>{label}\\x3C/span>' in source


def test_pb8_scenario_generator_guide_opens_exact_topic_and_anchor() -> None:
    """PB8 Guide actions must not fall through to the earlier PB7 Optimize topic."""
    page = (ROOT / "frontend" / "v7_optimize.html").read_text(encoding="utf-8")
    suite = (ROOT / "frontend" / "js" / "suite_editor.js").read_text(encoding="utf-8")

    assert "optimizeEditorAdapter.isV8 ? '43_pbv8_optimize' : '36_pbv7_optimize'" in page
    assert "window._openOptimizeHelp('scenario-generator')" in page
    assert "_suiteOpenScenarioGeneratorGuide()" in suite
    assert '>Guide\\x3C/button>' in suite


def test_pb8_optimize_exposes_bounded_ohlcv_start_date_modes() -> None:
    """PB8 Optimize must offer compact earliest-any and common-all buttons beside start_date."""
    page = (ROOT / "frontend" / "v7_optimize.html").read_text(encoding="utf-8")

    assert 'id="opted-ohlcv-start-mode"' not in page
    assert "setOptimizeStartDateFromOhlcv(\\'earliest\\',this)" in page
    assert "setOptimizeStartDateFromOhlcv(\\'all_markets\\',this)" in page
    assert '>1st</button>' in page
    assert '>All</button>' in page
    assert "apiFetch('/ohlcv-start-dates'" in page
    assert "apiFetch('/ohlcv-start-dates/' + encodeURIComponent(run.jobId))" in page
    assert "{method: 'DELETE'}" in page
    assert 'id="opted-ohlcv-start-progress-fill"' in page
    assert 'id="opted-ohlcv-start-stop"' in page
    assert "signature !== optimizeOhlcvStartLookupSignature(currentConfig)" in page
    assert "scenarioGenerator: optimizeEditorAdapter.isV8" in page


def test_ohlcv_start_lookup_rejects_last_valid_config_fallback() -> None:
    """An invalid Raw JSON editor must not query dates for a stale fallback config."""
    page = (ROOT / "frontend" / "v7_optimize.html").read_text(encoding="utf-8")
    function_source = _page_function(page, "optimizeOhlcvLookupConfig")
    script = textwrap.dedent(
        f"""
        const assert = require('node:assert/strict');
        {function_source}
        assert.deepEqual(optimizeOhlcvLookupConfig({{backtest: {{start_date: '2020-01-01'}}}}), {{backtest: {{start_date: '2020-01-01'}}}});
        assert.throws(
          () => optimizeOhlcvLookupConfig({{config: {{backtest: {{}}}}, note: 'Raw JSON is invalid'}}),
          /Raw JSON is invalid/
        );
        """
    )
    _run_node(script)


def test_ohlcv_start_progress_renders_real_pair_count() -> None:
    """The compact progress UI must show backend completed/total values and percentage."""
    page = (ROOT / "frontend" / "v7_optimize.html").read_text(encoding="utf-8")
    function_source = _page_function(page, "renderOptimizeStartDateProgress")
    script = textwrap.dedent(
        f"""
        const assert = require('node:assert/strict');
        const nodes = {{
          'opted-ohlcv-start-progress': {{style: {{}}}},
          'opted-ohlcv-start-progress-fill': {{style: {{}}}},
          'opted-ohlcv-start-progress-label': {{textContent: '', title: ''}},
          'opted-ohlcv-start-stop': {{disabled: false}}
        }};
        const el = id => nodes[id];
        let _optOhlcvStartDateRun = {{lastProgress: null}};
        {function_source}
        renderOptimizeStartDateProgress({{
          status: 'running',
          progress: {{completed: 3, total: 8, percent: 37.5, message: 'Resolving HYPE on bybit'}}
        }});
        assert.equal(nodes['opted-ohlcv-start-progress-fill'].style.width, '37.5%');
        assert.equal(nodes['opted-ohlcv-start-progress-label'].textContent, '3/8 · Resolving HYPE on bybit');
        assert.equal(nodes['opted-ohlcv-start-stop'].disabled, false);
        assert.equal(_optOhlcvStartDateRun.lastProgress.completed, 3);
        """
    )
    _run_node(script)


def test_optimize_start_date_normalization_strips_iso_time() -> None:
    """PB8 timestamp responses and previously stored values render as date-only strings."""
    page = (ROOT / "frontend" / "v7_optimize.html").read_text(encoding="utf-8")
    function_source = _page_function(page, "normalizeOptimizeDateOnlyValue")
    script = textwrap.dedent(
        f"""
        const assert = require('node:assert/strict');
        {function_source}
        assert.equal(normalizeOptimizeDateOnlyValue('2024-12-31T00:00:00'), '2024-12-31');
        assert.equal(normalizeOptimizeDateOnlyValue('2024-12-31 12:30:00'), '2024-12-31');
        assert.equal(normalizeOptimizeDateOnlyValue('now'), 'now');
        """
    )
    _run_node(script)


def test_late_symbol_load_preserves_first_approved_coin_clear() -> None:
    """Initial resolver completion must not restore selections cleared while it was pending."""
    page = (ROOT / "frontend" / "v7_optimize.html").read_text(encoding="utf-8")
    function_source = _page_function(page, "loadOptSymbols")
    script = textwrap.dedent(
        f"""
        const assert = require('node:assert/strict');
        let resolveCoins;
        const selected = {{
          'ms-opt-exchanges': ['binance'],
          'ms-opt-app-long': [],
          'ms-opt-app-short': [],
          'ms-opt-ign-long': [],
          'ms-opt-ign-short': [],
          'ms-opt-tags': []
        }};
        let _optSymbolsLoadSeq = 0;
        let _optMarketStatusVerified = false;
        let _optMarketLabels = {{}};
        const optimizeEditorAdapter = {{isV8: true}};
        const _optMsController = {{applyCoinStatus: () => {{}}}};
        const toast = () => {{}};
        const captureOptimizeRawAnchor = () => null;
        const restoreOptimizeRawAnchor = () => {{}};
        const coinSideSelection = (value, side) => (value && value[side] || []).slice();
        const currentMultiselectValues = (id, fallback) => selected[id] ? selected[id].slice() : fallback.slice();
        const uniqStrings = values => Array.from(new Set(values.filter(Boolean)));
        const optRebuildMs = (id, _options, values) => {{ selected[id] = values.slice(); }};
        const optFetchV7Json = path => {{
          if (path === '/coins/status') return new Promise(resolve => {{ resolveCoins = resolve; }});
          return Promise.resolve({{tags: []}});
        }};
        {function_source}

        (async () => {{
          const config = {{
            backtest: {{exchanges: ['binance']}},
            live: {{approved_coins: {{long: ['BTC', 'ETH'], short: ['BTC', 'ETH']}}, ignored_coins: {{long: [], short: []}}}},
            pbgui: {{tags: []}}
          }};
          const pending = loadOptSymbols(config, {{preferConfigValues: true}});
          assert.deepEqual(selected['ms-opt-app-long'], ['BTC', 'ETH']);
          selected['ms-opt-app-long'] = [];
          selected['ms-opt-app-short'] = [];
          resolveCoins({{symbols: ['BTC', 'ETH', 'XRP'], catalog: [], statuses: {{}}}});
          await pending;
          assert.deepEqual(selected['ms-opt-app-long'], []);
          assert.deepEqual(selected['ms-opt-app-short'], []);
        }})().catch(error => {{ console.error(error); process.exitCode = 1; }});
        """
    )
    _run_node(script)


def test_sweep_holdout_button_builds_standalone_backtest_config() -> None:
    """The sidebar Holdout action must remove training Suite state and set immutable dates."""
    page = (ROOT / "frontend" / "v7_optimize.html").read_text(encoding="utf-8")
    function_source = _page_function(page, "buildSweepHoldoutBacktestConfig")
    script = textwrap.dedent(
        f"""
        const assert = require('node:assert/strict');
        const deepClone = value => JSON.parse(JSON.stringify(value));
        {function_source}
        const source = {{
          backtest: {{
            suite_enabled: true,
            scenarios: [{{label: 'train_01'}}],
            reducer: {{default: 'median'}},
            start_date: '2025-01-01',
            end_date: '2025-03-31'
          }},
          pbgui: {{scenario_template: {{template: 'sweep_cycles'}}}}
        }};
        const result = buildSweepHoldoutBacktestConfig(
          source,
          {{label: 'holdout_01', start_date: '2026-06-01', end_date: '2026-08-29'}},
          'candidate_holdout_01'
        );
        assert.equal(result.backtest.start_date, '2026-06-01');
        assert.equal(result.backtest.end_date, '2026-08-29');
        assert.equal(result.backtest.base_dir, 'backtests/pbgui/candidate_holdout_01');
        assert.equal(Object.hasOwn(result.backtest, 'suite_enabled'), false);
        assert.equal(Object.hasOwn(result.backtest, 'scenarios'), false);
        assert.equal(Object.hasOwn(result.backtest, 'reducer'), false);
        assert.equal(Object.hasOwn(result.pbgui, 'scenario_template'), false);
        assert.equal(source.backtest.suite_enabled, true);
        """
    )
    _run_node(script)
    assert 'id="btn-holdout-selected-paretos"' in page
    assert "backtestSelectedSweepHoldouts().catch(handleError)" in page


def test_suite_generator_applies_training_only_and_invalidates_stale_provenance() -> None:
    """Applying a preview excludes holdout windows and later manual edits clear provenance."""
    script = textwrap.dedent(
        """
        const assert = require('node:assert/strict');
        const fs = require('node:fs');
        eval(fs.readFileSync('frontend/js/suite_editor.js', 'utf8'));
        _suiteRender = () => {};
        toast = () => {};
        _suiteState.getScenarioContext = () => ({
          start_date: '2023-01-01', end_date: '2024-01-31', exchanges: ['binance']
        });
        _suiteState.scenarioPreviewContext = _suiteScenarioContextSignature(_suiteScenarioContext());
        let appliedPreview = null;
        _suiteState.onApplyScenarioPreview = preview => { appliedPreview = preview; };
        _suiteState.scenarioPreview = {
          training_scenarios: [{label: 'train_01'}, {label: 'train_02'}],
          holdout_scenarios: [{label: 'holdout_01'}],
          reducer: {default: 'median'},
          provenance: {template: 'walk_forward', holdout_scenarios: [{label: 'holdout_01'}]}
        };

        _suiteApplyScenarioPreview();
        let collected = suiteCollect();
        assert.deepEqual(collected.scenarios.map(item => item.label), ['train_01', 'train_02']);
        assert.equal(collected.scenario_template.template, 'walk_forward');
        assert.deepEqual(collected.scenario_template.holdout_scenarios, [{label: 'holdout_01'}]);
        assert.equal(appliedPreview.provenance.template, 'walk_forward');

        _suiteNotifyStructuredSync();
        collected = suiteCollect();
        assert.equal(Object.hasOwn(collected, 'scenario_template'), false);

        _suiteState.scenarioPreview = {
          training_scenarios: [{label: 'stale_train'}], holdout_scenarios: [],
          reducer: {default: 'mean'}, provenance: {template: 'rolling_windows'}
        };
        _suiteState.scenarios = [{label: 'keep_current'}];
        _suiteState.getScenarioContext = () => ({
          start_date: '2023-02-01', end_date: '2024-01-31', exchanges: ['binance']
        });
        _suiteApplyScenarioPreview();
        assert.deepEqual(_suiteState.scenarios, [{label: 'keep_current'}]);
        """
    )
    _run_node(script)


def test_sweep_generator_aligns_stride_with_window_and_cooldown() -> None:
    """Sweep windows must contain a real no-trading gap before the next cycle."""
    script = textwrap.dedent(
        """
        const assert = require('node:assert/strict');
        const fs = require('node:fs');
        const fields = {
          'suite-generator-template': {value: 'sweep_cycles'},
          'suite-generator-window': {value: '90'},
          'suite-generator-stride': {value: '30'},
          'suite-generator-training': {value: '4'},
          'suite-generator-holdout': {value: '1'},
          'suite-generator-cooldown': {value: '7'}
        };
        global.document = {getElementById: id => fields[id] || null};
        eval(fs.readFileSync('frontend/js/suite_editor.js', 'utf8'));
        _suiteState.getScenarioContext = () => ({start_date: '2024-12-31', end_date: '2026-08-29'});
        _suiteAlignSweepStride();
        assert.equal(fields['suite-generator-stride'].value, '97');
        assert.equal(fields['suite-generator-training'].value, '5');
        """
    )
    _run_node(script)


def test_scenario_generator_recalculate_uses_current_base_dates() -> None:
    """Recalculate must replace stale counts after an OHLCV start-date change."""
    script = textwrap.dedent(
        """
        const assert = require('node:assert/strict');
        const fs = require('node:fs');
        const fields = {
          'suite-generator-template': {value: 'sweep_cycles'},
          'suite-generator-window': {value: '90'},
          'suite-generator-stride': {value: '90'},
          'suite-generator-training': {value: '3'},
          'suite-generator-holdout': {value: '1'},
          'suite-generator-exchange-mode': {value: 'inherit'},
          'suite-generator-multiplier': {value: '2'},
          'suite-generator-balance': {value: '1000'},
          'suite-generator-refill': {value: '0'},
          'suite-generator-cooldown': {value: '0'}
        };
        global.document = {getElementById: id => fields[id] || null};
        eval(fs.readFileSync('frontend/js/suite_editor.js', 'utf8'));
        let context = {start_date: '2025-01-01', end_date: '2025-12-31', exchanges: ['binance']};
        let renders = 0;
        global.toast = () => {};
        _suiteRender = () => { renders += 1; };
        _suiteState.getScenarioContext = () => context;
        _suiteState.scenarioPreview = {training_scenarios: [{label: 'stale'}]};
        _suiteState.scenarioRequestId = 4;

        context = {start_date: '2025-04-01', end_date: '2025-12-31', exchanges: ['doge-exchange']};
        _suiteRecalculateScenarioGenerator();

        assert.equal(fields['suite-generator-training'].value, '2');
        assert.equal(_suiteState.scenarioGeneratorDraft.training_windows, 2);
        assert.equal(_suiteState.scenarioPreview, null);
        assert.equal(_suiteState.scenarioPreviewContext, '');
        assert.equal(_suiteState.scenarioRequestId, 5);
        assert.equal(renders, 1);
        """
    )
    _run_node(script)


def test_suite_render_preserves_open_expander_while_disabled() -> None:
    """Preview and Recalculate renders must not collapse a manually opened generator."""
    script = textwrap.dedent(
        """
        const assert = require('node:assert/strict');
        const fs = require('node:fs');
        const container = {innerHTML: ''};
        const existing = {classList: {contains: name => name === 'open'}};
        global.document = {
          getElementById: id => id === 'suite' ? container : (id === 'exp-suite' ? existing : null)
        };
        eval(fs.readFileSync('frontend/js/suite_editor.js', 'utf8'));
        _suiteState.containerId = 'suite';
        _suiteState.enabled = false;
        _suiteState.expanded = false;
        _suiteRender();
        assert.equal(_suiteState.expanded, true);
        assert.match(container.innerHTML, /class="expander open" id="exp-suite"/);
        """
    )
    _run_node(script)


def test_scenario_preview_uses_readable_table_and_warning_text_sizes() -> None:
    """Scenario rows and orange notices must not use the extra-small text token."""
    source = (ROOT / "frontend" / "js" / "suite_editor.js").read_text(encoding="utf-8")

    assert 'class="tbl" style="font-size:var(--fs-sm)' in source
    assert 'font-size:var(--fs-sm);line-height:1.45;color:var(--orange)' in source


def test_pb8_suite_aggregate_renders_median_and_std_without_changing_pb7() -> None:
    """The shared reducer UI must display every PB8 method while PB7 stays compatible."""
    script = textwrap.dedent(
        """
        const assert = require('node:assert/strict');
        const fs = require('node:fs');
        eval(fs.readFileSync('frontend/js/suite_editor.js', 'utf8'));
        global.esc = value => String(value);
        _suiteState.aggregateMetrics = ['gain_strategy_eq'];
        _suiteState.aggregate = {default: 'median', gain_strategy_eq: 'std'};
        _suiteState.preserveMarketIdentifiers = true;
        const pb8 = _suiteRenderAggregate();
        assert.match(pb8, /value="median" selected>median/);
        assert.match(pb8, /value="std" selected>std/);

        _suiteState.preserveMarketIdentifiers = false;
        const pb7 = _suiteRenderAggregate();
        assert.doesNotMatch(pb7, /value="median"/);
        assert.doesNotMatch(pb7, /value="std"/);
        """
    )
    _run_node(script)


def _page_function(page: str, name: str) -> str:
    """Extract one top-level function declaration from the inline page script."""
    marker = f"function {name}("
    start = page.index(marker)
    if page[max(0, start - 6) : start] == "async ":
        start -= 6
    candidates = [
        position
        for token in ("\nfunction ", "\nasync function ")
        if (position := page.find(token, start + len(marker))) >= 0
    ]
    end = min(candidates) if candidates else len(page)
    return page[start:end].rstrip()


def _run_node(script: str) -> None:
    """Run one isolated Node contract and surface its assertion output."""
    completed = subprocess.run(["node", "-e", script], cwd=ROOT, text=True, capture_output=True, check=False)
    assert completed.returncode == 0, completed.stderr or completed.stdout


def test_v7_and_v8_use_one_optimize_template() -> None:
    """Optimize generations must share the existing PB7 template and its complete panel set."""
    page = (ROOT / "frontend" / "v7_optimize.html").read_text(encoding="utf-8")
    api = (ROOT / "api" / "optimize_v7.py").read_text(encoding="utf-8")
    api_v8 = (ROOT / "api" / "optimize_v8.py").read_text(encoding="utf-8")

    assert '"frontend" / "v7_optimize.html"' in api
    assert '"frontend" / "v7_optimize.html"' in api_v8
    assert '"%%OPTIMIZE_VERSION%%": "v8"' in api_v8
    assert '"%%OPTIMIZE_NAV_CURRENT%%": "v8_optimize"' in api_v8
    assert not (ROOT / "frontend" / "v8_optimize.html").exists()
    assert '/app/js/optimize_editor_adapter.js?v=12' in page
    assert "PBGuiOptimizeEditorAdapter.create(OPTIMIZE_VERSION" in page
    assert 'backtestVersion: BACKTEST_VERSION' in page
    for panel in ("panel-configs", "panel-queue", "panel-results", "panel-paretos"):
        assert f'id="{panel}"' in page
    for feature in ("suite-container", "opted-raw-json", "buildOptimizeEditorHtml", "collectEditorConfig"):
        assert feature in page


def test_pb8_editor_refreshes_an_ai_saved_open_config() -> None:
    """An approved AI save should reload the matching config already open in the editor."""
    page = (ROOT / "frontend" / "v7_optimize.html").read_text(encoding="utf-8")

    assert "pbgui:ai-action-completed" in page
    assert "result.action !== 'save' && result.action !== 'save_and_queue'" in page
    assert "Reloaded the config saved by PBGui AI." in page
    assert "var previousName = state.editingConfig" in page
    assert "state.editingConfig !== previousName" in page
    assert "Opened the new config saved by PBGui AI." in page
    assert "result.action === 'queue_backtests'" in page


def test_pb8_gpu_backend_and_typed_settings_are_preserved() -> None:
    """GPU must be explicit, capability-aware, and own its nested runtime settings."""
    page = (ROOT / "frontend" / "v7_optimize.html").read_text(encoding="utf-8")
    adapter = (ROOT / "frontend" / "js" / "optimize_editor_adapter.js").read_text(encoding="utf-8")

    assert "return !!normalized;" in page
    assert "optimize_backend_contract" in page
    assert "backendContract: source.backend_contract" in adapter
    assert "unavailable on this host" in page
    assert "Editor preview and Save remain available; Queue and Start are blocked" in page
    assert "disabled: unavailable" not in page
    assert 'id="optimize-gpu-section"' in page
    for field in (
        "opted-gpu-auto-lean",
        "opted-gpu-population-size",
        "opted-gpu-batch-size",
        "opted-gpu-max-dispatch-bars",
        "opted-gpu-halving-enabled",
        "opted-gpu-halving-fractions",
    ):
        assert field in page
    assert "collectOptimizeGpuSettings(optimize, strict);" in page
    assert "optimizeGpuAutoPlaceholder('population_size')" in page
    assert "optimizeGpuAutoPlaceholder('batch_size')" in page
    assert "optimizeGpuAutoPlaceholder('max_dispatch_candidate_bars')" in page
    assert "effective_defaults" in page
    assert "GPU population is owned by optimize.gpu.population_size" in page
    assert "options = options.filter(metricAvailableForCurrentBackend);" in page


def test_pb8_gpu_editor_uses_the_standard_eight_column_responsive_grid() -> None:
    """GPU settings should follow the editor's 8x1, 4x2, and 2x4 responsive grid."""
    page = (ROOT / "frontend" / "v7_optimize.html").read_text(encoding="utf-8")
    section = page.split("id=\"optimize-gpu-section\"", 1)[1].split("id=\"optimize-pymoo-section\"", 1)[0]

    for heading in (
        "Automatic sizing",
        "Exact validation &amp; checkpointing",
        "Drift safety",
        "Successive halving",
    ):
        assert heading in section
    assert "justify-content:space-between" not in section
    assert section.count("form-row cols-8") == 4
    assert section.count("gpu-desktop-headings") == 2
    assert section.count("gpu-mobile-heading") == 4
    assert "gpu-settings-grid" not in section
    assert "fieldNumber2('opted-gpu-max-dispatch-bars', 'max_dispatch_candidate_bars'" in section
    assert ".cols-8 { grid-template-columns: repeat(8, minmax(0, 1fr)); }" in page
    assert "@media (max-width: 1400px)" in page
    assert ".cols-8 { grid-template-columns: repeat(4, minmax(0, 1fr)); }" in page
    assert "@media (max-width: 700px)" in page
    assert ".cols-8 { grid-template-columns: repeat(2, minmax(0, 1fr)); }" in page
    for field in (
        "opted-gpu-population-size",
        "opted-gpu-batch-size",
        "opted-gpu-max-dispatch-bars",
        "opted-gpu-exact-workers",
        "opted-gpu-max-pending-exact",
        "opted-gpu-validate-generation",
        "opted-gpu-checkpoint-seconds",
        "opted-gpu-drift-probes",
        "opted-gpu-drift-window",
        "opted-gpu-drift-min-samples",
        "opted-gpu-drift-halt",
        "opted-gpu-halving-fractions",
        "opted-gpu-halving-survival",
        "opted-gpu-halving-min-survivors",
    ):
        call = section.split("'" + field + "'", 1)[1].split("\n", 1)[0]
        assert call.rstrip().endswith(", 1)") or ", 1, optimizeGpuAutoPlaceholder(" in call


def test_pb8_gpu_dashboard_uses_exact_budget_and_log_activity() -> None:
    """GPU progress should use exact validations and the API log section."""
    page = (ROOT / "frontend" / "v7_optimize.html").read_text(encoding="utf-8")

    assert "progress.exact_evaluations" in page
    assert "progress.target_exact_evaluations" in page
    assert "progress.proxy_evaluations" in page
    assert "progress.exact_inflight" in page
    assert "el('optlog-activity').textContent = log.last_line" in page
    assert "el('optlog-error').textContent = log.last_error" in page
    assert "buildBacktestMainPageUrl({ panel: 'queue' })" in page


def test_pb8_pareto_page_applies_typed_ai_selection_actions() -> None:
    """The AI may select exact visible Pareto candidates without executing arbitrary script."""
    page = (ROOT / "frontend" / "v7_optimize.html").read_text(encoding="utf-8")

    assert "pbgui:ai-ui-action" in page
    assert "action.type !== 'optimize.select_paretos'" in page
    assert "state.selectedParetos.add(pathsByName.get(name))" in page
    assert "event.preventDefault()" in page


def test_pareto_backtests_keep_the_scenario_selected_for_each_candidate() -> None:
    """Suite selections made on different scenario views must not become an exchange matrix."""
    page = (ROOT / "frontend" / "v7_optimize.html").read_text(encoding="utf-8")
    functions = "\n".join(
        _page_function(page, name)
        for name in ("syncSelectedParetoScenarios", "applyParetoBacktestScenario")
    )
    script = textwrap.dedent(
        f"""
        const assert = require('node:assert/strict');
        const state = {{
          selectedParetos: new Set(['/a.json']),
          selectedParetoScenarios: new Map(),
          paretoScenario: 'bybit'
        }};
        function deepClone(value) {{ return JSON.parse(JSON.stringify(value)); }}
        {functions}
        syncSelectedParetoScenarios();
        state.paretoScenario = 'hyperliquid';
        state.selectedParetos.add('/b.json');
        syncSelectedParetoScenarios();
        assert.deepEqual(Array.from(state.selectedParetoScenarios), [
          ['/a.json', 'bybit'], ['/b.json', 'hyperliquid']
        ]);

        const original = {{backtest: {{
          exchanges: ['binance', 'bybit', 'hyperliquid'], suite_enabled: true,
          scenarios: [
            {{label: 'binance', exchanges: ['binance']}},
            {{label: 'bybit', exchanges: ['bybit'], starting_balance: 2000}},
            {{label: 'hyperliquid', exchanges: ['hyperliquid']}}
          ]
        }}}};
        const prepared = applyParetoBacktestScenario(original, 'bybit');
        assert.deepEqual(prepared.backtest.exchanges, ['bybit']);
        assert.deepEqual(prepared.backtest.scenarios, [
          {{label: 'bybit', exchanges: ['bybit'], starting_balance: 2000}}
        ]);
        assert.equal(original.backtest.scenarios.length, 3);
        """
    )
    _run_node(script)


def test_optimize_page_registers_existing_queue_log_function_as_page_action() -> None:
    """The generic page action should resolve a selected item and reuse its log viewer."""
    page = (ROOT / "frontend" / "v7_optimize.html").read_text(encoding="utf-8")

    assert "window.PBGUI_AI_PAGE_ACTIONS" in page
    assert "id: 'show_log'" in page
    assert "entity_kind: 'optimizer_queue_item'" in page
    assert "openLogPanel(queueItem.filename, queueItem.name || queueItem.filename)" in page
    assert "item.status === 'running' || item.status === 'optimizing'" in page


def test_optimize_log_waits_for_first_evaluation_when_target_is_known() -> None:
    """A configured iteration target must not format a missing evaluation count."""
    page = (ROOT / "frontend" / "v7_optimize.html").read_text(encoding="utf-8")
    renderer = _page_function(page, "renderOptimizeLogDashboard")

    assert "if (progress.target_iters != null && progress.eval != null)" in renderer
    assert "progress.eval == null ? 'Waiting for evaluations...'" in renderer
    assert "counting history " in renderer
    assert "progress.estimated ? '≥ ' : ''" in renderer


def test_adapter_preserves_v7_and_round_trips_nested_v8_paths() -> None:
    """The adapter must leave PB7 flat values alone and map PB8 nested bot and bound paths."""
    script = textwrap.dedent(
        """
        const assert = require('node:assert/strict');
        const fs = require('node:fs');
        global.window = {};
        eval(fs.readFileSync('frontend/js/optimize_editor_adapter.js', 'utf8'));

        const v7 = window.PBGuiOptimizeEditorAdapter.create('%%OPTIMIZE_VERSION%%', {
          apiBase: 'https://example.test/api/optimize-v7',
          wsBase: 'wss://example.test'
        });
        const v7Side = { n_positions: 4, total_wallet_exposure_limit: 1.5, hsl_enabled: true };
        assert.equal(v7.version, 'v7');
        assert.equal(v7.getBotValue(v7Side, 'n_positions', 0), 4);
        v7.setBotValue(v7Side, 'n_positions', 5);
        assert.equal(v7Side.n_positions, 5);
        assert.equal(v7Side.risk, undefined);
        assert.deepEqual(v7.getBounds({ bounds: { long_n_positions: [1, 10, 1] } }), { long_n_positions: [1, 10, 1] });
        assert.equal(v7.queueLogFile('job'), 'optimizes/job.log');
        assert.equal(v7.websocketPath, '/api/optimize-v7/ws/opt7');

        const v8 = window.PBGuiOptimizeEditorAdapter.create('v8', {
          backtestVersion: 'v8',
          apiBase: 'https://example.test/api/optimize-v8',
          wsBase: 'wss://example.test',
          navSubtitle: 'PBv8 OPTIMIZE',
          navCurrent: 'v8_optimize'
        });
        const v8Side = { risk: { n_positions: 7, total_wallet_exposure_limit: 2 }, hsl: { enabled: true } };
        assert.equal(v8.getBotValue(v8Side, 'n_positions', 0), 7);
        v8.setBotValue(v8Side, 'total_wallet_exposure_limit', 2.5);
        assert.equal(v8Side.risk.total_wallet_exposure_limit, 2.5);
        assert.equal(v8.getBotHslValue(v8Side, 'enabled', false), true);
        assert.equal(v8.hslRuntimeOverrideKey('long', 'enabled'), 'bot.long.hsl.enabled');

        const optimize = { bounds: {
          long: { risk: { n_positions: [1, 10, 1] }, strategy: { ema_anchor: { offset: [0, 0.1, 0.001] } } },
          short: { risk: { total_wallet_exposure_limit: [0, 2, 0.1] } }
        } };
        const flat = v8.getBounds(optimize);
        assert.deepEqual(flat['long.risk.n_positions'], [1, 10, 1]);
        assert.deepEqual(flat['long.strategy.ema_anchor.offset'], [0, 0.1, 0.001]);
        assert.equal(v8.boundGroup('long.strategy.ema_anchor.offset'), 'long');
        assert.equal(v8.boundSuffix('long.strategy.ema_anchor.offset'), 'strategy.ema_anchor.offset');
        assert.deepEqual(v8.boundMetaKeys('long.risk.n_positions'), [
          'long.risk.n_positions', 'risk.n_positions', 'risk_n_positions', 'n_positions'
        ]);
        assert.deepEqual(v8.boundMetaKeys('long.strategy.ema_anchor.entry.initial_ema_dist'), [
          'long.strategy.ema_anchor.entry.initial_ema_dist',
          'strategy.ema_anchor.entry.initial_ema_dist',
          'strategy_ema_anchor_entry_initial_ema_dist',
          'entry_initial_ema_dist',
          'initial_ema_dist'
        ]);
        const saved = {};
        v8.setBounds(saved, flat);
        assert.deepEqual(saved.bounds, optimize.bounds);
        assert.equal(v8.queueLogFile('job'), 'optimizes_v8/job.log');
        assert.equal(v8.websocketPath, '/api/optimize-v8/ws/opt8');
        assert.equal(v8.backtestApiBase(), 'https://example.test/api/backtest-v8');
        assert.equal(v8.metadataApiBase(), 'https://example.test/api/v8');
        assert.equal(v8.canonicalFixedParam('long.strategy.*'), 'bot.long.strategy.*');
        assert.equal(v8.canonicalFixedParam('bot.long.strategy.*'), 'bot.long.strategy.*');

        const metadata = v8.normalizeMetadata({
          strategies: ['trailing_martingale', 'ema_anchor'],
          active_bounds: {
            trailing_martingale: {long: {strategy: {trailing_martingale: {entry: {threshold: [1, 2, 0.1]}}}}},
            ema_anchor: {long: {strategy: {ema_anchor: {entry: {offset: [3, 4, 0.1]}}}}}
          },
          strategy_defaults: {long: {ema_anchor: {entry: {offset: 3}}}},
          bounds: {long: {risk: {n_positions: [4, 12, 1]}}},
          runtime_overrides: [{key: 'future.runtime.option', type: 'json', defaultValue: {enabled: true}}],
          optimize_defaults: {}
        });
        assert.deepEqual(metadata.strategyBounds.ema_anchor['long.strategy.ema_anchor.entry.offset'], [3, 4, 0.1]);
        assert.equal(metadata.strategyDefaults.long.ema_anchor.entry.offset, 3);
        assert.deepEqual(metadata.hslSignalModes, ['coin', 'pside', 'unified']);
        assert.deepEqual(metadata.runtimeOverrides.map(field => field.key), [
          'bot.long.hsl.enabled',
          'bot.long.hsl.no_restart_drawdown_threshold',
          'bot.short.hsl.enabled',
          'bot.short.hsl.no_restart_drawdown_threshold',
          'future.runtime.option'
        ]);
        const runtimeMetadata = v8.normalizeMetadata({template: {
          bot: {
            long: {hsl: {enabled: false, no_restart_drawdown_threshold: 0.9}},
            short: {hsl: {enabled: true, no_restart_drawdown_threshold: 0.8}}
          },
          optimize: {fixed_runtime_overrides: {
            'bot.long.hsl.restart_after_red_policy': 'always',
            'bot.short.hsl.restart_after_red_policy': 'threshold'
          }}
        }});
        assert.deepEqual(runtimeMetadata.runtimeOverrides.map(field => field.key), [
          'bot.long.hsl.enabled',
          'bot.long.hsl.no_restart_drawdown_threshold',
          'bot.short.hsl.enabled',
          'bot.short.hsl.no_restart_drawdown_threshold',
          'bot.long.hsl.restart_after_red_policy',
          'bot.short.hsl.restart_after_red_policy'
        ]);
        const longPolicy = runtimeMetadata.runtimeOverrides.find(field => field.key === 'bot.long.hsl.restart_after_red_policy');
        assert.equal(longPolicy.label, 'Long HSL restart after RED');
        assert.deepEqual(longPolicy.choices, ['always', 'threshold', 'never']);
        const shortEnabled = runtimeMetadata.runtimeOverrides.find(field => field.key === 'bot.short.hsl.enabled');
        assert.equal(shortEnabled.defaultValue, true);
        assert.equal(shortEnabled.storage, 'bot_hsl');
        assert.equal(shortEnabled.botKey, 'enabled');
        const shortThreshold = runtimeMetadata.runtimeOverrides.find(field => field.key === 'bot.short.hsl.no_restart_drawdown_threshold');
        assert.equal(shortThreshold.storage, 'bot_hsl');
        assert.equal(shortThreshold.botKey, 'no_restart_drawdown_threshold');
        assert.equal(metadata.boundsMeta, null);
        assert.deepEqual(v8.normalizeMetadata({bounds_meta: {n_positions: [0, 100, 1, 1, 0]}}).boundsMeta, {
          n_positions: [0, 100, 1, 1, 0]
        });

        const cfg = {
          optimize: { seed: 123 },
          pbgui: { optimize_runtime: {
            mode: 'fresh', fine_tune_params: ['long.risk'], polish_percentage: 0.25,
            polish_bounds_mode: 'override-tunable'
          } }
        };
        const html = v8.versionRunSettingsHtml(cfg, String);
        assert.match(html, /opted-rng-seed/);
        assert.match(html, /value="123"/);
        assert.match(html, /polish_percentage \\(%\\)/);
        assert.match(html, /value="25"/);
        const fields = {
          'opted-rng-seed': { value: '99' },
          'opted-fine-tune-params': { value: 'long.risk, short.strategy' },
          'opted-polish-pct': { value: '20' },
          'opted-polish-bounds-mode': { value: 'override-all' }
        };
        v8.collectVersionRunSettings(cfg, (id) => fields[id], true);
        assert.equal(cfg.optimize.seed, 99);
        assert.deepEqual(cfg.pbgui.optimize_runtime, {
          mode: 'fresh',
          fine_tune_params: ['long.risk', 'short.strategy'],
          polish_percentage: 0.2,
          polish_bounds_mode: 'override-all'
        });
        assert.equal(
          v8.resumeQueueRequest('queue-id', '/managed/result').path,
          '/queue/queue-id/resume-checkpoint'
        );
        assert.equal(v8.resultResumeRequest('resume-name', '/managed/result').path, '/results/resume');
        assert.deepEqual(v8.resultCapabilities({
          has_pareto: true, resumable: false, has_config: true, supports_3d: false, supports_dash: true
        }), {
          hasPareto: true, resumable: false, hasConfig: true, supports3d: false, supportsDash: true
        });
        """
    )
    completed = subprocess.run(["node", "-e", script], cwd=ROOT, text=True, capture_output=True, check=False)
    assert completed.returncode == 0, completed.stderr or completed.stdout


def test_pb8_run_controls_and_exact_resume_are_version_routed() -> None:
    """PB8 RNG/fine-tune/polish controls and checkpoint resume must remain distinct actions."""
    page = (ROOT / "frontend" / "v7_optimize.html").read_text(encoding="utf-8")
    adapter = (ROOT / "frontend" / "js" / "optimize_editor_adapter.js").read_text(encoding="utf-8")

    for control in ("opted-rng-seed", "opted-fine-tune-params", "opted-polish-pct", "opted-polish-bounds-mode"):
        assert control in adapter
    assert "versionRunSettingsHtml(cfg, escapeHtml)" in page
    assert "collectVersionRunSettings(cfg, el, strict)" in page
    assert "continueOptimizeFromResult" in page
    assert "resumeOptimizeResult" in page
    assert "resultResumeRequest" in page
    assert "'/resume-checkpoint'" in adapter
    assert "JSON.stringify({ source: resultPath })" in adapter
    assert "optimize_runtime" in adapter
    assert "polish_percentage" in adapter
    assert "capabilities.resumable" in page
    assert "btn-resume-result" in page


def test_pb8_metadata_failure_shows_persistent_update_warning() -> None:
    """An unavailable PB8 runtime must remain visible without aborting page initialization."""
    page = (ROOT / "frontend" / "v7_optimize.html").read_text(encoding="utf-8")
    functions = "\n\n".join(
        _page_function(page, name)
        for name in ("setPb8RuntimeWarning", "handlePb8RuntimeUnavailable", "loadSettings")
    )
    script = textwrap.dedent(
        f"""
        const assert = require('node:assert/strict');
        const detail = {{textContent: ''}};
        const classes = new Set();
        const document = {{
          body: {{classList: {{toggle(name, active) {{ if (active) classes.add(name); else classes.delete(name); }}}}}}
        }};
        const optimizeEditorAdapter = {{isV8: true, metadataPath: '/metadata'}};
        const state = {{settingsLoadSeq: 0, settingsPushSeq: 0, navigationSeq: 0, settings: {{}}}};
        function el(id) {{ return id === 'pb8-runtime-warning-detail' ? detail : null; }}
        async function apiFetch() {{ const error = new Error('Run Update PB8.'); error.status = 503; throw error; }}
        {functions}
        loadSettings().then(() => {{
          assert.equal(detail.textContent, 'Run Update PB8.');
          assert.equal(classes.has('pb8-runtime-warning-visible'), true);
        }}).catch(error => {{ console.error(error); process.exit(1); }});
        """
    )

    assert 'id="pb8-runtime-warning"' in page
    assert "PB8 update required" in page
    assert 'href="/api/vps-manager/main_page"' in page
    _run_node(script)


def test_initial_configs_load_does_not_wait_for_settings_metadata() -> None:
    """PB8 configs should start loading immediately while slower settings initialize."""
    page = (ROOT / "frontend" / "v7_optimize.html").read_text(encoding="utf-8")
    init_function = _page_function(page, "init").split("\n\ninit().catch", 1)[0]
    script = textwrap.dedent(
        f"""
        const assert = require('node:assert/strict');
        let resolveSettings;
        let calls = [];
        const optimizeEditorAdapter = {{configureUi() {{}}}};
        const state = {{initialLoadPending: true}};
        const location = {{hash: ''}};
        const window = {{location: {{href: 'https://example.test/api/optimize-v8/main_page'}}}};
        const PANEL_META = {{}};
        function attachEventHandlers() {{}}
        function initSelections() {{}}
        function initSidebarResize() {{}}
        function initPlotModalWindow() {{}}
        function initOptimizeOhlcvPreflightController() {{}}
        function setPanel() {{}}
        function connectWS() {{}}
        function loadSettings() {{ calls.push('settings'); return new Promise(resolve => {{ resolveSettings = resolve; }}); }}
        function loadOptimizeMetadata() {{ calls.push('metadata'); return Promise.resolve(); }}
        function loadConfigs() {{ calls.push('configs'); return Promise.resolve(); }}
        function loadQueue() {{ calls.push('queue'); return Promise.resolve(); }}
        function loadResults() {{ calls.push('results'); return Promise.resolve(); }}
        function handleMigrationDraft() {{ calls.push('migration-draft'); return Promise.resolve(); }}
        function handleIncomingDraft() {{ calls.push('draft'); return Promise.resolve(); }}
        function handleOpenConfigParam() {{ calls.push('open'); return Promise.resolve(); }}
        {init_function}
        (async () => {{
          const pending = init();
          await new Promise(resolve => setImmediate(resolve));
          assert.deepEqual(calls, ['metadata', 'settings', 'configs', 'queue', 'results']);
          resolveSettings();
          await pending;
          assert.deepEqual(calls, ['metadata', 'settings', 'configs', 'queue', 'results', 'draft', 'open']);
        }})().catch(error => {{ console.error(error); process.exit(1); }});
        """
    )

    assert "?include_result_summary=false" in page
    _run_node(script)


def test_migration_preview_hides_config_list_while_metadata_loads() -> None:
    """The destination page should show a preview loader instead of flashing the Configs list."""
    page = (ROOT / "frontend" / "v7_optimize.html").read_text(encoding="utf-8")
    function = _page_function(page, "showMigrationDraftLoadingState")
    script = textwrap.dedent(
        f"""
        const assert = require('node:assert/strict');
        const nodes = {{
          'configs-editor': {{style: {{}}, innerHTML: ''}},
          'configs-toolbar': {{style: {{}}}},
          'configs-list-wrap': {{style: {{}}}},
          'sidebar-inner': {{style: {{}}}},
          'sidebar-editor': {{style: {{}}}}
        }};
        function el(id) {{ return nodes[id] || null; }}
        {function}
        showMigrationDraftLoadingState();
        assert.equal(nodes['configs-toolbar'].style.display, 'none');
        assert.equal(nodes['configs-list-wrap'].style.display, 'none');
        assert.equal(nodes['configs-editor'].style.display, 'block');
        assert.match(nodes['configs-editor'].innerHTML, /Loading converted PB8 Optimize preview/);
        assert.equal(nodes['sidebar-inner'].style.display, 'none');
        """
    )

    _run_node(script)


def test_pb8_queue_settings_keep_the_complete_shared_controls() -> None:
    """PB8 must expose the same autostart CPU and market-data settings as PB7."""
    page = (ROOT / "frontend" / "v7_optimize.html").read_text(encoding="utf-8")
    adapter = (ROOT / "frontend" / "js" / "optimize_editor_adapter.js").read_text(encoding="utf-8")

    for control in ("settings-cpu-value", "settings-cpu-override", "settings-use-pbgui-market-data"):
        assert f'id="{control}"' in page
        assert control not in adapter.split("configureUi: function", 1)[1]
    assert "JSON.stringify({ cpu: cpu, autostart: autostart, cpu_override: cpuOverride, use_pbgui_market_data: usePbguiMarketData })" in page


def test_queue_cpu_can_be_edited_before_override_is_enabled() -> None:
    """The saved autostart CPU remains editable while the override is inactive."""
    page = (ROOT / "frontend" / "v7_optimize.html").read_text(encoding="utf-8")
    functions = "\n\n".join(
        _page_function(page, name)
        for name in ("syncQueueSettingsModalFields", "adjustQueueSettingsCpu")
    )
    script = textwrap.dedent(
        f"""
        const assert = require('node:assert/strict');
        const nodes = {{
          'settings-cpu-value': {{value: '', disabled: true}},
          'settings-cpu-max': {{textContent: ''}},
          'settings-autostart': {{checked: false}},
          'settings-cpu-override': {{checked: false}},
          'settings-use-pbgui-market-data': {{checked: false}},
          'btn-settings-cpu-down': {{disabled: true}},
          'btn-settings-cpu-up': {{disabled: true}},
          'settings-cpu-effective': {{textContent: ''}}
        }};
        const state = {{
          settings: {{cpu: 8, cpu_max: 16, host_cpu_count: 16, cpu_override: false}},
          settingsModalDirty: false,
          settingsModalCpuDirty: false
        }};
        function el(id) {{ return nodes[id]; }}
        function normalizeOptimizePositiveInteger(value) {{
          const parsed = Number(value);
          return Number.isFinite(parsed) ? Math.max(1, Math.round(parsed)) : null;
        }}
        function normalizeAutostart(value) {{ return !!value; }}

        {functions}

        syncQueueSettingsModalFields();
        assert.equal(nodes['settings-cpu-value'].disabled, false);
        assert.equal(nodes['btn-settings-cpu-down'].disabled, false);
        assert.equal(nodes['btn-settings-cpu-up'].disabled, false);
        assert.match(nodes['settings-cpu-effective'].textContent, /saved override: 8 CPU/);
        adjustQueueSettingsCpu(1);
        assert.equal(nodes['settings-cpu-value'].value, '9');
        assert.equal(state.settings.cpu, 9);
        assert.equal(state.settingsModalCpuDirty, true);
        """
    )
    _run_node(script)


def test_saving_a_queue_opened_config_refreshes_that_queue_snapshot() -> None:
    """Explicit queue editing must persist the saved config back into that queue snapshot."""
    page = (ROOT / "frontend" / "v7_optimize.html").read_text(encoding="utf-8")
    functions = "\n\n".join(
        _page_function(page, name)
        for name in ("openQueueConfigEditor", "refreshOpenedQueueSnapshot")
    )
    save_source = _page_function(page, "saveEditor")
    script = textwrap.dedent(
        f"""
        const assert = require('node:assert/strict');
        const requests = [];
        const state = {{editorQueueFilename: '', editorReturnPanel: 'configs'}};
        async function apiFetch(path, options) {{
          requests.push({{path, options}});
          return path.endsWith('/config')
            ? {{name: 'queued-config', config: {{optimize: {{n_cpus: 1}}}}}}
            : {{ok: true}};
        }}
        function openEditorWithConfig(data, name, sourceName) {{
          assert.equal(data.config.optimize.n_cpus, 1);
          assert.equal(name, 'queued-config');
          assert.equal(sourceName, 'queued-config');
        }}
        function queueConfigChoiceCandidates() {{ return []; }}

        {functions}

        (async () => {{
          await openQueueConfigEditor('queue-job');
          assert.equal(state.editorQueueFilename, 'queue-job');
          assert.equal(state.editorReturnPanel, 'queue');
          await refreshOpenedQueueSnapshot(state.editorQueueFilename, 'queued-config');
          assert.equal(requests[1].path, '/queue/queue-job/repair-config');
          assert.equal(requests[1].options.method, 'POST');
          assert.deepEqual(JSON.parse(requests[1].options.body), {{name: 'queued-config'}});
        }})().catch((error) => {{ console.error(error); process.exitCode = 1; }});
        """
    )
    _run_node(script)
    assert "var queueFilename = state.editorQueueFilename;" in save_source
    assert "if (!(optimizeEditorAdapter.isV8 && isNewName))" in save_source
    assert "await refreshOpenedQueueSnapshot(queueFilename, name);" in save_source


def test_v8_save_and_queue_with_new_name_does_not_rebind_opened_job() -> None:
    """Renaming a queue-opened PB8 config creates a new job and leaves the old job immutable."""
    page = (ROOT / "frontend" / "v7_optimize.html").read_text(encoding="utf-8")
    save_source = _page_function(page, "saveEditor")
    script = textwrap.dedent(
        f"""
        const assert = require('node:assert/strict');
        const requests = [];
        const state = {{
          editorQueueFilename: 'job-030',
          editorSourceName: 'HYPE_trailing_030',
          selectedConfigs: new Set()
        }};
        const optimizeEditorAdapter = {{isV8: true}};
        function editorVisible() {{ return true; }}
        function ensureRawJsonValidForSave() {{ return true; }}
        function ensureStructuredJsonFieldsValidForSave() {{ return true; }}
        function collectEditorConfig() {{ return {{name: 'HYPE_trailing_035', config: {{optimize: {{n_cpus: 4}}}}}}; }}
        function setPageEditorStatus() {{}}
        function closeEditor() {{}}
        function toast() {{}}
        async function loadConfigs() {{}}
        async function loadQueue() {{}}
        function setPanel() {{}}
        async function refreshOpenedQueueSnapshot(filename, name) {{
          requests.push({{path: '/queue/' + filename + '/repair-config', name}});
        }}
        async function apiFetch(path, options) {{ requests.push({{path, options}}); return {{ok: true}}; }}

        {save_source}

        (async () => {{
          await saveEditor(true);
          assert.deepEqual(requests.map(item => item.path), [
            '/configs/HYPE_trailing_035',
            '/queue'
          ]);
          assert.deepEqual(JSON.parse(requests[1].options.body), {{name: 'HYPE_trailing_035'}});
        }})().catch((error) => {{ console.error(error); process.exitCode = 1; }});
        """
    )
    _run_node(script)


def test_home_returns_queue_opened_editor_to_queue() -> None:
    """Home and Save close a queue-opened editor back to its originating panel."""
    page = (ROOT / "frontend" / "v7_optimize.html").read_text(encoding="utf-8")
    close_source = _page_function(page, "closeEditor")
    script = textwrap.dedent(
        f"""
        const assert = require('node:assert/strict');
        const nodes = {{
          'configs-editor': {{style: {{display: 'block'}}, innerHTML: 'editor'}},
          'configs-toolbar': {{style: {{display: 'none'}}}},
          'configs-list-wrap': {{style: {{display: 'none'}}}},
          'sidebar-inner': {{style: {{display: 'none'}}}},
          'sidebar-editor': {{style: {{display: ''}}}}
        }};
        const state = {{
          editingConfig: 'queued-config',
          editorSourceName: 'queued-config',
          editorQueueFilename: 'queue-job',
          editorReturnPanel: 'queue',
          editorDraftName: 'queued-config',
          editorBackendHint: '',
          editorLastConfig: {{}}
        }};
        let selectedPanel = '';
        const window = {{PBGuiEditorShared: {{clearFixedValidationStatus() {{}}}}}};
        const _optOhlcvPreflightController = null;
        function editorVisible() {{ return true; }}
        function el(id) {{ return nodes[id]; }}
        function resetOptimizeEditorUiState() {{}}
        function setOptBotParamStatus() {{}}
        function setPanel(panel) {{ selectedPanel = panel; }}

        {close_source}

        closeEditor();
        assert.equal(selectedPanel, 'queue');
        assert.equal(state.editorQueueFilename, '');
        assert.equal(state.editorReturnPanel, 'configs');
        assert.equal(nodes['configs-editor'].style.display, 'none');
        """
    )
    _run_node(script)


def test_results_paretos_logs_and_handoffs_use_adapter_routes() -> None:
    """Version-owned artifacts and handoffs must not be sent through hard-coded PB7 routes."""
    page = (ROOT / "frontend" / "v7_optimize.html").read_text(encoding="utf-8")

    for contract in (
        "optimizeEditorAdapter.resultsPath",
        "optimizeEditorAdapter.resultConfigPath",
        "optimizeEditorAdapter.resultDeletePath",
        "optimizeEditorAdapter.paretosPath",
        "optimizeEditorAdapter.paretoFilePath",
        "optimizeEditorAdapter.paretoSeedBundlePath",
        "optimizeEditorAdapter.queueLogFile",
        "optimizeEditorAdapter.websocketPath",
        "optimizeEditorAdapter.backtestApiBase()",
        "optimizeEditorAdapter.archiveApiBase()",
    ):
        assert contract in page
    assert "API_BASE.replace('/optimize-v7'" not in page
    assert "WS_BASE + '/api/optimize-v7/ws/opt7'" not in page
    assert "'optimizes/' + filename" not in page
    assert "params.set('optimize_version', optimizeEditorAdapter.version)" in page


def test_pb8_optimize_archive_export_and_import_use_the_shared_service() -> None:
    """PB8 exposes existing archive controls and sends its version to the PB7-owned service."""
    page = (ROOT / "frontend" / "v7_optimize.html").read_text(encoding="utf-8")
    adapter = (ROOT / "frontend" / "js" / "optimize_editor_adapter.js").read_text(encoding="utf-8")

    assert "supportsArchive: true" in adapter
    assert "return apiBase.replace(/\\/optimize-v[78]$/, '/backtest-v7')" in adapter
    assert "'btn-archive-selected'" not in adapter.split("configureUi: function", 1)[1]
    assert "'/optimize-configs?version=' + encodeURIComponent(optimizeEditorAdapter.version)" in page
    assert "optimize_version: optimizeEditorAdapter.version" in page


def test_pb8_metadata_drives_bounds_limits_and_runtime_options() -> None:
    """PB8 editor options must be loaded from runtime metadata instead of copied PB7 defaults."""
    page = (ROOT / "frontend" / "v7_optimize.html").read_text(encoding="utf-8")
    adapter = (ROOT / "frontend" / "js" / "optimize_editor_adapter.js").read_text(encoding="utf-8")

    assert "metadataPath: isV8 ? '/metadata' : ''" in adapter
    assert "Promise.all([loadSettings(), metadataPromise])" in page
    assert "metadata.optimizeDefaults" in page
    assert "metadata.limitsMeta" in page
    assert "metadata.boundsMeta" in page
    assert "metadata.runtimeOverrides" in page
    assert "metadata.strategies" in page
    assert "metadata.strategyBounds" in page
    assert "metadata.strategyDefaults" in page
    assert "opted-strategy-kind" in page
    assert "changeOptimizeStrategyKind(this.value)" in page
    assert "state.optimizeStrategyBoundsCache" in page
    assert "getOptimizeStrategyBotDefault" in page
    assert "data-pb8-enable-override" in adapter
    assert "supportsParetoExplorer: true" in adapter
    assert "supportsBacktestHandoff: true" in adapter
    assert "optimizeEditorAdapter.getBounds(optimize)" in page
    assert "optimizeEditorAdapter.setBounds(optimize, collectedBounds)" in page
    assert "version: optimizeEditorAdapter.version" in page
    assert "var maxPendingInput = el('opted-max-pending-starting-evals')" in page


def test_pb8_runtime_metadata_preserves_hsl_enable_controls() -> None:
    """Runtime-provided overrides must extend rather than remove required HSL switches."""
    page = (ROOT / "frontend" / "v7_optimize.html").read_text(encoding="utf-8")
    load_metadata = "\n\n".join(
        _page_function(page, name) for name in ("applyOptimizeMetadata", "loadOptimizeMetadata")
    )
    script = textwrap.dedent(
        f"""
        const assert = require('node:assert/strict');
        const optimizeEditorAdapter = {{
          metadataPath: '/metadata',
          isV8: true,
          normalizeMetadata: value => ({{runtimeOverrides: [
            {{key: 'bot.long.hsl.enabled', side: 'long', type: 'boolean'}},
            {{key: 'bot.short.hsl.enabled', side: 'short', type: 'boolean'}},
            ...value.runtimeOverrides
          ]}})
        }};
        const state = {{settings: {{}}}};
        const OPT_BOUNDS_META = {{}};
        let OPT_FIXED_RUNTIME_OVERRIDE_FIELDS = [];
        const apiFetch = async () => ({{runtimeOverrides: [
          {{key: 'bot.long.hsl.restart_after_red_policy', type: 'string', defaultValue: 'always'}},
          {{key: 'bot.short.hsl.restart_after_red_policy', type: 'string', defaultValue: 'always'}}
        ]}});
        const normalizeLimitsMeta = value => value;
        const deepClone = value => JSON.parse(JSON.stringify(value));
        const setPb8RuntimeWarning = () => {{}};
        {load_metadata}
        (async () => {{
          await loadOptimizeMetadata();
          assert.deepEqual(OPT_FIXED_RUNTIME_OVERRIDE_FIELDS.map(field => field.key), [
            'bot.long.hsl.enabled',
            'bot.short.hsl.enabled',
            'bot.long.hsl.restart_after_red_policy',
            'bot.short.hsl.restart_after_red_policy'
          ]);
        }})().catch(error => {{ console.error(error); process.exit(1); }});
        """
    )
    _run_node(script)


def test_pb8_runtime_overrides_render_by_side_with_policy_selects() -> None:
    """PB8 Long/Short overrides must stay separated and enums must use selects."""
    page = (ROOT / "frontend" / "v7_optimize.html").read_text(encoding="utf-8")

    assert "Runtime overrides long" in page
    assert "Runtime overrides short" in page
    assert "field.choices" in _page_function(page, "renderOptimizeRuntimeOverridesEditor")
    assert "groups[side === 'long' || side === 'short' ? side : 'other']" in page


def test_pb8_hsl_controls_write_bot_config_and_not_runtime_overrides() -> None:
    """PB8 HSL values must survive the editor roundtrip at their PB8 schema paths."""
    page = (ROOT / "frontend" / "v7_optimize.html").read_text(encoding="utf-8")
    functions = "\n\n".join(
        _page_function(page, name)
        for name in (
            "normalizeOptimizeRuntimeOverrideMap",
            "normalizeOptimizeRuntimeOverrideBooleanValue",
            "parseOptimizeRuntimeOverrideValue",
            "normalizeOptimizeRuntimeOverrideInputText",
            "setOptimizeBotHslField",
            "toggleOptimizeRuntimeOverrideCheckbox",
            "updateOptimizeRuntimeOverrideValue",
            "collectOptimizeRuntimeOverrides",
        )
    )
    script = textwrap.dedent(
        f"""
        const assert = require('node:assert/strict');
        const fields = [
          {{key: 'bot.long.hsl.enabled', side: 'long', storage: 'bot_hsl', botKey: 'enabled', type: 'boolean'}},
          {{key: 'bot.long.hsl.no_restart_drawdown_threshold', side: 'long', storage: 'bot_hsl', botKey: 'no_restart_drawdown_threshold', type: 'number'}},
          {{key: 'bot.long.hsl.restart_after_red_policy', side: 'long', type: 'string'}}
        ];
        const textarea = {{
          value: JSON.stringify({{hsl: {{enabled: false, no_restart_drawdown_threshold: 1}}}}),
          dispatchEvent: () => {{}}
        }};
        const state = {{runtimeOverrideValues: {{'bot.long.hsl.restart_after_red_policy': 'always'}}}};
        const OPT_FIXED_RUNTIME_OVERRIDE_FIELDS = fields;
        const optimizeEditorAdapter = {{
          isV8: true,
          setBotHslValue: (root, key, value) => {{ root.hsl ||= {{}}; root.hsl[key] = value; }}
        }};
        const el = id => id === 'opted-bot-long' ? textarea : null;
        const getOptimizeRuntimeOverrideFieldMeta = key => fields.find(field => field.key === key);
        const renderOptimizeBoundsEditor = () => {{}};
        const scheduleStructuredEditorSync = () => {{}};
        const autoResizeTa = () => {{}};
        const cloneJsonValue = value => value === undefined ? undefined : JSON.parse(JSON.stringify(value));
        global.Event = class Event {{ constructor(type) {{ this.type = type; }} }};
        {functions}

        toggleOptimizeRuntimeOverrideCheckbox('bot.long.hsl.enabled', true);
        updateOptimizeRuntimeOverrideValue('bot.long.hsl.no_restart_drawdown_threshold', '0.75', null);
        const bot = JSON.parse(textarea.value);
        assert.equal(bot.hsl.enabled, true);
        assert.equal(bot.hsl.no_restart_drawdown_threshold, 0.75);
        assert.deepEqual(collectOptimizeRuntimeOverrides({{fallbackValue: {{}}}}), {{
          'bot.long.hsl.restart_after_red_policy': 'always'
        }});
        """
    )
    _run_node(script)


def test_pb8_default_bounds_do_not_limit_slider_minima() -> None:
    """PB8 default search ranges must not replace wider parameter slider ranges."""
    page = (ROOT / "frontend" / "v7_optimize.html").read_text(encoding="utf-8")
    functions = "\n\n".join(
        _page_function(page, name)
        for name in (
            "countOptimizeDecimals",
            "getOptimizeStepFromDecimals",
            "getOptimizeRoundToSignificantDigits",
            "getOptimizeBoundPrecisionFromStep",
            "getOptimizeBoundMeta",
        )
    )
    script = textwrap.dedent(
        f"""
        const assert = require('node:assert/strict');
        const fs = require('node:fs');
        global.window = {{}};
        eval(fs.readFileSync('frontend/js/optimize_editor_adapter.js', 'utf8'));
        const optimizeEditorAdapter = window.PBGuiOptimizeEditorAdapter.create('v8', {{}});
        const OPT_BOUNDS_META = {{
          n_positions: [0, 100, 1, 1, 0],
          entry_initial_ema_dist: [-1, 1, 0.0001, 0.00001, 4],
          hsl_red_threshold: [0.001, 1, 0.001, 0.00001, 3]
        }};
        function el() {{ return null; }}
        function getOptimizeBoundSuffix(key) {{ return optimizeEditorAdapter.boundSuffix(key); }}
        function getOptimizeBoundRequiredMin() {{ return null; }}

        {functions}

        const positions = getOptimizeBoundMeta('long.risk.n_positions', 4, 12, 1);
        assert.equal(positions.min, 0);
        assert.equal(positions.max, 100);
        assert.equal(positions.sliderStep, 1);
        assert.ok(1 >= positions.min && 1 <= positions.max);

        const strategy = getOptimizeBoundMeta(
          'long.strategy.trailing_martingale.entry.initial_ema_dist', -0.1, 0.1, 0.001
        );
        assert.equal(strategy.min, -1);
        assert.equal(strategy.max, 1);

        const hsl = getOptimizeBoundMeta('short.hsl.red_threshold', 0.01, 0.15, 0.001);
        assert.equal(hsl.min, 0.001);
        assert.equal(hsl.max, 1);
        """
    )
    _run_node(script)
    assert "if (maxPendingInput)" in page
    assert "el('opted-max-pending-starting-evals').value =" not in page


def test_pb8_forager_ema_span_sliders_require_positive_values() -> None:
    """PB8 Forager EMA spans use Fixed for exclusion instead of a zero range."""
    page = (ROOT / "frontend" / "v7_optimize.html").read_text(encoding="utf-8")
    functions = "\n\n".join(
        _page_function(page, name)
        for name in (
            "isOptimizeHslRedThresholdBound",
            "getOptimizeHslRedThresholdRequiredMin",
            "getOptimizeBoundRequiredMin",
        )
    )
    script = textwrap.dedent(
        f"""
        const assert = require('node:assert/strict');
        const optimizeEditorAdapter = {{isV8: true}};
        const OPT_HSL_RED_THRESHOLD_MIN = 0.000001;
        function getOptimizeBoundGroup() {{ return 'long'; }}
        function getOptimizeBoundSuffix(key) {{ return String(key).replace(/^long\\./, ''); }}
        function isOptimizeHslEnabledForSide() {{ return false; }}
        {functions}
        assert.equal(getOptimizeBoundRequiredMin('long.forager.volume_ema_span_1m'), 1);
        assert.equal(getOptimizeBoundRequiredMin('long.forager.volatility_ema_span_1m'), 1);
        assert.equal(getOptimizeBoundRequiredMin('long.forager.volume_drop_pct'), null);
        """
    )
    _run_node(script)


def test_cookie_auth_and_v7_migration_are_available_from_the_shared_page() -> None:
    """PB8 uses its HttpOnly cookie and PB7 exposes official Optimize migration."""
    page = (ROOT / "frontend" / "v7_optimize.html").read_text(encoding="utf-8")
    api_v7 = (ROOT / "api" / "optimize_v7.py").read_text(encoding="utf-8")

    assert "if (TOKEN) headers.Authorization = 'Bearer ' + TOKEN" in page
    assert "Object.assign({}, init.headers || {}, { Authorization" not in page
    assert "cfg-migrate-v8" in page
    assert "pareto-migrate-v8" in page
    assert "migrateOptimizeConfigToV8" in page
    assert "migrateParetoConfigToV8" in page
    assert "migration_draft_id=" in page
    assert "async function handleMigrationDraft(draftPromise)" in page
    assert "await handleMigrationDraft(migrationDraftPromise);" in page
    assert "showMigrationDraftLoadingState(true);" in page
    assert "showMigrationDraftLoadingState(false);" in page
    assert "applyOptimizeMetadata(draft.optimize_metadata);" in page
    assert "migration_report: state.editorMigrationReport" in page
    assert "Loaded unsaved V7 → V8 Optimize preview" in page
    assert "showOptimizeMigrationReviewWarnings" not in page
    assert "V8 conversion review recommended" not in page
    assert "/api/optimize-v8/migrate-v7" in page
    assert "json.dumps(\"\")" in api_v7

    optimize_migration = _page_function(page, "migrateOptimizeConfigToV8")
    pareto_migration = _page_function(page, "migrateParetoConfigToV8")
    assert "open_config=" not in optimize_migration
    assert "open_config=" not in pareto_migration


def test_optimize_migration_error_is_compact_and_actionable() -> None:
    """Migration failures must not dump the complete official report into a dialog."""
    page = (ROOT / "frontend" / "v7_optimize.html").read_text(encoding="utf-8")
    show_error = _page_function(page, "showOptimizeMigrationError")
    script = textwrap.dedent(
        f"""
        const assert = require('node:assert/strict');
        let dialog = null;
        const window = {{}};
        window.PBGuiDialogs = {{}};
        window.PBGuiDialogs.alert = async function(options) {{ dialog = options; }};
        {show_error}
        (async () => {{
          await showOptimizeMigrationError({{
            message: 'Migration requires manual review',
            detail: {{report: {{
              manual_review_fields: ['optimize.example'],
              dropped_unsupported_fields: ['bot.long.legacy'],
              moved_fields: Array.from({{length: 300}}, (_, index) => 'moved.' + index),
              behavior_change_warnings: ['Review changed optimizer behavior.']
            }}}}
          }});
          assert.match(dialog.detail, /optimize\\.example/);
          assert.match(dialog.detail, /bot\\.long\\.legacy/);
          assert.match(dialog.detail, /Review changed optimizer behavior/);
          assert.doesNotMatch(dialog.detail, /moved\\.299/);
          assert.ok(dialog.detail.length < 1000);
        }})().catch(error => {{ console.error(error); process.exit(1); }});
        """
    )
    _run_node(script)


def test_plot_modal_is_movable_and_resizable() -> None:
    """Pareto Dash and plot windows expose standard drag and eight-direction resize controls."""
    page = (ROOT / "frontend" / "v7_optimize.html").read_text(encoding="utf-8")

    for direction in ("n", "s", "w", "e", "nw", "ne", "sw", "se"):
        assert f'class="pnr pnr-{direction}" data-dir="{direction}"' in page
    assert "function initPlotModalWindow()" in page
    assert "initPlotModalWindow();" in page
    assert "frame.style.pointerEvents = 'none'" in page
    assert "header.addEventListener('mousedown'" in page


def test_multi_strategy_dom_switching_and_save_preserve_every_custom_block() -> None:
    """Two real DOM-style switches must retain every loaded long/short strategy block and bound."""
    page = (ROOT / "frontend" / "v7_optimize.html").read_text(encoding="utf-8")
    functions = "\n".join(
        _page_function(page, name)
        for name in (
            "normalizeOptimizeEnableOverrides",
            "optimizeOverrideRequiredStrategy",
            "filterOptimizeEnableOverridesForStrategy",
            "getOptimizeTpGridDirection",
            "syncOptimizeOverrideStrategyCompatibility",
            "normalizeOptimizeFixedParamKeys",
            "getOptimizeStrategyOptions",
            "getOptimizeStrategyBotDefault",
            "getOptimizeStrategyFromPath",
            "initializeOptimizeStrategyState",
            "getOptimizeStrategyBoundsView",
            "getOptimizeStrategyFixedView",
            "cacheOptimizeStrategyState",
            "collectAllOptimizeStrategyBounds",
            "collectAllOptimizeStrategyFixed",
            "changeOptimizeStrategyKind",
        )
    )
    script = textwrap.dedent(
        f"""
        const assert = require('node:assert/strict');
        function deepClone(value) {{ return JSON.parse(JSON.stringify(value == null ? {{}} : value)); }}
        function prettyJson(value) {{ return JSON.stringify(value, null, 2); }}
        function scheduleStructuredEditorSync() {{}}
        const attrs = {{'data-current-strategy': 'alpha'}};
        const nodes = {{
          'opted-strategy-kind': {{value: 'alpha', getAttribute: (key) => attrs[key], setAttribute: (key, value) => {{ attrs[key] = value; }}}},
          'opted-bot-long': {{value: ''}},
          'opted-bot-short': {{value: ''}}
        }};
        function el(id) {{ return nodes[id] || null; }}
        let currentBounds = {{}};
        function collectOptimizeBounds() {{ return deepClone(currentBounds); }}
        function setOptimizeBoundsData(bounds, fixed, runtime) {{
          currentBounds = deepClone(bounds);
          state.optimizeFixedParams = fixed.slice();
          state.runtimeOverrideValues = runtime;
        }}
        const optimizeEditorAdapter = {{
          isV8: true,
          getBounds: (optimize) => deepClone(optimize.bounds || {{}}),
          canonicalFixedParam: (value) => /^(long|short)(\\.|$)/.test(value) ? 'bot.' + value : value
        }};
        const state = {{
          settings: {{strategies: ['alpha', 'beta', 'gamma'], strategy_bounds: {{}}, strategy_defaults: {{long: {{}}, short: {{}}}}}},
          optimizeStrategyBoundsCache: {{}}, optimizeStrategyFixedCache: {{}},
          optimizeSharedBoundsCache: {{}}, optimizeSharedFixedCache: [],
          optimizeStrategyBotCache: {{long: {{}}, short: {{}}}}, runtimeOverrideValues: {{}}, optimizeFixedParams: []
        }};
        {functions}
        const config = {{
          live: {{strategy_kind: 'alpha'}},
          bot: {{
            long: {{risk: {{n_positions: 3}}, strategy: {{alpha: {{custom: 1}}, beta: {{custom: 2}}, gamma: {{custom: 3}}}}}},
            short: {{risk: {{n_positions: 2}}, strategy: {{alpha: {{custom: 4}}, beta: {{custom: 5}}, gamma: {{custom: 6}}}}}}
          }},
          optimize: {{
            bounds: {{
              'long.risk.n_positions': [1, 5],
              'long.strategy.alpha.custom': [1, 2],
              'long.strategy.beta.custom': [2, 3],
              'long.strategy.gamma.custom': [3, 4],
              'short.strategy.alpha.custom': [4, 5],
              'short.strategy.beta.custom': [5, 6],
              'short.strategy.gamma.custom': [6, 7]
            }},
            fixed_params: ['long.strategy.alpha.custom', 'bot.long.strategy.alpha.custom', 'short.strategy.*']
          }}
        }};
        state.editorLastConfig = deepClone(config);
        nodes['opted-bot-long'].value = prettyJson(config.bot.long);
        nodes['opted-bot-short'].value = prettyJson(config.bot.short);
        initializeOptimizeStrategyState(config);
        currentBounds = getOptimizeStrategyBoundsView('alpha');
        state.optimizeFixedParams = getOptimizeStrategyFixedView('alpha');
        let longBot = JSON.parse(nodes['opted-bot-long'].value);
        longBot.strategy.alpha.custom = 11;
        nodes['opted-bot-long'].value = prettyJson(longBot);
        changeOptimizeStrategyKind('beta');
        longBot = JSON.parse(nodes['opted-bot-long'].value);
        longBot.strategy.beta.custom = 22;
        nodes['opted-bot-long'].value = prettyJson(longBot);
        currentBounds['long.strategy.beta.custom'] = [20, 23];
        changeOptimizeStrategyKind('gamma');
        longBot = JSON.parse(nodes['opted-bot-long'].value);
        longBot.strategy.gamma.custom = 33;
        nodes['opted-bot-long'].value = prettyJson(longBot);
        const saved = {{
          bot: {{long: JSON.parse(nodes['opted-bot-long'].value), short: JSON.parse(nodes['opted-bot-short'].value)}},
          optimize: {{
            bounds: collectAllOptimizeStrategyBounds('gamma'),
            fixed_params: collectAllOptimizeStrategyFixed('gamma')
          }}
        }};
        assert.deepEqual(saved.bot.long.strategy, {{alpha: {{custom: 11}}, beta: {{custom: 22}}, gamma: {{custom: 33}}}});
        assert.deepEqual(saved.bot.short.strategy, {{alpha: {{custom: 4}}, beta: {{custom: 5}}, gamma: {{custom: 6}}}});
        assert.deepEqual(saved.optimize.bounds['long.strategy.alpha.custom'], [1, 2]);
        assert.deepEqual(saved.optimize.bounds['long.strategy.beta.custom'], [20, 23]);
        assert.deepEqual(saved.optimize.bounds['short.strategy.gamma.custom'], [6, 7]);
        assert.deepEqual(saved.optimize.fixed_params.sort(), ['bot.long.strategy.alpha.custom', 'bot.short.strategy.*']);
        """
    )
    _run_node(script)


def test_seed_runtime_unknown_overrides_and_pymoo_auto_execute_page_logic() -> None:
    """Seed state, future overrides, canonical selectors, and PB8 pymoo auto values execute losslessly."""
    page = (ROOT / "frontend" / "v7_optimize.html").read_text(encoding="utf-8")
    names = (
        "normalizeOptimizeSeedMode",
        "getOptimizeSeedState",
        "applyOptimizeSeedConfig",
        "normalizeOptimizeRuntimeOverrideMap",
        "normalizeOptimizeRuntimeOverrideBooleanValue",
        "parseOptimizeRuntimeOverrideValue",
        "collectOptimizeRuntimeOverrides",
        "normalizeOptimizePymooPopulationMode",
        "resolveOptimizePymooRequestedPopulationSize",
        "shouldPreserveOptimizePymooAutoPopulation",
        "formatOptimizePymooPopulationAutoLabel",
        "optimizeReferenceDirectionCount",
        "resolveOptimizePymooAutoRefDirPartitions",
    )
    functions = "\n".join(_page_function(page, name) for name in names)
    script = textwrap.dedent(
        f"""
        const assert = require('node:assert/strict');
        function deepClone(value) {{ return JSON.parse(JSON.stringify(value == null ? {{}} : value)); }}
        function cloneJsonValue(value) {{ return value === undefined ? undefined : deepClone(value); }}
        function ensureObjectSection(root, key) {{ if (!root[key] || typeof root[key] !== 'object') root[key] = {{}}; return root[key]; }}
        function normalizeOptimizePositiveInteger(value) {{ const parsed = Number(value); return Number.isFinite(parsed) ? Math.max(1, Math.round(parsed)) : null; }}
        function normalizeOptimizeBackendValue(value) {{ return String(value || '').toLowerCase(); }}
        const optimizeEditorAdapter = {{isV8: true}};
        const OPTIMIZE_PB8_NSGA2_AUTO_POPULATION = 250;
        const OPTIMIZE_NSGA3_AUTO_REF_DIR_BUDGET = 500;
        const OPT_FIXED_RUNTIME_OVERRIDE_FIELDS = [{{key: 'known', type: 'number'}}];
        const state = {{runtimeOverrideValues: {{known: '7', future: {{nested: true}}}}}};
        {functions}
        const stale = {{pbgui: {{optimize_runtime: {{mode: 'checkpoint_resume', source: '/old/checkpoint'}}, optimize_seed_mode: 'path', optimize_seed_path: '/stale'}}}};
        assert.deepEqual(getOptimizeSeedState(stale), {{mode: 'none', path: ''}});
        applyOptimizeSeedConfig(stale, 'none', '');
        assert.equal(stale.pbgui.optimize_runtime.mode, 'fresh');
        assert.equal(stale.pbgui.optimize_runtime.source, '');
        applyOptimizeSeedConfig(stale, 'self', '');
        assert.deepEqual(stale.pbgui.optimize_runtime, {{mode: 'pareto_seed', source: '__self__'}});
        applyOptimizeSeedConfig(stale, 'path', '/managed/pareto');
        assert.deepEqual(stale.pbgui.optimize_runtime, {{mode: 'pareto_seed', source: '/managed/pareto'}});
        assert.deepEqual(collectOptimizeRuntimeOverrides({{fallbackValue: {{future: {{nested: true}}, untouched: 9}}}}), {{
          future: {{nested: true}}, untouched: 9, known: 7
        }});
        assert.equal(resolveOptimizePymooRequestedPopulationSize('auto', 999), null);
        assert.equal(shouldPreserveOptimizePymooAutoPopulation('pymoo', 'nsga2', 'auto'), true);
        assert.equal(formatOptimizePymooPopulationAutoLabel({{showPymoo: true, effectiveAlgorithm: 'nsga2'}}), 'auto (250 PB8 native NSGA-II default)');
        const partitions = resolveOptimizePymooAutoRefDirPartitions(4, null);
        assert.ok(optimizeReferenceDirectionCount(4, partitions) <= 500);
        assert.ok(optimizeReferenceDirectionCount(4, partitions + 1) > 500);
        """
    )
    _run_node(script)


def test_request_generations_reject_stale_http_and_settings_merge_metadata() -> None:
    """Late HTTP responses cannot replace websocket state, newer requests, or metadata-rich settings."""
    page = (ROOT / "frontend" / "v7_optimize.html").read_text(encoding="utf-8")
    functions = "\n".join(_page_function(page, name) for name in ("loadSettings", "loadQueue", "loadResults"))
    script = textwrap.dedent(
        f"""
        const assert = require('node:assert/strict');
        const deferred = [];
        function apiFetch(path) {{ return new Promise((resolve) => deferred.push({{path, resolve}})); }}
        function el() {{ return {{classList: {{contains: () => false}}}}; }}
        function syncQueueSettingsModalFields() {{}}
        function updateMetaCounts() {{}}
        function renderQueue() {{}}
        function renderResults() {{}}
        const optimizeEditorAdapter = {{resultsPath: '/results'}};
        const state = {{
          settings: {{strategy_bounds: {{custom: true}}, runtime_options: {{future: true}}}},
          queue: [{{filename: 'ws'}}], results: [], settingsLoadSeq: 0, settingsPushSeq: 0,
          queueLoadSeq: 0, queuePushSeq: 0, resultsLoadSeq: 0, navigationSeq: 0
        }};
        {functions}
        (async () => {{
          const settingsLoad = loadSettings();
           deferred[0].resolve({{cpu: 8}});
           await settingsLoad;
           assert.equal(state.settings.cpu, 8);
           assert.deepEqual(state.settings.strategy_bounds, {{custom: true}});
           const pushedSettings = loadSettings();
           state.settingsPushSeq += 1;
           state.settings.cpu = 6;
           deferred[1].resolve({{
             cpu: 2,
             cpu_max: 16,
             host_cpu_count: 16,
             optimize_defaults: {{n_cpus: 16}}
           }});
           await pushedSettings;
           assert.equal(state.settings.cpu, 6);
           assert.equal(state.settings.cpu_max, 16);
           assert.equal(state.settings.host_cpu_count, 16);
           assert.deepEqual(state.settings.optimize_defaults, {{n_cpus: 16}});
           const staleQueue = loadQueue();
           state.queuePushSeq += 1;
           state.queue = [{{filename: 'new-ws'}}];
           deferred[2].resolve({{items: [{{filename: 'old-http'}}]}});
           await staleQueue;
           assert.equal(state.queue[0].filename, 'new-ws');
           const oldResults = loadResults();
           const newResults = loadResults();
           deferred[4].resolve({{results: [{{path: 'new'}}]}});
           await newResults;
           deferred[3].resolve({{results: [{{path: 'old'}}]}});
           await oldResults;
           assert.equal(state.results[0].path, 'new');
           const navigated = loadResults();
           state.navigationSeq += 1;
           deferred[5].resolve({{results: [{{path: 'stale-navigation'}}]}});
           await navigated;
           assert.equal(state.results[0].path, 'new');
        }})().catch((error) => {{ console.error(error); process.exitCode = 1; }});
        """
    )
    _run_node(script)


def test_switching_result_sets_clears_stale_paretos_before_loading() -> None:
    """Selecting another optimize result must clear old Pareto rows before its request completes."""
    page = (ROOT / "frontend" / "v7_optimize.html").read_text(encoding="utf-8")
    functions = "\n".join(
        _page_function(page, name)
        for name in ("clearParetoSelection", "loadParetos")
    )
    script = textwrap.dedent(
        f"""
        const assert = require('node:assert/strict');
        const deferred = [];
        function apiFetch(path) {{ return new Promise((resolve) => deferred.push({{path, resolve}})); }}
        function normalizeParetoStatistic(value) {{ return value || 'mean'; }}
        function clearParetoMeta() {{ state.paretoMode = 'none'; }}
        function applyParetoMeta(meta) {{ state.paretoMode = meta.mode || 'unknown'; }}
        const renders = [];
        function renderParetos() {{ renders.push(state.paretos.map((item) => item.path)); }}
        const optimizeEditorAdapter = {{paretosPath: (query) => '/paretos?' + query}};
        const state = {{
          paretoLoadSeq: 0,
          paretoMode: 'normal',
          paretoScenario: 'Aggregated',
          paretoStatistic: 'mean',
          paretoMetricReloadTimer: null,
          paretoMetricColumnsLoaded: true,
          paretoMetricColumns: [],
          selectedResultPath: '/old',
          selectedResultName: 'old',
          paretos: [{{path: '/old/pareto.json'}}],
          selectedParetos: new Set(['/old/pareto.json']),
          selectedParetoScenarios: new Map([['/old/pareto.json', 'Aggregated']])
        }};
        {functions}
        (async () => {{
          const firstLoad = loadParetos('/new-a', 'new-a');
          assert.deepEqual(state.paretos, []);
          assert.equal(state.selectedParetos.size, 0);
          assert.deepEqual(renders.at(-1), []);

          const secondLoad = loadParetos('/new-b', 'new-b');
          deferred[0].resolve({{paretos: [{{path: '/new-a/stale.json'}}], meta: {{mode: 'normal'}}}});
          await firstLoad;
          assert.deepEqual(state.paretos, []);

          deferred[1].resolve({{paretos: [{{path: '/new-b/current.json'}}], meta: {{mode: 'normal'}}}});
          await secondLoad;
          assert.equal(state.selectedResultPath, '/new-b');
          assert.deepEqual(state.paretos.map((item) => item.path), ['/new-b/current.json']);
          assert.deepEqual(renders.at(-1), ['/new-b/current.json']);
        }})().catch((error) => {{ console.error(error); process.exitCode = 1; }});
        """
    )
    _run_node(script)


def test_installed_override_helpers_and_backend_result_flags_are_visible() -> None:
    """Every installed helper renders and PB8 result controls use explicit backend capabilities."""
    script = textwrap.dedent(
        """
        const assert = require('node:assert/strict');
        const fs = require('node:fs');
        global.window = {};
        eval(fs.readFileSync('frontend/js/optimize_editor_adapter.js', 'utf8'));
        const adapter = window.PBGuiOptimizeEditorAdapter.create('v8');
        adapter.normalizeMetadata({
          optimize_defaults: {},
          optimizer_overrides: ['lossless_close_trailing', 'forward_tp_grid', 'backward_tp_grid', 'mirror_short_from_long']
        });
        const html = adapter.versionRunSettingsHtml({optimize: {enable_overrides: ['mirror_short_from_long']}, pbgui: {}}, String);
        for (const helper of ['lossless_close_trailing', 'forward_tp_grid', 'backward_tp_grid', 'mirror_short_from_long']) {
          assert.match(html, new RegExp('data-pb8-enable-override="' + helper + '"'));
        }
        assert.match(html, /mirror_short_from_long" checked/);
        """
    )
    _run_node(script)


def test_optimizer_overrides_are_filtered_for_the_active_strategy() -> None:
    """Strategy changes must remove stale native optimizer overrides before save or queue."""
    page = (ROOT / "frontend" / "v7_optimize.html").read_text(encoding="utf-8")
    functions = "\n".join(
        _page_function(page, name)
        for name in (
            "normalizeOptimizeEnableOverrides",
            "optimizeOverrideRequiredStrategy",
            "filterOptimizeEnableOverridesForStrategy",
        )
    )
    script = textwrap.dedent(
        f"""
        const assert = require('node:assert/strict');
        {functions}
        const overrides = ['lossless_close_trailing', 'forward_tp_grid', 'mirror_short_from_long'];
        assert.deepEqual(filterOptimizeEnableOverridesForStrategy(overrides, 'ema_anchor'), ['mirror_short_from_long']);
        assert.deepEqual(filterOptimizeEnableOverridesForStrategy(overrides, 'trailing_martingale'), ['lossless_close_trailing', 'mirror_short_from_long']);
        assert.deepEqual(filterOptimizeEnableOverridesForStrategy(overrides, 'trailing_grid_v7'), ['forward_tp_grid', 'mirror_short_from_long']);
        """
    )
    _run_node(script)


def test_v8_results_show_and_sort_strategy_without_changing_v7_columns() -> None:
    """Only PB8 result rows add the backend-provided strategy column."""
    page = (ROOT / "frontend" / "v7_optimize.html").read_text(encoding="utf-8")
    functions = "\n".join(
        _page_function(page, name)
        for name in ("resultHeaderLabel", "getResultSortValue", "sortResults")
    )
    assert "if (optimizeEditorAdapter.isV8) sortKeys.splice(2, 0, 'strategy');" in page
    assert "optimizeEditorAdapter.isV8 ? 7 : 6" in page
    assert "escapeHtml(result.strategy || '-')" in page
    script = textwrap.dedent(
        f"""
        const assert = require('node:assert/strict');
        const state = {{resultSortKey: 'strategy', resultSortDir: 'asc'}};
        {functions}
        assert.equal(resultHeaderLabel('strategy'), 'Strategy');
        assert.equal(getResultSortValue({{strategy: 'EMA_Anchor'}}, 'strategy'), 'ema_anchor');
        const rows = [
          {{name: 'grid', strategy: 'trailing_grid_v7'}},
          {{name: 'ema', strategy: 'ema_anchor'}},
          {{name: 'martingale', strategy: 'trailing_martingale'}}
        ];
        assert.deepEqual(sortResults(rows).map((row) => row.name), ['ema', 'grid', 'martingale']);
        """
    )
    _run_node(script)


def test_pareto_contract_enables_backend_advertised_median_and_scenario() -> None:
    """The toolbar consumes backend mode, scenario, and available-statistic metadata including median."""
    page = (ROOT / "frontend" / "v7_optimize.html").read_text(encoding="utf-8")
    functions = "\n".join(
        _page_function(page, name)
        for name in (
            "normalizeParetoStatistic",
            "normalizeParetoScenario",
            "orderParetoMetrics",
            "readStoredParetoColumns",
            "persistParetoColumns",
            "setParetoMetricColumns",
            "applyParetoMeta",
        )
    )
    script = textwrap.dedent(
        f"""
        const assert = require('node:assert/strict');
        const PARETO_STAT_OPTIONS = ['mean', 'min', 'max', 'std'];
        const PARETO_SUMMARY_ORDER = ['adg', 'gain', 'drawdown_worst'];
        const PARETO_COLUMNS_STORAGE_KEY = 'test';
        const state = {{
          paretoStatisticOptions: [], paretoStatistic: 'median', paretoScenario: 'bear',
          paretoScenarioLabels: [], paretoMode: 'none', paretoStatisticEnabled: true,
          paretos: [], paretoAvailableMetrics: [], paretoDefaultMetrics: [], paretoMetricColumns: [],
          paretoMetricColumnsLoaded: false, paretoSortKey: 'name', paretoSortDir: 'asc'
        }};
        {functions}
        applyParetoMeta({{
          mode: 'suite', scenario_labels: ['bull', 'bear'], selected_scenario: 'bear',
          selected_statistic: 'median', available_statistics: ['mean', 'median'], statistic_enabled: false,
          available_metrics: ['quality', 'gain', 'drawdown_worst'],
          default_metrics: ['gain', 'quality', 'drawdown_worst']
        }});
        assert.equal(state.paretoMode, 'suite');
        assert.deepEqual(state.paretoScenarioLabels, ['bull', 'bear']);
        assert.equal(state.paretoScenario, 'bear');
        assert.equal(state.paretoStatistic, 'median');
        assert.equal(state.paretoStatisticEnabled, false);
        assert.deepEqual(state.paretoMetricColumns, ['gain', 'drawdown_worst', 'quality']);
        """
    )
    _run_node(script)


def test_pareto_gain_order_and_numeric_sort_are_stable() -> None:
    """Canonical gain keeps PB7 column order and sorts numerically with missing values last."""
    page = (ROOT / "frontend" / "v7_optimize.html").read_text(encoding="utf-8")
    functions = "\n".join(
        _page_function(page, name)
        for name in ("getParetoSummaryKeys", "getParetoSortValue", "sortParetos", "toggleParetoSort")
    )
    script = textwrap.dedent(
        f"""
        const assert = require('node:assert/strict');
        const PARETO_SUMMARY_ORDER = ['adg', 'gain', 'drawdown_worst'];
        const state = {{paretoSortKey: 'name', paretoSortDir: 'asc'}};
        let renders = 0;
        function renderParetos() {{ renders += 1; }}
        {functions}

        const columns = getParetoSummaryKeys([
          {{summary: {{zeta: 1, gain: 2, adg: 3}}}},
          {{summary: {{alpha: 4}}}}
        ]);
        assert.deepEqual(columns, ['adg', 'gain', 'alpha', 'zeta']);

        toggleParetoSort('summary:gain');
        assert.equal(state.paretoSortKey, 'summary:gain');
        assert.equal(state.paretoSortDir, 'desc');
        assert.equal(renders, 1);
        const rows = [
          {{name: 'bravo', summary: {{gain: 5}}}},
          {{name: 'alpha', summary: {{gain: 5}}}},
          {{name: 'missing', summary: {{}}}},
          {{name: 'charlie', summary: {{gain: 2}}}}
        ];
        assert.deepEqual(sortParetos(rows, ['gain']).map((row) => row.name), ['alpha', 'bravo', 'charlie', 'missing']);
        state.paretoSortDir = 'asc';
        assert.deepEqual(sortParetos(rows, ['gain']).map((row) => row.name), ['charlie', 'alpha', 'bravo', 'missing']);
        """
    )
    _run_node(script)


def test_pareto_metric_columns_are_configurable_and_never_empty() -> None:
    """The column picker filters visible metrics, restores defaults, and keeps one column selected."""
    page = (ROOT / "frontend" / "v7_optimize.html").read_text(encoding="utf-8")
    functions = "\n".join(
        _page_function(page, name)
        for name in ("orderParetoMetrics", "persistParetoColumns", "setParetoMetricColumns", "getParetoSummaryKeys")
    )
    assert 'id="pareto-columns-picker"' in page
    assert 'id="pareto-columns-defaults"' in page
    assert 'id="pareto-columns-all"' in page
    script = textwrap.dedent(
        f"""
        const assert = require('node:assert/strict');
        const PARETO_SUMMARY_ORDER = ['adg', 'gain', 'drawdown_worst', 'sharpe_ratio'];
        const PARETO_COLUMNS_STORAGE_KEY = 'test';
        const state = {{
          paretoAvailableMetrics: ['gain', 'drawdown_worst', 'sharpe_ratio'],
          paretoDefaultMetrics: ['gain', 'drawdown_worst'],
          paretoMetricColumns: ['gain', 'drawdown_worst'],
          paretoSortKey: 'summary:drawdown_worst',
          paretoSortDir: 'desc'
        }};
        {functions}
        const rows = [{{summary: {{gain: 37, drawdown_worst: 0.12, sharpe_ratio: 0.14}}}}];
        assert.deepEqual(getParetoSummaryKeys(rows), ['gain', 'drawdown_worst']);
        setParetoMetricColumns(['sharpe_ratio'], false);
        assert.deepEqual(getParetoSummaryKeys(rows), ['sharpe_ratio']);
        assert.equal(state.paretoSortKey, 'name');
        setParetoMetricColumns([], false);
        assert.deepEqual(state.paretoMetricColumns, ['gain', 'drawdown_worst']);
        """
    )
    _run_node(script)


def test_pareto_metric_catalog_is_lazy_batched_and_dom_cached() -> None:
    """All metric names are selectable without rebuilding the picker or eagerly loading values."""
    page = (ROOT / "frontend" / "v7_optimize.html").read_text(encoding="utf-8")
    picker = _page_function(page, "renderParetoColumnPicker")
    scheduler = _page_function(page, "scheduleParetoMetricReload")
    loader = _page_function(page, "loadParetos")

    assert "options._paretoCatalogSignature !== catalogSignature" in picker
    assert "document.createDocumentFragment()" in picker
    assert "options.querySelectorAll('input[data-pareto-metric]')" in picker
    assert "}, 250);" in scheduler
    assert "requestedMetrics.join(',')" in loader
    assert "metrics=" in loader
    assert '>All (slower)</button>' in page


def test_pareto_fallback_statistics_include_median_and_render_commits_once() -> None:
    """Fallback controls include median and 1,000 Pareto rows are attached in one DOM commit."""
    page = (ROOT / "frontend" / "v7_optimize.html").read_text(encoding="utf-8")
    assert "var PARETO_STAT_OPTIONS = ['mean', 'min', 'max', 'std', 'median'];" in page
    function = _page_function(page, "renderParetos")
    script = textwrap.dedent(
        f"""
        const assert = require('node:assert/strict');
        let commits = 0;
        let rendered = '';
        const tbody = {{
          set innerHTML(value) {{ commits += 1; rendered = value; }},
          get innerHTML() {{ return rendered; }},
          appendChild() {{ throw new Error('per-row append is forbidden'); }}
        }};
        const nodes = {{
          'paretos-tbody': tbody,
          'pareto-result-chip': {{textContent: ''}}
        }};
        function el(id) {{ return nodes[id] || {{}}; }}
        function getParetoSummaryKeys() {{ return ['gain']; }}
        function renderParetoTableHead() {{}}
        function renderParetoToolbar() {{}}
        function renderEmpty() {{ throw new Error('unexpected empty state'); }}
        function sortParetos(items) {{ return items.slice(); }}
        function escapeHtml(value) {{
          return String(value).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
        }}
        function formatParetoMetricValue(value) {{ return Number(value).toFixed(4); }}
        function formatIso(value) {{ return String(value); }}
        function metricPills() {{ return ''; }}
        function updateParetoSelectionUi() {{}}
        function updateMetaCounts() {{}}
        const optimizeEditorAdapter = {{isV8: true}};
        const state = {{
          selectedResultPath: '/result', selectedResultName: 'result', selectedParetos: new Set(),
          paretos: Array.from({{length: 1000}}, (_, index) => ({{
            path: '/result/pareto/' + index + '.json', name: 'candidate-' + index,
            modified: '2026-07-21T00:00:00', summary: {{gain: index}}
          }}))
        }};
        {function}
        renderParetos();
        assert.equal(commits, 1);
        assert.equal((rendered.match(/<tr /g) || []).length, 1000);
        """
    )
    _run_node(script)


def test_sidebar_result_actions_follow_backend_capabilities() -> None:
    """Sidebar resume, config, pareto, Dash, and 3D controls use backend flags rather than guesses."""
    page = (ROOT / "frontend" / "v7_optimize.html").read_text(encoding="utf-8")
    function = _page_function(page, "updateResultSelectionUi")
    script = textwrap.dedent(
        f"""
        const assert = require('node:assert/strict');
        const ids = [
          'result-selection-summary', 'btn-open-result-paretos', 'btn-open-pareto-explorer-results',
          'btn-open-result-pareto-dash', 'btn-open-result-3d', 'btn-continue-result',
          'btn-resume-result', 'btn-open-result-config'
        ];
        const nodes = Object.fromEntries(ids.map((id) => [id, {{disabled: false, textContent: ''}}]));
        function el(id) {{ return nodes[id]; }}
        function pruneSelectionSet() {{}}
        const result = {{path: '/result', has_pareto: false, resumable: true, has_config: true, supports_3d: false, supports_dash: false}};
        const state = {{selectedResults: new Set(['/result']), results: [result]}};
        function syncSelectedResultFromSelection() {{ return result; }}
        const optimizeEditorAdapter = {{resultCapabilities: (value) => ({{
          hasPareto: value.has_pareto === true, resumable: value.resumable === true,
          hasConfig: value.has_config === true, supports3d: value.supports_3d === true,
          supportsDash: value.supports_dash === true
        }})}};
        {function}
        updateResultSelectionUi(state.results);
        assert.equal(nodes['btn-resume-result'].disabled, false);
        assert.equal(nodes['btn-open-result-config'].disabled, false);
        assert.equal(nodes['btn-open-result-paretos'].disabled, true);
        assert.equal(nodes['btn-continue-result'].disabled, true);
        assert.equal(nodes['btn-open-result-pareto-dash'].disabled, true);
        assert.equal(nodes['btn-open-result-3d'].disabled, true);
        """
    )
    _run_node(script)


def test_pb8_config_list_shows_sortable_strategy_column() -> None:
    """PB8 config rows should expose the strategy already returned by the API."""
    page = (ROOT / "frontend" / "v7_optimize.html").read_text(encoding="utf-8")

    assert "if (optimizeEditorAdapter.isV8) configSortKeys.push('strategy');" in page
    assert "if (sortKey === 'strategy') return 'Strategy';" in page
    assert "if (sortKey === 'strategy') return String(cfg && cfg.strategy || '').toLowerCase();" in page
    assert "optimizeEditorAdapter.isV8 ? '<td>' + escapeHtml(cfg.strategy || '-') + '</td>' : ''" in page


def test_pb8_scenario_bases_round_trip_without_changing_pb7_entries() -> None:
    """PB8 scoring and limit scenario fields must survive while PB7 keeps its legacy shape."""
    page = (ROOT / "frontend" / "v7_optimize.html").read_text(encoding="utf-8")
    functions = "\n".join(
        _page_function(page, name)
        for name in (
            "normalizeLimitEntry",
            "normalizeLimitForm",
            "limitEntryToForm",
            "buildLimitEntryFromForm",
            "normalizeScoringEntry",
            "normalizeScoringForm",
            "scoringEntryToForm",
            "buildScoringEntryFromForm",
            "validatePb8ScenarioBases",
        )
    )
    script = textwrap.dedent(
        f"""
        const assert = require('node:assert/strict');
        const optimizeEditorAdapter = {{isV8: true}};
        const limitsMeta = {{
          type_options: ['all'], stat_options: ['', 'mean', 'min', 'max', 'std', 'median'],
          currency_options: ['usd', 'btc'], goal_options: ['min', 'max'],
          limit_basis_field: 'reducer', scoring_basis_field: 'reducer'
        }};
        function currentLimitsMeta() {{ return limitsMeta; }}
        function canonicalizeLimitMetricName(value) {{ return String(value || '').trim(); }}
        function normalizeLimitPenalizeIf(value) {{ return String(value || 'greater_than'); }}
        function isLimitRangePenalize(value) {{ return value === 'outside_range' || value === 'inside_range'; }}
        function normalizeLimitNumber(value, fallback) {{ const parsed = Number(value); return Number.isFinite(parsed) ? parsed : fallback; }}
        function limitMetricOptions(_type, current) {{ return current ? [current] : ['metric']; }}
        function isLimitCurrencyMetric() {{ return false; }}
        function getMetricGroupFromMeta() {{ return 'all'; }}
        function splitLimitMetricName(metric) {{ return {{base_metric: metric, currency: '', metric}}; }}
        function buildLimitMetricName(metric) {{ return metric; }}
        function defaultScoringGoal() {{ return 'max'; }}
        function normalizeScoringGoal(value) {{ return String(value || 'max'); }}
        {functions}

        const scoring = normalizeScoringEntry({{
          metric: 'adg', goal: 'max', scenario: null, reducer: 'median', future_field: {{keep: true}}
        }});
        assert.equal(Object.hasOwn(scoring, 'scenario'), true);
        assert.equal(scoring.scenario, null);
        assert.equal(scoring.reducer, 'median');
        assert.deepEqual(scoring.future_field, {{keep: true}});
        assert.deepEqual(buildScoringEntryFromForm(scoringEntryToForm(scoring)), scoring);

        const namedScoring = normalizeScoringEntry({{metric: 'adg', goal: 'max', scenario: 'bull'}});
        assert.deepEqual(buildScoringEntryFromForm(scoringEntryToForm(namedScoring)), namedScoring);
        const inherited = normalizeScoringEntry({{metric: 'adg', goal: 'max'}});
        assert.equal(Object.hasOwn(inherited, 'scenario'), false);

        const limit = normalizeLimitEntry({{
          metric: 'drawdown', penalize_if: 'greater_than', scenario: 'bear', reducer: '', value: 0.2, future_field: ['keep']
        }});
        assert.deepEqual(buildLimitEntryFromForm(limitEntryToForm(limit)), limit);
        assert.equal(Object.hasOwn(limit, 'enabled'), false);
        assert.deepEqual(limit.future_field, ['keep']);
        assert.throws(
          () => buildLimitEntryFromForm({{...limitEntryToForm(limit), stat: 'max'}}),
          /cannot also use Stat/
        );
        assert.throws(
          () => validatePb8ScenarioBases([{{metric: 'adg', goal: 'max', reducer: 'mean'}}], [], 'bull', true, ['bull']),
          /inheriting a named objective_scenario/
        );
        limitsMeta.limit_basis_field = 'stat';
        limitsMeta.scoring_basis_field = 'aggregate';
        const legacyLimit = normalizeLimitEntry({{metric: 'drawdown', penalize_if: 'greater_than', stat: 'median', value: 0.2}});
        const legacyScoring = normalizeScoringEntry({{metric: 'adg', goal: 'max', aggregate: 'median'}});
        assert.equal(legacyLimit.stat, 'median');
        assert.equal(legacyScoring.aggregate, 'median');
        assert.throws(
          () => validatePb8ScenarioBases([{{metric: 'adg', goal: 'max', scenario: 'bera'}}], [], null, true, ['bear']),
          /Unknown scoring scenario/
        );
        assert.throws(
          () => validatePb8ScenarioBases([{{metric: 'adg', goal: 'max', scenario: null}}], [], null, false, []),
          /require Suite mode/
        );
        assert.throws(
          () => validatePb8ScenarioBases([{{metric: 'adg', goal: 'max', scenario: ''}}], [], null, true, ['bull']),
          /cannot be empty/
        );
        assert.throws(
          () => validatePb8ScenarioBases([], [], null, true, ['bull', 'bull']),
          /Duplicate Suite scenario label/
        );
        assert.throws(
          () => validatePb8ScenarioBases([], [], null, true, [' bull ']),
          /leading or trailing whitespace/
        );

        optimizeEditorAdapter.isV8 = false;
        assert.deepEqual(
          normalizeScoringEntry({{metric: 'adg', goal: 'max', scenario: 'bull', aggregate: 'median'}}),
          {{metric: 'adg', goal: 'max'}}
        );
        assert.equal(
          Object.hasOwn(normalizeLimitEntry({{metric: 'drawdown', scenario: 'bear', value: 0.2}}), 'scenario'),
          false
        );
        """
    )
    _run_node(script)


def test_pb8_scenario_controls_are_not_rendered_for_pb7() -> None:
    """The shared page must gate PB8 objective, Scenario, and Aggregate controls by version."""
    page = (ROOT / "frontend" / "v7_optimize.html").read_text(encoding="utf-8")

    assert "id=\"opted-objective-scenario-mode\"" in page
    assert "(isV8 ? '<th>Scenario</th>' : '')" in page
    assert "optimizeEditorAdapter.isV8" in _page_function(page, "renderOptimizeScoringEditor")
    assert "optimizeEditorAdapter.isV8" in _page_function(page, "renderOptimizeLimitsEditor")
    assert "field !== 'scenario_name'" in _page_function(page, "updateScoringEditField")
    assert "field !== 'scenario_name'" in _page_function(page, "updateLimitEditField")
    assert "Resolve or remove invalid PB8 market identifiers before saving." in page
    assert "PB8 market resolver error:" in page
    assert "PB8 market identifiers have not been verified." in page


def test_optimize_apply_filters_updates_both_sides_across_exchanges() -> None:
    """Optimize market filters must populate approved/ignored lists like Backtest."""
    page = (ROOT / "frontend" / "v7_optimize.html").read_text(encoding="utf-8")
    function = _page_function(page, "applyOptimizeFilters")
    assert 'id="opted-sidebar-apply-filters-btn"' in page
    script = textwrap.dedent(
        f"""
        const assert = require('node:assert/strict');
        const button = {{disabled: false}};
        const nodes = {{
          'opted-sidebar-apply-filters-btn': button,
          'opted-market-cap': {{value: '5000'}},
          'opted-vol-mcap': {{value: ''}},
          'opted-only-cpt': {{checked: true}},
          'opted-notices-ignore': {{checked: false}}
        }};
        const selections = {{'ms-opt-exchanges': ['binance', 'bybit'], 'ms-opt-tags': ['layer-1']}};
        const applied = {{}};
        const urls = [];
        const messages = [];
        const optimizeEditorAdapter = {{isV8: true, metadataApiBase: () => '/api/v8'}};
        function el(id) {{ return nodes[id]; }}
        function optGetMs(id) {{ return selections[id] || []; }}
        function optSetMs(id, values) {{ applied[id] = values; }}
        function toast(message, level) {{ messages.push({{message, level}}); }}
        async function authFetch(url) {{
          urls.push(url);
          if (url.includes('exchange=binance')) return {{approved: ['BTC', 'XRP'], ignored: ['DOGE', 'SOL'], unresolved: []}};
          return {{approved: ['DOGE', 'HYPE'], ignored: ['XRP'], unresolved: ['OLD']}};
        }}
        {function}
        applyOptimizeFilters().then(() => {{
          assert.equal(urls.length, 2);
          assert.match(urls[0], /market_cap=5000/);
          assert.match(urls[0], /vol_mcap=10/);
          assert.match(urls[0], /only_cpt=true/);
          assert.match(urls[0], /tags=layer-1/);
          const approved = ['BTC', 'DOGE', 'HYPE', 'XRP'];
          assert.deepEqual(applied['ms-opt-app-long'], approved);
          assert.deepEqual(applied['ms-opt-app-short'], approved);
          assert.deepEqual(applied['ms-opt-ign-long'], ['SOL']);
          assert.deepEqual(applied['ms-opt-ign-short'], ['SOL']);
          assert.equal(messages.at(-1).level, 'info');
          assert.match(messages.at(-1).message, /1 unavailable PB8 symbols skipped/);
          assert.equal(button.disabled, false);
        }}).catch(error => {{ console.error(error); process.exit(1); }});
        """
    )
    _run_node(script)
