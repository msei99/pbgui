"""Static and executable frontend contracts for the shared PB7/PB8 Optimize page."""

from pathlib import Path
import subprocess
import textwrap


ROOT = Path(__file__).resolve().parents[2]


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
    assert '/app/js/optimize_editor_adapter.js?v=11' in page
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
