"""Static frontend contracts for the standalone PB8 backtest page."""

from pathlib import Path
import subprocess
import textwrap


ROOT = Path(__file__).resolve().parents[2]


def test_v8_route_renders_the_v7_backtest_template() -> None:
    """PB8 must use the exact V7 page instead of maintaining a second editor."""
    api_source = (ROOT / "api" / "backtest_v8.py").read_text(encoding="utf-8")

    assert '"frontend" / "v7_backtest.html"' in api_source
    assert '"frontend" / "v8_backtest.html"' not in api_source
    assert not (ROOT / "frontend" / "v8_backtest.html").exists()
    assert '"%%BACKTEST_VERSION%%": "v8"' in api_source
    assert '"%%BACKTEST_NAV_CURRENT%%": "v8_backtest"' in api_source


def test_v7_page_offers_saved_config_conversion() -> None:
    """PB7 config and result rows should expose the V8 migration handoff."""
    source = (ROOT / "frontend" / "v7_backtest.html").read_text(encoding="utf-8")

    assert "convertConfigToV8" in source
    assert "convertResultToV8" in source
    assert 'id="sb-btn-convert-v8"' in source
    assert "btnConvertV8.disabled = !isExisting" in source
    assert "btnConvertV8.style.display = backtestEditorAdapter.isV8 ? 'none' : ''" in source
    assert "source_type: 'backtest_result'" in source
    assert "allowV8Convert: !backtestEditorAdapter.isV8" in source
    assert "/api/backtest-v8/migrate-v7" in source
    assert "source_name: name" in source


def test_v7_run_rows_offer_v8_conversion() -> None:
    """Each PB7 run config row should convert through the managed migration endpoint."""
    source = (ROOT / "frontend" / "v7_run.html").read_text(encoding="utf-8")

    assert 'data-convert-v8="' in source
    assert "function convertInstanceToV8(name)" in source
    assert "source_type: 'run_config'" in source
    assert "/api/backtest-v8/migrate-v7" in source
    assert "window.PBGuiDialogs.alert" in source


def test_v7_and_v8_share_the_same_backtest_shell() -> None:
    """The one backtest template must consume the shared shell and version adapter."""
    v7_source = (ROOT / "frontend" / "v7_backtest.html").read_text(encoding="utf-8")
    shell_source = (ROOT / "frontend" / "js" / "backtest_shell.js").read_text(encoding="utf-8")
    adapter_source = (ROOT / "frontend" / "js" / "backtest_editor_adapter.js").read_text(encoding="utf-8")

    assert '/app/css/backtest_shell.css?v=3' in v7_source
    assert '/app/js/backtest_shell.js?v=4' in v7_source
    assert '/app/js/backtest_editor_adapter.js?v=4' in v7_source
    assert "PBGuiBacktestShell.upgradeLegacy" in v7_source
    assert "PBGuiBacktestEditorAdapter.create(BACKTEST_VERSION)" in v7_source
    assert "sideConfig.risk" in adapter_source
    assert "setSideValue" in adapter_source
    for required_id in ("sidebar", "sidebar-inner", "sidebar-editor", "panel-configs", "panel-queue", "panel-results"):
        assert required_id in shell_source
    assert "source.remove()" in shell_source


def test_shared_results_compare_routes_each_version_to_its_own_api() -> None:
    """A mixed PB7/PB8 comparison must load both equity files from their owning API."""
    page_source = (ROOT / "frontend" / "v7_backtest.html").read_text(encoding="utf-8")
    shell_source = (ROOT / "frontend" / "js" / "backtest_shell.js").read_text(encoding="utf-8")

    assert 'id="results-version-filter"' in shell_source
    assert '<option value="both">Both</option>' in shell_source
    assert "return fetchCSV(path, 'equity', r)" in page_source
    assert "var cacheKey = version + ':' + path" in page_source
    assert "resultApiBase(result) + '/results/' + file" in page_source
    assert "'PB' + item.version.toUpperCase()" in page_source


def test_shared_results_delete_routes_each_version_to_its_own_api() -> None:
    """Mixed PB7/PB8 deletion must remain enabled and use each result's backend."""
    page_source = (ROOT / "frontend" / "v7_backtest.html").read_text(encoding="utf-8")

    assert 'data-cross-version-action onclick="deleteSelectedResults()"' in page_source
    assert "return resultApiFetch(result, '/results?path='" in page_source
    assert "encodeURIComponent(result.path)" in page_source


def test_v8_supports_every_shared_native_backtest_operation() -> None:
    """PB8 must expose every config, queue, and result route used by its shared page."""
    api_source = (ROOT / "api" / "backtest_v8.py").read_text(encoding="utf-8")
    required_routes = (
        '@router.get("/settings")',
        '@router.post("/settings")',
        '@router.get("/configs/new-config")',
        '@router.post("/configs/prepare")',
        '@router.get("/result-metrics")',
        '@router.get("/configs")',
        '@router.get("/configs/{name}")',
        '@router.put("/configs/{name}")',
        '@router.delete("/configs/{name}")',
        '@router.get("/queue")',
        '@router.post("/queue")',
        '@router.post("/queue/{filename}/start")',
        '@router.post("/queue/{filename}/restart")',
        '@router.post("/queue/{filename}/stop")',
        '@router.delete("/queue/{filename}")',
        '@router.post("/queue/clear-finished")',
        '@router.get("/queue/{filename}/log")',
        '@router.get("/results")',
        '@router.get("/results/analysis")',
        '@router.get("/results/config")',
        '@router.get("/results/files")',
        '@router.get("/results/equity")',
        '@router.get("/results/fills")',
        '@router.get("/results/image")',
        '@router.delete("/results")',
    )

    for route in required_routes:
        assert route in api_source


def test_notification_bell_opens_transient_gui_messages() -> None:
    """The global bell must show persisted GUI toasts rather than a backend-specific log."""
    page = (ROOT / "frontend" / "v7_backtest.html").read_text(encoding="utf-8")
    nav = (ROOT / "frontend" / "pbgui_nav.js").read_text(encoding="utf-8")

    assert "notificationFile:" not in page
    assert "defaultFile: 'PBGui.log'" in nav
    assert "defaultFile: 'PBV7UI.log'" not in nav


def test_shared_template_contains_the_full_visual_editor() -> None:
    """V8 receives every structured editor section because it renders the V7 template."""
    source = (ROOT / "frontend" / "v7_backtest.html").read_text(encoding="utf-8")

    for editor_contract in (
        "function showConfigEditor(",
        "Coins &amp; Filters",
        "Bot Configuration",
        "coin-overrides-container",
        "suite-container",
        "cfg-bot-long",
        "cfg-bot-short",
        "cfg-raw-json",
        "function collectConfig(",
    ):
        assert editor_contract in source
    assert "backtestEditorAdapter.getSideValue" in source
    assert "backtestEditorAdapter.setSideValue" in source
    assert "searchParams.get('config')" in source
    assert "editConfig(requestedConfig)" in source
    assert "backtestEditorAdapter.isV8 ? JSON.stringify(prepared.config, null, 2) : jsonEl.value" in source
    assert "create_only=true" in source
    assert "putEditorConfig(name, cfg, oldName, overrideSnapshot)" in source
    assert "override_configs: (overrideSnapshot && overrideSnapshot.files) || {}" in source
    assert "inherit_existing_overrides=false" in source
    assert "configEditRevision !== saveRevision" in source
    assert "var _resultsLoadGeneration = 0;" in source
    assert "loadGeneration !== _resultsLoadGeneration" in source
    assert "loadResults(selectedFilter, { emptyRetry: true })" in source
    assert "Checking for results" in source
    assert "endDateInput.dataset.semanticValue || endDateInput.value" in source


def test_v8_advanced_backtest_fields_use_the_intended_editor_sections() -> None:
    """Common PB8 fields are structured while dataset paths stay in the expert fallback."""
    source = (ROOT / "frontend" / "v7_backtest.html").read_text(encoding="utf-8")

    for contract in (
        "Market Settings Overrides",
        "Result Metrics",
        "marketSettingsCollect()",
        "resultMetricsCollect()",
        "PB8_ADVANCED_BT_PARAMS",
        "apiFetch('/result-metrics')",
    ):
        assert contract in source
    assert "'base_dir'," in source
    assert "extraBtKeys.length > 0 || managedBaseDir" in source
    assert source.count('id="managed-bt-base_dir"') == 1
    assert "'hlcvs_data_dir': {" in source
    assert "'hlcvs_data_override_mode': {" in source
    assert "options: ['intersection', 'dataset']" in source
    assert "Prepared Dataset Replay" not in source
    assert "cfg-hlcvs-data-dir" not in source
    additional_builder = source.split("function buildExtraBtExpanderHtml", 1)[1].split("function setCfgBotParamStatus", 1)[0]
    assert "buildResultMetricsHtml()" in additional_builder
    assert "title=\"' + escAttr(item.metric)" in source
    assert "'gateio','defx','paradex'" in source
    assert "<th>Maker</th>" not in source


def test_v8_advanced_field_transformations_round_trip() -> None:
    """Market overrides and visible-metric modes must retain their PB8 JSON semantics."""
    script = textwrap.dedent(
        """
        const assert = require('node:assert/strict');
        const fs = require('node:fs');
        global.window = {};
        eval(fs.readFileSync('frontend/js/backtest_advanced_fields.js', 'utf8'));
        const advanced = window.PBGuiBacktestAdvancedFields;

        const original = {
          future_root_field: { preserve: true },
          overrides: {
            BTC: { qty_step: 0.001, maker: -0.0001, future_field: 'preserve' }
          },
          overrides_by_exchange: {
            bybit: { ETH: { min_cost: 5, c_mult: 1 } }
          }
        };
        const rows = advanced.flattenMarketSettings(original);
        assert.equal(rows.length, 2);
        const extras = advanced.marketSettingsExtras(original);
        assert.deepEqual(advanced.serializeMarketSettings(rows, extras), original);
        assert.deepEqual(advanced.visibleMetricsState(null), { mode: 'default', selected: [] });
        assert.deepEqual(advanced.visibleMetricsState([]), { mode: 'all', selected: [] });
        assert.deepEqual(
          advanced.visibleMetricsState(['adg', 'sharpe_ratio']),
          { mode: 'custom', selected: ['adg', 'sharpe_ratio'] }
        );
        assert.equal(advanced.metricCategory('hard_stop_triggers_per_year'), 'Hard Stop');
        assert.throws(() => advanced.flattenMarketSettings('invalid'), /must be an object/);
        assert.throws(() => advanced.visibleMetricsState(['adg', 7]), /non-empty strings/);

        const specialRows = advanced.flattenMarketSettings(JSON.parse(
          '{"overrides":{},"overrides_by_exchange":{"__proto__":{"BTC":{"c_mult":2}}}}'
        ));
        const specialResult = advanced.serializeMarketSettings(specialRows, {});
        assert.equal(Object.prototype.c_mult, undefined);
        assert.equal(Object.prototype.BTC, undefined);
        assert.equal(Object.prototype.hasOwnProperty.call(specialResult.overrides_by_exchange, '__proto__'), true);
        assert.equal(Object.prototype.hasOwnProperty.call(specialResult.overrides_by_exchange.__proto__, 'BTC'), true);
        assert.equal(specialResult.overrides_by_exchange.__proto__.BTC.c_mult, 2);
        assert.equal(JSON.parse(JSON.stringify(specialResult)).overrides_by_exchange.__proto__.BTC.c_mult, 2);
        """
    )
    completed = subprocess.run(["node", "-e", script], cwd=ROOT, text=True, capture_output=True, check=False)
    assert completed.returncode == 0, completed.stderr or completed.stdout


def test_v8_advanced_fields_reject_invalid_raw_values_and_escape_attributes() -> None:
    """Raw-sync failures must remain visible, lossless, and safe to render."""
    source = (ROOT / "frontend" / "v7_backtest.html").read_text(encoding="utf-8")

    assert "_marketSettingsLoadError" in source
    assert "_resultMetricsLoadError" in source
    assert "&& !_marketSettingsLoadError" in source
    assert "&& !_resultMetricsLoadError" in source
    assert "id=\"extra-bt-' + escAttr(k)" in source
    assert "value=\"' + escAttr(v === null ? '' : String(v))" in source
    assert 'data-extra-bt-type="null"' in source
    assert "_cfgSymbolsLoadSeq += 1" in source
    reset_body = source.split("function resetBacktestEditorUiState()", 1)[1].split("function cfgRebuildMs", 1)[0]
    assert "_cfgSymbolsLoadSeq = 0" not in reset_body
    ensure_raw_body = source.split("function ensureRawJsonValidForSave()", 1)[1].split("function cfgShouldIgnoreStructuredSyncTarget", 1)[0]
    assert "raw !== _rawEditorLastApplied" in ensure_raw_body
    assert "cfgSyncEditorFromParsed" in ensure_raw_body
    assert "resultMetricsRender();" in source.split("function cfgSyncExtraBtFields", 1)[1].split("function cfgSyncEditorFromParsed", 1)[0]


def test_editor_adapter_preserves_v7_paths_and_writes_v8_risk_paths() -> None:
    """The shared editor's only generation difference is handled by its path adapter."""
    script = textwrap.dedent(
        """
        const assert = require('node:assert/strict');
        const fs = require('node:fs');
        global.window = { location: { origin: 'https://example.test' } };
        eval(fs.readFileSync('frontend/js/backtest_editor_adapter.js', 'utf8'));

        const v7 = window.PBGuiBacktestEditorAdapter.create('v7');
        const v7Side = { total_wallet_exposure_limit: 1.2, n_positions: 4 };
        assert.equal(v7.getSideValue(v7Side, 'n_positions', 0), 4);
        v7.setSideValue(v7Side, 'n_positions', 5);
        assert.equal(v7Side.n_positions, 5);
        assert.equal(v7Side.risk, undefined);

        const v8 = window.PBGuiBacktestEditorAdapter.create('v8');
        const v8Side = { risk: { total_wallet_exposure_limit: 2.5, n_positions: 8 }, strategy: {} };
        assert.equal(v8.getSideValue(v8Side, 'n_positions', 0), 8);
        v8.setSideValue(v8Side, 'total_wallet_exposure_limit', 3.0);
        assert.equal(v8Side.risk.total_wallet_exposure_limit, 3.0);
        assert.equal(v8Side.total_wallet_exposure_limit, undefined);
        assert.equal(v8.metadataApiBase('https://example.test/api/backtest-v8'), 'https://example.test/api/v7');
        assert.equal(v8.docsApiBase('https://example.test/api/backtest-v8'), 'https://example.test/api');
        assert.equal(v8.getHslValue({ hsl: { enabled: true } }, 'enabled', false), true);
        assert.equal(v7.getHslValue({ hsl_enabled: true }, 'enabled', false), true);
        assert.deepEqual(v8.initialPanels, ['configs', 'queue', 'results']);
        """
    )
    completed = subprocess.run(["node", "-e", script], cwd=ROOT, text=True, capture_output=True, check=False)
    assert completed.returncode == 0, completed.stderr or completed.stdout


def test_shared_coin_override_editor_preserves_nested_v8_paths() -> None:
    """Dotted V8 override selectors must round-trip as nested canonical objects."""
    script = textwrap.dedent(
        """
        const assert = require('node:assert/strict');
        const fs = require('node:fs');
        global.window = { PBGuiEditorShared: { clearFixedValidationStatus() {}, setFixedValidationError() {} } };
        eval(fs.readFileSync('frontend/js/coin_overrides_editor.js', 'utf8'));
        _covState.allowedParams = {
          bot: {
            long: { 'risk.n_positions': true, 'strategy.trailing_grid_v7.ema_span_0': true },
            short: {}
          }
        };
        const filtered = _covFilterOverrideConfig({
          bot: { long: {
            risk: { n_positions: 7, unsupported: 9 },
            strategy: { trailing_grid_v7: { ema_span_0: 240, removed: 1 } }
          } }
        });
        assert.deepEqual(filtered, { bot: { long: {
          risk: { n_positions: 7 },
          strategy: { trailing_grid_v7: { ema_span_0: 240 } }
        } } });
        const target = {};
        _covSetDotted(target, 'risk.total_wallet_exposure_limit', 2.5);
        assert.deepEqual(target, { risk: { total_wallet_exposure_limit: 2.5 } });
        _covDeleteDotted(target, 'risk.total_wallet_exposure_limit');
        _covCleanEmpty(target);
        assert.deepEqual(target, {});
        _covState.allowedParams = { bot: { long: { forager_score_weights: true }, short: {} } };
        const v7Filtered = _covFilterOverrideConfig({
          bot: { long: { forager_score_weights: { ema_readiness: 1, volume: 0 } } }
        });
        assert.deepEqual(v7Filtered, {
          bot: { long: { forager_score_weights: { ema_readiness: 1, volume: 0 } } }
        });
        assert.equal(_covParseParamValue('true', { type: 'boolean', default: false }, 'hsl.enabled'), true);
        assert.equal(_covParseParamValue('always', { type: 'string', default: 'threshold' }, 'hsl.restart_after_red_policy'), 'always');
        assert.equal(_covParseParamValue('2.75', { type: 'number', default: 1 }, 'risk.total_wallet_exposure_limit'), 2.75);
        assert.throws(() => _covParseParamValue('nope', { type: 'number' }, 'risk.n_positions'), /must be a number/);
        assert.throws(() => _covParseParamValue('maybe', { type: 'boolean' }, 'hsl.enabled'), /must be true or false/);
        _covState.deferConfigFileWrites = true;
        _covState.configName = 'demo';
        _covState.overrides = { HYPE: {} };
        _covState.editCoin = 'HYPE';
        _covValidateCfgJsonField = (side) => ({
          parsed: side === 'long' ? { risk: { n_positions: 3 } } : {},
          error: null,
        });
        global.document = {
            getElementById: (id) => ({
              value: id === 'cov-cfg-long' ? '{"risk":{"n_positions":3}}' : '{}',
              classList: { remove() {}, add() {}, toggle() {} },
              removeAttribute() {},
              setAttribute() {},
            })
        };
        assert.equal(_covSaveConfigFile('HYPE'), true);
        assert.equal(_covState.overrides.HYPE.override_config_path, 'HYPE.json');
        assert.deepEqual(_covState.pendingConfigFileWrites.HYPE.config, { bot: { long: { risk: { n_positions: 3 } } } });
        const firstSnapshot = coinOvSnapshotPendingFiles();
        assert.deepEqual(firstSnapshot.files['HYPE.json'], { bot: { long: { risk: { n_positions: 3 } } } });
        _covState.pendingConfigFileWrites.HYPE.config.bot.long.risk.n_positions = 4;
        coinOvAcknowledgePendingFiles(firstSnapshot);
        assert.equal(_covState.pendingConfigFileWrites.HYPE.config.bot.long.risk.n_positions, 4);
        const secondSnapshot = coinOvSnapshotPendingFiles();
        global.esc = (value) => String(value);
        _covRender = () => {};
        coinOvLoad({ coin_overrides: {
          HYPE: { override_config_path: 'HYPE.json' },
          '1000BONKUSDT': { override_config_path: '1000BONKUSDT.json' }
        } }, { preservePending: true });
        assert.equal(_covState.pendingConfigFileWrites.HYPE.config.bot.long.risk.n_positions, 4);
        coinOvAcknowledgePendingFiles(secondSnapshot);
        assert.equal(_covState.pendingConfigFileWrites.HYPE, undefined);
        assert.equal(_covState.overrides.BONK.override_config_path, '1000BONKUSDT.json');
        _covState.overrideConfigs.BONK = { old: true };
        coinOvLoad({ coin_overrides: { BONK: { override_config_path: 'BONK.json' } } }, { preservePending: true });
        assert.equal(_covState.overrideConfigs.BONK, undefined);
        let savedActiveEdit = false;
        _covState.editCoin = 'BONK';
        _covSaveEdit = () => { savedActiveEdit = true; return true; };
        coinOvEdit('BONK');
        assert.equal(savedActiveEdit, true);
        """
    )
    completed = subprocess.run(["node", "-e", script], cwd=ROOT, text=True, capture_output=True, check=False)
    assert completed.returncode == 0, completed.stderr or completed.stdout


def test_shared_backtest_shell_owns_v7_table_and_status_patterns() -> None:
    """V8 rows and statuses must use the same classes as the V7 page."""
    shell_source = (ROOT / "frontend" / "js" / "backtest_shell.js").read_text(encoding="utf-8")
    style_source = (ROOT / "frontend" / "css" / "backtest_shell.css").read_text(encoding="utf-8")

    assert "table', 'tbl'" in shell_source
    assert "badge-" in shell_source
    assert ".tbl tr.selected td" in style_source
    assert ".badge-running" in style_source
    assert ".badge-backtesting" in style_source
    assert ".badge-complete" in style_source
    assert "definition.selection.setSelected" in shell_source
