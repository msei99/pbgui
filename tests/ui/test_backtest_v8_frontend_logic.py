"""Static frontend contracts for the standalone PB8 backtest page."""

from pathlib import Path
import subprocess
import textwrap


ROOT = Path(__file__).resolve().parents[2]


def _extract_function(source: str, name: str) -> str:
    """Extract one named inline JavaScript function."""
    marker = f"function {name}("
    start = source.find(marker)
    assert start >= 0, f"Could not find JavaScript function {name!r}"
    async_start = source.rfind("async ", max(0, start - 8), start)
    if async_start >= 0:
        start = async_start
    brace_start = source.find("{", start)
    depth = 0
    quote: str | None = None
    escaped = False
    for index in range(brace_start, len(source)):
        char = source[index]
        if quote is not None:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == quote:
                quote = None
            continue
        if char in ("'", '"', "`"):
            quote = char
        elif char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                return source[start : index + 1]
    raise AssertionError(f"Could not extract complete JavaScript function {name!r}")


def test_v8_route_renders_the_v7_backtest_template() -> None:
    """PB8 must use the exact V7 page instead of maintaining a second editor."""
    api_source = (ROOT / "api" / "backtest_v8.py").read_text(encoding="utf-8")

    assert '"frontend" / "v7_backtest.html"' in api_source
    assert '"frontend" / "v8_backtest.html"' not in api_source
    assert not (ROOT / "frontend" / "v8_backtest.html").exists()
    assert '"%%BACKTEST_VERSION%%": "v8"' in api_source
    assert '"%%BACKTEST_NAV_CURRENT%%": "v8_backtest"' in api_source


def test_v8_optimize_result_draft_opens_without_repreparing() -> None:
    """A complete PB8 Pareto config must enter the Backtest editor unchanged."""
    source = (ROOT / "frontend" / "v7_backtest.html").read_text(encoding="utf-8")
    function = _extract_function(source, "openInitialBacktestDraftFromUrl")
    script = textwrap.dedent(
        f"""
        const assert = require('node:assert/strict');
        const resultConfig = {{
          bot: {{long: {{forager: {{volume_ema_span_1m: 0}}}}}},
          optimize: {{bounds: {{long: {{forager: {{volume_ema_span_1m: [0, 0, 1]}}}}}}}}
        }};
        const window = {{
          location: {{href: 'https://example.test/backtest?opt_draft_id=draft-1&draft_name=pareto'}},
          history: {{replaceState() {{}}}}
        }};
        const document = {{title: 'Backtest'}};
        const backtestEditorAdapter = {{isV8: true}};
        let prepareCalls = 0;
        let openedConfig = null;
        async function apiFetch() {{ return {{config: resultConfig}}; }}
        async function prepareImportedBacktestConfig() {{ prepareCalls += 1; throw new Error('must not prepare'); }}
        function getInitialBacktestDraftName() {{ return 'pareto'; }}
        function clearInitialBacktestUrlParams() {{}}
        function selectPanel() {{}}
        function showConfigEditor(_name, config) {{ openedConfig = config; }}
        function toast() {{}}
        let editingConfig = '';
        {function}
        (async () => {{
          assert.equal(await openInitialBacktestDraftFromUrl(), true);
          assert.equal(prepareCalls, 0);
          assert.equal(openedConfig, resultConfig);
        }})().catch(error => {{ console.error(error); process.exit(1); }});
        """
    )
    completed = subprocess.run(["node", "-e", script], cwd=ROOT, text=True, capture_output=True, check=False)
    assert completed.returncode == 0, completed.stderr or completed.stdout


def test_backtest_editor_preserves_zero_minimum_coin_age() -> None:
    """Saving a Pareto draft must not replace an explicit zero-day age gate with 30."""
    source = (ROOT / "frontend" / "v7_backtest.html").read_text(encoding="utf-8")
    collect = _extract_function(source, "collectConfig")
    snippet = "\n".join(
        line.strip()
        for line in collect.splitlines()
        if "minimumCoinAgeDays" in line
    )
    script = textwrap.dedent(
        f"""
        const assert = require('node:assert/strict');
        let inputValue = '';
        const document = {{getElementById(id) {{
          assert.equal(id, 'cfg-min-coin-age');
          return {{value: inputValue}};
        }}}};
        function serialize(value) {{
          inputValue = value;
          const cfg = {{live: {{}}}};
          {snippet}
          return cfg.live.minimum_coin_age_days;
        }}
        assert.equal(serialize('0'), 0);
        assert.equal(serialize('12.5'), 12.5);
        assert.equal(serialize(''), 30);
        assert.equal(serialize('invalid'), 30);
        """
    )
    completed = subprocess.run(["node", "-e", script], cwd=ROOT, text=True, capture_output=True, check=False)
    assert completed.returncode == 0, completed.stderr or completed.stdout


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


def test_v7_run_conversion_renders_structured_manual_review_error() -> None:
    """Run conversion must show actionable fields instead of stringifying detail objects."""
    source = (ROOT / "frontend" / "v7_run.html").read_text(encoding="utf-8")
    functions = "\n\n".join(
        _extract_function(source, name) for name in ("v8RunMigrationError", "v8RunMigrationDetail")
    )
    script = textwrap.dedent(
        f"""
        const assert = require('node:assert/strict');
        {functions}
        const error = v8RunMigrationError({{
          detail: {{
            code: 'migration_manual_review',
            message: 'Migration requires manual review: bot.long.example',
            report: {{
              manual_review_fields: ['bot.long.example'],
              dropped_unsupported_fields: ['bot.short.legacy'],
              mapped_fields: Array.from({{length: 100}}, (_, index) => 'mapped.' + index)
            }}
          }}
        }}, 422);
        assert.equal(error.message, 'Migration requires manual review: bot.long.example');
        assert.notEqual(error.message, '[object Object]');
        const detail = v8RunMigrationDetail(error);
        assert.match(detail, /bot\\.long\\.example/);
        assert.match(detail, /bot\\.short\\.legacy/);
        assert.doesNotMatch(detail, /mapped\\.99/);
        """
    )

    completed = subprocess.run(["node", "-e", script], cwd=ROOT, text=True, capture_output=True, check=False)
    assert completed.returncode == 0, completed.stderr or completed.stdout


def test_v7_run_manual_review_conversion_opens_unsaved_pb8_editor_draft() -> None:
    """Reviewable official migration output must navigate to the PB8 editor without publishing."""
    source = (ROOT / "frontend" / "v7_run.html").read_text(encoding="utf-8")
    functions = "\n\n".join(
        _extract_function(source, name)
        for name in ("v8RunMigrationError", "v8RunMigrationDetail", "convertInstanceToV8")
    )
    script = textwrap.dedent(
        f"""
        const assert = require('node:assert/strict');
        let requestBody = null;
        const window = {{
          location: {{origin: 'https://example.test', href: ''}},
          PBGuiDialogs: {{alert() {{ throw new Error('alert must not open for a review draft'); }}}}
        }};
        async function fetch(_url, options) {{
          requestBody = JSON.parse(options.body);
          return {{
            status: 200,
            ok: true,
            async json() {{ return {{review_required: true, draft_id: 'review-draft', name: 'demo_v8', editor: 'run'}}; }}
          }};
        }}
        function toast() {{}}
        {functions}
        convertInstanceToV8('demo').then(() => {{
          assert.equal(requestBody.allow_manual_review_output, true);
          assert.equal(
            window.location.href,
            'https://example.test/api/v8/edit_page?new=1&draft_id=review-draft'
          );
        }}).catch(error => {{ console.error(error); process.exit(1); }});
        """
    )

    completed = subprocess.run(["node", "-e", script], cwd=ROOT, text=True, capture_output=True, check=False)
    assert completed.returncode == 0, completed.stderr or completed.stdout
    run_editor_source = (ROOT / "frontend" / "v7_edit.html").read_text(encoding="utf-8")
    assert "showRunMigrationReview(draftResp)" in run_editor_source
    assert "status === 'pb_default' || status === 'review'" in run_editor_source
    assert "field === 'live.initial_entry_exec_max_market_dist_pct'" not in run_editor_source


def test_shared_editor_payload_preserves_migration_review_metadata() -> None:
    """Shared config normalization must not discard migration review details."""
    source = (ROOT / "frontend" / "js" / "editor_shared.js").read_text(encoding="utf-8")
    normalize = _extract_function(source, "normalizeEditorConfigPayload")
    script = textwrap.dedent(
        f"""
        const assert = require('node:assert/strict');
        {normalize}
        const report = {{manual_review_fields: ['live.price_distance_threshold']}};
        const values = {{'live.price_distance_threshold': 0.01}};
        const normalized = normalizeEditorConfigPayload({{
          config: {{live: {{}}}},
          param_status: {{}},
          migration_report: report,
          migration_review_values: values,
          migration_message: 'Migration requires manual review'
        }});
        assert.deepEqual(normalized.migration_report, report);
        assert.deepEqual(normalized.migration_review_values, values);
        assert.equal(normalized.migration_message, 'Migration requires manual review');
        """
    )

    completed = subprocess.run(["node", "-e", script], cwd=ROOT, text=True, capture_output=True, check=False)
    assert completed.returncode == 0, completed.stderr or completed.stdout


def test_v7_backtest_manual_review_conversion_opens_unsaved_pb8_editor_draft() -> None:
    """Backtest config/result conversion must use the same manual-review draft handoff."""
    source = (ROOT / "frontend" / "v7_backtest.html").read_text(encoding="utf-8")
    functions = "\n\n".join(
        _extract_function(source, name)
        for name in ("v8MigrationError", "v8MigrationReportText", "migrateV7SourceToV8")
    )
    script = textwrap.dedent(
        f"""
        const assert = require('node:assert/strict');
        let requestBody = null;
        const window = {{
          location: {{origin: 'https://example.test', href: ''}},
          PBGuiDialogs: {{alert() {{ throw new Error('alert must not open for a review draft'); }}}}
        }};
        async function fetch(_url, options) {{
          requestBody = JSON.parse(options.body);
          return {{
            status: 200,
            ok: true,
            async json() {{ return {{review_required: true, draft_id: 'review-draft', name: 'demo_v8'}}; }}
          }};
        }}
        function toast() {{}}
        {functions}
        migrateV7SourceToV8({{source_type: 'backtest_result', source_name: 'demo', target_name: 'demo_v8'}}).then(() => {{
          assert.equal(requestBody.allow_manual_review_output, true);
          assert.equal(
            window.location.href,
            'https://example.test/api/backtest-v8/main_page?opt_draft_id=review-draft&draft_name=demo_v8'
          );
        }}).catch(error => {{ console.error(error); process.exit(1); }});
        """
    )

    completed = subprocess.run(["node", "-e", script], cwd=ROOT, text=True, capture_output=True, check=False)
    assert completed.returncode == 0, completed.stderr or completed.stdout
    assert "_pendingMigrationReviewDraft = draft && draft.migration_report ? draft : null" in source
    assert "showMigrationReviewNotice(_pendingMigrationReviewDraft)" in source
    assert "migration-review-notice" in source


def test_v7_backtest_existing_v8_config_opens_without_redundant_dialog() -> None:
    """An existing migration target should open directly because there is no decision to make."""
    source = (ROOT / "frontend" / "v7_backtest.html").read_text(encoding="utf-8")
    functions = "\n\n".join(
        _extract_function(source, name)
        for name in ("v8MigrationError", "v8MigrationReportText", "migrateV7SourceToV8")
    )
    script = textwrap.dedent(
        f"""
        const assert = require('node:assert/strict');
        const window = {{
          location: {{origin: 'https://example.test', href: ''}},
          PBGuiDialogs: {{alert() {{ throw new Error('existing-config dialog must not open'); }}}}
        }};
        async function fetch() {{
          return {{status: 409, ok: false, async json() {{ return {{detail: 'already exists'}}; }}}};
        }}
        function toast() {{ throw new Error('409 must not show an error toast'); }}
        {functions}
        migrateV7SourceToV8({{source_type: 'backtest_config', source_name: 'demo', target_name: 'demo_v8'}}).then(() => {{
          assert.equal(
            window.location.href,
            'https://example.test/api/backtest-v8/main_page?config=demo_v8'
          );
        }}).catch(error => {{ console.error(error); process.exit(1); }});
        """
    )

    completed = subprocess.run(["node", "-e", script], cwd=ROOT, text=True, capture_output=True, check=False)
    assert completed.returncode == 0, completed.stderr or completed.stdout


def test_v7_and_v8_share_the_same_backtest_shell() -> None:
    """The one backtest template must consume the shared shell and version adapter."""
    v7_source = (ROOT / "frontend" / "v7_backtest.html").read_text(encoding="utf-8")
    shell_source = (ROOT / "frontend" / "js" / "backtest_shell.js").read_text(encoding="utf-8")
    adapter_source = (ROOT / "frontend" / "js" / "backtest_editor_adapter.js").read_text(encoding="utf-8")

    assert '/app/css/backtest_shell.css?v=3' in v7_source
    assert '/app/js/backtest_shell.js?v=4' in v7_source
    assert '/app/js/backtest_editor_adapter.js?v=10' in v7_source
    assert "PBGuiBacktestShell.upgradeLegacy" in v7_source
    assert "PBGuiBacktestEditorAdapter.create(BACKTEST_VERSION)" in v7_source
    assert "sideConfig.risk" in adapter_source
    assert "setSideValue" in adapter_source
    for required_id in ("sidebar", "sidebar-inner", "sidebar-editor", "panel-configs", "panel-queue", "panel-results"):
        assert required_id in shell_source
    assert "source.remove()" in shell_source


def test_delayed_config_load_does_not_replace_results_sidebar() -> None:
    """A stale config response must not reopen the editor after navigation."""
    source = (ROOT / "frontend" / "v7_backtest.html").read_text(encoding="utf-8")
    functions = "\n\n".join(_extract_function(source, name) for name in ("editConfig", "newConfig"))
    script = textwrap.dedent(
        f"""
        const assert = require('node:assert/strict');
        let currentPanel = 'configs';
        let editingConfig = null;
        let pendingResolve;
        let opened = [];
        function apiFetch() {{
          return new Promise(resolve => {{ pendingResolve = resolve; }});
        }}
        function showConfigEditor(name, config) {{ opened.push({{name, config}}); }}
        function toast() {{}}
        {functions}
        (async () => {{
          editConfig('alpha');
          currentPanel = 'results';
          pendingResolve({{name: 'alpha', config: {{source: 'saved'}}}});
          await new Promise(resolve => setImmediate(resolve));
          assert.deepEqual(opened, []);
          assert.equal(editingConfig, null);

          currentPanel = 'configs';
          editConfig('beta');
          pendingResolve({{name: 'beta', config: {{source: 'saved'}}}});
          await new Promise(resolve => setImmediate(resolve));
          assert.equal(opened.length, 1);
          assert.equal(opened[0].name, 'beta');

          const create = newConfig();
          currentPanel = 'results';
          pendingResolve({{config: {{source: 'template'}}}});
          await create;
          assert.equal(opened.length, 1);
          assert.equal(editingConfig, 'beta');
        }})().catch(error => {{ console.error(error); process.exit(1); }});
        """
    )
    completed = subprocess.run(["node", "-e", script], cwd=ROOT, text=True, capture_output=True, check=False)
    assert completed.returncode == 0, completed.stderr or completed.stdout


def test_editor_results_button_opens_the_unfiltered_results_panel() -> None:
    """The editor sidebar must always provide access to all backtest results."""
    source = (ROOT / "frontend" / "v7_backtest.html").read_text(encoding="utf-8")
    button_start = source.index('<button class="sb-btn" id="sb-btn-results"')
    button_markup = source[button_start : source.index("</button>", button_start)]
    functions = "\n\n".join(
        _extract_function(source, name) for name in ("showEditorSidebar", "goEditorResults")
    )
    script = textwrap.dedent(
        f"""
        const assert = require('node:assert/strict');
        const elements = {{
          'sb-btn-add-to-run': {{disabled: true}},
          'sb-btn-convert-v8': {{disabled: true, style: {{}}}},
          'results-filter': {{value: 'old text'}},
          'results-config-filter': {{value: 'old config'}}
        }};
        const document = {{getElementById(id) {{ return elements[id] || null; }}}};
        const backtestEditorAdapter = {{isV8: true}};
        const backtestShell = {{setEditorMode(value) {{ assert.equal(value, true); }}}};
        let editingConfig = '__new__';
        let _pendingResultFilter = 'old config';
        let closed = 0;
        let selectedPanel = '';
        function closeEditor() {{ closed += 1; }}
        function selectPanel(panel) {{ selectedPanel = panel; }}
        {functions}
        showEditorSidebar();
        assert.equal(elements['sb-btn-add-to-run'].disabled, true);
        assert.equal(elements['sb-btn-convert-v8'].style.display, 'none');
        goEditorResults();
        assert.equal(closed, 1);
        assert.equal(selectedPanel, 'results');
        assert.equal(_pendingResultFilter, '');
        assert.equal(elements['results-filter'].value, '');
        assert.equal(elements['results-config-filter'].value, '');
        """
    )

    assert " disabled" not in button_markup
    assert 'title="Show all backtest results"' in button_markup
    completed = subprocess.run(["node", "-e", script], cwd=ROOT, text=True, capture_output=True, check=False)
    assert completed.returncode == 0, completed.stderr or completed.stdout


def test_queue_result_action_applies_filter_before_showing_results() -> None:
    """Queue result navigation must never expose the stale unfiltered table."""
    source = (ROOT / "frontend" / "v7_backtest.html").read_text(encoding="utf-8")
    function = _extract_function(source, "viewConfigResults")
    script = textwrap.dedent(
        f"""
        const assert = require('node:assert/strict');
        const configFilter = {{
          options: [{{value: ''}}],
          value: '',
          appendChild(option) {{ this.options.push(option); }}
        }};
        const nodes = {{
          'results-filter': {{value: 'old search'}},
          'results-config-filter': configFilter,
          'results-list': {{innerHTML: '<table>all results</table>'}}
        }};
        const document = {{
          getElementById(id) {{ return nodes[id] || null; }},
          createElement(tag) {{ assert.equal(tag, 'option'); return {{value: '', textContent: ''}}; }}
        }};
        let results = [];
        let _pendingResultFilter = '';
        let selectedSnapshots = [];
        let rendered = 0;
        let loadArgs = [];
        let hiddenCounts = 0;
        function selectPanel(panel, options) {{
          selectedSnapshots.push({{
            panel,
            deferred: !!(options && options.deferResultsLoad),
            config: configFilter.value,
            list: nodes['results-list'].innerHTML
          }});
        }}
        function renderResults() {{ rendered += 1; }}
        function loadResults(name, options) {{ loadArgs.push({{name, keepVisible: !!(options && options.keepVisible)}}); }}
        function hideResultsCountLabel() {{ hiddenCounts += 1; }}
        {function}

        viewConfigResults('target config');
        assert.equal(nodes['results-filter'].value, '');
        assert.equal(configFilter.value, 'target config');
        assert.equal(_pendingResultFilter, 'target config');
        assert.match(selectedSnapshots[0].list, /Checking for matching results/);
        assert.equal(selectedSnapshots[0].deferred, true);
        assert.equal(hiddenCounts, 1);

        results = [{{config_name: 'target config'}}];
        viewConfigResults('target config');
        assert.equal(_pendingResultFilter, '');
        assert.equal(selectedSnapshots[1].config, 'target config');
        assert.equal(selectedSnapshots[1].deferred, true);
        assert.equal(rendered, 1);
        assert.deepEqual(loadArgs, [
          {{name: 'target config', keepVisible: false}},
          {{name: 'target config', keepVisible: true}}
        ]);
        """
    )

    completed = subprocess.run(["node", "-e", script], cwd=ROOT, text=True, capture_output=True, check=False)
    assert completed.returncode == 0, completed.stderr or completed.stdout


def test_results_load_progressively_and_use_server_config_filter() -> None:
    """Result pages must render incrementally and scope queue requests by config."""
    source = (ROOT / "frontend" / "v7_backtest.html").read_text(encoding="utf-8")
    function = _extract_function(source, "loadResults")
    script = textwrap.dedent(
        f"""
        const assert = require('node:assert/strict');
        const nodes = {{
          'results-list': {{innerHTML: '<table>stale</table>'}},
          'results-count-label': {{textContent: '', style: {{display: ''}}}}
        }};
        const document = {{getElementById(id) {{ return nodes[id] || null; }}}};
        const requests = [];
        const appliedCounts = [];
        let paintFrames = 0;
        const window = {{requestAnimationFrame(callback) {{ paintFrames += 1; setImmediate(callback); }}}};
        let results = [];
        let resultsByVersion = {{v7: [], v8: []}};
        let RESULTS_PAGE_SIZE = 5;
        let _pendingResultFilter = '';
        let _resultsLoadGeneration = 0;
        let _resultsEmptyRetryTimer = null;
        let _resultsEmptyRetryCount = 0;
        let currentPanel = 'results';
        function selectedResultsVersion() {{ return 'v8'; }}
        function backtestApiBase(version) {{ return '/api/backtest-' + version; }}
        function hideResultsCountLabel() {{ nodes['results-count-label'].style.display = 'none'; }}
        function resultsForSelectedVersion(items) {{ return items.filter(item => item.backtest_version === 'v8'); }}
        function _applyResultsData(items, filterName) {{
          assert.equal(filterName, 'target config');
          results = items;
          appliedCounts.push(items.length);
          nodes['results-count-label'].textContent = items.length + ' results';
          return items;
        }}
        function apiFetchFrom(base, path) {{
          requests.push(base + path);
          if (path.includes('offset=0')) return Promise.resolve({{
            results: [{{path: '/new', config_name: 'target config'}}],
            pagination: {{total: 2, has_more: true, next_offset: 1}}
          }});
          return Promise.resolve({{
            results: [{{path: '/old', config_name: 'target config'}}],
            pagination: {{total: 2, has_more: false, next_offset: 2}}
          }});
        }}
        function toast(message) {{ throw new Error(message); }}
        {function}

        loadResults('target config').then(() => {{
          assert.deepEqual(appliedCounts, [1, 2, 2]);
          assert.equal(results.length, 2);
          assert.match(requests[0], /offset=0&limit=5&name=target%20config$/);
          assert.match(requests[1], /offset=1&limit=5&name=target%20config$/);
          assert.equal(paintFrames, 2);
          assert.equal(nodes['results-count-label'].textContent, '2 results');
        }}).catch(error => {{ console.error(error); process.exit(1); }});
        """
    )

    completed = subprocess.run(["node", "-e", script], cwd=ROOT, text=True, capture_output=True, check=False)
    assert completed.returncode == 0, completed.stderr or completed.stdout


def test_delayed_v8_settings_load_does_not_replace_results_sidebar() -> None:
    """A deferred V8 editor render must stop after the user leaves Configs."""
    source = (ROOT / "frontend" / "v7_backtest.html").read_text(encoding="utf-8")
    function = _extract_function(source, "showConfigEditor")
    function = function[: function.index("  resetBacktestEditorUiState();")] + "  resetBacktestEditorUiState();\n}"
    script = textwrap.dedent(
        f"""
        const assert = require('node:assert/strict');
        const backtestEditorAdapter = {{isV8: true}};
        const settings = {{exchange_options: []}};
        let currentPanel = 'configs';
        let resolveSettings;
        let settingsLoadPromise = new Promise(resolve => {{
          resolveSettings = () => {{ settingsLoadPromise = null; resolve(); }};
        }});
        let editorRenderCount = 0;
        function resetBacktestEditorUiState() {{ editorRenderCount += 1; }}
        {function}
        (async () => {{
          showConfigEditor('alpha', {{}});
          currentPanel = 'results';
          resolveSettings();
          await new Promise(resolve => setImmediate(resolve));
          assert.equal(editorRenderCount, 0);
        }})().catch(error => {{ console.error(error); process.exit(1); }});
        """
    )
    completed = subprocess.run(["node", "-e", script], cwd=ROOT, text=True, capture_output=True, check=False)
    assert completed.returncode == 0, completed.stderr or completed.stdout


def test_v8_result_without_saved_config_opens_as_new_draft() -> None:
    """A PB8 result group must not be reused as a nonexistent source config when saving under a new name."""
    source = (ROOT / "frontend" / "v7_backtest.html").read_text(encoding="utf-8")
    function = source.split("function rebacktestSelected()", 1)[1].split("/* Multiple:", 1)[0]
    source_logic = "\n".join(
        line.strip()
        for line in function.splitlines()
        if "hasSavedSource" in line or "editingConfig = hasSavedSource" in line
    )
    script = textwrap.dedent(
        f"""
        const assert = require('node:assert/strict');
        function resultEditorSource(isV8, configNames) {{
          const backtestEditorAdapter = {{isV8}};
          const configs = configNames.map(name => ({{name}}));
          const name = 'hash';
          let editingConfig = null;
          {source_logic}
          return editingConfig;
        }}
        assert.equal(resultEditorSource(true, ['saved-config']), '__new__');
        assert.equal(resultEditorSource(true, ['saved-config', 'hash']), 'hash');
        assert.equal(resultEditorSource(false, []), 'hash');
        """
    )
    completed = subprocess.run(["node", "-e", script], cwd=ROOT, text=True, capture_output=True, check=False)
    assert completed.returncode == 0, completed.stderr or completed.stdout


def test_backtest_settings_modal_opens_immediately_then_refreshes_authoritative_values() -> None:
    """The settings dialog must render before its deduplicated backend refresh finishes."""
    source = (ROOT / "frontend" / "v7_backtest.html").read_text(encoding="utf-8")
    functions = "\n\n".join(
        _extract_function(source, name)
        for name in ("loadSettings", "renderSettingsModal", "syncOpenSettingsModal", "openSettingsModal", "settingsAdjustCpu")
    )
    script = textwrap.dedent(
        f"""
        const assert = require('node:assert/strict');
        let settings = {{cpu: 1, cpu_max: null, autostart: false, use_pbgui_market_data: false, hlcvs_cleanup_enabled: false, hlcvs_cleanup_days: 7, hlcvs_cleanup_interval_h: 24}};
        let settingsLoadPromise = null;
        let settingsModalDirty = false;
        let modalBody = '';
        let toastMessage = '';
        const elements = {{
          'set-cpu-val': {{value: '1'}},
          'set-cpu-max': {{textContent: ''}},
          'set-autostart': {{checked: false}},
          'set-pbgui-market-data': {{checked: false}},
          'set-cleanup-enabled': {{checked: false}},
          'set-cleanup-days': {{value: '7'}},
          'set-cleanup-interval': {{value: '24'}},
          'cleanup-opts': {{style: {{}}}}
        }};
        const window = {{navigator: {{hardwareConcurrency: 4}}}};
        const document = {{getElementById: id => elements[id] || null}};
        function showModal(_title, body) {{ modalBody = body; }}
        function toast(message) {{ toastMessage = message; }}
        function saveSettingsFromModal() {{}}
        let resolveFetch;
        let apiFetch = () => new Promise(resolve => {{ resolveFetch = resolve; }});
        {functions}
        (async () => {{
          const refresh = openSettingsModal();
          assert.match(modalBody, /max 4/);
          resolveFetch({{cpu: 8, cpu_max: 16, autostart: true, use_pbgui_market_data: true, hlcvs_cleanup_enabled: true, hlcvs_cleanup_days: 9, hlcvs_cleanup_interval_h: 12}});
          await refresh;
          assert.equal(elements['set-cpu-val'].value, 8);
          assert.equal(elements['set-cpu-max'].textContent, 'max 16');
          assert.equal(elements['set-autostart'].checked, true);
          settingsAdjustCpu(1);
          assert.equal(elements['set-cpu-val'].value, 9);
          apiFetch = () => Promise.reject(new Error('offline'));
          await openSettingsModal();
          assert.match(modalBody, /max 16/);
          assert.match(toastMessage, /Failed to refresh settings: offline/);
        }})().catch(error => {{ console.error(error); process.exit(1); }});
        """
    )
    completed = subprocess.run(["node", "-e", script], cwd=ROOT, text=True, capture_output=True, check=False)
    assert completed.returncode == 0, completed.stderr or completed.stdout


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


def test_v8_archive_results_can_open_the_pb8_run_editor() -> None:
    """PB8 keeps the shared Archive panel and routes PB8 configs into a PB8 Run draft."""
    page = (ROOT / "frontend" / "v7_backtest.html").read_text(encoding="utf-8")
    adapter = (ROOT / "frontend" / "js" / "backtest_editor_adapter.js").read_text(encoding="utf-8")

    assert "items.push({ panel: 'archive'" in adapter
    assert "'/backtest-v7'" in adapter
    assert "window.addToRunFromArchive = function()" in adapter
    assert "window.archiveResultApiFetch" in adapter
    assert "return navigate(config || {}, {}, name)" in adapter
    assert "'addResultToArchive'" not in adapter.split("var unsupported =", 1)[1].split("];", 1)[0]
    assert "backtest_version: selectedResult.backtest_version || backtestEditorAdapter.version" in page
    assert "archiveResultApiFetch" in page
    assert "{ showVersion: true, showStrategy: true }" in page
    assert "Add to Run is available only for PB7 archive results." not in page


def test_v8_backtest_result_can_open_pb8_optimize() -> None:
    """The existing PB8 optimize-draft contract must remain reachable from selected results."""
    adapter = (ROOT / "frontend" / "js" / "backtest_editor_adapter.js").read_text(encoding="utf-8")
    page = (ROOT / "frontend" / "v7_backtest.html").read_text(encoding="utf-8")

    assert "window.optimizeFromResult = function()" in adapter
    assert "'/api/optimize-v8/main_page?opt_draft_id='" in adapter
    unsupported = adapter.split("var unsupported =", 1)[1].split("];", 1)[0]
    assert "'optimizeFromResult'" not in unsupported
    assert "/app/js/backtest_editor_adapter.js?v=10" in page


def test_v8_result_add_to_run_uses_direct_canonical_draft() -> None:
    """PB8 result handoff must avoid a redundant result fetch and PB8 prepare subprocess."""

    adapter = (ROOT / "frontend" / "js" / "backtest_editor_adapter.js").read_text(encoding="utf-8")
    handoff = _extract_function(adapter, "installRunHandoff")

    assert "window.apiFetch('/results/run-draft'" in handoff
    assert "body: JSON.stringify({ path: selected[0] })" in handoff
    add_to_run = handoff.split("window.addToRun = function()", 1)[1].split("window.addToRunFromArchive", 1)[0]
    assert "'/results/config?path='" not in add_to_run
    assert "openDraft(payload, payload.name || 'pb8-run')" in add_to_run


def test_v8_backtest_strategy_explorer_handoffs_use_cookie_drafts() -> None:
    """PB8 config and result handoffs must remain visible and use opaque same-origin drafts."""
    adapter = (ROOT / "frontend" / "js" / "backtest_editor_adapter.js").read_text(encoding="utf-8")
    page = (ROOT / "frontend" / "v7_backtest.html").read_text(encoding="utf-8")

    unsupported = adapter.split("var unsupported =", 1)[1].split("];", 1)[0]
    assert "'goStrategyExplorer'" not in unsupported
    assert "'strategyExplorerFromResult'" not in unsupported
    assert "'/api/strategy-explorer-v8'" in page
    assert "credentials: 'same-origin'" in _extract_function(page, "goStrategyExplorer")
    result_handoff = _extract_function(page, "strategyExplorerFromResult")
    assert "override_configs: selectedResult.override_configs || {}" not in result_handoff
    assert "result_path: path" in result_handoff
    assert "provenance: { kind: 'backtest_result' }" in result_handoff
    assert "if (!selectedIsV8) url += '&result_path='" in result_handoff
    assert "Authorization" not in result_handoff


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
        '@router.post("/results/run-draft")',
        '@router.get("/results/files")',
        '@router.get("/results/equity")',
        '@router.get("/results/price")',
        '@router.get("/results/fills")',
        '@router.get("/results/image")',
        '@router.delete("/results")',
        '@router.post("/optimize-draft")',
        '@router.get("/optimize-draft/{draft_id}")',
        '@router.post("/queue-draft")',
        '@router.get("/queue-draft/{draft_id}")',
    )

    for route in required_routes:
        assert route in api_source


def test_combined_pb8_result_builds_price_market_options() -> None:
    """Configured exchanges from a combined PB8 result must populate the shared price selector."""
    source = (ROOT / "frontend" / "v7_backtest.html").read_text(encoding="utf-8")
    function = _extract_function(source, "resultPriceMarkets")
    script = textwrap.dedent(
        f"""
        const assert = require('node:assert/strict');
        {function}
        const markets = resultPriceMarkets({{
          exchange_dir: 'combined',
          exchanges: ['binance', 'bybit'],
          coins: ['HYPE']
        }});
        assert.deepEqual(markets, [
          {{exchange: 'binance', coin: 'HYPE'}},
          {{exchange: 'bybit', coin: 'HYPE'}}
        ]);
        assert.deepEqual(resultPriceMarkets({{
          exchange_dir: 'suite_runs',
          exchanges: ['bybit'],
          coins: ['HYPE']
        }}), [{{exchange: 'bybit', coin: 'HYPE'}}]);
        """
    )

    subprocess.run(["node", "-e", script], cwd=ROOT, check=True, capture_output=True, text=True)


def test_balance_chart_renders_market_price_on_second_axis() -> None:
    """The shared balance chart must add a close-price trace without replacing balance or equity."""
    source = (ROOT / "frontend" / "v7_backtest.html").read_text(encoding="utf-8")
    function = _extract_function(source, "renderBEChart")
    script = textwrap.dedent(
        f"""
        const assert = require('node:assert/strict');
        let plot = null;
        const document = {{getElementById() {{ return {{}}; }}}};
        const Plotly = {{newPlot(id, traces, layout) {{ plot = {{id, traces, layout}}; }}}};
        function fmtDate() {{ return 'date'; }}
        function _chartLayout(title, axis) {{ return {{title, yaxis: {{title: axis}}, margin: {{r: 20}}}}; }}
        function _plotlyConf() {{ return {{}}; }}
        {function}
        renderBEChart(
          {{id: 'be-chart-1', type: 'be', result: {{config_name: 'demo', exchange_dir: 'combined'}}}},
          {{time: ['2026-07-22T00:00:00Z'], balance: [1000], equity: [1010], balance_btc: [], equity_btc: []}},
          {{exchange: 'bybit', coin: 'ADA', time: ['2026-07-22T00:00:00Z'], close: [0.62]}}
        );
        assert.equal(plot.traces.length, 3);
        assert.equal(plot.traces[2].name, 'bybit / ADA close');
        assert.equal(plot.traces[2].yaxis, 'y2');
        assert.equal(plot.traces[2].line.color, 'rgba(217, 70, 239, 0.7)');
        assert.equal(plot.traces[2].line.width, 1.25);
        assert.equal(plot.traces[2].line.dash, 'dot');
        assert.equal(plot.layout.yaxis2.overlaying, 'y');
        assert.equal(plot.layout.yaxis2.side, 'right');
        """
    )

    subprocess.run(["node", "-e", script], cwd=ROOT, check=True, capture_output=True, text=True)


def test_price_coverage_is_measured_against_visible_chart_range() -> None:
    """Pre-inception config dates must not mark complete visible chart coverage as partial."""
    source = (ROOT / "frontend" / "v7_backtest.html").read_text(encoding="utf-8")
    coverage_function = _extract_function(source, "pricePayloadCoversChart")
    render_function = _extract_function(source, "renderBEWithSelectedPrice")
    script = textwrap.dedent(
        f"""
        const assert = require('node:assert/strict');
        var _priceRequestSeq = {{}};
        let status = null;
        const payload = {{
          available: true,
          coverage_complete: false,
          coverage_start: '2024-12-05T00:00:00Z',
          coverage_end: '2026-07-30T23:59:00Z',
          time: ['2024-12-05T00:00:00Z', '2026-07-30T23:59:00Z'],
          close: [10, 50]
        }};
        function selectedPriceMarket() {{ return {{exchange: 'bybit', coin: 'HYPE'}}; }}
        function setPriceOverlayStatus(_cd, text, warning) {{ status = {{text, warning}}; }}
        function loadPricePayload() {{ return Promise.resolve(payload); }}
        function resultPriceMarkets() {{ return [{{exchange: 'bybit', coin: 'HYPE'}}]; }}
        function renderBEChart() {{}}
        {coverage_function}
        {render_function}
        renderBEWithSelectedPrice(
          {{id: 'be-chart-1', type: 'be', path: '/result', result: {{}}, idx: 1}},
          {{time: ['2025-01-06T19:00:00Z', '2026-07-29T23:00:00Z']}},
          false
        );
        setImmediate(function() {{
          assert.equal(status.text, '2 price points, full chart coverage');
          assert.equal(status.warning, false);
        }});
        """
    )

    subprocess.run(["node", "-e", script], cwd=ROOT, check=True, capture_output=True, text=True)


def test_combined_result_prefers_exchange_with_full_price_coverage() -> None:
    """Initial combined charts should avoid a later-listed market when another exchange covers the chart."""
    source = (ROOT / "frontend" / "v7_backtest.html").read_text(encoding="utf-8")
    coverage_function = _extract_function(source, "pricePayloadCoversChart")
    render_function = _extract_function(source, "renderBEWithSelectedPrice")
    script = textwrap.dedent(
        f"""
        const assert = require('node:assert/strict');
        var _priceRequestSeq = {{}};
        let rendered = null;
        let status = null;
        const select = {{value: 'binance|HYPE'}};
        const document = {{getElementById() {{ return select; }}}};
        function selectedPriceMarket() {{ return {{exchange: 'binance', coin: 'HYPE'}}; }}
        function resultPriceMarkets() {{ return [
          {{exchange: 'binance', coin: 'HYPE'}},
          {{exchange: 'bybit', coin: 'HYPE'}}
        ]; }}
        function priceMarketOptionValue(market) {{ return market.exchange + '|' + market.coin; }}
        function loadPricePayload(_cd, market) {{ return Promise.resolve({{
          available: true,
          exchange: market.exchange,
          coin: market.coin,
          coverage_start: market.exchange === 'bybit' ? '2024-12-05T00:00:00Z' : '2025-05-30T00:00:00Z',
          coverage_end: '2026-07-31T23:59:00Z',
          time: ['2025-01-01T00:00:00Z'],
          close: [50]
        }}); }}
        function setPriceOverlayStatus(_cd, text, warning) {{ status = {{text, warning}}; }}
        function renderBEChart(_cd, _be, payload) {{ rendered = payload; }}
        {coverage_function}
        {render_function}
        renderBEWithSelectedPrice(
          {{id: 'be-chart-1', type: 'be', path: '/result', result: {{}}, idx: 1}},
          {{time: ['2025-02-01T00:00:00Z', '2026-07-31T00:00:00Z']}},
          true
        );
        setImmediate(function() {{
          assert.equal(select.value, 'bybit|HYPE');
          assert.equal(rendered.exchange, 'bybit');
          assert.match(status.text, /full chart coverage/);
          assert.equal(status.warning, false);
        }});
        """
    )

    subprocess.run(["node", "-e", script], cwd=ROOT, check=True, capture_output=True, text=True)


def test_pb8_symbol_pnl_uses_real_timestamps_and_net_fees() -> None:
    """PB8 fill timestamps must not be shifted to end_date and per-symbol PnL must be net of fees."""
    source = (ROOT / "frontend" / "v7_backtest.html").read_text(encoding="utf-8")
    time_function = _extract_function(source, "resolveFilsTimes")
    render_function = _extract_function(source, "renderPnlChart")
    script = textwrap.dedent(
        f"""
        const assert = require('node:assert/strict');
        let plot = null;
        let axisTitle = null;
        const document = {{getElementById: () => ({{}})}};
        const Plotly = {{newPlot(_id, traces) {{ plot = traces; }}}};
        function fmtDate() {{ return 'date'; }}
        function _chartLayout(_title, axis) {{ axisTitle = axis; return {{}}; }}
        function _plotlyConf() {{ return {{}}; }}
        {time_function}
        {render_function}
        renderPnlChart(
          {{id: 'pnl-1', result: {{end_date: '2030-01-01', config_name: 'demo'}}}},
          {{headers: ['', 'timestamp', 'minute', 'coin', 'pnl', 'fee_paid'], rows: [
            {{timestamp: '2026-07-20 12:00:00', minute: '10', coin: 'HYPE', pnl: '10', fee_paid: '-1'}},
            {{timestamp: '2026-07-21 13:00:00', minute: '20', coin: 'HYPE', pnl: '5', fee_paid: '-1'}}
          ]}}
        );
        assert.deepEqual(plot[0].x, ['2026-07-20T12:00:00.000Z', '2026-07-21T13:00:00.000Z']);
        assert.deepEqual(plot[0].y, [9, 13]);
        assert.equal(axisTitle, 'Net PnL');
        """
    )

    subprocess.run(["node", "-e", script], cwd=ROOT, check=True, capture_output=True, text=True)


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
    assert "var exchanges = backtestExchangeOptions();" in source
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
        const exact = advanced.serializeMarketSettings([
          { scope: 'global', coin: '1000ABC/USDT:USDT', values: { c_mult: 1000 } }
        ], {}, true);
        assert.deepEqual(exact.overrides, { '1000ABC/USDT:USDT': { c_mult: 1000 } });
        """
    )
    completed = subprocess.run(["node", "-e", script], cwd=ROOT, text=True, capture_output=True, check=False)
    assert completed.returncode == 0, completed.stderr or completed.stdout


def test_suite_coin_lists_preserve_pb8_identifiers_only() -> None:
    """Suite scenarios must keep exact PB8 identifiers while retaining PB7 uppercase behavior."""
    script = textwrap.dedent(
        """
        const assert = require('node:assert/strict');
        const fs = require('node:fs');
        eval(fs.readFileSync('frontend/js/suite_editor.js', 'utf8'));
        _suiteAvailableCoins = () => [];

        _suiteState.preserveMarketIdentifiers = false;
        _suiteEnsureCoinMsState('pb7', ['btc', '1000abcusdt']);
        assert.deepEqual(_suiteMsState.pb7.selected, ['BTC', '1000ABCUSDT']);

        _suiteState.preserveMarketIdentifiers = true;
        _suiteEnsureCoinMsState('pb8', ['bitget::1000ABCUSDT', 'xyz:TSLA', '1000ABC/USDT:USDT']);
        assert.deepEqual(_suiteMsState.pb8.selected, [
          'bitget::1000ABCUSDT', 'xyz:TSLA', '1000ABC/USDT:USDT'
        ]);
        """
    )
    completed = subprocess.run(["node", "-e", script], cwd=ROOT, text=True, capture_output=True, check=False)
    assert completed.returncode == 0, completed.stderr or completed.stdout


def test_suite_exchange_options_are_injected_for_pb8_and_reset_for_pb7() -> None:
    """Shared Suite selectors must use runtime PB8 choices without changing PB7."""
    script = textwrap.dedent(
        """
        const assert = require('node:assert/strict');
        const fs = require('node:fs');
        eval(fs.readFileSync('frontend/js/suite_editor.js', 'utf8'));
        _suiteLoadBotParams = () => Promise.resolve([]);

        suiteInit('suite', {version: 'v8', exchanges: ['binance', 'weex', 'weex']});
        assert.deepEqual(_suiteState.exchanges, ['binance', 'weex']);

        suiteInit('suite', {version: 'v7'});
        assert.deepEqual(_suiteState.exchanges, ['binance','bybit','bitget','okx','hyperliquid','kucoin']);
        """
    )
    completed = subprocess.run(["node", "-e", script], cwd=ROOT, text=True, capture_output=True, check=False)
    assert completed.returncode == 0, completed.stderr or completed.stdout


def test_pb8_config_editor_waits_for_runtime_exchange_settings() -> None:
    """A fast editor open must not render PB7 fallback exchanges for PB8."""
    source = (ROOT / "frontend" / "v7_backtest.html").read_text(encoding="utf-8")
    show_editor = source.split("function showConfigEditor", 1)[1].split("function ", 1)[0]

    assert "backtestEditorAdapter.isV8" in show_editor
    assert "settings.exchange_options" in show_editor
    assert "settingsLoadPromise.then" in show_editor


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
        assert.equal(v8.metadataApiBase('https://example.test/api/backtest-v8'), 'https://example.test/api/v8');
        assert.equal(v8.docsApiBase('https://example.test/api/backtest-v8'), 'https://example.test/api');
        assert.equal(v8.getHslValue({ hsl: { enabled: true } }, 'enabled', false), true);
        assert.equal(v7.getHslValue({ hsl_enabled: true }, 'enabled', false), true);
        assert.deepEqual(v8.initialPanels, ['configs', 'queue', 'results', 'archive']);
        assert.equal(v8.archiveApiBase('https://example.test/api/backtest-v8'), 'https://example.test/api/backtest-v7');
        """
    )
    completed = subprocess.run(["node", "-e", script], cwd=ROOT, text=True, capture_output=True, check=False)
    assert completed.returncode == 0, completed.stderr or completed.stdout


def test_shared_coin_override_editor_preserves_nested_v8_paths() -> None:
    """Dotted V8 override selectors must round-trip as nested canonical objects."""
    script = textwrap.dedent(
        r"""
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

        _covState.preserveMarketIdentifiers = true;
        _covState.pendingConfigFileWrites = {};
        _covState.overrideConfigs = {};
        coinOvLoad({ coin_overrides: {
          'bitget::ABCUSDT': { override_config_path: 'plain.json' },
          'bitget::1000ABCUSDT': { override_config_path: 'scaled.json' },
          'xyz:TSLA': {}
        } });
        assert.deepEqual(Object.keys(_covState.overrides).sort(), [
          'bitget::1000ABCUSDT', 'bitget::ABCUSDT', 'xyz:TSLA'
        ]);
        assert.equal(_covNormalizeCoin('1000ABC/USDT:USDT'), '1000ABC/USDT:USDT');
        _covState.overrides = { '1000ABC/USDT:USDT': {} };
        _covState.editCoin = '1000ABC/USDT:USDT';
        assert.equal(_covSaveConfigFile('1000ABC/USDT:USDT'), true);
        const exactFilename = _covState.overrides['1000ABC/USDT:USDT'].override_config_path;
        assert.match(exactFilename, /^1000ABC_USDT_USDT-[0-9a-f]{8}\.json$/);
        assert.doesNotMatch(exactFilename, /[/:]/);
        const moduleSource = fs.readFileSync('frontend/js/coin_overrides_editor.js', 'utf8');
        assert.doesNotMatch(moduleSource, /onclick="coinOvEdit\(\\'" \+ esc\(c\)/);
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


def test_v8_apply_filters_keeps_resolved_coins_when_some_symbols_are_unavailable() -> None:
    """PB8 filter projection gaps must not discard the valid approved and ignored selections."""
    source = (ROOT / "frontend" / "v7_backtest.html").read_text(encoding="utf-8")
    function = _extract_function(source, "cfgApplyFilters")
    script = textwrap.dedent(
        f"""
        const assert = require('node:assert/strict');
        const API_BASE = '/api/backtest-v8';
        const backtestEditorAdapter = {{
          isV8: true,
          metadataApiBase: () => '/api/v8'
        }};
        const fields = {{
          'cfg-market-cap': {{value: '1500'}},
          'cfg-vol-mcap': {{value: '10'}},
          'cfg-only-cpt': {{checked: false}},
          'cfg-notices-ignore': {{checked: false}}
        }};
        const document = {{getElementById: id => fields[id]}};
        const selected = {{}};
        const messages = [];
        function cfgGetMs(id) {{ return id === 'ms-cfg-exchanges' ? ['bybit'] : []; }}
        function cfgSetMs(id, values) {{ selected[id] = values; }}
        function bearerHeaders() {{ return {{}}; }}
        function toast(message, level) {{ messages.push({{message, level}}); }}
        function fetch(url) {{
          assert.match(url, /market_cap=1500/);
          return Promise.resolve({{
            ok: true,
            json: () => Promise.resolve({{
              approved: ['BTC', 'ETH'],
              ignored: ['DOGE'],
              unresolved: ['OPENAI', 'UNITREE']
            }})
          }});
        }}
        {function}
        (async () => {{
          await cfgApplyFilters();
          assert.deepEqual(selected['ms-cfg-app-long'], ['BTC', 'ETH']);
          assert.deepEqual(selected['ms-cfg-app-short'], ['BTC', 'ETH']);
          assert.deepEqual(selected['ms-cfg-ign-long'], ['DOGE']);
          assert.equal(messages[0].level, 'info');
          assert.match(messages[0].message, /2 unavailable PB8 symbols skipped/);
        }})().catch(error => {{ console.error(error); process.exit(1); }});
        """
    )
    completed = subprocess.run(["node", "-e", script], cwd=ROOT, text=True, capture_output=True, check=False)
    assert completed.returncode == 0, completed.stderr or completed.stdout


def test_backtest_v8_results_render_strategy_without_changing_v7_rows() -> None:
    """The shared local Results renderer adds Strategy only when V8 rows are visible."""
    source = (ROOT / "frontend" / "v7_backtest.html").read_text(encoding="utf-8")
    function = _extract_function(source, "_renderResultsTableInto")
    script = textwrap.dedent(
        f"""
        const assert = require('node:assert/strict');
        const window = {{}};
        let _activeResultsCtx = null;
        const backtestEditorAdapter = {{version: 'v8'}};
        const esc = (value) => String(value == null ? '' : value);
        const fmt = (value) => String(value == null ? '' : value);
        const fmtDate = (value) => String(value || '');
        function toggleResultAction() {{}}
        {function}
        const rth = (label, key) => '<th data-key="' + key + '">' + label + '</th>';
        const v8 = {{innerHTML: ''}};
        _renderResultsTableInto(v8, [{{
          backtest_version: 'v8', config_name: 'demo', result_name: 'run', strategy: 'ema_anchor',
          path: '/result', exchanges: ['bybit'], coins: [], modified: '2026-08-05',
          adg: 1, gain: 2, drawdown_worst: 3, sharpe_ratio: 4,
          starting_balance: 1000, final_balance: 1200, twe_long: 2, twe_short: 0,
          pos_long: 1, pos_short: 0
        }}], null, rth, {{showVersion: true, showStrategy: true}});
        assert.match(v8.innerHTML, /data-key="strategy">Strategy/);
        assert.match(v8.innerHTML, /class="mono">ema_anchor/);

        const v7 = {{innerHTML: ''}};
        _renderResultsTableInto(v7, [{{
          backtest_version: 'v7', config_name: 'legacy', result_name: 'run', path: '/legacy',
          exchanges: ['bybit'], coins: [], modified: '2026-08-05'
        }}], null, rth, {{showVersion: true, showStrategy: true}});
        assert.doesNotMatch(v7.innerHTML, /data-key="strategy"/);
        """
    )
    completed = subprocess.run(["node", "-e", script], cwd=ROOT, text=True, capture_output=True, check=False)
    assert completed.returncode == 0, completed.stderr or completed.stdout


def test_backtest_archive_enables_strategy_for_v8_rows() -> None:
    """Archive results must opt into the shared conditional Strategy column."""
    source = (ROOT / "frontend" / "v7_backtest.html").read_text(encoding="utf-8")
    function = _extract_function(source, "renderArchiveResults")

    assert "{ showVersion: true, showStrategy: true }" in function
    assert "(r.strategy || '')" in function


def test_backtest_v8_configs_render_sortable_strategy_without_changing_v7_rows() -> None:
    """The shared Configs renderer should add Strategy only for PB8."""
    source = (ROOT / "frontend" / "v7_backtest.html").read_text(encoding="utf-8")

    assert "configs: ['name', 'exchanges', 'strategy', 'coins'" in source
    assert "backtestEditorAdapter.isV8 ? thSort('Strategy', 'strategy') : ''" in source
    assert "backtestEditorAdapter.isV8 ? '<td>' + esc(c.strategy || '-') + '</td>' : ''" in source
