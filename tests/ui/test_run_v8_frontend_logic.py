"""Executable frontend contracts for PB8 Run strategy switching."""

from pathlib import Path
import json
import re
import subprocess
import textwrap


ROOT = Path(__file__).resolve().parents[2]


def _page_function(page: str, name: str) -> str:
    """Extract one top-level function declaration from the inline page script."""

    marker = f"function {name}("
    start = page.index(marker)
    candidates = [
        position
        for token in ("\nfunction ", "\nasync function ")
        if (position := page.find(token, start + len(marker))) >= 0
    ]
    end = min(candidates) if candidates else len(page)
    return page[start:end].rstrip()


def _run_node(script: str) -> None:
    """Run one isolated Node contract and surface assertion output."""

    completed = subprocess.run(["node", "-e", script], cwd=ROOT, text=True, capture_output=True, check=False)
    assert completed.returncode == 0, completed.stderr or completed.stdout


def _adapter_fields(source: str, name: str) -> dict[str, str]:
    """Return one adapter field-to-control mapping from its object literal."""

    start = source.index(f"var {name} = {{")
    end = source.index("\n    };", start)
    return dict(re.findall(r"\b([a-zA-Z0-9_]+): '([^']+)'", source[start:end]))


def test_every_runtime_control_has_forward_and_reverse_mapping() -> None:
    """Every runtime-driven control must participate in both collect and populate paths."""

    page = (ROOT / "frontend" / "v7_edit.html").read_text(encoding="utf-8")
    adapter = (ROOT / "frontend" / "js" / "run_editor_adapter.js").read_text(encoding="utf-8")
    collect = _page_function(page, "collectConfig")
    populate = _page_function(page, "populateForm")
    sections = {
        "sharedLiveFields": ("live", 57),
        "sharedLoggingFields": ("logging", 9),
        "sharedMonitorFields": ("monitor", 15),
    }

    for mapping_name, (section, expected_count) in sections.items():
        fields = _adapter_fields(adapter, mapping_name)
        assert len(fields) == expected_count
        for key, field_id in fields.items():
            assert f'id="{field_id}"' in page, f"{section}.{key} has no control"
            assert f"'{field_id}'" in collect, f"{section}.{key} is not written by collectConfig"
            if key in {"initial_entry_exec_max_market_dist_pct", "limit_order_create_max_market_dist_pct"}:
                assert "readLiveValue(live, 'initial_entry_exec_max_market_dist_pct')" in populate
                continue
            reverse = re.compile(
                rf"set(?:Val|Chk)\('{re.escape(field_id)}', [^;\n]*\b{section}\.{re.escape(key)}\b"
            )
            assert reverse.search(populate), f"{section}.{key} is not restored into {field_id}"

    assert "Object.assign({}, baseLive, extraLive" in collect
    assert "Object.assign({}, baseLogging)" in collect
    assert "Object.assign({}, baseMonitor)" in collect
    assert "JSON.parse(el.value)" in collect
    assert "_extraLiveKeys = Object.keys(liveCfg)" in populate
    form_controls = set(re.findall(r'\bid="(f-[^"]+)"', page))
    value_controls = {field_id for field_id in form_controls if not field_id.endswith("-status")}
    for field_id in value_controls:
        assert f"'{field_id}'" in collect or f"'{field_id}'" in populate, f"{field_id} has no config mapping"


def test_zero_values_are_not_replaced_by_editor_defaults() -> None:
    """Valid numeric zeroes must survive the Config-to-Form path unchanged."""

    page = (ROOT / "frontend" / "v7_edit.html").read_text(encoding="utf-8")
    populate = _page_function(page, "populateForm")
    zero_sensitive = {
        "f-leverage": "live.leverage",
        "f-pnls-lookback": "live.pnls_max_lookback_days",
        "f-max-cancel": "live.max_n_cancellations_per_batch",
        "f-max-create": "live.max_n_creations_per_batch",
        "f-max-restarts": "live.max_n_restarts_per_day",
        "f-max-disk-candles": "live.max_disk_candles_per_symbol_per_tf",
        "f-max-mem-candles": "live.max_memory_candles_per_symbol",
        "f-vol-refresh": "logging.volume_refresh_info_threshold_seconds",
        "f-max-ohlcv-fetches": "live.max_ohlcv_fetches_per_minute",
    }
    for field_id, source in zero_sensitive.items():
        assert f"setVal('{field_id}', {source} != null ? {source}" in populate


def test_pb8_template_hsl_coin_mode_has_a_select_option() -> None:
    """The PB8 template's coin HSL mode must survive Config-to-Form collection."""

    page = (ROOT / "frontend" / "v7_edit.html").read_text(encoding="utf-8")

    assert '<option value="coin" data-v8-only>coin</option>' in page
    assert "setVal('f-hsl-signal-mode', live.hsl_signal_mode || 'unified')" in page
    assert "hsl_signal_mode: getVal('f-hsl-signal-mode')" in page


def test_pb8_run_editor_exposes_cookie_authenticated_strategy_handoff() -> None:
    """PB8 Run must expose Strategy Explorer and include sparse override files in its draft."""
    page = (ROOT / "frontend" / "v7_edit.html").read_text(encoding="utf-8")
    adapter = (ROOT / "frontend" / "js" / "run_editor_adapter.js").read_text(encoding="utf-8")
    handoff = _page_function(page, "goStrategyExplorer")

    assert "supportsStrategyExplorer: true" in adapter
    assert 'data-v7-only onclick="goStrategyExplorer()"' not in page
    assert '/app/js/run_editor_adapter.js?v=7' in page
    assert "'/api/strategy-explorer-v8'" in page
    assert "coinOvSnapshotAllFiles()" in handoff
    assert "override_configs: overrideConfigs || {}" in handoff
    assert "credentials: 'same-origin'" in handoff
    assert "Authorization" not in handoff


def test_pb8_update_warning_only_uses_runtime_not_ready_hosts() -> None:
    """Only backend-confirmed PB8 runtime blockers should show the update prompt."""

    page = (ROOT / "frontend" / "v7_run.html").read_text(encoding="utf-8")
    render_warning = _page_function(page, "renderPb8UpdateWarning")
    script = textwrap.dedent(
        f"""
        const assert = require('node:assert/strict');
        const nodes = {{
          'pb8-update-warning': {{hidden: true}},
          'pb8-update-warning-hosts': {{textContent: ''}}
        }};
        const document = {{getElementById: (id) => nodes[id] || null}};
        let runListAdapter = {{isV8: true}};
        {render_warning}

        renderPb8UpdateWarning([{{status: 'blocked', blocked_on: ['cluster-host']}}]);
        assert.equal(nodes['pb8-update-warning'].hidden, true);
        assert.equal(nodes['pb8-update-warning-hosts'].textContent, '');

        renderPb8UpdateWarning([{{pb8_update_required_on: ['vps-b', 'vps-a', 'vps-a']}}]);
        assert.equal(nodes['pb8-update-warning'].hidden, false);
        assert.equal(nodes['pb8-update-warning-hosts'].textContent, 'The validated PB8 runtime is not ready on vps-a, vps-b.');

        runListAdapter = {{isV8: false}};
        renderPb8UpdateWarning([{{pb8_update_required_on: ['vps-a']}}]);
        assert.equal(nodes['pb8-update-warning'].hidden, true);
        """
    )
    _run_node(script)


def test_run_list_adds_strategy_only_for_pb8() -> None:
    """The shared Run list should add one escaped Strategy cell only for PB8."""
    page = (ROOT / "frontend" / "v7_run.html").read_text(encoding="utf-8")
    build_cells = _page_function(page, "buildCells")
    assert "if (runListAdapter.isV8) COLS.splice(2, 0, { key: 'strategy', label: 'Strategy' });" in page
    script = textwrap.dedent(
        f"""
        const assert = require('node:assert/strict');
        const STATUS_LABELS = {{disabled: 'disabled'}};
        const esc = (value) => String(value == null ? '' : value);
        let runListAdapter = {{isV8: true, supportsForcedModes: false, supportsConversion: false}};
        {build_cells}
        const row = {{name: 'demo', user: 'alice', strategy: 'ema_anchor', status: 'disabled'}};
        const v8 = buildCells(row);
        assert.equal(v8.length, 12);
        assert.equal(v8[2], 'ema_anchor');

        runListAdapter = {{isV8: false, supportsForcedModes: false, supportsConversion: false}};
        const v7 = buildCells(row);
        assert.equal(v7.length, 11);
        assert.notEqual(v7[2], 'ema_anchor');
        """
    )
    _run_node(script)


def test_run_editor_preserves_configured_target_when_capability_is_unconfirmed() -> None:
    """A stale capability response must not make a configured target look disabled."""

    page = (ROOT / "frontend" / "v7_edit.html").read_text(encoding="utf-8")
    refresh_hosts = "async " + _page_function(page, "refreshHostCapabilities")
    script = textwrap.dedent(
        f"""
        const assert = require('node:assert/strict');
        let allHosts = ['disabled', 'manibot01'];
        let hostCapabilitiesByName = {{}};
        const select = {{options: [], value: 'manibot62'}};
        const document = {{getElementById: () => select}};
        const runEditorAdapter = {{capabilityKey: 'pb7_capable', label: 'PB7'}};
        function getVal() {{ return select.value; }}
        async function requestHostCapabilities() {{
          return {{
            hosts: ['disabled', 'manibot01'],
            host_capabilities: {{manibot62: {{pb7_capable: false}}}}
          }};
        }}
        function populateHosts() {{
          select.options = allHosts.map(value => ({{value}}));
        }}
        {refresh_hosts}

        (async () => {{
          await refreshHostCapabilities();
          assert.deepEqual(allHosts, ['disabled', 'manibot01', 'manibot62']);
          assert.equal(select.value, 'manibot62');
        }})().catch(error => {{ console.error(error); process.exit(1); }});
        """
    )
    _run_node(script)


def test_run_editor_preserves_host_selected_during_capability_refresh() -> None:
    """A late capability response must not revert a host selected while it was loading."""

    page = (ROOT / "frontend" / "v7_edit.html").read_text(encoding="utf-8")
    refresh_hosts = "async " + _page_function(page, "refreshHostCapabilities")
    script = textwrap.dedent(
        f"""
        const assert = require('node:assert/strict');
        let allHosts = ['disabled'];
        let hostCapabilitiesByName = {{}};
        const select = {{options: [{{value: 'disabled'}}], value: 'disabled'}};
        const document = {{getElementById: () => select}};
        let resolveRequest;
        function getVal() {{ return select.value; }}
        function requestHostCapabilities() {{
          return new Promise(resolve => {{ resolveRequest = resolve; }});
        }}
        function populateHosts() {{
          select.options = allHosts.map(value => ({{value}}));
          select.value = allHosts[0];
        }}
        {refresh_hosts}

        (async () => {{
          const pending = refreshHostCapabilities();
          select.value = 'manibot51';
          resolveRequest({{
            hosts: ['disabled', 'manibot51'],
            host_capabilities: {{manibot51: {{pb7_capable: true}}}}
          }});
          await pending;
          assert.equal(select.value, 'manibot51');
        }})().catch(error => {{ console.error(error); process.exit(1); }});
        """
    )
    _run_node(script)


def test_v7_forced_mode_aliases_select_the_visible_editor_options() -> None:
    """Canonical PB7 forced modes must map to the editor's short option values."""

    page = (ROOT / "frontend" / "v7_edit.html").read_text(encoding="utf-8")
    normalize_mode = _page_function(page, "forcedModeSelectValue")
    script = textwrap.dedent(
        f"""
        const assert = require('node:assert/strict');
        let runEditorAdapter = {{isV8: false}};
        {normalize_mode}

        assert.equal(forcedModeSelectValue('graceful_stop'), 'gs');
        assert.equal(forcedModeSelectValue('tp_only'), 't');
        assert.equal(forcedModeSelectValue('panic'), 'p');
        assert.equal(forcedModeSelectValue('manual'), 'm');
        assert.equal(forcedModeSelectValue('normal'), 'n');
        assert.equal(forcedModeSelectValue('gs'), 'gs');
        assert.equal(forcedModeSelectValue(''), '');
        runEditorAdapter = {{isV8: true}};
        assert.equal(forcedModeSelectValue('graceful_stop'), 'graceful_stop');
        """
    )
    _run_node(script)

    populate = _page_function(page, "populateForm")
    assert "forcedModeSelectValue(live.forced_mode_long)" in populate
    assert "forcedModeSelectValue(live.forced_mode_short)" in populate


def test_log_panel_waits_for_remote_assignment_before_opening() -> None:
    """PB8 log opening must wait until enabled_on is populated from the config."""

    page = (ROOT / "frontend" / "v7_edit.html").read_text(encoding="utf-8")
    open_log_panel = _page_function(page, "openLogPanel")

    assert "async function openLogPanel()" in page
    assert "await _editorInitPromise" in open_log_panel
    assert "if (_logPanelOpen) return" in open_log_panel
    assert "_editorInitPromise = init();" in page


def test_import_user_field_is_searchable_and_rejects_unknown_users() -> None:
    """PB7/PB8 config import uses searchable suggestions without accepting arbitrary users."""
    page = (ROOT / "frontend" / "v7_edit.html").read_text(encoding="utf-8")
    import_fn = _page_function(page, "doImport")

    assert 'id="import-user" type="text"' in page
    assert 'placeholder="Type to search users..."' in page
    assert 'id="import-user-options" role="listbox" hidden' in page
    assert 'id="import-user-toggle" type="button"' in page
    assert 'aria-label="Show configured users"' in page
    assert "function renderImportUserOptions(showAll)" in page
    assert "max-height:220px" in page
    assert "String(user.name || '').toLowerCase().indexOf(query) !== -1" in page
    assert "moveImportUserSelection(event.key === 'ArrowDown' ? 1 : -1)" in page
    assert "toggle.addEventListener('click'" in page
    assert "var query = showAll ? ''" in page
    assert "renderImportUserOptions(true)" in page
    assert "input.addEventListener('input', function() { renderImportUserOptions(false); })" in page
    assert "input.addEventListener('input', renderImportUserOptions)" not in page
    assert "input.addEventListener('focus', renderImportUserOptions)" not in page
    assert "options.replaceChildren()" in page
    assert "String(requestedUser || '').trim().toLowerCase()" in import_fn
    assert "Select a configured user from the search suggestions." in import_fn


def test_collect_config_writes_every_managed_field_and_preserves_unknown_json() -> None:
    """Form-to-Config collection must overwrite all managed fields without dropping unknown JSON."""

    page = (ROOT / "frontend" / "v7_edit.html").read_text(encoding="utf-8")
    adapter = (ROOT / "frontend" / "js" / "run_editor_adapter.js").read_text(encoding="utf-8")
    collect = _page_function(page, "collectConfig")
    live_fields = _adapter_fields(adapter, "sharedLiveFields")
    logging_fields = _adapter_fields(adapter, "sharedLoggingFields")
    monitor_fields = _adapter_fields(adapter, "sharedMonitorFields")
    managed_live = sorted(set(live_fields) - {"initial_entry_exec_max_market_dist_pct"})
    values = {field_id: "12.5" for field_id in set(live_fields.values()) | set(logging_fields.values()) | set(monitor_fields.values())}
    values.update({
        "f-user": "alice",
        "f-strategy-kind": "beta",
        "f-enabled-on": "disabled",
        "f-margin-mode": "cross",
        "f-forced-long": "n",
        "f-forced-short": "gs",
        "f-hsl-signal-mode": "coin",
        "f-hsl-cooldown-policy": "panic",
        "f-time-in-force": "good_till_cancelled",
        "f-market-snapshot-strategy": "auto",
        "f-custom-endpoints-path": "",
        "f-startup-phase-budgets": '{"startup": 12}',
        "f-log-dir": "logs",
        "f-log-debug-profiles": '["fills"]',
        "f-monitor-root-dir": "monitor",
        "f-note": "roundtrip",
        "f-market-cap": "123",
        "f-vol-mcap": "4.5",
        "f-version": "7",
        "f-long-twe": "1.5",
        "f-long-npos": "5",
        "f-short-twe": "0",
        "f-short-npos": "1",
    })
    raw = {
        "live": {**{key: "stale" for key in managed_live}, "unknown_live": {"keep": True}},
        "logging": {**{key: "stale" for key in logging_fields}, "unknown_logging": 3},
        "monitor": {**{key: "stale" for key in monitor_fields}, "unknown_monitor": 4},
        "pbgui": {"unknown_pbgui": 5, "starting_config": True},
        "bot": {},
        "backtest": {"keep": 6},
        "optimize": {"keep": 7},
        "unknown_top": {"keep": 8},
    }
    script = textwrap.dedent(
        f"""
        const assert = require('node:assert/strict');
        const values = {json.dumps(values)};
        const raw = {json.dumps(raw)};
        const nodes = {{
          'cfg-raw-json': {{value: JSON.stringify(raw)}},
          'f-long-json': {{value: JSON.stringify({{risk: {{n_positions: 1}}, strategy: {{beta: {{entry: 1}}}}}})}},
          'f-short-json': {{value: JSON.stringify({{risk: {{n_positions: 1}}, strategy: {{beta: {{entry: 2}}}}}})}}
        }};
        const extras = {{
          future_flag: {{checked: false, getAttribute: () => 'boolean'}},
          future_obj: {{value: '{{"nested": 9}}', getAttribute: () => 'json'}}
        }};
        const document = {{getElementById: (id) => nodes[id] || null}};
        function getVal(id) {{ return values[id] == null ? '' : String(values[id]); }}
        function getNum(id) {{ return parseFloat(getVal(id)) || 0; }}
        function getInt(id) {{ return parseInt(getVal(id), 10) || 0; }}
        function getOptionalNum(id) {{ const value = getVal(id).trim(); return value === '' ? null : Number(value); }}
        function getChk() {{ return true; }}
        function getExtraLiveElement(key) {{ return extras[key]; }}
        function getMultiselectValues(id) {{
          if (id === 'ms-approved-long') return ['BTC'];
          if (id === 'ms-approved-short') return ['ETH'];
          if (id === 'ms-ignored-long') return ['XRP'];
          if (id === 'ms-ignored-short') return ['DOGE'];
          if (id === 'ms-tags') return ['layer-1'];
          return [];
        }}
        function collectApprovedCoinsValue(longValues, shortValues) {{ return {{long: longValues, short: shortValues}}; }}
        function coinOvCollect() {{ return {{coin_overrides: {{BTC: {{override_config_path: 'BTC.json'}}}}}}; }}
        const runEditorAdapter = {{
          isV8: true,
          managedLoggingKeys: {json.dumps(sorted(logging_fields))},
          managedMonitorKeys: {json.dumps(sorted(monitor_fields))},
          managedLiveValue: (key, source) => source[key === 'limit_order_create_max_market_dist_pct' ? 'initial_entry_exec_max_market_dist_pct' : key],
          setBotValue: (side, key, value) => {{ side.risk = side.risk || {{}}; side.risk[key] = value; }}
        }};
        const _managedLiveKeys = new Set({json.dumps(managed_live)});
        const _extraLiveKeys = ['future_flag', 'future_obj'];
        const cfg = {{pbgui: {{starting_config: true}}}};
        const _fromBacktestConfig = '';
        {collect}

        const result = collectConfig();
        for (const key of {json.dumps(managed_live)}) {{
          assert.notEqual(result.live[key], 'stale', 'live.' + key + ' was not collected');
          assert.notEqual(result.live[key], undefined, 'live.' + key + ' is missing');
        }}
        for (const key of {json.dumps(sorted(logging_fields))}) assert.notEqual(result.logging[key], 'stale');
        for (const key of {json.dumps(sorted(monitor_fields))}) assert.notEqual(result.monitor[key], 'stale');
        assert.deepEqual(result.live.unknown_live, {{keep: true}});
        assert.equal(result.live.future_flag, false);
        assert.deepEqual(result.live.future_obj, {{nested: 9}});
        assert.equal(result.logging.unknown_logging, 3);
        assert.equal(result.monitor.unknown_monitor, 4);
        assert.deepEqual(result.unknown_top, {{keep: 8}});
        assert.deepEqual(result.backtest, {{keep: 6}});
        assert.deepEqual(result.optimize, {{keep: 7}});
        assert.deepEqual(result.bot.long.strategy, {{beta: {{entry: 1}}}});
        assert.equal(result.bot.long.risk.total_wallet_exposure_limit, 1.5);
        assert.equal(result.bot.long.risk.n_positions, 5);
        assert.equal(result.bot.short.risk.total_wallet_exposure_limit, 0);
        assert.equal(result.bot.short.risk.n_positions, 1);
        assert.equal(result.live.strategy_kind, 'beta');
        assert.equal(result.live.user, 'alice');
        assert.deepEqual(result.live.approved_coins, {{long: ['BTC'], short: ['ETH']}});
        assert.deepEqual(result.live.ignored_coins, {{long: ['XRP'], short: ['DOGE']}});
        assert.equal(result.pbgui.version, 7);
        assert.equal(result.pbgui.enabled_on, 'disabled');
        assert.equal(result.pbgui.note, 'roundtrip');
        assert.equal(result.pbgui.market_cap, 123);
        assert.equal(result.pbgui.vol_mcap, 4.5);
        assert.deepEqual(result.pbgui.tags, ['layer-1']);
        assert.equal(result.pbgui.only_cpt, true);
        assert.equal(result.pbgui.notices_ignore, true);
        assert.equal(result.pbgui.unknown_pbgui, 5);
        assert.deepEqual(result.coin_overrides, {{BTC: {{override_config_path: 'BTC.json'}}}});
        assert.doesNotThrow(() => JSON.stringify(result));
        """
    )
    _run_node(script)


def test_populate_form_restores_every_runtime_field_into_its_control() -> None:
    """Config-to-Form population must restore every runtime value into the mapped control."""

    page = (ROOT / "frontend" / "v7_edit.html").read_text(encoding="utf-8")
    adapter = (ROOT / "frontend" / "js" / "run_editor_adapter.js").read_text(encoding="utf-8")
    live_fields = _adapter_fields(adapter, "sharedLiveFields")
    logging_fields = _adapter_fields(adapter, "sharedLoggingFields")
    monitor_fields = _adapter_fields(adapter, "sharedMonitorFields")
    checkbox_ids = set(re.findall(r'<input type="checkbox" id="([^"]+)"', page))

    def section_values(fields: dict[str, str], section: str) -> dict[str, object]:
        values: dict[str, object] = {}
        for index, (key, field_id) in enumerate(fields.items(), start=1):
            if key == "initial_entry_exec_max_market_dist_pct":
                continue
            if key == "startup_phase_budgets":
                values[key] = {"phase": index}
            elif key == "live_event_debug_profiles":
                values[key] = ["fills", "orders"]
            elif field_id in checkbox_ids:
                values[key] = index % 2 == 0
            elif field_id in {"f-margin-mode", "f-forced-long", "f-forced-short", "f-hsl-signal-mode", "f-hsl-cooldown-policy", "f-time-in-force", "f-market-snapshot-strategy"}:
                values[key] = f"{section}-{key}"
            elif field_id in {"f-custom-endpoints-path", "f-log-dir", "f-monitor-root-dir"}:
                values[key] = f"/{section}/{key}"
            else:
                values[key] = index + 0.25
        return values

    live = section_values(live_fields, "live")
    logging = section_values(logging_fields, "logging")
    monitor = section_values(monitor_fields, "monitor")
    live.update({
        "user": "alice",
        "strategy_kind": "alpha",
        "approved_coins": {"long": ["BTC"], "short": ["ETH"]},
        "ignored_coins": {"long": [], "short": []},
    })
    config = {
        "live": live,
        "logging": logging,
        "monitor": monitor,
        "pbgui": {
            "enabled_on": "disabled", "version": 7, "note": "reverse",
            "market_cap": 321, "vol_mcap": 6.5, "only_cpt": True,
            "notices_ignore": False, "tags": ["layer-1"],
        },
        "bot": {
            "long": {"risk": {"total_wallet_exposure_limit": 1.5, "n_positions": 5}, "strategy": {"alpha": {"entry": 1}}},
            "short": {"risk": {"total_wallet_exposure_limit": 0.5, "n_positions": 2}, "strategy": {"alpha": {"entry": 2}}},
        },
    }
    helper_functions = "\n".join(
        _page_function(page, name)
        for name in (
            "cloneRunConfigValue",
            "getRunStrategyDefault",
            "cacheRunStrategyBlocks",
            "selectRunStrategyConfig",
            "refreshBotParamStatusLabels",
            "forcedModeSelectValue",
            "populateForm",
        )
    )
    script = textwrap.dedent(
        f"""
        const assert = require('node:assert/strict');
        let cfg = {json.dumps(config)};
        const editorMetadata = {{strategies: ['alpha'], strategy_defaults: {{long: {{}}, short: {{}}}}}};
        const runEditorAdapter = {{
          isV8: true,
          readLiveValue: (source, key) => key === 'initial_entry_exec_max_market_dist_pct' ? source.limit_order_create_max_market_dist_pct : source[key],
          getBotValue: (side, key, fallback) => side.risk && side.risk[key] != null ? side.risk[key] : fallback
        }};
        const restored = {{}};
        const checked = {{}};
        const nodes = {{
          'f-long-json': {{value: ''}}, 'f-short-json': {{value: ''}},
          'lbl-long-json': {{innerHTML: ''}}, 'lbl-short-json': {{innerHTML: ''}},
          'extra-params-container': {{textContent: ''}},
          'cfg-raw-json': {{value: ''}}
        }};
        const document = {{getElementById: (id) => nodes[id] || null}};
        const window = {{PBGuiEditorShared: {{clearFixedValidationStatus: () => {{}}}}}};
        let paramStatus = {{long: {{}}, short: {{}}}};
        let _runStrategyBotCache = {{long: {{}}, short: {{}}}};
        let _activeRunStrategyKind = '';
        let _extraLiveKeys = [];
        let _extraLiveFieldIds = {{}};
        let _rawEditorLastApplied = '';
        let allHosts = ['disabled'];
        const KNOWN_LIVE_PARAMS = new Set({json.dumps(sorted(live))});
        function setVal(id, value) {{ restored[id] = value; if (nodes[id]) nodes[id].value = value; }}
        function setChk(id, value) {{ checked[id] = !!value; }}
        function ensureSelectOption() {{}}
        function populateHosts() {{}}
        function syncExecutionSyncFieldBounds() {{}}
        function updateEnabledOnAvailability() {{}}
        function coinOvLoad() {{}}
        function _applyBotJsonHighlight() {{}}
        function setRawJsonValidationError() {{}}
        function bindStructuredJsonFieldValidation() {{}}
        function updateDynamicIgnorePreview() {{}}
        function autoResizeTa() {{}}
        {helper_functions}

        populateForm();
        const liveMap = {json.dumps(live_fields)};
        const loggingMap = {json.dumps(logging_fields)};
        const monitorMap = {json.dumps(monitor_fields)};
        const checkboxIds = new Set({json.dumps(sorted(checkbox_ids))});
        function expected(section, key) {{
          if (key === 'initial_entry_exec_max_market_dist_pct') return cfg.live.limit_order_create_max_market_dist_pct;
          return cfg[section][key];
        }}
        for (const [section, mapping] of [['live', liveMap], ['logging', loggingMap], ['monitor', monitorMap]]) {{
          for (const [key, id] of Object.entries(mapping)) {{
            const wanted = expected(section, key);
            if (checkboxIds.has(id)) assert.equal(checked[id], wanted, section + '.' + key);
            else if (key === 'startup_phase_budgets' || key === 'live_event_debug_profiles') assert.deepEqual(JSON.parse(restored[id]), wanted, section + '.' + key);
            else assert.equal(restored[id], wanted, section + '.' + key);
          }}
        }}
        assert.equal(restored['f-user'], 'alice');
        assert.equal(restored['f-strategy-kind'], 'alpha');
        assert.equal(restored['f-enabled-on'], 'disabled');
        assert.equal(restored['f-version'], 7);
        assert.equal(restored['f-note'], 'reverse');
        assert.equal(restored['f-market-cap'], 321);
        assert.equal(restored['f-vol-mcap'], 6.5);
        assert.equal(checked['f-only-cpt'], true);
        assert.equal(checked['f-notices-ignore'], false);
        assert.deepEqual(JSON.parse(nodes['f-long-json'].value).strategy, {{alpha: {{entry: 1}}}});
        assert.equal(JSON.parse(nodes['cfg-raw-json'].value).live.strategy_kind, 'alpha');
        """
    )
    _run_node(script)


def test_run_strategy_switch_replaces_key_caches_edits_and_marks_runtime_defaults() -> None:
    """Changing strategy_kind must replace both side keys and highlight defaults until edited."""

    page = (ROOT / "frontend" / "v7_edit.html").read_text(encoding="utf-8")
    functions = "\n".join(
        _page_function(page, name)
        for name in (
            "cloneRunConfigValue",
            "getRunStrategyDefault",
            "cacheRunStrategyBlocks",
            "selectRunStrategyConfig",
            "refreshBotParamStatusLabels",
            "changeRunStrategyKind",
            "syncRunStrategyKindFromSideConfig",
            "clearEditedRunStrategyDefaultMark",
        )
    )
    script = textwrap.dedent(
        f"""
        const assert = require('node:assert/strict');
        const nodes = {{
          'f-strategy-kind': {{value: 'beta'}},
          'f-long-json': {{value: JSON.stringify({{risk: {{n_positions: 3}}, strategy: {{alpha: {{custom: 11}}}}}}), scrollTop: 4}},
          'f-short-json': {{value: JSON.stringify({{risk: {{n_positions: 2}}, strategy: {{alpha: {{custom: 12}}}}}}), scrollTop: 4}},
          'lbl-long-json': {{innerHTML: ''}},
          'lbl-short-json': {{innerHTML: ''}}
        }};
        const document = {{getElementById: (id) => nodes[id] || null}};
        const editorMetadata = {{
          strategies: ['alpha', 'beta'],
          strategy_defaults: {{
            long: {{beta: {{custom: 21}}}},
            short: {{beta: {{custom: 22}}}}
          }}
        }};
        const runEditorAdapter = {{isV8: true}};
        let cfg = {{live: {{strategy_kind: 'alpha'}}, bot: {{}}}};
        let paramStatus = {{long: {{}}, short: {{}}}};
        let _runStrategyBotCache = {{long: {{}}, short: {{}}}};
        let _activeRunStrategyKind = 'alpha';
        let scheduled = 0;
        function setVal(id, value) {{ nodes[id].value = value; }}
        function toast(message) {{ throw new Error(message); }}
        function autoResizeTa() {{}}
        function validateJsonFieldTextarea() {{}}
        function _applyBotJsonHighlight() {{}}
        function scheduleStructuredEditorSync() {{ scheduled += 1; }}
        {functions}

        changeRunStrategyKind('beta');
        let longConfig = JSON.parse(nodes['f-long-json'].value);
        let shortConfig = JSON.parse(nodes['f-short-json'].value);
        assert.equal(cfg.live.strategy_kind, 'beta');
        assert.deepEqual(longConfig.strategy, {{beta: {{custom: 21}}}});
        assert.deepEqual(shortConfig.strategy, {{beta: {{custom: 22}}}});
        assert.deepEqual(longConfig.risk, {{n_positions: 3}});
        assert.equal(paramStatus.long.beta, 'pb_default');
        assert.equal(paramStatus.short.beta, 'pb_default');
        assert.match(nodes['lbl-long-json'].innerHTML, /review/);
        assert.equal(scheduled, 1);

        longConfig.strategy.beta.custom = 99;
        nodes['f-long-json'].value = JSON.stringify(longConfig);
        clearEditedRunStrategyDefaultMark('long', longConfig);
        assert.equal(paramStatus.long.beta, undefined);
        changeRunStrategyKind('alpha');
        assert.deepEqual(JSON.parse(nodes['f-long-json'].value).strategy, {{alpha: {{custom: 11}}}});
        changeRunStrategyKind('beta');
        assert.deepEqual(JSON.parse(nodes['f-long-json'].value).strategy, {{beta: {{custom: 99}}}});
        assert.equal(paramStatus.long.beta, undefined);
        assert.equal(paramStatus.short.beta, 'pb_default');

        const manualLong = JSON.parse(nodes['f-long-json'].value);
        manualLong.strategy = {{alpha: {{custom: 77}}}};
        nodes['f-long-json'].value = JSON.stringify(manualLong);
        syncRunStrategyKindFromSideConfig(manualLong);
        assert.equal(nodes['f-strategy-kind'].value, 'alpha');
        assert.equal(cfg.live.strategy_kind, 'alpha');
        assert.deepEqual(JSON.parse(nodes['f-long-json'].value).strategy, {{alpha: {{custom: 77}}}});
        assert.deepEqual(JSON.parse(nodes['f-short-json'].value).strategy, {{alpha: {{custom: 12}}}});
        assert.equal(scheduled, 4);
        """
    )
    _run_node(script)
