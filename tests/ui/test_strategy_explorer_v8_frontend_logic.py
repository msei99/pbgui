"""Static and executable frontend contracts for the shared PB7/PB8 Strategy Explorer."""

from javascript_checks import run_javascript

from pathlib import Path
import re
import textwrap


ROOT = Path(__file__).resolve().parents[2]
PAGE_PATH = ROOT / "frontend" / "v7_strategy_explorer.html"


def _extract_function(source: str, name: str) -> str:
    """Extract one complete named JavaScript function declaration."""
    start = source.index(f"function {name}(")
    brace_start = source.index("{", start)
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
    raise AssertionError(f"Could not extract JavaScript function {name!r}")


def test_shared_page_detects_v8_and_switches_identity_without_a_second_template() -> None:
    """The PB8 route must reuse the PB7 shell while selecting PB8 title and navigation identity."""
    source = PAGE_PATH.read_text(encoding="utf-8")

    assert not (ROOT / "frontend" / "v8_strategy_explorer.html").exists()
    assert "var IS_V8 = /\\/api\\/strategy-explorer-v8" in source
    assert "PBGui - PB8 Strategy Explorer" in source
    assert "STRATEGY_LABEL + ' Strategy Explorer'" in source
    assert "subtitle: IS_V8 ? 'PBv8 Strategy Explorer'" in source
    assert "current: IS_V8 ? 'v8_strategy_explorer' : 'v7_strategy_explorer'" in source
    assert "page.strategy_label || 'PB8'" in source


def test_dynamic_metadata_reads_and_writes_nested_canonical_paths() -> None:
    """PB8 metadata bounds and dotted side paths must target canonical bot side objects."""
    source = PAGE_PATH.read_text(encoding="utf-8")
    functions = "\n".join(
        _extract_function(source, name)
        for name in (
            "deepGet",
            "deepEnsure",
            "paramBounds",
            "paramMeta",
            "paramFieldPath",
            "paramValue",
            "invalidateConfigRequests",
            "setParamValue",
            "paramNearBound",
        )
    )
    script = textwrap.dedent(
        f"""
        const assert = browserAssert;
        let PARAM_FIELD_META = {{
          'entry_distance': {{type: 'number', path: 'bot.long.strategy.ema.entry_distance', min: -2, max: 3, step: 0.25}}
        }};
        const IS_V8 = true;
        let snapshotRequestGeneration = 0;
        let compareRequestGeneration = 0;
        let simulationRequestGeneration = 0;
        let movieRequestGeneration = 0;
        let marketRequestGeneration = 0;
        let configRevision = 0;
        function stopSimulationProgressPolling() {{}}
        function stopCompareProgressPolling() {{}}
        function stopMovieProgressPolling() {{}}
        let state = {{config: {{
          bot: {{long: {{strategy: {{ema: {{entry_distance: 1.5}}}}}}}},
          optimize: {{bounds: {{long: {{strategy: {{ema: {{entry_distance: [-2, 3, 0.25]}}}}}}}}}},
          live: {{mode: 'normal'}}
        }}}};
        {functions}
        assert.deepEqual(paramBounds('entry_distance', 1.5), {{min: -2, max: 3, step: 0.25}});
        const sideParams = state.config.bot.long;
        assert.equal(paramValue(sideParams, 'entry_distance', 'long'), 1.5);
        setParamValue('long', 'entry_distance', 2.25);
        assert.equal(state.config.bot.long.strategy.ema.entry_distance, 2.25);
        setParamValue('short', 'risk.n_positions', 4);
        assert.equal(state.config.bot.short.risk.n_positions, 4);
        setParamValue('long', 'live.mode', 'graceful');
        assert.equal(state.config.live.mode, 'graceful');
        assert.equal(paramNearBound('long', 'entry_distance', 2.9), 'upper');
        assert.equal(paramNearBound('long', 'entry_distance', -1.9), 'lower');
        assert.equal(paramNearBound('long', 'entry_distance', 0), '');
        """
    )
    completed = run_javascript(["-e", script], cwd=ROOT, text=True, capture_output=True, check=False)
    assert completed.returncode == 0, completed.stderr or completed.stdout


def test_snapshot_adopts_dynamic_groups_metadata_and_pb8_visual_aliases() -> None:
    """Rendering must use backend PB8 field metadata while isolating optional visual aliases."""
    source = PAGE_PATH.read_text(encoding="utf-8")

    assert "normalizeParamGroups(snapshot.param_groups)" in source
    assert "IS_V8 && snapshot.param_field_meta" in source
    assert "IS_V8 && dynamicGroups ? dynamicGroups : DEFAULT_SEGMENTS" in source
    assert ": DEFAULT_PARAM_FIELD_META" in source
    assert "side.visual_params || side.params || {}" in source
    assert "['bot', sideKey].concat(path.slice(0, -1))" in source
    assert "paramMeta(name).path || name" in source
    assert "meta.type === 'bool' || meta.type === 'boolean'" in source
    assert "meta.type === 'string' || meta.type === 'text'" in source
    assert "function paramNearBound(sideKey, name, value)" in source
    assert "['optimize', 'bounds', sideKey].concat(field.path)" in source
    assert "Near lower bound" in source
    assert "Near upper bound" in source


def test_v8_fetches_use_only_same_origin_cookie_authentication() -> None:
    """Strategy Explorer API and movie export requests must never expose bearer tokens."""
    source = PAGE_PATH.read_text(encoding="utf-8")

    assert "%%TOKEN%%" not in source
    assert "Authorization" not in source
    assert "Bearer" not in source
    assert "opts.credentials = 'same-origin'" in source
    assert "credentials: 'same-origin'" in source


def test_v8_replay_compare_and_provenance_labels_do_not_claim_pb7() -> None:
    """PB8 mode must relabel the fixed shell and keep server result paths out of visible fields."""
    source = PAGE_PATH.read_text(encoding="utf-8")

    assert '<option value="pb8_engine">PB8 Native Replay</option>' in source
    assert "data.engine === 'pb8_engine'" in source
    assert "Stored PB8 Result vs Fresh PB8 Replay" in source
    assert "Stored PB8 Result only" in source
    assert "Fresh PB8 Replay only" in source
    assert "Loaded from selected PB8 result" in source
    assert "Current PB8 Config vs Pinned PB8 Baseline" in source
    assert "Current PB8 Config only" in source
    assert "Pinned PB8 Baseline only" in source
    assert "opts.pb7_backtest_dir = IS_V8 ? ''" in source
    assert "draft_id: IS_V8 ? (DRAFT_ID || '') : ''" in source
    assert "if (!IS_V8) qs += '&result_path='" in source


def test_v8_handoffs_keep_result_paths_in_post_bodies_only() -> None:
    """PB8 backtest and Pareto handoffs must navigate with only an opaque draft id."""
    backtest = (ROOT / "frontend" / "v7_backtest.html").read_text(encoding="utf-8")
    pareto = (ROOT / "frontend" / "v7_pareto_explorer.html").read_text(encoding="utf-8")
    result_handoff = _extract_function(backtest, "strategyExplorerFromResult")
    pareto_handoff = _extract_function(pareto, "openSelectedStrategyExplorer")

    assert "result_path: path" in result_handoff
    assert "provenance: { kind: 'backtest_result' }" in result_handoff
    assert "if (!selectedIsV8) url += '&result_path='" in result_handoff
    assert "credentials: 'same-origin'" in result_handoff
    assert "kind: 'optimize_result'" in pareto_handoff
    assert "state.optimizeVersion === 'v8'" in pareto
    assert "'/main_page?draft_id='" in pareto_handoff
    assert "&result_path=" not in pareto_handoff
    assert 'id="btn-open-strategy-explorer"' in pareto
    assert 'id="btn-pin-strategy-baseline"' in pareto
    assert "compare_config: compareBaseline ? compareBaseline.config" in pareto_handoff
    assert "function pinSelectedStrategyBaseline()" in pareto


def test_session_and_snapshot_requests_have_stale_response_generations() -> None:
    """Late session and snapshot responses must not replace newer editor state."""
    source = PAGE_PATH.read_text(encoding="utf-8")

    assert "var sessionRequestGeneration = 0;" in source
    assert "var snapshotRequestGeneration = 0;" in source
    assert "var marketRequestGeneration = 0;" in source
    assert "var compareRequestGeneration = 0;" in source
    assert "var simulationRequestGeneration = 0;" in source
    assert "var movieRequestGeneration = 0;" in source
    assert "function invalidateConfigRequests()" in source
    assert "invalidateConfigRequests();\n    rawConfigDirty = true;" in source
    assert "target[path[path.length - 1]] = value;\n  invalidateConfigRequests();" in source
    assert "generation !== sessionRequestGeneration" in source
    assert "generation !== snapshotRequestGeneration" in source
    assert "generation !== marketRequestGeneration" in source
    assert "generation !== compareRequestGeneration" in source
    assert "generation !== simulationRequestGeneration" in source
    assert "generation !== movieRequestGeneration" in source
    assert "progressId !== simulationProgressId" in source
    assert "progressId !== compareProgressId" in source
    assert "progressId !== movieProgressId" in source
    assert "simulationProgressId = '';" in source
    assert "compareProgressId = '';" in source
    assert "movieProgressId = '';" in source
    assert "function invalidateSimulationRequest()" in source
    assert "function invalidateCompareRequest()" in source
    assert "function invalidateMovieRequest()" in source


def test_pb8_market_selection_preserves_config_and_simulation_uses_replay_candles() -> None:
    """PB8 selectors are analysis options and native simulation plots use the replay's own window."""
    source = PAGE_PATH.read_text(encoding="utf-8")
    functions = "\n".join(
        _extract_function(source, name)
        for name in ("applySelectedMarketToConfig", "simulationSnapshotForPlot")
    )
    script = textwrap.dedent(
        f"""
        const assert = browserAssert;
        const IS_V8 = true;
        let invalidations = 0;
        function invalidateConfigRequests() {{ invalidations += 1; }}
        const original = {{
          backtest: {{exchanges: ['binance', 'bybit']}},
          live: {{approved_coins: {{long: ['BTC'], short: ['ETH']}}}}
        }};
        const state = {{
          config: JSON.parse(JSON.stringify(original)),
          snapshot: {{candles: [{{timestamp: 'old'}}], market: {{metadata: {{ohlcv: {{}}}}}}}}
        }};
        {functions}
        applySelectedMarketToConfig();
        assert.deepEqual(state.config, original);
        assert.equal(invalidations, 1);
        const plotted = simulationSnapshotForPlot({{
          candles: [{{timestamp: 'new'}}],
          metadata: {{start_timestamp_ms: 60000, end_timestamp_ms: 120000}}
        }});
        assert.equal(plotted.candles[0].timestamp, 'new');
        assert.equal(state.snapshot.candles[0].timestamp, 'old');
        assert.equal(plotted.market.metadata.ohlcv.selected_start, '1970-01-01T00:01:00.000Z');
        """
    )
    completed = run_javascript(["-e", script], cwd=ROOT, text=True, capture_output=True, check=False)
    assert completed.returncode == 0, completed.stderr or completed.stdout


def test_pb8_manual_state_and_unavailable_movie_emas_are_not_advertised() -> None:
    """PB8 hides unsupported manual positions and never labels candle extrema as EMA bands."""
    source = PAGE_PATH.read_text(encoding="utf-8")

    assert "if (option.value === 'manual') option.remove();" in source
    assert "startState.disabled = true;" in source
    assert "deepGet(f, ['candle', 'high']" not in source[source.index("var emaHigh ="):source.index("function orderPrices", source.index("var emaHigh ="))]
    assert "showlegend: hasEmaBands" in source


def test_pareto_detail_and_baseline_state_is_result_bound() -> None:
    """Late details and pinned baselines must not cross selections or result directories."""
    source = (ROOT / "frontend" / "v7_pareto_explorer.html").read_text(encoding="utf-8")
    detail_loader = _extract_function(source, "loadConfigDetail")
    handoff = _extract_function(source, "openSelectedStrategyExplorer")
    pin = _extract_function(source, "pinSelectedStrategyBaseline")

    assert "renderDetail(null);" in detail_loader
    selected_pos = detail_loader.index("state.selectedConfigIndex = configIndex;")
    highlight_pos = detail_loader.index("renderChampions(state.commandCenter);")
    clear_pos = detail_loader.index("renderDetail(null);")
    request_pos = detail_loader.index("return apiFetch('/config-detail'")
    assert selected_pos < highlight_pos < clear_pos < request_pos
    assert "el('detail-title').textContent = 'Loading #' + String(configIndex) + '...';" in detail_loader
    assert "resultPath !== state.resultPath" in detail_loader
    assert "detail.config_index !== state.selectedConfigIndex" in handoff
    assert "state.strategyCompareBaseline.result_path === state.resultPath" in handoff
    assert "result_path: state.resultPath" in pin
    assert "optimizeVersion() !== 'v8'" in pin
    assert "button.style.display = isV8 ? '' : 'none'" in source
    assert "button.disabled = !isV8" in source
    assert "resultContextGeneration" in source
    assert "function resolveBackgroundLoadResponse(data, extractPayload, isCurrent)" in source
    assert "typeof isCurrent === 'function' && !isCurrent()" in source
    assert "bootstrapRequestSeq" in source
    assert "detail.override_error" in source
    assert "compare_override_configs: compareBaseline ? compareBaseline.override_configs" in source


def test_movie_plot_container_preserves_plotly_playback_controls() -> None:
    """The container must match Plotly's normal height so its bottom controls are not clipped."""
    source = PAGE_PATH.read_text(encoding="utf-8")

    assert '.movie-plot { height: 760px; min-height: 760px;' in source
    assert "height: 760" in source
    assert "var normalHeight = id === 'movie-plot' ? 760 : 520;" in source
    assert 'id="btn-movie-play"' not in source


def test_pb8_movie_fills_keep_utc_and_full_order_type_classification() -> None:
    """PB8 fill markers must not shift into browser local time or lose entry/close types."""
    source = PAGE_PATH.read_text(encoding="utf-8")

    assert "return new Date(ms).toISOString();" in source
    assert "evType.indexOf('entry') >= 0" in source
    assert "evType.indexOf('close') >= 0" in source


def test_movie_grid_hover_and_paused_arrow_key_stepping() -> None:
    """Movie grid lines expose prices and paused playback advances one exact frame per arrow key."""
    source = PAGE_PATH.read_text(encoding="utf-8")
    function = _extract_function(source, "stepMovieFrame")
    script = textwrap.dedent(
        f"""
        const assert = browserAssert;
        const plot = {{}};
        const calls = [];
        const document = {{getElementById: () => plot}};
        const Plotly = {{animate: (_plot, frames, options) => calls.push({{frames, options}})}};
        const window = {{Plotly}};
        let moviePlaybackPaused = true;
        let movieCurrentFrame = 1;
        let lastMovieFigureSpec = {{frames: [{{name: '0'}}, {{name: '1'}}, {{name: '2'}}]}};
        function currentMovieFrameIndex() {{ return movieCurrentFrame; }}
        {function}
        assert.equal(stepMovieFrame(1), true);
        assert.equal(movieCurrentFrame, 2);
        assert.deepEqual(calls[0].frames, ['2']);
        assert.equal(calls[0].options.frame.duration, 0);
        assert.equal(calls[0].options.frame.redraw, false);
        assert.equal(stepMovieFrame(1), true);
        assert.equal(calls.length, 1);
        moviePlaybackPaused = false;
        assert.equal(stepMovieFrame(-1), false);
        assert.equal(movieCurrentFrame, 2);
        """
    )
    completed = run_javascript(["-e", script], cwd=ROOT, text=True, capture_output=True, check=False)

    assert completed.returncode == 0, completed.stderr or completed.stdout
    assert "Price: %{y:.8f}" in source
    assert "hoverdistance: 50" in source
    assert "gx.push(x0, x1, null); gy.push(price, price, null);" in source
    assert "type: 'scatter', mode: 'lines'" in source
    assert "lines+markers" not in source
    assert "type: 'scattergl'" not in source
    assert "type: 'scatter', mode: 'markers+text'" in source
    assert "xaxis: { range: [x0, x1] }" in source
    assert "rangeslider: { visible: false }, autorange: false" in source
    assert "delete layout.xaxis.autorange;" in source
    assert "traces: [3, 4, 5, 6, 7, 8]" in source
    assert "visibleCandleTrace" not in source
    assert "event.key !== 'ArrowLeft' && event.key !== 'ArrowRight'" in source
    assert "movieStage.classList.contains('active')" in source
    assert "moviePlaybackPaused = label === 'Pause';" in source


def test_pb8_refresh_cache_rejects_secrets_and_restores_expired_drafts() -> None:
    """Refresh recovery stores only a safe config subset and rebuilds when the in-memory draft is gone."""
    source = PAGE_PATH.read_text(encoding="utf-8")
    functions = "\n".join(
        _extract_function(source, name)
        for name in ("containsSensitiveRefreshKey", "refreshCacheConfig")
    )
    script = textwrap.dedent(
        f"""
        const assert = browserAssert;
        const IS_V8 = true;
        {functions}
        const sourceConfig = {{
          config_version: 8,
          backtest: {{exchanges: ['bybit']}},
          bot: {{long: {{risk: {{n_positions: 1}}}}}},
          live: {{approved_coins: {{long: ['BTC'], short: []}}}},
          coin_overrides: {{BTC: {{long: {{}}}}}},
          pbgui: {{runtime: 'not-persisted'}}
        }};
        const cached = refreshCacheConfig(sourceConfig);
        assert.deepEqual(Object.keys(cached).sort(), ['backtest', 'bot', 'coin_overrides', 'config_version', 'live']);
        cached.bot.long.risk.n_positions = 9;
        assert.equal(sourceConfig.bot.long.risk.n_positions, 1);
        assert.equal(refreshCacheConfig({{backtest: {{}}, nested: {{api_key: 'forbidden'}}}}), null);
        assert.equal(refreshCacheConfig({{live: {{private_key: 'forbidden'}}}}), null);
        """
    )
    completed = run_javascript(["-e", script], cwd=ROOT, text=True, capture_output=True, check=False)

    assert completed.returncode == 0, completed.stderr or completed.stdout
    load_session = "\n".join(_extract_function(source, name) for name in ("createStrategyRefreshRestore", "loadSession"))
    recovery_script = textwrap.dedent(
        f"""
        const assert = browserAssert;
        let sessionRequestGeneration = 0;
        let snapshotRequestGeneration = 0;
        let DRAFT_ID = 'expired-draft';
        const RESULT_PATH = '';
        const IS_V8 = true;
        const cached = {{config: {{backtest: {{}}, bot: {{}}, live: {{}}}}, controls: {{stage: 'movie'}}}};
        const calls = [];
        let applied = null;
        function refreshCacheControls() {{ return cached.controls; }}
        function restoreStrategyRefreshControls() {{}}
        function readStrategyRefreshState() {{ return cached; }}
        function cachedSnapshotOptions() {{ return {{start_date: '2026-01-01'}}; }}
        function apiFetch(path) {{
          calls.push(path);
          if (path.startsWith('/session')) return Promise.reject(new Error('{{"detail":"draft not found"}}'));
          return Promise.resolve({{ok: true, config: cached.config}});
        }}
        function applySessionBootstrap(data, snapshot, restored, warning) {{
          applied = {{data, snapshot, restored, warning}};
          return Promise.resolve(data);
        }}
        function setMessages(messages) {{ throw new Error('unexpected error: ' + JSON.stringify(messages)); }}
        {load_session}
        (async () => {{
          await loadSession();
          assert.equal(calls.length, 2);
          assert.match(calls[0], /^\\/session\\?draft_id=expired-draft/);
          assert.equal(calls[1], '/snapshot');
          assert.equal(DRAFT_ID, '');
          assert.equal(applied.restored, cached);
          assert.equal(applied.snapshot.source, 'refresh-cache');
          assert.match(applied.warning, /restored the non-sensitive Strategy Explorer/);
        }})().catch((error) => {{ console.error(error); testResult.exitCode = 1; }});
        """
    )
    recovered = run_javascript(["-e", recovery_script], cwd=ROOT, text=True, capture_output=True, check=False)
    assert recovered.returncode == 0, recovered.stderr or recovered.stdout
    assert "window.sessionStorage.setItem(STRATEGY_REFRESH_CACHE_KEY" in source
    assert "window.localStorage.setItem(STRATEGY_REFRESH_CACHE_KEY" not in source
    assert "String(err && err.message || '').indexOf('draft not found')" in source
    assert "DRAFT_ID = '';" in source
    assert "snapshot.source = 'refresh-cache';" in source
    assert "cached.movie_data" in source
    assert "bootstrapSnapshotGeneration === snapshotRequestGeneration" in source
    assert "bootstrapMovieGeneration === movieRequestGeneration" in source
    assert "window.addEventListener('beforeunload', persistStrategyRefreshState);" in source
    bootstrap = _extract_function(source, "applySessionBootstrap")
    assert bootstrap.index("selectStage(stage);") < bootstrap.index("applyMovieFrameResult(cached.movie_data);")
    assert "var initialRefreshState = readStrategyRefreshState();" in source
    assert "selectStage(String(initialRefreshState.controls.stage));" in source
    assert "Plotly.Plots.resize(plot)" in source


def test_all_inline_scripts_parse_as_javascript() -> None:
    """The shared production template must remain syntactically valid after version branching."""
    source = PAGE_PATH.read_text(encoding="utf-8")
    scripts = re.findall(r"<script(?![^>]*\bsrc=)[^>]*>(.*?)</script>", source, flags=re.DOTALL | re.IGNORECASE)

    assert scripts
    for script in scripts:
        completed = run_javascript(
            ["--check", "-"],
            cwd=ROOT,
            input=script,
            text=True,
            capture_output=True,
            check=False,
        )
        assert completed.returncode == 0, completed.stderr or completed.stdout


def _run_js(script):
    """Execute production functions in the browser via the offline Python test wrapper."""
    result = run_javascript(["-e", script], cwd=ROOT, text=True, capture_output=True, check=False, timeout=15)
    assert result.returncode == 0, result.stderr or result.stdout


def test_endpoint_timeouts_controller_timer_cleanup_and_pb7_behavior():
    """V8 endpoint replacement, timeout, exit and completion clean resources; PB7 keeps its fetch contract."""
    source = PAGE_PATH.read_text()
    functions = "\n".join(_extract_function(source, name) for name in ("apiFetch", "disposeExplorerRequests"))
    _run_js("""
      const assert = browserAssert;
      let IS_V8 = true;
      const API_BASE = '/prefix/api/strategy-explorer-v8';
      const explorerRequests = {};
      let sessionRequestGeneration = 0, snapshotRequestGeneration = 0, marketRequestGeneration = 0;
      const timers = new Map(); let timerId = 0;
      function setTimeout(callback, ms) { timers.set(++timerId, {callback, ms}); return timerId; }
      function clearTimeout(id) { timers.delete(id); }
      const requests = [];
      function fetch(url, opts) {
        return new Promise((resolve, reject) => {
          requests.push({url, opts, resolve, reject});
          if (opts.signal) {
            const abort = () => reject(new Error('aborted'));
            opts.signal.addEventListener('abort', abort, {once:true});
            if (opts.signal.aborted) abort();
          }
        });
      }
    """ + functions + """
      (async () => {
        for (const [endpoint, ms] of [['/session?draft_id=', 225000], ['/snapshot', 145000], ['/markets', 85000]]) {
          const pending = apiFetch(endpoint);
          assert.equal([...timers.values()][0].ms, ms);
          await Promise.resolve();
          assert.ok(requests.at(-1).url.startsWith('/prefix/'));
          requests.at(-1).resolve({ok:true, json: () => Promise.resolve({ok:true})});
          await pending;
          assert.equal(timers.size, 0);
          assert.deepEqual(Object.keys(explorerRequests), []);
        }
        const old = apiFetch('/session').catch(e => e.message);
        await Promise.resolve();
        const newer = apiFetch('/session').catch(e => e.message);
        await Promise.resolve();
        assert.equal(await old, 'aborted');
        assert.equal(timers.size, 1);
        assert.equal(Object.keys(explorerRequests).length, 1);
        [...timers.values()][0].callback();
        assert.equal(await newer, 'aborted');
        assert.equal(timers.size, 0);
        const exit = apiFetch('/snapshot').catch(e => e.message);
        await Promise.resolve();
        disposeExplorerRequests();
        assert.equal(await exit, 'aborted');
        assert.equal(timers.size, 0);
        assert.equal(sessionRequestGeneration, 1);
        const failure = apiFetch('/markets').catch(e => e.message);
        await Promise.resolve();
        requests.at(-1).resolve({ok:false, status:503, text:()=>Promise.resolve('{"detail":"synthetic busy"}')});
        assert.equal(await failure, 'synthetic busy');
        assert.equal(timers.size, 0);
        const upstream = new AbortController();
        const external = apiFetch('/snapshot', {signal:upstream.signal}).catch(e => e.message);
        upstream.abort();
        assert.equal(await external, 'aborted');
        assert.equal(timers.size, 0);
        IS_V8 = false;
        const pb7 = apiFetch('/session'); await Promise.resolve();
        assert.equal(timers.size, 0);
        assert.equal(requests.at(-1).opts.signal, undefined);
        requests.at(-1).resolve({ok:true, json:()=>Promise.resolve({pb7:true})});
        assert.deepEqual(await pb7, {pb7:true});
      })().catch(e => { console.error(e); testResult.exitCode = 1; });
    """)


def test_restore_failure_never_persists_defaults_and_stale_session_starts_no_followup():
    """503 recovery retains the actual storage value and edits, suppressing obsolete follow-up work."""
    source = PAGE_PATH.read_text()
    functions = "\n".join(_extract_function(source, name) for name in ("createStrategyRefreshRestore", "loadSession", "persistStrategyRefreshState"))
    _run_js("""
      const assert = browserAssert;
      let IS_V8 = true;
      let sessionRequestGeneration = 0, snapshotRequestGeneration = 0;
      let strategyRefreshRecoveryPending = false;
      let DRAFT_ID = ''; const RESULT_PATH = '';
      const cached = {config:{backtest:{starting_balance:4321}, bot:{long:{marker:7}}, live:{}}, controls:{stage:'movie', coin:'BTC', context_days:2}};
      const original = JSON.stringify(cached);
      let stored = original, writes = 0, bootstraps = 0, messages = [], restored = null;
      const STRATEGY_REFRESH_CACHE_KEY = 'test-cache', STRATEGY_REFRESH_CACHE_MAX_BYTES = 100000;
      const window = {sessionStorage:{setItem:(_key, value)=>{stored=value; writes++;}}};
      let state = {config:null, snapshot:null};
      const nodes = {};
      const document = {getElementById:id => nodes[id] || (nodes[id]={})};
      function refreshCacheConfig(config) { return config; }
      function refreshCacheControls() { return {stage:'analysis'}; }
      function refreshCacheMovieData() { return null; }
      let lastMovieData = null;
      function readStrategyRefreshState() { return cached; }
      function cachedSnapshotOptions() { return {}; }
      function restoreStrategyRefreshControls(value) { restored = value; }
      function syncRawFromState() {}
      function setMessages(value) { messages = value; }
      function applySessionBootstrap(data) { bootstraps++; persistStrategyRefreshState(); return data; }
      let calls = [], snapshotFails = true, sessionError = null, deferred = null;
      function apiFetch(path) {
        calls.push(path);
        if (path.startsWith('/session')) {
          if (deferred) return deferred;
          if (sessionError) return Promise.reject(new Error(sessionError));
          return Promise.resolve({snapshot:{config:{default:true}}});
        }
        return snapshotFails ? Promise.reject(new Error('HTTP 503 synthetic busy')) : Promise.resolve({config:cached.config});
      }
    """ + functions + """
      (async () => {
        await loadSession();
        assert.equal(bootstraps, 0);
        assert.deepEqual(state.config, cached.config);
        assert.equal(state.snapshot, null);
        assert.deepEqual(restored, cached);
        assert.match(messages[0].text, /503/);
        persistStrategyRefreshState(); // stage changes and beforeunload must also be safe
        assert.equal(writes, 0);
        assert.equal(stored, original);
        assert.equal(nodes['source-chip'].textContent, 'Session unavailable');
        DRAFT_ID = 'expired'; sessionError = 'draft not found';
        await loadSession();
        assert.equal(DRAFT_ID, 'expired');
        assert.equal(writes, 0);
        snapshotFails = false;
        await loadSession();
        assert.equal(DRAFT_ID, '');
        assert.equal(bootstraps, 1);
        // An obsolete successful session cannot start the cached snapshot POST.
        let resolve; deferred = new Promise(r => resolve = r); calls = [];
        const stale = loadSession();
        sessionRequestGeneration++;
        resolve({snapshot:{config:{default:true}}});
        await stale;
        assert.equal(calls.length, 1);
        // PB7 retains the existing fallback path.
        deferred = null; sessionError = null; snapshotFails = true; IS_V8 = false;
        await loadSession();
        assert.equal(bootstraps, 2);
      })().catch(e => { console.error(e); testResult.exitCode = 1; });
    """)


def test_stale_recovery_and_bootstrap_cannot_mutate_context_or_start_markets():
    """A replaced recovery cannot clear a draft, render defaults or start market/movie requests."""
    source = PAGE_PATH.read_text()
    functions = "\n".join(_extract_function(source, name) for name in ("createStrategyRefreshRestore", "loadSession", "applySessionBootstrap"))
    _run_js("""
      const assert = browserAssert;
      let sessionRequestGeneration = 0, snapshotRequestGeneration = 0;
      let strategyRefreshRecoveryPending = false;
      let IS_V8 = true, DRAFT_ID = 'expired'; const RESULT_PATH = '';
      const cached = {config:{bot:{}}, controls:{stage:'movie'}};
      const state = {snapshot:null};
      function refreshCacheControls() { return cached.controls; }
      function restoreStrategyRefreshControls() {}
      function readStrategyRefreshState() { return cached; }
      function cachedSnapshotOptions() { return {}; }
      const calls = []; let resolveSnapshot;
      function apiFetch(path) {
        calls.push(path);
        return path.startsWith('/session') ? Promise.reject(new Error('draft not found')) : new Promise(r => resolveSnapshot = r);
      }
      function setMessages() { throw new Error('stale error'); }
      function configureVersionUi() { throw new Error('stale bootstrap rendered'); }
    """ + functions + """
      (async () => {
        const pending = loadSession();
        await Promise.resolve(); await Promise.resolve();
        assert.equal(calls.length, 2);
        snapshotRequestGeneration++;
        resolveSnapshot({config:cached.config});
        await pending;
        assert.equal(DRAFT_ID, 'expired');
        assert.equal(state.snapshot, null);
        assert.equal(await applySessionBootstrap({}, {}, cached, '', 0, 0), null);
      })().catch(e => { console.error(e); testResult.exitCode = 1; });
    """)


def test_restore_merges_edits_and_ignores_programmatic_control_changes():
    """Only edits made during waits replace cached values, with no mutation of saved data."""
    function = _extract_function(PAGE_PATH.read_text(), 'createStrategyRefreshRestore')
    _run_js("""
      const assert = browserAssert;
      let IS_V8 = true;
      const cached = {config:{backtest:{starting_balance:4321}}, controls:{stage:'raw', movie_frames:'37', balance:'4321', movie_visible:'60'}};
      const original = JSON.stringify(cached);
      let displayed = {stage:'raw', movie_frames:'200', balance:'1000', movie_visible:'60'};
      function refreshCacheControls() { return Object.assign({}, displayed); }
      function restoreStrategyRefreshControls(value) { displayed = Object.assign({}, displayed, value.controls); }
    """ + function + """
      const restore = createStrategyRefreshRestore(cached);
      displayed.movie_frames = '99'; displayed.stage = 'movie';
      assert.equal(restore.capture().controls.movie_frames, '99');
      assert.equal(restore.capture().controls.stage, 'movie');
      // A response synchronizes defaults before the next market request.
      displayed.movie_frames = '200'; displayed.balance = '1000';
      restore.applied();
      // A later edit must merge with the earlier edit rather than the defaults.
      displayed.movie_visible = '75';
      const result = restore.capture();
      assert.equal(result.controls.movie_frames, '99');
      assert.equal(result.controls.movie_visible, '75');
      assert.equal(result.controls.balance, '4321');
      assert.equal(result.controls.stage, 'movie');
      assert.equal(JSON.stringify(cached), original);
      // Explicit edits to the template's initial value also replace saved values.
      const another = createStrategyRefreshRestore(cached);
      assert.equal(displayed.movie_frames, '37');
      displayed.movie_frames = '200';
      assert.equal(another.capture().controls.movie_frames, '200');
      IS_V8 = false;
      assert.equal(createStrategyRefreshRestore(cached), null);
      IS_V8 = true;
      assert.equal(createStrategyRefreshRestore(null), null);
    """)


def test_movie_followup_uses_initialized_generation_and_rejects_later_edits():
    """Real Frames/Generate handlers invalidate a queued rebuild without aborting the newer request."""
    source = PAGE_PATH.read_text()
    functions = "\n".join(_extract_function(source, name) for name in (
        "applySessionBootstrap", "invalidateMovieRequest", "buildMovieFrames"
    ))
    handlers = "\n".join(line for line in source.splitlines() if line.startswith((
        "document.getElementById('movie-frames-input').addEventListener(",
        "document.getElementById('btn-build-movie').addEventListener(",
    )))
    assert handlers.count("addEventListener") == 2
    _run_js("""
      const assert = browserAssert;
      let sessionRequestGeneration = 1, snapshotRequestGeneration = 1, movieRequestGeneration = 0;
      let strategyRefreshRecoveryPending = true;
      let movieAbortController = null, movieProgressId = '', lastMovieData = null;
      const RESULT_PATH = '';
      const state = {};
      const nodes = {};
      const document = {getElementById: id => nodes[id] || (nodes[id] = {
        value:'', textContent:'Generate Movie', handlers:{},
        addEventListener(type, callback) { this.handlers[type] = callback; }
      })};
      const window = {crypto:{randomUUID:()=> 'synthetic-progress'}};
      const pending = [], requests = [];
      function setTimeout(fn) { pending.push(fn); }
      function configureVersionUi() {}
      function deepGet(_object, _path, fallback) { return fallback; }
      function renderSnapshot() {}
      function syncOptionsFromConfig() {}
      function populateMarkets() { return Promise.resolve(); }
      function inferInitialSelectors() { snapshotRequestGeneration++; movieRequestGeneration++; }
      function restoreStrategyRefreshControls() {}
      function applyInitialResultPath() {}
      function applyBacktestHandoff() {}
      function setMovieExportAvailable() {}
      function selectStage() {}
      function setMessages() {}
      function persistStrategyRefreshState() {}
      function setMovieProgress() {}
      function clearMovieExportDownload() {}
      function startMovieProgressPolling() {}
      function stopMovieProgressPolling() {}
      function requestMovieFrames(progressId, signal, generation) {
        requests.push({progressId, signal, generation});
        return new Promise(() => {}); // synthetic pending request: no I/O/timers
      }
      const cached = {controls:{stage:'movie', movie_generated:true}};
    """ + functions + "\n" + handlers + """
      (async () => {
        await applySessionBootstrap({}, {}, cached, '', 1, 1);
        assert.equal(pending.length, 1);
        pending.shift()();
        assert.equal(requests.length, 1); // own selector setup must permit rebuilding

        await applySessionBootstrap({}, {}, cached, '', 1, snapshotRequestGeneration);
        const snapshotBeforeFramesEdit = snapshotRequestGeneration;
        const movieBeforeFramesEdit = movieRequestGeneration;
        nodes['movie-frames-input'].value = '99';
        nodes['movie-frames-input'].handlers.input();
        assert.equal(movieRequestGeneration, movieBeforeFramesEdit + 1);
        assert.equal(snapshotRequestGeneration, snapshotBeforeFramesEdit);
        pending.shift()();
        assert.equal(requests.length, 1); // a real Frames edit invalidates the timer

        await applySessionBootstrap({}, {}, cached, '', 1, snapshotRequestGeneration);
        const snapshotBeforeManualStart = snapshotRequestGeneration;
        nodes['btn-build-movie'].handlers.click();
        assert.equal(snapshotRequestGeneration, snapshotBeforeManualStart);
        assert.equal(requests.length, 2);
        const manualRequest = requests.at(-1);
        pending.shift()();
        assert.equal(requests.length, 2); // no second request from the old timer
        assert.equal(manualRequest.signal.aborted, false);
        assert.equal(manualRequest.generation, movieRequestGeneration);

        await applySessionBootstrap({}, {}, cached, '', 1, snapshotRequestGeneration);
        snapshotRequestGeneration++;
        pending.shift()();
        assert.equal(requests.length, 2);
        await applySessionBootstrap({}, {}, cached, '', 1, snapshotRequestGeneration);
        sessionRequestGeneration++;
        pending.shift()();
        assert.equal(requests.length, 2);
      })().catch(e => {console.error(e);testResult.exitCode=1;});
    """)
