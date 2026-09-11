"""Frontend contracts for focused Hyperliquid Market Data actions."""

from pathlib import Path
import subprocess
import textwrap


ROOT = Path(__file__).resolve().parents[2]
PAGE = ROOT / "frontend" / "hl_data_actions.html"


def _extract_function(source: str, name: str) -> str:
    """Extract one named JavaScript function from the embedded page."""
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


def test_build_coin_filters_combine_tradfi_and_local_data_state() -> None:
    """TradFi and no-local-data toggles should compose with the text filter."""
    source = PAGE.read_text(encoding="utf-8")
    function = _extract_function(source, "getBuildVisibleCoins")
    script = textwrap.dedent(
        f"""
        const assert = require('node:assert/strict');
        var buildCoins = ['BTC', 'ETH', 'xyz:AAPL', 'XYZ-MSFT'];
        var buildCoinsWithDownloadedHistory = new Set(['BTC', 'xyz:AAPL']);
        var buildFilter = '';
        var buildTradfiOnly = false;
        var buildNoLocalData = false;
        {function}
        assert.deepEqual(getBuildVisibleCoins(), buildCoins);
        buildTradfiOnly = true;
        assert.deepEqual(getBuildVisibleCoins(), ['xyz:AAPL', 'XYZ-MSFT']);
        buildNoLocalData = true;
        assert.deepEqual(getBuildVisibleCoins(), ['XYZ-MSFT']);
        buildTradfiOnly = false;
        assert.deepEqual(getBuildVisibleCoins(), ['ETH', 'XYZ-MSFT']);
        buildFilter = 'msft';
        assert.deepEqual(getBuildVisibleCoins(), ['XYZ-MSFT']);
        """
    )

    subprocess.run(["node", "-e", script], cwd=ROOT, check=True, capture_output=True, text=True)


def test_build_coin_filter_controls_and_payload_contract_are_present() -> None:
    """The focused panel should expose both toggles and consume backend availability metadata."""
    source = PAGE.read_text(encoding="utf-8")

    assert 'data-action="build-tradfi-only"' in source
    assert 'data-action="build-no-local-data"' in source
    assert "buildCoinsWithDownloadedHistory = new Set(data.coins_with_downloaded_history||[])" in source


def test_empty_build_info_retries_without_stale_page_updates() -> None:
    """Transient empty discovery should retry while ignoring detached or superseded pages."""
    source = PAGE.read_text(encoding="utf-8")
    retry = _extract_function(source, "scheduleBuildInfoRetry")
    init = _extract_function(source, "doInit")

    assert "BUILD_INFO_MAX_RETRIES = 12" in source
    assert "data.retryable === true" in retry
    assert "generation !== initGeneration || !ROOT.isConnected" in retry
    assert "bD.retryable === true" in init
    assert "applyBuildInfo(bD)" in init
    assert "escHtml(reason)" in source
    assert "buildRetryExhausted = true" in retry
    assert "Automatic retries stopped" in source


def test_build_info_retry_runtime_handles_stale_success_and_exhaustion() -> None:
    """The retry timer should stop for stale pages, successful data, and exhaustion."""
    source = PAGE.read_text(encoding="utf-8")
    apply_info = _extract_function(source, "applyBuildInfo")
    show_failure = _extract_function(source, "showBuildInfoFailure")
    transient_status = _extract_function(source, "isTransientBuildInfoStatus")
    retry = _extract_function(source, "scheduleBuildInfoRetry")
    script = textwrap.dedent(
        f"""
        const assert = require('node:assert/strict');
        var buildInfoRetryTimer = null;
        var BUILD_INFO_MAX_RETRIES = 12, BUILD_INFO_RETRY_DELAY = 5000;
        var initGeneration = 1, ROOT = {{isConnected: true}}, API_BASE = '/api';
        var buildCoins = [], buildCoinsWithDownloadedHistory = new Set(), buildEmptyReason = '';
        var buildRetryable = true, buildRetryExhausted = false;
        var timerCallback = null, fetchCalls = 0, populateCalls = 0;
        var responseStatus = 200, responseData = {{eligible_coins: []}};
        var jsonError = false, detachOnFetch = false, detachOnJson = false;
        function setTimeout(callback) {{ timerCallback = callback; return 1; }}
        function authOptions() {{ return {{}}; }}
        function populateBuild() {{ populateCalls += 1; }}
        async function fetch() {{
            fetchCalls += 1;
            if (detachOnFetch) {{ ROOT.isConnected = false; throw new Error('network'); }}
            return {{ok: responseStatus >= 200 && responseStatus < 300, status: responseStatus,
                json: async function() {{
                    if (detachOnJson) ROOT.isConnected = false;
                    if (jsonError) throw new Error('json');
                    return responseData;
                }}}};
        }}
        {apply_info}
        {show_failure}
        {transient_status}
        {retry}
        (async function() {{
            scheduleBuildInfoRetry(0, 0);
            await timerCallback();
            assert.equal(fetchCalls, 0);
            assert.equal(buildInfoRetryTimer, null);

            responseData = {{eligible_coins: ['BTC'], retryable: false}};
            scheduleBuildInfoRetry(1, 0);
            await timerCallback();
            assert.equal(fetchCalls, 1);
            assert.deepEqual(buildCoins, ['BTC']);
            assert.equal(buildInfoRetryTimer, null);

            responseData = {{eligible_coins: [], retryable: true}};
            scheduleBuildInfoRetry(1, 0);
            for (let attempt = 0; attempt < BUILD_INFO_MAX_RETRIES; attempt += 1) {{
                var callback = timerCallback;
                timerCallback = null;
                await callback();
            }}
            assert.equal(fetchCalls, BUILD_INFO_MAX_RETRIES + 1);
            assert.equal(buildRetryable, false);
            assert.equal(buildRetryExhausted, true);
            assert.equal(buildInfoRetryTimer, null);

            responseStatus = 401;
            buildRetryExhausted = false;
            scheduleBuildInfoRetry(1, 0);
            await timerCallback();
            assert.equal(buildRetryable, false);
            assert.equal(buildRetryExhausted, false);
            assert.match(buildEmptyReason, /HTTP 401/);
            assert.equal(buildInfoRetryTimer, null);

            responseStatus = 200;
            jsonError = true;
            scheduleBuildInfoRetry(1, 0);
            await timerCallback();
            assert.match(buildEmptyReason, /invalid server response/);
            assert.equal(buildInfoRetryTimer, null);

            var populateBeforeDetachedJson = populateCalls;
            var reasonBeforeDetachedJson = buildEmptyReason;
            detachOnJson = true;
            ROOT.isConnected = true;
            scheduleBuildInfoRetry(1, 0);
            await timerCallback();
            assert.equal(populateCalls, populateBeforeDetachedJson);
            assert.equal(buildEmptyReason, reasonBeforeDetachedJson);
            assert.equal(buildInfoRetryTimer, null);

            jsonError = false;
            detachOnJson = false;
            detachOnFetch = true;
            ROOT.isConnected = true;
            scheduleBuildInfoRetry(1, 0);
            await timerCallback();
            assert.equal(buildInfoRetryTimer, null);
        }})().catch(function(error) {{ console.error(error); process.exit(1); }});
        """
    )

    subprocess.run(["node", "-e", script], cwd=ROOT, check=True, capture_output=True, text=True)


def test_job_history_requests_filter_before_the_api_limit() -> None:
    """History tabs must ask the API for the focused job type before limiting results."""
    source = PAGE.read_text(encoding="utf-8")
    function = _extract_function(source, "loadHistoryTab")

    assert "&limit=20&job_type=' + encodeURIComponent(jt)" in function
    assert function.index("var jt = JOB_TYPES[ns]") < function.index("await fetch")


def test_queue_submissions_render_http_errors_instead_of_false_success() -> None:
    """Rejected download and build requests must never display queued-job success."""
    source = PAGE.read_text(encoding="utf-8")
    functions = "\n\n".join(
        _extract_function(source, name).replace(f"function {name}(", f"async function {name}(", 1)
        for name in ("submitDL", "submitBuild")
    )
    script = textwrap.dedent(
        f"""
        const assert = require('node:assert/strict');
        function loading() {{ return {{classList: {{add() {{}}, remove() {{}}}}}}; }}
        const elements = {{
            'dl-btn': {{disabled: false}}, 'dl-lo': loading(),
            'dl-sd': {{value: '2026-09-01'}}, 'dl-ed': {{value: '2026-09-10'}},
            'dl-only': {{checked: true}},
            'build-btn': {{disabled: false}}, 'build-lo': loading(),
            'build-sd': {{value: ''}}, 'build-ed': {{value: ''}},
            'build-refetch': {{checked: false}}
        }};
        function $(id) {{ return elements[id] || null; }}
        function inputToDay(value) {{ return value; }}
        function authOptions(options) {{ return options; }}
        function hideMsg() {{}}
        const messages = [], queued = [];
        function showMsg(ns, type, text) {{ messages.push({{ns, type, text}}); }}
        function showQueuedMsg(ns, data) {{ queued.push({{ns, data}}); }}
        var API_BASE = '/api';
        var dlSelected = new Set(), dlCoins = ['BTC'];
        var buildSelected = new Set(), buildCoins = ['BTC'];
        let response;
        async function fetch() {{ return response; }}
        {functions}

        (async function() {{
            response = {{ok: false, status: 409, json: async function() {{ return {{detail: 'A job is already queued'}}; }}}};
            await submitDL();
            assert.deepEqual(messages.pop(), {{ns: 'dl', type: 'error', text: 'A job is already queued'}});
            assert.equal(queued.length, 0);
            assert.equal(elements['dl-btn'].disabled, false);

            response = {{ok: false, status: 500, json: async function() {{ throw new Error('not json'); }}}};
            await submitBuild();
            assert.deepEqual(messages.pop(), {{ns: 'build', type: 'error', text: 'HTTP 500'}});
            assert.equal(queued.length, 0);
            assert.equal(elements['build-btn'].disabled, false);

            response = {{ok: true, status: 200, json: async function() {{ return {{job_id: 'job-1', coins_count: 1}}; }}}};
            await submitBuild();
            assert.equal(queued.length, 1);
            assert.equal(queued[0].ns, 'build');
        }})().catch(function(error) {{ console.error(error); process.exit(1); }});
        """
    )

    subprocess.run(["node", "-e", script], cwd=ROOT, check=True, capture_output=True, text=True)
