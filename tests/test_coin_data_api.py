"""Focused pool-readiness contracts for the Coin Data API."""

from __future__ import annotations

import json
from pathlib import Path
import subprocess
import textwrap
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

import api.coin_data as coin_data_api
import api.services as services_api


class _FakeCoinData:
    """Minimal CoinData state source with a secret-free configurable pool status."""

    def __init__(self, ready: bool, exchange: str = "binance", rows: list[dict] | None = None) -> None:
        self.cmc_pool_ready = ready
        self.exchanges = [exchange]
        self.exchange = exchange
        self._rows = list(rows or [])
        self.market_cap = 0.0
        self.vol_mcap = 10.0
        self.tags = []
        self.only_cpt = False
        self.notices_ignore = False

    def cmc_pool_status(self) -> dict:
        """Return the same non-sensitive shape as the real pool."""
        return {
            "ready": self.cmc_pool_ready,
            "active_credentials": 1 if self.cmc_pool_ready else 0,
            "keys": [{"id": "cmc_test", "status": "active"}] if self.cmc_pool_ready else [],
        }

    def load_exchange_mapping(self, _exchange: str) -> list:
        """Return the configured exchange rows for isolated state construction."""
        return self._rows

    def refresh_exchange_mapping(self, exchange: str, **_kwargs) -> dict:
        """Avoid external refresh work while matching the orchestrator contract."""
        return {
            "exchange": exchange,
            "markets_ok": True,
            "mapping_ok": True,
            "prices_ok": True,
            "ok": True,
        }

    def get_mapping_tags(self, _exchange: str, *, quote_filter: list[str]) -> list:
        """Return tags for rows in the selected quote families."""
        return sorted({
            tag
            for row in self._rows
            if row.get("quote") in quote_filter
            for tag in row.get("tags", [])
        })

    def filter_mapping_rows(self, **kwargs) -> list:
        """Return configured rows restricted to the requested quotes."""
        quote_filter = kwargs.get("quote_filter") or []
        return [row for row in self._rows if row.get("quote") in quote_filter]


def _payload() -> coin_data_api.CoinDataRefreshRequest:
    """Build the established empty refresh request."""
    return coin_data_api.CoinDataRefreshRequest()


@pytest.mark.parametrize("handler", [coin_data_api.refresh_cmc, coin_data_api.refresh_cmc_all])
def test_cmc_refresh_rejects_before_job_creation_without_active_local_key(monkeypatch, handler) -> None:
    """CMC refresh routes return 409 and create no job when the local pool is empty."""
    created = []
    monkeypatch.setattr(coin_data_api, "_new_coindata", lambda **kwargs: _FakeCoinData(False))
    monkeypatch.setattr(coin_data_api, "_start_refresh_job", lambda *args, **kwargs: created.append((args, kwargs)))

    with pytest.raises(HTTPException) as exc_info:
        handler(_payload(), SimpleNamespace())

    assert exc_info.value.status_code == 409
    assert created == []


@pytest.mark.parametrize("handler", [coin_data_api.refresh_cmc, coin_data_api.refresh_cmc_all])
def test_cmc_refresh_accepts_active_local_key_without_lease(monkeypatch, handler) -> None:
    """Readiness depends on active local materialization, not lease availability."""
    monkeypatch.setattr(coin_data_api, "_new_coindata", lambda **kwargs: _FakeCoinData(True))
    monkeypatch.setattr(coin_data_api, "_start_refresh_job", lambda *args, **kwargs: "existing-contract-job")

    assert handler(_payload(), SimpleNamespace()) == {
        "ok": True,
        "job_id": "existing-contract-job",
    }


def test_state_contains_secret_free_pool_readiness(monkeypatch, tmp_path: Path) -> None:
    """State exposes readiness and diagnostics without any stored credential value."""
    coindata_dir = tmp_path / "coindata"
    coindata_dir.mkdir()
    monkeypatch.setattr(coin_data_api, "COINDATA_DIR", coindata_dir)
    monkeypatch.setattr(coin_data_api, "_new_coindata", lambda **kwargs: _FakeCoinData(True))

    state = coin_data_api._build_state()

    assert state["cmc_pool"]["ready"] is True
    serialized = json.dumps(state)
    assert "api_key" not in serialized
    assert "secret" not in serialized


@pytest.mark.parametrize(
    ("serializer", "row"),
    [
        (
            coin_data_api._serialize_main_row,
            {
                "coin": "BTC",
                "symbol": "BTCUSDT",
                "ccxt_symbol": "BTC/USDT:USDT",
                "copy_trading": True,
            },
        ),
        (
            coin_data_api._serialize_hip3_row,
            {
                "dex": "xyz",
                "coin": "TSLA",
                "symbol": "xyz:TSLA",
                "ccxt_symbol": "XYZ-TSLA/USDC:USDC",
                "copy_trading": True,
            },
        ),
    ],
)
def test_serialized_rows_include_native_symbol_without_cpt_alias(serializer, row) -> None:
    """Matched and HIP-3 API rows expose native symbols without duplicating CPT state."""
    serialized = serializer(row, {})

    assert serialized["symbol"] == row["symbol"]
    assert serialized["copy_trading"] is True
    assert "cpt" not in serialized


def test_main_row_serialization_preserves_unavailable_cmc_metrics_as_null() -> None:
    """The JSON-facing row contract does not fabricate zeros for unavailable metrics."""
    serialized = coin_data_api._serialize_main_row(
        {
            "coin": "UNKNOWN",
            "symbol": "UNKNOWNUSDT",
            "cmc_rank": 0,
            "market_cap": None,
            "volume_24h": None,
            "vol/mcap": None,
        },
        {},
    )

    assert serialized["market_cap"] is None
    assert serialized["volume_24h"] is None
    assert serialized["vol_mcap"] is None
    assert serialized["cmc_rank"] is None


@pytest.mark.parametrize(
    ("exchange", "expected"),
    [
        ("binance", ["USDT"]),
        ("hyperliquid", ["USDC", "USDT0"]),
    ],
)
def test_state_quote_defaults_and_explicit_selection(monkeypatch, tmp_path: Path, exchange, expected) -> None:
    """Omitted quotes preserve exchange defaults while explicit selections override them."""
    rows = [
        {
            "coin": quote,
            "symbol": f"{quote}{quote}",
            "quote": quote,
            "cmc_id": index,
            "is_hip3": exchange == "hyperliquid",
            "active": True,
            "linear": True,
        }
        for index, quote in enumerate(["USDC", "USDT", "USDT0"], start=1)
    ]
    fake = _FakeCoinData(True, exchange=exchange, rows=rows)
    coindata_dir = tmp_path / "coindata"
    coindata_dir.mkdir()
    monkeypatch.setattr(coin_data_api, "COINDATA_DIR", coindata_dir)
    monkeypatch.setattr(coin_data_api, "_new_coindata", lambda **_kwargs: fake)

    default_state = coin_data_api._build_state(exchange=exchange)
    explicit_state = coin_data_api._build_state(exchange=exchange, quotes=["usdc"])

    assert default_state["available_quotes"] == ["USDC", "USDT", "USDT0"]
    assert default_state["selected_quotes"] == expected
    assert default_state["filters"]["quotes"] == expected
    assert explicit_state["selected_quotes"] == ["USDC"]
    assert {row["quote"] for row in explicit_state["rows"] + explicit_state["hip3_rows"]} == {"USDC"}


@pytest.mark.parametrize("quotes", [[], ["USD/C"], ["EUR"]])
def test_state_rejects_invalid_or_unavailable_quote_selection(quotes) -> None:
    """Quote selections must be syntactically valid and present in the mapping."""
    with pytest.raises(HTTPException) as exc_info:
        coin_data_api._select_quotes("binance", ["USDT"], quotes)

    assert exc_info.value.status_code == 422


def test_state_route_forwards_repeated_quote_query_values(monkeypatch) -> None:
    """The GET contract forwards every selected quote to state construction."""
    captured = {}
    monkeypatch.setattr(
        coin_data_api,
        "_build_state",
        lambda **kwargs: captured.update(kwargs) or {"ok": True},
    )

    result = coin_data_api.get_state(
        exchange="hyperliquid",
        market_cap=0,
        vol_mcap=10,
        tags=None,
        only_cpt=False,
        hide_notices=False,
        quotes=["USDC", "USDT0"],
        session=SimpleNamespace(),
    )

    assert result == {"ok": True}
    assert captured["quotes"] == ["USDC", "USDT0"]


def test_coin_data_frontend_sort_is_deterministic_for_reversed_equal_values() -> None:
    """Equal and unknown market caps use the normalized row identity regardless of input order."""
    source = Path("frontend/coin_data.html").read_text(encoding="utf-8")
    start = source.index("    function compareCoinRowTieBreak(")
    end = source.index("\n    function renderTags(", start)
    functions = source[start:end]
    script = textwrap.dedent(
        f"""
        const assert = require('node:assert/strict');
        var sortState = {{main: {{key: 'market_cap', dir: 'desc'}}}};
        {functions}
        const rows = [
          {{id: 'unknown-z', coin: ' gamma ', symbol: 'GAMMAUSDT', ccxt_symbol: 'GAMMA/USDT:USDT', market_cap: null, cmc_rank: null}},
          {{id: 'known-b', coin: 'BETA', symbol: 'BETAUSDT', ccxt_symbol: 'BETA/USDT:USDT', market_cap: 10, cmc_rank: 1}},
          {{id: 'unknown-a', coin: 'DELTA', symbol: 'DELTAUSDT', ccxt_symbol: 'DELTA/USDT:USDT', market_cap: null, cmc_rank: null}},
          {{id: 'known-a', coin: ' alpha', symbol: 'ALPHAUSDT', ccxt_symbol: 'ALPHA/USDT:USDT', market_cap: 10, cmc_rank: 2}},
          {{id: 'ccxt-b', coin: 'SAME', symbol: 'SAMEUSDT', ccxt_symbol: 'SAME-B/USDT:USDT', market_cap: 5, cmc_rank: 3}},
          {{id: 'ccxt-a', coin: 'same', symbol: 'sameusdt', ccxt_symbol: 'SAME-A/USDT:USDT', market_cap: 5, cmc_rank: 3}}
        ];
        const forward = sortRows(rows, 'main').map(row => row.id);
        const reversed = sortRows(rows.slice().reverse(), 'main').map(row => row.id);
        assert.deepEqual(forward, ['known-a', 'known-b', 'ccxt-a', 'ccxt-b', 'unknown-a', 'unknown-z']);
        assert.deepEqual(reversed, forward);
        sortState.main.dir = 'asc';
        assert.deepEqual(
          sortRows(rows.slice().reverse(), 'main').map(row => row.id),
          ['ccxt-a', 'ccxt-b', 'known-a', 'known-b', 'unknown-a', 'unknown-z']
        );
        sortState.main = {{key: 'cmc_rank', dir: 'asc'}};
        assert.deepEqual(
          sortRows(rows.slice().reverse(), 'main').map(row => row.id),
          ['known-b', 'known-a', 'ccxt-a', 'ccxt-b', 'unknown-a', 'unknown-z']
        );
        sortState.main.dir = 'desc';
        assert.deepEqual(
          sortRows(rows, 'main').map(row => row.id),
          ['ccxt-a', 'ccxt-b', 'known-a', 'known-b', 'unknown-a', 'unknown-z']
        );
        """
    )
    completed = subprocess.run(
        ["node", "-e", script],
        text=True,
        capture_output=True,
        timeout=10,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr or completed.stdout


def test_coin_data_frontend_keeps_null_metrics_unavailable_and_persists_quotes() -> None:
    """Frontend helpers render null as N/A and round-trip quote selection through the URL."""
    source = Path("frontend/coin_data.html").read_text(encoding="utf-8")
    format_start = source.index("    function formatCompact(")
    format_end = source.index("\n    function rowKey(", format_start)
    query_start = source.index("    function loadFiltersFromQuery(")
    query_end = source.index("\n    function jsonHeaders(", query_start)
    state_url_start = source.index("    function buildStateUrl(")
    state_url_end = source.index("\n    function applyServerState(", state_url_start)
    script = textwrap.dedent(
        f"""
        const assert = require('node:assert/strict');
        {source[format_start:format_end]}
        var filters = {{exchange: '', hip3_dex: '', market_cap: 0, vol_mcap: 10, tags: [], quotes: [], only_cpt: false}};
        var replacedUrl = '';
        var window = {{
          location: {{search: '?exchange=hyperliquid&quotes=USDC,USDT0', pathname: '/coin-data'}},
          history: {{replaceState: function (_state, _title, url) {{ replacedUrl = url; }}}}
        }};
        var API_BASE = '/api/coin-data';
        function supportsCopyTradingFilter() {{ return true; }}
        {source[query_start:query_end]}
        {source[state_url_start:state_url_end]}
        loadFiltersFromQuery();
        assert.deepEqual(filters.quotes, ['USDC', 'USDT0']);
        saveFiltersToQuery();
        assert.match(replacedUrl, /quotes=USDC%2CUSDT0/);
        const stateUrl = buildStateUrl();
        assert.match(stateUrl, /quotes=USDC/);
        assert.match(stateUrl, /quotes=USDT0/);
        assert.equal(formatCompact(null), 'N/A');
        assert.equal(formatPrice(null), 'N/A');
        assert.equal(formatRatio(null), 'N/A');
        assert.equal(formatCompact(0), '0.00');
        """
    )
    completed = subprocess.run(
        ["node", "-e", script],
        text=True,
        capture_output=True,
        timeout=10,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr or completed.stdout
    assert 'role="group" aria-labelledby="quote-filter-label"' in source
    assert 'aria-pressed="' in source


def test_services_key_status_uses_pool_without_legacy_api_key(monkeypatch) -> None:
    """Services status no longer blocks a pool-backed request on an empty compatibility property."""
    monkeypatch.setattr(
        services_api,
        "_cmc_pool_payload",
        lambda: {
            "ready": True,
            "active_credentials": 1,
            "keys": [{"provider_remaining": 9996}],
        },
    )

    result = services_api.get_pbcoindata_key_status(SimpleNamespace())

    assert result["ok"] is True
    assert result["keys"][0]["provider_remaining"] == 9996
    assert "api_key" not in json.dumps(result)


def test_coin_data_frontend_gates_only_cmc_refresh_and_keeps_cached_state() -> None:
    """The page consumes pool readiness, preserves cached views, and shows exact rejection detail."""
    source = Path("frontend/coin_data.html").read_text(encoding="utf-8")

    assert "var pool = serverState.cmc_pool || {};" in source
    assert "button.disabled = !hasMaterializedKey;" in source
    assert "Cached Coin Data remains readable." in source
    assert "response.status + ': ' + (payload.detail" in source
    assert "document.getElementById('btn-refresh-exchange').addEventListener" in source


def _capture_refresh_outcome(
    monkeypatch,
    handler,
    outcomes: dict[str, object],
    *,
    state_error: str | None = None,
):
    """Run a route's nested worker synchronously and return its terminal outcome."""
    fake = _FakeCoinData(True)

    def refresh(exchange: str, **_kwargs):
        outcome = outcomes[exchange]
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    fake.refresh_exchange_mapping = refresh
    captured = {}
    monkeypatch.setattr(coin_data_api, "_new_coindata", lambda **_kwargs: fake)
    monkeypatch.setattr(coin_data_api.V7, "list", staticmethod(lambda: list(outcomes)))
    if state_error is None:
        monkeypatch.setattr(coin_data_api, "_build_state", lambda **_kwargs: {"usable": True})
    else:
        def fail_state(**_kwargs):
            raise RuntimeError(state_error)

        monkeypatch.setattr(coin_data_api, "_build_state", fail_state)
    monkeypatch.setattr(coin_data_api, "_refresh_cmc_data", lambda *_args: None)

    def start(_title, _message, total_steps, runner, **_kwargs):
        captured["outcome"] = runner("job-id", total_steps)
        return "job-id"

    monkeypatch.setattr(coin_data_api, "_start_refresh_job", start)
    assert handler(_payload(), SimpleNamespace()) == {"ok": True, "job_id": "job-id"}
    return captured["outcome"]


@pytest.mark.parametrize("handler", [coin_data_api.refresh_all, coin_data_api.refresh_cmc_all])
def test_batch_refresh_retains_false_exception_and_partial_results(monkeypatch, handler) -> None:
    """Both all-exchange routes publish usable partial state and every failed result."""
    outcome = _capture_refresh_outcome(monkeypatch, handler, {
        "binance": {
            "exchange": "binance",
            "markets_ok": True,
            "mapping_ok": True,
            "prices_ok": True,
            "ok": True,
        },
        "bitget": False,
        "okx": RuntimeError("provider unavailable"),
    })

    assert outcome.status == "partial"
    assert outcome.state == {"usable": True}
    assert [result["exchange"] for result in outcome.exchange_results] == ["binance", "bitget", "okx"]
    assert [result["ok"] for result in outcome.exchange_results] == [True, False, False]
    assert "returned False" in outcome.exchange_results[1]["error"]
    assert outcome.exchange_results[2]["error"] == "provider unavailable"
    assert "bitget" in outcome.message and "okx" in outcome.message


@pytest.mark.parametrize("handler", [coin_data_api.refresh_all, coin_data_api.refresh_cmc_all])
def test_batch_refresh_zero_success_is_error_with_readable_state(monkeypatch, handler) -> None:
    """Both all-exchange routes classify zero successes as error without dropping state."""
    outcome = _capture_refresh_outcome(monkeypatch, handler, {
        "binance": False,
        "bitget": RuntimeError("offline"),
    })

    assert outcome.status == "error"
    assert outcome.state == {"usable": True}
    assert len(outcome.exchange_results) == 2
    assert all(not result["ok"] for result in outcome.exchange_results)
    assert len(outcome.warnings) == 2


@pytest.mark.parametrize("handler", [coin_data_api.refresh_all, coin_data_api.refresh_cmc_all])
def test_batch_refresh_all_success_is_completed(monkeypatch, handler) -> None:
    """Both all-exchange routes preserve the completed contract when every stage succeeds."""
    successful = {
        exchange: {
            "exchange": exchange,
            "markets_ok": True,
            "mapping_ok": True,
            "prices_ok": True,
            "ok": True,
        }
        for exchange in ("binance", "bitget")
    }
    outcome = _capture_refresh_outcome(monkeypatch, handler, successful)

    assert outcome.status == "completed"
    assert outcome.warnings == []
    assert len(outcome.exchange_results) == 2


@pytest.mark.parametrize("handler", [coin_data_api.refresh_all, coin_data_api.refresh_cmc_all])
def test_batch_refresh_all_partial_price_results_remain_partial(monkeypatch, handler) -> None:
    """Price gaps retain usable data and never become a completed or zero-success refresh."""
    outcome = _capture_refresh_outcome(monkeypatch, handler, {
        "hyperliquid": {
            "exchange": "hyperliquid",
            "markets_ok": True,
            "mapping_ok": True,
            "prices_ok": False,
            "partial": True,
            "price_update": {
                "ok": False,
                "partial": True,
                "requested": 3,
                "priced": 1,
                "missing": 2,
            },
            "ok": False,
        },
    })

    assert outcome.status == "partial"
    assert outcome.state == {"usable": True}
    assert "partial price coverage: 1/3 priced, 2 missing" in outcome.warnings[0]


@pytest.mark.parametrize(
    "handler",
    [
        coin_data_api.refresh_exchange,
        coin_data_api.refresh_all,
        coin_data_api.refresh_cmc,
        coin_data_api.refresh_cmc_all,
    ],
)
def test_state_reconstruction_failure_retains_partial_exchange_diagnostics(monkeypatch, handler) -> None:
    """Every refresh runner keeps primary partial data when page-state reconstruction fails."""
    outcome = _capture_refresh_outcome(
        monkeypatch,
        handler,
        {
            "binance": {
                "exchange": "binance",
                "markets_ok": True,
                "mapping_ok": True,
                "prices_ok": False,
                "partial": True,
                "price_update": {
                    "ok": False,
                    "partial": True,
                    "requested": 3,
                    "priced": 1,
                    "missing": 2,
                },
                "ok": False,
            },
        },
        state_error="mapping snapshot unavailable",
    )

    assert outcome.status == "partial"
    assert outcome.state is None
    assert outcome.exchange_results[0]["price_update"]["priced"] == 1
    assert "partial price coverage: 1/3 priced, 2 missing" in outcome.warnings[0]
    assert outcome.warnings[-1] == (
        "Post-refresh state reconstruction failed: mapping snapshot unavailable"
    )
    assert "Post-refresh state reconstruction failed" in outcome.message


@pytest.mark.parametrize(
    ("handler", "success_message"),
    [
        (coin_data_api.refresh_cmc, "CoinMarketCap data refreshed"),
        (coin_data_api.refresh_cmc_all, "CoinMarketCap data and all exchanges refreshed"),
    ],
)
def test_cmc_state_reconstruction_failure_is_not_labeled_as_cmc_failure(
    monkeypatch,
    handler,
    success_message,
) -> None:
    """A successful CMC operation remains distinguishable from a later state-read failure."""
    outcome = _capture_refresh_outcome(
        monkeypatch,
        handler,
        {
            "binance": {
                "exchange": "binance",
                "markets_ok": True,
                "mapping_ok": True,
                "prices_ok": True,
                "ok": True,
            },
        },
        state_error="mapping snapshot unavailable",
    )

    assert outcome.status == "partial"
    assert outcome.message.startswith(success_message)
    assert "Failed to refresh CoinMarketCap" not in outcome.message
    assert outcome.exchange_results[0]["ok"] is True


def test_refresh_jobs_coalesce_exact_requests_and_hide_internal_key() -> None:
    """Identical active requests share a worker without exposing the matching key."""
    release = coin_data_api.threading.Event()

    def runner(_job_id, _total_steps):
        assert release.wait(5)
        return coin_data_api._RefreshJobOutcome("completed", "complete", {}, [], [])

    with coin_data_api._REFRESH_JOBS_LOCK:
        coin_data_api._REFRESH_JOBS.clear()
        coin_data_api._REFRESH_THREADS.clear()
        coin_data_api._REFRESH_ACCEPTING = True
    try:
        first = coin_data_api._start_refresh_job(
            "Refresh", "Running", 1, runner, coalesce_key="same-request"
        )
        second = coin_data_api._start_refresh_job(
            "Refresh", "Running", 1, runner, coalesce_key="same-request"
        )

        assert second == first
        assert len(coin_data_api._REFRESH_THREADS) == 1
        assert "_coalesce_key" not in coin_data_api._get_refresh_job(first)
    finally:
        release.set()
        for thread in list(coin_data_api._REFRESH_THREADS.values()):
            thread.join(5)
        with coin_data_api._REFRESH_JOBS_LOCK:
            coin_data_api._REFRESH_JOBS.clear()
            coin_data_api._REFRESH_THREADS.clear()


def test_refresh_job_key_distinguishes_omitted_and_explicit_empty_quotes() -> None:
    """The internal key preserves the distinction between default and empty quotes."""
    fake = _FakeCoinData(True)

    omitted = coin_data_api._refresh_job_key(
        "exchange", fake, coin_data_api.CoinDataRefreshRequest()
    )
    explicit_empty = coin_data_api._refresh_job_key(
        "exchange", fake, coin_data_api.CoinDataRefreshRequest(quotes=[])
    )

    assert omitted != explicit_empty


@pytest.mark.parametrize(
    "handler",
    [
        coin_data_api.refresh_exchange,
        coin_data_api.refresh_all,
        coin_data_api.refresh_cmc,
        coin_data_api.refresh_cmc_all,
    ],
)
def test_refresh_routes_reject_explicit_empty_quotes_before_work(monkeypatch, handler) -> None:
    """Every refresh route returns 422 before constructing state or starting a runner."""
    monkeypatch.setattr(
        coin_data_api,
        "_new_coindata",
        lambda **_kwargs: pytest.fail("CoinData must not be constructed"),
    )
    monkeypatch.setattr(
        coin_data_api,
        "_start_refresh_job",
        lambda *_args, **_kwargs: pytest.fail("refresh runner must not start"),
    )

    with pytest.raises(HTTPException) as exc_info:
        handler(coin_data_api.CoinDataRefreshRequest(quotes=[]), SimpleNamespace())

    assert exc_info.value.status_code == 422
    assert exc_info.value.detail == "Select at least one quote"


def test_refresh_job_limit_rejects_excess_work_with_retry_hint(monkeypatch) -> None:
    """Distinct refreshes are bounded before another background thread is created."""
    release = coin_data_api.threading.Event()
    monkeypatch.setattr(coin_data_api, "_REFRESH_ACTIVE_JOB_LIMIT", 2)

    def runner(_job_id, _total_steps):
        assert release.wait(5)
        return coin_data_api._RefreshJobOutcome("completed", "complete", {}, [], [])

    with coin_data_api._REFRESH_JOBS_LOCK:
        coin_data_api._REFRESH_JOBS.clear()
        coin_data_api._REFRESH_THREADS.clear()
        coin_data_api._REFRESH_ACCEPTING = True
    try:
        for key in ("first", "second"):
            coin_data_api._start_refresh_job("Refresh", "Running", 1, runner, coalesce_key=key)

        with pytest.raises(HTTPException) as exc_info:
            coin_data_api._start_refresh_job(
                "Refresh", "Running", 1, runner, coalesce_key="third"
            )

        assert exc_info.value.status_code == 429
        assert "2 active refresh jobs" in str(exc_info.value.detail)
        assert exc_info.value.headers == {"Retry-After": "2"}
        assert len(coin_data_api._REFRESH_THREADS) == 2
    finally:
        release.set()
        for thread in list(coin_data_api._REFRESH_THREADS.values()):
            thread.join(5)
        with coin_data_api._REFRESH_JOBS_LOCK:
            coin_data_api._REFRESH_JOBS.clear()
            coin_data_api._REFRESH_THREADS.clear()


@pytest.mark.parametrize("failed_stage", ["listings", "metadata"])
def test_cmc_false_stage_return_fails_immediately(failed_stage) -> None:
    """A False CMC fetch stage is a failure rather than an unchecked continuation."""
    fake = SimpleNamespace(
        fetch_data=lambda: failed_stage != "listings",
        load_data=lambda: None,
        fetch_metadata=lambda: failed_stage != "metadata",
        load_metadata=lambda: None,
    )

    with pytest.raises(RuntimeError, match=f"{failed_stage} fetch returned False"):
        coin_data_api._refresh_cmc_data(fake, "missing-job", 6)


def test_cmc_refresh_relies_on_locked_publication_and_keeps_progress_monotonic(monkeypatch) -> None:
    """CMC orchestration never republishes and retains the existing six progress positions."""
    events = []
    fake = SimpleNamespace(
        fetch_data=lambda: events.append("fetch_data") or True,
        load_data=lambda: events.append("load_data"),
        fetch_metadata=lambda: events.append("fetch_metadata") or True,
        load_metadata=lambda: events.append("load_metadata"),
        save_data=lambda: pytest.fail("listings must only be published inside fetch_data"),
        save_metadata=lambda: pytest.fail("metadata must only be published inside fetch_metadata"),
    )
    progress = []
    monkeypatch.setattr(
        coin_data_api,
        "_set_refresh_job_progress",
        lambda _job_id, step, total, message: progress.append((step, total, message)),
    )

    coin_data_api._refresh_cmc_data(fake, "job-id", 12)

    assert events == ["fetch_data", "load_data", "fetch_metadata", "load_metadata"]
    assert [step for step, _total, _message in progress] == list(range(6))
    assert all(total == 12 for _step, total, _message in progress)
    assert "published" in progress[1][2]
    assert "published" in progress[4][2]


def test_failed_refresh_job_retains_diagnostics_for_polling() -> None:
    """Terminal error records keep readable state and per-exchange diagnostics."""
    job_id = coin_data_api._create_refresh_job("Refresh", "Running", 2)
    try:
        coin_data_api._fail_refresh_job(
            job_id,
            "No exchanges refreshed successfully",
            state={"usable": True},
            exchange_results=[{"exchange": "bitget", "ok": False, "error": "offline"}],
            warnings=["bitget: offline"],
        )
        job = coin_data_api._get_refresh_job(job_id)
        assert job["status"] == "error"
        assert job["state"] == {"usable": True}
        assert job["exchange_results"][0]["exchange"] == "bitget"
        assert job["warnings"] == ["bitget: offline"]
    finally:
        with coin_data_api._REFRESH_JOBS_LOCK:
            coin_data_api._REFRESH_JOBS.pop(job_id, None)


def test_coin_data_frontend_applies_partial_state_and_summarizes_failures() -> None:
    """Polling applies partial state and visibly names failed exchanges."""
    source = Path("frontend/coin_data.html").read_text(encoding="utf-8")
    message_start = source.index("    function refreshJobMessage(")
    message_end = source.index("\n    function toggleButtonState(", message_start)
    poll_start = source.index("    function pollRefreshJob(")
    poll_end = source.index("\n    function startRefreshJobPolling(", poll_start)
    functions = source[message_start:message_end] + "\n" + source[poll_start:poll_end]
    script = textwrap.dedent(
        f"""
        const assert = require('node:assert/strict');
        var busyJobId = 'partial-job';
        var applied = null;
        var status = null;
        var hidden = false;
        function buildRefreshJobUrl() {{ return '/job'; }}
        function updateBusyProgress() {{}}
        function applyServerState(state) {{ applied = state; }}
        function setActionStatus(message, level) {{ status = [message, level]; }}
        function hideBusy() {{ hidden = true; }}
        function fetch() {{
          return Promise.resolve({{
            ok: true,
            json: function () {{
              return Promise.resolve({{job: {{
                status: 'partial',
                percent: 100,
                message: '1/2 exchanges refreshed',
                state: {{generation: 2}},
                exchange_results: [
                  {{exchange: 'binance', ok: true}},
                  {{exchange: 'bitget', ok: false}}
                ]
              }}}});
            }}
          }});
        }}
        {functions}
        (async function () {{
          await pollRefreshJob('partial-job', 'fallback');
          assert.deepEqual(applied, {{generation: 2}});
          assert.match(status[0], /Failed: bitget/);
          assert.equal(status[1], 'warning');
          assert.equal(hidden, true);
        }})().catch(function (error) {{ console.error(error); process.exit(1); }});
        """
    )
    completed = subprocess.run(
        ["node", "-e", script],
        text=True,
        capture_output=True,
        timeout=10,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr or completed.stdout


def test_coin_data_frontend_reports_partial_price_coverage() -> None:
    """Partial ticker responses show explicit priced/requested coverage."""
    source = Path("frontend/coin_data.html").read_text(encoding="utf-8")
    start = source.index("    function refreshJobMessage(")
    end = source.index("\n    function toggleButtonState(", start)
    script = textwrap.dedent(
        f"""
        const assert = require('node:assert/strict');
        {source[start:end]}
        const message = refreshJobMessage({{
          result_message: 'Refresh incomplete',
          exchange_results: [{{
            exchange: 'hyperliquid',
            ok: false,
            partial: true,
            price_update: {{requested: 3, priced: 1, missing: 2}}
          }}]
        }}, 'fallback');
        assert.ok(message.includes('Partial price coverage: hyperliquid 1/3.'));
        """
    )
    completed = subprocess.run(
        ["node", "-e", script],
        text=True,
        capture_output=True,
        timeout=10,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr or completed.stdout


def test_coin_data_frontend_omits_quotes_until_selection_exists() -> None:
    """The browser uses omission for defaults and sends quotes only after selection."""
    source = Path("frontend/coin_data.html").read_text(encoding="utf-8")
    start = source.index("    function buildRefreshPayload(")
    end = source.index("\n    function updateBusyProgress(", start)
    script = textwrap.dedent(
        f"""
        const assert = require('node:assert/strict');
        var filters = {{
          exchange: 'binance', market_cap: 0, vol_mcap: 10,
          tags: [], quotes: [], only_cpt: false
        }};
        function supportsCopyTradingFilter() {{ return true; }}
        {source[start:end]}
        const defaults = buildRefreshPayload();
        assert.equal(Object.hasOwn(defaults, 'quotes'), false);
        filters.quotes = ['USDT'];
        assert.deepEqual(buildRefreshPayload().quotes, ['USDT']);
        """
    )
    completed = subprocess.run(
        ["node", "-e", script],
        text=True,
        capture_output=True,
        timeout=10,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr or completed.stdout
