"""Offline tests for the PB8 Strategy Explorer subprocess client and API router."""

from __future__ import annotations

import asyncio
import json
import subprocess
import time
from pathlib import Path

import pytest
from fastapi import HTTPException
from starlette.requests import Request

from api.auth import SessionToken
from api import backtest_v8
from api import strategy_explorer_v8 as api
from api import strategy_explorer as api_v7
import pb8_strategy_explorer as client


@pytest.fixture(autouse=True)
def _reset_client_lifecycle():
    """Keep helper lifecycle state isolated from API lifespan tests and sibling cases."""
    client.startup()
    yield
    client.shutdown()
    client.startup()


class _Lease:
    """Minimal idempotent runtime lease used by subprocess tests."""

    def __init__(self) -> None:
        self.released = False

    def release(self) -> None:
        """Record lease release."""
        self.released = True


class _Process:
    """Configurable subprocess facade implementing the client lifecycle surface."""

    def __init__(self, stdout: bytes = b'{"ok":true,"result":{"ok":true}}') -> None:
        self.stdout = stdout
        self.returncode = 0
        self.input = b""
        self.terminated = False
        self.killed = False

    def communicate(self, input: bytes, timeout: float) -> tuple[bytes, bytes]:
        """Capture stdin and return the configured response."""
        self.input = input
        assert timeout <= 300
        return self.stdout, b""

    def poll(self):
        """Report a running process until terminated."""
        return self.returncode if self.terminated or self.killed else None

    def terminate(self) -> None:
        """Record graceful termination."""
        self.terminated = True

    def wait(self, timeout: float) -> int:
        """Return after a recorded termination."""
        if not self.terminated and not self.killed:
            raise subprocess.TimeoutExpired("helper", timeout)
        return self.returncode

    def kill(self) -> None:
        """Record forced termination."""
        self.killed = True


def _session(token: str) -> SessionToken:
    """Build one authenticated session object for direct endpoint calls."""
    return SessionToken(token=token, user_id=token, created_at=1.0, expires_at=time.time() + 3600)


def _runtime(tmp_path: Path) -> dict:
    """Return a ready mocked PB8 runtime status."""
    return {"ready": True, "pb8dir": str(tmp_path), "pb8venv": str(tmp_path / "venv" / "python"), "errors": []}


def test_client_uses_configured_executable_cwd_and_stdin(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """The API process must launch only the configured interpreter and pass payload via stdin."""
    process = _Process()
    captured = {}
    lease = _Lease()

    def popen(argv, **kwargs):
        captured.update({"argv": argv, **kwargs})
        return process

    monkeypatch.setattr(client, "pb8_runtime_status", lambda: _runtime(tmp_path))
    monkeypatch.setattr(client, "acquire_master_runtime_lock", lambda _path: lease)
    monkeypatch.setattr(client.subprocess, "Popen", popen)

    result = client.snapshot({"live": {}}, {"coin": "BTC"}, operation_id="operation-1")

    assert result == {"ok": True}
    assert captured["argv"][0] == str(tmp_path / "venv" / "python")
    assert captured["cwd"] == str(tmp_path)
    assert captured["shell"] is False
    request = json.loads(process.input)
    assert request["operation"] == "snapshot"
    assert request["config"] == {"live": {}}
    assert captured["argv"] == [
        str(tmp_path / "venv" / "python"),
        str(Path(client.__file__).resolve().with_name("pb8_strategy_explorer_helper.py")),
    ]
    assert lease.released is True


def test_client_rejects_oversized_and_malformed_responses(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Malformed and over-limit helper output must fail before becoming API payloads."""
    monkeypatch.setattr(client, "pb8_runtime_status", lambda: _runtime(tmp_path))
    monkeypatch.setattr(client, "acquire_master_runtime_lock", lambda _path: _Lease())
    responses = iter([_Process(b"not-json"), _Process(b"x" * (client._MAX_RESPONSE_BYTES + 1))])
    monkeypatch.setattr(client.subprocess, "Popen", lambda *_args, **_kwargs: next(responses))

    with pytest.raises(client.PB8StrategyExplorerError, match="Invalid"):
        client.capabilities(operation_id="malformed")
    with pytest.raises(client.PB8StrategyExplorerError, match="32 MiB"):
        client.capabilities(operation_id="oversized")


def test_client_rejects_oversized_request_before_process_launch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A request over 2 MiB must never reach runtime locking or subprocess creation."""
    launched = []
    monkeypatch.setattr(client.subprocess, "Popen", lambda *_args, **_kwargs: launched.append(True))

    with pytest.raises(client.PB8StrategyExplorerError, match="2 MiB"):
        client.snapshot({"oversized": "x" * (client._MAX_REQUEST_BYTES + 1)})

    assert launched == []


def test_client_timeout_terminates_owned_process(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A timed-out helper must be terminated and reported as a bounded failure."""
    process = _Process()

    def communicate(*_args, **_kwargs):
        raise subprocess.TimeoutExpired("helper", 1)

    process.communicate = communicate
    monkeypatch.setattr(client, "pb8_runtime_status", lambda: _runtime(tmp_path))
    monkeypatch.setattr(client, "acquire_master_runtime_lock", lambda _path: _Lease())
    monkeypatch.setattr(client.subprocess, "Popen", lambda *_args, **_kwargs: process)

    with pytest.raises(client.PB8StrategyExplorerError, match="timed out"):
        client._call_helper("replay", {}, operation_id="timeout", timeout=999)
    assert process.terminated is True


def test_cancel_and_shutdown_only_stop_registered_helpers() -> None:
    """Cancellation and shutdown must act only on the client's owned registry."""
    client.startup()
    first = _Process()
    second = _Process()
    with client._PROCESS_LOCK:
        client._PROCESSES.update({"first": first, "second": second})
    try:
        assert client.cancel("missing") is False
        assert client.cancel("first") is True
        assert first.terminated is True
        assert second.terminated is False
        client.shutdown()
        assert second.terminated is True
        assert client._PROCESSES == {}
    finally:
        with client._PROCESS_LOCK:
            client._PROCESSES.clear()
            client._PENDING.clear()
            client._CANCELLED.clear()
        client.startup()


def test_api_module_does_not_import_pb8_runtime_modules() -> None:
    """The API layer may import the subprocess client but no PB8 source module."""
    source = Path(client.__file__).read_text(encoding="utf-8")
    forbidden = ("import backtest", "import passivbot_rust", "from config.")
    assert not any(token in source for token in forbidden)


def test_main_page_uses_cookie_auth_placeholders_without_session_token() -> None:
    """The shared HTML must receive the V8 API base but never the authenticated token."""
    secret = "never-render-this-session-token"
    request = Request(
        {
            "type": "http",
            "http_version": "1.1",
            "method": "GET",
            "scheme": "https",
            "path": "/api/strategy-explorer-v8/main_page",
            "raw_path": b"/api/strategy-explorer-v8/main_page",
            "query_string": b"",
            "headers": [(b"host", b"attacker.example")],
            "client": ("127.0.0.1", 1234),
            "server": ("example.test", 443),
            "root_path": "/api/team",
        }
    )

    response = api.main_page(request=request, draft_id="opaque-id", session=_session(secret))
    body = response.body.decode("utf-8")

    assert secret not in body
    assert "/api/team/api/strategy-explorer-v8" in body
    assert "attacker.example" not in body
    assert "/api/team/app/" in body
    assert "opaque-id" in body
    assert "%%API_BASE%%" not in body


def test_main_page_script_escapes_untrusted_draft_id() -> None:
    """A query value must not be able to terminate the inline bootstrap script."""
    request = Request(
        {
            "type": "http",
            "http_version": "1.1",
            "method": "GET",
            "scheme": "https",
            "path": "/api/strategy-explorer-v8/main_page",
            "raw_path": b"/api/strategy-explorer-v8/main_page",
            "query_string": b"",
            "headers": [],
            "client": ("127.0.0.1", 1234),
            "server": ("example.test", 443),
            "root_path": "",
        }
    )

    response = api.main_page(
        request=request,
        draft_id='</script><script id="injected">',
        session=_session("owner"),
    )
    body = response.body.decode("utf-8")

    assert '<script id="injected">' not in body
    assert "\\u003c/script\\u003e\\u003cscript id=" in body


def test_shared_v7_page_script_escapes_untrusted_query_values() -> None:
    """The PB7 route sharing the template must escape both query placeholders."""
    request = Request(
        {
            "type": "http",
            "http_version": "1.1",
            "method": "GET",
            "scheme": "https",
            "path": "/api/strategy-explorer/main_page",
            "raw_path": b"/api/strategy-explorer/main_page",
            "query_string": b"",
            "headers": [],
            "client": ("127.0.0.1", 1234),
            "server": ("example.test", 443),
            "root_path": "",
        }
    )

    response = api_v7.main_page(
        request=request,
        draft_id="</script><script>draft",
        result_path="</script><script>result",
        session=_session("owner"),
    )
    body = response.body.decode("utf-8")

    assert "</script><script>draft" not in body
    assert "</script><script>result" not in body
    assert body.count("\\u003c/script\\u003e\\u003cscript\\u003e") >= 2


def test_owner_bound_draft_isolation_and_no_result_path_response(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Draft ids must be opaque, owner-bound, and must not reveal validated result paths."""
    api._draft_store.clear()
    monkeypatch.setattr(api, "_canonical_config", lambda config: {"canonical": config})
    result_path = tmp_path / "secret-result"
    monkeypatch.setattr(api, "_validate_result_path", lambda _value: str(result_path))
    owner = _session("owner-token")
    other = _session("other-token")

    created = api.create_draft(
        {
            "config": {
                "x": 1,
                "coin_overrides": {"BTC": {"override_config_path": "BTC.json"}},
            },
            "result_path": str(result_path),
            "compare_config": {"y": 2},
            "override_configs": {"BTC.json": {"x": 2}},
        },
        owner,
    )
    loaded = api.get_draft(created["draft_id"], owner)

    assert loaded["config"]["canonical"]["coin_overrides"]["BTC"]["x"] == 2
    assert "override_config_path" not in loaded["config"]["canonical"]["coin_overrides"]["BTC"]
    assert loaded["compare_available"] is True
    assert "result_path" not in loaded
    assert str(result_path) not in json.dumps(loaded)
    with pytest.raises(HTTPException) as exc_info:
        api.get_draft(created["draft_id"], other)
    assert exc_info.value.status_code == 404


def test_sparse_overrides_require_payloads_and_preserve_inline_precedence() -> None:
    """Sparse files apply first while explicit per-coin values remain authoritative."""
    config = {
        "coin_overrides": {
            "BTC": {
                "override_config_path": "BTC.json",
                "bot": {"long": {"risk": {"n_positions": 3}}},
            }
        }
    }

    merged = api._merge_sparse_overrides(
        config,
        {"BTC.json": {"bot": {"long": {"risk": {"n_positions": 1, "total_wallet_exposure_limit": 2.0}}}}},
    )

    risk = merged["coin_overrides"]["BTC"]["bot"]["long"]["risk"]
    assert risk == {"n_positions": 3, "total_wallet_exposure_limit": 2.0}
    assert "override_config_path" not in merged["coin_overrides"]["BTC"]
    with pytest.raises(HTTPException, match="Override config not found"):
        api._merge_sparse_overrides(config, {})


def test_result_draft_loads_referenced_sparse_overrides_server_side(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A result handoff must resolve sparse files without exposing them in result rows."""
    from api import backtest_v8

    api._draft_store.clear()
    config = {"coin_overrides": {"BTC": {"override_config_path": "BTC.json"}}}
    monkeypatch.setattr(api, "_validate_result_path", lambda _value: str(tmp_path))
    monkeypatch.setattr(api, "_canonical_config", lambda value: value)
    monkeypatch.setattr(
        backtest_v8,
        "_load_override_payloads",
        lambda received, directory: {"BTC.json": {"bot": {"long": {"risk": {"n_positions": 2}}}}},
    )

    created = api.create_draft(
        {"config": config, "result_path": str(tmp_path), "provenance": {"kind": "backtest_result"}},
        _session("result-owner"),
    )
    loaded = api.get_draft(created["draft_id"], _session("result-owner"))

    assert loaded["config"]["coin_overrides"]["BTC"]["bot"]["long"]["risk"]["n_positions"] == 2


def test_session_uses_draft_id_without_exposing_filesystem_path(monkeypatch: pytest.MonkeyPatch) -> None:
    """Bootstrap data must identify handoff provenance only through its opaque draft id."""
    api._draft_store.clear()
    owner = _session("owner")
    now = time.time()
    draft_id = "opaque"
    api._draft_store[draft_id] = {
        "owner": api._owner(owner),
        "created_at": now,
        "touched_at": now,
        "config": {"bot": {}},
        "result_path": "/managed/private/result",
        "override_configs": {},
        "provenance": {"source": "backtest"},
    }
    monkeypatch.setattr(
        api.explorer,
        "capabilities",
        lambda _config=None: {"strategy": {"supported_kinds": ["dynamic"]}, "simulation_modes": []},
    )
    monkeypatch.setattr(
        api.explorer,
        "snapshot",
        lambda _config, _options: {"ok": True, "messages": [], "sides": {"long": {}, "short": {}}},
    )

    result = api.get_session(draft_id=draft_id, session=owner)

    assert result["draft_id"] == draft_id
    assert "/managed/private/result" not in json.dumps(result)
    assert "result_path" not in result


def test_session_returns_renderable_config_when_native_data_is_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A missing result-data window must not leave the entire Explorer shell blank."""
    api._draft_store.clear()
    owner = _session("fallback-owner")
    now = time.time()
    api._draft_store["fallback"] = {
        "owner": api._owner(owner),
        "created_at": now,
        "touched_at": now,
        "config": {
            "backtest": {"exchanges": ["binance"], "start_date": "2020-01-01"},
            "live": {
                "strategy_kind": "ema",
                "approved_coins": {"long": ["BTC"], "short": ["BTC"]},
            },
            "bot": {
                "long": {"risk": {"n_positions": 1, "total_wallet_exposure_limit": 1.0}},
                "short": {"risk": {"n_positions": 0, "total_wallet_exposure_limit": 0.0}},
            },
        },
        "compare_config": None,
        "result_path": "",
        "override_configs": {},
        "provenance": {},
    }
    monkeypatch.setattr(
        api.explorer,
        "capabilities",
        lambda _config=None: {
            "strategy": {
                "supported_kinds": ["ema"],
                "param_groups": [{"key": "risk", "label": "Risk", "fields": ["n_positions"]}],
                "param_field_meta": {"n_positions": {"type": "number"}},
            },
            "simulation_modes": [{"key": "pb8_engine", "label": "PB8 Native Replay"}],
        },
    )

    def unavailable(_config, _options):
        raise client.PB8StrategyExplorerError("No valid coins found with data")

    monkeypatch.setattr(api.explorer, "snapshot", unavailable)

    result = api.get_session(draft_id="fallback", session=owner)

    assert result["ok"] is True
    assert result["snapshot"]["ok"] is False
    assert result["snapshot"]["market"]["exchange"] == "binance"
    assert result["snapshot"]["market"]["coin"] == "BTC"
    assert result["snapshot"]["sides"]["long"]["params"]["risk"]["n_positions"] == 1
    assert "No valid coins found with data" in result["messages"][0]["text"]


def test_result_initial_options_use_actual_combined_source_and_first_fill(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Combined PB8 handoffs must use the source exchange and time recorded by the result."""
    dataset_path = tmp_path / "dataset.json"
    fills_path = tmp_path / "fills.csv"
    dataset_path.write_text(
        json.dumps(
            {
                "exchange": "combined",
                "coins": ["HYPE"],
                "preparation": {
                    "source_selection": {
                        "HYPE": {
                            "selected_exchange": "bybit",
                            "selected_quality": {"first_ts": 1733443200000},
                        }
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    fills_path.write_text(
        "timestamp,coin,price\n2024-12-31 04:50:00,HYPE,25.0\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        backtest_v8,
        "_resolve_result_file",
        lambda _result_dir, filename: dataset_path if filename == "dataset.json" else fills_path,
    )

    result = api._result_initial_options(
        str(tmp_path),
        {
            "backtest": {"exchanges": ["binance", "bybit"]},
            "live": {"approved_coins": {"long": ["HYPE"], "short": []}},
        },
    )

    assert result == {
        "exchange": "bybit",
        "coin": "HYPE",
        "start_date": "2024-12-31",
        "start_time": "04:50",
    }


def test_result_initial_options_skip_unapproved_first_fill(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A replacement coin must use its own first fill timestamp, not another coin's timestamp."""
    dataset_path = tmp_path / "dataset.json"
    fills_path = tmp_path / "fills.csv"
    dataset_path.write_text(
        json.dumps(
            {
                "exchange": "bybit",
                "coins": ["HYPE"],
                "preparation": {
                    "source_selection": {
                        "HYPE": {
                            "selected_exchange": "bybit",
                            "selected_quality": {"first_ts": 1733443200000},
                        }
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    fills_path.write_text(
        "timestamp,coin,price\n2024-12-30 01:00:00,ETH,10.0\n2024-12-31 04:50:00,HYPE,25.0\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        backtest_v8,
        "_resolve_result_file",
        lambda _result_dir, filename: dataset_path if filename == "dataset.json" else fills_path,
    )

    result = api._result_initial_options(
        str(tmp_path),
        {
            "backtest": {"exchanges": ["bybit"]},
            "live": {"approved_coins": {"long": ["HYPE"], "short": []}},
        },
    )

    assert result["coin"] == "HYPE"
    assert result["start_date"] == "2024-12-31"
    assert result["start_time"] == "04:50"


def test_request_parts_rejects_ohlcv_source_outside_approved_roots(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Posted Strategy Explorer configs may not direct PB8 to arbitrary filesystem paths."""
    pb8_dir = tmp_path / "pb8"
    pb8_dir.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    monkeypatch.setattr(api, "pb8_runtime_status", lambda: {"pb8dir": str(pb8_dir)})

    with pytest.raises(HTTPException) as exc_info:
        api._request_parts(
            {"config": {"backtest": {"ohlcv_source_dir": str(outside)}}},
            _session("source-owner"),
        )

    assert exc_info.value.status_code == 400


def test_simulation_rejects_manual_state_and_compare_requires_source() -> None:
    """PB8 must reject unsupported manual replay state and source-less comparisons."""
    session = _session("operation-owner")
    with pytest.raises(HTTPException) as simulation_error:
        api.run_simulation(
            {"config": {"backtest": {}}, "options": {"sim_start_state": "manual"}},
            session,
        )
    assert simulation_error.value.status_code == 422

    with pytest.raises(HTTPException) as compare_error:
        api.run_compare({"config": {"backtest": {}}, "options": {}}, session)
    assert compare_error.value.status_code == 422


def test_expired_draft_does_not_block_config_only_snapshot_requests() -> None:
    """An initialized page must keep recalculating after its provenance draft expires."""
    api._draft_store.clear()
    config = {"backtest": {}, "live": {}, "bot": {}}

    resolved_config, options, entry = api._request_parts(
        {"config": config, "options": {"draft_id": "expired", "coin": "HYPE"}},
        _session("expired-owner"),
    )

    assert resolved_config is config
    assert options == {"coin": "HYPE"}
    assert entry is None


def test_expired_draft_still_blocks_result_provenance_operations() -> None:
    """Compare must not silently lose its owner-bound stored-result provenance."""
    api._draft_store.clear()

    with pytest.raises(HTTPException) as exc_info:
        api._request_parts(
            {
                "config": {"backtest": {}, "live": {}, "bot": {}},
                "options": {"draft_id": "expired"},
            },
            _session("expired-owner"),
            require_draft=True,
        )

    assert exc_info.value.status_code == 404
    assert exc_info.value.detail == "draft not found"


def test_progress_is_owner_bound_and_bounded() -> None:
    """Polling records must not cross session ownership boundaries."""
    api._progress_store.clear()
    owner = _session("owner-progress")
    other = _session("other-progress")
    operation_id = api._progress_begin("poll-id", "Simulation", owner)
    api._progress_update("poll-id", operation_id=operation_id, progress=0.5, message="Halfway")

    assert api.get_simulation_progress("poll-id", owner)["progress"] == pytest.approx(0.5)
    hidden = api.get_simulation_progress("poll-id", other)
    assert hidden["ok"] is False
    assert hidden["done"] is True


def test_stale_operation_cannot_overwrite_reused_progress_id() -> None:
    """Late completion from an old operation must not replace a newer poll record."""
    api._progress_store.clear()
    owner = _session("owner-progress")
    old_operation = api._progress_begin("poll-id", "Simulation", owner)
    api._progress_update(
        "poll-id",
        operation_id=old_operation,
        progress=1.0,
        message="Old completion",
        done=True,
    )
    new_operation = api._progress_begin("poll-id", "Simulation", owner)

    api._progress_update(
        "poll-id",
        operation_id=old_operation,
        progress=1.0,
        message="Old completion",
        done=True,
    )
    api._progress_update(
        "poll-id",
        operation_id=new_operation,
        progress=0.25,
        message="Current operation",
    )

    current = api.get_simulation_progress("poll-id", owner)
    assert current["progress"] == pytest.approx(0.25)
    assert current["message"] == "Current operation"
    assert current["done"] is False


def test_active_progress_id_cannot_be_reused() -> None:
    """A same-owner retry may not orphan an unfinished helper operation."""
    api._progress_store.clear()
    owner = _session("active-progress")
    api._progress_begin("poll-id", "Simulation", owner)

    with pytest.raises(HTTPException, match="active operation") as exc_info:
        api._progress_begin("poll-id", "Simulation", owner)

    assert exc_info.value.status_code == 409


def test_movie_export_has_payload_output_and_concurrency_limits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PB8 movie exports must reject excessive or concurrent resource usage."""
    owner = _session("movie-owner")
    monkeypatch.setattr(api, "_MAX_MOVIE_EXPORT_PAYLOAD_BYTES", 10)
    with pytest.raises(HTTPException) as payload_error:
        api.export_movie({"figure": {"frames": ["too-large"]}}, owner)
    assert payload_error.value.status_code == 413

    monkeypatch.setattr(api, "_MAX_MOVIE_EXPORT_PAYLOAD_BYTES", 1024 * 1024)
    from api import strategy_explorer_export

    assert strategy_explorer_export._EXPORT_SLOT.acquire(blocking=False) is True
    try:
        with pytest.raises(HTTPException) as busy_error:
            api.export_movie({"figure": {"frames": [{}]}}, owner)
        assert busy_error.value.status_code == 503
    finally:
        strategy_explorer_export._EXPORT_SLOT.release()

    monkeypatch.setattr(api, "_MAX_MOVIE_EXPORT_OUTPUT_BYTES", 3)
    monkeypatch.setattr(
        strategy_explorer_export,
        "export_plotly_animation_to_mp4",
        lambda *_args, **_kwargs: (b"four", {"codec": "test"}),
    )
    with pytest.raises(HTTPException) as output_error:
        api.export_movie({"figure": {"frames": [{}]}}, owner)
    assert output_error.value.status_code == 413
    assert strategy_explorer_export._EXPORT_SLOT.acquire(blocking=False) is True
    strategy_explorer_export._EXPORT_SLOT.release()


def test_movie_export_without_progress_id_is_not_cancelled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Polling is optional, so an export without a progress record must continue."""
    from api import strategy_explorer_export

    def export_stub(_figure, **kwargs):
        assert kwargs["cancel_cb"]() is False
        return b"movie", {"codec": "test"}

    monkeypatch.setattr(strategy_explorer_export, "export_plotly_animation_to_mp4", export_stub)

    response = api.export_movie({"figure": {"frames": [{}]}}, _session("no-progress"))

    assert response.body == b"movie"


def test_movie_export_maps_pre_read_output_limit_to_413(monkeypatch: pytest.MonkeyPatch) -> None:
    """The shared exporter's pre-read size rejection must retain its HTTP status."""
    from api import strategy_explorer_export

    def export_stub(*_args, **_kwargs):
        raise strategy_explorer_export.MovieExportTooLargeError(
            "Movie export output exceeds the 512 MiB limit"
        )

    monkeypatch.setattr(strategy_explorer_export, "export_plotly_animation_to_mp4", export_stub)

    with pytest.raises(HTTPException) as exc_info:
        api.export_movie({"figure": {"frames": [{}]}}, _session("large-output"))

    assert exc_info.value.status_code == 413


def test_client_cancels_pending_launch_and_shutdown_stays_closed() -> None:
    """Cancellation and shutdown must cover operations before subprocess registration."""
    pending_id = "pending-operation"
    client.startup()
    with client._PROCESS_LOCK:
        client._PENDING.add(pending_id)
    try:
        assert client.cancel(pending_id) is True
        assert pending_id in client._CANCELLED
        client.shutdown()
        assert client._SHUTTING_DOWN is True
    finally:
        with client._PROCESS_LOCK:
            client._PENDING.clear()
            client._CANCELLED.clear()
        client.startup()


def test_draft_expiration_uses_last_touch_time(monkeypatch: pytest.MonkeyPatch) -> None:
    """An actively used draft must not expire based only on its creation time."""
    api._draft_store.clear()
    now = time.time()
    api._draft_store["active"] = {
        "owner": "owner",
        "created_at": now - api._DRAFT_TTL_SECONDS - 1,
        "touched_at": now,
    }
    monkeypatch.setattr(api.time, "time", lambda: now)

    api._clean_stores()

    assert "active" in api._draft_store


def test_compare_uses_owner_bound_pareto_baseline(monkeypatch: pytest.MonkeyPatch) -> None:
    """A pinned Pareto baseline must reach compare without returning it to the browser."""
    api._draft_store.clear()
    owner = _session("compare-owner")
    now = time.time()
    api._draft_store["compare-draft"] = {
        "owner": api._owner(owner),
        "created_at": now,
        "touched_at": now,
        "config": {"bot": {"long": {}}},
        "compare_config": {"bot": {"long": {"risk": {"n_positions": 2}}}},
        "result_path": "",
        "override_configs": {},
        "provenance": {"kind": "optimize_result"},
    }
    captured = {}

    def compare(config, options, **kwargs):
        captured.update({"config": config, "options": options, **kwargs})
        return {"ok": True, "message": "Compared", "summary": {}, "rows": {}}

    monkeypatch.setattr(api.explorer, "compare", compare)

    result = api.run_compare(
        {
            "config": {"bot": {"long": {}}},
            "options": {"draft_id": "compare-draft"},
        },
        owner,
    )

    assert result["ok"] is True
    assert captured["compare_config"]["bot"]["long"]["risk"]["n_positions"] == 2


def test_shutdown_is_idempotent_and_clears_stores(monkeypatch: pytest.MonkeyPatch) -> None:
    """API shutdown must stop helpers and clear both bounded stores on every call."""
    calls = []
    monkeypatch.setattr(api.explorer, "shutdown", lambda: calls.append("shutdown"))
    api._draft_store["x"] = {"touched_at": time.time()}
    api._progress_store["y"] = {"updated_at": time.time()}

    asyncio.run(api.shutdown())
    asyncio.run(api.shutdown())

    assert calls == ["shutdown", "shutdown"]
    assert api._draft_store == {}
    assert api._progress_store == {}


@pytest.fixture
def cached_runtime(monkeypatch, tmp_path):
    """Provide only synthetic runtime metadata and fresh bounded admission state."""
    import threading

    monkeypatch.setattr(client, "pb8_runtime_status", lambda: _runtime(tmp_path))
    monkeypatch.setattr(client, "_runtime_fingerprint", lambda _status: ("build-a",))
    monkeypatch.setattr(client, "_HELPER_SLOTS", threading.BoundedSemaphore(2))
    monkeypatch.setattr(client, "_ADMISSION_SLOTS", threading.BoundedSemaphore(16))
    monkeypatch.setattr(client, "_log", lambda *args, **kwargs: None)
    return tmp_path


def _assert_capacity_restored():
    """Assert all shared permits and owned registries are available after a call."""
    for semaphore, count in ((client._ADMISSION_SLOTS, 16), (client._HELPER_SLOTS, 2)):
        for _ in range(count):
            assert semaphore.acquire(blocking=False)
        assert not semaphore.acquire(blocking=False)
        for _ in range(count):
            semaphore.release()
    assert client._PROCESSES == {}
    assert client._PENDING == set()
    assert client._CANCELLED == set()


def test_capabilities_cache_ttl_copy_readiness_and_bypass(cached_runtime, monkeypatch):
    """Defaults share isolated copies for 30 seconds; explicit configs never share."""
    calls = []
    clock = [10.0]
    monkeypatch.setattr(client.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(client, "_call_helper", lambda *args, **kwargs: calls.append((args, kwargs)) or {"nested": {"value": 1}})
    first = client.capabilities()
    first["nested"]["value"] = 9
    assert client.capabilities()["nested"]["value"] == 1
    assert len(calls) == 1
    clock[0] = 40.0
    assert client.capabilities()["nested"]["value"] == 1
    assert len(calls) == 2
    for config in ({}, {"bot": {}}):
        client.capabilities(config)
        client.capabilities(config)
    client.capabilities(operation_id="explicit")
    client.capabilities(operation_id="")
    assert len(calls) == 8
    monkeypatch.setattr(client, "pb8_runtime_status", lambda: {"ready": False, "errors": ["synthetic unavailable"]})
    with pytest.raises(client.PB8StrategyExplorerUnavailableError):
        client.capabilities()
    _assert_capacity_restored()


@pytest.mark.parametrize("during_calculation", [False, True])
def test_capabilities_runtime_change_invalidates(cached_runtime, monkeypatch, during_calculation):
    """Runtime changes invalidate warm values and discard a changed cold result."""
    build = [1]
    calls = []
    monkeypatch.setattr(client, "_runtime_fingerprint", lambda _status: (build[0],))

    def calculate(*args, **kwargs):
        """Optionally change the runtime while a helper is calculating."""
        calls.append(build[0])
        if during_calculation:
            build[0] += 1
        return {"build": calls[-1]}

    monkeypatch.setattr(client, "_call_helper", calculate)
    if during_calculation:
        for _ in range(2):
            with pytest.raises(client.PB8StrategyExplorerBusyError, match="runtime changed"):
                client.capabilities()
        assert client._CAPABILITIES_CACHE is None
    else:
        assert client.capabilities() == {"build": 1}
        build[0] = 2
        assert client.capabilities() == {"build": 2}
    assert len(calls) == 2
    _assert_capacity_restored()


@pytest.mark.parametrize("error_class,translated", [
    (client.PB8RuntimeBusyError, client.PB8StrategyExplorerBusyError),
    (client.PB8ConfigurationError, client.PB8StrategyExplorerUnavailableError),
])
def test_fingerprint_errors_are_retryable_http_503_without_cache(cached_runtime, monkeypatch, error_class, translated):
    """Fingerprint failures retain exception chaining and reach HTTP as 503, never 500."""
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from api.auth import require_auth

    fault = error_class("synthetic fingerprint failure")
    calls = []

    def fail(_status):
        """Fail only at the shared runtime fingerprint boundary."""
        calls.append(True)
        raise fault

    monkeypatch.setattr(client, "_runtime_fingerprint", fail)
    monkeypatch.setattr(api, "_log", lambda *args, **kwargs: None)
    with pytest.raises(translated) as caught:
        client.capabilities()
    assert caught.value.__cause__ is fault
    app = FastAPI()
    app.include_router(api.router)
    app.dependency_overrides[require_auth] = lambda: _session("synthetic")
    with TestClient(app) as http:
        response = http.get("/session")
    assert response.status_code == 503
    assert len(calls) == 2
    assert client._CAPABILITIES_CACHE is None
    _assert_capacity_restored()


def test_fingerprint_scan_never_holds_explorer_lock_and_cache_hit_holds_no_permit(cached_runtime, monkeypatch):
    """Scans keep cancellation locks free; warm hits release their brief admission."""
    from concurrent.futures import ThreadPoolExecutor

    def fingerprint(_status):
        """Prove lock availability from a different thread, not RLock reentry."""
        def probe():
            """Acquire the process/condition lock without blocking."""
            acquired = client._PROCESS_LOCK.acquire(blocking=False)
            if acquired:
                client._PROCESS_LOCK.release()
            return acquired
        with ThreadPoolExecutor(1) as pool:
            assert pool.submit(probe).result(timeout=2)
        return ("build-a",)

    monkeypatch.setattr(client, "_runtime_fingerprint", fingerprint)
    monkeypatch.setattr(client, "_call_helper", lambda *args, **kwargs: {"ok": True})
    client.capabilities()
    for _ in range(15):
        assert client._ADMISSION_SLOTS.acquire(blocking=False)
    try:
        assert client.capabilities() == {"ok": True}
    finally:
        for _ in range(15):
            client._ADMISSION_SLOTS.release()
    _assert_capacity_restored()


@pytest.mark.parametrize("outcome", ["success", "error", "shutdown"])
def test_singleflight_mixed_budget_and_lifecycle(cached_runtime, monkeypatch, outcome):
    """One leader, one other helper and 14 followers exhaust exactly 16 permits."""
    import threading
    from concurrent.futures import ThreadPoolExecutor

    entered = threading.Event()
    release = threading.Event()
    follower_count = [0]
    followers_ready = threading.Event()
    calls = []
    original_wait = client._CAPABILITIES_CONDITION.wait

    def wait(timeout=None):
        """Signal when every follower has acquired its shared-budget permit."""
        follower_count[0] += 1
        if follower_count[0] == 14:
            followers_ready.set()
        return original_wait(timeout)

    class BlockingProcess(_Process):
        """Keep a real client helper slot occupied using an event, without PB8."""

        def communicate(self, input, timeout):
            """Block until all followers have joined the same flight."""
            calls.append(json.loads(input)["operation"])
            if len(calls) == 2:
                entered.set()
            assert release.wait(5)
            if outcome == "error":
                return b'{"ok": false, "detail": "synthetic flight error"}', b""
            return super().communicate(input, timeout)

    monkeypatch.setattr(client._CAPABILITIES_CONDITION, "wait", wait)
    monkeypatch.setattr(client, "acquire_master_runtime_lock", lambda _path: _Lease())
    monkeypatch.setattr(client.subprocess, "Popen", lambda *args, **kwargs: BlockingProcess())
    with ThreadPoolExecutor(16) as pool:
        leader = pool.submit(client.capabilities)
        other = pool.submit(client.snapshot, {})
        assert entered.wait(3)
        followers = [pool.submit(client.capabilities) for _ in range(14)]
        assert followers_ready.wait(3)
        assert len(calls) == 2
        with pytest.raises(client.PB8StrategyExplorerBusyError, match="request limit"):
            client.markets({})
        with pytest.raises(client.PB8StrategyExplorerBusyError, match="request limit"):
            client.capabilities()
        if outcome == "shutdown":
            client.shutdown()
            client.startup()  # old results must remain invalid after a new lifespan
        release.set()
        if outcome == "success":
            assert leader.result(3) == {"ok": True}
            assert other.result(3) == {"ok": True}
            values = [future.result(3) for future in followers]
            assert values == [{"ok": True}] * 14
            values[0]["mutated"] = True
            assert "mutated" not in values[1]
        else:
            errors = []
            for future in [leader, other, *followers]:
                with pytest.raises(client.PB8StrategyExplorerError) as exc:
                    future.result(3)
                errors.append(exc.value)
            if outcome == "error":
                assert all(error is errors[0] for error in errors[2:])
            assert client._CAPABILITIES_CACHE is None
    _assert_capacity_restored()


def test_singleflight_follower_uses_original_deadline(cached_runtime, monkeypatch):
    """Late followers do not extend a flight or spawn another helper after timeout."""
    import threading
    from concurrent.futures import ThreadPoolExecutor

    entered, release = threading.Event(), threading.Event()
    calls = []

    def calculate(*args, **kwargs):
        """Hold the flight while its monotonic deadline is moved into the past."""
        calls.append(True)
        entered.set()
        assert release.wait(3)
        return {"ok": True}

    monkeypatch.setattr(client, "_call_helper", calculate)
    with ThreadPoolExecutor(1) as pool:
        leader = pool.submit(client.capabilities)
        assert entered.wait(2)
        with client._CAPABILITIES_CONDITION:
            client._CAPABILITIES_FLIGHT.deadline = time.monotonic() - 1
        with pytest.raises(client.PB8StrategyExplorerBusyError, match="still loading"):
            client.capabilities()
        release.set()
        with pytest.raises(client.PB8StrategyExplorerBusyError):
            leader.result(2)
    assert calls == [True]
    assert client._CAPABILITIES_CACHE is None
    _assert_capacity_restored()


@pytest.mark.parametrize("operation", ["capabilities", "snapshot", "markets", "replay", "compare", "movie"])
@pytest.mark.parametrize("exit_kind", ["deadline", "cancel", "shutdown", "available"])
def test_slot_wait_policy_and_no_unacquired_release(cached_runtime, monkeypatch, operation, exit_kind):
    """Bootstrap waits at most 15s; replay rejects immediately and never releases a missing slot."""
    for _ in range(2):
        assert client._HELPER_SLOTS.acquire(blocking=False)
    clock = [0.0]
    waits = []
    leases = []
    released = [False]
    monkeypatch.setattr(client.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(client, "acquire_master_runtime_lock", lambda _path: leases.append(_Lease()) or leases[-1])
    monkeypatch.setattr(client.subprocess, "Popen", lambda *args, **kwargs: _Process())

    def pause(seconds):
        """Advance synthetic time and inject lifecycle events before slot acquisition."""
        assert not leases  # no runtime lease while waiting
        assert "waiting" in client._PENDING
        waits.append(seconds)
        clock[0] += seconds
        if exit_kind == "cancel":
            assert client.cancel("waiting")
        elif exit_kind == "shutdown":
            client.shutdown()
        elif exit_kind == "available":
            client._HELPER_SLOTS.release()
            released[0] = True

    monkeypatch.setattr(client.time, "sleep", pause)
    try:
        if operation in {"capabilities", "snapshot", "markets"} and exit_kind == "available":
            assert client._call_helper(operation, {}, operation_id="waiting") == {"ok": True}
            assert leases[0].released
        else:
            expected = client.PB8StrategyExplorerCancelledError if operation in {"capabilities", "snapshot", "markets"} and exit_kind in {"cancel", "shutdown"} else client.PB8StrategyExplorerBusyError
            with pytest.raises(expected):
                client._call_helper(operation, {}, operation_id="waiting")
        if operation in {"replay", "compare", "movie"}:
            assert waits == []
        elif exit_kind == "deadline":
            assert sum(waits) == pytest.approx(15.0)
        assert not leases or exit_kind == "available"
    finally:
        for _ in range(1 if released[0] else 2):
            client._HELPER_SLOTS.release()
    _assert_capacity_restored()


@pytest.mark.parametrize("failure", ["communicate", "lease", "pipes", "kill"])
def test_helper_cleanup_releases_every_resource_on_errors(cached_runtime, monkeypatch, failure):
    """Stop owned processes and release registry, pipes, leases, slots and admission on every exit."""
    lease = _Lease()
    process = _Process()
    closed = []

    class Pipe:
        """Record closure and optionally fail so later cleanups must still execute."""

        def close(self):
            """Close one synthetic pipe."""
            closed.append(True)
            if failure == "pipes":
                raise OSError("synthetic pipe error")

    output = process.stdout
    process.stdin = Pipe()
    process.stdout = Pipe()
    process.stderr = Pipe()

    def communicate(**kwargs):
        """Keep the synthetic response separate from the three owned pipe handles."""
        return output, b""

    process.communicate = communicate
    if failure == "communicate":
        def fail_communicate(**kwargs):
            """Represent an unexpected pipe failure while the helper is alive."""
            raise OSError("synthetic communicate error")
        process.communicate = fail_communicate
    if failure == "lease":
        def fail_release():
            """Prove a release failure cannot strand the semaphore permits."""
            lease.released = True
            raise OSError("synthetic lease error")
        lease.release = fail_release
    if failure == "kill":
        def slow_wait(timeout):
            """Force escalation with bounded waits at both stages."""
            assert timeout == 2
            raise subprocess.TimeoutExpired("helper", timeout)
        process.wait = slow_wait
    monkeypatch.setattr(client, "acquire_master_runtime_lock", lambda _path: lease)
    monkeypatch.setattr(client.subprocess, "Popen", lambda *args, **kwargs: process)
    if failure in {"communicate", "lease"}:
        with pytest.raises(OSError):
            client.snapshot({})
    else:
        assert client.snapshot({}) == {"ok": True}
    assert process.terminated
    assert process.killed == (failure == "kill")
    assert lease.released
    assert len(closed) == 3
    _assert_capacity_restored()


def test_helper_signature_invalidates_capabilities(cached_runtime, monkeypatch):
    """A changed Explorer helper invalidates defaults even with unchanged PB8 metadata."""
    original_stat = Path.stat
    revision = [1]
    calls = []

    def signature(path, *args, **kwargs):
        """Override only the synthetic helper signature without touching any file."""
        if path.name == "pb8_strategy_explorer_helper.py":
            from types import SimpleNamespace
            return SimpleNamespace(st_mtime_ns=revision[0], st_size=10)
        return original_stat(path, *args, **kwargs)

    monkeypatch.setattr(Path, "stat", signature)
    monkeypatch.setattr(client, "_call_helper", lambda *args, **kwargs: calls.append(True) or {"ok": True})
    client.capabilities()
    client.capabilities()
    revision[0] += 1
    client.capabilities()
    assert len(calls) == 2
    _assert_capacity_restored()


def test_session_and_draft_bursts_share_only_default_capabilities(cached_runtime, monkeypatch):
    """Real API callers share one default flight while draft canonicalization and snapshots stay fresh."""
    import threading
    import copy
    from concurrent.futures import ThreadPoolExecutor

    config = {"backtest": {"starting_balance": 1000}, "bot": {"long": {}, "short": {}}, "live": {}}
    calls = []
    calls_lock = threading.Lock()
    entered, release = threading.Event(), threading.Event()
    followers = [0]
    all_joined = threading.Event()
    original_wait = client._CAPABILITIES_CONDITION.wait

    def wait(timeout=None):
        """Release the flight only after all default sessions joined it."""
        followers[0] += 1
        if followers[0] == 3:
            all_joined.set()
        return original_wait(timeout)

    class SessionProcess(_Process):
        """Model canonical helper responses for the real API session chain."""

        def communicate(self, input, timeout):
            """Record each operation and gate only the first default calculation."""
            request = json.loads(input)
            with calls_lock:
                calls.append(request)
            if request['operation'] == 'capabilities' and 'config' not in request:
                entered.set()
                assert release.wait(4)
            self.terminated = True
            return json.dumps({"ok": True, "result": {"config": request.get('config', config), "canonical_config": request.get('config', config)}}).encode(), b''

    monkeypatch.setattr(client._CAPABILITIES_CONDITION, "wait", wait)
    monkeypatch.setattr(client, "acquire_master_runtime_lock", lambda _path: _Lease())
    monkeypatch.setattr(client.subprocess, "Popen", lambda *args, **kwargs: SessionProcess())
    monkeypatch.setattr(api, "_draft_store", {})
    session = _session('synthetic-burst')
    with ThreadPoolExecutor(4) as pool:
        first = pool.submit(api.get_session, '', session)
        assert entered.wait(2)
        rest = [pool.submit(api.get_session, '', session) for _ in range(3)]
        assert all_joined.wait(2)
        release.set()
        assert all(future.result(3)['ok'] for future in [first, *rest])
    assert len([call for call in calls if call['operation'] == 'capabilities']) == 1
    assert len([call for call in calls if call['operation'] == 'snapshot']) == 4
    # The actual canonicalization caller and draft session must bypass defaults,
    # including an explicit empty object, and use the bootstrap slot policy.
    draft_config = copy.deepcopy(config)
    draft_config['backtest']['starting_balance'] = 4321
    monkeypatch.setattr(api, '_draft', lambda draft_id, owner: {'config': draft_config})
    assert api.get_session('synthetic-draft', session)['snapshot']['config'] == draft_config
    assert api._canonical_config({}) == {}
    assert [call['config'] for call in calls if call['operation'] == 'capabilities' and 'config' in call] == [draft_config, {}]
    _assert_capacity_restored()


@pytest.mark.parametrize('phase', ['launch', 'communicate'])
def test_shutdown_during_launch_or_pipe_return_never_publishes(cached_runtime, monkeypatch, phase):
    """Even a shutdown/startup race rejects old results and cleans partial processes."""
    process = _Process()
    lease = _Lease()

    def popen(*args, **kwargs):
        """Simulate a process appearing just after the shutdown registry scan."""
        if phase == 'launch':
            client.shutdown()
            client.startup()
        return process

    if phase == 'communicate':
        def communicate(**kwargs):
            """Represent shutdown interrupting a pipe read."""
            client.shutdown()
            client.startup()
            raise ValueError('synthetic pipe closed')
        process.communicate = communicate
    monkeypatch.setattr(client, 'acquire_master_runtime_lock', lambda _path: lease)
    monkeypatch.setattr(client.subprocess, 'Popen', popen)
    with pytest.raises(client.PB8StrategyExplorerCancelledError):
        client.capabilities()
    assert process.terminated
    assert lease.released
    assert client._CAPABILITIES_CACHE is None
    _assert_capacity_restored()


def test_running_cancel_cleans_helper_and_admission(cached_runtime, monkeypatch):
    """Cancellation during execution rejects its result and frees every resource."""
    process, lease = _Process(), _Lease()

    def communicate(**kwargs):
        """Cancel the registered helper from its owner while it is running."""
        assert client.cancel('running-cancel')
        return b'{"ok": true, "result": {"ok": true}}', b''

    process.communicate = communicate
    monkeypatch.setattr(client, 'acquire_master_runtime_lock', lambda _path: lease)
    monkeypatch.setattr(client.subprocess, 'Popen', lambda *args, **kwargs: process)
    with pytest.raises(client.PB8StrategyExplorerCancelledError):
        client.snapshot({}, operation_id='running-cancel')
    assert process.terminated and lease.released
    _assert_capacity_restored()


@pytest.mark.parametrize('outcome', ['success', 'error', 'shutdown'])
def test_shared_budget_admits_before_actual_fingerprint_lock(cached_runtime, monkeypatch, outcome):
    """One helper and 15 scan-lock waiters consume the budget before any blocking scan."""
    import threading
    from concurrent.futures import ThreadPoolExecutor
    import pb8_config

    helper_entered, helper_release = threading.Event(), threading.Event()
    scans_entered = threading.Event()
    scan_calls, process_calls = [], []
    count_lock = threading.Lock()
    fingerprint = pb8_config._runtime_fingerprint
    monkeypatch.setattr(pb8_config, 'PBGDIR', str(cached_runtime))

    def scan(status):
        """Use the real shared scan lock against a synthetic empty runtime directory."""
        with count_lock:
            scan_calls.append(True)
            if len(scan_calls) == 15:
                scans_entered.set()
        result = fingerprint(status)
        if outcome == 'error':
            raise client.PB8ConfigurationError('synthetic scan failure')
        return result

    class Process(_Process):
        """Hold one helper while admitted metadata calls queue on the shared scan."""

        def communicate(self, input, timeout):
            """Delay only the unrelated snapshot; metadata helpers return immediately."""
            operation = json.loads(input)['operation']
            process_calls.append(operation)
            if operation == 'snapshot':
                helper_entered.set()
                assert helper_release.wait(5)
            return super().communicate(input, timeout)

    monkeypatch.setattr(client, '_runtime_fingerprint', scan)
    monkeypatch.setattr(client, 'acquire_master_runtime_lock', lambda _path: _Lease())
    monkeypatch.setattr(client.subprocess, 'Popen', lambda *args, **kwargs: Process())
    with ThreadPoolExecutor(16) as pool:
        helper = pool.submit(client.snapshot, {})
        assert helper_entered.wait(2)
        try:
            with pb8_config._fingerprint_lock:
                callers = [pool.submit(client.capabilities) for _ in range(15)]
                assert scans_entered.wait(3)
                assert not client._ADMISSION_SLOTS.acquire(blocking=False)
                with pytest.raises(client.PB8StrategyExplorerBusyError, match='request limit'):
                    client.capabilities()
                assert len(scan_calls) == 15  # rejection happens before scanning
                assert process_calls == ['snapshot']
                if outcome == 'shutdown':
                    client.shutdown()
                    client.startup()
        finally:
            helper_release.set()
        if outcome == 'shutdown':
            with pytest.raises(client.PB8StrategyExplorerCancelledError):
                helper.result(3)
        else:
            assert helper.result(3) == {'ok': True}
        for future in callers:
            if outcome == 'success':
                assert future.result(3) == {'ok': True}
            else:
                expected = client.PB8StrategyExplorerUnavailableError if outcome == 'error' else client.PB8StrategyExplorerBusyError
                with pytest.raises(expected):
                    future.result(3)
        if outcome != 'success':
            assert client._CAPABILITIES_CACHE is None
    _assert_capacity_restored()


def test_native_missing_ema_http_path_does_not_invent_missing_candles(cached_runtime, monkeypatch):
    """Helper failure stays HTTP 422; session fallback retains config and reports unverified candle availability."""
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from api.auth import require_auth

    detail = 'orchestrator compute_ideal_orders failed: MissingEma { symbol_idx: 0 }'
    config = {'backtest': {'exchanges': ['binance'], 'starting_balance': 4321},
              'bot': {'long': {}, 'short': {}}, 'live': {'approved_coins': {'long': ['BTC'], 'short': []}}}
    monkeypatch.setattr(api.explorer, 'capabilities', lambda *_args: {'canonical_config': config})
    monkeypatch.setattr(client, 'acquire_master_runtime_lock', lambda _path: _Lease())
    monkeypatch.setattr(client.subprocess, 'Popen', lambda *args, **kwargs: _Process(
        json.dumps({'ok': False, 'detail': detail}).encode()
    ))
    monkeypatch.setattr(api, '_log', lambda *args, **kwargs: None)
    app = FastAPI()
    app.include_router(api.router, prefix='/api/strategy-explorer-v8')
    app.dependency_overrides[require_auth] = lambda: _session('synthetic-ema')
    with TestClient(app) as http:
        failed = http.post('/api/strategy-explorer-v8/snapshot', json={'config': config})
        assert failed.status_code == 422
        assert failed.json()['detail'] == detail
        response = http.get('/api/strategy-explorer-v8/session')
    assert response.status_code == 200
    body = response.json()
    snapshot = body['snapshot']
    assert snapshot['ok'] is False
    assert snapshot['config'] == config
    assert snapshot['candles'] == []
    assert snapshot['market']['engine_status'] == 'PB8 snapshot unavailable'
    assert snapshot['market']['ohlcv_status'] == 'Candle availability unknown: native snapshot failed'
    assert snapshot['market']['metadata']['ohlcv']['rows'] is None
    assert detail in body['messages'][0]['text']
    assert 'No valid local PB8 candles' not in json.dumps(body)
    _assert_capacity_restored()
