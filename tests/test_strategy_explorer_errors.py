"""Offline HTTP contracts for PB7 Strategy Explorer configuration failures."""

import copy
import time

import pandas as pd
import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from api import pb7_bridge, strategy_explorer as api, strategy_explorer_calc as calc
from api.auth import SessionToken, require_auth
from api.strategy_explorer_errors import StrategyExplorerConfigurationError, safe_failure_reason
import logging_helpers
import pb7_config

ROUTES = [
    ("GET", "/session"), ("GET", "/markets"), ("POST", "/markets"),
    ("POST", "/snapshot"), ("POST", "/simulate"), ("POST", "/compare"),
    ("POST", "/movie/frames"),
]
REJECTION = (
    "bot.long.ema_span_0 is a legacy flat strategy field. "
    "Run `passivbot tool migrate-config-v7` for v7 configs."
)
PUBLIC_REJECTION = (
    "Legacy flat strategy fields are incompatible with the configured strategy. "
    "Fields: bot.long.ema_span_0. For v7 configs, run `passivbot tool migrate-config-v7`."
)
PUBLIC_RUNTIME = "PB7 configuration runtime unavailable. Check the configured PB7 path and environment."


@pytest.fixture
def explorer(monkeypatch):
    """Use only synthetic configs, in-memory progress and isolated HTTP auth."""
    monkeypatch.setattr(calc, "default_strategy_config", lambda: {"bot": {"long": {}}})
    monkeypatch.setattr(api, "movie_builder_status", lambda: {"message": "Synthetic movie"})
    monkeypatch.setattr(pb7_bridge, "get_hsl_signal_modes", lambda: ["pside"])
    for store in ("_draft_store", "_movie_progress", "_compare_progress", "_simulation_progress"):
        monkeypatch.setattr(api, store, {})
    logs = []
    monkeypatch.setattr(api, "_log", lambda *args, **kwargs: logs.append((args, kwargs)))
    app = FastAPI()
    app.include_router(api.router)
    app.dependency_overrides[require_auth] = lambda: SessionToken(
        token="synthetic", user_id="synthetic", created_at=1, expires_at=time.time() + 3600
    )
    with TestClient(app, raise_server_exceptions=False) as client:
        yield client, logs


@pytest.mark.parametrize("method,path", ROUTES)
@pytest.mark.parametrize("config", [{}, {"bot": {"long": {}}}])
@pytest.mark.parametrize("error_type,message,status", [
    (ValueError, REJECTION, 422),
    (Exception, "failed to format config: unknown flavor", 422),
    (pb7_config.PB7ConfigurationError, "Synthetic PB7 runtime missing", 503),
    (RuntimeError, "synthetic calculation fault", 500),
    (TypeError, "config must be a dict", 500),
    (LookupError, "synthetic internal fault", 500),
])
def test_all_routes_classify_preparation_failures(
    explorer, monkeypatch, method, path, config, error_type, message, status
):
    """Only canonical preparation rejection and runtime unavailability are public."""
    client, logs = explorer

    def fail(*args, **kwargs):
        """Inject at the real preparation boundary before any runtime reads."""
        raise error_type(message)

    monkeypatch.setattr(calc, "prepare_pb7_config_dict", fail)
    body = {"config": config, "progress_id": "synthetic-progress"}
    response = client.request(method, path, **({"json": body} if method == "POST" else {}))
    assert response.status_code == status
    detail = response.json()["detail"]
    if status == 422:
        public = PUBLIC_REJECTION if error_type is ValueError else "failed to format config: unknown flavor."
        assert public in detail
    elif status == 503:
        public = PUBLIC_RUNTIME
        assert public in detail
    else:
        public = "Strategy Explorer operation failed."
        assert detail == public
    assert len(logs) == 1
    assert logs[0][1]["level"] == ("ERROR" if status == 500 else "WARNING")
    if path in ("/simulate", "/compare", "/movie/frames"):
        progress_path = path.replace("/frames", "") + "/progress/synthetic-progress"
        progress = client.get(progress_path).json()
        assert progress["done"] and not progress["ok"]
        assert progress["progress"] == 1
        assert progress["error"] == public


@pytest.mark.parametrize("method,path", ROUTES)
def test_existing_http_errors_keep_status_detail_and_headers(explorer, monkeypatch, method, path):
    """An existing HTTPException is never converted to a different failure."""
    client, _ = explorer

    def fail(*args, **kwargs):
        """Represent an existing restart blocker or authorization boundary."""
        raise HTTPException(409, "Synthetic blocker", headers={"X-Synthetic": "retained"})

    monkeypatch.setattr(calc, "prepare_pb7_config_dict", fail)
    response = client.request(method, path, **({"json": {"config": {}}} if method == "POST" else {}))
    assert response.status_code == 409
    assert response.json() == {"detail": "Synthetic blocker"}
    assert response.headers["X-Synthetic"] == "retained"


@pytest.mark.parametrize("method,path", ROUTES)
def test_success_payloads_and_progress_remain_unchanged(explorer, monkeypatch, method, path):
    """The HTTP boundary retains full successful payloads and final progress."""
    client, logs = explorer
    payload = {"ok": True, "messages": [], "config": {"bot": {}}, "synthetic": "x" * 22000}
    for function in ("build_strategy_snapshot", "market_options", "build_strategy_simulation", "build_strategy_compare", "build_movie_frames"):
        monkeypatch.setattr(calc, function, lambda *args, **kwargs: copy.deepcopy(payload))
    response = client.request(method, path, **({"json": {"config": {}, "progress_id": "ok"}} if method == "POST" else {}))
    assert response.status_code == 200
    result = response.json()
    assert (result["snapshot"] if path == "/session" else result) == payload
    if path in ("/simulate", "/compare", "/movie/frames"):
        progress = client.get(path.replace("/frames", "") + "/progress/ok").json()
        assert progress["done"] and progress["ok"] and progress["error"] == ""
    assert logs == []


@pytest.mark.parametrize("query", ["", "?draft_id=", "?draft_id=&result_path=", "?draft_id=synthetic-draft"])
def test_session_queries_and_drafts_use_same_error_boundary(explorer, monkeypatch, query):
    """Cold starts, empty parameters and draft configs expose the same rejection."""
    client, _ = explorer
    api._draft_store["synthetic-draft"] = (time.time(), {"bot": {"long": {}}})

    def fail(*args, **kwargs):
        """Reject preparation for defaults and drafts alike."""
        raise ValueError(REJECTION)

    monkeypatch.setattr(calc, "prepare_pb7_config_dict", fail)
    assert client.get("/session" + query).status_code == 422


@pytest.mark.parametrize("stage", ["default", "handoff"])
@pytest.mark.parametrize("runtime", [False, True])
def test_session_guards_default_and_handoff(explorer, monkeypatch, tmp_path, stage, runtime):
    """The failure boundary begins before the initial snapshot call."""
    client, _ = explorer

    def fail(*args, **kwargs):
        """Use typed expected failures at the earlier bootstrap stages."""
        if runtime:
            raise pb7_config.PB7ConfigurationError("Synthetic runtime unavailable")
        raise StrategyExplorerConfigurationError(ValueError(REJECTION))

    def snapshot(*args, **kwargs):
        """Fail the test if bootstrap continued after an earlier rejection."""
        pytest.fail("snapshot must not run")

    monkeypatch.setattr(calc, "build_strategy_snapshot", snapshot)
    query = ""
    if stage == "default":
        monkeypatch.setattr(calc, "default_strategy_config", fail)
    else:
        monkeypatch.setattr(calc.strategy_explorer_core, "_resolve_safe_backtest_dir", lambda _: str(tmp_path))
        monkeypatch.setattr(calc, "backtest_result_handoff_options", fail)
        query = "?result_path=synthetic"
    assert client.get("/session" + query).status_code == (503 if runtime else 422)


def test_real_handoff_normalization_is_guarded(explorer, monkeypatch, tmp_path):
    """Exercise the actual handoff normalizer, before it reads result files."""
    client, _ = explorer
    monkeypatch.setattr(calc.strategy_explorer_core, "_resolve_safe_backtest_dir", lambda _: str(tmp_path))

    def fail(*args, **kwargs):
        """Reject the config at the handoff's own canonical preparation call."""
        raise ValueError(REJECTION)

    monkeypatch.setattr(calc, "prepare_pb7_config_dict", fail)
    response = client.get("/session?result_path=synthetic")
    assert response.status_code == 422
    assert PUBLIC_REJECTION in response.json()["detail"]


def test_result_loading_preserves_http_errors(explorer, monkeypatch, tmp_path):
    """The existing result-load fallback must not swallow explicit HTTP errors."""
    client, _ = explorer
    (tmp_path / "config.json").write_text("{}", encoding="utf-8")
    monkeypatch.setattr(calc.strategy_explorer_core, "_resolve_safe_backtest_dir", lambda _: str(tmp_path))

    def fail(*args, **kwargs):
        """Inject an existing boundary failure before result normalization."""
        raise HTTPException(409, "Synthetic result blocker")

    monkeypatch.setattr(pb7_config, "load_pb7_config", fail)
    response = client.get("/session?result_path=synthetic")
    assert response.status_code == 409
    assert response.json() == {"detail": "Synthetic result blocker"}


def test_snapshot_missing_config_retains_400(explorer):
    """Missing config keeps the pre-existing body validation response."""
    client, _ = explorer
    response = client.post("/snapshot", json={})
    assert response.status_code == 400
    assert response.json() == {"detail": "config must be an object"}


def test_errors_outside_preparation_are_not_config_rejections(explorer, monkeypatch):
    """A ValueError during snapshot construction is an internal error."""
    client, _ = explorer
    monkeypatch.setattr(calc, "prepare_pb7_config_dict", lambda config, **kwargs: config)

    def fail(*args, **kwargs):
        """Fail after canonical config preparation, without runtime access."""
        raise ValueError("synthetic data calculation failure")

    monkeypatch.setattr(calc, "_build_data", fail)
    response = client.post("/snapshot", json={"config": {}})
    assert response.status_code == 500
    assert response.json() == {"detail": "Strategy Explorer operation failed."}


def test_movie_stop_keeps_progress_and_avoids_error_traceback(explorer, monkeypatch):
    """Use the actual Movie Builder cancellation check with synthetic candles."""
    client, logs = explorer
    monkeypatch.setattr(calc, "prepare_pb7_config_dict", lambda config, **kwargs: config)

    def data(*args, **kwargs):
        """Simulate a concurrent Stop after the controller created its record."""
        api._movie_progress["stop"]["cancelled"] = True
        return None, {}, pd.DataFrame({"close": [100.0]})

    monkeypatch.setattr(calc, "_build_data", data)
    response = client.post("/movie/frames", json={"config": {}, "progress_id": "stop"})
    assert response.status_code == 500  # Preserve the existing failure status on Stop.
    assert response.json() == {"detail": "Movie Builder stopped."}
    progress = client.get("/movie/progress/stop").json()
    assert progress["done"] and progress["cancelled"]
    assert progress["error"] == "Movie Builder stopped."
    assert len(logs) == 1 and logs[0][1] == {"level": "INFO"}


@pytest.mark.parametrize("stage", ["load", "strip"])
def test_shared_pipeline_rejections_are_wrapped_only_for_explorer(monkeypatch, tmp_path, stage):
    """Editor/Optimize callers retain their original ValueError instance."""
    error = ValueError(REJECTION)

    def reject(*args, **kwargs):
        """Represent upstream loader or metadata-strip rejection."""
        raise error

    loader = reject if stage == "load" else lambda *args, **kwargs: {}
    strip = reject if stage == "strip" else copy.deepcopy
    monkeypatch.setattr(pb7_config, "_get_pb7_config_api", lambda: (loader, strip))
    monkeypatch.setattr(pb7_config.tempfile, "tempdir", str(tmp_path))
    with pytest.raises(ValueError) as direct:
        pb7_config.prepare_pb7_config_dict({})
    assert direct.value is error
    with pytest.raises(StrategyExplorerConfigurationError) as wrapped:
        calc.normalize_strategy_config({})
    assert wrapped.value.__cause__ is error
    assert list(tmp_path.iterdir()) == []


def test_wrapper_bookkeeping_value_error_is_not_configuration_rejection(explorer, monkeypatch, tmp_path):
    """Do not mistake a failure after upstream loading for config validation."""
    client, _ = explorer

    def finalize(*args, **kwargs):
        """Inject a programming fault in the shared wrapper's own bookkeeping."""
        raise ValueError("synthetic finalization bug")

    monkeypatch.setattr(pb7_config, "_get_pb7_config_api", lambda: (lambda *args, **kwargs: {}, copy.deepcopy))
    monkeypatch.setattr(pb7_config, "_finalize_prepared_pb7_config", finalize)
    monkeypatch.setattr(pb7_config.tempfile, "tempdir", str(tmp_path))
    response = client.post("/snapshot", json={"config": {}})
    assert response.status_code == 500
    assert response.json() == {"detail": "Strategy Explorer operation failed."}


def test_expected_details_and_logs_remove_secrets_and_config_dump(explorer, monkeypatch):
    """Keep cause/migration guidance without rendering untrusted config values."""
    client, logs = explorer

    def reject(*args, **kwargs):
        """Include synthetic secret forms and a complete untrusted config repr."""
        raise ValueError(REJECTION + " api_key=synthetic-secret config={'bot': 'synthetic-config-dump'}")

    monkeypatch.setattr(calc, "prepare_pb7_config_dict", reject)
    response = client.post("/snapshot", json={"config": {}})
    assert response.status_code == 422
    assert PUBLIC_REJECTION in response.json()["detail"]
    rendered = response.text + str(logs)
    assert "synthetic-secret" not in rendered and "synthetic-config-dump" not in rendered
    assert logging_helpers.LOG_GROUPS[api.SERVICE] == "PBGui"


def test_long_field_lists_keep_migration_hint():
    """Bound long diagnostics without removing the actionable upstream command."""
    reason = safe_failure_reason(ValueError("bot.long.field, " * 200 + REJECTION))
    assert len(reason) < 2100
    assert "passivbot tool migrate-config-v7" in reason


def test_unexpected_diagnostics_do_not_log_exception_payload(explorer, monkeypatch):
    """An internal exception cannot dump an entire config into logs or polling."""
    client, logs = explorer
    payload = {"bot": {"synthetic_private_config": 123}, "api_key": "synthetic-internal-secret"}

    def fail(*args, **kwargs):
        """Represent an upstream programming fault with a config in its text."""
        raise RuntimeError(str(payload))

    monkeypatch.setattr(calc, "prepare_pb7_config_dict", fail)
    response = client.post("/simulate", json={"config": {}, "progress_id": "internal"})
    assert response.status_code == 500
    rendered = response.text + client.get("/simulate/progress/internal").text + str(logs)
    assert "synthetic-internal-secret" not in rendered
    assert "synthetic_private_config" not in rendered
    assert "traceback" in logs[0][1]["meta"]


@pytest.mark.parametrize("method,path", ROUTES)
@pytest.mark.parametrize("error_type", [ValueError, pb7_config.PB7ConfigurationError])
@pytest.mark.parametrize("diagnostic,forbidden", [
    ("Invalid passphrase='synthetic first second third'", ["synthetic", "first", "second", "third"]),
    ('Invalid password="synthetic first\\\" second third"', ["synthetic", "first", "second", "third"]),
    ("could not convert string to float: 'synthetic-bare-sensitive-value'", ["synthetic-bare-sensitive-value"]),
    ("invalid literal for int() with base 10: 'synthetic-int-sensitive-value'", ["synthetic-int-sensitive-value"]),
    (REJECTION + " config={'api_key': 'synthetic config dump'}", ["synthetic", "config dump", "api_key"]),
])
def test_upstream_values_never_reach_responses_progress_or_logs(
    explorer, monkeypatch, tmp_path, method, path, error_type, diagnostic, forbidden
):
    """Real canonical loading can reject arbitrary quoted or unlabelled values."""
    client, logs = explorer

    def reject(*args, **kwargs):
        """Inject at the upstream loader, preserving the real wrapper traceback."""
        raise error_type(diagnostic)

    monkeypatch.setattr(pb7_config, "_get_pb7_config_api", lambda: (reject, copy.deepcopy))
    monkeypatch.setattr(pb7_config.tempfile, "tempdir", str(tmp_path))
    body = {"config": {}, "progress_id": "redaction"}
    response = client.request(method, path, **({"json": body} if method == "POST" else {}))
    assert response.status_code == (422 if error_type is ValueError else 503)
    rendered = response.text + str(logs)
    if path in ("/simulate", "/compare", "/movie/frames"):
        progress = client.get(path.replace("/frames", "") + "/progress/redaction").json()
        assert progress["done"] and not progress["ok"]
        rendered += str(progress)
    for value in forbidden:
        assert value not in rendered
    if diagnostic.startswith(REJECTION):
        assert "bot.long.ema_span_0" in rendered
        assert "passivbot tool migrate-config-v7" in rendered
    assert len(logs) == 1 and logs[0][1]["level"] == "WARNING"


def test_public_fields_cannot_be_derived_from_arbitrary_input_names():
    """Only the shipped PB7 parameter identifiers may appear in diagnostics."""
    reason = safe_failure_reason(ValueError("bot.long.synthetic_sensitive_field: synthetic_sensitive_value"))
    assert "synthetic_sensitive" not in reason
    assert reason == "Configuration rejected by the PB7 pipeline."
