"""Focused contracts for explicit PB8 8.6 HSL migration and cooldown metadata."""

from __future__ import annotations

import copy
from contextlib import nullcontext
import importlib
import json
import subprocess
import sys
from pathlib import Path
from types import ModuleType

import pytest

import pb8_config
import pb8_config_helper


def test_editor_fallback_is_limited_to_native_hsl_migration_errors(monkeypatch, tmp_path):
    """Launch loaders remain strict; only an editor can inspect legacy HSL."""
    calls = []
    monkeypatch.setattr(pb8_config, "_call_migration_helper", lambda op, **kw: calls.append((op, kw)) or {"config": {"old": True}, "hsl_migration_required": True})

    def rejected(_path):
        raise pb8_config.PB8ConfigurationError("use passivbot tool migrate-hsl")

    path = tmp_path / "config.json"
    assert pb8_config.load_pb8_editor_config(path, loader=rejected)["hsl_migration_required"]
    assert calls == [("load_hsl_editor", {"config_path": str(path)})]

    def invalid(_path):
        raise pb8_config.PB8ConfigurationError("invalid strategy")

    with pytest.raises(pb8_config.PB8ConfigurationError, match="invalid strategy"):
        pb8_config.load_pb8_editor_config(path, loader=invalid)
    assert len(calls) == 1


def test_helper_editor_rechecks_native_error_before_returning_raw(monkeypatch, tmp_path):
    """Raw editor access cannot bypass unrelated canonical validation failures."""
    source = tmp_path / "config.json"
    source.write_text('{"bot": {}}')

    def invalid(*_args, **_kwargs):
        raise ValueError("bad config version")

    monkeypatch.setattr(pb8_config_helper, "_load_pb8_modules", lambda _: {"load_prepared_config": invalid})
    with pytest.raises(ValueError, match="bad config version"):
        pb8_config_helper.handle({"operation": "load_hsl_editor", "config_path": str(source)})


def test_preview_uses_native_migration_and_preserves_source_and_pbgui(monkeypatch, tmp_path):
    """An explicit preview calls PB8's migration utility and never writes files."""
    source = tmp_path / "config.json"
    original = {"bot": {"long": {"hsl": {"enabled": True}}}, "pbgui": {"version": 4, "note": "keep"}}
    source.write_text(json.dumps(original))
    before = source.read_bytes()
    calls = []
    native = ModuleType("tools.migrate_hsl_config")

    def migrate(config, **kwargs):
        calls.append((copy.deepcopy(config), kwargs))
        assert "pbgui" not in config
        config["config_version"] = "v8.6.0"
        return config

    native.migrate = migrate
    monkeypatch.setitem(sys.modules, "tools.migrate_hsl_config", native)
    monkeypatch.setattr(pb8_config_helper, "_load_pb8_modules", lambda _: {})
    result = pb8_config_helper.handle({"operation": "migrate_hsl", "config_path": str(source), "restart_policies": {"long": "never"}})
    assert source.read_bytes() == before
    assert result["config"]["pbgui"] == original["pbgui"]
    assert result["config"]["config_version"] == "v8.6.0"
    assert calls[0][1] == {"restart_policies": {"long": "never"}, "portfolio": None, "base_config_path": str(source)}


def test_cooldown_override_metadata_includes_nullable_ceiling():
    """The official coin policy exposes all adaptive cooldown leaves."""
    side = {"entry_cooldown": {"base_duration_minutes": 0.05, "min_duration_minutes": 0, "max_duration_minutes": None, "weights_minutes": {"exposure_ratio": 2, "adverse_directionality": 3}}, "strategy": {"ema_anchor": {}}}
    policy = {"entry_cooldown": {"base_duration_minutes": True, "min_duration_minutes": True, "max_duration_minutes": True, "weights_minutes": {"exposure_ratio": True, "adverse_directionality": True}}, "strategy": {"ema_anchor": {}}}
    modules = {"normalize_hsl_signal_mode": lambda v: v, "normalize_strategy_kind": lambda v: v,
               "get_allowed_modifications": lambda **_: {"bot": {"long": policy, "short": policy}, "live": {}},
               "get_template_config": lambda: {"bot": {"long": side, "short": side}, "live": {}},
               "get_all_strategy_defaults": lambda: {}, "prepare_config": lambda c, **_: c, "sanitize": lambda c: c}
    fields = pb8_config_helper._coin_override_metadata(modules, {"strategy_kind": "ema_anchor"})["params"]["bot"]["long"]
    assert fields["entry_cooldown.max_duration_minutes"] == {"type": "number_or_null", "default": None}
    assert fields["entry_cooldown.weights_minutes.adverse_directionality"]["default"] == 3


@pytest.mark.parametrize("module_name", ["v8_instances", "backtest_v8", "optimize_v8"])
def test_authenticated_migration_endpoint_is_read_only_and_rejects_unsafe_overrides(monkeypatch, tmp_path, module_name):
    """Every editor validates stored paths before running the native preview."""
    from fastapi import FastAPI, HTTPException
    from fastapi.testclient import TestClient

    module = importlib.import_module("api." + module_name)
    path = tmp_path / 'alice' / 'config.json'
    path.parent.mkdir()
    path.write_text('{"bot": {}, "pbgui": {"version": 2}}')
    before = path.read_bytes()
    live = module_name == 'v8_instances'
    if live:
        monkeypatch.setattr(module, '_run_root', lambda: tmp_path)
    monkeypatch.setattr(module, '_config_path' if live else '_config_file', lambda _: path)
    monkeypatch.setattr(module, '_run_lock' if live else '_config_lock', nullcontext)
    monkeypatch.setattr(module, 'load_pb8_config', lambda _: json.loads(path.read_text()))
    calls = []
    monkeypatch.setattr(module, 'preview_pb8_hsl_migration', lambda source, body: calls.append((source, body)) or {'config': {'config_version': 'v8.6.0'}})
    app = FastAPI()
    prefix = '/api/v8' if live else '/api/' + module_name.replace('_', '-')
    app.include_router(module.router, prefix=prefix)
    endpoint = '/api/v8/instances/alice/migrate-hsl' if live else '/api/' + module_name.replace('_', '-') + '/configs/alice/migrate-hsl'
    choices = {'restart_policies': {'long': 'never', 'short': 'always'}}
    with TestClient(app) as client:
        assert client.post(endpoint, json=choices).status_code in (401, 403)
        app.dependency_overrides[module.require_auth] = lambda: None
        response = client.post(endpoint, json=choices)
        assert response.status_code == 200, response.text
        assert calls == [(path, choices)]
        assert path.read_bytes() == before

        marks = {'long': {'hsl': 'review'}, 'short': {}}
        monkeypatch.setattr(module, 'load_pb8_editor_config', lambda *args, **kwargs: {
            'config': {'bot': {}}, 'param_status': marks, 'migration_changes': [{'path': 'bot.long.hsl'}]})
        read_endpoint = '/api/v8/instances/alice/config' if live else prefix + '/configs/alice'
        opened = client.get(read_endpoint)
        assert opened.status_code == 200, opened.text
        assert opened.json()['param_status'] == marks
        assert opened.json()['migration_changes'] == [{'path': 'bot.long.hsl'}]
        assert path.read_bytes() == before

        def unsafe(*_args):
            raise HTTPException(status_code=400, detail='unsafe override path')

        monkeypatch.setattr(module, '_override_payloads_by_coin' if live else '_load_override_payloads', unsafe)
        assert client.post(endpoint, json=choices).status_code == 400
        assert len(calls) == 1
        assert path.read_bytes() == before


def test_editor_migration_reports_removed_hsl_leaves_without_marking_header(monkeypatch, tmp_path):
    """Exact deleted values reach the read-only diff, never the migrated config."""
    original = {"bot": {"long": {"hsl": {
        "enabled": True, "restart_after_red_policy": "always", "red_threshold": .141,
        "no_restart_drawdown_threshold": 1,
        "orange_tier_mode": "tp_only_with_active_entry_cancellation",
        "tier_ratios": {"orange": .75, "yellow": .5},
    }}}}
    snapshot = copy.deepcopy(original)
    native = ModuleType("tools.migrate_hsl_config")

    def migrate(source, **_kwargs):
        migrated = copy.deepcopy(source)
        hsl = migrated["bot"]["long"]["hsl"]
        for key in ("no_restart_drawdown_threshold", "orange_tier_mode", "tier_ratios"):
            del hsl[key]
        migrated["bot"]["long"]["forager"] = {"unilateralness_ema_span_1m": 60}
        return migrated

    native.migrate = migrate
    monkeypatch.setitem(sys.modules, "tools.migrate_hsl_config", native)
    result = pb8_config_helper._migrate_editor_hsl(original, tmp_path / "config.json", {})
    assert original == snapshot
    assert result["param_status"]["long"] == {
        "hsl.no_restart_drawdown_threshold": {"status": "removed", "before": 1},
        "hsl.orange_tier_mode": {"status": "removed", "before": "tp_only_with_active_entry_cancellation"},
        "hsl.tier_ratios.orange": {"status": "removed", "before": .75},
        "hsl.tier_ratios.yellow": {"status": "removed", "before": .5},
    }
    assert result["param_status"]["short"] == {}
    full_changes = result["param_status"]["config_changes"]
    assert full_changes["bot.long.forager.unilateralness_ema_span_1m"] == {
        "path": "bot.long.forager.unilateralness_ema_span_1m", "removed": False,
        "added": True, "before": None, "after": 60, "requires_choice": False,
    }
    assert len(full_changes) == 5
    assert "param_status" not in result["config"]
    assert len(result["migration_changes"]) == 4
    assert all(change["removed"] for change in result["migration_changes"])
    assert result["config"]["bot"]["long"]["hsl"] == {
        "enabled": True, "restart_after_red_policy": "always", "red_threshold": .141,
    }


@pytest.mark.local_runtime
@pytest.mark.parametrize("mode", ["coin", "pside", "unified"])
@pytest.mark.parametrize("policy", ["threshold", "always", "never"])
def test_installed_pb8_v86_native_migration_is_read_only(tmp_path, mode, policy):
    """Exercise installed PB8 code with synthetic configs only, never runtime data."""
    from pbgui_purefunc import pb8dir, pb8venv

    directory, python = pb8dir(), pb8venv()
    helper = Path(pb8_config_helper.__file__).resolve()

    def request(operation, **payload):
        completed = subprocess.run([python, str(helper)], cwd=directory,
                                   input=json.dumps({"pb8_dir": directory, "operation": operation, **payload}),
                                   capture_output=True, text=True, timeout=60)
        response = json.loads(completed.stdout)
        assert response["ok"], response.get("detail")
        return response["result"]

    config = request("default")["config"]
    if config["config_version"] != "v8.6.0":
        pytest.skip("Requires the v8.6.0 installed schema")
    config["config_version"] = "v8.5.0"
    config["live"].update(hsl_signal_mode=mode, hsl_engine="revised")
    config["bot"]["long"]["hsl"].update(enabled=True, restart_after_red_policy=policy)
    config["bot"]["long"]["hsl"]["tier_ratios"] = {"orange": .75, "yellow": .5}
    config["pbgui"] = {"note": "isolated native test"}
    if mode == "coin" and policy == "threshold":
        config["optimize"]["fixed_params"] = ["bot.long.forager.score_weights_ema_readiness", "bot.short.forager.score_weights_volume"]
        config["optimize"]["fixed_runtime_overrides"]["bot.long.hsl.restart_after_red_policy"] = "threshold"
    choices = {"restart_policies": {"long": "always", "short": "never"}}
    if mode == "unified":
        for side in ("long", "short"):
            config["optimize"]["bounds"][side].pop("hsl", None)
        config["optimize"]["fixed_runtime_overrides"] = {}
        choices = {"portfolio": {**config["bot"]["long"]["hsl"], "restart_after_red_policy": "never"}, "restart_policies": {"portfolio": "never"}}
    source = tmp_path / "legacy.json"
    source.write_text(json.dumps(config))
    before = source.read_bytes()
    draft = request("load_hsl_editor", config_path=str(source))
    assert not draft["hsl_migration_required"]
    assert draft["config"]["config_version"] == "v8.6.0"
    assert draft["migration_changes"]
    assert draft["param_status"]["config_changes"]["config_version"]["after"] == "v8.6.0"
    draft_hsl = draft["config"]["bot"]["hsl"] if mode == "unified" else draft["config"]["bot"]["long"]["hsl"]
    assert draft_hsl["restart_after_red_policy"] == (None if mode == "unified" or policy == "threshold" else policy)
    if mode == "unified":
        assert all(value is None for value in draft_hsl.values())
    else:
        assert draft_hsl["enabled"] is True
        if policy == "threshold":
            assert draft["param_status"]["long"]["hsl.restart_after_red_policy"] == "review_line"
    if mode == "unified" or policy == "threshold":
        invalid = subprocess.run([python, str(helper)], cwd=directory,
                                input=json.dumps({"pb8_dir": directory, "operation": "prepare", "config": draft["config"]}),
                                capture_output=True, text=True, timeout=60)
        assert not json.loads(invalid.stdout)["ok"]
    if mode == "coin" and policy == "threshold":
        assert draft["config"]["optimize"]["fixed_params"] == ["bot.long.forager.score_weights.ema_readiness", "bot.short.forager.score_weights.volume"]
        assert all(change["path"].startswith(("bot.long.hsl.", "bot.short.hsl.", "bot.hsl.", "live.hsl_")) for change in draft["migration_changes"])
        assert "hsl" not in draft["param_status"]["long"]
        assert draft["param_status"]["long"]["hsl.tier_ratios.orange"] == {"status": "removed", "before": .75}
        assert draft["param_status"]["long"]["hsl.tier_ratios.yellow"] == {"status": "removed", "before": .5}
        assert "bot.long.hsl.restart_after_red_policy" not in draft["config"]["optimize"]["fixed_runtime_overrides"]
        draft_hsl["restart_after_red_policy"] = "never"
        assert request("prepare", config=draft["config"])["config"]["bot"]["long"]["hsl"]["restart_after_red_policy"] == "never"
    assert source.read_bytes() == before
    result = request("migrate_hsl", config_path=str(source), **choices)["config"]
    assert source.read_bytes() == before
    assert result["config_version"] == "v8.6.0"
    assert result["pbgui"] == config["pbgui"]
    assert "hsl_engine" not in result["live"]
    hsl = result["bot"]["hsl"] if mode == "unified" else result["bot"]["long"]["hsl"]
    assert hsl["restart_after_red_policy"] == ("never" if mode == "unified" else "always")
    assert result["bot"]["long"]["entry_cooldown"]["max_duration_minutes"] is None


@pytest.mark.parametrize('filename', ['../secret.json', '/tmp/secret.json', 'bad\\name.json', 'bad\x00.json'])
def test_native_migration_rejects_outside_override_references(tmp_path, filename):
    """Native file resolution cannot escape an editor's managed config bundle."""
    with pytest.raises(ValueError, match='Invalid migration override filename'):
        pb8_config_helper._validate_migration_references(
            {'coin_overrides': {'BTC': {'override_config_path': filename}}}, tmp_path / 'config.json')


def test_native_migration_rejects_symlink_overrides(tmp_path):
    """A valid-looking override filename must not follow a symlink."""
    target = tmp_path / 'outside.json'
    target.write_text('{}')
    (tmp_path / 'BTC.json').symlink_to(target)
    with pytest.raises(ValueError, match='regular file'):
        pb8_config_helper._validate_migration_references(
            {'coin_overrides': {'BTC': {'override_config_path': 'BTC.json'}}}, tmp_path / 'config.json')
